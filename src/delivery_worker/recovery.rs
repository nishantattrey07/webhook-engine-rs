use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx::{PgPool, Row};

use super::{
    retry_policy::{FinalDeliveryState, decide_final_delivery_state, retry_delay},
    trace::{append_trace, append_trace_in_tx},
};

pub(super) async fn recover_stale_queued(pool: &PgPool) -> Result<u64, sqlx::Error> {
    let rows = sqlx::query(
        "UPDATE webhook_deliveries
         SET status = 'retrying',
             next_attempt_at = NOW(),
             last_queue_error = COALESCE(last_queue_error, 'queued delivery was not claimed in time'),
             queued_at = NULL,
             queue_token = NULL,
             published_at = NULL,
             redis_message_id = NULL,
             updated_at = NOW()
         WHERE status = 'queued'
           AND queued_at < NOW() - INTERVAL '30 seconds'
         RETURNING delivery_id, event_id",
    )
    .fetch_all(pool)
    .await?;

    for row in &rows {
        let delivery_id: i64 = row.get("delivery_id");
        let event_id: i64 = row.get("event_id");

        append_trace(
            pool,
            delivery_id,
            event_id,
            "queued_recovered",
            "retrying",
            "Queued delivery was not claimed; returned to retry queue",
            json!({ "reason": "queue_timeout" }),
        )
        .await?;
    }

    if !rows.is_empty() {
        tracing::warn!(count = rows.len(), "recovered stale queued deliveries");
    }

    Ok(rows.len() as u64)
}

pub(super) async fn recover_stuck_processing(pool: &PgPool) -> Result<u64, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT delivery_id
         FROM webhook_deliveries
         WHERE status = 'processing'
           AND processing_lease_expires_at IS NOT NULL
           AND processing_lease_expires_at <= NOW()
         ORDER BY processing_lease_expires_at, delivery_id",
    )
    .fetch_all(pool)
    .await?;

    let mut recovered = 0;
    for row in rows {
        let delivery_id: i64 = row.get("delivery_id");
        if recover_processing_delivery(pool, delivery_id).await? {
            recovered += 1;
        }
    }

    if recovered > 0 {
        tracing::warn!(count = recovered, "recovered expired processing deliveries");
    }

    Ok(recovered)
}

async fn recover_processing_delivery(pool: &PgPool, delivery_id: i64) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let Some(row) = sqlx::query(
        "SELECT
             d.delivery_id,
             d.event_id,
             d.max_attempts,
             d.current_attempt_id,
             COALESCE(current_attempt.attempt_id, latest_attempt.attempt_id) AS attempt_id,
             COALESCE(current_attempt.attempt_count, latest_attempt.attempt_count) AS attempt_count,
             COALESCE(current_attempt.outcome, latest_attempt.outcome) AS outcome,
             COALESCE(current_attempt.completed_at, latest_attempt.completed_at) AS completed_at
         FROM webhook_deliveries d
         LEFT JOIN delivery_attempts current_attempt
            ON current_attempt.attempt_id = d.current_attempt_id
         LEFT JOIN LATERAL (
            SELECT attempt_id, attempt_count, outcome, completed_at
            FROM delivery_attempts
            WHERE delivery_id = d.delivery_id
            ORDER BY attempt_count DESC, attempt_id DESC
            LIMIT 1
         ) latest_attempt
            ON current_attempt.attempt_id IS NULL
         WHERE d.delivery_id = $1
           AND d.status = 'processing'
           AND d.processing_lease_expires_at IS NOT NULL
           AND d.processing_lease_expires_at <= NOW()
         FOR UPDATE OF d",
    )
    .bind(delivery_id)
    .fetch_optional(&mut *tx)
    .await?
    else {
        tx.commit().await?;
        return Ok(false);
    };

    let event_id: i64 = row.get("event_id");
    let max_attempts: i64 = row.get("max_attempts");
    let current_attempt_id: Option<i64> = row.get("current_attempt_id");
    let attempt_id: Option<i64> = row.get("attempt_id");
    let attempt_count: Option<i64> = row.get("attempt_count");
    let mut outcome: Option<String> = row.get("outcome");
    let completed_at: Option<DateTime<Utc>> = row.get("completed_at");

    if let (Some(current_attempt_id), Some(attempt_id), Some(current_outcome), None) = (
        current_attempt_id,
        attempt_id,
        outcome.as_deref(),
        completed_at,
    ) && current_attempt_id == attempt_id
        && current_outcome == "unknown"
    {
        sqlx::query(
            "UPDATE delivery_attempts
             SET outcome = 'abandoned',
                 error_message = COALESCE(error_message, 'processing lease expired before attempt completed'),
                 completed_at = COALESCE(completed_at, NOW())
             WHERE attempt_id = $1
               AND outcome = 'unknown'
               AND completed_at IS NULL",
        )
        .bind(attempt_id)
        .execute(&mut *tx)
        .await?;
        outcome = Some("abandoned".to_string());
    }

    let attempt_count = attempt_count.unwrap_or(0);
    let outcome = outcome.as_deref().unwrap_or("unknown");
    let final_state = decide_final_delivery_state(outcome, attempt_count, max_attempts);

    match final_state {
        FinalDeliveryState::Delivered => {
            sqlx::query(
                "UPDATE webhook_deliveries
                 SET status = 'delivered',
                     next_attempt_at = NULL,
                     last_error = NULL,
                     processing_started_at = NULL,
                     processing_lease_token = NULL,
                     processing_lease_expires_at = NULL,
                     current_attempt_id = NULL,
                     processing_worker_id = NULL,
                     queue_token = NULL,
                     final_state_at = NOW(),
                     updated_at = NOW()
                 WHERE delivery_id = $1",
            )
            .bind(delivery_id)
            .execute(&mut *tx)
            .await?;

            append_trace_in_tx(
                &mut tx,
                delivery_id,
                event_id,
                "processing_recovered",
                "succeeded",
                "Recovered expired processing delivery as delivered",
                json!({ "reason": "processing_lease_expired", "attempt": attempt_count }),
            )
            .await?;
        }
        FinalDeliveryState::Retrying => {
            let next_attempt_at = Utc::now() + retry_delay(attempt_count.max(1));
            sqlx::query(
                "UPDATE webhook_deliveries
                 SET status = 'retrying',
                     next_attempt_at = $2,
                     last_error = COALESCE(last_error, 'processing lease expired before attempt completed'),
                     processing_started_at = NULL,
                     processing_lease_token = NULL,
                     processing_lease_expires_at = NULL,
                     current_attempt_id = NULL,
                     processing_worker_id = NULL,
                     queued_at = NULL,
                     queue_token = NULL,
                     published_at = NULL,
                     redis_message_id = NULL,
                     updated_at = NOW()
                 WHERE delivery_id = $1",
            )
            .bind(delivery_id)
            .bind(next_attempt_at)
            .execute(&mut *tx)
            .await?;

            append_trace_in_tx(
                &mut tx,
                delivery_id,
                event_id,
                "processing_recovered",
                "retrying",
                "Processing lease expired; delivery returned to retry queue",
                json!({
                    "reason": "processing_lease_expired",
                    "attempt": attempt_count,
                    "next_attempt_at": next_attempt_at
                }),
            )
            .await?;
        }
        FinalDeliveryState::DeadLettered => {
            sqlx::query(
                "UPDATE webhook_deliveries
                 SET status = 'dead_lettered',
                     next_attempt_at = NULL,
                     last_error = COALESCE(last_error, 'processing lease expired and attempts are exhausted'),
                     processing_started_at = NULL,
                     processing_lease_token = NULL,
                     processing_lease_expires_at = NULL,
                     current_attempt_id = NULL,
                     processing_worker_id = NULL,
                     queue_token = NULL,
                     final_state_at = NOW(),
                     updated_at = NOW()
                 WHERE delivery_id = $1",
            )
            .bind(delivery_id)
            .execute(&mut *tx)
            .await?;

            append_trace_in_tx(
                &mut tx,
                delivery_id,
                event_id,
                "processing_recovered",
                "dead_lettered",
                "Processing lease expired; delivery dead-lettered",
                json!({
                    "reason": "processing_lease_expired",
                    "attempt": attempt_count,
                    "outcome": outcome
                }),
            )
            .await?;
        }
    }

    tx.commit().await?;
    Ok(true)
}
