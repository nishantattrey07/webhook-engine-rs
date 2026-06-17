use chrono::Utc;
use serde_json::json;
use sqlx::PgPool;

use super::{
    retry_policy::{
        DeliveryOutcome, FinalDeliveryState, decide_final_delivery_state, is_retryable_outcome,
        retry_delay,
    },
    trace::append_trace_in_tx,
    types::{ClaimedDelivery, DeliveryResult},
};

pub(super) async fn finalize_attempt_and_delivery(
    pool: &PgPool,
    delivery: &ClaimedDelivery,
    result: &DeliveryResult,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    let guard = sqlx::query(
        "SELECT delivery_id
         FROM webhook_deliveries
         WHERE delivery_id = $1
           AND status = 'processing'
           AND current_attempt_id = $2
           AND processing_lease_token = $3::uuid
         FOR UPDATE",
    )
    .bind(delivery.delivery_id)
    .bind(delivery.attempt_id)
    .bind(&delivery.processing_lease_token)
    .fetch_optional(&mut *tx)
    .await?;

    if guard.is_none() {
        tx.rollback().await?;
        tracing::warn!(
            delivery_id = delivery.delivery_id,
            attempt_id = delivery.attempt_id,
            attempt = delivery.attempt_count,
            "stale worker completion ignored because delivery fence no longer matches"
        );
        return Ok(());
    }

    sqlx::query(
        "UPDATE delivery_attempts
         SET http_status = $2,
             outcome = $3,
             error_message = $4,
             response_body_sample = $5,
             completed_at = NOW()
         WHERE attempt_id = $1
           AND delivery_id = $6",
    )
    .bind(delivery.attempt_id)
    .bind(result.http_status.map(|status| status as i16))
    .bind(result.outcome.as_db_str())
    .bind(delivery_error(result))
    .bind(&result.response_body_sample)
    .bind(delivery.delivery_id)
    .execute(&mut *tx)
    .await?;

    match decide_final_delivery_state(
        result.outcome.as_db_str(),
        delivery.attempt_count,
        delivery.max_attempts,
    ) {
        FinalDeliveryState::Delivered => {
            let update = sqlx::query(
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
                 WHERE delivery_id = $1
                   AND status = 'processing'
                   AND current_attempt_id = $2
                   AND processing_lease_token = $3::uuid",
            )
            .bind(delivery.delivery_id)
            .bind(delivery.attempt_id)
            .bind(&delivery.processing_lease_token)
            .execute(&mut *tx)
            .await?;
            if update.rows_affected() == 0 {
                tx.rollback().await?;
                tracing::warn!(
                    delivery_id = delivery.delivery_id,
                    attempt_id = delivery.attempt_id,
                    "stale worker delivered transition ignored after guarded update matched zero rows"
                );
                return Ok(());
            }

            append_trace_in_tx(
                &mut tx,
                delivery.delivery_id,
                delivery.event_id,
                "delivered",
                "succeeded",
                "Delivery succeeded",
                json!({
                    "attempt": delivery.attempt_count,
                    "max_attempts": delivery.max_attempts,
                    "outcome": result.outcome.as_db_str(),
                    "http_status": result.http_status
                }),
            )
            .await?;
        }
        FinalDeliveryState::Retrying => {
            let retry_backoff = retry_delay(delivery.attempt_count);
            let next_attempt_at = Utc::now() + retry_backoff;
            let error_message = delivery_error(result);
            let update = sqlx::query(
                "UPDATE webhook_deliveries
                 SET status = 'retrying',
                     next_attempt_at = $4,
                     last_error = $5,
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
                 WHERE delivery_id = $1
                   AND status = 'processing'
                   AND current_attempt_id = $2
                   AND processing_lease_token = $3::uuid",
            )
            .bind(delivery.delivery_id)
            .bind(delivery.attempt_id)
            .bind(&delivery.processing_lease_token)
            .bind(next_attempt_at)
            .bind(&error_message)
            .execute(&mut *tx)
            .await?;
            if update.rows_affected() == 0 {
                tx.rollback().await?;
                tracing::warn!(
                    delivery_id = delivery.delivery_id,
                    attempt_id = delivery.attempt_id,
                    "stale worker retry transition ignored after guarded update matched zero rows"
                );
                return Ok(());
            }

            append_trace_in_tx(
                &mut tx,
                delivery.delivery_id,
                delivery.event_id,
                "retry_scheduled",
                "retrying",
                "Retry scheduled",
                json!({
                    "attempt": delivery.attempt_count,
                    "max_attempts": delivery.max_attempts,
                    "attempts_remaining": delivery.max_attempts.saturating_sub(delivery.attempt_count),
                    "outcome": result.outcome.as_db_str(),
                    "http_status": result.http_status,
                    "error": error_message,
                    "next_attempt_at": next_attempt_at,
                    "retry_delay_ms": retry_backoff.num_milliseconds()
                }),
            )
            .await?;
        }
        FinalDeliveryState::DeadLettered => {
            let error_message = delivery_error(result);
            let terminal_reason = if is_retryable_outcome(result.outcome.as_db_str())
                && delivery.attempt_count >= delivery.max_attempts
            {
                "attempts_exhausted"
            } else {
                "permanent_failure"
            };
            let update = sqlx::query(
                "UPDATE webhook_deliveries
                 SET status = 'dead_lettered',
                     next_attempt_at = NULL,
                     last_error = $4,
                     processing_started_at = NULL,
                     processing_lease_token = NULL,
                     processing_lease_expires_at = NULL,
                     current_attempt_id = NULL,
                     processing_worker_id = NULL,
                     queue_token = NULL,
                     final_state_at = NOW(),
                     updated_at = NOW()
                 WHERE delivery_id = $1
                   AND status = 'processing'
                   AND current_attempt_id = $2
                   AND processing_lease_token = $3::uuid",
            )
            .bind(delivery.delivery_id)
            .bind(delivery.attempt_id)
            .bind(&delivery.processing_lease_token)
            .bind(&error_message)
            .execute(&mut *tx)
            .await?;
            if update.rows_affected() == 0 {
                tx.rollback().await?;
                tracing::warn!(
                    delivery_id = delivery.delivery_id,
                    attempt_id = delivery.attempt_id,
                    "stale worker dead-letter transition ignored after guarded update matched zero rows"
                );
                return Ok(());
            }

            append_trace_in_tx(
                &mut tx,
                delivery.delivery_id,
                delivery.event_id,
                "dead_lettered",
                "dead_lettered",
                "Delivery dead-lettered",
                json!({
                    "attempt": delivery.attempt_count,
                    "max_attempts": delivery.max_attempts,
                    "outcome": result.outcome.as_db_str(),
                    "http_status": result.http_status,
                    "error": error_message,
                    "terminal_reason": terminal_reason
                }),
            )
            .await?;
        }
    }

    tx.commit().await?;
    Ok(())
}

fn delivery_error(result: &DeliveryResult) -> Option<String> {
    result
        .error_message
        .clone()
        .or_else(|| result.outcome.error_message())
        .or_else(|| match result.outcome {
            DeliveryOutcome::Success => None,
            _ => Some(result.outcome.as_db_str().to_string()),
        })
}
