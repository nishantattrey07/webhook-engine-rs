use chrono::Utc;
use redis::aio::ConnectionManager;
use serde_json::json;
use sqlx::{PgPool, Row};

use super::{
    config::WorkerConfig,
    recovery::{recover_stale_queued, recover_stuck_processing},
    redis_io::{
        connect_redis, ensure_consumer_group, publish_delivery, trim_redis_stream_if_configured,
    },
    retry_policy::queue_publish_backoff,
    trace::{append_trace, append_trace_in_tx},
    types::{QueuedDelivery, WorkerError},
};

pub(super) fn spawn_redis_relay(pool: PgPool, config: WorkerConfig, redis_url: String) {
    tokio::spawn(async move {
        let Some(mut redis) = connect_redis(&redis_url).await else {
            return;
        };

        if let Err(error) = ensure_consumer_group(
            &mut redis,
            &config.redis_stream,
            &config.redis_consumer_group,
        )
        .await
        {
            tracing::error!(%error, "failed to initialize Redis consumer group for relay");
            return;
        }

        tracing::info!(
            stream = config.redis_stream,
            group = config.redis_consumer_group,
            poll_ms = config.poll_interval.as_millis(),
            batch_size = config.batch_size,
            "Redis delivery relay started"
        );

        loop {
            match run_redis_relay_once(&pool, &mut redis, &config).await {
                Ok(published) if published > 0 => {
                    tracing::debug!(published, "published delivery jobs to Redis stream");
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::error!(%error, "Redis delivery relay tick failed");
                }
            }

            tokio::time::sleep(config.poll_interval).await;
        }
    });
}

async fn run_redis_relay_once(
    pool: &PgPool,
    redis: &mut ConnectionManager,
    config: &WorkerConfig,
) -> Result<usize, WorkerError> {
    recover_stuck_processing(pool).await?;
    recover_stale_queued(pool).await?;

    let queued_deliveries = claim_due_deliveries_for_queue(pool, config.batch_size).await?;
    let count = queued_deliveries.len();
    let mut failures = 0;

    for queued_delivery in queued_deliveries {
        match publish_delivery(redis, &config.redis_stream, &queued_delivery).await {
            Ok(message_id) => {
                mark_delivery_published(pool, &queued_delivery, &message_id).await?;
            }
            Err(error) => {
                mark_delivery_publish_failed(pool, &queued_delivery, &error.to_string()).await?;
                failures += 1;
                tracing::error!(
                    %error,
                    delivery_id = queued_delivery.delivery_id,
                    queue_token = %queued_delivery.queue_token,
                    "failed to publish delivery to Redis stream"
                );
            }
        }
    }

    if failures > 0 {
        tracing::warn!(
            failures,
            "some deliveries failed Redis publish and were returned to retrying"
        );
    }

    if count > 0 {
        trim_redis_stream_if_configured(redis, config).await?;
    }

    Ok(count)
}

async fn claim_due_deliveries_for_queue(
    pool: &PgPool,
    batch_size: i64,
) -> Result<Vec<QueuedDelivery>, sqlx::Error> {
    let rows = sqlx::query(
        "WITH due AS (
            SELECT delivery_id
            FROM webhook_deliveries
            WHERE status IN ('pending', 'retrying')
              AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
            ORDER BY created_at
            LIMIT $1
            FOR UPDATE SKIP LOCKED
         )
         UPDATE webhook_deliveries d
         SET status = 'queued',
             queued_at = NOW(),
             queue_token = gen_random_uuid(),
             queue_attempt_count = queue_attempt_count + 1,
             last_queue_error = NULL,
             published_at = NULL,
             redis_message_id = NULL,
             updated_at = NOW()
         FROM due
         WHERE d.delivery_id = due.delivery_id
         RETURNING d.delivery_id, d.queue_token::TEXT AS queue_token",
    )
    .bind(batch_size)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| QueuedDelivery {
            delivery_id: row.get("delivery_id"),
            queue_token: row.get("queue_token"),
        })
        .collect())
}

async fn mark_delivery_published(
    pool: &PgPool,
    delivery: &QueuedDelivery,
    message_id: &str,
) -> Result<(), sqlx::Error> {
    let row = sqlx::query(
        "UPDATE webhook_deliveries
         SET published_at = NOW(),
             redis_message_id = $2,
             updated_at = NOW()
         WHERE delivery_id = $1
           AND status IN ('queued', 'processing')
           AND queue_token = $3::uuid
         RETURNING event_id",
    )
    .bind(delivery.delivery_id)
    .bind(message_id)
    .bind(&delivery.queue_token)
    .fetch_optional(pool)
    .await?;

    let Some(row) = row else {
        tracing::warn!(
            delivery_id = delivery.delivery_id,
            queue_token = %delivery.queue_token,
            redis_message_id = message_id,
            "stale Redis publish success ignored because queue generation no longer matches"
        );
        return Ok(());
    };

    append_trace(
        pool,
        delivery.delivery_id,
        row.get("event_id"),
        "queued_to_redis",
        "succeeded",
        "Delivery queued to Redis",
        json!({ "redis_message_id": message_id, "queue_token": delivery.queue_token.clone() }),
    )
    .await?;

    Ok(())
}

async fn mark_delivery_publish_failed(
    pool: &PgPool,
    delivery: &QueuedDelivery,
    error: &str,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    let Some(row) = sqlx::query(
        "SELECT event_id, queue_attempt_count
         FROM webhook_deliveries
         WHERE delivery_id = $1
           AND status = 'queued'
           AND queue_token = $2::uuid
         FOR UPDATE",
    )
    .bind(delivery.delivery_id)
    .bind(&delivery.queue_token)
    .fetch_optional(&mut *tx)
    .await?
    else {
        tx.commit().await?;
        tracing::warn!(
            delivery_id = delivery.delivery_id,
            queue_token = %delivery.queue_token,
            "stale Redis publish failure ignored because queue generation no longer matches"
        );
        return Ok(());
    };

    let event_id: i64 = row.get("event_id");
    let queue_attempt_count: i64 = row.get("queue_attempt_count");
    let backoff = queue_publish_backoff(queue_attempt_count);
    let next_attempt_at = Utc::now() + backoff;
    let error = error.chars().take(1000).collect::<String>();

    sqlx::query(
        "UPDATE webhook_deliveries
         SET status = 'retrying',
             next_attempt_at = $3,
             last_queue_error = $4,
             queued_at = NULL,
             queue_token = NULL,
             published_at = NULL,
             redis_message_id = NULL,
             updated_at = NOW()
         WHERE delivery_id = $1
           AND status = 'queued'
           AND queue_token = $2::uuid",
    )
    .bind(delivery.delivery_id)
    .bind(&delivery.queue_token)
    .bind(next_attempt_at)
    .bind(&error)
    .execute(&mut *tx)
    .await?;

    append_trace_in_tx(
        &mut tx,
        delivery.delivery_id,
        event_id,
        "redis_publish_failed",
        "retrying",
        "Redis publish failed",
        json!({
            "error": error,
            "queue_token": delivery.queue_token.clone(),
            "queue_attempt": queue_attempt_count,
            "next_attempt_at": next_attempt_at,
            "backoff_ms": backoff.num_milliseconds()
        }),
    )
    .await?;

    tx.commit().await?;

    Ok(())
}
