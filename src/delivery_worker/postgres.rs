use chrono::Utc;
use sqlx::{PgPool, Postgres, Row, Transaction};

use super::types::{ClaimableDelivery, ClaimedDelivery};

pub(super) async fn claim_due_deliveries(
    pool: &PgPool,
    batch_size: i64,
    lease_duration: chrono::Duration,
    worker_id: &str,
) -> Result<Vec<ClaimedDelivery>, sqlx::Error> {
    let mut deliveries = Vec::new();

    for _ in 0..batch_size {
        let mut tx = pool.begin().await?;
        let Some(delivery) = sqlx::query_as::<_, ClaimableDelivery>(
            "SELECT
                 delivery_id,
                 event_id,
                 endpoint_id,
                 merchant_id,
                 endpoint_url,
                 secret_version_id,
                 max_attempts
             FROM webhook_deliveries
             WHERE status IN ('pending', 'retrying')
               AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
             ORDER BY created_at
             LIMIT 1
             FOR UPDATE SKIP LOCKED",
        )
        .fetch_optional(&mut *tx)
        .await?
        else {
            tx.commit().await?;
            break;
        };

        let claimed = claim_locked_delivery(&mut tx, delivery, lease_duration, worker_id).await?;
        tx.commit().await?;
        deliveries.push(claimed);
    }

    Ok(deliveries)
}

pub(super) async fn claim_locked_delivery(
    tx: &mut Transaction<'_, Postgres>,
    delivery: ClaimableDelivery,
    lease_duration: chrono::Duration,
    worker_id: &str,
) -> Result<ClaimedDelivery, sqlx::Error> {
    let lease_expires_at = Utc::now() + lease_duration;
    let attempt = sqlx::query(
        "INSERT INTO delivery_attempts (
            delivery_id,
            event_id,
            endpoint_id,
            attempt_count,
            outcome,
            started_at
         )
         SELECT
            $1,
            $2,
            $3,
            COALESCE(MAX(attempt_count), 0) + 1,
            'unknown',
            NOW()
         FROM delivery_attempts
         WHERE delivery_id = $1
         RETURNING attempt_id, attempt_count",
    )
    .bind(delivery.delivery_id)
    .bind(delivery.event_id)
    .bind(delivery.endpoint_id)
    .fetch_one(&mut **tx)
    .await?;

    let attempt_id: i64 = attempt.get("attempt_id");
    let attempt_count: i64 = attempt.get("attempt_count");

    let row = sqlx::query(
        "UPDATE webhook_deliveries
         SET status = 'processing',
             first_attempt_at = COALESCE(first_attempt_at, NOW()),
             processing_started_at = NOW(),
             processing_lease_token = gen_random_uuid(),
             processing_lease_expires_at = $2,
             current_attempt_id = $3,
             processing_worker_id = $4,
             updated_at = NOW()
         WHERE delivery_id = $1
           AND status IN ('pending', 'retrying', 'queued')
         RETURNING processing_lease_token::TEXT AS processing_lease_token",
    )
    .bind(delivery.delivery_id)
    .bind(lease_expires_at)
    .bind(attempt_id)
    .bind(worker_id)
    .fetch_one(&mut **tx)
    .await?;

    Ok(ClaimedDelivery {
        delivery_id: delivery.delivery_id,
        event_id: delivery.event_id,
        endpoint_id: delivery.endpoint_id,
        merchant_id: delivery.merchant_id,
        endpoint_url: delivery.endpoint_url,
        secret_version_id: delivery.secret_version_id,
        max_attempts: delivery.max_attempts,
        attempt_id,
        attempt_count,
        processing_lease_token: row.get("processing_lease_token"),
    })
}
