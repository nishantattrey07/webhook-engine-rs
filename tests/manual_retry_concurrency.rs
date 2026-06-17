mod support;

use sqlx::Row;
use webhook_engine::{
    models::RetryDeliveryRequest,
    services::{get_delivery, retry_delivery},
};

#[tokio::test]
async fn concurrent_manual_retries_create_only_one_active_child_when_test_db_is_set()
-> Result<(), Box<dyn std::error::Error>> {
    let _guard = support::acquire_db_lock().await;
    let Some(pool) = support::test_pool().await else {
        return Ok(());
    };

    let marker = chrono::Utc::now().timestamp_micros();
    let merchant_id = 9_200_000_000_i64 + (marker % 1_000_000);
    let payment_id: i64 = sqlx::query_scalar(
        "INSERT INTO payments (merchant_id, order_id, amount, status, mode_of_payment)
         VALUES ($1, $2, 100, 'succeeded', 'test')
         RETURNING payment_id",
    )
    .bind(merchant_id)
    .bind(marker)
    .fetch_one(&pool)
    .await?;

    let event_id: i64 = sqlx::query_scalar(
        "INSERT INTO domain_events (
            merchant_id,
            object_type,
            object_id,
            event_type
         )
         VALUES ($1, 'payment', $2, 'payment_succeeded')
         RETURNING event_id",
    )
    .bind(merchant_id)
    .bind(payment_id)
    .fetch_one(&pool)
    .await?;

    let endpoint_id: i64 = sqlx::query_scalar(
        "INSERT INTO webhook_endpoints (merchant_id, url, enabled, max_attempts)
         VALUES ($1, 'http://127.0.0.1:3000/test/retry-concurrency', TRUE, 5)
         RETURNING endpoint_id",
    )
    .bind(merchant_id)
    .fetch_one(&pool)
    .await?;

    let original_delivery_id: i64 = sqlx::query_scalar(
        "INSERT INTO webhook_deliveries (
            event_id,
            endpoint_id,
            merchant_id,
            endpoint_url,
            status,
            max_attempts
         )
         VALUES ($1, $2, $3, 'http://127.0.0.1:3000/test/retry-concurrency', 'dead_lettered', 5)
         RETURNING delivery_id",
    )
    .bind(event_id)
    .bind(endpoint_id)
    .bind(merchant_id)
    .fetch_one(&pool)
    .await?;

    let first_request = RetryDeliveryRequest {
        reason: Some("operator retry".to_string()),
        requested_by: Some("concurrency-test".to_string()),
    };
    let second_request = RetryDeliveryRequest {
        reason: Some("duplicate operator retry".to_string()),
        requested_by: Some("concurrency-test".to_string()),
    };

    let (first, second) = tokio::join!(
        retry_delivery(&pool, original_delivery_id, first_request),
        retry_delivery(&pool, original_delivery_id, second_request)
    );

    let first = first?;
    let second = second?;

    assert_eq!(first.new_delivery_id, second.new_delivery_id);
    assert_ne!(first.created, second.created);
    assert!(first.already_active_retry || second.already_active_retry);

    let active_child_count: i64 = sqlx::query(
        "SELECT COUNT(*)::BIGINT AS count
         FROM webhook_deliveries
         WHERE manually_retried_from_delivery_id = $1
           AND status IN ('pending', 'queued', 'processing', 'retrying')",
    )
    .bind(original_delivery_id)
    .fetch_one(&pool)
    .await?
    .get("count");

    assert_eq!(active_child_count, 1);
    assert_eq!(
        get_delivery(&pool, first.new_delivery_id).await?.status,
        "pending"
    );

    Ok(())
}
