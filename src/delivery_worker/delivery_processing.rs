use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde_json::{Value, json};
use sha2::Sha256;
use sqlx::{PgPool, Row};

use super::{
    finalization::finalize_attempt_and_delivery,
    retry_policy::{DeliveryOutcome, classify_status},
    signing::sha256_hex,
    target::validate_delivery_target,
    trace::append_trace,
    types::{ClaimedDelivery, DeliveryResult, WebhookSignature},
};

type HmacSha256 = Hmac<Sha256>;

pub(super) async fn process_delivery(
    pool: &PgPool,
    client: &Client,
    delivery: ClaimedDelivery,
) -> Result<(), sqlx::Error> {
    append_trace(
        pool,
        delivery.delivery_id,
        delivery.event_id,
        "worker_claimed",
        "succeeded",
        "Worker claimed delivery",
        json!({ "endpoint_id": delivery.endpoint_id }),
    )
    .await?;

    let payload = match build_payload(pool, &delivery).await {
        Ok(payload) => payload,
        Err(sqlx::Error::RowNotFound) => {
            let result = DeliveryResult {
                outcome: DeliveryOutcome::PermanentFailure,
                http_status: None,
                response_body_sample: None,
                error_message: Some("referenced event or payment row no longer exists".to_string()),
            };
            finalize_attempt_and_delivery(pool, &delivery, &result).await?;
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    append_trace(
        pool,
        delivery.delivery_id,
        delivery.event_id,
        "payload_enriched",
        "succeeded",
        "Payload enriched",
        json!({ "object_source": "domain_events + payments" }),
    )
    .await?;

    append_trace(
        pool,
        delivery.delivery_id,
        delivery.event_id,
        "http_request_started",
        "active",
        "HTTP request started",
        json!({ "attempt": delivery.attempt_count, "url": delivery.endpoint_url }),
    )
    .await?;

    let result = send_webhook(pool, client, &delivery, delivery.attempt_count, &payload).await;
    finalize_attempt_and_delivery(pool, &delivery, &result).await?;

    Ok(())
}

async fn build_payload(pool: &PgPool, delivery: &ClaimedDelivery) -> Result<Value, sqlx::Error> {
    let row = sqlx::query(
        "SELECT
            e.event_type,
            p.payment_id,
            p.merchant_id,
            p.order_id,
            p.amount,
            p.status,
            p.mode_of_payment,
            p.created_at,
            p.updated_at
         FROM domain_events e
         INNER JOIN payments p
            ON p.payment_id = e.object_id
         WHERE e.event_id = $1",
    )
    .bind(delivery.event_id)
    .fetch_one(pool)
    .await?;

    let event_type: String = row.get("event_type");
    let payment_created_at: DateTime<Utc> = row.get("created_at");
    let payment_updated_at: DateTime<Utc> = row.get("updated_at");

    Ok(json!({
        "event_id": delivery.event_id,
        "delivery_id": delivery.delivery_id,
        "event_type": event_type,
        "merchant_id": delivery.merchant_id,
        "endpoint_id": delivery.endpoint_id,
        "data": {
            "payment_id": row.get::<i64, _>("payment_id"),
            "merchant_id": row.get::<i64, _>("merchant_id"),
            "order_id": row.get::<i64, _>("order_id"),
            "amount": row.get::<i64, _>("amount"),
            "status": row.get::<String, _>("status"),
            "mode_of_payment": row.get::<String, _>("mode_of_payment"),
            "created_at": payment_created_at,
            "updated_at": payment_updated_at
        }
    }))
}

async fn send_webhook(
    pool: &PgPool,
    client: &Client,
    delivery: &ClaimedDelivery,
    attempt_count: i64,
    payload: &Value,
) -> DeliveryResult {
    if let Err(error) = validate_delivery_target(&delivery.endpoint_url).await {
        return DeliveryResult {
            outcome: DeliveryOutcome::PermanentFailure,
            http_status: None,
            response_body_sample: None,
            error_message: Some(error),
        };
    }

    let body = match serde_json::to_vec(payload) {
        Ok(body) => body,
        Err(error) => {
            return DeliveryResult {
                outcome: DeliveryOutcome::PermanentFailure,
                http_status: None,
                response_body_sample: None,
                error_message: Some(format!("failed to serialize payload: {error}")),
            };
        }
    };
    let request_body_hash = sha256_hex(&body);

    let signature = match sign_payload(pool, delivery, &body).await {
        Ok(signature) => signature,
        Err(error) => {
            return DeliveryResult {
                outcome: DeliveryOutcome::PermanentFailure,
                http_status: None,
                response_body_sample: None,
                error_message: Some(format!("failed to sign payload: {error}")),
            };
        }
    };

    if let Err(error) = persist_request_body_hash(
        pool,
        delivery.delivery_id,
        delivery.attempt_id,
        &request_body_hash,
    )
    .await
    {
        return DeliveryResult {
            outcome: DeliveryOutcome::PermanentFailure,
            http_status: None,
            response_body_sample: None,
            error_message: Some(format!(
                "failed to persist request body hash before send: {error}"
            )),
        };
    }

    let mut request = client
        .post(&delivery.endpoint_url)
        .header("content-type", "application/json")
        .header("x-webhook-event-id", delivery.event_id.to_string())
        .header("x-webhook-delivery-id", delivery.delivery_id.to_string())
        .header("x-webhook-attempt", attempt_count.to_string())
        .body(body);

    if let Some(signature) = signature {
        request = request
            .header("x-webhook-timestamp", signature.timestamp)
            .header("x-webhook-signature", signature.signature);

        if let Some(key_version) = signature.key_version {
            request = request.header("x-webhook-key-version", key_version.to_string());
        }
    }

    let response = request.send().await;

    match response {
        Ok(response) => {
            let status = response.status().as_u16();
            let text = response.text().await.unwrap_or_default();
            let sample = if text.is_empty() {
                None
            } else {
                Some(text.chars().take(1000).collect())
            };

            DeliveryResult {
                outcome: classify_status(status),
                http_status: Some(status),
                response_body_sample: sample,
                error_message: None,
            }
        }
        Err(error) if error.is_timeout() => DeliveryResult {
            outcome: DeliveryOutcome::Timeout,
            http_status: None,
            response_body_sample: None,
            error_message: None,
        },
        Err(error) => DeliveryResult {
            outcome: DeliveryOutcome::NetworkError(error.to_string()),
            http_status: None,
            response_body_sample: None,
            error_message: Some(error.to_string()),
        },
    }
}

async fn persist_request_body_hash(
    pool: &PgPool,
    delivery_id: i64,
    attempt_id: i64,
    request_body_hash: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE delivery_attempts
         SET request_body_hash = $3
         WHERE attempt_id = $1
           AND delivery_id = $2",
    )
    .bind(attempt_id)
    .bind(delivery_id)
    .bind(request_body_hash)
    .execute(pool)
    .await?;

    Ok(())
}

async fn sign_payload(
    pool: &PgPool,
    delivery: &ClaimedDelivery,
    body: &[u8],
) -> Result<Option<WebhookSignature>, sqlx::Error> {
    let Some(secret_version_id) = delivery.secret_version_id else {
        return Ok(None);
    };

    let secret: String = sqlx::query(
        "SELECT secret_value
         FROM webhook_endpoint_secrets
         WHERE secret_version_id = $1
           AND endpoint_id = $2
           AND is_active = TRUE
           AND (expires_at IS NULL OR expires_at > NOW())",
    )
    .bind(secret_version_id)
    .bind(delivery.endpoint_id)
    .fetch_one(pool)
    .await?
    .get("secret_value");

    let timestamp = Utc::now().timestamp().to_string();
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key size");
    mac.update(timestamp.as_bytes());
    mac.update(b".");
    mac.update(body);

    Ok(Some(WebhookSignature {
        timestamp,
        signature: format!("v1={}", hex::encode(mac.finalize().into_bytes())),
        key_version: Some(secret_version_id),
    }))
}
