use serde_json::json;
use sqlx::{PgPool, Postgres, Row, Transaction};
use url::Url;

use crate::{
    error::{AppError, AppResult},
    models::{
        CreateBulkPaymentsRequest, CreateBulkPaymentsResponse, CreateEndpointRequest,
        CreateEndpointResponse, CreatePaymentRequest, CreatePaymentResponse, DeliveryAttemptItem,
        DeliveryListItem, DeliveryTraceItem, EndpointListItem, EventFanoutResponse, EventListItem,
        UpdateEndpointRequest,
    },
};

const ALLOWED_EVENT_TYPES: &[&str] = &["payment_succeeded", "payment_failed", "payment_refund"];

pub async fn create_endpoint(
    pool: &PgPool,
    request: CreateEndpointRequest,
) -> AppResult<CreateEndpointResponse> {
    validate_endpoint_url(&request.url)?;

    if request.secret.trim().is_empty() {
        return Err(AppError::BadRequest(
            "endpoint secret cannot be empty".to_string(),
        ));
    }

    if request.subscribed_events.is_empty() {
        return Err(AppError::BadRequest(
            "at least one subscribed event is required".to_string(),
        ));
    }

    for event_type in &request.subscribed_events {
        if !ALLOWED_EVENT_TYPES.contains(&event_type.as_str()) {
            return Err(AppError::BadRequest(format!(
                "unsupported event type {}",
                event_type
            )));
        }
    }

    let mut tx = pool.begin().await?;

    let endpoint_id: i64 = sqlx::query(
        "INSERT INTO webhook_endpoints (
            merchant_id,
            url,
            enabled,
            description,
            created_at,
            updated_at
         )
         VALUES ($1, $2, TRUE, $3, NOW(), NOW())
         RETURNING endpoint_id",
    )
    .bind(request.merchant_id)
    .bind(request.url)
    .bind(request.description)
    .fetch_one(&mut *tx)
    .await?
    .get(0);

    let secret_version_id: i64 = sqlx::query(
        "INSERT INTO webhook_endpoint_secrets (
            endpoint_id,
            secret_value,
            created_at,
            is_active
         )
         VALUES ($1, $2, NOW(), TRUE)
         RETURNING secret_version_id",
    )
    .bind(endpoint_id)
    .bind(request.secret)
    .fetch_one(&mut *tx)
    .await?
    .get(0);

    sqlx::query(
        "UPDATE webhook_endpoints
         SET active_secret_version_id = $2,
             updated_at = NOW()
         WHERE endpoint_id = $1",
    )
    .bind(endpoint_id)
    .bind(secret_version_id)
    .execute(&mut *tx)
    .await?;

    for event_type in &request.subscribed_events {
        sqlx::query(
            "INSERT INTO webhook_endpoint_subscriptions (
                endpoint_id,
                event_type,
                created_at
             )
             VALUES ($1, $2, NOW())
             ON CONFLICT (endpoint_id, event_type) DO NOTHING",
        )
        .bind(endpoint_id)
        .bind(event_type)
        .execute(&mut *tx)
        .await?;
    }

    let subscription_count: i64 = sqlx::query(
        "SELECT COUNT(*)::BIGINT
         FROM webhook_endpoint_subscriptions
         WHERE endpoint_id = $1",
    )
    .bind(endpoint_id)
    .fetch_one(&mut *tx)
    .await?
    .get(0);

    tx.commit().await?;

    Ok(CreateEndpointResponse {
        endpoint_id,
        secret_version_id,
        subscription_count,
    })
}

pub async fn update_endpoint(
    pool: &PgPool,
    endpoint_id: i64,
    request: UpdateEndpointRequest,
) -> AppResult<EndpointListItem> {
    if let Some(url) = &request.url {
        validate_endpoint_url(url)?;
    }

    if let Some(events) = &request.subscribed_events {
        if events.is_empty() {
            return Err(AppError::BadRequest(
                "at least one subscribed event is required".to_string(),
            ));
        }

        for event_type in events {
            if !ALLOWED_EVENT_TYPES.contains(&event_type.as_str()) {
                return Err(AppError::BadRequest(format!(
                    "unsupported event type {}",
                    event_type
                )));
            }
        }
    }

    let mut tx = pool.begin().await?;

    let update_result = sqlx::query(
        "UPDATE webhook_endpoints
         SET url = COALESCE($2, url),
             enabled = COALESCE($3, enabled),
             description = COALESCE($4, description),
             updated_at = NOW()
         WHERE endpoint_id = $1",
    )
    .bind(endpoint_id)
    .bind(request.url)
    .bind(request.enabled)
    .bind(request.description)
    .execute(&mut *tx)
    .await?;

    if update_result.rows_affected() == 0 {
        return Err(AppError::NotFound(format!(
            "endpoint {} not found",
            endpoint_id
        )));
    }

    if let Some(events) = request.subscribed_events {
        sqlx::query(
            "DELETE FROM webhook_endpoint_subscriptions
             WHERE endpoint_id = $1",
        )
        .bind(endpoint_id)
        .execute(&mut *tx)
        .await?;

        for event_type in events {
            sqlx::query(
                "INSERT INTO webhook_endpoint_subscriptions (
                    endpoint_id,
                    event_type,
                    created_at
                 )
                 VALUES ($1, $2, NOW())
                 ON CONFLICT (endpoint_id, event_type) DO NOTHING",
            )
            .bind(endpoint_id)
            .bind(event_type)
            .execute(&mut *tx)
            .await?;
        }
    }

    tx.commit().await?;

    get_endpoint(pool, endpoint_id).await
}

pub async fn create_payment(
    pool: &PgPool,
    request: CreatePaymentRequest,
) -> AppResult<CreatePaymentResponse> {
    validate_payment_request(&request)?;

    let mut tx = pool.begin().await?;
    let response = create_payment_in_tx(&mut tx, request).await?;
    tx.commit().await?;

    Ok(response)
}

async fn create_payment_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    request: CreatePaymentRequest,
) -> AppResult<CreatePaymentResponse> {
    let status = request.status.as_db_str();
    let event_type = request.status.event_type();

    let payment_id: i64 = sqlx::query(
        "INSERT INTO payments (
            merchant_id,
            order_id,
            amount,
            status,
            mode_of_payment,
            created_at,
            updated_at
         )
         VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
         RETURNING payment_id",
    )
    .bind(request.merchant_id)
    .bind(request.order_id)
    .bind(request.amount)
    .bind(status)
    .bind(&request.mode_of_payment)
    .fetch_one(&mut **tx)
    .await?
    .get(0);

    let snapshot = json!({
        "payment_id": payment_id,
        "merchant_id": request.merchant_id,
        "order_id": request.order_id,
        "amount": request.amount,
        "status": status,
        "mode_of_payment": request.mode_of_payment,
    });

    let event_id: i64 = sqlx::query(
        "INSERT INTO domain_events (
            merchant_id,
            object_type,
            object_id,
            event_type,
            event_snapshot_json,
            created_at
         )
         VALUES ($1, 'payment', $2, $3, $4, NOW())
         RETURNING event_id",
    )
    .bind(request.merchant_id)
    .bind(payment_id)
    .bind(event_type)
    .bind(&snapshot)
    .fetch_one(&mut **tx)
    .await?
    .get(0);

    let delivery_count: i64 = sqlx::query(
        "WITH subscribed_endpoints AS (
            SELECT
                e.endpoint_id,
                e.merchant_id,
                e.url,
                e.active_secret_version_id
            FROM webhook_endpoints e
            INNER JOIN webhook_endpoint_subscriptions s
                ON s.endpoint_id = e.endpoint_id
            WHERE e.merchant_id = $1
              AND e.enabled = TRUE
              AND s.event_type = $2
         ),
         inserted AS (
            INSERT INTO webhook_deliveries (
                event_id,
                endpoint_id,
                merchant_id,
                endpoint_url,
                secret_version_id,
                status,
                max_attempts,
                created_at,
                updated_at
            )
            SELECT
                $3,
                endpoint_id,
                merchant_id,
                url,
                active_secret_version_id,
                'pending',
                5,
                NOW(),
                NOW()
            FROM subscribed_endpoints
            RETURNING delivery_id
         )
         SELECT COUNT(*)::BIGINT FROM inserted",
    )
    .bind(request.merchant_id)
    .bind(event_type)
    .bind(event_id)
    .fetch_one(&mut **tx)
    .await?
    .get(0);

    append_event_level_trace(tx, event_id, "payment_committed", "Payment committed").await?;
    append_event_level_trace(tx, event_id, "domain_event_created", "Domain event created").await?;

    sqlx::query(
        "INSERT INTO delivery_trace_events (
            delivery_id,
            event_id,
            step,
            status,
            title,
            metadata_json,
            occurred_at
         )
         SELECT
            delivery_id,
            event_id,
            'delivery_created',
            'succeeded',
            'Delivery created',
            jsonb_build_object('endpoint_id', endpoint_id, 'endpoint_url', endpoint_url),
            NOW()
         FROM webhook_deliveries
         WHERE event_id = $1",
    )
    .bind(event_id)
    .execute(&mut **tx)
    .await?;

    Ok(CreatePaymentResponse {
        payment_id,
        event_id,
        delivery_count,
    })
}

pub async fn create_bulk_payments(
    pool: &PgPool,
    request: CreateBulkPaymentsRequest,
) -> AppResult<CreateBulkPaymentsResponse> {
    if request.payments.is_empty() {
        return Err(AppError::BadRequest(
            "at least one payment is required".to_string(),
        ));
    }

    if request.payments.len() > 100 {
        return Err(AppError::BadRequest(
            "bulk payment creation is limited to 100 payments".to_string(),
        ));
    }

    for payment in &request.payments {
        validate_payment_request(payment)?;
    }

    let total_payments = request.payments.len();
    let mut created = Vec::with_capacity(request.payments.len());
    let mut total_deliveries = 0_i64;
    let mut tx = pool.begin().await?;

    for payment in request.payments {
        let response = create_payment_in_tx(&mut tx, payment).await?;
        total_deliveries += response.delivery_count;
        created.push(response);
    }

    tx.commit().await?;

    Ok(CreateBulkPaymentsResponse {
        total_payments,
        total_deliveries,
        created,
    })
}

fn validate_endpoint_url(url: &str) -> AppResult<()> {
    let parsed = Url::parse(url.trim())
        .map_err(|_| AppError::BadRequest("endpoint URL must be a valid URL".to_string()))?;

    match parsed.scheme() {
        "http" | "https" => {}
        _ => {
            return Err(AppError::BadRequest(
                "endpoint URL must use http or https".to_string(),
            ));
        }
    }

    if parsed.host_str().is_none() {
        return Err(AppError::BadRequest(
            "endpoint URL must include a host".to_string(),
        ));
    }

    Ok(())
}

fn validate_payment_request(request: &CreatePaymentRequest) -> AppResult<()> {
    if request.amount < 0 {
        return Err(AppError::BadRequest(
            "amount cannot be negative".to_string(),
        ));
    }

    if request.mode_of_payment.trim().is_empty() {
        return Err(AppError::BadRequest(
            "mode_of_payment cannot be empty".to_string(),
        ));
    }

    Ok(())
}

async fn append_event_level_trace(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    event_id: i64,
    step: &str,
    title: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO delivery_trace_events (
            delivery_id,
            event_id,
            step,
            status,
            title,
            metadata_json,
            occurred_at
         )
         VALUES (NULL, $1, $2, 'succeeded', $3, '{}'::jsonb, NOW())",
    )
    .bind(event_id)
    .bind(step)
    .bind(title)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

pub async fn list_events(pool: &PgPool) -> AppResult<Vec<EventListItem>> {
    let rows = sqlx::query_as::<_, EventListItem>(
        "SELECT
            event_id,
            merchant_id,
            object_type,
            object_id,
            event_type,
            event_snapshot_json,
            created_at
         FROM domain_events
         ORDER BY created_at DESC
         LIMIT 100",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

pub async fn get_event(pool: &PgPool, event_id: i64) -> AppResult<EventListItem> {
    let event = sqlx::query_as::<_, EventListItem>(
        "SELECT
            event_id,
            merchant_id,
            object_type,
            object_id,
            event_type,
            event_snapshot_json,
            created_at
         FROM domain_events
         WHERE event_id = $1",
    )
    .bind(event_id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| AppError::NotFound(format!("event {} not found", event_id)))?;

    Ok(event)
}

pub async fn get_event_fanout(pool: &PgPool, event_id: i64) -> AppResult<EventFanoutResponse> {
    let event = get_event(pool, event_id).await?;
    let deliveries = list_deliveries_for_event(pool, event_id).await?;

    Ok(EventFanoutResponse { event, deliveries })
}

pub async fn list_endpoints(pool: &PgPool) -> AppResult<Vec<EndpointListItem>> {
    let rows = sqlx::query_as::<_, EndpointListItem>(
        "SELECT
            e.endpoint_id,
            e.merchant_id,
            e.url,
            e.active_secret_version_id,
            e.enabled,
            e.description,
            e.created_at,
            e.updated_at,
            COALESCE(
                ARRAY_AGG(s.event_type ORDER BY s.event_type)
                    FILTER (WHERE s.event_type IS NOT NULL),
                ARRAY[]::TEXT[]
            ) AS subscribed_events
         FROM webhook_endpoints e
         LEFT JOIN webhook_endpoint_subscriptions s
            ON s.endpoint_id = e.endpoint_id
         GROUP BY e.endpoint_id
         ORDER BY e.created_at DESC",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

pub async fn get_endpoint(pool: &PgPool, endpoint_id: i64) -> AppResult<EndpointListItem> {
    let endpoint = sqlx::query_as::<_, EndpointListItem>(
        "SELECT
            e.endpoint_id,
            e.merchant_id,
            e.url,
            e.active_secret_version_id,
            e.enabled,
            e.description,
            e.created_at,
            e.updated_at,
            COALESCE(
                ARRAY_AGG(s.event_type ORDER BY s.event_type)
                    FILTER (WHERE s.event_type IS NOT NULL),
                ARRAY[]::TEXT[]
            ) AS subscribed_events
         FROM webhook_endpoints e
         LEFT JOIN webhook_endpoint_subscriptions s
            ON s.endpoint_id = e.endpoint_id
         WHERE e.endpoint_id = $1
         GROUP BY e.endpoint_id",
    )
    .bind(endpoint_id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| AppError::NotFound(format!("endpoint {} not found", endpoint_id)))?;

    Ok(endpoint)
}

pub async fn list_deliveries(pool: &PgPool) -> AppResult<Vec<DeliveryListItem>> {
    let rows = sqlx::query_as::<_, DeliveryListItem>(
        "SELECT
            d.delivery_id,
            d.event_id,
            d.merchant_id,
            d.endpoint_id,
            d.endpoint_url,
            d.status,
            d.max_attempts,
            d.next_attempt_at,
            d.last_error,
            d.created_at,
            d.updated_at,
            d.final_state_at,
            COUNT(a.attempt_id)::BIGINT AS attempt_count
         FROM webhook_deliveries d
         LEFT JOIN delivery_attempts a
            ON a.delivery_id = d.delivery_id
         GROUP BY d.delivery_id
         ORDER BY d.created_at DESC
         LIMIT 100",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

pub async fn list_deliveries_for_event(
    pool: &PgPool,
    event_id: i64,
) -> AppResult<Vec<DeliveryListItem>> {
    let rows = sqlx::query_as::<_, DeliveryListItem>(
        "SELECT
            d.delivery_id,
            d.event_id,
            d.merchant_id,
            d.endpoint_id,
            d.endpoint_url,
            d.status,
            d.max_attempts,
            d.next_attempt_at,
            d.last_error,
            d.created_at,
            d.updated_at,
            d.final_state_at,
            COUNT(a.attempt_id)::BIGINT AS attempt_count
         FROM webhook_deliveries d
         LEFT JOIN delivery_attempts a
            ON a.delivery_id = d.delivery_id
         WHERE d.event_id = $1
         GROUP BY d.delivery_id
         ORDER BY d.created_at ASC",
    )
    .bind(event_id)
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

pub async fn get_delivery(pool: &PgPool, delivery_id: i64) -> AppResult<DeliveryListItem> {
    let row = sqlx::query_as::<_, DeliveryListItem>(
        "SELECT
            d.delivery_id,
            d.event_id,
            d.merchant_id,
            d.endpoint_id,
            d.endpoint_url,
            d.status,
            d.max_attempts,
            d.next_attempt_at,
            d.last_error,
            d.created_at,
            d.updated_at,
            d.final_state_at,
            COUNT(a.attempt_id)::BIGINT AS attempt_count
         FROM webhook_deliveries d
         LEFT JOIN delivery_attempts a
            ON a.delivery_id = d.delivery_id
         WHERE d.delivery_id = $1
         GROUP BY d.delivery_id",
    )
    .bind(delivery_id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| AppError::NotFound(format!("delivery {} not found", delivery_id)))?;

    Ok(row)
}

pub async fn list_delivery_attempts(
    pool: &PgPool,
    delivery_id: i64,
) -> AppResult<Vec<DeliveryAttemptItem>> {
    let rows = sqlx::query_as::<_, DeliveryAttemptItem>(
        "SELECT
            attempt_id,
            delivery_id,
            event_id,
            endpoint_id,
            attempt_count,
            http_status,
            outcome,
            error_message,
            response_body_sample,
            started_at,
            completed_at,
            duration_ms
         FROM delivery_attempts
         WHERE delivery_id = $1
         ORDER BY attempt_count ASC",
    )
    .bind(delivery_id)
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

pub async fn list_delivery_trace(
    pool: &PgPool,
    delivery_id: i64,
) -> AppResult<Vec<DeliveryTraceItem>> {
    let rows = sqlx::query_as::<_, DeliveryTraceItem>(
        "SELECT
            trace_id,
            delivery_id,
            event_id,
            step,
            status,
            title,
            detail,
            metadata_json,
            occurred_at,
            duration_ms
         FROM delivery_trace_events
         WHERE delivery_id = $1
         ORDER BY occurred_at ASC, trace_id ASC",
    )
    .bind(delivery_id)
    .fetch_all(pool)
    .await?;

    Ok(rows)
}
