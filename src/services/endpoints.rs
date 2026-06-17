use chrono::Utc;
use serde_json::json;
use sqlx::{PgPool, Row};

use super::{
    ALLOWED_EVENT_TYPES,
    trace::{append_delivery_trace_in_tx, append_event_level_trace},
    validation::{
        normalize_exact_filter, normalize_optional_text, validate_endpoint_url,
        validate_max_attempts,
    },
};
use crate::{
    error::{AppError, AppResult},
    models::{
        CreateEndpointRequest, CreateEndpointResponse, EndpointDeliveriesQuery,
        EndpointDeliveryItem, EndpointDetailResponse, EndpointHealthSummary, EndpointListItem,
        EndpointStatsItem, PaginatedEndpointDeliveriesResponse, TestEndpointRequest,
        TestEndpointResponse, UpdateEndpointRequest,
    },
};

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
    let max_attempts = validate_max_attempts(request.max_attempts)?;

    for event_type in &request.subscribed_events {
        if !ALLOWED_EVENT_TYPES.contains(&event_type.as_str()) {
            return Err(AppError::BadRequest(format!(
                "unsupported event type {}",
                event_type
            )));
        }
    }
    let enabled = request.enabled.unwrap_or(true);

    let mut tx = pool.begin().await?;

    let endpoint_id: i64 = sqlx::query(
        "INSERT INTO webhook_endpoints (
            merchant_id,
            url,
            enabled,
            max_attempts,
            description,
            created_at,
            updated_at
         )
         VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
         RETURNING endpoint_id",
    )
    .bind(request.merchant_id)
    .bind(request.url)
    .bind(enabled)
    .bind(max_attempts)
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
    if let Some(max_attempts) = request.max_attempts {
        validate_max_attempts(Some(max_attempts))?;
    }

    let mut tx = pool.begin().await?;

    let update_result = sqlx::query(
        "UPDATE webhook_endpoints
         SET url = COALESCE($2, url),
             enabled = COALESCE($3, enabled),
             description = COALESCE($4, description),
             max_attempts = COALESCE($5, max_attempts),
             updated_at = NOW()
         WHERE endpoint_id = $1",
    )
    .bind(endpoint_id)
    .bind(request.url)
    .bind(request.enabled)
    .bind(request.description)
    .bind(request.max_attempts)
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

pub async fn list_endpoints(pool: &PgPool) -> AppResult<Vec<EndpointListItem>> {
    let rows = sqlx::query_as::<_, EndpointListItem>(
        "SELECT
            e.endpoint_id,
            e.merchant_id,
            e.url,
            e.active_secret_version_id,
            e.enabled,
            e.max_attempts,
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
            e.max_attempts,
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

pub async fn get_endpoint_detail(
    pool: &PgPool,
    endpoint_id: i64,
) -> AppResult<EndpointDetailResponse> {
    let endpoint = get_endpoint(pool, endpoint_id).await?;

    let health = sqlx::query(
        "WITH latest_delivery AS (
            SELECT
                d.delivery_id,
                d.status,
                d.created_at,
                latest.http_status
            FROM webhook_deliveries d
            LEFT JOIN LATERAL (
                SELECT http_status
                FROM delivery_attempts
                WHERE delivery_id = d.delivery_id
                ORDER BY attempt_count DESC, attempt_id DESC
                LIMIT 1
            ) latest ON TRUE
            WHERE d.endpoint_id = $1
            ORDER BY d.created_at DESC, d.delivery_id DESC
            LIMIT 1
         ),
         latency AS (
            SELECT
                percentile_cont(0.95) WITHIN GROUP (ORDER BY a.duration_ms)::DOUBLE PRECISION AS p95_latency_ms
            FROM delivery_attempts a
            WHERE a.endpoint_id = $1
              AND a.duration_ms IS NOT NULL
         )
         SELECT
            COUNT(d.delivery_id)::BIGINT AS total_deliveries,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'delivered')::BIGINT AS delivered_deliveries,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'retrying')::BIGINT AS retrying_deliveries,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'dead_lettered')::BIGINT AS dead_lettered_deliveries,
            latest_delivery.created_at AS last_delivery_at,
            latest_delivery.status AS last_delivery_status,
            latest_delivery.http_status AS last_http_status,
            CASE
                WHEN COUNT(d.delivery_id) > 0
                THEN COUNT(d.delivery_id) FILTER (WHERE d.status = 'delivered')::DOUBLE PRECISION / COUNT(d.delivery_id)::DOUBLE PRECISION
                ELSE NULL
            END AS success_rate,
            latency.p95_latency_ms
         FROM webhook_deliveries d
         CROSS JOIN latency
         LEFT JOIN latest_delivery ON TRUE
         WHERE d.endpoint_id = $1
         GROUP BY
            latest_delivery.created_at,
            latest_delivery.status,
            latest_delivery.http_status,
            latency.p95_latency_ms",
    )
    .bind(endpoint_id)
    .fetch_optional(pool)
    .await?;

    let delivery_health = match health {
        Some(row) => EndpointHealthSummary {
            total_deliveries: row.get("total_deliveries"),
            delivered_deliveries: row.get("delivered_deliveries"),
            retrying_deliveries: row.get("retrying_deliveries"),
            dead_lettered_deliveries: row.get("dead_lettered_deliveries"),
            last_delivery_at: row.get("last_delivery_at"),
            last_delivery_status: row.get("last_delivery_status"),
            last_http_status: row.get("last_http_status"),
            success_rate: row.get("success_rate"),
            p95_latency_ms: row.get("p95_latency_ms"),
        },
        None => EndpointHealthSummary {
            total_deliveries: 0,
            delivered_deliveries: 0,
            retrying_deliveries: 0,
            dead_lettered_deliveries: 0,
            last_delivery_at: None,
            last_delivery_status: None,
            last_http_status: None,
            success_rate: None,
            p95_latency_ms: None,
        },
    };

    Ok(EndpointDetailResponse {
        endpoint_id: endpoint.endpoint_id,
        merchant_id: endpoint.merchant_id,
        url: endpoint.url,
        description: endpoint.description,
        enabled: endpoint.enabled,
        max_attempts: endpoint.max_attempts,
        active_secret_version_id: endpoint.active_secret_version_id,
        created_at: endpoint.created_at,
        updated_at: endpoint.updated_at,
        subscribed_events: endpoint.subscribed_events,
        delivery_health,
    })
}

pub async fn list_endpoint_deliveries(
    pool: &PgPool,
    endpoint_id: i64,
    query: EndpointDeliveriesQuery,
) -> AppResult<PaginatedEndpointDeliveriesResponse> {
    get_endpoint(pool, endpoint_id).await?;

    let limit = query.limit.unwrap_or(50).clamp(1, 100);
    let fetch_limit = limit + 1;
    let status = normalize_exact_filter(query.status);

    let rows = sqlx::query_as::<_, EndpointDeliveryItem>(
        "SELECT
            d.delivery_id,
            d.event_id,
            e.event_type,
            d.status,
            COUNT(a.attempt_id)::BIGINT AS attempt_count,
            d.max_attempts,
            latest.http_status AS last_http_status,
            latest.outcome AS last_outcome,
            latest.duration_ms,
            d.next_attempt_at,
            d.created_at,
            d.updated_at
         FROM webhook_deliveries d
         INNER JOIN domain_events e
            ON e.event_id = d.event_id
         LEFT JOIN delivery_attempts a
            ON a.delivery_id = d.delivery_id
         LEFT JOIN LATERAL (
            SELECT
                http_status,
                outcome,
                duration_ms
            FROM delivery_attempts
            WHERE delivery_id = d.delivery_id
            ORDER BY attempt_count DESC, attempt_id DESC
            LIMIT 1
         ) latest ON TRUE
         WHERE d.endpoint_id = $1
           AND ($2::TEXT IS NULL OR d.status = $2)
           AND ($3::BIGINT IS NULL OR d.delivery_id < $3)
         GROUP BY d.delivery_id, e.event_type, latest.http_status, latest.outcome, latest.duration_ms
         ORDER BY d.delivery_id DESC
         LIMIT $4",
    )
    .bind(endpoint_id)
    .bind(status)
    .bind(query.cursor)
    .bind(fetch_limit)
    .fetch_all(pool)
    .await?;

    let mut items = rows;
    let next_cursor = if items.len() > limit as usize {
        items.pop().map(|item| item.delivery_id)
    } else {
        None
    };

    Ok(PaginatedEndpointDeliveriesResponse {
        items,
        next_cursor,
        limit,
    })
}

pub async fn test_endpoint(
    pool: &PgPool,
    endpoint_id: i64,
    request: TestEndpointRequest,
) -> AppResult<TestEndpointResponse> {
    let requested_by = normalize_optional_text(request.requested_by, "requested_by", 200)?;
    let endpoint = get_endpoint(pool, endpoint_id).await?;

    if !endpoint.enabled {
        return Err(AppError::BadRequest(format!(
            "endpoint {} is disabled; enable it before sending a test delivery",
            endpoint_id
        )));
    }

    let event_type = request
        .event_type
        .or_else(|| endpoint.subscribed_events.first().cloned())
        .ok_or_else(|| AppError::BadRequest("endpoint has no subscribed events".to_string()))?;

    if !endpoint.subscribed_events.contains(&event_type) {
        return Err(AppError::BadRequest(format!(
            "endpoint {} is not subscribed to {}",
            endpoint_id, event_type
        )));
    }

    let (payment_status, amount) = payment_status_for_event_type(&event_type)?;
    let order_id = Utc::now().timestamp_micros();
    let mut tx = pool.begin().await?;

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
         VALUES ($1, $2, $3, $4, 'endpoint_test', NOW(), NOW())
         RETURNING payment_id",
    )
    .bind(endpoint.merchant_id)
    .bind(order_id)
    .bind(amount)
    .bind(payment_status)
    .fetch_one(&mut *tx)
    .await?
    .get(0);

    let event_id: i64 = sqlx::query(
        "INSERT INTO domain_events (
            merchant_id,
            object_type,
            object_id,
            event_type,
            created_at
         )
         VALUES ($1, 'payment', $2, $3, NOW())
         RETURNING event_id",
    )
    .bind(endpoint.merchant_id)
    .bind(payment_id)
    .bind(&event_type)
    .fetch_one(&mut *tx)
    .await?
    .get(0);

    let delivery_id: i64 = sqlx::query(
        "INSERT INTO webhook_deliveries (
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
         VALUES ($1, $2, $3, $4, $5, 'pending', $6, NOW(), NOW())
         RETURNING delivery_id",
    )
    .bind(event_id)
    .bind(endpoint.endpoint_id)
    .bind(endpoint.merchant_id)
    .bind(&endpoint.url)
    .bind(endpoint.active_secret_version_id)
    .bind(endpoint.max_attempts)
    .fetch_one(&mut *tx)
    .await?
    .get(0);

    append_event_level_trace(&mut tx, event_id, "payment_committed", "Payment committed").await?;
    append_event_level_trace(
        &mut tx,
        event_id,
        "domain_event_created",
        "Domain event created",
    )
    .await?;
    append_delivery_trace_in_tx(
        &mut tx,
        delivery_id,
        event_id,
        "delivery_created",
        "succeeded",
        "Endpoint test delivery created",
        json!({
            "endpoint_id": endpoint.endpoint_id,
            "endpoint_url": endpoint.url,
            "requested_by": requested_by,
            "source": "endpoint_test"
        }),
    )
    .await?;

    tx.commit().await?;

    Ok(TestEndpointResponse {
        payment_id,
        event_id,
        delivery_ids: vec![delivery_id],
    })
}

pub async fn list_endpoint_stats(pool: &PgPool) -> AppResult<Vec<EndpointStatsItem>> {
    let rows = sqlx::query_as::<_, EndpointStatsItem>(
        "SELECT
            e.endpoint_id,
            e.merchant_id,
            e.url,
            e.enabled,
            e.description,
            COUNT(d.delivery_id)::BIGINT AS total_deliveries,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'delivered')::BIGINT AS delivered_deliveries,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'retrying')::BIGINT AS retrying_deliveries,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'dead_lettered')::BIGINT AS dead_lettered_deliveries,
            MAX(d.created_at) AS last_delivery_at
         FROM webhook_endpoints e
         LEFT JOIN webhook_deliveries d
            ON d.endpoint_id = e.endpoint_id
         GROUP BY e.endpoint_id
         ORDER BY total_deliveries DESC, e.created_at DESC",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

fn payment_status_for_event_type(event_type: &str) -> AppResult<(&'static str, i64)> {
    match event_type {
        "payment_succeeded" => Ok(("succeeded", 100)),
        "payment_failed" => Ok(("failed", 0)),
        "payment_refund" => Ok(("refunded", 100)),
        _ => Err(AppError::BadRequest(format!(
            "unsupported event type {}",
            event_type
        ))),
    }
}
