use sqlx::{PgPool, Row};

use super::{
    ALLOWED_EVENT_TYPES, ATTEMPT_OUTCOMES, DELIVERY_STATUSES,
    events::get_event,
    trace::{list_delivery_attempts, list_delivery_trace},
    validation::{
        extract_search_number, normalize_exact_filter, normalize_resolution_filter,
        normalize_search, time_range_start,
    },
};
use crate::{
    error::{AppError, AppResult},
    models::{
        DeliveryDetailResponse, DeliveryListItem, DeliveryListQuery, DeliveryRetryLineage,
        EventFanoutDeliveryItem, PaginatedDeliveriesResponse,
    },
};

#[derive(Debug, Clone)]
struct EndpointSearch {
    pattern: Option<String>,
    number: Option<i64>,
}

#[derive(Debug, Clone)]
struct DeliverySearch {
    delivery_id: Option<i64>,
    event_id: Option<i64>,
    merchant_id: Option<i64>,
    endpoint_id: Option<i64>,
    http_status: Option<i64>,
    event_type: Option<String>,
    status: Option<String>,
    outcome: Option<String>,
    endpoint_pattern: Option<String>,
}

fn endpoint_search(value: Option<&str>) -> EndpointSearch {
    EndpointSearch {
        pattern: normalize_search(value),
        number: extract_search_number(value),
    }
}

fn delivery_search(value: Option<&str>) -> DeliverySearch {
    let Some(raw) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return DeliverySearch {
            delivery_id: None,
            event_id: None,
            merchant_id: None,
            endpoint_id: None,
            http_status: None,
            event_type: None,
            status: None,
            outcome: None,
            endpoint_pattern: None,
        };
    };

    let normalized = raw.to_ascii_lowercase();
    let number = extract_search_number(Some(raw));

    let mut search = DeliverySearch {
        delivery_id: None,
        event_id: None,
        merchant_id: None,
        endpoint_id: None,
        http_status: None,
        event_type: None,
        status: None,
        outcome: None,
        endpoint_pattern: None,
    };

    if normalized.starts_with("del_") || normalized.starts_with("delivery ") {
        search.delivery_id = number;
        return search;
    }

    if normalized.starts_with("evt_") || normalized.starts_with("event ") {
        search.event_id = number;
        return search;
    }

    if normalized.starts_with("merchant ") {
        search.merchant_id = number;
        return search;
    }

    if normalized.starts_with("endpoint ") {
        search.endpoint_id = number;
        return search;
    }

    if ALLOWED_EVENT_TYPES.contains(&normalized.as_str()) {
        search.event_type = Some(normalized);
        return search;
    }

    if DELIVERY_STATUSES.contains(&normalized.as_str()) {
        search.status = Some(normalized);
        return search;
    }

    if ATTEMPT_OUTCOMES.contains(&normalized.as_str()) {
        search.outcome = Some(normalized);
        return search;
    }

    if let Some(number) = number.filter(|_| raw.chars().all(|character| character.is_ascii_digit()))
    {
        search.delivery_id = Some(number);
        search.event_id = Some(number);
        search.merchant_id = Some(number);
        search.endpoint_id = Some(number);
        search.http_status = Some(number);
        return search;
    }

    search.endpoint_pattern = normalize_search(Some(raw));
    search
}

pub async fn list_deliveries(
    pool: &PgPool,
    query: DeliveryListQuery,
) -> AppResult<PaginatedDeliveriesResponse> {
    let limit = query.limit.unwrap_or(50).clamp(1, 100);
    let fetch_limit = limit + 1;
    let status = normalize_exact_filter(query.status);
    let event_type = normalize_exact_filter(query.event_type);
    let resolution = normalize_resolution_filter(query.resolution)?;
    let search = delivery_search(query.search.as_deref());
    let endpoint = endpoint_search(query.endpoint.as_deref());
    let time_range_start = time_range_start(query.time_range.as_deref())?;

    let rows = sqlx::query_as::<_, DeliveryListItem>(
        "SELECT
            d.delivery_id,
            d.event_id,
            e.event_type,
            d.merchant_id,
            d.endpoint_id,
            d.endpoint_url,
            d.status,
            COUNT(a.attempt_id)::BIGINT AS attempt_count,
            d.max_attempts,
            latest.http_status AS last_http_status,
            latest.outcome AS last_outcome,
            latest.duration_ms,
            CASE
                WHEN latest.outcome = 'success' THEN NULL
                ELSE COALESCE(d.last_error, latest.error_message)
            END AS last_error,
            d.next_attempt_at,
            d.scenario_id,
            d.operator_resolved_at,
            d.operator_resolved_by,
            d.operator_resolution_note,
            d.created_at,
            d.updated_at
         FROM webhook_deliveries d
         INNER JOIN domain_events e
            ON e.event_id = d.event_id
         LEFT JOIN delivery_attempts a
            ON a.delivery_id = d.delivery_id
         LEFT JOIN LATERAL (
            SELECT
                attempt_id,
                http_status,
                outcome,
                error_message,
                duration_ms
            FROM delivery_attempts
            WHERE delivery_id = d.delivery_id
            ORDER BY attempt_count DESC, attempt_id DESC
            LIMIT 1
         ) latest ON TRUE
         WHERE ($1::BIGINT IS NULL OR d.merchant_id = $1)
           AND ($2::BIGINT IS NULL OR d.endpoint_id = $2)
           AND ($3::BIGINT IS NULL OR d.event_id = $3)
           AND ($4::TEXT IS NULL OR d.status = $4)
           AND ($5::TEXT IS NULL OR e.event_type = $5)
           AND (
                ($6::TEXT IS NULL AND $7::BIGINT IS NULL)
                OR d.endpoint_url ILIKE $6
                OR d.endpoint_id = $7
           )
           AND (
                (
                    $8::BIGINT IS NULL
                    AND $9::BIGINT IS NULL
                    AND $10::BIGINT IS NULL
                    AND $11::BIGINT IS NULL
                    AND $12::BIGINT IS NULL
                    AND $13::TEXT IS NULL
                    AND $14::TEXT IS NULL
                    AND $15::TEXT IS NULL
                    AND $16::TEXT IS NULL
                )
                OR d.delivery_id = $8
                OR d.event_id = $9
                OR d.merchant_id = $10
                OR d.endpoint_id = $11
                OR latest.http_status::BIGINT = $12
                OR e.event_type = $13
                OR d.status = $14
                OR latest.outcome = $15
                OR d.endpoint_url ILIKE $16
           )
           AND ($17::TIMESTAMPTZ IS NULL OR d.created_at >= $17)
           AND ($18::BIGINT IS NULL OR d.delivery_id < $18)
           AND ($21::BIGINT IS NULL OR d.scenario_id = $21)
           AND (
                $19::TEXT IS NULL
                OR ($19 = 'resolved' AND d.operator_resolved_at IS NOT NULL)
                OR ($19 = 'unresolved' AND d.operator_resolved_at IS NULL)
           )
         GROUP BY d.delivery_id, e.event_type, latest.http_status, latest.outcome, latest.error_message, latest.duration_ms
         ORDER BY d.delivery_id DESC
         LIMIT $20",
    )
    .bind(query.merchant_id)
    .bind(query.endpoint_id)
    .bind(query.event_id)
    .bind(status)
    .bind(event_type)
    .bind(endpoint.pattern)
    .bind(endpoint.number)
    .bind(search.delivery_id)
    .bind(search.event_id)
    .bind(search.merchant_id)
    .bind(search.endpoint_id)
    .bind(search.http_status)
    .bind(search.event_type)
    .bind(search.status)
    .bind(search.outcome)
    .bind(search.endpoint_pattern)
    .bind(time_range_start)
    .bind(query.cursor)
    .bind(resolution)
    .bind(fetch_limit)
    .bind(query.scenario_id)
    .fetch_all(pool)
    .await?;

    let mut items = rows;
    let next_cursor = if items.len() > limit as usize {
        items.pop().map(|item| item.delivery_id)
    } else {
        None
    };

    Ok(PaginatedDeliveriesResponse {
        items,
        next_cursor,
        limit,
    })
}

pub async fn list_deliveries_for_event(
    pool: &PgPool,
    event_id: i64,
) -> AppResult<Vec<EventFanoutDeliveryItem>> {
    let rows = sqlx::query_as::<_, EventFanoutDeliveryItem>(
        "SELECT
            d.delivery_id,
            d.event_id,
            e.event_type,
            d.merchant_id,
            d.endpoint_id,
            d.endpoint_url,
            d.status,
            COUNT(a.attempt_id)::BIGINT AS attempt_count,
            d.max_attempts,
            latest.http_status AS last_http_status,
            latest.outcome AS last_outcome,
            latest.duration_ms,
            CASE
                WHEN latest.outcome = 'success' THEN NULL
                ELSE COALESCE(d.last_error, latest.error_message)
            END AS last_error,
            d.next_attempt_at,
            d.scenario_id,
            d.operator_resolved_at,
            d.operator_resolved_by,
            d.operator_resolution_note,
            d.created_at,
            d.updated_at,
            endpoint.description AS endpoint_description,
            endpoint.enabled AS endpoint_enabled,
            COALESCE(subscriptions.subscribed_events, ARRAY[]::TEXT[]) AS endpoint_subscribed_events
         FROM webhook_deliveries d
         INNER JOIN domain_events e
            ON e.event_id = d.event_id
         INNER JOIN webhook_endpoints endpoint
            ON endpoint.endpoint_id = d.endpoint_id
         LEFT JOIN delivery_attempts a
            ON a.delivery_id = d.delivery_id
         LEFT JOIN LATERAL (
            SELECT
                attempt_id,
                http_status,
                outcome,
                error_message,
                duration_ms
            FROM delivery_attempts
            WHERE delivery_id = d.delivery_id
            ORDER BY attempt_count DESC, attempt_id DESC
            LIMIT 1
         ) latest ON TRUE
         LEFT JOIN LATERAL (
            SELECT array_agg(s.event_type ORDER BY s.event_type) AS subscribed_events
            FROM webhook_endpoint_subscriptions s
            WHERE s.endpoint_id = d.endpoint_id
         ) subscriptions ON TRUE
         WHERE d.event_id = $1
         GROUP BY
            d.delivery_id,
            e.event_type,
            endpoint.description,
            endpoint.enabled,
            subscriptions.subscribed_events,
            latest.http_status,
            latest.outcome,
            latest.error_message,
            latest.duration_ms
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
            e.event_type,
            d.merchant_id,
            d.endpoint_id,
            d.endpoint_url,
            d.status,
            COUNT(a.attempt_id)::BIGINT AS attempt_count,
            d.max_attempts,
            latest.http_status AS last_http_status,
            latest.outcome AS last_outcome,
            latest.duration_ms,
            CASE
                WHEN latest.outcome = 'success' THEN NULL
                ELSE COALESCE(d.last_error, latest.error_message)
            END AS last_error,
            d.next_attempt_at,
            d.scenario_id,
            d.operator_resolved_at,
            d.operator_resolved_by,
            d.operator_resolution_note,
            d.created_at,
            d.updated_at
         FROM webhook_deliveries d
         INNER JOIN domain_events e
            ON e.event_id = d.event_id
         LEFT JOIN delivery_attempts a
            ON a.delivery_id = d.delivery_id
         LEFT JOIN LATERAL (
            SELECT
                attempt_id,
                http_status,
                outcome,
                error_message,
                duration_ms
            FROM delivery_attempts
            WHERE delivery_id = d.delivery_id
            ORDER BY attempt_count DESC, attempt_id DESC
            LIMIT 1
         ) latest ON TRUE
         WHERE d.delivery_id = $1
         GROUP BY d.delivery_id, e.event_type, latest.http_status, latest.outcome, latest.error_message, latest.duration_ms",
    )
    .bind(delivery_id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| AppError::NotFound(format!("delivery {} not found", delivery_id)))?;

    Ok(row)
}

pub async fn get_delivery_detail(
    pool: &PgPool,
    delivery_id: i64,
) -> AppResult<DeliveryDetailResponse> {
    let delivery = get_delivery(pool, delivery_id).await?;
    let event = get_event(pool, delivery.event_id).await?;
    let attempts = list_delivery_attempts(pool, delivery_id).await?;
    let trace = list_delivery_trace(pool, delivery_id).await?;
    let retry_lineage = get_delivery_retry_lineage(pool, delivery_id).await?;

    Ok(DeliveryDetailResponse {
        delivery,
        event,
        attempts,
        trace,
        retry_lineage,
    })
}

async fn get_delivery_retry_lineage(
    pool: &PgPool,
    delivery_id: i64,
) -> AppResult<DeliveryRetryLineage> {
    let original_delivery_id: Option<i64> = sqlx::query(
        "SELECT manually_retried_from_delivery_id
         FROM webhook_deliveries
         WHERE delivery_id = $1",
    )
    .bind(delivery_id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| AppError::NotFound(format!("delivery {} not found", delivery_id)))?
    .get("manually_retried_from_delivery_id");

    let retry_rows = sqlx::query(
        "SELECT delivery_id
         FROM webhook_deliveries
         WHERE manually_retried_from_delivery_id = $1
         ORDER BY delivery_id ASC",
    )
    .bind(delivery_id)
    .fetch_all(pool)
    .await?;

    Ok(DeliveryRetryLineage {
        original_delivery_id,
        retry_delivery_ids: retry_rows
            .into_iter()
            .map(|row| row.get("delivery_id"))
            .collect(),
    })
}
