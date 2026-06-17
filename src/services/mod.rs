use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    time::Duration,
};

use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx::{PgPool, Postgres, Row, Transaction};
use url::Url;

use crate::{
    delivery_worker::{DeliveryTransport, WorkerConfig},
    error::{AppError, AppResult},
    models::{
        BulkRetryDeliveriesRequest, BulkRetryDeliveriesResponse, BulkRetrySkip,
        CreateBulkPaymentsRequest, CreateBulkPaymentsResponse, CreateEndpointRequest,
        CreateEndpointResponse, CreatePaymentRequest, CreatePaymentResponse, DashboardSummary,
        DeliveryAttemptItem, DeliveryDetailResponse, DeliveryListItem, DeliveryListQuery,
        DeliveryRetryLineage, DeliveryTraceItem, EndpointDeliveriesQuery, EndpointDeliveryItem,
        EndpointDetailResponse, EndpointHealthSummary, EndpointListItem, EndpointStatsItem,
        EventFanoutDeliveryItem, EventFanoutResponse, EventListItem, EventListQuery,
        PaginatedDeliveriesResponse, PaginatedEndpointDeliveriesResponse, PaginatedEventsResponse,
        PaymentStatusInput, ResolveDeliveryRequest, ResolveDeliveryResponse, RetryBacklogSummary,
        RetryDeliveryRequest, RetryDeliveryResponse, ScenarioArtifacts, ScenarioDetailResponse,
        ScenarioPlanDetail, ScenarioPlannedEndpoint, ScenarioPlannedExpectations,
        ScenarioRunRequest, ScenarioRunResponse, ScenarioSummary, TestEndpointRequest,
        TestEndpointResponse, TraceGraphEdge, TraceGraphNode, TraceGraphResponse,
        UnresolveDeliveryResponse, UpdateEndpointRequest,
    },
};

mod scenarios;

pub use scenarios::{
    PlannedScenarioEndpoint, PlannedScenarioRun, build_scenario_run_plan, plan_scenario_endpoints,
    receiver_behavior_catalog, resolve_receiver_behavior, resolve_scenario_key, scenario_catalog,
};

const ALLOWED_EVENT_TYPES: &[&str] = &["payment_succeeded", "payment_failed", "payment_refund"];
const DELIVERY_STATUSES: &[&str] = &[
    "pending",
    "queued",
    "processing",
    "retrying",
    "delivered",
    "dead_lettered",
];

pub fn allowed_event_types() -> Vec<&'static str> {
    ALLOWED_EVENT_TYPES.to_vec()
}
const ATTEMPT_OUTCOMES: &[&str] = &[
    "success",
    "temporary_failure",
    "permanent_failure",
    "timeout",
    "abandoned",
    "unknown",
];

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

#[derive(Debug, Clone)]
struct EventSearch {
    event_id: Option<i64>,
    merchant_id: Option<i64>,
    object_id: Option<i64>,
    event_type: Option<String>,
    object_type: Option<String>,
}

fn normalize_search(value: Option<&str>) -> Option<String> {
    let value = value?.trim();

    if value.is_empty() {
        None
    } else {
        Some(format!("%{}%", value))
    }
}

fn normalize_exact_filter(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty() && !value.eq_ignore_ascii_case("all"))
}

fn normalize_resolution_filter(value: Option<String>) -> AppResult<Option<String>> {
    let Some(value) = normalize_exact_filter(value) else {
        return Ok(None);
    };

    if matches!(value.as_str(), "resolved" | "unresolved") {
        Ok(Some(value))
    } else {
        Err(AppError::BadRequest(
            "resolution must be resolved, unresolved, or all".to_string(),
        ))
    }
}

fn normalize_optional_text(
    value: Option<String>,
    field: &str,
    max_len: usize,
) -> AppResult<Option<String>> {
    let Some(value) = value else {
        return Ok(None);
    };

    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }

    if value.len() > max_len {
        return Err(AppError::BadRequest(format!(
            "{} must be {} characters or fewer",
            field, max_len
        )));
    }

    Ok(Some(value.to_string()))
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

fn event_search(value: Option<&str>) -> EventSearch {
    let Some(raw_value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return EventSearch {
            event_id: None,
            merchant_id: None,
            object_id: None,
            event_type: None,
            object_type: None,
        };
    };

    let normalized = raw_value.to_ascii_lowercase();
    let number = extract_search_number(Some(raw_value));
    let mut search = EventSearch {
        event_id: None,
        merchant_id: None,
        object_id: None,
        event_type: None,
        object_type: None,
    };

    if normalized.starts_with("evt_") || normalized.starts_with("event ") {
        search.event_id = number;
        return search;
    }

    if normalized.starts_with("merchant ") {
        search.merchant_id = number;
        return search;
    }

    if normalized.starts_with("object ") || normalized.starts_with("payment ") {
        search.object_id = number;
        return search;
    }

    if let Some(number) = number {
        search.event_id = Some(number);
        search.merchant_id = Some(number);
        search.object_id = Some(number);
        return search;
    }

    if ALLOWED_EVENT_TYPES.contains(&normalized.as_str()) {
        search.event_type = Some(normalized);
    } else if normalized == "payment" {
        search.object_type = Some(normalized);
    }

    search
}

fn extract_search_number(value: Option<&str>) -> Option<i64> {
    let value = value?.trim();
    let mut digits = String::new();

    for character in value.chars() {
        if character.is_ascii_digit() {
            digits.push(character);
        } else if !digits.is_empty() {
            break;
        }
    }

    if digits.is_empty() {
        None
    } else {
        digits.parse().ok()
    }
}

fn time_range_start(value: Option<&str>) -> AppResult<Option<DateTime<Utc>>> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };

    let now = Utc::now();
    let start = match value {
        "15m" | "last_15m" => Some(now - chrono::Duration::minutes(15)),
        "1h" | "last_1h" => Some(now - chrono::Duration::hours(1)),
        "24h" | "last_24h" => Some(now - chrono::Duration::hours(24)),
        "7d" | "last_7d" => Some(now - chrono::Duration::days(7)),
        "all" | "all_time" => None,
        other => {
            return Err(AppError::BadRequest(format!(
                "unsupported time_range {}",
                other
            )));
        }
    };

    Ok(start)
}

async fn redis_pending_from_runtime() -> Option<i64> {
    let config = WorkerConfig::from_env();

    if !config.enabled || config.transport != DeliveryTransport::Redis {
        return None;
    }

    let redis_url = config.redis_url.as_deref()?;

    let pending = redis_pending(
        redis_url,
        &config.redis_stream,
        &config.redis_consumer_group,
    );

    match tokio::time::timeout(Duration::from_millis(750), pending).await {
        Ok(Ok(value)) => Some(value),
        Ok(Err(error)) if error.to_string().contains("NOGROUP") => Some(0),
        Ok(Err(error)) => {
            tracing::warn!(%error, "failed to read Redis consumer-group pending count");
            None
        }
        Err(_) => {
            tracing::warn!("timed out reading Redis consumer-group pending count");
            None
        }
    }
}

async fn redis_pending(redis_url: &str, stream: &str, group: &str) -> redis::RedisResult<i64> {
    let client = redis::Client::open(redis_url)?;
    let mut connection = client.get_connection_manager().await?;
    let value: redis::Value = redis::cmd("XPENDING")
        .arg(stream)
        .arg(group)
        .query_async(&mut connection)
        .await?;

    Ok(parse_xpending_count(&value).unwrap_or(0))
}

fn parse_xpending_count(value: &redis::Value) -> Option<i64> {
    match value {
        redis::Value::Array(items) => items.first().and_then(redis_value_to_i64),
        redis::Value::Int(value) => Some(*value),
        _ => None,
    }
}

fn redis_value_to_i64(value: &redis::Value) -> Option<i64> {
    match value {
        redis::Value::BulkString(bytes) => std::str::from_utf8(bytes).ok()?.parse().ok(),
        redis::Value::Int(value) => Some(*value),
        redis::Value::SimpleString(value) => value.parse().ok(),
        _ => None,
    }
}

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
         VALUES ($1, $2, TRUE, $3, $4, NOW(), NOW())
         RETURNING endpoint_id",
    )
    .bind(request.merchant_id)
    .bind(request.url)
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

pub async fn create_payment(
    pool: &PgPool,
    request: CreatePaymentRequest,
) -> AppResult<CreatePaymentResponse> {
    validate_payment_request(&request)?;

    let mut tx = pool.begin().await?;
    let response = create_payment_in_tx(&mut tx, request, None).await?;
    tx.commit().await?;

    Ok(response)
}

async fn create_payment_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    request: CreatePaymentRequest,
    scenario_id: Option<i64>,
) -> AppResult<CreatePaymentResponse> {
    let status = request.status.as_db_str();
    let event_type = request.status.event_type();

    let payment_id: i64 = sqlx::query(
        "INSERT INTO payments (
            scenario_id,
            merchant_id,
            order_id,
            amount,
            status,
            mode_of_payment,
            created_at,
            updated_at
         )
         VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
         RETURNING payment_id",
    )
    .bind(scenario_id)
    .bind(request.merchant_id)
    .bind(request.order_id)
    .bind(request.amount)
    .bind(status)
    .bind(&request.mode_of_payment)
    .fetch_one(&mut **tx)
    .await?
    .get(0);

    let event_id: i64 = sqlx::query(
        "INSERT INTO domain_events (
            scenario_id,
            merchant_id,
            object_type,
            object_id,
            event_type,
            created_at
         )
         VALUES ($1, $2, 'payment', $3, $4, NOW())
         RETURNING event_id",
    )
    .bind(scenario_id)
    .bind(request.merchant_id)
    .bind(payment_id)
    .bind(event_type)
    .fetch_one(&mut **tx)
    .await?
    .get(0);

    let delivery_count: i64 = sqlx::query(
        "WITH subscribed_endpoints AS (
            SELECT
                e.endpoint_id,
                e.merchant_id,
                e.url,
                e.active_secret_version_id,
                e.max_attempts
            FROM webhook_endpoints e
            INNER JOIN webhook_endpoint_subscriptions s
                ON s.endpoint_id = e.endpoint_id
            WHERE e.merchant_id = $1
              AND e.enabled = TRUE
              AND s.event_type = $2
         ),
         inserted AS (
            INSERT INTO webhook_deliveries (
                scenario_id,
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
                $4,
                $3,
                endpoint_id,
                merchant_id,
                url,
                active_secret_version_id,
                'pending',
                max_attempts,
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
    .bind(scenario_id)
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
        let response = create_payment_in_tx(&mut tx, payment, None).await?;
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
    validate_endpoint_url_with_local_policy(url, allow_local_webhook_targets())
}

fn validate_endpoint_url_with_local_policy(url: &str, allow_local_targets: bool) -> AppResult<()> {
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

    if !allow_local_targets && is_blocked_webhook_host(parsed.host_str().unwrap_or_default()) {
        return Err(AppError::BadRequest(
            "endpoint URL targets localhost, private, link-local, multicast, metadata, or unspecified addresses; set ALLOW_LOCAL_WEBHOOK_TARGETS=1 for local demo targets".to_string(),
        ));
    }

    Ok(())
}

fn allow_local_webhook_targets() -> bool {
    std::env::var("ALLOW_LOCAL_WEBHOOK_TARGETS")
        .map(|value| value == "1")
        .unwrap_or(false)
}

fn is_blocked_webhook_host(host: &str) -> bool {
    let host = host.trim().trim_matches(['[', ']']).to_ascii_lowercase();

    if matches!(
        host.as_str(),
        "localhost" | "metadata" | "metadata.google.internal"
    ) {
        return true;
    }

    match host.parse::<IpAddr>() {
        Ok(IpAddr::V4(addr)) => is_blocked_ipv4(addr),
        Ok(IpAddr::V6(addr)) => is_blocked_ipv6(addr),
        Err(_) => false,
    }
}

fn is_blocked_ipv4(addr: Ipv4Addr) -> bool {
    addr.is_loopback()
        || addr.is_private()
        || addr.is_link_local()
        || addr.is_multicast()
        || addr.is_unspecified()
        || addr == Ipv4Addr::new(169, 254, 169, 254)
}

fn is_blocked_ipv6(addr: Ipv6Addr) -> bool {
    addr.is_loopback()
        || addr.is_multicast()
        || addr.is_unspecified()
        || is_ipv6_unique_local(addr)
        || is_ipv6_unicast_link_local(addr)
}

fn is_ipv6_unique_local(addr: Ipv6Addr) -> bool {
    (addr.segments()[0] & 0xfe00) == 0xfc00
}

fn is_ipv6_unicast_link_local(addr: Ipv6Addr) -> bool {
    (addr.segments()[0] & 0xffc0) == 0xfe80
}

fn validate_max_attempts(max_attempts: Option<i64>) -> AppResult<i64> {
    let max_attempts = max_attempts.unwrap_or(5);

    if !(1..=20).contains(&max_attempts) {
        return Err(AppError::BadRequest(
            "max_attempts must be between 1 and 20".to_string(),
        ));
    }

    Ok(max_attempts)
}

fn validate_endpoint_count(endpoint_count: Option<i64>, default_value: i64) -> AppResult<i64> {
    let endpoint_count = endpoint_count.unwrap_or(default_value);

    if !(1..=20).contains(&endpoint_count) {
        return Err(AppError::BadRequest(
            "endpoint_count must be between 1 and 20".to_string(),
        ));
    }

    Ok(endpoint_count)
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

pub async fn list_events(
    pool: &PgPool,
    query: EventListQuery,
) -> AppResult<PaginatedEventsResponse> {
    let limit = query.limit.unwrap_or(50).clamp(1, 100);
    let fetch_limit = limit + 1;
    let event_type = normalize_exact_filter(query.event_type);
    let search = event_search(query.search.as_deref());

    let rows = sqlx::query_as::<_, EventListItem>(
        "SELECT
            e.event_id,
            e.merchant_id,
            e.object_type,
            e.object_id,
            e.event_type,
            e.created_at,
            e.scenario_id,
            COUNT(d.delivery_id)::BIGINT AS delivery_count,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'pending')::BIGINT AS pending_count,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'queued')::BIGINT AS queued_count,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'processing')::BIGINT AS processing_count,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'retrying')::BIGINT AS retrying_count,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'delivered')::BIGINT AS delivered_count,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'dead_lettered')::BIGINT AS dead_lettered_count
         FROM domain_events e
         LEFT JOIN webhook_deliveries d
            ON d.event_id = e.event_id
         WHERE ($1::BIGINT IS NULL OR e.merchant_id = $1)
           AND ($2::TEXT IS NULL OR e.event_type = $2)
           AND ($10::BIGINT IS NULL OR e.scenario_id = $10)
           AND (
                (
                    $3::BIGINT IS NULL
                    AND $4::BIGINT IS NULL
                    AND $5::BIGINT IS NULL
                    AND $6::TEXT IS NULL
                    AND $7::TEXT IS NULL
                )
                OR e.event_id = $3
                OR e.merchant_id = $4
                OR e.object_id = $5
                OR e.event_type = $6
                OR e.object_type = $7
           )
           AND ($8::BIGINT IS NULL OR e.event_id < $8)
         GROUP BY e.event_id
         ORDER BY e.event_id DESC
         LIMIT $9",
    )
    .bind(query.merchant_id)
    .bind(event_type)
    .bind(search.event_id)
    .bind(search.merchant_id)
    .bind(search.object_id)
    .bind(search.event_type)
    .bind(search.object_type)
    .bind(query.cursor)
    .bind(fetch_limit)
    .bind(query.scenario_id)
    .fetch_all(pool)
    .await?;

    let mut items = rows;
    let next_cursor = if items.len() > limit as usize {
        items.pop().map(|item| item.event_id)
    } else {
        None
    };

    Ok(PaginatedEventsResponse {
        items,
        next_cursor,
        limit,
    })
}

pub async fn get_event(pool: &PgPool, event_id: i64) -> AppResult<EventListItem> {
    let event = sqlx::query_as::<_, EventListItem>(
        "SELECT
            e.event_id,
            e.merchant_id,
            e.object_type,
            e.object_id,
            e.event_type,
            e.created_at,
            e.scenario_id,
            COUNT(d.delivery_id)::BIGINT AS delivery_count,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'pending')::BIGINT AS pending_count,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'queued')::BIGINT AS queued_count,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'processing')::BIGINT AS processing_count,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'retrying')::BIGINT AS retrying_count,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'delivered')::BIGINT AS delivered_count,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'dead_lettered')::BIGINT AS dead_lettered_count
         FROM domain_events e
         LEFT JOIN webhook_deliveries d
            ON d.event_id = e.event_id
         WHERE e.event_id = $1
         GROUP BY e.event_id",
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

fn payment_status_input_for_event_type(event_type: &str) -> AppResult<PaymentStatusInput> {
    match event_type {
        "payment_succeeded" => Ok(PaymentStatusInput::Succeeded),
        "payment_failed" => Ok(PaymentStatusInput::Failed),
        "payment_refund" => Ok(PaymentStatusInput::Refunded),
        _ => Err(AppError::BadRequest(format!(
            "unsupported event type {}",
            event_type
        ))),
    }
}

pub async fn run_scenario(
    pool: &PgPool,
    receiver_base_url: &str,
    request: ScenarioRunRequest,
) -> AppResult<ScenarioRunResponse> {
    let config = request.config.unwrap_or_default();
    let merchant_id = config
        .merchant_id
        .unwrap_or_else(|| 700_000_000 + (Utc::now().timestamp_micros() % 100_000_000));
    let plan = build_scenario_run_plan(&request.scenario_key, merchant_id, &config)?;
    let requested_by = normalize_optional_text(request.requested_by, "requested_by", 200)?;
    let config_json = json!({
        "requested": serde_json::to_value(&config).unwrap_or_else(|_| json!({})),
        "effective": {
            "scenario_key": plan.scenario_key,
            "scenario_kind": plan.scenario_kind,
            "merchant_id": plan.merchant_id,
            "payment_count": plan.payment_count,
            "event_type": plan.event_type,
            "endpoint_count": plan.expected_endpoint_count,
            "receiver_behavior": plan.receiver_behavior,
            "expected_payment_count": plan.expected_payment_count,
            "expected_event_count": plan.expected_event_count,
            "expected_endpoint_count": plan.expected_endpoint_count,
            "expected_delivery_count": plan.expected_delivery_count
        }
    });
    let initial_step_log = scenario_plan_step_log(&plan);

    let mut tx = pool.begin().await?;
    let row = sqlx::query(
        "INSERT INTO scenarios (
            scenario_key,
            status,
            requested_by,
            merchant_id,
            config_json,
            receiver_config_json,
            step_log_json,
            started_at
         )
         VALUES ($1, 'running', $2, $3, $4, '[]'::jsonb, $5, NOW())
         RETURNING scenario_id, started_at",
    )
    .bind(&plan.scenario_key)
    .bind(requested_by.as_deref())
    .bind(plan.merchant_id)
    .bind(config_json)
    .bind(json!(initial_step_log))
    .fetch_one(&mut *tx)
    .await?;

    let scenario_id: i64 = row.get("scenario_id");
    let started_at: DateTime<Utc> = row.get("started_at");
    persist_scenario_plan(&mut tx, scenario_id, &plan).await?;
    tx.commit().await?;

    if let Err(error) = setup_scenario(pool, receiver_base_url, scenario_id, &plan).await {
        let message = error.to_string();
        sqlx::query(
            "UPDATE scenarios
             SET status = 'failed',
                 error_message = $2,
                 completed_at = NOW()
             WHERE scenario_id = $1",
        )
        .bind(scenario_id)
        .bind(&message)
        .execute(pool)
        .await?;
        return Err(error);
    }

    Ok(ScenarioRunResponse {
        scenario_id,
        scenario_key: plan.scenario_key,
        status: "running".to_string(),
        started_at,
        merchant_id: plan.merchant_id,
    })
}

async fn setup_scenario(
    pool: &PgPool,
    receiver_base_url: &str,
    scenario_id: i64,
    plan: &PlannedScenarioRun,
) -> AppResult<()> {
    let mut receiver_configs = Vec::with_capacity(plan.endpoints.len());
    let mut step_log = scenario_plan_step_log(plan);
    persist_scenario_runtime_state(pool, scenario_id, &receiver_configs, &step_log).await?;

    for endpoint in &plan.endpoints {
        configure_mock_receiver_endpoint(receiver_base_url, plan.merchant_id, endpoint).await?;
        receiver_configs.push(json!({
            "merchant_id": plan.merchant_id,
            "ordinal": endpoint.ordinal,
            "endpoint_role": endpoint.endpoint_role,
            "endpoint_key": endpoint.endpoint_key,
            "behavior_key": endpoint.behavior_key,
            "behavior": endpoint.behavior,
            "base_delay_ms": endpoint.base_delay_ms,
            "max_attempts": endpoint.max_attempts,
            "event_types": endpoint.event_types
        }));

        let response = create_endpoint(
            pool,
            CreateEndpointRequest {
                merchant_id: plan.merchant_id,
                url: format!(
                    "{}/webhook/{}/{}",
                    receiver_base_url.trim_end_matches('/'),
                    plan.merchant_id,
                    endpoint.endpoint_key
                ),
                secret: endpoint.secret.clone(),
                description: Some(endpoint.description.clone()),
                max_attempts: Some(endpoint.max_attempts),
                subscribed_events: endpoint.event_types.clone(),
            },
        )
        .await?;
        sqlx::query(
            "UPDATE scenario_run_endpoints
             SET created_endpoint_id = $3
             WHERE scenario_id = $1 AND ordinal = $2",
        )
        .bind(scenario_id)
        .bind(endpoint.ordinal)
        .bind(response.endpoint_id)
        .execute(pool)
        .await?;
        step_log.push(json!({
            "step": "endpoint_created",
            "endpoint_id": response.endpoint_id,
            "endpoint_key": endpoint.endpoint_key,
            "ordinal": endpoint.ordinal
        }));
        persist_scenario_runtime_state(pool, scenario_id, &receiver_configs, &step_log).await?;
    }

    let mut tx = pool.begin().await?;
    for index in 0..plan.payment_count {
        let created = create_payment_in_tx(
            &mut tx,
            CreatePaymentRequest {
                merchant_id: plan.merchant_id,
                order_id: Utc::now().timestamp_micros() + index,
                amount: 1_000 + index,
                status: plan.payment_status.clone(),
                mode_of_payment: "scenario_lab".to_string(),
            },
            Some(scenario_id),
        )
        .await?;
        step_log.push(json!({
            "step": "payment_created",
            "payment_id": created.payment_id,
            "event_id": created.event_id,
            "delivery_count": created.delivery_count
        }));
    }
    tx.commit().await?;
    persist_scenario_runtime_state(pool, scenario_id, &receiver_configs, &step_log).await?;

    Ok(())
}

fn scenario_plan_step_log(plan: &PlannedScenarioRun) -> Vec<serde_json::Value> {
    vec![json!({
        "step": "plan_persisted",
        "scenario_key": plan.scenario_key,
        "scenario_kind": plan.scenario_kind,
        "event_type": plan.event_type,
        "payment_count": plan.payment_count,
        "endpoint_count": plan.expected_endpoint_count,
        "expected_delivery_count": plan.expected_delivery_count
    })]
}

async fn persist_scenario_plan(
    tx: &mut Transaction<'_, Postgres>,
    scenario_id: i64,
    plan: &PlannedScenarioRun,
) -> AppResult<()> {
    sqlx::query(
        "INSERT INTO scenario_run_expectations (
            scenario_id,
            expected_payment_count,
            expected_event_count,
            expected_endpoint_count,
            expected_delivery_count
         )
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(scenario_id)
    .bind(plan.expected_payment_count)
    .bind(plan.expected_event_count)
    .bind(plan.expected_endpoint_count)
    .bind(plan.expected_delivery_count)
    .execute(&mut **tx)
    .await?;

    for endpoint in &plan.endpoints {
        sqlx::query(
            "INSERT INTO scenario_run_endpoints (
                scenario_id,
                ordinal,
                endpoint_role,
                endpoint_key,
                behavior_key,
                behavior_json,
                base_delay_ms,
                max_attempts
             )
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(scenario_id)
        .bind(endpoint.ordinal)
        .bind(endpoint.endpoint_role.as_deref())
        .bind(&endpoint.endpoint_key)
        .bind(&endpoint.behavior_key)
        .bind(&endpoint.behavior)
        .bind(endpoint.base_delay_ms)
        .bind(endpoint.max_attempts)
        .execute(&mut **tx)
        .await?;
    }

    Ok(())
}

async fn persist_scenario_runtime_state(
    pool: &PgPool,
    scenario_id: i64,
    receiver_configs: &[serde_json::Value],
    step_log: &[serde_json::Value],
) -> AppResult<()> {
    sqlx::query(
        "UPDATE scenarios
         SET receiver_config_json = $2,
             step_log_json = $3
         WHERE scenario_id = $1",
    )
    .bind(scenario_id)
    .bind(json!(receiver_configs))
    .bind(json!(step_log))
    .execute(pool)
    .await?;

    Ok(())
}

async fn configure_mock_receiver_endpoint(
    receiver_base_url: &str,
    merchant_id: i64,
    endpoint: &PlannedScenarioEndpoint,
) -> AppResult<()> {
    let url = format!(
        "{}/admin/endpoints",
        receiver_base_url.trim_end_matches('/')
    );
    let response = reqwest::Client::new()
        .post(url)
        .json(&json!({
            "merchant_id": merchant_id,
            "endpoint_key": endpoint.endpoint_key,
            "behavior": endpoint.behavior,
            "base_delay_ms": endpoint.base_delay_ms,
            "secret": endpoint.secret,
            "verify_signature": true,
            "enabled": true
        }))
        .send()
        .await
        .map_err(|error| AppError::BadRequest(format!("mock receiver is unreachable: {error}")))?;

    if !response.status().is_success() {
        return Err(AppError::BadRequest(format!(
            "mock receiver endpoint configuration failed with status {}",
            response.status()
        )));
    }

    Ok(())
}

pub async fn get_scenario(pool: &PgPool, scenario_id: i64) -> AppResult<ScenarioDetailResponse> {
    let scenario = sqlx::query(
        "SELECT
            scenario_id,
            scenario_key,
            status,
            requested_by,
            merchant_id,
            receiver_config_json,
            step_log_json,
            error_message,
            started_at,
            completed_at
         FROM scenarios
         WHERE scenario_id = $1",
    )
    .bind(scenario_id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| AppError::NotFound(format!("scenario {} not found", scenario_id)))?;

    let summary_row = sqlx::query(
        "SELECT
            (SELECT COUNT(*)::BIGINT FROM payments WHERE scenario_id = $1) AS payments_created,
            (SELECT COUNT(*)::BIGINT FROM domain_events WHERE scenario_id = $1) AS events_created,
            COUNT(d.delivery_id)::BIGINT AS deliveries_created,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'delivered')::BIGINT AS delivered_count,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'retrying')::BIGINT AS retrying_count,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'dead_lettered')::BIGINT AS dead_lettered_count,
            COUNT(d.delivery_id) FILTER (WHERE d.status IN ('pending', 'queued', 'processing', 'retrying'))::BIGINT AS active_count
         FROM webhook_deliveries d
         WHERE d.scenario_id = $1",
    )
    .bind(scenario_id)
    .fetch_one(pool)
    .await?;

    let artifacts_row = sqlx::query(
        "SELECT
            COALESCE((SELECT array_agg(payment_id ORDER BY payment_id) FROM payments WHERE scenario_id = $1), ARRAY[]::BIGINT[]) AS payment_ids,
            COALESCE((SELECT array_agg(event_id ORDER BY event_id) FROM domain_events WHERE scenario_id = $1), ARRAY[]::BIGINT[]) AS event_ids,
            COALESCE((SELECT array_agg(delivery_id ORDER BY delivery_id) FROM webhook_deliveries WHERE scenario_id = $1), ARRAY[]::BIGINT[]) AS delivery_ids,
            COALESCE((SELECT array_agg(DISTINCT endpoint_id ORDER BY endpoint_id) FROM webhook_deliveries WHERE scenario_id = $1), ARRAY[]::BIGINT[]) AS endpoint_ids",
    )
    .bind(scenario_id)
    .fetch_one(pool)
    .await?;
    let expectations_row = sqlx::query(
        "SELECT
            expected_payment_count,
            expected_event_count,
            expected_endpoint_count,
            expected_delivery_count
         FROM scenario_run_expectations
         WHERE scenario_id = $1",
    )
    .bind(scenario_id)
    .fetch_optional(pool)
    .await?;
    let planned_endpoint_rows = sqlx::query(
        "SELECT
            ordinal,
            endpoint_role,
            endpoint_key,
            behavior_key,
            behavior_json,
            base_delay_ms,
            max_attempts,
            created_endpoint_id
         FROM scenario_run_endpoints
         WHERE scenario_id = $1
         ORDER BY ordinal",
    )
    .bind(scenario_id)
    .fetch_all(pool)
    .await?;

    let db_status: String = scenario.get("status");
    let active_count: i64 = summary_row.get("active_count");
    let deliveries_created: i64 = summary_row.get("deliveries_created");
    let computed_status = if db_status == "failed" {
        "failed"
    } else if deliveries_created > 0 && active_count == 0 {
        "completed"
    } else {
        "running"
    };

    if computed_status == "completed" && db_status != "completed" {
        sqlx::query(
            "UPDATE scenarios
             SET status = 'completed',
                 completed_at = COALESCE(completed_at, NOW())
             WHERE scenario_id = $1",
        )
        .bind(scenario_id)
        .execute(pool)
        .await?;
    }

    let planned = expectations_row.map(|expectations| ScenarioPlanDetail {
        expectations: ScenarioPlannedExpectations {
            payment_count: expectations.get("expected_payment_count"),
            event_count: expectations.get("expected_event_count"),
            endpoint_count: expectations.get("expected_endpoint_count"),
            delivery_count: expectations.get("expected_delivery_count"),
        },
        endpoints: planned_endpoint_rows
            .into_iter()
            .map(|row| ScenarioPlannedEndpoint {
                ordinal: row.get("ordinal"),
                endpoint_role: row.get("endpoint_role"),
                endpoint_key: row.get("endpoint_key"),
                behavior_key: row.get("behavior_key"),
                behavior: row.get("behavior_json"),
                base_delay_ms: row.get("base_delay_ms"),
                max_attempts: row.get("max_attempts"),
                created_endpoint_id: row.get("created_endpoint_id"),
            })
            .collect(),
    });

    Ok(ScenarioDetailResponse {
        scenario_id,
        scenario_key: scenario.get("scenario_key"),
        status: computed_status.to_string(),
        started_at: scenario.get("started_at"),
        completed_at: scenario.get("completed_at"),
        merchant_id: scenario.get("merchant_id"),
        requested_by: scenario.get("requested_by"),
        summary: ScenarioSummary {
            payments_created: summary_row.get("payments_created"),
            events_created: summary_row.get("events_created"),
            deliveries_created,
            delivered_count: summary_row.get("delivered_count"),
            retrying_count: summary_row.get("retrying_count"),
            dead_lettered_count: summary_row.get("dead_lettered_count"),
        },
        artifacts: ScenarioArtifacts {
            payment_ids: artifacts_row.get("payment_ids"),
            event_ids: artifacts_row.get("event_ids"),
            delivery_ids: artifacts_row.get("delivery_ids"),
            endpoint_ids: artifacts_row.get("endpoint_ids"),
        },
        planned,
        receiver_config: scenario.get("receiver_config_json"),
        step_log: scenario.get("step_log_json"),
        error_message: scenario.get("error_message"),
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

pub async fn get_dashboard_summary(pool: &PgPool) -> AppResult<DashboardSummary> {
    let redis_pending = redis_pending_from_runtime().await;

    let row = sqlx::query(
        "SELECT
            (SELECT COUNT(*)::BIGINT FROM domain_events) AS total_events,
            (SELECT COUNT(*)::BIGINT FROM webhook_deliveries) AS total_deliveries,
            (SELECT COUNT(*)::BIGINT FROM webhook_deliveries WHERE status = 'pending') AS pending_deliveries,
            (SELECT COUNT(*)::BIGINT FROM webhook_deliveries WHERE status = 'queued') AS queued_deliveries,
            (SELECT COUNT(*)::BIGINT FROM webhook_deliveries WHERE status = 'processing') AS processing_deliveries,
            (SELECT COUNT(*)::BIGINT FROM webhook_deliveries WHERE status = 'retrying') AS retrying_deliveries,
            (SELECT COUNT(*)::BIGINT FROM webhook_deliveries WHERE status = 'delivered') AS delivered_deliveries,
            (SELECT COUNT(*)::BIGINT FROM webhook_deliveries WHERE status = 'dead_lettered') AS dead_lettered_deliveries,
            (SELECT COUNT(*)::BIGINT FROM webhook_endpoints) AS total_endpoints,
            (SELECT COUNT(*)::BIGINT FROM webhook_endpoints WHERE enabled = TRUE) AS enabled_endpoints,
            (SELECT COUNT(*)::BIGINT FROM delivery_attempts WHERE started_at >= NOW() - INTERVAL '24 hours') AS attempts_24h,
            (
                SELECT COUNT(*)::BIGINT
                FROM delivery_attempts
                WHERE started_at >= NOW() - INTERVAL '24 hours'
                  AND outcome IN ('temporary_failure', 'permanent_failure', 'timeout', 'abandoned')
            ) AS failed_attempts_24h,
            (
                SELECT COUNT(*)::BIGINT
                FROM webhook_deliveries
                WHERE status IN ('queued', 'processing', 'retrying')
            ) AS active_deliveries,
            (
                SELECT COUNT(*)::BIGINT
                FROM webhook_deliveries
                WHERE status = 'pending'
                  AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
            ) AS pending_due_now,
            (
                SELECT COUNT(*)::BIGINT
                FROM webhook_deliveries
                WHERE status = 'retrying'
                  AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
            ) AS retry_due_now,
            (
                SELECT COUNT(*)::BIGINT
                FROM webhook_deliveries
                WHERE status = 'retrying'
                  AND next_attempt_at > NOW()
                  AND next_attempt_at <= NOW() + INTERVAL '5 minutes'
            ) AS retry_due_0_to_5_min,
            (
                SELECT COUNT(*)::BIGINT
                FROM webhook_deliveries
                WHERE status = 'retrying'
                  AND next_attempt_at > NOW() + INTERVAL '5 minutes'
                  AND next_attempt_at <= NOW() + INTERVAL '15 minutes'
            ) AS retry_due_5_to_15_min,
            (
                SELECT COUNT(*)::BIGINT
                FROM webhook_deliveries
                WHERE status = 'retrying'
                  AND next_attempt_at > NOW() + INTERVAL '15 minutes'
            ) AS retry_due_15_min_plus,
            (
                SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms)::DOUBLE PRECISION
                FROM delivery_attempts
                WHERE duration_ms IS NOT NULL
                  AND started_at >= NOW() - INTERVAL '24 hours'
            ) AS p95_latency_ms",
    )
    .fetch_one(pool)
    .await?;

    let total_deliveries: i64 = row.get("total_deliveries");
    let delivered_deliveries: i64 = row.get("delivered_deliveries");
    let queued_deliveries: i64 = row.get("queued_deliveries");
    let processing_deliveries: i64 = row.get("processing_deliveries");
    let retrying_deliveries: i64 = row.get("retrying_deliveries");
    let dead_lettered_deliveries: i64 = row.get("dead_lettered_deliveries");
    let pending_due_now: i64 = row.get("pending_due_now");
    let retry_due_now: i64 = row.get("retry_due_now");

    Ok(DashboardSummary {
        total_events: row.get("total_events"),
        total_deliveries,
        pending_deliveries: row.get("pending_deliveries"),
        queued_deliveries,
        processing_deliveries,
        retrying_deliveries,
        delivered_deliveries,
        dead_lettered_deliveries,
        total_endpoints: row.get("total_endpoints"),
        enabled_endpoints: row.get("enabled_endpoints"),
        attempts_24h: row.get("attempts_24h"),
        failed_attempts_24h: row.get("failed_attempts_24h"),
        success_rate: if total_deliveries > 0 {
            Some(delivered_deliveries as f64 / total_deliveries as f64)
        } else {
            None
        },
        active_deliveries: active_deliveries(
            queued_deliveries,
            processing_deliveries,
            retrying_deliveries,
        ),
        queued_count: queued_deliveries,
        processing_count: processing_deliveries,
        retrying_count: retrying_deliveries,
        dead_letter_count: dead_lettered_deliveries,
        queue_depth: queue_depth(pending_due_now, queued_deliveries, retry_due_now),
        redis_pending,
        p95_latency_ms: row.get("p95_latency_ms"),
        // TODO: Replace with a real worker telemetry source, such as Redis heartbeat keys,
        // a worker heartbeat table, or an in-process supervised task registry.
        active_workers: None,
        retry_backlog: RetryBacklogSummary {
            due_now: retry_due_now,
            due_0_to_5_min: row.get("retry_due_0_to_5_min"),
            due_5_to_15_min: row.get("retry_due_5_to_15_min"),
            due_15_min_plus: row.get("retry_due_15_min_plus"),
        },
    })
}

fn queue_depth(pending_due_now: i64, queued: i64, retry_due_now: i64) -> i64 {
    pending_due_now + queued + retry_due_now
}

fn active_deliveries(queued: i64, processing: i64, retrying: i64) -> i64 {
    queued + processing + retrying
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

pub async fn retry_delivery(
    pool: &PgPool,
    delivery_id: i64,
    request: RetryDeliveryRequest,
) -> AppResult<RetryDeliveryResponse> {
    let mut tx = pool.begin().await?;
    let response = retry_delivery_in_tx(
        &mut tx,
        delivery_id,
        request.reason.as_deref(),
        request.requested_by.as_deref(),
    )
    .await?;
    tx.commit().await?;

    Ok(response)
}

pub async fn bulk_retry_deliveries(
    pool: &PgPool,
    request: BulkRetryDeliveriesRequest,
) -> AppResult<BulkRetryDeliveriesResponse> {
    if request.delivery_ids.is_empty() {
        return Err(AppError::BadRequest(
            "at least one delivery id is required".to_string(),
        ));
    }

    if request.delivery_ids.len() > 100 {
        return Err(AppError::BadRequest(
            "bulk retry is limited to 100 deliveries".to_string(),
        ));
    }

    let total_requested = request.delivery_ids.len();
    let mut retried = Vec::new();
    let mut skipped = Vec::new();
    let mut tx = pool.begin().await?;

    for delivery_id in dedupe_delivery_ids(request.delivery_ids) {
        match retry_delivery_in_tx(
            &mut tx,
            delivery_id,
            request.reason.as_deref(),
            request.requested_by.as_deref(),
        )
        .await
        {
            Ok(response) => retried.push(response),
            Err(AppError::NotFound(reason)) | Err(AppError::BadRequest(reason)) => {
                skipped.push(BulkRetrySkip {
                    delivery_id,
                    reason,
                });
            }
            Err(error) => return Err(error),
        }
    }

    tx.commit().await?;

    let failed_ids = skipped.iter().map(|item| item.delivery_id).collect();

    Ok(BulkRetryDeliveriesResponse {
        queued_count: retried.len(),
        failed_ids,
        total_requested,
        total_retried: retried.len(),
        total_skipped: skipped.len(),
        retried,
        skipped,
    })
}

pub async fn resolve_delivery(
    pool: &PgPool,
    delivery_id: i64,
    request: ResolveDeliveryRequest,
) -> AppResult<ResolveDeliveryResponse> {
    let resolved_by = normalize_optional_text(request.resolved_by, "resolved_by", 200)?;
    let note = normalize_optional_text(request.note, "note", 2_000)?;
    let mut tx = pool.begin().await?;

    let row = sqlx::query(
        "WITH target AS (
            SELECT
                delivery_id,
                operator_resolved_at IS NOT NULL AS was_already_resolved
            FROM webhook_deliveries
            WHERE delivery_id = $1
              AND status = 'dead_lettered'
            FOR UPDATE
         ),
         updated AS (
            UPDATE webhook_deliveries d
            SET operator_resolved_at = COALESCE(d.operator_resolved_at, NOW()),
                operator_resolved_by = CASE
                    WHEN target.was_already_resolved THEN d.operator_resolved_by
                    ELSE $2
                END,
                operator_resolution_note = CASE
                    WHEN target.was_already_resolved THEN d.operator_resolution_note
                    ELSE $3
                END,
                updated_at = CASE
                    WHEN target.was_already_resolved THEN d.updated_at
                    ELSE NOW()
                END
            FROM target
            WHERE d.delivery_id = target.delivery_id
            RETURNING
                d.delivery_id,
                d.event_id,
                d.operator_resolved_at,
                d.operator_resolved_by,
                d.operator_resolution_note,
                target.was_already_resolved
         )
         SELECT * FROM updated",
    )
    .bind(delivery_id)
    .bind(resolved_by.as_deref())
    .bind(note.as_deref())
    .fetch_optional(&mut *tx)
    .await?;

    let row = match row {
        Some(row) => row,
        None => {
            let status = sqlx::query_scalar::<_, String>(
                "SELECT status FROM webhook_deliveries WHERE delivery_id = $1",
            )
            .bind(delivery_id)
            .fetch_optional(&mut *tx)
            .await?;

            return match status {
                Some(status) => Err(AppError::BadRequest(format!(
                    "delivery {} cannot be operator-resolved while status is {}",
                    delivery_id, status
                ))),
                None => Err(AppError::NotFound(format!(
                    "delivery {} not found",
                    delivery_id
                ))),
            };
        }
    };

    let event_id: i64 = row.get("event_id");
    let operator_resolved_at: DateTime<Utc> = row.get("operator_resolved_at");
    let operator_resolved_by: Option<String> = row.get("operator_resolved_by");
    let operator_resolution_note: Option<String> = row.get("operator_resolution_note");
    let was_already_resolved: bool = row.get("was_already_resolved");

    if !was_already_resolved {
        append_delivery_trace_in_tx(
            &mut tx,
            delivery_id,
            event_id,
            "operator_resolved",
            "succeeded",
            "Operator resolved dead letter",
            json!({
                "operator_resolved_at": operator_resolved_at,
                "operator_resolved_by": operator_resolved_by,
                "operator_resolution_note": operator_resolution_note
            }),
        )
        .await?;
    }

    tx.commit().await?;

    Ok(ResolveDeliveryResponse {
        success: true,
        delivery_id,
        operator_resolved_at,
        operator_resolved_by,
        operator_resolution_note,
    })
}

pub async fn unresolve_delivery(
    pool: &PgPool,
    delivery_id: i64,
) -> AppResult<UnresolveDeliveryResponse> {
    let mut tx = pool.begin().await?;

    let row = sqlx::query(
        "WITH target AS (
            SELECT
                delivery_id,
                operator_resolved_at IS NOT NULL AS was_resolved
            FROM webhook_deliveries
            WHERE delivery_id = $1
              AND status = 'dead_lettered'
            FOR UPDATE
         ),
         updated AS (
            UPDATE webhook_deliveries d
            SET operator_resolved_at = NULL,
                operator_resolved_by = NULL,
                operator_resolution_note = NULL,
                updated_at = CASE
                    WHEN target.was_resolved THEN NOW()
                    ELSE d.updated_at
                END
            FROM target
            WHERE d.delivery_id = target.delivery_id
            RETURNING d.delivery_id, d.event_id, target.was_resolved
         )
         SELECT * FROM updated",
    )
    .bind(delivery_id)
    .fetch_optional(&mut *tx)
    .await?;

    let row = match row {
        Some(row) => row,
        None => {
            let status = sqlx::query_scalar::<_, String>(
                "SELECT status FROM webhook_deliveries WHERE delivery_id = $1",
            )
            .bind(delivery_id)
            .fetch_optional(&mut *tx)
            .await?;

            return match status {
                Some(status) => Err(AppError::BadRequest(format!(
                    "delivery {} cannot be reopened while status is {}",
                    delivery_id, status
                ))),
                None => Err(AppError::NotFound(format!(
                    "delivery {} not found",
                    delivery_id
                ))),
            };
        }
    };

    let event_id: i64 = row.get("event_id");
    let was_resolved: bool = row.get("was_resolved");

    if was_resolved {
        append_delivery_trace_in_tx(
            &mut tx,
            delivery_id,
            event_id,
            "operator_resolution_reopened",
            "succeeded",
            "Operator reopened dead letter",
            json!({}),
        )
        .await?;
    }

    tx.commit().await?;

    Ok(UnresolveDeliveryResponse {
        success: true,
        delivery_id,
        operator_resolved_at: None,
        operator_resolved_by: None,
        operator_resolution_note: None,
    })
}

async fn retry_delivery_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    delivery_id: i64,
    reason: Option<&str>,
    requested_by: Option<&str>,
) -> AppResult<RetryDeliveryResponse> {
    let original = sqlx::query(
        "SELECT
            d.delivery_id,
            d.event_id,
            e.event_type,
            d.endpoint_id,
            d.merchant_id,
            d.endpoint_url,
            d.secret_version_id,
            d.status,
            d.max_attempts
         FROM webhook_deliveries d
         JOIN domain_events e ON e.event_id = d.event_id
         WHERE d.delivery_id = $1
         FOR UPDATE OF d",
    )
    .bind(delivery_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| AppError::NotFound(format!("delivery {} not found", delivery_id)))?;

    let status: String = original.get("status");
    if !matches!(status.as_str(), "delivered" | "dead_lettered") {
        return Err(AppError::BadRequest(format!(
            "delivery {} cannot be manually retried while status is {}",
            delivery_id, status
        )));
    }

    let event_id: i64 = original.get("event_id");
    let event_type: String = original.get("event_type");
    let endpoint_id: i64 = original.get("endpoint_id");
    let merchant_id: i64 = original.get("merchant_id");
    let endpoint_url: String = original.get("endpoint_url");
    let secret_version_id: Option<i64> = original.get("secret_version_id");
    let max_attempts: i64 = original.get("max_attempts");

    if let Some(active_retry) = sqlx::query(
        "SELECT delivery_id, status
         FROM webhook_deliveries
         WHERE manually_retried_from_delivery_id = $1
           AND status IN ('pending', 'queued', 'processing', 'retrying')
         ORDER BY delivery_id DESC
         LIMIT 1
         FOR UPDATE",
    )
    .bind(delivery_id)
    .fetch_optional(&mut **tx)
    .await?
    {
        let new_delivery_id: i64 = active_retry.get("delivery_id");
        let new_delivery_status: String = active_retry.get("status");

        return Ok(RetryDeliveryResponse {
            success: true,
            created: false,
            already_active_retry: true,
            original_delivery_id: delivery_id,
            new_delivery_id,
            event_id,
            event_type,
            merchant_id,
            endpoint_id,
            endpoint_url,
            new_delivery_status,
        });
    }

    let new_delivery = sqlx::query(
        "INSERT INTO webhook_deliveries (
            event_id,
            endpoint_id,
            merchant_id,
            endpoint_url,
            secret_version_id,
            status,
            max_attempts,
            next_attempt_at,
            manually_retried_from_delivery_id,
            retry_reason,
            requested_by,
            created_at,
            updated_at
         )
         VALUES (
            $1,
            $2,
            $3,
            $4,
            $5,
            'pending',
            $6,
            NOW(),
            $7,
            $8,
            $9,
            NOW(),
            NOW()
         )
         RETURNING delivery_id, status",
    )
    .bind(event_id)
    .bind(endpoint_id)
    .bind(merchant_id)
    .bind(&endpoint_url)
    .bind(secret_version_id)
    .bind(max_attempts)
    .bind(delivery_id)
    .bind(reason)
    .bind(requested_by)
    .fetch_one(&mut **tx)
    .await?;

    let new_delivery_id: i64 = new_delivery.get("delivery_id");
    let new_delivery_status: String = new_delivery.get("status");

    append_delivery_trace_in_tx(
        tx,
        delivery_id,
        event_id,
        "manual_retry_requested",
        "succeeded",
        "Manual retry requested",
        json!({
            "created": true,
            "event_id": event_id,
            "event_type": event_type,
            "new_delivery_id": new_delivery_id,
            "new_delivery_status": new_delivery_status,
            "reason": reason,
            "requested_by": requested_by
        }),
    )
    .await?;

    append_delivery_trace_in_tx(
        tx,
        new_delivery_id,
        event_id,
        "delivery_created",
        "succeeded",
        "Retry delivery created",
        json!({
            "original_delivery_id": delivery_id,
            "event_id": event_id,
            "event_type": event_type,
            "reason": reason,
            "requested_by": requested_by
        }),
    )
    .await?;

    Ok(RetryDeliveryResponse {
        success: true,
        created: true,
        already_active_retry: false,
        original_delivery_id: delivery_id,
        new_delivery_id,
        event_id,
        event_type,
        merchant_id,
        endpoint_id,
        endpoint_url,
        new_delivery_status,
    })
}

async fn append_delivery_trace_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    delivery_id: i64,
    event_id: i64,
    step: &str,
    status: &str,
    title: &str,
    metadata: serde_json::Value,
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
         VALUES ($1, $2, $3, $4, $5, $6, NOW())",
    )
    .bind(delivery_id)
    .bind(event_id)
    .bind(step)
    .bind(status)
    .bind(title)
    .bind(metadata)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

fn dedupe_delivery_ids(delivery_ids: Vec<i64>) -> Vec<i64> {
    let mut deduped = Vec::with_capacity(delivery_ids.len());

    for delivery_id in delivery_ids {
        if !deduped.contains(&delivery_id) {
            deduped.push(delivery_id);
        }
    }

    deduped
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
            request_body_hash,
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
    let event_id: i64 = sqlx::query_scalar(
        "SELECT event_id
         FROM webhook_deliveries
         WHERE delivery_id = $1",
    )
    .bind(delivery_id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| AppError::NotFound(format!("delivery {} not found", delivery_id)))?;

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
            OR (delivery_id IS NULL AND event_id = $2)
         ORDER BY occurred_at ASC, trace_id ASC",
    )
    .bind(delivery_id)
    .bind(event_id)
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

pub async fn get_delivery_trace_graph(
    pool: &PgPool,
    delivery_id: i64,
) -> AppResult<TraceGraphResponse> {
    let trace = list_delivery_trace(pool, delivery_id).await?;
    Ok(build_trace_graph(trace))
}

fn build_trace_graph(trace: Vec<DeliveryTraceItem>) -> TraceGraphResponse {
    let nodes = trace
        .iter()
        .map(|item| TraceGraphNode {
            id: format!("trace-{}", item.trace_id),
            step: item.step.clone(),
            status: item.status.clone(),
            title: item.title.clone(),
            occurred_at: item.occurred_at,
            metadata_json: item.metadata_json.clone(),
        })
        .collect::<Vec<_>>();

    let edges = nodes
        .windows(2)
        .map(|window| TraceGraphEdge {
            id: format!("{}-{}", window[0].id, window[1].id),
            source: window[0].id.clone(),
            target: window[1].id.clone(),
        })
        .collect::<Vec<_>>();

    TraceGraphResponse { nodes, edges }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::Value;
    use sqlx::postgres::PgPoolOptions;

    fn trace_item(
        trace_id: i64,
        delivery_id: Option<i64>,
        event_id: i64,
        step: &str,
    ) -> DeliveryTraceItem {
        DeliveryTraceItem {
            trace_id,
            delivery_id,
            event_id,
            step: step.to_string(),
            status: "succeeded".to_string(),
            title: step.to_string(),
            detail: None,
            metadata_json: Value::Object(Default::default()),
            occurred_at: Utc
                .with_ymd_and_hms(2026, 6, 12, 12, 0, trace_id as u32)
                .single()
                .expect("valid test timestamp"),
            duration_ms: None,
        }
    }

    #[test]
    fn trace_graph_uses_ordered_persisted_trace_rows() {
        let graph = build_trace_graph(vec![
            trace_item(1, None, 10, "payment_committed"),
            trace_item(2, None, 10, "domain_event_created"),
            trace_item(3, Some(20), 10, "delivery_created"),
            trace_item(4, Some(20), 10, "worker_claimed"),
        ]);

        let steps = graph
            .nodes
            .iter()
            .map(|node| node.step.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            steps,
            vec![
                "payment_committed",
                "domain_event_created",
                "delivery_created",
                "worker_claimed"
            ]
        );
        assert_eq!(graph.edges.len(), 3);
        assert_eq!(graph.edges[0].source, "trace-1");
        assert_eq!(graph.edges[0].target, "trace-2");
    }

    #[test]
    fn dashboard_queue_depth_includes_pending_due_queued_and_retry_due() {
        assert_eq!(queue_depth(2, 3, 5), 10);
    }

    #[test]
    fn dashboard_active_deliveries_excludes_pending() {
        assert_eq!(active_deliveries(3, 4, 5), 12);
    }

    #[test]
    fn event_search_routes_prefixed_event_id() {
        let search = event_search(Some("evt_000137"));

        assert_eq!(search.event_id, Some(137));
        assert_eq!(search.merchant_id, None);
    }

    #[test]
    fn event_search_routes_numeric_to_indexed_ids() {
        let search = event_search(Some("334"));

        assert_eq!(search.event_id, Some(334));
        assert_eq!(search.merchant_id, Some(334));
        assert_eq!(search.object_id, Some(334));
    }

    #[test]
    fn event_search_routes_event_type() {
        let search = event_search(Some("payment_succeeded"));

        assert_eq!(search.event_type.as_deref(), Some("payment_succeeded"));
    }

    #[test]
    fn resolution_filter_accepts_supported_values() {
        assert_eq!(
            normalize_resolution_filter(Some(" unresolved ".to_string())).unwrap(),
            Some("unresolved".to_string())
        );
        assert_eq!(
            normalize_resolution_filter(Some("resolved".to_string())).unwrap(),
            Some("resolved".to_string())
        );
        assert_eq!(
            normalize_resolution_filter(Some("all".to_string())).unwrap(),
            None
        );
    }

    #[test]
    fn optional_text_is_trimmed_and_bounded() {
        assert_eq!(
            normalize_optional_text(Some(" Operator ".to_string()), "resolved_by", 20).unwrap(),
            Some("Operator".to_string())
        );
        assert_eq!(
            normalize_optional_text(Some("   ".to_string()), "note", 20).unwrap(),
            None
        );
        assert!(normalize_optional_text(Some("too long".to_string()), "note", 3).is_err());
    }

    #[test]
    fn endpoint_url_rejects_local_targets_without_demo_allowance() {
        for url in [
            "http://localhost:3000/webhook",
            "http://127.0.0.1:3000/webhook",
            "http://10.1.2.3/webhook",
            "http://172.16.0.1/webhook",
            "http://192.168.1.10/webhook",
            "http://169.254.169.254/latest/meta-data",
            "http://[::1]:3000/webhook",
            "http://[fc00::1]/webhook",
            "http://[fe80::1]/webhook",
        ] {
            assert!(
                validate_endpoint_url_with_local_policy(url, false).is_err(),
                "{url} should be rejected"
            );
        }
    }

    #[test]
    fn endpoint_url_allows_local_targets_with_demo_allowance() {
        for url in [
            "http://localhost:3000/webhook",
            "http://127.0.0.1:3000/webhook",
            "http://10.1.2.3/webhook",
            "http://[::1]:3000/webhook",
        ] {
            assert!(
                validate_endpoint_url_with_local_policy(url, true).is_ok(),
                "{url} should be allowed in local demo mode"
            );
        }
    }

    #[test]
    fn endpoint_url_allows_public_https_targets_without_demo_allowance() {
        assert!(
            validate_endpoint_url_with_local_policy("https://example.com/webhook", false).is_ok()
        );
    }

    #[tokio::test]
    async fn list_delivery_trace_includes_event_rows_and_only_this_delivery_rows_when_test_db_is_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let database_url = match std::env::var("TEST_DATABASE_URL") {
            Ok(value) => value,
            Err(_) => {
                eprintln!("skipping db-backed trace test; TEST_DATABASE_URL is not set");
                return Ok(());
            }
        };

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await?;

        let marker = Utc::now().timestamp_micros();
        let merchant_id = 9_000_000_000_i64 + (marker % 1_000_000);
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

        let endpoint_one_id: i64 = sqlx::query_scalar(
            "INSERT INTO webhook_endpoints (merchant_id, url, enabled, max_attempts)
             VALUES ($1, 'http://127.0.0.1:3000/test/one', TRUE, 5)
             RETURNING endpoint_id",
        )
        .bind(merchant_id)
        .fetch_one(&pool)
        .await?;

        let endpoint_two_id: i64 = sqlx::query_scalar(
            "INSERT INTO webhook_endpoints (merchant_id, url, enabled, max_attempts)
             VALUES ($1, 'http://127.0.0.1:3000/test/two', TRUE, 5)
             RETURNING endpoint_id",
        )
        .bind(merchant_id)
        .fetch_one(&pool)
        .await?;

        let delivery_one_id: i64 = sqlx::query_scalar(
            "INSERT INTO webhook_deliveries (
                event_id,
                endpoint_id,
                merchant_id,
                endpoint_url,
                status,
                max_attempts
             )
             VALUES ($1, $2, $3, 'http://127.0.0.1:3000/test/one', 'pending', 5)
             RETURNING delivery_id",
        )
        .bind(event_id)
        .bind(endpoint_one_id)
        .bind(merchant_id)
        .fetch_one(&pool)
        .await?;

        let delivery_two_id: i64 = sqlx::query_scalar(
            "INSERT INTO webhook_deliveries (
                event_id,
                endpoint_id,
                merchant_id,
                endpoint_url,
                status,
                max_attempts
             )
             VALUES ($1, $2, $3, 'http://127.0.0.1:3000/test/two', 'pending', 5)
             RETURNING delivery_id",
        )
        .bind(event_id)
        .bind(endpoint_two_id)
        .bind(merchant_id)
        .fetch_one(&pool)
        .await?;

        let occurred_at = Utc::now();
        for (delivery_id, step) in [
            (None, "payment_committed"),
            (None, "domain_event_created"),
            (Some(delivery_one_id), "delivery_one_created"),
            (Some(delivery_two_id), "delivery_two_created"),
        ] {
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
                 VALUES ($1, $2, $3, 'succeeded', $3, '{}'::jsonb, $4)",
            )
            .bind(delivery_id)
            .bind(event_id)
            .bind(step)
            .bind(occurred_at)
            .execute(&pool)
            .await?;
        }

        let delivery_one_trace = list_delivery_trace(&pool, delivery_one_id).await?;
        let delivery_one_steps = delivery_one_trace
            .iter()
            .map(|item| item.step.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            delivery_one_steps,
            vec![
                "payment_committed",
                "domain_event_created",
                "delivery_one_created"
            ]
        );
        assert!(
            delivery_one_trace
                .iter()
                .any(|item| item.delivery_id.is_none())
        );
        assert!(
            delivery_one_trace
                .iter()
                .all(|item| item.delivery_id.is_none() || item.delivery_id == Some(delivery_one_id))
        );

        let delivery_two_trace = list_delivery_trace(&pool, delivery_two_id).await?;
        let delivery_two_steps = delivery_two_trace
            .iter()
            .map(|item| item.step.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            delivery_two_steps,
            vec![
                "payment_committed",
                "domain_event_created",
                "delivery_two_created"
            ]
        );

        let graph = get_delivery_trace_graph(&pool, delivery_one_id).await?;
        let graph_steps = graph
            .nodes
            .iter()
            .map(|node| node.step.as_str())
            .collect::<Vec<_>>();
        assert_eq!(graph_steps, delivery_one_steps);

        sqlx::query("DELETE FROM payments WHERE payment_id = $1")
            .bind(payment_id)
            .execute(&pool)
            .await?;
        sqlx::query("DELETE FROM webhook_endpoints WHERE merchant_id = $1")
            .bind(merchant_id)
            .execute(&pool)
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn manual_retry_returns_existing_active_child_when_test_db_is_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let database_url = match std::env::var("TEST_DATABASE_URL") {
            Ok(value) => value,
            Err(_) => {
                eprintln!("skipping db-backed retry test; TEST_DATABASE_URL is not set");
                return Ok(());
            }
        };

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await?;

        let marker = Utc::now().timestamp_micros();
        let merchant_id = 9_100_000_000_i64 + (marker % 1_000_000);
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
             VALUES ($1, 'http://127.0.0.1:3000/test/retry', TRUE, 5)
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
             VALUES ($1, $2, $3, 'http://127.0.0.1:3000/test/retry', 'dead_lettered', 5)
             RETURNING delivery_id",
        )
        .bind(event_id)
        .bind(endpoint_id)
        .bind(merchant_id)
        .fetch_one(&pool)
        .await?;

        let first = retry_delivery(
            &pool,
            original_delivery_id,
            RetryDeliveryRequest {
                reason: Some("test retry".to_string()),
                requested_by: Some("test".to_string()),
            },
        )
        .await?;

        let second = retry_delivery(
            &pool,
            original_delivery_id,
            RetryDeliveryRequest {
                reason: Some("test retry duplicate".to_string()),
                requested_by: Some("test".to_string()),
            },
        )
        .await?;

        assert!(first.success);
        assert!(first.created);
        assert!(!first.already_active_retry);
        assert_eq!(first.event_id, event_id);
        assert_eq!(first.event_type, "payment_succeeded");

        assert!(second.success);
        assert!(!second.created);
        assert!(second.already_active_retry);
        assert_eq!(second.new_delivery_id, first.new_delivery_id);
        assert_eq!(second.new_delivery_status, "pending");

        sqlx::query("DELETE FROM payments WHERE payment_id = $1")
            .bind(payment_id)
            .execute(&pool)
            .await?;
        sqlx::query("DELETE FROM webhook_endpoints WHERE merchant_id = $1")
            .bind(merchant_id)
            .execute(&pool)
            .await?;

        Ok(())
    }
}
