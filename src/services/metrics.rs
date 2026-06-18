use chrono::{DateTime, Duration, Utc};
use sqlx::{PgPool, Row};

use super::{
    dashboard::{queue_depth, redis_pending_from_runtime},
    validation::normalize_optional_text,
};
use crate::{
    error::{AppError, AppResult},
    models::{
        MetricsDeadLetterEndpointItem, MetricsDeadLettersResponse, MetricsDeliveryStatusItem,
        MetricsDeliveryStatusResponse, MetricsEndpointItem, MetricsEndpointsResponse,
        MetricsFailureEndpointItem, MetricsFailureOutcomeItem, MetricsFailuresResponse,
        MetricsHttpStatusItem, MetricsHttpStatusResponse, MetricsLatencyHistogramBucket,
        MetricsLatencyPoint, MetricsLatencyResponse, MetricsLifecycleFunnelResponse,
        MetricsLifecycleStepItem, MetricsQuery, MetricsQueueResponse, MetricsRetriesResponse,
        MetricsRetryAttemptDistributionItem, MetricsScenarioItem, MetricsScenariosResponse,
        MetricsSummaryResponse, MetricsThroughputPoint, MetricsThroughputResponse, MetricsWindow,
        RetryBacklogSummary,
    },
};

#[derive(Debug, Clone)]
struct MetricsFilters {
    window: MetricsWindow,
    merchant_id: Option<i64>,
    endpoint_id: Option<i64>,
    scenario_id: Option<i64>,
    scenario_key: Option<String>,
    event_type: Option<String>,
    include_hidden: bool,
    limit: i64,
}

impl MetricsFilters {
    fn from_query(query: MetricsQuery) -> AppResult<Self> {
        let now = Utc::now();
        let range = query.range.unwrap_or_else(|| "24h".to_string());
        let start_at = range_start(&range, now)?;
        let bucket = normalize_bucket(query.bucket, &range)?;
        let scenario_key = normalize_optional_text(query.scenario_key, "scenario_key", 100)?;
        let event_type = normalize_optional_text(query.event_type, "event_type", 100)?;

        Ok(Self {
            window: MetricsWindow {
                range,
                bucket,
                start_at,
                end_at: now,
            },
            merchant_id: query.merchant_id,
            endpoint_id: query.endpoint_id,
            scenario_id: query.scenario_id,
            scenario_key,
            event_type,
            include_hidden: query.include_hidden.unwrap_or(false),
            limit: query.limit.unwrap_or(10).clamp(1, 50),
        })
    }

    fn bucket(&self) -> &str {
        self.window.bucket.as_str()
    }
}

pub async fn get_metrics_summary(
    pool: &PgPool,
    query: MetricsQuery,
) -> AppResult<MetricsSummaryResponse> {
    let filters = MetricsFilters::from_query(query)?;
    let redis_pending = redis_pending_from_runtime().await;

    let row = sqlx::query(
        "SELECT
            COUNT(DISTINCT d.delivery_id)::BIGINT AS total_deliveries,
            COUNT(DISTINCT d.delivery_id) FILTER (WHERE d.status = 'delivered')::BIGINT AS delivered_deliveries,
            COUNT(DISTINCT d.delivery_id) FILTER (WHERE d.status = 'dead_lettered')::BIGINT AS dead_lettered_deliveries,
            COUNT(DISTINCT d.delivery_id) FILTER (WHERE d.status IN ('queued', 'processing', 'retrying'))::BIGINT AS active_deliveries,
            COUNT(DISTINCT d.delivery_id) FILTER (WHERE d.status = 'queued')::BIGINT AS queued_deliveries,
            COUNT(DISTINCT d.delivery_id) FILTER (WHERE d.status = 'processing')::BIGINT AS processing_deliveries,
            COUNT(DISTINCT d.delivery_id) FILTER (
                WHERE d.status = 'pending' AND (d.next_attempt_at IS NULL OR d.next_attempt_at <= NOW())
            )::BIGINT AS pending_due_now,
            COUNT(DISTINCT d.delivery_id) FILTER (
                WHERE d.status = 'retrying' AND (d.next_attempt_at IS NULL OR d.next_attempt_at <= NOW())
            )::BIGINT AS retry_due_now,
            COUNT(DISTINCT d.delivery_id) FILTER (
                WHERE d.status = 'retrying' AND d.next_attempt_at > NOW() AND d.next_attempt_at <= NOW() + INTERVAL '5 minutes'
            )::BIGINT AS retry_due_0_to_5_min,
            COUNT(DISTINCT d.delivery_id) FILTER (
                WHERE d.status = 'retrying' AND d.next_attempt_at > NOW() + INTERVAL '5 minutes' AND d.next_attempt_at <= NOW() + INTERVAL '15 minutes'
            )::BIGINT AS retry_due_5_to_15_min,
            COUNT(DISTINCT d.delivery_id) FILTER (
                WHERE d.status = 'retrying' AND d.next_attempt_at > NOW() + INTERVAL '15 minutes'
            )::BIGINT AS retry_due_15_min_plus,
            COUNT(DISTINCT e.event_id)::BIGINT AS total_events,
            COUNT(DISTINCT a.attempt_id)::BIGINT AS total_attempts,
            COUNT(DISTINCT a.attempt_id) FILTER (
                WHERE a.outcome IN ('temporary_failure', 'permanent_failure', 'timeout', 'abandoned')
            )::BIGINT AS failed_attempts,
            percentile_cont(0.50) WITHIN GROUP (ORDER BY a.duration_ms)::DOUBLE PRECISION AS p50_latency_ms,
            percentile_cont(0.95) WITHIN GROUP (ORDER BY a.duration_ms)::DOUBLE PRECISION AS p95_latency_ms,
            percentile_cont(0.99) WITHIN GROUP (ORDER BY a.duration_ms)::DOUBLE PRECISION AS p99_latency_ms
         FROM webhook_deliveries d
         INNER JOIN domain_events e ON e.event_id = d.event_id
         LEFT JOIN scenarios s ON s.scenario_id = d.scenario_id
         LEFT JOIN delivery_attempts a
            ON a.delivery_id = d.delivery_id
           AND ($1::TIMESTAMPTZ IS NULL OR a.started_at >= $1)
         WHERE ($1::TIMESTAMPTZ IS NULL OR d.created_at >= $1)
           AND ($2::BIGINT IS NULL OR d.merchant_id = $2)
           AND ($3::BIGINT IS NULL OR d.endpoint_id = $3)
           AND ($4::BIGINT IS NULL OR d.scenario_id = $4)
           AND ($5::TEXT IS NULL OR s.scenario_key = $5)
           AND ($6::TEXT IS NULL OR e.event_type = $6)
           AND ($7::BOOLEAN = TRUE OR d.scenario_id IS NULL OR COALESCE(s.include_in_history, TRUE) = TRUE)",
    )
    .bind(filters.window.start_at)
    .bind(filters.merchant_id)
    .bind(filters.endpoint_id)
    .bind(filters.scenario_id)
    .bind(&filters.scenario_key)
    .bind(&filters.event_type)
    .bind(filters.include_hidden)
    .fetch_one(pool)
    .await?;

    let total_deliveries: i64 = row.get("total_deliveries");
    let delivered_deliveries: i64 = row.get("delivered_deliveries");
    let total_attempts: i64 = row.get("total_attempts");
    let failed_attempts: i64 = row.get("failed_attempts");
    let pending_due_now: i64 = row.get("pending_due_now");
    let queued_deliveries: i64 = row.get("queued_deliveries");
    let retry_due_now: i64 = row.get("retry_due_now");
    let retrying_deliveries = retry_due_now
        + row.get::<i64, _>("retry_due_0_to_5_min")
        + row.get::<i64, _>("retry_due_5_to_15_min")
        + row.get::<i64, _>("retry_due_15_min_plus");

    Ok(MetricsSummaryResponse {
        window: filters.window,
        total_events: row.get("total_events"),
        total_deliveries,
        total_attempts,
        delivered_deliveries,
        dead_lettered_deliveries: row.get("dead_lettered_deliveries"),
        active_deliveries: row.get("active_deliveries"),
        queue_depth: queue_depth(pending_due_now, queued_deliveries, retry_due_now),
        redis_pending,
        success_rate: ratio(delivered_deliveries, total_deliveries),
        failure_rate: ratio(failed_attempts, total_attempts),
        retry_rate: ratio(retrying_deliveries, total_deliveries),
        p50_latency_ms: row.get("p50_latency_ms"),
        p95_latency_ms: row.get("p95_latency_ms"),
        p99_latency_ms: row.get("p99_latency_ms"),
        retry_backlog: RetryBacklogSummary {
            due_now: retry_due_now,
            due_0_to_5_min: row.get("retry_due_0_to_5_min"),
            due_5_to_15_min: row.get("retry_due_5_to_15_min"),
            due_15_min_plus: row.get("retry_due_15_min_plus"),
        },
    })
}

pub async fn get_metrics_throughput(
    pool: &PgPool,
    query: MetricsQuery,
) -> AppResult<MetricsThroughputResponse> {
    let filters = MetricsFilters::from_query(query)?;
    let rows = sqlx::query_as::<_, MetricsThroughputPoint>(
        "WITH delivery_series AS (
            SELECT
                date_trunc($8, d.created_at) AS bucket_start,
                COUNT(DISTINCT e.event_id)::BIGINT AS events_created,
                COUNT(DISTINCT d.delivery_id)::BIGINT AS deliveries_created,
                COUNT(DISTINCT p.payment_id)::BIGINT AS payments_created
            FROM webhook_deliveries d
            INNER JOIN domain_events e ON e.event_id = d.event_id
            LEFT JOIN payments p ON p.payment_id = e.object_id
            LEFT JOIN scenarios s ON s.scenario_id = d.scenario_id
            WHERE ($1::TIMESTAMPTZ IS NULL OR d.created_at >= $1)
              AND ($2::BIGINT IS NULL OR d.merchant_id = $2)
              AND ($3::BIGINT IS NULL OR d.endpoint_id = $3)
              AND ($4::BIGINT IS NULL OR d.scenario_id = $4)
              AND ($5::TEXT IS NULL OR s.scenario_key = $5)
              AND ($6::TEXT IS NULL OR e.event_type = $6)
              AND ($7::BOOLEAN = TRUE OR d.scenario_id IS NULL OR COALESCE(s.include_in_history, TRUE) = TRUE)
            GROUP BY bucket_start
         ),
         attempt_series AS (
            SELECT
                date_trunc($8, a.started_at) AS bucket_start,
                COUNT(DISTINCT a.attempt_id)::BIGINT AS attempts_started
            FROM delivery_attempts a
            INNER JOIN webhook_deliveries d ON d.delivery_id = a.delivery_id
            INNER JOIN domain_events e ON e.event_id = d.event_id
            LEFT JOIN scenarios s ON s.scenario_id = d.scenario_id
            WHERE ($1::TIMESTAMPTZ IS NULL OR a.started_at >= $1)
              AND ($2::BIGINT IS NULL OR d.merchant_id = $2)
              AND ($3::BIGINT IS NULL OR d.endpoint_id = $3)
              AND ($4::BIGINT IS NULL OR d.scenario_id = $4)
              AND ($5::TEXT IS NULL OR s.scenario_key = $5)
              AND ($6::TEXT IS NULL OR e.event_type = $6)
              AND ($7::BOOLEAN = TRUE OR d.scenario_id IS NULL OR COALESCE(s.include_in_history, TRUE) = TRUE)
            GROUP BY bucket_start
         )
         SELECT
            COALESCE(d.bucket_start, a.bucket_start) AS bucket_start,
            COALESCE(d.events_created, 0)::BIGINT AS events_created,
            COALESCE(d.deliveries_created, 0)::BIGINT AS deliveries_created,
            COALESCE(a.attempts_started, 0)::BIGINT AS attempts_started,
            COALESCE(d.payments_created, 0)::BIGINT AS payments_created
         FROM delivery_series d
         FULL OUTER JOIN attempt_series a ON a.bucket_start = d.bucket_start
         ORDER BY bucket_start",
    )
    .bind(filters.window.start_at)
    .bind(filters.merchant_id)
    .bind(filters.endpoint_id)
    .bind(filters.scenario_id)
    .bind(&filters.scenario_key)
    .bind(&filters.event_type)
    .bind(filters.include_hidden)
    .bind(filters.bucket())
    .fetch_all(pool)
    .await?;

    Ok(MetricsThroughputResponse {
        window: filters.window,
        points: rows,
    })
}

pub async fn get_metrics_delivery_status(
    pool: &PgPool,
    query: MetricsQuery,
) -> AppResult<MetricsDeliveryStatusResponse> {
    let filters = MetricsFilters::from_query(query)?;
    let rows = sqlx::query_as::<_, MetricsDeliveryStatusItem>(
        "SELECT d.status, COUNT(*)::BIGINT AS count
         FROM webhook_deliveries d
         INNER JOIN domain_events e ON e.event_id = d.event_id
         LEFT JOIN scenarios s ON s.scenario_id = d.scenario_id
         WHERE ($1::TIMESTAMPTZ IS NULL OR d.created_at >= $1)
           AND ($2::BIGINT IS NULL OR d.merchant_id = $2)
           AND ($3::BIGINT IS NULL OR d.endpoint_id = $3)
           AND ($4::BIGINT IS NULL OR d.scenario_id = $4)
           AND ($5::TEXT IS NULL OR s.scenario_key = $5)
           AND ($6::TEXT IS NULL OR e.event_type = $6)
           AND ($7::BOOLEAN = TRUE OR d.scenario_id IS NULL OR COALESCE(s.include_in_history, TRUE) = TRUE)
         GROUP BY d.status
         ORDER BY count DESC",
    )
    .bind(filters.window.start_at)
    .bind(filters.merchant_id)
    .bind(filters.endpoint_id)
    .bind(filters.scenario_id)
    .bind(&filters.scenario_key)
    .bind(&filters.event_type)
    .bind(filters.include_hidden)
    .fetch_all(pool)
    .await?;

    Ok(MetricsDeliveryStatusResponse {
        window: filters.window,
        current: rows,
    })
}

pub async fn get_metrics_latency(
    pool: &PgPool,
    query: MetricsQuery,
) -> AppResult<MetricsLatencyResponse> {
    let filters = MetricsFilters::from_query(query)?;
    let points = sqlx::query_as::<_, MetricsLatencyPoint>(
        "SELECT
            date_trunc($8, a.started_at) AS bucket_start,
            COUNT(*)::BIGINT AS attempt_count,
            percentile_cont(0.50) WITHIN GROUP (ORDER BY a.duration_ms)::DOUBLE PRECISION AS p50_ms,
            percentile_cont(0.95) WITHIN GROUP (ORDER BY a.duration_ms)::DOUBLE PRECISION AS p95_ms,
            percentile_cont(0.99) WITHIN GROUP (ORDER BY a.duration_ms)::DOUBLE PRECISION AS p99_ms,
            AVG(a.duration_ms)::DOUBLE PRECISION AS avg_ms
         FROM delivery_attempts a
         INNER JOIN webhook_deliveries d ON d.delivery_id = a.delivery_id
         INNER JOIN domain_events e ON e.event_id = d.event_id
         LEFT JOIN scenarios s ON s.scenario_id = d.scenario_id
         WHERE a.duration_ms IS NOT NULL
           AND ($1::TIMESTAMPTZ IS NULL OR a.started_at >= $1)
           AND ($2::BIGINT IS NULL OR d.merchant_id = $2)
           AND ($3::BIGINT IS NULL OR d.endpoint_id = $3)
           AND ($4::BIGINT IS NULL OR d.scenario_id = $4)
           AND ($5::TEXT IS NULL OR s.scenario_key = $5)
           AND ($6::TEXT IS NULL OR e.event_type = $6)
           AND ($7::BOOLEAN = TRUE OR d.scenario_id IS NULL OR COALESCE(s.include_in_history, TRUE) = TRUE)
         GROUP BY bucket_start
         ORDER BY bucket_start",
    )
    .bind(filters.window.start_at)
    .bind(filters.merchant_id)
    .bind(filters.endpoint_id)
    .bind(filters.scenario_id)
    .bind(&filters.scenario_key)
    .bind(&filters.event_type)
    .bind(filters.include_hidden)
    .bind(filters.bucket())
    .fetch_all(pool)
    .await?;

    let histogram = sqlx::query_as::<_, MetricsLatencyHistogramBucket>(
        "SELECT
            CASE
                WHEN a.duration_ms < 100 THEN '<100ms'
                WHEN a.duration_ms < 500 THEN '100-499ms'
                WHEN a.duration_ms < 1000 THEN '500-999ms'
                WHEN a.duration_ms < 3000 THEN '1-3s'
                ELSE '>3s'
            END AS bucket_label,
            COUNT(*)::BIGINT AS attempt_count
         FROM delivery_attempts a
         INNER JOIN webhook_deliveries d ON d.delivery_id = a.delivery_id
         INNER JOIN domain_events e ON e.event_id = d.event_id
         LEFT JOIN scenarios s ON s.scenario_id = d.scenario_id
         WHERE a.duration_ms IS NOT NULL
           AND ($1::TIMESTAMPTZ IS NULL OR a.started_at >= $1)
           AND ($2::BIGINT IS NULL OR d.merchant_id = $2)
           AND ($3::BIGINT IS NULL OR d.endpoint_id = $3)
           AND ($4::BIGINT IS NULL OR d.scenario_id = $4)
           AND ($5::TEXT IS NULL OR s.scenario_key = $5)
           AND ($6::TEXT IS NULL OR e.event_type = $6)
           AND ($7::BOOLEAN = TRUE OR d.scenario_id IS NULL OR COALESCE(s.include_in_history, TRUE) = TRUE)
         GROUP BY bucket_label
         ORDER BY MIN(a.duration_ms)",
    )
    .bind(filters.window.start_at)
    .bind(filters.merchant_id)
    .bind(filters.endpoint_id)
    .bind(filters.scenario_id)
    .bind(&filters.scenario_key)
    .bind(&filters.event_type)
    .bind(filters.include_hidden)
    .fetch_all(pool)
    .await?;

    Ok(MetricsLatencyResponse {
        window: filters.window,
        points,
        histogram,
    })
}

pub async fn get_metrics_failures(
    pool: &PgPool,
    query: MetricsQuery,
) -> AppResult<MetricsFailuresResponse> {
    let filters = MetricsFilters::from_query(query)?;
    let by_outcome = sqlx::query_as::<_, MetricsFailureOutcomeItem>(
        "SELECT a.outcome, COUNT(*)::BIGINT AS count
         FROM delivery_attempts a
         INNER JOIN webhook_deliveries d ON d.delivery_id = a.delivery_id
         INNER JOIN domain_events e ON e.event_id = d.event_id
         LEFT JOIN scenarios s ON s.scenario_id = d.scenario_id
         WHERE a.outcome <> 'success'
           AND ($1::TIMESTAMPTZ IS NULL OR a.started_at >= $1)
           AND ($2::BIGINT IS NULL OR d.merchant_id = $2)
           AND ($3::BIGINT IS NULL OR d.endpoint_id = $3)
           AND ($4::BIGINT IS NULL OR d.scenario_id = $4)
           AND ($5::TEXT IS NULL OR s.scenario_key = $5)
           AND ($6::TEXT IS NULL OR e.event_type = $6)
           AND ($7::BOOLEAN = TRUE OR d.scenario_id IS NULL OR COALESCE(s.include_in_history, TRUE) = TRUE)
         GROUP BY a.outcome
         ORDER BY count DESC",
    )
    .bind(filters.window.start_at)
    .bind(filters.merchant_id)
    .bind(filters.endpoint_id)
    .bind(filters.scenario_id)
    .bind(&filters.scenario_key)
    .bind(&filters.event_type)
    .bind(filters.include_hidden)
    .fetch_all(pool)
    .await?;

    let worst_endpoints = sqlx::query_as::<_, MetricsFailureEndpointItem>(
        "SELECT
            d.endpoint_id,
            MAX(d.endpoint_url) AS endpoint_url,
            COUNT(a.attempt_id) FILTER (WHERE a.outcome <> 'success')::BIGINT AS failure_count,
            COUNT(a.attempt_id) FILTER (WHERE a.outcome = 'timeout')::BIGINT AS timeout_count,
            COUNT(DISTINCT d.delivery_id) FILTER (WHERE d.status = 'dead_lettered')::BIGINT AS dead_lettered_count
         FROM webhook_deliveries d
         INNER JOIN domain_events e ON e.event_id = d.event_id
         LEFT JOIN scenarios s ON s.scenario_id = d.scenario_id
         LEFT JOIN delivery_attempts a
            ON a.delivery_id = d.delivery_id
           AND ($1::TIMESTAMPTZ IS NULL OR a.started_at >= $1)
         WHERE ($1::TIMESTAMPTZ IS NULL OR d.created_at >= $1)
           AND ($2::BIGINT IS NULL OR d.merchant_id = $2)
           AND ($3::BIGINT IS NULL OR d.endpoint_id = $3)
           AND ($4::BIGINT IS NULL OR d.scenario_id = $4)
           AND ($5::TEXT IS NULL OR s.scenario_key = $5)
           AND ($6::TEXT IS NULL OR e.event_type = $6)
           AND ($7::BOOLEAN = TRUE OR d.scenario_id IS NULL OR COALESCE(s.include_in_history, TRUE) = TRUE)
         GROUP BY d.endpoint_id
         HAVING COUNT(a.attempt_id) FILTER (WHERE a.outcome <> 'success') > 0
             OR COUNT(DISTINCT d.delivery_id) FILTER (WHERE d.status = 'dead_lettered') > 0
         ORDER BY failure_count DESC, dead_lettered_count DESC
         LIMIT $8",
    )
    .bind(filters.window.start_at)
    .bind(filters.merchant_id)
    .bind(filters.endpoint_id)
    .bind(filters.scenario_id)
    .bind(&filters.scenario_key)
    .bind(&filters.event_type)
    .bind(filters.include_hidden)
    .bind(filters.limit)
    .fetch_all(pool)
    .await?;

    Ok(MetricsFailuresResponse {
        window: filters.window,
        by_outcome,
        worst_endpoints,
    })
}

pub async fn get_metrics_retries(
    pool: &PgPool,
    query: MetricsQuery,
) -> AppResult<MetricsRetriesResponse> {
    let filters = MetricsFilters::from_query(query)?;
    let row = sqlx::query(
        "SELECT
            COUNT(DISTINCT d.delivery_id) FILTER (
                WHERE d.status = 'retrying' AND (d.next_attempt_at IS NULL OR d.next_attempt_at <= NOW())
            )::BIGINT AS retry_due_now,
            COUNT(DISTINCT d.delivery_id) FILTER (
                WHERE d.status = 'retrying' AND d.next_attempt_at > NOW() AND d.next_attempt_at <= NOW() + INTERVAL '5 minutes'
            )::BIGINT AS retry_due_0_to_5_min,
            COUNT(DISTINCT d.delivery_id) FILTER (
                WHERE d.status = 'retrying' AND d.next_attempt_at > NOW() + INTERVAL '5 minutes' AND d.next_attempt_at <= NOW() + INTERVAL '15 minutes'
            )::BIGINT AS retry_due_5_to_15_min,
            COUNT(DISTINCT d.delivery_id) FILTER (
                WHERE d.status = 'retrying' AND d.next_attempt_at > NOW() + INTERVAL '15 minutes'
            )::BIGINT AS retry_due_15_min_plus,
            COUNT(DISTINCT d.delivery_id) FILTER (WHERE d.status = 'retrying')::BIGINT AS retrying_deliveries,
            COUNT(DISTINCT d.delivery_id) FILTER (WHERE d.manually_retried_from_delivery_id IS NOT NULL)::BIGINT AS manual_retry_deliveries,
            COUNT(DISTINCT d.delivery_id) FILTER (WHERE d.status = 'dead_lettered')::BIGINT AS exhausted_deliveries
         FROM webhook_deliveries d
         INNER JOIN domain_events e ON e.event_id = d.event_id
         LEFT JOIN scenarios s ON s.scenario_id = d.scenario_id
         WHERE ($1::TIMESTAMPTZ IS NULL OR d.created_at >= $1)
           AND ($2::BIGINT IS NULL OR d.merchant_id = $2)
           AND ($3::BIGINT IS NULL OR d.endpoint_id = $3)
           AND ($4::BIGINT IS NULL OR d.scenario_id = $4)
           AND ($5::TEXT IS NULL OR s.scenario_key = $5)
           AND ($6::TEXT IS NULL OR e.event_type = $6)
           AND ($7::BOOLEAN = TRUE OR d.scenario_id IS NULL OR COALESCE(s.include_in_history, TRUE) = TRUE)",
    )
    .bind(filters.window.start_at)
    .bind(filters.merchant_id)
    .bind(filters.endpoint_id)
    .bind(filters.scenario_id)
    .bind(&filters.scenario_key)
    .bind(&filters.event_type)
    .bind(filters.include_hidden)
    .fetch_one(pool)
    .await?;

    let attempt_distribution = sqlx::query_as::<_, MetricsRetryAttemptDistributionItem>(
        "WITH per_delivery AS (
            SELECT
                d.delivery_id,
                COALESCE(MAX(a.attempt_count), 0)::BIGINT AS attempt_count
            FROM webhook_deliveries d
            INNER JOIN domain_events e ON e.event_id = d.event_id
            LEFT JOIN scenarios s ON s.scenario_id = d.scenario_id
            LEFT JOIN delivery_attempts a ON a.delivery_id = d.delivery_id
            WHERE ($1::TIMESTAMPTZ IS NULL OR d.created_at >= $1)
              AND ($2::BIGINT IS NULL OR d.merchant_id = $2)
              AND ($3::BIGINT IS NULL OR d.endpoint_id = $3)
              AND ($4::BIGINT IS NULL OR d.scenario_id = $4)
              AND ($5::TEXT IS NULL OR s.scenario_key = $5)
              AND ($6::TEXT IS NULL OR e.event_type = $6)
              AND ($7::BOOLEAN = TRUE OR d.scenario_id IS NULL OR COALESCE(s.include_in_history, TRUE) = TRUE)
            GROUP BY d.delivery_id
         )
         SELECT
            attempt_count,
            COUNT(*)::BIGINT AS delivery_count
         FROM per_delivery
         GROUP BY attempt_count
         ORDER BY attempt_count",
    )
    .bind(filters.window.start_at)
    .bind(filters.merchant_id)
    .bind(filters.endpoint_id)
    .bind(filters.scenario_id)
    .bind(&filters.scenario_key)
    .bind(&filters.event_type)
    .bind(filters.include_hidden)
    .fetch_all(pool)
    .await?;

    Ok(MetricsRetriesResponse {
        window: filters.window,
        retry_backlog: RetryBacklogSummary {
            due_now: row.get("retry_due_now"),
            due_0_to_5_min: row.get("retry_due_0_to_5_min"),
            due_5_to_15_min: row.get("retry_due_5_to_15_min"),
            due_15_min_plus: row.get("retry_due_15_min_plus"),
        },
        retrying_deliveries: row.get("retrying_deliveries"),
        manual_retry_deliveries: row.get("manual_retry_deliveries"),
        exhausted_deliveries: row.get("exhausted_deliveries"),
        attempt_distribution,
    })
}

pub async fn get_metrics_queue(
    pool: &PgPool,
    query: MetricsQuery,
) -> AppResult<MetricsQueueResponse> {
    let filters = MetricsFilters::from_query(query)?;
    let redis_pending = redis_pending_from_runtime().await;
    let row = sqlx::query(
        "SELECT
            COUNT(DISTINCT d.delivery_id) FILTER (
                WHERE d.status = 'pending' AND (d.next_attempt_at IS NULL OR d.next_attempt_at <= NOW())
            )::BIGINT AS pending_due_now,
            COUNT(DISTINCT d.delivery_id) FILTER (WHERE d.status = 'queued')::BIGINT AS queued,
            COUNT(DISTINCT d.delivery_id) FILTER (WHERE d.status = 'processing')::BIGINT AS processing,
            COUNT(DISTINCT d.delivery_id) FILTER (
                WHERE d.status = 'retrying' AND (d.next_attempt_at IS NULL OR d.next_attempt_at <= NOW())
            )::BIGINT AS retrying_due_now,
            (
                SELECT COUNT(*)::BIGINT
                FROM delivery_trace_events t
                WHERE t.step = 'redis_publish_failed'
                  AND ($1::TIMESTAMPTZ IS NULL OR t.occurred_at >= $1)
            ) AS redis_publish_failures,
            (
                SELECT COUNT(*)::BIGINT
                FROM delivery_trace_events t
                WHERE t.step = 'queued_recovered'
                  AND ($1::TIMESTAMPTZ IS NULL OR t.occurred_at >= $1)
            ) AS stale_queued_recovered,
            (
                SELECT COUNT(*)::BIGINT
                FROM delivery_trace_events t
                WHERE t.step = 'processing_recovered'
                  AND ($1::TIMESTAMPTZ IS NULL OR t.occurred_at >= $1)
            ) AS stuck_processing_recovered
         FROM webhook_deliveries d
         INNER JOIN domain_events e ON e.event_id = d.event_id
         LEFT JOIN scenarios s ON s.scenario_id = d.scenario_id
         WHERE ($1::TIMESTAMPTZ IS NULL OR d.created_at >= $1)
           AND ($2::BIGINT IS NULL OR d.merchant_id = $2)
           AND ($3::BIGINT IS NULL OR d.endpoint_id = $3)
           AND ($4::BIGINT IS NULL OR d.scenario_id = $4)
           AND ($5::TEXT IS NULL OR s.scenario_key = $5)
           AND ($6::TEXT IS NULL OR e.event_type = $6)
           AND ($7::BOOLEAN = TRUE OR d.scenario_id IS NULL OR COALESCE(s.include_in_history, TRUE) = TRUE)",
    )
    .bind(filters.window.start_at)
    .bind(filters.merchant_id)
    .bind(filters.endpoint_id)
    .bind(filters.scenario_id)
    .bind(&filters.scenario_key)
    .bind(&filters.event_type)
    .bind(filters.include_hidden)
    .fetch_one(pool)
    .await?;

    let pending_due_now = row.get("pending_due_now");
    let queued = row.get("queued");
    let retrying_due_now = row.get("retrying_due_now");

    Ok(MetricsQueueResponse {
        window: filters.window,
        pending_due_now,
        queued,
        processing: row.get("processing"),
        retrying_due_now,
        queue_depth: queue_depth(pending_due_now, queued, retrying_due_now),
        redis_pending,
        redis_publish_failures: row.get("redis_publish_failures"),
        stale_queued_recovered: row.get("stale_queued_recovered"),
        stuck_processing_recovered: row.get("stuck_processing_recovered"),
    })
}

pub async fn get_metrics_endpoints(
    pool: &PgPool,
    query: MetricsQuery,
) -> AppResult<MetricsEndpointsResponse> {
    let filters = MetricsFilters::from_query(query)?;
    let rows = sqlx::query_as::<_, MetricsEndpointItem>(
        "SELECT
            d.endpoint_id,
            d.merchant_id,
            MAX(d.endpoint_url) AS endpoint_url,
            BOOL_OR(we.enabled) AS enabled,
            COUNT(DISTINCT d.delivery_id)::BIGINT AS delivery_count,
            COUNT(DISTINCT d.delivery_id) FILTER (WHERE d.status = 'delivered')::BIGINT AS delivered_count,
            COUNT(a.attempt_id) FILTER (WHERE a.outcome <> 'success')::BIGINT AS failed_attempt_count,
            COUNT(DISTINCT d.delivery_id) FILTER (WHERE d.status = 'dead_lettered')::BIGINT AS dead_lettered_count,
            CASE
                WHEN COUNT(DISTINCT d.delivery_id) = 0 THEN NULL
                ELSE COUNT(DISTINCT d.delivery_id) FILTER (WHERE d.status = 'delivered')::DOUBLE PRECISION
                     / COUNT(DISTINCT d.delivery_id)::DOUBLE PRECISION
            END AS success_rate,
            percentile_cont(0.95) WITHIN GROUP (ORDER BY a.duration_ms)::DOUBLE PRECISION AS p95_latency_ms
         FROM webhook_deliveries d
         INNER JOIN webhook_endpoints we ON we.endpoint_id = d.endpoint_id
         INNER JOIN domain_events e ON e.event_id = d.event_id
         LEFT JOIN scenarios s ON s.scenario_id = d.scenario_id
         LEFT JOIN delivery_attempts a ON a.delivery_id = d.delivery_id
         WHERE ($1::TIMESTAMPTZ IS NULL OR d.created_at >= $1)
           AND ($2::BIGINT IS NULL OR d.merchant_id = $2)
           AND ($3::BIGINT IS NULL OR d.endpoint_id = $3)
           AND ($4::BIGINT IS NULL OR d.scenario_id = $4)
           AND ($5::TEXT IS NULL OR s.scenario_key = $5)
           AND ($6::TEXT IS NULL OR e.event_type = $6)
           AND ($7::BOOLEAN = TRUE OR d.scenario_id IS NULL OR COALESCE(s.include_in_history, TRUE) = TRUE)
         GROUP BY d.endpoint_id, d.merchant_id
         ORDER BY failed_attempt_count DESC, dead_lettered_count DESC, delivery_count DESC
         LIMIT $8",
    )
    .bind(filters.window.start_at)
    .bind(filters.merchant_id)
    .bind(filters.endpoint_id)
    .bind(filters.scenario_id)
    .bind(&filters.scenario_key)
    .bind(&filters.event_type)
    .bind(filters.include_hidden)
    .bind(filters.limit)
    .fetch_all(pool)
    .await?;

    Ok(MetricsEndpointsResponse {
        window: filters.window,
        items: rows,
    })
}

pub async fn get_metrics_scenarios(
    pool: &PgPool,
    query: MetricsQuery,
) -> AppResult<MetricsScenariosResponse> {
    let filters = MetricsFilters::from_query(query)?;
    let rows = sqlx::query_as::<_, MetricsScenarioItem>(
        "SELECT
            s.scenario_key,
            COUNT(DISTINCT s.scenario_id)::BIGINT AS run_count,
            COUNT(DISTINCT s.scenario_id) FILTER (
                WHERE s.status = 'completed'
                   OR (COALESCE(delivery_counts.deliveries_created, 0) > 0 AND COALESCE(delivery_counts.active_count, 0) = 0)
            )::BIGINT AS completed_count,
            COUNT(DISTINCT s.scenario_id) FILTER (WHERE s.status = 'failed')::BIGINT AS failed_count,
            COALESCE(SUM(delivery_counts.deliveries_created), 0)::BIGINT AS delivery_count,
            COALESCE(SUM(delivery_counts.delivered_count), 0)::BIGINT AS delivered_count,
            COALESCE(SUM(delivery_counts.dead_lettered_count), 0)::BIGINT AS dead_lettered_count,
            CASE
                WHEN COALESCE(SUM(delivery_counts.deliveries_created), 0) = 0 THEN NULL
                ELSE COALESCE(SUM(delivery_counts.delivered_count), 0)::DOUBLE PRECISION
                     / COALESCE(SUM(delivery_counts.deliveries_created), 0)::DOUBLE PRECISION
            END AS success_rate
         FROM scenarios s
         LEFT JOIN LATERAL (
            SELECT
                COUNT(*)::BIGINT AS deliveries_created,
                COUNT(*) FILTER (WHERE status = 'delivered')::BIGINT AS delivered_count,
                COUNT(*) FILTER (WHERE status = 'dead_lettered')::BIGINT AS dead_lettered_count,
                COUNT(*) FILTER (WHERE status IN ('pending', 'queued', 'processing', 'retrying'))::BIGINT AS active_count
            FROM webhook_deliveries
            WHERE scenario_id = s.scenario_id
         ) delivery_counts ON TRUE
         WHERE ($1::TIMESTAMPTZ IS NULL OR s.started_at >= $1)
           AND ($2::BIGINT IS NULL OR s.merchant_id = $2)
           AND (
                $3::BIGINT IS NULL
                OR EXISTS (
                    SELECT 1 FROM webhook_deliveries d
                    WHERE d.scenario_id = s.scenario_id AND d.endpoint_id = $3
                )
           )
           AND ($4::BIGINT IS NULL OR s.scenario_id = $4)
           AND ($5::TEXT IS NULL OR s.scenario_key = $5)
           AND (
                $6::TEXT IS NULL
                OR EXISTS (
                    SELECT 1 FROM domain_events e
                    WHERE e.scenario_id = s.scenario_id AND e.event_type = $6
                )
           )
           AND ($7::BOOLEAN = TRUE OR s.include_in_history = TRUE)
         GROUP BY s.scenario_key
         ORDER BY run_count DESC, delivery_count DESC
         LIMIT $8",
    )
    .bind(filters.window.start_at)
    .bind(filters.merchant_id)
    .bind(filters.endpoint_id)
    .bind(filters.scenario_id)
    .bind(&filters.scenario_key)
    .bind(&filters.event_type)
    .bind(filters.include_hidden)
    .bind(filters.limit)
    .fetch_all(pool)
    .await?;

    Ok(MetricsScenariosResponse {
        window: filters.window,
        items: rows,
    })
}

pub async fn get_metrics_http_status(
    pool: &PgPool,
    query: MetricsQuery,
) -> AppResult<MetricsHttpStatusResponse> {
    let filters = MetricsFilters::from_query(query)?;
    let rows = sqlx::query_as::<_, MetricsHttpStatusItem>(
        "SELECT
            CASE
                WHEN a.http_status IS NULL AND a.outcome = 'timeout' THEN 'timeout'
                WHEN a.http_status IS NULL THEN 'network'
                WHEN a.http_status BETWEEN 200 AND 299 THEN '2xx'
                WHEN a.http_status BETWEEN 300 AND 399 THEN '3xx'
                WHEN a.http_status BETWEEN 400 AND 499 THEN '4xx'
                WHEN a.http_status BETWEEN 500 AND 599 THEN '5xx'
                ELSE 'other'
            END AS status_family,
            a.http_status,
            COUNT(*)::BIGINT AS attempt_count
         FROM delivery_attempts a
         INNER JOIN webhook_deliveries d ON d.delivery_id = a.delivery_id
         INNER JOIN domain_events e ON e.event_id = d.event_id
         LEFT JOIN scenarios s ON s.scenario_id = d.scenario_id
         WHERE ($1::TIMESTAMPTZ IS NULL OR a.started_at >= $1)
           AND ($2::BIGINT IS NULL OR d.merchant_id = $2)
           AND ($3::BIGINT IS NULL OR d.endpoint_id = $3)
           AND ($4::BIGINT IS NULL OR d.scenario_id = $4)
           AND ($5::TEXT IS NULL OR s.scenario_key = $5)
           AND ($6::TEXT IS NULL OR e.event_type = $6)
           AND ($7::BOOLEAN = TRUE OR d.scenario_id IS NULL OR COALESCE(s.include_in_history, TRUE) = TRUE)
         GROUP BY status_family, a.http_status
         ORDER BY status_family, a.http_status",
    )
    .bind(filters.window.start_at)
    .bind(filters.merchant_id)
    .bind(filters.endpoint_id)
    .bind(filters.scenario_id)
    .bind(&filters.scenario_key)
    .bind(&filters.event_type)
    .bind(filters.include_hidden)
    .fetch_all(pool)
    .await?;

    Ok(MetricsHttpStatusResponse {
        window: filters.window,
        items: rows,
    })
}

pub async fn get_metrics_dead_letters(
    pool: &PgPool,
    query: MetricsQuery,
) -> AppResult<MetricsDeadLettersResponse> {
    let filters = MetricsFilters::from_query(query)?;
    let row = sqlx::query(
        "SELECT
            COUNT(DISTINCT d.delivery_id)::BIGINT AS total_dead_lettered,
            COUNT(DISTINCT d.delivery_id) FILTER (WHERE d.operator_resolved_at IS NULL)::BIGINT AS unresolved_dead_lettered,
            COUNT(DISTINCT d.delivery_id) FILTER (WHERE d.operator_resolved_at IS NOT NULL)::BIGINT AS resolved_dead_lettered
         FROM webhook_deliveries d
         INNER JOIN domain_events e ON e.event_id = d.event_id
         LEFT JOIN scenarios s ON s.scenario_id = d.scenario_id
         WHERE d.status = 'dead_lettered'
           AND ($1::TIMESTAMPTZ IS NULL OR d.final_state_at >= $1 OR d.created_at >= $1)
           AND ($2::BIGINT IS NULL OR d.merchant_id = $2)
           AND ($3::BIGINT IS NULL OR d.endpoint_id = $3)
           AND ($4::BIGINT IS NULL OR d.scenario_id = $4)
           AND ($5::TEXT IS NULL OR s.scenario_key = $5)
           AND ($6::TEXT IS NULL OR e.event_type = $6)
           AND ($7::BOOLEAN = TRUE OR d.scenario_id IS NULL OR COALESCE(s.include_in_history, TRUE) = TRUE)",
    )
    .bind(filters.window.start_at)
    .bind(filters.merchant_id)
    .bind(filters.endpoint_id)
    .bind(filters.scenario_id)
    .bind(&filters.scenario_key)
    .bind(&filters.event_type)
    .bind(filters.include_hidden)
    .fetch_one(pool)
    .await?;

    let top_endpoints = sqlx::query_as::<_, MetricsDeadLetterEndpointItem>(
        "SELECT
            d.endpoint_id,
            MAX(d.endpoint_url) AS endpoint_url,
            COUNT(DISTINCT d.delivery_id)::BIGINT AS dead_lettered_count,
            COUNT(DISTINCT d.delivery_id) FILTER (WHERE d.operator_resolved_at IS NULL)::BIGINT AS unresolved_count
         FROM webhook_deliveries d
         INNER JOIN domain_events e ON e.event_id = d.event_id
         LEFT JOIN scenarios s ON s.scenario_id = d.scenario_id
         WHERE d.status = 'dead_lettered'
           AND ($1::TIMESTAMPTZ IS NULL OR d.final_state_at >= $1 OR d.created_at >= $1)
           AND ($2::BIGINT IS NULL OR d.merchant_id = $2)
           AND ($3::BIGINT IS NULL OR d.endpoint_id = $3)
           AND ($4::BIGINT IS NULL OR d.scenario_id = $4)
           AND ($5::TEXT IS NULL OR s.scenario_key = $5)
           AND ($6::TEXT IS NULL OR e.event_type = $6)
           AND ($7::BOOLEAN = TRUE OR d.scenario_id IS NULL OR COALESCE(s.include_in_history, TRUE) = TRUE)
         GROUP BY d.endpoint_id
         ORDER BY dead_lettered_count DESC, unresolved_count DESC
         LIMIT $8",
    )
    .bind(filters.window.start_at)
    .bind(filters.merchant_id)
    .bind(filters.endpoint_id)
    .bind(filters.scenario_id)
    .bind(&filters.scenario_key)
    .bind(&filters.event_type)
    .bind(filters.include_hidden)
    .bind(filters.limit)
    .fetch_all(pool)
    .await?;

    Ok(MetricsDeadLettersResponse {
        window: filters.window,
        total_dead_lettered: row.get("total_dead_lettered"),
        unresolved_dead_lettered: row.get("unresolved_dead_lettered"),
        resolved_dead_lettered: row.get("resolved_dead_lettered"),
        top_endpoints,
    })
}

pub async fn get_metrics_lifecycle_funnel(
    pool: &PgPool,
    query: MetricsQuery,
) -> AppResult<MetricsLifecycleFunnelResponse> {
    let filters = MetricsFilters::from_query(query)?;
    let rows = sqlx::query_as::<_, MetricsLifecycleStepItem>(
        "SELECT
            t.step,
            t.status,
            COUNT(*)::BIGINT AS count,
            MIN(t.occurred_at) AS first_seen_at,
            MAX(t.occurred_at) AS last_seen_at
         FROM delivery_trace_events t
         LEFT JOIN webhook_deliveries d ON d.delivery_id = t.delivery_id
         INNER JOIN domain_events e ON e.event_id = t.event_id
         LEFT JOIN scenarios s ON s.scenario_id = e.scenario_id
         WHERE ($1::TIMESTAMPTZ IS NULL OR t.occurred_at >= $1)
           AND ($2::BIGINT IS NULL OR e.merchant_id = $2)
           AND ($3::BIGINT IS NULL OR d.endpoint_id = $3)
           AND ($4::BIGINT IS NULL OR e.scenario_id = $4)
           AND ($5::TEXT IS NULL OR s.scenario_key = $5)
           AND ($6::TEXT IS NULL OR e.event_type = $6)
           AND ($7::BOOLEAN = TRUE OR e.scenario_id IS NULL OR COALESCE(s.include_in_history, TRUE) = TRUE)
         GROUP BY t.step, t.status
         ORDER BY MIN(t.occurred_at), t.step, t.status",
    )
    .bind(filters.window.start_at)
    .bind(filters.merchant_id)
    .bind(filters.endpoint_id)
    .bind(filters.scenario_id)
    .bind(&filters.scenario_key)
    .bind(&filters.event_type)
    .bind(filters.include_hidden)
    .fetch_all(pool)
    .await?;

    Ok(MetricsLifecycleFunnelResponse {
        window: filters.window,
        steps: rows,
    })
}

fn range_start(range: &str, now: DateTime<Utc>) -> AppResult<Option<DateTime<Utc>>> {
    match range.trim().to_ascii_lowercase().as_str() {
        "1h" => Ok(Some(now - Duration::hours(1))),
        "6h" => Ok(Some(now - Duration::hours(6))),
        "24h" => Ok(Some(now - Duration::hours(24))),
        "7d" => Ok(Some(now - Duration::days(7))),
        "30d" => Ok(Some(now - Duration::days(30))),
        "all" => Ok(None),
        value => Err(AppError::BadRequest(format!(
            "unsupported metrics range {}",
            value
        ))),
    }
}

fn normalize_bucket(bucket: Option<String>, range: &str) -> AppResult<String> {
    let default = match range {
        "1h" | "6h" | "24h" => "hour",
        "7d" | "30d" => "day",
        "all" => "day",
        _ => "hour",
    };
    let bucket = bucket.unwrap_or_else(|| default.to_string());
    match bucket.trim().to_ascii_lowercase().as_str() {
        "minute" => Ok("minute".to_string()),
        "hour" => Ok("hour".to_string()),
        "day" => Ok("day".to_string()),
        value => Err(AppError::BadRequest(format!(
            "unsupported metrics bucket {}",
            value
        ))),
    }
}

fn ratio(numerator: i64, denominator: i64) -> Option<f64> {
    if denominator > 0 {
        Some(numerator as f64 / denominator as f64)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn empty_query() -> MetricsQuery {
        MetricsQuery {
            range: None,
            bucket: None,
            merchant_id: None,
            endpoint_id: None,
            scenario_id: None,
            scenario_key: None,
            event_type: None,
            include_hidden: None,
            limit: None,
        }
    }

    #[test]
    fn metrics_range_start_accepts_supported_windows() {
        let now = Utc.with_ymd_and_hms(2026, 6, 18, 12, 0, 0).unwrap();

        assert_eq!(
            range_start("1h", now).unwrap(),
            Some(now - Duration::hours(1))
        );
        assert_eq!(
            range_start("6h", now).unwrap(),
            Some(now - Duration::hours(6))
        );
        assert_eq!(
            range_start("24h", now).unwrap(),
            Some(now - Duration::hours(24))
        );
        assert_eq!(
            range_start("7d", now).unwrap(),
            Some(now - Duration::days(7))
        );
        assert_eq!(
            range_start("30d", now).unwrap(),
            Some(now - Duration::days(30))
        );
        assert_eq!(range_start("all", now).unwrap(), None);
    }

    #[test]
    fn metrics_range_start_rejects_unknown_window() {
        assert!(range_start("90d", Utc::now()).is_err());
    }

    #[test]
    fn metrics_bucket_defaults_follow_window_size() {
        assert_eq!(normalize_bucket(None, "24h").unwrap(), "hour");
        assert_eq!(normalize_bucket(None, "7d").unwrap(), "day");
        assert_eq!(
            normalize_bucket(Some(" minute ".to_string()), "24h").unwrap(),
            "minute"
        );
        assert!(normalize_bucket(Some("week".to_string()), "30d").is_err());
    }

    #[test]
    fn metrics_filters_clamp_top_list_limit_and_default_hidden_runs_off() {
        let mut query = empty_query();
        query.limit = Some(500);

        let filters = MetricsFilters::from_query(query).unwrap();

        assert_eq!(filters.limit, 50);
        assert!(!filters.include_hidden);
    }
}
