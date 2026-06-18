use std::time::Duration;

use sqlx::{PgPool, Row};

use crate::{
    delivery_worker::{DeliveryTransport, WorkerConfig},
    error::AppResult,
    models::{DashboardSummary, RetryBacklogSummary},
};

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
    let queued_deliveries: i64 = row.get("queued_deliveries");
    let processing_deliveries: i64 = row.get("processing_deliveries");
    let retrying_deliveries: i64 = row.get("retrying_deliveries");
    let delivered_deliveries: i64 = row.get("delivered_deliveries");
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

pub(crate) fn queue_depth(pending_due_now: i64, queued: i64, retry_due_now: i64) -> i64 {
    pending_due_now + queued + retry_due_now
}

pub(crate) fn active_deliveries(queued: i64, processing: i64, retrying: i64) -> i64 {
    queued + processing + retrying
}

pub(crate) async fn redis_pending_from_runtime() -> Option<i64> {
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
