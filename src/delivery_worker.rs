use std::time::Duration;

use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use redis::{
    AsyncCommands, RedisError,
    aio::ConnectionManager,
    streams::{StreamAutoClaimOptions, StreamAutoClaimReply, StreamId, StreamReadReply},
};
use reqwest::Client;
use serde_json::{Value, json};
use sha2::Sha256;
use sqlx::{PgPool, Row};

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub enabled: bool,
    pub transport: DeliveryTransport,
    pub poll_interval: Duration,
    pub batch_size: i64,
    pub request_timeout: Duration,
    pub redis_url: Option<String>,
    pub redis_stream: String,
    pub redis_consumer_group: String,
    pub redis_consumer_name: String,
    pub redis_block_timeout: Duration,
}

impl WorkerConfig {
    pub fn from_env() -> Self {
        let enabled = std::env::var("WORKER_ENABLED")
            .map(|value| value != "0")
            .unwrap_or(true);

        let poll_interval = std::env::var("WORKER_POLL_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or_else(|| Duration::from_millis(1000));

        let batch_size = std::env::var("WORKER_BATCH_SIZE")
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(10)
            .clamp(1, 100);

        let request_timeout = std::env::var("WEBHOOK_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or_else(|| Duration::from_millis(3000));

        let transport = std::env::var("DELIVERY_TRANSPORT")
            .ok()
            .and_then(|value| DeliveryTransport::parse(&value))
            .unwrap_or_default();

        let redis_url = std::env::var("REDIS_URL").ok();

        let redis_stream =
            std::env::var("REDIS_STREAM").unwrap_or_else(|_| "webhook_delivery_stream".to_string());

        let redis_consumer_group =
            std::env::var("REDIS_CONSUMER_GROUP").unwrap_or_else(|_| "webhook_workers".to_string());

        let redis_consumer_name = std::env::var("REDIS_CONSUMER_NAME")
            .unwrap_or_else(|_| format!("worker-local-{}", std::process::id()));

        let redis_block_timeout = std::env::var("REDIS_BLOCK_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or_else(|| Duration::from_millis(5000));

        Self {
            enabled,
            transport,
            poll_interval,
            batch_size,
            request_timeout,
            redis_url,
            redis_stream,
            redis_consumer_group,
            redis_consumer_name,
            redis_block_timeout,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DeliveryTransport {
    #[default]
    Postgres,
    Redis,
}

impl DeliveryTransport {
    fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "postgres" | "db" => Some(Self::Postgres),
            "redis" | "redis_stream" | "redis-stream" => Some(Self::Redis),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct ClaimedDelivery {
    delivery_id: i64,
    event_id: i64,
    endpoint_id: i64,
    merchant_id: i64,
    endpoint_url: String,
    secret_version_id: Option<i64>,
    max_attempts: i64,
}

#[derive(Debug, Clone)]
struct WebhookSignature {
    timestamp: String,
    signature: String,
    key_version: Option<i64>,
}

#[derive(Debug, Clone)]
struct AttemptStart {
    attempt_id: i64,
    attempt_count: i64,
}

#[derive(Debug, Clone)]
enum DeliveryOutcome {
    Success,
    TemporaryFailure,
    PermanentFailure,
    Timeout,
    NetworkError(String),
}

impl DeliveryOutcome {
    fn as_db_str(&self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::TemporaryFailure => "temporary_failure",
            Self::PermanentFailure => "permanent_failure",
            Self::Timeout => "timeout",
            Self::NetworkError(_) => "temporary_failure",
        }
    }

    fn error_message(&self) -> Option<String> {
        match self {
            Self::NetworkError(message) => Some(message.clone()),
            Self::Timeout => Some("request timed out".to_string()),
            _ => None,
        }
    }

    fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::TemporaryFailure | Self::Timeout | Self::NetworkError(_)
        )
    }
}

#[derive(Debug, Clone)]
struct DeliveryResult {
    outcome: DeliveryOutcome,
    http_status: Option<u16>,
    response_body_sample: Option<String>,
    error_message: Option<String>,
}

#[derive(Debug, thiserror::Error)]
enum WorkerError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Redis(#[from] RedisError),
}

pub fn spawn(pool: PgPool, config: WorkerConfig) {
    if !config.enabled {
        tracing::info!("delivery worker disabled");
        return;
    }

    match config.transport {
        DeliveryTransport::Postgres => spawn_postgres_worker(pool, config),
        DeliveryTransport::Redis => spawn_redis_transport(pool, config),
    }
}

fn spawn_postgres_worker(pool: PgPool, config: WorkerConfig) {
    tokio::spawn(async move {
        let Some(client) = build_http_client(config.request_timeout) else {
            return;
        };

        tracing::info!(
            batch_size = config.batch_size,
            poll_ms = config.poll_interval.as_millis(),
            timeout_ms = config.request_timeout.as_millis(),
            "Postgres delivery worker started"
        );

        loop {
            match run_postgres_once(&pool, &client, config.batch_size).await {
                Ok(processed) if processed > 0 => {
                    tracing::debug!(processed, "processed delivery batch");
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::error!(%error, "delivery worker tick failed");
                }
            }

            tokio::time::sleep(config.poll_interval).await;
        }
    });
}

fn spawn_redis_transport(pool: PgPool, config: WorkerConfig) {
    let Some(redis_url) = config.redis_url.clone() else {
        tracing::error!("DELIVERY_TRANSPORT=redis requires REDIS_URL");
        return;
    };

    spawn_redis_relay(pool.clone(), config.clone(), redis_url.clone());
    spawn_redis_worker(pool, config, redis_url);
}

fn build_http_client(request_timeout: Duration) -> Option<Client> {
    match Client::builder().timeout(request_timeout).build() {
        Ok(client) => Some(client),
        Err(error) => {
            tracing::error!(%error, "failed to build HTTP client for delivery worker");
            None
        }
    }
}

async fn run_postgres_once(
    pool: &PgPool,
    client: &Client,
    batch_size: i64,
) -> Result<usize, sqlx::Error> {
    recover_stuck_processing(pool).await?;

    let deliveries = claim_due_deliveries(pool, batch_size).await?;
    let count = deliveries.len();

    for delivery in deliveries {
        let delivery_id = delivery.delivery_id;
        if let Err(error) = process_delivery(pool, client, delivery).await {
            tracing::error!(%error, delivery_id, "failed to process claimed delivery");
        }
    }

    Ok(count)
}

fn spawn_redis_relay(pool: PgPool, config: WorkerConfig, redis_url: String) {
    tokio::spawn(async move {
        let Some(mut redis) = connect_redis(&redis_url).await else {
            return;
        };

        tracing::info!(
            stream = config.redis_stream,
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

fn spawn_redis_worker(pool: PgPool, config: WorkerConfig, redis_url: String) {
    tokio::spawn(async move {
        let Some(client) = build_http_client(config.request_timeout) else {
            return;
        };

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
            tracing::error!(%error, "failed to initialize Redis consumer group");
            return;
        }

        tracing::info!(
            stream = config.redis_stream,
            group = config.redis_consumer_group,
            consumer = config.redis_consumer_name,
            batch_size = config.batch_size,
            timeout_ms = config.request_timeout.as_millis(),
            "Redis delivery worker started"
        );

        loop {
            if let Err(error) = recover_stuck_processing(&pool).await {
                tracing::error!(%error, "failed to recover stuck processing deliveries");
            }

            match run_redis_worker_once(&pool, &client, &mut redis, &config).await {
                Ok(processed) if processed > 0 => {
                    tracing::debug!(processed, "processed Redis delivery messages");
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::error!(%error, "Redis delivery worker tick failed");
                    tokio::time::sleep(config.poll_interval).await;
                }
            }
        }
    });
}

async fn connect_redis(redis_url: &str) -> Option<ConnectionManager> {
    let client = match redis::Client::open(redis_url) {
        Ok(client) => client,
        Err(error) => {
            tracing::error!(%error, "failed to create Redis client");
            return None;
        }
    };

    match client.get_connection_manager().await {
        Ok(connection) => Some(connection),
        Err(error) => {
            tracing::error!(%error, "failed to connect to Redis");
            None
        }
    }
}

async fn ensure_consumer_group(
    redis: &mut ConnectionManager,
    stream: &str,
    group: &str,
) -> redis::RedisResult<()> {
    let result: redis::RedisResult<()> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(stream)
        .arg(group)
        .arg("0")
        .arg("MKSTREAM")
        .query_async(redis)
        .await;

    match result {
        Ok(()) => Ok(()),
        Err(error) if error.to_string().contains("BUSYGROUP") => Ok(()),
        Err(error) => Err(error),
    }
}

async fn run_redis_relay_once(
    pool: &PgPool,
    redis: &mut ConnectionManager,
    config: &WorkerConfig,
) -> Result<usize, WorkerError> {
    recover_stuck_processing(pool).await?;
    recover_stale_queued(pool).await?;

    let delivery_ids = claim_due_deliveries_for_queue(pool, config.batch_size).await?;
    let count = delivery_ids.len();
    let mut failures = 0;

    for delivery_id in delivery_ids {
        match publish_delivery(redis, &config.redis_stream, delivery_id).await {
            Ok(message_id) => {
                mark_delivery_published(pool, delivery_id, &message_id).await?;
            }
            Err(error) => {
                mark_delivery_publish_failed(pool, delivery_id, &error.to_string()).await?;
                failures += 1;
                tracing::error!(
                    %error,
                    delivery_id,
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

    Ok(count)
}

async fn run_redis_worker_once(
    pool: &PgPool,
    client: &Client,
    redis: &mut ConnectionManager,
    config: &WorkerConfig,
) -> Result<usize, WorkerError> {
    let reply = read_delivery_messages(redis, config).await?;
    let mut processed = 0;

    for stream_key in reply.keys {
        for stream_id in stream_key.ids {
            processed += process_redis_message(pool, client, redis, config, stream_id).await?;
        }
    }

    processed += reclaim_pending_messages(pool, client, redis, config).await?;

    Ok(processed)
}

async fn process_redis_message(
    pool: &PgPool,
    client: &Client,
    redis: &mut ConnectionManager,
    config: &WorkerConfig,
    stream_id: StreamId,
) -> Result<usize, WorkerError> {
    let Some(delivery_id) = stream_id
        .map
        .get("delivery_id")
        .and_then(redis_value_to_i64)
    else {
        tracing::warn!(
            message_id = stream_id.id,
            "Redis delivery message missing delivery_id"
        );
        ack_message(
            redis,
            &config.redis_stream,
            &config.redis_consumer_group,
            &stream_id.id,
        )
        .await?;
        return Ok(0);
    };

    match claim_queued_delivery(pool, delivery_id).await? {
        Some(delivery) => {
            process_delivery(pool, client, delivery).await?;
            ack_message(
                redis,
                &config.redis_stream,
                &config.redis_consumer_group,
                &stream_id.id,
            )
            .await?;
            Ok(1)
        }
        None => {
            ack_message(
                redis,
                &config.redis_stream,
                &config.redis_consumer_group,
                &stream_id.id,
            )
            .await?;
            Ok(0)
        }
    }
}

async fn reclaim_pending_messages(
    pool: &PgPool,
    client: &Client,
    redis: &mut ConnectionManager,
    config: &WorkerConfig,
) -> Result<usize, WorkerError> {
    let reply = autoclaim_pending_messages(redis, config).await?;
    let mut processed = 0;

    for stream_id in reply.claimed {
        processed += process_redis_message(pool, client, redis, config, stream_id).await?;
    }

    for deleted_id in reply.deleted_ids {
        ack_message(
            redis,
            &config.redis_stream,
            &config.redis_consumer_group,
            &deleted_id,
        )
        .await?;
    }

    Ok(processed)
}

async fn claim_due_deliveries_for_queue(
    pool: &PgPool,
    batch_size: i64,
) -> Result<Vec<i64>, sqlx::Error> {
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
             queue_attempt_count = queue_attempt_count + 1,
             last_queue_error = NULL,
             updated_at = NOW()
         FROM due
         WHERE d.delivery_id = due.delivery_id
         RETURNING d.delivery_id",
    )
    .bind(batch_size)
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(|row| row.get("delivery_id")).collect())
}

async fn publish_delivery(
    redis: &mut ConnectionManager,
    stream: &str,
    delivery_id: i64,
) -> redis::RedisResult<String> {
    redis
        .xadd(stream, "*", &[("delivery_id", delivery_id)])
        .await
}

async fn mark_delivery_published(
    pool: &PgPool,
    delivery_id: i64,
    message_id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE webhook_deliveries
         SET published_at = NOW(),
             redis_message_id = $2,
             updated_at = NOW()
         WHERE delivery_id = $1",
    )
    .bind(delivery_id)
    .bind(message_id)
    .execute(pool)
    .await?;

    append_delivery_trace_by_id(
        pool,
        delivery_id,
        "queued_to_redis",
        "succeeded",
        "Delivery queued to Redis",
        json!({ "redis_message_id": message_id }),
    )
    .await?;

    Ok(())
}

async fn mark_delivery_publish_failed(
    pool: &PgPool,
    delivery_id: i64,
    error: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE webhook_deliveries
         SET status = 'retrying',
             next_attempt_at = NOW(),
             last_queue_error = $2,
             updated_at = NOW()
         WHERE delivery_id = $1",
    )
    .bind(delivery_id)
    .bind(error.chars().take(1000).collect::<String>())
    .execute(pool)
    .await?;

    append_delivery_trace_by_id(
        pool,
        delivery_id,
        "redis_publish_failed",
        "retrying",
        "Redis publish failed",
        json!({ "error": error.chars().take(1000).collect::<String>() }),
    )
    .await?;

    Ok(())
}

async fn read_delivery_messages(
    redis: &mut ConnectionManager,
    config: &WorkerConfig,
) -> redis::RedisResult<StreamReadReply> {
    redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg(&config.redis_consumer_group)
        .arg(&config.redis_consumer_name)
        .arg("COUNT")
        .arg(config.batch_size)
        .arg("BLOCK")
        .arg(config.redis_block_timeout.as_millis() as usize)
        .arg("STREAMS")
        .arg(&config.redis_stream)
        .arg(">")
        .query_async(redis)
        .await
}

async fn autoclaim_pending_messages(
    redis: &mut ConnectionManager,
    config: &WorkerConfig,
) -> redis::RedisResult<StreamAutoClaimReply> {
    let min_idle_ms = config.request_timeout.as_millis().max(60_000) as usize;
    let options = StreamAutoClaimOptions::default().count(config.batch_size as usize);

    redis
        .xautoclaim_options(
            &config.redis_stream,
            &config.redis_consumer_group,
            &config.redis_consumer_name,
            min_idle_ms,
            "0-0",
            options,
        )
        .await
}

async fn claim_queued_delivery(
    pool: &PgPool,
    delivery_id: i64,
) -> Result<Option<ClaimedDelivery>, sqlx::Error> {
    sqlx::query_as::<_, ClaimedDelivery>(
        "UPDATE webhook_deliveries d
         SET status = 'processing',
             first_attempt_at = COALESCE(first_attempt_at, NOW()),
             processing_started_at = NOW(),
             updated_at = NOW()
         WHERE d.delivery_id = $1
           AND d.status = 'queued'
         RETURNING
             d.delivery_id,
             d.event_id,
             d.endpoint_id,
             d.merchant_id,
             d.endpoint_url,
             d.secret_version_id,
             d.max_attempts",
    )
    .bind(delivery_id)
    .fetch_optional(pool)
    .await
}

async fn ack_message(
    redis: &mut ConnectionManager,
    stream: &str,
    group: &str,
    message_id: &str,
) -> redis::RedisResult<()> {
    let _: i64 = redis.xack(stream, group, &[message_id]).await?;
    Ok(())
}

fn redis_value_to_i64(value: &redis::Value) -> Option<i64> {
    match value {
        redis::Value::BulkString(bytes) => std::str::from_utf8(bytes).ok()?.parse().ok(),
        redis::Value::Int(value) => Some(*value),
        redis::Value::SimpleString(value) => value.parse().ok(),
        _ => None,
    }
}

async fn recover_stale_queued(pool: &PgPool) -> Result<u64, sqlx::Error> {
    let rows = sqlx::query(
        "UPDATE webhook_deliveries
         SET status = 'retrying',
             next_attempt_at = NOW(),
             last_queue_error = COALESCE(last_queue_error, 'queued delivery was not claimed in time'),
             updated_at = NOW()
         WHERE status = 'queued'
           AND queued_at < NOW() - INTERVAL '30 seconds'
         RETURNING delivery_id, event_id",
    )
    .fetch_all(pool)
    .await?;

    for row in &rows {
        let delivery_id: i64 = row.get("delivery_id");
        let event_id: i64 = row.get("event_id");

        append_trace(
            pool,
            delivery_id,
            event_id,
            "queued_recovered",
            "retrying",
            "Queued delivery was not claimed; returned to retry queue",
            json!({ "reason": "queue_timeout" }),
        )
        .await?;
    }

    if !rows.is_empty() {
        tracing::warn!(count = rows.len(), "recovered stale queued deliveries");
    }

    Ok(rows.len() as u64)
}

async fn recover_stuck_processing(pool: &PgPool) -> Result<u64, sqlx::Error> {
    let rows = sqlx::query(
        "UPDATE webhook_deliveries
         SET status = 'retrying',
             next_attempt_at = NOW(),
             last_error = COALESCE(last_error, 'processing lease expired before attempt completed'),
             processing_started_at = NULL,
             updated_at = NOW()
         WHERE status = 'processing'
           AND processing_started_at < NOW() - INTERVAL '30 seconds'
         RETURNING delivery_id, event_id",
    )
    .fetch_all(pool)
    .await?;

    for row in &rows {
        let delivery_id: i64 = row.get("delivery_id");
        let event_id: i64 = row.get("event_id");

        sqlx::query(
            "UPDATE delivery_attempts
             SET outcome = 'abandoned',
                 error_message = COALESCE(error_message, 'processing lease expired before attempt completed'),
                 completed_at = COALESCE(completed_at, NOW())
             WHERE delivery_id = $1
               AND outcome = 'unknown'
               AND completed_at IS NULL",
        )
        .bind(delivery_id)
        .execute(pool)
        .await?;

        append_trace(
            pool,
            delivery_id,
            event_id,
            "processing_recovered",
            "retrying",
            "Processing lease expired; delivery returned to retry queue",
            json!({ "reason": "processing_timeout" }),
        )
        .await?;
    }

    if !rows.is_empty() {
        tracing::warn!(count = rows.len(), "recovered stuck processing deliveries");
    }

    Ok(rows.len() as u64)
}

async fn claim_due_deliveries(
    pool: &PgPool,
    batch_size: i64,
) -> Result<Vec<ClaimedDelivery>, sqlx::Error> {
    sqlx::query_as::<_, ClaimedDelivery>(
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
         SET status = 'processing',
             first_attempt_at = COALESCE(first_attempt_at, NOW()),
             processing_started_at = NOW(),
             updated_at = NOW()
         FROM due
         WHERE d.delivery_id = due.delivery_id
         RETURNING
             d.delivery_id,
             d.event_id,
             d.endpoint_id,
             d.merchant_id,
             d.endpoint_url,
             d.secret_version_id,
             d.max_attempts",
    )
    .bind(batch_size)
    .fetch_all(pool)
    .await
}

async fn process_delivery(
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
            dead_letter_unrecoverable(
                pool,
                &delivery,
                "referenced event or payment row no longer exists",
            )
            .await?;
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

    let attempt = create_attempt(pool, &delivery).await?;
    append_trace(
        pool,
        delivery.delivery_id,
        delivery.event_id,
        "http_request_started",
        "active",
        "HTTP request started",
        json!({ "attempt": attempt.attempt_count, "url": delivery.endpoint_url }),
    )
    .await?;

    let result = send_webhook(pool, client, &delivery, attempt.attempt_count, &payload).await;
    complete_attempt(pool, attempt.attempt_id, &result).await?;

    append_trace(
        pool,
        delivery.delivery_id,
        delivery.event_id,
        "attempt_recorded",
        "succeeded",
        "Attempt recorded",
        json!({
            "attempt": attempt.attempt_count,
            "outcome": result.outcome.as_db_str(),
            "http_status": result.http_status
        }),
    )
    .await?;

    transition_delivery(pool, &delivery, attempt.attempt_count, &result).await?;

    Ok(())
}

async fn build_payload(pool: &PgPool, delivery: &ClaimedDelivery) -> Result<Value, sqlx::Error> {
    let row = sqlx::query(
        "SELECT
            e.event_type,
            e.event_snapshot_json,
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
    let event_snapshot: Value = row.get("event_snapshot_json");
    let payment_created_at: DateTime<Utc> = row.get("created_at");
    let payment_updated_at: DateTime<Utc> = row.get("updated_at");

    Ok(json!({
        "event_id": delivery.event_id,
        "delivery_id": delivery.delivery_id,
        "event_type": event_type,
        "merchant_id": delivery.merchant_id,
        "endpoint_id": delivery.endpoint_id,
        "event_snapshot": event_snapshot,
        "current_object": {
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

async fn create_attempt(
    pool: &PgPool,
    delivery: &ClaimedDelivery,
) -> Result<AttemptStart, sqlx::Error> {
    let row = sqlx::query(
        "INSERT INTO delivery_attempts (
            delivery_id,
            event_id,
            endpoint_id,
            attempt_count,
            outcome,
            started_at
         )
         VALUES (
            $1,
            $2,
            $3,
            (
                SELECT COUNT(*) + 1
                FROM delivery_attempts
                WHERE delivery_id = $1
            ),
            'unknown',
            NOW()
         )
         RETURNING attempt_id, attempt_count",
    )
    .bind(delivery.delivery_id)
    .bind(delivery.event_id)
    .bind(delivery.endpoint_id)
    .fetch_one(pool)
    .await?;

    Ok(AttemptStart {
        attempt_id: row.get("attempt_id"),
        attempt_count: row.get("attempt_count"),
    })
}

async fn send_webhook(
    pool: &PgPool,
    client: &Client,
    delivery: &ClaimedDelivery,
    attempt_count: i64,
    payload: &Value,
) -> DeliveryResult {
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

fn classify_status(status: u16) -> DeliveryOutcome {
    match status {
        200..=299 => DeliveryOutcome::Success,
        408 | 429 => DeliveryOutcome::TemporaryFailure,
        500..=599 => DeliveryOutcome::TemporaryFailure,
        400..=499 => DeliveryOutcome::PermanentFailure,
        _ => DeliveryOutcome::TemporaryFailure,
    }
}

async fn complete_attempt(
    pool: &PgPool,
    attempt_id: i64,
    result: &DeliveryResult,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE delivery_attempts
         SET http_status = $2,
             outcome = $3,
             error_message = $4,
             response_body_sample = $5,
             completed_at = NOW()
         WHERE attempt_id = $1",
    )
    .bind(attempt_id)
    .bind(result.http_status.map(|status| status as i16))
    .bind(result.outcome.as_db_str())
    .bind(delivery_error(result))
    .bind(&result.response_body_sample)
    .execute(pool)
    .await?;

    Ok(())
}

async fn transition_delivery(
    pool: &PgPool,
    delivery: &ClaimedDelivery,
    attempt_count: i64,
    result: &DeliveryResult,
) -> Result<(), sqlx::Error> {
    if matches!(result.outcome, DeliveryOutcome::Success) {
        sqlx::query(
            "UPDATE webhook_deliveries
             SET status = 'delivered',
                 next_attempt_at = NULL,
                 last_error = NULL,
                 processing_started_at = NULL,
                 final_state_at = NOW(),
                 updated_at = NOW()
             WHERE delivery_id = $1",
        )
        .bind(delivery.delivery_id)
        .execute(pool)
        .await?;

        append_trace(
            pool,
            delivery.delivery_id,
            delivery.event_id,
            "delivered",
            "succeeded",
            "Delivery succeeded",
            json!({ "attempt": attempt_count }),
        )
        .await?;

        return Ok(());
    }

    if result.outcome.is_retryable() && attempt_count < delivery.max_attempts {
        let next_attempt_at = Utc::now() + retry_delay(attempt_count);
        sqlx::query(
            "UPDATE webhook_deliveries
             SET status = 'retrying',
                 next_attempt_at = $2,
                 last_error = $3,
                 processing_started_at = NULL,
                 queued_at = NULL,
                 published_at = NULL,
                 redis_message_id = NULL,
                 updated_at = NOW()
             WHERE delivery_id = $1",
        )
        .bind(delivery.delivery_id)
        .bind(next_attempt_at)
        .bind(delivery_error(result))
        .execute(pool)
        .await?;

        append_trace(
            pool,
            delivery.delivery_id,
            delivery.event_id,
            "retry_scheduled",
            "retrying",
            "Retry scheduled",
            json!({ "attempt": attempt_count, "next_attempt_at": next_attempt_at }),
        )
        .await?;

        return Ok(());
    }

    sqlx::query(
        "UPDATE webhook_deliveries
         SET status = 'dead_lettered',
             next_attempt_at = NULL,
             last_error = $2,
             processing_started_at = NULL,
             final_state_at = NOW(),
             updated_at = NOW()
         WHERE delivery_id = $1",
    )
    .bind(delivery.delivery_id)
    .bind(delivery_error(result))
    .execute(pool)
    .await?;

    append_trace(
        pool,
        delivery.delivery_id,
        delivery.event_id,
        "dead_lettered",
        "dead_lettered",
        "Delivery dead-lettered",
        json!({ "attempt": attempt_count, "outcome": result.outcome.as_db_str() }),
    )
    .await?;

    Ok(())
}

fn retry_delay(attempt_count: i64) -> chrono::Duration {
    let exponent = (attempt_count - 1).clamp(0, 6) as u32;
    let seconds = 5_i64.saturating_mul(2_i64.saturating_pow(exponent));
    chrono::Duration::seconds(seconds)
}

fn delivery_error(result: &DeliveryResult) -> String {
    result.error_message.clone().unwrap_or_else(|| {
        result
            .outcome
            .error_message()
            .unwrap_or_else(|| result.outcome.as_db_str().to_string())
    })
}

async fn dead_letter_unrecoverable(
    pool: &PgPool,
    delivery: &ClaimedDelivery,
    reason: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE webhook_deliveries
         SET status = 'dead_lettered',
             next_attempt_at = NULL,
             last_error = $2,
             processing_started_at = NULL,
             final_state_at = NOW(),
             updated_at = NOW()
         WHERE delivery_id = $1",
    )
    .bind(delivery.delivery_id)
    .bind(reason)
    .execute(pool)
    .await?;

    append_trace(
        pool,
        delivery.delivery_id,
        delivery.event_id,
        "dead_lettered",
        "dead_lettered",
        "Delivery dead-lettered",
        json!({ "reason": reason }),
    )
    .await?;

    Ok(())
}

async fn append_trace(
    pool: &PgPool,
    delivery_id: i64,
    event_id: i64,
    step: &str,
    status: &str,
    title: &str,
    metadata: Value,
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
    .execute(pool)
    .await?;

    Ok(())
}

async fn append_delivery_trace_by_id(
    pool: &PgPool,
    delivery_id: i64,
    step: &str,
    status: &str,
    title: &str,
    metadata: Value,
) -> Result<(), sqlx::Error> {
    let Some(row) = sqlx::query("SELECT event_id FROM webhook_deliveries WHERE delivery_id = $1")
        .bind(delivery_id)
        .fetch_optional(pool)
        .await?
    else {
        return Ok(());
    };

    append_trace(
        pool,
        delivery_id,
        row.get("event_id"),
        step,
        status,
        title,
        metadata,
    )
    .await
}
