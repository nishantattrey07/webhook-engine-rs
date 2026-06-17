use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    time::Duration,
};

use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use redis::{
    AsyncCommands, RedisError,
    aio::ConnectionManager,
    streams::{StreamAutoClaimOptions, StreamAutoClaimReply, StreamId, StreamReadReply},
};
use reqwest::{Client, Url};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Row, Transaction};
use tokio::net::lookup_host;

type HmacSha256 = Hmac<Sha256>;
type DeliveryTaskResult = Result<(i64, Result<(), sqlx::Error>), tokio::task::JoinError>;
type RedisDeliveryTaskResult =
    Result<(String, i64, Result<(), sqlx::Error>), tokio::task::JoinError>;

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub enabled: bool,
    pub transport: DeliveryTransport,
    pub poll_interval: Duration,
    pub batch_size: i64,
    pub concurrency: usize,
    pub request_timeout: Duration,
    pub redis_url: Option<String>,
    pub redis_stream: String,
    pub redis_consumer_group: String,
    pub redis_consumer_name: String,
    pub redis_block_timeout: Duration,
    pub redis_stream_max_len: Option<usize>,
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

        let concurrency = std::env::var("WORKER_CONCURRENCY")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
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

        let redis_stream_max_len = std::env::var("REDIS_STREAM_MAX_LEN")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0);

        Self {
            enabled,
            transport,
            poll_interval,
            batch_size,
            concurrency,
            request_timeout,
            redis_url,
            redis_stream,
            redis_consumer_group,
            redis_consumer_name,
            redis_block_timeout,
            redis_stream_max_len,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DeliveryTransport {
    Postgres,
    #[default]
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
    attempt_id: i64,
    attempt_count: i64,
    processing_lease_token: String,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct ClaimableDelivery {
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
struct RedisDeliveryJob {
    message_id: String,
    delivery: ClaimedDelivery,
}

#[derive(Debug, Clone)]
struct QueuedDelivery {
    delivery_id: i64,
    queue_token: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FinalDeliveryState {
    Delivered,
    Retrying,
    DeadLettered,
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
            concurrency = config.concurrency,
            poll_ms = config.poll_interval.as_millis(),
            timeout_ms = config.request_timeout.as_millis(),
            "Postgres delivery worker started"
        );

        loop {
            match run_postgres_once(
                &pool,
                &client,
                config.batch_size,
                config.concurrency,
                config.request_timeout,
                &config.redis_consumer_name,
            )
            .await
            {
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
    match Client::builder()
        .timeout(request_timeout)
        .redirect(reqwest::redirect::Policy::none())
        .build()
    {
        Ok(client) => Some(client),
        Err(error) => {
            tracing::error!(%error, "failed to build HTTP client for delivery worker");
            None
        }
    }
}

fn processing_lease_duration(request_timeout: Duration) -> chrono::Duration {
    let millis = request_timeout
        .saturating_add(Duration::from_secs(30))
        .as_millis()
        .min(i64::MAX as u128) as i64;

    chrono::Duration::milliseconds(millis)
}

async fn run_postgres_once(
    pool: &PgPool,
    client: &Client,
    batch_size: i64,
    concurrency: usize,
    request_timeout: Duration,
    worker_id: &str,
) -> Result<usize, sqlx::Error> {
    recover_stuck_processing(pool).await?;

    let lease_duration = processing_lease_duration(request_timeout);
    let deliveries = claim_due_deliveries(pool, batch_size, lease_duration, worker_id).await?;
    let count = deliveries.len();

    process_claimed_deliveries(pool, client, deliveries, concurrency).await;

    Ok(count)
}

async fn process_claimed_deliveries(
    pool: &PgPool,
    client: &Client,
    deliveries: Vec<ClaimedDelivery>,
    concurrency: usize,
) -> usize {
    let mut processed = 0;
    let mut in_flight = tokio::task::JoinSet::new();
    let concurrency = concurrency.max(1);

    for delivery in deliveries {
        while in_flight.len() >= concurrency {
            if handle_delivery_join(in_flight.join_next().await).await {
                processed += 1;
            }
        }

        let pool = pool.clone();
        let client = client.clone();
        in_flight.spawn(async move {
            let delivery_id = delivery.delivery_id;
            let result = process_delivery(&pool, &client, delivery).await;
            (delivery_id, result)
        });
    }

    while let Some(result) = in_flight.join_next().await {
        if handle_delivery_join(Some(result)).await {
            processed += 1;
        }
    }

    processed
}

async fn handle_delivery_join(result: Option<DeliveryTaskResult>) -> bool {
    match result {
        Some(Ok((_, Ok(())))) => true,
        Some(Ok((delivery_id, Err(error)))) => {
            tracing::error!(%error, delivery_id, "failed to process claimed delivery");
            false
        }
        Some(Err(error)) => {
            tracing::error!(%error, "delivery task join failed");
            false
        }
        None => false,
    }
}

fn spawn_redis_relay(pool: PgPool, config: WorkerConfig, redis_url: String) {
    tokio::spawn(async move {
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
            tracing::error!(%error, "failed to initialize Redis consumer group for relay");
            return;
        }

        tracing::info!(
            stream = config.redis_stream,
            group = config.redis_consumer_group,
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
            concurrency = config.concurrency,
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

    let queued_deliveries = claim_due_deliveries_for_queue(pool, config.batch_size).await?;
    let count = queued_deliveries.len();
    let mut failures = 0;

    for queued_delivery in queued_deliveries {
        match publish_delivery(redis, &config.redis_stream, &queued_delivery).await {
            Ok(message_id) => {
                mark_delivery_published(pool, &queued_delivery, &message_id).await?;
            }
            Err(error) => {
                mark_delivery_publish_failed(pool, &queued_delivery, &error.to_string()).await?;
                failures += 1;
                tracing::error!(
                    %error,
                    delivery_id = queued_delivery.delivery_id,
                    queue_token = %queued_delivery.queue_token,
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

    if count > 0 {
        trim_redis_stream_if_configured(redis, config).await?;
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
    let mut jobs = Vec::new();

    for stream_key in reply.keys {
        for stream_id in stream_key.ids {
            if let Some(job) = claim_redis_message(pool, redis, config, stream_id).await? {
                jobs.push(job);
            }
        }
    }

    let mut processed = process_redis_jobs(pool, client, redis, config, jobs).await?;
    processed += reclaim_pending_messages(pool, client, redis, config).await?;

    Ok(processed)
}

async fn claim_redis_message(
    pool: &PgPool,
    redis: &mut ConnectionManager,
    config: &WorkerConfig,
    stream_id: StreamId,
) -> Result<Option<RedisDeliveryJob>, WorkerError> {
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
        return Ok(None);
    };

    let Some(queue_token) = stream_id
        .map
        .get("queue_token")
        .and_then(redis_value_to_uuid_text)
    else {
        tracing::warn!(
            message_id = stream_id.id,
            delivery_id,
            "Redis delivery message missing invalid queue_token; acking without processing"
        );
        ack_message(
            redis,
            &config.redis_stream,
            &config.redis_consumer_group,
            &stream_id.id,
        )
        .await?;
        return Ok(None);
    };

    let lease_duration = processing_lease_duration(config.request_timeout);
    match claim_queued_delivery(
        pool,
        delivery_id,
        &queue_token,
        lease_duration,
        &config.redis_consumer_name,
    )
    .await?
    {
        Some(delivery) => Ok(Some(RedisDeliveryJob {
            message_id: stream_id.id,
            delivery,
        })),
        None => {
            tracing::warn!(
                message_id = stream_id.id,
                delivery_id,
                queue_token,
                "Redis delivery message skipped because delivery is missing, terminal, already claimed, or queue token is stale"
            );
            ack_message(
                redis,
                &config.redis_stream,
                &config.redis_consumer_group,
                &stream_id.id,
            )
            .await?;
            Ok(None)
        }
    }
}

async fn process_redis_jobs(
    pool: &PgPool,
    client: &Client,
    redis: &mut ConnectionManager,
    config: &WorkerConfig,
    jobs: Vec<RedisDeliveryJob>,
) -> Result<usize, WorkerError> {
    let mut processed = 0;
    let mut successful_message_ids = Vec::new();
    let mut in_flight = tokio::task::JoinSet::new();
    let concurrency = config.concurrency.max(1);

    for job in jobs {
        while in_flight.len() >= concurrency {
            if let Some(message_id) = handle_redis_delivery_join(in_flight.join_next().await).await
            {
                successful_message_ids.push(message_id);
                processed += 1;
            }
        }

        let pool = pool.clone();
        let client = client.clone();
        in_flight.spawn(async move {
            let delivery_id = job.delivery.delivery_id;
            let message_id = job.message_id;
            let result = process_delivery(&pool, &client, job.delivery).await;
            (message_id, delivery_id, result)
        });
    }

    while let Some(result) = in_flight.join_next().await {
        if let Some(message_id) = handle_redis_delivery_join(Some(result)).await {
            successful_message_ids.push(message_id);
            processed += 1;
        }
    }

    for message_id in successful_message_ids {
        ack_message(
            redis,
            &config.redis_stream,
            &config.redis_consumer_group,
            &message_id,
        )
        .await?;
    }

    Ok(processed)
}

async fn handle_redis_delivery_join(result: Option<RedisDeliveryTaskResult>) -> Option<String> {
    match result {
        Some(Ok((message_id, _, Ok(())))) => Some(message_id),
        Some(Ok((_, delivery_id, Err(error)))) => {
            tracing::error!(%error, delivery_id, "failed to process Redis delivery");
            None
        }
        Some(Err(error)) => {
            tracing::error!(%error, "Redis delivery task join failed");
            None
        }
        None => None,
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
        if let Some(job) = claim_redis_message(pool, redis, config, stream_id).await? {
            processed += process_redis_jobs(pool, client, redis, config, vec![job]).await?;
        }
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
) -> Result<Vec<QueuedDelivery>, sqlx::Error> {
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
             queue_token = gen_random_uuid(),
             queue_attempt_count = queue_attempt_count + 1,
             last_queue_error = NULL,
             published_at = NULL,
             redis_message_id = NULL,
             updated_at = NOW()
         FROM due
         WHERE d.delivery_id = due.delivery_id
         RETURNING d.delivery_id, d.queue_token::TEXT AS queue_token",
    )
    .bind(batch_size)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| QueuedDelivery {
            delivery_id: row.get("delivery_id"),
            queue_token: row.get("queue_token"),
        })
        .collect())
}

async fn publish_delivery(
    redis: &mut ConnectionManager,
    stream: &str,
    delivery: &QueuedDelivery,
) -> redis::RedisResult<String> {
    redis
        .xadd(
            stream,
            "*",
            &[
                ("delivery_id", delivery.delivery_id.to_string()),
                ("queue_token", delivery.queue_token.clone()),
            ],
        )
        .await
}

async fn trim_redis_stream_if_configured(
    redis: &mut ConnectionManager,
    config: &WorkerConfig,
) -> redis::RedisResult<()> {
    let Some(max_len) = config.redis_stream_max_len else {
        return Ok(());
    };

    let _: i64 = redis::cmd("XTRIM")
        .arg(&config.redis_stream)
        .arg("MAXLEN")
        .arg("~")
        .arg(max_len)
        .query_async(redis)
        .await?;

    Ok(())
}

async fn mark_delivery_published(
    pool: &PgPool,
    delivery: &QueuedDelivery,
    message_id: &str,
) -> Result<(), sqlx::Error> {
    let row = sqlx::query(
        "UPDATE webhook_deliveries
         SET published_at = NOW(),
             redis_message_id = $2,
             updated_at = NOW()
         WHERE delivery_id = $1
           AND status IN ('queued', 'processing')
           AND queue_token = $3::uuid
         RETURNING event_id",
    )
    .bind(delivery.delivery_id)
    .bind(message_id)
    .bind(&delivery.queue_token)
    .fetch_optional(pool)
    .await?;

    let Some(row) = row else {
        tracing::warn!(
            delivery_id = delivery.delivery_id,
            queue_token = %delivery.queue_token,
            redis_message_id = message_id,
            "stale Redis publish success ignored because queue generation no longer matches"
        );
        return Ok(());
    };

    append_trace(
        pool,
        delivery.delivery_id,
        row.get("event_id"),
        "queued_to_redis",
        "succeeded",
        "Delivery queued to Redis",
        json!({ "redis_message_id": message_id, "queue_token": delivery.queue_token.clone() }),
    )
    .await?;

    Ok(())
}

async fn mark_delivery_publish_failed(
    pool: &PgPool,
    delivery: &QueuedDelivery,
    error: &str,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    let Some(row) = sqlx::query(
        "SELECT event_id, queue_attempt_count
         FROM webhook_deliveries
         WHERE delivery_id = $1
           AND status = 'queued'
           AND queue_token = $2::uuid
         FOR UPDATE",
    )
    .bind(delivery.delivery_id)
    .bind(&delivery.queue_token)
    .fetch_optional(&mut *tx)
    .await?
    else {
        tx.commit().await?;
        tracing::warn!(
            delivery_id = delivery.delivery_id,
            queue_token = %delivery.queue_token,
            "stale Redis publish failure ignored because queue generation no longer matches"
        );
        return Ok(());
    };

    let event_id: i64 = row.get("event_id");
    let queue_attempt_count: i64 = row.get("queue_attempt_count");
    let backoff = queue_publish_backoff(queue_attempt_count);
    let next_attempt_at = Utc::now() + backoff;
    let error = error.chars().take(1000).collect::<String>();

    sqlx::query(
        "UPDATE webhook_deliveries
         SET status = 'retrying',
             next_attempt_at = $3,
             last_queue_error = $4,
             queued_at = NULL,
             queue_token = NULL,
             published_at = NULL,
             redis_message_id = NULL,
             updated_at = NOW()
         WHERE delivery_id = $1
           AND status = 'queued'
           AND queue_token = $2::uuid",
    )
    .bind(delivery.delivery_id)
    .bind(&delivery.queue_token)
    .bind(next_attempt_at)
    .bind(&error)
    .execute(&mut *tx)
    .await?;

    append_trace_in_tx(
        &mut tx,
        delivery.delivery_id,
        event_id,
        "redis_publish_failed",
        "retrying",
        "Redis publish failed",
        json!({
            "error": error,
            "queue_token": delivery.queue_token.clone(),
            "queue_attempt": queue_attempt_count,
            "next_attempt_at": next_attempt_at,
            "backoff_ms": backoff.num_milliseconds()
        }),
    )
    .await?;

    tx.commit().await?;

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
    queue_token: &str,
    lease_duration: chrono::Duration,
    worker_id: &str,
) -> Result<Option<ClaimedDelivery>, sqlx::Error> {
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
         WHERE delivery_id = $1
           AND status = 'queued'
           AND queue_token = $2::uuid
         FOR UPDATE SKIP LOCKED",
    )
    .bind(delivery_id)
    .bind(queue_token)
    .fetch_optional(&mut *tx)
    .await?
    else {
        tx.commit().await?;
        return Ok(None);
    };

    let claimed = claim_locked_delivery(&mut tx, delivery, lease_duration, worker_id).await?;
    tx.commit().await?;

    Ok(Some(claimed))
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

fn redis_value_to_uuid_text(value: &redis::Value) -> Option<String> {
    let text = match value {
        redis::Value::BulkString(bytes) => std::str::from_utf8(bytes).ok()?,
        redis::Value::SimpleString(value) => value,
        _ => return None,
    };

    if is_uuid_text(text) {
        Some(text.to_ascii_lowercase())
    } else {
        None
    }
}

fn is_uuid_text(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 36 {
        return false;
    }

    for (index, byte) in bytes.iter().enumerate() {
        match index {
            8 | 13 | 18 | 23 => {
                if *byte != b'-' {
                    return false;
                }
            }
            _ if !byte.is_ascii_hexdigit() => return false,
            _ => {}
        }
    }

    true
}

async fn recover_stale_queued(pool: &PgPool) -> Result<u64, sqlx::Error> {
    let rows = sqlx::query(
        "UPDATE webhook_deliveries
         SET status = 'retrying',
             next_attempt_at = NOW(),
             last_queue_error = COALESCE(last_queue_error, 'queued delivery was not claimed in time'),
             queued_at = NULL,
             queue_token = NULL,
             published_at = NULL,
             redis_message_id = NULL,
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
        "SELECT delivery_id
         FROM webhook_deliveries
         WHERE status = 'processing'
           AND processing_lease_expires_at IS NOT NULL
           AND processing_lease_expires_at <= NOW()
         ORDER BY processing_lease_expires_at, delivery_id",
    )
    .fetch_all(pool)
    .await?;

    let mut recovered = 0;
    for row in rows {
        let delivery_id: i64 = row.get("delivery_id");
        if recover_processing_delivery(pool, delivery_id).await? {
            recovered += 1;
        }
    }

    if recovered > 0 {
        tracing::warn!(count = recovered, "recovered expired processing deliveries");
    }

    Ok(recovered)
}

async fn recover_processing_delivery(pool: &PgPool, delivery_id: i64) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let Some(row) = sqlx::query(
        "SELECT
             d.delivery_id,
             d.event_id,
             d.max_attempts,
             d.current_attempt_id,
             COALESCE(current_attempt.attempt_id, latest_attempt.attempt_id) AS attempt_id,
             COALESCE(current_attempt.attempt_count, latest_attempt.attempt_count) AS attempt_count,
             COALESCE(current_attempt.outcome, latest_attempt.outcome) AS outcome,
             COALESCE(current_attempt.completed_at, latest_attempt.completed_at) AS completed_at
         FROM webhook_deliveries d
         LEFT JOIN delivery_attempts current_attempt
            ON current_attempt.attempt_id = d.current_attempt_id
         LEFT JOIN LATERAL (
            SELECT attempt_id, attempt_count, outcome, completed_at
            FROM delivery_attempts
            WHERE delivery_id = d.delivery_id
            ORDER BY attempt_count DESC, attempt_id DESC
            LIMIT 1
         ) latest_attempt
            ON current_attempt.attempt_id IS NULL
         WHERE d.delivery_id = $1
           AND d.status = 'processing'
           AND d.processing_lease_expires_at IS NOT NULL
           AND d.processing_lease_expires_at <= NOW()
         FOR UPDATE OF d",
    )
    .bind(delivery_id)
    .fetch_optional(&mut *tx)
    .await?
    else {
        tx.commit().await?;
        return Ok(false);
    };

    let event_id: i64 = row.get("event_id");
    let max_attempts: i64 = row.get("max_attempts");
    let current_attempt_id: Option<i64> = row.get("current_attempt_id");
    let attempt_id: Option<i64> = row.get("attempt_id");
    let attempt_count: Option<i64> = row.get("attempt_count");
    let mut outcome: Option<String> = row.get("outcome");
    let completed_at: Option<DateTime<Utc>> = row.get("completed_at");

    if let (Some(current_attempt_id), Some(attempt_id), Some(current_outcome), None) = (
        current_attempt_id,
        attempt_id,
        outcome.as_deref(),
        completed_at,
    ) && current_attempt_id == attempt_id
        && current_outcome == "unknown"
    {
        sqlx::query(
            "UPDATE delivery_attempts
             SET outcome = 'abandoned',
                 error_message = COALESCE(error_message, 'processing lease expired before attempt completed'),
                 completed_at = COALESCE(completed_at, NOW())
             WHERE attempt_id = $1
               AND outcome = 'unknown'
               AND completed_at IS NULL",
        )
        .bind(attempt_id)
        .execute(&mut *tx)
        .await?;
        outcome = Some("abandoned".to_string());
    }

    let attempt_count = attempt_count.unwrap_or(0);
    let outcome = outcome.as_deref().unwrap_or("unknown");
    let final_state = decide_final_delivery_state(outcome, attempt_count, max_attempts);

    match final_state {
        FinalDeliveryState::Delivered => {
            sqlx::query(
                "UPDATE webhook_deliveries
                 SET status = 'delivered',
                     next_attempt_at = NULL,
                     last_error = NULL,
                     processing_started_at = NULL,
                     processing_lease_token = NULL,
                     processing_lease_expires_at = NULL,
                     current_attempt_id = NULL,
                     processing_worker_id = NULL,
                     queue_token = NULL,
                     final_state_at = NOW(),
                     updated_at = NOW()
                 WHERE delivery_id = $1",
            )
            .bind(delivery_id)
            .execute(&mut *tx)
            .await?;

            append_trace_in_tx(
                &mut tx,
                delivery_id,
                event_id,
                "processing_recovered",
                "succeeded",
                "Recovered expired processing delivery as delivered",
                json!({ "reason": "processing_lease_expired", "attempt": attempt_count }),
            )
            .await?;
        }
        FinalDeliveryState::Retrying => {
            let next_attempt_at = Utc::now() + retry_delay(attempt_count.max(1));
            sqlx::query(
                "UPDATE webhook_deliveries
                 SET status = 'retrying',
                     next_attempt_at = $2,
                     last_error = COALESCE(last_error, 'processing lease expired before attempt completed'),
                     processing_started_at = NULL,
                     processing_lease_token = NULL,
                     processing_lease_expires_at = NULL,
                     current_attempt_id = NULL,
                     processing_worker_id = NULL,
                     queued_at = NULL,
                     queue_token = NULL,
                     published_at = NULL,
                     redis_message_id = NULL,
                     updated_at = NOW()
                 WHERE delivery_id = $1",
            )
            .bind(delivery_id)
            .bind(next_attempt_at)
            .execute(&mut *tx)
            .await?;

            append_trace_in_tx(
                &mut tx,
                delivery_id,
                event_id,
                "processing_recovered",
                "retrying",
                "Processing lease expired; delivery returned to retry queue",
                json!({
                    "reason": "processing_lease_expired",
                    "attempt": attempt_count,
                    "next_attempt_at": next_attempt_at
                }),
            )
            .await?;
        }
        FinalDeliveryState::DeadLettered => {
            sqlx::query(
                "UPDATE webhook_deliveries
                 SET status = 'dead_lettered',
                     next_attempt_at = NULL,
                     last_error = COALESCE(last_error, 'processing lease expired and attempts are exhausted'),
                     processing_started_at = NULL,
                     processing_lease_token = NULL,
                     processing_lease_expires_at = NULL,
                     current_attempt_id = NULL,
                     processing_worker_id = NULL,
                     queue_token = NULL,
                     final_state_at = NOW(),
                     updated_at = NOW()
                 WHERE delivery_id = $1",
            )
            .bind(delivery_id)
            .execute(&mut *tx)
            .await?;

            append_trace_in_tx(
                &mut tx,
                delivery_id,
                event_id,
                "processing_recovered",
                "dead_lettered",
                "Processing lease expired; delivery dead-lettered",
                json!({
                    "reason": "processing_lease_expired",
                    "attempt": attempt_count,
                    "outcome": outcome
                }),
            )
            .await?;
        }
    }

    tx.commit().await?;
    Ok(true)
}

async fn claim_due_deliveries(
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

async fn claim_locked_delivery(
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

async fn validate_delivery_target(endpoint_url: &str) -> Result<(), String> {
    let parsed = Url::parse(endpoint_url.trim())
        .map_err(|_| "endpoint URL must be a valid URL".to_string())?;

    match parsed.scheme() {
        "http" | "https" => {}
        _ => return Err("endpoint URL must use http or https".to_string()),
    }

    let host = parsed
        .host_str()
        .ok_or_else(|| "endpoint URL must include a host".to_string())?;

    if allow_local_webhook_targets() {
        return Ok(());
    }

    if is_blocked_webhook_host(host) {
        return Err("endpoint URL resolves to a blocked local or private target".to_string());
    }

    let port = parsed
        .port_or_known_default()
        .ok_or_else(|| "endpoint URL must include a valid port".to_string())?;
    let resolved = lookup_host((host, port))
        .await
        .map_err(|error| format!("failed to resolve endpoint host: {error}"))?
        .collect::<Vec<_>>();

    if resolved.is_empty() {
        return Err("endpoint host did not resolve to any addresses".to_string());
    }

    if resolved.iter().any(|address| is_blocked_ip(address.ip())) {
        return Err("endpoint URL resolves to a blocked local or private target".to_string());
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

    host.parse::<IpAddr>().map(is_blocked_ip).unwrap_or(false)
}

fn is_blocked_ip(addr: IpAddr) -> bool {
    match addr {
        IpAddr::V4(addr) => is_blocked_ipv4(addr),
        IpAddr::V6(addr) => is_blocked_ipv6(addr),
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

fn sha256_hex(body: &[u8]) -> String {
    hex::encode(Sha256::digest(body))
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

fn classify_status(status: u16) -> DeliveryOutcome {
    match status {
        200..=299 => DeliveryOutcome::Success,
        300..=399 => DeliveryOutcome::PermanentFailure,
        408 | 429 => DeliveryOutcome::TemporaryFailure,
        500..=599 => DeliveryOutcome::TemporaryFailure,
        400..=499 => DeliveryOutcome::PermanentFailure,
        _ => DeliveryOutcome::TemporaryFailure,
    }
}

async fn finalize_attempt_and_delivery(
    pool: &PgPool,
    delivery: &ClaimedDelivery,
    result: &DeliveryResult,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    let guard = sqlx::query(
        "SELECT delivery_id
         FROM webhook_deliveries
         WHERE delivery_id = $1
           AND status = 'processing'
           AND current_attempt_id = $2
           AND processing_lease_token = $3::uuid
         FOR UPDATE",
    )
    .bind(delivery.delivery_id)
    .bind(delivery.attempt_id)
    .bind(&delivery.processing_lease_token)
    .fetch_optional(&mut *tx)
    .await?;

    if guard.is_none() {
        tx.rollback().await?;
        tracing::warn!(
            delivery_id = delivery.delivery_id,
            attempt_id = delivery.attempt_id,
            attempt = delivery.attempt_count,
            "stale worker completion ignored because delivery fence no longer matches"
        );
        return Ok(());
    }

    sqlx::query(
        "UPDATE delivery_attempts
         SET http_status = $2,
             outcome = $3,
             error_message = $4,
             response_body_sample = $5,
             completed_at = NOW()
         WHERE attempt_id = $1
           AND delivery_id = $6",
    )
    .bind(delivery.attempt_id)
    .bind(result.http_status.map(|status| status as i16))
    .bind(result.outcome.as_db_str())
    .bind(delivery_error(result))
    .bind(&result.response_body_sample)
    .bind(delivery.delivery_id)
    .execute(&mut *tx)
    .await?;

    match decide_final_delivery_state(
        result.outcome.as_db_str(),
        delivery.attempt_count,
        delivery.max_attempts,
    ) {
        FinalDeliveryState::Delivered => {
            let update = sqlx::query(
                "UPDATE webhook_deliveries
                 SET status = 'delivered',
                     next_attempt_at = NULL,
                     last_error = NULL,
                     processing_started_at = NULL,
                     processing_lease_token = NULL,
                     processing_lease_expires_at = NULL,
                     current_attempt_id = NULL,
                     processing_worker_id = NULL,
                     queue_token = NULL,
                     final_state_at = NOW(),
                     updated_at = NOW()
                 WHERE delivery_id = $1
                   AND status = 'processing'
                   AND current_attempt_id = $2
                   AND processing_lease_token = $3::uuid",
            )
            .bind(delivery.delivery_id)
            .bind(delivery.attempt_id)
            .bind(&delivery.processing_lease_token)
            .execute(&mut *tx)
            .await?;
            if update.rows_affected() == 0 {
                tx.rollback().await?;
                tracing::warn!(
                    delivery_id = delivery.delivery_id,
                    attempt_id = delivery.attempt_id,
                    "stale worker delivered transition ignored after guarded update matched zero rows"
                );
                return Ok(());
            }

            append_trace_in_tx(
                &mut tx,
                delivery.delivery_id,
                delivery.event_id,
                "delivered",
                "succeeded",
                "Delivery succeeded",
                json!({
                    "attempt": delivery.attempt_count,
                    "max_attempts": delivery.max_attempts,
                    "outcome": result.outcome.as_db_str(),
                    "http_status": result.http_status
                }),
            )
            .await?;
        }
        FinalDeliveryState::Retrying => {
            let retry_backoff = retry_delay(delivery.attempt_count);
            let next_attempt_at = Utc::now() + retry_backoff;
            let error_message = delivery_error(result);
            let update = sqlx::query(
                "UPDATE webhook_deliveries
                 SET status = 'retrying',
                     next_attempt_at = $4,
                     last_error = $5,
                     processing_started_at = NULL,
                     processing_lease_token = NULL,
                     processing_lease_expires_at = NULL,
                     current_attempt_id = NULL,
                     processing_worker_id = NULL,
                     queued_at = NULL,
                     queue_token = NULL,
                     published_at = NULL,
                     redis_message_id = NULL,
                     updated_at = NOW()
                 WHERE delivery_id = $1
                   AND status = 'processing'
                   AND current_attempt_id = $2
                   AND processing_lease_token = $3::uuid",
            )
            .bind(delivery.delivery_id)
            .bind(delivery.attempt_id)
            .bind(&delivery.processing_lease_token)
            .bind(next_attempt_at)
            .bind(&error_message)
            .execute(&mut *tx)
            .await?;
            if update.rows_affected() == 0 {
                tx.rollback().await?;
                tracing::warn!(
                    delivery_id = delivery.delivery_id,
                    attempt_id = delivery.attempt_id,
                    "stale worker retry transition ignored after guarded update matched zero rows"
                );
                return Ok(());
            }

            append_trace_in_tx(
                &mut tx,
                delivery.delivery_id,
                delivery.event_id,
                "retry_scheduled",
                "retrying",
                "Retry scheduled",
                json!({
                    "attempt": delivery.attempt_count,
                    "max_attempts": delivery.max_attempts,
                    "attempts_remaining": delivery.max_attempts.saturating_sub(delivery.attempt_count),
                    "outcome": result.outcome.as_db_str(),
                    "http_status": result.http_status,
                    "error": error_message,
                    "next_attempt_at": next_attempt_at,
                    "retry_delay_ms": retry_backoff.num_milliseconds()
                }),
            )
            .await?;
        }
        FinalDeliveryState::DeadLettered => {
            let error_message = delivery_error(result);
            let terminal_reason = if is_retryable_outcome(result.outcome.as_db_str())
                && delivery.attempt_count >= delivery.max_attempts
            {
                "attempts_exhausted"
            } else {
                "permanent_failure"
            };
            let update = sqlx::query(
                "UPDATE webhook_deliveries
                 SET status = 'dead_lettered',
                     next_attempt_at = NULL,
                     last_error = $4,
                     processing_started_at = NULL,
                     processing_lease_token = NULL,
                     processing_lease_expires_at = NULL,
                     current_attempt_id = NULL,
                     processing_worker_id = NULL,
                     queue_token = NULL,
                     final_state_at = NOW(),
                     updated_at = NOW()
                 WHERE delivery_id = $1
                   AND status = 'processing'
                   AND current_attempt_id = $2
                   AND processing_lease_token = $3::uuid",
            )
            .bind(delivery.delivery_id)
            .bind(delivery.attempt_id)
            .bind(&delivery.processing_lease_token)
            .bind(&error_message)
            .execute(&mut *tx)
            .await?;
            if update.rows_affected() == 0 {
                tx.rollback().await?;
                tracing::warn!(
                    delivery_id = delivery.delivery_id,
                    attempt_id = delivery.attempt_id,
                    "stale worker dead-letter transition ignored after guarded update matched zero rows"
                );
                return Ok(());
            }

            append_trace_in_tx(
                &mut tx,
                delivery.delivery_id,
                delivery.event_id,
                "dead_lettered",
                "dead_lettered",
                "Delivery dead-lettered",
                json!({
                    "attempt": delivery.attempt_count,
                    "max_attempts": delivery.max_attempts,
                    "outcome": result.outcome.as_db_str(),
                    "http_status": result.http_status,
                    "error": error_message,
                    "terminal_reason": terminal_reason
                }),
            )
            .await?;
        }
    }

    tx.commit().await?;
    Ok(())
}

fn decide_final_delivery_state(
    outcome: &str,
    attempt_count: i64,
    max_attempts: i64,
) -> FinalDeliveryState {
    if outcome == "success" {
        return FinalDeliveryState::Delivered;
    }

    if is_retryable_outcome(outcome) && attempt_count < max_attempts {
        return FinalDeliveryState::Retrying;
    }

    FinalDeliveryState::DeadLettered
}

fn is_retryable_outcome(outcome: &str) -> bool {
    matches!(
        outcome,
        "temporary_failure" | "timeout" | "abandoned" | "unknown"
    )
}

fn retry_delay(attempt_count: i64) -> chrono::Duration {
    let exponent = (attempt_count - 1).clamp(0, 6) as u32;
    let seconds = 5_i64.saturating_mul(2_i64.saturating_pow(exponent));
    chrono::Duration::seconds(seconds)
}

fn queue_publish_backoff(queue_attempt_count: i64) -> chrono::Duration {
    let exponent = (queue_attempt_count - 1).clamp(0, 6) as u32;
    let seconds = 5_i64
        .saturating_mul(2_i64.saturating_pow(exponent))
        .min(300);
    chrono::Duration::seconds(seconds)
}

fn delivery_error(result: &DeliveryResult) -> Option<String> {
    result
        .error_message
        .clone()
        .or_else(|| result.outcome.error_message())
        .or_else(|| match result.outcome {
            DeliveryOutcome::Success => None,
            _ => Some(result.outcome.as_db_str().to_string()),
        })
}

async fn append_trace_in_tx(
    tx: &mut Transaction<'_, Postgres>,
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
    .execute(&mut **tx)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn success_attempt_terminalizes_as_delivered() {
        assert_eq!(
            decide_final_delivery_state("success", 1, 5),
            FinalDeliveryState::Delivered
        );
    }

    #[test]
    fn retryable_attempt_with_remaining_budget_schedules_retry() {
        assert_eq!(
            decide_final_delivery_state("temporary_failure", 1, 5),
            FinalDeliveryState::Retrying
        );
    }

    #[test]
    fn max_attempts_one_abandoned_attempt_terminalizes() {
        assert_eq!(
            decide_final_delivery_state("abandoned", 1, 1),
            FinalDeliveryState::DeadLettered
        );
    }

    #[test]
    fn unknown_exhausted_attempt_terminalizes() {
        assert_eq!(
            decide_final_delivery_state("unknown", 3, 3),
            FinalDeliveryState::DeadLettered
        );
    }

    #[test]
    fn permanent_failure_terminalizes_even_with_remaining_budget() {
        assert_eq!(
            decide_final_delivery_state("permanent_failure", 1, 5),
            FinalDeliveryState::DeadLettered
        );
    }

    #[test]
    fn redirect_status_is_permanent_failure() {
        assert!(matches!(
            classify_status(302),
            DeliveryOutcome::PermanentFailure
        ));
        assert!(matches!(
            classify_status(307),
            DeliveryOutcome::PermanentFailure
        ));
    }

    #[test]
    fn sha256_hex_hashes_exact_body_bytes() {
        let body = br#"{"event_id":1,"delivery_id":2}"#;
        assert_eq!(
            sha256_hex(body),
            "c97e4f7c261e1fb5e7a8c0db118ebd23d822fd09a288a17733f4c4c16e4c8d50"
        );
    }

    #[test]
    fn successful_delivery_has_no_error_message() {
        let result = DeliveryResult {
            outcome: DeliveryOutcome::Success,
            http_status: Some(200),
            response_body_sample: None,
            error_message: None,
        };

        assert_eq!(delivery_error(&result), None);
    }

    #[test]
    fn http_client_builder_uses_redirect_policy_none() {
        assert!(build_http_client(Duration::from_millis(100)).is_some());
    }

    #[test]
    fn delivery_target_guard_blocks_literal_local_and_private_hosts() {
        for host in [
            "localhost",
            "127.0.0.1",
            "10.0.0.5",
            "172.16.0.1",
            "192.168.1.2",
            "169.254.169.254",
            "::1",
            "fc00::1",
            "fe80::1",
            "metadata.google.internal",
        ] {
            assert!(is_blocked_webhook_host(host), "{host} should be blocked");
        }
    }

    #[test]
    fn delivery_target_guard_allows_public_literal_hosts() {
        assert!(!is_blocked_webhook_host("93.184.216.34"));
        assert!(!is_blocked_webhook_host(
            "2606:2800:220:1:248:1893:25c8:1946"
        ));
    }

    #[test]
    fn processing_lease_duration_exceeds_request_timeout() {
        let timeout = Duration::from_millis(3_000);
        assert!(processing_lease_duration(timeout) > chrono::Duration::from_std(timeout).unwrap());
    }

    #[test]
    fn redis_queue_token_parser_accepts_uuid_text() {
        let token = "A0EebC99-9C0B-4EF8-BB6D-6BB9BD380A11";
        let value = redis::Value::BulkString(token.as_bytes().to_vec());

        assert_eq!(
            redis_value_to_uuid_text(&value).as_deref(),
            Some("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
        );
    }

    #[test]
    fn redis_queue_token_parser_rejects_missing_or_invalid_token() {
        assert!(redis_value_to_uuid_text(&redis::Value::Nil).is_none());
        assert!(
            redis_value_to_uuid_text(&redis::Value::BulkString(b"not-a-uuid".to_vec())).is_none()
        );
        assert!(redis_value_to_uuid_text(&redis::Value::Int(42)).is_none());
    }

    #[test]
    fn queue_publish_backoff_moves_retry_into_future_and_caps() {
        assert_eq!(queue_publish_backoff(1), chrono::Duration::seconds(5));
        assert_eq!(queue_publish_backoff(2), chrono::Duration::seconds(10));
        assert_eq!(queue_publish_backoff(3), chrono::Duration::seconds(20));
        assert_eq!(queue_publish_backoff(100), chrono::Duration::seconds(300));
    }
}
