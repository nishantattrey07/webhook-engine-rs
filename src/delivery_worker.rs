use std::time::Duration;

use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use redis::{aio::ConnectionManager, streams::StreamId};
use reqwest::Client;
use serde_json::{Value, json};
use sha2::Sha256;
use sqlx::{PgPool, Row};

#[doc(hidden)]
pub mod config;
#[doc(hidden)]
pub mod http_client;
mod postgres;
mod recovery;
mod redis_io;
#[doc(hidden)]
pub mod redis_support;
#[doc(hidden)]
pub mod retry_policy;
#[doc(hidden)]
pub mod signing;
#[doc(hidden)]
pub mod target;
mod trace;
mod types;

pub use config::{DeliveryTransport, WorkerConfig};
use http_client::{build_http_client, processing_lease_duration};
use postgres::{claim_due_deliveries, claim_locked_delivery};
use recovery::{recover_stale_queued, recover_stuck_processing};
use redis_io::{
    ack_message, autoclaim_pending_messages, connect_redis, ensure_consumer_group,
    publish_delivery, read_delivery_messages, trim_redis_stream_if_configured,
};
use redis_support::{redis_value_to_i64, redis_value_to_uuid_text};
use retry_policy::{
    DeliveryOutcome, FinalDeliveryState, classify_status, decide_final_delivery_state,
    is_retryable_outcome, queue_publish_backoff, retry_delay,
};
use signing::sha256_hex;
use target::validate_delivery_target;
use trace::{append_trace, append_trace_in_tx};
use types::{
    ClaimableDelivery, ClaimedDelivery, DeliveryResult, DeliveryTaskResult, QueuedDelivery,
    RedisDeliveryJob, RedisDeliveryTaskResult, WebhookSignature, WorkerError,
};

type HmacSha256 = Hmac<Sha256>;

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
