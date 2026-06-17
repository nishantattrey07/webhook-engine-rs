use redis::{aio::ConnectionManager, streams::StreamId};
use reqwest::Client;
use sqlx::PgPool;

use super::{
    config::WorkerConfig,
    delivery_processing::process_delivery,
    http_client::{build_http_client, processing_lease_duration},
    postgres::claim_locked_delivery,
    recovery::recover_stuck_processing,
    redis_io::{
        ack_message, autoclaim_pending_messages, connect_redis, ensure_consumer_group,
        read_delivery_messages,
    },
    redis_support::{redis_value_to_i64, redis_value_to_uuid_text},
    types::{
        ClaimableDelivery, ClaimedDelivery, RedisDeliveryJob, RedisDeliveryTaskResult, WorkerError,
    },
};

pub(super) fn spawn_redis_worker(pool: PgPool, config: WorkerConfig, redis_url: String) {
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
