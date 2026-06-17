use std::time::Duration;

use reqwest::Client;
use sqlx::PgPool;

#[doc(hidden)]
pub mod config;
mod consumer;
mod delivery_processing;
mod finalization;
#[doc(hidden)]
pub mod http_client;
mod postgres;
mod recovery;
mod redis_io;
#[doc(hidden)]
pub mod redis_support;
mod relay;
#[doc(hidden)]
pub mod retry_policy;
#[doc(hidden)]
pub mod signing;
#[doc(hidden)]
pub mod target;
mod trace;
mod types;

pub use config::{DeliveryTransport, WorkerConfig};
use consumer::spawn_redis_worker;
use delivery_processing::process_delivery;
use http_client::{build_http_client, processing_lease_duration};
use postgres::claim_due_deliveries;
use recovery::recover_stuck_processing;
use relay::spawn_redis_relay;
use types::{ClaimedDelivery, DeliveryTaskResult};

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
