use redis::{
    AsyncCommands,
    aio::ConnectionManager,
    streams::{StreamAutoClaimOptions, StreamAutoClaimReply, StreamReadReply},
};

use super::{config::WorkerConfig, types::QueuedDelivery};

pub(super) async fn connect_redis(redis_url: &str) -> Option<ConnectionManager> {
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

pub(super) async fn ensure_consumer_group(
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

pub(super) async fn publish_delivery(
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

pub(super) async fn trim_redis_stream_if_configured(
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

pub(super) async fn read_delivery_messages(
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

pub(super) async fn autoclaim_pending_messages(
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

pub(super) async fn ack_message(
    redis: &mut ConnectionManager,
    stream: &str,
    group: &str,
    message_id: &str,
) -> redis::RedisResult<()> {
    let _: i64 = redis.xack(stream, group, &[message_id]).await?;
    Ok(())
}
