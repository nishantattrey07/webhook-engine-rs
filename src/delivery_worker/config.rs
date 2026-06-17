use std::time::Duration;

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
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "postgres" | "db" => Some(Self::Postgres),
            "redis" | "redis_stream" | "redis-stream" => Some(Self::Redis),
            _ => None,
        }
    }
}
