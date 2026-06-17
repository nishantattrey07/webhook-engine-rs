use redis::RedisError;

use super::retry_policy::DeliveryOutcome;

pub(super) type DeliveryTaskResult = Result<(i64, Result<(), sqlx::Error>), tokio::task::JoinError>;
pub(super) type RedisDeliveryTaskResult =
    Result<(String, i64, Result<(), sqlx::Error>), tokio::task::JoinError>;

#[derive(Debug, Clone, sqlx::FromRow)]
pub(super) struct ClaimedDelivery {
    pub(super) delivery_id: i64,
    pub(super) event_id: i64,
    pub(super) endpoint_id: i64,
    pub(super) merchant_id: i64,
    pub(super) endpoint_url: String,
    pub(super) secret_version_id: Option<i64>,
    pub(super) max_attempts: i64,
    pub(super) attempt_id: i64,
    pub(super) attempt_count: i64,
    pub(super) processing_lease_token: String,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub(super) struct ClaimableDelivery {
    pub(super) delivery_id: i64,
    pub(super) event_id: i64,
    pub(super) endpoint_id: i64,
    pub(super) merchant_id: i64,
    pub(super) endpoint_url: String,
    pub(super) secret_version_id: Option<i64>,
    pub(super) max_attempts: i64,
}

#[derive(Debug, Clone)]
pub(super) struct WebhookSignature {
    pub(super) timestamp: String,
    pub(super) signature: String,
    pub(super) key_version: Option<i64>,
}

#[derive(Debug, Clone)]
pub(super) struct RedisDeliveryJob {
    pub(super) message_id: String,
    pub(super) delivery: ClaimedDelivery,
}

#[derive(Debug, Clone)]
pub(super) struct QueuedDelivery {
    pub(super) delivery_id: i64,
    pub(super) queue_token: String,
}

#[derive(Debug, Clone)]
pub(super) struct DeliveryResult {
    pub(super) outcome: DeliveryOutcome,
    pub(super) http_status: Option<u16>,
    pub(super) response_body_sample: Option<String>,
    pub(super) error_message: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum WorkerError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Redis(#[from] RedisError),
}
