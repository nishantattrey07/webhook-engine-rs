use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Deserialize)]
pub struct CreatePaymentRequest {
    pub merchant_id: i64,
    pub order_id: i64,
    pub amount: i64,
    pub status: PaymentStatusInput,
    pub mode_of_payment: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaymentStatusInput {
    Succeeded,
    Failed,
    Refunded,
}

impl PaymentStatusInput {
    pub fn as_db_str(&self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Refunded => "refunded",
        }
    }

    pub fn event_type(&self) -> &'static str {
        match self {
            Self::Succeeded => "payment_succeeded",
            Self::Failed => "payment_failed",
            Self::Refunded => "payment_refund",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct CreatePaymentResponse {
    pub payment_id: i64,
    pub event_id: i64,
    pub delivery_count: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateBulkPaymentsRequest {
    pub payments: Vec<CreatePaymentRequest>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CreateBulkPaymentsResponse {
    pub created: Vec<CreatePaymentResponse>,
    pub total_payments: usize,
    pub total_deliveries: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateEndpointRequest {
    pub merchant_id: i64,
    pub url: String,
    pub secret: String,
    pub description: Option<String>,
    pub subscribed_events: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpdateEndpointRequest {
    pub url: Option<String>,
    pub enabled: Option<bool>,
    pub description: Option<String>,
    pub subscribed_events: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CreateEndpointResponse {
    pub endpoint_id: i64,
    pub secret_version_id: i64,
    pub subscription_count: i64,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct EndpointListItem {
    pub endpoint_id: i64,
    pub merchant_id: i64,
    pub url: String,
    pub active_secret_version_id: Option<i64>,
    pub enabled: bool,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub subscribed_events: Vec<String>,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct EventListItem {
    pub event_id: i64,
    pub merchant_id: i64,
    pub object_type: String,
    pub object_id: i64,
    pub event_type: String,
    pub event_snapshot_json: Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct DeliveryListItem {
    pub delivery_id: i64,
    pub event_id: i64,
    pub merchant_id: i64,
    pub endpoint_id: i64,
    pub endpoint_url: String,
    pub status: String,
    pub max_attempts: i64,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub final_state_at: Option<DateTime<Utc>>,
    pub attempt_count: i64,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct DeliveryAttemptItem {
    pub attempt_id: i64,
    pub delivery_id: Option<i64>,
    pub event_id: i64,
    pub endpoint_id: Option<i64>,
    pub attempt_count: i64,
    pub http_status: Option<i16>,
    pub outcome: String,
    pub error_message: Option<String>,
    pub response_body_sample: Option<String>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub duration_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct DeliveryTraceItem {
    pub trace_id: i64,
    pub delivery_id: Option<i64>,
    pub event_id: i64,
    pub step: String,
    pub status: String,
    pub title: String,
    pub detail: Option<String>,
    pub metadata_json: Value,
    pub occurred_at: DateTime<Utc>,
    pub duration_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct EventFanoutResponse {
    pub event: EventListItem,
    pub deliveries: Vec<DeliveryListItem>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RetryDeliveryRequest {
    pub reason: Option<String>,
    pub requested_by: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RetryDeliveryResponse {
    pub original_delivery_id: i64,
    pub new_delivery_id: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BulkRetryDeliveriesRequest {
    pub delivery_ids: Vec<i64>,
    pub reason: Option<String>,
    pub requested_by: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BulkRetryDeliveriesResponse {
    pub retried: Vec<RetryDeliveryResponse>,
    pub skipped: Vec<BulkRetrySkip>,
    pub total_requested: usize,
    pub total_retried: usize,
    pub total_skipped: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct BulkRetrySkip {
    pub delivery_id: i64,
    pub reason: String,
}
