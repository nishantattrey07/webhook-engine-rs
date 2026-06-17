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
    pub enabled: Option<bool>,
    pub description: Option<String>,
    pub max_attempts: Option<i64>,
    pub subscribed_events: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpdateEndpointRequest {
    pub url: Option<String>,
    pub enabled: Option<bool>,
    pub description: Option<String>,
    pub max_attempts: Option<i64>,
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
    pub max_attempts: i64,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub subscribed_events: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct EndpointHealthSummary {
    pub total_deliveries: i64,
    pub delivered_deliveries: i64,
    pub retrying_deliveries: i64,
    pub dead_lettered_deliveries: i64,
    pub last_delivery_at: Option<DateTime<Utc>>,
    pub last_delivery_status: Option<String>,
    pub last_http_status: Option<i16>,
    pub success_rate: Option<f64>,
    pub p95_latency_ms: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct EndpointDetailResponse {
    pub endpoint_id: i64,
    pub merchant_id: i64,
    pub url: String,
    pub description: Option<String>,
    pub enabled: bool,
    pub max_attempts: i64,
    pub active_secret_version_id: Option<i64>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub subscribed_events: Vec<String>,
    pub delivery_health: EndpointHealthSummary,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EndpointDeliveriesQuery {
    pub status: Option<String>,
    pub cursor: Option<i64>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct EndpointDeliveryItem {
    pub delivery_id: i64,
    pub event_id: i64,
    pub event_type: String,
    pub status: String,
    pub attempt_count: i64,
    pub max_attempts: i64,
    pub last_http_status: Option<i16>,
    pub last_outcome: Option<String>,
    pub duration_ms: Option<i64>,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PaginatedEndpointDeliveriesResponse {
    pub items: Vec<EndpointDeliveryItem>,
    pub next_cursor: Option<i64>,
    pub limit: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TestEndpointRequest {
    pub event_type: Option<String>,
    pub requested_by: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TestEndpointResponse {
    pub payment_id: i64,
    pub event_id: i64,
    pub delivery_ids: Vec<i64>,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct EventListItem {
    pub event_id: i64,
    pub merchant_id: i64,
    pub object_type: String,
    pub object_id: i64,
    pub event_type: String,
    pub created_at: DateTime<Utc>,
    pub scenario_id: Option<i64>,
    pub delivery_count: i64,
    pub pending_count: i64,
    pub queued_count: i64,
    pub processing_count: i64,
    pub retrying_count: i64,
    pub delivered_count: i64,
    pub dead_lettered_count: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EventListQuery {
    pub merchant_id: Option<i64>,
    pub event_type: Option<String>,
    pub scenario_id: Option<i64>,
    pub search: Option<String>,
    pub cursor: Option<i64>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PaginatedEventsResponse {
    pub items: Vec<EventListItem>,
    pub next_cursor: Option<i64>,
    pub limit: i64,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct DeliveryListItem {
    pub delivery_id: i64,
    pub event_id: i64,
    pub event_type: String,
    pub merchant_id: i64,
    pub endpoint_id: i64,
    pub endpoint_url: String,
    pub status: String,
    pub attempt_count: i64,
    pub max_attempts: i64,
    pub last_http_status: Option<i16>,
    pub last_outcome: Option<String>,
    pub duration_ms: Option<i64>,
    pub last_error: Option<String>,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub scenario_id: Option<i64>,
    pub operator_resolved_at: Option<DateTime<Utc>>,
    pub operator_resolved_by: Option<String>,
    pub operator_resolution_note: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
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
    pub request_body_hash: Option<String>,
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
    pub deliveries: Vec<EventFanoutDeliveryItem>,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct EventFanoutDeliveryItem {
    pub delivery_id: i64,
    pub event_id: i64,
    pub event_type: String,
    pub merchant_id: i64,
    pub endpoint_id: i64,
    pub endpoint_url: String,
    pub status: String,
    pub attempt_count: i64,
    pub max_attempts: i64,
    pub last_http_status: Option<i16>,
    pub last_outcome: Option<String>,
    pub duration_ms: Option<i64>,
    pub last_error: Option<String>,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub scenario_id: Option<i64>,
    pub operator_resolved_at: Option<DateTime<Utc>>,
    pub operator_resolved_by: Option<String>,
    pub operator_resolution_note: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub endpoint_description: Option<String>,
    pub endpoint_enabled: bool,
    pub endpoint_subscribed_events: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RetryDeliveryRequest {
    pub reason: Option<String>,
    pub requested_by: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RetryDeliveryResponse {
    pub success: bool,
    pub created: bool,
    pub already_active_retry: bool,
    pub original_delivery_id: i64,
    pub new_delivery_id: i64,
    pub event_id: i64,
    pub event_type: String,
    pub merchant_id: i64,
    pub endpoint_id: i64,
    pub endpoint_url: String,
    pub new_delivery_status: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BulkRetryDeliveriesRequest {
    pub delivery_ids: Vec<i64>,
    pub reason: Option<String>,
    pub requested_by: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BulkRetryDeliveriesResponse {
    pub queued_count: usize,
    pub failed_ids: Vec<i64>,
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

#[derive(Debug, Clone, Deserialize)]
pub struct ResolveDeliveryRequest {
    pub resolved_by: Option<String>,
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResolveDeliveryResponse {
    pub success: bool,
    pub delivery_id: i64,
    pub operator_resolved_at: DateTime<Utc>,
    pub operator_resolved_by: Option<String>,
    pub operator_resolution_note: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct UnresolveDeliveryResponse {
    pub success: bool,
    pub delivery_id: i64,
    pub operator_resolved_at: Option<DateTime<Utc>>,
    pub operator_resolved_by: Option<String>,
    pub operator_resolution_note: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DeliveryListQuery {
    pub merchant_id: Option<i64>,
    pub endpoint_id: Option<i64>,
    pub event_id: Option<i64>,
    pub event_type: Option<String>,
    pub scenario_id: Option<i64>,
    pub endpoint: Option<String>,
    pub status: Option<String>,
    pub resolution: Option<String>,
    pub search: Option<String>,
    pub time_range: Option<String>,
    pub cursor: Option<i64>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PaginatedDeliveriesResponse {
    pub items: Vec<DeliveryListItem>,
    pub next_cursor: Option<i64>,
    pub limit: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct DashboardSummary {
    pub total_events: i64,
    pub total_deliveries: i64,
    pub pending_deliveries: i64,
    pub queued_deliveries: i64,
    pub processing_deliveries: i64,
    pub retrying_deliveries: i64,
    pub delivered_deliveries: i64,
    pub dead_lettered_deliveries: i64,
    pub total_endpoints: i64,
    pub enabled_endpoints: i64,
    pub attempts_24h: i64,
    pub failed_attempts_24h: i64,
    pub success_rate: Option<f64>,
    pub active_deliveries: i64,
    pub queued_count: i64,
    pub processing_count: i64,
    pub retrying_count: i64,
    pub dead_letter_count: i64,
    pub queue_depth: i64,
    pub redis_pending: Option<i64>,
    pub p95_latency_ms: Option<f64>,
    pub active_workers: Option<i64>,
    pub retry_backlog: RetryBacklogSummary,
}

#[derive(Debug, Clone, Serialize)]
pub struct RetryBacklogSummary {
    pub due_now: i64,
    pub due_0_to_5_min: i64,
    pub due_5_to_15_min: i64,
    pub due_15_min_plus: i64,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct EndpointStatsItem {
    pub endpoint_id: i64,
    pub merchant_id: i64,
    pub url: String,
    pub enabled: bool,
    pub description: Option<String>,
    pub total_deliveries: i64,
    pub delivered_deliveries: i64,
    pub retrying_deliveries: i64,
    pub dead_lettered_deliveries: i64,
    pub last_delivery_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScenarioCatalogItem {
    pub scenario_key: String,
    pub label: String,
    pub description: String,
    pub category: String,
    pub scenario_kind: String,
    pub config_knobs: Vec<String>,
    pub allows_receiver_behavior: bool,
    pub requires_receiver_behavior: bool,
    pub aliases: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReceiverBehaviorOption {
    pub receiver_behavior: String,
    pub label: String,
    pub description: String,
    pub aliases: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ScenarioRunRequest {
    pub scenario_key: String,
    pub requested_by: Option<String>,
    pub config: Option<ScenarioRunConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct ScenarioRunConfig {
    pub merchant_id: Option<i64>,
    pub payment_count: Option<i64>,
    pub event_type: Option<String>,
    pub endpoint_count: Option<i64>,
    pub max_attempts: Option<i64>,
    pub receiver_behavior: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScenarioRunResponse {
    pub scenario_id: i64,
    pub scenario_key: String,
    pub status: String,
    pub started_at: DateTime<Utc>,
    pub merchant_id: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScenarioSummary {
    pub payments_created: i64,
    pub events_created: i64,
    pub deliveries_created: i64,
    pub delivered_count: i64,
    pub retrying_count: i64,
    pub dead_lettered_count: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScenarioArtifacts {
    pub payment_ids: Vec<i64>,
    pub event_ids: Vec<i64>,
    pub delivery_ids: Vec<i64>,
    pub endpoint_ids: Vec<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScenarioPlannedExpectations {
    pub payment_count: i64,
    pub event_count: i64,
    pub endpoint_count: i64,
    pub delivery_count: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScenarioPlannedEndpoint {
    pub ordinal: i64,
    pub endpoint_role: Option<String>,
    pub endpoint_key: String,
    pub behavior_key: String,
    pub behavior: Value,
    pub base_delay_ms: i64,
    pub max_attempts: i64,
    pub created_endpoint_id: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScenarioPlanDetail {
    pub expectations: ScenarioPlannedExpectations,
    pub endpoints: Vec<ScenarioPlannedEndpoint>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScenarioDetailResponse {
    pub scenario_id: i64,
    pub scenario_key: String,
    pub status: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub merchant_id: i64,
    pub requested_by: Option<String>,
    pub summary: ScenarioSummary,
    pub artifacts: ScenarioArtifacts,
    pub planned: Option<ScenarioPlanDetail>,
    pub receiver_config: Value,
    pub step_log: Value,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeliveryRetryLineage {
    pub original_delivery_id: Option<i64>,
    pub retry_delivery_ids: Vec<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeliveryDetailResponse {
    pub delivery: DeliveryListItem,
    pub event: EventListItem,
    pub attempts: Vec<DeliveryAttemptItem>,
    pub trace: Vec<DeliveryTraceItem>,
    pub retry_lineage: DeliveryRetryLineage,
}

#[derive(Debug, Clone, Serialize)]
pub struct TraceGraphResponse {
    pub nodes: Vec<TraceGraphNode>,
    pub edges: Vec<TraceGraphEdge>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TraceGraphNode {
    pub id: String,
    pub step: String,
    pub status: String,
    pub title: String,
    pub occurred_at: DateTime<Utc>,
    pub metadata_json: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct TraceGraphEdge {
    pub id: String,
    pub source: String,
    pub target: String,
}
