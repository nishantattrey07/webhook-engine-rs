use axum::{
    Json, Router,
    extract::{Path, State},
    routing::{get, patch, post},
};
use sqlx::PgPool;
use tower_http::{cors::CorsLayer, trace::TraceLayer};

use crate::{
    error::AppResult,
    models::{CreatePaymentRequest, HealthResponse},
    services,
};

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/api/health", get(health))
        .route("/api/endpoints", get(list_endpoints).post(create_endpoint))
        .route("/api/endpoints/:endpoint_id", patch(update_endpoint))
        .route("/api/payments", post(create_payment))
        .route("/api/payments/bulk", post(create_bulk_payments))
        .route("/api/events", get(list_events))
        .route("/api/events/:event_id", get(get_event))
        .route("/api/events/:event_id/fanout", get(get_event_fanout))
        .route("/api/deliveries", get(list_deliveries))
        .route("/api/deliveries/:delivery_id", get(get_delivery))
        .route(
            "/api/deliveries/:delivery_id/attempts",
            get(list_delivery_attempts),
        )
        .route(
            "/api/deliveries/:delivery_id/trace",
            get(list_delivery_trace),
        )
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state)
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

async fn create_payment(
    State(state): State<AppState>,
    Json(request): Json<CreatePaymentRequest>,
) -> AppResult<Json<crate::models::CreatePaymentResponse>> {
    let response = services::create_payment(&state.pool, request).await?;
    Ok(Json(response))
}

async fn create_bulk_payments(
    State(state): State<AppState>,
    Json(request): Json<crate::models::CreateBulkPaymentsRequest>,
) -> AppResult<Json<crate::models::CreateBulkPaymentsResponse>> {
    let response = services::create_bulk_payments(&state.pool, request).await?;
    Ok(Json(response))
}

async fn create_endpoint(
    State(state): State<AppState>,
    Json(request): Json<crate::models::CreateEndpointRequest>,
) -> AppResult<Json<crate::models::CreateEndpointResponse>> {
    let response = services::create_endpoint(&state.pool, request).await?;
    Ok(Json(response))
}

async fn list_endpoints(
    State(state): State<AppState>,
) -> AppResult<Json<Vec<crate::models::EndpointListItem>>> {
    let endpoints = services::list_endpoints(&state.pool).await?;
    Ok(Json(endpoints))
}

async fn update_endpoint(
    State(state): State<AppState>,
    Path(endpoint_id): Path<i64>,
    Json(request): Json<crate::models::UpdateEndpointRequest>,
) -> AppResult<Json<crate::models::EndpointListItem>> {
    let endpoint = services::update_endpoint(&state.pool, endpoint_id, request).await?;
    Ok(Json(endpoint))
}

async fn list_events(
    State(state): State<AppState>,
) -> AppResult<Json<Vec<crate::models::EventListItem>>> {
    let events = services::list_events(&state.pool).await?;
    Ok(Json(events))
}

async fn get_event(
    State(state): State<AppState>,
    Path(event_id): Path<i64>,
) -> AppResult<Json<crate::models::EventListItem>> {
    let event = services::get_event(&state.pool, event_id).await?;
    Ok(Json(event))
}

async fn get_event_fanout(
    State(state): State<AppState>,
    Path(event_id): Path<i64>,
) -> AppResult<Json<crate::models::EventFanoutResponse>> {
    let fanout = services::get_event_fanout(&state.pool, event_id).await?;
    Ok(Json(fanout))
}

async fn list_deliveries(
    State(state): State<AppState>,
) -> AppResult<Json<Vec<crate::models::DeliveryListItem>>> {
    let deliveries = services::list_deliveries(&state.pool).await?;
    Ok(Json(deliveries))
}

async fn get_delivery(
    State(state): State<AppState>,
    Path(delivery_id): Path<i64>,
) -> AppResult<Json<crate::models::DeliveryListItem>> {
    let delivery = services::get_delivery(&state.pool, delivery_id).await?;
    Ok(Json(delivery))
}

async fn list_delivery_attempts(
    State(state): State<AppState>,
    Path(delivery_id): Path<i64>,
) -> AppResult<Json<Vec<crate::models::DeliveryAttemptItem>>> {
    let attempts = services::list_delivery_attempts(&state.pool, delivery_id).await?;
    Ok(Json(attempts))
}

async fn list_delivery_trace(
    State(state): State<AppState>,
    Path(delivery_id): Path<i64>,
) -> AppResult<Json<Vec<crate::models::DeliveryTraceItem>>> {
    let trace = services::list_delivery_trace(&state.pool, delivery_id).await?;
    Ok(Json(trace))
}
