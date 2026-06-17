use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{
        HeaderMap, HeaderValue, Method,
        header::{AUTHORIZATION, CONTENT_TYPE},
    },
    routing::{get, post},
};
use sqlx::PgPool;
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};

use crate::{
    error::{AppError, AppResult},
    models::{CreatePaymentRequest, HealthResponse},
    services,
};

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub mock_receiver_base_url: String,
    pub cors_allowed_origins: Vec<String>,
    pub cors_allow_any_origin: bool,
    pub admin_api_key: Option<String>,
}

pub fn router(state: AppState) -> Router {
    let cors = cors_layer(&state);

    Router::new()
        .route("/health", get(health))
        .route("/api/health", get(health))
        .route("/api/endpoints", get(list_endpoints).post(create_endpoint))
        .route(
            "/api/endpoints/:endpoint_id",
            get(get_endpoint_detail).patch(update_endpoint),
        )
        .route(
            "/api/endpoints/:endpoint_id/deliveries",
            get(list_endpoint_deliveries),
        )
        .route("/api/endpoints/:endpoint_id/test", post(test_endpoint))
        .route("/api/payments", post(create_payment))
        .route("/api/payments/bulk", post(create_bulk_payments))
        .route("/api/dashboard/summary", get(get_dashboard_summary))
        .route("/api/endpoints/stats", get(list_endpoint_stats))
        .route("/api/event-types", get(list_event_types))
        .route("/api/receiver-behaviors", get(list_receiver_behaviors))
        .route("/api/events", get(list_events))
        .route("/api/events/:event_id", get(get_event))
        .route("/api/events/:event_id/fanout", get(get_event_fanout))
        .route("/api/scenarios", get(list_scenarios))
        .route("/api/scenarios/history", get(list_scenario_history))
        .route("/api/scenarios/run", post(run_scenario))
        .route("/api/scenarios/:scenario_id", get(get_scenario))
        .route("/api/deliveries", get(list_deliveries))
        .route("/api/deliveries/bulk-retry", post(bulk_retry_deliveries))
        .route("/api/deliveries/:delivery_id", get(get_delivery))
        .route(
            "/api/deliveries/:delivery_id/detail",
            get(get_delivery_detail),
        )
        .route("/api/deliveries/:delivery_id/retry", post(retry_delivery))
        .route(
            "/api/deliveries/:delivery_id/resolve",
            post(resolve_delivery),
        )
        .route(
            "/api/deliveries/:delivery_id/unresolve",
            post(unresolve_delivery),
        )
        .route(
            "/api/deliveries/:delivery_id/attempts",
            get(list_delivery_attempts),
        )
        .route(
            "/api/deliveries/:delivery_id/trace",
            get(list_delivery_trace),
        )
        .route(
            "/api/deliveries/:delivery_id/trace-graph",
            get(get_delivery_trace_graph),
        )
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .with_state(state)
}

fn cors_layer(state: &AppState) -> CorsLayer {
    let layer = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST, Method::PATCH, Method::OPTIONS])
        .allow_headers([CONTENT_TYPE, AUTHORIZATION]);

    if state.cors_allow_any_origin {
        layer.allow_origin(Any)
    } else {
        let origins = state
            .cors_allowed_origins
            .iter()
            .filter_map(|origin| match HeaderValue::from_str(origin) {
                Ok(origin) => Some(origin),
                Err(error) => {
                    tracing::warn!(%error, origin, "ignoring invalid CORS origin");
                    None
                }
            })
            .collect::<Vec<_>>();

        layer.allow_origin(origins)
    }
}

fn ensure_admin_authorized(state: &AppState, headers: &HeaderMap) -> AppResult<()> {
    let Some(expected_key) = state.admin_api_key.as_deref() else {
        return Ok(());
    };

    let Some(header_value) = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    else {
        return Err(AppError::Unauthorized(
            "missing Authorization bearer token".to_string(),
        ));
    };

    let Some(actual_key) = header_value.strip_prefix("Bearer ") else {
        return Err(AppError::Unauthorized(
            "Authorization must use Bearer token".to_string(),
        ));
    };

    if actual_key == expected_key {
        Ok(())
    } else {
        Err(AppError::Unauthorized(
            "invalid Authorization bearer token".to_string(),
        ))
    }
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

async fn create_payment(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreatePaymentRequest>,
) -> AppResult<Json<crate::models::CreatePaymentResponse>> {
    ensure_admin_authorized(&state, &headers)?;
    let response = services::create_payment(&state.pool, request).await?;
    Ok(Json(response))
}

async fn create_bulk_payments(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<crate::models::CreateBulkPaymentsRequest>,
) -> AppResult<Json<crate::models::CreateBulkPaymentsResponse>> {
    ensure_admin_authorized(&state, &headers)?;
    let response = services::create_bulk_payments(&state.pool, request).await?;
    Ok(Json(response))
}

async fn create_endpoint(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<crate::models::CreateEndpointRequest>,
) -> AppResult<Json<crate::models::CreateEndpointResponse>> {
    ensure_admin_authorized(&state, &headers)?;
    let response = services::create_endpoint(&state.pool, request).await?;
    Ok(Json(response))
}

async fn list_endpoints(
    State(state): State<AppState>,
) -> AppResult<Json<Vec<crate::models::EndpointListItem>>> {
    let endpoints = services::list_endpoints(&state.pool).await?;
    Ok(Json(endpoints))
}

async fn get_endpoint_detail(
    State(state): State<AppState>,
    Path(endpoint_id): Path<i64>,
) -> AppResult<Json<crate::models::EndpointDetailResponse>> {
    let endpoint = services::get_endpoint_detail(&state.pool, endpoint_id).await?;
    Ok(Json(endpoint))
}

async fn update_endpoint(
    State(state): State<AppState>,
    Path(endpoint_id): Path<i64>,
    headers: HeaderMap,
    Json(request): Json<crate::models::UpdateEndpointRequest>,
) -> AppResult<Json<crate::models::EndpointListItem>> {
    ensure_admin_authorized(&state, &headers)?;
    let endpoint = services::update_endpoint(&state.pool, endpoint_id, request).await?;
    Ok(Json(endpoint))
}

async fn list_endpoint_deliveries(
    State(state): State<AppState>,
    Path(endpoint_id): Path<i64>,
    Query(query): Query<crate::models::EndpointDeliveriesQuery>,
) -> AppResult<Json<crate::models::PaginatedEndpointDeliveriesResponse>> {
    let deliveries = services::list_endpoint_deliveries(&state.pool, endpoint_id, query).await?;
    Ok(Json(deliveries))
}

async fn test_endpoint(
    State(state): State<AppState>,
    Path(endpoint_id): Path<i64>,
    headers: HeaderMap,
    Json(request): Json<crate::models::TestEndpointRequest>,
) -> AppResult<Json<crate::models::TestEndpointResponse>> {
    ensure_admin_authorized(&state, &headers)?;
    let response = services::test_endpoint(&state.pool, endpoint_id, request).await?;
    Ok(Json(response))
}

async fn list_endpoint_stats(
    State(state): State<AppState>,
) -> AppResult<Json<Vec<crate::models::EndpointStatsItem>>> {
    let stats = services::list_endpoint_stats(&state.pool).await?;
    Ok(Json(stats))
}

async fn list_event_types() -> Json<Vec<&'static str>> {
    Json(services::allowed_event_types())
}

async fn list_receiver_behaviors() -> Json<Vec<crate::models::ReceiverBehaviorOption>> {
    Json(services::receiver_behavior_catalog())
}

async fn list_scenarios() -> Json<Vec<crate::models::ScenarioCatalogItem>> {
    Json(services::scenario_catalog())
}

async fn list_scenario_history(
    State(state): State<AppState>,
    Query(query): Query<crate::models::ScenarioHistoryQuery>,
) -> AppResult<Json<crate::models::PaginatedScenarioHistoryResponse>> {
    let response = services::list_scenario_history(&state.pool, query).await?;
    Ok(Json(response))
}

async fn run_scenario(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<crate::models::ScenarioRunRequest>,
) -> AppResult<Json<crate::models::ScenarioRunResponse>> {
    ensure_admin_authorized(&state, &headers)?;
    let response =
        services::run_scenario(&state.pool, &state.mock_receiver_base_url, request).await?;
    Ok(Json(response))
}

async fn get_scenario(
    State(state): State<AppState>,
    Path(scenario_id): Path<i64>,
) -> AppResult<Json<crate::models::ScenarioDetailResponse>> {
    let scenario = services::get_scenario(&state.pool, scenario_id).await?;
    Ok(Json(scenario))
}

async fn get_dashboard_summary(
    State(state): State<AppState>,
) -> AppResult<Json<crate::models::DashboardSummary>> {
    let summary = services::get_dashboard_summary(&state.pool).await?;
    Ok(Json(summary))
}

async fn list_events(
    State(state): State<AppState>,
    Query(query): Query<crate::models::EventListQuery>,
) -> AppResult<Json<crate::models::PaginatedEventsResponse>> {
    let events = services::list_events(&state.pool, query).await?;
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
    Query(query): Query<crate::models::DeliveryListQuery>,
) -> AppResult<Json<crate::models::PaginatedDeliveriesResponse>> {
    let deliveries = services::list_deliveries(&state.pool, query).await?;
    Ok(Json(deliveries))
}

async fn get_delivery(
    State(state): State<AppState>,
    Path(delivery_id): Path<i64>,
) -> AppResult<Json<crate::models::DeliveryListItem>> {
    let delivery = services::get_delivery(&state.pool, delivery_id).await?;
    Ok(Json(delivery))
}

async fn get_delivery_detail(
    State(state): State<AppState>,
    Path(delivery_id): Path<i64>,
) -> AppResult<Json<crate::models::DeliveryDetailResponse>> {
    let detail = services::get_delivery_detail(&state.pool, delivery_id).await?;
    Ok(Json(detail))
}

async fn retry_delivery(
    State(state): State<AppState>,
    Path(delivery_id): Path<i64>,
    headers: HeaderMap,
    Json(request): Json<crate::models::RetryDeliveryRequest>,
) -> AppResult<Json<crate::models::RetryDeliveryResponse>> {
    ensure_admin_authorized(&state, &headers)?;
    let response = services::retry_delivery(&state.pool, delivery_id, request).await?;
    Ok(Json(response))
}

async fn bulk_retry_deliveries(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<crate::models::BulkRetryDeliveriesRequest>,
) -> AppResult<Json<crate::models::BulkRetryDeliveriesResponse>> {
    ensure_admin_authorized(&state, &headers)?;
    let response = services::bulk_retry_deliveries(&state.pool, request).await?;
    Ok(Json(response))
}

async fn resolve_delivery(
    State(state): State<AppState>,
    Path(delivery_id): Path<i64>,
    headers: HeaderMap,
    Json(request): Json<crate::models::ResolveDeliveryRequest>,
) -> AppResult<Json<crate::models::ResolveDeliveryResponse>> {
    ensure_admin_authorized(&state, &headers)?;
    let response = services::resolve_delivery(&state.pool, delivery_id, request).await?;
    Ok(Json(response))
}

async fn unresolve_delivery(
    State(state): State<AppState>,
    Path(delivery_id): Path<i64>,
    headers: HeaderMap,
) -> AppResult<Json<crate::models::UnresolveDeliveryResponse>> {
    ensure_admin_authorized(&state, &headers)?;
    let response = services::unresolve_delivery(&state.pool, delivery_id).await?;
    Ok(Json(response))
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

async fn get_delivery_trace_graph(
    State(state): State<AppState>,
    Path(delivery_id): Path<i64>,
) -> AppResult<Json<crate::models::TraceGraphResponse>> {
    let graph = services::get_delivery_trace_graph(&state.pool, delivery_id).await?;
    Ok(Json(graph))
}
