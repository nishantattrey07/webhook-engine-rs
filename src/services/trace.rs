use sqlx::{PgPool, Postgres, Transaction};

use crate::{
    error::{AppError, AppResult},
    models::{
        DeliveryAttemptItem, DeliveryTraceItem, TraceGraphEdge, TraceGraphNode, TraceGraphResponse,
    },
};

pub(crate) async fn append_event_level_trace(
    tx: &mut Transaction<'_, Postgres>,
    event_id: i64,
    step: &str,
    title: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO delivery_trace_events (
            delivery_id,
            event_id,
            step,
            status,
            title,
            metadata_json,
            occurred_at
         )
         VALUES (NULL, $1, $2, 'succeeded', $3, '{}'::jsonb, NOW())",
    )
    .bind(event_id)
    .bind(step)
    .bind(title)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

pub(crate) async fn append_delivery_trace_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    delivery_id: i64,
    event_id: i64,
    step: &str,
    status: &str,
    title: &str,
    metadata: serde_json::Value,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO delivery_trace_events (
            delivery_id,
            event_id,
            step,
            status,
            title,
            metadata_json,
            occurred_at
         )
         VALUES ($1, $2, $3, $4, $5, $6, NOW())",
    )
    .bind(delivery_id)
    .bind(event_id)
    .bind(step)
    .bind(status)
    .bind(title)
    .bind(metadata)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

pub async fn list_delivery_attempts(
    pool: &PgPool,
    delivery_id: i64,
) -> AppResult<Vec<DeliveryAttemptItem>> {
    let rows = sqlx::query_as::<_, DeliveryAttemptItem>(
        "SELECT
            attempt_id,
            delivery_id,
            event_id,
            endpoint_id,
            attempt_count,
            http_status,
            outcome,
            error_message,
            response_body_sample,
            request_body_hash,
            started_at,
            completed_at,
            duration_ms
         FROM delivery_attempts
         WHERE delivery_id = $1
         ORDER BY attempt_count ASC",
    )
    .bind(delivery_id)
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

pub async fn list_delivery_trace(
    pool: &PgPool,
    delivery_id: i64,
) -> AppResult<Vec<DeliveryTraceItem>> {
    let event_id: i64 = sqlx::query_scalar(
        "SELECT event_id
         FROM webhook_deliveries
         WHERE delivery_id = $1",
    )
    .bind(delivery_id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| AppError::NotFound(format!("delivery {} not found", delivery_id)))?;

    let rows = sqlx::query_as::<_, DeliveryTraceItem>(
        "SELECT
            trace_id,
            delivery_id,
            event_id,
            step,
            status,
            title,
            detail,
            metadata_json,
            occurred_at,
            duration_ms
         FROM delivery_trace_events
         WHERE delivery_id = $1
            OR (delivery_id IS NULL AND event_id = $2)
         ORDER BY occurred_at ASC, trace_id ASC",
    )
    .bind(delivery_id)
    .bind(event_id)
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

pub async fn get_delivery_trace_graph(
    pool: &PgPool,
    delivery_id: i64,
) -> AppResult<TraceGraphResponse> {
    let trace = list_delivery_trace(pool, delivery_id).await?;
    Ok(build_trace_graph(trace))
}

pub(crate) fn build_trace_graph(trace: Vec<DeliveryTraceItem>) -> TraceGraphResponse {
    let nodes = trace
        .iter()
        .map(|item| TraceGraphNode {
            id: format!("trace-{}", item.trace_id),
            step: item.step.clone(),
            status: item.status.clone(),
            title: item.title.clone(),
            occurred_at: item.occurred_at,
            metadata_json: item.metadata_json.clone(),
        })
        .collect::<Vec<_>>();

    let edges = nodes
        .windows(2)
        .map(|window| TraceGraphEdge {
            id: format!("{}-{}", window[0].id, window[1].id),
            source: window[0].id.clone(),
            target: window[1].id.clone(),
        })
        .collect::<Vec<_>>();

    TraceGraphResponse { nodes, edges }
}
