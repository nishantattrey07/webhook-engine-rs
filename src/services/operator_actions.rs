use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx::{PgPool, Postgres, Row, Transaction};

use super::{append_delivery_trace_in_tx, normalize_optional_text};
use crate::{
    error::{AppError, AppResult},
    models::{
        BulkRetryDeliveriesRequest, BulkRetryDeliveriesResponse, BulkRetrySkip,
        ResolveDeliveryRequest, ResolveDeliveryResponse, RetryDeliveryRequest,
        RetryDeliveryResponse, UnresolveDeliveryResponse,
    },
};

pub async fn retry_delivery(
    pool: &PgPool,
    delivery_id: i64,
    request: RetryDeliveryRequest,
) -> AppResult<RetryDeliveryResponse> {
    let mut tx = pool.begin().await?;
    let response = retry_delivery_in_tx(
        &mut tx,
        delivery_id,
        request.reason.as_deref(),
        request.requested_by.as_deref(),
    )
    .await?;
    tx.commit().await?;

    Ok(response)
}

pub async fn bulk_retry_deliveries(
    pool: &PgPool,
    request: BulkRetryDeliveriesRequest,
) -> AppResult<BulkRetryDeliveriesResponse> {
    if request.delivery_ids.is_empty() {
        return Err(AppError::BadRequest(
            "at least one delivery id is required".to_string(),
        ));
    }

    if request.delivery_ids.len() > 100 {
        return Err(AppError::BadRequest(
            "bulk retry is limited to 100 deliveries".to_string(),
        ));
    }

    let total_requested = request.delivery_ids.len();
    let mut retried = Vec::new();
    let mut skipped = Vec::new();
    let mut tx = pool.begin().await?;

    for delivery_id in dedupe_delivery_ids(request.delivery_ids) {
        match retry_delivery_in_tx(
            &mut tx,
            delivery_id,
            request.reason.as_deref(),
            request.requested_by.as_deref(),
        )
        .await
        {
            Ok(response) => retried.push(response),
            Err(AppError::NotFound(reason)) | Err(AppError::BadRequest(reason)) => {
                skipped.push(BulkRetrySkip {
                    delivery_id,
                    reason,
                });
            }
            Err(error) => return Err(error),
        }
    }

    tx.commit().await?;

    let failed_ids = skipped.iter().map(|item| item.delivery_id).collect();

    Ok(BulkRetryDeliveriesResponse {
        queued_count: retried.len(),
        failed_ids,
        total_requested,
        total_retried: retried.len(),
        total_skipped: skipped.len(),
        retried,
        skipped,
    })
}

pub async fn resolve_delivery(
    pool: &PgPool,
    delivery_id: i64,
    request: ResolveDeliveryRequest,
) -> AppResult<ResolveDeliveryResponse> {
    let resolved_by = normalize_optional_text(request.resolved_by, "resolved_by", 200)?;
    let note = normalize_optional_text(request.note, "note", 2_000)?;
    let mut tx = pool.begin().await?;

    let row = sqlx::query(
        "WITH target AS (
            SELECT
                delivery_id,
                operator_resolved_at IS NOT NULL AS was_already_resolved
            FROM webhook_deliveries
            WHERE delivery_id = $1
              AND status = 'dead_lettered'
            FOR UPDATE
         ),
         updated AS (
            UPDATE webhook_deliveries d
            SET operator_resolved_at = COALESCE(d.operator_resolved_at, NOW()),
                operator_resolved_by = CASE
                    WHEN target.was_already_resolved THEN d.operator_resolved_by
                    ELSE $2
                END,
                operator_resolution_note = CASE
                    WHEN target.was_already_resolved THEN d.operator_resolution_note
                    ELSE $3
                END,
                updated_at = CASE
                    WHEN target.was_already_resolved THEN d.updated_at
                    ELSE NOW()
                END
            FROM target
            WHERE d.delivery_id = target.delivery_id
            RETURNING
                d.delivery_id,
                d.event_id,
                d.operator_resolved_at,
                d.operator_resolved_by,
                d.operator_resolution_note,
                target.was_already_resolved
         )
         SELECT * FROM updated",
    )
    .bind(delivery_id)
    .bind(resolved_by.as_deref())
    .bind(note.as_deref())
    .fetch_optional(&mut *tx)
    .await?;

    let row = match row {
        Some(row) => row,
        None => {
            let status = sqlx::query_scalar::<_, String>(
                "SELECT status FROM webhook_deliveries WHERE delivery_id = $1",
            )
            .bind(delivery_id)
            .fetch_optional(&mut *tx)
            .await?;

            return match status {
                Some(status) => Err(AppError::BadRequest(format!(
                    "delivery {} cannot be operator-resolved while status is {}",
                    delivery_id, status
                ))),
                None => Err(AppError::NotFound(format!(
                    "delivery {} not found",
                    delivery_id
                ))),
            };
        }
    };

    let event_id: i64 = row.get("event_id");
    let operator_resolved_at: DateTime<Utc> = row.get("operator_resolved_at");
    let operator_resolved_by: Option<String> = row.get("operator_resolved_by");
    let operator_resolution_note: Option<String> = row.get("operator_resolution_note");
    let was_already_resolved: bool = row.get("was_already_resolved");

    if !was_already_resolved {
        append_delivery_trace_in_tx(
            &mut tx,
            delivery_id,
            event_id,
            "operator_resolved",
            "succeeded",
            "Operator resolved dead letter",
            json!({
                "operator_resolved_at": operator_resolved_at,
                "operator_resolved_by": operator_resolved_by,
                "operator_resolution_note": operator_resolution_note
            }),
        )
        .await?;
    }

    tx.commit().await?;

    Ok(ResolveDeliveryResponse {
        success: true,
        delivery_id,
        operator_resolved_at,
        operator_resolved_by,
        operator_resolution_note,
    })
}

pub async fn unresolve_delivery(
    pool: &PgPool,
    delivery_id: i64,
) -> AppResult<UnresolveDeliveryResponse> {
    let mut tx = pool.begin().await?;

    let row = sqlx::query(
        "WITH target AS (
            SELECT
                delivery_id,
                operator_resolved_at IS NOT NULL AS was_resolved
            FROM webhook_deliveries
            WHERE delivery_id = $1
              AND status = 'dead_lettered'
            FOR UPDATE
         ),
         updated AS (
            UPDATE webhook_deliveries d
            SET operator_resolved_at = NULL,
                operator_resolved_by = NULL,
                operator_resolution_note = NULL,
                updated_at = CASE
                    WHEN target.was_resolved THEN NOW()
                    ELSE d.updated_at
                END
            FROM target
            WHERE d.delivery_id = target.delivery_id
            RETURNING d.delivery_id, d.event_id, target.was_resolved
         )
         SELECT * FROM updated",
    )
    .bind(delivery_id)
    .fetch_optional(&mut *tx)
    .await?;

    let row = match row {
        Some(row) => row,
        None => {
            let status = sqlx::query_scalar::<_, String>(
                "SELECT status FROM webhook_deliveries WHERE delivery_id = $1",
            )
            .bind(delivery_id)
            .fetch_optional(&mut *tx)
            .await?;

            return match status {
                Some(status) => Err(AppError::BadRequest(format!(
                    "delivery {} cannot be reopened while status is {}",
                    delivery_id, status
                ))),
                None => Err(AppError::NotFound(format!(
                    "delivery {} not found",
                    delivery_id
                ))),
            };
        }
    };

    let event_id: i64 = row.get("event_id");
    let was_resolved: bool = row.get("was_resolved");

    if was_resolved {
        append_delivery_trace_in_tx(
            &mut tx,
            delivery_id,
            event_id,
            "operator_resolution_reopened",
            "succeeded",
            "Operator reopened dead letter",
            json!({}),
        )
        .await?;
    }

    tx.commit().await?;

    Ok(UnresolveDeliveryResponse {
        success: true,
        delivery_id,
        operator_resolved_at: None,
        operator_resolved_by: None,
        operator_resolution_note: None,
    })
}

async fn retry_delivery_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    delivery_id: i64,
    reason: Option<&str>,
    requested_by: Option<&str>,
) -> AppResult<RetryDeliveryResponse> {
    let original = sqlx::query(
        "SELECT
            d.delivery_id,
            d.event_id,
            e.event_type,
            d.endpoint_id,
            d.merchant_id,
            d.endpoint_url,
            d.secret_version_id,
            d.status,
            d.max_attempts
         FROM webhook_deliveries d
         JOIN domain_events e ON e.event_id = d.event_id
         WHERE d.delivery_id = $1
         FOR UPDATE OF d",
    )
    .bind(delivery_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| AppError::NotFound(format!("delivery {} not found", delivery_id)))?;

    let status: String = original.get("status");
    if !matches!(status.as_str(), "delivered" | "dead_lettered") {
        return Err(AppError::BadRequest(format!(
            "delivery {} cannot be manually retried while status is {}",
            delivery_id, status
        )));
    }

    let event_id: i64 = original.get("event_id");
    let event_type: String = original.get("event_type");
    let endpoint_id: i64 = original.get("endpoint_id");
    let merchant_id: i64 = original.get("merchant_id");
    let endpoint_url: String = original.get("endpoint_url");
    let secret_version_id: Option<i64> = original.get("secret_version_id");
    let max_attempts: i64 = original.get("max_attempts");

    if let Some(active_retry) = sqlx::query(
        "SELECT delivery_id, status
         FROM webhook_deliveries
         WHERE manually_retried_from_delivery_id = $1
           AND status IN ('pending', 'queued', 'processing', 'retrying')
         ORDER BY delivery_id DESC
         LIMIT 1
         FOR UPDATE",
    )
    .bind(delivery_id)
    .fetch_optional(&mut **tx)
    .await?
    {
        let new_delivery_id: i64 = active_retry.get("delivery_id");
        let new_delivery_status: String = active_retry.get("status");

        return Ok(RetryDeliveryResponse {
            success: true,
            created: false,
            already_active_retry: true,
            original_delivery_id: delivery_id,
            new_delivery_id,
            event_id,
            event_type,
            merchant_id,
            endpoint_id,
            endpoint_url,
            new_delivery_status,
        });
    }

    let new_delivery = sqlx::query(
        "INSERT INTO webhook_deliveries (
            event_id,
            endpoint_id,
            merchant_id,
            endpoint_url,
            secret_version_id,
            status,
            max_attempts,
            next_attempt_at,
            manually_retried_from_delivery_id,
            retry_reason,
            requested_by,
            created_at,
            updated_at
         )
         VALUES (
            $1,
            $2,
            $3,
            $4,
            $5,
            'pending',
            $6,
            NOW(),
            $7,
            $8,
            $9,
            NOW(),
            NOW()
         )
         RETURNING delivery_id, status",
    )
    .bind(event_id)
    .bind(endpoint_id)
    .bind(merchant_id)
    .bind(&endpoint_url)
    .bind(secret_version_id)
    .bind(max_attempts)
    .bind(delivery_id)
    .bind(reason)
    .bind(requested_by)
    .fetch_one(&mut **tx)
    .await?;

    let new_delivery_id: i64 = new_delivery.get("delivery_id");
    let new_delivery_status: String = new_delivery.get("status");

    append_delivery_trace_in_tx(
        tx,
        delivery_id,
        event_id,
        "manual_retry_requested",
        "succeeded",
        "Manual retry requested",
        json!({
            "created": true,
            "event_id": event_id,
            "event_type": event_type,
            "new_delivery_id": new_delivery_id,
            "new_delivery_status": new_delivery_status,
            "reason": reason,
            "requested_by": requested_by
        }),
    )
    .await?;

    append_delivery_trace_in_tx(
        tx,
        new_delivery_id,
        event_id,
        "delivery_created",
        "succeeded",
        "Retry delivery created",
        json!({
            "original_delivery_id": delivery_id,
            "event_id": event_id,
            "event_type": event_type,
            "reason": reason,
            "requested_by": requested_by
        }),
    )
    .await?;

    Ok(RetryDeliveryResponse {
        success: true,
        created: true,
        already_active_retry: false,
        original_delivery_id: delivery_id,
        new_delivery_id,
        event_id,
        event_type,
        merchant_id,
        endpoint_id,
        endpoint_url,
        new_delivery_status,
    })
}

fn dedupe_delivery_ids(delivery_ids: Vec<i64>) -> Vec<i64> {
    let mut deduped = Vec::with_capacity(delivery_ids.len());

    for delivery_id in delivery_ids {
        if !deduped.contains(&delivery_id) {
            deduped.push(delivery_id);
        }
    }

    deduped
}
