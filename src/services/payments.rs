use sqlx::{PgPool, Postgres, Row, Transaction};

use super::{trace::append_event_level_trace, validation::validate_payment_request};
use crate::{
    error::{AppError, AppResult},
    models::{
        CreateBulkPaymentsRequest, CreateBulkPaymentsResponse, CreatePaymentRequest,
        CreatePaymentResponse,
    },
};

pub async fn create_payment(
    pool: &PgPool,
    request: CreatePaymentRequest,
) -> AppResult<CreatePaymentResponse> {
    validate_payment_request(&request)?;

    let mut tx = pool.begin().await?;
    let response = create_payment_in_tx(&mut tx, request, None).await?;
    tx.commit().await?;

    Ok(response)
}

pub(crate) async fn create_payment_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    request: CreatePaymentRequest,
    scenario_id: Option<i64>,
) -> AppResult<CreatePaymentResponse> {
    let status = request.status.as_db_str();
    let event_type = request.status.event_type();

    let payment_id: i64 = sqlx::query(
        "INSERT INTO payments (
            scenario_id,
            merchant_id,
            order_id,
            amount,
            status,
            mode_of_payment,
            created_at,
            updated_at
         )
         VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
         RETURNING payment_id",
    )
    .bind(scenario_id)
    .bind(request.merchant_id)
    .bind(request.order_id)
    .bind(request.amount)
    .bind(status)
    .bind(&request.mode_of_payment)
    .fetch_one(&mut **tx)
    .await?
    .get(0);

    let event_id: i64 = sqlx::query(
        "INSERT INTO domain_events (
            scenario_id,
            merchant_id,
            object_type,
            object_id,
            event_type,
            created_at
         )
         VALUES ($1, $2, 'payment', $3, $4, NOW())
         RETURNING event_id",
    )
    .bind(scenario_id)
    .bind(request.merchant_id)
    .bind(payment_id)
    .bind(event_type)
    .fetch_one(&mut **tx)
    .await?
    .get(0);

    let delivery_count: i64 = sqlx::query(
        "WITH subscribed_endpoints AS (
            SELECT
                e.endpoint_id,
                e.merchant_id,
                e.url,
                e.active_secret_version_id,
                e.max_attempts
            FROM webhook_endpoints e
            INNER JOIN webhook_endpoint_subscriptions s
                ON s.endpoint_id = e.endpoint_id
            WHERE e.merchant_id = $1
              AND e.enabled = TRUE
              AND s.event_type = $2
         ),
         inserted AS (
            INSERT INTO webhook_deliveries (
                scenario_id,
                event_id,
                endpoint_id,
                merchant_id,
                endpoint_url,
                secret_version_id,
                status,
                max_attempts,
                created_at,
                updated_at
            )
            SELECT
                $4,
                $3,
                endpoint_id,
                merchant_id,
                url,
                active_secret_version_id,
                'pending',
                max_attempts,
                NOW(),
                NOW()
            FROM subscribed_endpoints
            RETURNING delivery_id
         )
         SELECT COUNT(*)::BIGINT FROM inserted",
    )
    .bind(request.merchant_id)
    .bind(event_type)
    .bind(event_id)
    .bind(scenario_id)
    .fetch_one(&mut **tx)
    .await?
    .get(0);

    append_event_level_trace(tx, event_id, "payment_committed", "Payment committed").await?;
    append_event_level_trace(tx, event_id, "domain_event_created", "Domain event created").await?;

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
         SELECT
            delivery_id,
            event_id,
            'delivery_created',
            'succeeded',
            'Delivery created',
            jsonb_build_object('endpoint_id', endpoint_id, 'endpoint_url', endpoint_url),
            NOW()
         FROM webhook_deliveries
         WHERE event_id = $1",
    )
    .bind(event_id)
    .execute(&mut **tx)
    .await?;

    Ok(CreatePaymentResponse {
        payment_id,
        event_id,
        delivery_count,
    })
}

pub async fn create_bulk_payments(
    pool: &PgPool,
    request: CreateBulkPaymentsRequest,
) -> AppResult<CreateBulkPaymentsResponse> {
    if request.payments.is_empty() {
        return Err(AppError::BadRequest(
            "at least one payment is required".to_string(),
        ));
    }

    if request.payments.len() > 100 {
        return Err(AppError::BadRequest(
            "bulk payment creation is limited to 100 payments".to_string(),
        ));
    }

    for payment in &request.payments {
        validate_payment_request(payment)?;
    }

    let total_payments = request.payments.len();
    let mut created = Vec::with_capacity(request.payments.len());
    let mut total_deliveries = 0_i64;
    let mut tx = pool.begin().await?;

    for payment in request.payments {
        let response = create_payment_in_tx(&mut tx, payment, None).await?;
        total_deliveries += response.delivery_count;
        created.push(response);
    }

    tx.commit().await?;

    Ok(CreateBulkPaymentsResponse {
        total_payments,
        total_deliveries,
        created,
    })
}
