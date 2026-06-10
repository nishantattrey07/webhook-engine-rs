use std::convert::TryFrom;
use std::time::{Duration, SystemTime};

use crate::types::{Db, DeliveryOutcome, WebhookPayload, WebhookPayloadData};
use crate::webhook_simulator::send_webhook;

const MAX_ATTEMPT: u64 = 5;

fn retry_delay(attempt_count: u64) -> Duration {
    let shift = attempt_count.saturating_sub(1).min(10) as u32;
    let multiplier = 1_u64 << shift;
    Duration::from_millis(100 * multiplier)
}

fn to_db_i64(value: u64, field: &str) -> i64 {
    i64::try_from(value).unwrap_or_else(|_| panic!("{}={} does not fit into BIGINT", field, value))
}

pub fn run_worker(db: Db, event_id: u64) {
    println!("worker start {}", event_id);

    let (merchant_id, payment_id, event_type, attempt_count) = {
        let mut client = db.lock().unwrap();

        let row = match client
            .query_opt(
                "UPDATE webhook_events
                 SET status = 'processing',
                     attempt_count = attempt_count + 1,
                     first_attempt_at = COALESCE(first_attempt_at, NOW()),
                     updated_at = NOW()
                 WHERE event_id = $1
                   AND status = 'queued'
                 RETURNING merchant_id, payment_id, event_type, attempt_count",
                &[&to_db_i64(event_id, "event_id")],
            )
            .unwrap()
        {
            Some(row) => row,
            None => return,
        };

        let merchant_id: i64 = row.get(0);
        let payment_id: i64 = row.get(1);
        let event_type_db: String = row.get(2);
        let attempt_count: i64 = row.get(3);

        let event_type = match event_type_db.as_str() {
            "payment_succeeded" => crate::types::EventType::PaymentSucceeded,
            "payment_failed" => crate::types::EventType::PaymentFailed,
            "payment_refund" => crate::types::EventType::PaymentRefund,
            other => panic!("unknown event_type {}", other),
        };

        (
            u64::try_from(merchant_id).expect("merchant_id fits into u64"),
            u64::try_from(payment_id).expect("payment_id fits into u64"),
            event_type,
            u64::try_from(attempt_count).expect("attempt_count fits into u64"),
        )
    };

    let (order_id, mode_of_payment, amount, status) = {
        let mut client = db.lock().unwrap();

        let row = client
            .query_one(
                "SELECT order_id, mode_of_payment::text, amount, status
                 FROM payments
                 WHERE payment_id = $1",
                &[&to_db_i64(payment_id, "payment_id")],
            )
            .unwrap();

        let order_id: i64 = row.get(0);
        let mode_of_payment_json: String = row.get(1);
        let amount: i64 = row.get(2);
        let status_db: String = row.get(3);

        let mode_of_payment = crate::types::ModeOfPayment::from_json_str(&mode_of_payment_json)
            .unwrap_or_else(|err| panic!("invalid mode_of_payment JSON: {}", err));
        let status = crate::types::PaymentStatus::from_db_str(&status_db)
            .unwrap_or_else(|| panic!("invalid payment status: {}", status_db));

        (
            u64::try_from(order_id).expect("order_id fits into u64"),
            mode_of_payment,
            amount,
            status,
        )
    };

    let payload = WebhookPayload {
        event_id,
        event_type,
        data: WebhookPayloadData {
            payment_id,
            order_id,
            mode_of_payment,
            amount,
            status,
        },
    };

    let started_at = SystemTime::now();
    let (outcome, http_status) = send_webhook(merchant_id, payload);
    let completed_at = SystemTime::now();

    {
        let mut client = db.lock().unwrap();
        client
            .execute(
                "INSERT INTO delivery_attempts (
                    event_id,
                    attempt_count,
                    http_status,
                    outcome,
                    started_at,
                    completed_at
                )
                VALUES ($1, $2, $3, $4, $5, $6)",
                &[
                    &to_db_i64(event_id, "event_id"),
                    &to_db_i64(attempt_count, "attempt_count"),
                    &http_status.map(|status| status as i16),
                    &outcome.as_db_str(),
                    &started_at,
                    &completed_at,
                ],
            )
            .unwrap();
    }

    {
        let mut client = db.lock().unwrap();

        match outcome {
            DeliveryOutcome::Success => {
                client
                    .execute(
                        "UPDATE webhook_events
                         SET status = 'delivered',
                             next_attempt_at = NULL,
                             final_state_at = $2,
                             updated_at = NOW()
                         WHERE event_id = $1
                           AND status = 'processing'",
                        &[&to_db_i64(event_id, "event_id"), &completed_at],
                    )
                    .unwrap();
            }

            DeliveryOutcome::TemporaryFailure | DeliveryOutcome::Timeout => {
                if attempt_count >= MAX_ATTEMPT {
                    client
                        .execute(
                            "UPDATE webhook_events
                             SET status = 'dead_lettered',
                                 next_attempt_at = NULL,
                                 final_state_at = $2,
                                 updated_at = NOW()
                             WHERE event_id = $1
                               AND status = 'processing'",
                            &[&to_db_i64(event_id, "event_id"), &completed_at],
                        )
                        .unwrap();
                } else {
                    let next_attempt_at = completed_at
                        .checked_add(retry_delay(attempt_count))
                        .unwrap_or(completed_at);

                    client
                        .execute(
                            "UPDATE webhook_events
                             SET status = 'pending',
                                 next_attempt_at = $2,
                                 final_state_at = NULL,
                                 updated_at = NOW()
                             WHERE event_id = $1
                               AND status = 'processing'",
                            &[&to_db_i64(event_id, "event_id"), &next_attempt_at],
                        )
                        .unwrap();
                }
            }

            DeliveryOutcome::PermanentFailure => {
                client
                    .execute(
                        "UPDATE webhook_events
                         SET status = 'dead_lettered',
                             next_attempt_at = NULL,
                             final_state_at = $2,
                             updated_at = NOW()
                         WHERE event_id = $1
                           AND status = 'processing'",
                        &[&to_db_i64(event_id, "event_id"), &completed_at],
                    )
                    .unwrap();
            }
        }
    }

    println!("worker end {}", event_id);
}
