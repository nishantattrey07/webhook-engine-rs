use std::convert::TryFrom;

use crate::data::seed_payments;
use crate::types::{Db, EventType};

fn to_db_i64(value: u64, field: &str) -> i64 {
    i64::try_from(value).unwrap_or_else(|_| panic!("{}={} does not fit into BIGINT", field, value))
}

pub fn add_payments_db(db: Db) -> bool {
    let data = seed_payments();
    let mut client = db.lock().unwrap();
    let mut transaction = client.transaction().unwrap();

    for seed in data {
        let merchant_id = to_db_i64(seed.merchant_id, "merchant_id");
        let order_id = to_db_i64(seed.order_id, "order_id");
        let mode_json = seed.mode_of_payment.to_json_string();

        let payment_row = transaction
            .query_one(
                "INSERT INTO payments (merchant_id, order_id, amount, status, mode_of_payment, created_at)
                 VALUES ($1, $2, $3, $4, $5, NOW())
                 RETURNING payment_id",
                &[
                    &merchant_id,
                    &order_id,
                    &seed.amount,
                    &seed.status.as_db_str(),
                    &mode_json,
                ],
            )
            .unwrap();

        let payment_id: i64 = payment_row.get(0);
        let event_type = match seed.status {
            crate::types::PaymentStatus::Succeeded => EventType::PaymentSucceeded,
            crate::types::PaymentStatus::Failed => EventType::PaymentFailed,
            crate::types::PaymentStatus::Refunded => EventType::PaymentRefund,
        };

        transaction
            .execute(
                "INSERT INTO webhook_events (
                    payment_id,
                    merchant_id,
                    event_type,
                    status,
                    attempt_count,
                    created_at,
                    updated_at
                )
                VALUES ($1, $2, $3, 'pending', 0, NOW(), NOW())",
                &[&payment_id, &merchant_id, &event_type.as_db_str()],
            )
            .unwrap();
    }

    transaction.commit().unwrap();
    true
}
