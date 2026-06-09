use std::time::{Duration, SystemTime};

use crate::queue::enqueue;
use crate::types::{
    DeliveryAttempt, DeliveryOutcome, Db, EventStatus, WebhookPayload, WebhookPayloadData,
};
use crate::webhook_simulator::send_webhook;

const MAX_ATTEMPT: u64 = 5;

fn retry_delay(attempt_count: u64) -> Duration {
    let shift = attempt_count.saturating_sub(1).min(10) as u32;
    let multiplier = 1_u64 << shift;
    Duration::from_millis(100 * multiplier)
}

pub fn run_worker(db: Db, event_id: u64) {
    println!("worker start {}", event_id);

    // Read current event state and move it into Processing.
    let (
        merchant_id,
        payment_id,
        event_type,
        attempt_count,
    ) = {
        let mut store = db.lock().unwrap();
        let event = match store.domain_events.get_mut(&event_id) {
            Some(event) => event,
            None => return,
        };

        if event.status != EventStatus::Queued {
            return;
        }

        let now = SystemTime::now();

        event.mark_event_processing();

        if event.first_attempt_at.is_none() {
            event.first_attempt_at = Some(now);
        }

        event.attempt_count += 1;

        (
            event.merchant_id,
            event.object_id,
            event.event_type.clone(),
            event.attempt_count,
        )
    };

    // Build payload.
    let (
        order_id,
        mode_of_payment,
        amount,
        status,
    ) = {
        let store = db.lock().unwrap();

        let payment = match store.payments.get(&payment_id) {
            Some(payment) => payment,
            None => return,
        };

        (
            payment.order_id,
            payment.mode_of_payment.clone(),
            payment.amount,
            payment.status.clone(),
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

    // Record attempt timing.
    let started_at = SystemTime::now();
    let (outcome, http_status) = send_webhook(merchant_id, payload);
    let completed_at = SystemTime::now();

    // Store attempt history.
    {
        let mut store = db.lock().unwrap();
        let attempt_id = store.next_attempt_id;

        store.next_attempt_id += 1;

        store.attempt_history.push(DeliveryAttempt {
            attempt_id,
            event_id,
            attempt_count,
            http_status,
            outcome: outcome.clone(),
            started_at,
            completed_at,
            timestamp: completed_at,
        });
    }

    // Update event state.
    {
        let mut store = db.lock().unwrap();
        let event = match store.domain_events.get_mut(&event_id) {
            Some(event) => event,
            None => return,
        };

        match outcome {
            DeliveryOutcome::Success => {
                event.mark_event_delivered(completed_at);
            }

            DeliveryOutcome::TemporaryFailure | DeliveryOutcome::Timeout => {
                if attempt_count >= MAX_ATTEMPT {
                    event.mark_event_deadlettered(completed_at);
                } else {
                    let next_attempt_at = completed_at
                        .checked_add(retry_delay(attempt_count))
                        .unwrap_or(completed_at);

                    event.mark_event_pending(Some(next_attempt_at));
                }
            }

            DeliveryOutcome::PermanentFailure => {
                event.mark_event_deadlettered(completed_at);
            }
        }
    }

    println!("worker end {}", event_id);
}
