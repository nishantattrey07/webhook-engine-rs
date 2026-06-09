use crate::types::{DeliveryOutcome, EventStatus,Db, WebhookPayload, WebhookPayloadData,DeliveryAttempt};
use crate::webhook_simulator::{send_webhook};
use std::time::{Instant, SystemTime};
use crate::queue::enqueue;
const MAX_ATTEMPT:u64 = 5;


pub fn run_worker(db:Db, event_id: u64) {
    let mut store = db.lock().unwrap();
    // Read current event state
    let (
        merchant_id,
        payment_id,
        event_type,
    ) = {
        let event = match store.domain_events.get(&event_id) {
            Some(event) => event,
            None => return,
        };
    
        if event.status != EventStatus::Pending {
            return;
        }
    
        (
            event.merchant_id,
            event.object_id,
            event.event_type.clone(),
        )
    };


    // Build payload
    let payment_data = match store.payments.get(&payment_id) {
        Some(payment) => payment,
        None => return,
    };

    

    let payload = WebhookPayload {
        event_id,
        event_type,
        data: WebhookPayloadData {
            payment_id,
            order_id: payment_data.order_id,
            mode_of_payment: payment_data.mode_of_payment.clone(),
            amount: payment_data.amount,
            status: payment_data.status.clone(),
        },
    };

    let attempt_count = {
        let event = match store.domain_events.get_mut(&event_id) {
            Some(event) => event,
            None => return,
        };
    
        if event.first_attempt_at.is_none() {
            event.first_attempt_at = Some(Instant::now());
        }
    
        event.attempt_count += 1;
        event.attempt_count
    };
    
    // Record attempt timing
    let timestamp = SystemTime::now();
    let started_at = Instant::now();
    
    let (outcome, http_status) =
        send_webhook(merchant_id, payload);
    
    let completed_at = Instant::now();
 
    

    
    // Store attempt history
    
    let attempt_id = db.lock().unwrap().next_attempt_id;

    db.lock().unwrap().next_attempt_id += 1;

    db.lock().unwrap().attempt_history.push(DeliveryAttempt {
        attempt_id,
        event_id,
        http_status,
        outcome: outcome.clone(),
    
        started_at,
        completed_at,
    
        timestamp,
        attempt_count,
    });

    
    // Update event state
    let event = match store.domain_events.get_mut(&event_id) {
        Some(event) => event,
        None => return,
    };

    
    match outcome {
        DeliveryOutcome::Success => {
            event.mark_event_delivered();
            event.final_state_at = Some(completed_at);
        }
    
        DeliveryOutcome::TemporaryFailure
        | DeliveryOutcome::Timeout => {
            if attempt_count >= MAX_ATTEMPT {
                event.mark_event_deadlettered();
                event.final_state_at = Some(completed_at);
            } else {
                event.mark_event_pending();
    
                enqueue(event_id);
            }
        }
    
        DeliveryOutcome::PermanentFailure => {
            event.mark_event_deadlettered();
            event.final_state_at = Some(completed_at);
        }
    }
}