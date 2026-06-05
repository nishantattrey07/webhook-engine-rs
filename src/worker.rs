use crate::types::{DeliveryOutcome, EventStatus,Event};
use crate::queue::RETRY_QUEUE;
use crate::webhook_simulator::simulate_delivery;


pub fn run_worker(event:&mut Event){
        if event.status==EventStatus::Pending{
            event.attempt_count+=1;
            let outcome = simulate_delivery(
                event.merchant_id,
                event.attempt_count
            );
            
            match outcome {
                DeliveryOutcome::Success => 
                   event.mark_event_delivered(),
                DeliveryOutcome::TemporaryFailure =>{
                    event.mark_event_pending();
                    // i am still not good with this line section
                    // help me understand it properly
                    RETRY_QUEUE.with(|queue_cell|{
                        let mut queue = queue_cell.borrow_mut();
                        queue.push_back(event.event_id.clone());
                    })
                },
                DeliveryOutcome::PermanentFailure =>
                    event.mark_event_deadlettered(),
            }
        }
}