use crate::webhook_simulator::simulate_delivery;
use crate::queue:: RETRY_QUEUE;
use crate::types::{DeliveryOutcome, InMemoryStore};
const MAX_RETRY:u64 = 5;

pub fn retry(db:&mut InMemoryStore){

    while let Some(event) =
        RETRY_QUEUE.with(
            |queue_cell|
                queue_cell.borrow_mut().pop_front()
        ){

            
            if let Some(original_event) = db.domain_events.get_mut(&event){
                let outcome = simulate_delivery
                    (original_event.merchant_id, original_event.retry_count);
                match outcome {
                    DeliveryOutcome::Success =>{
                        original_event.retry_count+=1;
                        original_event.mark_event_delivered();
                    },
                    DeliveryOutcome::TemporaryFailure =>{
                        original_event.retry_count+=1;
                        if original_event.retry_count>MAX_RETRY{
                            original_event.mark_event_deadlettered();
                        }else {
                            RETRY_QUEUE.with(|queue_cell| {
                                     queue_cell.borrow_mut().push_back(event);
                            });
                        }
                    },
                    DeliveryOutcome::PermanentFailure =>
                        original_event.mark_event_deadlettered(),
                }
            };
            
    }
}