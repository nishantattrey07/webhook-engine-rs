use crate::types::{DeliveryOutcome, EventStatus, InMemoryStore, WebhookPayload, WebhookPayloadData};
use crate::queue::RETRY_QUEUE;
use crate::webhook_simulator::{send_webhook};
const MAX_ATTEMPT:u64 = 5;
 
pub fn run_worker(db:&mut InMemoryStore,event_id:u64){

   if let Some(event) = db.domain_events.get_mut(&event_id){
       if event.status==EventStatus::Pending{

           // getting payment data
           let payment_id=event.object_id;
           let payment_data = db.payments.get(&payment_id).unwrap();
           let payload:WebhookPayload = 
               WebhookPayload { 
                    event_id,
                    event_type: event.event_type.clone(),
                    data: WebhookPayloadData {
                        payment_id,
                        order_id: payment_data.order_id,
                        mode_of_payment: payment_data.mode_of_payment.clone(),
                        amount: payment_data.amount,
                        status: payment_data.status.clone()
                    } 
           };

           
           event.attempt_count+=1;
           let outcome = send_webhook(
               event.merchant_id,
               payload
           );

           let result = match outcome{
               Ok(data)=>data,
               Err(err)=> {
                    eprintln!("Error occurred while sending webhook: {}", err);
                    return;
               }
           };

           match result {
               DeliveryOutcome::Success => 
                  event.mark_event_delivered(),
               DeliveryOutcome::TemporaryFailure |
               DeliveryOutcome::Timeout =>{

                   if event.attempt_count >= MAX_ATTEMPT {
                       event.mark_event_deadlettered();
                   }
                   else{
                       event.mark_event_pending();
                       // i am still not good with this line section
                       // help me understand it properly
                       RETRY_QUEUE.with(|queue_cell|{
                           let mut queue = queue_cell.borrow_mut();
                           queue.push_back(event.event_id.clone());
                       })
                   }
                   
               },
               DeliveryOutcome::PermanentFailure =>
                   event.mark_event_deadlettered(),
           }
           
           };

           
           
       }
   }
        
