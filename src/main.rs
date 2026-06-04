use webhook_engine::types::{DeliveryOutcome, EventStatus, InMemoryStore};
use webhook_engine::data::seed_payments;



fn simulate_delivery(id:u64)->DeliveryOutcome{

    match id%3  {
        0 => DeliveryOutcome::Success,
        1 => DeliveryOutcome::TemporaryFailure,
        2=> DeliveryOutcome::PermanentFailure,
        _ => unreachable!(),
    }
       
}


fn main() {
    let mut store = InMemoryStore::new();

    let data = seed_payments();
    for i in data{
        let (payment_id,event_id) =store.create_payment_and_event(
            i.merchant_id,
            i.order_id,
            i.amount,
            i.status, 
            i.mode_of_payment);
        println!("Payment created with id: {} \nEvent created with id: {}",payment_id,event_id);
        println!(" ");
    }
    println!("*************************************");
    println!("Before Simulation");
    println!(" ");
    println!("Delivered Events: {:#?}",store.event_delivered_count());
    println!("Pending Events: {:#?}",store.event_pending_count());
    println!("DeadLettered Events: {:#?}",store.event_deadlettered_count());
    println!(" ");
    for event in store.domain_events.values_mut(){
        if event.status==EventStatus::Pending{
            let outcome = simulate_delivery(event.merchant_id);
            match outcome {
                DeliveryOutcome::Success => event.mark_event_delivered(),
                DeliveryOutcome::TemporaryFailure => event.mark_event_pending(),
                DeliveryOutcome::PermanentFailure => event.mark_event_deadlettered(),
            }
        }
        
    }

    println!("*************************************");
    println!("After Simulation");
    println!(" ");
    println!("Delivered Events: {:#?}",store.event_delivered_count());
    println!("Pending Events: {:#?}",store.event_pending_count());
    println!("DeadLettered Events: {:#?}",store.event_deadlettered_count());
    println!(" ");

    println!("*************************************");
    println!("After retry");
    println!(" ");
    store.process_pending_events();
    println!("Delivered Events: {:#?}",store.event_delivered_count());
    println!("Pending Events: {:#?}",store.event_pending_count());
    println!("DeadLettered Events: {:#?}",store.event_deadlettered_count());
    println!(" ");

    println!("*************************************");
    println!("Result");
    store.verify_invariant();
    println!(" ");

    
    
}
