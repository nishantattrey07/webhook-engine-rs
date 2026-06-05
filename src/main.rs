use webhook_engine::types::InMemoryStore;
use webhook_engine::payment_service::add_payments_db;
use webhook_engine::dispatcher:: run_dispatcher;
use webhook_engine::retry::retry;








fn main() {
    let mut store = InMemoryStore::new();

    add_payments_db(&mut store);

    println!("*************************************");
    println!("Before Simulation");
    println!(" ");
    println!("Delivered Events: {:#?}",store.event_delivered_count());
    println!("Pending Events: {:#?}",store.event_pending_count());
    println!("DeadLettered Events: {:#?}",store.event_deadlettered_count());
    println!(" ");

    
      run_dispatcher(&mut store);
   
    
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
        retry(&mut store);
    
    println!("Delivered Events: {:#?}",store.event_delivered_count());
    println!("Pending Events: {:#?}",store.event_pending_count());
    println!("DeadLettered Events: {:#?}",store.event_deadlettered_count());
    println!(" ");

    println!("*************************************");
    println!("Result");
    store.verify_invariant();
    println!(" ");

    
    
}
