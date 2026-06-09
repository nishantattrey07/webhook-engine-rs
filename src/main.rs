use webhook_engine::types::InMemoryStore;
use webhook_engine::payment_service::add_payments_db;
use webhook_engine::dispatcher::run_dispatcher;
use webhook_engine::retry::retry;
use webhook_engine::report::print_engine_report;
use std::time::Instant;
use std::sync::{Arc,Mutex};

fn main() {
    let start = Instant::now();

    let mut store:Arc<Mutex<InMemoryStore>> = 
        Arc::new(Mutex::new(InMemoryStore::new()));

    add_payments_db(store.clone());

    run_dispatcher(store.clone());
    retry(&mut store);

    let elapsed = start.elapsed();

    print_engine_report(&store, elapsed);
}
