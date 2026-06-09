use webhook_engine::types::{Db, InMemoryStore};
use webhook_engine::payment_service::add_payments_db;
use webhook_engine::dispatcher::run_dispatcher;
use webhook_engine::report::print_engine_report;
use webhook_engine::worker_pool::worker_pool;
use std::time::Instant;
use std::sync::{Arc,Mutex};

fn main() {
    let start = Instant::now();

    let store:Db = 
        Arc::new(Mutex::new(InMemoryStore::new()));

    add_payments_db(store.clone());

    run_dispatcher(store.clone());

    worker_pool(store.clone());

    let elapsed = start.elapsed();

    print_engine_report(store.clone(), elapsed);
}
