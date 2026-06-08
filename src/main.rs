use webhook_engine::types::InMemoryStore;
use webhook_engine::payment_service::add_payments_db;
use webhook_engine::dispatcher::run_dispatcher;
use webhook_engine::retry::retry;
use webhook_engine::report::print_engine_report;
use std::time::Instant;

fn main() {
    let start = Instant::now();

    let mut store = InMemoryStore::new();

    add_payments_db(&mut store);

    run_dispatcher(&mut store);
    retry(&mut store);

    let elapsed = start.elapsed();

    print_engine_report(&store, elapsed);
}
