use webhook_engine::types::{Db, InMemoryStore};
use webhook_engine::payment_service::add_payments_db;
use webhook_engine::dispatcher::run_dispatcher;
use webhook_engine::report::print_engine_report;
use webhook_engine::worker_pool::worker_pool;
use std::time::Instant;
use std::sync::{Arc,Mutex};

fn main() {
    let start = Instant::now();

    let store: Db =
        Arc::new(
            Mutex::new(
                InMemoryStore::new()
            )
        );

    add_payments_db(store.clone());

    run_dispatcher(store.clone());

    worker_pool(store.clone());

    loop {
        let done = {
            let store = store.lock().unwrap();

            store.event_delivered_count()
                + store.event_deadlettered_count()
                == store.domain_events.len() as u64
        };

        if done {
            break;
        }

        std::thread::sleep(
            std::time::Duration::from_millis(100)
        );
    }

    let elapsed = start.elapsed();

    print_engine_report(
        store.clone(),
        elapsed,
    );
}
