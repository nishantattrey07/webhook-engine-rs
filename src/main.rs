use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};

use webhook_engine::dispatcher::run_dispatcher;
use webhook_engine::payment_service::add_payments_db;
use webhook_engine::report::print_engine_report;
use webhook_engine::types::{Db, InMemoryStore};
use webhook_engine::worker_pool::worker_pool;

fn main() {
    let start = Instant::now();

    let store: Db = Arc::new(Mutex::new(InMemoryStore::new()));

    add_payments_db(store.clone());

    let shutdown = Arc::new(AtomicBool::new(false));
    let workers = worker_pool(store.clone(), shutdown.clone());

    loop {
        run_dispatcher(store.clone());

        let done = {
            let store = store.lock().unwrap();
            store.event_terminal_count() == store.domain_events.len() as u64
        };

        if done {
            break;
        }

        std::thread::sleep(Duration::from_millis(50));
    }

    shutdown.store(true, Ordering::Relaxed);

    for handle in workers {
        handle.join().unwrap();
    }

    let elapsed = start.elapsed();

    print_engine_report(store.clone(), elapsed);
}
