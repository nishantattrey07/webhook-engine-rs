use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::Duration;

use crate::queue::dequeue;
use crate::types::Db;
use crate::worker::run_worker;

pub fn worker_pool(db: Db, shutdown: Arc<AtomicBool>) -> Vec<thread::JoinHandle<()>> {
    let mut handles = Vec::new();

    for _ in 0..4 {
        let db = db.clone();
        let shutdown = shutdown.clone();

        let handle = thread::spawn(move || {
            worker_loop(db, shutdown);
        });

        handles.push(handle);
    }

    handles
}

fn worker_loop(db: Db, shutdown: Arc<AtomicBool>) {
    loop {
        match dequeue() {
            Some(event_id) => {
                run_worker(db.clone(), event_id);
            }

            None => {
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }

                thread::sleep(Duration::from_millis(10));
            }
        }
    }
}
