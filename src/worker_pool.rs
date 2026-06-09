use crate::types::Db;
use crate::queue::dequeue;
use crate::worker::run_worker;
use std::thread;


pub fn worker_pool(db:Db){
    for _ in 0..4 {
    
        let db = db.clone();
    
        thread::spawn(move || {
            worker_loop(db);
        });
    
    }
}

fn worker_loop(db:Db) {
    loop {
        let event_id = dequeue();

        match event_id {
            Some(event_id) => {
                run_worker(db.clone(), event_id);
            }

            None => {
                break;
            }
        }
    }
}

