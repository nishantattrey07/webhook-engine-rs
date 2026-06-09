use crate::types::{InMemoryStore};
use std::sync::{Arc,Mutex};
use crate::queue::enqueue;



pub fn run_dispatcher(db:Arc<Mutex<InMemoryStore>>)->bool{
    
    let collection = db.clone().lock().unwrap().pending_events();

    for event_id in collection {
            enqueue(event_id);
    }
    
    true
}
