use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use crate::queue::enqueue;
use crate::types::InMemoryStore;

pub fn run_dispatcher(db: Arc<Mutex<InMemoryStore>>) -> usize {
    let now = SystemTime::now();

    let event_ids = {
        let mut store = db.lock().unwrap();
        let mut ids = Vec::new();

        for event_id in store.due_pending_events(now) {
            if let Some(event) = store.domain_events.get_mut(&event_id) {
                event.mark_event_queued();
                ids.push(event_id);
            }
        }

        ids
    };

    for event_id in &event_ids {
        enqueue(*event_id);
    }

    event_ids.len()
}
