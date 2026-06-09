use std::collections::VecDeque;
use std::sync::{Arc, Mutex, LazyLock};

use crate::types::WorkQueue;


pub static WORK_QUEUE: LazyLock<WorkQueue> =
    LazyLock::new(|| {
        Arc::new(
            Mutex::new(
                VecDeque::new()
            )
        )
    });


// Add a new event to the back of the queue.
pub fn enqueue(event_id: u64) {
    WORK_QUEUE
        .lock()
        .unwrap()
        .push_back(event_id);
}

// Remove one event from the front of the queue.
// Returns:
// - Some(event_id) if work exists
// - None if queue is empty
pub fn dequeue() -> Option<u64> {
    WORK_QUEUE
        .lock()
        .unwrap()
        .pop_front()
}

// Current queue depth.
// Useful for debugging and future metrics.
pub fn queue_len() -> usize {
    WORK_QUEUE
        .lock()
        .unwrap()
        .len()
}