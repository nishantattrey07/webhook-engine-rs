use std::collections::VecDeque;
use std::sync::{
    Arc, LazyLock, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use crate::types::WorkQueue;

pub static WORK_QUEUE: LazyLock<WorkQueue> =
    LazyLock::new(|| Arc::new(Mutex::new(VecDeque::new())));
pub static MAX_QUEUE_DEPTH: LazyLock<AtomicUsize> = LazyLock::new(|| AtomicUsize::new(0));

fn update_max_queue_depth(current_depth: usize) {
    let mut observed = MAX_QUEUE_DEPTH.load(Ordering::Relaxed);

    while current_depth > observed {
        match MAX_QUEUE_DEPTH.compare_exchange(
            observed,
            current_depth,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(previous) => observed = previous,
        }
    }
}

pub fn enqueue(event_id: u64) {
    let mut queue = WORK_QUEUE.lock().unwrap();
    queue.push_back(event_id);
    update_max_queue_depth(queue.len());
}

pub fn dequeue() -> Option<u64> {
    WORK_QUEUE.lock().unwrap().pop_front()
}

pub fn queue_len() -> usize {
    WORK_QUEUE.lock().unwrap().len()
}

pub fn max_queue_len() -> usize {
    MAX_QUEUE_DEPTH.load(Ordering::Relaxed)
}
