use crate::queue:: RETRY_QUEUE;
use crate::types::{InMemoryStore};
use crate::worker::run_worker;


pub fn retry(db:&mut InMemoryStore){

    while let Some(event_id) =
        RETRY_QUEUE.with(
            |queue_cell|
                queue_cell.borrow_mut().pop_front()
        )
    {
        run_worker(db, event_id);
    }
}