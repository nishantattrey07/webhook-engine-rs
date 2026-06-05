use crate::types::{InMemoryStore};
use crate::worker::run_worker;



// it will check for pending events after the events are stored in
// event store
pub fn run_dispatcher(db:&mut InMemoryStore)->bool{
    let collection = db.pending_events();

    for event in collection{
        run_worker(db,event);
    }

    true
}
