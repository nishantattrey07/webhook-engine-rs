// use crate::queue:: RETRY_QUEUE;
// use crate::types::{InMemoryStore};
// use crate::worker::run_worker;


// pub fn retry(db:&mut InMemoryStore){

//     while let Some(event) =
//         RETRY_QUEUE.with(
//             |queue_cell|
//                 queue_cell.borrow_mut().pop_front()
//         ){
//             if let Some(original_event) = db.domain_events.get_mut(&event){
//                 run_worker(original_event);
//             };   
//     }
// }