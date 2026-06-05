use std::{cell::RefCell, collections::VecDeque};


// Declare a thread-local global variable.
thread_local! {
    pub static RETRY_QUEUE:RefCell<VecDeque<u64>> = RefCell::new(VecDeque::new());
}

