use crate::types::{InMemoryStore};
use crate::data::seed_payments;
use std::sync::{Arc,Mutex};



 
pub fn add_payments_db(db:Arc<Mutex<InMemoryStore>>)->bool{
    let data = seed_payments();
    let mut db = db.lock().unwrap();
    
    for i in data{
        
        // let (payment_id,event_id) =db.create_payment_and_event(
        db.create_payment_and_event(
            i.merchant_id,
            i.order_id,
            i.amount,
            i.status, 
            i.mode_of_payment);
        // println!("Payment created with id: {} \nEvent created with id: {}",payment_id,event_id);
        // println!(" ");
    }
    true
}