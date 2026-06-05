use crate::types::{InMemoryStore};
use crate::data::seed_payments;


// for v1 we will be running dispatcher after adding all the data in db
// even though we can do something like once the data is added i call the
// dispatcher and retries can run later after complete data is added
// 
pub fn add_payments_db(db:&mut InMemoryStore)->bool{
    let data = seed_payments();
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