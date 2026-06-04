use std::time::Instant;
use std::collections::HashMap;

#[derive(Debug, Clone,PartialEq)]
pub enum PaymentStatus {
    Succeeded,
    Failed,
    Refunded,
}

#[derive(Debug, Clone,PartialEq)]
pub enum EventStatus{
    Pending,
    Delivered,
    DeadLettered
}

#[derive(Debug, Clone,PartialEq)]
pub enum CardType{
    Credit,
    Debit
}

#[derive(Debug, Clone,PartialEq)]
pub enum ModeOfPayment{
    Cash,
    Card(CardType),
    NetBanking,
    Upi  
}

#[derive(Debug, Clone,PartialEq)]
pub enum EventType{
    PaymentSucceeded,
    PaymentFailed,
    PaymentRefund
}


pub enum DeliveryOutcome {
    Success,
    TemporaryFailure,
    PermanentFailure,
}


#[derive(Debug, Clone,PartialEq)]
pub struct SeedPayment {
    pub merchant_id: u64,
    pub order_id: u64,
    pub amount: i64,
    pub status: PaymentStatus,
    pub mode_of_payment: ModeOfPayment,
}

#[derive(Debug, Clone,PartialEq)]
pub struct Payment{
        pub payment_id:u64 ,
        pub merchant_id:u64,
        pub order_id:u64,
        pub mode_of_payment:ModeOfPayment,
        pub amount:i64,
        pub status:PaymentStatus,
        pub created_at:Instant,    
}

#[derive(Debug, Clone,PartialEq)]
pub struct Event{
    pub event_id:u64,
    pub event_type:EventType,
    pub object_id:u64,
    pub merchant_id:u64,
    pub status:EventStatus
}

#[derive(Debug, Clone,PartialEq)]
pub struct InMemoryStore {
    pub next_payment_id: u64,
    pub next_event_id: u64,
    pub payments: HashMap<u64, Payment>,
    pub domain_events: HashMap<u64, Event>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        InMemoryStore { 
            next_payment_id: 1,
            next_event_id: 101,
            payments: HashMap::new(),
            domain_events: HashMap::new(),
        }
    }

    pub fn create_payment_and_event(&mut self,
        merchant_id:u64,
        order_id:u64,
        amount:i64,
        status:PaymentStatus,
        mode_of_payment:ModeOfPayment)->(u64,u64) {

            let event_type = match status{
                PaymentStatus::Succeeded => EventType::PaymentSucceeded,
                PaymentStatus::Failed=> EventType::PaymentFailed,
                PaymentStatus::Refunded=>EventType::PaymentRefund
            };

          let payment_id =  self.insert_payment(merchant_id, order_id, amount, status, mode_of_payment);
        
          let event_id = self.insert_event(merchant_id, payment_id, event_type, EventStatus::Pending);

          (payment_id,event_id)
            
    }


    fn insert_payment(&mut self,
        merchant_id:u64,
        order_id:u64,
        amount:i64,
        status:PaymentStatus,
        mode_of_payment:ModeOfPayment) -> u64{

            let payment_id = self.next_payment_id;
            let payment:Payment  = Payment{ 
                payment_id,
                merchant_id,
                order_id,
                mode_of_payment,
                amount, status,
                created_at:Instant::now() 
            };

            self.payments.insert(payment_id,payment);
            self.next_payment_id+=1;

            payment_id
    }


    fn insert_event(&mut self,
        merchant_id:u64,
        object_id:u64,
        event_type:EventType,
        event_status:EventStatus)->u64{

            let event_id =self.next_event_id;
            let event:Event= Event { event_id, event_type, object_id, merchant_id, status: event_status };
            self.domain_events.insert(event_id, event);
            self.next_event_id+=1;
        

            event_id
    }


  


    pub fn pending_events(&self)->Vec<&Event>{
        
        self.domain_events.values()
            .filter(|v|v.status==EventStatus::Pending).collect()
    }

    pub fn delivered_events(&self)->Vec<&Event>{
        
        self.domain_events.values()
            .filter(|v|v.status==EventStatus::Delivered).collect()
    }

    
    pub fn find_payment(&self,id:u64){

        println!("{:#?}",self.payments.get(&id));
    }


    pub fn process_pending_events(&mut self){
        for i in self.domain_events.iter_mut().map(|(_,d)| d).filter(|v| v.status==EventStatus::Pending){
            i.status=EventStatus::Delivered
        }
    }


    pub fn event_pending_count(&self)->i32{
        let mut count = 0;
        for _ in self.domain_events.iter().map(|(_,d)| d).filter(|v| v.status==EventStatus::Pending){
            count+=1;
        }

        count
    }

    pub fn event_delivered_count(&self)->i32{
        let mut count = 0;
        for _ in self.domain_events.iter().map(|(_,d)| d).filter(|v| v.status==EventStatus::Delivered){
            count+=1;
        }

        count
    }

    pub fn event_deadlettered_count(&self)->i32{
        let mut count = 0;
        for _ in self.domain_events.iter().map(|(_,d)| d).filter(|v| v.status==EventStatus::DeadLettered){
            count+=1;
        }

        count
    }


    pub fn verify_invariant(&self){
        if self.payments.len() == self.domain_events.len(){
            println!("PASS");
        }else{
           println!("FAIL"); 
        }
        
    }


    
}


impl Event {
    pub fn mark_event_delivered (&mut self){
            self.status=EventStatus::Delivered
    }

    pub fn mark_event_pending(&mut self){
            self.status=EventStatus::Pending  
    }

    pub fn mark_event_deadlettered(&mut self){
            self.status=EventStatus::DeadLettered
        
    }
}