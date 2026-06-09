use std::time::{Instant, SystemTime};
use std::collections::{HashMap,VecDeque};
use serde::{Deserialize, Serialize};
use std::sync::{Arc,Mutex};

pub type Db= 
    Arc<Mutex<InMemoryStore>>;
pub type WorkQueue =
    Arc<Mutex<VecDeque<u64>>>;

#[derive(Debug, Clone,PartialEq,Serialize, Deserialize)]
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

#[derive(Debug, Clone,PartialEq,Serialize, Deserialize)]
pub enum CardType{
    Credit,
    Debit
}

#[derive(Debug, Clone,PartialEq,Serialize, Deserialize)]
pub enum ModeOfPayment{
    Cash,
    Card(CardType),
    NetBanking,
    Upi  
}

#[derive(Debug, Clone,PartialEq,Serialize, Deserialize)]
pub enum EventType{
    PaymentSucceeded,
    PaymentFailed,
    PaymentRefund
}

#[derive(Debug, Clone,PartialEq)]
pub enum DeliveryOutcome {
    Success,
    TemporaryFailure,
    PermanentFailure,
    Timeout
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

#[derive(Debug, Serialize, Deserialize)]
pub struct WebhookPayloadData {  
    pub payment_id: u64,
    pub order_id: u64,
    pub mode_of_payment: ModeOfPayment,
    pub amount: i64,
    pub status: PaymentStatus,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WebhookPayload {
    pub event_id: u64,
    pub event_type: EventType,
    pub data: WebhookPayloadData,
}

#[derive(Debug, Clone,PartialEq)]
pub struct Event{
    pub event_id:u64,
    pub event_type:EventType,
    pub object_id:u64,
    pub merchant_id:u64,
    pub status:EventStatus,
    pub attempt_count:u64,

    pub created_at: Instant,
    pub first_attempt_at: Option<Instant>,
    pub final_state_at: Option<Instant>,
}

#[derive(Debug, Clone,PartialEq)]
pub struct DeliveryAttempt {
    pub attempt_id:u64,
    pub attempt_count:u64,
    pub event_id:u64,
    pub http_status: Option<u16>,
    pub outcome:DeliveryOutcome,

    pub started_at: Instant,
    pub completed_at: Instant,

    pub timestamp:SystemTime,
}

#[derive(Debug, Clone,PartialEq)]
pub struct InMemoryStore {
    pub next_payment_id: u64,
    pub next_event_id: u64,
    pub next_attempt_id:u64,
    pub payments: HashMap<u64, Payment>,
    pub domain_events: HashMap<u64, Event>,
    pub attempt_history: Vec<DeliveryAttempt>,
}

#[derive(Debug)]
pub struct InvariantReport {
    pub lifecycle_passed: bool,
    pub permanent_failure_passed: bool,
    pub attempt_consistency_passed: bool,

    pub lifecycle_lhs: u64,
    pub lifecycle_rhs: u64,

    pub permanent_failure_violations: u64,
    pub attempt_count_violations: u64,
}

impl InMemoryStore {
    pub fn new() -> Self {
        InMemoryStore { 
            next_payment_id: 1,
            next_event_id: 101,
            next_attempt_id:1,
            payments: HashMap::new(),
            domain_events: HashMap::new(),
            attempt_history: Vec::new(),
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

          let payment_id =  self.insert_payment(
              merchant_id,
              order_id,
              amount,
              status,
              mode_of_payment);
        
          let event_id = self.insert_event(
              merchant_id,
              payment_id,
              event_type,
              EventStatus::Pending);

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
            let event:Event= Event {
                event_id,
                event_type,
                object_id,
                merchant_id,
                status: event_status,
                attempt_count:0,
            
                created_at: Instant::now(),
                first_attempt_at: None,
                final_state_at: None,
            };
            self.domain_events.insert(event_id, event);
            self.next_event_id+=1;
        

            event_id
    }


  


    pub fn pending_events(&self) -> Vec<u64> {
        self.domain_events
            .values()
            .filter(|v| v.status == EventStatus::Pending)
            .map(|v| v.event_id) 
            .collect()
    }

    pub fn delivered_events(&self)->Vec<&Event>{
        
        self.domain_events.values()
            .filter(|v|v.status==EventStatus::Delivered).collect()
    }

    
    pub fn find_payment(&self,id:u64){

        println!("{:#?}",self.payments.get(&id));
    }




    pub fn event_pending_count(&self)->u64{
        
        self.domain_events.values()
            .filter(|v| v.status==EventStatus::Pending)
            .count() as u64   
    }

    pub fn event_delivered_count(&self) -> u64 {
        self.domain_events.values()
            .filter(|e| e.status == EventStatus::Delivered)
            .count() as u64
    }

    pub fn event_deadlettered_count(&self)->u64{
        
        self.domain_events.values()
            .filter(|v| v.status==EventStatus::DeadLettered)
            .count() as u64    
    }

    pub fn verify_invariant(&self) -> InvariantReport {
    
        // Invariant 1
        let delivered = self.event_delivered_count();
        let deadlettered = self.event_deadlettered_count();
        let total_events = self.domain_events.len() as u64;
    
        let lifecycle_lhs = delivered + deadlettered;
        let lifecycle_rhs = total_events;
    
        let lifecycle_passed =
            lifecycle_lhs == lifecycle_rhs;
    
        // Invariant 2
    
        let mut permanent_failure_violations = 0;
    
        for (index, attempt) in self.attempt_history.iter().enumerate() {
    
            if attempt.outcome == DeliveryOutcome::PermanentFailure {
    
                for later_attempt in self.attempt_history.iter().skip(index + 1) {
    
                    if later_attempt.event_id == attempt.event_id {
                        permanent_failure_violations += 1;
                        break;
                    }
                }
            }
        }
    
        let permanent_failure_passed =
            permanent_failure_violations == 0;
    
        // Invariant 3
    
        let mut attempt_count_violations = 0;
    
        for event in self.domain_events.values() {
    
            let history_count = self
                .attempt_history
                .iter()
                .filter(|attempt| attempt.event_id == event.event_id)
                .count() as u64;
    
            if history_count != event.attempt_count {
                attempt_count_violations += 1;
            }
        }
    
        let attempt_consistency_passed =
            attempt_count_violations == 0;
    
        InvariantReport {
            lifecycle_passed,
            permanent_failure_passed,
            attempt_consistency_passed,
    
            lifecycle_lhs,
            lifecycle_rhs,
    
            permanent_failure_violations,
            attempt_count_violations,
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


impl InvariantReport {
    pub fn all_passed(&self) -> bool {
        self.lifecycle_passed
            && self.permanent_failure_passed
            && self.attempt_consistency_passed
    }
}