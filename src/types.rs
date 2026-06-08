use std::time::{Instant, SystemTime};
use std::collections::HashMap;
use serde::{Deserialize, Serialize};

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
    pub attempt_count:u64
}

#[derive(Debug, Clone,PartialEq)]
pub struct DeliveryAttempt {
    pub attempt_id:u64,
    pub attempt_count:u64,
    pub event_id:u64,
    pub http_status: Option<u16>,
    pub outcome:DeliveryOutcome,
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
            let event:Event= Event { event_id, event_type, object_id, merchant_id, status: event_status,attempt_count:0 };
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


    pub fn process_pending_events(&mut self){
        for i in self.domain_events.iter_mut().map(|(_,d)| d).filter(|v| v.status==EventStatus::Pending){
            i.status=EventStatus::Delivered
        }
    }


    pub fn event_pending_count(&self)->u64{
        let mut count = 0;
        for _ in self.domain_events.iter().map(|(_,d)| d).filter(|v| v.status==EventStatus::Pending){
            count+=1;
        }

        count
    }

    pub fn event_delivered_count(&self)->u64{
        let mut count = 0;
        for _ in self.domain_events.iter().map(|(_,d)| d).filter(|v| v.status==EventStatus::Delivered){
            count+=1;
        }

        count
    }

    pub fn event_deadlettered_count(&self)->u64{
        let mut count = 0;
        for _ in self.domain_events.iter().map(|(_,d)| d).filter(|v| v.status==EventStatus::DeadLettered){
            count+=1;
        }

        count
    }


    pub fn verify_invariant(&self) {
        println!("\n=== INVARIANT REPORT ===");
    
        // -------------------------
        // Invariant 1
        // Delivered + DeadLettered == Total Events
        // -------------------------
    
        let delivered = self.event_delivered_count();
        let deadlettered = self.event_deadlettered_count();
        let total_events = self.domain_events.len() as u64;
    
        println!("\n[Lifecycle Invariant]");
    
        println!("Delivered Events    : {}", delivered);
        println!("DeadLettered Events : {}", deadlettered);
        println!("Total Events        : {}", total_events);
    
        if delivered + deadlettered == total_events {
            println!("PASS");
        } else {
            println!("FAIL");
        }
    
        // -------------------------
        // Invariant 2
        // PermanentFailure must be terminal
        // -------------------------
    
        println!("\n[PermanentFailure Terminal Invariant]");
    
        let mut violations = 0;
    
        for (index, attempt) in self.attempt_history.iter().enumerate() {
            if attempt.outcome == DeliveryOutcome::PermanentFailure {
    
                for later_attempt in self.attempt_history.iter().skip(index + 1) {
    
                    if later_attempt.event_id == attempt.event_id {
                        violations += 1;
    
                        println!(
                            "FAIL: Event {} had another attempt after PermanentFailure",
                            attempt.event_id
                        );
    
                        break;
                    }
                }
            }
        }
    
        println!(
            "Events Checked : {}",
            self.attempt_history.len()
        );
    
        println!(
            "Violations     : {}",
            violations
        );
    
        if violations == 0 {
            println!("PASS");
        }
    
        // -------------------------
        // Invariant 3
        // Event.attempt_count matches history
        // -------------------------
    
        println!("\n[Attempt Count Consistency Invariant]");
    
        let mut count_violations = 0;
    
        for event in self.domain_events.values() {
    
            let history_count = self
                .attempt_history
                .iter()
                .filter(|attempt| attempt.event_id == event.event_id)
                .count() as u64;
    
            if history_count != event.attempt_count {
    
                count_violations += 1;
    
                println!(
                    "FAIL: Event {} -> event.attempt_count={} history_count={}",
                    event.event_id,
                    event.attempt_count,
                    history_count
                );
            }
        }
    
        println!(
            "Events Checked : {}",
            self.domain_events.len()
        );
    
        println!(
            "Violations     : {}",
            count_violations
        );
    
        if count_violations == 0 {
            println!("PASS");
        }
    
        println!("\n=== END REPORT ===");
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