use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use postgres::Client;

pub type Db = Arc<Mutex<Client>>;
pub type WorkQueue = Arc<Mutex<VecDeque<u64>>>;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PaymentStatus {
    Succeeded,
    Failed,
    Refunded,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum EventStatus {
    Pending,
    Queued,
    Processing,
    Delivered,
    DeadLettered,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CardType {
    Credit,
    Debit,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ModeOfPayment {
    Cash,
    Card(CardType),
    NetBanking,
    Upi,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum EventType {
    PaymentSucceeded,
    PaymentFailed,
    PaymentRefund,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DeliveryOutcome {
    Success,
    TemporaryFailure,
    PermanentFailure,
    Timeout,
}

impl PaymentStatus {
    pub fn as_db_str(&self) -> &'static str {
        match self {
            PaymentStatus::Succeeded => "succeeded",
            PaymentStatus::Failed => "failed",
            PaymentStatus::Refunded => "refunded",
        }
    }

    pub fn from_db_str(value: &str) -> Option<Self> {
        match value {
            "succeeded" => Some(Self::Succeeded),
            "failed" => Some(Self::Failed),
            "refunded" => Some(Self::Refunded),
            _ => None,
        }
    }
}

impl EventStatus {
    pub fn as_db_str(&self) -> &'static str {
        match self {
            EventStatus::Pending => "pending",
            EventStatus::Queued => "queued",
            EventStatus::Processing => "processing",
            EventStatus::Delivered => "delivered",
            EventStatus::DeadLettered => "dead_lettered",
        }
    }

    pub fn from_db_str(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "queued" => Some(Self::Queued),
            "processing" => Some(Self::Processing),
            "delivered" => Some(Self::Delivered),
            "dead_lettered" => Some(Self::DeadLettered),
            _ => None,
        }
    }
}

impl EventType {
    pub fn as_db_str(&self) -> &'static str {
        match self {
            EventType::PaymentSucceeded => "payment_succeeded",
            EventType::PaymentFailed => "payment_failed",
            EventType::PaymentRefund => "payment_refund",
        }
    }

    pub fn from_db_str(value: &str) -> Option<Self> {
        match value {
            "payment_succeeded" => Some(Self::PaymentSucceeded),
            "payment_failed" => Some(Self::PaymentFailed),
            "payment_refund" => Some(Self::PaymentRefund),
            _ => None,
        }
    }
}

impl DeliveryOutcome {
    pub fn as_db_str(&self) -> &'static str {
        match self {
            DeliveryOutcome::Success => "success",
            DeliveryOutcome::TemporaryFailure => "temporary_failure",
            DeliveryOutcome::PermanentFailure => "permanent_failure",
            DeliveryOutcome::Timeout => "timeout",
        }
    }

    pub fn from_db_str(value: &str) -> Option<Self> {
        match value {
            "success" => Some(Self::Success),
            "temporary_failure" => Some(Self::TemporaryFailure),
            "permanent_failure" => Some(Self::PermanentFailure),
            "timeout" => Some(Self::Timeout),
            _ => None,
        }
    }
}

impl ModeOfPayment {
    pub fn to_json_string(&self) -> String {
        serde_json::to_string(self).expect("serialize mode_of_payment")
    }

    pub fn from_json_str(value: &str) -> serde_json::Result<Self> {
        serde_json::from_str(value)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SeedPayment {
    pub merchant_id: u64,
    pub order_id: u64,
    pub amount: i64,
    pub status: PaymentStatus,
    pub mode_of_payment: ModeOfPayment,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Payment {
    pub payment_id: u64,
    pub merchant_id: u64,
    pub order_id: u64,
    pub mode_of_payment: ModeOfPayment,
    pub amount: i64,
    pub status: PaymentStatus,
    pub created_at: SystemTime,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct WebhookPayloadData {
    pub payment_id: u64,
    pub order_id: u64,
    pub mode_of_payment: ModeOfPayment,
    pub amount: i64,
    pub status: PaymentStatus,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct WebhookPayload {
    pub event_id: u64,
    pub event_type: EventType,
    pub data: WebhookPayloadData,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub event_id: u64,
    pub event_type: EventType,
    pub object_id: u64,
    pub merchant_id: u64,
    pub status: EventStatus,
    pub attempt_count: u64,
    pub next_attempt_at: Option<SystemTime>,
    pub created_at: SystemTime,
    pub first_attempt_at: Option<SystemTime>,
    pub final_state_at: Option<SystemTime>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeliveryAttempt {
    pub attempt_id: u64,
    pub attempt_count: u64,
    pub event_id: u64,
    pub http_status: Option<u16>,
    pub outcome: DeliveryOutcome,
    pub started_at: SystemTime,
    pub completed_at: SystemTime,
    pub timestamp: SystemTime,
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

#[derive(Debug, Clone, PartialEq)]
pub struct InMemoryStore {
    pub next_payment_id: u64,
    pub next_event_id: u64,
    pub next_attempt_id: u64,
    pub payments: HashMap<u64, Payment>,
    pub domain_events: HashMap<u64, Event>,
    pub attempt_history: Vec<DeliveryAttempt>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        InMemoryStore {
            next_payment_id: 1,
            next_event_id: 101,
            next_attempt_id: 1,
            payments: HashMap::new(),
            domain_events: HashMap::new(),
            attempt_history: Vec::new(),
        }
    }
}

impl Event {
    pub fn is_due(&self, now: SystemTime) -> bool {
        self.status == EventStatus::Pending
            && self.next_attempt_at.map_or(true, |due_at| due_at <= now)
    }

    pub fn mark_event_queued(&mut self) {
        self.status = EventStatus::Queued;
    }

    pub fn mark_event_processing(&mut self) {
        self.status = EventStatus::Processing;
    }

    pub fn mark_event_delivered(&mut self, completed_at: SystemTime) {
        self.status = EventStatus::Delivered;
        self.next_attempt_at = None;
        self.final_state_at = Some(completed_at);
    }

    pub fn mark_event_pending(&mut self, next_attempt_at: Option<SystemTime>) {
        self.status = EventStatus::Pending;
        self.next_attempt_at = next_attempt_at;
        self.final_state_at = None;
    }

    pub fn mark_event_deadlettered(&mut self, completed_at: SystemTime) {
        self.status = EventStatus::DeadLettered;
        self.next_attempt_at = None;
        self.final_state_at = Some(completed_at);
    }
}

impl InvariantReport {
    pub fn all_passed(&self) -> bool {
        self.lifecycle_passed && self.permanent_failure_passed && self.attempt_consistency_passed
    }
}
