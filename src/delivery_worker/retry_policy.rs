#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinalDeliveryState {
    Delivered,
    Retrying,
    DeadLettered,
}

#[derive(Debug, Clone)]
pub enum DeliveryOutcome {
    Success,
    TemporaryFailure,
    PermanentFailure,
    Timeout,
    NetworkError(String),
}

impl DeliveryOutcome {
    pub fn as_db_str(&self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::TemporaryFailure => "temporary_failure",
            Self::PermanentFailure => "permanent_failure",
            Self::Timeout => "timeout",
            Self::NetworkError(_) => "temporary_failure",
        }
    }

    pub fn error_message(&self) -> Option<String> {
        match self {
            Self::NetworkError(message) => Some(message.clone()),
            Self::Timeout => Some("request timed out".to_string()),
            _ => None,
        }
    }
}

pub fn classify_status(status: u16) -> DeliveryOutcome {
    match status {
        200..=299 => DeliveryOutcome::Success,
        300..=399 => DeliveryOutcome::PermanentFailure,
        408 | 429 => DeliveryOutcome::TemporaryFailure,
        500..=599 => DeliveryOutcome::TemporaryFailure,
        400..=499 => DeliveryOutcome::PermanentFailure,
        _ => DeliveryOutcome::TemporaryFailure,
    }
}

pub fn decide_final_delivery_state(
    outcome: &str,
    attempt_count: i64,
    max_attempts: i64,
) -> FinalDeliveryState {
    if outcome == "success" {
        return FinalDeliveryState::Delivered;
    }

    if is_retryable_outcome(outcome) && attempt_count < max_attempts {
        return FinalDeliveryState::Retrying;
    }

    FinalDeliveryState::DeadLettered
}

pub fn is_retryable_outcome(outcome: &str) -> bool {
    matches!(
        outcome,
        "temporary_failure" | "timeout" | "abandoned" | "unknown"
    )
}

pub fn retry_delay(attempt_count: i64) -> chrono::Duration {
    let exponent = (attempt_count - 1).clamp(0, 6) as u32;
    let seconds = 5_i64.saturating_mul(2_i64.saturating_pow(exponent));
    chrono::Duration::seconds(seconds)
}

pub fn queue_publish_backoff(queue_attempt_count: i64) -> chrono::Duration {
    let exponent = (queue_attempt_count - 1).clamp(0, 6) as u32;
    let seconds = 5_i64
        .saturating_mul(2_i64.saturating_pow(exponent))
        .min(300);
    chrono::Duration::seconds(seconds)
}
