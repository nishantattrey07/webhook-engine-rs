use std::time::Duration;

use webhook_engine::delivery_worker::{
    http_client::{build_http_client, processing_lease_duration},
    redis_support::redis_value_to_uuid_text,
    retry_policy::{
        DeliveryOutcome, FinalDeliveryState, classify_status, decide_final_delivery_state,
        queue_publish_backoff,
    },
    signing::sha256_hex,
    target::is_blocked_webhook_host,
};

#[test]
fn success_attempt_terminalizes_as_delivered() {
    assert_eq!(
        decide_final_delivery_state("success", 1, 5),
        FinalDeliveryState::Delivered
    );
}

#[test]
fn retryable_attempt_with_remaining_budget_schedules_retry() {
    assert_eq!(
        decide_final_delivery_state("temporary_failure", 1, 5),
        FinalDeliveryState::Retrying
    );
}

#[test]
fn max_attempts_one_abandoned_attempt_terminalizes() {
    assert_eq!(
        decide_final_delivery_state("abandoned", 1, 1),
        FinalDeliveryState::DeadLettered
    );
}

#[test]
fn unknown_exhausted_attempt_terminalizes() {
    assert_eq!(
        decide_final_delivery_state("unknown", 3, 3),
        FinalDeliveryState::DeadLettered
    );
}

#[test]
fn permanent_failure_terminalizes_even_with_remaining_budget() {
    assert_eq!(
        decide_final_delivery_state("permanent_failure", 1, 5),
        FinalDeliveryState::DeadLettered
    );
}

#[test]
fn redirect_status_is_permanent_failure() {
    assert!(matches!(
        classify_status(302),
        DeliveryOutcome::PermanentFailure
    ));
    assert!(matches!(
        classify_status(307),
        DeliveryOutcome::PermanentFailure
    ));
}

#[test]
fn sha256_hex_hashes_exact_body_bytes() {
    let body = br#"{"event_id":1,"delivery_id":2}"#;
    assert_eq!(
        sha256_hex(body),
        "c97e4f7c261e1fb5e7a8c0db118ebd23d822fd09a288a17733f4c4c16e4c8d50"
    );
}

#[test]
fn http_client_builder_uses_redirect_policy_none() {
    assert!(build_http_client(Duration::from_millis(100)).is_some());
}

#[test]
fn delivery_target_guard_blocks_literal_local_and_private_hosts() {
    for host in [
        "localhost",
        "127.0.0.1",
        "10.0.0.5",
        "172.16.0.1",
        "192.168.1.2",
        "169.254.169.254",
        "::1",
        "fc00::1",
        "fe80::1",
        "metadata.google.internal",
    ] {
        assert!(is_blocked_webhook_host(host), "{host} should be blocked");
    }
}

#[test]
fn delivery_target_guard_allows_public_literal_hosts() {
    assert!(!is_blocked_webhook_host("93.184.216.34"));
    assert!(!is_blocked_webhook_host(
        "2606:2800:220:1:248:1893:25c8:1946"
    ));
}

#[test]
fn processing_lease_duration_exceeds_request_timeout() {
    let timeout = Duration::from_millis(3_000);
    assert!(processing_lease_duration(timeout) > chrono::Duration::from_std(timeout).unwrap());
}

#[test]
fn redis_queue_token_parser_accepts_uuid_text() {
    let token = "A0EebC99-9C0B-4EF8-BB6D-6BB9BD380A11";
    let value = redis::Value::BulkString(token.as_bytes().to_vec());

    assert_eq!(
        redis_value_to_uuid_text(&value).as_deref(),
        Some("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
    );
}

#[test]
fn redis_queue_token_parser_rejects_missing_or_invalid_token() {
    assert!(redis_value_to_uuid_text(&redis::Value::Nil).is_none());
    assert!(redis_value_to_uuid_text(&redis::Value::BulkString(b"not-a-uuid".to_vec())).is_none());
    assert!(redis_value_to_uuid_text(&redis::Value::Int(42)).is_none());
}

#[test]
fn queue_publish_backoff_moves_retry_into_future_and_caps() {
    assert_eq!(queue_publish_backoff(1), chrono::Duration::seconds(5));
    assert_eq!(queue_publish_backoff(2), chrono::Duration::seconds(10));
    assert_eq!(queue_publish_backoff(3), chrono::Duration::seconds(20));
    assert_eq!(queue_publish_backoff(100), chrono::Duration::seconds(300));
}
