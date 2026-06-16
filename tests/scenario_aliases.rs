use webhook_engine::services::{resolve_receiver_behavior, resolve_scenario_key};

#[test]
fn scenario_aliases_resolve_to_canonical_keys() {
    assert_eq!(
        resolve_scenario_key("success").as_deref(),
        Some("successful_delivery")
    );
    assert_eq!(
        resolve_scenario_key("mixed_endpoint_outcomes").as_deref(),
        Some("mixed_endpoint_outcomes")
    );
}

#[test]
fn receiver_behavior_aliases_resolve_to_canonical_keys() {
    assert_eq!(
        resolve_receiver_behavior("always_500"),
        Some("return_server_error")
    );
    assert_eq!(
        resolve_receiver_behavior("deliver_successfully"),
        Some("deliver_successfully")
    );
}
