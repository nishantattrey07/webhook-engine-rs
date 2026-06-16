use webhook_engine::{models::ScenarioRunConfig, services::plan_scenario_endpoints};

#[test]
fn custom_receiver_behavior_requires_explicit_behavior() {
    let error = plan_scenario_endpoints(
        "custom_receiver_behavior",
        &ScenarioRunConfig::default(),
        "payment_succeeded",
    )
    .expect_err("custom scenario should require receiver behavior");

    assert!(
        error.to_string().contains("requires receiver_behavior"),
        "unexpected error: {error}"
    );
}

#[test]
fn custom_receiver_behavior_accepts_explicit_behavior() {
    let config = ScenarioRunConfig {
        receiver_behavior: Some("return_server_error".to_string()),
        endpoint_count: Some(4),
        payment_count: Some(3),
        event_type: Some("payment_succeeded".to_string()),
        ..ScenarioRunConfig::default()
    };

    let specs = plan_scenario_endpoints("custom_receiver_behavior", &config, "payment_succeeded")
        .expect("custom scenario plan");

    let keys = specs
        .iter()
        .map(|spec| spec.endpoint_key.as_str())
        .collect::<Vec<_>>();

    assert_eq!(keys, vec!["custom-1", "custom-2", "custom-3", "custom-4"]);
}
