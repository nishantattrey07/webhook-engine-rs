use webhook_engine::{
    models::ScenarioRunConfig,
    services::{build_scenario_run_plan, plan_scenario_endpoints},
};

#[test]
fn custom_receiver_behavior_plan_tracks_expected_counts_and_behavior() {
    let config = ScenarioRunConfig {
        payment_count: Some(2),
        endpoint_count: Some(4),
        event_type: Some("payment_failed".to_string()),
        max_attempts: Some(4),
        receiver_behavior: Some("return_server_error".to_string()),
        ..ScenarioRunConfig::default()
    };

    let plan = build_scenario_run_plan("custom_receiver_behavior", 793_189_338, &config)
        .expect("build custom scenario plan");

    assert_eq!(plan.scenario_key, "custom_receiver_behavior");
    assert_eq!(plan.scenario_kind, "custom");
    assert_eq!(plan.merchant_id, 793_189_338);
    assert_eq!(plan.event_type, "payment_failed");
    assert_eq!(plan.payment_count, 2);
    assert_eq!(
        plan.receiver_behavior.as_deref(),
        Some("return_server_error")
    );
    assert_eq!(plan.expected_payment_count, 2);
    assert_eq!(plan.expected_event_count, 2);
    assert_eq!(plan.expected_endpoint_count, 4);
    assert_eq!(plan.expected_delivery_count, 8);
    assert_eq!(plan.endpoints.len(), 4);
    assert!(
        plan.endpoints
            .iter()
            .all(|endpoint| endpoint.behavior_key == "return_server_error")
    );
    assert_eq!(
        plan.endpoints
            .iter()
            .map(|endpoint| endpoint.ordinal)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
}

#[test]
fn mixed_endpoint_outcomes_plan_cycles_roles_for_requested_endpoint_count() {
    let config = ScenarioRunConfig {
        payment_count: Some(3),
        endpoint_count: Some(5),
        event_type: Some("payment_succeeded".to_string()),
        ..ScenarioRunConfig::default()
    };

    let plan = build_scenario_run_plan("mixed_endpoint_outcomes", 101, &config)
        .expect("build mixed scenario plan");
    let endpoints =
        plan_scenario_endpoints("mixed_endpoint_outcomes", &config, "payment_succeeded")
            .expect("plan mixed endpoints");

    assert_eq!(plan.expected_endpoint_count, 5);
    assert_eq!(plan.expected_delivery_count, 15);
    assert_eq!(
        endpoints
            .iter()
            .map(|endpoint| endpoint.endpoint_role.as_deref())
            .collect::<Vec<_>>(),
        vec![
            Some("accounting"),
            Some("crm"),
            Some("analytics"),
            Some("accounting"),
            Some("crm"),
        ]
    );
    assert_eq!(
        endpoints
            .iter()
            .map(|endpoint| endpoint.behavior_key.as_str())
            .collect::<Vec<_>>(),
        vec![
            "deliver_successfully",
            "return_server_error",
            "return_client_error",
            "deliver_successfully",
            "return_server_error",
        ]
    );
}
