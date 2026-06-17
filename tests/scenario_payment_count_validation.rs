use webhook_engine::{
    models::ScenarioRunConfig,
    services::{build_scenario_run_plan, plan_scenario_endpoints},
};

#[test]
fn scenario_payment_count_rejects_zero_instead_of_clamping() {
    let config = ScenarioRunConfig {
        payment_count: Some(0),
        ..ScenarioRunConfig::default()
    };

    let error = build_scenario_run_plan("successful_delivery", 101, &config)
        .expect_err("zero payment_count should be rejected");

    assert!(error.to_string().contains("payment_count"));
}

#[test]
fn scenario_payment_count_rejects_values_above_limit_instead_of_clamping() {
    let config = ScenarioRunConfig {
        payment_count: Some(101),
        ..ScenarioRunConfig::default()
    };

    let error = build_scenario_run_plan("successful_delivery", 101, &config)
        .expect_err("too-large payment_count should be rejected");

    assert!(error.to_string().contains("payment_count"));
}

#[test]
fn scenario_endpoint_planning_still_ignores_payment_count() {
    let config = ScenarioRunConfig {
        payment_count: Some(0),
        endpoint_count: Some(2),
        ..ScenarioRunConfig::default()
    };

    let endpoints = plan_scenario_endpoints("successful_delivery", &config, "payment_succeeded")
        .expect("endpoint-only planning should not require payment_count");

    assert_eq!(endpoints.len(), 2);
}
