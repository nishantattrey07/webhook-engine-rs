use webhook_engine::{models::ScenarioRunConfig, services::plan_scenario_endpoints};

#[test]
fn preset_scenarios_reject_receiver_behavior_overrides() {
    let config = ScenarioRunConfig {
        receiver_behavior: Some("deliver_successfully".to_string()),
        ..ScenarioRunConfig::default()
    };

    let error = plan_scenario_endpoints("persistent_server_errors", &config, "payment_succeeded")
        .expect_err("preset scenarios should reject receiver behavior overrides");

    assert!(
        error
            .to_string()
            .contains("does not allow receiver_behavior"),
        "unexpected error: {error}"
    );
}
