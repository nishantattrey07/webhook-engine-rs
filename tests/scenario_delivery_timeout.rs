use webhook_engine::{models::ScenarioRunConfig, services::plan_scenario_endpoints};

#[test]
fn delivery_timeout_expands_with_endpoint_count() {
    let config = ScenarioRunConfig {
        endpoint_count: Some(2),
        ..ScenarioRunConfig::default()
    };

    let specs = plan_scenario_endpoints("delivery_timeout", &config, "payment_succeeded")
        .expect("timeout plan");

    let keys = specs
        .iter()
        .map(|spec| spec.endpoint_key.as_str())
        .collect::<Vec<_>>();

    assert_eq!(keys, vec!["timeout-1", "timeout-2"]);
}
