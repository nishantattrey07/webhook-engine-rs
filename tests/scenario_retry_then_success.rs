use webhook_engine::{models::ScenarioRunConfig, services::plan_scenario_endpoints};

#[test]
fn retry_then_success_expands_with_endpoint_count() {
    let config = ScenarioRunConfig {
        endpoint_count: Some(2),
        ..ScenarioRunConfig::default()
    };

    let specs = plan_scenario_endpoints("retry_then_success", &config, "payment_succeeded")
        .expect("retry then success plan");

    let keys = specs
        .iter()
        .map(|spec| spec.endpoint_key.as_str())
        .collect::<Vec<_>>();

    assert_eq!(keys, vec!["retry-success-1", "retry-success-2"]);
}
