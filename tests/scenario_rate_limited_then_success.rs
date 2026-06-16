use webhook_engine::{models::ScenarioRunConfig, services::plan_scenario_endpoints};

#[test]
fn rate_limited_then_success_expands_with_endpoint_count() {
    let config = ScenarioRunConfig {
        endpoint_count: Some(3),
        ..ScenarioRunConfig::default()
    };

    let specs = plan_scenario_endpoints("rate_limited_then_success", &config, "payment_succeeded")
        .expect("rate limit plan");

    let keys = specs
        .iter()
        .map(|spec| spec.endpoint_key.as_str())
        .collect::<Vec<_>>();

    assert_eq!(keys, vec!["rate-limit-1", "rate-limit-2", "rate-limit-3"]);
}
