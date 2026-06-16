use webhook_engine::{models::ScenarioRunConfig, services::plan_scenario_endpoints};

#[test]
fn client_error_dead_letter_expands_with_endpoint_count() {
    let config = ScenarioRunConfig {
        endpoint_count: Some(2),
        ..ScenarioRunConfig::default()
    };

    let specs = plan_scenario_endpoints("client_error_dead_letter", &config, "payment_succeeded")
        .expect("client error plan");

    let keys = specs
        .iter()
        .map(|spec| spec.endpoint_key.as_str())
        .collect::<Vec<_>>();

    assert_eq!(keys, vec!["bad-request-1", "bad-request-2"]);
}
