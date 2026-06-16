use webhook_engine::{models::ScenarioRunConfig, services::plan_scenario_endpoints};

#[test]
fn successful_delivery_repeats_by_endpoint_count() {
    let config = ScenarioRunConfig {
        endpoint_count: Some(4),
        ..ScenarioRunConfig::default()
    };

    let specs = plan_scenario_endpoints("successful_delivery", &config, "payment_succeeded")
        .expect("successful delivery plan");

    let keys = specs
        .iter()
        .map(|spec| spec.endpoint_key.as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        keys,
        vec!["success-1", "success-2", "success-3", "success-4"]
    );
}
