use webhook_engine::{models::ScenarioRunConfig, services::plan_scenario_endpoints};

#[test]
fn persistent_server_errors_default_to_one_endpoint() {
    let specs = plan_scenario_endpoints(
        "persistent_server_errors",
        &ScenarioRunConfig::default(),
        "payment_succeeded",
    )
    .expect("server error plan");

    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].endpoint_key, "server-error");
}

#[test]
fn persistent_server_errors_expand_with_endpoint_count() {
    let config = ScenarioRunConfig {
        endpoint_count: Some(3),
        ..ScenarioRunConfig::default()
    };

    let specs = plan_scenario_endpoints("persistent_server_errors", &config, "payment_succeeded")
        .expect("expanded server error plan");

    let keys = specs
        .iter()
        .map(|spec| spec.endpoint_key.as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        keys,
        vec!["server-error-1", "server-error-2", "server-error-3"]
    );
}
