use webhook_engine::{models::ScenarioRunConfig, services::plan_scenario_endpoints};

#[test]
fn mixed_endpoint_outcomes_preserve_default_shape() {
    let specs = plan_scenario_endpoints(
        "mixed_endpoint_outcomes",
        &ScenarioRunConfig::default(),
        "payment_succeeded",
    )
    .expect("mixed scenario defaults");

    let keys = specs
        .iter()
        .map(|spec| spec.endpoint_key.as_str())
        .collect::<Vec<_>>();

    assert_eq!(keys, vec!["accounting", "crm", "analytics"]);
}

#[test]
fn mixed_endpoint_outcomes_expand_with_endpoint_count() {
    let config = ScenarioRunConfig {
        endpoint_count: Some(5),
        ..ScenarioRunConfig::default()
    };

    let specs = plan_scenario_endpoints("mixed_endpoint_outcomes", &config, "payment_succeeded")
        .expect("expanded mixed scenario");

    let keys = specs
        .iter()
        .map(|spec| spec.endpoint_key.as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        keys,
        vec![
            "accounting-1",
            "crm-2",
            "analytics-3",
            "accounting-4",
            "crm-5"
        ]
    );
}
