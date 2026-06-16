mod support;

use serde_json::Value;
use sqlx::Row;
use webhook_engine::{
    models::{ScenarioRunConfig, ScenarioRunRequest},
    services::{get_scenario, run_scenario},
};

#[tokio::test]
async fn run_scenario_persists_plan_and_executes_from_it_when_test_db_is_set()
-> Result<(), Box<dyn std::error::Error>> {
    let _guard = support::acquire_db_lock().await;
    let Some(pool) = support::test_pool().await else {
        return Ok(());
    };

    let receiver = support::spawn_mock_receiver().await;
    let result = async {
        unsafe {
            std::env::set_var("ALLOW_LOCAL_WEBHOOK_TARGETS", "1");
        }

        let response = run_scenario(
            &pool,
            &receiver.base_url,
            ScenarioRunRequest {
                scenario_key: "custom_receiver_behavior".to_string(),
                requested_by: Some("Operator".to_string()),
                config: Some(ScenarioRunConfig {
                    merchant_id: Some(793_189_338),
                    payment_count: Some(2),
                    event_type: Some("payment_failed".to_string()),
                    endpoint_count: Some(3),
                    max_attempts: Some(4),
                    receiver_behavior: Some("return_server_error".to_string()),
                }),
            },
        )
        .await?;

        let expectation = sqlx::query(
            "SELECT
                expected_payment_count,
                expected_event_count,
                expected_endpoint_count,
                expected_delivery_count
             FROM scenario_run_expectations
             WHERE scenario_id = $1",
        )
        .bind(response.scenario_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(expectation.get::<i64, _>("expected_payment_count"), 2);
        assert_eq!(expectation.get::<i64, _>("expected_event_count"), 2);
        assert_eq!(expectation.get::<i64, _>("expected_endpoint_count"), 3);
        assert_eq!(expectation.get::<i64, _>("expected_delivery_count"), 6);

        let endpoint_rows = sqlx::query(
            "SELECT
                ordinal,
                endpoint_role,
                endpoint_key,
                behavior_key,
                max_attempts,
                created_endpoint_id
             FROM scenario_run_endpoints
             WHERE scenario_id = $1
             ORDER BY ordinal",
        )
        .bind(response.scenario_id)
        .fetch_all(&pool)
        .await?;
        assert_eq!(endpoint_rows.len(), 3);
        for row in &endpoint_rows {
            assert_eq!(row.get::<Option<&str>, _>("endpoint_role"), Some("custom"));
            assert_eq!(row.get::<&str, _>("behavior_key"), "return_server_error");
            assert_eq!(row.get::<i64, _>("max_attempts"), 4);
            assert!(row.get::<Option<i64>, _>("created_endpoint_id").is_some());
        }

        let delivery_count: i64 = sqlx::query(
            "SELECT COUNT(*)::BIGINT AS count
             FROM webhook_deliveries
             WHERE scenario_id = $1",
        )
        .bind(response.scenario_id)
        .fetch_one(&pool)
        .await?
        .get("count");
        assert_eq!(delivery_count, 6);

        let detail = get_scenario(&pool, response.scenario_id).await?;
        assert_eq!(detail.scenario_key, "custom_receiver_behavior");
        assert_eq!(detail.requested_by.as_deref(), Some("Operator"));
        assert_eq!(detail.summary.payments_created, 2);
        assert_eq!(detail.summary.events_created, 2);
        assert_eq!(detail.summary.deliveries_created, 6);
        assert_eq!(detail.artifacts.endpoint_ids.len(), 3);
        let planned = detail.planned.expect("planned scenario detail");
        assert_eq!(planned.expectations.payment_count, 2);
        assert_eq!(planned.expectations.event_count, 2);
        assert_eq!(planned.expectations.endpoint_count, 3);
        assert_eq!(planned.expectations.delivery_count, 6);
        assert_eq!(planned.endpoints.len(), 3);
        assert!(
            planned
                .endpoints
                .iter()
                .all(|endpoint| endpoint.behavior_key == "return_server_error")
        );
        assert!(
            planned
                .endpoints
                .iter()
                .all(|endpoint| endpoint.created_endpoint_id.is_some())
        );

        let scenario_row = sqlx::query(
            "SELECT config_json, receiver_config_json, step_log_json
             FROM scenarios
             WHERE scenario_id = $1",
        )
        .bind(response.scenario_id)
        .fetch_one(&pool)
        .await?;
        let config_json: Value = scenario_row.get("config_json");
        let receiver_config_json: Value = scenario_row.get("receiver_config_json");
        let step_log_json: Value = scenario_row.get("step_log_json");

        assert_eq!(
            config_json["effective"]["expected_delivery_count"].as_i64(),
            Some(6)
        );
        assert_eq!(receiver_config_json.as_array().map(Vec::len), Some(3));
        assert_eq!(step_log_json.as_array().map(Vec::len), Some(6));
        assert_eq!(step_log_json[0]["step"].as_str(), Some("plan_persisted"));

        let receiver_requests = receiver.requests.lock().await;
        assert_eq!(receiver_requests.len(), 3);
        assert!(
            receiver_requests
                .iter()
                .all(|request| request["merchant_id"].as_i64() == Some(793_189_338))
        );

        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;

    receiver.shutdown().await;
    result
}
