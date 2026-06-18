#[cfg(test)]
use crate::models::DeliveryTraceItem;

mod dashboard;
mod deliveries;
mod endpoints;
mod events;
mod metrics;
mod operator_actions;
mod payments;
mod scenarios;
mod trace;
mod validation;

pub use dashboard::get_dashboard_summary;
#[cfg(test)]
use dashboard::{active_deliveries, queue_depth};
pub use deliveries::{
    get_delivery, get_delivery_detail, list_deliveries, list_deliveries_for_event,
};
pub use endpoints::{
    create_endpoint, get_endpoint, get_endpoint_detail, list_endpoint_deliveries,
    list_endpoint_stats, list_endpoints, test_endpoint, update_endpoint,
};
#[cfg(test)]
use events::event_search;
pub use events::{get_event, get_event_fanout, list_events};
pub use metrics::{
    get_metrics_dead_letters, get_metrics_delivery_status, get_metrics_endpoints,
    get_metrics_failures, get_metrics_http_status, get_metrics_latency,
    get_metrics_lifecycle_funnel, get_metrics_queue, get_metrics_retries, get_metrics_scenarios,
    get_metrics_summary, get_metrics_throughput,
};
pub use operator_actions::{
    bulk_retry_deliveries, resolve_delivery, retry_delivery, unresolve_delivery,
};
pub use payments::{create_bulk_payments, create_payment};
pub use scenarios::{
    PlannedScenarioEndpoint, PlannedScenarioRun, build_scenario_run_plan, get_scenario,
    list_scenario_history, plan_scenario_endpoints, receiver_behavior_catalog,
    resolve_receiver_behavior, resolve_scenario_key, run_scenario, scenario_catalog,
};
#[cfg(test)]
use trace::build_trace_graph;
pub use trace::{get_delivery_trace_graph, list_delivery_attempts, list_delivery_trace};
#[cfg(test)]
use validation::{
    normalize_optional_text, normalize_resolution_filter, validate_endpoint_url_with_local_policy,
};

const ALLOWED_EVENT_TYPES: &[&str] = &["payment_succeeded", "payment_failed", "payment_refund"];
const DELIVERY_STATUSES: &[&str] = &[
    "pending",
    "queued",
    "processing",
    "retrying",
    "delivered",
    "dead_lettered",
];

pub fn allowed_event_types() -> Vec<&'static str> {
    ALLOWED_EVENT_TYPES.to_vec()
}
const ATTEMPT_OUTCOMES: &[&str] = &[
    "success",
    "temporary_failure",
    "permanent_failure",
    "timeout",
    "abandoned",
    "unknown",
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::RetryDeliveryRequest;
    use chrono::{TimeZone, Utc};
    use serde_json::Value;
    use sqlx::postgres::PgPoolOptions;

    fn trace_item(
        trace_id: i64,
        delivery_id: Option<i64>,
        event_id: i64,
        step: &str,
    ) -> DeliveryTraceItem {
        DeliveryTraceItem {
            trace_id,
            delivery_id,
            event_id,
            step: step.to_string(),
            status: "succeeded".to_string(),
            title: step.to_string(),
            detail: None,
            metadata_json: Value::Object(Default::default()),
            occurred_at: Utc
                .with_ymd_and_hms(2026, 6, 12, 12, 0, trace_id as u32)
                .single()
                .expect("valid test timestamp"),
            duration_ms: None,
        }
    }

    #[test]
    fn trace_graph_uses_ordered_persisted_trace_rows() {
        let graph = build_trace_graph(vec![
            trace_item(1, None, 10, "payment_committed"),
            trace_item(2, None, 10, "domain_event_created"),
            trace_item(3, Some(20), 10, "delivery_created"),
            trace_item(4, Some(20), 10, "worker_claimed"),
        ]);

        let steps = graph
            .nodes
            .iter()
            .map(|node| node.step.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            steps,
            vec![
                "payment_committed",
                "domain_event_created",
                "delivery_created",
                "worker_claimed"
            ]
        );
        assert_eq!(graph.edges.len(), 3);
        assert_eq!(graph.edges[0].source, "trace-1");
        assert_eq!(graph.edges[0].target, "trace-2");
    }

    #[test]
    fn dashboard_queue_depth_includes_pending_due_queued_and_retry_due() {
        assert_eq!(queue_depth(2, 3, 5), 10);
    }

    #[test]
    fn dashboard_active_deliveries_excludes_pending() {
        assert_eq!(active_deliveries(3, 4, 5), 12);
    }

    #[test]
    fn event_search_routes_prefixed_event_id() {
        let search = event_search(Some("evt_000137"));

        assert_eq!(search.event_id, Some(137));
        assert_eq!(search.merchant_id, None);
    }

    #[test]
    fn event_search_routes_numeric_to_indexed_ids() {
        let search = event_search(Some("334"));

        assert_eq!(search.event_id, Some(334));
        assert_eq!(search.merchant_id, Some(334));
        assert_eq!(search.object_id, Some(334));
    }

    #[test]
    fn event_search_routes_event_type() {
        let search = event_search(Some("payment_succeeded"));

        assert_eq!(search.event_type.as_deref(), Some("payment_succeeded"));
    }

    #[test]
    fn resolution_filter_accepts_supported_values() {
        assert_eq!(
            normalize_resolution_filter(Some(" unresolved ".to_string())).unwrap(),
            Some("unresolved".to_string())
        );
        assert_eq!(
            normalize_resolution_filter(Some("resolved".to_string())).unwrap(),
            Some("resolved".to_string())
        );
        assert_eq!(
            normalize_resolution_filter(Some("all".to_string())).unwrap(),
            None
        );
    }

    #[test]
    fn optional_text_is_trimmed_and_bounded() {
        assert_eq!(
            normalize_optional_text(Some(" Operator ".to_string()), "resolved_by", 20).unwrap(),
            Some("Operator".to_string())
        );
        assert_eq!(
            normalize_optional_text(Some("   ".to_string()), "note", 20).unwrap(),
            None
        );
        assert!(normalize_optional_text(Some("too long".to_string()), "note", 3).is_err());
    }

    #[test]
    fn endpoint_url_rejects_local_targets_without_demo_allowance() {
        for url in [
            "http://localhost:3000/webhook",
            "http://127.0.0.1:3000/webhook",
            "http://10.1.2.3/webhook",
            "http://172.16.0.1/webhook",
            "http://192.168.1.10/webhook",
            "http://169.254.169.254/latest/meta-data",
            "http://[::1]:3000/webhook",
            "http://[fc00::1]/webhook",
            "http://[fe80::1]/webhook",
        ] {
            assert!(
                validate_endpoint_url_with_local_policy(url, false).is_err(),
                "{url} should be rejected"
            );
        }
    }

    #[test]
    fn endpoint_url_allows_local_targets_with_demo_allowance() {
        for url in [
            "http://localhost:3000/webhook",
            "http://127.0.0.1:3000/webhook",
            "http://10.1.2.3/webhook",
            "http://[::1]:3000/webhook",
        ] {
            assert!(
                validate_endpoint_url_with_local_policy(url, true).is_ok(),
                "{url} should be allowed in local demo mode"
            );
        }
    }

    #[test]
    fn endpoint_url_allows_public_https_targets_without_demo_allowance() {
        assert!(
            validate_endpoint_url_with_local_policy("https://example.com/webhook", false).is_ok()
        );
    }

    #[tokio::test]
    async fn list_delivery_trace_includes_event_rows_and_only_this_delivery_rows_when_test_db_is_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let database_url = match std::env::var("TEST_DATABASE_URL") {
            Ok(value) => value,
            Err(_) => {
                eprintln!("skipping db-backed trace test; TEST_DATABASE_URL is not set");
                return Ok(());
            }
        };

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await?;

        let marker = Utc::now().timestamp_micros();
        let merchant_id = 9_000_000_000_i64 + (marker % 1_000_000);
        let payment_id: i64 = sqlx::query_scalar(
            "INSERT INTO payments (merchant_id, order_id, amount, status, mode_of_payment)
             VALUES ($1, $2, 100, 'succeeded', 'test')
             RETURNING payment_id",
        )
        .bind(merchant_id)
        .bind(marker)
        .fetch_one(&pool)
        .await?;

        let event_id: i64 = sqlx::query_scalar(
            "INSERT INTO domain_events (
                merchant_id,
                object_type,
                object_id,
                event_type
             )
             VALUES ($1, 'payment', $2, 'payment_succeeded')
             RETURNING event_id",
        )
        .bind(merchant_id)
        .bind(payment_id)
        .fetch_one(&pool)
        .await?;

        let endpoint_one_id: i64 = sqlx::query_scalar(
            "INSERT INTO webhook_endpoints (merchant_id, url, enabled, max_attempts)
             VALUES ($1, 'http://127.0.0.1:3000/test/one', TRUE, 5)
             RETURNING endpoint_id",
        )
        .bind(merchant_id)
        .fetch_one(&pool)
        .await?;

        let endpoint_two_id: i64 = sqlx::query_scalar(
            "INSERT INTO webhook_endpoints (merchant_id, url, enabled, max_attempts)
             VALUES ($1, 'http://127.0.0.1:3000/test/two', TRUE, 5)
             RETURNING endpoint_id",
        )
        .bind(merchant_id)
        .fetch_one(&pool)
        .await?;

        let delivery_one_id: i64 = sqlx::query_scalar(
            "INSERT INTO webhook_deliveries (
                event_id,
                endpoint_id,
                merchant_id,
                endpoint_url,
                status,
                max_attempts
             )
             VALUES ($1, $2, $3, 'http://127.0.0.1:3000/test/one', 'pending', 5)
             RETURNING delivery_id",
        )
        .bind(event_id)
        .bind(endpoint_one_id)
        .bind(merchant_id)
        .fetch_one(&pool)
        .await?;

        let delivery_two_id: i64 = sqlx::query_scalar(
            "INSERT INTO webhook_deliveries (
                event_id,
                endpoint_id,
                merchant_id,
                endpoint_url,
                status,
                max_attempts
             )
             VALUES ($1, $2, $3, 'http://127.0.0.1:3000/test/two', 'pending', 5)
             RETURNING delivery_id",
        )
        .bind(event_id)
        .bind(endpoint_two_id)
        .bind(merchant_id)
        .fetch_one(&pool)
        .await?;

        let occurred_at = Utc::now();
        for (delivery_id, step) in [
            (None, "payment_committed"),
            (None, "domain_event_created"),
            (Some(delivery_one_id), "delivery_one_created"),
            (Some(delivery_two_id), "delivery_two_created"),
        ] {
            sqlx::query(
                "INSERT INTO delivery_trace_events (
                    delivery_id,
                    event_id,
                    step,
                    status,
                    title,
                    metadata_json,
                    occurred_at
                 )
                 VALUES ($1, $2, $3, 'succeeded', $3, '{}'::jsonb, $4)",
            )
            .bind(delivery_id)
            .bind(event_id)
            .bind(step)
            .bind(occurred_at)
            .execute(&pool)
            .await?;
        }

        let delivery_one_trace = list_delivery_trace(&pool, delivery_one_id).await?;
        let delivery_one_steps = delivery_one_trace
            .iter()
            .map(|item| item.step.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            delivery_one_steps,
            vec![
                "payment_committed",
                "domain_event_created",
                "delivery_one_created"
            ]
        );
        assert!(
            delivery_one_trace
                .iter()
                .any(|item| item.delivery_id.is_none())
        );
        assert!(
            delivery_one_trace
                .iter()
                .all(|item| item.delivery_id.is_none() || item.delivery_id == Some(delivery_one_id))
        );

        let delivery_two_trace = list_delivery_trace(&pool, delivery_two_id).await?;
        let delivery_two_steps = delivery_two_trace
            .iter()
            .map(|item| item.step.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            delivery_two_steps,
            vec![
                "payment_committed",
                "domain_event_created",
                "delivery_two_created"
            ]
        );

        let graph = get_delivery_trace_graph(&pool, delivery_one_id).await?;
        let graph_steps = graph
            .nodes
            .iter()
            .map(|node| node.step.as_str())
            .collect::<Vec<_>>();
        assert_eq!(graph_steps, delivery_one_steps);

        sqlx::query("DELETE FROM payments WHERE payment_id = $1")
            .bind(payment_id)
            .execute(&pool)
            .await?;
        sqlx::query("DELETE FROM webhook_endpoints WHERE merchant_id = $1")
            .bind(merchant_id)
            .execute(&pool)
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn manual_retry_returns_existing_active_child_when_test_db_is_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let database_url = match std::env::var("TEST_DATABASE_URL") {
            Ok(value) => value,
            Err(_) => {
                eprintln!("skipping db-backed retry test; TEST_DATABASE_URL is not set");
                return Ok(());
            }
        };

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await?;

        let marker = Utc::now().timestamp_micros();
        let merchant_id = 9_100_000_000_i64 + (marker % 1_000_000);
        let payment_id: i64 = sqlx::query_scalar(
            "INSERT INTO payments (merchant_id, order_id, amount, status, mode_of_payment)
             VALUES ($1, $2, 100, 'succeeded', 'test')
             RETURNING payment_id",
        )
        .bind(merchant_id)
        .bind(marker)
        .fetch_one(&pool)
        .await?;

        let event_id: i64 = sqlx::query_scalar(
            "INSERT INTO domain_events (
                merchant_id,
                object_type,
                object_id,
                event_type
             )
             VALUES ($1, 'payment', $2, 'payment_succeeded')
             RETURNING event_id",
        )
        .bind(merchant_id)
        .bind(payment_id)
        .fetch_one(&pool)
        .await?;

        let endpoint_id: i64 = sqlx::query_scalar(
            "INSERT INTO webhook_endpoints (merchant_id, url, enabled, max_attempts)
             VALUES ($1, 'http://127.0.0.1:3000/test/retry', TRUE, 5)
             RETURNING endpoint_id",
        )
        .bind(merchant_id)
        .fetch_one(&pool)
        .await?;

        let original_delivery_id: i64 = sqlx::query_scalar(
            "INSERT INTO webhook_deliveries (
                event_id,
                endpoint_id,
                merchant_id,
                endpoint_url,
                status,
                max_attempts
             )
             VALUES ($1, $2, $3, 'http://127.0.0.1:3000/test/retry', 'dead_lettered', 5)
             RETURNING delivery_id",
        )
        .bind(event_id)
        .bind(endpoint_id)
        .bind(merchant_id)
        .fetch_one(&pool)
        .await?;

        let first = retry_delivery(
            &pool,
            original_delivery_id,
            RetryDeliveryRequest {
                reason: Some("test retry".to_string()),
                requested_by: Some("test".to_string()),
            },
        )
        .await?;

        let second = retry_delivery(
            &pool,
            original_delivery_id,
            RetryDeliveryRequest {
                reason: Some("test retry duplicate".to_string()),
                requested_by: Some("test".to_string()),
            },
        )
        .await?;

        assert!(first.success);
        assert!(first.created);
        assert!(!first.already_active_retry);
        assert_eq!(first.event_id, event_id);
        assert_eq!(first.event_type, "payment_succeeded");

        assert!(second.success);
        assert!(!second.created);
        assert!(second.already_active_retry);
        assert_eq!(second.new_delivery_id, first.new_delivery_id);
        assert_eq!(second.new_delivery_status, "pending");

        sqlx::query("DELETE FROM payments WHERE payment_id = $1")
            .bind(payment_id)
            .execute(&pool)
            .await?;
        sqlx::query("DELETE FROM webhook_endpoints WHERE merchant_id = $1")
            .bind(merchant_id)
            .execute(&pool)
            .await?;

        Ok(())
    }
}
