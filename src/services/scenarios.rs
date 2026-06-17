use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx::{PgPool, Postgres, Row, Transaction};

use super::{
    endpoints::create_endpoint,
    payments::create_payment_in_tx,
    validation::{normalize_optional_text, validate_endpoint_count, validate_max_attempts},
};
use crate::{
    error::{AppError, AppResult},
    models::{
        CreateEndpointRequest, CreatePaymentRequest, PaymentStatusInput, ReceiverBehaviorOption,
        ScenarioArtifacts, ScenarioCatalogItem, ScenarioDetailResponse, ScenarioPlanDetail,
        ScenarioPlannedEndpoint, ScenarioPlannedExpectations, ScenarioRunConfig,
        ScenarioRunRequest, ScenarioRunResponse, ScenarioSummary,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScenarioKind {
    Preset,
    Custom,
}

impl ScenarioKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Preset => "preset",
            Self::Custom => "custom",
        }
    }

    fn allows_receiver_behavior(self) -> bool {
        matches!(self, Self::Custom)
    }

    fn requires_receiver_behavior(self) -> bool {
        matches!(self, Self::Custom)
    }
}

#[derive(Debug, Clone, Copy)]
struct ScenarioDefinition {
    scenario_key: &'static str,
    label: &'static str,
    description: &'static str,
    category: &'static str,
    scenario_kind: ScenarioKind,
    config_knobs: &'static [&'static str],
    aliases: &'static [&'static str],
}

fn scenario_definitions() -> &'static [ScenarioDefinition] {
    const PRESET_KNOBS: &[&str] = &[
        "merchant_id",
        "payment_count",
        "event_type",
        "endpoint_count",
        "max_attempts",
    ];
    const CUSTOM_KNOBS: &[&str] = &[
        "merchant_id",
        "payment_count",
        "event_type",
        "endpoint_count",
        "max_attempts",
        "receiver_behavior",
    ];

    const DEFINITIONS: &[ScenarioDefinition] = &[
        ScenarioDefinition {
            scenario_key: "successful_delivery",
            label: "Successful delivery",
            description: "Creates receiver endpoints that should deliver successfully.",
            category: "happy_path",
            scenario_kind: ScenarioKind::Preset,
            config_knobs: PRESET_KNOBS,
            aliases: &["success"],
        },
        ScenarioDefinition {
            scenario_key: "persistent_server_errors",
            label: "Temporary failures to DLQ",
            description: "Creates receiver endpoints that always return HTTP 500 so retries can be inspected.",
            category: "failure",
            scenario_kind: ScenarioKind::Preset,
            config_knobs: PRESET_KNOBS,
            aliases: &["always_500"],
        },
        ScenarioDefinition {
            scenario_key: "delivery_timeout",
            label: "Timeout delivery",
            description: "Creates receiver endpoints that time out before eventually exhausting retry budget.",
            category: "failure",
            scenario_kind: ScenarioKind::Preset,
            config_knobs: PRESET_KNOBS,
            aliases: &["timeout"],
        },
        ScenarioDefinition {
            scenario_key: "client_error_dead_letter",
            label: "Permanent failure",
            description: "Creates receiver endpoints that return HTTP 400 and dead-letter immediately.",
            category: "failure",
            scenario_kind: ScenarioKind::Preset,
            config_knobs: PRESET_KNOBS,
            aliases: &["permanent_400"],
        },
        ScenarioDefinition {
            scenario_key: "rate_limited_then_success",
            label: "Rate limit then success",
            description: "Creates receiver endpoints that return HTTP 429 once and then succeed.",
            category: "retry",
            scenario_kind: ScenarioKind::Preset,
            config_knobs: PRESET_KNOBS,
            aliases: &["rate_limit_429"],
        },
        ScenarioDefinition {
            scenario_key: "retry_then_success",
            label: "Retry recovery",
            description: "Creates receiver endpoints that fail once with HTTP 500 and then succeed.",
            category: "retry",
            scenario_kind: ScenarioKind::Preset,
            config_knobs: PRESET_KNOBS,
            aliases: &["fail_then_succeed"],
        },
        ScenarioDefinition {
            scenario_key: "mixed_endpoint_outcomes",
            label: "Mixed fan-out",
            description: "Creates multiple endpoints with a mixed success, retryable failure, and permanent failure pattern.",
            category: "fanout",
            scenario_kind: ScenarioKind::Preset,
            config_knobs: PRESET_KNOBS,
            aliases: &["fanout_mixed"],
        },
        ScenarioDefinition {
            scenario_key: "custom_receiver_behavior",
            label: "Custom receiver behavior",
            description: "Creates receiver endpoints using the explicitly selected receiver behavior.",
            category: "custom",
            scenario_kind: ScenarioKind::Custom,
            config_knobs: CUSTOM_KNOBS,
            aliases: &[],
        },
    ];

    DEFINITIONS
}

pub fn scenario_catalog() -> Vec<ScenarioCatalogItem> {
    scenario_definitions()
        .iter()
        .map(scenario_catalog_item_from_definition)
        .collect()
}

fn scenario_catalog_item_from_definition(definition: &ScenarioDefinition) -> ScenarioCatalogItem {
    ScenarioCatalogItem {
        scenario_key: definition.scenario_key.to_string(),
        label: definition.label.to_string(),
        description: definition.description.to_string(),
        category: definition.category.to_string(),
        scenario_kind: definition.scenario_kind.as_str().to_string(),
        config_knobs: definition
            .config_knobs
            .iter()
            .map(|knob| knob.to_string())
            .collect(),
        allows_receiver_behavior: definition.scenario_kind.allows_receiver_behavior(),
        requires_receiver_behavior: definition.scenario_kind.requires_receiver_behavior(),
        aliases: definition
            .aliases
            .iter()
            .map(|alias| alias.to_string())
            .collect(),
    }
}

pub fn receiver_behavior_catalog() -> Vec<ReceiverBehaviorOption> {
    vec![
        receiver_behavior_option(
            "deliver_successfully",
            "Deliver successfully",
            "Receiver always returns HTTP 200.",
            &["success"],
        ),
        receiver_behavior_option(
            "return_server_error",
            "Return server error",
            "Receiver always returns HTTP 500.",
            &["always_500"],
        ),
        receiver_behavior_option(
            "simulate_timeout",
            "Simulate timeout",
            "Receiver delays long enough for the delivery request to time out.",
            &["timeout"],
        ),
        receiver_behavior_option(
            "return_client_error",
            "Return client error",
            "Receiver always returns HTTP 400.",
            &["permanent_400"],
        ),
        receiver_behavior_option(
            "rate_limit_then_succeed",
            "Rate limit then succeed",
            "Receiver returns HTTP 429 once, then succeeds.",
            &["rate_limit_429"],
        ),
        receiver_behavior_option(
            "fail_once_then_succeed",
            "Fail once then succeed",
            "Receiver returns HTTP 500 once, then succeeds.",
            &["fail_then_succeed"],
        ),
    ]
}

fn receiver_behavior_option(
    receiver_behavior: &str,
    label: &str,
    description: &str,
    aliases: &[&str],
) -> ReceiverBehaviorOption {
    ReceiverBehaviorOption {
        receiver_behavior: receiver_behavior.to_string(),
        label: label.to_string(),
        description: description.to_string(),
        aliases: aliases.iter().map(|alias| alias.to_string()).collect(),
    }
}

#[derive(Debug, Clone)]
pub struct PlannedScenarioRun {
    pub scenario_key: String,
    pub scenario_kind: String,
    pub merchant_id: i64,
    pub event_type: String,
    pub payment_count: i64,
    pub payment_status: PaymentStatusInput,
    pub receiver_behavior: Option<String>,
    pub expected_payment_count: i64,
    pub expected_event_count: i64,
    pub expected_endpoint_count: i64,
    pub expected_delivery_count: i64,
    pub endpoints: Vec<PlannedScenarioEndpoint>,
}

#[derive(Debug, Clone)]
pub struct PlannedScenarioEndpoint {
    pub ordinal: i64,
    pub endpoint_role: Option<String>,
    pub endpoint_key: String,
    pub description: String,
    pub behavior_key: String,
    pub behavior: serde_json::Value,
    pub secret: String,
    pub event_types: Vec<String>,
    pub max_attempts: i64,
    pub base_delay_ms: i64,
}

pub fn resolve_scenario_key(value: &str) -> Option<String> {
    canonical_scenario_key(value)
}

pub fn resolve_receiver_behavior(value: &str) -> Option<&'static str> {
    canonical_receiver_behavior(value)
}

fn payment_status_input_for_event_type(event_type: &str) -> AppResult<PaymentStatusInput> {
    match event_type {
        "payment_succeeded" => Ok(PaymentStatusInput::Succeeded),
        "payment_failed" => Ok(PaymentStatusInput::Failed),
        "payment_refund" => Ok(PaymentStatusInput::Refunded),
        _ => Err(AppError::BadRequest(format!(
            "unsupported event type {}",
            event_type
        ))),
    }
}

pub fn build_scenario_run_plan(
    scenario_key: &str,
    merchant_id: i64,
    config: &ScenarioRunConfig,
) -> AppResult<PlannedScenarioRun> {
    let definition = resolve_scenario_definition(scenario_key)
        .ok_or_else(|| AppError::BadRequest(format!("unsupported scenario {}", scenario_key)))?;
    validate_scenario_request(&definition, config)?;

    let event_type = config
        .event_type
        .clone()
        .unwrap_or_else(|| "payment_succeeded".to_string());
    let payment_status = payment_status_input_for_event_type(&event_type)?;
    let payment_count = validate_payment_count(config.payment_count)?;
    let receiver_behavior = config
        .receiver_behavior
        .as_deref()
        .and_then(canonical_receiver_behavior)
        .map(|value| value.to_string());
    let endpoints = scenario_endpoint_specs(definition.scenario_key, config, &event_type)?;
    let expected_endpoint_count = endpoints.len() as i64;
    let expected_delivery_count = payment_count
        .checked_mul(expected_endpoint_count)
        .ok_or_else(|| AppError::BadRequest("scenario delivery count overflowed".to_string()))?;

    Ok(PlannedScenarioRun {
        scenario_key: definition.scenario_key.to_string(),
        scenario_kind: definition.scenario_kind.as_str().to_string(),
        merchant_id,
        event_type,
        payment_count,
        payment_status,
        receiver_behavior,
        expected_payment_count: payment_count,
        expected_event_count: payment_count,
        expected_endpoint_count,
        expected_delivery_count,
        endpoints,
    })
}

fn validate_payment_count(payment_count: Option<i64>) -> AppResult<i64> {
    let payment_count = payment_count.unwrap_or(1);

    if !(1..=100).contains(&payment_count) {
        return Err(AppError::BadRequest(
            "payment_count must be between 1 and 100".to_string(),
        ));
    }

    Ok(payment_count)
}

pub fn plan_scenario_endpoints(
    scenario_key: &str,
    config: &ScenarioRunConfig,
    event_type: &str,
) -> AppResult<Vec<PlannedScenarioEndpoint>> {
    let mut plan_config = config.clone();
    plan_config.event_type = Some(event_type.to_string());
    plan_config.payment_count = None;
    build_scenario_run_plan(scenario_key, 0, &plan_config).map(|plan| plan.endpoints)
}

fn resolve_scenario_definition(value: &str) -> Option<ScenarioDefinition> {
    let normalized = value.trim().to_ascii_lowercase();
    scenario_definitions()
        .iter()
        .find(|definition| {
            definition.scenario_key == normalized
                || definition.aliases.iter().any(|alias| *alias == normalized)
        })
        .copied()
}

fn validate_scenario_request(
    definition: &ScenarioDefinition,
    config: &ScenarioRunConfig,
) -> AppResult<()> {
    if let Some(receiver_behavior) = config.receiver_behavior.as_deref() {
        canonical_receiver_behavior(receiver_behavior).ok_or_else(|| {
            AppError::BadRequest(format!(
                "unsupported receiver_behavior {}",
                receiver_behavior
            ))
        })?;

        if !definition.scenario_kind.allows_receiver_behavior() {
            return Err(AppError::BadRequest(format!(
                "scenario {} does not allow receiver_behavior; use custom_receiver_behavior",
                definition.scenario_key
            )));
        }
    } else if definition.scenario_kind.requires_receiver_behavior() {
        return Err(AppError::BadRequest(
            "custom_receiver_behavior requires receiver_behavior".to_string(),
        ));
    }

    Ok(())
}

pub async fn run_scenario(
    pool: &PgPool,
    receiver_base_url: &str,
    request: ScenarioRunRequest,
) -> AppResult<ScenarioRunResponse> {
    let config = request.config.unwrap_or_default();
    let merchant_id = config
        .merchant_id
        .unwrap_or_else(|| 700_000_000 + (Utc::now().timestamp_micros() % 100_000_000));
    let plan = build_scenario_run_plan(&request.scenario_key, merchant_id, &config)?;
    let requested_by = normalize_optional_text(request.requested_by, "requested_by", 200)?;
    let config_json = json!({
        "requested": serde_json::to_value(&config).unwrap_or_else(|_| json!({})),
        "effective": {
            "scenario_key": plan.scenario_key,
            "scenario_kind": plan.scenario_kind,
            "merchant_id": plan.merchant_id,
            "payment_count": plan.payment_count,
            "event_type": plan.event_type,
            "endpoint_count": plan.expected_endpoint_count,
            "receiver_behavior": plan.receiver_behavior,
            "expected_payment_count": plan.expected_payment_count,
            "expected_event_count": plan.expected_event_count,
            "expected_endpoint_count": plan.expected_endpoint_count,
            "expected_delivery_count": plan.expected_delivery_count
        }
    });
    let initial_step_log = scenario_plan_step_log(&plan);

    let mut tx = pool.begin().await?;
    let row = sqlx::query(
        "INSERT INTO scenarios (
            scenario_key,
            status,
            requested_by,
            merchant_id,
            config_json,
            receiver_config_json,
            step_log_json,
            started_at
         )
         VALUES ($1, 'running', $2, $3, $4, '[]'::jsonb, $5, NOW())
         RETURNING scenario_id, started_at",
    )
    .bind(&plan.scenario_key)
    .bind(requested_by.as_deref())
    .bind(plan.merchant_id)
    .bind(config_json)
    .bind(json!(initial_step_log))
    .fetch_one(&mut *tx)
    .await?;

    let scenario_id: i64 = row.get("scenario_id");
    let started_at: DateTime<Utc> = row.get("started_at");
    persist_scenario_plan(&mut tx, scenario_id, &plan).await?;
    tx.commit().await?;

    if let Err(error) = setup_scenario(pool, receiver_base_url, scenario_id, &plan).await {
        let message = error.to_string();
        sqlx::query(
            "UPDATE scenarios
             SET status = 'failed',
                 error_message = $2,
                 completed_at = NOW()
             WHERE scenario_id = $1",
        )
        .bind(scenario_id)
        .bind(&message)
        .execute(pool)
        .await?;
        return Err(error);
    }

    Ok(ScenarioRunResponse {
        scenario_id,
        scenario_key: plan.scenario_key,
        status: "running".to_string(),
        started_at,
        merchant_id: plan.merchant_id,
    })
}

async fn setup_scenario(
    pool: &PgPool,
    receiver_base_url: &str,
    scenario_id: i64,
    plan: &PlannedScenarioRun,
) -> AppResult<()> {
    let mut receiver_configs = Vec::with_capacity(plan.endpoints.len());
    let mut step_log = scenario_plan_step_log(plan);
    let mut created_endpoint_ids = Vec::with_capacity(plan.endpoints.len());
    persist_scenario_runtime_state(pool, scenario_id, &receiver_configs, &step_log).await?;

    let setup_result = async {
        for endpoint in &plan.endpoints {
            configure_mock_receiver_endpoint(receiver_base_url, plan.merchant_id, endpoint).await?;
            receiver_configs.push(json!({
                "merchant_id": plan.merchant_id,
                "ordinal": endpoint.ordinal,
                "endpoint_role": endpoint.endpoint_role,
                "endpoint_key": endpoint.endpoint_key,
                "behavior_key": endpoint.behavior_key,
                "behavior": endpoint.behavior,
                "base_delay_ms": endpoint.base_delay_ms,
                "max_attempts": endpoint.max_attempts,
                "event_types": endpoint.event_types
            }));

            let response = create_endpoint(
                pool,
                CreateEndpointRequest {
                    merchant_id: plan.merchant_id,
                    url: format!(
                        "{}/webhook/{}/{}",
                        receiver_base_url.trim_end_matches('/'),
                        plan.merchant_id,
                        endpoint.endpoint_key
                    ),
                    secret: endpoint.secret.clone(),
                    enabled: Some(false),
                    description: Some(endpoint.description.clone()),
                    max_attempts: Some(endpoint.max_attempts),
                    subscribed_events: endpoint.event_types.clone(),
                },
            )
            .await?;
            created_endpoint_ids.push(response.endpoint_id);
            sqlx::query(
                "UPDATE scenario_run_endpoints
                 SET created_endpoint_id = $3
                 WHERE scenario_id = $1 AND ordinal = $2",
            )
            .bind(scenario_id)
            .bind(endpoint.ordinal)
            .bind(response.endpoint_id)
            .execute(pool)
            .await?;
            step_log.push(json!({
                "step": "endpoint_created",
                "endpoint_id": response.endpoint_id,
                "endpoint_key": endpoint.endpoint_key,
                "ordinal": endpoint.ordinal,
                "enabled": false
            }));
            persist_scenario_runtime_state(pool, scenario_id, &receiver_configs, &step_log).await?;
        }

        set_scenario_endpoints_enabled(pool, &created_endpoint_ids, true).await?;
        step_log.push(json!({
            "step": "endpoints_enabled",
            "endpoint_ids": created_endpoint_ids
        }));
        persist_scenario_runtime_state(pool, scenario_id, &receiver_configs, &step_log).await?;

        let mut tx = pool.begin().await?;
        for index in 0..plan.payment_count {
            let created = create_payment_in_tx(
                &mut tx,
                CreatePaymentRequest {
                    merchant_id: plan.merchant_id,
                    order_id: Utc::now().timestamp_micros() + index,
                    amount: 1_000 + index,
                    status: plan.payment_status.clone(),
                    mode_of_payment: "scenario_lab".to_string(),
                },
                Some(scenario_id),
            )
            .await?;
            step_log.push(json!({
                "step": "payment_created",
                "payment_id": created.payment_id,
                "event_id": created.event_id,
                "delivery_count": created.delivery_count
            }));
        }
        tx.commit().await?;
        persist_scenario_runtime_state(pool, scenario_id, &receiver_configs, &step_log).await?;

        Ok::<(), AppError>(())
    }
    .await;

    if let Err(error) = setup_result {
        cleanup_failed_scenario_setup(
            pool,
            scenario_id,
            &created_endpoint_ids,
            &receiver_configs,
            &mut step_log,
        )
        .await?;
        return Err(error);
    }

    Ok(())
}

async fn set_scenario_endpoints_enabled(
    pool: &PgPool,
    endpoint_ids: &[i64],
    enabled: bool,
) -> AppResult<()> {
    if endpoint_ids.is_empty() {
        return Ok(());
    }

    sqlx::query(
        "UPDATE webhook_endpoints
         SET enabled = $2,
             updated_at = NOW()
         WHERE endpoint_id = ANY($1)",
    )
    .bind(endpoint_ids)
    .bind(enabled)
    .execute(pool)
    .await?;

    Ok(())
}

async fn cleanup_failed_scenario_setup(
    pool: &PgPool,
    scenario_id: i64,
    created_endpoint_ids: &[i64],
    receiver_configs: &[serde_json::Value],
    step_log: &mut Vec<serde_json::Value>,
) -> AppResult<()> {
    set_scenario_endpoints_enabled(pool, created_endpoint_ids, false).await?;
    step_log.push(json!({
        "step": "setup_cleanup",
        "action": "disabled_created_endpoints",
        "endpoint_ids": created_endpoint_ids
    }));
    persist_scenario_runtime_state(pool, scenario_id, receiver_configs, step_log).await?;

    Ok(())
}

fn scenario_plan_step_log(plan: &PlannedScenarioRun) -> Vec<serde_json::Value> {
    vec![json!({
        "step": "plan_persisted",
        "scenario_key": plan.scenario_key,
        "scenario_kind": plan.scenario_kind,
        "event_type": plan.event_type,
        "payment_count": plan.payment_count,
        "endpoint_count": plan.expected_endpoint_count,
        "expected_delivery_count": plan.expected_delivery_count
    })]
}

async fn persist_scenario_plan(
    tx: &mut Transaction<'_, Postgres>,
    scenario_id: i64,
    plan: &PlannedScenarioRun,
) -> AppResult<()> {
    sqlx::query(
        "INSERT INTO scenario_run_expectations (
            scenario_id,
            expected_payment_count,
            expected_event_count,
            expected_endpoint_count,
            expected_delivery_count
         )
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(scenario_id)
    .bind(plan.expected_payment_count)
    .bind(plan.expected_event_count)
    .bind(plan.expected_endpoint_count)
    .bind(plan.expected_delivery_count)
    .execute(&mut **tx)
    .await?;

    for endpoint in &plan.endpoints {
        sqlx::query(
            "INSERT INTO scenario_run_endpoints (
                scenario_id,
                ordinal,
                endpoint_role,
                endpoint_key,
                behavior_key,
                behavior_json,
                base_delay_ms,
                max_attempts
             )
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(scenario_id)
        .bind(endpoint.ordinal)
        .bind(endpoint.endpoint_role.as_deref())
        .bind(&endpoint.endpoint_key)
        .bind(&endpoint.behavior_key)
        .bind(&endpoint.behavior)
        .bind(endpoint.base_delay_ms)
        .bind(endpoint.max_attempts)
        .execute(&mut **tx)
        .await?;
    }

    Ok(())
}

async fn persist_scenario_runtime_state(
    pool: &PgPool,
    scenario_id: i64,
    receiver_configs: &[serde_json::Value],
    step_log: &[serde_json::Value],
) -> AppResult<()> {
    sqlx::query(
        "UPDATE scenarios
         SET receiver_config_json = $2,
             step_log_json = $3
         WHERE scenario_id = $1",
    )
    .bind(scenario_id)
    .bind(json!(receiver_configs))
    .bind(json!(step_log))
    .execute(pool)
    .await?;

    Ok(())
}

async fn configure_mock_receiver_endpoint(
    receiver_base_url: &str,
    merchant_id: i64,
    endpoint: &PlannedScenarioEndpoint,
) -> AppResult<()> {
    let url = format!(
        "{}/admin/endpoints",
        receiver_base_url.trim_end_matches('/')
    );
    let response = reqwest::Client::new()
        .post(url)
        .json(&json!({
            "merchant_id": merchant_id,
            "endpoint_key": endpoint.endpoint_key,
            "behavior": endpoint.behavior,
            "base_delay_ms": endpoint.base_delay_ms,
            "secret": endpoint.secret,
            "verify_signature": true,
            "enabled": true
        }))
        .send()
        .await
        .map_err(|error| AppError::BadRequest(format!("mock receiver is unreachable: {error}")))?;

    if !response.status().is_success() {
        return Err(AppError::BadRequest(format!(
            "mock receiver endpoint configuration failed with status {}",
            response.status()
        )));
    }

    Ok(())
}

pub async fn get_scenario(pool: &PgPool, scenario_id: i64) -> AppResult<ScenarioDetailResponse> {
    let scenario = sqlx::query(
        "SELECT
            scenario_id,
            scenario_key,
            status,
            requested_by,
            merchant_id,
            receiver_config_json,
            step_log_json,
            error_message,
            started_at,
            completed_at
         FROM scenarios
         WHERE scenario_id = $1",
    )
    .bind(scenario_id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| AppError::NotFound(format!("scenario {} not found", scenario_id)))?;

    let summary_row = sqlx::query(
        "SELECT
            (SELECT COUNT(*)::BIGINT FROM payments WHERE scenario_id = $1) AS payments_created,
            (SELECT COUNT(*)::BIGINT FROM domain_events WHERE scenario_id = $1) AS events_created,
            COUNT(d.delivery_id)::BIGINT AS deliveries_created,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'delivered')::BIGINT AS delivered_count,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'retrying')::BIGINT AS retrying_count,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'dead_lettered')::BIGINT AS dead_lettered_count,
            COUNT(d.delivery_id) FILTER (WHERE d.status IN ('pending', 'queued', 'processing', 'retrying'))::BIGINT AS active_count
         FROM webhook_deliveries d
         WHERE d.scenario_id = $1",
    )
    .bind(scenario_id)
    .fetch_one(pool)
    .await?;

    let artifacts_row = sqlx::query(
        "SELECT
            COALESCE((SELECT array_agg(payment_id ORDER BY payment_id) FROM payments WHERE scenario_id = $1), ARRAY[]::BIGINT[]) AS payment_ids,
            COALESCE((SELECT array_agg(event_id ORDER BY event_id) FROM domain_events WHERE scenario_id = $1), ARRAY[]::BIGINT[]) AS event_ids,
            COALESCE((SELECT array_agg(delivery_id ORDER BY delivery_id) FROM webhook_deliveries WHERE scenario_id = $1), ARRAY[]::BIGINT[]) AS delivery_ids,
            COALESCE((SELECT array_agg(DISTINCT endpoint_id ORDER BY endpoint_id) FROM webhook_deliveries WHERE scenario_id = $1), ARRAY[]::BIGINT[]) AS endpoint_ids",
    )
    .bind(scenario_id)
    .fetch_one(pool)
    .await?;
    let expectations_row = sqlx::query(
        "SELECT
            expected_payment_count,
            expected_event_count,
            expected_endpoint_count,
            expected_delivery_count
         FROM scenario_run_expectations
         WHERE scenario_id = $1",
    )
    .bind(scenario_id)
    .fetch_optional(pool)
    .await?;
    let planned_endpoint_rows = sqlx::query(
        "SELECT
            ordinal,
            endpoint_role,
            endpoint_key,
            behavior_key,
            behavior_json,
            base_delay_ms,
            max_attempts,
            created_endpoint_id
         FROM scenario_run_endpoints
         WHERE scenario_id = $1
         ORDER BY ordinal",
    )
    .bind(scenario_id)
    .fetch_all(pool)
    .await?;

    let db_status: String = scenario.get("status");
    let active_count: i64 = summary_row.get("active_count");
    let deliveries_created: i64 = summary_row.get("deliveries_created");
    let computed_status = if db_status == "failed" {
        "failed"
    } else if deliveries_created > 0 && active_count == 0 {
        "completed"
    } else {
        "running"
    };

    if computed_status == "completed" && db_status != "completed" {
        sqlx::query(
            "UPDATE scenarios
             SET status = 'completed',
                 completed_at = COALESCE(completed_at, NOW())
             WHERE scenario_id = $1",
        )
        .bind(scenario_id)
        .execute(pool)
        .await?;
    }

    let planned = expectations_row.map(|expectations| ScenarioPlanDetail {
        expectations: ScenarioPlannedExpectations {
            payment_count: expectations.get("expected_payment_count"),
            event_count: expectations.get("expected_event_count"),
            endpoint_count: expectations.get("expected_endpoint_count"),
            delivery_count: expectations.get("expected_delivery_count"),
        },
        endpoints: planned_endpoint_rows
            .into_iter()
            .map(|row| ScenarioPlannedEndpoint {
                ordinal: row.get("ordinal"),
                endpoint_role: row.get("endpoint_role"),
                endpoint_key: row.get("endpoint_key"),
                behavior_key: row.get("behavior_key"),
                behavior: row.get("behavior_json"),
                base_delay_ms: row.get("base_delay_ms"),
                max_attempts: row.get("max_attempts"),
                created_endpoint_id: row.get("created_endpoint_id"),
            })
            .collect(),
    });

    Ok(ScenarioDetailResponse {
        scenario_id,
        scenario_key: scenario.get("scenario_key"),
        status: computed_status.to_string(),
        started_at: scenario.get("started_at"),
        completed_at: scenario.get("completed_at"),
        merchant_id: scenario.get("merchant_id"),
        requested_by: scenario.get("requested_by"),
        summary: ScenarioSummary {
            payments_created: summary_row.get("payments_created"),
            events_created: summary_row.get("events_created"),
            deliveries_created,
            delivered_count: summary_row.get("delivered_count"),
            retrying_count: summary_row.get("retrying_count"),
            dead_lettered_count: summary_row.get("dead_lettered_count"),
        },
        artifacts: ScenarioArtifacts {
            payment_ids: artifacts_row.get("payment_ids"),
            event_ids: artifacts_row.get("event_ids"),
            delivery_ids: artifacts_row.get("delivery_ids"),
            endpoint_ids: artifacts_row.get("endpoint_ids"),
        },
        planned,
        receiver_config: scenario.get("receiver_config_json"),
        step_log: scenario.get("step_log_json"),
        error_message: scenario.get("error_message"),
    })
}

fn scenario_endpoint_specs(
    scenario_key: &str,
    config: &ScenarioRunConfig,
    event_type: &str,
) -> AppResult<Vec<PlannedScenarioEndpoint>> {
    let max_attempts = validate_max_attempts(config.max_attempts)?;

    let spec = |ordinal: i64,
                role: Option<&str>,
                key: &str,
                description: &str,
                behavior_key: &str,
                behavior: serde_json::Value,
                attempts: i64| PlannedScenarioEndpoint {
        ordinal,
        endpoint_role: role.map(|value| value.to_string()),
        endpoint_key: key.to_string(),
        description: description.to_string(),
        behavior_key: behavior_key.to_string(),
        behavior,
        secret: format!("whsec_scenario_{}_{}", scenario_key, key),
        event_types: vec![event_type.to_string()],
        max_attempts: attempts,
        base_delay_ms: 20,
    };

    let simple_repeated_specs = |base_role: &str,
                                 base_key: &str,
                                 description: &str,
                                 behavior_key: &str,
                                 behavior: serde_json::Value,
                                 attempts: i64|
     -> AppResult<Vec<PlannedScenarioEndpoint>> {
        let endpoint_count = validate_endpoint_count(config.endpoint_count, 1)?;
        Ok((1..=endpoint_count)
            .map(|index| {
                let endpoint_key = if endpoint_count == 1 {
                    base_key.to_string()
                } else {
                    format!("{}-{}", base_key, index)
                };
                spec(
                    index,
                    Some(base_role),
                    &endpoint_key,
                    description,
                    behavior_key,
                    behavior.clone(),
                    attempts,
                )
            })
            .collect())
    };

    let specs = match scenario_key {
        "successful_delivery" => simple_repeated_specs(
            "success",
            "success",
            "scenario:success",
            "deliver_successfully",
            json!({ "type": "always_succeed" }),
            max_attempts,
        )?,
        "persistent_server_errors" => simple_repeated_specs(
            "server-error",
            "server-error",
            "scenario:always_500",
            "return_server_error",
            json!({ "type": "always_fail", "status": 500 }),
            config.max_attempts.unwrap_or(2).clamp(1, 20),
        )?,
        "delivery_timeout" => simple_repeated_specs(
            "timeout",
            "timeout",
            "scenario:timeout",
            "simulate_timeout",
            json!({ "type": "always_timeout", "delay_ms": 30_000 }),
            config.max_attempts.unwrap_or(2).clamp(1, 20),
        )?,
        "client_error_dead_letter" => simple_repeated_specs(
            "bad-request",
            "bad-request",
            "scenario:permanent_400",
            "return_client_error",
            json!({ "type": "always_fail", "status": 400 }),
            max_attempts,
        )?,
        "rate_limited_then_success" => simple_repeated_specs(
            "rate-limit",
            "rate-limit",
            "scenario:rate_limit_429",
            "rate_limit_then_succeed",
            json!({ "type": "sequence", "statuses": [429, 200] }),
            max_attempts,
        )?,
        "retry_then_success" => simple_repeated_specs(
            "retry-success",
            "retry-success",
            "scenario:fail_then_succeed",
            "fail_once_then_succeed",
            json!({ "type": "fail_first_n_then_succeed", "failures": 1, "status": 500 }),
            max_attempts,
        )?,
        "mixed_endpoint_outcomes" => {
            let endpoint_count = validate_endpoint_count(config.endpoint_count, 3)?;
            let mixed_attempts = config.max_attempts.unwrap_or(5).clamp(1, 20);
            let patterns = [
                (
                    "accounting",
                    "accounting",
                    "scenario:fanout_mixed:accounting",
                    "deliver_successfully",
                    json!({ "type": "always_succeed" }),
                    max_attempts,
                ),
                (
                    "crm",
                    "crm",
                    "scenario:fanout_mixed:crm",
                    "return_server_error",
                    json!({ "type": "always_fail", "status": 500 }),
                    mixed_attempts,
                ),
                (
                    "analytics",
                    "analytics",
                    "scenario:fanout_mixed:analytics",
                    "return_client_error",
                    json!({ "type": "always_fail", "status": 400 }),
                    max_attempts,
                ),
            ];
            (0..endpoint_count)
                .map(|index| {
                    let ordinal = index + 1;
                    let pattern = &patterns[index as usize % patterns.len()];
                    let endpoint_key = if endpoint_count <= patterns.len() as i64 {
                        pattern.1.to_string()
                    } else {
                        format!("{}-{}", pattern.1, ordinal)
                    };
                    spec(
                        ordinal,
                        Some(pattern.0),
                        &endpoint_key,
                        pattern.2,
                        pattern.3,
                        pattern.4.clone(),
                        pattern.5,
                    )
                })
                .collect()
        }
        "custom_receiver_behavior" => {
            let behavior_key = config
                .receiver_behavior
                .as_deref()
                .and_then(canonical_receiver_behavior)
                .ok_or_else(|| {
                    AppError::BadRequest(
                        "custom_receiver_behavior requires receiver_behavior".to_string(),
                    )
                })?;
            let behavior = receiver_behavior_json(behavior_key).ok_or_else(|| {
                AppError::BadRequest(format!("unsupported receiver_behavior {}", behavior_key))
            })?;
            simple_repeated_specs(
                "custom",
                "custom",
                "scenario:custom_receiver_behavior",
                behavior_key,
                behavior,
                max_attempts,
            )?
        }
        _ => {
            return Err(AppError::BadRequest(format!(
                "unsupported scenario {}",
                scenario_key
            )));
        }
    };

    Ok(specs)
}

fn receiver_behavior_json(value: &str) -> Option<serde_json::Value> {
    match canonical_receiver_behavior(value)? {
        "deliver_successfully" => Some(json!({ "type": "always_succeed" })),
        "return_server_error" => Some(json!({ "type": "always_fail", "status": 500 })),
        "simulate_timeout" => Some(json!({ "type": "always_timeout", "delay_ms": 30_000 })),
        "return_client_error" => Some(json!({ "type": "always_fail", "status": 400 })),
        "rate_limit_then_succeed" => Some(json!({ "type": "sequence", "statuses": [429, 200] })),
        "fail_once_then_succeed" => {
            Some(json!({ "type": "fail_first_n_then_succeed", "failures": 1, "status": 500 }))
        }
        _ => None,
    }
}

fn canonical_scenario_key(value: &str) -> Option<String> {
    resolve_scenario_definition(value).map(|definition| definition.scenario_key.to_string())
}

fn canonical_receiver_behavior(value: &str) -> Option<&'static str> {
    match value.trim().to_ascii_lowercase().as_str() {
        "deliver_successfully" | "success" => Some("deliver_successfully"),
        "return_server_error" | "always_500" => Some("return_server_error"),
        "simulate_timeout" | "timeout" => Some("simulate_timeout"),
        "return_client_error" | "permanent_400" => Some("return_client_error"),
        "rate_limit_then_succeed" | "rate_limit_429" => Some("rate_limit_then_succeed"),
        "fail_once_then_succeed" | "fail_then_succeed" => Some("fail_once_then_succeed"),
        _ => None,
    }
}
