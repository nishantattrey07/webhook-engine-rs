use serde_json::json;

use super::{payment_status_input_for_event_type, validate_endpoint_count, validate_max_attempts};
use crate::{
    error::{AppError, AppResult},
    models::{PaymentStatusInput, ReceiverBehaviorOption, ScenarioCatalogItem, ScenarioRunConfig},
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
    let payment_count = config.payment_count.unwrap_or(1).clamp(1, 100);
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

pub fn plan_scenario_endpoints(
    scenario_key: &str,
    config: &ScenarioRunConfig,
    event_type: &str,
) -> AppResult<Vec<PlannedScenarioEndpoint>> {
    let mut plan_config = config.clone();
    plan_config.event_type = Some(event_type.to_string());
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
