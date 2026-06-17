use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use chrono::{DateTime, Utc};
use url::Url;

use crate::{
    error::{AppError, AppResult},
    models::CreatePaymentRequest,
};

pub(crate) fn normalize_search(value: Option<&str>) -> Option<String> {
    let value = value?.trim();

    if value.is_empty() {
        None
    } else {
        Some(format!("%{}%", value))
    }
}

pub(crate) fn normalize_exact_filter(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty() && !value.eq_ignore_ascii_case("all"))
}

pub(crate) fn normalize_resolution_filter(value: Option<String>) -> AppResult<Option<String>> {
    let Some(value) = normalize_exact_filter(value) else {
        return Ok(None);
    };

    if matches!(value.as_str(), "resolved" | "unresolved") {
        Ok(Some(value))
    } else {
        Err(AppError::BadRequest(
            "resolution must be resolved, unresolved, or all".to_string(),
        ))
    }
}

pub(crate) fn normalize_optional_text(
    value: Option<String>,
    field: &str,
    max_len: usize,
) -> AppResult<Option<String>> {
    let Some(value) = value else {
        return Ok(None);
    };

    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }

    if value.len() > max_len {
        return Err(AppError::BadRequest(format!(
            "{} must be {} characters or fewer",
            field, max_len
        )));
    }

    Ok(Some(value.to_string()))
}

pub(crate) fn extract_search_number(value: Option<&str>) -> Option<i64> {
    let value = value?.trim();
    let mut digits = String::new();

    for character in value.chars() {
        if character.is_ascii_digit() {
            digits.push(character);
        } else if !digits.is_empty() {
            break;
        }
    }

    if digits.is_empty() {
        None
    } else {
        digits.parse().ok()
    }
}

pub(crate) fn time_range_start(value: Option<&str>) -> AppResult<Option<DateTime<Utc>>> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };

    let now = Utc::now();
    let start = match value {
        "15m" | "last_15m" => Some(now - chrono::Duration::minutes(15)),
        "1h" | "last_1h" => Some(now - chrono::Duration::hours(1)),
        "24h" | "last_24h" => Some(now - chrono::Duration::hours(24)),
        "7d" | "last_7d" => Some(now - chrono::Duration::days(7)),
        "all" | "all_time" => None,
        other => {
            return Err(AppError::BadRequest(format!(
                "unsupported time_range {}",
                other
            )));
        }
    };

    Ok(start)
}

pub(crate) fn validate_endpoint_url(url: &str) -> AppResult<()> {
    validate_endpoint_url_with_local_policy(url, allow_local_webhook_targets())
}

pub(crate) fn validate_endpoint_url_with_local_policy(
    url: &str,
    allow_local_targets: bool,
) -> AppResult<()> {
    let parsed = Url::parse(url.trim())
        .map_err(|_| AppError::BadRequest("endpoint URL must be a valid URL".to_string()))?;

    match parsed.scheme() {
        "http" | "https" => {}
        _ => {
            return Err(AppError::BadRequest(
                "endpoint URL must use http or https".to_string(),
            ));
        }
    }

    if parsed.host_str().is_none() {
        return Err(AppError::BadRequest(
            "endpoint URL must include a host".to_string(),
        ));
    }

    if !allow_local_targets && is_blocked_webhook_host(parsed.host_str().unwrap_or_default()) {
        return Err(AppError::BadRequest(
            "endpoint URL targets localhost, private, link-local, multicast, metadata, or unspecified addresses; set ALLOW_LOCAL_WEBHOOK_TARGETS=1 for local demo targets".to_string(),
        ));
    }

    Ok(())
}

fn allow_local_webhook_targets() -> bool {
    std::env::var("ALLOW_LOCAL_WEBHOOK_TARGETS")
        .map(|value| value == "1")
        .unwrap_or(false)
}

fn is_blocked_webhook_host(host: &str) -> bool {
    let host = host.trim().trim_matches(['[', ']']).to_ascii_lowercase();

    if matches!(
        host.as_str(),
        "localhost" | "metadata" | "metadata.google.internal"
    ) {
        return true;
    }

    match host.parse::<IpAddr>() {
        Ok(IpAddr::V4(addr)) => is_blocked_ipv4(addr),
        Ok(IpAddr::V6(addr)) => is_blocked_ipv6(addr),
        Err(_) => false,
    }
}

fn is_blocked_ipv4(addr: Ipv4Addr) -> bool {
    addr.is_loopback()
        || addr.is_private()
        || addr.is_link_local()
        || addr.is_multicast()
        || addr.is_unspecified()
        || addr == Ipv4Addr::new(169, 254, 169, 254)
}

fn is_blocked_ipv6(addr: Ipv6Addr) -> bool {
    addr.is_loopback()
        || addr.is_multicast()
        || addr.is_unspecified()
        || is_ipv6_unique_local(addr)
        || is_ipv6_unicast_link_local(addr)
}

fn is_ipv6_unique_local(addr: Ipv6Addr) -> bool {
    (addr.segments()[0] & 0xfe00) == 0xfc00
}

fn is_ipv6_unicast_link_local(addr: Ipv6Addr) -> bool {
    (addr.segments()[0] & 0xffc0) == 0xfe80
}

pub(crate) fn validate_max_attempts(max_attempts: Option<i64>) -> AppResult<i64> {
    let max_attempts = max_attempts.unwrap_or(5);

    if !(1..=20).contains(&max_attempts) {
        return Err(AppError::BadRequest(
            "max_attempts must be between 1 and 20".to_string(),
        ));
    }

    Ok(max_attempts)
}

pub(crate) fn validate_endpoint_count(
    endpoint_count: Option<i64>,
    default_value: i64,
) -> AppResult<i64> {
    let endpoint_count = endpoint_count.unwrap_or(default_value);

    if !(1..=20).contains(&endpoint_count) {
        return Err(AppError::BadRequest(
            "endpoint_count must be between 1 and 20".to_string(),
        ));
    }

    Ok(endpoint_count)
}

pub(crate) fn validate_payment_request(request: &CreatePaymentRequest) -> AppResult<()> {
    if request.amount < 0 {
        return Err(AppError::BadRequest(
            "amount cannot be negative".to_string(),
        ));
    }

    if request.mode_of_payment.trim().is_empty() {
        return Err(AppError::BadRequest(
            "mode_of_payment cannot be empty".to_string(),
        ));
    }

    Ok(())
}
