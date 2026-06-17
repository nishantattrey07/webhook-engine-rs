use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use reqwest::Url;
use tokio::net::lookup_host;

pub async fn validate_delivery_target(endpoint_url: &str) -> Result<(), String> {
    let parsed = Url::parse(endpoint_url.trim())
        .map_err(|_| "endpoint URL must be a valid URL".to_string())?;

    match parsed.scheme() {
        "http" | "https" => {}
        _ => return Err("endpoint URL must use http or https".to_string()),
    }

    let host = parsed
        .host_str()
        .ok_or_else(|| "endpoint URL must include a host".to_string())?;

    if allow_local_webhook_targets() {
        return Ok(());
    }

    if is_blocked_webhook_host(host) {
        return Err("endpoint URL resolves to a blocked local or private target".to_string());
    }

    let port = parsed
        .port_or_known_default()
        .ok_or_else(|| "endpoint URL must include a valid port".to_string())?;
    let resolved = lookup_host((host, port))
        .await
        .map_err(|error| format!("failed to resolve endpoint host: {error}"))?
        .collect::<Vec<_>>();

    if resolved.is_empty() {
        return Err("endpoint host did not resolve to any addresses".to_string());
    }

    if resolved.iter().any(|address| is_blocked_ip(address.ip())) {
        return Err("endpoint URL resolves to a blocked local or private target".to_string());
    }

    Ok(())
}

fn allow_local_webhook_targets() -> bool {
    std::env::var("ALLOW_LOCAL_WEBHOOK_TARGETS")
        .map(|value| value == "1")
        .unwrap_or(false)
}

pub fn is_blocked_webhook_host(host: &str) -> bool {
    let host = host.trim().trim_matches(['[', ']']).to_ascii_lowercase();

    if matches!(
        host.as_str(),
        "localhost" | "metadata" | "metadata.google.internal"
    ) {
        return true;
    }

    host.parse::<IpAddr>().map(is_blocked_ip).unwrap_or(false)
}

fn is_blocked_ip(addr: IpAddr) -> bool {
    match addr {
        IpAddr::V4(addr) => is_blocked_ipv4(addr),
        IpAddr::V6(addr) => is_blocked_ipv6(addr),
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
