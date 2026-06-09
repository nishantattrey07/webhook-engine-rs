use crate::types::{DeliveryOutcome, WebhookPayload};
use ureq::Agent;

// tech debt - implement DeliveryError enum
// enum DeliveryError {
//     Timeout,
//     ConnectionRefused,
//     DnsFailure,
// }

pub fn send_webhook(
    merchant_id: u64,
    payload: WebhookPayload,
) -> (DeliveryOutcome, Option<u16>) {
    let url = format!(
        "http://0.0.0.0:3000/webhook/{}",
        merchant_id
    );

    let agent: Agent = Agent::config_builder()
        .http_status_as_error(false)
        .timeout_global(Some(std::time::Duration::from_secs(20)))
        .build()
        .into();

    let response = match agent
        .post(&url)
        .header("Content-Type", "application/json")
        .send_json(&payload)
    {
        Ok(response) => response,

        Err(ureq::Error::Timeout(_)) => {
            println!(
                "Merchant-id: {}      status-code: TIMEOUT",
                merchant_id
            );

            return (
                DeliveryOutcome::Timeout,
                None,
            );
        }

        Err(err) => {
            println!(
                "Merchant-id: {}      transport-error: {}",
                merchant_id,
                err
            );

            return (
                DeliveryOutcome::TemporaryFailure,
                None,
            );
        }
    };

    let http_status = response.status().as_u16();

    println!(
        "Merchant-id: {}   Event-id:{}   status-code: {}",
        merchant_id,
        payload.event_id,
        response.status()
    );

    match http_status {
        200..=299 => (
            DeliveryOutcome::Success,
            Some(http_status),
        ),

        429 => (
            DeliveryOutcome::TemporaryFailure,
            Some(http_status),
        ),

        500..=599 => (
            DeliveryOutcome::TemporaryFailure,
            Some(http_status),
        ),

        400..=499 => (
            DeliveryOutcome::PermanentFailure,
            Some(http_status),
        ),

        _ => (
            DeliveryOutcome::TemporaryFailure,
            Some(http_status),
        ),
    }
}
