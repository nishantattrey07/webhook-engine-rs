use crate::types::{DeliveryOutcome, WebhookPayload};
use ureq::Agent;

// tech deb- implement DeliveryError enum
// enum DeliveryError {
//     Timeout,
//     ConnectionRefused,
//     DnsFailure,
// }

pub fn test_call(
    merchant_id: u64,
    payload: WebhookPayload,
) -> Result<DeliveryOutcome, Box<dyn std::error::Error>> {
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

            return Ok(DeliveryOutcome::Timeout);
        }

        Err(err) => {
            return Err(Box::new(err));
        }
    };

    let status_code = response.status().as_u16();

    println!(
        "Merchant-id: {}      status-code: {}",
        merchant_id,
        response.status()
    );

    let outcome = match status_code {
        200..=299 => DeliveryOutcome::Success,

        429 => DeliveryOutcome::TemporaryFailure,

        500..=599 => DeliveryOutcome::TemporaryFailure,

        400..=499 => DeliveryOutcome::PermanentFailure,

        _ => DeliveryOutcome::TemporaryFailure,
    };

    Ok(outcome)
}

