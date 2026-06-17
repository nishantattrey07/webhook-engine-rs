use std::time::Duration;

use reqwest::Client;

pub fn build_http_client(request_timeout: Duration) -> Option<Client> {
    match Client::builder()
        .timeout(request_timeout)
        .redirect(reqwest::redirect::Policy::none())
        .build()
    {
        Ok(client) => Some(client),
        Err(error) => {
            tracing::error!(%error, "failed to build webhook HTTP client");
            None
        }
    }
}

pub fn processing_lease_duration(request_timeout: Duration) -> chrono::Duration {
    let timeout = chrono::Duration::from_std(request_timeout)
        .unwrap_or_else(|_| chrono::Duration::seconds(3));
    timeout + chrono::Duration::seconds(30)
}
