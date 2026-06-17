#![allow(dead_code)]

use std::sync::OnceLock;

use axum::{Json, Router, extract::State, routing::post};
use serde_json::{Value, json};
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::{
    net::TcpListener,
    sync::{Mutex, oneshot},
};
use webhook_engine::db;

static TEST_DB_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

pub async fn acquire_db_lock() -> tokio::sync::MutexGuard<'static, ()> {
    TEST_DB_LOCK.get_or_init(|| Mutex::new(())).lock().await
}

pub async fn test_pool() -> Option<PgPool> {
    let database_url = match std::env::var("TEST_DATABASE_URL") {
        Ok(value) => value,
        Err(_) => {
            eprintln!("skipping db-backed scenario test; TEST_DATABASE_URL is not set");
            return None;
        }
    };

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("connect test database");
    db::run_schema_bootstrap(&pool)
        .await
        .expect("bootstrap schema");
    reset_db(&pool).await.expect("reset test database");
    Some(pool)
}

pub async fn reset_db(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "TRUNCATE TABLE
            scenario_run_endpoints,
            scenario_run_expectations,
            delivery_trace_events,
            delivery_attempts,
            webhook_deliveries,
            webhook_endpoint_subscriptions,
            webhook_endpoint_secrets,
            webhook_endpoints,
            scenarios,
            domain_events,
            payments
         CASCADE",
    )
    .execute(pool)
    .await?;

    Ok(())
}

pub struct MockReceiver {
    pub base_url: String,
    pub requests: std::sync::Arc<Mutex<Vec<Value>>>,
    shutdown: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<()>,
}

impl MockReceiver {
    pub async fn shutdown(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let _ = self.task.await;
    }
}

pub async fn spawn_mock_receiver() -> MockReceiver {
    let requests = std::sync::Arc::new(Mutex::new(Vec::<Value>::new()));
    let app = Router::new()
        .route("/admin/endpoints", post(record_endpoint))
        .with_state(requests.clone());

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock receiver");
    let address = listener.local_addr().expect("mock receiver local addr");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("serve mock receiver");
    });

    MockReceiver {
        base_url: format!("http://{}", address),
        requests,
        shutdown: Some(shutdown_tx),
        task,
    }
}

async fn record_endpoint(
    State(requests): State<std::sync::Arc<Mutex<Vec<Value>>>>,
    Json(payload): Json<Value>,
) -> Json<Value> {
    requests.lock().await.push(payload);
    Json(json!({ "success": true }))
}
