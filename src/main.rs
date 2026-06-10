use webhook_engine::{api, config::AppConfig, db, delivery_worker};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "webhook_engine=info,tower_http=info".into()),
        )
        .init();

    let config = AppConfig::from_env()?;
    let pool = db::connect(&config).await?;
    db::run_schema_bootstrap(&pool).await?;

    delivery_worker::spawn(pool.clone(), delivery_worker::WorkerConfig::from_env());

    let app = api::router(api::AppState { pool });
    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;

    tracing::info!(
        "webhook engine API listening on http://{}",
        config.bind_addr
    );

    axum::serve(listener, app).await?;

    Ok(())
}
