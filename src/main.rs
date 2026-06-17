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
    if config.auto_run_migrations {
        db::run_schema_bootstrap(&pool).await?;
    } else {
        tracing::info!("schema bootstrap skipped; set AUTO_RUN_MIGRATIONS=1 to enable it");
    }

    delivery_worker::spawn(pool.clone(), delivery_worker::WorkerConfig::from_env());

    let app = api::router(api::AppState {
        pool,
        mock_receiver_base_url: config.mock_receiver_base_url,
        cors_allowed_origins: config.cors_allowed_origins,
        cors_allow_any_origin: config.cors_allow_any_origin,
        admin_api_key: config.admin_api_key,
    });
    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;

    tracing::info!(
        "webhook engine API listening on http://{}",
        config.bind_addr
    );

    axum::serve(listener, app).await?;

    Ok(())
}
