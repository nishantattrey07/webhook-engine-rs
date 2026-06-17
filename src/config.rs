use std::net::{IpAddr, Ipv4Addr, SocketAddr};

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub database_url: String,
    pub bind_addr: SocketAddr,
    pub db_max_connections: u32,
    pub mock_receiver_base_url: String,
    pub cors_allowed_origins: Vec<String>,
    pub cors_allow_any_origin: bool,
    pub admin_api_key: Option<String>,
    pub auto_run_migrations: bool,
}

impl AppConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        let database_url =
            std::env::var("DATABASE_URL").map_err(|_| ConfigError::MissingEnv("DATABASE_URL"))?;

        let bind_host = std::env::var("BIND_HOST")
            .unwrap_or_else(|_| Ipv4Addr::LOCALHOST.to_string())
            .parse::<IpAddr>()
            .map_err(|_| ConfigError::InvalidEnv("BIND_HOST"))?;

        let port = std::env::var("PORT")
            .ok()
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or(8080);

        let db_max_connections = std::env::var("DB_MAX_CONNECTIONS")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(5);

        let mock_receiver_base_url = std::env::var("MOCK_RECEIVER_BASE_URL")
            .or_else(|_| std::env::var("RECEIVER_BASE"))
            .unwrap_or_else(|_| "http://127.0.0.1:3000".to_string());

        let cors_allowed_origins = std::env::var("CORS_ALLOWED_ORIGINS")
            .unwrap_or_else(|_| "http://localhost:3000,http://127.0.0.1:3000".to_string())
            .split(',')
            .map(str::trim)
            .filter(|origin| !origin.is_empty())
            .map(ToString::to_string)
            .collect();

        let admin_api_key = std::env::var("ADMIN_API_KEY")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());

        Ok(Self {
            database_url,
            bind_addr: SocketAddr::new(bind_host, port),
            db_max_connections,
            mock_receiver_base_url,
            cors_allowed_origins,
            cors_allow_any_origin: env_flag("CORS_ALLOW_ANY_ORIGIN", false),
            admin_api_key,
            auto_run_migrations: env_flag("AUTO_RUN_MIGRATIONS", false),
        })
    }
}

fn env_flag(name: &'static str, default: bool) -> bool {
    std::env::var(name)
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes"
            )
        })
        .unwrap_or(default)
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("missing required environment variable {0}")]
    MissingEnv(&'static str),

    #[error("invalid value for environment variable {0}")]
    InvalidEnv(&'static str),
}
