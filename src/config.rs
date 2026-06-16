use std::net::{IpAddr, Ipv4Addr, SocketAddr};

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub database_url: String,
    pub bind_addr: SocketAddr,
    pub db_max_connections: u32,
    pub mock_receiver_base_url: String,
}

impl AppConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        let database_url =
            std::env::var("DATABASE_URL").map_err(|_| ConfigError::MissingEnv("DATABASE_URL"))?;

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

        Ok(Self {
            database_url,
            bind_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port),
            db_max_connections,
            mock_receiver_base_url,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("missing required environment variable {0}")]
    MissingEnv(&'static str),
}
