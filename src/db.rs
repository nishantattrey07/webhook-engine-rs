use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::time::{Duration, timeout};

use crate::{
    config::AppConfig,
    error::{AppError, AppResult},
};

pub async fn connect(config: &AppConfig) -> AppResult<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(config.db_max_connections)
        .connect(&config.database_url)
        .await?;

    Ok(pool)
}

pub async fn run_schema_bootstrap(pool: &PgPool) -> AppResult<()> {
    let schemas = [
        include_str!("../migrations/001_phase1_schema.sql"),
        include_str!("../migrations/002_delivery_search_indexes.sql"),
        include_str!("../migrations/003_scenario_run_planning.sql"),
    ];

    for schema in schemas {
        for statement in split_sql_statements(schema) {
            let preview = statement_preview(&statement);
            tracing::debug!("running schema statement: {}", preview);

            timeout(
                Duration::from_secs(10),
                sqlx::query(&statement).execute(pool),
            )
            .await
            .map_err(|_| AppError::SchemaBootstrapTimeout(preview))??;
        }
    }

    Ok(())
}

fn statement_preview(statement: &str) -> String {
    statement
        .split_whitespace()
        .take(12)
        .collect::<Vec<_>>()
        .join(" ")
}

fn split_sql_statements(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut in_do_block = false;

    for line in sql.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("DO $$") {
            in_do_block = true;
        }

        current.push_str(line);
        current.push('\n');

        if in_do_block {
            if trimmed == "END $$;" {
                statements.push(current.trim().to_string());
                current.clear();
                in_do_block = false;
            }
        } else if trimmed.ends_with(';') {
            statements.push(current.trim().to_string());
            current.clear();
        }
    }

    if !current.trim().is_empty() {
        statements.push(current.trim().to_string());
    }

    statements
}
