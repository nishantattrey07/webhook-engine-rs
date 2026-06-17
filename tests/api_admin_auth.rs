use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use sqlx::postgres::PgPoolOptions;
use tower::ServiceExt;
use webhook_engine::api::{self, AppState};

#[tokio::test]
async fn admin_api_key_protects_mutating_routes_but_not_health() {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://postgres:postgres@127.0.0.1:5432/postgres")
        .expect("lazy pool does not connect");
    let app = api::router(AppState {
        pool,
        mock_receiver_base_url: "http://127.0.0.1:3000".to_string(),
        cors_allowed_origins: vec!["http://localhost:3000".to_string()],
        cors_allow_any_origin: false,
        admin_api_key: Some("test-admin-key".to_string()),
    });

    let health = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/health")
                .body(Body::empty())
                .expect("health request"),
        )
        .await
        .expect("health response");
    assert_eq!(health.status(), StatusCode::OK);

    let unauthorized = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/payments")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "merchant_id": 1,
                        "order_id": 1,
                        "amount": 100,
                        "status": "succeeded",
                        "mode_of_payment": "test"
                    })
                    .to_string(),
                ))
                .expect("protected request"),
        )
        .await
        .expect("protected response");

    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
}
