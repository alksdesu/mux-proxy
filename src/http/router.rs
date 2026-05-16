//! axum Router 装配：healthz、admin 子树、dashboard 静态、客户端 /v1/* 分派、
//! host_guard 全局兜底。

use crate::admin;
use crate::app::AppState;
use crate::dashboard;
use crate::http::middleware::host_guard::host_guard_layer;
use crate::http::middleware::trace::trace_id_layer;
use crate::http::v1::build_v1_router;
use axum::Router;
use axum::middleware::{from_fn, from_fn_with_state};
use axum::routing::get;

pub fn build(state: AppState) -> Router {
    let admin_router = admin::build_admin_router(state.clone());
    let dashboard_router = dashboard::build_router(state.clone());
    let v1_router = build_v1_router(state.clone());

    Router::new()
        .route("/healthz", get(health))
        .route("/metrics", get(metrics_export))
        .merge(admin_router)
        .merge(dashboard_router)
        .merge(v1_router)
        .layer(from_fn(trace_id_layer))
        .layer(from_fn_with_state(state.clone(), host_guard_layer))
        .with_state(state)
}

async fn health() -> &'static str {
    "ok"
}

async fn metrics_export() -> axum::response::Response {
    use axum::http::{StatusCode, header};
    use axum::response::IntoResponse;
    match crate::metrics::GLOBAL.encode_text() {
        Ok(text) => ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], text).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("metrics encode failed: {e}"))
            .into_response(),
    }
}
