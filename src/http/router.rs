//! axum Router 装配：healthz、admin 子树、dashboard 静态、host_guard 全局兜底。

use crate::admin;
use crate::app::AppState;
use crate::dashboard;
use crate::http::middleware::host_guard::host_guard_layer;
use crate::http::middleware::trace::trace_id_layer;
use axum::Router;
use axum::middleware::{from_fn, from_fn_with_state};
use axum::routing::get;

pub fn build(state: AppState) -> Router {
    let admin_router = admin::build_admin_router(state.clone());
    let dashboard_router = dashboard::build_router(state.clone());

    Router::new()
        .route("/healthz", get(health))
        .merge(admin_router)
        .merge(dashboard_router)
        .layer(from_fn(trace_id_layer))
        .layer(from_fn_with_state(state.clone(), host_guard_layer))
        .with_state(state)
}

async fn health() -> &'static str {
    "ok"
}
