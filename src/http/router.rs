//! axum Router 装配：各业务模块把自己的子路由 merge 进来。

use crate::app::AppState;
use axum::Router;
use axum::routing::get;

pub fn build(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(health))
        .with_state(state)
}

async fn health() -> &'static str {
    "ok"
}
