//! Dashboard 静态资源 handler。rust-embed 把 worktree/dashboard/ 整个目录打进二进制，
//! 路径由 `cfg.dashboard_path` 决定（默认 /p-f7077038），单页就够，没多文件。

use crate::app::AppState;
use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "dashboard/"]
struct Assets;

pub fn build_router(state: AppState) -> Router<AppState> {
    let path = state.cfg.dashboard_path.clone();
    let trailing = format!("{path}/");
    let r = Router::new()
        .route(&path, get(index_handler))
        .route(&trailing, get(index_handler));
    r
}

async fn index_handler(State(_state): State<AppState>) -> Response {
    match Assets::get("index.html") {
        Some(file) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
            file.data,
        )
            .into_response(),
        None => (StatusCode::NOT_FOUND, "dashboard not embedded").into_response(),
    }
}
