//! Dashboard 静态资源 handler。rust-embed 把 worktree/dashboard/ 整个目录打进二进制，
//! 根路径由 `cfg.dashboard_path` 决定，子路径返回内嵌的 vendor / css / js 资源。

use crate::app::AppState;
use axum::Router;
use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::get;
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "dashboard/"]
struct Assets;

pub fn build_router(state: AppState) -> Router<AppState> {
    let path = state.cfg.dashboard_path.clone();
    let trailing = format!("{path}/");
    let asset_pattern = format!("{path}/*asset");
    let redirect_target = trailing.clone();
    Router::new()
        .route(&path, get(move || {
            let target = redirect_target.clone();
            async move { Redirect::permanent(&target) }
        }))
        .route(&trailing, get(index_handler))
        .route(&asset_pattern, get(asset_handler))
}

async fn index_handler(State(_state): State<AppState>) -> Response {
    serve_asset("index.html")
}

async fn asset_handler(Path(asset): Path<String>) -> Response {
    serve_asset(&asset)
}

fn serve_asset(path: &str) -> Response {
    // rust-embed 用 / 分隔，请求侧 / 与 \ 都接受 → 归一化。
    let normalized = path.replace('\\', "/");
    match Assets::get(&normalized) {
        Some(file) => {
            let mime = mime_guess::from_path(&normalized).first_or_octet_stream();
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, mime.essence_str().to_string())],
                file.data,
            )
                .into_response()
        }
        None => (StatusCode::NOT_FOUND, "asset not embedded").into_response(),
    }
}
