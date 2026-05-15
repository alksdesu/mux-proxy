//! Host 白名单 + cf-connecting-ip 必填校验。复刻旧 server.ts 用 444 静默拒绝，
//! 不暴露任何信息给非 CF 路径的扫描器。

use crate::app::AppState;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

const CF_CONNECTING_IP: &str = "cf-connecting-ip";

/// 占位结构体，便于上层 layer 引用名字。
pub struct HostGuard;

pub async fn host_guard_layer(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    let cfg = &state.cfg;
    if cfg.host_whitelist.is_empty() && !cfg.require_cf_connecting_ip {
        return next.run(req).await;
    }

    let host = req
        .headers()
        .get(axum::http::header::HOST)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("")
        .split(':')
        .next()
        .unwrap_or("");

    if !cfg.host_whitelist.is_empty()
        && !cfg.host_whitelist.iter().any(|h| h.eq_ignore_ascii_case(host))
    {
        return drop_connection();
    }

    if cfg.require_cf_connecting_ip && req.headers().get(CF_CONNECTING_IP).is_none() {
        return drop_connection();
    }

    next.run(req).await
}

fn drop_connection() -> Response {
    // 444 Nginx 自定义状态，含义是"不发响应直接断"。
    // axum 不能真断 TCP，但 444 在前置 CF 看是关闭信号，效果一致。
    (StatusCode::from_u16(444).unwrap_or(StatusCode::FORBIDDEN), "").into_response()
}
