//! 客户端 sk-xxx 鉴权：从 `x-api-key` 或 `Authorization: Bearer` 取 raw key，
//! 经 KeyCache + SingleFlight 拿 `KeyCacheEntry`，挂到 request extensions。
//! 命中失败返 401（与 admin 路径返 404 形态不同，这里是终端用户接口，可见错误）。

use crate::app::AppState;
use crate::auth::{KEY_CACHE_TTL, KeyCacheEntry};
use crate::db;
use crate::error::AppError;
use axum::extract::Request;
use axum::http::HeaderMap;
use std::sync::Arc;
use std::time::Instant;

const HDR_API_KEY: &str = "x-api-key";

/// 占位类型，便于在 router 里以名字挂上 layer。
pub struct ClientAuth;

pub fn extract_raw_key(headers: &HeaderMap) -> Option<String> {
    if let Some(v) = headers.get(HDR_API_KEY).and_then(|h| h.to_str().ok()) {
        let trimmed = v.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    let auth = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())?;
    let trimmed = auth.trim_start_matches("Bearer ").trim();
    if trimmed.is_empty() { None } else { Some(trimmed.to_string()) }
}

/// 走 cache → singleflight → DB 拿一份 KeyCacheEntry。
/// 命中负缓存（DB 也没有）返 None；DB 报错向上抛 AppError。
pub async fn resolve_client_key(
    state: &AppState,
    raw_key: &str,
) -> Result<Option<KeyCacheEntry>, AppError> {
    if let Some(hit) = state.key_cache.get_fresh(raw_key) {
        return Ok(Some(hit));
    }

    let db = state.db.clone();
    let raw = raw_key.to_string();
    let loaded = state
        .key_loader_sf
        .run(raw.clone(), move || async move {
            let row = db::keys::find_by_key(&db, &raw).await?;
            Ok(row.map(|k| KeyCacheEntry {
                id: k.id,
                name: k.name,
                upstream_key: k.upstream_key,
                quota: k.quota,
                allow_fast: k.allow_fast,
                max_concurrency: k.max_concurrency,
                rpm_limit: k.rpm_limit,
                allowed_models: crate::auth::key_cache::parse_allowed_models(&k.allowed_models),
                channel_kind: k.channel_kind,
                fetched_at: Instant::now(),
            }))
        })
        .await
        .map_err(|arc| match Arc::try_unwrap(arc) {
            Ok(e) => e,
            Err(arc) => AppError::Internal(arc.to_string()),
        })?;

    if let Some(entry) = loaded.as_ref() {
        state.key_cache.insert(raw_key.to_string(), entry.clone());
    }
    Ok(loaded)
}

/// 用 axum middleware 形态：拿 raw key、解析、放进 extensions、放行；
/// 解析失败或缺 key 返 401。`/v1/*` 之类终端用户接口才挂这一层，
/// admin 走 admin_auth 的 404 分支。
pub async fn client_auth_layer(
    axum::extract::State(state): axum::extract::State<AppState>,
    mut req: Request,
    next: axum::middleware::Next,
) -> Result<axum::response::Response, AppError> {
    let raw = extract_raw_key(req.headers()).ok_or(AppError::Unauthorized)?;
    let entry = resolve_client_key(&state, &raw)
        .await?
        .ok_or(AppError::Unauthorized)?;
    req.extensions_mut().insert(entry);
    Ok(next.run(req).await)
}

/// 给单元测试和 admin 端点用的常量参考。
pub const CLIENT_KEY_TTL: std::time::Duration = KEY_CACHE_TTL;

#[cfg(test)]
mod tests {
    use super::*;

    fn make_headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(
                axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                axum::http::HeaderValue::from_str(v).unwrap(),
            );
        }
        h
    }

    #[test]
    fn extract_prefers_x_api_key() {
        let h = make_headers(&[
            ("x-api-key", "sk-xkey"),
            ("authorization", "Bearer sk-bearer"),
        ]);
        assert_eq!(extract_raw_key(&h).as_deref(), Some("sk-xkey"));
    }

    #[test]
    fn extract_falls_back_to_bearer() {
        let h = make_headers(&[("authorization", "Bearer sk-bearer ")]);
        assert_eq!(extract_raw_key(&h).as_deref(), Some("sk-bearer"));
    }

    #[test]
    fn extract_none_when_empty() {
        let h = HeaderMap::new();
        assert!(extract_raw_key(&h).is_none());
    }

    #[test]
    fn extract_ignores_blank_x_api_key() {
        let h = make_headers(&[("x-api-key", "   "), ("authorization", "Bearer sk-real")]);
        assert_eq!(extract_raw_key(&h).as_deref(), Some("sk-real"));
    }
}
