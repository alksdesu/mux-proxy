//! /admin/keys CRUD：list / list_full / create / patch / delete。
//! POST 没显式指定 channel_kind 时按 upstream_key 前缀推断，
//! PATCH 改名同步 usage_logs / error_logs / spend_cache。

use crate::admin::query::{clamp_limit, clamp_offset, parse_channel, parse_id_required};
use crate::app::AppState;
use crate::channels::{ChannelKind, route_by_upstream_key};
use crate::db;
use crate::db::schema::{ApiKey, ApiKeyPatch};
use crate::error::{AppError, AppResult};
use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize)]
pub struct KeyListItemMasked {
    pub id: i64,
    pub key: String,
    pub name: String,
    pub quota: Value,
    pub allow_fast: bool,
    pub max_concurrency: Value,
    pub current_concurrency: u32,
    pub rpm_limit: i64,
    pub rpm_current: u32,
    pub used: String,
    pub created_at: String,
    pub channel_kind: ChannelKind,
}

#[derive(Debug, Clone, Serialize)]
pub struct KeyListItemFull {
    pub id: i64,
    pub key: String,
    pub name: String,
    pub upstream_key: String,
    pub quota: f64,
    pub quota_display: String,
    pub allow_fast: bool,
    pub max_concurrency: i64,
    pub current_concurrency: u32,
    pub rpm_limit: i64,
    pub rpm_current: u32,
    pub used: f64,
    pub used_display: String,
    pub created_at: String,
    pub channel_kind: ChannelKind,
}

#[derive(Debug, Deserialize)]
pub struct CreateBody {
    pub name: String,
    pub upstream_key: String,
    #[serde(default = "default_quota")]
    pub quota: f64,
    #[serde(default = "default_allow_fast")]
    pub allow_fast: bool,
    #[serde(default = "default_max_concurrency")]
    pub max_concurrency: i64,
    #[serde(default = "default_rpm_limit")]
    pub rpm_limit: i64,
    pub channel_kind: Option<ChannelKind>,
}

fn default_quota() -> f64 { -1.0 }
fn default_allow_fast() -> bool { true }
fn default_max_concurrency() -> i64 { -1 }
fn default_rpm_limit() -> i64 { -1 }

pub async fn list_handler(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> AppResult<axum::response::Response> {
    let channel = parse_channel(params.get("channel").map(String::as_str))?;
    let limit = clamp_limit(params.get("limit").map(String::as_str), 1000);
    let offset = clamp_offset(params.get("offset").map(String::as_str));
    let full = matches!(params.get("full").map(String::as_str), Some("1"));

    let keys = db::keys::list(&state.db, channel, limit, offset).await?;
    if full {
        let body: Vec<KeyListItemFull> = keys.into_iter().map(|k| render_full(&state, k)).collect();
        Ok(Json(body).into_response())
    } else {
        let body: Vec<KeyListItemMasked> = keys.into_iter().map(|k| render_masked(&state, k)).collect();
        Ok(Json(body).into_response())
    }
}

pub async fn list_full_handler(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> AppResult<axum::response::Response> {
    let channel = parse_channel(params.get("channel").map(String::as_str))?;
    let limit = clamp_limit(params.get("limit").map(String::as_str), 1000);
    let offset = clamp_offset(params.get("offset").map(String::as_str));
    let keys = db::keys::list(&state.db, channel, limit, offset).await?;
    let body: Vec<KeyListItemFull> = keys.into_iter().map(|k| render_full(&state, k)).collect();
    Ok(Json(body).into_response())
}

fn render_full(state: &AppState, k: ApiKey) -> KeyListItemFull {
    let used = state.spend.get(&k.name);
    KeyListItemFull {
        id: k.id,
        key: k.key,
        name: k.name.clone(),
        upstream_key: k.upstream_key,
        quota: k.quota,
        quota_display: if k.quota < 0.0 { "unlimited".to_string() } else { format!("${:.2}", k.quota) },
        allow_fast: k.allow_fast,
        max_concurrency: k.max_concurrency,
        current_concurrency: state.limiter.current(&k.name),
        rpm_limit: k.rpm_limit,
        rpm_current: state.rate_limiter.current(&k.name) as u32,
        used,
        used_display: format!("${:.2}", used),
        created_at: k.created_at,
        channel_kind: k.channel_kind,
    }
}

fn render_masked(state: &AppState, k: ApiKey) -> KeyListItemMasked {
    let used = state.spend.get(&k.name);
    KeyListItemMasked {
        id: k.id,
        key: mask_key(&k.key),
        name: k.name.clone(),
        quota: quota_to_value(k.quota),
        allow_fast: k.allow_fast,
        max_concurrency: max_concurrency_to_value(k.max_concurrency),
        current_concurrency: state.limiter.current(&k.name),
        rpm_limit: k.rpm_limit,
        rpm_current: state.rate_limiter.current(&k.name) as u32,
        used: format!("${:.2}", used),
        created_at: k.created_at,
        channel_kind: k.channel_kind,
    }
}

fn mask_key(raw: &str) -> String {
    if raw.len() <= 12 {
        return raw.to_string();
    }
    let head: String = raw.chars().take(8).collect();
    let tail: String = raw.chars().rev().take(4).collect::<Vec<_>>().into_iter().rev().collect();
    format!("{head}...{tail}")
}

fn quota_to_value(quota: f64) -> Value {
    if quota < 0.0 { json!("unlimited") } else { json!(format!("${:.2}", quota)) }
}

fn max_concurrency_to_value(max: i64) -> Value {
    if max < 0 { json!("unlimited") } else { json!(max) }
}

pub async fn create_handler(
    State(state): State<AppState>,
    Json(body): Json<CreateBody>,
) -> AppResult<axum::response::Response> {
    let name = body.name.trim();
    let upstream = body.upstream_key.trim();
    if name.is_empty() || upstream.is_empty() {
        return Err(AppError::BadRequest("name and upstream_key are required".into()));
    }

    let inferred = route_by_upstream_key(upstream);
    let channel = match body.channel_kind {
        Some(explicit) if explicit != inferred => {
            return Err(AppError::BadRequest(format!(
                "channel_kind={explicit} conflicts with upstream_key prefix (inferred {inferred})"
            )));
        }
        Some(explicit) => explicit,
        None => inferred,
    };

    let created = db::keys::create(
        &state.db,
        name,
        upstream,
        body.quota,
        body.allow_fast,
        body.max_concurrency,
        body.rpm_limit,
        channel,
    )
    .await?;
    state.snapshot.bump();
    Ok(Json(json!({
        "ok": true,
        "id": created.id,
        "key": created.key,
        "name": created.name,
        "quota": created.quota,
        "allow_fast": created.allow_fast,
        "max_concurrency": created.max_concurrency,
        "channel_kind": created.channel_kind,
    }))
    .into_response())
}

pub async fn patch_handler(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
    Json(patch): Json<ApiKeyPatch>,
) -> AppResult<axum::response::Response> {
    let id = parse_id_required(params.get("id").map(String::as_str))?;

    let existing = db::keys::find_by_id(&state.db, id)
        .await?
        .ok_or(AppError::NotFound)?;

    if let (Some(uk), Some(ch_explicit)) = (patch.upstream_key.as_deref(), patch.channel_kind) {
        let inferred = route_by_upstream_key(uk);
        if inferred != ch_explicit {
            return Err(AppError::BadRequest(format!(
                "channel_kind={ch_explicit} conflicts with upstream_key prefix (inferred {inferred})"
            )));
        }
    }

    let updated = db::keys::update(&state.db, id, patch.clone())
        .await?
        .ok_or(AppError::NotFound)?;

    if let Some(new_name) = patch.name.as_deref() {
        if !new_name.is_empty() && new_name != existing.name {
            state.spend.rename(&existing.name, new_name);
        }
    }
    state.key_cache.invalidate(&existing.key);
    state.snapshot.bump();
    Ok(Json(json!({
        "ok": true,
        "id": updated.id,
        "name": updated.name,
        "channel_kind": updated.channel_kind,
    }))
    .into_response())
}

pub async fn delete_handler(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> AppResult<axum::response::Response> {
    let id = parse_id_required(params.get("id").map(String::as_str))?;
    let existing = db::keys::find_by_id(&state.db, id)
        .await?
        .ok_or(AppError::NotFound)?;
    let deleted_name = db::keys::delete(&state.db, id).await?;
    if let Some(name) = deleted_name {
        state.spend.drop_key(&name);
    }
    state.key_cache.invalidate(&existing.key);
    state.snapshot.bump();
    Ok((StatusCode::OK, Json(json!({ "ok": true }))).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mask_key_short_passthrough() {
        assert_eq!(mask_key("sk-x"), "sk-x");
        assert_eq!(mask_key("123456789012"), "123456789012");
    }

    #[test]
    fn mask_key_long_format() {
        let raw = "sk-abcdef0123456789abcdef0123456789";
        let masked = mask_key(raw);
        assert!(masked.starts_with("sk-abcde"));
        assert!(masked.contains("..."));
        assert!(masked.ends_with("6789"));
    }

    #[test]
    fn quota_value_unlimited_when_negative() {
        let v = quota_to_value(-1.0);
        assert_eq!(v, json!("unlimited"));
    }

    #[test]
    fn quota_value_dollar_format() {
        let v = quota_to_value(100.5);
        assert_eq!(v, json!("$100.50"));
    }

    #[test]
    fn max_concurrency_unlimited_when_negative() {
        assert_eq!(max_concurrency_to_value(-1), json!("unlimited"));
        assert_eq!(max_concurrency_to_value(0), json!(0));
        assert_eq!(max_concurrency_to_value(5), json!(5));
    }
}
