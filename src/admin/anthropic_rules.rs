//! Anthropic rewrite rules CRUD：list/create/patch/delete。每次写完调
//! ``app::reload_anthropic_rules`` 把 DB 新状态原子换进 ``AppState.anthropic_rules``，
//! 同时 ``snapshot.bump`` 让 WS 客户端知道有变化。

use crate::admin::query::parse_id_required;
use crate::app::{AppState, reload_anthropic_rules};
use crate::db::anthropic_rules::{self, RewriteRulePatch};
use crate::error::{AppError, AppResult};
use axum::Json;
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
pub struct CreateBody {
    pub prefix: String,
    pub target: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

fn default_enabled() -> bool {
    true
}

pub async fn list_handler(
    State(state): State<AppState>,
) -> AppResult<axum::response::Response> {
    let rows = anthropic_rules::list_all(&state.db).await?;
    Ok(Json(rows).into_response())
}

pub async fn create_handler(
    State(state): State<AppState>,
    Json(body): Json<CreateBody>,
) -> AppResult<axum::response::Response> {
    let prefix = body.prefix.trim();
    let target = body.target.trim();
    if prefix.is_empty() || target.is_empty() {
        return Err(AppError::BadRequest(
            "prefix and target are required and non-empty".into(),
        ));
    }
    let row = anthropic_rules::create(&state.db, prefix, target, body.enabled).await?;
    reload_anthropic_rules(&state).await?;
    Ok(Json(json!({"ok": true, "rule": row})).into_response())
}

pub async fn patch_handler(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
    Json(patch): Json<RewriteRulePatch>,
) -> AppResult<axum::response::Response> {
    let id = parse_id_required(params.get("id").map(String::as_str))?;
    if let Some(prefix) = patch.prefix.as_deref() {
        if prefix.trim().is_empty() {
            return Err(AppError::BadRequest("prefix cannot be empty".into()));
        }
    }
    if let Some(target) = patch.target.as_deref() {
        if target.trim().is_empty() {
            return Err(AppError::BadRequest("target cannot be empty".into()));
        }
    }
    let row = anthropic_rules::update(&state.db, id, patch).await?;
    match row {
        Some(r) => {
            reload_anthropic_rules(&state).await?;
            Ok(Json(json!({"ok": true, "rule": r})).into_response())
        }
        None => Err(AppError::NotFound),
    }
}

pub async fn delete_handler(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> AppResult<axum::response::Response> {
    let id = parse_id_required(params.get("id").map(String::as_str))?;
    let deleted = anthropic_rules::delete(&state.db, id).await?;
    if !deleted {
        return Err(AppError::NotFound);
    }
    reload_anthropic_rules(&state).await?;
    Ok(Json(json!({"ok": true})).into_response())
}
