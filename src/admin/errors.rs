//! /admin/errors 列表 + 详情 + 批量删除。`response_body` 以 `[local]` 前缀的是本地拒绝。

use crate::admin::query::{ERROR_LIMIT_DEFAULT, clamp_limit, clamp_offset, parse_channel};
use crate::app::AppState;
use crate::db;
use crate::db::schema::ErrorLog;
use crate::error::{AppError, AppResult};
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize)]
pub struct ErrorRowMasked {
    pub id: i64,
    pub time: String,
    pub key_name: String,
    pub status: i32,
    pub path: String,
    pub model: String,
    pub ip: String,
    pub is_local: bool,
    pub channel_kind: crate::channels::ChannelKind,
}

impl From<ErrorLog> for ErrorRowMasked {
    fn from(e: ErrorLog) -> Self {
        let is_local = e.response_body.starts_with("[local]");
        Self {
            id: e.id,
            time: e.time,
            key_name: e.key_name,
            status: e.status,
            path: e.path,
            model: e.model,
            ip: e.ip,
            is_local,
            channel_kind: e.channel_kind,
        }
    }
}

pub async fn list_handler(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> AppResult<axum::response::Response> {
    let channel = parse_channel(params.get("channel").map(String::as_str))?;
    let limit = clamp_limit(params.get("limit").map(String::as_str), ERROR_LIMIT_DEFAULT);
    let offset = clamp_offset(params.get("offset").map(String::as_str));
    let key = params.get("key").map(String::as_str);
    let full = matches!(params.get("full").map(String::as_str), Some("1"));

    let rows = db::errors::list_errors(&state.db, key, channel, limit, offset).await?;
    if full {
        Ok(Json(rows).into_response())
    } else {
        let lite: Vec<ErrorRowMasked> = rows.into_iter().map(Into::into).collect();
        Ok(Json(lite).into_response())
    }
}

pub async fn detail_handler(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> AppResult<axum::response::Response> {
    let row = db::errors::get_error_by_id(&state.db, id)
        .await?
        .ok_or(AppError::NotFound)?;
    Ok(Json(row).into_response())
}

pub async fn delete_handler(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> AppResult<axum::response::Response> {
    let channel = parse_channel(params.get("channel").map(String::as_str))?;
    let key = params.get("key").map(String::as_str);
    let n = db::errors::delete_all(&state.db, key, channel).await?;
    Ok(Json(serde_json::json!({ "ok": true, "deleted": n })).into_response())
}
