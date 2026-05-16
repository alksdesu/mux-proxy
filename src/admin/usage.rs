//! /admin/usage 列表 + /admin/usage/:id 详情 + /admin/usage/ips 聚合。
//! 默认 limit=20，上限 1000；`?key=` + `?channel=` 都支持。

use crate::admin::query::{USAGE_LIMIT_DEFAULT, clamp_limit, clamp_offset, parse_channel};
use crate::app::AppState;
use crate::db;
use crate::db::schema::UsageLog;
use crate::error::{AppError, AppResult};
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize)]
pub struct UsageRowMasked {
    pub id: i64,
    pub time: String,
    pub model: String,
    pub input_tokens: i64,
    pub output_tokens: i64,
    /// 5m + 1h 总和。dashboard 旧字段，前端可直读两个明细自行重算。
    pub cache_creation_tokens: i64,
    pub cache_creation_5m_tokens: i64,
    pub cache_creation_1h_tokens: i64,
    pub cache_read_tokens: i64,
    pub key_name: String,
    pub ip: String,
    pub cost_usd: f64,
    pub channel_kind: crate::channels::ChannelKind,
}

impl From<UsageLog> for UsageRowMasked {
    fn from(u: UsageLog) -> Self {
        Self {
            id: u.id,
            time: u.time,
            model: u.model,
            input_tokens: u.input_tokens,
            output_tokens: u.output_tokens,
            cache_creation_tokens: u.cache_creation_tokens,
            cache_creation_5m_tokens: u.cache_creation_5m_tokens,
            cache_creation_1h_tokens: u.cache_creation_1h_tokens,
            cache_read_tokens: u.cache_read_tokens,
            key_name: u.key_name,
            ip: u.ip,
            cost_usd: u.cost_usd,
            channel_kind: u.channel_kind,
        }
    }
}

pub async fn list_handler(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> AppResult<axum::response::Response> {
    let channel = parse_channel(params.get("channel").map(String::as_str))?;
    let limit = clamp_limit(params.get("limit").map(String::as_str), USAGE_LIMIT_DEFAULT);
    let offset = clamp_offset(params.get("offset").map(String::as_str));
    let key = params.get("key").map(String::as_str);
    let full = matches!(params.get("full").map(String::as_str), Some("1"));

    let rows = db::usage::list_usage(&state.db, key, channel, limit, offset).await?;
    if full {
        Ok(Json(rows).into_response())
    } else {
        let lite: Vec<UsageRowMasked> = rows.into_iter().map(Into::into).collect();
        Ok(Json(lite).into_response())
    }
}

pub async fn detail_handler(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> AppResult<axum::response::Response> {
    let row = db::usage::get_usage_by_id(&state.db, id)
        .await?
        .ok_or(AppError::NotFound)?;
    Ok(Json(row).into_response())
}

pub async fn ips_handler(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> AppResult<axum::response::Response> {
    let channel = parse_channel(params.get("channel").map(String::as_str))?;
    let rows = db::stats::usage_by_ip(&state.db, channel).await?;
    Ok(Json(rows).into_response())
}
