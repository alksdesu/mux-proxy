//! /admin/stats/timeseries：按小时桶聚合，支持 ?hours= 和 ?channel=。

use crate::admin::query::parse_channel;
use crate::app::AppState;
use crate::db;
use crate::error::AppResult;
use axum::Json;
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use std::collections::HashMap;

const HOURS_DEFAULT: i64 = 24;
const HOURS_MAX: i64 = 24 * 30;

pub async fn handler(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> AppResult<axum::response::Response> {
    let channel = parse_channel(params.get("channel").map(String::as_str))?;
    let hours: i64 = params
        .get("hours")
        .and_then(|s| s.parse().ok())
        .unwrap_or(HOURS_DEFAULT)
        .clamp(1, HOURS_MAX);
    let rows = db::stats::timeseries(&state.db, hours, channel).await?;
    Ok(Json(rows).into_response())
}
