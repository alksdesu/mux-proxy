//! /stats 全局聚合 + /stats/reset。返回结构按渠道分桶，
//! 兼容旧 dashboard 的 totalRequests / totalErrors / byModel / byKey 顶层字段。

use crate::admin::query::parse_channel;
use crate::app::AppState;
use crate::billing::{anthropic_rate, copilot_rate};
use crate::channels::ChannelKind;
use crate::db;
use crate::error::AppResult;
use axum::Json;
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use serde::Serialize;
use serde_json::{Value, json};
use sqlx::Row;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Default)]
pub struct ChannelStats {
    pub total_requests: i64,
    pub total_errors: i64,
    pub total_cost: f64,
    pub standard_cost: f64,
    pub fast_cost: f64,
    pub cache_saved: f64,
}

pub async fn stats_handler(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> AppResult<axum::response::Response> {
    let channel = parse_channel(params.get("channel").map(String::as_str))?;
    let body = collect(&state, channel).await?;
    Ok(Json(body).into_response())
}

pub async fn reset_handler(State(state): State<AppState>) -> AppResult<axum::response::Response> {
    sqlx::query("DELETE FROM usage_logs").execute(state.db.pool()).await?;
    let snapshot = state.spend.snapshot();
    for k in snapshot.keys() {
        state.spend.drop_key(k);
    }
    state.snapshot.bump();
    Ok(Json(json!({ "ok": true, "message": "stats cleared" })).into_response())
}

async fn collect(state: &AppState, channel_filter: Option<ChannelKind>) -> AppResult<Value> {
    let pool = state.db.pool();

    let totals_rows = sqlx::query(
        "SELECT channel_kind, \
                COUNT(*) AS requests, \
                COALESCE(SUM(input_tokens), 0)::BIGINT AS input_tokens, \
                COALESCE(SUM(output_tokens), 0)::BIGINT AS output_tokens, \
                COALESCE(SUM(cache_creation_tokens), 0)::BIGINT AS cache_creation_tokens, \
                COALESCE(SUM(cache_read_tokens), 0)::BIGINT AS cache_read_tokens, \
                COALESCE(SUM(cost_usd), 0)::DOUBLE PRECISION AS cost_usd \
         FROM usage_logs GROUP BY channel_kind",
    )
    .fetch_all(pool)
    .await?;

    let mut channels: HashMap<ChannelKind, ChannelStats> = HashMap::new();
    let mut totals_all = ChannelStats::default();
    let mut total_input: i64 = 0;
    let mut total_output: i64 = 0;
    let mut total_cache_w: i64 = 0;
    let mut total_cache_r: i64 = 0;

    for row in totals_rows {
        let ch_str: String = row.try_get("channel_kind")?;
        let Some(ch) = ChannelKind::parse(&ch_str) else { continue };
        let entry = channels.entry(ch).or_default();
        entry.total_requests = row.try_get("requests")?;
        entry.total_cost = row.try_get("cost_usd")?;
        if channel_filter.map(|f| f == ch).unwrap_or(true) {
            total_input += row.try_get::<i64, _>("input_tokens")?;
            total_output += row.try_get::<i64, _>("output_tokens")?;
            total_cache_w += row.try_get::<i64, _>("cache_creation_tokens")?;
            total_cache_r += row.try_get::<i64, _>("cache_read_tokens")?;
            totals_all.total_requests += entry.total_requests;
            totals_all.total_cost += entry.total_cost;
        }
    }

    let by_model_rows = sqlx::query(
        "SELECT channel_kind, model, \
                COUNT(*) AS requests, \
                COALESCE(SUM(input_tokens), 0)::BIGINT AS input_tokens, \
                COALESCE(SUM(output_tokens), 0)::BIGINT AS output_tokens, \
                COALESCE(SUM(cache_creation_tokens), 0)::BIGINT AS cache_creation_tokens, \
                COALESCE(SUM(cache_read_tokens), 0)::BIGINT AS cache_read_tokens, \
                COALESCE(SUM(cost_usd), 0)::DOUBLE PRECISION AS cost_usd \
         FROM usage_logs GROUP BY channel_kind, model",
    )
    .fetch_all(pool)
    .await?;

    let mut by_model: HashMap<String, Value> = HashMap::new();
    for row in by_model_rows {
        let ch_str: String = row.try_get("channel_kind")?;
        let Some(ch) = ChannelKind::parse(&ch_str) else { continue };
        let model: String = row.try_get("model")?;
        if let Some(f) = channel_filter {
            if f != ch { continue; }
        }
        let requests: i64 = row.try_get("requests")?;
        let inp: i64 = row.try_get("input_tokens")?;
        let outp: i64 = row.try_get("output_tokens")?;
        let cw: i64 = row.try_get("cache_creation_tokens")?;
        let cr: i64 = row.try_get("cache_read_tokens")?;
        let cost: f64 = row.try_get("cost_usd")?;
        let rate = match ch {
            ChannelKind::Copilot => copilot_rate(&model),
            ChannelKind::Anthropic => anthropic_rate(&model),
        };
        let cache_saved = cr as f64 / 1_000_000.0 * (rate.input - rate.cache_read);
        let is_fast = model.to_lowercase().contains("fast");
        let bucket = by_model.entry(model).or_insert_with(|| {
            json!({
                "requests": 0_i64,
                "inputTokens": 0_i64,
                "outputTokens": 0_i64,
                "cacheCreationTokens": 0_i64,
                "cacheReadTokens": 0_i64,
                "cost": 0.0_f64,
                "cacheSaved": 0.0_f64,
                "channels": [],
            })
        });
        let obj = bucket.as_object_mut().expect("entry is object");
        bump_i64(obj, "requests", requests);
        bump_i64(obj, "inputTokens", inp);
        bump_i64(obj, "outputTokens", outp);
        bump_i64(obj, "cacheCreationTokens", cw);
        bump_i64(obj, "cacheReadTokens", cr);
        bump_f64(obj, "cost", cost);
        bump_f64(obj, "cacheSaved", cache_saved);
        if let Some(channels_arr) = obj.get_mut("channels").and_then(|v| v.as_array_mut()) {
            let token = json!(ch.as_str());
            if !channels_arr.iter().any(|v| v == &token) {
                channels_arr.push(token);
            }
        }
        let entry = channels.entry(ch).or_default();
        if is_fast {
            entry.fast_cost += cost;
            if channel_filter.map(|f| f == ch).unwrap_or(true) {
                totals_all.fast_cost += cost;
            }
        } else {
            entry.standard_cost += cost;
            if channel_filter.map(|f| f == ch).unwrap_or(true) {
                totals_all.standard_cost += cost;
            }
        }
        entry.cache_saved += cache_saved;
        if channel_filter.map(|f| f == ch).unwrap_or(true) {
            totals_all.cache_saved += cache_saved;
        }
    }

    let by_key_rows = sqlx::query(
        "SELECT key_name, channel_kind, \
                COUNT(*) AS requests, \
                COALESCE(SUM(input_tokens), 0)::BIGINT AS input_tokens, \
                COALESCE(SUM(output_tokens), 0)::BIGINT AS output_tokens, \
                COALESCE(SUM(cache_creation_tokens), 0)::BIGINT AS cache_creation_tokens, \
                COALESCE(SUM(cache_read_tokens), 0)::BIGINT AS cache_read_tokens, \
                COALESCE(SUM(cost_usd), 0)::DOUBLE PRECISION AS cost_usd \
         FROM usage_logs GROUP BY key_name, channel_kind",
    )
    .fetch_all(pool)
    .await?;

    let mut by_key: HashMap<String, Value> = HashMap::new();
    for row in by_key_rows {
        let ch_str: String = row.try_get("channel_kind")?;
        let Some(ch) = ChannelKind::parse(&ch_str) else { continue };
        if let Some(f) = channel_filter {
            if f != ch { continue; }
        }
        let key_name: String = row.try_get("key_name")?;
        let bucket = by_key.entry(key_name).or_insert_with(|| {
            json!({
                "requests": 0_i64,
                "inputTokens": 0_i64,
                "outputTokens": 0_i64,
                "cacheCreationTokens": 0_i64,
                "cacheReadTokens": 0_i64,
                "cost": 0.0_f64,
                "cacheSaved": 0.0_f64,
            })
        });
        let obj = bucket.as_object_mut().expect("entry is object");
        bump_i64(obj, "requests", row.try_get("requests")?);
        bump_i64(obj, "inputTokens", row.try_get("input_tokens")?);
        bump_i64(obj, "outputTokens", row.try_get("output_tokens")?);
        bump_i64(obj, "cacheCreationTokens", row.try_get("cache_creation_tokens")?);
        bump_i64(obj, "cacheReadTokens", row.try_get("cache_read_tokens")?);
        bump_f64(obj, "cost", row.try_get("cost_usd")?);
    }

    let active_keys: i64 = db::keys::count(&state.db, channel_filter).await?;
    let total_errors = db::stats::total_errors(&state.db, channel_filter).await?;

    for (ch, st) in channels.iter_mut() {
        st.total_errors = db::stats::total_errors(&state.db, Some(*ch)).await?;
    }
    totals_all.total_errors = total_errors;

    let recent_rows = sqlx::query(
        "SELECT time, model, input_tokens, output_tokens, cache_creation_tokens, cache_read_tokens, \
                key_name, channel_kind, cost_usd \
         FROM usage_logs ORDER BY id DESC LIMIT 50",
    )
    .fetch_all(pool)
    .await?;

    let mut recent: Vec<Value> = Vec::with_capacity(recent_rows.len());
    for row in recent_rows.into_iter().rev() {
        let ch_str: String = row.try_get("channel_kind")?;
        let Some(ch) = ChannelKind::parse(&ch_str) else { continue };
        if let Some(f) = channel_filter {
            if f != ch { continue; }
        }
        recent.push(json!({
            "time": row.try_get::<String, _>("time")?,
            "model": row.try_get::<String, _>("model")?,
            "key": row.try_get::<String, _>("key_name")?,
            "channel": ch.as_str(),
            "inputTokens": row.try_get::<i64, _>("input_tokens")?,
            "outputTokens": row.try_get::<i64, _>("output_tokens")?,
            "cacheCreationTokens": row.try_get::<i64, _>("cache_creation_tokens")?,
            "cacheReadTokens": row.try_get::<i64, _>("cache_read_tokens")?,
            "cost": format!("${:.6}", row.try_get::<f64, _>("cost_usd")?),
        }));
    }

    let channels_json: HashMap<String, Value> = channels
        .into_iter()
        .map(|(ch, s)| (ch.as_str().to_string(), serde_json::to_value(s).unwrap_or(Value::Null)))
        .collect();

    Ok(json!({
        "totalRequests": totals_all.total_requests,
        "totalErrors": totals_all.total_errors,
        "totalInputTokens": total_input,
        "totalOutputTokens": total_output,
        "totalCacheCreationTokens": total_cache_w,
        "totalCacheReadTokens": total_cache_r,
        "activeKeys": active_keys,
        "billing": {
            "totalCost": format!("${:.6}", totals_all.total_cost),
            "standardCost": format!("${:.6}", totals_all.standard_cost),
            "fastCost": format!("${:.6}", totals_all.fast_cost),
            "cacheSaved": format!("${:.6}", totals_all.cache_saved),
        },
        "byModel": by_model,
        "byKey": by_key,
        "channels": channels_json,
        "recentRequests": recent,
    }))
}

fn bump_i64(obj: &mut serde_json::Map<String, Value>, key: &str, delta: i64) {
    let cur = obj.get(key).and_then(|v| v.as_i64()).unwrap_or(0);
    obj.insert(key.to_string(), json!(cur + delta));
}

fn bump_f64(obj: &mut serde_json::Map<String, Value>, key: &str, delta: f64) {
    let cur = obj.get(key).and_then(|v| v.as_f64()).unwrap_or(0.0);
    obj.insert(key.to_string(), json!(cur + delta));
}
