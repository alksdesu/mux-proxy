//! /admin/upstream CRUD + /admin/upstream/breaker。
//! 写操作经 UpstreamChangeNotifier 通知 key_pool 强制刷新。
//! breaker 同时覆盖 Copilot（外置 ``Breaker``）与 Anthropic（``KeyPool`` 内置熔断器）。

use crate::admin::query::{parse_channel, parse_id_required};
use crate::app::AppState;
use crate::channels::{BreakerSnapshot, ChannelKind, route_by_upstream_key};
use crate::db;
use crate::db::schema::UpstreamKeyPatch;
use crate::error::{AppError, AppResult};
use axum::Json;
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use serde::Deserialize;
use std::collections::HashMap;

pub fn collect_breaker_snapshot(
    state: &AppState,
    channel: Option<ChannelKind>,
) -> Vec<BreakerSnapshot> {
    let mut out: Vec<BreakerSnapshot> = Vec::new();
    if matches!(channel, None | Some(ChannelKind::Copilot)) {
        out.extend(state.copilot_breaker.snapshot());
    }
    if matches!(channel, None | Some(ChannelKind::Anthropic)) {
        out.extend(state.anthropic_pool.snapshot_breakers());
    }
    out
}

#[derive(Debug, Deserialize)]
pub struct CreateBody {
    pub key: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub note: String,
    pub channel_kind: Option<ChannelKind>,
}

pub async fn list_handler(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> AppResult<axum::response::Response> {
    let channel = parse_channel(params.get("channel").map(String::as_str))?;
    let rows = db::upstream::list(&state.db, channel).await?;
    Ok(Json(rows).into_response())
}

pub async fn create_handler(
    State(state): State<AppState>,
    Json(body): Json<CreateBody>,
) -> AppResult<axum::response::Response> {
    if body.key.trim().is_empty() {
        return Err(AppError::BadRequest("key is required".into()));
    }
    let inferred = route_by_upstream_key(&body.key);
    let channel = match body.channel_kind {
        Some(explicit) if explicit != inferred => {
            return Err(AppError::BadRequest(format!(
                "channel_kind={explicit} conflicts with key prefix (inferred {inferred})"
            )));
        }
        Some(explicit) => explicit,
        None => inferred,
    };

    let created = db::upstream::create(
        &state.db,
        body.key.trim(),
        body.name.trim(),
        body.note.trim(),
        channel,
        &state.upstream_notifier,
    )
    .await?;
    state.snapshot.bump();
    Ok(Json(created).into_response())
}

pub async fn patch_handler(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
    Json(patch): Json<UpstreamKeyPatch>,
) -> AppResult<axum::response::Response> {
    let id = parse_id_required(params.get("id").map(String::as_str))?;
    if let (Some(k), Some(ch_explicit)) = (patch.key.as_deref(), patch.channel_kind) {
        let inferred = route_by_upstream_key(k);
        if inferred != ch_explicit {
            return Err(AppError::BadRequest(format!(
                "channel_kind={ch_explicit} conflicts with key prefix (inferred {inferred})"
            )));
        }
    }
    let updated = db::upstream::update(&state.db, id, patch, &state.upstream_notifier)
        .await?
        .ok_or(AppError::NotFound)?;
    state.snapshot.bump();
    Ok(Json(updated).into_response())
}

pub async fn delete_handler(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> AppResult<axum::response::Response> {
    let id = parse_id_required(params.get("id").map(String::as_str))?;
    let _ = db::upstream::find_by_id(&state.db, id)
        .await?
        .ok_or(AppError::NotFound)?;
    let ok = db::upstream::delete(&state.db, id, &state.upstream_notifier).await?;
    if ok {
        state.snapshot.bump();
    }
    Ok(Json(serde_json::json!({ "ok": ok })).into_response())
}

pub async fn breaker_get_handler(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> AppResult<axum::response::Response> {
    let channel = parse_channel(params.get("channel").map(String::as_str))?;
    let snapshot = collect_breaker_snapshot(&state, channel);
    Ok(Json(snapshot).into_response())
}

#[derive(Debug)]
enum BreakerAction {
    Reset,
    Disable,
}

impl BreakerAction {
    fn parse(s: &str) -> Result<Self, AppError> {
        match s {
            "reset" => Ok(BreakerAction::Reset),
            "disable" => Ok(BreakerAction::Disable),
            _ => Err(AppError::BadRequest("action must be reset or disable".into())),
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            BreakerAction::Reset => "reset",
            BreakerAction::Disable => "disable",
        }
    }
}

pub async fn breaker_post_handler(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> AppResult<axum::response::Response> {
    let id = parse_id_required(params.get("id").map(String::as_str))?;
    let action = params
        .get("action")
        .map(String::as_str)
        .ok_or_else(|| AppError::BadRequest("missing action".into()))?;
    let act = BreakerAction::parse(action)?;
    let upstream = db::upstream::find_by_id(&state.db, id)
        .await?
        .ok_or(AppError::NotFound)?;

    match (upstream.channel_kind, &act) {
        (ChannelKind::Copilot, BreakerAction::Reset) => state.copilot_breaker.reset(id),
        (ChannelKind::Copilot, BreakerAction::Disable) => state.copilot_breaker.force_disable(id),
        (ChannelKind::Anthropic, BreakerAction::Reset) => state.anthropic_pool.reset_breaker(id),
        (ChannelKind::Anthropic, BreakerAction::Disable) => {
            state.anthropic_pool.force_disable_breaker(id)
        }
    }
    state.snapshot.bump();

    Ok(Json(serde_json::json!({
        "status": "ok",
        "action": act.as_str(),
        "channel": upstream.channel_kind.as_str(),
        "applied": true,
    }))
    .into_response())
}
