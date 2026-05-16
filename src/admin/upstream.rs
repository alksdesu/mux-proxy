//! /admin/upstream CRUD + /admin/upstream/breaker。
//! 写操作经 UpstreamChangeNotifier 通知 key_pool 强制刷新。
//! breaker 同时覆盖 Copilot（外置 ``Breaker``）与 Anthropic（``KeyPool`` 内置熔断器）。

use crate::admin::query::{parse_channel, parse_id_required};
use crate::app::AppState;
use crate::channels::anthropic::model_splice::RewriteRule;
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
    /// 非 None 即按数组写入；空数组语义与 PATCH 一致（清回 NULL = 落全局兜底）。
    /// 仅对 Anthropic 渠道生效；Copilot 渠道传了也存，但不参与运行时逻辑。
    pub rewrite_rules: Option<Vec<RewriteRule>>,
    /// 非 None 即按数组写入；空数组 → NULL（不限 model）。
    pub allowed_models: Option<Vec<String>>,
}

/// 把 Option<Vec<T>> 规约为"语义化的写入值"：空数组等同于 None（清回 NULL/兜底）。
/// PATCH/POST 共用同一规约逻辑保证语义一致。
fn normalize_empty<T>(v: Option<Vec<T>>) -> Option<Vec<T>> {
    match v {
        Some(arr) if arr.is_empty() => None,
        other => other,
    }
}

/// 校验：rewrite_rules 不能有空 prefix / 空 target；allowed_models 不能含空字符串。
/// 都是用户面校验，DB 层不重复做。
fn validate_rewrite_rules(rules: &[RewriteRule]) -> AppResult<()> {
    for r in rules {
        if r.prefix.trim().is_empty() {
            return Err(AppError::BadRequest(
                "rewrite_rules entry has empty prefix".into(),
            ));
        }
        if r.target.trim().is_empty() {
            return Err(AppError::BadRequest(
                "rewrite_rules entry has empty target".into(),
            ));
        }
    }
    Ok(())
}

fn validate_allowed_models(models: &[String]) -> AppResult<()> {
    for m in models {
        if m.trim().is_empty() {
            return Err(AppError::BadRequest(
                "allowed_models entry must not be empty".into(),
            ));
        }
    }
    Ok(())
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
    // upstream_keys 表的 key 列必须是真上游 token（``prefix:token`` 或 ``sk-ant-...``），
    // 没有"走池占位"语义；推不出渠道直接 400 而不是默认 copilot。
    let channel = match (route_by_upstream_key(&body.key), body.channel_kind) {
        (Some(inferred), Some(explicit)) if explicit != inferred => {
            return Err(AppError::BadRequest(format!(
                "channel_kind={explicit} conflicts with key prefix (inferred {inferred})"
            )));
        }
        (Some(inferred), _) => inferred,
        (None, Some(explicit)) => explicit,
        (None, None) => {
            return Err(AppError::BadRequest(
                "unknown upstream key format; expected prefix:token or sk-ant-...".into(),
            ));
        }
    };

    let rewrite_rules = normalize_empty(body.rewrite_rules);
    if let Some(rules) = &rewrite_rules {
        validate_rewrite_rules(rules)?;
    }
    let allowed_models = normalize_empty(body.allowed_models);
    if let Some(models) = &allowed_models {
        validate_allowed_models(models)?;
    }

    let created = db::upstream::create(
        &state.db,
        body.key.trim(),
        body.name.trim(),
        body.note.trim(),
        channel,
        rewrite_rules.as_deref(),
        allowed_models.as_deref(),
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
        if let Some(inferred) = route_by_upstream_key(k) {
            if inferred != ch_explicit {
                return Err(AppError::BadRequest(format!(
                    "channel_kind={ch_explicit} conflicts with key prefix (inferred {inferred})"
                )));
            }
        }
    }
    if let Some(rules) = patch.rewrite_rules.as_deref() {
        if !rules.is_empty() {
            validate_rewrite_rules(rules)?;
        }
    }
    if let Some(models) = patch.allowed_models.as_deref() {
        if !models.is_empty() {
            validate_allowed_models(models)?;
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
