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

/// 控制字符黑名单：行分隔符 / NUL / TAB / 等号 / 逗号 / 引号。
/// dashboard 用换行 + 等号做 prefix=target 解析，DB 直注含这些字符的值会破坏 round-trip
/// 显示（一条规则被拆成多行）或后续 admin 校验绕过。
fn contains_forbidden_chars(s: &str) -> bool {
    s.chars()
        .any(|c| matches!(c, '\n' | '\r' | '\0' | '\t' | ',' | '"'))
}

/// 校验：rewrite_rules 不能有空 prefix / 空 target / 控制字符；allowed_models 同理。
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
        if contains_forbidden_chars(&r.prefix) || contains_forbidden_chars(&r.target) {
            return Err(AppError::BadRequest(
                "rewrite_rules entry contains forbidden control characters (newline/tab/comma/quote)".into(),
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
        if contains_forbidden_chars(m) {
            return Err(AppError::BadRequest(
                "allowed_models entry contains forbidden control characters (newline/tab/comma/quote)".into(),
            ));
        }
    }
    Ok(())
}

/// 把"channel != anthropic 但 body 想写 per-key override 字段"翻成 400。
/// 后端不静默接受无意义字段，避免 admin 误以为生效。
fn reject_overrides_for_non_anthropic(
    channel: ChannelKind,
    has_rewrite: bool,
    has_allowed: bool,
) -> AppResult<()> {
    if channel != ChannelKind::Anthropic && (has_rewrite || has_allowed) {
        return Err(AppError::BadRequest(
            "rewrite_rules and allowed_models are only valid for anthropic channel".into(),
        ));
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
    let allowed_models = normalize_empty(body.allowed_models);
    reject_overrides_for_non_anthropic(
        channel,
        rewrite_rules.is_some(),
        allowed_models.is_some(),
    )?;
    if let Some(rules) = &rewrite_rules {
        validate_rewrite_rules(rules)?;
    }
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
    // 渠道隔离：patch 想动 override 字段时，必须先确定 patch 生效后的 channel 是 anthropic。
    // channel 可能从 patch 显式带来，否则要查 DB 当前行。
    if patch.rewrite_rules.is_some() || patch.allowed_models.is_some() {
        let effective_channel = match patch.channel_kind {
            Some(ch) => ch,
            None => {
                db::upstream::find_by_id(&state.db, id)
                    .await?
                    .ok_or(AppError::NotFound)?
                    .channel_kind
            }
        };
        reject_overrides_for_non_anthropic(
            effective_channel,
            patch.rewrite_rules.is_some(),
            patch.allowed_models.is_some(),
        )?;
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
