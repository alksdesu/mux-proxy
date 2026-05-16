//! upstream_keys CRUD。写操作完成后 notify_changed 通知 key_pool 强制刷新。
//! rewrite_rules / allowed_models 两列采用 JSONB：业务层用 [`RewriteRule`] / `Vec<String>`，
//! PATCH 时空数组等价于"清回 NULL"（即回落全局兜底 / 解除 model 限制）。

use crate::channels::anthropic::model_splice::RewriteRule;
use crate::db::pool::Db;
use crate::db::schema::{ChannelKind, UpstreamKey, UpstreamKeyPatch};
use crate::error::AppResult;
use chrono::Utc;
use std::sync::Arc;
use tokio::sync::Notify;

/// 所有 SELECT/RETURNING 列清单。schema 改动时只需要同步这一行。
const COLS: &str =
    "id, key, name, enabled, note, created_at, channel_kind, rewrite_rules, allowed_models";

#[derive(Clone, Default)]
pub struct UpstreamChangeNotifier {
    inner: Arc<Notify>,
}

impl UpstreamChangeNotifier {
    pub fn new() -> Self {
        Self { inner: Arc::new(Notify::new()) }
    }

    pub fn notify(&self) {
        self.inner.notify_waiters();
    }

    pub fn handle(&self) -> Arc<Notify> {
        self.inner.clone()
    }
}

/// PATCH 时一个 JSONB 字段的语义：缺失 / 空数组 / 非空数组分别对应"不改 / 清回 NULL / 完整覆盖"。
enum JsonbColumnPatch<T> {
    Skip,
    ClearToNull,
    Set(Vec<T>),
}

impl<T: Clone> JsonbColumnPatch<T> {
    fn from_patch(field: &Option<Vec<T>>) -> Self {
        match field {
            None => Self::Skip,
            Some(v) if v.is_empty() => Self::ClearToNull,
            Some(v) => Self::Set(v.clone()),
        }
    }
}

pub async fn list(db: &Db, channel: Option<ChannelKind>) -> AppResult<Vec<UpstreamKey>> {
    let rows = match channel {
        Some(ch) => sqlx::query_as::<_, UpstreamKey>(&format!(
            "SELECT {COLS} FROM upstream_keys WHERE channel_kind = $1 ORDER BY id",
        ))
        .bind(ch.as_str())
        .fetch_all(db.pool())
        .await?,
        None => sqlx::query_as::<_, UpstreamKey>(&format!(
            "SELECT {COLS} FROM upstream_keys ORDER BY id",
        ))
        .fetch_all(db.pool())
        .await?,
    };
    Ok(rows)
}

pub async fn list_enabled(db: &Db, channel: ChannelKind) -> AppResult<Vec<UpstreamKey>> {
    let rows = sqlx::query_as::<_, UpstreamKey>(&format!(
        "SELECT {COLS} FROM upstream_keys WHERE channel_kind = $1 AND enabled = 1 ORDER BY id",
    ))
    .bind(channel.as_str())
    .fetch_all(db.pool())
    .await?;
    Ok(rows)
}

pub async fn find_by_id(db: &Db, id: i64) -> AppResult<Option<UpstreamKey>> {
    let row = sqlx::query_as::<_, UpstreamKey>(&format!(
        "SELECT {COLS} FROM upstream_keys WHERE id = $1",
    ))
    .bind(id)
    .fetch_optional(db.pool())
    .await?;
    Ok(row)
}

pub async fn create(
    db: &Db,
    key: &str,
    name: &str,
    note: &str,
    channel_kind: ChannelKind,
    rewrite_rules: Option<&[RewriteRule]>,
    allowed_models: Option<&[String]>,
    notifier: &UpstreamChangeNotifier,
) -> AppResult<UpstreamKey> {
    let created_at = Utc::now().to_rfc3339();
    let rewrite_json = rewrite_rules.map(|v| sqlx::types::Json(v.to_vec()));
    let allowed_json = allowed_models.map(|v| sqlx::types::Json(v.to_vec()));
    let sql = format!(
        "INSERT INTO upstream_keys (key, name, note, created_at, channel_kind, enabled, rewrite_rules, allowed_models) \
         VALUES ($1, $2, $3, $4, $5, 1, $6, $7) \
         RETURNING {COLS}",
    );
    let row = sqlx::query_as::<_, UpstreamKey>(&sql)
        .bind(key)
        .bind(name)
        .bind(note)
        .bind(&created_at)
        .bind(channel_kind.as_str())
        .bind(rewrite_json)
        .bind(allowed_json)
        .fetch_one(db.pool())
        .await?;
    notifier.notify();
    Ok(row)
}

pub async fn update(
    db: &Db,
    id: i64,
    patch: UpstreamKeyPatch,
    notifier: &UpstreamChangeNotifier,
) -> AppResult<Option<UpstreamKey>> {
    let mut sets: Vec<String> = Vec::new();
    let mut idx = 1u32;

    macro_rules! push_placeholder {
        ($col:literal) => {{
            sets.push(format!("{} = ${}", $col, idx));
            idx += 1;
        }};
    }

    if patch.key.is_some() {
        push_placeholder!("key");
    }
    if patch.name.is_some() {
        push_placeholder!("name");
    }
    if patch.enabled.is_some() {
        push_placeholder!("enabled");
    }
    if patch.note.is_some() {
        push_placeholder!("note");
    }
    if patch.channel_kind.is_some() {
        push_placeholder!("channel_kind");
    }

    // JSONB 列两种写法：清回 NULL 直接 inline literal 不占占位符，避免给 sqlx 绑定 None 时
    // 的类型推断歧义；写值才占占位符。
    let rewrite_patch = JsonbColumnPatch::from_patch(&patch.rewrite_rules);
    match &rewrite_patch {
        JsonbColumnPatch::Skip => {}
        JsonbColumnPatch::ClearToNull => sets.push("rewrite_rules = NULL".to_string()),
        JsonbColumnPatch::Set(_) => push_placeholder!("rewrite_rules"),
    }

    let allowed_patch = JsonbColumnPatch::from_patch(&patch.allowed_models);
    match &allowed_patch {
        JsonbColumnPatch::Skip => {}
        JsonbColumnPatch::ClearToNull => sets.push("allowed_models = NULL".to_string()),
        JsonbColumnPatch::Set(_) => push_placeholder!("allowed_models"),
    }

    if sets.is_empty() {
        return find_by_id(db, id).await;
    }

    let sql = format!(
        "UPDATE upstream_keys SET {} WHERE id = ${} RETURNING {COLS}",
        sets.join(", "),
        idx
    );
    let mut q = sqlx::query_as::<_, UpstreamKey>(&sql);

    if let Some(v) = patch.key.as_deref() {
        q = q.bind(v.to_string());
    }
    if let Some(v) = patch.name.as_deref() {
        q = q.bind(v.to_string());
    }
    if let Some(v) = patch.enabled {
        q = q.bind(if v { 1i32 } else { 0i32 });
    }
    if let Some(v) = patch.note.as_deref() {
        q = q.bind(v.to_string());
    }
    if let Some(v) = patch.channel_kind {
        q = q.bind(v.as_str().to_string());
    }
    if let JsonbColumnPatch::Set(v) = rewrite_patch {
        q = q.bind(sqlx::types::Json(v));
    }
    if let JsonbColumnPatch::Set(v) = allowed_patch {
        q = q.bind(sqlx::types::Json(v));
    }
    q = q.bind(id);

    let updated = q.fetch_optional(db.pool()).await?;
    if updated.is_some() {
        notifier.notify();
    }
    Ok(updated)
}

pub async fn update_enabled(
    db: &Db,
    id: i64,
    enabled: bool,
    notifier: &UpstreamChangeNotifier,
) -> AppResult<Option<UpstreamKey>> {
    let row = sqlx::query_as::<_, UpstreamKey>(&format!(
        "UPDATE upstream_keys SET enabled = $1 WHERE id = $2 RETURNING {COLS}",
    ))
    .bind(if enabled { 1i32 } else { 0i32 })
    .bind(id)
    .fetch_optional(db.pool())
    .await?;
    if row.is_some() {
        notifier.notify();
    }
    Ok(row)
}

pub async fn delete(
    db: &Db,
    id: i64,
    notifier: &UpstreamChangeNotifier,
) -> AppResult<bool> {
    let affected = sqlx::query("DELETE FROM upstream_keys WHERE id = $1")
        .bind(id)
        .execute(db.pool())
        .await?
        .rows_affected();
    if affected > 0 {
        notifier.notify();
    }
    Ok(affected > 0)
}
