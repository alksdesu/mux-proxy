//! upstream_keys CRUD。写操作完成后 notify_changed 通知 key_pool 强制刷新。

use crate::db::pool::Db;
use crate::db::schema::{ChannelKind, UpstreamKey, UpstreamKeyPatch};
use crate::error::AppResult;
use chrono::Utc;
use std::sync::Arc;
use tokio::sync::Notify;

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

pub async fn list(db: &Db, channel: Option<ChannelKind>) -> AppResult<Vec<UpstreamKey>> {
    let rows = match channel {
        Some(ch) => sqlx::query_as::<_, UpstreamKey>(
            "SELECT id, key, name, enabled, note, created_at, channel_kind \
             FROM upstream_keys WHERE channel_kind = $1 ORDER BY id",
        )
        .bind(ch.as_str())
        .fetch_all(db.pool())
        .await?,
        None => sqlx::query_as::<_, UpstreamKey>(
            "SELECT id, key, name, enabled, note, created_at, channel_kind \
             FROM upstream_keys ORDER BY id",
        )
        .fetch_all(db.pool())
        .await?,
    };
    Ok(rows)
}

pub async fn list_enabled(db: &Db, channel: ChannelKind) -> AppResult<Vec<UpstreamKey>> {
    let rows = sqlx::query_as::<_, UpstreamKey>(
        "SELECT id, key, name, enabled, note, created_at, channel_kind \
         FROM upstream_keys WHERE channel_kind = $1 AND enabled = 1 ORDER BY id",
    )
    .bind(channel.as_str())
    .fetch_all(db.pool())
    .await?;
    Ok(rows)
}

pub async fn find_by_id(db: &Db, id: i64) -> AppResult<Option<UpstreamKey>> {
    let row = sqlx::query_as::<_, UpstreamKey>(
        "SELECT id, key, name, enabled, note, created_at, channel_kind \
         FROM upstream_keys WHERE id = $1",
    )
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
    notifier: &UpstreamChangeNotifier,
) -> AppResult<UpstreamKey> {
    let created_at = Utc::now().to_rfc3339();
    let row = sqlx::query_as::<_, UpstreamKey>(
        "INSERT INTO upstream_keys (key, name, note, created_at, channel_kind, enabled) \
         VALUES ($1, $2, $3, $4, $5, 1) \
         RETURNING id, key, name, enabled, note, created_at, channel_kind",
    )
    .bind(key)
    .bind(name)
    .bind(note)
    .bind(&created_at)
    .bind(channel_kind.as_str())
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

    macro_rules! push_set {
        ($col:literal) => {{
            sets.push(format!("{} = ${}", $col, idx));
            idx += 1;
        }};
    }

    if patch.key.is_some() {
        push_set!("key");
    }
    if patch.name.is_some() {
        push_set!("name");
    }
    if patch.enabled.is_some() {
        push_set!("enabled");
    }
    if patch.note.is_some() {
        push_set!("note");
    }
    if patch.channel_kind.is_some() {
        push_set!("channel_kind");
    }

    if sets.is_empty() {
        return find_by_id(db, id).await;
    }

    let sql = format!(
        "UPDATE upstream_keys SET {} WHERE id = ${} \
         RETURNING id, key, name, enabled, note, created_at, channel_kind",
        sets.join(", "),
        idx
    );
    let mut q = sqlx::query_as::<_, UpstreamKey>(&sql);

    if let Some(v) = patch.key.as_deref() {
        q = q.bind(v);
    }
    if let Some(v) = patch.name.as_deref() {
        q = q.bind(v);
    }
    if let Some(v) = patch.enabled {
        q = q.bind(if v { 1i32 } else { 0i32 });
    }
    if let Some(v) = patch.note.as_deref() {
        q = q.bind(v);
    }
    if let Some(v) = patch.channel_kind {
        q = q.bind(v.as_str());
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
    let row = sqlx::query_as::<_, UpstreamKey>(
        "UPDATE upstream_keys SET enabled = $1 WHERE id = $2 \
         RETURNING id, key, name, enabled, note, created_at, channel_kind",
    )
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
