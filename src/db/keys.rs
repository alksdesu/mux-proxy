//! api_keys CRUD。改名要事务包住 3 个 UPDATE 同步 usage_logs / error_logs，防止改名丢计费。

use crate::db::pool::Db;
use crate::db::schema::{ApiKey, ApiKeyPatch, ChannelKind};
use crate::error::AppResult;
use chrono::Utc;
use sqlx::Row;
use uuid::Uuid;

pub async fn find_by_key(db: &Db, key: &str) -> AppResult<Option<ApiKey>> {
    let row = sqlx::query_as::<_, ApiKey>(
        "SELECT id, key, name, upstream_key, quota, allow_fast, max_concurrency, created_at, channel_kind \
         FROM api_keys WHERE key = $1",
    )
    .bind(key)
    .fetch_optional(db.pool())
    .await?;
    Ok(row)
}

pub async fn find_by_id(db: &Db, id: i64) -> AppResult<Option<ApiKey>> {
    let row = sqlx::query_as::<_, ApiKey>(
        "SELECT id, key, name, upstream_key, quota, allow_fast, max_concurrency, created_at, channel_kind \
         FROM api_keys WHERE id = $1",
    )
    .bind(id as i32)
    .fetch_optional(db.pool())
    .await?;
    Ok(row)
}

pub async fn list(
    db: &Db,
    channel: Option<ChannelKind>,
    limit: i64,
    offset: i64,
) -> AppResult<Vec<ApiKey>> {
    let rows = match channel {
        Some(ch) => sqlx::query_as::<_, ApiKey>(
            "SELECT id, key, name, upstream_key, quota, allow_fast, max_concurrency, created_at, channel_kind \
             FROM api_keys WHERE channel_kind = $1 ORDER BY id LIMIT $2 OFFSET $3",
        )
        .bind(ch.as_str())
        .bind(limit)
        .bind(offset)
        .fetch_all(db.pool())
        .await?,
        None => sqlx::query_as::<_, ApiKey>(
            "SELECT id, key, name, upstream_key, quota, allow_fast, max_concurrency, created_at, channel_kind \
             FROM api_keys ORDER BY id LIMIT $1 OFFSET $2",
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(db.pool())
        .await?,
    };
    Ok(rows)
}

pub async fn count(db: &Db, channel: Option<ChannelKind>) -> AppResult<i64> {
    let n: i64 = match channel {
        Some(ch) => sqlx::query("SELECT COUNT(*) AS cnt FROM api_keys WHERE channel_kind = $1")
            .bind(ch.as_str())
            .fetch_one(db.pool())
            .await?
            .try_get("cnt")?,
        None => sqlx::query("SELECT COUNT(*) AS cnt FROM api_keys")
            .fetch_one(db.pool())
            .await?
            .try_get("cnt")?,
    };
    Ok(n)
}

pub async fn create(
    db: &Db,
    name: &str,
    upstream_key: &str,
    quota: f64,
    allow_fast: bool,
    max_concurrency: i64,
    channel_kind: ChannelKind,
) -> AppResult<ApiKey> {
    let raw = Uuid::new_v4().simple().to_string();
    let key = format!("sk-{}", raw);
    let created_at = Utc::now().to_rfc3339();
    let allow_fast_int: i32 = if allow_fast { 1 } else { 0 };

    let row = sqlx::query_as::<_, ApiKey>(
        "INSERT INTO api_keys (key, name, upstream_key, created_at, quota, allow_fast, max_concurrency, channel_kind) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
         RETURNING id, key, name, upstream_key, quota, allow_fast, max_concurrency, created_at, channel_kind",
    )
    .bind(&key)
    .bind(name)
    .bind(upstream_key)
    .bind(&created_at)
    .bind(quota)
    .bind(allow_fast_int)
    .bind(max_concurrency)
    .bind(channel_kind.as_str())
    .fetch_one(db.pool())
    .await?;
    Ok(row)
}

pub async fn update(db: &Db, id: i64, patch: ApiKeyPatch) -> AppResult<Option<ApiKey>> {
    let mut tx = db.pool().begin().await?;

    let existing: Option<ApiKey> = sqlx::query_as::<_, ApiKey>(
        "SELECT id, key, name, upstream_key, quota, allow_fast, max_concurrency, created_at, channel_kind \
         FROM api_keys WHERE id = $1",
    )
    .bind(id as i32)
    .fetch_optional(&mut *tx)
    .await?;

    let Some(current) = existing else {
        tx.rollback().await?;
        return Ok(None);
    };

    let rename = patch
        .name
        .as_deref()
        .filter(|n| !n.is_empty() && *n != current.name)
        .map(|n| n.to_string());

    let mut sets: Vec<String> = Vec::new();
    let mut idx = 1u32;

    macro_rules! push_set {
        ($col:literal) => {{
            sets.push(format!("{} = ${}", $col, idx));
            idx += 1;
        }};
    }

    if patch.name.is_some() {
        push_set!("name");
    }
    if patch.upstream_key.is_some() {
        push_set!("upstream_key");
    }
    if patch.quota.is_some() {
        push_set!("quota");
    }
    if patch.allow_fast.is_some() {
        push_set!("allow_fast");
    }
    if patch.max_concurrency.is_some() {
        push_set!("max_concurrency");
    }
    if patch.channel_kind.is_some() {
        push_set!("channel_kind");
    }

    if sets.is_empty() {
        tx.rollback().await?;
        return Ok(Some(current));
    }

    let sql = format!(
        "UPDATE api_keys SET {} WHERE id = ${} \
         RETURNING id, key, name, upstream_key, quota, allow_fast, max_concurrency, created_at, channel_kind",
        sets.join(", "),
        idx
    );
    let mut q = sqlx::query_as::<_, ApiKey>(&sql);

    if let Some(name) = patch.name.as_deref() {
        q = q.bind(name);
    }
    if let Some(uk) = patch.upstream_key.as_deref() {
        q = q.bind(uk);
    }
    if let Some(quota) = patch.quota {
        q = q.bind(quota);
    }
    if let Some(af) = patch.allow_fast {
        q = q.bind(if af { 1i32 } else { 0i32 });
    }
    if let Some(mc) = patch.max_concurrency {
        q = q.bind(mc);
    }
    if let Some(ch) = patch.channel_kind {
        q = q.bind(ch.as_str());
    }
    q = q.bind(id as i32);

    let updated = q.fetch_one(&mut *tx).await?;

    if let Some(new_name) = rename {
        sqlx::query("UPDATE usage_logs SET key_name = $1 WHERE key_name = $2")
            .bind(&new_name)
            .bind(&current.name)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE error_logs SET key_name = $1 WHERE key_name = $2")
            .bind(&new_name)
            .bind(&current.name)
            .execute(&mut *tx)
            .await?;
    }

    tx.commit().await?;
    Ok(Some(updated))
}

pub async fn delete(db: &Db, id: i64) -> AppResult<Option<String>> {
    let row = sqlx::query("DELETE FROM api_keys WHERE id = $1 RETURNING name")
        .bind(id as i32)
        .fetch_optional(db.pool())
        .await?;
    Ok(row.map(|r| r.get::<String, _>("name")))
}
