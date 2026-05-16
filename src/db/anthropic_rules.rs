//! Anthropic 渠道 rewrite rules CRUD。每行 (prefix, target)，``enabled=1`` 才参与匹配。
//! 业务侧匹配第一条命中的 rule，所以表内顺序由 ``id ASC`` 保证；同 target 多 prefix 是
//! 合法用法（多个客户端 model 映射到同一上游真实 model）。

use crate::db::pool::Db;
use crate::error::AppResult;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::Row;
use sqlx::postgres::PgRow;

#[derive(Debug, Clone, Serialize)]
pub struct RewriteRuleRow {
    pub id: i64,
    pub prefix: String,
    pub target: String,
    pub enabled: bool,
    pub created_at: String,
}

impl<'r> sqlx::FromRow<'r, PgRow> for RewriteRuleRow {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let enabled_int: i32 = row.try_get("enabled")?;
        Ok(Self {
            id: row.try_get("id")?,
            prefix: row.try_get("prefix")?,
            target: row.try_get("target")?,
            enabled: enabled_int != 0,
            created_at: row.try_get("created_at")?,
        })
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct RewriteRulePatch {
    pub prefix: Option<String>,
    pub target: Option<String>,
    pub enabled: Option<bool>,
}

pub async fn list_all(db: &Db) -> AppResult<Vec<RewriteRuleRow>> {
    let rows = sqlx::query_as::<_, RewriteRuleRow>(
        "SELECT id, prefix, target, enabled, created_at \
         FROM anthropic_rewrite_rules ORDER BY id ASC",
    )
    .fetch_all(db.pool())
    .await?;
    Ok(rows)
}

pub async fn list_enabled(db: &Db) -> AppResult<Vec<RewriteRuleRow>> {
    let rows = sqlx::query_as::<_, RewriteRuleRow>(
        "SELECT id, prefix, target, enabled, created_at \
         FROM anthropic_rewrite_rules WHERE enabled = 1 ORDER BY id ASC",
    )
    .fetch_all(db.pool())
    .await?;
    Ok(rows)
}

pub async fn count(db: &Db) -> AppResult<i64> {
    let n: i64 = sqlx::query("SELECT COUNT(*) AS cnt FROM anthropic_rewrite_rules")
        .fetch_one(db.pool())
        .await?
        .try_get("cnt")?;
    Ok(n)
}

pub async fn create(
    db: &Db,
    prefix: &str,
    target: &str,
    enabled: bool,
) -> AppResult<RewriteRuleRow> {
    let created_at = Utc::now().to_rfc3339();
    let enabled_int: i32 = if enabled { 1 } else { 0 };
    let row = sqlx::query_as::<_, RewriteRuleRow>(
        "INSERT INTO anthropic_rewrite_rules (prefix, target, enabled, created_at) \
         VALUES ($1, $2, $3, $4) \
         RETURNING id, prefix, target, enabled, created_at",
    )
    .bind(prefix)
    .bind(target)
    .bind(enabled_int)
    .bind(&created_at)
    .fetch_one(db.pool())
    .await?;
    Ok(row)
}

pub async fn update(
    db: &Db,
    id: i64,
    patch: RewriteRulePatch,
) -> AppResult<Option<RewriteRuleRow>> {
    let mut sets: Vec<String> = Vec::new();
    let mut idx = 1u32;

    macro_rules! push_set {
        ($col:literal) => {{
            sets.push(format!("{} = ${}", $col, idx));
            idx += 1;
        }};
    }

    if patch.prefix.is_some() {
        push_set!("prefix");
    }
    if patch.target.is_some() {
        push_set!("target");
    }
    if patch.enabled.is_some() {
        push_set!("enabled");
    }
    if sets.is_empty() {
        return Ok(
            sqlx::query_as::<_, RewriteRuleRow>(
                "SELECT id, prefix, target, enabled, created_at \
                 FROM anthropic_rewrite_rules WHERE id = $1",
            )
            .bind(id)
            .fetch_optional(db.pool())
            .await?,
        );
    }

    let sql = format!(
        "UPDATE anthropic_rewrite_rules SET {} WHERE id = ${} \
         RETURNING id, prefix, target, enabled, created_at",
        sets.join(", "),
        idx
    );
    let mut q = sqlx::query_as::<_, RewriteRuleRow>(&sql);
    if let Some(p) = patch.prefix.as_deref() {
        q = q.bind(p.to_string());
    }
    if let Some(t) = patch.target.as_deref() {
        q = q.bind(t.to_string());
    }
    if let Some(e) = patch.enabled {
        q = q.bind(if e { 1i32 } else { 0i32 });
    }
    q = q.bind(id);
    let row = q.fetch_optional(db.pool()).await?;
    Ok(row)
}

pub async fn delete(db: &Db, id: i64) -> AppResult<bool> {
    let res = sqlx::query("DELETE FROM anthropic_rewrite_rules WHERE id = $1")
        .bind(id)
        .execute(db.pool())
        .await?;
    Ok(res.rows_affected() > 0)
}

/// 首次启动种子：DB 表空 且 env var 给了 ``prefix=target,prefix=target`` → 批量写入。
/// 已有数据则跳过，避免覆盖运维侧改动。
pub async fn seed_from_env_if_empty(
    db: &Db,
    env_spec: &str,
) -> AppResult<usize> {
    let existing = count(db).await?;
    if existing > 0 {
        return Ok(0);
    }
    let trimmed = env_spec.trim();
    if trimmed.is_empty() {
        return Ok(0);
    }
    let mut inserted = 0usize;
    for entry in trimmed.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let Some((prefix, target)) = entry.split_once('=') else {
            continue;
        };
        let prefix = prefix.trim();
        let target = target.trim();
        if prefix.is_empty() || target.is_empty() {
            continue;
        }
        create(db, prefix, target, true).await?;
        inserted += 1;
    }
    Ok(inserted)
}
