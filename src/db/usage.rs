//! usage_logs 写入 + 查询 + cleanup。错误日志实现在 `db::errors`，本模块 re-export。
//! cleanup 由调用方 spawn（usage_writer 控制时机），DB 层只暴露纯函数。

use crate::db::pool::Db;
use crate::db::schema::{ChannelKind, UsageLog, UsageLogInput};
use crate::error::AppResult;
use futures::stream::BoxStream;
use sqlx::Row;

pub use crate::db::errors::{cleanup_old_errors, insert_error};

pub const USAGE_LOG_LIMIT: i64 = 500;

pub async fn insert_usage(db: &Db, rec: UsageLogInput) -> AppResult<i64> {
    let row = sqlx::query(
        "INSERT INTO usage_logs \
            (time, model, input_tokens, output_tokens, cache_creation_tokens, cache_read_tokens, \
             key_name, request_body, ip, cost_usd, channel_kind) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) \
         RETURNING id",
    )
    .bind(&rec.time)
    .bind(&rec.model)
    .bind(rec.input_tokens)
    .bind(rec.output_tokens)
    .bind(rec.cache_creation_tokens)
    .bind(rec.cache_read_tokens)
    .bind(&rec.key_name)
    .bind(&rec.request_body)
    .bind(&rec.ip)
    .bind(rec.cost_usd)
    .bind(rec.channel_kind.as_str())
    .fetch_one(db.pool())
    .await?;

    let id: i64 = row.try_get("id")?;
    Ok(id)
}

pub async fn cleanup_request_bodies(db: &Db, key_name: &str) -> AppResult<()> {
    sqlx::query(
        "UPDATE usage_logs SET request_body = '' \
         WHERE key_name = $1 \
           AND id NOT IN ( \
             SELECT id FROM usage_logs \
              WHERE key_name = $1 \
              ORDER BY id DESC LIMIT $2 \
           )",
    )
    .bind(key_name)
    .bind(USAGE_LOG_LIMIT)
    .execute(db.pool())
    .await?;
    Ok(())
}

pub async fn list_usage(
    db: &Db,
    key: Option<&str>,
    channel: Option<ChannelKind>,
    limit: i64,
    offset: i64,
) -> AppResult<Vec<UsageLog>> {
    match (key, channel) {
        (Some(k), Some(c)) => Ok(sqlx::query_as::<_, UsageLog>(
            "SELECT id, time, model, input_tokens, output_tokens, cache_creation_tokens, cache_read_tokens, \
                    key_name, request_body, ip, cost_usd, channel_kind \
             FROM usage_logs WHERE key_name = $1 AND channel_kind = $2 \
             ORDER BY id DESC LIMIT $3 OFFSET $4",
        )
        .bind(k)
        .bind(c.as_str())
        .bind(limit)
        .bind(offset)
        .fetch_all(db.pool())
        .await?),
        (Some(k), None) => Ok(sqlx::query_as::<_, UsageLog>(
            "SELECT id, time, model, input_tokens, output_tokens, cache_creation_tokens, cache_read_tokens, \
                    key_name, request_body, ip, cost_usd, channel_kind \
             FROM usage_logs WHERE key_name = $1 \
             ORDER BY id DESC LIMIT $2 OFFSET $3",
        )
        .bind(k)
        .bind(limit)
        .bind(offset)
        .fetch_all(db.pool())
        .await?),
        (None, Some(c)) => Ok(sqlx::query_as::<_, UsageLog>(
            "SELECT id, time, model, input_tokens, output_tokens, cache_creation_tokens, cache_read_tokens, \
                    key_name, request_body, ip, cost_usd, channel_kind \
             FROM usage_logs WHERE channel_kind = $1 \
             ORDER BY id DESC LIMIT $2 OFFSET $3",
        )
        .bind(c.as_str())
        .bind(limit)
        .bind(offset)
        .fetch_all(db.pool())
        .await?),
        (None, None) => Ok(sqlx::query_as::<_, UsageLog>(
            "SELECT id, time, model, input_tokens, output_tokens, cache_creation_tokens, cache_read_tokens, \
                    key_name, request_body, ip, cost_usd, channel_kind \
             FROM usage_logs ORDER BY id DESC LIMIT $1 OFFSET $2",
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(db.pool())
        .await?),
    }
}

pub async fn get_usage_by_id(db: &Db, id: i64) -> AppResult<Option<UsageLog>> {
    let row = sqlx::query_as::<_, UsageLog>(
        "SELECT id, time, model, input_tokens, output_tokens, cache_creation_tokens, cache_read_tokens, \
                key_name, request_body, ip, cost_usd, channel_kind FROM usage_logs WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(db.pool())
    .await?;
    Ok(row)
}

pub fn export_usage_stream<'a>(
    db: &'a Db,
    key: Option<&'a str>,
    channel: Option<ChannelKind>,
) -> BoxStream<'a, Result<UsageLog, sqlx::Error>> {
    match (key, channel) {
        (Some(k), Some(c)) => sqlx::query_as::<_, UsageLog>(
            "SELECT id, time, model, input_tokens, output_tokens, cache_creation_tokens, cache_read_tokens, \
                    key_name, request_body, ip, cost_usd, channel_kind \
             FROM usage_logs WHERE key_name = $1 AND channel_kind = $2 ORDER BY id DESC",
        )
        .bind(k)
        .bind(c.as_str())
        .fetch(db.pool()),
        (Some(k), None) => sqlx::query_as::<_, UsageLog>(
            "SELECT id, time, model, input_tokens, output_tokens, cache_creation_tokens, cache_read_tokens, \
                    key_name, request_body, ip, cost_usd, channel_kind \
             FROM usage_logs WHERE key_name = $1 ORDER BY id DESC",
        )
        .bind(k)
        .fetch(db.pool()),
        (None, Some(c)) => sqlx::query_as::<_, UsageLog>(
            "SELECT id, time, model, input_tokens, output_tokens, cache_creation_tokens, cache_read_tokens, \
                    key_name, request_body, ip, cost_usd, channel_kind \
             FROM usage_logs WHERE channel_kind = $1 ORDER BY id DESC",
        )
        .bind(c.as_str())
        .fetch(db.pool()),
        (None, None) => sqlx::query_as::<_, UsageLog>(
            "SELECT id, time, model, input_tokens, output_tokens, cache_creation_tokens, cache_read_tokens, \
                    key_name, request_body, ip, cost_usd, channel_kind \
             FROM usage_logs ORDER BY id DESC",
        )
        .fetch(db.pool()),
    }
}
