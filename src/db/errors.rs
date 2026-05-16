//! error_logs 写入 + 查询 + cleanup。cleanup 是 pub 函数，
//! 调用方在 INSERT 完成后顺序触发，避免 DELETE 抢在前。

use crate::db::pool::Db;
use crate::db::schema::{ChannelKind, ErrorLog, ErrorLogInput};
use crate::error::AppResult;
use sqlx::Row;

pub const ERROR_LOG_LIMIT: i64 = 200;

pub async fn insert_error(db: &Db, input: ErrorLogInput) -> AppResult<i64> {
    let row = sqlx::query(
        "INSERT INTO error_logs \
            (time, key_name, status, path, model, request_body, response_body, ip, channel_kind) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
         RETURNING id",
    )
    .bind(&input.time)
    .bind(&input.key_name)
    .bind(input.status)
    .bind(&input.path)
    .bind(&input.model)
    .bind(&input.request_body)
    .bind(&input.response_body)
    .bind(&input.ip)
    .bind(input.channel_kind.as_str())
    .fetch_one(db.pool())
    .await?;

    let id: i64 = row.try_get("id")?;
    Ok(id)
}

pub async fn cleanup_old_errors(db: &Db, key_name: &str) -> AppResult<()> {
    sqlx::query(
        "DELETE FROM error_logs \
         WHERE key_name = $1 \
           AND id NOT IN ( \
             SELECT id FROM error_logs \
              WHERE key_name = $1 \
              ORDER BY id DESC LIMIT $2 \
           )",
    )
    .bind(key_name)
    .bind(ERROR_LOG_LIMIT)
    .execute(db.pool())
    .await?;
    Ok(())
}

pub async fn list_errors(
    db: &Db,
    key: Option<&str>,
    channel: Option<ChannelKind>,
    limit: i64,
    offset: i64,
) -> AppResult<Vec<ErrorLog>> {
    match (key, channel) {
        (Some(k), Some(c)) => Ok(sqlx::query_as::<_, ErrorLog>(
            "SELECT id, time, key_name, status, path, model, request_body, response_body, ip, channel_kind \
             FROM error_logs WHERE key_name = $1 AND channel_kind = $2 \
             ORDER BY id DESC LIMIT $3 OFFSET $4",
        )
        .bind(k)
        .bind(c.as_str())
        .bind(limit)
        .bind(offset)
        .fetch_all(db.pool())
        .await?),
        (Some(k), None) => Ok(sqlx::query_as::<_, ErrorLog>(
            "SELECT id, time, key_name, status, path, model, request_body, response_body, ip, channel_kind \
             FROM error_logs WHERE key_name = $1 \
             ORDER BY id DESC LIMIT $2 OFFSET $3",
        )
        .bind(k)
        .bind(limit)
        .bind(offset)
        .fetch_all(db.pool())
        .await?),
        (None, Some(c)) => Ok(sqlx::query_as::<_, ErrorLog>(
            "SELECT id, time, key_name, status, path, model, request_body, response_body, ip, channel_kind \
             FROM error_logs WHERE channel_kind = $1 \
             ORDER BY id DESC LIMIT $2 OFFSET $3",
        )
        .bind(c.as_str())
        .bind(limit)
        .bind(offset)
        .fetch_all(db.pool())
        .await?),
        (None, None) => Ok(sqlx::query_as::<_, ErrorLog>(
            "SELECT id, time, key_name, status, path, model, request_body, response_body, ip, channel_kind \
             FROM error_logs ORDER BY id DESC LIMIT $1 OFFSET $2",
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(db.pool())
        .await?),
    }
}

pub async fn get_error_by_id(db: &Db, id: i64) -> AppResult<Option<ErrorLog>> {
    let row = sqlx::query_as::<_, ErrorLog>(
        "SELECT id, time, key_name, status, path, model, request_body, response_body, ip, channel_kind \
         FROM error_logs WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(db.pool())
    .await?;
    Ok(row)
}

pub async fn delete_all(
    db: &Db,
    key: Option<&str>,
    channel: Option<ChannelKind>,
) -> AppResult<u64> {
    let affected = match (key, channel) {
        (Some(k), Some(c)) => sqlx::query(
            "DELETE FROM error_logs WHERE key_name = $1 AND channel_kind = $2",
        )
        .bind(k)
        .bind(c.as_str())
        .execute(db.pool())
        .await?
        .rows_affected(),
        (Some(k), None) => sqlx::query("DELETE FROM error_logs WHERE key_name = $1")
            .bind(k)
            .execute(db.pool())
            .await?
            .rows_affected(),
        (None, Some(c)) => sqlx::query("DELETE FROM error_logs WHERE channel_kind = $1")
            .bind(c.as_str())
            .execute(db.pool())
            .await?
            .rows_affected(),
        (None, None) => sqlx::query("DELETE FROM error_logs")
            .execute(db.pool())
            .await?
            .rows_affected(),
    };
    Ok(affected)
}
