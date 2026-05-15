//! 启动期 spend 预热 + 运行期统计聚合（供 /stats /timeseries /ips 用）。
//! cost_usd 列由 usage_writer 写入时落地，聚合时直接 SUM 不需要回查价表。

use crate::db::pool::Db;
use crate::db::schema::ChannelKind;
use crate::error::AppResult;
use serde::Serialize;
use sqlx::Row;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize)]
pub struct TimePoint {
    pub bucket: String,
    pub key_name: String,
    pub model: String,
    pub channel_kind: String,
    pub requests: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_creation_tokens: i64,
    pub cache_read_tokens: i64,
    pub cost_usd: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct IpAgg {
    pub ip: String,
    pub request_count: i64,
    pub first_seen: String,
    pub last_seen: String,
    pub keys_used: i64,
}

pub async fn init_spend_cache(db: &Db) -> AppResult<HashMap<String, f64>> {
    let rows = sqlx::query(
        "SELECT key_name, COALESCE(SUM(cost_usd), 0)::DOUBLE PRECISION AS total \
         FROM usage_logs GROUP BY key_name",
    )
    .fetch_all(db.pool())
    .await?;

    let mut out = HashMap::with_capacity(rows.len());
    for row in rows {
        let k: String = row.try_get("key_name")?;
        let v: f64 = row.try_get("total")?;
        out.insert(k, v);
    }
    Ok(out)
}

pub async fn total_requests(db: &Db, channel: Option<ChannelKind>) -> AppResult<i64> {
    let row = match channel {
        Some(ch) => {
            sqlx::query("SELECT COUNT(*) AS cnt FROM usage_logs WHERE channel_kind = $1")
                .bind(ch.as_str())
                .fetch_one(db.pool())
                .await?
        }
        None => sqlx::query("SELECT COUNT(*) AS cnt FROM usage_logs")
            .fetch_one(db.pool())
            .await?,
    };
    let n: i64 = row.try_get("cnt")?;
    Ok(n)
}

pub async fn total_errors(db: &Db, channel: Option<ChannelKind>) -> AppResult<i64> {
    let row = match channel {
        Some(ch) => {
            sqlx::query("SELECT COUNT(*) AS cnt FROM error_logs WHERE channel_kind = $1")
                .bind(ch.as_str())
                .fetch_one(db.pool())
                .await?
        }
        None => sqlx::query("SELECT COUNT(*) AS cnt FROM error_logs")
            .fetch_one(db.pool())
            .await?,
    };
    let n: i64 = row.try_get("cnt")?;
    Ok(n)
}

pub async fn timeseries(
    db: &Db,
    hours: i64,
    channel: Option<ChannelKind>,
) -> AppResult<Vec<TimePoint>> {
    let cutoff = chrono::Utc::now() - chrono::Duration::hours(hours.max(1));
    let cutoff_iso = cutoff.to_rfc3339();

    let base = "SELECT SUBSTRING(time FROM 1 FOR 13) || ':00:00' AS bucket, \
                 COUNT(*) AS requests, \
                 COALESCE(SUM(input_tokens), 0)::BIGINT AS input_tokens, \
                 COALESCE(SUM(output_tokens), 0)::BIGINT AS output_tokens, \
                 COALESCE(SUM(cache_creation_tokens), 0)::BIGINT AS cache_creation_tokens, \
                 COALESCE(SUM(cache_read_tokens), 0)::BIGINT AS cache_read_tokens, \
                 COALESCE(SUM(cost_usd), 0)::DOUBLE PRECISION AS cost_usd, \
                 key_name, model, channel_kind \
                FROM usage_logs WHERE time >= $1";

    let sql = match channel {
        Some(_) => format!("{base} AND channel_kind = $2 GROUP BY bucket, key_name, model, channel_kind ORDER BY bucket"),
        None => format!("{base} GROUP BY bucket, key_name, model, channel_kind ORDER BY bucket"),
    };

    let mut q = sqlx::query(&sql).bind(&cutoff_iso);
    if let Some(ch) = channel {
        q = q.bind(ch.as_str());
    }
    let rows = q.fetch_all(db.pool()).await?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push(TimePoint {
            bucket: row.try_get("bucket")?,
            key_name: row.try_get("key_name")?,
            model: row.try_get("model")?,
            channel_kind: row.try_get("channel_kind")?,
            requests: row.try_get("requests")?,
            input_tokens: row.try_get("input_tokens")?,
            output_tokens: row.try_get("output_tokens")?,
            cache_creation_tokens: row.try_get("cache_creation_tokens")?,
            cache_read_tokens: row.try_get("cache_read_tokens")?,
            cost_usd: row.try_get("cost_usd")?,
        });
    }
    Ok(out)
}

pub async fn usage_by_ip(db: &Db, channel: Option<ChannelKind>) -> AppResult<Vec<IpAgg>> {
    let base = "SELECT ip, COUNT(*) AS request_count, \
                  MIN(time) AS first_seen, MAX(time) AS last_seen, \
                  COUNT(DISTINCT key_name) AS keys_used \
                FROM usage_logs WHERE ip <> '' AND ip IS NOT NULL";

    let sql = match channel {
        Some(_) => format!("{base} AND channel_kind = $1 GROUP BY ip ORDER BY request_count DESC LIMIT 200"),
        None => format!("{base} GROUP BY ip ORDER BY request_count DESC LIMIT 200"),
    };

    let mut q = sqlx::query(&sql);
    if let Some(ch) = channel {
        q = q.bind(ch.as_str());
    }
    let rows = q.fetch_all(db.pool()).await?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push(IpAgg {
            ip: row.try_get("ip")?,
            request_count: row.try_get("request_count")?,
            first_seen: row.try_get("first_seen")?,
            last_seen: row.try_get("last_seen")?,
            keys_used: row.try_get("keys_used")?,
        });
    }
    Ok(out)
}
