//! sqlx 行模型与入参 struct。time 列是 TEXT/ISO 字符串，一律 String 接。
//! bool 列在 PG 是 INTEGER 0/1，用 i32 接，业务面暴露 bool。
//! ChannelKind 直接复用业务层的 `crate::channels::ChannelKind`，避免双份枚举漂移。

pub use crate::channels::ChannelKind;

use crate::channels::anthropic::model_splice::RewriteRule;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgRow;
use sqlx::Row;

#[derive(Debug, Clone, Serialize)]
pub struct ApiKey {
    pub id: i64,
    pub key: String,
    pub name: String,
    pub upstream_key: String,
    pub quota: f64,
    pub allow_fast: bool,
    pub max_concurrency: i64,
    pub rpm_limit: i64,
    /// 逗号分隔的精确 model 白名单；空串 = 不限制。比对前两边 lowercase。
    pub allowed_models: String,
    pub created_at: String,
    pub channel_kind: ChannelKind,
}

impl<'r> sqlx::FromRow<'r, PgRow> for ApiKey {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let allow_fast_int: i32 = row.try_get("allow_fast")?;
        Ok(ApiKey {
            id: row.try_get("id")?,
            key: row.try_get("key")?,
            name: row.try_get("name")?,
            upstream_key: row.try_get("upstream_key")?,
            quota: row.try_get("quota")?,
            allow_fast: allow_fast_int != 0,
            max_concurrency: row.try_get("max_concurrency")?,
            rpm_limit: row.try_get("rpm_limit").unwrap_or(-1),
            allowed_models: row.try_get("allowed_models").unwrap_or_default(),
            created_at: row.try_get("created_at")?,
            channel_kind: row.try_get("channel_kind")?,
        })
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ApiKeyPatch {
    pub name: Option<String>,
    pub upstream_key: Option<String>,
    pub quota: Option<f64>,
    pub allow_fast: Option<bool>,
    pub max_concurrency: Option<i64>,
    pub rpm_limit: Option<i64>,
    pub allowed_models: Option<String>,
    pub channel_kind: Option<ChannelKind>,
}

#[derive(Debug, Clone, Serialize)]
pub struct UsageLog {
    pub id: i64,
    pub time: String,
    pub model: String,
    pub input_tokens: i64,
    pub output_tokens: i64,
    /// 5m + 1h 总和。旧字段保留兼容旧客户端；新明细看下面两列。
    pub cache_creation_tokens: i64,
    pub cache_creation_5m_tokens: i64,
    pub cache_creation_1h_tokens: i64,
    pub cache_read_tokens: i64,
    pub key_name: String,
    pub request_body: String,
    pub ip: String,
    pub cost_usd: f64,
    pub channel_kind: ChannelKind,
}

impl<'r> sqlx::FromRow<'r, PgRow> for UsageLog {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        Ok(UsageLog {
            id: row.try_get("id")?,
            time: row.try_get("time")?,
            model: row.try_get("model")?,
            input_tokens: row.try_get("input_tokens")?,
            output_tokens: row.try_get("output_tokens")?,
            cache_creation_tokens: row.try_get("cache_creation_tokens")?,
            // migration 007 之前的行没有这两列；try_get 失败 unwrap_or(0) 兼容历史 fixture。
            cache_creation_5m_tokens: row.try_get("cache_creation_5m_tokens").unwrap_or(0),
            cache_creation_1h_tokens: row.try_get("cache_creation_1h_tokens").unwrap_or(0),
            cache_read_tokens: row.try_get("cache_read_tokens")?,
            key_name: row.try_get("key_name")?,
            request_body: row.try_get::<Option<String>, _>("request_body")?.unwrap_or_default(),
            ip: row.try_get("ip")?,
            cost_usd: row.try_get("cost_usd")?,
            channel_kind: row.try_get("channel_kind")?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct UsageLogInput {
    pub time: String,
    pub model: String,
    pub input_tokens: i64,
    pub output_tokens: i64,
    /// 5m + 1h 总和。usage_writer 写入时由 5m_tokens + 1h_tokens 派生，无需调用方手填。
    pub cache_creation_tokens: i64,
    pub cache_creation_5m_tokens: i64,
    pub cache_creation_1h_tokens: i64,
    pub cache_read_tokens: i64,
    pub key_name: String,
    pub request_body: String,
    pub ip: String,
    pub cost_usd: f64,
    pub channel_kind: ChannelKind,
}

#[derive(Debug, Clone, Serialize)]
pub struct ErrorLog {
    pub id: i64,
    pub time: String,
    pub key_name: String,
    pub status: i32,
    pub path: String,
    pub model: String,
    pub request_body: String,
    pub response_body: String,
    pub ip: String,
    pub channel_kind: ChannelKind,
}

impl<'r> sqlx::FromRow<'r, PgRow> for ErrorLog {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        Ok(ErrorLog {
            id: row.try_get("id")?,
            time: row.try_get("time")?,
            key_name: row.try_get("key_name")?,
            status: row.try_get("status")?,
            path: row.try_get("path")?,
            model: row.try_get("model")?,
            request_body: row.try_get::<Option<String>, _>("request_body")?.unwrap_or_default(),
            response_body: row.try_get::<Option<String>, _>("response_body")?.unwrap_or_default(),
            ip: row.try_get("ip")?,
            channel_kind: row.try_get("channel_kind")?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ErrorLogInput {
    pub time: String,
    pub key_name: String,
    pub status: i32,
    pub path: String,
    pub model: String,
    pub request_body: String,
    pub response_body: String,
    pub ip: String,
    pub channel_kind: ChannelKind,
}

#[derive(Debug, Clone, Serialize)]
pub struct UpstreamKey {
    pub id: i64,
    pub key: String,
    pub name: String,
    pub enabled: bool,
    pub note: String,
    pub created_at: String,
    pub channel_kind: ChannelKind,
    /// NULL → 落全局 anthropic_rewrite_rules；非 NULL → 完整覆盖全局规则集。
    /// 仅 Anthropic 渠道生效，其它渠道字段被忽略。
    pub rewrite_rules: Option<Vec<RewriteRule>>,
    /// NULL → 该 key 接受所有 model；非 NULL 数组 → 精确白名单（比较前两侧 lowercase）。
    pub allowed_models: Option<Vec<String>>,
}

impl<'r> sqlx::FromRow<'r, PgRow> for UpstreamKey {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let enabled_int: i32 = row.try_get("enabled")?;
        let id_for_log: Option<i64> = row.try_get("id").ok();
        // migration 008 之前的 fixture 行没有这两列；列缺失 → None 兼容旧 schema。
        // JSONB 内容损坏（运维手动 SQL 写了非数组结构）也会落 None，但**必须 warn**
        // 否则 admin 看 dashboard 显示该 key 没 per-key 配置，DB 里其实有脏数据，定时炸弹。
        let rewrite_rules = match row
            .try_get::<Option<sqlx::types::Json<Vec<RewriteRule>>>, _>("rewrite_rules")
        {
            Ok(opt) => opt.map(|j| j.0),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    key_id = ?id_for_log,
                    "upstream_keys.rewrite_rules JSONB malformed; falling back to global rules",
                );
                None
            }
        };
        let allowed_models = match row
            .try_get::<Option<sqlx::types::Json<Vec<String>>>, _>("allowed_models")
        {
            Ok(opt) => opt.map(|j| j.0),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    key_id = ?id_for_log,
                    "upstream_keys.allowed_models JSONB malformed; falling back to no restriction",
                );
                None
            }
        };
        Ok(UpstreamKey {
            id: row.try_get("id")?,
            key: row.try_get("key")?,
            name: row.try_get("name")?,
            enabled: enabled_int != 0,
            note: row.try_get::<Option<String>, _>("note")?.unwrap_or_default(),
            created_at: row.try_get("created_at")?,
            channel_kind: row.try_get("channel_kind")?,
            rewrite_rules,
            allowed_models,
        })
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct UpstreamKeyPatch {
    pub key: Option<String>,
    pub name: Option<String>,
    pub enabled: Option<bool>,
    pub note: Option<String>,
    pub channel_kind: Option<ChannelKind>,
    /// 空数组 → 清回 NULL（落全局兜底）；非空数组 → 完整覆盖；缺失 → 不改。
    pub rewrite_rules: Option<Vec<RewriteRule>>,
    /// 空数组 → 清回 NULL（无 model 限制）；非空 → 精确白名单；缺失 → 不改。
    pub allowed_models: Option<Vec<String>>,
}
