//! 计费记账落库器：异步重试写 DB，写失败不进 spend 防虚账。
//! INSERT 成功后按 per-key 计数器节流 spawn cleanup（usage 每 50 条一次、error 每 20 条），
//! 避免高 QPS 时每条写入都跑一次 DELETE WHERE NOT IN 把 PG 打爆。

use crate::billing::pricing::{BillingRecord, calc_cost};
use crate::billing::snapshot_version::SnapshotVersion;
use crate::billing::spend_cache::SpendCache;
use crate::channels::ChannelKind;
use crate::db::Db;
use crate::db::schema::{ErrorLogInput, UsageLogInput};
use crate::util::retry::with_retry;
use chrono::Utc;
use dashmap::DashMap;
use std::sync::Arc;
use tracing::error;

#[derive(Debug, Clone)]
pub struct ErrorLogRecord {
    pub channel: ChannelKind,
    pub key_name: String,
    pub status: u16,
    pub path: String,
    pub model: String,
    pub request_body: String,
    pub response_body: String,
    pub ip: String,
}

#[derive(Clone)]
pub struct UsageWriter {
    db: Db,
    spend: Arc<SpendCache>,
    snapshot: Arc<SnapshotVersion>,
    usage_cleanup_counters: Arc<DashMap<String, u32>>,
    error_cleanup_counters: Arc<DashMap<String, u32>>,
}

const WRITE_MAX_ATTEMPTS: u32 = 3;
const WRITE_BACKOFF_MS: u64 = 200;
const USAGE_CLEANUP_INTERVAL: u32 = 50;
const ERROR_CLEANUP_INTERVAL: u32 = 20;

impl UsageWriter {
    pub fn new(db: Db, spend: Arc<SpendCache>, snapshot: Arc<SnapshotVersion>) -> Self {
        Self {
            db,
            spend,
            snapshot,
            usage_cleanup_counters: Arc::new(DashMap::new()),
            error_cleanup_counters: Arc::new(DashMap::new()),
        }
    }

    /// 节流计数器：累计到 interval 就归零并返回 true。DashMap entry 持有 shard 写锁，
    /// 高并发同 key 不会出现 lost update。
    fn should_cleanup(counters: &DashMap<String, u32>, key_name: &str, interval: u32) -> bool {
        let mut entry = counters.entry(key_name.to_string()).or_insert(0);
        *entry += 1;
        if *entry >= interval {
            *entry = 0;
            true
        } else {
            false
        }
    }

    /// 异步写一条 usage_log。spend 只在 DB 写入成功后累加，避免长期与库失同步。
    pub fn record(&self, rec: BillingRecord) {
        let cost = calc_cost(&rec);
        let db = self.db.clone();
        let spend = self.spend.clone();
        let snapshot = self.snapshot.clone();
        let counters = self.usage_cleanup_counters.clone();

        tokio::spawn(async move {
            let input = UsageLogInput {
                time: Utc::now().to_rfc3339(),
                model: rec.model.clone(),
                input_tokens: rec.input_tokens as i64,
                output_tokens: rec.output_tokens as i64,
                cache_creation_tokens: rec.cache_creation_tokens as i64,
                cache_read_tokens: rec.cache_read_tokens as i64,
                key_name: rec.key_name.clone(),
                request_body: rec.request_body.clone(),
                ip: rec.ip.clone().unwrap_or_default(),
                cost_usd: cost,
                channel_kind: rec.channel,
            };

            let result = with_retry(WRITE_MAX_ATTEMPTS, WRITE_BACKOFF_MS, || {
                let db = db.clone();
                let input = input.clone();
                async move { crate::db::usage::insert_usage(&db, input).await }
            })
            .await;

            match result {
                Ok(_) => {
                    spend.add(&rec.key_name, cost);
                    snapshot.bump();
                    if Self::should_cleanup(&counters, &rec.key_name, USAGE_CLEANUP_INTERVAL) {
                        let db_cleanup = db.clone();
                        let key = rec.key_name.clone();
                        tokio::spawn(async move {
                            if let Err(e) =
                                crate::db::usage::cleanup_request_bodies(&db_cleanup, &key).await
                            {
                                error!(key = %key, error = ?e, "usage cleanup failed");
                            }
                        });
                    }
                }
                Err(e) => {
                    error!(key = %rec.key_name, error = ?e, "usage write failed after retries");
                }
            }
        });
    }

    /// 错误日志写入；调用方在 response_body 头部加 [local] 前缀表示本地拒绝。
    pub fn record_error(&self, rec: ErrorLogRecord) {
        let db = self.db.clone();
        let counters = self.error_cleanup_counters.clone();

        tokio::spawn(async move {
            let input = ErrorLogInput {
                time: Utc::now().to_rfc3339(),
                key_name: rec.key_name.clone(),
                status: rec.status as i32,
                path: rec.path.clone(),
                model: rec.model.clone(),
                request_body: rec.request_body.clone(),
                response_body: rec.response_body.clone(),
                ip: rec.ip.clone(),
                channel_kind: rec.channel,
            };

            let result = with_retry(WRITE_MAX_ATTEMPTS, WRITE_BACKOFF_MS, || {
                let db = db.clone();
                let input = input.clone();
                async move { crate::db::errors::insert_error(&db, input).await }
            })
            .await;

            match result {
                Ok(_) => {
                    if Self::should_cleanup(&counters, &rec.key_name, ERROR_CLEANUP_INTERVAL) {
                        let db_cleanup = db.clone();
                        let key = rec.key_name.clone();
                        tokio::spawn(async move {
                            if let Err(e) =
                                crate::db::errors::cleanup_old_errors(&db_cleanup, &key).await
                            {
                                error!(key = %key, error = ?e, "error cleanup failed");
                            }
                        });
                    }
                }
                Err(e) => {
                    error!(key = %rec.key_name, error = ?e, "error write failed after retries");
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_cleanup_fires_at_interval() {
        let counters: DashMap<String, u32> = DashMap::new();
        for _ in 0..49 {
            assert!(!UsageWriter::should_cleanup(&counters, "k", 50));
        }
        assert!(UsageWriter::should_cleanup(&counters, "k", 50));
        // 触发后归零，下一轮再 49 次不触发
        for _ in 0..49 {
            assert!(!UsageWriter::should_cleanup(&counters, "k", 50));
        }
        assert!(UsageWriter::should_cleanup(&counters, "k", 50));
    }

    #[test]
    fn should_cleanup_per_key_independent() {
        let counters: DashMap<String, u32> = DashMap::new();
        for _ in 0..50 {
            UsageWriter::should_cleanup(&counters, "a", 50);
        }
        // a 刚触发归零后再 +1，b 才第一次
        assert_eq!(*counters.get("a").unwrap(), 0);
        UsageWriter::should_cleanup(&counters, "b", 50);
        assert_eq!(*counters.get("b").unwrap(), 1);
        assert_eq!(*counters.get("a").unwrap(), 0);
    }

    #[test]
    fn should_cleanup_interval_one_always_fires() {
        let counters: DashMap<String, u32> = DashMap::new();
        for _ in 0..10 {
            assert!(UsageWriter::should_cleanup(&counters, "x", 1));
        }
    }
}
