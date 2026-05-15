//! 计费层：价表 + spend 累计 + usage_logs 异步写入 + snapshot 版本号。
//! snapshot_version 在 spend/concurrency/key CRUD 处递增，dashboard WS 据此决定是否推送。

pub mod pricing;
pub mod snapshot_version;
pub mod spend_cache;
pub mod usage_writer;

pub use pricing::{
    BillingRecord, CostBreakdown, PriceRate, anthropic_rate, calc_cost, copilot_rate, rate_for,
};
pub use snapshot_version::SnapshotVersion;
pub use spend_cache::SpendCache;
pub use usage_writer::{ErrorLogRecord, UsageWriter};
