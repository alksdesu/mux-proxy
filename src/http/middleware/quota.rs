//! Quota 中间件：从 extensions 取 KeyCacheEntry，对照 state.spend.get(name) 与 entry.quota。
//! quota=-1 不限，quota=0 全禁，quota>0 美元上限。超即 429 rate_limit_error。

use crate::app::AppState;
use crate::auth::KeyCacheEntry;
use crate::billing::SpendCache;
use crate::error::AppError;
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::Response;

/// 占位类型，供 router 装配引用名字。
pub struct Quota;

pub async fn quota_layer(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Result<Response, AppError> {
    let entry = req
        .extensions()
        .get::<KeyCacheEntry>()
        .ok_or_else(|| AppError::Internal("quota layer requires client_auth first".into()))?;
    enforce_quota(entry, &state.spend)?;
    Ok(next.run(req).await)
}

fn enforce_quota(entry: &KeyCacheEntry, spend: &SpendCache) -> Result<(), AppError> {
    if entry.quota == 0.0 {
        return Err(AppError::QuotaExceeded);
    }
    if entry.quota > 0.0 && spend.get(&entry.name) >= entry.quota {
        return Err(AppError::QuotaExceeded);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channels::ChannelKind;
    use std::time::Instant;

    fn entry(name: &str, quota: f64) -> KeyCacheEntry {
        KeyCacheEntry {
            id: 1,
            name: name.into(),
            upstream_key: "*".into(),
            quota,
            allow_fast: true,
            max_concurrency: -1,
            channel_kind: ChannelKind::Copilot,
            fetched_at: Instant::now(),
        }
    }

    #[test]
    fn unlimited_quota_passes_even_with_high_spend() {
        let spend = SpendCache::new();
        spend.add("alice", 9999.0);
        assert!(enforce_quota(&entry("alice", -1.0), &spend).is_ok());
    }

    #[test]
    fn zero_quota_always_rejects() {
        let spend = SpendCache::new();
        assert!(matches!(
            enforce_quota(&entry("alice", 0.0), &spend),
            Err(AppError::QuotaExceeded)
        ));
    }

    #[test]
    fn quota_below_limit_passes() {
        let spend = SpendCache::new();
        spend.add("alice", 4.99);
        assert!(enforce_quota(&entry("alice", 5.0), &spend).is_ok());
    }

    #[test]
    fn quota_at_limit_rejects() {
        let spend = SpendCache::new();
        spend.add("alice", 5.0);
        assert!(matches!(
            enforce_quota(&entry("alice", 5.0), &spend),
            Err(AppError::QuotaExceeded)
        ));
    }

    #[test]
    fn quota_status_code_is_429() {
        assert_eq!(AppError::QuotaExceeded.status_code(), 429);
    }
}
