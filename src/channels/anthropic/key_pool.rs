//! 官方 Anthropic key 池：纯随机选 + 401/403 直接熔断、429 滑动窗口累计。
//! 熔断逻辑走 ``shared::breaker`` 泛型实现；本模块负责 keys 缓存 + DB 同步 +
//! ``BreakerInfo`` 到带 ``ChannelKind`` 的 ``BreakerSnapshot`` 的 wrap。

use crate::channels::anthropic::upstream_key;
use crate::channels::{BreakerSnapshot, ChannelKind};
use crate::db::Db;
use crate::db::upstream::UpstreamChangeNotifier;
use crate::error::{AppError, AppResult};
use crate::shared::breaker::{
    Breaker as SharedBreaker, BreakerConfig, SlidingWindowStrategy,
};
use parking_lot::Mutex;
use rand::seq::SliceRandom;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tracing::{debug, warn};

pub const POOL_TTL: Duration = Duration::from_secs(60);
pub const BREAKER_THRESHOLD: u32 = 5;
pub const BREAKER_WINDOW: Duration = Duration::from_secs(300);
pub const BREAKER_RECOVER: Duration = Duration::from_secs(900);

#[derive(Debug, Clone)]
pub struct PooledKey {
    pub id: i64,
    pub name: String,
    /// 裸 token (``sk-ant-xxx``)。已经被 ``upstream_key::parse`` 校验过。
    pub token: String,
}

struct PoolInner {
    keys: Vec<PooledKey>,
    loaded_at: Instant,
}

impl PoolInner {
    fn empty() -> Self {
        Self {
            keys: Vec::new(),
            loaded_at: Instant::now() - POOL_TTL - Duration::from_secs(1),
        }
    }
}

pub struct KeyPool {
    inner: Mutex<PoolInner>,
    breaker: SharedBreaker<SlidingWindowStrategy>,
    db: Db,
    notifier: UpstreamChangeNotifier,
    notify_handle: Arc<Notify>,
}

fn new_breaker() -> SharedBreaker<SlidingWindowStrategy> {
    SharedBreaker::new(BreakerConfig {
        threshold: BREAKER_THRESHOLD,
        window: BREAKER_WINDOW,
        recover: BREAKER_RECOVER,
    })
}

impl KeyPool {
    pub fn new(db: Db, notifier: UpstreamChangeNotifier) -> Arc<Self> {
        let notify_handle = notifier.handle();
        Arc::new(Self {
            inner: Mutex::new(PoolInner::empty()),
            breaker: new_breaker(),
            db,
            notifier,
            notify_handle,
        })
    }

    /// 从池中拿一把可用 key。``exclude_ids`` 用于上轮命中 401 后避开同一把。
    /// 返回 None 表示池空或全部熔断。``is_disabled`` 命中会顺手把超时熔断自动复位。
    pub async fn pick(&self, exclude_ids: &[i64]) -> AppResult<Option<PooledKey>> {
        self.ensure_fresh().await?;
        let guard = self.inner.lock();
        let alive: Vec<&PooledKey> = guard
            .keys
            .iter()
            .filter(|k| !exclude_ids.contains(&k.id) && !self.breaker.is_disabled(k.id))
            .collect();
        if alive.is_empty() {
            return Ok(None);
        }
        let mut rng = rand::thread_rng();
        Ok(alive.choose(&mut rng).map(|k| (*k).clone()))
    }

    /// 上游返回 401/403 → 立刻把该 key 熔断（避免重试又用同一把）。
    pub fn report_auth_failure(&self, key_id: i64) {
        self.breaker.force_disable(key_id);
        warn!(key_id, "anthropic key auth failed, breaker opened");
    }

    /// 上游 429 → 滑动窗口累计失败计数，到阈值开熔断。
    pub fn report_rate_limited(&self, key_id: i64) {
        let tripped = self.breaker.record_failure(key_id);
        debug!(key_id, tripped, "anthropic key rate limited, recorded");
    }

    /// 业务上验证 key 健康（成功响应）时清掉历史失败。
    pub fn report_success(&self, key_id: i64) {
        self.breaker.reset(key_id);
    }

    /// 强制下次 ``pick`` 重新加载 DB。``Notifier::notify`` 之后 admin 端的改动应该已落库。
    pub fn force_reload(&self) {
        let mut guard = self.inner.lock();
        guard.loaded_at = Instant::now() - POOL_TTL - Duration::from_secs(1);
    }

    /// 后台任务：监听 ``UpstreamChangeNotifier::notify`` 触发立即重载。
    pub async fn run_change_listener(self: Arc<Self>) {
        loop {
            self.notify_handle.notified().await;
            self.force_reload();
        }
    }

    async fn ensure_fresh(&self) -> AppResult<()> {
        let need_reload = {
            let guard = self.inner.lock();
            guard.loaded_at.elapsed() >= POOL_TTL || guard.keys.is_empty()
        };
        if !need_reload {
            return Ok(());
        }
        let rows = crate::db::upstream::list_enabled(&self.db, ChannelKind::Anthropic).await?;
        let mut parsed: Vec<PooledKey> = Vec::with_capacity(rows.len());
        for r in rows {
            match upstream_key::parse(&r.key) {
                Ok(k) => parsed.push(PooledKey {
                    id: r.id,
                    name: r.name,
                    token: k.token,
                }),
                Err(e) => warn!(id = r.id, error = ?e, "skip invalid anthropic upstream key"),
            }
        }
        let mut guard = self.inner.lock();
        guard.keys = parsed;
        guard.loaded_at = Instant::now();
        Ok(())
    }

    pub fn notifier(&self) -> &UpstreamChangeNotifier {
        &self.notifier
    }

    pub fn snapshot_breakers(&self) -> Vec<BreakerSnapshot> {
        self.breaker
            .snapshot()
            .into_iter()
            .map(|info| BreakerSnapshot {
                id: info.id,
                channel_kind: ChannelKind::Anthropic,
                count: info.count,
                disabled: info.disabled,
                first_at_ms_ago: info.first_at_ms_ago,
                last_at_ms_ago: info.last_at_ms_ago,
            })
            .collect()
    }

    /// admin 手动恢复：清掉指定 key 的失败计数与 open_until。
    pub fn reset_breaker(&self, key_id: i64) {
        self.breaker.reset(key_id);
    }

    /// admin 手动熔断：把指定 key 强制置为 open，``BREAKER_RECOVER`` 后自动恢复。
    pub fn force_disable_breaker(&self, key_id: i64) {
        self.breaker.force_disable(key_id);
    }

    /// 集成测试用：跳过 DB 加载，把 keys 直接塞进 inner state，
    /// loaded_at 标记为 now 让 ensure_fresh 视作"刚拉过 DB"。
    #[doc(hidden)]
    pub fn test_only_with_keys(
        keys: Vec<PooledKey>,
        db: Db,
        notifier: UpstreamChangeNotifier,
    ) -> Arc<Self> {
        let notify_handle = notifier.handle();
        Arc::new(Self {
            inner: Mutex::new(PoolInner {
                keys,
                loaded_at: Instant::now(),
            }),
            breaker: new_breaker(),
            db,
            notifier,
            notify_handle,
        })
    }
}

/// 上游返回的状态映射到 KeyPool 的反馈动作。``call_site`` 用于日志区分。
pub fn classify_status(status: u16) -> KeyFeedback {
    match status {
        401 | 403 => KeyFeedback::AuthFailure,
        429 => KeyFeedback::RateLimited,
        s if (200..300).contains(&s) => KeyFeedback::Success,
        _ => KeyFeedback::Neutral,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyFeedback {
    Success,
    AuthFailure,
    RateLimited,
    Neutral,
}

impl KeyPool {
    pub fn apply_feedback(&self, key_id: i64, fb: KeyFeedback) {
        match fb {
            KeyFeedback::AuthFailure => self.report_auth_failure(key_id),
            KeyFeedback::RateLimited => self.report_rate_limited(key_id),
            KeyFeedback::Success => self.report_success(key_id),
            KeyFeedback::Neutral => {}
        }
    }
}

/// 池空时给 handler 兜底用的错误。
pub fn pool_empty_error() -> AppError {
    AppError::Upstream("no anthropic upstream key available".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_status_buckets() {
        assert_eq!(classify_status(200), KeyFeedback::Success);
        assert_eq!(classify_status(204), KeyFeedback::Success);
        assert_eq!(classify_status(401), KeyFeedback::AuthFailure);
        assert_eq!(classify_status(403), KeyFeedback::AuthFailure);
        assert_eq!(classify_status(429), KeyFeedback::RateLimited);
        assert_eq!(classify_status(500), KeyFeedback::Neutral);
        assert_eq!(classify_status(502), KeyFeedback::Neutral);
    }
}
