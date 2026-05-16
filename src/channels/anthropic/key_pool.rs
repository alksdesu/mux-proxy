//! 官方 Anthropic key 池：纯随机选 + 401/403 直接熔断、429 滑动窗口累计。
//! 阈值 5 次 / 300s 窗口 / 900s 自动恢复，比 Copilot 渠道宽松。
//! DB 加载走 60s TTL + ``UpstreamChangeNotifier`` 信号触发即时重载。

use crate::channels::anthropic::upstream_key;
use crate::channels::{BreakerSnapshot, ChannelKind};
use crate::db::Db;
use crate::db::upstream::UpstreamChangeNotifier;
use crate::error::{AppError, AppResult};
use parking_lot::Mutex;
use rand::seq::SliceRandom;
use std::collections::HashMap;
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

#[derive(Debug, Default)]
struct BreakerEntry {
    failure_times: Vec<Instant>,
    open_until: Option<Instant>,
}

impl BreakerEntry {
    fn is_open(&mut self, now: Instant) -> bool {
        if let Some(deadline) = self.open_until {
            if now >= deadline {
                self.open_until = None;
                self.failure_times.clear();
                false
            } else {
                true
            }
        } else {
            false
        }
    }

    fn record_failure(&mut self, now: Instant) {
        self.failure_times.retain(|t| now.duration_since(*t) <= BREAKER_WINDOW);
        self.failure_times.push(now);
        if self.failure_times.len() as u32 >= BREAKER_THRESHOLD {
            self.open_until = Some(now + BREAKER_RECOVER);
            self.failure_times.clear();
        }
    }

    fn reset(&mut self) {
        self.failure_times.clear();
        self.open_until = None;
    }
}

struct PoolInner {
    keys: Vec<PooledKey>,
    loaded_at: Instant,
    breakers: HashMap<i64, BreakerEntry>,
}

impl PoolInner {
    fn empty() -> Self {
        Self {
            keys: Vec::new(),
            loaded_at: Instant::now() - POOL_TTL - Duration::from_secs(1),
            breakers: HashMap::new(),
        }
    }
}

pub struct KeyPool {
    inner: Mutex<PoolInner>,
    db: Db,
    notifier: UpstreamChangeNotifier,
    notify_handle: Arc<Notify>,
}

impl KeyPool {
    pub fn new(db: Db, notifier: UpstreamChangeNotifier) -> Arc<Self> {
        let notify_handle = notifier.handle();
        Arc::new(Self {
            inner: Mutex::new(PoolInner::empty()),
            db,
            notifier,
            notify_handle,
        })
    }

    /// 从池中拿一把可用 key。``exclude_ids`` 用于上轮命中 401 后避开同一把。
    /// 返回 None 表示池空或全部熔断。``is_open`` 命中会顺手把超时熔断器自动复位。
    pub async fn pick(&self, exclude_ids: &[i64]) -> AppResult<Option<PooledKey>> {
        self.ensure_fresh().await?;
        let mut guard = self.inner.lock();
        let now = Instant::now();
        let key_ids: Vec<i64> = guard
            .keys
            .iter()
            .map(|k| k.id)
            .filter(|id| !exclude_ids.contains(id))
            .collect();
        let mut alive_ids: Vec<i64> = Vec::with_capacity(key_ids.len());
        for id in key_ids {
            let open = guard
                .breakers
                .get_mut(&id)
                .map(|entry| entry.is_open(now))
                .unwrap_or(false);
            if !open {
                alive_ids.push(id);
            }
        }
        if alive_ids.is_empty() {
            return Ok(None);
        }
        let mut rng = rand::thread_rng();
        let chosen_id = match alive_ids.choose(&mut rng) {
            Some(id) => *id,
            None => return Ok(None),
        };
        Ok(guard.keys.iter().find(|k| k.id == chosen_id).cloned())
    }

    /// 上游返回 401/403 → 立刻把该 key 熔断（避免重试又用同一把）。
    pub fn report_auth_failure(&self, key_id: i64) {
        let mut guard = self.inner.lock();
        let entry = guard.breakers.entry(key_id).or_default();
        entry.open_until = Some(Instant::now() + BREAKER_RECOVER);
        warn!(key_id, "anthropic key auth failed, breaker opened");
    }

    /// 上游 429 → 滑动窗口累计失败计数，到阈值开熔断。
    pub fn report_rate_limited(&self, key_id: i64) {
        let mut guard = self.inner.lock();
        let entry = guard.breakers.entry(key_id).or_default();
        entry.record_failure(Instant::now());
        debug!(
            key_id,
            failures = entry.failure_times.len(),
            "anthropic key rate limited, recorded"
        );
    }

    /// 业务上验证 key 健康（成功响应）时清掉历史失败。
    pub fn report_success(&self, key_id: i64) {
        let mut guard = self.inner.lock();
        if let Some(entry) = guard.breakers.get_mut(&key_id) {
            entry.reset();
        }
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
        let guard = self.inner.lock();
        let now = Instant::now();
        guard
            .breakers
            .iter()
            .filter_map(|(id, e)| {
                let disabled = matches!(e.open_until, Some(d) if now < d);
                if !disabled && e.failure_times.is_empty() {
                    return None;
                }
                let (count, anchor) = if disabled {
                    let trip = e.open_until.and_then(|d| d.checked_sub(BREAKER_RECOVER)).unwrap_or(now);
                    (BREAKER_THRESHOLD, (trip, trip))
                } else {
                    let first = e.failure_times.first().copied().unwrap_or(now);
                    let last = e.failure_times.last().copied().unwrap_or(now);
                    (e.failure_times.len() as u32, (first, last))
                };
                Some(BreakerSnapshot {
                    id: *id,
                    channel_kind: ChannelKind::Anthropic,
                    count,
                    disabled,
                    first_at_ms_ago: now.checked_duration_since(anchor.0).unwrap_or_default().as_millis(),
                    last_at_ms_ago: now.checked_duration_since(anchor.1).unwrap_or_default().as_millis(),
                })
            })
            .collect()
    }

    /// admin 手动恢复：清掉指定 key 的失败计数与 open_until。
    pub fn reset_breaker(&self, key_id: i64) {
        let mut guard = self.inner.lock();
        if let Some(entry) = guard.breakers.get_mut(&key_id) {
            entry.reset();
        }
    }

    /// admin 手动熔断：把指定 key 强制置为 open，``BREAKER_RECOVER`` 后自动恢复。
    pub fn force_disable_breaker(&self, key_id: i64) {
        let mut guard = self.inner.lock();
        let entry = guard.breakers.entry(key_id).or_default();
        entry.open_until = Some(Instant::now() + BREAKER_RECOVER);
        entry.failure_times.clear();
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
                breakers: HashMap::new(),
            }),
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
    fn breaker_opens_at_threshold() {
        let mut b = BreakerEntry::default();
        let now = Instant::now();
        for _ in 0..BREAKER_THRESHOLD - 1 {
            b.record_failure(now);
            assert!(!b.is_open(now));
        }
        b.record_failure(now);
        assert!(b.is_open(now));
    }

    #[test]
    fn breaker_window_drops_old_failures() {
        let mut b = BreakerEntry::default();
        let old = Instant::now() - BREAKER_WINDOW - Duration::from_secs(10);
        b.failure_times = vec![old; 4];
        let now = Instant::now();
        b.record_failure(now);
        assert!(!b.is_open(now), "old failures must not count");
        assert_eq!(b.failure_times.len(), 1);
    }

    #[test]
    fn breaker_recovers_after_window() {
        let mut b = BreakerEntry::default();
        let now = Instant::now();
        b.open_until = Some(now - Duration::from_secs(1));
        assert!(!b.is_open(now), "expired breaker must auto-close");
    }

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
