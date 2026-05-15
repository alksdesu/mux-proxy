//! 上游 key 429 滑窗熔断。窗口内累计达阈值就 disable，过 recover 时长自动恢复并清零。
//! threshold/window/recover 数值与旧 TS 版完全一致，便于灰度对比观测。

use dashmap::DashMap;
use std::time::{Duration, Instant};

pub const BREAKER_THRESHOLD: u32 = 10;
pub const BREAKER_WINDOW: Duration = Duration::from_secs(600);
pub const BREAKER_RECOVER: Duration = Duration::from_secs(1800);

#[derive(Clone, Debug)]
struct Entry {
    count: u32,
    first_at: Instant,
    last_at: Instant,
    disabled_at: Option<Instant>,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct BreakerSnapshot {
    pub id: i64,
    pub count: u32,
    pub disabled: bool,
    pub first_at_ms_ago: u128,
    pub last_at_ms_ago: u128,
}

pub struct Breaker {
    inner: DashMap<i64, Entry>,
    threshold: u32,
    window: Duration,
    recover: Duration,
}

impl Breaker {
    pub fn new() -> Self {
        Self::with(BREAKER_THRESHOLD, BREAKER_WINDOW, BREAKER_RECOVER)
    }

    pub fn with(threshold: u32, window: Duration, recover: Duration) -> Self {
        Self {
            inner: DashMap::new(),
            threshold,
            window,
            recover,
        }
    }

    /// 记录一次 429。窗口外的旧记录直接重置。返回是否触发了 disable。
    pub fn record_429(&self, upstream_id: i64) -> bool {
        let now = Instant::now();
        let mut entry = self.inner.entry(upstream_id).or_insert(Entry {
            count: 0,
            first_at: now,
            last_at: now,
            disabled_at: None,
        });
        if now.duration_since(entry.first_at) > self.window {
            entry.count = 0;
            entry.first_at = now;
            entry.disabled_at = None;
        }
        entry.count += 1;
        entry.last_at = now;
        if entry.count >= self.threshold && entry.disabled_at.is_none() {
            entry.disabled_at = Some(now);
            true
        } else {
            false
        }
    }

    /// 检查 upstream 是否被熔断。disabled_at 超过 recover 自动恢复并清零。
    pub fn is_disabled(&self, upstream_id: i64) -> bool {
        let now = Instant::now();
        let mut entry = match self.inner.get_mut(&upstream_id) {
            Some(e) => e,
            None => return false,
        };
        match entry.disabled_at {
            None => false,
            Some(at) if now.duration_since(at) > self.recover => {
                entry.count = 0;
                entry.first_at = now;
                entry.disabled_at = None;
                false
            }
            Some(_) => true,
        }
    }

    /// admin 手动重置：清掉计数与 disabled 状态。
    pub fn reset(&self, upstream_id: i64) {
        self.inner.remove(&upstream_id);
    }

    /// admin 手动熔断：强制把某个 upstream 标记为 disabled。
    pub fn force_disable(&self, upstream_id: i64) {
        let now = Instant::now();
        self.inner
            .entry(upstream_id)
            .and_modify(|e| {
                e.disabled_at = Some(now);
                e.last_at = now;
            })
            .or_insert(Entry {
                count: 0,
                first_at: now,
                last_at: now,
                disabled_at: Some(now),
            });
    }

    /// dashboard / admin 接口用的状态快照。
    pub fn snapshot(&self) -> Vec<BreakerSnapshot> {
        let now = Instant::now();
        self.inner
            .iter()
            .map(|kv| {
                let e = kv.value();
                BreakerSnapshot {
                    id: *kv.key(),
                    count: e.count,
                    disabled: e.disabled_at.is_some(),
                    first_at_ms_ago: now.duration_since(e.first_at).as_millis(),
                    last_at_ms_ago: now.duration_since(e.last_at).as_millis(),
                }
            })
            .collect()
    }

    pub fn tracked(&self) -> usize {
        self.inner.len()
    }
}

impl Default for Breaker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_below_threshold_not_disabled() {
        let b = Breaker::new();
        for _ in 0..(BREAKER_THRESHOLD - 1) {
            assert!(!b.record_429(1));
        }
        assert!(!b.is_disabled(1));
    }

    #[test]
    fn record_at_threshold_disables() {
        let b = Breaker::new();
        for i in 0..BREAKER_THRESHOLD {
            let tripped = b.record_429(1);
            if i + 1 == BREAKER_THRESHOLD {
                assert!(tripped, "should disable on threshold");
            } else {
                assert!(!tripped);
            }
        }
        assert!(b.is_disabled(1));
    }

    #[test]
    fn window_expiry_resets_count() {
        let b = Breaker::with(3, Duration::from_millis(10), BREAKER_RECOVER);
        b.record_429(1);
        b.record_429(1);
        std::thread::sleep(Duration::from_millis(15));
        assert!(!b.record_429(1), "window expired, fresh count starts at 1");
        assert!(!b.is_disabled(1));
    }

    #[test]
    fn recover_resets_disabled() {
        let b = Breaker::with(2, BREAKER_WINDOW, Duration::from_millis(10));
        b.record_429(1);
        b.record_429(1);
        assert!(b.is_disabled(1));
        std::thread::sleep(Duration::from_millis(15));
        assert!(!b.is_disabled(1));
    }

    #[test]
    fn reset_clears_state() {
        let b = Breaker::new();
        for _ in 0..BREAKER_THRESHOLD {
            b.record_429(1);
        }
        assert!(b.is_disabled(1));
        b.reset(1);
        assert!(!b.is_disabled(1));
        assert_eq!(b.tracked(), 0);
    }

    #[test]
    fn force_disable_works_without_prior_records() {
        let b = Breaker::new();
        b.force_disable(7);
        assert!(b.is_disabled(7));
    }

    #[test]
    fn snapshot_lists_entries() {
        let b = Breaker::new();
        b.record_429(1);
        b.force_disable(2);
        let snap = b.snapshot();
        assert_eq!(snap.len(), 2);
        let s1 = snap.iter().find(|x| x.id == 1).unwrap();
        assert_eq!(s1.count, 1);
        assert!(!s1.disabled);
        let s2 = snap.iter().find(|x| x.id == 2).unwrap();
        assert!(s2.disabled);
    }

    #[test]
    fn multiple_keys_independent() {
        let b = Breaker::new();
        for _ in 0..BREAKER_THRESHOLD {
            b.record_429(1);
        }
        assert!(b.is_disabled(1));
        assert!(!b.is_disabled(2));
    }
}
