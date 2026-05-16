//! Copilot 上游 key 429 累计熔断。重置式固定窗口，逻辑由 ``shared::breaker``
//! 的泛型 ``Breaker<S>`` 提供；本模块负责渠道默认参数与 ``BreakerInfo`` → ``BreakerSnapshot`` 的 wrap。

use crate::channels::{BreakerSnapshot, ChannelKind};
use crate::shared::breaker::{
    Breaker as SharedBreaker, BreakerConfig, ResetWindowStrategy,
};
use std::time::Duration;

pub const BREAKER_THRESHOLD: u32 = 10;
pub const BREAKER_WINDOW: Duration = Duration::from_secs(600);
pub const BREAKER_RECOVER: Duration = Duration::from_secs(1800);

pub struct Breaker(SharedBreaker<ResetWindowStrategy>);

impl Breaker {
    pub fn new() -> Self {
        Self::with(BREAKER_THRESHOLD, BREAKER_WINDOW, BREAKER_RECOVER)
    }

    pub fn with(threshold: u32, window: Duration, recover: Duration) -> Self {
        Self(SharedBreaker::new(BreakerConfig {
            threshold,
            window,
            recover,
        }))
    }

    pub fn record_failure(&self, upstream_id: i64) -> bool {
        self.0.record_failure(upstream_id)
    }

    pub fn is_disabled(&self, upstream_id: i64) -> bool {
        self.0.is_disabled(upstream_id)
    }

    pub fn reset(&self, upstream_id: i64) {
        self.0.reset(upstream_id);
    }

    pub fn force_disable(&self, upstream_id: i64) {
        self.0.force_disable(upstream_id);
    }

    pub fn snapshot(&self) -> Vec<BreakerSnapshot> {
        self.0
            .snapshot()
            .into_iter()
            .map(|info| BreakerSnapshot {
                id: info.id,
                channel_kind: ChannelKind::Copilot,
                count: info.count,
                disabled: info.disabled,
                first_at_ms_ago: info.first_at_ms_ago,
                last_at_ms_ago: info.last_at_ms_ago,
            })
            .collect()
    }

    pub fn tracked(&self) -> usize {
        self.0.tracked()
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
            assert!(!b.record_failure(1));
        }
        assert!(!b.is_disabled(1));
    }

    #[test]
    fn record_at_threshold_disables() {
        let b = Breaker::new();
        for i in 0..BREAKER_THRESHOLD {
            let tripped = b.record_failure(1);
            if i + 1 == BREAKER_THRESHOLD {
                assert!(tripped);
            } else {
                assert!(!tripped);
            }
        }
        assert!(b.is_disabled(1));
    }

    #[test]
    fn window_expiry_resets_count() {
        let b = Breaker::with(3, Duration::from_millis(10), BREAKER_RECOVER);
        b.record_failure(1);
        b.record_failure(1);
        std::thread::sleep(Duration::from_millis(15));
        assert!(!b.record_failure(1));
        assert!(!b.is_disabled(1));
    }

    #[test]
    fn recover_resets_disabled() {
        let b = Breaker::with(2, BREAKER_WINDOW, Duration::from_millis(10));
        b.record_failure(1);
        b.record_failure(1);
        assert!(b.is_disabled(1));
        std::thread::sleep(Duration::from_millis(15));
        assert!(!b.is_disabled(1));
    }

    #[test]
    fn reset_clears_state() {
        let b = Breaker::new();
        for _ in 0..BREAKER_THRESHOLD {
            b.record_failure(1);
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
    fn snapshot_lists_entries_with_copilot_kind() {
        let b = Breaker::new();
        b.record_failure(1);
        b.force_disable(2);
        let snap = b.snapshot();
        assert_eq!(snap.len(), 2);
        for s in &snap {
            assert_eq!(s.channel_kind, ChannelKind::Copilot);
        }
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
            b.record_failure(1);
        }
        assert!(b.is_disabled(1));
        assert!(!b.is_disabled(2));
    }
}
