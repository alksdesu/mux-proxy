//! 上游 key 熔断器：泛型 ``Breaker<S>`` + 两个窗口策略。
//! 两渠道靠 ``BreakerStrategy::CLEAR_ON_TRIP`` 区分 trip 后是否清失败状态：
//! - 重置式窗口：trip 后保留 count，recover 后才清；
//! - 滑动窗口：trip 时立即清失败列表，snapshot 期间 count 已为 0，由调用方决定渲染。
//!
//! 本模块不引用任何渠道层类型；``snapshot()`` 返回中性的 ``BreakerInfo``，
//! 渠道层自己 wrap 成带 ``channel_kind`` 的对外快照。

use dashmap::DashMap;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug)]
pub struct BreakerConfig {
    pub threshold: u32,
    pub window: Duration,
    pub recover: Duration,
}

/// ``Breaker::snapshot()`` 的中性输出：纯失败状态视图，不含渠道信息。
#[derive(Clone, Debug)]
pub struct BreakerInfo {
    pub id: i64,
    pub count: u32,
    pub disabled: bool,
    pub first_at_ms_ago: u128,
    pub last_at_ms_ago: u128,
}

/// 失败计数语义。``CLEAR_ON_TRIP`` 控制 trip 那一刻是否清失败状态。
pub trait BreakerStrategy: Default + Send + Sync + 'static {
    /// trip 时清状态：滑动窗口 true，重置式窗口 false。
    const CLEAR_ON_TRIP: bool;

    /// 记录一次失败，注入当前时间与窗口宽度。
    fn record(&mut self, now: Instant, window: Duration);
    /// 当前失败计数（受策略本身的窗口语义影响）。
    fn count(&self) -> u32;
    /// 首次失败时刻（若不存在用 ``None``）。
    fn first_at(&self) -> Option<Instant>;
    /// 最近一次失败时刻。
    fn last_at(&self) -> Option<Instant>;
    /// 把失败状态清空（``recover`` 自动恢复或 trip 时调用）。
    fn clear(&mut self);
}

/// 重置式固定窗口：窗口外的旧记录整批丢弃，新窗口从 1 开始。Copilot 渠道用。
#[derive(Default, Debug)]
pub struct ResetWindowStrategy {
    count: u32,
    first_at: Option<Instant>,
    last_at: Option<Instant>,
}

impl BreakerStrategy for ResetWindowStrategy {
    const CLEAR_ON_TRIP: bool = false;

    fn record(&mut self, now: Instant, window: Duration) {
        let in_window = self
            .first_at
            .map(|f| now.duration_since(f) <= window)
            .unwrap_or(false);
        if !in_window {
            self.count = 0;
            self.first_at = Some(now);
        }
        self.count += 1;
        self.last_at = Some(now);
    }

    fn count(&self) -> u32 {
        self.count
    }
    fn first_at(&self) -> Option<Instant> {
        self.first_at
    }
    fn last_at(&self) -> Option<Instant> {
        self.last_at
    }
    fn clear(&mut self) {
        self.count = 0;
        self.first_at = None;
        self.last_at = None;
    }
}

/// 滑动窗口：保留过去 ``window`` 时长内的失败时刻列表。Anthropic 渠道用。
#[derive(Default, Debug)]
pub struct SlidingWindowStrategy {
    failures: Vec<Instant>,
}

impl BreakerStrategy for SlidingWindowStrategy {
    const CLEAR_ON_TRIP: bool = true;

    fn record(&mut self, now: Instant, window: Duration) {
        self.failures.retain(|t| now.duration_since(*t) <= window);
        self.failures.push(now);
    }

    fn count(&self) -> u32 {
        self.failures.len() as u32
    }
    fn first_at(&self) -> Option<Instant> {
        self.failures.first().copied()
    }
    fn last_at(&self) -> Option<Instant> {
        self.failures.last().copied()
    }
    fn clear(&mut self) {
        self.failures.clear();
    }
}

#[derive(Debug)]
struct Cell<S> {
    strategy: S,
    open_until: Option<Instant>,
}

impl<S: Default> Default for Cell<S> {
    fn default() -> Self {
        Self {
            strategy: S::default(),
            open_until: None,
        }
    }
}

/// 泛型熔断器：DashMap 持每个 upstream id 的 (strategy, open_until)，跨渠道复用同一 API。
pub struct Breaker<S: BreakerStrategy> {
    inner: DashMap<i64, Cell<S>>,
    config: BreakerConfig,
}

impl<S: BreakerStrategy> Breaker<S> {
    pub fn new(config: BreakerConfig) -> Self {
        Self {
            inner: DashMap::new(),
            config,
        }
    }

    pub fn config(&self) -> BreakerConfig {
        self.config
    }

    /// 记录一次失败。返回 ``true`` 表示本次新触发了 open（达阈值且之前未 open）。
    pub fn record_failure(&self, id: i64) -> bool {
        let now = Instant::now();
        let mut cell = self.inner.entry(id).or_default();
        cell.strategy.record(now, self.config.window);
        if cell.strategy.count() >= self.config.threshold && cell.open_until.is_none() {
            cell.open_until = Some(now + self.config.recover);
            if S::CLEAR_ON_TRIP {
                cell.strategy.clear();
            }
            return true;
        }
        false
    }

    /// 判定是否被熔断。Happy path 走 ``get`` read lock；只有 recover 到期需要清状态时
    /// 才升级到 ``get_mut`` write lock，避免热路径上 pick 一次 10 把 key 全部写锁。
    pub fn is_disabled(&self, id: i64) -> bool {
        let now = Instant::now();
        let deadline = {
            let cell = match self.inner.get(&id) {
                Some(c) => c,
                None => return false,
            };
            match cell.open_until {
                None => return false,
                Some(d) => d,
            }
        };
        if now < deadline {
            return true;
        }
        // 到期才升级 write lock 做 recover；CAS 失败 / 已被并发清就放它过
        if let Some(mut cell) = self.inner.get_mut(&id) {
            if matches!(cell.open_until, Some(d) if now >= d) {
                cell.open_until = None;
                cell.strategy.clear();
            }
        }
        false
    }

    /// admin 手动恢复：直接抹掉条目。
    pub fn reset(&self, id: i64) {
        self.inner.remove(&id);
    }

    /// admin 强制熔断：``recover`` 计时器立即开始，失败状态按策略决定是否清空。
    pub fn force_disable(&self, id: i64) {
        let now = Instant::now();
        let mut cell = self.inner.entry(id).or_default();
        cell.open_until = Some(now + self.config.recover);
        if S::CLEAR_ON_TRIP {
            cell.strategy.clear();
        }
    }

    /// admin / WS 用的中性快照。``count==0 && !disabled`` 的条目跳过；
    /// 渠道层负责把 ``BreakerInfo`` wrap 成带 channel 标记的对外类型。
    pub fn snapshot(&self) -> Vec<BreakerInfo> {
        let now = Instant::now();
        self.inner
            .iter()
            .filter_map(|kv| self.snapshot_one(*kv.key(), kv.value(), now))
            .collect()
    }

    fn snapshot_one(&self, id: i64, cell: &Cell<S>, now: Instant) -> Option<BreakerInfo> {
        let disabled = matches!(cell.open_until, Some(d) if now < d);
        let raw_count = cell.strategy.count();
        if !disabled && raw_count == 0 {
            return None;
        }
        let trip_at = cell
            .open_until
            .and_then(|d| d.checked_sub(self.config.recover));
        let count = if disabled && S::CLEAR_ON_TRIP {
            self.config.threshold
        } else {
            raw_count
        };
        let first = cell.strategy.first_at().or(trip_at).unwrap_or(now);
        let last = cell.strategy.last_at().or(trip_at).unwrap_or(now);
        let first_ms = now.checked_duration_since(first).unwrap_or_default().as_millis();
        let last_ms = now.checked_duration_since(last).unwrap_or_default().as_millis();
        Some(BreakerInfo {
            id,
            count,
            disabled,
            first_at_ms_ago: first_ms,
            last_at_ms_ago: last_ms,
        })
    }

    pub fn tracked(&self) -> usize {
        self.inner.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(threshold: u32, window: Duration, recover: Duration) -> BreakerConfig {
        BreakerConfig {
            threshold,
            window,
            recover,
        }
    }
    fn cfg_reset(threshold: u32, window: Duration, recover: Duration) -> BreakerConfig {
        cfg(threshold, window, recover)
    }
    fn cfg_sliding(threshold: u32, window: Duration, recover: Duration) -> BreakerConfig {
        cfg(threshold, window, recover)
    }

    #[test]
    fn reset_window_below_threshold_not_disabled() {
        let b: Breaker<ResetWindowStrategy> = Breaker::new(cfg_reset(
            3,
            Duration::from_secs(60),
            Duration::from_secs(60),
        ));
        assert!(!b.record_failure(1));
        assert!(!b.record_failure(1));
        assert!(!b.is_disabled(1));
    }

    #[test]
    fn reset_window_at_threshold_disables() {
        let b: Breaker<ResetWindowStrategy> = Breaker::new(cfg_reset(
            3,
            Duration::from_secs(60),
            Duration::from_secs(60),
        ));
        assert!(!b.record_failure(1));
        assert!(!b.record_failure(1));
        assert!(b.record_failure(1));
        assert!(b.is_disabled(1));
    }

    #[test]
    fn reset_window_expiry_resets() {
        let b: Breaker<ResetWindowStrategy> = Breaker::new(cfg_reset(
            3,
            Duration::from_millis(10),
            Duration::from_secs(60),
        ));
        b.record_failure(1);
        b.record_failure(1);
        std::thread::sleep(Duration::from_millis(15));
        assert!(!b.record_failure(1));
        assert!(!b.is_disabled(1));
    }

    #[test]
    fn sliding_window_at_threshold_disables() {
        let b: Breaker<SlidingWindowStrategy> = Breaker::new(cfg_sliding(
            3,
            Duration::from_secs(60),
            Duration::from_secs(60),
        ));
        assert!(!b.record_failure(1));
        assert!(!b.record_failure(1));
        assert!(b.record_failure(1));
        assert!(b.is_disabled(1));
    }

    #[test]
    fn sliding_window_drops_old_failures() {
        let b: Breaker<SlidingWindowStrategy> = Breaker::new(cfg_sliding(
            3,
            Duration::from_millis(10),
            Duration::from_secs(60),
        ));
        b.record_failure(1);
        b.record_failure(1);
        std::thread::sleep(Duration::from_millis(15));
        assert!(!b.record_failure(1));
    }

    #[test]
    fn auto_recover_clears_disabled() {
        let b: Breaker<ResetWindowStrategy> = Breaker::new(cfg_reset(
            2,
            Duration::from_secs(60),
            Duration::from_millis(10),
        ));
        b.record_failure(1);
        b.record_failure(1);
        assert!(b.is_disabled(1));
        std::thread::sleep(Duration::from_millis(15));
        assert!(!b.is_disabled(1));
    }

    #[test]
    fn reset_clears_entry() {
        let b: Breaker<ResetWindowStrategy> = Breaker::new(cfg_reset(
            2,
            Duration::from_secs(60),
            Duration::from_secs(60),
        ));
        b.record_failure(1);
        b.record_failure(1);
        assert!(b.is_disabled(1));
        b.reset(1);
        assert!(!b.is_disabled(1));
        assert_eq!(b.tracked(), 0);
    }

    #[test]
    fn force_disable_works_without_prior_records() {
        let b: Breaker<ResetWindowStrategy> = Breaker::new(cfg_reset(
            10,
            Duration::from_secs(60),
            Duration::from_secs(60),
        ));
        b.force_disable(7);
        assert!(b.is_disabled(7));
    }

    #[test]
    fn snapshot_lists_disabled_and_counted() {
        let b: Breaker<ResetWindowStrategy> = Breaker::new(cfg_reset(
            2,
            Duration::from_secs(60),
            Duration::from_secs(60),
        ));
        b.record_failure(1);
        b.force_disable(2);
        let snap = b.snapshot();
        assert_eq!(snap.len(), 2);
        let s1 = snap.iter().find(|x| x.id == 1).expect("id 1");
        assert_eq!(s1.count, 1);
        assert!(!s1.disabled);
        let s2 = snap.iter().find(|x| x.id == 2).expect("id 2");
        assert!(s2.disabled);
    }

    #[test]
    fn snapshot_sliding_disabled_uses_threshold() {
        let b: Breaker<SlidingWindowStrategy> = Breaker::new(cfg_sliding(
            3,
            Duration::from_secs(60),
            Duration::from_secs(60),
        ));
        b.record_failure(1);
        b.record_failure(1);
        b.record_failure(1);
        assert!(b.is_disabled(1));
        let snap = b.snapshot();
        let s = snap.iter().find(|x| x.id == 1).expect("id 1");
        assert!(s.disabled);
        // CLEAR_ON_TRIP=true 把 failures 清空，snapshot 应用 threshold 当 count
        assert_eq!(s.count, 3);
    }

    #[test]
    fn snapshot_skips_clean_entries() {
        let b: Breaker<ResetWindowStrategy> = Breaker::new(cfg_reset(
            2,
            Duration::from_secs(60),
            Duration::from_secs(60),
        ));
        b.record_failure(1);
        b.reset(1);
        let snap = b.snapshot();
        assert!(snap.is_empty());
    }
}
