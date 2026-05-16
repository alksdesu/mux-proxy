//! per-key RPM 滑动窗口限流。窗口 60s，超阈值拒。
//! DashMap<key_name, VecDeque<Instant>> 持久化命中时间，每次 check 时把超窗的旧条目弹出。
//! 周期 GC 清掉所有最近 5 分钟没活动的 key entry。

use dashmap::DashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub const WINDOW: Duration = Duration::from_secs(60);
pub const STALE_AFTER: Duration = Duration::from_secs(300);
pub const GC_INTERVAL: Duration = Duration::from_secs(60);

pub struct RateLimiter {
    inner: DashMap<String, Entry>,
}

#[derive(Default)]
struct Entry {
    hits: VecDeque<Instant>,
}

impl RateLimiter {
    pub fn new() -> Arc<Self> {
        Arc::new(Self { inner: DashMap::new() })
    }

    /// 尝试放行一次请求。limit 语义：
    /// - `-1` 不限 → 永远 Ok
    /// - `0` 全禁 → 永远 Err
    /// - 正数 → 60s 窗口内命中 < limit 才放行，并记录此次命中
    pub fn try_acquire(&self, key_name: &str, limit: i64) -> Result<(), ()> {
        if limit < 0 {
            return Ok(());
        }
        if limit == 0 {
            return Err(());
        }
        let now = Instant::now();
        let mut entry = self.inner.entry(key_name.to_string()).or_default();
        while let Some(&t) = entry.hits.front() {
            if now.duration_since(t) > WINDOW {
                entry.hits.pop_front();
            } else {
                break;
            }
        }
        if entry.hits.len() as i64 >= limit {
            return Err(());
        }
        entry.hits.push_back(now);
        Ok(())
    }

    pub fn current(&self, key_name: &str) -> usize {
        let now = Instant::now();
        self.inner
            .get(key_name)
            .map(|e| e.hits.iter().filter(|t| now.duration_since(**t) <= WINDOW).count())
            .unwrap_or(0)
    }

    pub fn tracked_keys(&self) -> usize {
        self.inner.len()
    }

    pub fn run_gc_once(&self) {
        let now = Instant::now();
        self.inner.retain(|_, e| {
            while let Some(&t) = e.hits.front() {
                if now.duration_since(t) > WINDOW {
                    e.hits.pop_front();
                } else {
                    break;
                }
            }
            match e.hits.back() {
                Some(&last) => now.duration_since(last) < STALE_AFTER,
                None => false,
            }
        });
    }

    pub async fn run_gc(self: Arc<Self>) {
        let mut tick = tokio::time::interval(GC_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            self.run_gc_once();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limit_minus_one_unlimited() {
        let r = RateLimiter::new();
        for _ in 0..1000 {
            assert!(r.try_acquire("a", -1).is_ok());
        }
    }

    #[test]
    fn limit_zero_rejects_all() {
        let r = RateLimiter::new();
        assert!(r.try_acquire("a", 0).is_err());
    }

    #[test]
    fn limit_positive_caps_window() {
        let r = RateLimiter::new();
        for _ in 0..5 {
            assert!(r.try_acquire("a", 5).is_ok());
        }
        assert!(r.try_acquire("a", 5).is_err());
    }

    #[test]
    fn limits_isolated_per_key() {
        let r = RateLimiter::new();
        for _ in 0..3 {
            assert!(r.try_acquire("a", 3).is_ok());
        }
        assert!(r.try_acquire("a", 3).is_err());
        assert!(r.try_acquire("b", 3).is_ok(), "other key independent");
    }

    #[test]
    fn gc_drops_stale_keys() {
        let r = RateLimiter::new();
        r.try_acquire("ghost", 100).unwrap();
        // 手动把 hits 推到 STALE 之前
        if let Some(mut e) = r.inner.get_mut("ghost") {
            let stale_time = Instant::now() - STALE_AFTER - Duration::from_secs(1);
            e.hits.clear();
            e.hits.push_back(stale_time);
        }
        r.run_gc_once();
        assert_eq!(r.tracked_keys(), 0);
    }
}
