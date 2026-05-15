//! per-key 并发计数 + RAII guard。max 语义：-1 不限、0 全禁、>0 严格上限。
//! 周期 GC 清掉 last_seen 超 STALE 的项，兜底异常路径漏 release 永久占位。

use crate::billing::SnapshotVersion;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

pub const STALE_AFTER: Duration = Duration::from_secs(600);
pub const GC_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug)]
struct Entry {
    count: u32,
    last_seen: Instant,
}

pub struct Limiter {
    inner: DashMap<String, Entry>,
    snapshot: Arc<SnapshotVersion>,
    stale_after: Duration,
    gc_interval: Duration,
}

impl Limiter {
    pub fn new(snapshot: Arc<SnapshotVersion>) -> Arc<Self> {
        Arc::new(Self {
            inner: DashMap::new(),
            snapshot,
            stale_after: STALE_AFTER,
            gc_interval: GC_INTERVAL,
        })
    }

    pub fn with_intervals(
        snapshot: Arc<SnapshotVersion>,
        stale_after: Duration,
        gc_interval: Duration,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner: DashMap::new(),
            snapshot,
            stale_after,
            gc_interval,
        })
    }

    /// 尝试占用一个并发位。max 语义：
    /// - `-1` 不限 → 永远成功
    /// - `0` 全禁 → 永远 None
    /// - 正数 → 当前 count < max 才成功
    pub fn try_acquire(self: &Arc<Self>, key_name: &str, max: i64) -> Option<ConcurrencyGuard> {
        if max == 0 {
            return None;
        }
        let mut entry = self.inner.entry(key_name.to_string()).or_insert(Entry {
            count: 0,
            last_seen: Instant::now(),
        });
        if max > 0 && entry.count as i64 >= max {
            return None;
        }
        entry.count += 1;
        entry.last_seen = Instant::now();
        drop(entry);
        self.snapshot.bump();
        Some(ConcurrencyGuard {
            limiter: Arc::downgrade(self),
            key_name: key_name.to_string(),
        })
    }

    pub fn current(&self, key_name: &str) -> u32 {
        self.inner.get(key_name).map(|e| e.count).unwrap_or(0)
    }

    pub fn snapshot(&self) -> HashMap<String, u32> {
        self.inner
            .iter()
            .map(|kv| (kv.key().clone(), kv.value().count))
            .collect()
    }

    pub fn tracked_keys(&self) -> usize {
        self.inner.len()
    }

    fn release(&self, key_name: &str) {
        let mut should_remove = false;
        if let Some(mut e) = self.inner.get_mut(key_name) {
            if e.count <= 1 {
                should_remove = true;
            } else {
                e.count -= 1;
                e.last_seen = Instant::now();
            }
        }
        if should_remove {
            self.inner.remove_if(key_name, |_, e| e.count <= 1);
        }
        self.snapshot.bump();
    }

    /// 单次 GC 扫描：删 last_seen 超过 stale_after 且 count==0 的项。
    /// 正在使用（count>0）的项即使老也保留——可能是 240s 上游超时 + thinking 的长任务。
    /// 真的 600s 没动过且 count>0 视为 leak，强制清掉。
    pub fn run_gc_once(&self) {
        let now = Instant::now();
        let stale = self.stale_after;
        self.inner.retain(|_, e| now.duration_since(e.last_seen) < stale);
    }

    /// 长期 GC 循环。caller spawn 一次即可，进程退出时通过 tokio runtime 终止。
    pub async fn run_gc(self: Arc<Self>) {
        let mut tick = tokio::time::interval(self.gc_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            self.run_gc_once();
        }
    }
}

pub struct ConcurrencyGuard {
    limiter: Weak<Limiter>,
    key_name: String,
}

impl ConcurrencyGuard {
    pub fn key_name(&self) -> &str {
        &self.key_name
    }
}

impl Drop for ConcurrencyGuard {
    fn drop(&mut self) {
        if let Some(l) = self.limiter.upgrade() {
            l.release(&self.key_name);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_limiter() -> (Arc<Limiter>, Arc<SnapshotVersion>) {
        let snap = Arc::new(SnapshotVersion::new());
        (Limiter::new(snap.clone()), snap)
    }

    #[test]
    fn acquire_drop_decrements() {
        let (l, _) = new_limiter();
        {
            let _g = l.try_acquire("a", 5).expect("acquire");
            assert_eq!(l.current("a"), 1);
        }
        assert_eq!(l.current("a"), 0);
    }

    #[test]
    fn limit_zero_rejects_all() {
        let (l, _) = new_limiter();
        assert!(l.try_acquire("a", 0).is_none());
        assert_eq!(l.current("a"), 0);
    }

    #[test]
    fn limit_minus_one_unlimited() {
        let (l, _) = new_limiter();
        let mut guards = Vec::new();
        for _ in 0..100 {
            guards.push(l.try_acquire("a", -1).expect("unlimited"));
        }
        assert_eq!(l.current("a"), 100);
        drop(guards);
        assert_eq!(l.current("a"), 0);
    }

    #[test]
    fn limit_positive_caps() {
        let (l, _) = new_limiter();
        let g1 = l.try_acquire("a", 2).expect("1");
        let g2 = l.try_acquire("a", 2).expect("2");
        assert!(l.try_acquire("a", 2).is_none());
        drop(g1);
        let _g3 = l.try_acquire("a", 2).expect("after release");
        assert_eq!(l.current("a"), 2);
        drop(g2);
    }

    #[test]
    fn snapshot_bumps_on_change() {
        let (l, snap) = new_limiter();
        let before = snap.current();
        let g = l.try_acquire("a", 5).expect("acquire");
        assert!(snap.current() > before);
        let after_acq = snap.current();
        drop(g);
        assert!(snap.current() > after_acq);
    }

    #[test]
    fn snapshot_view() {
        let (l, _) = new_limiter();
        let _a = l.try_acquire("a", -1).expect("a");
        let _b1 = l.try_acquire("b", -1).expect("b1");
        let _b2 = l.try_acquire("b", -1).expect("b2");
        let snap = l.snapshot();
        assert_eq!(snap.get("a"), Some(&1));
        assert_eq!(snap.get("b"), Some(&2));
    }

    #[tokio::test]
    async fn many_tasks_no_overflow() {
        let (l, _) = new_limiter();
        let mut handles = Vec::new();
        for _ in 0..256 {
            let l = l.clone();
            handles.push(tokio::spawn(async move {
                let _g = l.try_acquire("k", -1).expect("unlimited");
                tokio::time::sleep(Duration::from_millis(2)).await;
            }));
        }
        for h in handles {
            h.await.expect("join");
        }
        assert_eq!(l.current("k"), 0);
    }

    #[test]
    fn gc_drops_stale_entries() {
        let snap = Arc::new(SnapshotVersion::new());
        let l = Limiter::with_intervals(
            snap.clone(),
            Duration::from_millis(0),
            Duration::from_secs(60),
        );
        let g = l.try_acquire("a", -1).expect("acquire");
        drop(g);
        std::thread::sleep(Duration::from_millis(2));
        l.run_gc_once();
        assert_eq!(l.tracked_keys(), 0);
    }
}
