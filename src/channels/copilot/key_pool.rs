//! Copilot 上游 key 池。每 60s 单飞刷新一次；admin 写入 upstream_keys 后 notify 立刻失效。
//! 选 key 是纯随机，剔除熔断 + exclude 集合 + 不在 allowed 集合的项。

use crate::channels::ChannelKind;
use crate::channels::copilot::breaker::Breaker;
use crate::channels::copilot::upstream_key::{ParsedUpstreamKey, parse_raw_key};
use crate::db::Db;
use crate::db::upstream::list_enabled;
use crate::error::AppResult;
use parking_lot::RwLock;
use rand::Rng;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tokio::sync::Semaphore;

pub const UPSTREAM_POOL_TTL: Duration = Duration::from_secs(60);

#[derive(Clone, Debug)]
pub struct PoolEntry {
    pub id: i64,
    pub raw: String,
    pub name: String,
}

#[derive(Clone, Debug)]
pub struct PickedUpstream {
    pub id: i64,
    pub parsed: ParsedUpstreamKey,
}

struct PoolState {
    entries: Vec<PoolEntry>,
    loaded_at: Option<Instant>,
}

/// 上游 key 池容器。Arc 共享，多 handler 并发拉同一份。
pub struct UpstreamPool {
    db: Db,
    breaker: Arc<Breaker>,
    state: RwLock<PoolState>,
    refresh_gate: Semaphore,
    invalidate: Arc<Notify>,
    ttl: Duration,
}

impl UpstreamPool {
    pub fn new(db: Db, breaker: Arc<Breaker>, invalidate: Arc<Notify>) -> Arc<Self> {
        Self::with_ttl(db, breaker, invalidate, UPSTREAM_POOL_TTL)
    }

    pub fn with_ttl(
        db: Db,
        breaker: Arc<Breaker>,
        invalidate: Arc<Notify>,
        ttl: Duration,
    ) -> Arc<Self> {
        let pool = Arc::new(Self {
            db,
            breaker,
            state: RwLock::new(PoolState {
                entries: Vec::new(),
                loaded_at: None,
            }),
            refresh_gate: Semaphore::new(1),
            invalidate,
            ttl,
        });
        pool.clone().spawn_invalidate_watcher();
        pool
    }

    pub fn breaker(&self) -> Arc<Breaker> {
        self.breaker.clone()
    }

    /// 后台监听 invalidate 通知（admin 写入 upstream_keys 时触发），
    /// 把 loaded_at 置 None 强制下次 ensure_fresh 再拉一次 DB。
    fn spawn_invalidate_watcher(self: Arc<Self>) {
        let invalidate = self.invalidate.clone();
        tokio::spawn(async move {
            loop {
                invalidate.notified().await;
                self.state.write().loaded_at = None;
            }
        });
    }

    fn is_fresh(&self) -> bool {
        let guard = self.state.read();
        match guard.loaded_at {
            Some(at) => at.elapsed() < self.ttl && !guard.entries.is_empty(),
            None => false,
        }
    }

    /// 同步快照：返回当前内存中的 entries（不刷新）。
    pub fn snapshot(&self) -> Vec<PoolEntry> {
        self.state.read().entries.clone()
    }

    /// 确保 entries 新鲜。多并发只会跑一次 DB query（Semaphore=1）。
    pub async fn ensure_fresh(&self) -> AppResult<()> {
        if self.is_fresh() {
            return Ok(());
        }
        let _permit = self.refresh_gate.acquire().await.expect("semaphore closed");
        if self.is_fresh() {
            return Ok(());
        }
        let rows = list_enabled(&self.db, ChannelKind::Copilot).await?;
        let entries: Vec<PoolEntry> = rows
            .into_iter()
            .map(|r| PoolEntry {
                id: r.id,
                raw: r.key,
                name: r.name,
            })
            .collect();
        let mut guard = self.state.write();
        guard.entries = entries;
        guard.loaded_at = Some(Instant::now());
        Ok(())
    }

    /// 纯随机选一个未熔断、在 allowed 集合内、不在 exclude 集合内的 entry。
    pub fn pick(
        &self,
        allowed: Option<&[i64]>,
        exclude: &HashSet<i64>,
    ) -> Option<PickedUpstream> {
        let snap = self.state.read();
        let mut candidates: Vec<&PoolEntry> = snap
            .entries
            .iter()
            .filter(|e| {
                allowed.is_none_or(|ids| ids.contains(&e.id))
                    && !exclude.contains(&e.id)
                    && !self.breaker.is_disabled(e.id)
            })
            .collect();
        if candidates.is_empty() {
            return None;
        }
        let idx = rand::thread_rng().gen_range(0..candidates.len());
        let chosen = candidates.swap_remove(idx);
        let parsed = parse_raw_key(&chosen.raw)?;
        Some(PickedUpstream {
            id: chosen.id,
            parsed,
        })
    }

    /// 重试上限：min(pool.len(), 5)。pool 模式 401/403/429 触发换 key 时最多换这么多次。
    pub fn max_retries(&self) -> usize {
        self.state.read().entries.len().min(5)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::billing::SnapshotVersion;
    use crate::concurrency::Limiter;

    // 没法在单元测试里挂真实 PG，只能验证内部数据结构。
    // 直接 build 一个空 Pool，把 entries 塞进去测 pick / breaker / exclude / allowed。
    fn make_pool() -> Arc<UpstreamPool> {
        // 拿任意 PgPool 占位是没法做到的，这里用 unsafe transmute 不行。
        // 改成走一个 fake：暴露 test-only constructor。
        unreachable!("constructed via test_only_with_entries")
    }

    impl UpstreamPool {
        /// 仅供单元测试用：填充 entries 而不连接 DB。
        pub fn test_only_with_entries(
            entries: Vec<PoolEntry>,
            breaker: Arc<Breaker>,
        ) -> Arc<Self> {
            Arc::new(Self {
                db: unsafe_test_db(),
                breaker,
                state: RwLock::new(PoolState {
                    entries,
                    loaded_at: Some(Instant::now()),
                }),
                refresh_gate: Semaphore::new(1),
                invalidate: Arc::new(Notify::new()),
                ttl: UPSTREAM_POOL_TTL,
            })
        }
    }

    fn unsafe_test_db() -> Db {
        // sqlx PgPool::connect_lazy 不需要真连，只需要合法 URL 字符串。
        let pool = sqlx::PgPool::connect_lazy("postgres://localhost/never_used")
            .expect("connect_lazy");
        Db::from_pool(pool)
    }

    fn entry(id: i64, raw: &str) -> PoolEntry {
        PoolEntry {
            id,
            raw: raw.into(),
            name: format!("k{id}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pick_random_from_pool() {
        let breaker = Arc::new(Breaker::new());
        let pool = UpstreamPool::test_only_with_entries(
            vec![
                entry(1, "enterprise:tok1"),
                entry(2, "enterprise:tok2"),
                entry(3, "enterprise:tok3"),
            ],
            breaker,
        );
        let picked = pool.pick(None, &HashSet::new()).expect("pick");
        assert!([1, 2, 3].contains(&picked.id));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pick_respects_exclude() {
        let breaker = Arc::new(Breaker::new());
        let pool = UpstreamPool::test_only_with_entries(
            vec![entry(1, "enterprise:tok1"), entry(2, "enterprise:tok2")],
            breaker,
        );
        let mut exclude = HashSet::new();
        exclude.insert(1);
        for _ in 0..20 {
            let p = pool.pick(None, &exclude).expect("pick");
            assert_eq!(p.id, 2);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pick_respects_allowed() {
        let breaker = Arc::new(Breaker::new());
        let pool = UpstreamPool::test_only_with_entries(
            vec![entry(1, "enterprise:tok1"), entry(2, "enterprise:tok2"), entry(3, "enterprise:tok3")],
            breaker,
        );
        let allowed = [2_i64, 3];
        for _ in 0..20 {
            let p = pool.pick(Some(&allowed), &HashSet::new()).expect("pick");
            assert!(allowed.contains(&p.id));
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pick_skips_disabled() {
        let breaker = Arc::new(Breaker::new());
        breaker.force_disable(2);
        let pool = UpstreamPool::test_only_with_entries(
            vec![entry(1, "enterprise:tok1"), entry(2, "enterprise:tok2")],
            breaker.clone(),
        );
        for _ in 0..20 {
            let p = pool.pick(None, &HashSet::new()).expect("pick");
            assert_eq!(p.id, 1);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pick_returns_none_when_pool_empty() {
        let breaker = Arc::new(Breaker::new());
        let pool = UpstreamPool::test_only_with_entries(vec![], breaker);
        assert!(pool.pick(None, &HashSet::new()).is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pick_returns_none_when_all_excluded() {
        let breaker = Arc::new(Breaker::new());
        let pool = UpstreamPool::test_only_with_entries(
            vec![entry(1, "enterprise:tok1")],
            breaker,
        );
        let mut exclude = HashSet::new();
        exclude.insert(1);
        assert!(pool.pick(None, &exclude).is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn max_retries_capped_to_5() {
        let breaker = Arc::new(Breaker::new());
        let entries: Vec<_> = (1..=10).map(|i| entry(i, "enterprise:t")).collect();
        let pool = UpstreamPool::test_only_with_entries(entries, breaker);
        assert_eq!(pool.max_retries(), 5);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn max_retries_uses_pool_size_when_smaller() {
        let breaker = Arc::new(Breaker::new());
        let pool = UpstreamPool::test_only_with_entries(
            vec![entry(1, "enterprise:t"), entry(2, "enterprise:t")],
            breaker,
        );
        assert_eq!(pool.max_retries(), 2);
    }

    // 防止 dead-code 警告：unsafe_test_db / make_pool 用 _ 引用
    #[allow(dead_code)]
    fn _link_helpers() {
        let _ = make_pool;
        let _ = (SnapshotVersion::new(), Limiter::new(Arc::new(SnapshotVersion::new())));
    }
}
