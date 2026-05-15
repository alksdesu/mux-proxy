//! key_name → 累计美元花费的内存计数器。启动时一次性聚合 usage_logs 预热，
//! 之后每次写 usage 成功 addSpend。quota 检查走纯内存，避免热路径每次扫表。

use crate::db::Db;
use crate::error::AppResult;
use dashmap::DashMap;
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct SpendCache {
    inner: DashMap<String, f64>,
}

impl SpendCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// 启动时调一次：把 usage_logs 全部聚合到内存。
    /// 失败直接报错——数据库不可用时整个服务都没法跑。
    pub async fn init_from_db(db: &Db) -> AppResult<Self> {
        let totals = crate::db::stats::init_spend_cache(db).await?;
        let cache = SpendCache::new();
        for (key_name, cost) in totals {
            cache.inner.insert(key_name, cost);
        }
        Ok(cache)
    }

    pub fn add(&self, key_name: &str, cost: f64) {
        if cost == 0.0 {
            return;
        }
        self.inner
            .entry(key_name.to_string())
            .and_modify(|v| *v += cost)
            .or_insert(cost);
    }

    pub fn get(&self, key_name: &str) -> f64 {
        self.inner.get(key_name).map(|v| *v).unwrap_or(0.0)
    }

    /// `quota` 单位美元；`quota <= 0` 视为不限（-1 不限，0 直接禁用走另一条路径）。
    pub fn over_quota(&self, key_name: &str, quota: f64) -> bool {
        if quota <= 0.0 {
            return false;
        }
        self.get(key_name) >= quota
    }

    pub fn snapshot(&self) -> HashMap<String, f64> {
        self.inner
            .iter()
            .map(|kv| (kv.key().clone(), *kv.value()))
            .collect()
    }

    /// 改名同步：admin PATCH 改名时把旧 key_name 的累计搬到新名。
    pub fn rename(&self, old_name: &str, new_name: &str) {
        if old_name == new_name {
            return;
        }
        if let Some((_, v)) = self.inner.remove(old_name) {
            self.inner
                .entry(new_name.to_string())
                .and_modify(|cur| *cur += v)
                .or_insert(v);
        }
    }

    /// 删 key 时清掉本地累计——记录不删，但 cache 不再保留。
    pub fn drop_key(&self, key_name: &str) {
        self.inner.remove(key_name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_get() {
        let c = SpendCache::new();
        c.add("a", 1.5);
        c.add("a", 2.5);
        c.add("b", 0.1);
        assert!((c.get("a") - 4.0).abs() < 1e-9);
        assert!((c.get("b") - 0.1).abs() < 1e-9);
        assert_eq!(c.get("missing"), 0.0);
    }

    #[test]
    fn add_zero_is_noop() {
        let c = SpendCache::new();
        c.add("a", 0.0);
        assert_eq!(c.get("a"), 0.0);
    }

    #[test]
    fn over_quota_respects_unlimited() {
        let c = SpendCache::new();
        c.add("a", 100.0);
        assert!(!c.over_quota("a", -1.0));
        assert!(!c.over_quota("a", 0.0));
        assert!(!c.over_quota("a", 100.01));
        assert!(c.over_quota("a", 100.0));
        assert!(c.over_quota("a", 50.0));
    }

    #[test]
    fn rename_moves_balance() {
        let c = SpendCache::new();
        c.add("old", 5.0);
        c.add("dest", 1.0);
        c.rename("old", "dest");
        assert_eq!(c.get("old"), 0.0);
        assert!((c.get("dest") - 6.0).abs() < 1e-9);
    }

    #[test]
    fn rename_same_name_noop() {
        let c = SpendCache::new();
        c.add("a", 5.0);
        c.rename("a", "a");
        assert_eq!(c.get("a"), 5.0);
    }

    #[test]
    fn drop_key_clears() {
        let c = SpendCache::new();
        c.add("a", 1.0);
        c.drop_key("a");
        assert_eq!(c.get("a"), 0.0);
    }
}
