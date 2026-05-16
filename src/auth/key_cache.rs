//! LRU + TTL 内存缓存 api_keys 行。命中即免一次 PG 查询，
//! TTL 60s 保证 PATCH /admin/keys 的改动在一分钟内被所有节点观察到。
//!
//! get_or_load 调用方传入 loader 由 SingleFlight 共享，避免雪崩。

use crate::channels::ChannelKind;
use crate::error::AppResult;
use lru::LruCache;
use parking_lot::Mutex;
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

pub const KEY_CACHE_MAX: usize = 500;
pub const KEY_CACHE_TTL: Duration = Duration::from_secs(60);

#[derive(Debug, Clone)]
pub struct KeyCacheEntry {
    pub id: i64,
    pub name: String,
    pub upstream_key: String,
    pub quota: f64,
    pub allow_fast: bool,
    pub max_concurrency: i64,
    pub rpm_limit: i64,
    /// 已 lowercase + 去空 entry 的白名单。空 Vec = 不限制。
    pub allowed_models: Vec<String>,
    pub channel_kind: ChannelKind,
    pub fetched_at: Instant,
}

impl KeyCacheEntry {
    pub fn is_fresh(&self, ttl: Duration) -> bool {
        self.fetched_at.elapsed() < ttl
    }

    /// 命中白名单返 true；空白名单视为不限制。``eq_ignore_ascii_case`` 避免
    /// 每次请求都 alloc 一个 lowercase needle，热路径 ns 级开销。
    pub fn model_allowed(&self, model: &str) -> bool {
        if self.allowed_models.is_empty() {
            return true;
        }
        self.allowed_models
            .iter()
            .any(|m| m.eq_ignore_ascii_case(model))
    }
}

/// 把逗号分隔的白名单字符串拆成 lowercase Vec，过滤空 entry。
/// DB 写入侧不强制规范化（避免吞用户写入），运行时统一在这里规范化。
pub fn parse_allowed_models(spec: &str) -> Vec<String> {
    spec.split(',')
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
        .collect()
}

pub struct KeyCache {
    inner: Mutex<LruCache<String, KeyCacheEntry>>,
    ttl: Duration,
}

impl KeyCache {
    pub fn new() -> Self {
        Self::with_capacity(KEY_CACHE_MAX, KEY_CACHE_TTL)
    }

    pub fn with_capacity(cap: usize, ttl: Duration) -> Self {
        let cap = NonZeroUsize::new(cap.max(1)).expect("non-zero capacity");
        Self {
            inner: Mutex::new(LruCache::new(cap)),
            ttl,
        }
    }

    /// 命中且未过期返回克隆；过期或缺失返回 None 且把过期项移除。
    pub fn get_fresh(&self, raw_key: &str) -> Option<KeyCacheEntry> {
        let mut guard = self.inner.lock();
        match guard.get(raw_key) {
            Some(entry) if entry.is_fresh(self.ttl) => Some(entry.clone()),
            Some(_) => {
                guard.pop(raw_key);
                None
            }
            None => None,
        }
    }

    /// 写入（或刷新）一项。LruCache::put 自动驱逐冷项 + promote 到尾。
    pub fn insert(&self, raw_key: String, entry: KeyCacheEntry) {
        self.inner.lock().put(raw_key, entry);
    }

    /// 显式踢出。admin PATCH/DELETE 时调用，避免改完 60s 内陈旧。
    pub fn invalidate(&self, raw_key: &str) {
        self.inner.lock().pop(raw_key);
    }

    /// 全局踢出。upstream_keys 池变化时所有 entry 的 upstream_key 字段都可能过时。
    pub fn invalidate_all(&self) {
        self.inner.lock().clear();
    }

    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().is_empty()
    }

    /// loader 形态由 SingleFlight 包一层使用。本函数只负责"看一眼缓存有没有"
    /// 的快速路径——异步加载逻辑放到 SingleFlight，避免持锁穿越 await。
    pub async fn get_or_load<F, Fut>(&self, raw_key: &str, loader: F) -> AppResult<Option<KeyCacheEntry>>
    where
        F: FnOnce(String) -> Fut,
        Fut: std::future::Future<Output = AppResult<Option<KeyCacheEntry>>>,
    {
        if let Some(hit) = self.get_fresh(raw_key) {
            return Ok(Some(hit));
        }
        let loaded = loader(raw_key.to_string()).await?;
        if let Some(entry) = loaded.as_ref() {
            self.insert(raw_key.to_string(), entry.clone());
        }
        Ok(loaded)
    }
}

impl Default for KeyCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(name: &str) -> KeyCacheEntry {
        KeyCacheEntry {
            id: 1,
            rpm_limit: -1,
            name: name.into(),
            upstream_key: "enterprise:ghp_x".into(),
            quota: -1.0,
            allow_fast: true,
            max_concurrency: -1,
            allowed_models: Vec::new(),
            channel_kind: ChannelKind::Copilot,
            fetched_at: Instant::now(),
        }
    }

    #[test]
    fn insert_then_hit() {
        let cache = KeyCache::new();
        cache.insert("sk-a".into(), entry("a"));
        let hit = cache.get_fresh("sk-a").expect("hit");
        assert_eq!(hit.name, "a");
    }

    #[test]
    fn miss_returns_none() {
        let cache = KeyCache::new();
        assert!(cache.get_fresh("sk-missing").is_none());
    }

    #[test]
    fn ttl_expiry_pops() {
        let cache = KeyCache::with_capacity(8, Duration::from_millis(0));
        let mut e = entry("a");
        e.fetched_at = Instant::now() - Duration::from_secs(1);
        cache.insert("sk-a".into(), e);
        assert!(cache.get_fresh("sk-a").is_none());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn invalidate_removes() {
        let cache = KeyCache::new();
        cache.insert("sk-a".into(), entry("a"));
        cache.invalidate("sk-a");
        assert!(cache.get_fresh("sk-a").is_none());
    }

    #[test]
    fn invalidate_all_clears() {
        let cache = KeyCache::new();
        cache.insert("sk-a".into(), entry("a"));
        cache.insert("sk-b".into(), entry("b"));
        cache.invalidate_all();
        assert!(cache.is_empty());
    }

    #[test]
    fn lru_eviction_drops_cold() {
        let cache = KeyCache::with_capacity(2, KEY_CACHE_TTL);
        cache.insert("a".into(), entry("a"));
        cache.insert("b".into(), entry("b"));
        let _ = cache.get_fresh("a");
        cache.insert("c".into(), entry("c"));
        assert!(cache.get_fresh("b").is_none(), "b should be evicted");
        assert!(cache.get_fresh("a").is_some());
        assert!(cache.get_fresh("c").is_some());
    }

    #[tokio::test]
    async fn get_or_load_hits_cache_first() {
        let cache = KeyCache::new();
        cache.insert("sk-a".into(), entry("a"));
        let mut calls = 0;
        let out = cache
            .get_or_load("sk-a", |_| {
                calls += 1;
                async move { Ok(Some(entry("loaded"))) }
            })
            .await
            .expect("ok")
            .expect("some");
        assert_eq!(out.name, "a");
        assert_eq!(calls, 0);
    }

    #[tokio::test]
    async fn get_or_load_miss_calls_loader_and_caches() {
        let cache = KeyCache::new();
        let out = cache
            .get_or_load("sk-x", |_| async move { Ok(Some(entry("loaded"))) })
            .await
            .expect("ok")
            .expect("some");
        assert_eq!(out.name, "loaded");
        assert!(cache.get_fresh("sk-x").is_some());
    }

    #[test]
    fn parse_allowed_models_basic() {
        assert_eq!(
            parse_allowed_models("claude-opus-4-7, claude-sonnet-4-5,,"),
            vec!["claude-opus-4-7".to_string(), "claude-sonnet-4-5".to_string()]
        );
        assert!(parse_allowed_models("   ").is_empty());
    }

    #[test]
    fn model_allowed_empty_whitelist_permits_all() {
        let e = entry("a");
        assert!(e.model_allowed("anything"));
    }

    #[test]
    fn model_allowed_exact_match_lowercase() {
        let mut e = entry("a");
        e.allowed_models = vec!["claude-opus-4-7".into()];
        assert!(e.model_allowed("claude-opus-4-7"));
        assert!(e.model_allowed("Claude-OPUS-4-7"));
        assert!(!e.model_allowed("claude-opus-4-6"));
    }

    #[tokio::test]
    async fn get_or_load_negative_not_cached() {
        let cache = KeyCache::new();
        let out = cache
            .get_or_load("sk-missing", |_| async move { Ok(None) })
            .await
            .expect("ok");
        assert!(out.is_none());
        assert!(cache.get_fresh("sk-missing").is_none());
    }
}
