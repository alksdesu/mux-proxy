//! ghu_/gho_ → Copilot session token 兑换。LRU 1000 项，提前 60s 续期；
//! 并发同一 ghu 只调一次上游（DashMap + OnceCell 单飞）；5 分钟周期清过期项。

use crate::error::{AppError, AppResult};
use dashmap::DashMap;
use lru::LruCache;
use parking_lot::Mutex;
use reqwest::Client;
use serde::Deserialize;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::OnceCell;

pub const SESSION_TOKEN_CACHE_MAX: usize = 1000;
pub const RENEWAL_LEEWAY: Duration = Duration::from_secs(60);
pub const TOKEN_FETCH_TIMEOUT: Duration = Duration::from_secs(10);
pub const CLEANUP_INTERVAL: Duration = Duration::from_secs(300);

const GITHUB_TOKEN_URL: &str = "https://api.github.com/copilot_internal/v2/token";

#[derive(Clone, Debug)]
pub struct TokenEntry {
    pub token: String,
    /// unix epoch 秒。判定时 `now < expires_at - 60s` 命中。
    pub expires_at: u64,
}

#[derive(Deserialize)]
struct TokenResponse {
    token: String,
    expires_at: u64,
}

type InflightCell = Arc<OnceCell<Result<TokenEntry, String>>>;

pub struct SessionTokenCache {
    cache: Mutex<LruCache<String, TokenEntry>>,
    inflight: DashMap<String, InflightCell>,
    http: Client,
}

impl SessionTokenCache {
    pub fn new() -> Arc<Self> {
        Self::with_client(default_client())
    }

    pub fn with_client(http: Client) -> Arc<Self> {
        let cap = NonZeroUsize::new(SESSION_TOKEN_CACHE_MAX).expect("non-zero");
        let cache = Arc::new(Self {
            cache: Mutex::new(LruCache::new(cap)),
            inflight: DashMap::new(),
            http,
        });
        cache.clone().spawn_cleanup();
        cache
    }

    fn spawn_cleanup(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(CLEANUP_INTERVAL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                self.purge_expired();
            }
        });
    }

    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    fn is_fresh(entry: &TokenEntry, now: u64) -> bool {
        now < entry.expires_at.saturating_sub(RENEWAL_LEEWAY.as_secs())
    }

    fn try_hit(&self, ghu: &str) -> Option<TokenEntry> {
        let mut guard = self.cache.lock();
        let entry = guard.get(ghu)?.clone();
        if Self::is_fresh(&entry, Self::now_secs()) {
            Some(entry)
        } else {
            guard.pop(ghu);
            None
        }
    }

    fn store(&self, ghu: String, entry: TokenEntry) {
        let mut guard = self.cache.lock();
        guard.put(ghu, entry);
    }

    fn purge_expired(&self) {
        let now = Self::now_secs();
        let mut guard = self.cache.lock();
        let stale: Vec<String> = guard
            .iter()
            .filter(|(_, v)| now >= v.expires_at)
            .map(|(k, _)| k.clone())
            .collect();
        for k in stale {
            guard.pop(&k);
        }
    }

    /// 取或换 session token。已命中（含未到期）直接返；否则 inflight 单飞调上游。
    pub async fn get_or_fetch(&self, ghu: &str) -> AppResult<String> {
        if let Some(hit) = self.try_hit(ghu) {
            return Ok(hit.token);
        }

        let cell = self
            .inflight
            .entry(ghu.to_string())
            .or_insert_with(|| Arc::new(OnceCell::new()))
            .clone();

        let ghu_owned = ghu.to_string();
        let http = self.http.clone();
        let result = cell
            .get_or_init(|| async move {
                fetch_token(&http, &ghu_owned)
                    .await
                    .map_err(|e| e.to_string())
            })
            .await
            .clone();

        // 单飞完成，清掉占位项。后续命中直接走缓存。
        self.inflight.remove(ghu);

        match result {
            Ok(entry) => {
                self.store(ghu.to_string(), entry.clone());
                Ok(entry.token)
            }
            Err(e) => Err(AppError::Upstream(e)),
        }
    }

    pub fn cached_count(&self) -> usize {
        self.cache.lock().len()
    }
}

async fn fetch_token(http: &Client, ghu: &str) -> AppResult<TokenEntry> {
    let resp = http
        .get(GITHUB_TOKEN_URL)
        .header("Authorization", format!("token {ghu}"))
        .header("Editor-Version", "vscode/1.96.0")
        .timeout(TOKEN_FETCH_TIMEOUT)
        .send()
        .await
        .map_err(|e| AppError::Upstream(format!("session token request failed: {e}")))?;
    if !resp.status().is_success() {
        return Err(AppError::Upstream(format!(
            "session token exchange failed: {}",
            resp.status()
        )));
    }
    let body: TokenResponse = resp
        .json()
        .await
        .map_err(|e| AppError::Upstream(format!("session token parse failed: {e}")))?;
    Ok(TokenEntry {
        token: body.token,
        expires_at: body.expires_at,
    })
}

fn default_client() -> Client {
    Client::builder()
        .user_agent("copilot/1.0.5 (mux-proxy)")
        .build()
        .expect("reqwest client build")
}

// 防止 Instant 警告
#[allow(dead_code)]
fn _instant_in_use() -> Instant {
    Instant::now()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn future_entry(secs_until_expiry: u64) -> TokenEntry {
        TokenEntry {
            token: "tok".into(),
            expires_at: SessionTokenCache::now_secs() + secs_until_expiry,
        }
    }

    #[test]
    fn fresh_within_leeway_returns_hit() {
        let entry = future_entry(120);
        let now = SessionTokenCache::now_secs();
        assert!(SessionTokenCache::is_fresh(&entry, now));
    }

    #[test]
    fn expired_within_leeway_treated_as_stale() {
        let entry = future_entry(30);
        let now = SessionTokenCache::now_secs();
        assert!(!SessionTokenCache::is_fresh(&entry, now));
    }

    #[test]
    fn past_expiry_treated_as_stale() {
        let entry = TokenEntry {
            token: "tok".into(),
            expires_at: SessionTokenCache::now_secs().saturating_sub(10),
        };
        let now = SessionTokenCache::now_secs();
        assert!(!SessionTokenCache::is_fresh(&entry, now));
    }

    #[tokio::test]
    async fn store_then_hit() {
        let cache = SessionTokenCache::new();
        cache.store("ghu_a".into(), future_entry(300));
        let hit = cache.try_hit("ghu_a").expect("hit");
        assert_eq!(hit.token, "tok");
    }

    #[tokio::test]
    async fn stale_hit_is_popped() {
        let cache = SessionTokenCache::new();
        cache.store("ghu_a".into(), future_entry(30)); // 在 leeway 内
        assert!(cache.try_hit("ghu_a").is_none());
        assert_eq!(cache.cached_count(), 0);
    }

    #[tokio::test]
    async fn purge_expired_removes_past_entries() {
        let cache = SessionTokenCache::new();
        cache.store(
            "ghu_dead".into(),
            TokenEntry {
                token: "x".into(),
                expires_at: SessionTokenCache::now_secs().saturating_sub(1),
            },
        );
        cache.store("ghu_live".into(), future_entry(600));
        cache.purge_expired();
        assert!(cache.try_hit("ghu_dead").is_none());
        assert!(cache.try_hit("ghu_live").is_some());
    }

    #[tokio::test]
    async fn lru_capacity_evicts_oldest() {
        let small_cache = Arc::new(SessionTokenCache {
            cache: Mutex::new(LruCache::new(NonZeroUsize::new(2).unwrap())),
            inflight: DashMap::new(),
            http: default_client(),
        });
        small_cache.store("a".into(), future_entry(300));
        small_cache.store("b".into(), future_entry(300));
        small_cache.store("c".into(), future_entry(300));
        assert!(small_cache.try_hit("a").is_none(), "a should be evicted");
        assert!(small_cache.try_hit("b").is_some());
        assert!(small_cache.try_hit("c").is_some());
    }
}
