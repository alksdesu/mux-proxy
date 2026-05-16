//! 官方 Anthropic key 池：纯随机选 + 401/403 直接熔断、429 滑动窗口累计。
//! 熔断逻辑走 ``shared::breaker`` 泛型实现；本模块负责 keys 缓存 + DB 同步 +
//! ``BreakerInfo`` 到带 ``ChannelKind`` 的 ``BreakerSnapshot`` 的 wrap。
//! per-key 的 rewrite_rules / allowed_models 在 [`PooledKey`] 上随 key 一起加载，
//! handler 选完 key 后直接读，无需二次查库。

use crate::channels::anthropic::model_splice::RewriteRule;
use crate::channels::anthropic::upstream_key;
use crate::channels::{BreakerSnapshot, ChannelKind};
use crate::db::Db;
use crate::db::upstream::UpstreamChangeNotifier;
use crate::error::{AppError, AppResult};
use crate::shared::breaker::{
    Breaker as SharedBreaker, BreakerConfig, SlidingWindowStrategy,
};
use parking_lot::Mutex;
use rand::seq::SliceRandom;
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
    /// per-key 改写规则。None 表示落全局 anthropic_rewrite_rules 兜底；
    /// Some(vec) 完整覆盖全局，即使 vec 是空也代表"该 key 显式禁用 rewrite"
    /// （DB 层 PATCH 时空数组已被归一化成 NULL，所以这里 Some 永远非空）。
    pub rewrite_rules: Option<Vec<RewriteRule>>,
    /// per-key 允许的客户端 model 白名单。None 表示无限制；
    /// Some(vec) 表示精确匹配（lowercase 比对）。
    pub allowed_models: Option<Vec<String>>,
}

impl PooledKey {
    /// 判断该 key 是否允许给 ``model`` 用。
    /// - 该 key 没配 allowed_models（None）：永远放行。
    /// - 客户端没传 model 字段（``model=None``，发生在 GET /v1/models / count_tokens 等
    ///   无 body 路径）：跳过白名单过滤，让请求落到上游裁定，**避免代理把"无对话 model"
    ///   的合法路径预拦截成 model_not_supported**。
    /// - 客户端传了 model：走 lowercase 精确比对（前后 trim）。
    pub fn allows_model(&self, model: Option<&str>) -> bool {
        let Some(allowed) = self.allowed_models.as_deref() else {
            return true;
        };
        let Some(m) = model else {
            return true;
        };
        let needle = m.trim().to_ascii_lowercase();
        allowed
            .iter()
            .any(|a| a.trim().to_ascii_lowercase() == needle)
    }
}

/// pick 一次的结果：``Picked`` 拿到 key；``PoolEmpty`` 池里没有可用 key（全空/全熔断）；
/// ``NoKeyForModel`` 池非空但都不允许这个 model — 直接给客户端返 400 model_not_supported，
/// 不再尝试发上游（防止红队特征错误冒出来）。
#[derive(Debug, Clone)]
pub enum PickOutcome {
    Picked(PooledKey),
    PoolEmpty,
    NoKeyForModel,
}

struct PoolInner {
    keys: Vec<PooledKey>,
    loaded_at: Instant,
}

impl PoolInner {
    fn empty() -> Self {
        Self {
            keys: Vec::new(),
            loaded_at: Instant::now() - POOL_TTL - Duration::from_secs(1),
        }
    }
}

pub struct KeyPool {
    inner: Mutex<PoolInner>,
    breaker: SharedBreaker<SlidingWindowStrategy>,
    db: Db,
    notifier: UpstreamChangeNotifier,
    notify_handle: Arc<Notify>,
}

fn new_breaker() -> SharedBreaker<SlidingWindowStrategy> {
    SharedBreaker::new(BreakerConfig {
        threshold: BREAKER_THRESHOLD,
        window: BREAKER_WINDOW,
        recover: BREAKER_RECOVER,
    })
}

impl KeyPool {
    pub fn new(db: Db, notifier: UpstreamChangeNotifier) -> Arc<Self> {
        let notify_handle = notifier.handle();
        Arc::new(Self {
            inner: Mutex::new(PoolInner::empty()),
            breaker: new_breaker(),
            db,
            notifier,
            notify_handle,
        })
    }

    /// 从池中拿一把可用 key，按 ``requested_model`` 过滤 allowed_models 白名单。
    /// ``exclude_ids`` 用于上轮命中 401 后避开同一把。
    /// 返回值见 [`PickOutcome`]：池空 / 池非空但都不允许该 model / 选中。
    /// ``is_disabled`` 命中会顺手把超时熔断自动复位。
    pub async fn pick(
        &self,
        exclude_ids: &[i64],
        requested_model: Option<&str>,
    ) -> AppResult<PickOutcome> {
        self.ensure_fresh().await?;
        let guard = self.inner.lock();
        let alive: Vec<&PooledKey> = guard
            .keys
            .iter()
            .filter(|k| !exclude_ids.contains(&k.id) && !self.breaker.is_disabled(k.id))
            .collect();
        if alive.is_empty() {
            return Ok(PickOutcome::PoolEmpty);
        }
        let allowed: Vec<&PooledKey> = alive
            .iter()
            .copied()
            .filter(|k| k.allows_model(requested_model))
            .collect();
        if allowed.is_empty() {
            return Ok(PickOutcome::NoKeyForModel);
        }
        let mut rng = rand::thread_rng();
        Ok(allowed
            .choose(&mut rng)
            .map(|k| PickOutcome::Picked((*k).clone()))
            .expect("non-empty list has random element"))
    }

    /// 上游返回 401/403 → 立刻把该 key 熔断（避免重试又用同一把）。
    pub fn report_auth_failure(&self, key_id: i64) {
        self.breaker.force_disable(key_id);
        warn!(key_id, "anthropic key auth failed, breaker opened");
    }

    /// 上游 429 → 滑动窗口累计失败计数，到阈值开熔断。
    pub fn report_rate_limited(&self, key_id: i64) {
        let tripped = self.breaker.record_failure(key_id);
        debug!(key_id, tripped, "anthropic key rate limited, recorded");
    }

    /// 业务上验证 key 健康（成功响应）时清掉历史失败。
    pub fn report_success(&self, key_id: i64) {
        self.breaker.reset(key_id);
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
                    rewrite_rules: r.rewrite_rules,
                    allowed_models: r.allowed_models,
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
        self.breaker
            .snapshot()
            .into_iter()
            .map(|info| BreakerSnapshot {
                id: info.id,
                channel_kind: ChannelKind::Anthropic,
                count: info.count,
                disabled: info.disabled,
                first_at_ms_ago: info.first_at_ms_ago,
                last_at_ms_ago: info.last_at_ms_ago,
            })
            .collect()
    }

    /// admin 手动恢复：清掉指定 key 的失败计数与 open_until。
    pub fn reset_breaker(&self, key_id: i64) {
        self.breaker.reset(key_id);
    }

    /// admin 手动熔断：把指定 key 强制置为 open，``BREAKER_RECOVER`` 后自动恢复。
    pub fn force_disable_breaker(&self, key_id: i64) {
        self.breaker.force_disable(key_id);
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
            }),
            breaker: new_breaker(),
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

/// 客户端请求的 model 不在任何 upstream key 的白名单内时使用。
/// 信息与 Anthropic 官方 invalid_request_error 同构，避免暴露代理路由决策细节。
pub fn model_not_supported_error() -> AppError {
    AppError::BadRequest("model not supported".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_key(id: i64, allowed: Option<Vec<&str>>) -> PooledKey {
        PooledKey {
            id,
            name: format!("key-{id}"),
            token: format!("sk-ant-test-{id}"),
            rewrite_rules: None,
            allowed_models: allowed.map(|v| v.into_iter().map(String::from).collect()),
        }
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

    #[test]
    fn allows_model_none_whitelist_accepts_everything() {
        let k = make_key(1, None);
        assert!(k.allows_model(Some("claude-opus-4-7")));
        assert!(k.allows_model(Some("anything-at-all")));
        // 没传 model 字段也允许（无白名单意味着无限制）。
        assert!(k.allows_model(None));
    }

    #[test]
    fn allows_model_whitelist_case_insensitive() {
        let k = make_key(1, Some(vec!["claude-opus-4-7", "claude-opus-4-7-fast"]));
        assert!(k.allows_model(Some("claude-opus-4-7")));
        assert!(k.allows_model(Some("CLAUDE-OPUS-4-7")));
        assert!(k.allows_model(Some("  claude-opus-4-7-fast  ")));
    }

    #[test]
    fn allows_model_whitelist_rejects_unlisted() {
        let k = make_key(1, Some(vec!["claude-opus-4-7"]));
        assert!(!k.allows_model(Some("claude-sonnet-4-5")));
        assert!(!k.allows_model(Some("claude-opus-4-7-x")));
    }

    #[test]
    fn allows_model_none_bypasses_whitelist_for_non_chat_paths() {
        // GET /v1/models, count_tokens 等无 body / 无 model 字段的请求必须能透传到上游。
        // 白名单只在客户端显式指定 model 时才参与判定。
        let k = make_key(1, Some(vec!["claude-opus-4-7"]));
        assert!(k.allows_model(None));
    }
}
