//! 上游 key 熔断器寄存。P2 这里只暴露最小读/操作接口，
//! P3 渠道 handler 会把真正的滑动窗口逻辑接进来。
//! 接口要稳定，admin 端点和 dashboard 都依赖它。

use crate::channels::ChannelKind;
use dashmap::DashMap;
use serde::Serialize;
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, Clone, Copy)]
pub struct BreakerEntry {
    pub count: u32,
    pub disabled: bool,
    pub first_at: Instant,
    pub last_at: Instant,
    pub disabled_at: Option<Instant>,
}

impl BreakerEntry {
    pub fn new(now: Instant) -> Self {
        Self {
            count: 0,
            disabled: false,
            first_at: now,
            last_at: now,
            disabled_at: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct BreakerStatus {
    pub id: i64,
    pub channel_kind: ChannelKind,
    pub count: u32,
    pub disabled: bool,
    pub first_at: String,
    pub last_at: String,
}

#[derive(Debug)]
pub struct Registry {
    entries: DashMap<(ChannelKind, i64), BreakerEntry>,
    started: Instant,
}

impl Registry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            entries: DashMap::new(),
            started: Instant::now(),
        })
    }

    /// admin POST action=reset 用，清掉计数+解除 disabled。
    pub fn reset(&self, channel: ChannelKind, id: i64) {
        self.entries.remove(&(channel, id));
    }

    /// admin POST action=disable 用，强制熔断到下一次手动 reset。
    pub fn force_disable(&self, channel: ChannelKind, id: i64) {
        let now = Instant::now();
        let mut entry = self
            .entries
            .entry((channel, id))
            .or_insert_with(|| BreakerEntry::new(now));
        entry.disabled = true;
        entry.disabled_at = Some(now);
        entry.last_at = now;
    }

    pub fn snapshot(&self, channel: Option<ChannelKind>) -> Vec<BreakerStatus> {
        let started = self.started;
        let started_walltime = chrono::Utc::now()
            - chrono::Duration::from_std(started.elapsed()).unwrap_or_else(|_| chrono::Duration::zero());
        self.entries
            .iter()
            .filter(|kv| channel.map(|c| kv.key().0 == c).unwrap_or(true))
            .map(|kv| {
                let (ch, id) = *kv.key();
                let e = kv.value();
                BreakerStatus {
                    id,
                    channel_kind: ch,
                    count: e.count,
                    disabled: e.disabled,
                    first_at: instant_to_iso(started, started_walltime, e.first_at),
                    last_at: instant_to_iso(started, started_walltime, e.last_at),
                }
            })
            .collect()
    }
}

fn instant_to_iso(
    started: Instant,
    started_walltime: chrono::DateTime<chrono::Utc>,
    at: Instant,
) -> String {
    let delta = at.saturating_duration_since(started);
    let wall = started_walltime
        + chrono::Duration::from_std(delta).unwrap_or_else(|_| chrono::Duration::zero());
    wall.to_rfc3339()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reset_removes_entry() {
        let reg = Registry::new();
        reg.force_disable(ChannelKind::Copilot, 1);
        assert_eq!(reg.snapshot(None).len(), 1);
        reg.reset(ChannelKind::Copilot, 1);
        assert!(reg.snapshot(None).is_empty());
    }

    #[test]
    fn snapshot_filters_by_channel() {
        let reg = Registry::new();
        reg.force_disable(ChannelKind::Copilot, 1);
        reg.force_disable(ChannelKind::Anthropic, 2);
        assert_eq!(reg.snapshot(Some(ChannelKind::Copilot)).len(), 1);
        assert_eq!(reg.snapshot(Some(ChannelKind::Anthropic)).len(), 1);
        assert_eq!(reg.snapshot(None).len(), 2);
    }

    #[test]
    fn force_disable_idempotent() {
        let reg = Registry::new();
        reg.force_disable(ChannelKind::Copilot, 1);
        reg.force_disable(ChannelKind::Copilot, 1);
        let snap = reg.snapshot(None);
        assert_eq!(snap.len(), 1);
        assert!(snap[0].disabled);
    }
}
