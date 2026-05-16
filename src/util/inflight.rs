//! in-flight 连接计数器。spawn 一个 connection task 前 `enter()` 拿 RAII guard，
//! task 完成时自动 fetch_sub；shutdown 路径 `wait_drained()` 阻塞到计数清零或超时。

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;

#[derive(Clone)]
pub struct InflightTracker {
    inner: Arc<Inner>,
}

struct Inner {
    count: AtomicUsize,
    drained: Notify,
}

impl InflightTracker {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                count: AtomicUsize::new(0),
                drained: Notify::new(),
            }),
        }
    }

    pub fn enter(&self) -> InflightGuard {
        self.inner.count.fetch_add(1, Ordering::SeqCst);
        InflightGuard { inner: self.inner.clone() }
    }

    pub fn current(&self) -> usize {
        self.inner.count.load(Ordering::SeqCst)
    }

    /// 阻塞到 in-flight 清零。先注册 notified() 再 load 计数，防止
    /// load 与 notify_waiters 之间的窗口让本 Future 永远挂起。
    pub async fn wait_drained(&self) {
        loop {
            let notified = self.inner.drained.notified();
            if self.inner.count.load(Ordering::SeqCst) == 0 {
                return;
            }
            notified.await;
        }
    }
}

impl Default for InflightTracker {
    fn default() -> Self {
        Self::new()
    }
}

pub struct InflightGuard {
    inner: Arc<Inner>,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        if self.inner.count.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.inner.drained.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn drain_resolves_when_all_released() {
        let t = InflightTracker::new();
        let g1 = t.enter();
        let g2 = t.enter();
        assert_eq!(t.current(), 2);

        let t2 = t.clone();
        let drain = tokio::spawn(async move { t2.wait_drained().await });

        drop(g1);
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(!drain.is_finished(), "still 1 in-flight");

        drop(g2);
        tokio::time::timeout(Duration::from_millis(100), drain)
            .await
            .expect("drain should complete")
            .expect("join");
    }

    #[tokio::test]
    async fn drain_returns_immediately_when_idle() {
        let t = InflightTracker::new();
        tokio::time::timeout(Duration::from_millis(100), t.wait_drained())
            .await
            .expect("idle drain returns instantly");
    }
}
