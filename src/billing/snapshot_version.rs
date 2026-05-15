//! 全局快照版本号。spend / concurrency / key CRUD 任一变化都 bump，
//! dashboard WebSocket 每 3s 轮询，version 变了才推快照。

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct SnapshotVersion(AtomicU64);

impl SnapshotVersion {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn bump(&self) -> u64 {
        self.0.fetch_add(1, Ordering::AcqRel).wrapping_add(1)
    }

    pub fn current(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn starts_at_zero() {
        let v = SnapshotVersion::new();
        assert_eq!(v.current(), 0);
    }

    #[test]
    fn bump_increments() {
        let v = SnapshotVersion::new();
        assert_eq!(v.bump(), 1);
        assert_eq!(v.bump(), 2);
        assert_eq!(v.current(), 2);
    }

    #[tokio::test]
    async fn bumps_are_atomic_across_tasks() {
        let v = Arc::new(SnapshotVersion::new());
        let mut handles = Vec::new();
        for _ in 0..64 {
            let v = v.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..100 {
                    v.bump();
                }
            }));
        }
        for h in handles {
            h.await.expect("join");
        }
        assert_eq!(v.current(), 6400);
    }
}
