//! 异步 singleflight：同一 key 的并发请求共享一次 loader，挡 cache-miss 雪崩。
//! 失败也对所有等待者广播，重试策略让调用方自决；KeyCache miss 默认不重试。

use crate::error::AppError;
use dashmap::DashMap;
use futures::FutureExt;
use futures::future::{BoxFuture, Shared};
use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;

type SharedResult<V> = Shared<BoxFuture<'static, Result<V, Arc<AppError>>>>;

pub struct SingleFlight<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    inner: Arc<DashMap<K, SharedResult<V>>>,
}

impl<K, V> SingleFlight<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
        }
    }

    /// 已在飞 → 等同一个 future；否则发起新的 loader。
    /// loader 错误用 Arc 包裹，因为同一个错误要广播给多个等待者。
    pub async fn run<F, Fut>(&self, key: K, loader: F) -> Result<V, Arc<AppError>>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<V, AppError>> + Send + 'static,
    {
        if let Some(existing) = self.inner.get(&key) {
            let fut = existing.clone();
            drop(existing);
            return fut.await;
        }

        let fut: Fut = loader();
        let map = self.inner.clone();
        let cleanup_key = key.clone();
        let wrapped: BoxFuture<'static, Result<V, Arc<AppError>>> = async move {
            let result = fut.await.map_err(Arc::new);
            map.remove(&cleanup_key);
            result
        }
        .boxed();
        let shared = wrapped.shared();

        match self.inner.entry(key) {
            dashmap::mapref::entry::Entry::Occupied(occ) => {
                let waiter = occ.get().clone();
                drop(occ);
                waiter.await
            }
            dashmap::mapref::entry::Entry::Vacant(vac) => {
                vac.insert(shared.clone());
                shared.await
            }
        }
    }

    pub fn in_flight(&self) -> usize {
        self.inner.len()
    }
}

impl<K, V> Default for SingleFlight<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Clone for SingleFlight<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    #[tokio::test]
    async fn single_call_only_loader_once() {
        let sf: SingleFlight<String, u32> = SingleFlight::new();
        let calls = Arc::new(AtomicU32::new(0));

        let mut handles = Vec::new();
        for _ in 0..16 {
            let sf = sf.clone();
            let calls = calls.clone();
            handles.push(tokio::spawn(async move {
                sf.run("k".into(), move || {
                    let calls = calls.clone();
                    async move {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(42_u32)
                    }
                })
                .await
            }));
        }
        for h in handles {
            let v = h.await.expect("join").expect("result");
            assert_eq!(v, 42);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn different_keys_run_independently() {
        let sf: SingleFlight<String, u32> = SingleFlight::new();
        let calls = Arc::new(AtomicU32::new(0));

        let a = {
            let calls = calls.clone();
            sf.run("a".into(), move || {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(1)
                }
            })
        };
        let b = {
            let calls = calls.clone();
            sf.run("b".into(), move || {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(2)
                }
            })
        };
        let (ra, rb) = tokio::join!(a, b);
        assert_eq!(ra.expect("a"), 1);
        assert_eq!(rb.expect("b"), 2);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn error_propagates_to_all_waiters() {
        let sf: SingleFlight<String, u32> = SingleFlight::new();
        let mut handles = Vec::new();
        for _ in 0..4 {
            let sf = sf.clone();
            handles.push(tokio::spawn(async move {
                sf.run("k".into(), || async {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    Err(AppError::Internal("boom".into()))
                })
                .await
            }));
        }
        for h in handles {
            let err = h.await.expect("join").expect_err("must err");
            assert!(matches!(&*err, AppError::Internal(s) if s == "boom"));
        }
    }

    #[tokio::test]
    async fn entry_removed_after_completion() {
        let sf: SingleFlight<String, u32> = SingleFlight::new();
        sf.run("k".into(), || async { Ok(1_u32) }).await.expect("ok");
        assert_eq!(sf.in_flight(), 0);
    }
}
