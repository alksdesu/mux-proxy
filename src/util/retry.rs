//! 线性退避重试。base * (attempt+1) 间隔睡眠，最后一次失败把原 error 抛出。

use std::future::Future;
use std::time::Duration;

/// `op` 最多被调 `max_attempts` 次。失败之后 sleep `base_backoff_ms * (attempt + 1)` ms 再试，
/// `max_attempts == 0` 直接返回 Ok(default 不存在) 是无意义的，所以函数 panic。
pub async fn with_retry<F, Fut, T, E>(
    max_attempts: u32,
    base_backoff_ms: u64,
    mut op: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    assert!(max_attempts >= 1, "max_attempts must be >= 1");

    let last_attempt = max_attempts - 1;
    for attempt in 0..max_attempts {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) if attempt == last_attempt => return Err(e),
            Err(_) => {
                let delay = base_backoff_ms.saturating_mul((attempt + 1) as u64);
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
        }
    }
    unreachable!("retry loop exited without returning");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[tokio::test]
    async fn succeeds_first_try() {
        let count = Arc::new(AtomicU32::new(0));
        let c = count.clone();
        let out: Result<u32, &'static str> = with_retry(3, 1, move || {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(42)
            }
        })
        .await;
        assert_eq!(out, Ok(42));
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn succeeds_on_third_attempt() {
        let count = Arc::new(AtomicU32::new(0));
        let c = count.clone();
        let out: Result<u32, &'static str> = with_retry(3, 1, move || {
            let c = c.clone();
            async move {
                let n = c.fetch_add(1, Ordering::SeqCst) + 1;
                if n < 3 { Err("transient") } else { Ok(n) }
            }
        })
        .await;
        assert_eq!(out, Ok(3));
    }

    #[tokio::test]
    async fn fails_after_max_attempts() {
        let count = Arc::new(AtomicU32::new(0));
        let c = count.clone();
        let out: Result<u32, &'static str> = with_retry(3, 1, move || {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Err("nope")
            }
        })
        .await;
        assert_eq!(out, Err("nope"));
        assert_eq!(count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    #[should_panic(expected = "max_attempts must be >= 1")]
    async fn zero_attempts_panics() {
        let _: Result<u32, &'static str> = with_retry(0, 1, || async { Ok(0) }).await;
    }
}
