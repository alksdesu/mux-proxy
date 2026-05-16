//! per-key RPM 限流。挂在 client_auth + quota 之后，dispatch 之前。
//! 命中 entry.rpm_limit 后拒 429，metric 计数。

use crate::app::AppState;
use crate::auth::KeyCacheEntry;
use crate::error::AppError;
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::Response;

pub struct RateLimit;

pub async fn rate_limit_layer(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Result<Response, AppError> {
    let entry = req
        .extensions()
        .get::<KeyCacheEntry>()
        .ok_or_else(|| AppError::Internal("rate_limit layer requires client_auth first".into()))?;
    if state.rate_limiter.try_acquire(&entry.name, entry.rpm_limit).is_err() {
        crate::metrics::GLOBAL.rate_limit_rejections_total.inc();
        return Err(AppError::RateLimited(format!(
            "key '{}' exceeded RPM limit of {}",
            entry.name, entry.rpm_limit
        )));
    }
    Ok(next.run(req).await)
}
