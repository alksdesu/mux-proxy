//! 客户端 /v1/* 路由分派。client_auth + quota 由 layer 完成；本 handler 占并发位、
//! 按 KeyCacheEntry.channel_kind 分派到 copilot 或 anthropic handler，再桥到 axum::Response。

use crate::app::AppState;
use crate::auth::KeyCacheEntry;
use crate::channels::ChannelKind;
use crate::channels::anthropic::handler::{
    self as anthropic_handler, HandlerContext as AnthropicCtx, ProxyRequest, ProxyResponse,
};
use crate::channels::copilot::{
    ChannelContext as CopilotCtx, CopilotHandler, HandlerOutcome, UpstreamConfig,
    resolve_upstream_config,
};
use crate::concurrency::ConcurrencyGuard;
use crate::error::AppError;
use crate::http::middleware::auth::client_auth_layer;
use crate::http::middleware::quota::quota_layer;
use crate::http::middleware::rate_limit::rate_limit_layer;
use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, Method, Response as AxumResponse};
use axum::middleware::from_fn_with_state;
use axum::routing::any;
use bytes::Bytes;

/// 客户端单次请求体最大尺寸。32 MiB 与 anthropic handler 内部 buffer cap 对齐。
pub const MAX_REQUEST_BODY: usize = 32 * 1024 * 1024;

pub fn build_v1_router(state: AppState) -> Router<AppState> {
    // layer 应用顺序与请求处理顺序相反：client_auth 最先跑（最外层 layer），
    // 然后 quota，再 rate_limit，最后 dispatch。
    Router::new()
        .route("/v1", any(dispatch))
        .route("/v1/*path", any(dispatch))
        .layer(from_fn_with_state(state.clone(), rate_limit_layer))
        .layer(from_fn_with_state(state.clone(), quota_layer))
        .layer(from_fn_with_state(state, client_auth_layer))
}

async fn dispatch(
    State(state): State<AppState>,
    req: Request,
) -> Result<AxumResponse<Body>, AppError> {
    let entry = req
        .extensions()
        .get::<KeyCacheEntry>()
        .cloned()
        .ok_or_else(|| AppError::Internal("dispatch requires client_auth".into()))?;
    let channel = entry.channel_kind;
    let started = std::time::Instant::now();

    let guard = match state.limiter.try_acquire(&entry.name, entry.max_concurrency) {
        Some(g) => g,
        None => {
            crate::metrics::GLOBAL.concurrency_rejections_total.inc();
            return Err(AppError::ConcurrencyExceeded);
        }
    };

    let (parts, body) = req.into_parts();
    let bytes = axum::body::to_bytes(body, MAX_REQUEST_BODY)
        .await
        .map_err(|e| AppError::BadRequest(format!("read request body: {e}")))?;

    let method = parts.method;
    let path = parts.uri.path().to_string();
    let query = parts.uri.query().unwrap_or("").to_string();
    let headers = parts.headers;
    let client_ip = extract_client_ip(&headers);

    // 每 key 模型白名单。空白名单 = 不限制；非空 + 请求里有 model 字段 + 不命中 → 403。
    // 没 model 字段（GET / 非 JSON / 体内无 model）直接放行不拦。
    if !entry.allowed_models.is_empty() {
        let content_type = headers
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if let Some(requested) =
            crate::shared::model_field::extract_model_field(&bytes, content_type)
        {
            if !entry.model_allowed(&requested) {
                return Err(AppError::ModelNotAllowed { model: requested });
            }
        }
    }

    let result = match channel {
        ChannelKind::Copilot => {
            dispatch_copilot(state, entry, guard, method, path, query, headers, bytes, client_ip)
                .await
        }
        ChannelKind::Anthropic => {
            dispatch_anthropic(state, entry, guard, method, path, query, headers, bytes, client_ip)
                .await
        }
    };

    let elapsed = started.elapsed().as_secs_f64();
    let status = match &result {
        Ok(resp) => resp.status().as_u16(),
        Err(e) => e.status_code(),
    };
    crate::metrics::GLOBAL.record_request(channel, status, elapsed);
    result
}

#[allow(clippy::too_many_arguments)]
async fn dispatch_copilot(
    state: AppState,
    entry: KeyCacheEntry,
    guard: ConcurrencyGuard,
    method: Method,
    path: String,
    query: String,
    headers: HeaderMap,
    body: Bytes,
    client_ip: String,
) -> Result<AxumResponse<Body>, AppError> {
    let upstream_cfg: UpstreamConfig = resolve_upstream_config(&entry.upstream_key)
        .ok_or_else(|| {
            AppError::Internal(format!(
                "invalid upstream_key for key {}: {}",
                entry.name, entry.upstream_key
            ))
        })?;

    let handler = CopilotHandler {
        http: state.copilot_http.clone(),
        pool: state.copilot_pool.clone(),
        breaker: state.copilot_breaker.clone(),
        session_token: state.copilot_session.clone(),
        usage_writer: state.usage_writer.clone(),
        config: state.cfg.clone(),
    };

    let ctx = CopilotCtx {
        method,
        path,
        query,
        headers,
        body,
        key: entry,
        upstream_cfg,
        client_ip,
        guard: Some(guard),
    };

    let outcome = handler.handle(ctx).await?;
    Ok(outcome_to_response(outcome))
}

#[allow(clippy::too_many_arguments)]
async fn dispatch_anthropic(
    state: AppState,
    entry: KeyCacheEntry,
    guard: ConcurrencyGuard,
    method: Method,
    path: String,
    query: String,
    headers: HeaderMap,
    body: Bytes,
    client_ip: String,
) -> Result<AxumResponse<Body>, AppError> {
    // load_full 拿到的 Arc 在本 dispatch 生命周期内固定，
    // admin 改 rules 后下一个 dispatch 才看到新版本。直接传 Arc 不深 clone。
    let ctx = AnthropicCtx {
        client: state.anthropic_client.clone(),
        key_pool: state.anthropic_pool.clone(),
        usage_writer: state.usage_writer.clone(),
        rewrite_rules: state.anthropic_rules.load_full(),
        key_cache_entry: entry,
        client_ip: if client_ip.is_empty() { None } else { Some(client_ip) },
        concurrency_guard: guard,
    };

    let req = ProxyRequest {
        method,
        path,
        raw_query: if query.is_empty() { None } else { Some(query) },
        headers,
        body,
    };

    let resp = anthropic_handler::handle(ctx, req).await?;
    Ok(proxy_to_axum(resp))
}

fn outcome_to_response(outcome: HandlerOutcome) -> AxumResponse<Body> {
    match outcome {
        HandlerOutcome::Bytes { status, headers, body } => {
            let mut builder = AxumResponse::builder().status(status);
            for (name, value) in headers.iter() {
                builder = builder.header(name, value);
            }
            builder
                .body(Body::from(body))
                .expect("axum response from buffered bytes")
        }
        HandlerOutcome::Stream { status, headers, body } => {
            // BoxedStream: Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Unpin>
            // axum::body::Body::from_stream 接受 TryStream，自动实现于 Stream<Item=Result<_,_>>。
            let stream_body = Body::from_stream(BoxedStreamCompat(body));
            let mut builder = AxumResponse::builder().status(status);
            for (name, value) in headers.iter() {
                builder = builder.header(name, value);
            }
            builder
                .body(stream_body)
                .expect("axum response from stream body")
        }
    }
}

/// 适配 channels::copilot::handler::BoxedStream 到 axum Body::from_stream 期望的形态。
/// 直接 from_stream(box_stream) 会因 dyn Stream + Unpin 的 GAT 推导失败，包一层显式 newtype 解决。
struct BoxedStreamCompat(crate::channels::copilot::handler::BoxedStream);

impl futures::Stream for BoxedStreamCompat {
    type Item = Result<Bytes, std::io::Error>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::pin::Pin::new(&mut *self.0).poll_next(cx)
    }
}

fn proxy_to_axum(resp: ProxyResponse) -> AxumResponse<Body> {
    let (parts, body) = resp.into_parts();
    let axum_body = Body::new(body);
    AxumResponse::from_parts(parts, axum_body)
}

fn extract_client_ip(headers: &HeaderMap) -> String {
    if let Some(ip) = headers.get("cf-connecting-ip").and_then(|v| v.to_str().ok()) {
        let trimmed = ip.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    if let Some(ip) = headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.split(',').next())
    {
        let trimmed = ip.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    String::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn headers_with(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(
                axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        h
    }

    #[test]
    fn extract_ip_prefers_cf_connecting() {
        let h = headers_with(&[
            ("cf-connecting-ip", "1.2.3.4"),
            ("x-forwarded-for", "5.6.7.8, 9.10.11.12"),
        ]);
        assert_eq!(extract_client_ip(&h), "1.2.3.4");
    }

    #[test]
    fn extract_ip_falls_back_to_xff_first_hop() {
        let h = headers_with(&[("x-forwarded-for", "5.6.7.8, 9.10.11.12")]);
        assert_eq!(extract_client_ip(&h), "5.6.7.8");
    }

    #[test]
    fn extract_ip_empty_when_no_header() {
        let h = HeaderMap::new();
        assert_eq!(extract_client_ip(&h), "");
    }

    #[test]
    fn extract_ip_ignores_blank_cf_header() {
        let h = headers_with(&[
            ("cf-connecting-ip", "   "),
            ("x-forwarded-for", "5.6.7.8"),
        ]);
        assert_eq!(extract_client_ip(&h), "5.6.7.8");
    }
}
