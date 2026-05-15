//! Anthropic 渠道主流程：拿 key_pool key → 上游 forward → 四象限响应路由
//! (is_sse × is_gzip) → 字节透传 / gzip 重压 / SSE tee 计费。
//! HeaderMap 写入的 mixed-case header 名只有在共享 server 层启用
//! ``http1::Builder::preserve_header_case(true)`` 之后才能 wire 上保留。

use crate::auth::KeyCacheEntry;
use crate::billing::UsageWriter;
use crate::channels::anthropic::gzip_passthrough::rewrite_gzip;
use crate::channels::anthropic::header_case::canonicalize;
use crate::channels::anthropic::key_pool::{classify_status, pool_empty_error, KeyPool};
use crate::channels::anthropic::model_restore::rewrite_json_response;
use crate::channels::anthropic::model_splice::{rewrite_body, RewriteRule};
use crate::channels::anthropic::request_strip::is_response_hop_by_hop;
use crate::channels::anthropic::sse_tee::{
    spawn_sniffer, try_send_to_sniffer, ForwardSplitter, SniffContext,
};
use crate::channels::anthropic::upstream_client::AnthropicUpstreamClient;
use crate::concurrency::ConcurrencyGuard;
use crate::error::{AppError, AppResult};
use async_stream::stream;
use axum::body::Body;
use axum::response::Response;
use bytes::{Bytes, BytesMut};
use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use http_body_util::BodyExt;
use std::sync::Arc;

/// 大响应字节透传降级阈值。单条响应超过这个值不再 buffer + 改写 model，避免 OOM。
pub const MAX_RESPONSE_BUFFER: usize = 32 * 1024 * 1024;

/// Handler 调用上下文。AppState 不直接耦合本模块，由 admin 层组装好后传入。
pub struct HandlerContext {
    pub client: AnthropicUpstreamClient,
    pub key_pool: Arc<KeyPool>,
    pub usage_writer: UsageWriter,
    pub rewrite_rules: Vec<RewriteRule>,
    pub key_cache_entry: KeyCacheEntry,
    pub client_ip: Option<String>,
    pub concurrency_guard: ConcurrencyGuard,
}

/// 单次请求入参。axum extractor 拆出来填进去。
pub struct ProxyRequest {
    pub method: Method,
    pub path: String,
    pub raw_query: Option<String>,
    pub headers: HeaderMap,
    pub body: Bytes,
}

/// 主入口。返回 axum Response，body 是 ``axum::body::Body``（可包流）。
pub async fn handle(ctx: HandlerContext, req: ProxyRequest) -> AppResult<Response> {
    let content_type = req
        .headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let outcome = rewrite_body(req.body, &content_type, &ctx.rewrite_rules);
    let request_body_for_log = String::new();
    let rewritten = outcome.rewritten();
    let original_model = outcome.original_model.clone();
    let new_model = outcome.new_model.clone();

    let pooled = match ctx.key_pool.pick(&[]).await? {
        Some(k) => k,
        None => return Err(pool_empty_error()),
    };

    let upstream_resp = ctx
        .client
        .forward(
            &req.method,
            &req.path,
            req.raw_query.as_deref(),
            &req.headers,
            outcome.body,
            &pooled.token,
        )
        .await;

    let upstream = match upstream_resp {
        Ok(resp) => resp,
        Err(e) => {
            ctx.key_pool.apply_feedback(pooled.id, classify_status(0));
            return Ok(bad_gateway_response(&e));
        }
    };

    let status = upstream.status();
    let fb = classify_status(status.as_u16());
    ctx.key_pool.apply_feedback(pooled.id, fb);

    let (parts, body) = upstream.into_parts();
    let resp_headers = parts.headers;
    let content_type_resp = lower_header_str(&resp_headers, http::header::CONTENT_TYPE);
    let content_encoding = lower_header_str(&resp_headers, http::header::CONTENT_ENCODING);
    let is_sse = content_type_resp.contains("text/event-stream");
    let is_json = content_type_resp.contains("application/json");
    let is_gzip = content_encoding.contains("gzip");

    let prepared_headers = build_response_headers(&resp_headers);

    if !rewritten {
        return Ok(forward_body_as_is(status, prepared_headers, body));
    }

    let (current_model, original_model) = match (new_model, original_model) {
        (Some(c), Some(o)) => (c, o),
        _ => return Ok(forward_body_as_is(status, prepared_headers, body)),
    };

    if is_sse && !is_gzip {
        return Ok(sse_tee_response(
            status,
            prepared_headers,
            body,
            current_model,
            original_model,
            ctx.usage_writer,
            ctx.key_cache_entry.name.clone(),
            request_body_for_log,
            ctx.client_ip.clone(),
            ctx.concurrency_guard,
        ));
    }

    let buffered = match collect_with_cap(body, MAX_RESPONSE_BUFFER).await {
        Ok(b) => b,
        Err(CollectError::OverCap(stream_pass)) => {
            return Ok(forward_buffered(status, prepared_headers, stream_pass));
        }
        Err(CollectError::Io(e)) => {
            return Err(AppError::Upstream(format!("read upstream body: {e}")));
        }
    };

    drop(ctx.concurrency_guard);

    let rewritten = if is_gzip {
        rewrite_gzip(buffered, &current_model, &original_model, is_sse)
    } else if is_json {
        rewrite_json_response(buffered, &current_model, &original_model)
    } else {
        buffered
    };

    Ok(forward_buffered(status, prepared_headers, rewritten))
}

fn lower_header_str(headers: &HeaderMap, name: http::HeaderName) -> String {
    headers
        .get(&name)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase()
}

fn build_response_headers(src: &HeaderMap) -> Vec<(HeaderName, HeaderValue)> {
    let mut out: Vec<(HeaderName, HeaderValue)> = Vec::with_capacity(src.len());
    for (name, value) in src.iter() {
        let lower = name.as_str().to_ascii_lowercase();
        if is_response_hop_by_hop(&lower) {
            continue;
        }
        let canon = canonicalize(&lower);
        let name_bytes = if canon == lower { name.as_str() } else { canon };
        let Ok(new_name) = HeaderName::from_bytes(name_bytes.as_bytes()) else {
            continue;
        };
        out.push((new_name, value.clone()));
    }
    out
}

fn apply_headers(builder: http::response::Builder, headers: Vec<(HeaderName, HeaderValue)>) -> http::response::Builder {
    let mut b = builder;
    for (name, value) in headers {
        b = b.header(name, value);
    }
    b
}

fn forward_body_as_is(
    status: StatusCode,
    headers: Vec<(HeaderName, HeaderValue)>,
    body: hyper::body::Incoming,
) -> Response {
    let stream_body = Body::new(body.map_err(|e| std::io::Error::other(e.to_string())));
    let builder = http::Response::builder().status(status);
    apply_headers(builder, headers)
        .body(stream_body)
        .expect("response builder must accept valid parts")
}

fn forward_buffered(
    status: StatusCode,
    headers: Vec<(HeaderName, HeaderValue)>,
    body: Bytes,
) -> Response {
    let builder = http::Response::builder().status(status);
    apply_headers(builder, headers)
        .body(Body::from(body))
        .expect("response builder must accept valid parts")
}

fn bad_gateway_response(err: &AppError) -> Response {
    let body = serde_json::json!({
        "error": {"type": "upstream_error", "message": err.to_string()}
    })
    .to_string();
    http::Response::builder()
        .status(StatusCode::BAD_GATEWAY)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .expect("bad gateway response build")
}

#[allow(clippy::too_many_arguments)]
fn sse_tee_response(
    status: StatusCode,
    headers: Vec<(HeaderName, HeaderValue)>,
    upstream: hyper::body::Incoming,
    current_model: String,
    original_model: String,
    writer: UsageWriter,
    key_name: String,
    request_body: String,
    ip: Option<String>,
    guard: ConcurrencyGuard,
) -> Response {
    let (sniffer_tx, sniffer_handle) = spawn_sniffer(SniffContext {
        writer,
        key_name,
        original_model: original_model.clone(),
        request_body,
        ip,
    });
    let mut splitter = ForwardSplitter::new(current_model.clone(), original_model);
    let mut upstream = upstream;
    let body_stream = stream! {
        let _guard = guard;
        let _handle = sniffer_handle;
        let _tx = sniffer_tx.clone();
        loop {
            match upstream.frame().await {
                Some(Ok(frame)) => {
                    if let Ok(data) = frame.into_data() {
                        try_send_to_sniffer(&sniffer_tx, data.clone());
                        for ev in splitter.ingest_chunk(data) {
                            yield Ok::<_, std::io::Error>(ev);
                        }
                    }
                }
                Some(Err(e)) => {
                    yield Err(std::io::Error::other(e.to_string()));
                    break;
                }
                None => break,
            }
        }
        if let Some(tail) = splitter.flush() {
            yield Ok(tail);
        }
        drop(sniffer_tx);
    };
    let body = Body::from_stream(body_stream);
    let builder = http::Response::builder().status(status);
    apply_headers(builder, headers)
        .body(body)
        .expect("sse response builder")
}

enum CollectError {
    OverCap(Bytes),
    Io(std::io::Error),
}

async fn collect_with_cap(
    mut body: hyper::body::Incoming,
    cap: usize,
) -> Result<Bytes, CollectError> {
    let mut buf = BytesMut::with_capacity(8 * 1024);
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|e| CollectError::Io(std::io::Error::other(e.to_string())))?;
        if let Ok(data) = frame.into_data() {
            if buf.len() + data.len() > cap {
                return Err(CollectError::OverCap(buf.freeze()));
            }
            buf.extend_from_slice(&data);
        }
    }
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::header::HeaderValue;

    #[test]
    fn build_headers_drops_hop_by_hop_keeps_connection() {
        let mut src = HeaderMap::new();
        src.insert(http::header::CONTENT_LENGTH, HeaderValue::from_static("99"));
        src.insert(http::header::CONNECTION, HeaderValue::from_static("keep-alive"));
        src.insert(http::header::TRANSFER_ENCODING, HeaderValue::from_static("chunked"));
        src.insert(
            HeaderName::from_static("cf-ray"),
            HeaderValue::from_static("abc-LAX"),
        );
        let out = build_response_headers(&src);
        let names: Vec<&str> = out.iter().map(|(n, _)| n.as_str()).collect();
        assert!(!names.contains(&"content-length"));
        assert!(!names.contains(&"transfer-encoding"));
        assert!(names.iter().any(|n| n.eq_ignore_ascii_case("connection")));
        let cf_ray = out
            .iter()
            .find(|(n, _)| n.as_str().eq_ignore_ascii_case("cf-ray"))
            .expect("cf-ray preserved");
        assert_eq!(cf_ray.1.to_str().unwrap(), "abc-LAX");
    }

    #[test]
    fn bad_gateway_returns_502_json() {
        let resp = bad_gateway_response(&AppError::Upstream("connection reset".into()));
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
        let ct = resp
            .headers()
            .get(http::header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(ct, "application/json");
    }
}
