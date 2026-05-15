//! Anthropic 渠道主流程：拿 key_pool key → 上游 forward → 四象限响应路由
//! (is_sse × is_gzip) → 字节透传 / gzip 重压 / SSE tee 计费。
//! 返 ``hyper::Response<BoxBody>`` 绕开 axum 的 ``IntoResponse``，把上游
//! ``Response.extensions``（含 hyper 私有 HeaderCaseMap）整体 forward 给共享 server。

use crate::auth::KeyCacheEntry;
use crate::billing::UsageWriter;
use crate::channels::anthropic::billing_hook::record_non_sse_usage;
use crate::channels::anthropic::gzip_passthrough::{decompress_gzip, rewrite_gzip};
use crate::channels::anthropic::header_case::canonicalize;
use crate::channels::anthropic::key_pool::{KeyPool, classify_status, pool_empty_error};
use crate::channels::anthropic::model_restore::rewrite_json_response;
use crate::channels::anthropic::model_splice::{RewriteRule, rewrite_body};
use crate::channels::anthropic::request_strip::is_response_hop_by_hop;
use crate::channels::anthropic::sse_tee::{
    ForwardSplitter, SniffContext, spawn_sniffer, try_send_to_sniffer,
};
use crate::channels::anthropic::upstream_client::AnthropicUpstreamClient;
use crate::concurrency::ConcurrencyGuard;
use crate::error::{AppError, AppResult};
use async_stream::stream;
use bytes::{Bytes, BytesMut};
use http::{Extensions, HeaderMap, HeaderName, HeaderValue, Method, Response, StatusCode};
use http_body::Frame;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full, StreamBody};
use std::sync::Arc;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
pub type ProxyBody = BoxBody<Bytes, BoxError>;
pub type ProxyResponse = Response<ProxyBody>;

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

/// 单次请求入参。共享 server 层 axum extractor / 直连 hyper handler 都能填充。
pub struct ProxyRequest {
    pub method: Method,
    pub path: String,
    pub raw_query: Option<String>,
    pub headers: HeaderMap,
    pub body: Bytes,
}

/// 主入口。返回 ``hyper::Response<BoxBody>``，body 是流或 buffered；
/// extensions 整体复制自上游响应，让共享 server 的 preserve_header_case 能读到 HeaderCaseMap。
pub async fn handle(ctx: HandlerContext, req: ProxyRequest) -> AppResult<ProxyResponse> {
    let content_type = req
        .headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let outcome = rewrite_body(req.body, &content_type, &ctx.rewrite_rules);
    let request_body_for_log = String::new();
    let rewritten_marker = outcome.rewritten();
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
    let resp_extensions = parts.extensions;
    let content_type_resp = lower_header_str(&resp_headers, http::header::CONTENT_TYPE);
    let content_encoding = lower_header_str(&resp_headers, http::header::CONTENT_ENCODING);
    let is_sse = content_type_resp.contains("text/event-stream");
    let is_json = content_type_resp.contains("application/json");
    let is_gzip = content_encoding.contains("gzip");

    let prepared_headers = build_response_headers(&resp_headers);

    if !rewritten_marker {
        return Ok(forward_body_as_is(
            status,
            prepared_headers,
            resp_extensions,
            body,
        ));
    }

    let (current_model, original_model) = match (new_model, original_model) {
        (Some(c), Some(o)) => (c, o),
        _ => {
            return Ok(forward_body_as_is(
                status,
                prepared_headers,
                resp_extensions,
                body,
            ));
        }
    };

    if is_sse && !is_gzip {
        return Ok(sse_tee_response(
            status,
            prepared_headers,
            resp_extensions,
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
            return Ok(forward_buffered(
                status,
                prepared_headers,
                resp_extensions,
                stream_pass,
            ));
        }
        Err(CollectError::Io(e)) => {
            return Err(AppError::Upstream(format!("read upstream body: {e}")));
        }
    };

    if is_json && status.is_success() {
        let plain_for_billing = if is_gzip {
            decompress_gzip(&buffered)
        } else {
            Some(buffered.clone())
        };
        if let Some(plain) = plain_for_billing {
            record_non_sse_usage(
                &ctx.usage_writer,
                &plain,
                &ctx.key_cache_entry.name,
                &original_model,
                request_body_for_log,
                ctx.client_ip.clone(),
            );
        }
    }

    drop(ctx.concurrency_guard);

    let rewritten = if is_gzip {
        rewrite_gzip(buffered, &current_model, &original_model, is_sse)
    } else if is_json {
        rewrite_json_response(buffered, &current_model, &original_model)
    } else {
        buffered
    };

    Ok(forward_buffered(
        status,
        prepared_headers,
        resp_extensions,
        rewritten,
    ))
}

fn lower_header_str(headers: &HeaderMap, name: http::HeaderName) -> String {
    headers
        .get(&name)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase()
}

/// 剥 hop-by-hop 后构造 (name, value) 列表，name 走 canonical case 表查一次。
/// ``HeaderName::from_bytes`` 内部仍把 buf 小写化，wire 大小写真正来自上游 extensions 里的
/// HeaderCaseMap，server preserve_header_case 启用后即可保真。
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

fn assemble_response<B>(
    status: StatusCode,
    headers: Vec<(HeaderName, HeaderValue)>,
    extensions: Extensions,
    body: B,
) -> Response<B> {
    let mut builder = Response::builder().status(status);
    for (name, value) in headers {
        builder = builder.header(name, value);
    }
    let mut resp = builder.body(body).expect("response builder valid parts");
    *resp.extensions_mut() = extensions;
    resp
}

fn full_body(bytes: Bytes) -> ProxyBody {
    Full::new(bytes).map_err(|e| Box::new(e) as BoxError).boxed()
}

fn forward_body_as_is(
    status: StatusCode,
    headers: Vec<(HeaderName, HeaderValue)>,
    extensions: Extensions,
    body: hyper::body::Incoming,
) -> ProxyResponse {
    let boxed = body.map_err(|e| Box::new(e) as BoxError).boxed();
    assemble_response(status, headers, extensions, boxed)
}

fn forward_buffered(
    status: StatusCode,
    headers: Vec<(HeaderName, HeaderValue)>,
    extensions: Extensions,
    body: Bytes,
) -> ProxyResponse {
    assemble_response(status, headers, extensions, full_body(body))
}

fn bad_gateway_response(err: &AppError) -> ProxyResponse {
    let body = serde_json::json!({
        "error": {"type": "upstream_error", "message": err.to_string()}
    })
    .to_string();
    Response::builder()
        .status(StatusCode::BAD_GATEWAY)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(full_body(Bytes::from(body)))
        .expect("bad gateway response build")
}

#[allow(clippy::too_many_arguments)]
fn sse_tee_response(
    status: StatusCode,
    headers: Vec<(HeaderName, HeaderValue)>,
    extensions: Extensions,
    upstream: hyper::body::Incoming,
    current_model: String,
    original_model: String,
    writer: UsageWriter,
    key_name: String,
    request_body: String,
    ip: Option<String>,
    guard: ConcurrencyGuard,
) -> ProxyResponse {
    let log_key_name = key_name.clone();
    let log_model = original_model.clone();
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
        loop {
            match upstream.frame().await {
                Some(Ok(frame)) => {
                    if let Ok(data) = frame.into_data() {
                        try_send_to_sniffer(&sniffer_tx, data.clone(), &log_key_name, &log_model);
                        for ev in splitter.ingest_chunk(data) {
                            yield Ok::<Frame<Bytes>, BoxError>(Frame::data(ev));
                        }
                    }
                }
                Some(Err(e)) => {
                    yield Err(Box::new(e) as BoxError);
                    break;
                }
                None => break,
            }
        }
        if let Some(tail) = splitter.flush() {
            yield Ok(Frame::data(tail));
        }
        drop(sniffer_tx);
    };
    let body = StreamBody::new(body_stream).boxed();
    assemble_response(status, headers, extensions, body)
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

    #[test]
    fn assemble_preserves_extensions() {
        #[derive(Clone, Debug, PartialEq, Eq)]
        struct Marker(u32);
        let mut ext = Extensions::new();
        ext.insert(Marker(7));
        let body = full_body(Bytes::from_static(b"x"));
        let resp = assemble_response(StatusCode::OK, vec![], ext, body);
        assert_eq!(resp.extensions().get::<Marker>(), Some(&Marker(7)));
    }
}
