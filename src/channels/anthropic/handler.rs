//! Anthropic 渠道主流程：拿 key_pool key → 上游 forward → 四象限响应路由
//! (is_sse × is_gzip) → 字节透传 / gzip 重压 / SSE tee 计费。
//! 返 ``hyper::Response<BoxBody>`` 绕开 axum 的 ``IntoResponse``，把上游
//! ``Response.extensions``（含 hyper 私有 HeaderCaseMap）整体 forward 给共享 server。

use crate::auth::KeyCacheEntry;
use crate::billing::{ErrorLogRecord, UsageWriter};
use crate::channels::anthropic::billing_hook::record_non_sse_usage;
use crate::channels::ChannelKind;
use crate::channels::anthropic::gzip_passthrough::{decompress_gzip, rewrite_gzip};
use crate::channels::anthropic::header_case::canonicalize;
use crate::channels::anthropic::key_pool::{KeyPool, classify_status, pool_empty_error};
use crate::channels::anthropic::model_restore::rewrite_json_response;
use crate::channels::anthropic::model_splice::{RewriteRule, extract_client_model, rewrite_body};
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
use tracing::warn;

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
    let client_model = extract_client_model(&req.body, &content_type);
    // 在 rewrite 改字节前保存客户端原始 body 用于计费/审计日志，超 256KB 截断避免内存膨胀。
    let request_body_for_log = body_for_log(&req.body);
    let outcome = rewrite_body(req.body, &content_type, &ctx.rewrite_rules);
    let rewritten_marker = outcome.rewritten();
    let original_model = outcome.original_model.clone();
    let new_model = outcome.new_model.clone();
    // rewritten=true 时 original_model = 客户端 model；rewritten=false 时来自上面 extract。
    // 永远拿得到客户端实际 model 名做计费兜底。
    let billing_model_fallback = original_model
        .clone()
        .or_else(|| client_model.clone())
        .unwrap_or_else(|| "unknown".to_string());

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
    if status.as_u16() == 429 {
        crate::metrics::GLOBAL.record_upstream_429(ChannelKind::Anthropic);
    }
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

    // SSE 路径无条件走 sse_tee：sniffer 段 always 抓 message_delta.usage 落 BillingRecord；
    // splice 段按 rewritten 决定（None 时事件按字节透传不动）。计费与改字节解耦。
    // gzip+SSE 极少见（Anthropic 默认 SSE 不带 gzip），暂走下面 buffered 路径兜底。
    if is_sse && !is_gzip {
        let splice = match (rewritten_marker, new_model.clone(), original_model.clone()) {
            (true, Some(c), Some(o)) => Some((c, o)),
            _ => None,
        };
        return Ok(sse_tee_response(
            status,
            prepared_headers,
            resp_extensions,
            body,
            splice,
            ctx.usage_writer,
            ctx.key_cache_entry.name.clone(),
            billing_model_fallback.clone(),
            request_body_for_log,
            ctx.client_ip.clone(),
            ctx.concurrency_guard,
        ));
    }

    // Non-SSE：无论 rewritten 都 buffer + 计费/错误日志，再决定是否 splice。
    // 计费语义是"用户消耗了多少 token"，与代理是否改字节无关。
    // gzip+SSE 落到本分支：sse_tee 拿不到行级 usage（buffered 后只能整体 parse JSON）。
    // Anthropic 实测不返 gzip+SSE，触发即可观测异常上游配置。
    if is_sse && is_gzip {
        warn!(
            key = %ctx.key_cache_entry.name,
            "upstream returned SSE+gzip; sse_tee billing path skipped, falling back to buffered no-bill"
        );
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

    let billing_model = billing_model_fallback.clone();

    // gzip 路径明文需要解压一次给计费 parse；下面 rewrite_gzip 还会独立再解一次自己做改写。
    // 解开放在不同模块各自维护失败兜底逻辑（rewrite_gzip 内部畸形 gzip 返原 raw，
    // 这里 decompress_gzip 失败时 caller 直接走 fallback），双重解压代价 < 100KB 响应 < 1ms。
    match classify_billing_action(is_json, status, rewritten_marker) {
        BillingAction::RecordUsage => {
            let plain = if is_gzip { decompress_gzip(&buffered) } else { Some(buffered.clone()) };
            if let Some(plain) = plain {
                record_non_sse_usage(
                    &ctx.usage_writer,
                    &plain,
                    &ctx.key_cache_entry.name,
                    &billing_model,
                    request_body_for_log.clone(),
                    ctx.client_ip.clone(),
                );
            }
        }
        BillingAction::RecordError => {
            let plain = if is_gzip { decompress_gzip(&buffered) } else { Some(buffered.clone()) };
            let response_body = plain
                .as_deref()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .unwrap_or_default();
            ctx.usage_writer.record_error(ErrorLogRecord {
                channel: ChannelKind::Anthropic,
                key_name: ctx.key_cache_entry.name.clone(),
                status: status.as_u16(),
                path: req.path.clone(),
                model: billing_model.clone(),
                request_body: request_body_for_log.clone(),
                response_body,
                ip: ctx.client_ip.clone().unwrap_or_default(),
            });
        }
        BillingAction::Skip => {}
    }

    drop(ctx.concurrency_guard);

    let final_body = match (rewritten_marker, new_model, original_model) {
        (true, Some(current), Some(original)) if is_gzip => {
            rewrite_gzip(buffered, &current, &original, is_sse)
        }
        (true, Some(current), Some(original)) if is_json => {
            rewrite_json_response(buffered, &current, &original)
        }
        _ => buffered,
    };

    Ok(forward_buffered(
        status,
        prepared_headers,
        resp_extensions,
        final_body,
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
    splice: Option<(String, String)>,
    writer: UsageWriter,
    key_name: String,
    billing_model_fallback: String,
    request_body: String,
    ip: Option<String>,
    guard: ConcurrencyGuard,
) -> ProxyResponse {
    let log_key_name = key_name.clone();
    let log_model = billing_model_fallback.clone();
    let (sniffer_tx, sniffer_handle) = spawn_sniffer(SniffContext {
        writer,
        key_name,
        original_model: billing_model_fallback,
        request_body,
        ip,
    });
    let mut splitter = ForwardSplitter::new(splice);
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

/// 决定一条 non-SSE JSON 响应应该计 usage、记错误日志、还是不计费。
/// 与 rewritten_marker 解耦——计费看 user → token 消耗，不看代理是否改字节。
/// rewritten=false 也必须返回 RecordUsage 走 record_non_sse_usage。
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum BillingAction {
    RecordUsage,
    RecordError,
    Skip,
}

pub(crate) fn classify_billing_action(
    is_json: bool,
    status: StatusCode,
    _rewritten: bool,
) -> BillingAction {
    if !is_json {
        return BillingAction::Skip;
    }
    if status.is_success() {
        BillingAction::RecordUsage
    } else {
        BillingAction::RecordError
    }
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

/// 请求体日志最大保留字节数，超出截断尾部。和 anthropic 单条响应 buffer 上限保持同数量级，
/// 防止超大 prompt 撑爆 PG 单行存储。
pub const REQUEST_BODY_LOG_LIMIT: usize = 256 * 1024;

fn body_for_log(bytes: &Bytes) -> String {
    let slice = if bytes.len() > REQUEST_BODY_LOG_LIMIT {
        &bytes[..REQUEST_BODY_LOG_LIMIT]
    } else {
        &bytes[..]
    };
    String::from_utf8_lossy(slice).into_owned()
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

    #[test]
    fn non_sse_records_usage_even_when_no_rewrite_rule_matches() {
        // 同一 status / is_json，rewritten true 或 false 必须落 RecordUsage。
        // 这条防退化锁死：trade-off A（rewritten=false 不计费）一旦回归就立即失败。
        let with_rewrite = classify_billing_action(true, StatusCode::OK, true);
        let without_rewrite = classify_billing_action(true, StatusCode::OK, false);
        assert_eq!(with_rewrite, BillingAction::RecordUsage);
        assert_eq!(without_rewrite, BillingAction::RecordUsage);
    }

    #[test]
    fn classify_skips_non_json() {
        assert_eq!(
            classify_billing_action(false, StatusCode::OK, true),
            BillingAction::Skip
        );
        assert_eq!(
            classify_billing_action(false, StatusCode::INTERNAL_SERVER_ERROR, false),
            BillingAction::Skip
        );
    }

    #[test]
    fn classify_error_status_records_error() {
        for st in [
            StatusCode::BAD_REQUEST,
            StatusCode::UNAUTHORIZED,
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::BAD_GATEWAY,
        ] {
            assert_eq!(
                classify_billing_action(true, st, true),
                BillingAction::RecordError,
                "status={st:?}"
            );
            assert_eq!(
                classify_billing_action(true, st, false),
                BillingAction::RecordError,
                "rewritten=false status={st:?}"
            );
        }
    }
}
