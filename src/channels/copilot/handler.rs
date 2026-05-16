//! Copilot 渠道主流程编排。鉴权 / quota / 并发由共享中间件提前完成，handler 拿到 ChannelContext。
//! 主要职责：池选择 + 重试 + session token + 请求体改写 + 转发 + 响应清洗 + SSE 处理 + web search loop。

use crate::auth::KeyCacheEntry;
use crate::billing::{BillingRecord, ErrorLogRecord, UsageWriter};
use crate::channels::ChannelKind;
use crate::channels::copilot::breaker::Breaker;
use crate::channels::copilot::direct::DirectFlags;
use crate::channels::copilot::headers::build_upstream_headers;
use crate::channels::copilot::key_pool::UpstreamPool;
use crate::channels::copilot::ratelimit_headers::inject as inject_ratelimit;
use crate::channels::copilot::request_xform::{
    XformContext, strip_unsupported_params, transform_request_body,
};
use crate::channels::copilot::response_xform::{
    normalize_error_status, sanitize_error_message, sanitize_response_body,
};
use crate::channels::copilot::session_token::SessionTokenCache;
use crate::channels::copilot::sse::{SseProcessor, fallback_partial_usage};
use crate::channels::copilot::upstream_key::{CopilotPrefix, ParsedUpstreamKey, UpstreamConfig};
use crate::channels::copilot::web_search;
use crate::concurrency::ConcurrencyGuard;
use crate::config::Config;
use crate::error::{AppError, AppResult};
use crate::shared::ids::gen_response_request_id;
use bytes::Bytes;
use futures::StreamExt;
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use reqwest::Client;
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::warn;

/// 请求上下文：调用方（中间件）已经完成鉴权 + quota + 并发占位。
pub struct ChannelContext {
    pub method: http::Method,
    pub path: String,
    pub query: String,
    pub headers: HeaderMap,
    pub body: Bytes,
    pub key: KeyCacheEntry,
    pub upstream_cfg: UpstreamConfig,
    pub client_ip: String,
    /// 占住的并发位，handler 结束（流式则在流末尾）后 drop 释放。
    pub guard: Option<ConcurrencyGuard>,
}

pub struct CopilotHandler {
    pub http: Arc<Client>,
    pub pool: Arc<UpstreamPool>,
    pub breaker: Arc<Breaker>,
    pub session_token: Arc<SessionTokenCache>,
    pub usage_writer: UsageWriter,
    pub config: Arc<Config>,
}

const REWRITE_EXACT: &[(&str, &str)] = &[
    ("/v1/chat/completions", "/chat/completions"),
    ("/v1/models", "/models"),
    ("/v1/responses", "/responses"),
];
const REWRITE_PREFIX: &[(&str, &str)] = &[
    ("/v1/chat/completions/", "/chat/completions"),
    ("/v1/models/", "/models"),
    ("/v1/responses/", "/responses"),
];

/// `/v1/x` → `/x`，其它路径透传。
pub fn resolve_upstream_path(path: &str) -> String {
    for (k, v) in REWRITE_EXACT {
        if path == *k {
            return (*v).to_string();
        }
    }
    for (k, v) in REWRITE_PREFIX {
        if let Some(rest) = path.strip_prefix(k) {
            return format!("{v}/{rest}");
        }
    }
    path.to_string()
}

/// handler 出参：http 响应骨架（status / headers / body 形态）。
pub enum HandlerOutcome {
    Bytes {
        status: StatusCode,
        headers: HeaderMap,
        body: Bytes,
    },
    Stream {
        status: StatusCode,
        headers: HeaderMap,
        body: BoxedStream,
    },
}

pub type BoxedStream =
    Box<dyn futures::Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin>;

impl CopilotHandler {
    pub async fn handle(&self, ctx: ChannelContext) -> AppResult<HandlerOutcome> {
        let body_text = String::from_utf8(ctx.body.to_vec()).unwrap_or_default();
        let parsed_body: Option<Value> = if body_text.is_empty() {
            None
        } else {
            serde_json::from_str(&body_text).ok()
        };

        // 决定走 direct 还是 pool
        let direct_flags = if ctx.upstream_cfg.is_direct() {
            DirectFlags::PASS_THROUGH
        } else {
            DirectFlags::SHARED_POOL
        };

        // 池/直传 → 拿首个 ParsedUpstreamKey 与 upstream_id（pool 模式才有）
        let (mut current_parsed, mut current_upstream_id, allowed_ids): (
            Option<ParsedUpstreamKey>,
            Option<i64>,
            Option<Vec<i64>>,
        ) = match &ctx.upstream_cfg {
            UpstreamConfig::Direct(k) => (Some(k.clone()), None, None),
            UpstreamConfig::PoolAll => {
                self.pool.ensure_fresh().await?;
                let picked = self.pool.pick(None, &HashSet::new());
                match picked {
                    Some(p) => (Some(p.parsed), Some(p.id), None),
                    None => (None, None, None),
                }
            }
            UpstreamConfig::PoolFiltered(ids) => {
                self.pool.ensure_fresh().await?;
                let picked = self.pool.pick(Some(ids), &HashSet::new());
                match picked {
                    Some(p) => (Some(p.parsed), Some(p.id), Some(ids.clone())),
                    None => (None, None, Some(ids.clone())),
                }
            }
        };

        let mut request_body_value = parsed_body.clone();

        // web_search 检测：仅 /v1/messages 且有 EXA key 时启用
        let mut web_search_cfg = web_search::WebSearchConfig::default();
        if ctx.path == "/v1/messages" {
            if !self.config.exa_api_keys.is_empty() {
                if let Some(ref mut payload) = request_body_value {
                    web_search_cfg = web_search::detect_and_replace(payload);
                }
            }
        }

        // Opus 4.7 采样硬拒（在 transform 之前先验 — 与旧实现位置一致）
        if !direct_flags.direct {
            if let Some(payload) = &request_body_value {
                crate::channels::copilot::request_xform::opus47_rejects_sampling(payload)?;
            }
        }

        // 请求体改写
        let upstream_body: Bytes = match (&ctx.method, request_body_value.as_mut()) {
            (m, Some(v)) if *m != http::Method::GET && *m != http::Method::HEAD => {
                if direct_flags.direct {
                    strip_unsupported_params(v);
                } else {
                    transform_request_body(
                        v,
                        XformContext {
                            is_direct: false,
                            is_individual_base: current_parsed
                                .as_ref()
                                .map(|p| p.prefix == CopilotPrefix::Individual)
                                .unwrap_or(false),
                        },
                    )?;
                }
                Bytes::from(serde_json::to_vec(v)?)
            }
            (m, None) if *m == http::Method::GET || *m == http::Method::HEAD => Bytes::new(),
            _ => ctx.body.clone(),
        };

        let upstream_path = resolve_upstream_path(&ctx.path);
        let urlsearch = if ctx.query.is_empty() {
            String::new()
        } else {
            format!("?{}", ctx.query)
        };

        // 重试循环
        let max_retries = match direct_flags.direct {
            true => 1,
            false => self.pool.max_retries().max(1),
        };
        let is_stream_req = request_body_value
            .as_ref()
            .and_then(|v| v.get("stream"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let mut failed_ids: HashSet<i64> = HashSet::new();
        let mut upstream_resp: Option<reqwest::Response> = None;
        let mut last_parsed: Option<ParsedUpstreamKey> = current_parsed.clone();

        for attempt in 0..max_retries {
            let Some(parsed) = current_parsed.clone() else {
                break;
            };
            // ghu_/gho_ → session token
            let auth_token = if parsed.is_session_token_required() {
                match self.session_token.get_or_fetch(&parsed.token).await {
                    Ok(t) => t,
                    Err(e) => {
                        if !direct_flags.direct
                            && attempt + 1 < max_retries
                            && let Some(id) = current_upstream_id
                        {
                            failed_ids.insert(id);
                            let picked = self.pool.pick(allowed_ids.as_deref(), &failed_ids);
                            if let Some(p) = picked {
                                current_upstream_id = Some(p.id);
                                current_parsed = Some(p.parsed);
                                continue;
                            }
                        }
                        warn!(error = ?e, "session token exchange failed");
                        return Err(AppError::UpstreamConnect("session token exchange failed".into()));
                    }
                }
            } else {
                parsed.token.clone()
            };

            // individual base 模型映射：在已 transform 过的 body 上再做一次（直传或非 individual 不动）
            let final_body =
                maybe_remap_body_for_individual(&upstream_body, parsed.prefix);

            let target = format!(
                "{}{upstream_path}{urlsearch}",
                parsed.upstream_base()
            );
            let headers = build_upstream_headers(
                &auth_token,
                if upstream_body.is_empty() {
                    None
                } else {
                    Some("application/json")
                },
            );
            let timeout = if is_stream_req {
                self.config.copilot_upstream_timeout_stream
            } else {
                self.config.copilot_upstream_timeout_unary
            };

            let resp = self
                .http
                .request(reqwest_method(&ctx.method), &target)
                .headers(headers)
                .timeout(timeout)
                .body(final_body)
                .send()
                .await;
            let resp = match resp {
                Ok(r) => r,
                Err(e) => {
                    warn!(error = ?e, attempt, "upstream request error");
                    if !direct_flags.direct && attempt + 1 < max_retries {
                        if let Some(id) = current_upstream_id {
                            failed_ids.insert(id);
                        }
                        let picked = self.pool.pick(allowed_ids.as_deref(), &failed_ids);
                        if let Some(p) = picked {
                            current_upstream_id = Some(p.id);
                            current_parsed = Some(p.parsed);
                            continue;
                        }
                    }
                    return Err(AppError::UpstreamConnect("upstream request failed".into()));
                }
            };

            // pool 模式 + 401/403/429 切下一个
            let status = resp.status();
            let needs_swap = !direct_flags.direct
                && matches!(
                    status,
                    StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN | StatusCode::TOO_MANY_REQUESTS
                )
                && attempt + 1 < max_retries;
            if status == StatusCode::TOO_MANY_REQUESTS {
                crate::metrics::GLOBAL.record_upstream_429(crate::channels::ChannelKind::Copilot);
                if let Some(id) = current_upstream_id {
                    self.breaker.record_429(id);
                }
            }
            if needs_swap {
                if let Some(id) = current_upstream_id {
                    failed_ids.insert(id);
                }
                let picked = self.pool.pick(allowed_ids.as_deref(), &failed_ids);
                if let Some(p) = picked {
                    current_upstream_id = Some(p.id);
                    current_parsed = Some(p.parsed);
                    last_parsed = current_parsed.clone();
                    continue;
                }
            }
            upstream_resp = Some(resp);
            last_parsed = Some(parsed);
            break;
        }

        let Some(upstream_resp) = upstream_resp else {
            return Err(AppError::UpstreamConnect("all upstream retries failed".into()));
        };

        let status = upstream_resp.status();
        let upstream_headers = upstream_resp.headers().clone();
        let mut resp_headers = base_response_headers(&upstream_headers);

        // 4xx/5xx 错误分支
        if status.as_u16() >= 400 {
            let body_text = upstream_resp.text().await.unwrap_or_default();
            self.log_error(&ctx, status.as_u16(), &body_text, direct_flags);
            if direct_flags.direct {
                resp_headers.insert(
                    HeaderName::from_static("content-type"),
                    HeaderValue::from_static("application/json"),
                );
                return Ok(HandlerOutcome::Bytes {
                    status,
                    headers: resp_headers,
                    body: Bytes::from(body_text),
                });
            }
            let message = sanitize_error_message(&body_text, status.as_u16());
            let normalized = normalize_error_status(status.as_u16());
            let error_type = crate::shared::generic_errors::error_type(normalized);
            let request_id = gen_response_request_id();
            let body_json = serde_json::json!({
                "type": "error",
                "error": {"type": error_type, "message": message},
                "request_id": request_id,
            });
            resp_headers.insert(
                HeaderName::from_static("content-type"),
                HeaderValue::from_static("application/json"),
            );
            resp_headers.insert(
                HeaderName::from_static("request-id"),
                HeaderValue::from_str(&request_id).unwrap_or(HeaderValue::from_static("req_x")),
            );
            return Ok(HandlerOutcome::Bytes {
                status: StatusCode::from_u16(normalized).unwrap_or(StatusCode::BAD_GATEWAY),
                headers: resp_headers,
                body: Bytes::from(body_json.to_string()),
            });
        }

        // 2xx：流式 vs 非流式
        let is_event_stream = upstream_headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.contains("text/event-stream"))
            .unwrap_or(false);

        if is_event_stream {
            self.stream_through(
                ctx,
                upstream_resp,
                resp_headers,
                direct_flags,
                last_parsed.unwrap_or_else(|| current_parsed.clone().unwrap()),
            )
            .await
        } else {
            self.non_stream_through(
                ctx,
                upstream_resp,
                resp_headers,
                direct_flags,
                web_search_cfg,
                request_body_value,
                last_parsed.unwrap_or_else(|| current_parsed.clone().unwrap()),
                upstream_path,
                urlsearch,
            )
            .await
        }
    }

    async fn stream_through(
        &self,
        mut ctx: ChannelContext,
        resp: reqwest::Response,
        mut headers: HeaderMap,
        direct: DirectFlags,
        _parsed: ParsedUpstreamKey,
    ) -> AppResult<HandlerOutcome> {
        headers.insert(
            HeaderName::from_static("content-type"),
            HeaderValue::from_static("text/event-stream"),
        );
        headers.remove("content-length");

        let status = resp.status();
        let writer = self.usage_writer.clone();
        let key_name = ctx.key.name.clone();
        let request_body_text = String::from_utf8(ctx.body.to_vec()).unwrap_or_default();
        let client_ip = ctx.client_ip.clone();
        let guard = ctx.guard.take();

        let mut processor = SseProcessor::new(direct);
        let mut upstream_stream = resp.bytes_stream();

        let stream = async_stream::stream! {
            // guard 必须 move 进 stream，stream end 时 drop 才释放并发位
            let _guard = guard;
            loop {
                match upstream_stream.next().await {
                    Some(Ok(chunk)) => {
                        let out = processor.feed(&chunk);
                        if !out.is_empty() {
                            yield Ok::<_, std::io::Error>(out);
                        }
                    }
                    Some(Err(e)) => {
                        warn!(error = ?e, "upstream stream error");
                        break;
                    }
                    None => break,
                }
            }
            let tail = processor.finish();
            if !tail.is_empty() {
                yield Ok(tail);
            }

            // 计费：优先用 final_usage，否则 fallback partial
            if !direct.direct {
                let stats = processor.stats().clone();
                let model = stats
                    .stream_model
                    .clone()
                    .unwrap_or_else(|| "unknown".into());
                if let Some(final_u) = stats.final_usage.clone() {
                    writer.record(BillingRecord {
                        channel: ChannelKind::Copilot,
                        model,
                        key_name: key_name.clone(),
                        input_tokens: final_u.input_tokens,
                        output_tokens: final_u.output_tokens,
                        cache_creation_tokens: final_u.cache_creation_tokens,
                        cache_read_tokens: final_u.cache_read_tokens,
                        request_body: request_body_text.clone(),
                        ip: Some(client_ip.clone()),
                    });
                } else if let Some(partial) = fallback_partial_usage(&stats) {
                    writer.record(BillingRecord {
                        channel: ChannelKind::Copilot,
                        model,
                        key_name: key_name.clone(),
                        input_tokens: partial.input_tokens,
                        output_tokens: partial.output_tokens,
                        cache_creation_tokens: partial.cache_creation_tokens,
                        cache_read_tokens: partial.cache_read_tokens,
                        request_body: request_body_text.clone(),
                        ip: Some(client_ip.clone()),
                    });
                }
            }
        };

        let boxed: BoxedStream = Box::new(Box::pin(stream));
        Ok(HandlerOutcome::Stream {
            status,
            headers,
            body: boxed,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn non_stream_through(
        &self,
        ctx: ChannelContext,
        resp: reqwest::Response,
        mut headers: HeaderMap,
        direct: DirectFlags,
        web_cfg: web_search::WebSearchConfig,
        parsed_body: Option<Value>,
        parsed_key: ParsedUpstreamKey,
        upstream_path: String,
        urlsearch: String,
    ) -> AppResult<HandlerOutcome> {
        let status = resp.status();
        let body_text = resp.text().await.unwrap_or_default();

        // 尝试 parse JSON；失败 → 502
        let mut json: Value = match serde_json::from_str(&body_text) {
            Ok(v) => v,
            Err(_) => {
                headers.insert(
                    HeaderName::from_static("content-type"),
                    HeaderValue::from_static("application/json"),
                );
                return Ok(HandlerOutcome::Bytes {
                    status: StatusCode::BAD_GATEWAY,
                    headers,
                    body: Bytes::from(
                        serde_json::json!({
                            "type": "error",
                            "error": {"type": "api_error", "message": "unexpected response format"}
                        })
                        .to_string(),
                    ),
                });
            }
        };

        // Web Search loop
        if web_cfg.active && json.get("stop_reason").and_then(|v| v.as_str()) == Some("tool_use") {
            if let Some(exa_key) = web_search::pick_exa_key(&self.config.exa_api_keys) {
                let parsed_body_value = parsed_body.unwrap_or(Value::Null);
                let outcome = web_search::run_loop(web_search::LoopInputs {
                    http: self.http.clone(),
                    session_token: self.session_token.clone(),
                    exa_api_key: exa_key,
                    upstream_base: parsed_key.upstream_base(),
                    upstream_token: &parsed_key.token,
                    upstream_path: &upstream_path,
                    upstream_query: &urlsearch,
                    prefix: parsed_key.prefix,
                    config: web_cfg.clone(),
                    parsed_body: parsed_body_value,
                    initial_response: json.clone(),
                })
                .await?;

                let model = outcome
                    .final_response
                    .get("model")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                if !direct.direct {
                    record_usage_for_billing(
                        &self.usage_writer,
                        &outcome.accumulated_usage,
                        &model,
                        &ctx,
                    );
                }

                let mut final_resp = outcome.final_response;
                if !direct.skip_response_sanitize() {
                    let is_fast = model.to_ascii_lowercase().contains("fast");
                    sanitize_response_body(&mut final_resp, is_fast);
                }

                // 客户端原本要 stream → 合成 SSE 回放
                if web_cfg.original_stream {
                    headers.insert(
                        HeaderName::from_static("content-type"),
                        HeaderValue::from_static("text/event-stream"),
                    );
                    headers.remove("content-length");
                    headers.remove("transfer-encoding");
                    let synth = web_search::synthesize_sse(&final_resp);
                    return Ok(HandlerOutcome::Bytes {
                        status: StatusCode::OK,
                        headers,
                        body: Bytes::from(synth),
                    });
                }

                let body = serde_json::to_vec(&final_resp)?;
                headers.insert(
                    HeaderName::from_static("content-type"),
                    HeaderValue::from_static("application/json"),
                );
                set_content_length(&mut headers, body.len());
                headers.remove("transfer-encoding");
                return Ok(HandlerOutcome::Bytes {
                    status: StatusCode::OK,
                    headers,
                    body: Bytes::from(body),
                });
            }
        }

        // 普通 non-stream：清洗 + usage 计费
        let model = json
            .get("model")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        if !direct.direct {
            if let Some(usage) = json.get("usage").cloned() {
                record_usage_for_billing(&self.usage_writer, &usage, &model, &ctx);
            }
        }
        if !direct.skip_response_sanitize() {
            let is_fast = model.to_ascii_lowercase().contains("fast");
            sanitize_response_body(&mut json, is_fast);
        }
        let body = serde_json::to_vec(&json)?;
        headers.insert(
            HeaderName::from_static("content-type"),
            HeaderValue::from_static("application/json"),
        );
        set_content_length(&mut headers, body.len());
        headers.remove("transfer-encoding");
        Ok(HandlerOutcome::Bytes {
            status,
            headers,
            body: Bytes::from(body),
        })
    }

    fn log_error(&self, ctx: &ChannelContext, status: u16, body_text: &str, direct: DirectFlags) {
        let model = if direct.omit_error_model() {
            String::new()
        } else {
            ctx.body_as_json()
                .and_then(|v| {
                    v.get("model").and_then(|m| m.as_str()).map(|s| s.to_string())
                })
                .unwrap_or_default()
        };
        self.usage_writer.record_error(ErrorLogRecord {
            channel: ChannelKind::Copilot,
            key_name: ctx.key.name.clone(),
            status,
            path: ctx.path.clone(),
            model,
            request_body: String::from_utf8(ctx.body.to_vec()).unwrap_or_default(),
            response_body: body_text.to_string(),
            ip: ctx.client_ip.clone(),
        });
    }
}

impl ChannelContext {
    fn body_as_json(&self) -> Option<Value> {
        serde_json::from_slice(&self.body).ok()
    }
}

fn maybe_remap_body_for_individual(body: &Bytes, prefix: CopilotPrefix) -> Bytes {
    if prefix != CopilotPrefix::Individual || body.is_empty() {
        return body.clone();
    }
    let Ok(text) = std::str::from_utf8(body) else {
        return body.clone();
    };
    if !(text.contains("\"top_p\"")
        || text.contains("\"top_k\"")
        || text.contains("\"context_management\"")
        || text.contains("claude-opus-4-6")
        || text.contains("claude-sonnet-4-6"))
    {
        return body.clone();
    }
    let Ok(mut value): Result<Value, _> = serde_json::from_str(text) else {
        return body.clone();
    };
    let mut changed = strip_unsupported_params(&mut value);
    if crate::channels::copilot::request_xform::remap_individual_model(&mut value) {
        changed = true;
    }
    if !changed {
        return body.clone();
    }
    Bytes::from(serde_json::to_vec(&value).unwrap_or_default())
}

fn record_usage_for_billing(
    writer: &UsageWriter,
    usage: &Value,
    model: &str,
    ctx: &ChannelContext,
) {
    fn token(v: &Value, key: &str) -> u64 {
        v.get(key).and_then(|x| x.as_u64()).unwrap_or(0)
    }
    writer.record(BillingRecord {
        channel: ChannelKind::Copilot,
        model: model.to_string(),
        key_name: ctx.key.name.clone(),
        input_tokens: token(usage, "input_tokens"),
        output_tokens: token(usage, "output_tokens"),
        cache_creation_tokens: token(usage, "cache_creation_input_tokens"),
        cache_read_tokens: token(usage, "cache_read_input_tokens"),
        request_body: String::from_utf8(ctx.body.to_vec()).unwrap_or_default(),
        ip: Some(ctx.client_ip.clone()),
    });
}

fn base_response_headers(upstream: &HeaderMap) -> HeaderMap {
    let mut out = HeaderMap::new();
    const WHITELIST: &[&str] = &[
        "content-type",
        "cache-control",
        "vary",
        "connection",
    ];
    for name in WHITELIST {
        if let Some(v) = upstream.get(*name) {
            out.insert(HeaderName::from_static(name), v.clone());
        }
    }
    out.insert(
        HeaderName::from_static("access-control-allow-origin"),
        HeaderValue::from_static("*"),
    );
    out.insert(
        HeaderName::from_static("access-control-expose-headers"),
        HeaderValue::from_static("request-id, anthropic-ratelimit-requests-limit, anthropic-ratelimit-requests-remaining, anthropic-ratelimit-requests-reset, anthropic-ratelimit-tokens-limit, anthropic-ratelimit-tokens-remaining, anthropic-ratelimit-tokens-reset"),
    );
    inject_ratelimit(&mut out);
    out.insert(
        HeaderName::from_static("request-id"),
        HeaderValue::from_str(&gen_response_request_id())
            .unwrap_or(HeaderValue::from_static("req_x")),
    );
    out
}

fn set_content_length(headers: &mut HeaderMap, len: usize) {
    if let Ok(v) = HeaderValue::from_str(&len.to_string()) {
        headers.insert(HeaderName::from_static("content-length"), v);
    }
}

fn reqwest_method(m: &http::Method) -> reqwest::Method {
    match *m {
        http::Method::GET => reqwest::Method::GET,
        http::Method::POST => reqwest::Method::POST,
        http::Method::PUT => reqwest::Method::PUT,
        http::Method::DELETE => reqwest::Method::DELETE,
        http::Method::PATCH => reqwest::Method::PATCH,
        http::Method::OPTIONS => reqwest::Method::OPTIONS,
        http::Method::HEAD => reqwest::Method::HEAD,
        _ => reqwest::Method::POST,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_path_exact_matches() {
        assert_eq!(resolve_upstream_path("/v1/chat/completions"), "/chat/completions");
        assert_eq!(resolve_upstream_path("/v1/models"), "/models");
        assert_eq!(resolve_upstream_path("/v1/responses"), "/responses");
    }

    #[test]
    fn resolve_path_prefix_keeps_tail() {
        assert_eq!(
            resolve_upstream_path("/v1/chat/completions/foo"),
            "/chat/completions/foo"
        );
        assert_eq!(resolve_upstream_path("/v1/models/x"), "/models/x");
    }

    #[test]
    fn resolve_path_unknown_passthrough() {
        assert_eq!(resolve_upstream_path("/v1/messages"), "/v1/messages");
        assert_eq!(resolve_upstream_path("/healthz"), "/healthz");
    }

    #[test]
    fn maybe_remap_individual_rewrites_dot_form() {
        let body = Bytes::from(r#"{"model":"claude-opus-4-6","top_p":0.9}"#);
        let out = maybe_remap_body_for_individual(&body, CopilotPrefix::Individual);
        let s = std::str::from_utf8(&out).unwrap();
        assert!(s.contains("claude-opus-4.6"));
        assert!(!s.contains("top_p"));
    }

    #[test]
    fn maybe_remap_non_individual_unchanged() {
        let body = Bytes::from(r#"{"model":"claude-opus-4-6"}"#);
        let out = maybe_remap_body_for_individual(&body, CopilotPrefix::Enterprise);
        assert_eq!(out, body);
    }

}
