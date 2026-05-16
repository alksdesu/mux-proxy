//! 透明 web_search：客户端发 Anthropic 风格 tool，代理转成 __proxy_web_search 函数 tool，
//! 调 Exa 搜，loop 喂回上游直到 stop_reason != tool_use。流式请求最后用 synthesize_sse 合成回放。

use crate::channels::copilot::headers::build_upstream_headers;
use crate::channels::copilot::model_map::forward as model_forward;
use crate::channels::copilot::session_token::SessionTokenCache;
use crate::channels::copilot::upstream_key::CopilotPrefix;
use crate::error::AppResult;
use crate::shared::ids::gen_srvtoolu_id;
use crate::shared::json::{as_array_mut, as_object_mut};
use base64::Engine;
use reqwest::Client;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::sync::Arc;
use std::time::Duration;

pub const WEB_SEARCH_TOOL_NAME: &str = "__proxy_web_search";
pub const WEB_SEARCH_MAX_LOOP: u32 = 5;
pub const EXA_SEARCH_TIMEOUT: Duration = Duration::from_secs(15);
pub const FOLLOW_UP_TIMEOUT: Duration = Duration::from_secs(120);
pub const SEARCH_USD_COST: f64 = 0.01;
const EXA_SEARCH_URL: &str = "https://api.exa.ai/search";
const EXA_NUM_RESULTS: u32 = 5;
const EXA_HIGHLIGHTS_SENTENCES: u32 = 5;

/// 客户端原始 web_search tool 类型集合（Anthropic 三代）
pub const WEB_SEARCH_TOOL_TYPES: &[&str] = &[
    "web_search_20250305",
    "web_search_20260209",
    "web_search",
];

#[derive(Clone, Debug)]
pub struct WebSearchConfig {
    pub active: bool,
    pub max_uses: u32,
    /// 客户端原始 stream 标记：true 时最终需要 synthesize_sse 回放
    pub original_stream: bool,
    pub allowed_domains: Option<Vec<String>>,
    pub blocked_domains: Option<Vec<String>>,
}

impl Default for WebSearchConfig {
    fn default() -> Self {
        Self {
            active: false,
            max_uses: 5,
            original_stream: false,
            allowed_domains: None,
            blocked_domains: None,
        }
    }
}

/// 在请求体里寻找首个 web_search 工具，替换为 __proxy_web_search function tool；
/// 同时强制 `stream=false`（内部走 non-stream，再合成 SSE 回放给客户端）。
pub fn detect_and_replace(payload: &mut Value) -> WebSearchConfig {
    let mut cfg = WebSearchConfig::default();
    let Some(map) = as_object_mut(payload) else {
        return cfg;
    };
    let original_stream = map.get("stream").and_then(|v| v.as_bool()).unwrap_or(false);

    let Some(tools) = map.get_mut("tools").and_then(|v| as_array_mut(v)) else {
        return cfg;
    };

    let mut hit_index: Option<usize> = None;
    let mut found_max_uses = 5_u32;
    let mut allowed: Option<Vec<String>> = None;
    let mut blocked: Option<Vec<String>> = None;

    for (i, tool) in tools.iter().enumerate() {
        let Some(t) = tool.as_object() else { continue };
        let Some(ty) = t.get("type").and_then(|v| v.as_str()) else {
            continue;
        };
        if WEB_SEARCH_TOOL_TYPES.contains(&ty) {
            if let Some(mu) = t.get("max_uses").and_then(|v| v.as_u64()) {
                found_max_uses = mu as u32;
            }
            allowed = extract_string_array(t.get("allowed_domains"));
            blocked = extract_string_array(t.get("blocked_domains"));
            hit_index = Some(i);
            break;
        }
    }

    let Some(idx) = hit_index else {
        return cfg;
    };

    tools[idx] = function_tool_value();
    // 删除剩余的同类型 tool（自后向前避免索引位移）
    let mut j = tools.len();
    while j > idx + 1 {
        j -= 1;
        let drop_it = tools[j]
            .as_object()
            .and_then(|t| t.get("type"))
            .and_then(|v| v.as_str())
            .map(|s| WEB_SEARCH_TOOL_TYPES.contains(&s))
            .unwrap_or(false);
        if drop_it {
            tools.remove(j);
        }
    }

    // 同步 tool_choice
    if let Some(tc) = map.get_mut("tool_choice").and_then(|v| as_object_mut(v)) {
        if tc.get("name").and_then(|v| v.as_str()) == Some("web_search") {
            tc.insert("name".into(), Value::String(WEB_SEARCH_TOOL_NAME.into()));
        }
    }

    // 强制内部 non-stream
    map.insert("stream".into(), Value::Bool(false));

    cfg.active = true;
    cfg.max_uses = found_max_uses;
    cfg.original_stream = original_stream;
    cfg.allowed_domains = allowed;
    cfg.blocked_domains = blocked;
    cfg
}

fn extract_string_array(v: Option<&Value>) -> Option<Vec<String>> {
    let arr = v?.as_array()?;
    let items: Vec<String> = arr
        .iter()
        .filter_map(|x| x.as_str().map(|s| s.to_string()))
        .collect();
    if items.is_empty() { None } else { Some(items) }
}

fn function_tool_value() -> Value {
    json!({
        "name": WEB_SEARCH_TOOL_NAME,
        "description": "Search the web for current information. Use this when you need up-to-date information beyond your knowledge cutoff. Return relevant results with titles, URLs, and content excerpts.",
        "input_schema": {
            "type": "object",
            "properties": { "query": { "type": "string", "description": "The search query" } },
            "required": ["query"],
        },
    })
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExaResult {
    #[serde(default)]
    pub title: String,
    pub url: String,
    #[serde(default, rename = "publishedDate")]
    pub published_date: Option<String>,
    #[serde(default)]
    pub highlights: Option<Vec<String>>,
    #[serde(default)]
    pub text: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ExaResponse {
    #[serde(default)]
    results: Vec<ExaResult>,
}

/// 调用 Exa /search 一次。失败返回空 vec（不报 error 让 loop 兜底成 unavailable）。
pub async fn call_exa(
    http: &Client,
    query: &str,
    api_key: &str,
    allowed: Option<&[String]>,
    blocked: Option<&[String]>,
) -> Vec<ExaResult> {
    let mut body = serde_json::Map::new();
    body.insert("query".into(), Value::String(query.into()));
    body.insert("type".into(), Value::String("auto".into()));
    body.insert(
        "numResults".into(),
        Value::Number(EXA_NUM_RESULTS.into()),
    );
    body.insert(
        "contents".into(),
        json!({"highlights": {"numSentences": EXA_HIGHLIGHTS_SENTENCES}}),
    );
    if let Some(allowed) = allowed {
        if !allowed.is_empty() {
            body.insert("includeDomains".into(), Value::Array(strings_to_json(allowed)));
        }
    }
    if let Some(blocked) = blocked {
        if !blocked.is_empty() {
            body.insert("excludeDomains".into(), Value::Array(strings_to_json(blocked)));
        }
    }

    let req = http
        .post(EXA_SEARCH_URL)
        .header("Content-Type", "application/json")
        .header("x-api-key", api_key)
        .json(&Value::Object(body))
        .timeout(EXA_SEARCH_TIMEOUT);

    let Ok(resp) = req.send().await else {
        return Vec::new();
    };
    if !resp.status().is_success() {
        return Vec::new();
    }
    let parsed: Result<ExaResponse, _> = resp.json().await;
    match parsed {
        Ok(r) => r.results,
        Err(_) => Vec::new(),
    }
}

fn strings_to_json(v: &[String]) -> Vec<Value> {
    v.iter().map(|s| Value::String(s.clone())).collect()
}

/// Exa 结果 → 发给客户端的 server_tool_use + web_search_tool_result 块对。
pub fn build_client_blocks(query: &str, results: &[ExaResult], tool_use_id: &str) -> Vec<Value> {
    let server_tool_use = json!({
        "type": "server_tool_use",
        "id": tool_use_id,
        "name": "web_search",
        "input": { "query": query },
    });

    let content = if results.is_empty() {
        json!({"type": "web_search_tool_result_error", "error_code": "unavailable"})
    } else {
        let mut items: Vec<Value> = Vec::with_capacity(results.len());
        for r in results {
            let text = r
                .highlights
                .as_ref()
                .map(|h| h.join("\n\n"))
                .filter(|s| !s.is_empty())
                .or_else(|| r.text.clone())
                .unwrap_or_default();
            let encrypted_content =
                base64::engine::general_purpose::STANDARD.encode(text.as_bytes());
            let mut block = json!({
                "type": "web_search_result",
                "url": r.url,
                "title": r.title,
                "encrypted_content": encrypted_content,
            });
            if let Some(date) = r.published_date.as_deref() {
                block
                    .as_object_mut()
                    .unwrap()
                    .insert("page_age".into(), Value::String(date.into()));
            }
            items.push(block);
        }
        Value::Array(items)
    };

    let tool_result = json!({
        "type": "web_search_tool_result",
        "tool_use_id": tool_use_id,
        "content": content,
    });

    vec![server_tool_use, tool_result]
}

/// Exa 结果 → 发给上游的纯文本 tool_result 块（user 消息）。
pub fn build_upstream_tool_result(tool_use_id: &str, results: &[ExaResult]) -> Value {
    let text = results
        .iter()
        .enumerate()
        .map(|(i, r)| {
            let highlights = r
                .highlights
                .as_ref()
                .map(|h| h.join("\n\n"))
                .unwrap_or_default();
            format!("[{}] {}\nURL: {}\n{}", i + 1, r.title, r.url, highlights)
        })
        .collect::<Vec<_>>()
        .join("\n\n---\n\n");
    let content_text = if text.is_empty() {
        "No results found.".to_string()
    } else {
        text
    };
    json!({
        "role": "user",
        "content": [{ "type": "tool_result", "tool_use_id": tool_use_id, "content": content_text }],
    })
}

/// 合并两个 usage object：键名固定四个 token 字段，缺省 0。
pub fn merge_usage(a: &Value, b: &Value) -> Value {
    let mut out = Map::new();
    for key in [
        "input_tokens",
        "output_tokens",
        "cache_creation_input_tokens",
        "cache_read_input_tokens",
    ] {
        let av = a.get(key).and_then(|v| v.as_u64()).unwrap_or(0);
        let bv = b.get(key).and_then(|v| v.as_u64()).unwrap_or(0);
        let total = av + bv;
        if total > 0 {
            out.insert(
                key.into(),
                Value::Number(serde_json::Number::from(total)),
            );
        }
    }
    Value::Object(out)
}

/// 合成 SSE 帧序列：模拟 message_start / content_block_* / message_delta / message_stop。
/// 仅用于 original_stream=true 时把最终 non-stream JSON 转成流给客户端。
pub fn synthesize_sse(final_json: &Value) -> String {
    let mut out = String::new();
    let empty_vec: Vec<Value> = Vec::new();
    let content: &[Value] = final_json
        .get("content")
        .and_then(|v| v.as_array())
        .map(|v| v.as_slice())
        .unwrap_or(&empty_vec);

    let mut msg_copy = final_json.clone();
    if let Some(m) = as_object_mut(&mut msg_copy) {
        m.insert("content".into(), Value::Array(Vec::new()));
    }
    write_sse_event(
        &mut out,
        "message_start",
        &json!({"type": "message_start", "message": msg_copy}),
    );

    for (i, block) in content.iter().enumerate() {
        let block_type = block.get("type").and_then(|v| v.as_str()).unwrap_or("");
        match block_type {
            "text" => {
                let text = block.get("text").and_then(|v| v.as_str()).unwrap_or("");
                write_sse_event(
                    &mut out,
                    "content_block_start",
                    &json!({
                        "type": "content_block_start",
                        "index": i,
                        "content_block": {"type": "text", "text": ""}
                    }),
                );
                if !text.is_empty() {
                    write_sse_event(
                        &mut out,
                        "content_block_delta",
                        &json!({
                            "type": "content_block_delta",
                            "index": i,
                            "delta": {"type": "text_delta", "text": text}
                        }),
                    );
                }
                write_sse_event(
                    &mut out,
                    "content_block_stop",
                    &json!({"type": "content_block_stop", "index": i}),
                );
            }
            "server_tool_use" => {
                let id = block.get("id").and_then(|v| v.as_str()).unwrap_or("");
                let name = block.get("name").and_then(|v| v.as_str()).unwrap_or("");
                let input_str = block
                    .get("input")
                    .map(|v| serde_json::to_string(v).unwrap_or_default())
                    .unwrap_or_default();
                write_sse_event(
                    &mut out,
                    "content_block_start",
                    &json!({
                        "type": "content_block_start",
                        "index": i,
                        "content_block": {"type": "server_tool_use", "id": id, "name": name}
                    }),
                );
                write_sse_event(
                    &mut out,
                    "content_block_delta",
                    &json!({
                        "type": "content_block_delta",
                        "index": i,
                        "delta": {"type": "input_json_delta", "partial_json": input_str}
                    }),
                );
                write_sse_event(
                    &mut out,
                    "content_block_stop",
                    &json!({"type": "content_block_stop", "index": i}),
                );
            }
            _ => {
                write_sse_event(
                    &mut out,
                    "content_block_start",
                    &json!({
                        "type": "content_block_start",
                        "index": i,
                        "content_block": block.clone()
                    }),
                );
                write_sse_event(
                    &mut out,
                    "content_block_stop",
                    &json!({"type": "content_block_stop", "index": i}),
                );
            }
        }
    }

    let stop_reason = final_json
        .get("stop_reason")
        .and_then(|v| v.as_str())
        .unwrap_or("end_turn");
    let usage = final_json
        .get("usage")
        .cloned()
        .unwrap_or_else(|| Value::Object(Map::new()));
    write_sse_event(
        &mut out,
        "message_delta",
        &json!({
            "type": "message_delta",
            "delta": {"stop_reason": stop_reason},
            "usage": usage
        }),
    );
    write_sse_event(&mut out, "message_stop", &json!({"type": "message_stop"}));
    out
}

fn write_sse_event(out: &mut String, kind: &str, data: &Value) {
    out.push_str("event: ");
    out.push_str(kind);
    out.push_str("\ndata: ");
    out.push_str(&serde_json::to_string(data).unwrap_or_default());
    out.push_str("\n\n");
}

/// 从 key 池里随机选一个非空 key。
pub fn pick_exa_key(pool: &[String]) -> Option<String> {
    let valid: Vec<&str> = pool.iter().map(|s| s.trim()).filter(|s| !s.is_empty()).collect();
    if valid.is_empty() {
        return None;
    }
    use rand::Rng;
    Some(valid[rand::thread_rng().gen_range(0..valid.len())].to_string())
}

/// 折算搜索次数到等价 input_tokens 数：用本模型 input 单价折算 $0.01/搜索的成本向上取整。
pub fn search_tokens_equivalent(search_count: u32, input_rate_per_million: f64) -> u64 {
    if search_count == 0 || input_rate_per_million <= 0.0 {
        return 0;
    }
    let usd_cost = (search_count as f64) * SEARCH_USD_COST;
    (usd_cost / (input_rate_per_million / 1_000_000.0)).ceil() as u64
}

// ----------- Loop 主流程 -----------

pub struct LoopInputs<'a> {
    pub http: Arc<Client>,
    pub session_token: Arc<SessionTokenCache>,
    pub exa_api_key: String,
    pub upstream_base: &'a str,
    pub upstream_token: &'a str,
    pub upstream_path: &'a str,
    pub upstream_query: &'a str,
    pub prefix: CopilotPrefix,
    pub config: WebSearchConfig,
    pub parsed_body: Value,
    pub initial_response: Value,
}

#[derive(Clone, Debug)]
pub struct LoopOutcome {
    pub final_response: Value,
    pub accumulated_usage: Value,
    pub search_count: u32,
}

/// Web Search 主循环。WEB_SEARCH_MAX_LOOP 轮内拿到非 tool_use 结束就停。
/// follow-up 上游调用每轮：换 session token（ghu/gho 时）+ individual remap + 120s timeout。
pub async fn run_loop(inputs: LoopInputs<'_>) -> AppResult<LoopOutcome> {
    let LoopInputs {
        http,
        session_token,
        exa_api_key,
        upstream_base,
        upstream_token,
        upstream_path,
        upstream_query,
        prefix,
        config,
        parsed_body,
        initial_response,
    } = inputs;

    let mut current_response = initial_response;
    let mut accumulated_usage = current_response
        .get("usage")
        .cloned()
        .unwrap_or_else(|| Value::Object(Map::new()));
    let mut search_count: u32 = 0;

    // messages 历史拷一份用来 follow-up
    let mut messages: Vec<Value> = parsed_body
        .get("messages")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let mut client_blocks: Vec<Value> = Vec::new();

    for _ in 0..WEB_SEARCH_MAX_LOOP {
        let content: Vec<Value> = current_response
            .get("content")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let stop_reason = current_response
            .get("stop_reason")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        if stop_reason != "tool_use" {
            client_blocks.extend(content);
            break;
        }

        let mut our_calls: Vec<Value> = Vec::new();
        let mut other_calls: Vec<Value> = Vec::new();
        let mut text_blocks: Vec<Value> = Vec::new();
        for block in &content {
            let bt = block.get("type").and_then(|v| v.as_str()).unwrap_or("");
            match bt {
                "tool_use" => {
                    let name = block.get("name").and_then(|v| v.as_str()).unwrap_or("");
                    if name == WEB_SEARCH_TOOL_NAME {
                        our_calls.push(block.clone());
                    } else {
                        other_calls.push(block.clone());
                    }
                }
                "text" => text_blocks.push(block.clone()),
                _ => {}
            }
        }
        if our_calls.is_empty() {
            client_blocks.extend(content);
            break;
        }
        client_blocks.extend(text_blocks);

        let mut tool_result_msgs: Vec<Value> = Vec::new();
        // 并行搜
        let mut search_tasks: Vec<_> = Vec::with_capacity(our_calls.len());
        for tc in &our_calls {
            let query = tc
                .get("input")
                .and_then(|v| v.as_object())
                .and_then(|m| m.get("query"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let tc_id = tc
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if search_count >= config.max_uses {
                search_tasks.push(SearchHandle::exceeded(query, tc_id));
                continue;
            }
            search_count += 1;
            let http = http.clone();
            let api_key = exa_api_key.clone();
            let allowed = config.allowed_domains.clone();
            let blocked = config.blocked_domains.clone();
            let task = tokio::spawn(async move {
                let results = call_exa(
                    &http,
                    &query,
                    &api_key,
                    allowed.as_deref(),
                    blocked.as_deref(),
                )
                .await;
                (query, tc_id, Some(results))
            });
            search_tasks.push(SearchHandle::Async(task));
        }

        for handle in search_tasks {
            let (query, tc_id, results) = handle.await_into().await;
            let tool_use_id = gen_srvtoolu_id();
            match results {
                None => {
                    // max_uses_exceeded
                    client_blocks.push(json!({
                        "type": "server_tool_use",
                        "id": tool_use_id,
                        "name": "web_search",
                        "input": {"query": query}
                    }));
                    client_blocks.push(json!({
                        "type": "web_search_tool_result",
                        "tool_use_id": tool_use_id,
                        "content": {"type": "web_search_tool_result_error", "error_code": "max_uses_exceeded"}
                    }));
                    tool_result_msgs.push(json!({
                        "role": "user",
                        "content": [{
                            "type": "tool_result",
                            "tool_use_id": tc_id,
                            "content": "Error: max web search uses exceeded.",
                            "is_error": true
                        }],
                    }));
                }
                Some(results) => {
                    let blocks = build_client_blocks(&query, &results, &tool_use_id);
                    client_blocks.extend(blocks);
                    tool_result_msgs.push(build_upstream_tool_result(&tc_id, &results));
                }
            }
        }

        // 客户端调了别的 tool（非 web_search） — 跳过 follow-up 直接返回，让客户端自己处理
        if !other_calls.is_empty() {
            client_blocks.extend(other_calls);
            if let Some(m) = as_object_mut(&mut current_response) {
                m.insert("content".into(), Value::Array(client_blocks.clone()));
                m.insert("stop_reason".into(), Value::String("tool_use".into()));
            }
            return Ok(LoopOutcome {
                final_response: current_response,
                accumulated_usage,
                search_count,
            });
        }

        // 给上游加 assistant tool_use 消息 + 工具结果
        let assistant_msg = json!({"role": "assistant", "content": content});
        messages.push(assistant_msg);
        messages.extend(tool_result_msgs);

        // 构造 follow-up body（删 messages/stream，重新设）
        let mut follow_up = Map::new();
        if let Some(parsed_map) = parsed_body.as_object() {
            for (k, v) in parsed_map {
                if k != "messages" && k != "stream" {
                    follow_up.insert(k.clone(), v.clone());
                }
            }
        }
        follow_up.insert("messages".into(), Value::Array(messages.clone()));
        follow_up.insert("stream".into(), Value::Bool(false));
        let mut follow_up_value = Value::Object(follow_up);

        // ghu_/gho_ → session token
        let current_token = if upstream_token.starts_with("ghu_") || upstream_token.starts_with("gho_")
        {
            match session_token.get_or_fetch(upstream_token).await {
                Ok(t) => t,
                Err(_) => break,
            }
        } else {
            upstream_token.to_string()
        };

        if prefix == CopilotPrefix::Individual {
            if let Some(map) = as_object_mut(&mut follow_up_value) {
                if let Some(model) = map.get("model").and_then(|v| v.as_str()) {
                    if let Some(mapped) = model_forward(model) {
                        map.insert("model".into(), Value::String(mapped.to_string()));
                    }
                }
            }
        }

        let target = format!("{upstream_base}{upstream_path}{upstream_query}");
        let headers = build_upstream_headers(&current_token, Some("application/json"));
        let result = http
            .post(&target)
            .headers(headers)
            .timeout(FOLLOW_UP_TIMEOUT)
            .json(&follow_up_value)
            .send()
            .await;
        let Ok(resp) = result else {
            client_blocks.push(json!({"type": "text", "text": ""}));
            break;
        };
        if !resp.status().is_success() {
            client_blocks.push(json!({"type": "text", "text": ""}));
            break;
        }
        let next_json: Value = match resp.json().await {
            Ok(v) => v,
            Err(_) => break,
        };
        if let Some(u) = next_json.get("usage") {
            accumulated_usage = merge_usage(&accumulated_usage, u);
        }
        current_response = next_json;
    }

    if let Some(m) = as_object_mut(&mut current_response) {
        m.insert("content".into(), Value::Array(client_blocks));
    }

    // usage 末尾贴 server_tool_use.web_search_requests
    let final_usage = if search_count > 0 {
        let rate = crate::billing::pricing::copilot_rate(
            current_response
                .get("model")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
        );
        let extra = search_tokens_equivalent(search_count, rate.input);
        let mut usage_map = match accumulated_usage.clone() {
            Value::Object(m) => m,
            _ => Map::new(),
        };
        let prev = usage_map
            .get("input_tokens")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        usage_map.insert(
            "input_tokens".into(),
            Value::Number(serde_json::Number::from(prev + extra)),
        );
        usage_map.insert(
            "server_tool_use".into(),
            json!({"web_search_requests": search_count}),
        );
        Value::Object(usage_map)
    } else {
        accumulated_usage.clone()
    };

    if let Some(m) = as_object_mut(&mut current_response) {
        m.insert("usage".into(), final_usage.clone());
    }

    Ok(LoopOutcome {
        final_response: current_response,
        accumulated_usage: final_usage,
        search_count,
    })
}

enum SearchHandle {
    Async(tokio::task::JoinHandle<(String, String, Option<Vec<ExaResult>>)>),
    Exceeded { query: String, tc_id: String },
}

impl SearchHandle {
    fn exceeded(query: String, tc_id: String) -> Self {
        Self::Exceeded { query, tc_id }
    }

    async fn await_into(self) -> (String, String, Option<Vec<ExaResult>>) {
        match self {
            SearchHandle::Async(handle) => match handle.await {
                Ok(t) => t,
                Err(_) => (String::new(), String::new(), Some(Vec::new())),
            },
            SearchHandle::Exceeded { query, tc_id } => (query, tc_id, None),
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::billing::pricing::copilot_rate;

    fn json_with_tools(tools: Vec<Value>) -> Value {
        json!({"model": "claude-opus-4.6", "messages": [], "tools": tools, "stream": true})
    }

    #[test]
    fn detect_replaces_first_web_search_tool() {
        let mut p = json_with_tools(vec![
            json!({"type": "web_search_20260209", "max_uses": 3, "allowed_domains": ["a.com"]}),
            json!({"type": "custom", "name": "x"}),
        ]);
        let cfg = detect_and_replace(&mut p);
        assert!(cfg.active);
        assert_eq!(cfg.max_uses, 3);
        assert!(cfg.original_stream);
        assert_eq!(cfg.allowed_domains.as_deref(), Some(&["a.com".to_string()][..]));
        assert_eq!(p["tools"][0]["name"], WEB_SEARCH_TOOL_NAME);
        assert_eq!(p["tools"][1]["name"], "x");
        assert_eq!(p["stream"], false);
    }

    #[test]
    fn detect_drops_subsequent_web_search_tools() {
        let mut p = json_with_tools(vec![
            json!({"type": "web_search_20260209"}),
            json!({"type": "web_search_20250305"}),
            json!({"type": "custom", "name": "x"}),
        ]);
        let cfg = detect_and_replace(&mut p);
        assert!(cfg.active);
        let tools = p["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 2);
        assert_eq!(tools[0]["name"], WEB_SEARCH_TOOL_NAME);
        assert_eq!(tools[1]["name"], "x");
    }

    #[test]
    fn detect_inactive_when_no_web_search() {
        let mut p = json_with_tools(vec![json!({"type": "custom", "name": "x"})]);
        let cfg = detect_and_replace(&mut p);
        assert!(!cfg.active);
        assert_eq!(p["stream"], true, "stream not forced when no web_search");
    }

    #[test]
    fn detect_syncs_tool_choice_name() {
        let mut p = json!({
            "model": "claude-opus-4.6",
            "tools": [{"type": "web_search_20260209"}],
            "tool_choice": {"type": "tool", "name": "web_search"}
        });
        let cfg = detect_and_replace(&mut p);
        assert!(cfg.active);
        assert_eq!(p["tool_choice"]["name"], WEB_SEARCH_TOOL_NAME);
    }

    #[test]
    fn build_client_blocks_when_empty_emits_unavailable_error() {
        let blocks = build_client_blocks("q", &[], "srvtoolu_x");
        assert_eq!(blocks[0]["type"], "server_tool_use");
        assert_eq!(blocks[1]["type"], "web_search_tool_result");
        assert_eq!(
            blocks[1]["content"]["error_code"],
            "unavailable"
        );
    }

    #[test]
    fn build_client_blocks_encodes_highlights() {
        let results = vec![ExaResult {
            title: "T".into(),
            url: "https://x.com".into(),
            published_date: Some("2024-01-01".into()),
            highlights: Some(vec!["alpha".into(), "beta".into()]),
            text: None,
        }];
        let blocks = build_client_blocks("q", &results, "srvtoolu_x");
        let result_block = &blocks[1]["content"][0];
        assert_eq!(result_block["url"], "https://x.com");
        assert_eq!(result_block["page_age"], "2024-01-01");
        let encoded = result_block["encrypted_content"].as_str().unwrap();
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .unwrap();
        assert_eq!(decoded, b"alpha\n\nbeta");
    }

    #[test]
    fn build_upstream_tool_result_renders_numbered_text() {
        let results = vec![
            ExaResult {
                title: "T1".into(),
                url: "u1".into(),
                published_date: None,
                highlights: Some(vec!["a".into()]),
                text: None,
            },
            ExaResult {
                title: "T2".into(),
                url: "u2".into(),
                published_date: None,
                highlights: Some(vec!["b".into()]),
                text: None,
            },
        ];
        let msg = build_upstream_tool_result("toolu_x", &results);
        let text = msg["content"][0]["content"].as_str().unwrap();
        assert!(text.contains("[1] T1"));
        assert!(text.contains("[2] T2"));
        assert!(text.contains("---"));
    }

    #[test]
    fn merge_usage_sums_known_keys() {
        let a = json!({"input_tokens": 10, "output_tokens": 5});
        let b = json!({"input_tokens": 7, "cache_read_input_tokens": 3});
        let out = merge_usage(&a, &b);
        assert_eq!(out["input_tokens"], 17);
        assert_eq!(out["output_tokens"], 5);
        assert_eq!(out["cache_read_input_tokens"], 3);
    }

    #[test]
    fn search_tokens_equivalent_rounds_up() {
        let rate = copilot_rate("claude-opus-4.6");
        // input = $5/MTok, 1 次搜索 = $0.01 → 2_000 tokens
        let tokens = search_tokens_equivalent(1, rate.input);
        assert_eq!(tokens, 2_000);
        // 0 次返 0
        assert_eq!(search_tokens_equivalent(0, rate.input), 0);
    }

    #[test]
    fn pick_exa_key_handles_pool() {
        let pool = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let k = pick_exa_key(&pool).expect("some");
        assert!(["a", "b", "c"].contains(&k.as_str()));
        assert!(pick_exa_key(&[]).is_none());
        assert!(pick_exa_key(&["".to_string(), " ".to_string()]).is_none());
    }

    #[test]
    fn synthesize_sse_covers_text_and_tool_use() {
        let final_json = json!({
            "id": "msg_bdrk_01x",
            "model": "claude-opus-4-6",
            "content": [
                {"type": "text", "text": "hello"},
                {"type": "server_tool_use", "id": "srvtoolu_x", "name": "web_search", "input": {"query": "q"}}
            ],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 1, "output_tokens": 2}
        });
        let out = synthesize_sse(&final_json);
        assert!(out.contains("event: message_start"));
        assert!(out.contains("event: content_block_start"));
        assert!(out.contains("text_delta"));
        assert!(out.contains("input_json_delta"));
        assert!(out.contains("event: message_delta"));
        assert!(out.contains("event: message_stop"));
    }
}
