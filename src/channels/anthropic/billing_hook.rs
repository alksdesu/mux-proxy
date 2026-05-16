//! SSE sniffer 的 ``usage`` 累加器。从 ``message_start`` 抓 input/cache tokens、
//! 从 ``message_delta.usage`` 抓 output，组装 BillingRecord 走共享 UsageWriter。
//! sniffer 路径与响应转发互不干扰，parse 失败丢账不丢响应。
//!
//! Cache 计费区分：``usage.cache_creation.ephemeral_5m_input_tokens`` 和
//! ``ephemeral_1h_input_tokens`` 分别按 1.25× / 2.0× input 单价计费；缺失子对象时
//! 用顶层 ``cache_creation_input_tokens`` 总和兜底，全部计入 5m（保守一侧）。

use crate::billing::{BillingRecord, UsageWriter};
use crate::channels::ChannelKind;
use serde::Deserialize;
use tracing::warn;

/// non-SSE JSON 响应体 parse 上限。超过这个值不解析以省内存，走 fallback 0 tokens 计费。
pub const NON_SSE_PARSE_LIMIT: usize = 1024 * 1024;

/// 一次 SSE 流的 usage 累计状态。``input_tokens / cache_*`` 在 ``message_start`` 一次给齐，
/// ``output_tokens`` 在 ``message_delta`` 给。``finalize`` 时合成 BillingRecord 落账。
#[derive(Debug, Default)]
pub struct SseUsageAggregator {
    input_tokens: u64,
    output_tokens: u64,
    cache_creation_5m_tokens: u64,
    cache_creation_1h_tokens: u64,
    cache_read_tokens: u64,
    model: Option<String>,
    seen_message_start: bool,
}

#[derive(Debug, Deserialize, Default)]
struct CacheCreationDetails {
    #[serde(default)]
    ephemeral_5m_input_tokens: Option<u64>,
    #[serde(default)]
    ephemeral_1h_input_tokens: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct UsageJson {
    #[serde(default)]
    input_tokens: Option<u64>,
    #[serde(default)]
    output_tokens: Option<u64>,
    #[serde(default)]
    cache_creation_input_tokens: Option<u64>,
    #[serde(default)]
    cache_read_input_tokens: Option<u64>,
    /// ttl 维度的明细。Anthropic 上游 prompt caching 才会发；缺失时回退用
    /// ``cache_creation_input_tokens`` 总和当全 5m。
    #[serde(default)]
    cache_creation: Option<CacheCreationDetails>,
}

#[derive(Debug, Deserialize)]
struct MessageStartPayload {
    #[serde(default)]
    message: Option<MessageStartMessage>,
}

#[derive(Debug, Deserialize)]
struct MessageStartMessage {
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    usage: Option<UsageJson>,
}

#[derive(Debug, Deserialize)]
struct MessageDeltaPayload {
    #[serde(default)]
    usage: Option<UsageJson>,
}

/// non-SSE 一次性响应体 schema（``POST /v1/messages`` stream=false）。
/// 顶层 ``model`` + ``usage`` 直接给齐，不需要 message_start/delta 拼接。
#[derive(Debug, Deserialize)]
struct NonStreamingResponse {
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    usage: Option<UsageJson>,
}

/// 把 UsageJson 的 cache_creation 嵌套和顶层总和折叠成 (5m, 1h) 元组。
/// 优先用嵌套明细；没有时把总和归到 5m，1h=0。这是对历史响应或非 cache 请求的兜底。
fn split_cache_creation(u: &UsageJson) -> (u64, u64) {
    if let Some(detail) = u.cache_creation.as_ref() {
        let m5 = detail.ephemeral_5m_input_tokens.unwrap_or(0);
        let h1 = detail.ephemeral_1h_input_tokens.unwrap_or(0);
        // 即便明细对象存在但两字段全 0，仍然以顶层 total 兜底（防止上游只给汇总）
        if m5 == 0 && h1 == 0 {
            return (u.cache_creation_input_tokens.unwrap_or(0), 0);
        }
        return (m5, h1);
    }
    (u.cache_creation_input_tokens.unwrap_or(0), 0)
}

/// 纯函数版本：parse 出 BillingRecord 字段值，写入由 caller 决定。
/// 体超过 ``NON_SSE_PARSE_LIMIT`` 跳过 parse；parse 失败也走 0 tokens 路径。
/// 返回的 ``model`` 永远非空（不命中时用 original_model 兜底）。
pub fn parse_non_sse_billing(
    plain: &[u8],
    key_name: &str,
    original_model: &str,
) -> NonSseBilling {
    if plain.len() > NON_SSE_PARSE_LIMIT {
        warn!(
            key_name = %key_name,
            size = plain.len(),
            "non-SSE response exceeds parse limit, billing with 0 tokens"
        );
        return NonSseBilling::fallback(original_model);
    }
    match serde_json::from_slice::<NonStreamingResponse>(plain) {
        Ok(parsed) => {
            let usage = parsed.usage.unwrap_or_default();
            let (cache_5m, cache_1h) = split_cache_creation(&usage);
            NonSseBilling {
                model: parsed.model.unwrap_or_else(|| original_model.to_string()),
                input_tokens: usage.input_tokens.unwrap_or(0),
                output_tokens: usage.output_tokens.unwrap_or(0),
                cache_creation_5m_tokens: cache_5m,
                cache_creation_1h_tokens: cache_1h,
                cache_read_tokens: usage.cache_read_input_tokens.unwrap_or(0),
                fallback: false,
            }
        }
        Err(e) => {
            warn!(
                key_name = %key_name,
                error = ?e,
                "non-SSE response parse failed, billing with 0 tokens"
            );
            NonSseBilling::fallback(original_model)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NonSseBilling {
    pub model: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_creation_5m_tokens: u64,
    pub cache_creation_1h_tokens: u64,
    pub cache_read_tokens: u64,
    /// 是否走了 fallback（超限或 parse 失败）。仅供日志/单测断言用。
    pub fallback: bool,
}

impl NonSseBilling {
    fn fallback(original_model: &str) -> Self {
        Self {
            model: original_model.to_string(),
            input_tokens: 0,
            output_tokens: 0,
            cache_creation_5m_tokens: 0,
            cache_creation_1h_tokens: 0,
            cache_read_tokens: 0,
            fallback: true,
        }
    }
}

/// non-SSE JSON 响应一次性 parse usage 并写入 UsageWriter。
pub fn record_non_sse_usage(
    writer: &UsageWriter,
    plain: &[u8],
    key_name: &str,
    original_model: &str,
    request_body: String,
    ip: Option<String>,
) {
    let billing = parse_non_sse_billing(plain, key_name, original_model);
    writer.record(BillingRecord {
        channel: ChannelKind::Anthropic,
        model: billing.model,
        key_name: key_name.to_string(),
        input_tokens: billing.input_tokens,
        output_tokens: billing.output_tokens,
        cache_creation_5m_tokens: billing.cache_creation_5m_tokens,
        cache_creation_1h_tokens: billing.cache_creation_1h_tokens,
        cache_read_tokens: billing.cache_read_tokens,
        request_body,
        ip,
    });
}

impl Default for UsageJson {
    fn default() -> Self {
        Self {
            input_tokens: None,
            output_tokens: None,
            cache_creation_input_tokens: None,
            cache_read_input_tokens: None,
            cache_creation: None,
        }
    }
}

impl SseUsageAggregator {
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    pub fn input_tokens(&self) -> u64 {
        self.input_tokens
    }

    #[cfg(test)]
    pub fn output_tokens(&self) -> u64 {
        self.output_tokens
    }

    #[cfg(test)]
    pub fn cache_creation_5m_tokens(&self) -> u64 {
        self.cache_creation_5m_tokens
    }

    #[cfg(test)]
    pub fn cache_creation_1h_tokens(&self) -> u64 {
        self.cache_creation_1h_tokens
    }

    #[cfg(test)]
    pub fn cache_read_tokens(&self) -> u64 {
        self.cache_read_tokens
    }

    #[cfg(test)]
    pub fn model(&self) -> Option<&str> {
        self.model.as_deref()
    }

    #[cfg(test)]
    pub fn seen_message_start(&self) -> bool {
        self.seen_message_start
    }

    /// 处理一条 ``event: message_start`` 后跟随的 ``data: {...}``。
    pub fn ingest_message_start(&mut self, data: &[u8]) {
        let Ok(parsed) = serde_json::from_slice::<MessageStartPayload>(data) else {
            return;
        };
        let Some(msg) = parsed.message else {
            return;
        };
        self.seen_message_start = true;
        if let Some(m) = msg.model {
            self.model = Some(m);
        }
        if let Some(u) = msg.usage {
            if let Some(v) = u.input_tokens {
                self.input_tokens = v;
            }
            let (m5, h1) = split_cache_creation(&u);
            self.cache_creation_5m_tokens = m5;
            self.cache_creation_1h_tokens = h1;
            if let Some(v) = u.cache_read_input_tokens {
                self.cache_read_tokens = v;
            }
        }
    }

    /// 处理一条 ``event: message_delta`` 后跟随的 ``data: {...}``。
    pub fn ingest_message_delta(&mut self, data: &[u8]) {
        let Ok(parsed) = serde_json::from_slice::<MessageDeltaPayload>(data) else {
            return;
        };
        if let Some(u) = parsed.usage {
            if let Some(v) = u.output_tokens {
                self.output_tokens = v;
            }
            if let Some(v) = u.input_tokens {
                self.input_tokens = self.input_tokens.max(v);
            }
            // delta 里若上游又给了 cache 明细就 max 取大，避免覆盖 message_start 已有值。
            let (m5, h1) = split_cache_creation(&u);
            self.cache_creation_5m_tokens = self.cache_creation_5m_tokens.max(m5);
            self.cache_creation_1h_tokens = self.cache_creation_1h_tokens.max(h1);
            if let Some(v) = u.cache_read_input_tokens {
                self.cache_read_tokens = self.cache_read_tokens.max(v);
            }
        }
    }

    /// 流结束（成功或客户端断开）后写一条 usage_log。``model`` 用 ``original`` 兜底，
    /// 这样客户端日志里看到的就是 ``发请求那条模型名``，不暴露后端 splice 后的 current。
    pub fn finalize(
        &self,
        writer: &UsageWriter,
        key_name: &str,
        original_model: &str,
        request_body: String,
        ip: Option<String>,
    ) {
        if !self.seen_message_start {
            return;
        }
        let model = self
            .model
            .clone()
            .unwrap_or_else(|| original_model.to_string());
        let record = BillingRecord {
            channel: ChannelKind::Anthropic,
            model,
            key_name: key_name.to_string(),
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
            cache_creation_5m_tokens: self.cache_creation_5m_tokens,
            cache_creation_1h_tokens: self.cache_creation_1h_tokens,
            cache_read_tokens: self.cache_read_tokens,
            request_body,
            ip,
        };
        writer.record(record);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ingests_message_start_usage_with_top_level_total_falls_to_5m() {
        let mut agg = SseUsageAggregator::new();
        agg.ingest_message_start(
            br#"{"message":{"model":"claude-opus-4-7","usage":{"input_tokens":120,"cache_creation_input_tokens":80,"cache_read_input_tokens":40}}}"#,
        );
        assert!(agg.seen_message_start);
        assert_eq!(agg.input_tokens, 120);
        assert_eq!(agg.cache_creation_5m_tokens, 80);
        assert_eq!(agg.cache_creation_1h_tokens, 0);
        assert_eq!(agg.cache_read_tokens, 40);
        assert_eq!(agg.model.as_deref(), Some("claude-opus-4-7"));
    }

    #[test]
    fn ingests_message_start_with_split_ephemeral_details() {
        let mut agg = SseUsageAggregator::new();
        agg.ingest_message_start(
            br#"{"message":{"model":"claude-opus-4-7","usage":{"input_tokens":2048,"cache_creation_input_tokens":248,"cache_read_input_tokens":1800,"cache_creation":{"ephemeral_5m_input_tokens":148,"ephemeral_1h_input_tokens":100}}}}"#,
        );
        assert_eq!(agg.cache_creation_5m_tokens, 148);
        assert_eq!(agg.cache_creation_1h_tokens, 100);
        assert_eq!(agg.cache_read_tokens, 1800);
    }

    #[test]
    fn ingests_message_delta_output() {
        let mut agg = SseUsageAggregator::new();
        agg.ingest_message_start(br#"{"message":{"model":"x","usage":{"input_tokens":10}}}"#);
        agg.ingest_message_delta(br#"{"usage":{"output_tokens":777}}"#);
        assert_eq!(agg.input_tokens, 10);
        assert_eq!(agg.output_tokens, 777);
    }

    #[test]
    fn malformed_json_is_ignored() {
        let mut agg = SseUsageAggregator::new();
        agg.ingest_message_start(b"not valid json");
        agg.ingest_message_delta(b"{");
        assert!(!agg.seen_message_start);
        assert_eq!(agg.input_tokens, 0);
    }

    #[test]
    fn delta_does_not_decrease_input_tokens() {
        let mut agg = SseUsageAggregator::new();
        agg.ingest_message_start(br#"{"message":{"usage":{"input_tokens":100}}}"#);
        agg.ingest_message_delta(br#"{"usage":{"input_tokens":1,"output_tokens":2}}"#);
        assert_eq!(agg.input_tokens, 100);
        assert_eq!(agg.output_tokens, 2);
    }

    #[test]
    fn delta_with_cache_details_max_strategy() {
        let mut agg = SseUsageAggregator::new();
        agg.ingest_message_start(
            br#"{"message":{"usage":{"cache_creation":{"ephemeral_5m_input_tokens":50,"ephemeral_1h_input_tokens":20}}}}"#,
        );
        agg.ingest_message_delta(
            br#"{"usage":{"cache_creation":{"ephemeral_5m_input_tokens":10,"ephemeral_1h_input_tokens":80}}}"#,
        );
        // 5m max(50, 10) = 50；1h max(20, 80) = 80
        assert_eq!(agg.cache_creation_5m_tokens, 50);
        assert_eq!(agg.cache_creation_1h_tokens, 80);
    }

    #[test]
    fn non_sse_json_records_usage_with_split_cache() {
        let body = br#"{"id":"msg_x","model":"claude-jupiter-v1-p","usage":{"input_tokens":42,"output_tokens":17,"cache_creation_input_tokens":248,"cache_read_input_tokens":3,"cache_creation":{"ephemeral_5m_input_tokens":148,"ephemeral_1h_input_tokens":100}}}"#;
        let billing = parse_non_sse_billing(body, "test-key", "claude-opus-4-7");
        assert!(!billing.fallback);
        assert_eq!(billing.model, "claude-jupiter-v1-p");
        assert_eq!(billing.input_tokens, 42);
        assert_eq!(billing.output_tokens, 17);
        assert_eq!(billing.cache_creation_5m_tokens, 148);
        assert_eq!(billing.cache_creation_1h_tokens, 100);
        assert_eq!(billing.cache_read_tokens, 3);
    }

    #[test]
    fn non_sse_json_without_cache_details_falls_to_5m() {
        let body = br#"{"id":"msg_x","model":"m","usage":{"input_tokens":10,"output_tokens":5,"cache_creation_input_tokens":80}}"#;
        let billing = parse_non_sse_billing(body, "k", "orig");
        assert_eq!(billing.cache_creation_5m_tokens, 80);
        assert_eq!(billing.cache_creation_1h_tokens, 0);
    }

    #[test]
    fn non_sse_malformed_skips_billing_silently() {
        let billing = parse_non_sse_billing(b"not json", "test-key", "claude-opus-4-7");
        assert!(billing.fallback);
        assert_eq!(billing.model, "claude-opus-4-7");
        assert_eq!(billing.input_tokens, 0);
        assert_eq!(billing.output_tokens, 0);
    }

    #[test]
    fn non_sse_oversized_falls_back_to_zero_tokens() {
        let mut blob = b"{\"model\":\"m\",\"usage\":{\"input_tokens\":99},\"padding\":\"".to_vec();
        blob.extend(std::iter::repeat(b'A').take(NON_SSE_PARSE_LIMIT + 1));
        blob.extend_from_slice(b"\"}");
        let billing = parse_non_sse_billing(&blob, "test-key", "orig");
        assert!(billing.fallback);
        assert_eq!(billing.model, "orig");
        assert_eq!(billing.input_tokens, 0);
    }

    #[test]
    fn non_sse_missing_usage_fields_default_zero() {
        let body = br#"{"model":"x","usage":{}}"#;
        let billing = parse_non_sse_billing(body, "k", "orig");
        assert!(!billing.fallback);
        assert_eq!(billing.model, "x");
        assert_eq!(billing.input_tokens, 0);
        assert_eq!(billing.output_tokens, 0);
    }

    #[test]
    fn empty_cache_creation_subobject_falls_back_to_total() {
        // 上游有时给了空对象 cache_creation:{} 总和又非零；不能错把整段当 0。
        let body = br#"{"model":"x","usage":{"cache_creation_input_tokens":60,"cache_creation":{}}}"#;
        let billing = parse_non_sse_billing(body, "k", "orig");
        assert_eq!(billing.cache_creation_5m_tokens, 60);
        assert_eq!(billing.cache_creation_1h_tokens, 0);
    }
}
