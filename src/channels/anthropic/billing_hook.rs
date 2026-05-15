//! SSE sniffer 的 ``usage`` 累加器。从 ``message_start`` 抓 input/cache tokens、
//! 从 ``message_delta.usage`` 抓 output，组装 BillingRecord 走共享 UsageWriter。
//! sniffer 路径与响应转发互不干扰，parse 失败丢账不丢响应。

use crate::billing::{BillingRecord, UsageWriter};
use crate::channels::ChannelKind;
use serde::Deserialize;

/// 一次 SSE 流的 usage 累计状态。``input_tokens / cache_*`` 在 ``message_start`` 一次给齐，
/// ``output_tokens`` 在 ``message_delta`` 给。``finalize`` 时合成 BillingRecord 落账。
#[derive(Debug, Default)]
pub struct SseUsageAggregator {
    input_tokens: u64,
    output_tokens: u64,
    cache_creation_tokens: u64,
    cache_read_tokens: u64,
    model: Option<String>,
    seen_message_start: bool,
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

impl SseUsageAggregator {
    pub fn new() -> Self {
        Self::default()
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
            if let Some(v) = u.cache_creation_input_tokens {
                self.cache_creation_tokens = v;
            }
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
            if let Some(v) = u.cache_creation_input_tokens {
                self.cache_creation_tokens = self.cache_creation_tokens.max(v);
            }
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
            cache_creation_tokens: self.cache_creation_tokens,
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
    fn ingests_message_start_usage() {
        let mut agg = SseUsageAggregator::new();
        agg.ingest_message_start(
            br#"{"message":{"model":"claude-opus-4-7","usage":{"input_tokens":120,"cache_creation_input_tokens":80,"cache_read_input_tokens":40}}}"#,
        );
        assert!(agg.seen_message_start);
        assert_eq!(agg.input_tokens, 120);
        assert_eq!(agg.cache_creation_tokens, 80);
        assert_eq!(agg.cache_read_tokens, 40);
        assert_eq!(agg.model.as_deref(), Some("claude-opus-4-7"));
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
}
