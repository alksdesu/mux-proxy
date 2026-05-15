//! SSE 解析 + 清洗 + 计费提取。设计成纯状态机：
//! - feed(chunk) → 返回需要写给客户端的字节段；
//! - finish() → flush 残留 buffer。
//!
//! 关键约束：stream_usage_recorded 必须在 spawn write_usage 前**同步**置位，
//! 否则 finally 看到 false 会再写一笔重复账。

use crate::channels::copilot::direct::DirectFlags;
use crate::channels::copilot::response_xform::sanitize_sse_event;
use crate::shared::sse_event::{is_allowed, is_passthrough};
use serde_json::Value;

#[derive(Clone, Debug, Default)]
pub struct StreamStartUsage {
    pub input_tokens: Option<u64>,
    pub cache_creation_input_tokens: Option<u64>,
    pub cache_read_input_tokens: Option<u64>,
}

#[derive(Clone, Debug, Default)]
pub struct FinalUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_creation_tokens: u64,
    pub cache_read_tokens: u64,
}

#[derive(Clone, Debug, Default)]
pub struct SseStats {
    pub stream_start_usage: Option<StreamStartUsage>,
    pub final_usage: Option<FinalUsage>,
    pub stream_model: Option<String>,
    /// 计费写出 race 标记：transform_data_line 内部命中 message_delta 即置 true，
    /// 由 handler 用来判定 finally 是否要补偿写 partial。
    pub usage_recorded: bool,
}

pub struct SseProcessor {
    direct: DirectFlags,
    /// 跨 chunk 残留行（不含末尾 LF）
    buffer: String,
    /// 上一行 `event:` 头解析出来的类型
    pending_event_type: String,
    stats: SseStats,
}

impl SseProcessor {
    pub fn new(direct: DirectFlags) -> Self {
        Self {
            direct,
            buffer: String::new(),
            pending_event_type: String::new(),
            stats: SseStats::default(),
        }
    }

    pub fn stats(&self) -> &SseStats {
        &self.stats
    }

    /// 标记 partial flush 已写出，避免外层 finally 再 fire 一次。
    pub fn mark_usage_recorded(&mut self) {
        self.stats.usage_recorded = true;
    }

    /// 喂一段 chunk，返回应转发给客户端的字符串。
    pub fn feed(&mut self, chunk: &str) -> String {
        self.buffer.push_str(chunk);
        let mut out = String::with_capacity(chunk.len());
        // 按 \n 切，最后一行（无 \n 终止）留给下次拼接
        loop {
            let Some(idx) = self.buffer.find('\n') else {
                break;
            };
            let line = self.buffer[..idx].to_string();
            self.buffer.drain(..=idx);
            self.process_line(&line, &mut out);
        }
        out
    }

    /// 结束流：buffer 残留也走一次 flush（保最后 chunk 落 usage）。
    pub fn finish(&mut self) -> String {
        let mut out = String::new();
        if !self.buffer.trim().is_empty() {
            let lines: Vec<String> = self.buffer.split('\n').map(|s| s.to_string()).collect();
            self.buffer.clear();
            for line in lines {
                self.process_line(&line, &mut out);
            }
        }
        out
    }

    fn process_line(&mut self, line: &str, out: &mut String) {
        let trimmed = line.trim_end_matches('\r');
        if let Some(rest) = trimmed.strip_prefix("event: ") {
            let kind = rest.trim().to_string();
            self.pending_event_type = kind.clone();
            if is_allowed(&kind) {
                out.push_str("event: ");
                out.push_str(&kind);
                out.push('\n');
            }
            return;
        }
        if let Some(rest) = trimmed.strip_prefix("data: ") {
            let data = rest.trim();
            let event_type = std::mem::take(&mut self.pending_event_type);
            self.process_data_line(data, &event_type, out);
            return;
        }
        if trimmed.is_empty() {
            // SSE 帧分隔。透传可保留客户端流的节奏感。
            out.push('\n');
            return;
        }
        // 注释行 (`:xxx`) 等。direct 模式透传，非 direct 丢弃。
        if self.direct.passthrough_sse_comments() {
            out.push_str(trimmed);
            out.push('\n');
        }
    }

    fn process_data_line(&mut self, data: &str, event_type_hint: &str, out: &mut String) {
        if data == "[DONE]" {
            // OpenAI 风格的 [DONE] sentinel，原样透传防止客户端期待
            out.push_str("data: [DONE]\n\n");
            return;
        }

        if is_passthrough(event_type_hint) {
            out.push_str("data: ");
            out.push_str(data);
            out.push_str("\n\n");
            return;
        }

        let mut event: Value = match serde_json::from_str(data) {
            Ok(v) => v,
            Err(_) => return,
        };
        let resolved_type = if let Some(t) = event.get("type").and_then(|v| v.as_str()) {
            t.to_string()
        } else if !event_type_hint.is_empty() {
            if let Some(obj) = event.as_object_mut() {
                obj.insert("type".into(), Value::String(event_type_hint.to_string()));
            }
            event_type_hint.to_string()
        } else {
            String::new()
        };

        if !self.direct.direct && !is_allowed(&resolved_type) {
            return;
        }

        match resolved_type.as_str() {
            "message_start" => {
                if let Some(msg) = event.get("message") {
                    if let Some(model) = msg.get("model").and_then(|v| v.as_str()) {
                        self.stats.stream_model = Some(model.to_string());
                    }
                    if let Some(usage) = msg.get("usage").and_then(|v| v.as_object()) {
                        self.stats.stream_start_usage = Some(StreamStartUsage {
                            input_tokens: usage.get("input_tokens").and_then(|v| v.as_u64()),
                            cache_creation_input_tokens: usage
                                .get("cache_creation_input_tokens")
                                .and_then(|v| v.as_u64()),
                            cache_read_input_tokens: usage
                                .get("cache_read_input_tokens")
                                .and_then(|v| v.as_u64()),
                        });
                    }
                }
            }
            "message_delta" => {
                if let Some(usage) = event.get("usage").and_then(|v| v.as_object()) {
                    // 必须**同步**置位，handler finally 才不会再写 partial
                    self.stats.usage_recorded = true;
                    let start = self.stats.stream_start_usage.clone().unwrap_or_default();
                    let mut final_u = FinalUsage::default();
                    final_u.input_tokens = usage
                        .get("input_tokens")
                        .and_then(|v| v.as_u64())
                        .or(start.input_tokens)
                        .unwrap_or(0);
                    final_u.cache_creation_tokens = usage
                        .get("cache_creation_input_tokens")
                        .and_then(|v| v.as_u64())
                        .or(start.cache_creation_input_tokens)
                        .unwrap_or(0);
                    final_u.cache_read_tokens = usage
                        .get("cache_read_input_tokens")
                        .and_then(|v| v.as_u64())
                        .or(start.cache_read_input_tokens)
                        .unwrap_or(0);
                    final_u.output_tokens =
                        usage.get("output_tokens").and_then(|v| v.as_u64()).unwrap_or(0);
                    self.stats.final_usage = Some(final_u);
                }
            }
            _ => {}
        }

        if !self.direct.skip_sse_sanitize() {
            let is_fast = self
                .stats
                .stream_model
                .as_deref()
                .map(|m| m.to_ascii_lowercase().contains("fast"))
                .unwrap_or(false);
            sanitize_sse_event(&mut event, is_fast);
        }

        out.push_str("data: ");
        out.push_str(&serde_json::to_string(&event).unwrap_or_default());
        out.push_str("\n\n");
    }
}

/// 流异常 / 客户端断开 / 没等到 message_delta 时的 partial 兜底。
/// 用 stream_start_usage 拼一个 output_tokens=0 的 FinalUsage 返回给 handler 落库。
pub fn fallback_partial_usage(stats: &SseStats) -> Option<FinalUsage> {
    if stats.usage_recorded {
        return None;
    }
    let start = stats.stream_start_usage.as_ref()?;
    Some(FinalUsage {
        input_tokens: start.input_tokens.unwrap_or(0),
        output_tokens: 0,
        cache_creation_tokens: start.cache_creation_input_tokens.unwrap_or(0),
        cache_read_tokens: start.cache_read_input_tokens.unwrap_or(0),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool() -> SseProcessor {
        SseProcessor::new(DirectFlags::SHARED_POOL)
    }

    #[test]
    fn passthrough_event_types_skip_parse() {
        let mut p = pool();
        let out = p.feed("event: ping\ndata: {}\n\n");
        assert!(out.contains("event: ping"));
        assert!(out.contains("data: {}"));
    }

    #[test]
    fn cross_chunk_message_delta_extracts_usage() {
        let mut p = pool();
        let chunk1 = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_x\",\"model\":\"claude-opus-4.6\",\"usage\":{\"input_tokens\":100,\"cache_read_input_tokens\":50}}}\n\n";
        let chunk2_part1 = "event: message_delta\ndata: {\"type\":\"message_delta\",";
        let chunk2_part2 = "\"usage\":{\"output_tokens\":42}}\n\n";
        p.feed(chunk1);
        p.feed(chunk2_part1);
        p.feed(chunk2_part2);
        assert!(p.stats().usage_recorded);
        let usage = p.stats().final_usage.as_ref().expect("usage");
        assert_eq!(usage.input_tokens, 100);
        assert_eq!(usage.output_tokens, 42);
        assert_eq!(usage.cache_read_tokens, 50);
    }

    #[test]
    fn buffer_residual_flushes_on_finish() {
        let mut p = pool();
        // 注意：最后一行没有终止 \n
        p.feed("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"x\",\"model\":\"claude-opus-4.6\",\"usage\":{\"input_tokens\":5}}}\n\n");
        p.feed("event: message_delta\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":7}}");
        let _ = p.finish();
        assert!(p.stats().usage_recorded);
        assert_eq!(p.stats().final_usage.as_ref().unwrap().output_tokens, 7);
    }

    #[test]
    fn non_whitelisted_event_dropped_in_pool_mode() {
        let mut p = pool();
        let out = p.feed("event: copilot_usage\ndata: {\"type\":\"copilot_usage\"}\n\n");
        assert!(!out.contains("copilot_usage"));
    }

    #[test]
    fn direct_mode_passes_unknown_events() {
        let mut p = SseProcessor::new(DirectFlags::PASS_THROUGH);
        let out = p.feed("event: copilot_usage\ndata: {\"type\":\"copilot_usage\",\"foo\":1}\n\n");
        assert!(out.contains("copilot_usage"));
    }

    #[test]
    fn comment_lines_dropped_in_pool_passthrough_in_direct() {
        let mut p = pool();
        let out_pool = p.feed(":heartbeat\n\n");
        assert!(!out_pool.contains("heartbeat"));

        let mut d = SseProcessor::new(DirectFlags::PASS_THROUGH);
        let out_direct = d.feed(":heartbeat\n\n");
        assert!(out_direct.contains("heartbeat"));
    }

    #[test]
    fn done_sentinel_forwarded() {
        let mut p = pool();
        let out = p.feed("data: [DONE]\n\n");
        assert!(out.contains("[DONE]"));
    }

    #[test]
    fn fallback_partial_uses_start_when_not_recorded() {
        let stats = SseStats {
            stream_start_usage: Some(StreamStartUsage {
                input_tokens: Some(7),
                cache_creation_input_tokens: None,
                cache_read_input_tokens: Some(3),
            }),
            ..Default::default()
        };
        let u = fallback_partial_usage(&stats).expect("some");
        assert_eq!(u.input_tokens, 7);
        assert_eq!(u.output_tokens, 0);
        assert_eq!(u.cache_read_tokens, 3);
    }

    #[test]
    fn fallback_partial_skips_when_recorded() {
        let stats = SseStats {
            usage_recorded: true,
            stream_start_usage: Some(StreamStartUsage::default()),
            ..Default::default()
        };
        assert!(fallback_partial_usage(&stats).is_none());
    }

    #[test]
    fn sanitize_runs_in_pool_mode() {
        let mut p = pool();
        let chunk = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_anthropic_x\",\"model\":\"claude-opus-4.6\",\"provider\":\"anthropic\"}}\n\n";
        let out = p.feed(chunk);
        // provider 应被剥掉，id 应被改写
        assert!(!out.contains("\"provider\""));
        assert!(out.contains("msg_bdrk_"));
    }

    #[test]
    fn sanitize_skipped_in_direct_mode() {
        let mut p = SseProcessor::new(DirectFlags::PASS_THROUGH);
        let chunk = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_xyz\",\"model\":\"claude-opus-4.6\",\"provider\":\"foo\"}}\n\n";
        let out = p.feed(chunk);
        assert!(out.contains("\"provider\""));
        assert!(out.contains("msg_xyz"));
    }
}
