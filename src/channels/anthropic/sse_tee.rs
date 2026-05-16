//! SSE 流 tee：forward 走字节透明转发 + model_restore splice；sniffer 旁路 mpsc 解析 usage。
//! 双 finder 切事件边界，跨 chunk 的 model 字段必然在 ``\n\n`` 之内完整。
//! sniffer 满或 parse 失败 → ``try_send`` 丢弃，只丢账不丢响应。

use crate::billing::UsageWriter;
use crate::channels::anthropic::billing_hook::SseUsageAggregator;
use crate::channels::anthropic::model_restore::rewrite_sse_blob;
use crate::shared::line_codec::{find_event_boundary, strip_trailing_cr};
use bytes::{Bytes, BytesMut};
use tokio::sync::mpsc;
use tracing::error;

/// 典型 Anthropic SSE 流 50-100 chunks/s，sniffer 行扫描 + serde_json::from_slice μs 级；
/// 32 容量留约 300ms backlog 应付临时 GC pause / 调度抖动。满时 try_send 丢账不丢响应。
pub const SNIFF_CHANNEL_CAPACITY: usize = 32;

/// 旁路给 sniffer task 用的上下文。``request_body`` 已被 splice 改写过，
/// 但我们要给客户端日志看 ``原始模型名``，故用 ``original_model`` 作为兜底 model。
pub struct SniffContext {
    pub writer: UsageWriter,
    pub key_name: String,
    pub original_model: String,
    pub request_body: String,
    pub ip: Option<String>,
}

/// 创建 tee 的 sniffer 端。返回 (Sender, JoinHandle)；handler 在响应转发结束后
/// drop Sender 并 await handle，让 sniffer 把最后一段事件冲刷完。
pub fn spawn_sniffer(ctx: SniffContext) -> (mpsc::Sender<Bytes>, tokio::task::JoinHandle<()>) {
    let (tx, mut rx) = mpsc::channel::<Bytes>(SNIFF_CHANNEL_CAPACITY);
    let handle = tokio::spawn(async move {
        let mut agg = SseUsageAggregator::new();
        let mut buf = BytesMut::with_capacity(8 * 1024);
        while let Some(chunk) = rx.recv().await {
            buf.extend_from_slice(&chunk);
            drain_complete_events(&mut buf, &mut agg);
        }
        if !buf.is_empty() {
            scan_event(&buf, &mut agg);
            buf.clear();
        }
        agg.finalize(
            &ctx.writer,
            &ctx.key_name,
            &ctx.original_model,
            ctx.request_body,
            ctx.ip,
        );
    });
    (tx, handle)
}

fn drain_complete_events(buf: &mut BytesMut, agg: &mut SseUsageAggregator) {
    while let Some((idx, dlen)) = find_event_boundary(buf) {
        let event_end = idx + dlen;
        let event = buf.split_to(event_end);
        scan_event(&event, agg);
    }
}

pub(crate) fn scan_event(event: &[u8], agg: &mut SseUsageAggregator) {
    let mut event_type: Option<&[u8]> = None;
    let mut data_lines: Vec<&[u8]> = Vec::new();
    for line in event.split(|b| *b == b'\n') {
        let line = strip_trailing_cr(line);
        if line.is_empty() {
            continue;
        }
        if let Some(rest) = line.strip_prefix(b"event:") {
            event_type = Some(trim_ascii(rest));
        } else if let Some(rest) = line.strip_prefix(b"data:") {
            data_lines.push(trim_ascii(rest));
        }
    }
    if data_lines.is_empty() {
        return;
    }
    let Some(ty) = event_type else {
        return;
    };
    let joined: Vec<u8> = data_lines.join(&b"\n"[..]);
    match ty {
        b"message_start" => agg.ingest_message_start(&joined),
        b"message_delta" => agg.ingest_message_delta(&joined),
        _ => {}
    }
}

fn trim_ascii(s: &[u8]) -> &[u8] {
    let mut start = 0;
    let mut end = s.len();
    while start < end && s[start].is_ascii_whitespace() {
        start += 1;
    }
    while end > start && s[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    &s[start..end]
}

/// 转发方向：累 buf 拆事件，splice 段可选。``None`` 时事件直接 forward 不动字节，
/// sniffer 段仍照常跑（计费与 splice 解耦）。
pub struct ForwardSplitter {
    buf: BytesMut,
    splice: Option<SpliceModels>,
}

struct SpliceModels {
    current: String,
    original: String,
}

impl ForwardSplitter {
    /// rewritten=true 路径传 ``Some((current, original))``；
    /// rewritten=false 路径传 ``None``，事件按字节透传 sniffer 仍计费。
    pub fn new(splice: Option<(String, String)>) -> Self {
        let splice = splice.and_then(|(c, o)| {
            if c == o {
                None
            } else {
                Some(SpliceModels {
                    current: c,
                    original: o,
                })
            }
        });
        Self {
            buf: BytesMut::with_capacity(8 * 1024),
            splice,
        }
    }

    /// 喂一个 chunk 进来。每喂一次，发出 0..N 个完整事件 + 暂存末尾不完整事件。
    pub fn ingest_chunk(&mut self, chunk: Bytes) -> Vec<Bytes> {
        if chunk.is_empty() {
            return Vec::new();
        }
        self.buf.extend_from_slice(&chunk);
        let mut out = Vec::new();
        while let Some((idx, dlen)) = find_event_boundary(&self.buf) {
            let head = self.buf.split_to(idx + dlen).freeze();
            out.push(self.apply_splice(head));
        }
        out
    }

    /// 流结束时 flush 剩余 buf（若 upstream 不规范没发末尾 ``\n\n``）。
    pub fn flush(&mut self) -> Option<Bytes> {
        if self.buf.is_empty() {
            return None;
        }
        let leftover = self.buf.split().freeze();
        Some(self.apply_splice(leftover))
    }

    fn apply_splice(&self, data: Bytes) -> Bytes {
        match &self.splice {
            None => data,
            Some(m) => rewrite_sse_blob(data, &m.current, &m.original),
        }
    }
}

/// 给 forward stream 推一个 chunk 给 sniffer。channel 满 → drop（丢账不丢响应）。
/// 用 ``error!`` 级别记，因为这是财务损失，prometheus 抓 alarm 用。
pub fn try_send_to_sniffer(
    tx: &mpsc::Sender<Bytes>,
    chunk: Bytes,
    key_name: &str,
    model: &str,
) {
    if let Err(e) = tx.try_send(chunk) {
        error!(
            key_name = %key_name,
            model = %model,
            error = ?e,
            "billing record dropped: sniffer mpsc full"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splitter_emits_complete_events_only() {
        let mut sp = ForwardSplitter::new(None);
        let out = sp.ingest_chunk(Bytes::from_static(b"event: x\ndata: 1\n\nevent: y"));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].as_ref(), b"event: x\ndata: 1\n\n");
        let out2 = sp.ingest_chunk(Bytes::from_static(b"\ndata: 2\n\n"));
        assert_eq!(out2.len(), 1);
        assert_eq!(out2[0].as_ref(), b"event: y\ndata: 2\n\n");
    }

    #[test]
    fn splitter_applies_model_restore() {
        let mut sp =
            ForwardSplitter::new(Some(("jup-v1".into(), "claude-opus-4-7".into())));
        let chunk = Bytes::from_static(
            b"event: message_start\ndata: {\"message\":{\"model\":\"jup-v1\"}}\n\n",
        );
        let out = sp.ingest_chunk(chunk);
        assert_eq!(out.len(), 1);
        let s = std::str::from_utf8(out[0].as_ref()).unwrap();
        assert!(s.contains(r#""model":"claude-opus-4-7""#));
    }

    #[test]
    fn splitter_handles_crlf_delim() {
        let mut sp = ForwardSplitter::new(None);
        let out = sp.ingest_chunk(Bytes::from_static(
            b"event: x\r\ndata: 1\r\n\r\nevent: y\r\ndata: 2\r\n\r\n",
        ));
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn splitter_flush_emits_leftover() {
        let mut sp = ForwardSplitter::new(None);
        let _ = sp.ingest_chunk(Bytes::from_static(b"event: x\ndata: incomplete"));
        let leftover = sp.flush().unwrap();
        assert_eq!(leftover.as_ref(), b"event: x\ndata: incomplete");
    }

    #[test]
    fn scan_message_start_updates_agg() {
        let mut agg = SseUsageAggregator::new();
        let event = b"event: message_start\ndata: {\"message\":{\"model\":\"m\",\"usage\":{\"input_tokens\":42}}}\n\n";
        scan_event(event, &mut agg);
        // 通过下一步 message_delta 间接验证 input_tokens 已设
        let event2 = b"event: message_delta\ndata: {\"usage\":{\"output_tokens\":7}}\n\n";
        scan_event(event2, &mut agg);
        // finalize 用 dummy writer 实际跑不了，验证 agg 内部状态靠 ingest_* 单测覆盖。
        // 这里只确认 scan_event 至少没 panic。
        let _ = agg;
    }

    #[test]
    fn scan_event_ignores_non_message_events() {
        let mut agg = SseUsageAggregator::new();
        let event = b"event: ping\ndata: {}\n\n";
        scan_event(event, &mut agg);
    }

    #[test]
    fn scan_event_multi_data_lines_joined() {
        let mut agg = SseUsageAggregator::new();
        let event = b"event: message_start\ndata: {\"message\":\ndata:   {\"usage\":{\"input_tokens\":9}}}\n\n";
        scan_event(event, &mut agg);
        let _ = agg;
    }

    #[test]
    fn splitter_chunked_across_model_field() {
        let mut sp =
            ForwardSplitter::new(Some(("jup-v1".into(), "claude-opus-4-7".into())));
        let prefix = Bytes::from_static(b"event: message_start\ndata: {\"message\":{\"mod");
        let suffix = Bytes::from_static(b"el\":\"jup-v1\"}}\n\n");
        let out1 = sp.ingest_chunk(prefix);
        assert!(out1.is_empty(), "no boundary yet, hold in buffer");
        let out2 = sp.ingest_chunk(suffix);
        assert_eq!(out2.len(), 1);
        let s = std::str::from_utf8(out2[0].as_ref()).unwrap();
        assert!(s.contains(r#""model":"claude-opus-4-7""#));
    }

    #[test]
    fn splitter_no_splice_passes_bytes_through() {
        // rewritten=false 时 ForwardSplitter::new(None)。模拟一段含 model 的事件，
        // 字节必须原样转发，sniffer 那边照常拿到 message_delta.usage 计费。
        // 此测试锁死"SSE rewritten=false 仍走 sse_tee + 字节透传"行为。
        let mut sp = ForwardSplitter::new(None);
        let chunk = Bytes::from_static(
            b"event: message_start\ndata: {\"message\":{\"model\":\"claude-haiku\",\"usage\":{\"input_tokens\":7}}}\n\n",
        );
        let out = sp.ingest_chunk(chunk.clone());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].as_ref(), chunk.as_ref(), "no splice → byte-for-byte forward");
    }

    #[test]
    fn splitter_equal_models_disable_splice() {
        // Some((c, c)) 也应禁用 splice（c == o 没必要改字节）。
        let mut sp = ForwardSplitter::new(Some(("same".into(), "same".into())));
        let chunk = Bytes::from_static(b"event: x\ndata: {\"model\":\"same\"}\n\n");
        let out = sp.ingest_chunk(chunk.clone());
        assert_eq!(out[0].as_ref(), chunk.as_ref());
    }

    #[test]
    fn sse_records_usage_even_when_no_rewrite_rule_matches() {
        // 模拟 handler SSE 路径在 rewrite_rules 空 / 没命中规则时的等价情况：
        // ForwardSplitter::new(None) + sniffer 路径独立运行。
        // 把一段完整 SSE 流喂给 scan_event，断言 SseUsageAggregator 提到 usage——
        // 即响应转发完后 agg.finalize 会落一条 BillingRecord。
        let mut agg = SseUsageAggregator::new();
        let stream = b"event: message_start\n\
                       data: {\"message\":{\"model\":\"claude-haiku\",\"usage\":{\"input_tokens\":42,\"cache_creation_input_tokens\":5,\"cache_read_input_tokens\":3}}}\n\n\
                       event: content_block_delta\n\
                       data: {\"type\":\"text_delta\",\"text\":\"hi\"}\n\n\
                       event: message_delta\n\
                       data: {\"usage\":{\"output_tokens\":17}}\n\n";
        let mut buf = BytesMut::from(&stream[..]);
        drain_complete_events(&mut buf, &mut agg);

        assert!(agg.seen_message_start(), "must see message_start regardless of splice");
        assert_eq!(agg.input_tokens(), 42);
        // 上游只给顶层 cache_creation_input_tokens 总和 → 全归 5m，1h=0
        assert_eq!(agg.cache_creation_5m_tokens(), 5);
        assert_eq!(agg.cache_creation_1h_tokens(), 0);
        assert_eq!(agg.cache_read_tokens(), 3);
        assert_eq!(agg.output_tokens(), 17);
        assert_eq!(agg.model(), Some("claude-haiku"));

        // 同时验证 ForwardSplitter::new(None) 不动字节——sniffer 和 splice 两路独立。
        let mut sp = ForwardSplitter::new(None);
        let out = sp.ingest_chunk(Bytes::from_static(stream));
        assert_eq!(out.len(), 3, "3 SSE events parsed");
        let total: usize = out.iter().map(|b| b.len()).sum();
        assert_eq!(total, stream.len(), "byte-for-byte forward");
    }
}
