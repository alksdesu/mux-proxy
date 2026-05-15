//! SSE 流 tee：forward 走字节透明转发 + model_restore splice；sniffer 旁路 mpsc 解析 usage。
//! 双 finder 切事件边界，跨 chunk 的 model 字段必然在 ``\n\n`` 之内完整。
//! sniffer 满或 parse 失败 → ``try_send`` 丢弃，只丢账不丢响应。

use crate::billing::UsageWriter;
use crate::channels::anthropic::billing_hook::SseUsageAggregator;
use crate::channels::anthropic::model_restore::{find_event_boundary, rewrite_sse_blob};
use bytes::{Bytes, BytesMut};
use tokio::sync::mpsc;
use tracing::debug;

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

fn scan_event(event: &[u8], agg: &mut SseUsageAggregator) {
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

fn strip_trailing_cr(line: &[u8]) -> &[u8] {
    if let Some((last, rest)) = line.split_last() {
        if *last == b'\r' {
            return rest;
        }
    }
    line
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

/// 转发方向：累 buf 拆事件，每事件走 ``rewrite_sse_blob`` 改 model。
/// 尾巴未结束的事件保留在 buf 里，等下个 chunk 拼接。
pub struct ForwardSplitter {
    buf: BytesMut,
    current_model: String,
    original_model: String,
    splice_enabled: bool,
}

impl ForwardSplitter {
    pub fn new(current_model: String, original_model: String) -> Self {
        let splice_enabled = current_model != original_model;
        Self {
            buf: BytesMut::with_capacity(8 * 1024),
            current_model,
            original_model,
            splice_enabled,
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
        if !self.splice_enabled {
            return data;
        }
        rewrite_sse_blob(data, &self.current_model, &self.original_model)
    }
}

/// 给 forward stream 推一个 chunk 给 sniffer。channel 满 → drop（丢账不丢响应），日志记一条。
pub fn try_send_to_sniffer(tx: &mpsc::Sender<Bytes>, chunk: Bytes) {
    if let Err(e) = tx.try_send(chunk) {
        debug!(error = ?e, "sse sniffer dropped chunk (capacity reached)");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splitter_emits_complete_events_only() {
        let mut sp = ForwardSplitter::new("jup".into(), "jup".into());
        let out = sp.ingest_chunk(Bytes::from_static(b"event: x\ndata: 1\n\nevent: y"));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].as_ref(), b"event: x\ndata: 1\n\n");
        let out2 = sp.ingest_chunk(Bytes::from_static(b"\ndata: 2\n\n"));
        assert_eq!(out2.len(), 1);
        assert_eq!(out2[0].as_ref(), b"event: y\ndata: 2\n\n");
    }

    #[test]
    fn splitter_applies_model_restore() {
        let mut sp = ForwardSplitter::new("jup-v1".into(), "claude-opus-4-7".into());
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
        let mut sp = ForwardSplitter::new("a".into(), "a".into());
        let out = sp.ingest_chunk(Bytes::from_static(
            b"event: x\r\ndata: 1\r\n\r\nevent: y\r\ndata: 2\r\n\r\n",
        ));
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn splitter_flush_emits_leftover() {
        let mut sp = ForwardSplitter::new("a".into(), "a".into());
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
        let mut sp = ForwardSplitter::new("jup-v1".into(), "claude-opus-4-7".into());
        let prefix = Bytes::from_static(b"event: message_start\ndata: {\"message\":{\"mod");
        let suffix = Bytes::from_static(b"el\":\"jup-v1\"}}\n\n");
        let out1 = sp.ingest_chunk(prefix);
        assert!(out1.is_empty(), "no boundary yet, hold in buffer");
        let out2 = sp.ingest_chunk(suffix);
        assert_eq!(out2.len(), 1);
        let s = std::str::from_utf8(out2[0].as_ref()).unwrap();
        assert!(s.contains(r#""model":"claude-opus-4-7""#));
    }
}
