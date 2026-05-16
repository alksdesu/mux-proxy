//! 字节级响应 ``"model"`` 字段还原：把上游真实 current_model 改回客户端原本写的
//! original_model。正则按 current_model 编译并 DashMap 缓存避免每响应重编译。

use bytes::Bytes;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use regex::bytes::Regex;
use std::borrow::Cow;
use std::sync::Arc;

pub use crate::shared::line_codec::find_event_boundary;

static RESTORE_CACHE: Lazy<DashMap<String, Arc<Regex>>> = Lazy::new(DashMap::new);

fn restore_regex(current_model: &str) -> Arc<Regex> {
    if let Some(hit) = RESTORE_CACHE.get(current_model) {
        return hit.clone();
    }
    let pattern = format!(
        r#"("model"\s*:\s*"){}(")"#,
        regex::escape(current_model)
    );
    let compiled = Arc::new(
        Regex::new(&pattern).expect("escaped pattern always compiles"),
    );
    RESTORE_CACHE.insert(current_model.to_string(), compiled.clone());
    compiled
}

/// 整段 JSON 体的还原。``current==original`` 或无匹配时返原 body 共享底层 buffer；
/// 仅在真有替换发生（Cow::Owned）时把新 Vec 转 Bytes。
pub fn rewrite_json_response(
    body: Bytes,
    current_model: &str,
    original_model: &str,
) -> Bytes {
    if body.is_empty() || current_model == original_model {
        return body;
    }
    let re = restore_regex(current_model);
    let original_bytes = original_model.as_bytes();
    let replaced = re.replace_all(&body, |caps: &regex::bytes::Captures| {
        let mut buf = Vec::with_capacity(
            caps[1].len() + original_bytes.len() + caps[2].len(),
        );
        buf.extend_from_slice(&caps[1]);
        buf.extend_from_slice(original_bytes);
        buf.extend_from_slice(&caps[2]);
        buf
    });
    match replaced {
        Cow::Borrowed(_) => body,
        Cow::Owned(v) => Bytes::from(v),
    }
}

/// SSE blob 的还原。一个 blob 可能包含多个事件，但所有事件都按 ``"model":"X"``
/// 字符模式找——和 JSON 路径同一条 regex，逻辑等价。
pub fn rewrite_sse_blob(
    blob: Bytes,
    current_model: &str,
    original_model: &str,
) -> Bytes {
    rewrite_json_response(blob, current_model, original_model)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restore_json_body() {
        let body = Bytes::from_static(br#"{"model":"claude-jupiter-v1-p","x":1}"#);
        let out = rewrite_json_response(body, "claude-jupiter-v1-p", "claude-opus-4-7");
        let s = std::str::from_utf8(&out).unwrap();
        assert!(s.contains(r#""model":"claude-opus-4-7""#));
        assert!(s.contains(r#""x":1"#));
    }

    #[test]
    fn restore_skips_when_equal() {
        let body = Bytes::from_static(br#"{"model":"claude-opus-4-7"}"#);
        let same = body.clone();
        let out = rewrite_json_response(body, "claude-opus-4-7", "claude-opus-4-7");
        assert_eq!(out, same);
    }

    #[test]
    fn restore_handles_repeated_field_in_sse_blob() {
        let blob = Bytes::from_static(
            b"event: message_start\n\
              data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-jupiter-v1-p\"}}\n\n\
              event: message_delta\n\
              data: {\"type\":\"message_delta\",\"model\":\"claude-jupiter-v1-p\"}\n\n",
        );
        let out = rewrite_sse_blob(blob, "claude-jupiter-v1-p", "claude-opus-4-7");
        let s = std::str::from_utf8(&out).unwrap();
        assert_eq!(s.matches(r#""model":"claude-opus-4-7""#).count(), 2);
        assert!(!s.contains("jupiter"));
    }

    #[test]
    fn restore_special_chars_escaped() {
        let body = Bytes::from_static(br#"{"model":"a+b/c=d"}"#);
        let out = rewrite_json_response(body, "a+b/c=d", "orig");
        let s = std::str::from_utf8(&out).unwrap();
        assert_eq!(s, r#"{"model":"orig"}"#);
    }

    #[test]
    fn no_match_returns_original_buffer_without_copy() {
        // current 与 body 里的 model 不匹配 → regex 返 Cow::Borrowed → 必须复用原 Bytes 不拷贝
        let body = Bytes::from_static(br#"{"model":"claude-haiku","x":1}"#);
        let body_ptr = body.as_ptr();
        let out = rewrite_json_response(body.clone(), "not-in-body", "ignored");
        assert_eq!(out.as_ref(), body.as_ref());
        assert_eq!(out.as_ptr(), body_ptr, "no-match path must reuse the original buffer");
    }

    #[test]
    fn cache_returns_same_regex() {
        let r1 = restore_regex("claude-jupiter-v1-p");
        let r2 = restore_regex("claude-jupiter-v1-p");
        assert!(Arc::ptr_eq(&r1, &r2));
    }

    #[test]
    fn find_event_boundary_lf() {
        let buf = b"event: x\ndata: y\n\nrest";
        let (idx, dlen) = find_event_boundary(buf).unwrap();
        assert_eq!(&buf[idx..idx + dlen], b"\n\n");
    }

    #[test]
    fn find_event_boundary_crlf() {
        let buf = b"event: x\r\ndata: y\r\n\r\nrest";
        let (idx, dlen) = find_event_boundary(buf).unwrap();
        assert_eq!(&buf[idx..idx + dlen], b"\r\n\r\n");
    }

    #[test]
    fn find_event_boundary_picks_earlier_of_mixed() {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"line\n\n");
        buf.extend_from_slice(b"x\r\n\r\n");
        let (idx, dlen) = find_event_boundary(&buf).unwrap();
        assert_eq!(idx, 4);
        assert_eq!(dlen, 2);
    }

    #[test]
    fn find_event_boundary_none() {
        assert!(find_event_boundary(b"no terminator here").is_none());
        assert!(find_event_boundary(b"").is_none());
    }
}
