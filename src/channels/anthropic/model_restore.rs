//! 字节级响应 ``"model"`` 字段还原：把上游真实 current_model 改回客户端原本写的
//! original_model。正则按 current_model 编译并 DashMap 缓存避免每响应重编译。

use bytes::Bytes;
use dashmap::DashMap;
use memchr::memmem;
use once_cell::sync::Lazy;
use regex::bytes::Regex;
use std::sync::Arc;

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

/// 整段 JSON 体的还原。``current==original`` 时直接返原 body。
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
    Bytes::copy_from_slice(replaced.as_ref())
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

static FINDER_LF: Lazy<memmem::Finder<'static>> =
    Lazy::new(|| memmem::Finder::new(b"\n\n"));
static FINDER_CRLF: Lazy<memmem::Finder<'static>> =
    Lazy::new(|| memmem::Finder::new(b"\r\n\r\n"));

/// 找首个 SSE 事件边界。返回 ``(idx, delim_len)``；找不到返回 None。
/// 双 finder 走 SIMD，比 regex 快得多。
pub fn find_event_boundary(buf: &[u8]) -> Option<(usize, usize)> {
    let a = FINDER_LF.find(buf);
    let b = FINDER_CRLF.find(buf);
    match (a, b) {
        (Some(i), Some(j)) if i <= j => Some((i, 2)),
        (Some(_), Some(j)) => Some((j, 4)),
        (Some(i), None) => Some((i, 2)),
        (None, Some(j)) => Some((j, 4)),
        (None, None) => None,
    }
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
