//! 字节流行 / 事件边界查找。SSE 事件边界用 ``\n\n`` 或 ``\r\n\r\n``，
//! 行内逻辑用单字符 ``\n``。两条渠道的字节路径 SSE 解析共用本模块。

use memchr::{memchr, memmem};
use once_cell::sync::Lazy;

static FINDER_LF: Lazy<memmem::Finder<'static>> = Lazy::new(|| memmem::Finder::new(b"\n\n"));
static FINDER_CRLF: Lazy<memmem::Finder<'static>> =
    Lazy::new(|| memmem::Finder::new(b"\r\n\r\n"));

/// 找首个 SSE 事件边界。返回 ``(idx, delim_len)``：``idx`` 是分隔符起点，
/// ``delim_len`` 是分隔符长度（2 表示 ``\n\n``，4 表示 ``\r\n\r\n``）。
/// 两种分隔同时存在时取更靠前的。``\n\n`` 比 ``\r\n\r\n`` 早一字节也算 \n\n 路径，
/// 与 SSE spec 不冲突（spec 允许 CRLF 但服务端常发 LF）。
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

/// 找首个行边界 ``\n``。返回 ``(idx, delim_len=1)``。
/// 调用方自己处理 ``\r`` 尾巴（``strip_trailing_cr``）。
pub fn find_line_boundary(buf: &[u8]) -> Option<(usize, usize)> {
    memchr(b'\n', buf).map(|idx| (idx, 1))
}

/// 行尾 ``\r`` 剥除工具。空切片返回空切片。
pub fn strip_trailing_cr(line: &[u8]) -> &[u8] {
    if let Some((last, rest)) = line.split_last() {
        if *last == b'\r' {
            return rest;
        }
    }
    line
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_boundary_lf() {
        let buf = b"event: x\ndata: y\n\nrest";
        let (idx, dlen) = find_event_boundary(buf).expect("some");
        assert_eq!(&buf[idx..idx + dlen], b"\n\n");
    }

    #[test]
    fn event_boundary_crlf() {
        let buf = b"event: x\r\ndata: y\r\n\r\nrest";
        let (idx, dlen) = find_event_boundary(buf).expect("some");
        assert_eq!(&buf[idx..idx + dlen], b"\r\n\r\n");
    }

    #[test]
    fn event_boundary_picks_earlier() {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"a\n\nb\r\n\r\nc");
        let (idx, dlen) = find_event_boundary(&buf).expect("some");
        assert_eq!(&buf[idx..idx + dlen], b"\n\n");
    }

    #[test]
    fn event_boundary_none() {
        assert!(find_event_boundary(b"no terminator here").is_none());
        assert!(find_event_boundary(b"").is_none());
    }

    #[test]
    fn line_boundary_basic() {
        let buf = b"event: x\ndata: y\n";
        let (idx, dlen) = find_line_boundary(buf).expect("some");
        assert_eq!(idx, 8);
        assert_eq!(dlen, 1);
    }

    #[test]
    fn line_boundary_none() {
        assert!(find_line_boundary(b"no newline").is_none());
        assert!(find_line_boundary(b"").is_none());
    }

    #[test]
    fn strip_cr_removes_trailing() {
        assert_eq!(strip_trailing_cr(b"event: x\r"), b"event: x");
        assert_eq!(strip_trailing_cr(b"event: x"), b"event: x");
        assert_eq!(strip_trailing_cr(b""), b"");
    }
}
