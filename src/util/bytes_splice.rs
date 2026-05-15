//! 预分配单次 splice。响应清洗时需要在 JSON / SSE 字节流里替换 model 名等
//! 小段内容；按总长度一次性分配 BytesMut，三段 put_slice 单次写入，避免反复 to_vec 拼接。

use bytes::{BufMut, Bytes, BytesMut};
use std::ops::Range;

/// 把 `src[range]` 替换为 `replacement`，返回新的 `Bytes`。
///
/// # Panics
/// `range.end < range.start` 或 `range.end > src.len()` 时 panic —— 上游传递
/// 错位区间属于编程错误，不静默吞。
pub fn splice_bytes(src: &[u8], range: Range<usize>, replacement: &[u8]) -> Bytes {
    assert!(range.start <= range.end, "splice range reversed");
    assert!(range.end <= src.len(), "splice range out of bounds");

    let head_len = range.start;
    let tail_start = range.end;
    let tail_len = src.len() - tail_start;
    let total = head_len + replacement.len() + tail_len;

    let mut buf = BytesMut::with_capacity(total);
    buf.put_slice(&src[..head_len]);
    buf.put_slice(replacement);
    buf.put_slice(&src[tail_start..]);
    buf.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splice_middle() {
        let out = splice_bytes(b"hello world", 6..11, b"rust!");
        assert_eq!(&out[..], b"hello rust!");
    }

    #[test]
    fn splice_head() {
        let out = splice_bytes(b"hello world", 0..5, b"howdy");
        assert_eq!(&out[..], b"howdy world");
    }

    #[test]
    fn splice_tail() {
        let out = splice_bytes(b"hello world", 5..11, b"!");
        assert_eq!(&out[..], b"hello!");
    }

    #[test]
    fn splice_empty_replacement() {
        let out = splice_bytes(b"hello world", 5..6, b"");
        assert_eq!(&out[..], b"helloworld");
    }

    #[test]
    fn splice_into_empty() {
        let out = splice_bytes(b"", 0..0, b"new");
        assert_eq!(&out[..], b"new");
    }

    #[test]
    fn splice_zero_width_in_middle() {
        let out = splice_bytes(b"abcd", 2..2, b"XY");
        assert_eq!(&out[..], b"abXYcd");
    }

    #[test]
    fn splice_full_replace() {
        let out = splice_bytes(b"abcd", 0..4, b"XYZ");
        assert_eq!(&out[..], b"XYZ");
    }

    #[test]
    #[should_panic(expected = "splice range out of bounds")]
    fn splice_oob_panics() {
        splice_bytes(b"abc", 0..10, b"");
    }

    #[test]
    #[should_panic(expected = "splice range reversed")]
    fn splice_reversed_panics() {
        let _ = splice_bytes(b"abcd", 3..1, b"");
    }
}
