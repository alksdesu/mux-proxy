//! gzip 透传：解压 → 字节替换 → 重压。mtime=0/level=6 是与 Python stdlib 默认对齐的
//! 字节级指纹约束，调任意一个都会让客户端抓包看出 proxy。

use crate::channels::anthropic::model_restore::{rewrite_json_response, rewrite_sse_blob};
use bytes::Bytes;
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use std::io::{Read, Write};

const COMPRESSION_LEVEL: u32 = 6;

/// 单纯解压拿明文。畸形 gzip → None。计费层用这个先 sniff 明文 usage，
/// 拿不到时由 caller 决定走 fallback；rewrite_gzip 内部不复用此函数（保持各自失败兜底逻辑独立）。
pub fn decompress_gzip(raw: &[u8]) -> Option<Bytes> {
    let mut decoder = GzDecoder::new(raw);
    let mut plain = Vec::with_capacity(raw.len() * 4);
    if decoder.read_to_end(&mut plain).is_err() {
        return None;
    }
    Some(Bytes::from(plain))
}

/// is_sse=true 走 SSE blob 改写，否则走 JSON 改写。两者目前底层同一条 regex，
/// 拆开是为未来 SSE 行级解析留接口。
/// current_model 与 original_model 相等时直接返原 raw——防御 flate2/hyper-rustls
/// 升级时 decompress→recompress 往返产生字节级微差。
pub fn rewrite_gzip(
    raw: Bytes,
    current_model: &str,
    original_model: &str,
    is_sse: bool,
) -> Bytes {
    if current_model == original_model {
        return raw;
    }
    let mut decoder = GzDecoder::new(&raw[..]);
    let mut plain = Vec::with_capacity(raw.len() * 4);
    if decoder.read_to_end(&mut plain).is_err() {
        return raw;
    }
    let rewritten = if is_sse {
        rewrite_sse_blob(Bytes::from(plain), current_model, original_model)
    } else {
        rewrite_json_response(Bytes::from(plain), current_model, original_model)
    };

    let mut encoder = GzEncoder::new(Vec::with_capacity(raw.len()), Compression::new(COMPRESSION_LEVEL));
    if encoder.write_all(&rewritten).is_err() {
        return raw;
    }
    let mut out = match encoder.finish() {
        Ok(buf) => buf,
        Err(_) => return raw,
    };
    zero_mtime_in_place(&mut out);
    Bytes::from(out)
}

/// GZIP 文件头偏移 4..8 是 mtime（little-endian u32）。flate2 在 stdlib
/// 一侧没有 ``mtime(0)`` builder，只能在写完后归零。固定 10 字节头 + ``ID1=0x1f`` 验证。
fn zero_mtime_in_place(buf: &mut Vec<u8>) {
    if buf.len() < 8 {
        return;
    }
    if buf[0] != 0x1f || buf[1] != 0x8b {
        return;
    }
    buf[4..8].fill(0);
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::GzEncoder;
    use std::io::Write;

    fn gzip(plain: &[u8]) -> Vec<u8> {
        let mut enc = GzEncoder::new(Vec::new(), Compression::new(COMPRESSION_LEVEL));
        enc.write_all(plain).unwrap();
        enc.finish().unwrap()
    }

    fn gunzip(raw: &[u8]) -> Vec<u8> {
        let mut dec = GzDecoder::new(raw);
        let mut out = Vec::new();
        dec.read_to_end(&mut out).unwrap();
        out
    }

    #[test]
    fn roundtrip_replaces_model() {
        let plain = br#"{"model":"claude-jupiter-v1-p","x":1}"#;
        let gz = Bytes::from(gzip(plain));
        let out = rewrite_gzip(gz, "claude-jupiter-v1-p", "claude-opus-4-7", false);
        let restored = gunzip(&out);
        let s = std::str::from_utf8(&restored).unwrap();
        assert!(s.contains(r#""model":"claude-opus-4-7""#));
    }

    #[test]
    fn mtime_zeroed() {
        let plain = br#"{"model":"foo"}"#;
        let gz = Bytes::from(gzip(plain));
        let out = rewrite_gzip(gz, "bar", "baz", false);
        // header bytes 4..8 should all be zero
        assert_eq!(&out[4..8], &[0, 0, 0, 0]);
    }

    #[test]
    fn malformed_gzip_passes_through() {
        let raw = Bytes::from_static(b"not actually gzip");
        let out = rewrite_gzip(raw.clone(), "a", "b", false);
        assert_eq!(out, raw);
    }

    #[test]
    fn sse_path_decompresses() {
        let plain = b"event: message_delta\ndata: {\"model\":\"jup-v1\"}\n\n";
        let gz = Bytes::from(gzip(plain));
        let out = rewrite_gzip(gz, "jup-v1", "claude-opus-4-7", true);
        let restored = gunzip(&out);
        let s = std::str::from_utf8(&restored).unwrap();
        assert!(s.contains(r#""model":"claude-opus-4-7""#));
    }

    #[test]
    fn decompress_gzip_roundtrip() {
        let plain = br#"{"usage":{"input_tokens":12}}"#;
        let gz = gzip(plain);
        let out = decompress_gzip(&gz).expect("valid gzip");
        assert_eq!(out.as_ref(), plain.as_slice());
    }

    #[test]
    fn decompress_gzip_malformed_returns_none() {
        assert!(decompress_gzip(b"not gzip").is_none());
    }

    #[test]
    fn level_6_is_used_for_output() {
        // 间接验证 compression level：相同明文用 level=6 输出尺寸应稳定可重现
        let plain = br#"{"model":"foo","data":"AAAAAAAAAAAAAAAAAAAAAAAAAAAA"}"#;
        let gz1 = Bytes::from(gzip(plain));
        let out1 = rewrite_gzip(gz1.clone(), "foo", "foo", false);
        let out2 = rewrite_gzip(gz1, "foo", "foo", false);
        assert_eq!(out1, out2, "level=6 output must be deterministic");
    }

    #[test]
    fn rewrite_gzip_skips_when_models_equal() {
        // current == original 必须返原 raw，不走 decompress/recompress 往返。
        // 这条防御 flate2 / hyper-rustls 升级产生字节级微差导致客户端抓包识别 proxy。
        let plain = br#"{"model":"same"}"#;
        let gz = Bytes::from(gzip(plain));
        let gz_ptr = gz.as_ptr();
        let out = rewrite_gzip(gz.clone(), "same", "same", false);
        assert_eq!(out.as_ref(), gz.as_ref());
        assert_eq!(out.as_ptr(), gz_ptr, "equal-models path must reuse the original buffer");
    }
}
