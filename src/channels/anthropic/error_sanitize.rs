//! Anthropic 渠道响应 leak 扫描：检测红队 key 暴露身份的特征关键词。
//! 命中后整段响应替换为通用 400 model_not_supported；上游原始字节走 DB 仅供 admin 排查。
//!
//! 设计上仅扫描已 buffered + 解压后的明文，**不在** SSE 流 chunk 上做：red-team key
//! 的拒绝文案以 HTTP 4xx 一次性 JSON 形式返回，从未在流式响应中观察到。如果未来上游
//! 改用流式回错，这里需要补 SSE 路径的扫描，触发条件可由 metrics 监控。

use bytes::Bytes;

/// 触发 leak 重写的关键词清单（lowercase substr 匹配）。
/// 关键词来自红队 key 上游对未授权 model 的标准回退文案，**任意命中一条** 即视为暴露身份。
/// 全部小写存储是为了在比对前对响应体做一次性 lowercase，避免逐 pattern 重复转换。
pub const LEAK_PATTERNS: &[&str] = &[
    "red teaming",
    "bug bounty",
    "please use your specified alias",
    "anthropic's bug bounty program",
];

/// 扫描 plain bytes 是否命中任意 leak 关键词。非 UTF-8 数据视为不命中
/// （二进制响应不可能是 anthropic JSON 错误，跳过最安全）。
pub fn contains_leak(plain: &[u8]) -> bool {
    let s = match std::str::from_utf8(plain) {
        Ok(s) => s,
        Err(_) => return false,
    };
    let lower = s.to_ascii_lowercase();
    LEAK_PATTERNS.iter().any(|p| lower.contains(p))
}

/// 命中 leak 时返回给客户端的响应体。与 Anthropic 官方 invalid_request_error 同构，
/// 客户端层面无法区分这是预过滤拒绝、上游 alias 错误、还是其它 model 不支持的 400。
pub fn model_not_supported_body() -> Bytes {
    Bytes::from_static(
        br#"{"type":"error","error":{"type":"invalid_request_error","message":"model not supported"}}"#,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_red_teaming_phrase() {
        let body = br#"{"error":{"message":"Please use your specified alias for red teaming."}}"#;
        assert!(contains_leak(body));
    }

    #[test]
    fn detects_bug_bounty_phrase() {
        let body = br#"{"error":{"message":"Reach out via Anthropic's bug bounty program."}}"#;
        assert!(contains_leak(body));
    }

    #[test]
    fn case_insensitive_match() {
        let body = br#"{"error":{"message":"RED TEAMING IS FORBIDDEN"}}"#;
        assert!(contains_leak(body));
    }

    #[test]
    fn benign_body_does_not_match() {
        let body = br#"{"id":"msg_x","content":[{"type":"text","text":"hello"}]}"#;
        assert!(!contains_leak(body));
    }

    #[test]
    fn invalid_utf8_does_not_panic() {
        let mut raw = vec![0xff, 0xfe];
        raw.extend_from_slice(b"red teaming");
        // 整体 utf8 解析失败 → 不命中（安全侧倾向：宁可漏一条也别误报）
        assert!(!contains_leak(&raw));
    }

    #[test]
    fn standard_body_format_is_anthropic_shape() {
        let body = model_not_supported_body();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["type"], "error");
        assert_eq!(parsed["error"]["type"], "invalid_request_error");
        assert_eq!(parsed["error"]["message"], "model not supported");
    }
}
