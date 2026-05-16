//! 通用 ``"model"`` 字段字节级提取。两个渠道都可能要从 JSON 请求体里看
//! 客户端发的 model 名做计费 / 白名单 / 路由判断，但 anthropic 渠道还要保
//! HMAC thinking 块字节不变，所以一律用字节正则而不是 ``serde_json::Value`` 解析。
//!
//! anthropic 渠道的 model splice 重写也复用 ``MODEL_FIELD`` 这条 regex，
//! 通过 ``model_field_regex()`` 暴露给字节替换路径。

use once_cell::sync::Lazy;
use regex::bytes::Regex;

static MODEL_FIELD: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"("model"\s*:\s*")([^"]+)(")"#).expect("MODEL_FIELD compiles"));

/// 跨渠道暴露的 regex 引用，给字节级 splice / 替换用。
pub fn model_field_regex() -> &'static Regex {
    &MODEL_FIELD
}

/// 在 JSON 请求体里抽 ``"model"`` 字段值。空 body / 非 JSON content-type /
/// UTF-8 解码失败均返 None。仅取首处匹配（顶层 model 字段）。
pub fn extract_model_field(body: &[u8], content_type: &str) -> Option<String> {
    if body.is_empty() {
        return None;
    }
    if !content_type.to_ascii_lowercase().contains("application/json") {
        return None;
    }
    let caps = MODEL_FIELD.captures(body)?;
    let value_match = caps.get(2)?;
    std::str::from_utf8(value_match.as_bytes())
        .ok()
        .map(String::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_basic() {
        let body = br#"{"model":"claude-opus-4-7","x":1}"#;
        assert_eq!(
            extract_model_field(body, "application/json"),
            Some("claude-opus-4-7".to_string())
        );
    }

    #[test]
    fn extract_skips_non_json() {
        let body = br#"{"model":"x"}"#;
        assert_eq!(extract_model_field(body, "text/plain"), None);
    }

    #[test]
    fn extract_handles_whitespace_around_colon() {
        let body = br#"{"model"  :  "claude-haiku"}"#;
        assert_eq!(
            extract_model_field(body, "application/json"),
            Some("claude-haiku".to_string())
        );
    }

    #[test]
    fn extract_returns_first_match_only() {
        let body = br#"{"model":"first","other":{"model":"second"}}"#;
        assert_eq!(
            extract_model_field(body, "application/json"),
            Some("first".to_string())
        );
    }

    #[test]
    fn extract_empty_body() {
        assert_eq!(extract_model_field(b"", "application/json"), None);
    }
}
