//! Anthropic 上游 key 解析。两种合法形态：
//! `anthropic:sk-ant-xxx` 与裸 `sk-ant-xxx`。其它一律拒绝，避免误归到本渠道。

use crate::error::{AppError, AppResult};

const PREFIX_WITH_TAG: &str = "anthropic:";
const SK_ANT_PREFIX: &str = "sk-ant-";

/// 渠道入站 key 拆解结果。``token`` 永远是裸 `sk-ant-xxx`，
/// 直接当 ``x-api-key`` 请求头发往 api.anthropic.com。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnthropicUpstreamKey {
    pub token: String,
}

impl AnthropicUpstreamKey {
    pub fn token(&self) -> &str {
        &self.token
    }
}

/// 从 admin 配置串里解析出 token。空白前后 trim 之后必须以 ``sk-ant-`` 开头。
pub fn parse(raw: &str) -> AppResult<AnthropicUpstreamKey> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(AppError::BadRequest("empty anthropic upstream key".into()));
    }

    let token = if let Some(rest) = trimmed.strip_prefix(PREFIX_WITH_TAG) {
        rest.trim()
    } else {
        trimmed
    };

    if !token.starts_with(SK_ANT_PREFIX) {
        return Err(AppError::BadRequest(
            "anthropic upstream key must start with sk-ant-".into(),
        ));
    }
    if token.len() < SK_ANT_PREFIX.len() + 8 {
        return Err(AppError::BadRequest(
            "anthropic upstream key too short to be valid".into(),
        ));
    }

    Ok(AnthropicUpstreamKey {
        token: token.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_with_anthropic_tag() {
        let k = parse("anthropic:sk-ant-api03-abcd1234").unwrap();
        assert_eq!(k.token(), "sk-ant-api03-abcd1234");
    }

    #[test]
    fn parse_bare_sk_ant() {
        let k = parse("sk-ant-api03-abcd1234").unwrap();
        assert_eq!(k.token(), "sk-ant-api03-abcd1234");
    }

    #[test]
    fn parse_trims_whitespace() {
        let k = parse("   sk-ant-api03-abcd1234\n").unwrap();
        assert_eq!(k.token(), "sk-ant-api03-abcd1234");
    }

    #[test]
    fn parse_rejects_empty() {
        assert!(parse("").is_err());
        assert!(parse("   ").is_err());
    }

    #[test]
    fn parse_rejects_non_sk_ant() {
        assert!(parse("ghp_xxxxxxxx").is_err());
        assert!(parse("anthropic:ghp_xxxxxxxx").is_err());
        assert!(parse("enterprise:ghp_xxxxxxxx").is_err());
    }

    #[test]
    fn parse_rejects_too_short() {
        assert!(parse("sk-ant-x").is_err());
    }
}
