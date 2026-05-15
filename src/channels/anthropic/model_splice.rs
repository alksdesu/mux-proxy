//! 字节级请求体 ``"model"`` 字段替换。多轮 thinking 块含 HMAC 签名，
//! 任何 JSON parse/dump 都会改字节顺序或 escape，把签名搞废。

use bytes::Bytes;
use once_cell::sync::Lazy;
use regex::bytes::Regex;

/// 单条改写规则：prefix 串前缀匹配则替换为 target。``str::starts_with`` 大小写敏感。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RewriteRule {
    pub prefix: String,
    pub target: String,
}

impl RewriteRule {
    pub fn new(prefix: impl Into<String>, target: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
            target: target.into(),
        }
    }

    pub fn matches(&self, model: &str) -> bool {
        model.starts_with(&self.prefix)
    }
}

/// 解析 ``MUX_ANTHROPIC_REWRITE_RULES`` env 串：``prefix1=target1,prefix2=target2``。
/// 逗号分割条目、等号分割 prefix/target。空字符串/纯空白 → Ok(空 Vec)。
/// 任何条目缺 ``=``、prefix 空、target 空 → 返 Err 让 Config::from_env 启动 fatal。
pub fn parse_rewrite_rules(spec: &str) -> Result<Vec<RewriteRule>, String> {
    let trimmed = spec.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for entry in trimmed.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let Some((prefix, target)) = entry.split_once('=') else {
            return Err(format!("rewrite rule missing '=': {entry:?}"));
        };
        let prefix = prefix.trim();
        let target = target.trim();
        if prefix.is_empty() {
            return Err(format!("rewrite rule has empty prefix: {entry:?}"));
        }
        if target.is_empty() {
            return Err(format!("rewrite rule has empty target: {entry:?}"));
        }
        out.push(RewriteRule::new(prefix, target));
    }
    Ok(out)
}

/// 一次改写的结果。``original_model``/``new_model`` 仅在确实替换时填充，
/// 便于日志一行记录 ``orig->new`` 又不必重复正则。
#[derive(Debug, Clone)]
pub struct RewriteOutcome {
    pub body: Bytes,
    pub original_model: Option<String>,
    pub new_model: Option<String>,
}

impl RewriteOutcome {
    pub fn rewritten(&self) -> bool {
        self.new_model.is_some()
    }

    fn untouched(body: Bytes) -> Self {
        Self {
            body,
            original_model: None,
            new_model: None,
        }
    }
}

static MODEL_FIELD: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"("model"\s*:\s*")([^"]+)(")"#).expect("MODEL_FIELD compiles"));

/// 单纯从请求体里抽出 ``"model"`` 字段的值，不做改写。给计费兜底用：
/// 即使 rewritten=false（没命中任何 rule），handler 也能拿到客户端 model 名喂给 SniffContext。
pub fn extract_client_model(body: &[u8], content_type: &str) -> Option<String> {
    if body.is_empty() {
        return None;
    }
    if !content_type.to_ascii_lowercase().contains("application/json") {
        return None;
    }
    let caps = MODEL_FIELD.captures(body)?;
    let value_match = caps.get(2)?;
    std::str::from_utf8(value_match.as_bytes()).ok().map(String::from)
}

/// 检查请求体里第一处 ``"model": "X"`` 是否命中改写规则，若命中替换 X 为 target。
/// 非 JSON content-type、空 body、无规则、UTF-8 解码失败、target==original 都走原样透传。
pub fn rewrite_body(body: Bytes, content_type: &str, rules: &[RewriteRule]) -> RewriteOutcome {
    if body.is_empty() || rules.is_empty() {
        return RewriteOutcome::untouched(body);
    }
    if !content_type.to_ascii_lowercase().contains("application/json") {
        return RewriteOutcome::untouched(body);
    }

    let caps = match MODEL_FIELD.captures(&body) {
        Some(c) => c,
        None => return RewriteOutcome::untouched(body),
    };

    let value_match = caps.get(2).expect("group 2 is non-optional in pattern");
    let original = match std::str::from_utf8(value_match.as_bytes()) {
        Ok(s) => s.to_string(),
        Err(_) => return RewriteOutcome::untouched(body),
    };

    let target = match rules.iter().find(|r| r.matches(&original)) {
        Some(r) => r.target.clone(),
        None => return RewriteOutcome::untouched(body),
    };
    if target == original {
        return RewriteOutcome::untouched(body);
    }

    let value_start = value_match.start();
    let value_end = value_match.end();
    let new_body = crate::util::bytes_splice::splice_bytes(
        &body[..],
        value_start..value_end,
        target.as_bytes(),
    );

    RewriteOutcome {
        body: new_body,
        original_model: Some(original),
        new_model: Some(target),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rules() -> Vec<RewriteRule> {
        vec![RewriteRule::new(
            "claude-opus-4-7",
            "claude-jupiter-v1-p",
        )]
    }

    #[test]
    fn empty_body_passes_through() {
        let out = rewrite_body(Bytes::new(), "application/json", &rules());
        assert!(!out.rewritten());
        assert!(out.body.is_empty());
    }

    #[test]
    fn non_json_content_type_passes_through() {
        let body = Bytes::from_static(br#"{"model":"claude-opus-4-7"}"#);
        let out = rewrite_body(body.clone(), "text/plain", &rules());
        assert!(!out.rewritten());
        assert_eq!(out.body, body);
    }

    #[test]
    fn matches_first_model_only() {
        let body = Bytes::from_static(
            br#"{"model":"claude-opus-4-7-x","messages":[{"role":"user","content":"\"model\":\"injected\""}]}"#,
        );
        let out = rewrite_body(body, "application/json", &rules());
        assert!(out.rewritten());
        assert_eq!(out.original_model.unwrap(), "claude-opus-4-7-x");
        assert_eq!(out.new_model.unwrap(), "claude-jupiter-v1-p");
        let s = std::str::from_utf8(&out.body).unwrap();
        assert!(s.contains(r#""model":"claude-jupiter-v1-p""#));
        assert!(s.contains(r#"\"injected\""#));
    }

    #[test]
    fn utf8_failure_passes_through() {
        let mut raw: Vec<u8> = br#"{"model":""#.to_vec();
        raw.extend_from_slice(&[0xff, 0xfe, 0xfd]);
        raw.extend_from_slice(br#"","x":1}"#);
        let body = Bytes::from(raw);
        let out = rewrite_body(body.clone(), "application/json", &rules());
        assert!(!out.rewritten());
        assert_eq!(out.body, body);
    }

    #[test]
    fn target_equals_original_passes_through() {
        let rules = vec![RewriteRule::new("claude-opus-4-7", "claude-opus-4-7")];
        let body = Bytes::from_static(br#"{"model":"claude-opus-4-7"}"#);
        let out = rewrite_body(body.clone(), "application/json", &rules);
        assert!(!out.rewritten());
        assert_eq!(out.body, body);
    }

    #[test]
    fn unmatched_prefix_passes_through() {
        let body = Bytes::from_static(br#"{"model":"claude-sonnet-4"}"#);
        let out = rewrite_body(body.clone(), "application/json", &rules());
        assert!(!out.rewritten());
        assert_eq!(out.body, body);
    }

    #[test]
    fn rule_order_first_wins() {
        let rules = vec![
            RewriteRule::new("claude-opus", "first-target"),
            RewriteRule::new("claude-opus-4-7", "second-target"),
        ];
        let body = Bytes::from_static(br#"{"model":"claude-opus-4-7-stable"}"#);
        let out = rewrite_body(body, "application/json", &rules);
        assert_eq!(out.new_model.as_deref(), Some("first-target"));
    }

    #[test]
    fn hmac_block_bytes_preserved() {
        let signed_block = r#"{"messages":[{"role":"assistant","content":[{"type":"thinking","thinking":"deliberate","signature":"AAA/B+C==base64+padding="}]}],"model":"claude-opus-4-7"}"#;
        let body = Bytes::copy_from_slice(signed_block.as_bytes());
        let out = rewrite_body(body, "application/json", &rules());
        assert!(out.rewritten());
        let new_text = std::str::from_utf8(&out.body).unwrap();
        assert!(new_text.contains(r#""signature":"AAA/B+C==base64+padding=""#));
        assert!(new_text.contains(r#""model":"claude-jupiter-v1-p""#));
    }

    #[test]
    fn whitespace_around_colon_allowed() {
        let body = Bytes::from_static(br#"{"model"  :   "claude-opus-4-7"}"#);
        let out = rewrite_body(body, "application/json", &rules());
        assert!(out.rewritten());
        let s = std::str::from_utf8(&out.body).unwrap();
        assert!(s.contains(r#""model"  :   "claude-jupiter-v1-p""#));
    }

    #[test]
    fn parse_rewrite_rules_basic() {
        let rules = parse_rewrite_rules("claude-opus-4-7=claude-jupiter-v1-p,claude-haiku=jupiter-mini")
            .expect("parse ok");
        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0].prefix, "claude-opus-4-7");
        assert_eq!(rules[0].target, "claude-jupiter-v1-p");
        assert_eq!(rules[1].prefix, "claude-haiku");
        assert_eq!(rules[1].target, "jupiter-mini");
    }

    #[test]
    fn parse_rewrite_rules_empty() {
        assert!(parse_rewrite_rules("").unwrap().is_empty());
        assert!(parse_rewrite_rules("   ").unwrap().is_empty());
        // 末尾逗号 / 多余空白 / 内部空条目都视为空，不算 malformed
        assert_eq!(parse_rewrite_rules("a=b,,").unwrap().len(), 1);
    }

    #[test]
    fn parse_rewrite_rules_malformed_errors() {
        // 缺 '='
        assert!(parse_rewrite_rules("no-equals-sign").is_err());
        // prefix 空
        assert!(parse_rewrite_rules("=target").is_err());
        // target 空
        assert!(parse_rewrite_rules("prefix=").is_err());
        // 多条里有一条坏的，整体拒绝
        assert!(parse_rewrite_rules("a=b,broken,c=d").is_err());
    }

    #[test]
    fn parse_rewrite_rules_trims_around_equals() {
        let rules = parse_rewrite_rules("  pfx  =  tgt  ").expect("parse ok");
        assert_eq!(rules[0].prefix, "pfx");
        assert_eq!(rules[0].target, "tgt");
    }
}
