//! 上游 key 字符串解析。格式 `prefix:token`，prefix ∈ {individual, business, enterprise}。
//! 客户端 api_keys.upstream_key 字段可以是 `*` (走池) / `id1,id2` (限定池子) / `prefix:token` (直传)。

use std::str::FromStr;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CopilotPrefix {
    Individual,
    Business,
    Enterprise,
}

impl CopilotPrefix {
    pub fn upstream_base(self) -> &'static str {
        match self {
            CopilotPrefix::Individual => "https://api.individual.githubcopilot.com",
            CopilotPrefix::Business => "https://api.business.githubcopilot.com",
            CopilotPrefix::Enterprise => "https://api.enterprise.githubcopilot.com",
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            CopilotPrefix::Individual => "individual",
            CopilotPrefix::Business => "business",
            CopilotPrefix::Enterprise => "enterprise",
        }
    }
}

impl FromStr for CopilotPrefix {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "individual" => Ok(CopilotPrefix::Individual),
            "business" => Ok(CopilotPrefix::Business),
            "enterprise" => Ok(CopilotPrefix::Enterprise),
            _ => Err(()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedUpstreamKey {
    pub prefix: CopilotPrefix,
    pub token: String,
}

impl ParsedUpstreamKey {
    pub fn upstream_base(&self) -> &'static str {
        self.prefix.upstream_base()
    }

    pub fn is_session_token_required(&self) -> bool {
        self.token.starts_with("ghu_") || self.token.starts_with("gho_")
    }
}

/// 解析 `prefix:token`。空 token / 未知 prefix / 无冒号一律返 None。
pub fn parse_raw_key(raw: &str) -> Option<ParsedUpstreamKey> {
    let idx = raw.find(':')?;
    if idx == 0 {
        return None;
    }
    let (prefix_str, rest) = raw.split_at(idx);
    let token = &rest[1..];
    if token.is_empty() {
        return None;
    }
    let prefix = CopilotPrefix::from_str(prefix_str).ok()?;
    Some(ParsedUpstreamKey {
        prefix,
        token: token.to_string(),
    })
}

/// 描述 api_keys.upstream_key 字段的取值含义：池模式（全部 / 限定 id 集合）或直传单个上游 key。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UpstreamConfig {
    /// 用整个 enabled 池
    PoolAll,
    /// 限定到指定的 upstream_keys.id 子集
    PoolFiltered(Vec<i64>),
    /// 客户端直传一个 prefix:token，跳过池 / 清洗 / 计费 model 等
    Direct(ParsedUpstreamKey),
}

impl UpstreamConfig {
    pub fn is_direct(&self) -> bool {
        matches!(self, UpstreamConfig::Direct(_))
    }
}

/// 解析 api_keys.upstream_key 字段。`""` 或 `"*"` → PoolAll；
/// `^\d+(,\d+)*$` → PoolFiltered；其它视为 prefix:token 直传。
pub fn resolve_upstream_config(field: &str) -> Option<UpstreamConfig> {
    let trimmed = field.trim();
    if trimmed.is_empty() || trimmed == "*" {
        return Some(UpstreamConfig::PoolAll);
    }
    if is_pool_ids(trimmed) {
        let ids: Vec<i64> = trimmed
            .split(',')
            .filter_map(|s| s.parse::<i64>().ok())
            .collect();
        if ids.is_empty() {
            return None;
        }
        return Some(UpstreamConfig::PoolFiltered(ids));
    }
    parse_raw_key(trimmed).map(UpstreamConfig::Direct)
}

fn is_pool_ids(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    let mut prev_was_comma = true;
    for c in s.chars() {
        match c {
            '0'..='9' => prev_was_comma = false,
            ',' if !prev_was_comma => prev_was_comma = true,
            _ => return false,
        }
    }
    !prev_was_comma
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_enterprise() {
        let k = parse_raw_key("enterprise:ghp_xxx").expect("ok");
        assert_eq!(k.prefix, CopilotPrefix::Enterprise);
        assert_eq!(k.token, "ghp_xxx");
        assert_eq!(k.upstream_base(), "https://api.enterprise.githubcopilot.com");
        assert!(!k.is_session_token_required());
    }

    #[test]
    fn parse_individual_ghu_needs_session_token() {
        let k = parse_raw_key("individual:ghu_aaa").expect("ok");
        assert!(k.is_session_token_required());
    }

    #[test]
    fn parse_business_gho() {
        let k = parse_raw_key("business:gho_bbb").expect("ok");
        assert_eq!(k.prefix, CopilotPrefix::Business);
        assert!(k.is_session_token_required());
    }

    #[test]
    fn parse_case_insensitive_prefix() {
        assert!(parse_raw_key("ENTERPRISE:tok").is_some());
        assert!(parse_raw_key("Individual:tok").is_some());
    }

    #[test]
    fn parse_rejects_unknown_prefix() {
        assert!(parse_raw_key("anthropic:sk-ant-x").is_none());
        assert!(parse_raw_key("foo:bar").is_none());
    }

    #[test]
    fn parse_rejects_missing_token() {
        assert!(parse_raw_key("enterprise:").is_none());
        assert!(parse_raw_key("enterprise").is_none());
        assert!(parse_raw_key(":token").is_none());
        assert!(parse_raw_key("").is_none());
    }

    #[test]
    fn resolve_pool_all() {
        assert_eq!(resolve_upstream_config(""), Some(UpstreamConfig::PoolAll));
        assert_eq!(resolve_upstream_config("*"), Some(UpstreamConfig::PoolAll));
        assert_eq!(resolve_upstream_config("  *  "), Some(UpstreamConfig::PoolAll));
    }

    #[test]
    fn resolve_pool_filtered() {
        assert_eq!(
            resolve_upstream_config("1,2,3"),
            Some(UpstreamConfig::PoolFiltered(vec![1, 2, 3])),
        );
        assert_eq!(
            resolve_upstream_config("42"),
            Some(UpstreamConfig::PoolFiltered(vec![42])),
        );
    }

    #[test]
    fn resolve_direct() {
        let cfg = resolve_upstream_config("enterprise:ghp_x").expect("ok");
        assert!(cfg.is_direct());
        match cfg {
            UpstreamConfig::Direct(k) => assert_eq!(k.prefix, CopilotPrefix::Enterprise),
            _ => panic!("expected direct"),
        }
    }

    #[test]
    fn resolve_invalid_returns_none() {
        // 既不是 *、也不是数字 id、也不是合法 prefix:token
        assert!(resolve_upstream_config("foo:bar").is_none());
        assert!(resolve_upstream_config("1,,2").is_none());
        assert!(resolve_upstream_config("1,").is_none());
    }
}
