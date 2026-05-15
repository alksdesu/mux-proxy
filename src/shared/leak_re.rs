//! 上游身份泄露探测正则。命中即把错误消息替换成 generic 文案，
//! 避免把 GitHub Copilot / Vertex / individual 等内部名称回灌给客户端。

use once_cell::sync::Lazy;
use regex::Regex;

pub static UPSTREAM_LEAK_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)github|copilot|vertex|individual|enterprise|personal.access.token")
        .expect("UPSTREAM_LEAK_RE compile")
});

pub static WEB_SEARCH_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)web.?search").expect("WEB_SEARCH_RE compile"));

/// 任一正则命中即视为可能暴露代理实现细节。
pub fn looks_leaked(msg: &str) -> bool {
    UPSTREAM_LEAK_RE.is_match(msg) || WEB_SEARCH_RE.is_match(msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upstream_leak_hits() {
        assert!(looks_leaked("GitHub upstream returned 502"));
        assert!(looks_leaked("Personal Access Tokens are not supported"));
        assert!(looks_leaked("VERTEX api error"));
        assert!(looks_leaked("individual tier exhausted"));
        assert!(looks_leaked("Enterprise endpoint timeout"));
        assert!(looks_leaked("copilot rate limit"));
    }

    #[test]
    fn web_search_hits() {
        assert!(looks_leaked("web_search tool failed"));
        assert!(looks_leaked("web search unavailable"));
        assert!(looks_leaked("WebSearch error"));
    }

    #[test]
    fn benign_messages_pass() {
        assert!(!looks_leaked("rate limit exceeded"));
        assert!(!looks_leaked("invalid request parameters"));
        assert!(!looks_leaked(""));
    }
}
