//! individual base 用点号模型名（claude-opus-4.6），客户端发的是连字符（claude-opus-4-6）。
//! 请求方向用 forward_map 把顶层 model 改成点号；响应方向先查 reverse_map 再 fallback 正则。

use once_cell::sync::Lazy;
use regex::Regex;
use std::collections::HashMap;

const FORWARD_PAIRS: &[(&str, &str)] = &[
    ("claude-opus-4-6", "claude-opus-4.6"),
    ("claude-sonnet-4-6", "claude-sonnet-4.6"),
];

static FORWARD_MAP: Lazy<HashMap<&'static str, &'static str>> =
    Lazy::new(|| FORWARD_PAIRS.iter().copied().collect());

static REVERSE_MAP: Lazy<HashMap<&'static str, &'static str>> =
    Lazy::new(|| FORWARD_PAIRS.iter().map(|(k, v)| (*v, *k)).collect());

/// fallback：把模型名里所有 `N.M` 段还原成 `N-M`，覆盖 `-fast` 后缀变体。
static DOT_VERSION_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(\d+)\.(\d+)").expect("DOT_VERSION_RE compile"));

/// 请求方向（individual base 专用）：客户端 model → 上游 model。
/// 未命中返回 None，调用方保留原值。
pub fn forward(model: &str) -> Option<&'static str> {
    FORWARD_MAP.get(model).copied()
}

/// 响应方向：上游 model → 客户端 model。先查反向表，再 regex 兜底。
pub fn restore(model: &str) -> String {
    if let Some(mapped) = REVERSE_MAP.get(model) {
        return (*mapped).to_string();
    }
    DOT_VERSION_RE.replace_all(model, "$1-$2").into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forward_known_models() {
        assert_eq!(forward("claude-opus-4-6"), Some("claude-opus-4.6"));
        assert_eq!(forward("claude-sonnet-4-6"), Some("claude-sonnet-4.6"));
    }

    #[test]
    fn forward_unknown_returns_none() {
        assert_eq!(forward("claude-haiku-4-5"), None);
        assert_eq!(forward("claude-opus-4.6"), None);
        assert_eq!(forward(""), None);
    }

    #[test]
    fn restore_exact_match() {
        assert_eq!(restore("claude-opus-4.6"), "claude-opus-4-6");
        assert_eq!(restore("claude-sonnet-4.6"), "claude-sonnet-4-6");
    }

    #[test]
    fn restore_fast_suffix_falls_back_to_regex() {
        assert_eq!(restore("claude-opus-4.6-fast"), "claude-opus-4-6-fast");
        assert_eq!(restore("claude-opus-4.7"), "claude-opus-4-7");
    }

    #[test]
    fn restore_haiku_with_dot() {
        assert_eq!(restore("claude-haiku-4.5"), "claude-haiku-4-5");
    }

    #[test]
    fn restore_already_dashed_is_unchanged() {
        assert_eq!(restore("claude-opus-4-6"), "claude-opus-4-6");
        assert_eq!(restore("claude-sonnet-4-6-fast"), "claude-sonnet-4-6-fast");
    }

    #[test]
    fn restore_handles_no_version_at_all() {
        assert_eq!(restore("custom-model"), "custom-model");
        assert_eq!(restore(""), "");
    }
}
