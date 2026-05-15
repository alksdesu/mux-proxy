//! 伪造 anthropic-ratelimit-* 响应头：让客户端误以为命中的是 Anthropic 官方限流。
//! 数值固定不动态（旧 TS 实现也是常量），reset 时间 = now+60s ISO8601。

use chrono::{Duration, SecondsFormat, Utc};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

pub const REQUESTS_LIMIT: &str = "4000";
pub const REQUESTS_REMAINING: &str = "3999";
pub const TOKENS_LIMIT: &str = "400000";
pub const TOKENS_REMAINING: &str = "399000";

/// 给客户端的响应注入完整伪造头。会覆盖已有同名值。
pub fn inject(headers: &mut HeaderMap) {
    set(headers, "anthropic-ratelimit-requests-limit", REQUESTS_LIMIT);
    set(headers, "anthropic-ratelimit-requests-remaining", REQUESTS_REMAINING);
    set(headers, "anthropic-ratelimit-tokens-limit", TOKENS_LIMIT);
    set(headers, "anthropic-ratelimit-tokens-remaining", TOKENS_REMAINING);

    let reset = (Utc::now() + Duration::seconds(60)).to_rfc3339_opts(SecondsFormat::Secs, true);
    set(headers, "anthropic-ratelimit-requests-reset", &reset);
    set(headers, "anthropic-ratelimit-tokens-reset", &reset);
}

fn set(h: &mut HeaderMap, name: &'static str, value: &str) {
    if let Ok(v) = HeaderValue::from_str(value) {
        h.insert(HeaderName::from_static(name), v);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_six_headers_present() {
        let mut h = HeaderMap::new();
        inject(&mut h);
        for name in [
            "anthropic-ratelimit-requests-limit",
            "anthropic-ratelimit-requests-remaining",
            "anthropic-ratelimit-tokens-limit",
            "anthropic-ratelimit-tokens-remaining",
            "anthropic-ratelimit-requests-reset",
            "anthropic-ratelimit-tokens-reset",
        ] {
            assert!(h.get(name).is_some(), "{name} missing");
        }
    }

    #[test]
    fn limit_constants_match_spec() {
        let mut h = HeaderMap::new();
        inject(&mut h);
        assert_eq!(h.get("anthropic-ratelimit-requests-limit").unwrap(), "4000");
        assert_eq!(h.get("anthropic-ratelimit-tokens-limit").unwrap(), "400000");
        assert_eq!(h.get("anthropic-ratelimit-requests-remaining").unwrap(), "3999");
        assert_eq!(h.get("anthropic-ratelimit-tokens-remaining").unwrap(), "399000");
    }

    #[test]
    fn reset_is_future_iso8601() {
        let mut h = HeaderMap::new();
        inject(&mut h);
        let v = h.get("anthropic-ratelimit-requests-reset").unwrap().to_str().unwrap();
        let parsed = chrono::DateTime::parse_from_rfc3339(v).expect("rfc3339");
        let now = Utc::now();
        assert!(parsed.to_utc() > now, "reset must be future");
    }

    #[test]
    fn injection_overwrites_existing_value() {
        let mut h = HeaderMap::new();
        h.insert(
            HeaderName::from_static("anthropic-ratelimit-requests-limit"),
            HeaderValue::from_static("1"),
        );
        inject(&mut h);
        assert_eq!(h.get("anthropic-ratelimit-requests-limit").unwrap(), "4000");
    }
}
