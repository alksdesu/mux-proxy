//! Admin endpoint 共用的 query 解析。`?channel=` 接受 copilot/anthropic/all，
//! 非法值 400；`?limit=` clamp 到 [1, 1000]。

use crate::channels::ChannelKind;
use crate::error::AppError;

pub const USAGE_LIMIT_DEFAULT: i64 = 20;
pub const ERROR_LIMIT_DEFAULT: i64 = 50;
pub const LIMIT_MAX: i64 = 1000;

/// 把 `?channel=` 的字面值映射成 `Option<ChannelKind>`：
/// `None` / `all` → `Ok(None)`（不过滤）；`copilot` / `anthropic` → `Ok(Some(_))`。
pub fn parse_channel(raw: Option<&str>) -> Result<Option<ChannelKind>, AppError> {
    match raw.map(str::trim).filter(|s| !s.is_empty()) {
        None | Some("all") => Ok(None),
        Some(v) => ChannelKind::parse(v)
            .map(Some)
            .ok_or_else(|| AppError::BadRequest(format!("invalid channel: {v}"))),
    }
}

pub fn clamp_limit(raw: Option<&str>, default: i64) -> i64 {
    let n: i64 = raw
        .and_then(|s| s.parse().ok())
        .unwrap_or(default);
    n.clamp(1, LIMIT_MAX)
}

pub fn clamp_offset(raw: Option<&str>) -> i64 {
    raw.and_then(|s| s.parse::<i64>().ok())
        .map(|n| n.max(0))
        .unwrap_or(0)
}

pub fn parse_id_required(raw: Option<&str>) -> Result<i64, AppError> {
    raw.and_then(|s| s.parse::<i64>().ok())
        .filter(|n| *n > 0)
        .ok_or_else(|| AppError::BadRequest("missing or invalid id parameter".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_default_to_none() {
        assert_eq!(parse_channel(None).unwrap(), None);
        assert_eq!(parse_channel(Some("")).unwrap(), None);
        assert_eq!(parse_channel(Some("  ")).unwrap(), None);
        assert_eq!(parse_channel(Some("all")).unwrap(), None);
    }

    #[test]
    fn channel_explicit_values() {
        assert_eq!(parse_channel(Some("copilot")).unwrap(), Some(ChannelKind::Copilot));
        assert_eq!(parse_channel(Some("anthropic")).unwrap(), Some(ChannelKind::Anthropic));
    }

    #[test]
    fn channel_invalid_returns_400() {
        let err = parse_channel(Some("vertex")).unwrap_err();
        assert!(matches!(err, AppError::BadRequest(_)));
    }

    #[test]
    fn limit_uses_default_when_missing() {
        assert_eq!(clamp_limit(None, 20), 20);
        assert_eq!(clamp_limit(Some("abc"), 20), 20);
    }

    #[test]
    fn limit_clamped_to_range() {
        assert_eq!(clamp_limit(Some("0"), 20), 1);
        assert_eq!(clamp_limit(Some("-5"), 20), 1);
        assert_eq!(clamp_limit(Some("999999"), 20), LIMIT_MAX);
        assert_eq!(clamp_limit(Some("50"), 20), 50);
    }

    #[test]
    fn offset_floors_at_zero() {
        assert_eq!(clamp_offset(None), 0);
        assert_eq!(clamp_offset(Some("-1")), 0);
        assert_eq!(clamp_offset(Some("100")), 100);
    }

    #[test]
    fn id_must_be_positive_integer() {
        assert!(parse_id_required(Some("5")).is_ok());
        assert!(parse_id_required(None).is_err());
        assert!(parse_id_required(Some("0")).is_err());
        assert!(parse_id_required(Some("-1")).is_err());
        assert!(parse_id_required(Some("abc")).is_err());
    }
}
