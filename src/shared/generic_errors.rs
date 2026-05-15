//! 状态码 → 通用文案兜底表。命中泄露正则或上游不给 message 时使用。
//! 文案需与 dashboard / SDK 的错误正则匹配规则对齐，不要随手改文案。

/// Anthropic 错误类型。复刻 proxy.ts STATUS_TO_ERROR_TYPE。
pub fn error_type(status: u16) -> &'static str {
    match status {
        400 | 422 => "invalid_request_error",
        401 => "authentication_error",
        403 => "permission_error",
        404 => "not_found_error",
        408 => "request_timeout",
        413 => "request_too_large",
        429 => "rate_limit_error",
        503 | 529 => "overloaded_error",
        500 | 502 => "api_error",
        _ => "api_error",
    }
}

/// 通用错误文案。未列出的状态码退化为按 5xx / 4xx 区分的两条兜底。
pub fn generic_message(status: u16) -> &'static str {
    match status {
        400 => "invalid request",
        401 => "authentication required",
        403 => "forbidden",
        404 => "the requested resource could not be found",
        422 => "invalid request parameters",
        429 => "rate limit exceeded",
        500 => "internal server error",
        502 => "an upstream service error occurred",
        503 => "service is temporarily overloaded",
        s if s < 500 => "invalid request",
        _ => "an upstream service error occurred",
    }
}

/// 状态码归一化：非标 < 500 折成 400，>=500 折成 502。
/// 用于响应清洗时把上游的 421 / 451 / 580 等冷门码统一掉。
pub fn normalize_status(status: u16) -> u16 {
    match status {
        400 | 401 | 403 | 404 | 408 | 413 | 422 | 429 | 500 | 502 | 503 | 529 => status,
        s if s < 500 => 400,
        _ => 502,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_status_messages() {
        assert_eq!(generic_message(400), "invalid request");
        assert_eq!(generic_message(401), "authentication required");
        assert_eq!(generic_message(429), "rate limit exceeded");
        assert_eq!(generic_message(502), "an upstream service error occurred");
    }

    #[test]
    fn unknown_status_fallback() {
        assert_eq!(generic_message(418), "invalid request");
        assert_eq!(generic_message(451), "invalid request");
        assert_eq!(generic_message(580), "an upstream service error occurred");
    }

    #[test]
    fn error_type_mapping() {
        assert_eq!(error_type(400), "invalid_request_error");
        assert_eq!(error_type(422), "invalid_request_error");
        assert_eq!(error_type(401), "authentication_error");
        assert_eq!(error_type(429), "rate_limit_error");
        assert_eq!(error_type(503), "overloaded_error");
        assert_eq!(error_type(529), "overloaded_error");
        assert_eq!(error_type(500), "api_error");
        assert_eq!(error_type(999), "api_error");
    }

    #[test]
    fn normalize_known_passthrough() {
        assert_eq!(normalize_status(400), 400);
        assert_eq!(normalize_status(429), 429);
        assert_eq!(normalize_status(502), 502);
    }

    #[test]
    fn normalize_oddballs() {
        assert_eq!(normalize_status(418), 400);
        assert_eq!(normalize_status(421), 400);
        assert_eq!(normalize_status(580), 502);
        assert_eq!(normalize_status(599), 502);
    }
}
