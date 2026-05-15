//! hop-by-hop header 清单。响应方向故意保留 ``connection``：
//! Anthropic CF 边缘每次都发，剥掉就是一个一字节指纹。

/// 请求方向剥除项。``host`` 由 hyper 按 base_url 重写、``content-length`` 由 transport 重算、
/// ``accept-encoding`` 不在剥除项里——保留客户端原值后再被 ``force_accept_encoding_gzip`` 覆盖。
pub const REQUEST_STRIP: &[&str] = &[
    "host",
    "content-length",
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
];

/// 响应方向剥除项。故意不含 ``connection``。
pub const RESPONSE_STRIP: &[&str] = &[
    "content-length",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
];

pub fn is_request_hop_by_hop(name_lower: &str) -> bool {
    REQUEST_STRIP.contains(&name_lower)
}

pub fn is_response_hop_by_hop(name_lower: &str) -> bool {
    RESPONSE_STRIP.contains(&name_lower)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_strip_set() {
        assert!(is_request_hop_by_hop("host"));
        assert!(is_request_hop_by_hop("transfer-encoding"));
        assert!(is_request_hop_by_hop("connection"));
        assert!(!is_request_hop_by_hop("authorization"));
        assert!(!is_request_hop_by_hop("x-api-key"));
    }

    #[test]
    fn response_strip_keeps_connection() {
        assert!(!is_response_hop_by_hop("connection"));
        assert!(is_response_hop_by_hop("content-length"));
        assert!(is_response_hop_by_hop("transfer-encoding"));
    }
}
