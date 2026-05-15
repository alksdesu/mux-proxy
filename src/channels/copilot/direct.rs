//! 直传分支的判断与开关：客户端 api_keys.upstream_key 直接给 prefix:token 时，
//! 跳过 transform 13 步（保留 step 1 strip）、跳过 sanitize_sse_event / sanitize_response_body、
//! 跳过 error_logs.model 字段；SSE 行级注释 `:xxx` 也透传不丢。

/// 直传模式下的开关集合。handler 据此短路 transform / sanitize / SSE 注释丢弃。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DirectFlags {
    /// 是否走直传（来自 UpstreamConfig::Direct 分支）。
    pub direct: bool,
}

impl DirectFlags {
    pub const PASS_THROUGH: Self = Self { direct: true };
    pub const SHARED_POOL: Self = Self { direct: false };

    /// 跳过 transform 13 步（除 step 1 strip）。
    pub fn skip_request_transform(self) -> bool {
        self.direct
    }
    /// 跳过 SSE 事件清洗。
    pub fn skip_sse_sanitize(self) -> bool {
        self.direct
    }
    /// 跳过非流响应清洗。
    pub fn skip_response_sanitize(self) -> bool {
        self.direct
    }
    /// error_logs.model 字段写空（不暴露上游 model）。
    pub fn omit_error_model(self) -> bool {
        self.direct
    }
    /// SSE 注释行 `:keep-alive` 等也原样透传。
    pub fn passthrough_sse_comments(self) -> bool {
        self.direct
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_mode_runs_full_pipeline() {
        let f = DirectFlags::SHARED_POOL;
        assert!(!f.skip_request_transform());
        assert!(!f.skip_sse_sanitize());
        assert!(!f.skip_response_sanitize());
        assert!(!f.omit_error_model());
        assert!(!f.passthrough_sse_comments());
    }

    #[test]
    fn direct_mode_skips_all() {
        let f = DirectFlags::PASS_THROUGH;
        assert!(f.skip_request_transform());
        assert!(f.skip_sse_sanitize());
        assert!(f.skip_response_sanitize());
        assert!(f.omit_error_model());
        assert!(f.passthrough_sse_comments());
    }
}
