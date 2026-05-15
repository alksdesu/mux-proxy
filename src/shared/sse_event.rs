//! Anthropic SSE 事件白名单。任何不在 ALLOWED 集合内的事件类型必须丢弃，
//! 防止上游私有事件（如 copilot_usage、ping_extended）泄露给客户端。

pub const ALLOWED_EVENT_TYPES: &[&str] = &[
    "message_start",
    "content_block_start",
    "content_block_delta",
    "content_block_stop",
    "message_delta",
    "message_stop",
    "ping",
    "error",
];

/// 不需要 parse 即可透传的事件类型（频率高、payload 大、SSE 处理热路径）。
pub const PASSTHROUGH_EVENT_TYPES: &[&str] = &[
    "content_block_delta",
    "content_block_start",
    "content_block_stop",
    "ping",
];

pub fn is_allowed(event_type: &str) -> bool {
    ALLOWED_EVENT_TYPES.contains(&event_type)
}

pub fn is_passthrough(event_type: &str) -> bool {
    PASSTHROUGH_EVENT_TYPES.contains(&event_type)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allowed_set() {
        assert!(is_allowed("message_start"));
        assert!(is_allowed("ping"));
        assert!(is_allowed("error"));
        assert!(!is_allowed("copilot_usage"));
        assert!(!is_allowed("ping_extended"));
        assert!(!is_allowed(""));
    }

    #[test]
    fn passthrough_subset_of_allowed() {
        for t in PASSTHROUGH_EVENT_TYPES {
            assert!(is_allowed(t), "{t} marked passthrough but not allowed");
        }
    }

    #[test]
    fn message_events_need_parsing() {
        assert!(!is_passthrough("message_start"));
        assert!(!is_passthrough("message_delta"));
        assert!(!is_passthrough("message_stop"));
        assert!(!is_passthrough("error"));
    }
}
