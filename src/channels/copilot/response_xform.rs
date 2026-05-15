//! 响应清洗：删上游泄露字段、msg id 改成 msg_bdrk_、模型名还原、usage 清洗。
//! 错误改写在 handler 里调 sanitize_error_message + normalize_status。

use crate::channels::copilot::model_map;
use crate::shared::generic_errors::{generic_message, normalize_status};
use crate::shared::ids::gen_msg_id;
use crate::shared::json::as_object_mut;
use crate::shared::leak_re::looks_leaked;
use serde_json::Value;

/// non-stream 响应 / message_start.message 共用的清洗器。fast=true 时透传 id 不重写。
pub fn sanitize_response_body(json: &mut Value, is_fast: bool) {
    let Some(map) = as_object_mut(json) else {
        return;
    };
    for key in [
        "stop_details",
        "system_fingerprint",
        "provider",
        "provider_info",
        "telemetry",
        "debug",
        "trace_id",
        "copilot_usage",
    ] {
        map.remove(key);
    }

    if !is_fast {
        if let Some(id) = map.get("id").and_then(|v| v.as_str()) {
            if !id.starts_with("msg_bdrk_") {
                map.insert("id".into(), Value::String(gen_msg_id()));
            }
        }
    }

    if let Some(model) = map.get("model").and_then(|v| v.as_str()) {
        let restored = model_map::restore(model);
        if restored != model {
            map.insert("model".into(), Value::String(restored));
        }
    }

    if let Some(usage) = map.get_mut("usage") {
        if let Some(u) = as_object_mut(usage) {
            u.remove("inference_geo");
            u.remove("service_tier");
        }
    }
}

/// SSE 单事件清洗（非 direct 模式）。message_start 内嵌的 message 走 sanitize_response_body。
pub fn sanitize_sse_event(event: &mut Value, is_fast: bool) {
    let Some(map) = as_object_mut(event) else {
        return;
    };
    map.remove("copilot_usage");

    let event_type = map
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    match event_type.as_str() {
        "message_start" => {
            if let Some(msg) = map.get_mut("message") {
                sanitize_response_body(msg, is_fast);
            }
        }
        "message_delta" => {
            map.remove("stop_details");
            if let Some(usage) = map.get_mut("usage") {
                if let Some(u) = as_object_mut(usage) {
                    u.remove("inference_geo");
                    u.remove("service_tier");
                }
            }
            if let Some(delta) = map.get_mut("delta") {
                if let Some(d) = as_object_mut(delta) {
                    d.remove("stop_details");
                }
            }
        }
        "error" => {
            if let Some(err) = map.get_mut("error") {
                if let Some(err_map) = as_object_mut(err) {
                    if let Some(message) = err_map.get("message").and_then(|v| v.as_str()) {
                        if looks_leaked(message) {
                            err_map.insert(
                                "message".into(),
                                Value::String("an unexpected error occurred".into()),
                            );
                        }
                    }
                }
            }
        }
        _ => {}
    }
}

/// 4xx/5xx 上游响应里的 error.message 改写（pool 模式 + 非 direct）。
/// 命中 UPSTREAM_LEAK_RE / web_search 字样 / 空消息 → 用 generic_message 兜底。
pub fn sanitize_error_message(raw_body: &str, status: u16) -> String {
    let mut extracted = String::new();
    if status == 400 || status == 422 {
        if let Ok(parsed) = serde_json::from_str::<Value>(raw_body) {
            if let Some(obj) = parsed.as_object() {
                if let Some(err_obj) = obj.get("error").and_then(|v| v.as_object()) {
                    if let Some(msg) = err_obj.get("message").and_then(|v| v.as_str()) {
                        extracted = msg.to_string();
                    }
                }
                if extracted.is_empty() {
                    if let Some(msg) = obj.get("message").and_then(|v| v.as_str()) {
                        extracted = msg.to_string();
                    }
                }
            }
        }
    }
    if !extracted.is_empty() && (looks_leaked(&extracted) || extracted.contains("__proxy_web_search"))
    {
        extracted.clear();
    }
    if extracted.is_empty() {
        return generic_message(status).to_string();
    }
    extracted
}

/// 状态码归一化：非标 < 500 → 400；>= 500 → 502；标准码透传。
pub fn normalize_error_status(status: u16) -> u16 {
    normalize_status(status)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn strip_known_leak_fields() {
        let mut v = json!({
            "id": "msg_bdrk_existing",
            "model": "claude-opus-4.6",
            "provider": "anthropic",
            "stop_details": {"type": "end_turn"},
            "system_fingerprint": "fp_x",
            "trace_id": "t_x",
            "telemetry": {"x": 1},
            "copilot_usage": {"y": 1}
        });
        sanitize_response_body(&mut v, false);
        for key in [
            "provider",
            "stop_details",
            "system_fingerprint",
            "trace_id",
            "telemetry",
            "copilot_usage",
        ] {
            assert!(v.get(key).is_none(), "{key} not stripped");
        }
    }

    #[test]
    fn non_fast_rewrites_non_bdrk_id() {
        let mut v = json!({"id": "msg_anthropic_xyz", "model": "claude-opus-4.6"});
        sanitize_response_body(&mut v, false);
        assert!(v["id"].as_str().unwrap().starts_with("msg_bdrk_01"));
    }

    #[test]
    fn fast_preserves_id() {
        let mut v = json!({"id": "msg_anthropic_xyz", "model": "claude-opus-4.6-fast"});
        sanitize_response_body(&mut v, true);
        assert_eq!(v["id"], "msg_anthropic_xyz");
    }

    #[test]
    fn already_bdrk_id_preserved() {
        let mut v = json!({"id": "msg_bdrk_abc", "model": "claude-opus-4.6"});
        sanitize_response_body(&mut v, false);
        assert_eq!(v["id"], "msg_bdrk_abc");
    }

    #[test]
    fn model_dot_to_dash() {
        let mut v = json!({"id": "msg_bdrk_x", "model": "claude-opus-4.6"});
        sanitize_response_body(&mut v, false);
        assert_eq!(v["model"], "claude-opus-4-6");
    }

    #[test]
    fn model_fast_suffix_dot_to_dash() {
        let mut v = json!({"id": "msg_bdrk_x", "model": "claude-opus-4.6-fast"});
        sanitize_response_body(&mut v, true);
        assert_eq!(v["model"], "claude-opus-4-6-fast");
    }

    #[test]
    fn usage_drops_inference_geo_service_tier() {
        let mut v = json!({
            "id": "msg_bdrk_x",
            "model": "claude-opus-4.6",
            "usage": {"input_tokens": 10, "inference_geo": "us", "service_tier": "x"}
        });
        sanitize_response_body(&mut v, false);
        assert_eq!(v["usage"]["input_tokens"], 10);
        assert!(v["usage"].get("inference_geo").is_none());
        assert!(v["usage"].get("service_tier").is_none());
    }

    #[test]
    fn sse_message_start_sanitizes_embedded_message() {
        let mut e = json!({"type": "message_start", "message": {
            "id": "msg_anthropic_x", "model": "claude-opus-4.6", "provider": "anthropic"
        }});
        sanitize_sse_event(&mut e, false);
        let msg = &e["message"];
        assert!(msg["id"].as_str().unwrap().starts_with("msg_bdrk_"));
        assert_eq!(msg["model"], "claude-opus-4-6");
        assert!(msg.get("provider").is_none());
    }

    #[test]
    fn sse_message_delta_strips_stop_details_and_usage_taint() {
        let mut e = json!({
            "type": "message_delta",
            "stop_details": {"x": 1},
            "usage": {"input_tokens": 5, "inference_geo": "us"},
            "delta": {"stop_details": {"x": 1}, "stop_reason": "end_turn"}
        });
        sanitize_sse_event(&mut e, false);
        assert!(e.get("stop_details").is_none());
        assert!(e["usage"].get("inference_geo").is_none());
        assert!(e["delta"].get("stop_details").is_none());
        assert_eq!(e["delta"]["stop_reason"], "end_turn");
    }

    #[test]
    fn sse_error_replaces_leaky_message() {
        let mut e = json!({"type": "error", "error": {"message": "GitHub Copilot upstream blew up"}});
        sanitize_sse_event(&mut e, false);
        assert_eq!(e["error"]["message"], "an unexpected error occurred");
    }

    #[test]
    fn sse_error_keeps_clean_message() {
        let mut e = json!({"type": "error", "error": {"message": "rate limit exceeded"}});
        sanitize_sse_event(&mut e, false);
        assert_eq!(e["error"]["message"], "rate limit exceeded");
    }

    #[test]
    fn sanitize_error_message_extracts_clean_body() {
        let raw = r#"{"error": {"message": "model not available"}}"#;
        let out = sanitize_error_message(raw, 400);
        assert_eq!(out, "model not available");
    }

    #[test]
    fn sanitize_error_message_replaces_leak() {
        let raw = r#"{"error": {"message": "github copilot quota exhausted"}}"#;
        let out = sanitize_error_message(raw, 400);
        assert_eq!(out, generic_message(400));
    }

    #[test]
    fn sanitize_error_message_falls_back_when_no_extractable_text() {
        let out = sanitize_error_message("plain text", 500);
        assert_eq!(out, generic_message(500));
    }

    #[test]
    fn normalize_status_maps_oddballs() {
        assert_eq!(normalize_error_status(421), 400);
        assert_eq!(normalize_error_status(580), 502);
        assert_eq!(normalize_error_status(429), 429);
    }
}
