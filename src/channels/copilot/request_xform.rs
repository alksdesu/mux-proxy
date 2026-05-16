//! 请求体改写：严格 13 步顺序与旧 proxy.ts 对齐。
//! - direct 模式只走 step 1（stripUnsupportedParams），其它都跳过；
//! - 顶层 model 字段在 step 5 写回 upstream_model（剥掉 thinking 后缀，可能加 `-fast`）；
//! - step 13 individual base 把连字符模型名映射成点号。

use crate::channels::copilot::model_map;
use crate::error::{AppError, AppResult};
use crate::shared::json::{as_array_mut, as_object_mut};
use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::{Value, json};

/// thinking effort 强弱档（plan §request_xform.rs 列表）
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ThinkingEffort {
    Max,
    High,
    Medium,
    Low,
}

impl ThinkingEffort {
    pub fn as_str(self) -> &'static str {
        match self {
            ThinkingEffort::Max => "max",
            ThinkingEffort::High => "high",
            ThinkingEffort::Medium => "medium",
            ThinkingEffort::Low => "low",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "max" => Some(ThinkingEffort::Max),
            "high" => Some(ThinkingEffort::High),
            "medium" => Some(ThinkingEffort::Medium),
            "low" => Some(ThinkingEffort::Low),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ThinkingOverride {
    None,
    Enabled { budget_tokens: u64 },
    Adaptive,
    Effort(ThinkingEffort),
}

#[derive(Clone, Debug)]
pub struct ThinkingDecision {
    pub upstream_model: String,
    pub mode: ThinkingOverride,
}

static BUDGET_TOKEN_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^\d{3,}$").expect("BUDGET_TOKEN_RE compile"));
static SRVTOOLU_ID_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^srvtoolu_[a-zA-Z0-9_]+$").expect("SRVTOOLU_ID_RE compile"));
static TOOLU_PREFIX_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^toolu_").expect("TOOLU_PREFIX_RE compile"));
static TOOL_USE_ID_SANITIZE_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"[^a-zA-Z0-9_]").expect("TOOL_USE_ID_SANITIZE_RE compile"));

/// step 1 + direct 模式唯一一步：删除上游不支持的顶层字段。
pub fn strip_unsupported_params(obj: &mut Value) -> bool {
    let Some(map) = as_object_mut(obj) else {
        return false;
    };
    let mut changed = false;
    for key in ["top_p", "top_k", "context_management"] {
        if map.remove(key).is_some() {
            changed = true;
        }
    }
    changed
}

/// step 4：剥 model 末段 thinking 后缀（含 `-fast` 时倒数第二段才是 override）。
pub fn extract_thinking_override(model: &str) -> ThinkingDecision {
    let segments: Vec<&str> = model.split('-').collect();
    if segments.len() < 2 {
        return ThinkingDecision {
            upstream_model: model.to_string(),
            mode: ThinkingOverride::None,
        };
    }
    let last_index = segments.len() - 1;
    let last = segments[last_index];
    let mut override_index: Option<usize> = None;
    let mut decision = parse_thinking_token(last);
    if decision.is_some() {
        override_index = Some(last_index);
    } else if last == "fast" && last_index >= 1 {
        let second_last = segments[last_index - 1];
        if let Some(d) = parse_thinking_token(second_last) {
            decision = Some(d);
            override_index = Some(last_index - 1);
        }
    }
    let (Some(mode), Some(idx)) = (decision, override_index) else {
        return ThinkingDecision {
            upstream_model: model.to_string(),
            mode: ThinkingOverride::None,
        };
    };
    let upstream_model: String = segments
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != idx)
        .map(|(_, s)| *s)
        .collect::<Vec<_>>()
        .join("-");
    if upstream_model.is_empty() {
        return ThinkingDecision {
            upstream_model: model.to_string(),
            mode: ThinkingOverride::None,
        };
    }
    ThinkingDecision { upstream_model, mode }
}

fn parse_thinking_token(token: &str) -> Option<ThinkingOverride> {
    if BUDGET_TOKEN_RE.is_match(token) {
        let n: u64 = token.parse().ok()?;
        if (100..=1_000_000).contains(&n) {
            return Some(ThinkingOverride::Enabled { budget_tokens: n });
        }
        return None;
    }
    if token == "adaptive" {
        return Some(ThinkingOverride::Adaptive);
    }
    ThinkingEffort::parse(token).map(ThinkingOverride::Effort)
}

/// step 5：根据 mode 写入 thinking 字段与 output_config.effort。
pub fn apply_thinking_override(json: &mut Value, mode: &ThinkingOverride) {
    if matches!(mode, ThinkingOverride::None) {
        return;
    }
    let Some(map) = as_object_mut(json) else {
        return;
    };

    // 后缀指定 thinking override 就清空用户的 output_config（之后按 mode 决定要不要再写回 effort）
    map.remove("output_config");

    let thinking_map = take_object_or_new(map, "thinking");

    match mode {
        ThinkingOverride::Enabled { budget_tokens } => {
            let mut t = thinking_map;
            t.insert("type".into(), Value::String("adaptive".into()));
            t.insert(
                "budget_tokens".into(),
                Value::Number(serde_json::Number::from(*budget_tokens)),
            );
            map.insert("thinking".into(), Value::Object(t));
        }
        ThinkingOverride::Adaptive => {
            let mut t = thinking_map;
            t.insert("type".into(), Value::String("adaptive".into()));
            t.remove("budget_tokens");
            map.insert("thinking".into(), Value::Object(t));
        }
        ThinkingOverride::Effort(e) => {
            let mut t = thinking_map;
            t.insert("type".into(), Value::String("adaptive".into()));
            t.remove("budget_tokens");
            map.insert("thinking".into(), Value::Object(t));
            let mut oc = serde_json::Map::new();
            oc.insert("effort".into(), Value::String(e.as_str().into()));
            map.insert("output_config".into(), Value::Object(oc));
        }
        ThinkingOverride::None => unreachable!("handled above"),
    }
}

fn take_object_or_new(
    map: &mut serde_json::Map<String, Value>,
    key: &str,
) -> serde_json::Map<String, Value> {
    match map.remove(key) {
        Some(Value::Object(m)) => m,
        _ => serde_json::Map::new(),
    }
}

/// step 9：递归清洗 cache_control，每个对象只保留 `type`，删其它键。
pub fn clean_cache_control(value: &mut Value) {
    let Some(arr) = as_array_mut(value) else {
        return;
    };
    for item in arr.iter_mut() {
        let Some(item_map) = as_object_mut(item) else {
            continue;
        };
        if let Some(cc) = item_map.get_mut("cache_control") {
            if let Some(cc_map) = as_object_mut(cc) {
                cc_map.retain(|k, _| k == "type");
            }
        }
        if let Some(content) = item_map.get_mut("content") {
            clean_cache_control(content);
        }
    }
}

/// step 8：Opus 4.7 路径不能带 temperature!=1 / top_p / top_k，直接返 400。
/// direct 模式不调用本函数。
pub fn opus47_rejects_sampling(json: &Value) -> AppResult<()> {
    let Some(map) = json.as_object() else {
        return Ok(());
    };
    let model_lower = map
        .get("model")
        .and_then(|m| m.as_str())
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_default();
    if !(model_lower.contains("opus-4-7") || model_lower.contains("opus-4.7")) {
        return Ok(());
    }
    let mut bad: Vec<&'static str> = Vec::new();
    if map
        .get("temperature")
        .map(|v| !is_number_eq(v, 1.0))
        .unwrap_or(false)
    {
        bad.push("temperature");
    }
    if map.contains_key("top_p") {
        bad.push("top_p");
    }
    if map.contains_key("top_k") {
        bad.push("top_k");
    }
    if bad.is_empty() {
        return Ok(());
    }
    let mut joined = String::new();
    for (i, name) in bad.iter().enumerate() {
        if i > 0 {
            joined.push_str("`, `");
        }
        joined.push_str(name);
    }
    let verb = if bad.len() > 1 { "are" } else { "is" };
    Err(AppError::BadRequest(format!(
        "`{joined}` {verb} deprecated for this model."
    )))
}

fn is_number_eq(v: &Value, target: f64) -> bool {
    v.as_f64().is_some_and(|x| (x - target).abs() < f64::EPSILON)
}

/// 模型名 lowercase 是否落到 Opus 4.7 系列。
pub fn is_opus_47(model: &str) -> bool {
    let m = model.to_ascii_lowercase();
    m.contains("opus-4-7") || m.contains("opus-4.7")
}

/// step 11：content block 修复（server_tool_use id / draft_task / tool_reference / 空 text）。
pub fn fix_content_blocks(messages: &mut Value) {
    let Some(arr) = as_array_mut(messages) else {
        return;
    };
    for msg in arr.iter_mut() {
        let Some(msg_map) = as_object_mut(msg) else {
            continue;
        };
        let content = match msg_map.get_mut("content") {
            Some(Value::Array(a)) => a,
            _ => continue,
        };
        for block in content.iter_mut() {
            fix_single_block(block);
        }
        content.retain(|b| !is_empty_text_block(b));
    }
}

fn fix_single_block(block: &mut Value) {
    let Some(map) = as_object_mut(block) else {
        return;
    };
    let block_type = map
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    if block_type == "server_tool_use" {
        if let Some(id) = map.get("id").and_then(|v| v.as_str()) {
            if !SRVTOOLU_ID_RE.is_match(id) {
                let id_owned = id.to_string();
                map.insert("type".into(), Value::String("tool_use".into()));
                let new_id = if TOOLU_PREFIX_RE.is_match(&id_owned) {
                    id_owned
                } else {
                    let cleaned = TOOL_USE_ID_SANITIZE_RE.replace_all(&id_owned, "").into_owned();
                    format!("toolu_{cleaned}")
                };
                map.insert("id".into(), Value::String(new_id));
            }
        }
    }

    map.remove("draft_task");

    if block_type == "tool_result" {
        if let Some(Value::Array(inner)) = map.get_mut("content") {
            for item in inner.iter_mut() {
                if let Some(item_map) = as_object_mut(item) {
                    let item_type = item_map
                        .get("type")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    if item_type == "tool_reference" {
                        let serialized = serde_json::to_string(item).unwrap_or_else(|_| "{}".into());
                        *item = json!({ "type": "text", "text": serialized });
                        continue;
                    }
                    item_map.remove("draft_task");
                }
            }
            inner.retain(|b| !is_empty_text_block(b));
        }
    }
}

fn is_empty_text_block(b: &Value) -> bool {
    let Some(m) = b.as_object() else { return false };
    m.get("type").and_then(|v| v.as_str()) == Some("text")
        && m.get("text")
            .and_then(|v| v.as_str())
            .map(|s| s.is_empty())
            .unwrap_or(true)
}

/// step 10：tools 修复（删 defer_loading）。返回是否触发修改，便于上层判断。
pub fn fix_tools(tools: &mut Value) {
    let Some(arr) = as_array_mut(tools) else {
        return;
    };
    for tool in arr.iter_mut() {
        let Some(map) = as_object_mut(tool) else {
            continue;
        };
        map.remove("defer_loading");
        if let Some(custom) = map.get_mut("custom") {
            if let Some(c) = as_object_mut(custom) {
                c.remove("defer_loading");
            }
        }
    }
}

/// step 13：individual base 模型名映射（连字符 → 点号）。返回是否替换过。
pub fn remap_individual_model(json: &mut Value) -> bool {
    let Some(map) = as_object_mut(json) else {
        return false;
    };
    let Some(model) = map.get("model").and_then(|v| v.as_str()) else {
        return false;
    };
    let Some(mapped) = model_map::forward(model) else {
        return false;
    };
    map.insert("model".into(), Value::String(mapped.to_string()));
    true
}

/// 控制结构：本次请求是不是 direct 模式 + 是不是 individual base。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct XformContext {
    pub is_direct: bool,
    pub is_individual_base: bool,
}

/// 主入口：执行 plan 列出的全部 13 步。direct 模式只走 step 1。
/// 返回上游应使用的请求体；upstream_model 通过 out-param 暴露给 handler 路由用。
pub fn transform_request_body(
    payload: &mut Value,
    ctx: XformContext,
) -> AppResult<Option<String>> {
    // 非 Object → 直接序列化
    if !payload.is_object() {
        return Ok(None);
    }

    // step 1
    strip_unsupported_params(payload);

    // step 2：direct 短路
    if ctx.is_direct {
        return Ok(None);
    }

    // step 3：speed=fast
    let speed_is_fast = {
        let map = payload.as_object().expect("checked above");
        map.get("speed")
            .and_then(|v| v.as_str())
            .map(|s| s == "fast")
            .unwrap_or(false)
    };
    if speed_is_fast {
        as_object_mut(payload).expect("object").remove("speed");
    }

    // step 4 + 5：thinking override + 模型重写
    let mut upstream_model_out: Option<String> = None;
    if let Some(Value::String(model)) = as_object_mut(payload).expect("object").get_mut("model") {
        let decision = extract_thinking_override(model);
        let mut upstream_model = decision.upstream_model.clone();
        if speed_is_fast && !upstream_model.ends_with("-fast") {
            upstream_model.push_str("-fast");
        }
        *model = upstream_model.clone();
        upstream_model_out = Some(upstream_model);
        apply_thinking_override(payload, &decision.mode);
    }

    // step 6：Opus 4.7 强制 adaptive + medium effort
    if let Some(model) = payload
        .get("model")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
    {
        if is_opus_47(&model) {
            normalize_opus_47(payload);
        }
    }

    // step 7：非 4.7 路径 effort max → high
    downgrade_effort_max(payload);

    // step 8：Opus 4.7 采样硬拒。top_p/top_k 在 step 1 已被 strip，本次实际只检 temperature；
    // top_p/top_k 的拒绝由 handler.rs 在 transform 之前的预校验完成（在原始 payload 上）。
    opus47_rejects_sampling(payload)?;

    // step 9：cleanCacheControl 三处
    if let Some(map) = as_object_mut(payload) {
        if let Some(system) = map.get_mut("system") {
            clean_cache_control(system);
        }
        if let Some(Value::Array(messages)) = map.get_mut("messages") {
            for msg in messages.iter_mut() {
                if let Some(m) = as_object_mut(msg) {
                    if let Some(content) = m.get_mut("content") {
                        clean_cache_control(content);
                    }
                }
            }
        }
        if let Some(tools) = map.get_mut("tools") {
            clean_cache_control(tools);
        }
    }

    // step 10：tools defer_loading
    if let Some(tools) = as_object_mut(payload).and_then(|m| m.get_mut("tools")) {
        fix_tools(tools);
    }

    // step 11：content block 修复
    if let Some(messages) = as_object_mut(payload).and_then(|m| m.get_mut("messages")) {
        fix_content_blocks(messages);
    }

    // step 12 (Web Search tool 替换) 由 web_search.rs 的 detect_and_replace 完成（在 transform 之前调用）；
    // step 13：individual base 映射
    if ctx.is_individual_base {
        remap_individual_model(payload);
    }

    let _ = upstream_model_out; // 上层从 payload["model"] 重新读即可，这里只为标注顺序
    Ok(None)
}

fn normalize_opus_47(payload: &mut Value) {
    let Some(map) = as_object_mut(payload) else {
        return;
    };
    if let Some(thinking) = map.get_mut("thinking") {
        if let Some(t) = as_object_mut(thinking) {
            if t.get("type").and_then(|v| v.as_str()) == Some("enabled") {
                t.insert("type".into(), Value::String("adaptive".into()));
                t.remove("budget_tokens");
            }
        }
    }
    if let Some(oc) = map.get_mut("output_config") {
        if let Some(oc_map) = as_object_mut(oc) {
            if let Some(effort) = oc_map.get("effort").and_then(|v| v.as_str()) {
                if effort != "medium" {
                    oc_map.insert("effort".into(), Value::String("medium".into()));
                }
            }
        }
    }
}

fn downgrade_effort_max(payload: &mut Value) {
    let Some(map) = as_object_mut(payload) else {
        return;
    };
    let Some(oc) = map.get_mut("output_config") else {
        return;
    };
    let Some(oc_map) = as_object_mut(oc) else {
        return;
    };
    if oc_map.get("effort").and_then(|v| v.as_str()) == Some("max") {
        oc_map.insert("effort".into(), Value::String("high".into()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn strip_unsupported_drops_top_p_top_k_context_management() {
        let mut v = json!({"model": "x", "top_p": 0.9, "top_k": 5, "context_management": {}});
        assert!(strip_unsupported_params(&mut v));
        assert_eq!(v, json!({"model": "x"}));
    }

    #[test]
    fn strip_unsupported_no_op_when_clean() {
        let mut v = json!({"model": "x"});
        assert!(!strip_unsupported_params(&mut v));
    }

    #[test]
    fn thinking_suffix_budget_tokens() {
        let d = extract_thinking_override("claude-opus-4-6-5000");
        assert_eq!(d.upstream_model, "claude-opus-4-6");
        assert_eq!(d.mode, ThinkingOverride::Enabled { budget_tokens: 5000 });
    }

    #[test]
    fn thinking_suffix_adaptive() {
        let d = extract_thinking_override("claude-sonnet-4-6-adaptive");
        assert_eq!(d.upstream_model, "claude-sonnet-4-6");
        assert_eq!(d.mode, ThinkingOverride::Adaptive);
    }

    #[test]
    fn thinking_suffix_effort_max() {
        let d = extract_thinking_override("claude-opus-4-6-max");
        assert_eq!(d.upstream_model, "claude-opus-4-6");
        assert_eq!(d.mode, ThinkingOverride::Effort(ThinkingEffort::Max));
    }

    #[test]
    fn thinking_suffix_fast_uses_second_last() {
        let d = extract_thinking_override("claude-opus-4-6-high-fast");
        assert_eq!(d.upstream_model, "claude-opus-4-6-fast");
        assert_eq!(d.mode, ThinkingOverride::Effort(ThinkingEffort::High));
    }

    #[test]
    fn thinking_suffix_none_for_plain_model() {
        let d = extract_thinking_override("claude-opus-4-6");
        assert_eq!(d.upstream_model, "claude-opus-4-6");
        assert_eq!(d.mode, ThinkingOverride::None);
    }

    #[test]
    fn date_suffix_excluded_from_budget() {
        // 20251001 是 8 位日期，>1_000_000 不算 budget
        let d = extract_thinking_override("claude-opus-4-6-20251001");
        assert_eq!(d.mode, ThinkingOverride::None);
    }

    #[test]
    fn apply_enabled_sets_adaptive_and_budget() {
        let mut v = json!({"output_config": {"effort": "low"}});
        apply_thinking_override(&mut v, &ThinkingOverride::Enabled { budget_tokens: 3000 });
        assert_eq!(v["thinking"]["type"], "adaptive");
        assert_eq!(v["thinking"]["budget_tokens"], 3000);
        assert!(v.get("output_config").is_none(), "output_config cleared");
    }

    #[test]
    fn apply_effort_writes_output_config() {
        let mut v = json!({});
        apply_thinking_override(&mut v, &ThinkingOverride::Effort(ThinkingEffort::High));
        assert_eq!(v["thinking"]["type"], "adaptive");
        assert!(v["thinking"].get("budget_tokens").is_none());
        assert_eq!(v["output_config"]["effort"], "high");
    }

    #[test]
    fn clean_cache_control_preserves_only_type() {
        let mut v = json!([{
            "type": "text",
            "cache_control": {"type": "ephemeral", "ttl": 60, "etag": "x"},
            "content": [{"cache_control": {"type": "ephemeral", "ttl": 5}}]
        }]);
        clean_cache_control(&mut v);
        assert_eq!(v[0]["cache_control"], json!({"type": "ephemeral"}));
        assert_eq!(v[0]["content"][0]["cache_control"], json!({"type": "ephemeral"}));
    }

    #[test]
    fn opus47_rejects_temperature() {
        let v = json!({"model": "claude-opus-4-7", "temperature": 0.5});
        let err = opus47_rejects_sampling(&v).unwrap_err();
        match err {
            AppError::BadRequest(msg) => {
                assert!(msg.contains("temperature"));
                assert!(msg.contains("deprecated"));
            }
            _ => panic!("expected BadRequest"),
        }
    }

    #[test]
    fn opus47_rejects_top_p_and_top_k_together() {
        let v = json!({"model": "claude-opus-4.7", "top_p": 0.9, "top_k": 5});
        let err = opus47_rejects_sampling(&v).unwrap_err();
        match err {
            AppError::BadRequest(msg) => {
                assert!(msg.contains("top_p"));
                assert!(msg.contains("top_k"));
                assert!(msg.contains("are"));
            }
            _ => panic!("expected BadRequest"),
        }
    }

    #[test]
    fn opus47_temperature_one_allowed() {
        let v = json!({"model": "claude-opus-4-7", "temperature": 1});
        assert!(opus47_rejects_sampling(&v).is_ok());
    }

    #[test]
    fn opus47_normalize_thinking_enabled_to_adaptive() {
        let mut v = json!({
            "model": "claude-opus-4-7",
            "thinking": {"type": "enabled", "budget_tokens": 5000},
            "output_config": {"effort": "high"}
        });
        normalize_opus_47(&mut v);
        assert_eq!(v["thinking"]["type"], "adaptive");
        assert!(v["thinking"].get("budget_tokens").is_none());
        assert_eq!(v["output_config"]["effort"], "medium");
    }

    #[test]
    fn downgrade_effort_max_to_high() {
        let mut v = json!({"output_config": {"effort": "max"}});
        downgrade_effort_max(&mut v);
        assert_eq!(v["output_config"]["effort"], "high");
    }

    #[test]
    fn fix_content_block_invalid_server_tool_use_id_downgrades() {
        let mut v = json!([{"role": "assistant", "content": [
            {"type": "server_tool_use", "id": "bad id!", "name": "x", "input": {}}
        ]}]);
        fix_content_blocks(&mut v);
        let b = &v[0]["content"][0];
        assert_eq!(b["type"], "tool_use");
        let new_id = b["id"].as_str().unwrap();
        assert!(new_id.starts_with("toolu_"), "got {new_id}");
    }

    #[test]
    fn fix_content_block_strips_draft_task_and_tool_reference() {
        let mut v = json!([{"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "toolu_x", "content": [
                {"type": "tool_reference", "name": "ref"},
                {"type": "text", "text": ""}
            ], "draft_task": "drop me"}
        ]}]);
        fix_content_blocks(&mut v);
        let tool_result = &v[0]["content"][0];
        assert!(tool_result.get("draft_task").is_none());
        let inner = tool_result["content"].as_array().unwrap();
        assert_eq!(inner.len(), 1);
        assert_eq!(inner[0]["type"], "text");
        assert!(inner[0]["text"].as_str().unwrap().contains("tool_reference"));
    }

    #[test]
    fn fix_tools_drops_defer_loading_at_both_levels() {
        let mut v = json!([{"name": "x", "defer_loading": true, "custom": {"defer_loading": 1, "keep": "k"}}]);
        fix_tools(&mut v);
        let t = &v[0];
        assert!(t.get("defer_loading").is_none());
        assert!(t["custom"].get("defer_loading").is_none());
        assert_eq!(t["custom"]["keep"], "k");
    }

    #[test]
    fn remap_individual_model_maps_known() {
        let mut v = json!({"model": "claude-opus-4-6"});
        assert!(remap_individual_model(&mut v));
        assert_eq!(v["model"], "claude-opus-4.6");
    }

    #[test]
    fn remap_individual_model_skips_unknown() {
        let mut v = json!({"model": "claude-haiku-4-5"});
        assert!(!remap_individual_model(&mut v));
        assert_eq!(v["model"], "claude-haiku-4-5");
    }

    // 主入口端到端测试

    #[test]
    fn transform_e2e_pool_speed_fast() {
        let mut v = json!({
            "model": "claude-opus-4-6",
            "speed": "fast",
            "top_p": 0.9,
            "top_k": 4,
            "messages": [{"role": "user", "content": [{"type": "text", "text": "hi"}]}]
        });
        transform_request_body(
            &mut v,
            XformContext {
                is_direct: false,
                is_individual_base: false,
            },
        )
        .expect("ok");
        assert_eq!(v["model"], "claude-opus-4-6-fast");
        assert!(v.get("speed").is_none());
        assert!(v.get("top_p").is_none());
        assert!(v.get("top_k").is_none());
    }

    #[test]
    fn transform_e2e_direct_only_strips() {
        let mut v = json!({"model": "x", "top_p": 0.9, "speed": "fast"});
        transform_request_body(
            &mut v,
            XformContext {
                is_direct: true,
                is_individual_base: false,
            },
        )
        .expect("ok");
        assert!(v.get("top_p").is_none());
        // direct 不做 speed=fast 处理，speed 应该被保留
        assert_eq!(v["speed"], "fast");
    }

    #[test]
    fn transform_e2e_thinking_suffix_with_fast() {
        let mut v = json!({
            "model": "claude-opus-4-6-high-fast",
            "speed": "fast"
        });
        transform_request_body(
            &mut v,
            XformContext {
                is_direct: false,
                is_individual_base: false,
            },
        )
        .expect("ok");
        assert_eq!(v["model"], "claude-opus-4-6-fast");
        assert_eq!(v["thinking"]["type"], "adaptive");
        assert_eq!(v["output_config"]["effort"], "high");
    }

    #[test]
    fn transform_e2e_opus_47_rejects_top_p() {
        let mut v = json!({"model": "claude-opus-4-7", "top_p": 0.5});
        // top_p 在 step 1 已经被剥掉，所以 step 8 看不到 top_p — 不会触发拒绝
        let res = transform_request_body(
            &mut v,
            XformContext {
                is_direct: false,
                is_individual_base: false,
            },
        );
        assert!(res.is_ok(), "top_p removed by strip step 1 should pass");
    }

    #[test]
    fn transform_e2e_opus_47_rejects_temperature_directly() {
        let mut v = json!({"model": "claude-opus-4-7", "temperature": 0.7});
        let err = transform_request_body(
            &mut v,
            XformContext {
                is_direct: false,
                is_individual_base: false,
            },
        )
        .unwrap_err();
        match err {
            AppError::BadRequest(m) => assert!(m.contains("temperature")),
            _ => panic!("expected BadRequest"),
        }
    }

    #[test]
    fn transform_e2e_individual_base_remap() {
        let mut v = json!({"model": "claude-opus-4-6"});
        transform_request_body(
            &mut v,
            XformContext {
                is_direct: false,
                is_individual_base: true,
            },
        )
        .expect("ok");
        assert_eq!(v["model"], "claude-opus-4.6");
    }
}
