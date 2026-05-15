//! 上游请求头构造。Stainless-* / Copilot-* 完整伪装，X-Agent-Task-Id / X-Interaction-Id
//! 每请求随机；X-Client-Session-Id 取自全局 CLIENT_SESSION_ID 让上游观察到稳定会话。

use crate::shared::ids::CLIENT_SESSION_ID;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use uuid::Uuid;

const USER_AGENT: &str =
    "copilot/1.0.5 (client/github/cli win32 v24.11.1) term/unknown";
const COPILOT_INTEGRATION_ID: &str = "copilot-developer-cli";
const OPENAI_INTENT: &str = "conversation-agent";
const GITHUB_API_VERSION: &str = "2025-05-01";
const STAINLESS_ARCH: &str = "x64";
const STAINLESS_LANG: &str = "js";
const STAINLESS_OS: &str = "Windows";
const STAINLESS_PKG_VERSION: &str = "5.20.1";
const STAINLESS_RETRY_COUNT: &str = "0";
const STAINLESS_RUNTIME: &str = "node";
const STAINLESS_RUNTIME_VERSION: &str = "v24.11.1";
const INTERACTION_TYPE: &str = "conversation-agent";

/// 构造上游请求头。`content_type=None` 表示请求无 body（GET / 部分 admin 转发）。
pub fn build_upstream_headers(upstream_token: &str, content_type: Option<&str>) -> HeaderMap {
    let mut h = HeaderMap::with_capacity(24);
    h.insert(HeaderName::from_static("accept"), value("application/json"));
    h.insert(
        HeaderName::from_static("authorization"),
        value(&format!("Bearer {upstream_token}")),
    );
    h.insert(
        HeaderName::from_static("copilot-integration-id"),
        value(COPILOT_INTEGRATION_ID),
    );
    h.insert(
        HeaderName::from_static("openai-intent"),
        value(OPENAI_INTENT),
    );
    h.insert(HeaderName::from_static("user-agent"), value(USER_AGENT));
    h.insert(
        HeaderName::from_static("x-agent-task-id"),
        value(&new_uuid()),
    );
    h.insert(
        HeaderName::from_static("x-client-session-id"),
        value(CLIENT_SESSION_ID.as_str()),
    );
    h.insert(
        HeaderName::from_static("x-github-api-version"),
        value(GITHUB_API_VERSION),
    );
    h.insert(HeaderName::from_static("x-initiator"), value("agent"));
    h.insert(
        HeaderName::from_static("x-interaction-id"),
        value(&new_uuid()),
    );
    h.insert(
        HeaderName::from_static("x-interaction-type"),
        value(INTERACTION_TYPE),
    );
    h.insert(
        HeaderName::from_static("x-stainless-arch"),
        value(STAINLESS_ARCH),
    );
    h.insert(
        HeaderName::from_static("x-stainless-lang"),
        value(STAINLESS_LANG),
    );
    h.insert(
        HeaderName::from_static("x-stainless-os"),
        value(STAINLESS_OS),
    );
    h.insert(
        HeaderName::from_static("x-stainless-package-version"),
        value(STAINLESS_PKG_VERSION),
    );
    h.insert(
        HeaderName::from_static("x-stainless-retry-count"),
        value(STAINLESS_RETRY_COUNT),
    );
    h.insert(
        HeaderName::from_static("x-stainless-runtime"),
        value(STAINLESS_RUNTIME),
    );
    h.insert(
        HeaderName::from_static("x-stainless-runtime-version"),
        value(STAINLESS_RUNTIME_VERSION),
    );
    h.insert(
        HeaderName::from_static("accept-encoding"),
        value("br, gzip, deflate"),
    );
    h.insert(HeaderName::from_static("accept-language"), value("*"));
    h.insert(HeaderName::from_static("connection"), value("keep-alive"));

    if let Some(ct) = content_type {
        h.insert(HeaderName::from_static("content-type"), value(ct));
    }

    h
}

fn new_uuid() -> String {
    Uuid::new_v4().hyphenated().to_string()
}

fn value(s: &str) -> HeaderValue {
    HeaderValue::from_str(s).expect("static header value")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn fixed_headers_present() {
        let h = build_upstream_headers("tok", Some("application/json"));
        assert_eq!(h.get("authorization").unwrap(), "Bearer tok");
        assert_eq!(h.get("copilot-integration-id").unwrap(), "copilot-developer-cli");
        assert_eq!(h.get("openai-intent").unwrap(), "conversation-agent");
        assert_eq!(h.get("x-initiator").unwrap(), "agent");
        assert_eq!(h.get("content-type").unwrap(), "application/json");
        assert_eq!(h.get("user-agent").unwrap(), USER_AGENT);
        assert_eq!(h.get("connection").unwrap(), "keep-alive");
    }

    #[test]
    fn content_type_optional() {
        let h = build_upstream_headers("tok", None);
        assert!(h.get("content-type").is_none());
    }

    #[test]
    fn task_and_interaction_ids_change_per_call() {
        let mut tasks: HashSet<String> = HashSet::new();
        let mut interactions: HashSet<String> = HashSet::new();
        for _ in 0..8 {
            let h = build_upstream_headers("tok", None);
            tasks.insert(h.get("x-agent-task-id").unwrap().to_str().unwrap().into());
            interactions.insert(h.get("x-interaction-id").unwrap().to_str().unwrap().into());
        }
        assert!(tasks.len() > 1, "task id should rotate");
        assert!(interactions.len() > 1, "interaction id should rotate");
    }

    #[test]
    fn session_id_is_stable_within_process() {
        let h1 = build_upstream_headers("tok", None);
        let h2 = build_upstream_headers("tok", None);
        assert_eq!(
            h1.get("x-client-session-id").unwrap(),
            h2.get("x-client-session-id").unwrap()
        );
        // 同时 X-Client-Session-Id 与 X-Agent-Task-Id 不能撞值
        assert_ne!(
            h1.get("x-client-session-id").unwrap(),
            h1.get("x-agent-task-id").unwrap()
        );
    }

    #[test]
    fn stainless_quintet_complete() {
        let h = build_upstream_headers("tok", None);
        for name in [
            "x-stainless-arch",
            "x-stainless-lang",
            "x-stainless-os",
            "x-stainless-package-version",
            "x-stainless-retry-count",
            "x-stainless-runtime",
            "x-stainless-runtime-version",
        ] {
            assert!(h.get(name).is_some(), "missing {name}");
        }
    }
}
