//! 各类响应 ID 生成。整个进程共享一个 CLIENT_SESSION_ID，
//! 上游某些路径需要稳定的 session 标识。

use once_cell::sync::Lazy;
use rand::Rng;
use rand::distributions::Alphanumeric;

const MSG_ID_PREFIX: &str = "msg_bdrk_01";
const SRVTOOLU_PREFIX: &str = "srvtoolu_01";
const REQUEST_ID_PREFIX: &str = "req_01";
const MSG_RAND_LEN: usize = 22;
const SRVTOOLU_RAND_LEN: usize = 22;
const REQUEST_ID_RAND_LEN: usize = 20;
const RESPONSE_REQUEST_ID_LEN: usize = 24;

pub static CLIENT_SESSION_ID: Lazy<String> =
    Lazy::new(|| uuid::Uuid::new_v4().simple().to_string());

fn random_alnum(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

/// 上游 message id 统一为 `msg_bdrk_01` + 22 char base62，
/// 避免暴露原始上游 id 形态。
pub fn gen_msg_id() -> String {
    let mut s = String::with_capacity(MSG_ID_PREFIX.len() + MSG_RAND_LEN);
    s.push_str(MSG_ID_PREFIX);
    s.push_str(&random_alnum(MSG_RAND_LEN));
    s
}

/// 伪造 web_search server_tool_use id。
pub fn gen_srvtoolu_id() -> String {
    let mut s = String::with_capacity(SRVTOOLU_PREFIX.len() + SRVTOOLU_RAND_LEN);
    s.push_str(SRVTOOLU_PREFIX);
    s.push_str(&random_alnum(SRVTOOLU_RAND_LEN));
    s
}

/// JSON error body 里的 `request_id` 字段。
pub fn gen_request_id() -> String {
    let mut s = String::with_capacity(REQUEST_ID_PREFIX.len() + REQUEST_ID_RAND_LEN);
    s.push_str(REQUEST_ID_PREFIX);
    s.push_str(&random_alnum(REQUEST_ID_RAND_LEN));
    s
}

/// 响应头 `request-id`。复刻 TS `req_` + uuid_v4[..24]，与 JSON body 内的
/// `req_01` 形态故意不同，匹配上游观测到的格式。
pub fn gen_response_request_id() -> String {
    let uuid = uuid::Uuid::new_v4().simple().to_string();
    let mut s = String::with_capacity(4 + RESPONSE_REQUEST_ID_LEN);
    s.push_str("req_");
    s.push_str(&uuid[..RESPONSE_REQUEST_ID_LEN]);
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn msg_id_shape() {
        let id = gen_msg_id();
        assert!(id.starts_with(MSG_ID_PREFIX));
        assert_eq!(id.len(), MSG_ID_PREFIX.len() + MSG_RAND_LEN);
        assert!(id[MSG_ID_PREFIX.len()..].chars().all(|c| c.is_ascii_alphanumeric()));
    }

    #[test]
    fn srvtoolu_shape() {
        let id = gen_srvtoolu_id();
        assert!(id.starts_with(SRVTOOLU_PREFIX));
        assert_eq!(id.len(), SRVTOOLU_PREFIX.len() + SRVTOOLU_RAND_LEN);
    }

    #[test]
    fn request_id_shape() {
        let id = gen_request_id();
        assert!(id.starts_with(REQUEST_ID_PREFIX));
        assert_eq!(id.len(), REQUEST_ID_PREFIX.len() + REQUEST_ID_RAND_LEN);
    }

    #[test]
    fn response_request_id_shape() {
        let id = gen_response_request_id();
        assert!(id.starts_with("req_"));
        assert_eq!(id.len(), 4 + RESPONSE_REQUEST_ID_LEN);
        assert!(id[4..].chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn ids_are_unique() {
        let a = gen_msg_id();
        let b = gen_msg_id();
        assert_ne!(a, b);
    }

    #[test]
    fn client_session_id_stable() {
        let a = CLIENT_SESSION_ID.clone();
        let b = CLIENT_SESSION_ID.clone();
        assert_eq!(a, b);
        assert_eq!(a.len(), 32);
    }
}
