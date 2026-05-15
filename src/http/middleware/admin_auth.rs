//! Admin Bearer 鉴权。匹配失败一律回 404 文本，避免暴露管理面存在。
//! token 比较走 `constant_time_eq` 风格的逐字节判断，挡住计时侧信道。

use crate::app::AppState;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

const NOT_FOUND_BODY: &str = "not found";

/// 静态包装类型，方便后续从 router 引用同一个名字。当前仅作占位标识。
pub struct AdminAuth;

pub async fn admin_auth_layer(
    State(state): State<AppState>,
    headers: HeaderMap,
    req: Request,
    next: Next,
) -> Response {
    if !verify_bearer(&headers, state.cfg.admin_key.as_bytes()) {
        return (StatusCode::NOT_FOUND, NOT_FOUND_BODY).into_response();
    }
    next.run(req).await
}

/// 也允许从 query 取 `token=`（导出端点 dashboard 用 a[download] 拉文件时走这条）。
pub async fn admin_auth_with_query_token(
    State(state): State<AppState>,
    headers: HeaderMap,
    req: Request,
    next: Next,
) -> Response {
    if verify_bearer(&headers, state.cfg.admin_key.as_bytes()) {
        return next.run(req).await;
    }
    let token = req
        .uri()
        .query()
        .and_then(|q| {
            q.split('&').find_map(|kv| {
                let (k, v) = kv.split_once('=')?;
                if k == "token" { Some(v) } else { None }
            })
        })
        .unwrap_or_default();
    let decoded = urlencoding_decode(token);
    if constant_time_eq(decoded.as_bytes(), state.cfg.admin_key.as_bytes()) {
        return next.run(req).await;
    }
    (StatusCode::NOT_FOUND, NOT_FOUND_BODY).into_response()
}

fn verify_bearer(headers: &HeaderMap, expected: &[u8]) -> bool {
    let Some(raw) = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
    else {
        return false;
    };
    let Some(token) = raw.strip_prefix("Bearer ").map(str::trim) else {
        return false;
    };
    constant_time_eq(token.as_bytes(), expected)
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for i in 0..a.len() {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

fn urlencoding_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hi = hex_val(bytes[i + 1]);
                let lo = hex_val(bytes[i + 2]);
                match (hi, lo) {
                    (Some(h), Some(l)) => {
                        out.push((h << 4) | l);
                        i += 3;
                    }
                    _ => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8(out).unwrap_or_default()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn const_time_eq_matches_byte_for_byte() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
        assert!(!constant_time_eq(b"", b"x"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn bearer_extract_ok() {
        let mut h = HeaderMap::new();
        h.insert(axum::http::header::AUTHORIZATION, "Bearer secret123".parse().unwrap());
        assert!(verify_bearer(&h, b"secret123"));
    }

    #[test]
    fn bearer_extract_wrong_token() {
        let mut h = HeaderMap::new();
        h.insert(axum::http::header::AUTHORIZATION, "Bearer wrong".parse().unwrap());
        assert!(!verify_bearer(&h, b"secret123"));
    }

    #[test]
    fn bearer_missing_prefix() {
        let mut h = HeaderMap::new();
        h.insert(axum::http::header::AUTHORIZATION, "secret123".parse().unwrap());
        assert!(!verify_bearer(&h, b"secret123"));
    }

    #[test]
    fn bearer_empty_header_rejected() {
        let h = HeaderMap::new();
        assert!(!verify_bearer(&h, b"secret123"));
    }

    #[test]
    fn urldecode_basics() {
        assert_eq!(urlencoding_decode("abc"), "abc");
        assert_eq!(urlencoding_decode("a%20b"), "a b");
        assert_eq!(urlencoding_decode("a+b"), "a b");
        assert_eq!(urlencoding_decode("%41%42"), "AB");
        assert_eq!(urlencoding_decode("%zz"), "%zz");
    }
}
