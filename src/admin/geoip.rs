//! /admin/geoip?ip=x.x.x.x：把 ip-api.com 的免费查询转一道，
//! 失败返 `{"status":"fail"}` 不抛错，dashboard 期望永远是 JSON。

use crate::app::AppState;
use crate::error::{AppError, AppResult};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use std::collections::HashMap;
use std::time::Duration;

const GEO_TIMEOUT: Duration = Duration::from_secs(5);
const FAIL_BODY: &str = r#"{"status":"fail"}"#;

pub async fn handler(
    State(_state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> AppResult<Response> {
    let ip = params
        .get("ip")
        .map(|s| s.trim().trim_start_matches("::ffff:"))
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::BadRequest("missing ip".into()))?;

    let client = reqwest::Client::builder()
        .timeout(GEO_TIMEOUT)
        .build()
        .map_err(|e| AppError::Internal(e.to_string()))?;
    let url = format!(
        "http://ip-api.com/json/{}?fields=status,lat,lon,country,city",
        urlencoding_minimal(ip)
    );
    let resp = client.get(&url).send().await;
    let body = match resp {
        Ok(r) => r.text().await.unwrap_or_else(|_| FAIL_BODY.to_string()),
        Err(_) => FAIL_BODY.to_string(),
    };
    Ok((
        StatusCode::OK,
        [("Content-Type", "application/json")],
        body,
    )
        .into_response())
}

fn urlencoding_minimal(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            '0'..='9' | 'a'..='z' | 'A'..='Z' | '.' | ':' | '-' | '_' => c.to_string(),
            other => {
                let mut buf = [0u8; 4];
                let bytes = other.encode_utf8(&mut buf).as_bytes();
                bytes.iter().map(|b| format!("%{:02X}", b)).collect()
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urlencode_passes_safe_chars() {
        assert_eq!(urlencoding_minimal("1.2.3.4"), "1.2.3.4");
        assert_eq!(urlencoding_minimal("::1"), "::1");
    }

    #[test]
    fn urlencode_escapes_unsafe() {
        assert_eq!(urlencoding_minimal(" "), "%20");
    }
}
