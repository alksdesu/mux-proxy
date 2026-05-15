//! /admin/usage/export：流式 JSON 数组，分批拉避免 OOM。
//! 鉴权由 admin_auth_with_query_token 处理（同时接受 Bearer 头和 ?token=）。

use crate::admin::query::parse_channel;
use crate::app::AppState;
use crate::db;
use crate::error::AppResult;
use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::Response;
use bytes::Bytes;
use chrono::Utc;
use futures::StreamExt;
use std::collections::HashMap;

pub async fn handler(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> AppResult<Response> {
    let channel = parse_channel(params.get("channel").map(String::as_str))?;
    let key_owned = params.get("key").cloned();

    let filename = format!(
        "usage_{}_{}.json",
        key_owned.as_deref().unwrap_or("all"),
        Utc::now().format("%Y-%m-%d"),
    );
    let disp = format!(r#"attachment; filename="{}""#, filename);

    let db = state.db.clone();
    let stream = async_stream::stream! {
        yield Ok::<Bytes, std::io::Error>(Bytes::from_static(b"[\n"));
        let key_ref = key_owned.as_deref();
        let mut s = db::usage::export_usage_stream(&db, key_ref, channel);
        let mut first = true;
        while let Some(item) = s.next().await {
            match item {
                Ok(row) => {
                    let prefix: &'static [u8] = if first { b"" } else { b",\n" };
                    first = false;
                    let line = serde_json::to_vec(&row).unwrap_or_else(|_| b"null".to_vec());
                    let mut buf = Vec::with_capacity(prefix.len() + line.len());
                    buf.extend_from_slice(prefix);
                    buf.extend_from_slice(&line);
                    yield Ok(Bytes::from(buf));
                }
                Err(_) => break,
            }
        }
        yield Ok(Bytes::from_static(b"\n]\n"));
    };

    let body = Body::from_stream(stream);
    let resp = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, HeaderValue::from_static("application/json"))
        .header(header::CONTENT_DISPOSITION, HeaderValue::from_str(&disp).unwrap_or_else(|_| HeaderValue::from_static("attachment")))
        .body(body)
        .map_err(|e| crate::error::AppError::Internal(e.to_string()))?;
    Ok(resp)
}
