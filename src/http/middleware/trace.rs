//! 给每个请求挂一个 `x-request-id`：客户端有就用客户端的，否则生成 UUID。
//! 写到 `Request::extensions` 供下游 handler 拿，同时镜像到响应头。

use axum::extract::Request;
use axum::http::HeaderValue;
use axum::middleware::Next;
use axum::response::Response;

const HEADER: &str = "x-request-id";

#[derive(Clone, Debug)]
pub struct TraceId(pub String);

pub async fn trace_id_layer(mut req: Request, next: Next) -> Response {
    let incoming = req
        .headers()
        .get(HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let id = incoming.unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string());

    req.extensions_mut().insert(TraceId(id.clone()));
    let mut resp = next.run(req).await;
    if let Ok(v) = HeaderValue::from_str(&id) {
        resp.headers_mut().insert(HEADER, v);
    }
    resp
}
