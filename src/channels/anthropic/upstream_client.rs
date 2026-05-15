//! hyper 1.x 上游 client：preserve_header_case=true 保住 ``CF-RAY`` 等头；
//! ALPN 锁 http/1.1 避免 h2 框架强制 lowercase header。reqwest 会自动解 gzip
//! 破坏字节保真，故这里直接用 hyper-util legacy Client。

use crate::channels::anthropic::request_strip::{is_request_hop_by_hop, REQUEST_STRIP};
use crate::error::{AppError, AppResult};
use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue, Method, Request, Uri};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper_rustls::{ConfigBuilderExt, HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::{Client, connect::HttpConnector};
use hyper_util::rt::TokioExecutor;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

pub type Body = Full<Bytes>;
pub type UpstreamResponse = http::Response<Incoming>;

/// Anthropic 上游连接器。一个实例对应一个 base_url（默认 ``https://api.anthropic.com``）。
/// Clone 廉价，内部 ``Arc<Client>``。
#[derive(Clone)]
pub struct AnthropicUpstreamClient {
    inner: Arc<Inner>,
}

struct Inner {
    base_authority: String,
    base_scheme: http::uri::Scheme,
    base_path_prefix: String,
    client: Client<HttpsConnector<HttpConnector>, Body>,
    request_timeout: Duration,
}

impl AnthropicUpstreamClient {
    pub fn new(base_url: &str, request_timeout: Duration) -> AppResult<Self> {
        let parsed: Uri = base_url
            .parse()
            .map_err(|e| AppError::Config(format!("invalid anthropic base url: {e}")))?;
        let scheme = parsed
            .scheme()
            .cloned()
            .ok_or_else(|| AppError::Config("anthropic base url missing scheme".into()))?;
        let authority = parsed
            .authority()
            .cloned()
            .ok_or_else(|| AppError::Config("anthropic base url missing authority".into()))?
            .to_string();
        let path = parsed.path().trim_end_matches('/').to_string();

        let tls_config = rustls::ClientConfig::builder()
            .with_native_roots()
            .map_err(|e| AppError::Config(format!("rustls native roots: {e}")))?
            .with_no_client_auth();

        let https = HttpsConnectorBuilder::new()
            .with_tls_config(tls_config)
            .https_or_http()
            .enable_http1()
            .build();

        let client: Client<HttpsConnector<HttpConnector>, Body> = Client::builder(TokioExecutor::new())
            .pool_idle_timeout(Duration::from_secs(90))
            .http1_preserve_header_case(true)
            .http1_title_case_headers(false)
            .build(https);

        Ok(Self {
            inner: Arc::new(Inner {
                base_authority: authority,
                base_scheme: scheme,
                base_path_prefix: path,
                client,
                request_timeout,
            }),
        })
    }

    /// 转发一次请求到 upstream。
    /// - ``path`` 已带前导 ``/``，``raw_query`` 不带 ``?``、原样字节附加。
    /// - ``headers`` 是客户端原始 header（含大小写信息），本函数负责 strip + 强制 accept-encoding。
    /// - ``api_key`` 由 key_pool 决策，写到 ``x-api-key``。
    pub async fn forward(
        &self,
        method: &Method,
        path: &str,
        raw_query: Option<&str>,
        headers: &HeaderMap,
        body: Bytes,
        api_key: &str,
    ) -> AppResult<UpstreamResponse> {
        let uri = self.build_uri(path, raw_query)?;
        let mut builder = Request::builder().method(method.clone()).uri(uri);
        let req_headers = builder
            .headers_mut()
            .ok_or_else(|| AppError::Internal("request builder headers missing".into()))?;
        copy_request_headers(headers, req_headers);
        force_accept_encoding_gzip(req_headers);
        insert_api_key(req_headers, api_key)?;

        let request = builder
            .body(Full::new(body))
            .map_err(|e| AppError::Upstream(format!("build request: {e}")))?;

        let send = self.inner.client.request(request);
        match timeout(self.inner.request_timeout, send).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(e)) => Err(AppError::Upstream(format!("upstream error: {e}"))),
            Err(_) => Err(AppError::UpstreamTimeout),
        }
    }

    fn build_uri(&self, path: &str, raw_query: Option<&str>) -> AppResult<Uri> {
        let mut full = String::with_capacity(64 + path.len());
        full.push_str(self.inner.base_scheme.as_str());
        full.push_str("://");
        full.push_str(&self.inner.base_authority);
        full.push_str(&self.inner.base_path_prefix);
        if !path.starts_with('/') {
            full.push('/');
        }
        full.push_str(path);
        if let Some(q) = raw_query {
            if !q.is_empty() {
                full.push('?');
                full.push_str(q);
            }
        }
        full.parse::<Uri>()
            .map_err(|e| AppError::Upstream(format!("compose upstream uri: {e}")))
    }
}

fn copy_request_headers(src: &HeaderMap, dst: &mut HeaderMap) {
    for (name, value) in src.iter() {
        let lower = name.as_str().to_ascii_lowercase();
        if is_request_hop_by_hop(&lower) {
            continue;
        }
        dst.append(name.clone(), value.clone());
    }
    let _ = REQUEST_STRIP;
}

fn force_accept_encoding_gzip(headers: &mut HeaderMap) {
    headers.remove(http::header::ACCEPT_ENCODING);
    headers.insert(http::header::ACCEPT_ENCODING, HeaderValue::from_static("gzip"));
}

fn insert_api_key(headers: &mut HeaderMap, api_key: &str) -> AppResult<()> {
    let value = HeaderValue::from_str(api_key)
        .map_err(|_| AppError::Config("anthropic api key contains invalid bytes".into()))?;
    let name = HeaderName::from_static("x-api-key");
    headers.remove(&name);
    headers.insert(name, value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::header::HOST;
    use std::time::Duration;

    fn client() -> AnthropicUpstreamClient {
        AnthropicUpstreamClient::new("https://api.anthropic.com", Duration::from_secs(60)).unwrap()
    }

    #[test]
    fn build_uri_appends_path_and_query() {
        let c = client();
        let uri = c.build_uri("/v1/messages", Some("foo=bar&baz=1")).unwrap();
        assert_eq!(uri.to_string(), "https://api.anthropic.com/v1/messages?foo=bar&baz=1");
    }

    #[test]
    fn build_uri_without_query() {
        let c = client();
        let uri = c.build_uri("/v1/models", None).unwrap();
        assert_eq!(uri.to_string(), "https://api.anthropic.com/v1/models");
    }

    #[test]
    fn build_uri_inserts_leading_slash() {
        let c = client();
        let uri = c.build_uri("v1/foo", None).unwrap();
        assert_eq!(uri.to_string(), "https://api.anthropic.com/v1/foo");
    }

    #[test]
    fn copy_drops_hop_by_hop_and_host() {
        let mut src = HeaderMap::new();
        src.insert(HOST, HeaderValue::from_static("client.local"));
        src.insert(http::header::CONNECTION, HeaderValue::from_static("keep-alive"));
        src.insert(http::header::TRANSFER_ENCODING, HeaderValue::from_static("chunked"));
        src.insert(http::header::AUTHORIZATION, HeaderValue::from_static("Bearer x"));
        src.insert(http::header::USER_AGENT, HeaderValue::from_static("ut"));
        let mut dst = HeaderMap::new();
        copy_request_headers(&src, &mut dst);
        assert!(!dst.contains_key(HOST));
        assert!(!dst.contains_key(http::header::CONNECTION));
        assert!(!dst.contains_key(http::header::TRANSFER_ENCODING));
        assert!(dst.contains_key(http::header::AUTHORIZATION));
        assert!(dst.contains_key(http::header::USER_AGENT));
    }

    #[test]
    fn force_accept_encoding_overwrites() {
        let mut h = HeaderMap::new();
        h.insert(http::header::ACCEPT_ENCODING, HeaderValue::from_static("br, zstd, identity"));
        force_accept_encoding_gzip(&mut h);
        assert_eq!(h.get(http::header::ACCEPT_ENCODING).unwrap(), "gzip");
        // 仅一个值，不应留下 br/zstd
        assert_eq!(h.get_all(http::header::ACCEPT_ENCODING).iter().count(), 1);
    }

    #[test]
    fn insert_api_key_writes_lowercase_name() {
        let mut h = HeaderMap::new();
        insert_api_key(&mut h, "sk-ant-secret").unwrap();
        let name = HeaderName::from_static("x-api-key");
        assert_eq!(h.get(&name).unwrap(), "sk-ant-secret");
    }
}
