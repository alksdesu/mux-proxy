//! Anthropic 渠道端到端集成测试。wiremock 启 mock 上游验三种关键路径：
//! 1) 非 gzip JSON 响应 → model 字节还原 + 字节透传
//! 2) gzip JSON 响应 → 解压改写重压 + 客户端拿到的明文 model 已还原
//! 3) 502 兜底 → upstream 5xx 不破坏代理响应链路

use bytes::Bytes;
use flate2::Compression;
use flate2::write::GzEncoder;
use http::{HeaderMap, Method, StatusCode};
use http_body_util::BodyExt;
use mux_proxy::auth::KeyCacheEntry;
use mux_proxy::billing::{SnapshotVersion, SpendCache, UsageWriter};
use mux_proxy::channels::ChannelKind;
use mux_proxy::channels::anthropic::handler::{self, HandlerContext, ProxyRequest};
use mux_proxy::channels::anthropic::key_pool::{KeyPool, PooledKey};
use mux_proxy::channels::anthropic::model_splice::RewriteRule;
use mux_proxy::channels::anthropic::upstream_client::AnthropicUpstreamClient;
use mux_proxy::concurrency::{ConcurrencyGuard, Limiter};
use mux_proxy::db::Db;
use mux_proxy::db::upstream::UpstreamChangeNotifier;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn lazy_db() -> Db {
    // connect_lazy 不真连 PG；UsageWriter spawn 写入会失败但 fire-and-forget，
    // 测试只验响应字节，不验 spend 累计。
    let pool = sqlx::PgPool::connect_lazy("postgres://stub:stub@127.0.0.1:1/stub")
        .expect("connect_lazy");
    Db::from_pool(pool)
}

fn dummy_entry() -> KeyCacheEntry {
    KeyCacheEntry {
        id: 1,
        name: "test-key".into(),
        upstream_key: "anthropic:sk-ant-test".into(),
        quota: -1.0,
        allow_fast: true,
        max_concurrency: -1,
        rpm_limit: -1,
        allowed_models: Vec::new(),
        channel_kind: ChannelKind::Anthropic,
        fetched_at: Instant::now(),
    }
}

fn build_ctx(
    base_url: &str,
    rewrite_rules: Vec<RewriteRule>,
) -> (HandlerContext, Arc<Limiter>) {
    let snapshot = Arc::new(SnapshotVersion::new());
    let spend = Arc::new(SpendCache::new());
    let limiter = Limiter::new(snapshot.clone());
    let db = lazy_db();
    let usage_writer = UsageWriter::new(db.clone(), spend, snapshot);
    let notifier = UpstreamChangeNotifier::new();

    let key_pool = KeyPool::test_only_with_keys(
        vec![PooledKey {
            id: 1,
            name: "mock-up".into(),
            token: "sk-ant-test".into(),
        }],
        db,
        notifier,
    );

    let client = AnthropicUpstreamClient::new(base_url, Duration::from_secs(10))
        .expect("build upstream client");
    let guard: ConcurrencyGuard = limiter
        .try_acquire("test-key", -1)
        .expect("acquire guard for test");

    let ctx = HandlerContext {
        client,
        key_pool,
        usage_writer,
        rewrite_rules,
        key_cache_entry: dummy_entry(),
        client_ip: Some("127.0.0.1".into()),
        concurrency_guard: guard,
    };
    (ctx, limiter)
}

async fn collect_body(resp: handler::ProxyResponse) -> (StatusCode, HeaderMap, Bytes) {
    let (parts, body) = resp.into_parts();
    let collected = body.collect().await.expect("collect body");
    (parts.status, parts.headers, collected.to_bytes())
}

#[tokio::test]
async fn non_gzip_json_response_model_restored_to_client_name() {
    let server = MockServer::start().await;

    let upstream_body =
        r#"{"id":"msg_01","model":"claude-3-5-sonnet-20241022","content":[],"usage":{"input_tokens":10,"output_tokens":5}}"#;

    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(upstream_body.as_bytes().to_vec(), "application/json"),
        )
        .mount(&server)
        .await;

    let (ctx, _limiter) = build_ctx(
        &server.uri(),
        vec![RewriteRule::new("claude-sonnet-4-5", "claude-3-5-sonnet-20241022")],
    );

    let mut req_headers = HeaderMap::new();
    req_headers.insert(http::header::CONTENT_TYPE, "application/json".parse().unwrap());
    let req = ProxyRequest {
        method: Method::POST,
        path: "/v1/messages".into(),
        raw_query: None,
        headers: req_headers,
        body: Bytes::from(
            r#"{"model":"claude-sonnet-4-5","messages":[{"role":"user","content":"hi"}]}"#,
        ),
    };

    let resp = handler::handle(ctx, req).await.expect("handle non-gzip");
    let (status, _headers, body) = collect_body(resp).await;
    assert_eq!(status, StatusCode::OK);

    let body_str = std::str::from_utf8(&body).expect("utf8 body");
    // 响应 model 被还原成客户端发送的原始名（rewrite_rule 反向回放）。
    assert!(
        body_str.contains("\"model\":\"claude-sonnet-4-5\""),
        "expected client model to be restored, got: {body_str}"
    );
    // 上游真名不出现在响应里。
    assert!(
        !body_str.contains("claude-3-5-sonnet-20241022"),
        "upstream model must not leak: {body_str}"
    );
}

#[tokio::test]
async fn no_matching_rewrite_rule_passes_response_through_byte_for_byte() {
    let server = MockServer::start().await;

    let upstream_body = r#"{"id":"msg_02","model":"claude-other-model","content":[]}"#;

    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(upstream_body.as_bytes().to_vec(), "application/json"),
        )
        .mount(&server)
        .await;

    let (ctx, _limiter) = build_ctx(
        &server.uri(),
        vec![RewriteRule::new("claude-sonnet-4-5", "claude-3-5-sonnet-20241022")],
    );

    let mut req_headers = HeaderMap::new();
    req_headers.insert(http::header::CONTENT_TYPE, "application/json".parse().unwrap());
    let req = ProxyRequest {
        method: Method::POST,
        path: "/v1/messages".into(),
        raw_query: None,
        headers: req_headers,
        body: Bytes::from(r#"{"model":"claude-haiku","messages":[]}"#),
    };

    let resp = handler::handle(ctx, req).await.expect("handle no-match");
    let (status, _, body) = collect_body(resp).await;
    assert_eq!(status, StatusCode::OK);

    // 客户端 model 不命中任何 rewrite_rule，响应字节应原样透传。
    assert_eq!(&body[..], upstream_body.as_bytes());
}

#[tokio::test]
async fn gzip_json_response_decompressed_rewritten_recompressed() {
    let server = MockServer::start().await;

    let plain_upstream =
        r#"{"id":"msg_03","model":"claude-3-5-sonnet-20241022","content":[]}"#;
    let mut encoder = GzEncoder::new(Vec::new(), Compression::new(6));
    encoder.write_all(plain_upstream.as_bytes()).unwrap();
    let gzipped = encoder.finish().unwrap();

    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(gzipped.clone(), "application/json")
                .insert_header("content-encoding", "gzip"),
        )
        .mount(&server)
        .await;

    let (ctx, _limiter) = build_ctx(
        &server.uri(),
        vec![RewriteRule::new("claude-sonnet-4-5", "claude-3-5-sonnet-20241022")],
    );

    let mut req_headers = HeaderMap::new();
    req_headers.insert(http::header::CONTENT_TYPE, "application/json".parse().unwrap());
    let req = ProxyRequest {
        method: Method::POST,
        path: "/v1/messages".into(),
        raw_query: None,
        headers: req_headers,
        body: Bytes::from(r#"{"model":"claude-sonnet-4-5","messages":[]}"#),
    };

    let resp = handler::handle(ctx, req).await.expect("handle gzip");
    let (status, headers, body) = collect_body(resp).await;
    assert_eq!(status, StatusCode::OK);

    // 响应仍标 gzip，客户端 gunzip 后看到的 model 应被还原成客户端原始名。
    let content_encoding = headers
        .iter()
        .find(|(n, _)| n.as_str().eq_ignore_ascii_case("content-encoding"))
        .map(|(_, v)| v.to_str().unwrap_or(""));
    assert_eq!(content_encoding, Some("gzip"), "must preserve gzip encoding");

    use flate2::read::GzDecoder;
    use std::io::Read;
    let mut decoder = GzDecoder::new(&body[..]);
    let mut decompressed = String::new();
    decoder
        .read_to_string(&mut decompressed)
        .expect("decompress response gzip");

    assert!(
        decompressed.contains("\"model\":\"claude-sonnet-4-5\""),
        "decompressed model must be restored, got: {decompressed}"
    );
    assert!(
        !decompressed.contains("claude-3-5-sonnet-20241022"),
        "upstream model name leaked through gzip path: {decompressed}"
    );
}

#[tokio::test]
async fn sse_stream_model_restored_in_each_event() {
    let server = MockServer::start().await;

    // 三段 SSE 事件，模拟 message_start / content_block_delta / message_delta(含 usage)。
    // 注意上游 model 字段在 message_start 和 message_delta 各出现一次，都要被还原。
    let sse_body = "event: message_start\n\
data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_x\",\"model\":\"claude-3-5-sonnet-20241022\",\"usage\":{\"input_tokens\":12}}}\n\
\n\
event: content_block_delta\n\
data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\
\n\
event: message_delta\n\
data: {\"type\":\"message_delta\",\"model\":\"claude-3-5-sonnet-20241022\",\"usage\":{\"output_tokens\":7}}\n\
\n";

    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(sse_body.as_bytes().to_vec(), "text/event-stream"),
        )
        .mount(&server)
        .await;

    let (ctx, _limiter) = build_ctx(
        &server.uri(),
        vec![RewriteRule::new("claude-sonnet-4-5", "claude-3-5-sonnet-20241022")],
    );

    let mut req_headers = HeaderMap::new();
    req_headers.insert(http::header::CONTENT_TYPE, "application/json".parse().unwrap());
    let req = ProxyRequest {
        method: Method::POST,
        path: "/v1/messages".into(),
        raw_query: None,
        headers: req_headers,
        body: Bytes::from(r#"{"model":"claude-sonnet-4-5","stream":true,"messages":[]}"#),
    };

    let resp = handler::handle(ctx, req).await.expect("handle sse");
    let (status, headers, body) = collect_body(resp).await;
    assert_eq!(status, StatusCode::OK);

    let ct = headers
        .iter()
        .find(|(n, _)| n.as_str().eq_ignore_ascii_case("content-type"))
        .map(|(_, v)| v.to_str().unwrap_or(""));
    assert_eq!(ct, Some("text/event-stream"));

    let body_str = std::str::from_utf8(&body).expect("utf8 sse");
    let restored_count = body_str.matches(r#""model":"claude-sonnet-4-5""#).count();
    assert_eq!(
        restored_count, 2,
        "expected model to be restored in both message_start + message_delta events, got body: {body_str}"
    );
    assert!(
        !body_str.contains("claude-3-5-sonnet-20241022"),
        "upstream model leaked through SSE path: {body_str}"
    );
    // event 行结构保留
    assert!(body_str.contains("event: message_start"));
    assert!(body_str.contains("event: message_delta"));
}

#[tokio::test]
async fn upstream_5xx_passes_through_with_buffered_body() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(
            ResponseTemplate::new(503).set_body_raw(
                br#"{"error":{"type":"overloaded","message":"upstream busy"}}"#.to_vec(),
                "application/json",
            ),
        )
        .mount(&server)
        .await;

    let (ctx, _limiter) = build_ctx(&server.uri(), vec![]);

    let mut req_headers = HeaderMap::new();
    req_headers.insert(http::header::CONTENT_TYPE, "application/json".parse().unwrap());
    let req = ProxyRequest {
        method: Method::POST,
        path: "/v1/messages".into(),
        raw_query: None,
        headers: req_headers,
        body: Bytes::from(r#"{"model":"claude-haiku"}"#),
    };

    let resp = handler::handle(ctx, req).await.expect("handle 5xx");
    let (status, _, body) = collect_body(resp).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        std::str::from_utf8(&body).unwrap().contains("overloaded"),
        "5xx body should pass through verbatim"
    );
}
