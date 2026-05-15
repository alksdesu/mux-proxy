//! 进程启动：装配 AppState、绑定 hyper http1::Builder、接 sigterm/sigint 做受控关停。
//! preserve_header_case(true) + title_case_headers(false) 让 Anthropic 渠道 wire 大小写保真。
//! axum::serve 走 IntoMakeService 路径强制 lowercase header name 写回，故不用。

use crate::auth::{KeyCache, KeyCacheEntry, SingleFlight};
use crate::billing::{SnapshotVersion, SpendCache, UsageWriter};
use crate::channels::anthropic::key_pool::KeyPool as AnthropicKeyPool;
use crate::channels::anthropic::upstream_client::AnthropicUpstreamClient;
use crate::channels::copilot::{Breaker as CopilotBreaker, SessionTokenCache, UpstreamPool as CopilotPool};
use crate::concurrency::Limiter;
use crate::config::Config;
use crate::db::upstream::UpstreamChangeNotifier;
use crate::error::{AppError, AppResult};
use axum::extract::ConnectInfo;
use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::broadcast;
use tower::Service;
use tracing::{debug, info, warn};

/// 在 SIGTERM 信号到达后，最多再等这么久让 in-flight 请求跑完，
/// 防止计费日志半截写。超时后强制退出 accept loop 完成进程退出。
const GRACEFUL_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

pub async fn run() -> AppResult<()> {
    let cfg = Config::from_env()?;
    init_tracing(&cfg.log_level);

    let bind_addr = cfg.http_addr;
    info!(addr = %bind_addr, "starting copilot-proxy");

    let state = AppState::init(cfg).await?;
    let router = crate::http::router::build(state.clone());

    let listener = TcpListener::bind(bind_addr).await?;
    info!(addr = %bind_addr, "listening");

    let (shutdown_tx, _shutdown_rx) = broadcast::channel::<()>(1);
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            accept = listener.accept() => {
                let (stream, peer_addr) = match accept {
                    Ok(p) => p,
                    Err(e) => {
                        warn!(error = ?e, "accept failed; backing off 100ms");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                };
                spawn_connection(stream, peer_addr, router.clone(), shutdown_tx.subscribe());
            }
            _ = &mut shutdown => {
                info!("shutdown signal received; closing accept loop");
                let _ = shutdown_tx.send(());
                break;
            }
        }
    }

    // 给 in-flight conn task 一段时间完成 graceful_shutdown 后的清理。
    info!(timeout = ?GRACEFUL_DRAIN_TIMEOUT, "waiting for in-flight requests");
    tokio::time::sleep(GRACEFUL_DRAIN_TIMEOUT).await;
    info!("shutdown complete");
    Ok(())
}

fn spawn_connection(
    stream: tokio::net::TcpStream,
    peer_addr: SocketAddr,
    router: axum::Router,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    tokio::spawn(async move {
        let io = TokioIo::new(stream);
        let svc = hyper::service::service_fn(move |mut req: hyper::Request<hyper::body::Incoming>| {
            // 把 peer 地址塞 extensions，host_guard / 客户端 IP 提取走 cf-connecting-ip
            // 优先，本字段是 fallback 用。
            req.extensions_mut().insert(ConnectInfo(peer_addr));
            let mut svc = router.clone();
            async move {
                let req: hyper::Request<axum::body::Body> =
                    req.map(axum::body::Body::new);
                svc.call(req).await
            }
        });
        let conn = http1::Builder::new()
            .preserve_header_case(true)
            .title_case_headers(false)
            .keep_alive(true)
            .serve_connection(io, svc)
            .with_upgrades();
        tokio::pin!(conn);
        tokio::select! {
            res = conn.as_mut() => {
                if let Err(e) = res {
                    debug!(error = ?e, peer = %peer_addr, "connection ended");
                }
            }
            _ = shutdown_rx.recv() => {
                conn.as_mut().graceful_shutdown();
                if let Err(e) = conn.await {
                    debug!(error = ?e, peer = %peer_addr, "connection drain ended");
                }
            }
        }
    });
}

/// 全局共享状态。所有 handler 经 `axum::extract::State` 拿。
#[derive(Clone)]
pub struct AppState {
    pub cfg: Arc<Config>,
    pub db: crate::db::Db,
    pub key_cache: Arc<KeyCache>,
    pub key_loader_sf: SingleFlight<String, Option<KeyCacheEntry>>,
    pub spend: Arc<SpendCache>,
    pub limiter: Arc<Limiter>,
    pub snapshot: Arc<SnapshotVersion>,
    pub usage_writer: UsageWriter,
    /// admin 写 upstream_keys 时 bump，让 key_pool 下一轮 acquire 强制重读。
    pub upstream_notifier: UpstreamChangeNotifier,
    /// Copilot 渠道运行时：池 / 熔断 / session token / 共享 HTTP 客户端。
    pub copilot_breaker: Arc<CopilotBreaker>,
    pub copilot_pool: Arc<CopilotPool>,
    pub copilot_session: Arc<SessionTokenCache>,
    pub copilot_http: Arc<reqwest::Client>,
    /// Anthropic 渠道运行时：官方 key 池 + 字节保真 hyper 上游客户端。
    pub anthropic_pool: Arc<AnthropicKeyPool>,
    pub anthropic_client: AnthropicUpstreamClient,
}

impl AppState {
    pub async fn init(cfg: Config) -> AppResult<Self> {
        let db = crate::db::init_pool(&cfg.database_url).await?;
        let snapshot = Arc::new(SnapshotVersion::new());
        let spend = Arc::new(SpendCache::init_from_db(&db).await?);
        let limiter = Limiter::new(snapshot.clone());
        tokio::spawn(limiter.clone().run_gc());

        let usage_writer = UsageWriter::new(db.clone(), spend.clone(), snapshot.clone());
        let upstream_notifier = UpstreamChangeNotifier::new();

        // 共享 reqwest 客户端：Copilot handler / session_token / web_search 都走它。
        // 禁自动 redirect；超时由各 caller 通过 .timeout() 显式给。
        let copilot_http = Arc::new(
            reqwest::Client::builder()
                .pool_idle_timeout(Duration::from_secs(90))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|e| AppError::Internal(format!("build copilot reqwest client: {e}")))?,
        );

        let copilot_breaker = Arc::new(CopilotBreaker::new());
        let copilot_pool = CopilotPool::new(
            db.clone(),
            copilot_breaker.clone(),
            upstream_notifier.handle(),
        );
        let copilot_session = SessionTokenCache::with_client((*copilot_http).clone());

        let anthropic_pool = AnthropicKeyPool::new(db.clone(), upstream_notifier.clone());
        tokio::spawn(anthropic_pool.clone().run_change_listener());

        let anthropic_client = AnthropicUpstreamClient::new(
            &cfg.anthropic_upstream_base,
            cfg.anthropic_upstream_timeout,
        )?;

        Ok(Self {
            cfg: Arc::new(cfg),
            db,
            key_cache: Arc::new(KeyCache::new()),
            key_loader_sf: SingleFlight::new(),
            spend,
            limiter,
            snapshot,
            usage_writer,
            upstream_notifier,
            copilot_breaker,
            copilot_pool,
            copilot_session,
            copilot_http,
            anthropic_pool,
            anthropic_client,
        })
    }
}

fn init_tracing(level: &str) {
    use tracing_subscriber::EnvFilter;
    use tracing_subscriber::fmt;
    use tracing_subscriber::prelude::*;

    let filter = EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().with_target(false).with_thread_ids(false))
        .init();
}

#[cfg(unix)]
async fn shutdown_signal() {
    let ctrl_c = async { signal::ctrl_c().await.ok(); };
    let term = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };
    tokio::select! {
        _ = ctrl_c => info!("SIGINT received"),
        _ = term => info!("SIGTERM received"),
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    signal::ctrl_c().await.ok();
    info!("SIGINT received");
}
