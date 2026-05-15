//! 进程启动：装配 AppState、绑定 axum、接 sigterm/sigint 做受控关停。
//! 关停信号到达后停止接新连接，已有请求跑完才退出，避免计费日志半截写。

use crate::auth::{KeyCache, KeyCacheEntry, SingleFlight};
use crate::billing::{SnapshotVersion, SpendCache, UsageWriter};
use crate::concurrency::Limiter;
use crate::config::Config;
use crate::db::upstream::UpstreamChangeNotifier;
use crate::error::{AppError, AppResult};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal;
use tracing::info;

pub async fn run() -> AppResult<()> {
    let cfg = Config::from_env()?;
    init_tracing(&cfg.log_level);

    info!(addr = %cfg.http_addr, "starting copilot-proxy");

    let state = AppState::init(cfg.clone()).await?;
    let router = crate::http::router::build(state.clone());

    let listener = TcpListener::bind(cfg.http_addr).await?;
    axum::serve(listener, router.into_make_service_with_connect_info::<std::net::SocketAddr>())
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|e| AppError::Internal(format!("server error: {e}")))?;

    info!("shutdown complete");
    Ok(())
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
}

impl AppState {
    pub async fn init(cfg: Config) -> AppResult<Self> {
        let db = crate::db::init_pool(&cfg.database_url).await?;
        let snapshot = Arc::new(SnapshotVersion::new());
        let spend = Arc::new(SpendCache::init_from_db(&db).await?);
        let limiter = Limiter::new(snapshot.clone());
        tokio::spawn(limiter.clone().run_gc());

        let usage_writer = UsageWriter::new(db.clone(), spend.clone(), snapshot.clone());

        Ok(Self {
            cfg: Arc::new(cfg),
            db,
            key_cache: Arc::new(KeyCache::new()),
            key_loader_sf: SingleFlight::new(),
            spend,
            limiter,
            snapshot,
            usage_writer,
            upstream_notifier: UpstreamChangeNotifier::new(),
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
