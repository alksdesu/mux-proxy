//! Admin 子路由装配。`/admin/*` 与 `/stats*` 都套 admin_auth 中间件，
//! `/admin/usage/export` 单独允许 ?token=，匹配旧 dashboard `a[download]` 行为。
//! `/ws` 由 axum::extract::WebSocketUpgrade 自己处理升级，鉴权挪进 socket loop。

use crate::admin::{errors, export, geoip, keys, stats, timeseries, upstream, usage, ws};
use crate::app::AppState;
use crate::http::middleware::admin_auth::{admin_auth_layer, admin_auth_with_query_token};
use axum::Router;
use axum::middleware::from_fn_with_state;
use axum::routing::{get, post};

pub fn build_admin_router(state: AppState) -> Router<AppState> {
    let auth_layer = from_fn_with_state(state.clone(), admin_auth_layer);
    let token_layer = from_fn_with_state(state.clone(), admin_auth_with_query_token);

    let admin = Router::new()
        .route(
            "/admin/keys",
            get(keys::list_handler)
                .post(keys::create_handler)
                .patch(keys::patch_handler)
                .delete(keys::delete_handler),
        )
        .route("/admin/keys/full", get(keys::list_full_handler))
        .route("/admin/usage", get(usage::list_handler))
        .route("/admin/usage/ips", get(usage::ips_handler))
        .route("/admin/usage/{id}", get(usage::detail_handler))
        .route(
            "/admin/errors",
            get(errors::list_handler).delete(errors::delete_handler),
        )
        .route("/admin/errors/{id}", get(errors::detail_handler))
        .route(
            "/admin/upstream",
            get(upstream::list_handler)
                .post(upstream::create_handler)
                .patch(upstream::patch_handler)
                .delete(upstream::delete_handler),
        )
        .route(
            "/admin/upstream/breaker",
            get(upstream::breaker_get_handler).post(upstream::breaker_post_handler),
        )
        .route("/admin/geoip", get(geoip::handler))
        .route("/admin/stats/timeseries", get(timeseries::handler))
        .route("/stats", get(stats::stats_handler))
        .route("/stats/reset", post(stats::reset_handler))
        .layer(auth_layer);

    let export_router = Router::new()
        .route("/admin/usage/export", get(export::handler))
        .layer(token_layer);

    let ws_router = Router::new().route("/ws", get(ws::upgrade_handler));

    Router::new()
        .merge(admin)
        .merge(export_router)
        .merge(ws_router)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::query::parse_channel;
    use crate::channels::ChannelKind;
    use axum::body::to_bytes;
    use axum::http::{Request, StatusCode, header};
    use tower::ServiceExt;

    fn make_request(method: &str, uri: &str, auth: Option<&str>) -> Request<axum::body::Body> {
        let mut b = Request::builder().method(method).uri(uri);
        if let Some(token) = auth {
            b = b.header(header::AUTHORIZATION, format!("Bearer {token}"));
        }
        b.body(axum::body::Body::empty()).expect("build request")
    }

    #[test]
    fn channel_param_accepts_known_values() {
        assert_eq!(parse_channel(Some("all")).unwrap(), None);
        assert_eq!(parse_channel(Some("copilot")).unwrap(), Some(ChannelKind::Copilot));
        assert_eq!(parse_channel(Some("anthropic")).unwrap(), Some(ChannelKind::Anthropic));
    }

    #[test]
    fn channel_param_rejects_unknown() {
        assert!(parse_channel(Some("vertex")).is_err());
    }

    #[tokio::test]
    async fn admin_bearer_returns_404_on_missing_token() {
        let app = router_with_dummy_state();
        let resp = app
            .oneshot(make_request("GET", "/stats", None))
            .await
            .expect("oneshot");
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = to_bytes(resp.into_body(), 4096).await.unwrap();
        assert!(std::str::from_utf8(&body).unwrap().contains("not found"));
    }

    #[tokio::test]
    async fn admin_bearer_returns_404_on_wrong_token() {
        let app = router_with_dummy_state();
        let resp = app
            .oneshot(make_request("GET", "/stats", Some("wrong-token-1234")))
            .await
            .expect("oneshot");
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    fn router_with_dummy_state() -> Router {
        use crate::app::AppState;
        use crate::auth::{KeyCache, SingleFlight};
        use crate::billing::{SnapshotVersion, SpendCache, UsageWriter};
        use crate::breaker::Registry as BreakerRegistry;
        use crate::concurrency::Limiter;
        use crate::config::Config;
        use crate::db::upstream::UpstreamChangeNotifier;
        use std::sync::Arc;

        let cfg = Config {
            http_addr: "0.0.0.0:0".parse().unwrap(),
            tls_addr: None,
            tls_cert_path: None,
            tls_key_path: None,
            database_url: "postgres://stub".into(),
            admin_key: "stub-admin-key-must-be-at-least-16-chars".into(),
            exa_api_keys: vec![],
            dashboard_path: "/p-test".into(),
            allow_fast_models: true,
            copilot_upstream_timeout_stream: std::time::Duration::from_secs(240),
            copilot_upstream_timeout_unary: std::time::Duration::from_secs(60),
            anthropic_upstream_base: "https://api.anthropic.com".into(),
            anthropic_upstream_timeout: std::time::Duration::from_secs(600),
            host_whitelist: vec![],
            require_cf_connecting_ip: false,
            log_level: "info".into(),
        };

        let snapshot = Arc::new(SnapshotVersion::new());
        let spend = Arc::new(SpendCache::new());
        let limiter = Limiter::new(snapshot.clone());
        let db = stub_db();
        let usage_writer = UsageWriter::new(db.clone(), spend.clone(), snapshot.clone());
        let state = AppState {
            cfg: Arc::new(cfg),
            db,
            key_cache: Arc::new(KeyCache::new()),
            key_loader_sf: SingleFlight::new(),
            spend,
            limiter,
            snapshot,
            usage_writer,
            breaker: BreakerRegistry::new(),
            upstream_notifier: UpstreamChangeNotifier::new(),
        };
        super::build_admin_router(state.clone()).with_state(state)
    }

    fn stub_db() -> crate::db::Db {
        // 单元测试不会触达 DB pool；用 lazy pool 占位，oneshot 401/404 路径不会取连接。
        // 真要在测试里跑 SQL 走集成测试目录单独建。
        use sqlx::postgres::PgPoolOptions;
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_lazy("postgres://stub:stub@127.0.0.1:1/stub")
            .expect("lazy pool init");
        crate::db::Db::from_pool(pool)
    }
}
