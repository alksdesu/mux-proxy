//! 全局配置。所有可调项都从环境变量加载，启动失败时直接 panic 不要静默。

use crate::channels::anthropic::model_splice::{RewriteRule, parse_rewrite_rules};
use crate::error::{AppError, AppResult};
use std::env;
use std::net::SocketAddr;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Config {
    pub http_addr: SocketAddr,
    pub tls_addr: Option<SocketAddr>,
    pub tls_cert_path: Option<String>,
    pub tls_key_path: Option<String>,
    pub database_url: String,
    pub admin_key: String,
    pub exa_api_keys: Vec<String>,
    pub dashboard_path: String,
    pub allow_fast_models: bool,
    pub copilot_upstream_timeout_stream: Duration,
    pub copilot_upstream_timeout_unary: Duration,
    pub anthropic_upstream_base: String,
    pub anthropic_upstream_timeout: Duration,
    pub anthropic_rewrite_rules: Vec<RewriteRule>,
    pub host_whitelist: Vec<String>,
    pub require_cf_connecting_ip: bool,
    pub log_level: String,
}

impl Config {
    pub fn from_env() -> AppResult<Self> {
        let http_port: u16 = env_or("PORT", "3000").parse()
            .map_err(|e| AppError::Config(format!("invalid PORT: {e}")))?;
        let http_addr = SocketAddr::from(([0, 0, 0, 0], http_port));

        let tls_port: Option<u16> = match env::var("TLS_PORT").ok().filter(|s| !s.is_empty()) {
            Some(s) => Some(s.parse().map_err(|e| AppError::Config(format!("invalid TLS_PORT: {e}")))?),
            None => None,
        };
        let tls_addr = tls_port.map(|p| SocketAddr::from(([0, 0, 0, 0], p)));

        let database_url = env::var("DATABASE_URL")
            .map_err(|_| AppError::Config("DATABASE_URL must be set".into()))?;

        let admin_key = env::var("ADMIN_KEY")
            .map_err(|_| AppError::Config("ADMIN_KEY must be set".into()))?;
        if admin_key.len() < 16 {
            return Err(AppError::Config("ADMIN_KEY must be at least 16 chars".into()));
        }

        let exa_api_keys = env::var("EXA_API_KEY")
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();

        let host_whitelist = env::var("HOST_WHITELIST")
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let anthropic_rewrite_rules = parse_rewrite_rules(
            &env::var("MUX_ANTHROPIC_REWRITE_RULES").unwrap_or_default(),
        )
        .map_err(|e| AppError::Config(format!("invalid MUX_ANTHROPIC_REWRITE_RULES: {e}")))?;

        Ok(Config {
            http_addr,
            tls_addr,
            tls_cert_path: env::var("TLS_CERT_PATH").ok(),
            tls_key_path: env::var("TLS_KEY_PATH").ok(),
            database_url,
            admin_key,
            exa_api_keys,
            dashboard_path: env_or("DASHBOARD_PATH", "/p-f7077038"),
            allow_fast_models: env_or("ALLOW_FAST_MODELS", "true").parse().unwrap_or(true),
            copilot_upstream_timeout_stream: Duration::from_secs(
                env_or("COPILOT_TIMEOUT_STREAM", "240").parse().unwrap_or(240),
            ),
            copilot_upstream_timeout_unary: Duration::from_secs(
                env_or("COPILOT_TIMEOUT_UNARY", "60").parse().unwrap_or(60),
            ),
            anthropic_upstream_base: env_or("ANTHROPIC_UPSTREAM", "https://api.anthropic.com"),
            anthropic_upstream_timeout: Duration::from_secs(
                env_or("ANTHROPIC_TIMEOUT", "600").parse().unwrap_or(600),
            ),
            anthropic_rewrite_rules,
            host_whitelist,
            require_cf_connecting_ip: env_or("REQUIRE_CF_CONNECTING_IP", "false")
                .parse()
                .unwrap_or(false),
            log_level: env_or("RUST_LOG", "info"),
        })
    }
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}
