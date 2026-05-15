//! 顶层错误类型。所有 fallible 业务路径返回 `Result<T, AppError>`。
//! IntoResponse 实现挂在 `http::error_resp` 里，避免本文件依赖 axum。

use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("config error: {0}")]
    Config(String),

    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),

    #[error("database migration failed: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),

    #[error("unauthorized")]
    Unauthorized,

    #[error("forbidden: {0}")]
    Forbidden(String),

    #[error("not found")]
    NotFound,

    #[error("bad request: {0}")]
    BadRequest(String),

    #[error("rate limited: {0}")]
    RateLimited(String),

    #[error("quota exceeded")]
    QuotaExceeded,

    #[error("concurrency exceeded")]
    ConcurrencyExceeded,

    #[error("upstream error: {0}")]
    Upstream(String),

    #[error("upstream timeout")]
    UpstreamTimeout,

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("internal error: {0}")]
    Internal(String),
}

impl AppError {
    pub fn status_code(&self) -> u16 {
        match self {
            AppError::Unauthorized => 401,
            AppError::Forbidden(_) => 403,
            AppError::NotFound => 404,
            AppError::BadRequest(_) => 400,
            AppError::RateLimited(_) | AppError::QuotaExceeded | AppError::ConcurrencyExceeded => 429,
            AppError::Upstream(_) | AppError::UpstreamTimeout => 502,
            AppError::Config(_)
            | AppError::Db(_)
            | AppError::Migrate(_)
            | AppError::Io(_)
            | AppError::Serde(_)
            | AppError::Internal(_) => 500,
        }
    }
}

pub type AppResult<T> = Result<T, AppError>;
