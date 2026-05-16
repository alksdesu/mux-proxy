//! AppError → axum Response。GENERIC_ERROR_MESSAGES 兜底文案见 `shared::generic_errors`。

use crate::error::AppError;
use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = StatusCode::from_u16(self.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

        // NotFound 走纯文本短回包，避免泄露 admin 路由的存在
        if matches!(self, AppError::NotFound) {
            return (StatusCode::NOT_FOUND, "not found").into_response();
        }

        let body = json!({
            "error": {
                "type": error_type(&self),
                "message": self.to_string(),
            }
        });
        (status, Json(body)).into_response()
    }
}

fn error_type(err: &AppError) -> &'static str {
    match err {
        AppError::Unauthorized => "authentication_error",
        AppError::Forbidden(_) | AppError::ModelNotAllowed { .. } => "permission_error",
        AppError::NotFound => "not_found_error",
        AppError::BadRequest(_) => "invalid_request_error",
        AppError::RateLimited(_) | AppError::QuotaExceeded | AppError::ConcurrencyExceeded => "rate_limit_error",
        AppError::Upstream(_)
        | AppError::UpstreamTimeout
        | AppError::UpstreamConnect(_)
        | AppError::UpstreamProtocol(_)
        | AppError::UpstreamStatus { .. } => "api_error",
        _ => "api_error",
    }
}
