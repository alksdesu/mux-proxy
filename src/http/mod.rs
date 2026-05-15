//! HTTP 层：路由装配、中间件、错误响应转换、客户端 /v1/* 分派。

pub mod error_resp;
pub mod middleware;
pub mod router;
pub mod v1;
