//! Admin API：/admin/* + /stats + WebSocket /ws + dashboard 静态资源入口。
//! 所有列表/统计端点支持 `?channel=copilot|anthropic|all`，非法值返 400。
//! 未认证统一返 404，避免暴露管理面存在。

pub mod errors;
pub mod export;
pub mod geoip;
pub mod keys;
pub mod pricing;
pub mod query;
pub mod routes;
pub mod stats;
pub mod timeseries;
pub mod upstream;
pub mod usage;
pub mod ws;

pub use routes::build_admin_router;
