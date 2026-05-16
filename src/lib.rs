//! mux-proxy — Rust 多渠道反代核心库
//!
//! 顶层模块按"共享层 vs 渠道层"组织。渠道之间禁止互相 `use`，公共能力放 `shared`。
//! 入口见 `app::run`；HTTP 配线见 `http::router::build`；渠道分派见 `channels`。

pub mod admin;
pub mod app;
pub mod auth;
pub mod billing;
pub mod channels;
pub mod concurrency;
pub mod config;
pub mod dashboard;
pub mod db;
pub mod error;
pub mod http;
pub mod metrics;
pub mod rate_limit;
pub mod shared;
pub mod util;

pub use error::AppError;
