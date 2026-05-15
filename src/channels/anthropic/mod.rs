//! Anthropic 官 API 渠道：保留上游指纹，对外像直连 api.anthropic.com。
//! 字节级 model splice 不解析 JSON 是为了保住 thinking 块的 HMAC 签名。

pub mod billing_hook;
pub mod gzip_passthrough;
pub mod handler;
pub mod header_case;
pub mod key_pool;
pub mod model_restore;
pub mod model_splice;
pub mod request_strip;
pub mod sse_tee;
pub mod upstream_client;
pub mod upstream_key;

pub use handler::{handle, HandlerContext, ProxyRequest};
