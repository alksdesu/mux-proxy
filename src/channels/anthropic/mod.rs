//! Anthropic 官 API 渠道：字节级 model splice 不解析 JSON 是为了保住 thinking 块的 HMAC 签名。
//! handler 返 ``hyper::Response<BoxBody>`` 并整体 forward 上游 extensions；
//! wire header case 真正生效依赖共享 server 层启用 ``preserve_header_case(true)``。

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

pub use handler::{BoxError, HandlerContext, ProxyBody, ProxyRequest, ProxyResponse, handle};
