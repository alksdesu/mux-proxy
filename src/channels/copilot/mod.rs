//! GitHub Copilot 渠道：消除上游指纹，对外伪装成 Anthropic API。
//! 与 `crate::channels::anthropic` 完全隔离，公共能力一律走 `crate::shared`。

pub mod breaker;
pub mod direct;
pub mod handler;
pub mod headers;
pub mod key_pool;
pub mod model_map;
pub mod ratelimit_headers;
pub mod request_xform;
pub mod response_xform;
pub mod session_token;
pub mod sse;
pub mod upstream_key;
pub mod web_search;

pub use breaker::Breaker;
pub use direct::DirectFlags;
pub use handler::{ChannelContext, CopilotHandler, HandlerOutcome, resolve_upstream_path};
pub use key_pool::UpstreamPool;
pub use session_token::SessionTokenCache;
pub use upstream_key::{CopilotPrefix, ParsedUpstreamKey, UpstreamConfig, parse_raw_key, resolve_upstream_config};
