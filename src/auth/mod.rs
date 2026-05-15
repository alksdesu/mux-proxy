//! 客户端 sk-xxx key 鉴权层：LRU + TTL 缓存 + singleflight。
//! 渠道分派依赖 KeyCacheEntry.channel_kind 直接读 DB 列，热路径不再解析 prefix。

pub mod key_cache;
pub mod singleflight;

pub use key_cache::{KEY_CACHE_MAX, KEY_CACHE_TTL, KeyCache, KeyCacheEntry};
pub use singleflight::SingleFlight;
