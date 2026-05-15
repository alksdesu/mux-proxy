//! per-key 并发计数器 + RAII guard + 周期 GC。
//! 流式响应在 SSE 流结束时才释放并发位，由 ConcurrencyGuard drop 触发。

pub mod limiter;

pub use limiter::{ConcurrencyGuard, Limiter};
