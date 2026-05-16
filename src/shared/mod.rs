//! 跨渠道共享能力。渠道实现禁止互相 use，公共部分都走这里。

pub mod breaker;
pub mod generic_errors;
pub mod ids;
pub mod json;
pub mod leak_re;
pub mod line_codec;
pub mod sse_event;
