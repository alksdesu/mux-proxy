//! /admin/pricing：把 billing::pricing 的费率表序列化给 dashboard 用，
//! 消除前端硬编码费率漂移。

use crate::billing::pricing::{
    ANTHROPIC_HAIKU, ANTHROPIC_OPUS, ANTHROPIC_SONNET, COPILOT_HAIKU, COPILOT_OPUS,
    COPILOT_OPUS_FAST, COPILOT_SONNET, PriceRate,
};
use axum::Json;
use axum::response::IntoResponse;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct PricingPayload {
    pub copilot: ChannelPricing,
    pub anthropic: ChannelPricing,
}

#[derive(Debug, Serialize)]
pub struct ChannelPricing {
    pub opus: PriceView,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub opus_fast: Option<PriceView>,
    pub sonnet: PriceView,
    pub haiku: PriceView,
}

#[derive(Debug, Serialize)]
pub struct PriceView {
    pub input: f64,
    pub output: f64,
    pub cache_write: f64,
    pub cache_read: f64,
}

impl From<PriceRate> for PriceView {
    fn from(r: PriceRate) -> Self {
        Self {
            input: r.input,
            output: r.output,
            cache_write: r.cache_write,
            cache_read: r.cache_read,
        }
    }
}

pub async fn handler() -> impl IntoResponse {
    Json(PricingPayload {
        copilot: ChannelPricing {
            opus: COPILOT_OPUS.into(),
            opus_fast: Some(COPILOT_OPUS_FAST.into()),
            sonnet: COPILOT_SONNET.into(),
            haiku: COPILOT_HAIKU.into(),
        },
        anthropic: ChannelPricing {
            opus: ANTHROPIC_OPUS.into(),
            opus_fast: None,
            sonnet: ANTHROPIC_SONNET.into(),
            haiku: ANTHROPIC_HAIKU.into(),
        },
    })
}
