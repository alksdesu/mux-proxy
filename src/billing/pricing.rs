//! 按 (渠道, 模型) 索引的价格常量 + cost 计算。
//! 调价改下面的 PriceRate 常量即可，热路径不命中此模块。

use crate::channels::ChannelKind;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PriceRate {
    pub input: f64,
    pub output: f64,
    pub cache_write: f64,
    pub cache_read: f64,
}

impl PriceRate {
    pub const fn new(input: f64, output: f64, cache_write: f64, cache_read: f64) -> Self {
        Self {
            input,
            output,
            cache_write,
            cache_read,
        }
    }
}

pub const COPILOT_OPUS: PriceRate = PriceRate::new(5.0, 25.0, 6.25, 0.50);
pub const COPILOT_OPUS_FAST: PriceRate = PriceRate::new(30.0, 150.0, 37.50, 3.00);
pub const COPILOT_SONNET: PriceRate = PriceRate::new(3.0, 15.0, 3.75, 0.30);
pub const COPILOT_HAIKU: PriceRate = PriceRate::new(1.0, 5.0, 1.25, 0.10);

pub const ANTHROPIC_OPUS: PriceRate = PriceRate::new(15.0, 75.0, 18.75, 1.50);
pub const ANTHROPIC_SONNET: PriceRate = PriceRate::new(3.0, 15.0, 3.75, 0.30);
pub const ANTHROPIC_HAIKU: PriceRate = PriceRate::new(0.80, 4.0, 1.0, 0.08);

/// Copilot 模型映射：复刻 proxy.ts getModelRate。fast 后缀在 sonnet/haiku 没有特殊价，
/// 仅 opus 区分标准/fast。
pub fn copilot_rate(model: &str) -> PriceRate {
    let m = model.to_lowercase();
    if m.contains("haiku") {
        return COPILOT_HAIKU;
    }
    if m.contains("sonnet") {
        return COPILOT_SONNET;
    }
    if m.contains("fast") {
        return COPILOT_OPUS_FAST;
    }
    COPILOT_OPUS
}

/// Anthropic 官方价。fast 后缀不存在，model 字符串里不会出现。
pub fn anthropic_rate(model: &str) -> PriceRate {
    let m = model.to_lowercase();
    if m.contains("haiku") {
        return ANTHROPIC_HAIKU;
    }
    if m.contains("sonnet") {
        return ANTHROPIC_SONNET;
    }
    ANTHROPIC_OPUS
}

pub fn rate_for(channel: ChannelKind, model: &str) -> PriceRate {
    match channel {
        ChannelKind::Copilot => copilot_rate(model),
        ChannelKind::Anthropic => anthropic_rate(model),
    }
}

#[derive(Debug, Clone)]
pub struct BillingRecord {
    pub channel: ChannelKind,
    pub model: String,
    pub key_name: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_creation_tokens: u64,
    pub cache_read_tokens: u64,
    pub request_body: String,
    pub ip: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CostBreakdown {
    pub total: f64,
    pub input_cost: f64,
    pub output_cost: f64,
    pub cache_write_cost: f64,
    pub cache_read_cost: f64,
    pub cache_saved: f64,
}

const TOKENS_PER_MILLION: f64 = 1_000_000.0;

pub fn breakdown(rate: PriceRate, rec: &BillingRecord) -> CostBreakdown {
    let input_cost = rec.input_tokens as f64 / TOKENS_PER_MILLION * rate.input;
    let output_cost = rec.output_tokens as f64 / TOKENS_PER_MILLION * rate.output;
    let cache_write_cost = rec.cache_creation_tokens as f64 / TOKENS_PER_MILLION * rate.cache_write;
    let cache_read_cost = rec.cache_read_tokens as f64 / TOKENS_PER_MILLION * rate.cache_read;
    let cache_saved =
        rec.cache_read_tokens as f64 / TOKENS_PER_MILLION * (rate.input - rate.cache_read);
    CostBreakdown {
        total: input_cost + output_cost + cache_write_cost + cache_read_cost,
        input_cost,
        output_cost,
        cache_write_cost,
        cache_read_cost,
        cache_saved,
    }
}

pub fn calc_cost(rec: &BillingRecord) -> f64 {
    breakdown(rate_for(rec.channel, &rec.model), rec).total
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec_for(model: &str, channel: ChannelKind) -> BillingRecord {
        BillingRecord {
            channel,
            model: model.into(),
            key_name: "test".into(),
            input_tokens: 1_000_000,
            output_tokens: 1_000_000,
            cache_creation_tokens: 0,
            cache_read_tokens: 0,
            request_body: String::new(),
            ip: None,
        }
    }

    #[test]
    fn copilot_opus_per_million() {
        let cost = calc_cost(&rec_for("claude-opus-4.6", ChannelKind::Copilot));
        assert!((cost - 30.0).abs() < 1e-9);
    }

    #[test]
    fn copilot_opus_fast_distinct() {
        let cost = calc_cost(&rec_for("claude-opus-4.6-fast", ChannelKind::Copilot));
        assert!((cost - 180.0).abs() < 1e-9);
    }

    #[test]
    fn copilot_sonnet() {
        let cost = calc_cost(&rec_for("claude-sonnet-4.6", ChannelKind::Copilot));
        assert!((cost - 18.0).abs() < 1e-9);
    }

    #[test]
    fn copilot_haiku() {
        let cost = calc_cost(&rec_for("claude-haiku-4.5", ChannelKind::Copilot));
        assert!((cost - 6.0).abs() < 1e-9);
    }

    #[test]
    fn anthropic_opus_higher_than_copilot() {
        let copilot = calc_cost(&rec_for("claude-opus-4.6", ChannelKind::Copilot));
        let direct = calc_cost(&rec_for("claude-opus-4.6", ChannelKind::Anthropic));
        assert!(direct > copilot);
        assert!((direct - 90.0).abs() < 1e-9);
    }

    #[test]
    fn anthropic_haiku_cheaper() {
        let cost = calc_cost(&rec_for("claude-haiku-4.5", ChannelKind::Anthropic));
        assert!((cost - 4.8).abs() < 1e-9);
    }

    #[test]
    fn cache_breakdown_components() {
        let mut rec = rec_for("claude-sonnet-4.6", ChannelKind::Copilot);
        rec.cache_creation_tokens = 1_000_000;
        rec.cache_read_tokens = 1_000_000;
        let br = breakdown(rate_for(rec.channel, &rec.model), &rec);
        assert!((br.cache_write_cost - 3.75).abs() < 1e-9);
        assert!((br.cache_read_cost - 0.30).abs() < 1e-9);
        assert!((br.cache_saved - (3.0 - 0.30)).abs() < 1e-9);
    }

    #[test]
    fn rate_for_routes_per_channel() {
        assert_eq!(rate_for(ChannelKind::Copilot, "opus"), COPILOT_OPUS);
        assert_eq!(rate_for(ChannelKind::Anthropic, "opus"), ANTHROPIC_OPUS);
    }
}
