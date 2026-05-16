//! Prometheus 指标。所有 metric 走单一 Registry，``/metrics`` 端点导出文本格式。
//! 命名遵循 `prometheus naming best practices`：单位走后缀、_total 用于累计计数。

use crate::channels::ChannelKind;
use once_cell::sync::Lazy;
use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts,
    Registry, TextEncoder,
};

pub struct Metrics {
    pub registry: Registry,
    pub http_requests_total: IntCounterVec,
    pub http_request_duration_seconds: HistogramVec,
    pub upstream_429_total: IntCounterVec,
    pub breaker_open: IntGaugeVec,
    pub quota_rejections_total: IntCounter,
    pub concurrency_rejections_total: IntCounter,
    pub rate_limit_rejections_total: IntCounter,
    pub spend_usd_total: IntGauge,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();

        let http_requests_total = IntCounterVec::new(
            Opts::new("mux_http_requests_total", "Total HTTP requests proxied"),
            &["channel", "status"],
        )
        .expect("counter");
        let http_request_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "mux_http_request_duration_seconds",
                "Wall-clock latency from accept to response",
            )
            .buckets(vec![0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0]),
            &["channel"],
        )
        .expect("histogram");
        let upstream_429_total = IntCounterVec::new(
            Opts::new("mux_upstream_429_total", "Upstream HTTP 429 responses"),
            &["channel"],
        )
        .expect("counter");
        let breaker_open = IntGaugeVec::new(
            Opts::new("mux_breaker_open", "Open breaker count per channel"),
            &["channel"],
        )
        .expect("gauge");
        let quota_rejections_total = IntCounter::new(
            "mux_quota_rejections_total",
            "Requests rejected for exceeding spend quota",
        )
        .expect("counter");
        let concurrency_rejections_total = IntCounter::new(
            "mux_concurrency_rejections_total",
            "Requests rejected for exceeding concurrency cap",
        )
        .expect("counter");
        let rate_limit_rejections_total = IntCounter::new(
            "mux_rate_limit_rejections_total",
            "Requests rejected by per-key RPM rate limit",
        )
        .expect("counter");
        let spend_usd_total = IntGauge::new(
            "mux_spend_usd_milli_total",
            "Cumulative spend across all keys in milli-USD (1/1000 USD, integer)",
        )
        .expect("gauge");

        registry.register(Box::new(http_requests_total.clone())).expect("register");
        registry.register(Box::new(http_request_duration_seconds.clone())).expect("register");
        registry.register(Box::new(upstream_429_total.clone())).expect("register");
        registry.register(Box::new(breaker_open.clone())).expect("register");
        registry.register(Box::new(quota_rejections_total.clone())).expect("register");
        registry.register(Box::new(concurrency_rejections_total.clone())).expect("register");
        registry.register(Box::new(rate_limit_rejections_total.clone())).expect("register");
        registry.register(Box::new(spend_usd_total.clone())).expect("register");

        Self {
            registry,
            http_requests_total,
            http_request_duration_seconds,
            upstream_429_total,
            breaker_open,
            quota_rejections_total,
            concurrency_rejections_total,
            rate_limit_rejections_total,
            spend_usd_total,
        }
    }

    pub fn record_request(&self, channel: ChannelKind, status: u16, duration_seconds: f64) {
        let ch = channel.as_str();
        let status_str = bucket_status(status);
        self.http_requests_total.with_label_values(&[ch, status_str]).inc();
        self.http_request_duration_seconds.with_label_values(&[ch]).observe(duration_seconds);
    }

    pub fn record_upstream_429(&self, channel: ChannelKind) {
        self.upstream_429_total.with_label_values(&[channel.as_str()]).inc();
    }

    pub fn set_breaker_open(&self, channel: ChannelKind, count: i64) {
        self.breaker_open.with_label_values(&[channel.as_str()]).set(count);
    }

    pub fn set_spend_milli_usd(&self, total: i64) {
        self.spend_usd_total.set(total);
    }

    pub fn encode_text(&self) -> Result<String, prometheus::Error> {
        let mut buf = Vec::new();
        TextEncoder::new().encode(&self.registry.gather(), &mut buf)?;
        String::from_utf8(buf).map_err(|e| prometheus::Error::Msg(format!("utf8: {e}")))
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

/// 状态码归桶（2xx/3xx/4xx/5xx）减少 cardinality。
fn bucket_status(status: u16) -> &'static str {
    match status {
        100..=199 => "1xx",
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        500..=599 => "5xx",
        _ => "unknown",
    }
}

pub static GLOBAL: Lazy<Metrics> = Lazy::new(Metrics::new);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_increment() {
        let m = Metrics::new();
        m.record_request(ChannelKind::Copilot, 200, 0.1);
        m.record_request(ChannelKind::Copilot, 500, 1.5);
        m.record_upstream_429(ChannelKind::Anthropic);
        m.set_breaker_open(ChannelKind::Copilot, 3);
        let text = m.encode_text().expect("encode");
        assert!(text.contains("mux_http_requests_total"));
        assert!(text.contains("channel=\"copilot\""));
        assert!(text.contains("mux_upstream_429_total"));
        assert!(text.contains("mux_breaker_open"));
    }

    #[test]
    fn status_buckets() {
        assert_eq!(bucket_status(204), "2xx");
        assert_eq!(bucket_status(404), "4xx");
        assert_eq!(bucket_status(502), "5xx");
        assert_eq!(bucket_status(999), "unknown");
    }
}
