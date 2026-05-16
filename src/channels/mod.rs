//! 渠道路由与 trait 定义。Copilot / Anthropic 两条渠道**禁止互相 use**，
//! 共享能力一律放 `crate::shared`。

pub mod anthropic;
pub mod copilot;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChannelKind {
    Copilot,
    Anthropic,
}

/// 上游 key 熔断状态快照。两条渠道返回同一形状，admin/ws 不再做 wire 类型转换。
#[derive(Clone, Debug, Serialize)]
pub struct BreakerSnapshot {
    pub id: i64,
    pub channel_kind: ChannelKind,
    pub count: u32,
    pub disabled: bool,
    pub first_at_ms_ago: u128,
    pub last_at_ms_ago: u128,
}

impl ChannelKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            ChannelKind::Copilot => "copilot",
            ChannelKind::Anthropic => "anthropic",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "copilot" => Some(ChannelKind::Copilot),
            "anthropic" => Some(ChannelKind::Anthropic),
            _ => None,
        }
    }
}

impl std::fmt::Display for ChannelKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// sqlx::Type 让该枚举可作为 TEXT 列绑定与读取
impl sqlx::Type<sqlx::Postgres> for ChannelKind {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <&str as sqlx::Type<sqlx::Postgres>>::type_info()
    }
}

impl<'q> sqlx::Encode<'q, sqlx::Postgres> for ChannelKind {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, Box<dyn std::error::Error + Send + Sync>> {
        <&str as sqlx::Encode<sqlx::Postgres>>::encode_by_ref(&self.as_str(), buf)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for ChannelKind {
    fn decode(
        value: sqlx::postgres::PgValueRef<'r>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let s = <&str as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        ChannelKind::parse(s).ok_or_else(|| format!("unknown channel kind: {s}").into())
    }
}

/// 按上游 key 前缀推断渠道。仅在 `POST/PATCH /admin/keys` 入库时调用，
/// 热路径直接读 DB 的 channel_kind 列，不再 prefix 解析。
pub fn route_by_upstream_key(upstream_key: &str) -> ChannelKind {
    let s = upstream_key.trim();
    if s.starts_with("anthropic:") || s.starts_with("sk-ant-") {
        ChannelKind::Anthropic
    } else {
        ChannelKind::Copilot
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_anthropic_prefix() {
        assert_eq!(route_by_upstream_key("anthropic:sk-ant-foo"), ChannelKind::Anthropic);
        assert_eq!(route_by_upstream_key("sk-ant-bar"), ChannelKind::Anthropic);
    }

    #[test]
    fn route_copilot_default() {
        assert_eq!(route_by_upstream_key("enterprise:ghp_xxx"), ChannelKind::Copilot);
        assert_eq!(route_by_upstream_key("individual:ghu_yyy"), ChannelKind::Copilot);
        assert_eq!(route_by_upstream_key("ghp_raw"), ChannelKind::Copilot);
        assert_eq!(route_by_upstream_key("*"), ChannelKind::Copilot);
    }

    #[test]
    fn parse_roundtrip() {
        assert_eq!(ChannelKind::parse("copilot"), Some(ChannelKind::Copilot));
        assert_eq!(ChannelKind::parse("anthropic"), Some(ChannelKind::Anthropic));
        assert_eq!(ChannelKind::parse("xyz"), None);
    }
}
