//! Anthropic 官 API 渠道：保留上游指纹，对外像直连 api.anthropic.com。
//! 字节级 model splice 不解析 JSON 是为了保住 thinking 块的 HMAC 签名。
