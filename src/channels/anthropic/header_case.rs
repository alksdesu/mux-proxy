//! 响应方向的 header 大小写复刻表。Cloudflare 边缘把这些 header 以混合大小写发出，
//! 小写化会变成一个一字节指纹。

/// (lowercase, canonical) — 查表只比小写键，写出时用 canonical 字节。
pub const RESPONSE_HEADER_CASE: &[(&str, &str)] = &[
    ("date", "Date"),
    ("content-type", "Content-Type"),
    ("transfer-encoding", "Transfer-Encoding"),
    ("connection", "Connection"),
    ("content-encoding", "Content-Encoding"),
    ("content-security-policy", "Content-Security-Policy"),
    ("x-robots-tag", "X-Robots-Tag"),
    ("cf-ray", "CF-RAY"),
    ("server", "Server"),
];

/// 查 ``lower`` 对应的 canonical 形式。未命中返回原值。
pub fn canonicalize(lower: &str) -> &str {
    for (k, v) in RESPONSE_HEADER_CASE {
        if *k == lower {
            return v;
        }
    }
    lower
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_known() {
        assert_eq!(canonicalize("cf-ray"), "CF-RAY");
        assert_eq!(canonicalize("content-type"), "Content-Type");
        assert_eq!(canonicalize("x-robots-tag"), "X-Robots-Tag");
        assert_eq!(canonicalize("server"), "Server");
    }

    #[test]
    fn returns_input_when_unknown() {
        assert_eq!(canonicalize("x-custom"), "x-custom");
        assert_eq!(canonicalize(""), "");
    }
}
