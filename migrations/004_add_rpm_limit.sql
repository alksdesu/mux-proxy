-- 每 key 的 RPM (每分钟请求数) 限制。-1 不限、0 全禁、N 严格上限。
-- 由 rate_limit 模块在 client_auth 后的 middleware 链中强制。
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS rpm_limit BIGINT NOT NULL DEFAULT -1;
