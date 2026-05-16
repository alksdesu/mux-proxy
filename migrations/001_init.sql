-- 全量建表（新部署）。复刻旧 schema.sql，token 列升 BIGINT，
-- 每张表直接带 channel_kind，usage_logs 多一列 cost_usd 预算好直接写。

CREATE TABLE IF NOT EXISTS usage_logs (
  id BIGSERIAL PRIMARY KEY,
  time TEXT NOT NULL,
  model TEXT NOT NULL,
  input_tokens BIGINT NOT NULL DEFAULT 0,
  output_tokens BIGINT NOT NULL DEFAULT 0,
  cache_creation_tokens BIGINT NOT NULL DEFAULT 0,
  cache_read_tokens BIGINT NOT NULL DEFAULT 0,
  key_name TEXT NOT NULL DEFAULT 'owner',
  request_body TEXT NOT NULL DEFAULT '',
  ip TEXT NOT NULL DEFAULT '',
  cost_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
  channel_kind TEXT NOT NULL DEFAULT 'copilot'
);

CREATE TABLE IF NOT EXISTS api_keys (
  id BIGSERIAL PRIMARY KEY,
  key TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  upstream_key TEXT NOT NULL DEFAULT '*',
  quota DOUBLE PRECISION NOT NULL DEFAULT -1,
  allow_fast INTEGER NOT NULL DEFAULT 1,
  max_concurrency BIGINT NOT NULL DEFAULT -1,
  rpm_limit BIGINT NOT NULL DEFAULT -1,
  created_at TEXT NOT NULL,
  channel_kind TEXT NOT NULL DEFAULT 'copilot'
);

CREATE TABLE IF NOT EXISTS error_logs (
  id BIGSERIAL PRIMARY KEY,
  time TEXT NOT NULL,
  key_name TEXT NOT NULL DEFAULT '',
  status INTEGER NOT NULL DEFAULT 0,
  path TEXT NOT NULL DEFAULT '',
  model TEXT NOT NULL DEFAULT '',
  request_body TEXT NOT NULL DEFAULT '',
  response_body TEXT NOT NULL DEFAULT '',
  ip TEXT NOT NULL DEFAULT '',
  channel_kind TEXT NOT NULL DEFAULT 'copilot'
);

CREATE TABLE IF NOT EXISTS upstream_keys (
  id BIGSERIAL PRIMARY KEY,
  key TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL DEFAULT '',
  enabled INTEGER NOT NULL DEFAULT 1,
  note TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  channel_kind TEXT NOT NULL DEFAULT 'copilot'
);

CREATE INDEX IF NOT EXISTS idx_usage_key             ON usage_logs(key_name);
CREATE INDEX IF NOT EXISTS idx_usage_key_model       ON usage_logs(key_name, model);
CREATE INDEX IF NOT EXISTS idx_usage_ip              ON usage_logs(ip);
CREATE INDEX IF NOT EXISTS idx_usage_time            ON usage_logs(time);
CREATE INDEX IF NOT EXISTS idx_error_key             ON error_logs(key_name);
CREATE INDEX IF NOT EXISTS idx_error_id_desc         ON error_logs(id DESC);

CREATE INDEX IF NOT EXISTS idx_api_keys_channel        ON api_keys(channel_kind);
CREATE INDEX IF NOT EXISTS idx_usage_channel_key       ON usage_logs(channel_kind, key_name);
CREATE INDEX IF NOT EXISTS idx_usage_channel_key_id    ON usage_logs(channel_kind, key_name, id DESC);
CREATE INDEX IF NOT EXISTS idx_error_channel_key       ON error_logs(channel_kind, key_name);
CREATE INDEX IF NOT EXISTS idx_error_channel_key_id    ON error_logs(channel_kind, key_name, id DESC);
CREATE INDEX IF NOT EXISTS idx_upstream_channel        ON upstream_keys(channel_kind);
