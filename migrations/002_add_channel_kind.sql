-- 给旧库增量补 channel_kind + cost_usd + upstream_keys.note，幂等可重跑。

ALTER TABLE api_keys      ADD COLUMN IF NOT EXISTS channel_kind TEXT NOT NULL DEFAULT 'copilot';
ALTER TABLE usage_logs    ADD COLUMN IF NOT EXISTS channel_kind TEXT NOT NULL DEFAULT 'copilot';
ALTER TABLE error_logs    ADD COLUMN IF NOT EXISTS channel_kind TEXT NOT NULL DEFAULT 'copilot';
ALTER TABLE upstream_keys ADD COLUMN IF NOT EXISTS channel_kind TEXT NOT NULL DEFAULT 'copilot';

ALTER TABLE usage_logs    ADD COLUMN IF NOT EXISTS cost_usd DOUBLE PRECISION NOT NULL DEFAULT 0;
ALTER TABLE upstream_keys ADD COLUMN IF NOT EXISTS note TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_api_keys_channel        ON api_keys(channel_kind);
CREATE INDEX IF NOT EXISTS idx_usage_channel_key       ON usage_logs(channel_kind, key_name);
CREATE INDEX IF NOT EXISTS idx_usage_channel_key_id    ON usage_logs(channel_kind, key_name, id DESC);
CREATE INDEX IF NOT EXISTS idx_error_channel_key       ON error_logs(channel_kind, key_name);
CREATE INDEX IF NOT EXISTS idx_error_channel_key_id    ON error_logs(channel_kind, key_name, id DESC);
CREATE INDEX IF NOT EXISTS idx_upstream_channel        ON upstream_keys(channel_kind);

UPDATE api_keys      SET channel_kind = 'copilot' WHERE channel_kind IS NULL OR channel_kind = '';
UPDATE usage_logs    SET channel_kind = 'copilot' WHERE channel_kind IS NULL OR channel_kind = '';
UPDATE error_logs    SET channel_kind = 'copilot' WHERE channel_kind IS NULL OR channel_kind = '';
UPDATE upstream_keys SET channel_kind = 'copilot' WHERE channel_kind IS NULL OR channel_kind = '';
