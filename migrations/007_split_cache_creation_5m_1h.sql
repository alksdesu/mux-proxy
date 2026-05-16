-- Anthropic 区分 5m / 1h cache write 计费（1.25× vs 2.0× 输入价）。
-- 旧列 cache_creation_tokens 保留作总和，新列按 ttl 维度分项，避免重算历史 cost_usd。
-- Copilot 渠道不暴露 1h 概念，写入侧把 total 全填进 5m 列、1h=0，等价旧行为。

ALTER TABLE usage_logs
  ADD COLUMN IF NOT EXISTS cache_creation_5m_tokens BIGINT NOT NULL DEFAULT 0;

ALTER TABLE usage_logs
  ADD COLUMN IF NOT EXISTS cache_creation_1h_tokens BIGINT NOT NULL DEFAULT 0;
