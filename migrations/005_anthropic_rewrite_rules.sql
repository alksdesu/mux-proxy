-- Anthropic 渠道字节级 model 改写规则。admin API 改完写本表 + bump 内存 ArcSwap，
-- 不重启 service 即时生效。enabled=0 软删保历史，由内存层过滤。

CREATE TABLE IF NOT EXISTS anthropic_rewrite_rules (
  id BIGSERIAL PRIMARY KEY,
  prefix TEXT NOT NULL,
  target TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_anthropic_rewrite_rules_enabled
  ON anthropic_rewrite_rules(enabled);
