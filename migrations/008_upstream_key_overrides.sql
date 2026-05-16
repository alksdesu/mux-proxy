-- per-upstream-key 改写规则与模型白名单。
-- rewrite_rules: NULL 落全局 anthropic_rewrite_rules 兜底；非 NULL 完整覆盖全局规则集。
-- allowed_models: NULL 表示该上游 key 无 model 限制；非 NULL 数组表示精确白名单。
-- 两列都用 JSONB（PG 原生 schema-free，校验在应用层），不建索引（每次 pick 全表扫描，
-- upstream_keys 行数量级 < 100，O(n) 可接受）。

ALTER TABLE upstream_keys
  ADD COLUMN IF NOT EXISTS rewrite_rules JSONB;

ALTER TABLE upstream_keys
  ADD COLUMN IF NOT EXISTS allowed_models JSONB;
