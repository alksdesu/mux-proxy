-- 每 key 模型白名单。逗号分隔精确 model 名（小写比对），空串 = 不限制。
-- 由 client_auth 后的 model_guard middleware 强制；不命中返 403 permission_error。

ALTER TABLE api_keys
  ADD COLUMN IF NOT EXISTS allowed_models TEXT NOT NULL DEFAULT '';
