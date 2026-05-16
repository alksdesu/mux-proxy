-- 把 SERIAL (i32) 主键升到 BIGSERIAL (i64)，避免 21 亿上限。
-- 同时给关联 sequence 切 AS BIGINT，避免 nextval 仍按 i32 卡上限。
-- 幂等：sequence 类型若已是 bigint，ALTER 无副作用。

ALTER TABLE usage_logs    ALTER COLUMN id TYPE BIGINT;
ALTER TABLE api_keys      ALTER COLUMN id TYPE BIGINT;
ALTER TABLE error_logs    ALTER COLUMN id TYPE BIGINT;
ALTER TABLE upstream_keys ALTER COLUMN id TYPE BIGINT;

ALTER SEQUENCE usage_logs_id_seq    AS BIGINT;
ALTER SEQUENCE api_keys_id_seq      AS BIGINT;
ALTER SEQUENCE error_logs_id_seq    AS BIGINT;
ALTER SEQUENCE upstream_keys_id_seq AS BIGINT;
