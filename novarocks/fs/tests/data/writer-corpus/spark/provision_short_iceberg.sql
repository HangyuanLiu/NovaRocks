CREATE NAMESPACE ice_rest.uea4a2_short_20260923;

CREATE TABLE ice_rest.uea4a2_short_20260923.short_v1 (
  id BIGINT,
  value BIGINT
) USING iceberg
TBLPROPERTIES (
  'format-version' = '2',
  'write.format.default' = 'parquet',
  'write.distribution-mode' = 'none'
);

INSERT INTO ice_rest.uea4a2_short_20260923.short_v1
SELECT CAST(id AS BIGINT), CAST(id * 3 AS BIGINT)
FROM range(0, 4096);
