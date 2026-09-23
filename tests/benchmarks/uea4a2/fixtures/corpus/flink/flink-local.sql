SET 'execution.runtime-mode' = 'batch';
SET 'parallelism.default' = '1';
SET 'table.dml-sync' = 'true';
SET 'table.local-time-zone' = 'UTC';
CREATE TABLE flink_fixture (
  id BIGINT, category INT, label STRING, amount DECIMAL(12,2),
  event_time TIMESTAMP(3), nested ARRAY<INT>
) WITH (
  'connector' = 'filesystem',
  'path' = 'file:///tmp/uea4a2-flink-local-corpus-20260923-r7/data',
  'format' = 'parquet',
  'parquet.utc-timezone' = 'true',
  'parquet.compression' = 'SNAPPY'
);
INSERT INTO flink_fixture
SELECT CAST(x.id AS BIGINT), CAST(MOD(x.id, 17) AS INT),
  CASE WHEN MOD(x.id, 17) = 0 THEN CAST(NULL AS STRING)
       ELSE CONCAT('label-', CAST(MOD(x.id, 11) AS STRING)) END,
  CAST(x.id * 0.01 AS DECIMAL(12,2)),
  TIMESTAMP '2024-01-01 00:00:00',
  ARRAY[CAST(MOD(x.id, 5) AS INT), CAST(MOD(x.id, 7) AS INT)]
FROM (
  SELECT a.n * 64 + b.n AS id
  FROM (VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12), (13), (14), (15), (16), (17), (18), (19), (20), (21), (22), (23), (24), (25), (26), (27), (28), (29), (30), (31), (32), (33), (34), (35), (36), (37), (38), (39), (40), (41), (42), (43), (44), (45), (46), (47), (48), (49), (50), (51), (52), (53), (54), (55), (56), (57), (58), (59), (60), (61), (62), (63)) AS a(n)
  CROSS JOIN (VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12), (13), (14), (15), (16), (17), (18), (19), (20), (21), (22), (23), (24), (25), (26), (27), (28), (29), (30), (31), (32), (33), (34), (35), (36), (37), (38), (39), (40), (41), (42), (43), (44), (45), (46), (47), (48), (49), (50), (51), (52), (53), (54), (55), (56), (57), (58), (59), (60), (61), (62), (63)) AS b(n)
) AS x;
