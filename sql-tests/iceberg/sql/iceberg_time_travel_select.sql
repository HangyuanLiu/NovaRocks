-- @order_sensitive=true
-- Validate SELECT … FOR VERSION AS OF on iceberg branches and snapshot IDs
-- including cross-ref join (the same table at two different snapshots).

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_cat_${suite_uuid0}.iceberg_tt_db_${uuid0};
USE iceberg_cat_${suite_uuid0}.iceberg_tt_db_${uuid0};
CREATE TABLE t_tt_${uuid0} (id INT, v INT);

-- query 2
-- @skip_result_check=true
INSERT INTO t_tt_${uuid0} VALUES (1, 10), (2, 20);

-- query 3
-- @skip_result_check=true
ALTER TABLE iceberg_cat_${suite_uuid0}.iceberg_tt_db_${uuid0}.t_tt_${uuid0} CREATE BRANCH backup;

-- query 4
-- @skip_result_check=true
INSERT INTO t_tt_${uuid0} VALUES (3, 30);

-- query 5 default SELECT sees the latest main (3 rows).
SELECT id, v FROM t_tt_${uuid0} ORDER BY id;

-- query 6 FOR VERSION AS OF 'main' sees 3 rows.
SELECT id, v FROM t_tt_${uuid0} FOR VERSION AS OF 'main' ORDER BY id;

-- query 7 FOR VERSION AS OF 'backup' sees only the pre-third-row state (2 rows).
SELECT id, v FROM t_tt_${uuid0} FOR VERSION AS OF 'backup' ORDER BY id;

-- query 8 cross-ref join main (3 rows) LEFT JOIN backup (2 rows) on id.
SELECT
  m.id AS main_id, m.v AS main_v,
  b.id AS bak_id, b.v AS bak_v
  FROM t_tt_${uuid0} FOR VERSION AS OF 'main' m
  LEFT JOIN t_tt_${uuid0} FOR VERSION AS OF 'backup' b
    ON m.id = b.id
  ORDER BY m.id;

-- query 9
-- @skip_result_check=true
SET catalog default_catalog;
ALTER TABLE iceberg_cat_${suite_uuid0}.iceberg_tt_db_${uuid0}.t_tt_${uuid0} DROP BRANCH backup;
DROP TABLE iceberg_cat_${suite_uuid0}.iceberg_tt_db_${uuid0}.t_tt_${uuid0};
DROP DATABASE iceberg_cat_${suite_uuid0}.iceberg_tt_db_${uuid0};
