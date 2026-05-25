-- @order_sensitive=true
-- Test Point: Iceberg v3 copy-on-write UPDATE preserves _row_id
-- Method: insert two rows into a v3 row-lineage table, UPDATE one row's
-- non-partition column, and verify the updated value is visible exactly
-- once and the row's `_row_id` survives.
-- Scope: standalone Iceberg table DDL, INSERT INTO, UPDATE, SELECT, v3
--        row-lineage copy-on-write update sidecar.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_v3_update_cow FORCE;
CREATE TABLE ${case_db}.t_v3_update_cow (
  id BIGINT,
  v STRING
)
TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ${case_db}.t_v3_update_cow VALUES
  (1, 'a'),
  (2, 'b');

-- query 2
SELECT id, v
FROM ${case_db}.t_v3_update_cow
ORDER BY id;

-- query 3
-- @skip_result_check=true
UPDATE ${case_db}.t_v3_update_cow AS t SET v = 'bb' WHERE t.id = 2;

-- query 4
SELECT id, v
FROM ${case_db}.t_v3_update_cow
ORDER BY id;

-- query 5
SELECT COUNT(DISTINCT _row_id) AS distinct_row_ids, COUNT(*) AS total_rows
FROM ${case_db}.t_v3_update_cow;

-- query 6
-- @skip_result_check=true
DROP TABLE ${case_db}.t_v3_update_cow FORCE;
