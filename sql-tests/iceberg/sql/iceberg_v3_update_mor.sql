-- @order_sensitive=true
-- Test Point: Iceberg v3 merge-on-read UPDATE preserves _row_id
-- Method: insert two rows into a v3 row-lineage table whose update mode
-- is merge-on-read, UPDATE one row, and verify the updated value is
-- visible exactly once (DV deletes the old row, the new data file
-- contributes the rewritten row) and the row's `_row_id` survives.
-- Scope: standalone Iceberg table DDL, INSERT INTO, UPDATE, SELECT, v3
--        row-lineage merge-on-read update via Puffin DV + added data
--        file.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_v3_update_mor FORCE;
CREATE TABLE ${case_db}.t_v3_update_mor (
  id BIGINT,
  v STRING
)
TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true",
  "novarocks.update.mode" = "merge-on-read"
);
INSERT INTO ${case_db}.t_v3_update_mor VALUES
  (1, 'a'),
  (2, 'b');

-- query 2
SELECT id, v
FROM ${case_db}.t_v3_update_mor
ORDER BY id;

-- query 3
-- @skip_result_check=true
UPDATE ${case_db}.t_v3_update_mor AS t SET v = 'bb' WHERE t.id = 2;

-- query 4
SELECT id, v
FROM ${case_db}.t_v3_update_mor
ORDER BY id;

-- query 5
SELECT COUNT(DISTINCT _row_id) AS distinct_row_ids, COUNT(*) AS total_rows
FROM ${case_db}.t_v3_update_mor;

-- query 6
-- @skip_result_check=true
DROP TABLE ${case_db}.t_v3_update_mor FORCE;
