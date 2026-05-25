-- @order_sensitive=true
-- Test Point: Iceberg v3 merge-on-read MERGE INTO covers MATCHED UPDATE +
--             NOT MATCHED INSERT atomically and preserves row lineage.
-- Method: load a v3 row-lineage table whose update mode is merge-on-read
--         with two rows. MERGE in a source that updates id=2 ("b" -> "bb")
--         and inserts id=3. Verify the updated value is visible exactly
--         once (DV deletes the old row, the rewritten row appears via the
--         added data file), the new row is appended, and `_row_id` values
--         remain unique.
-- Scope:  standalone Iceberg DDL/DML, MERGE INTO, MOR UPDATE snapshot
--         (Operation::Delete with NovaRocks update marker) + FastAppend
--         INSERT snapshot.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_v3_merge_mor FORCE;
DROP TABLE IF EXISTS ${case_db}.s_v3_merge_mor FORCE;
CREATE TABLE ${case_db}.t_v3_merge_mor (
  id BIGINT,
  v STRING
)
TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true",
  "novarocks.update.mode" = "merge-on-read"
);
INSERT INTO ${case_db}.t_v3_merge_mor VALUES
  (1, 'a'),
  (2, 'b');
CREATE TABLE ${case_db}.s_v3_merge_mor (
  id BIGINT,
  v STRING
)
TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ${case_db}.s_v3_merge_mor VALUES
  (2, 'bb'),
  (3, 'c');

-- query 2
SELECT id, v
FROM ${case_db}.t_v3_merge_mor
ORDER BY id;

-- query 3
-- @skip_result_check=true
MERGE INTO ${case_db}.t_v3_merge_mor AS t
USING ${case_db}.s_v3_merge_mor AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET v = s.v
WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v);

-- query 4
SELECT id, v
FROM ${case_db}.t_v3_merge_mor
ORDER BY id;

-- query 5
SELECT COUNT(DISTINCT _row_id) AS distinct_row_ids, COUNT(*) AS total_rows
FROM ${case_db}.t_v3_merge_mor;

-- query 6
-- @skip_result_check=true
DROP TABLE ${case_db}.t_v3_merge_mor FORCE;
DROP TABLE ${case_db}.s_v3_merge_mor FORCE;
