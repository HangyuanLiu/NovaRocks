-- @order_sensitive=true
-- Test Point: Iceberg v3 row-lineage metadata after schema evolution
-- Method: create a v3 row-lineage table, insert rows, rename and widen a column, insert another row, and query metadata columns
-- Scope: ordinary Iceberg SELECT over v3 row-lineage tables

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.read_sem_rl FORCE;
CREATE TABLE ${case_db}.read_sem_rl (
  id BIGINT NOT NULL,
  amount FLOAT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ${case_db}.read_sem_rl VALUES
  (1, 10.5),
  (2, 20.5);
ALTER TABLE ${case_db}.read_sem_rl RENAME COLUMN amount TO total_amount;
ALTER TABLE ${case_db}.read_sem_rl MODIFY COLUMN total_amount DOUBLE;
INSERT INTO ${case_db}.read_sem_rl VALUES (3, 30.5);

-- query 2
SELECT id,
       total_amount,
       _row_id IS NOT NULL AS has_row_id,
       _last_updated_sequence_number IS NOT NULL AS has_seq
FROM ${case_db}.read_sem_rl
ORDER BY id;

-- query 3
-- @skip_result_check=true
DROP TABLE ${case_db}.read_sem_rl FORCE;
