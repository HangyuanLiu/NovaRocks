-- @order_sensitive=true
-- Test Point: Iceberg v3 deletion-vector DELETE works across historical partition specs
-- Method: create a v3 row-lineage bucket-partitioned table, insert old-spec rows, evolve to a new bucket spec, insert new-spec rows, delete rows from both specs, and verify remaining rows
-- Scope: standalone Iceberg table DDL, INSERT INTO, SELECT, DELETE FROM, v3 row-lineage deletion vectors

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_partition_evolution_v3_delete FORCE;
CREATE TABLE ${case_db}.t_partition_evolution_v3_delete (
  id BIGINT,
  user_id BIGINT,
  score INT
)
PARTITION BY bucket(user_id, 4)
TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ${case_db}.t_partition_evolution_v3_delete VALUES
  (1, 10, 100),
  (2, 20, 200),
  (3, 30, 300);
ALTER TABLE ${case_db}.t_partition_evolution_v3_delete DROP PARTITION COLUMN bucket(user_id, 4);
ALTER TABLE ${case_db}.t_partition_evolution_v3_delete ADD PARTITION COLUMN bucket(user_id, 8);
INSERT INTO ${case_db}.t_partition_evolution_v3_delete VALUES
  (4, 40, 400),
  (5, 50, 500),
  (6, 60, 600);
DELETE FROM ${case_db}.t_partition_evolution_v3_delete WHERE id IN (2, 5);

-- query 2
SELECT COUNT(*) AS cnt
FROM ${case_db}.t_partition_evolution_v3_delete;

-- query 3
SELECT id, user_id, score
FROM ${case_db}.t_partition_evolution_v3_delete
ORDER BY id;

-- query 4
-- @skip_result_check=true
DROP TABLE ${case_db}.t_partition_evolution_v3_delete FORCE;
