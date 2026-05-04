-- Test Point: Unsupported writes on evolved Iceberg partition specs fail fast
-- Method: evolve a partition spec, then verify INSERT OVERWRITE and ADD EQUALITY DELETE return clear unsupported errors
-- Scope: standalone Iceberg table DDL, unsupported write guards

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_partition_evolution_unsupported FORCE;
CREATE TABLE ${case_db}.t_partition_evolution_unsupported (
  id BIGINT,
  user_id BIGINT,
  score INT
)
PARTITION BY bucket(user_id, 4)
TBLPROPERTIES ("format-version" = "2");
INSERT INTO ${case_db}.t_partition_evolution_unsupported VALUES (1, 10, 100);
ALTER TABLE ${case_db}.t_partition_evolution_unsupported DROP PARTITION COLUMN bucket(user_id, 4);
ALTER TABLE ${case_db}.t_partition_evolution_unsupported ADD PARTITION COLUMN bucket(user_id, 8);

-- query 2
-- @expect_error=INSERT OVERWRITE on an evolved Iceberg table is not supported yet
INSERT OVERWRITE ${case_db}.t_partition_evolution_unsupported
SELECT * FROM ${case_db}.t_partition_evolution_unsupported;

-- query 3
-- @expect_error=ADD EQUALITY DELETE on an evolved Iceberg table is not supported yet
ALTER TABLE ${case_db}.t_partition_evolution_unsupported ADD EQUALITY DELETE (id) VALUES (1);

-- query 4
-- @skip_result_check=true
DROP TABLE ${case_db}.t_partition_evolution_unsupported FORCE;
