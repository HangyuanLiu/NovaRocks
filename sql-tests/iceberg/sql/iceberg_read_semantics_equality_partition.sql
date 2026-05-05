-- @order_sensitive=true
-- Test Point: Iceberg equality delete visibility across partition evolution
-- Method: add an equality delete on an unpartitioned table, evolve to a partitioned spec, insert a new row with the deleted key, and verify sequence-aware visibility
-- Scope: ordinary Iceberg SELECT over current snapshot live rows

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.read_sem_eq_part FORCE;
CREATE TABLE ${case_db}.read_sem_eq_part (
  id BIGINT NOT NULL,
  city STRING,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "2"
);
INSERT INTO ${case_db}.read_sem_eq_part VALUES
  (1, 'A', 10),
  (2, 'B', 20);
ALTER TABLE ${case_db}.read_sem_eq_part
ADD EQUALITY DELETE (id) VALUES (2);

-- query 2
SELECT id, city, amount
FROM ${case_db}.read_sem_eq_part
ORDER BY id;

-- query 3
-- @skip_result_check=true
ALTER TABLE ${case_db}.read_sem_eq_part ADD PARTITION COLUMN city;
INSERT INTO ${case_db}.read_sem_eq_part VALUES
  (2, 'B', 25),
  (3, 'A', 30);

-- query 4
SELECT id, city, amount
FROM ${case_db}.read_sem_eq_part
ORDER BY id, amount;

-- query 5
-- @skip_result_check=true
DROP TABLE ${case_db}.read_sem_eq_part FORCE;
