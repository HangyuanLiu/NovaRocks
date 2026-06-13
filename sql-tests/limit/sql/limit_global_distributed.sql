-- @tags=limit,distributed
-- Test Objective:
-- Validate that LIMIT without ORDER BY is still a global LIMIT in distributed
-- execution. A LIMIT 1 marker must produce one row for the query, not one row
-- per BE fragment instance.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_limit_global;

-- query 2
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_limit_global (
  k1 INT NOT NULL
)
TBLPROPERTIES ("format-version" = "3");

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.t_limit_global VALUES
  (1), (2), (3);

-- query 4
-- @skip_result_check=true
INSERT INTO ${case_db}.t_limit_global VALUES
  (4), (5), (6);

-- query 5
-- @skip_result_check=true
INSERT INTO ${case_db}.t_limit_global VALUES
  (7), (8), (9);

-- query 6
SELECT COUNT(*) FROM (
  SELECT 1 AS marker FROM ${case_db}.t_limit_global LIMIT 1
) x;

-- query 7
SELECT COUNT(*) FROM (
  SELECT 42 AS host_key
) h
LEFT JOIN (
  SELECT 1 AS marker FROM ${case_db}.t_limit_global LIMIT 1
) m ON true;
