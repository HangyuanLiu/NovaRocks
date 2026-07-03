-- @sequential=true

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.resilience_series (
  id BIGINT
)
TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.resilience_series
SELECT generate_series FROM TABLE(generate_series(1, 1000000));

-- query 2
-- @kill_be_index=1
-- @expect_error=BE[1]
SELECT COUNT(*) FROM ${case_db}.resilience_series;

-- query 3
-- @heartbeat_delay_ms=3000
-- @result_contains=1000000
SELECT COUNT(*) FROM TABLE(generate_series(1, 1000000));

-- query 4
-- @kill_be_index=1
-- @restart_be_delay_ms=0
-- @heartbeat_delay_ms=3000
-- @skip_result_check=true
SELECT 1;
