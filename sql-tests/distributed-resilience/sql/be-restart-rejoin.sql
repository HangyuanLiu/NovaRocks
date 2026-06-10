-- @sequential=true

-- query 1
-- @skip_result_check=true
CREATE DATABASE IF NOT EXISTS ${case_db};
USE ${case_db};

-- query 2
-- @kill_be_index=1
-- @restart_be_delay_ms=1500
-- @skip_result_check=true
SELECT 1;

-- query 3
-- @heartbeat_delay_ms=20000
-- @retry_count=5
-- @retry_interval_ms=2000
-- @result_contains=1000000
SELECT COUNT(*) FROM TABLE(generate_series(1, 1000000));
