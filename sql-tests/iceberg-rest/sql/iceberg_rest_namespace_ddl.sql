-- @order_sensitive=true
-- Validate REST namespace API: CREATE/DROP DATABASE incl. IF (NOT) EXISTS
-- idempotency, plus the error path of CREATE DATABASE on an existing namespace.
-- Positive verification: write a row inside the new namespace and read it back.

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_rest_${suite_uuid0}.iceberg_rest_ns_db_${uuid0};

-- query 2
-- @skip_result_check=true
-- IF NOT EXISTS on an existing namespace must be a no-op.
CREATE DATABASE IF NOT EXISTS iceberg_rest_${suite_uuid0}.iceberg_rest_ns_db_${uuid0};

-- query 3
-- @expect_error=exists
-- Bare CREATE DATABASE on an existing namespace must fail.
CREATE DATABASE iceberg_rest_${suite_uuid0}.iceberg_rest_ns_db_${uuid0};

-- query 4
-- @skip_result_check=true
CREATE TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_ns_db_${uuid0}.t_ns_${uuid0} (id INT);

-- query 5
-- @skip_result_check=true
INSERT INTO iceberg_rest_${suite_uuid0}.iceberg_rest_ns_db_${uuid0}.t_ns_${uuid0} VALUES (1), (2);

-- query 6
-- The namespace works for table operations: read back the inserted rows.
SELECT id FROM iceberg_rest_${suite_uuid0}.iceberg_rest_ns_db_${uuid0}.t_ns_${uuid0} ORDER BY id;

-- query 7
-- @skip_result_check=true
DROP TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_ns_db_${uuid0}.t_ns_${uuid0};

-- query 8
-- @skip_result_check=true
DROP DATABASE iceberg_rest_${suite_uuid0}.iceberg_rest_ns_db_${uuid0};

-- query 9
-- @skip_result_check=true
-- IF EXISTS on already-dropped namespace must be a no-op.
DROP DATABASE IF EXISTS iceberg_rest_${suite_uuid0}.iceberg_rest_ns_db_${uuid0};
