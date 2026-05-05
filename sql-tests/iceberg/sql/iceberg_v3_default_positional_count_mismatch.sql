-- @order_sensitive=true
-- Test Point: Positional INSERT INTO t VALUES (...) with fewer values than columns continues to error (write-default does NOT auto-fill positional INSERT).
-- Method: CREATE v3 table with 2 columns where the second has DEFAULT 5, INSERT positional with only one value, expect a count-mismatch error.
-- Scope: D4 — positional INSERT semantics are unchanged.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_v3_default_positional FORCE;
CREATE TABLE ${case_db}.t_v3_default_positional (
  a INT,
  b INT DEFAULT 5
)
TBLPROPERTIES (
  "format-version" = "3"
);

-- query 2
-- @expect_error=mismatch
INSERT INTO ${case_db}.t_v3_default_positional VALUES (1);

-- query 3
-- @skip_result_check=true
DROP TABLE ${case_db}.t_v3_default_positional FORCE;
