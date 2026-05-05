-- @order_sensitive=true
-- Test Point: ALTER ADD COLUMN ... DEFAULT round-trips for each supported primitive type.
-- Method: CREATE a v3 table with one anchor column, INSERT one row, then sequentially ADD COLUMN for each primitive type with a DEFAULT, SELECT to confirm initial-default backfills the existing row.
-- Scope: type coverage D2 — boolean / tinyint / smallint / int / bigint / float / double / decimal / string / date / datetime.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_v3_default_types FORCE;
CREATE TABLE ${case_db}.t_v3_default_types (
  id INT
)
TBLPROPERTIES (
  "format-version" = "3"
);
INSERT INTO ${case_db}.t_v3_default_types VALUES (1);
ALTER TABLE ${case_db}.t_v3_default_types ADD COLUMN c_bool BOOLEAN DEFAULT TRUE;
ALTER TABLE ${case_db}.t_v3_default_types ADD COLUMN c_tinyint TINYINT DEFAULT 1;
ALTER TABLE ${case_db}.t_v3_default_types ADD COLUMN c_smallint SMALLINT DEFAULT 2;
ALTER TABLE ${case_db}.t_v3_default_types ADD COLUMN c_int INT DEFAULT 3;
ALTER TABLE ${case_db}.t_v3_default_types ADD COLUMN c_bigint BIGINT DEFAULT 4;
ALTER TABLE ${case_db}.t_v3_default_types ADD COLUMN c_float FLOAT DEFAULT 1.5;
ALTER TABLE ${case_db}.t_v3_default_types ADD COLUMN c_double DOUBLE DEFAULT 2.25;
ALTER TABLE ${case_db}.t_v3_default_types ADD COLUMN c_decimal DECIMAL(10,2) DEFAULT 3.14;
ALTER TABLE ${case_db}.t_v3_default_types ADD COLUMN c_string STRING DEFAULT 'hi';
ALTER TABLE ${case_db}.t_v3_default_types ADD COLUMN c_date DATE DEFAULT '1970-01-02';
ALTER TABLE ${case_db}.t_v3_default_types ADD COLUMN c_datetime DATETIME DEFAULT '1970-01-01 00:00:01';

-- query 2
SELECT id, c_bool, c_tinyint, c_smallint, c_int, c_bigint, c_float, c_double, c_decimal, c_string, c_date, c_datetime
FROM ${case_db}.t_v3_default_types
ORDER BY id;

-- query 3
-- @skip_result_check=true
DROP TABLE ${case_db}.t_v3_default_types FORCE;
