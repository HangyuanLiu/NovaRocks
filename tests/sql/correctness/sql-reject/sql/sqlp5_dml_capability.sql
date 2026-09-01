-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information regarding copyright
-- ownership.  The ASF licenses this file to you under the Apache License,
-- Version 2.0.

-- SQLP-5 rejects are admitted by typed parser/AST owners and retain stable
-- frontend error descriptors.  These statements must never fall back to a
-- raw sqlparser converter.

-- @expect_sql_code=sql.parse.unexpected_token
-- @expect_sql_phase=Parse
DELETE FROM;

-- @expect_sql_code=sql.validate.invalid_structure
-- @expect_sql_phase=Validate
CREATE TABLE sqlp5_duplicate_column (id INT, id BIGINT);

-- @expect_sql_code=sql.admit.delete_requires_where
-- @expect_sql_phase=Admit
DELETE FROM sqlp5_reject_target;

-- @expect_sql_code=sql.admit.update_unsupported_form
-- @expect_sql_phase=Admit
UPDATE sqlp5_reject_target SET target.id = 1;

-- @expect_sql_code=sql.admit.merge_unsupported_form
-- @expect_sql_phase=Admit
MERGE INTO sqlp5_reject_target target USING sqlp5_reject_source source ON target.id = source.id
WHEN MATCHED THEN DELETE
WHEN MATCHED THEN DELETE;

-- @expect_sql_code=sql.admit.create_table_unsupported_form
-- @expect_sql_phase=Admit
CREATE TEMPORARY TABLE sqlp5_reject_target (id INT);

-- @expect_sql_code=sql.admit.create_table_unsupported_form
-- @expect_sql_phase=Admit
CREATE TABLE sqlp5_reject_target (id INT) ENGINE = olap;

-- @expect_sql_code=sql.admit.create_table_unsupported_form
-- @expect_sql_phase=Admit
CREATE TABLE sqlp5_reject_target (id INT) ORDER BY (id);
