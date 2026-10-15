-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0.
--
-- @sequential=true
-- @tags=iceberg,ddl,reserved-properties

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_ddl_cat_${suite_uuid0}.doc_props_${uuid0};
USE iceberg_ddl_cat_${suite_uuid0}.doc_props_${uuid0};
CREATE TABLE safe_table (id INT) TBLPROPERTIES ('ordinary.application.key' = 'unchanged');
INSERT INTO safe_table VALUES (1);

-- query 2
-- @expect_error=novarocks.* namespace is reserved
CREATE TABLE rejected_0 (id INT) TBLPROPERTIES ('novarocks.doc.v2.root.test' = 'forbidden');

-- query 3
-- Success with the exact same name proves the rejected CREATE was invisible.
-- @skip_result_check=true
CREATE TABLE rejected_0 (id INT);
DROP TABLE rejected_0;

-- query 4
-- @expect_error=novarocks.* namespace is reserved
ALTER TABLE safe_table SET TBLPROPERTIES ('novarocks.doc.v2.root.test' = 'forbidden');

-- query 5
-- @expect_error=novarocks.* namespace is reserved
CREATE TABLE rejected_1 (id INT) TBLPROPERTIES ('novarocks.doc.future.version' = 'forbidden');

-- query 6
-- Success with the exact same name proves the rejected CREATE was invisible.
-- @skip_result_check=true
CREATE TABLE rejected_1 (id INT);
DROP TABLE rejected_1;

-- query 7
-- @expect_error=novarocks.* namespace is reserved
ALTER TABLE safe_table SET TBLPROPERTIES ('novarocks.doc.future.version' = 'forbidden');

-- query 8
-- @expect_error=novarocks.* namespace is reserved
CREATE TABLE rejected_2 (id INT) TBLPROPERTIES ('novarocks.documents.v1' = 'forbidden');

-- query 9
-- Success with the exact same name proves the rejected CREATE was invisible.
-- @skip_result_check=true
CREATE TABLE rejected_2 (id INT);
DROP TABLE rejected_2;

-- query 10
-- @expect_error=novarocks.* namespace is reserved
ALTER TABLE safe_table SET TBLPROPERTIES ('novarocks.documents.v1' = 'forbidden');

-- query 11
-- @expect_error=novarocks.* namespace is reserved
CREATE TABLE rejected_3 (id INT) TBLPROPERTIES ('novarocks.internal.pending-documents.v1' = 'forbidden');

-- query 12
-- Success with the exact same name proves the rejected CREATE was invisible.
-- @skip_result_check=true
CREATE TABLE rejected_3 (id INT);
DROP TABLE rejected_3;

-- query 13
-- @expect_error=novarocks.* namespace is reserved
ALTER TABLE safe_table SET TBLPROPERTIES ('novarocks.internal.pending-documents.v1' = 'forbidden');

-- query 14
-- @expect_error=novarocks.* namespace is reserved
CREATE TABLE rejected_4 (id INT) TBLPROPERTIES ('novarocks.documents.preparation-digest.v1' = 'forbidden');

-- query 15
-- Success with the exact same name proves the rejected CREATE was invisible.
-- @skip_result_check=true
CREATE TABLE rejected_4 (id INT);
DROP TABLE rejected_4;

-- query 16
-- @expect_error=novarocks.* namespace is reserved
ALTER TABLE safe_table SET TBLPROPERTIES ('novarocks.documents.preparation-digest.v1' = 'forbidden');

-- query 17
-- @expect_error=novarocks.* namespace is reserved
CREATE TABLE rejected_5 (id INT) TBLPROPERTIES ('novarocks.documents.update.operation-id.v1' = 'forbidden');

-- query 18
-- Success with the exact same name proves the rejected CREATE was invisible.
-- @skip_result_check=true
CREATE TABLE rejected_5 (id INT);
DROP TABLE rejected_5;

-- query 19
-- @expect_error=novarocks.* namespace is reserved
ALTER TABLE safe_table SET TBLPROPERTIES ('novarocks.documents.update.operation-id.v1' = 'forbidden');

-- query 20
-- @expect_error=novarocks.* namespace is reserved
CREATE TABLE rejected_6 (id INT) TBLPROPERTIES ('novarocks.managed.owner' = 'forbidden');

-- query 21
-- Success with the exact same name proves the rejected CREATE was invisible.
-- @skip_result_check=true
CREATE TABLE rejected_6 (id INT);
DROP TABLE rejected_6;

-- query 22
-- @expect_error=novarocks.* namespace is reserved
ALTER TABLE safe_table SET TBLPROPERTIES ('novarocks.managed.owner' = 'forbidden');

-- query 23
SELECT id FROM safe_table ORDER BY id;

-- query 24
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE safe_table;
DROP DATABASE iceberg_ddl_cat_${suite_uuid0}.doc_props_${uuid0};
