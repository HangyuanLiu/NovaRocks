-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- @order_sensitive=true
-- ALTER TABLE SET / UNSET TBLPROPERTIES happy path.
-- Note: SHOW CREATE TABLE does not surface TBLPROPERTIES in NovaRocks today.
-- Each SELECT confirms the table is still queryable after each property change.

-- query 1
CREATE DATABASE iceberg_ddl_cat_${suite_uuid0}.tblprops_${uuid0};
USE iceberg_ddl_cat_${suite_uuid0}.tblprops_${uuid0};
DROP TABLE IF EXISTS p;
CREATE TABLE p (id INT) TBLPROPERTIES ("format-version" = "2");
INSERT INTO p VALUES (1);

-- query 2
SELECT id FROM p ORDER BY id;

-- query 3
ALTER TABLE p SET TBLPROPERTIES ('write.parquet.compression-codec' = 'zstd');

-- query 4
SELECT id FROM p ORDER BY id;

-- query 5
ALTER TABLE p SET TBLPROPERTIES ('comment' = 'hello', 'gc.enabled' = 'true');

-- query 6
SELECT id FROM p ORDER BY id;

-- query 7
-- Overwrite an existing key.
ALTER TABLE p SET TBLPROPERTIES ('comment' = 'world');

-- query 8
SELECT id FROM p ORDER BY id;

-- query 9
ALTER TABLE p UNSET TBLPROPERTIES ('comment');

-- query 10
SELECT id FROM p ORDER BY id;

-- query 11
DROP TABLE p;
DROP DATABASE iceberg_ddl_cat_${suite_uuid0}.tblprops_${uuid0};
