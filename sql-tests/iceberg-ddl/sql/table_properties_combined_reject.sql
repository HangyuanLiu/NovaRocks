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
-- Parser-level rejects: empty parens, duplicate keys, unsupported grammar.

-- query 1
CREATE DATABASE iceberg_ddl_cat_${suite_uuid0}.tblprops_grammar_${uuid0};
USE iceberg_ddl_cat_${suite_uuid0}.tblprops_grammar_${uuid0};
DROP TABLE IF EXISTS p;
CREATE TABLE p (id INT) TBLPROPERTIES ("format-version" = "2");

-- query 2
-- @expect_error=at least one
ALTER TABLE p SET TBLPROPERTIES ();

-- query 3
-- @expect_error=at least one
ALTER TABLE p UNSET TBLPROPERTIES ();

-- query 4
-- @expect_error=duplicate
ALTER TABLE p SET TBLPROPERTIES ('a' = '1', 'a' = '2');

-- query 5
-- @expect_error=duplicate
ALTER TABLE p UNSET TBLPROPERTIES ('a', 'a');

-- query 6
DROP TABLE p;
DROP DATABASE iceberg_ddl_cat_${suite_uuid0}.tblprops_grammar_${uuid0};
