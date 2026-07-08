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
-- @tags=iceberg_dml,delete
-- Test Objective:
-- DELETE WHERE col IN (...) removes matching rows on an Iceberg v3 table.
DROP TABLE IF EXISTS ${case_db}.t_delete_in_list;
CREATE TABLE ${case_db}.t_delete_in_list (
  id INT,
  v INT
) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t_delete_in_list VALUES (1, 10), (2, 20), (3, 30), (4, 40);
DELETE FROM ${case_db}.t_delete_in_list WHERE id IN (1, 3);
SELECT id, v FROM ${case_db}.t_delete_in_list ORDER BY id;
