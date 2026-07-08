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
-- Test Point: CREATE TABLE with non-NULL DEFAULT on a v2 (default) Iceberg table is hard-rejected.
-- Method: CREATE TABLE without explicit format-version (defaults to v2), include DEFAULT 5; expect a clear error mentioning format-version 3.
-- Scope: v2 table policy (D5 in spec) — fail-fast at DDL.

-- query 1
-- @expect_error=format-version 3
CREATE TABLE ${case_db}.t_v3_default_v2_rej (
  a INT,
  b INT DEFAULT 5
);
