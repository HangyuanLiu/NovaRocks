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
-- @tags=project,string
-- Test Objective:
-- 1. Validate LOCATE argument order semantics and INSTR compatibility.
-- 2. Validate LOCATE with start position and UTF-8 character index behavior.
-- Test Flow:
-- 1. Evaluate constant LOCATE/INSTR expressions.
-- 2. Cover LOCATE(start_pos), empty needle, and UTF-8 input.
-- 3. Assert deterministic scalar outputs.
SELECT
  LOCATE('b', 'abc') AS locate_basic,
  INSTR('abc', 'b') AS instr_basic,
  LOCATE('b', 'abc', 2) AS locate_with_pos,
  LOCATE('', 'abc', 2) AS locate_empty_needle,
  LOCATE('é', 'aébc', 2) AS locate_utf8;
