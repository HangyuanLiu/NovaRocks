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

-- The current parser rejects residual tokens after a complete statement.
-- This is a drift assertion until SQLP-1 registers a parser-domain code and
-- the engine reports locations against original SQL text.
-- @expect_error_tier=drift
-- @expect_error=ERROR 1064 (42000): sql parser error: syntax error
SELECT * FROM reject_placeholder_left JOIN reject_placeholder_right
    ON reject_placeholder_left.id => reject_placeholder_right.id;
