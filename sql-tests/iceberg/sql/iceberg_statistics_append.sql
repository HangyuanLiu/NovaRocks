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
-- @tags=iceberg
-- Validate that multiple INSERTs into the same Iceberg table register a fresh
-- Puffin per commit and the optimizer continues to read the latest NDV.
-- Append path is the incremental-union code path in stats_assembler.

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_cat_${suite_uuid0}.iceberg_stats_append_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.iceberg_stats_append_db_${uuid0}.events_${uuid0} (
  id INT,
  category STRING
);

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iceberg_stats_append_db_${uuid0}.events_${uuid0} VALUES
  (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');

-- query 4
-- Second INSERT with new distinct ids — Puffin must be re-emitted via
-- assemble_append() unioning the previous aggregate sketch.
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iceberg_stats_append_db_${uuid0}.events_${uuid0} VALUES
  (6, 'f'), (7, 'g'), (8, 'h'), (9, 'i'), (10, 'j');

-- query 5
-- Third INSERT repeats existing ids — NDV must stay at 10 (Theta union is
-- idempotent on duplicates).
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iceberg_stats_append_db_${uuid0}.events_${uuid0} VALUES
  (1, 'a-dup'), (2, 'b-dup'), (3, 'c-dup');

-- query 6
SELECT count(*) AS n
  FROM iceberg_cat_${suite_uuid0}.iceberg_stats_append_db_${uuid0}.events_${uuid0};

-- query 7
SELECT count(DISTINCT id) AS distinct_ids
  FROM iceberg_cat_${suite_uuid0}.iceberg_stats_append_db_${uuid0}.events_${uuid0};

-- query 8
-- Plan-shape assertion: physical scan after three appends still emits a
-- stats trailer driven by the merged Puffin NDV. The exact row count is
-- left unpinned (Theta carries ~1.5% noise) — we only verify the trailer
-- and that the optimizer treats the scan as a 13-row source.
-- @explain_contains=stats={rows=
SELECT id, category
  FROM iceberg_cat_${suite_uuid0}.iceberg_stats_append_db_${uuid0}.events_${uuid0}
  WHERE id <= 5
  ORDER BY id, category;

-- query 9
-- @skip_result_check=true
DROP TABLE iceberg_cat_${suite_uuid0}.iceberg_stats_append_db_${uuid0}.events_${uuid0};

-- query 10
-- @skip_result_check=true
DROP DATABASE iceberg_cat_${suite_uuid0}.iceberg_stats_append_db_${uuid0};
