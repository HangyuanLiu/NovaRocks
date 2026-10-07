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

-- Test Objective:
-- 1. Validate JSON value-form IN / NOT IN subquery membership over persisted
--    JSON probe and RHS columns: one nullable Boolean per probe row, outer
--    duplicates preserved, and the three-valued reduction (empty RHS, match
--    plus SQL NULL, no match plus SQL NULL, all FALSE).
-- 2. Lock the existing JSON pair comparison: object key order is ignored,
--    duplicate keys keep the last value, array order matters, integer and
--    float number categories differ (1 vs 1.0), -0 is a float equal to 0.0,
--    string escapes compare after decoding, and JSON null is a value rather
--    than SQL NULL.
-- 3. Cover SELECT, WHERE OR operand, HAVING OR operand with an
--    aggregate-derived probe, CTE-sourced RHS and a CASE probe.
-- 4. Keep a plain STRING control column holding each JSON value's source
--    text: STRING IN uses ordinary text equality.
-- 5. Reject the excluded shapes explicitly.
--
-- Seed layout. jm_probe keeps one input row per fixture_row_ordinal; rows 17
-- and 18 repeat pid 1 and pid 16 so outer multiplicity is observable. jm_rhs
-- groups RHS rows by tag; no row carries tag 'empty'. json_object keeps the
-- caller's top-level key order and duplicate keys in its text; parse_json
-- stores normalized text. Column s holds the source text of each value.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.jm_probe (
  fixture_row_ordinal BIGINT NOT NULL,
  pid BIGINT NOT NULL,
  js JSON NULL,
  s STRING NULL
) TBLPROPERTIES ("format-version" = "3");

CREATE TABLE ${case_db}.jm_rhs (
  fixture_row_ordinal BIGINT NOT NULL,
  tag STRING NOT NULL,
  js JSON NULL,
  s STRING NULL
) TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.jm_probe VALUES (1, 1, json_object('a', 1, 'b', 2), '{"a":1,"b":2}');
INSERT INTO ${case_db}.jm_probe VALUES (2, 2, json_object('b', 2, 'a', 1), '{"b":2,"a":1}');
INSERT INTO ${case_db}.jm_probe VALUES (3, 3, json_object('a', 1, 'a', 2), '{"a":1,"a":2}');
INSERT INTO ${case_db}.jm_probe VALUES (4, 4, parse_json('[1,2]'), '[1,2]');
INSERT INTO ${case_db}.jm_probe VALUES (5, 5, parse_json('[2,1]'), '[2,1]');
INSERT INTO ${case_db}.jm_probe VALUES (6, 6, parse_json('1'), '1');
INSERT INTO ${case_db}.jm_probe VALUES (7, 7, parse_json('1.0'), '1.0');
INSERT INTO ${case_db}.jm_probe VALUES (8, 8, parse_json('1e0'), '1e0');
INSERT INTO ${case_db}.jm_probe VALUES (9, 9, parse_json('18446744073709551615'), '18446744073709551615');
INSERT INTO ${case_db}.jm_probe VALUES (10, 10, parse_json('18446744073709551616'), '18446744073709551616');
INSERT INTO ${case_db}.jm_probe VALUES (11, 11, parse_json('-0'), '-0');
INSERT INTO ${case_db}.jm_probe VALUES (12, 12, parse_json('0'), '0');
INSERT INTO ${case_db}.jm_probe VALUES (13, 13, parse_json('"\\u0041"'), '"\\u0041"');
INSERT INTO ${case_db}.jm_probe VALUES (14, 14, parse_json('null'), 'null');
INSERT INTO ${case_db}.jm_probe VALUES (15, 15, parse_json('{"a":null}'), '{"a":null}');
INSERT INTO ${case_db}.jm_probe VALUES (16, 16, NULL, NULL);
INSERT INTO ${case_db}.jm_probe VALUES (17, 1, json_object('a', 1, 'b', 2), '{"a":1,"b":2}');
INSERT INTO ${case_db}.jm_probe VALUES (18, 16, NULL, NULL);

INSERT INTO ${case_db}.jm_rhs VALUES (1, 'sem', json_object('a', 1, 'b', 2), '{"a":1,"b":2}');
INSERT INTO ${case_db}.jm_rhs VALUES (2, 'sem', parse_json('{"a":2}'), '{"a":2}');
INSERT INTO ${case_db}.jm_rhs VALUES (3, 'sem', parse_json('[1,2]'), '[1,2]');
INSERT INTO ${case_db}.jm_rhs VALUES (4, 'sem', parse_json('1.0'), '1.0');
INSERT INTO ${case_db}.jm_rhs VALUES (5, 'sem', parse_json('18446744073709551617'), '18446744073709551617');
INSERT INTO ${case_db}.jm_rhs VALUES (6, 'sem', parse_json('0.0'), '0.0');
INSERT INTO ${case_db}.jm_rhs VALUES (7, 'sem', parse_json('"A"'), '"A"');
INSERT INTO ${case_db}.jm_rhs VALUES (8, 'sem', parse_json('null'), 'null');
INSERT INTO ${case_db}.jm_rhs VALUES (9, 'sem', parse_json('{ "b" : 2 , "a" : 1 }'), '{ "b" : 2 , "a" : 1 }');
INSERT INTO ${case_db}.jm_rhs VALUES (10, 'match_null', json_object('b', 2, 'a', 1), '{"b":2,"a":1}');
INSERT INTO ${case_db}.jm_rhs VALUES (11, 'match_null', NULL, NULL);
INSERT INTO ${case_db}.jm_rhs VALUES (12, 'miss_null', parse_json('{"a":3}'), '{"a":3}');
INSERT INTO ${case_db}.jm_rhs VALUES (13, 'miss_null', NULL, NULL);
INSERT INTO ${case_db}.jm_rhs VALUES (14, 'miss', parse_json('{"a":3}'), '{"a":3}');
INSERT INTO ${case_db}.jm_rhs VALUES (15, 'miss', parse_json('[3]'), '[3]');
INSERT INTO ${case_db}.jm_rhs VALUES (16, 'jnull', parse_json('null'), 'null');

-- query 2
-- SELECT value membership against the comparison-semantics RHS. The RHS holds
-- two JSON-equal objects (rows 1 and 9); a probe row must still appear once.
SELECT pid, js IN (SELECT js FROM ${case_db}.jm_rhs WHERE tag = 'sem') AS json_in
FROM ${case_db}.jm_probe
ORDER BY pid;

-- query 3
SELECT pid, js NOT IN (SELECT js FROM ${case_db}.jm_rhs WHERE tag = 'sem') AS json_not_in
FROM ${case_db}.jm_probe
ORDER BY pid;

-- query 4
-- Plain STRING control over the same source text: ordinary text equality.
SELECT pid, s IN (SELECT s FROM ${case_db}.jm_rhs WHERE tag = 'sem') AS string_in
FROM ${case_db}.jm_probe
ORDER BY pid;

-- query 5
-- Output rows equal probe rows: 18 rows, 16 non-NULL, 11 TRUE.
SELECT count(*) AS total_rows,
       count(json_in) AS non_null_rows,
       sum(CASE WHEN json_in THEN 1 ELSE 0 END) AS true_rows
FROM (
  SELECT pid, js IN (SELECT js FROM ${case_db}.jm_rhs WHERE tag = 'sem') AS json_in
  FROM ${case_db}.jm_probe
) q;

-- query 6
-- Empty RHS: IN is FALSE for every probe, including SQL NULL probes.
SELECT pid, js IN (SELECT js FROM ${case_db}.jm_rhs WHERE tag = 'empty') AS in_empty
FROM ${case_db}.jm_probe
WHERE pid IN (1, 5, 14, 16)
ORDER BY pid;

-- query 7
SELECT pid, js NOT IN (SELECT js FROM ${case_db}.jm_rhs WHERE tag = 'empty') AS not_in_empty
FROM ${case_db}.jm_probe
WHERE pid IN (1, 5, 14, 16)
ORDER BY pid;

-- query 8
-- A match wins over an RHS SQL NULL; without a match the result is NULL.
SELECT pid, js IN (SELECT js FROM ${case_db}.jm_rhs WHERE tag = 'match_null') AS in_match_null
FROM ${case_db}.jm_probe
WHERE pid IN (1, 2, 3, 14, 16)
ORDER BY pid;

-- query 9
SELECT pid, js NOT IN (SELECT js FROM ${case_db}.jm_rhs WHERE tag = 'match_null') AS not_in_match_null
FROM ${case_db}.jm_probe
WHERE pid IN (1, 2, 3, 14, 16)
ORDER BY pid;

-- query 10
-- No match plus an RHS SQL NULL is NULL for IN and NOT IN.
SELECT pid, js IN (SELECT js FROM ${case_db}.jm_rhs WHERE tag = 'miss_null') AS in_miss_null
FROM ${case_db}.jm_probe
WHERE pid IN (1, 4, 14, 15, 16)
ORDER BY pid;

-- query 11
SELECT pid, js NOT IN (SELECT js FROM ${case_db}.jm_rhs WHERE tag = 'miss_null') AS not_in_miss_null
FROM ${case_db}.jm_probe
WHERE pid IN (1, 4, 14, 15, 16)
ORDER BY pid;

-- query 12
-- Every comparison FALSE: IN FALSE, NOT IN TRUE; a SQL NULL probe stays NULL.
SELECT pid, js IN (SELECT js FROM ${case_db}.jm_rhs WHERE tag = 'miss') AS in_miss
FROM ${case_db}.jm_probe
WHERE pid IN (1, 4, 14, 15, 16)
ORDER BY pid;

-- query 13
SELECT pid, js NOT IN (SELECT js FROM ${case_db}.jm_rhs WHERE tag = 'miss') AS not_in_miss
FROM ${case_db}.jm_probe
WHERE pid IN (1, 4, 14, 15, 16)
ORDER BY pid;

-- query 14
-- JSON null in the RHS is a value: it matches only the JSON null probe and
-- never turns a mismatch into NULL.
SELECT pid, js IN (SELECT js FROM ${case_db}.jm_rhs WHERE tag = 'jnull') AS in_json_null
FROM ${case_db}.jm_probe
WHERE pid IN (1, 14, 15, 16)
ORDER BY pid;

-- query 15
SELECT pid, js NOT IN (SELECT js FROM ${case_db}.jm_rhs WHERE tag = 'jnull') AS not_in_json_null
FROM ${case_db}.jm_probe
WHERE pid IN (1, 14, 15, 16)
ORDER BY pid;

-- query 16
-- WHERE OR operand: TRUE keeps the row, NULL keeps it only through the other
-- operand.
SELECT fixture_row_ordinal AS ord, pid
FROM ${case_db}.jm_probe
WHERE js IN (SELECT js FROM ${case_db}.jm_rhs WHERE tag = 'match_null') OR pid = 6
ORDER BY ord;

-- query 17
-- NOT IN as an OR operand below a top-level AND.
SELECT fixture_row_ordinal AS ord, pid
FROM ${case_db}.jm_probe
WHERE pid <> 9
  AND (js NOT IN (SELECT js FROM ${case_db}.jm_rhs WHERE tag = 'sem') OR pid = 14)
ORDER BY ord;

-- query 18
-- HAVING OR operand with an aggregate-derived probe evaluated once per group.
SELECT pid, count(*) AS n
FROM ${case_db}.jm_probe
GROUP BY pid
HAVING CASE WHEN count(*) > 1 THEN parse_json('{"b":2,"a":1}') ELSE parse_json('[2,1]') END
         IN (SELECT js FROM ${case_db}.jm_rhs WHERE tag = 'sem')
    OR pid = 5
ORDER BY pid;

-- query 19
SELECT pid, count(*) AS n
FROM ${case_db}.jm_probe
GROUP BY pid
HAVING CASE WHEN count(*) > 1 THEN parse_json('{"a":3}')
            WHEN pid = 7 THEN NULL
            ELSE parse_json('{"a":4}') END
         NOT IN (SELECT js FROM ${case_db}.jm_rhs WHERE tag = 'miss')
    OR pid = 16
ORDER BY pid;

-- query 20
-- CTE-sourced probe and RHS.
WITH probe AS (
  SELECT pid, js FROM ${case_db}.jm_probe WHERE pid IN (1, 2, 3, 16)
), rhs AS (
  SELECT js FROM ${case_db}.jm_rhs WHERE tag = 'match_null'
)
SELECT pid, js IN (SELECT js FROM rhs) AS cte_in
FROM probe
ORDER BY pid;

-- query 21
-- One CTE feeds both the probe and the RHS.
WITH sc AS (
  SELECT pid, js FROM ${case_db}.jm_probe
)
SELECT pid, js NOT IN (SELECT js FROM sc WHERE pid IN (2, 3)) AS self_not_in
FROM sc
WHERE pid IN (1, 2, 3, 4, 16)
ORDER BY pid;

-- query 22
-- CASE probe: the NULL branch is a SQL NULL probe against a non-empty RHS.
SELECT pid,
       CASE WHEN pid <= 3 THEN js ELSE NULL END
         IN (SELECT js FROM ${case_db}.jm_rhs WHERE tag = 'sem') AS case_in
FROM ${case_db}.jm_probe
WHERE pid IN (1, 3, 5, 14)
ORDER BY pid;

-- query 23
-- Rejected: a top-level WHERE filter is a semi-join shape, not a value.
-- @expect_error_tier=drift
-- @expect_error=JSON membership requires SELECT value or WHERE/HAVING OR operand
SELECT pid
FROM ${case_db}.jm_probe
WHERE js IN (SELECT js FROM ${case_db}.jm_rhs WHERE tag = 'sem');

-- query 24
-- Rejected: NOT without an OR owner in WHERE.
-- @expect_error_tier=drift
-- @expect_error=JSON membership requires SELECT value or WHERE/HAVING OR operand
SELECT pid
FROM ${case_db}.jm_probe
WHERE NOT (js IN (SELECT js FROM ${case_db}.jm_rhs WHERE tag = 'sem'));

-- query 25
-- Rejected: CASE without an OR owner in WHERE.
-- @expect_error_tier=drift
-- @expect_error=JSON membership requires SELECT value or WHERE/HAVING OR operand
SELECT pid
FROM ${case_db}.jm_probe
WHERE CASE WHEN js IN (SELECT js FROM ${case_db}.jm_rhs WHERE tag = 'sem') THEN true ELSE false END;

-- query 26
-- Rejected: a top-level HAVING filter without an OR owner.
-- @expect_error_tier=drift
-- @expect_error=JSON membership requires SELECT value or WHERE/HAVING OR operand
SELECT pid, count(*) AS n
FROM ${case_db}.jm_probe
GROUP BY pid
HAVING CASE WHEN count(*) > 1 THEN parse_json('{"a":1,"b":2}') ELSE parse_json('[1,2]') END
         IN (SELECT js FROM ${case_db}.jm_rhs WHERE tag = 'sem');

-- query 27
-- Rejected: membership inside a HAVING aggregate argument.
-- @expect_error_tier=drift
-- @expect_error=JSON membership is not supported in aggregate arguments
SELECT pid, count(*) AS n
FROM ${case_db}.jm_probe
GROUP BY pid
HAVING sum(CASE WHEN parse_json('{"a":2}') IN (SELECT js FROM ${case_db}.jm_rhs WHERE tag = 'sem') THEN 1 ELSE 0 END) > 0
    OR count(*) = 0;

-- query 28
-- Rejected: membership inside a SELECT aggregate argument.
-- @expect_error_tier=drift
-- @expect_error=JSON membership requires SELECT value or WHERE/HAVING OR operand
SELECT sum(CASE WHEN parse_json('{"a":2}') IN (SELECT js FROM ${case_db}.jm_rhs WHERE tag = 'sem') THEN 1 ELSE 0 END) AS hits
FROM ${case_db}.jm_probe;

-- query 29
-- Rejected: JOIN ON.
-- @expect_error_tier=drift
-- @expect_error=JSON membership is not supported in JOIN ON
SELECT a.pid
FROM ${case_db}.jm_probe a
JOIN ${case_db}.jm_rhs b
  ON a.fixture_row_ordinal = b.fixture_row_ordinal
 AND a.js IN (SELECT js FROM ${case_db}.jm_rhs WHERE tag = 'sem');

-- query 30
-- Rejected: a correlated RHS.
-- @expect_error_tier=drift
-- @expect_error=correlated JSON membership is not supported
SELECT p.pid,
       p.js IN (SELECT r.js FROM ${case_db}.jm_rhs r WHERE r.fixture_row_ordinal = p.pid) AS m
FROM ${case_db}.jm_probe p;

-- query 31
-- Rejected: a multi-column RHS for a single JSON probe.
-- @expect_error_tier=drift
-- @expect_error=JSON membership requires exactly one RHS column
SELECT pid, js IN (SELECT js, s FROM ${case_db}.jm_rhs WHERE tag = 'sem') AS m
FROM ${case_db}.jm_probe;

-- query 32
-- Rejected: JSON probe against a plain STRING RHS.
-- @expect_error_tier=drift
-- @expect_error=JSON membership requires proven JSON operands on both sides
SELECT pid, js IN (SELECT s FROM ${case_db}.jm_rhs WHERE tag = 'sem') AS m
FROM ${case_db}.jm_probe;

-- query 33
-- Rejected: plain STRING probe against a JSON RHS.
-- @expect_error_tier=drift
-- @expect_error=JSON operands require single-column value membership
SELECT pid, s IN (SELECT js FROM ${case_db}.jm_rhs WHERE tag = 'sem') AS m
FROM ${case_db}.jm_probe;

-- query 34
-- Rejected: a multi-column probe tuple carrying JSON.
-- @expect_error_tier=drift
-- @expect_error=JSON operands require single-column value membership
SELECT pid,
       (js, pid) IN (SELECT js, fixture_row_ordinal FROM ${case_db}.jm_rhs WHERE tag = 'sem') AS m
FROM ${case_db}.jm_probe;
