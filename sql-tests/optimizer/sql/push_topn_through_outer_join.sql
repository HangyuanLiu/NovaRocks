-- @tags=optimizer,topn,outer_join
-- Test Objective:
-- Lock in PushTopNThroughJoin plan-shape coverage. LEFT/RIGHT OUTER JOIN
-- preserved-side ORDER BY can push a TopN below the join; null-producing-side
-- and disabled-rule cases must not.

DROP TABLE IF EXISTS ${case_db}.topn_outer_left;
DROP TABLE IF EXISTS ${case_db}.topn_outer_right;
CREATE TABLE ${case_db}.topn_outer_left (id INT, score INT);
CREATE TABLE ${case_db}.topn_outer_right (id INT, payload INT);
INSERT INTO ${case_db}.topn_outer_left
    SELECT generate_series, 100000 - generate_series
    FROM TABLE(generate_series(1, 100000));
INSERT INTO ${case_db}.topn_outer_right
    SELECT generate_series, generate_series * 10
    FROM TABLE(generate_series(1, 1000));
ANALYZE TABLE ${case_db}.topn_outer_left;
ANALYZE TABLE ${case_db}.topn_outer_right;

SET disable_optimizer_rules = '';

EXPLAIN VERBOSE
SELECT l.id, l.score, r.payload
FROM ${case_db}.topn_outer_left l
LEFT JOIN ${case_db}.topn_outer_right r ON l.id = r.id
ORDER BY l.score ASC
LIMIT 5;

EXPLAIN VERBOSE
SELECT l.id, l.score, r.payload
FROM ${case_db}.topn_outer_left l
RIGHT JOIN ${case_db}.topn_outer_right r ON l.id = r.id
ORDER BY r.payload DESC
LIMIT 5;

SET disable_optimizer_rules = 'PushTopNThroughJoin';

EXPLAIN VERBOSE
SELECT l.id, l.score, r.payload
FROM ${case_db}.topn_outer_left l
LEFT JOIN ${case_db}.topn_outer_right r ON l.id = r.id
ORDER BY l.score ASC
LIMIT 5;

SET disable_optimizer_rules = '';

EXPLAIN VERBOSE
SELECT l.id, l.score, r.payload
FROM ${case_db}.topn_outer_left l
LEFT JOIN ${case_db}.topn_outer_right r ON l.id = r.id
ORDER BY r.payload DESC
LIMIT 5;

EXPLAIN VERBOSE
SELECT l.id, l.score, r.payload
FROM ${case_db}.topn_outer_left l
INNER JOIN ${case_db}.topn_outer_right r ON l.id = r.id
ORDER BY l.score ASC
LIMIT 5;

SET disable_optimizer_rules = '';
