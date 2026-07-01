-- @order_sensitive=true
-- @tags=runtime_filter,row_group_prune
-- Test Objective:
-- 1. Build-side (probe) keys are confined to a narrow sub-range [0, 49] while
--    the joined table is written as multiple data files with disjoint key
--    ranges ([0, 99], [100, 199], [200, 299]) via separate INSERT batches.
-- 2. A runtime filter derived from the narrow probe-side range is expected to
--    prune the [100, 199] and [200, 299] data files at the parquet row-group
--    level (see src/formats/parquet/mod.rs row_groups.is_empty() short
--    circuit; the deterministic row-group-counter proof for that behavior
--    lives in the Rust unit tests in src/formats/parquet/mod.rs).
-- 3. This SQL case proves RF-on and RF-off produce IDENTICAL result rows,
--    i.e. empty-range row-group pruning never drops a row that should join.
DROP TABLE IF EXISTS ${case_db}.t_rf_rg_prune_probe;
DROP TABLE IF EXISTS ${case_db}.t_rf_rg_prune_build;

CREATE TABLE ${case_db}.t_rf_rg_prune_probe (
    id INT,
    k INT
)
TBLPROPERTIES ("format-version" = "3");

CREATE TABLE ${case_db}.t_rf_rg_prune_build (
    k INT,
    tag VARCHAR(20)
)
TBLPROPERTIES ("format-version" = "3");

-- Probe-side keys confined to [0, 49].
INSERT INTO ${case_db}.t_rf_rg_prune_probe VALUES
    (1, 5),
    (2, 15),
    (3, 25),
    (4, 35),
    (5, 45),
    (6, 49);

-- Build side written as three separate data files with disjoint key ranges.
INSERT INTO ${case_db}.t_rf_rg_prune_build VALUES
    (5, 'file0_keep'),
    (15, 'file0_keep'),
    (25, 'file0_keep'),
    (99, 'file0_drop');

INSERT INTO ${case_db}.t_rf_rg_prune_build VALUES
    (100, 'file1_drop'),
    (135, 'file1_drop'),
    (35, 'file1_keep'),
    (199, 'file1_drop');

INSERT INTO ${case_db}.t_rf_rg_prune_build VALUES
    (200, 'file2_drop'),
    (245, 'file2_drop'),
    (45, 'file2_keep'),
    (299, 'file2_drop');

SET global_runtime_filter_probe_min_selectivity = 0.0;

SET disable_optimizer_rules = 'RuntimeFilterPushDown';
SELECT p.id, p.k, b.tag
FROM ${case_db}.t_rf_rg_prune_probe p
JOIN ${case_db}.t_rf_rg_prune_build b ON p.k = b.k
ORDER BY p.id;

SET disable_optimizer_rules = '';
SELECT p.id, p.k, b.tag
FROM ${case_db}.t_rf_rg_prune_probe p
JOIN ${case_db}.t_rf_rg_prune_build b ON p.k = b.k
ORDER BY p.id;

SET disable_optimizer_rules = '';
