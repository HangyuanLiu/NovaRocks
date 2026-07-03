-- @tags=aggregate,p5,typedesc,intermediate-state,cross-process
CREATE TABLE ${case_db}.agg_state_typedesc_contract (
    grp INT,
    k INT,
    v BIGINT,
    d DOUBLE
)
TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.agg_state_typedesc_contract VALUES
    (1, 10, 10, 0.10),
    (1, 11, 20, 0.20),
    (1, 12, 20, 0.30),
    (2, 20, 100, 0.40),
    (2, 21, 200, 0.50),
    (2, 22, NULL, NULL);

SELECT /*+ SET_VAR(streaming_preaggregation_mode='force_preaggregation') */
       grp,
       CAST(avg(v) AS DECIMAL(18, 4)) AS avg_v,
       ndv(k) AS ndv_k,
       hll_union_agg(hll_hash(k)) AS hll_k,
       CAST(percentile_approx(d, 0.5) AS DECIMAL(18, 4)) AS p50_d
FROM ${case_db}.agg_state_typedesc_contract
GROUP BY grp
ORDER BY grp;

SELECT /*+ SET_VAR(streaming_preaggregation_mode='force_streaming') */
       grp,
       CAST(avg(v) AS DECIMAL(18, 4)) AS avg_v,
       ndv(k) AS ndv_k
FROM ${case_db}.agg_state_typedesc_contract
GROUP BY grp
ORDER BY grp;
