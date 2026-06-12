-- @tags=analytic,p5,typedesc,topn,cross-process
DROP TABLE IF EXISTS ${case_db}.analytic_preagg_state_contract;
CREATE TABLE ${case_db}.analytic_preagg_state_contract (
    grp INT,
    score INT,
    v BIGINT,
    d DOUBLE
);
INSERT INTO ${case_db}.analytic_preagg_state_contract VALUES
    (1, 100, 10, 0.10),
    (1, 90, 20, 0.20),
    (1, 90, 30, 0.30),
    (2, 100, 100, 0.40),
    (2, 80, 200, 0.50),
    (2, 70, NULL, NULL);

SELECT grp, score, avg_v, bitmap_v
FROM (
    SELECT
        grp,
        score,
        CAST(avg(v) OVER (
            PARTITION BY grp
            ORDER BY score DESC, v
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS DECIMAL(18, 4)) AS avg_v,
        bitmap_union_count(to_bitmap(v)) OVER (PARTITION BY grp) AS bitmap_v,
        rank() OVER (PARTITION BY grp ORDER BY score DESC) AS rk
    FROM ${case_db}.analytic_preagg_state_contract
) t
WHERE rk <= 2
ORDER BY grp, score DESC, avg_v;
