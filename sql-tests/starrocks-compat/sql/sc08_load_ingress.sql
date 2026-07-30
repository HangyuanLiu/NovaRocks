TRUNCATE TABLE starrocks_compat_suite_setup.load_ingress_rows;

-- @compat_probe=stream-load
-- @compat_probe=transaction-load
SELECT 'load ingress probes' AS probe_status;

SELECT k, source
FROM starrocks_compat_suite_setup.load_ingress_rows
WHERE k IN (7001, 7002)
ORDER BY k;
