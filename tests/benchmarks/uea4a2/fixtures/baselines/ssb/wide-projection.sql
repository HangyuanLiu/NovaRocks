SELECT snapshot_id FROM uea4a2_ssb.ssb.lineorder.refs WHERE name = 'main';
SELECT COUNT(*) AS row_count, SUM(lo_orderkey) AS orderkey_sum, SUM(lo_partkey) AS partkey_sum, SUM(lo_ordtotalprice) AS ordtotalprice_sum, SUM(lo_revenue) AS revenue_sum FROM uea4a2_ssb.ssb.lineorder;
