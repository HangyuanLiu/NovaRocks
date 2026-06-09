-- @catalog=iceberg_opt
-- Create the iceberg catalog the optimizer suite uses for its base tables, so
-- ANALYZE-derived NDV (Puffin statistics) reaches the cost-based optimizer.
-- Managed-lake (StarRocks-type) tables are intentionally not exercised by this
-- suite. The catalog name is stable (per-case case_db reset isolates data);
-- a distinct warehouse sub-path keeps these tables separate from other iceberg
-- suites that share the warehouse root.
CREATE EXTERNAL CATALOG IF NOT EXISTS `iceberg_opt`
PROPERTIES (
    "type"="iceberg",
    "iceberg.catalog.type"="${iceberg_catalog_type}",
    "iceberg.catalog.warehouse"="${iceberg_catalog_warehouse}/optimizer",
    "aws.s3.access_key"="${oss_ak}",
    "aws.s3.secret_key"="${oss_sk}",
    "aws.s3.endpoint"="${oss_endpoint}",
    "aws.s3.enable_path_style_access"="true"
);
