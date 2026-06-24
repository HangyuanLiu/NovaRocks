-- @catalog=iceberg_opt
-- Distributed optimizer parity suite. Keep this catalog aligned with the main
-- optimizer suite so ANALYZE-derived NDV reaches the cost-based optimizer.
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
