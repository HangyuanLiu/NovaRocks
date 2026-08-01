CREATE EXTERNAL CATALOG compat_iceberg_write
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "iceberg.catalog.uri" = "${iceberg_rest_uri}",
  "iceberg.catalog.warehouse" = "${iceberg_rest_warehouse}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.region" = "us-east-1",
  "aws.s3.enable_path_style_access" = "true",
  "enable_iceberg_metadata_cache" = "false"
);

CREATE DATABASE IF NOT EXISTS compat_iceberg_write.${case_db};

DROP TABLE IF EXISTS compat_iceberg_write.${case_db}.writer_rows;

CREATE TABLE compat_iceberg_write.${case_db}.writer_rows (
  k INT,
  v STRING
) PROPERTIES (
  "format-version" = "2"
);

-- @be_log_contains=compat_connector_write carrier=common collector=fragment_owned projector=provider_owned
INSERT INTO compat_iceberg_write.${case_db}.writer_rows VALUES
  (1, 'one'),
  (2, 'two'),
  (3, 'three');

SELECT 'committed' AS write_status;

DROP TABLE compat_iceberg_write.${case_db}.writer_rows;
DROP DATABASE compat_iceberg_write.${case_db};
DROP CATALOG compat_iceberg_write;
