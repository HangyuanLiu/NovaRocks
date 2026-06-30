//! ADD FILES implementation for Iceberg tables.
//!
//! Registers existing parquet files from S3/OSS into an Iceberg table's
//! metadata without data movement.

use std::collections::{BTreeMap, HashMap};

use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableIdent};

use crate::connector::iceberg::catalog::registry::{
    IcebergCatalogEntry, block_on_iceberg, build_hadoop_catalog, load_table,
};
use crate::engine::catalog::normalize_identifier;
use crate::fs::object_store::{ObjectStoreConfig, build_oss_operator};

/// Execute ADD FILES: register parquet files from an S3 directory into an Iceberg table.
pub(crate) fn add_files(
    entry: &IcebergCatalogEntry,
    namespace: &str,
    table_name: &str,
    s3_directory: &str,
) -> Result<usize, String> {
    let loaded = load_table(entry, namespace, table_name)?;
    let s3_config = build_s3_config_from_properties(&entry.properties)?;

    let files = list_parquet_files(&s3_config, s3_directory)?;
    tracing::info!(
        "ADD FILES: found {} parquet files in {s3_directory}",
        files.len()
    );
    for (path, size) in &files {
        tracing::info!("  file: {path} ({size} bytes)");
    }
    if files.is_empty() {
        return Err(format!(
            "ADD FILES: no parquet files found in {s3_directory} (bucket={}, prefix from parse)",
            parse_s3_path(s3_directory)
                .map(|(b, _)| b)
                .unwrap_or_default()
        ));
    }

    let mut data_files = Vec::with_capacity(files.len());
    for (file_path, file_size) in &files {
        let record_count = read_parquet_record_count(&s3_config, file_path, *file_size)?;
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(file_path.clone())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(*file_size)
            .record_count(record_count)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .build()
            .map_err(|e| format!("build DataFile failed: {e}"))?;
        data_files.push(data_file);
    }

    let count = data_files.len();

    let catalog = build_hadoop_catalog(entry)?;
    let ns = NamespaceIdent::new(normalize_identifier(namespace)?);
    let _ = block_on_iceberg(async { catalog.create_namespace(&ns, HashMap::new()).await });
    let table_ident = TableIdent::from_strs([
        normalize_identifier(namespace)?,
        normalize_identifier(table_name)?,
    ])
    .map_err(|e| format!("build table ident: {e}"))?;
    let metadata_location = loaded
        .table
        .metadata_location()
        .ok_or_else(|| "no metadata location for table".to_string())?
        .to_string();
    let _ = block_on_iceberg(async {
        catalog
            .register_table(&table_ident, metadata_location)
            .await
    });

    block_on_iceberg(async {
        let tx = Transaction::new(&loaded.table);
        let tx = tx
            .fast_append()
            .add_data_files(data_files)
            .apply(tx)
            .map_err(|e| format!("append files failed: {e}"))?;
        tx.commit(&catalog)
            .await
            .map_err(|e| format!("commit failed: {e}"))
    })
    .map_err(|e| format!("add_files runtime: {e}"))?
    .map_err(|e| format!("add_files failed: {e}"))?;

    tracing::info!("ADD FILES: registered {count} parquet files into {namespace}.{table_name}");
    Ok(count)
}

// ---------------------------------------------------------------------------
// S3 config helpers
// ---------------------------------------------------------------------------

fn build_s3_config_from_properties(
    properties: &[(String, String)],
) -> Result<ObjectStoreConfig, String> {
    let props_map: BTreeMap<String, String> = properties
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let credentials =
        crate::fs::object_store_credentials::ObjectStoreCredentials::from_aws_s3_properties(
            crate::fs::object_store_credentials::ObjectStoreCredentialsSource::AwsS3Properties,
            &props_map,
        )?;
    let mut cfg = credentials.to_object_store_config("", "");
    crate::fs::object_store::apply_object_store_runtime_defaults(&mut cfg);
    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::build_s3_config_from_properties;

    #[test]
    fn build_s3_config_accepts_shared_aliases_and_retry_values() {
        let props = vec![
            (
                "aws.s3.endpoint_url".to_string(),
                "http://localhost:9000".to_string(),
            ),
            ("aws.s3.accessKeyId".to_string(), "ak".to_string()),
            ("aws.s3.accessKeySecret".to_string(), "sk".to_string()),
            (
                "aws.s3.enable_path_style_access".to_string(),
                "1".to_string(),
            ),
            ("aws.s3.max_retries".to_string(), "7".to_string()),
            ("aws.s3.retry_min_delay_ms".to_string(), "11".to_string()),
            ("aws.s3.retry_max_delay_ms".to_string(), "99".to_string()),
            ("aws.s3.request_timeout_ms".to_string(), "1234".to_string()),
            ("aws.s3.io_timeout_ms".to_string(), "5678".to_string()),
        ];

        let cfg = build_s3_config_from_properties(&props).expect("build S3 config");

        assert_eq!(cfg.endpoint, "http://localhost:9000");
        assert_eq!(cfg.bucket, "");
        assert_eq!(cfg.root, "");
        assert_eq!(cfg.access_key_id, "ak");
        assert_eq!(cfg.access_key_secret, "sk");
        assert_eq!(cfg.session_token, None);
        assert_eq!(cfg.enable_path_style_access, Some(true));
        assert_eq!(cfg.region, None);
        assert_eq!(cfg.retry_max_times, Some(7));
        assert_eq!(cfg.retry_min_delay_ms, Some(11));
        assert_eq!(cfg.retry_max_delay_ms, Some(99));
        assert_eq!(cfg.timeout_ms, Some(1234));
        assert_eq!(cfg.io_timeout_ms, Some(5678));
    }

    #[test]
    fn build_s3_config_leaves_retry_and_timeout_to_runtime_defaults_when_omitted() {
        let props = vec![
            (
                "aws.s3.endpoint_url".to_string(),
                "http://localhost:9000".to_string(),
            ),
            ("aws.s3.accessKeyId".to_string(), "ak".to_string()),
            ("aws.s3.accessKeySecret".to_string(), "sk".to_string()),
        ];

        let cfg = build_s3_config_from_properties(&props).expect("build S3 config");

        assert_eq!(cfg.retry_max_times, None);
        assert_eq!(cfg.retry_min_delay_ms, None);
        assert_eq!(cfg.retry_max_delay_ms, None);
        assert_eq!(cfg.timeout_ms, None);
        assert_eq!(cfg.io_timeout_ms, None);
    }
}

pub(crate) fn parse_s3_path(path: &str) -> Result<(String, String), String> {
    let stripped = path
        .strip_prefix("s3://")
        .or_else(|| path.strip_prefix("s3a://"))
        .or_else(|| path.strip_prefix("oss://"))
        .ok_or_else(|| format!("unsupported path scheme: {path}"))?;
    let slash = stripped
        .find('/')
        .ok_or_else(|| format!("path has no key: {path}"))?;
    Ok((
        stripped[..slash].to_string(),
        stripped[slash + 1..].to_string(),
    ))
}

// ---------------------------------------------------------------------------
// File listing + parquet metadata
// ---------------------------------------------------------------------------

fn list_parquet_files(
    base_config: &ObjectStoreConfig,
    directory: &str,
) -> Result<Vec<(String, u64)>, String> {
    let (bucket, prefix) = parse_s3_path(directory)?;
    let scheme = if directory.starts_with("oss://") {
        "oss"
    } else {
        "s3"
    };
    let mut cfg = base_config.clone();
    cfg.bucket = bucket.clone();
    let op = build_oss_operator(&cfg).map_err(|e| format!("build S3 operator: {e}"))?;

    let prefix = if prefix.ends_with('/') {
        prefix
    } else {
        format!("{prefix}/")
    };

    block_on_iceberg(async {
        let entries = op
            .list(&prefix)
            .await
            .map_err(|e| format!("list {directory}: {e}"))?;

        let mut result = Vec::new();
        for entry in entries {
            let name = entry.name().to_string();
            if name.ends_with(".parquet") && !name.starts_with('.') && !name.starts_with('_') {
                let meta = op
                    .stat(entry.path())
                    .await
                    .map_err(|e| format!("stat {}: {e}", entry.path()))?;
                let full_path = format!("{scheme}://{bucket}/{}", entry.path());
                result.push((full_path, meta.content_length()));
            }
        }
        Ok(result)
    })
    .map_err(|e| format!("list_parquet_files runtime: {e}"))?
}

fn read_parquet_record_count(
    base_config: &ObjectStoreConfig,
    s3_path: &str,
    file_size: u64,
) -> Result<u64, String> {
    let (bucket, key) = parse_s3_path(s3_path)?;
    let mut cfg = base_config.clone();
    cfg.bucket = bucket;
    let op = build_oss_operator(&cfg).map_err(|e| format!("build operator: {e}"))?;

    block_on_iceberg(async {
        if file_size < 12 {
            return Err(format!("parquet file too small: {s3_path}"));
        }
        // Parquet footer: last 8 bytes = [footer_len(4 LE), magic "PAR1"(4)]
        let tail = op
            .read_with(&key)
            .range(file_size - 8..file_size)
            .await
            .map_err(|e| format!("read footer tail: {e}"))?
            .to_bytes();
        if tail.len() < 8 || &tail[4..8] != b"PAR1" {
            return Err(format!("invalid parquet footer: {s3_path}"));
        }
        let footer_len = u32::from_le_bytes([tail[0], tail[1], tail[2], tail[3]]) as u64;

        // Read the Thrift-encoded FileMetaData
        let footer_start = file_size - 8 - footer_len;
        let footer_bytes = op
            .read_with(&key)
            .range(footer_start..file_size - 8)
            .await
            .map_err(|e| format!("read footer: {e}"))?
            .to_bytes();

        // Build suffix bytes (footer_data + footer_len_bytes + magic) and parse
        let mut suffix_buf = Vec::with_capacity(footer_bytes.len() + 8);
        suffix_buf.extend_from_slice(&footer_bytes);
        suffix_buf.extend_from_slice(&tail);
        let suffix = bytes::Bytes::from(suffix_buf);

        use parquet::file::metadata::ParquetMetaDataReader;
        let mut reader = ParquetMetaDataReader::new();
        reader
            .try_parse_sized(&suffix, file_size)
            .map_err(|e| format!("parse parquet metadata: {e}"))?;
        let metadata = reader
            .finish()
            .map_err(|e| format!("finish parquet metadata: {e}"))?;
        Ok(metadata.file_metadata().num_rows() as u64)
    })
    .map_err(|e| format!("read_record_count runtime: {e}"))?
}
