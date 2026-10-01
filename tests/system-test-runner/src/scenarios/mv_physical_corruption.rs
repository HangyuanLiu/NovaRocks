// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Physical corruption confined to one runner-owned private target table.
//! No Catalog metadata, snapshot, manifest, or MV document is mutated.

use anyhow::{Context, Result, ensure};
use apache_avro::{Reader, types::Value as Avro};
use arrow_array::{Array, Int32Array, RecordBatch};
use bytes::Bytes;
use novarocks_cluster_harness::isolated_iceberg_rest::IsolatedS3Identity;
use novarocks_connector_iceberg::opendal::{Operator, services::S3};
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
use parquet::file::properties::WriterProperties;
use reqwest::blocking::Client;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::Read;
use std::sync::Arc;
use std::time::{Duration, Instant};

const MAX_BYTES: usize = 8 * 1024 * 1024;
const MAX_RECORDS: usize = 64;
const MAX_ROWS: usize = 1024;

pub(super) fn replace_visible_value(
    rest_uri: &str,
    object_endpoint: &str,
    identity: IsolatedS3Identity,
    namespace: &str,
    table: &str,
    timeout: Duration,
) -> Result<String> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .context("physical corruption deadline overflow")?;
    let client = Client::builder().no_proxy().build()?;
    let url = format!("{rest_uri}/v1/namespaces/{namespace}/tables/{table}");
    let metadata_before = bounded_metadata(&client, &url, deadline)?;
    let metadata = &metadata_before["metadata"];
    let location = metadata["location"]
        .as_str()
        .context("target location absent")?;
    let (bucket, root) = s3_path(location)?;
    ensure!(
        !root.is_empty(),
        "physical corruption requires a private table prefix"
    );
    let prefix = format!("{}/", root.trim_end_matches('/'));
    let snapshot = metadata["current-snapshot-id"]
        .as_i64()
        .context("published target snapshot absent")?;
    let manifest_list = metadata["snapshots"]
        .as_array()
        .context("target snapshots absent")?
        .iter()
        .find(|entry| entry["snapshot-id"].as_i64() == Some(snapshot))
        .and_then(|entry| entry["manifest-list"].as_str())
        .context("exact target manifest list absent")?;
    let current_schema = metadata["current-schema-id"]
        .as_i64()
        .context("target schema ID absent")?;
    let k1_id = metadata["schemas"]
        .as_array()
        .context("target schemas absent")?
        .iter()
        .find(|schema| schema["schema-id"].as_i64() == Some(current_schema))
        .and_then(|schema| schema["fields"].as_array())
        .and_then(|fields| {
            fields
                .iter()
                .find(|field| field["name"].as_str() == Some("k1"))
        })
        .and_then(|field| field["id"].as_i64())
        .context("visible k1 target field absent")?;
    let service = S3::default()
        .bucket(bucket)
        .endpoint(object_endpoint)
        .region("us-east-1")
        .access_key_id(&identity.access_key_id)
        .secret_access_key(&identity.secret_access_key)
        .disable_config_load();
    let operator = Operator::new(service)?.finish();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let corruption = async {
        let manifest_list = read_private(&operator, bucket, &prefix, manifest_list).await?;
        let mut files = BTreeMap::new();
        for manifest in avro_records(&manifest_list)? {
            if integer(field(&manifest, "content")?)? != 0 {
                continue;
            }
            let manifest_path = string(field(&manifest, "manifest_path")?)?;
            let manifest_bytes = read_private(&operator, bucket, &prefix, manifest_path).await?;
            for entry in avro_records(&manifest_bytes)? {
                let status = integer(field(&entry, "status")?)?;
                ensure!(
                    matches!(status, 0..=2),
                    "invalid target manifest entry status"
                );
                if status == 2 {
                    continue;
                }
                let data = field(&entry, "data_file")?;
                ensure!(
                    integer(field(data, "content")?)? == 0,
                    "target baseline contains delete files"
                );
                ensure!(
                    string(field(data, "file_format")?)? == "PARQUET",
                    "target baseline is not Parquet"
                );
                let path = string(field(data, "file_path")?)?.to_owned();
                let size = usize::try_from(integer(field(data, "file_size_in_bytes")?)?)?;
                let rows = usize::try_from(integer(field(data, "record_count")?)?)?;
                ensure!(
                    size <= MAX_BYTES && rows <= MAX_ROWS,
                    "physical corruption target exceeds fixture bound"
                );
                ensure!(
                    files.insert(path, (size, rows)).is_none() && files.len() <= MAX_RECORDS,
                    "target data file inventory is duplicate or unbounded"
                );
            }
        }
        let mut replacement = None;
        for (path, (size, rows)) in files {
            let original = read_private(&operator, bucket, &prefix, &path).await?;
            ensure!(
                original.len() == size,
                "target data file differs from exact manifest size"
            );
            if let Some(mut changed) = rewrite_one_value(&original, rows, k1_id)? {
                ensure!(
                    replacement.is_none(),
                    "more than one target file contains the original visible value"
                );
                ensure!(
                    changed.len() <= size,
                    "replacement Parquet exceeds original manifest size"
                );
                pad_before_footer(&mut changed, size)?;
                verify_replacement(&original, &changed, rows, k1_id)?;
                replacement = Some((path, original, changed, rows));
            }
        }
        let (path, original, changed, rows) =
            replacement.context("exact published target has no k1=1 tuple")?;
        let (_, key) = private_key(bucket, &prefix, &path)?;
        operator
            .write(key, changed.clone())
            .await
            .context("overwrite only the runner-owned target Parquet")?;
        let read_back = read_private(&operator, bucket, &prefix, &path).await?;
        ensure!(
            read_back == changed && read_back.len() == original.len(),
            "physical corruption object read-back mismatch"
        );
        verify_replacement(&original, &read_back, rows, k1_id)?;
        Ok::<_, anyhow::Error>(format!(
            "physical corruption fault verified: schema and field IDs retained; one visible k1=1 replaced by k1=9; original object length retained; sha256_before={:x} sha256_after={:x}",
            Sha256::digest(&original),
            Sha256::digest(&read_back)
        ))
    };
    let evidence = runtime
        .block_on(async {
            tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), corruption).await
        })
        .context("bounded physical corruption fixture timed out")??;
    ensure!(
        bounded_metadata(&client, &url, deadline)? == metadata_before,
        "physical corruption changed target metadata, snapshot, manifests or documents"
    );
    Ok(evidence)
}

fn bounded_metadata(client: &Client, url: &str, deadline: Instant) -> Result<Value> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    ensure!(
        !remaining.is_zero(),
        "physical corruption metadata deadline expired"
    );
    let response = client
        .get(url)
        .timeout(remaining)
        .send()?
        .error_for_status()?;
    let mut bytes = Vec::new();
    response
        .take((MAX_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() <= MAX_BYTES,
        "target metadata exceeds fixture bound"
    );
    Ok(serde_json::from_slice(&bytes)?)
}

fn s3_path(path: &str) -> Result<(&str, &str)> {
    let (bucket, key) = path
        .strip_prefix("s3://")
        .context("private fixture object must be S3")?
        .split_once('/')
        .context("private fixture object lacks bucket/key")?;
    ensure!(
        !bucket.is_empty() && !key.split('/').any(|component| component == ".."),
        "invalid private S3 object path"
    );
    Ok((bucket, key))
}

fn private_key<'a>(bucket: &str, prefix: &str, path: &'a str) -> Result<(&'a str, &'a str)> {
    let (actual_bucket, key) = s3_path(path)?;
    ensure!(
        actual_bucket == bucket && key.starts_with(prefix),
        "physical corruption attempted an object outside its private target prefix"
    );
    Ok((actual_bucket, key))
}

async fn read_private(
    operator: &Operator,
    bucket: &str,
    prefix: &str,
    path: &str,
) -> Result<Vec<u8>> {
    let (_, key) = private_key(bucket, prefix, path)?;
    let size = operator.stat(key).await?.content_length();
    ensure!(
        size <= MAX_BYTES as u64,
        "private fixture object exceeds bound"
    );
    let bytes = operator.read(key).await?.to_bytes();
    ensure!(
        bytes.len() as u64 == size && bytes.len() <= MAX_BYTES,
        "private fixture object changed during read"
    );
    Ok(bytes.to_vec())
}

fn avro_records(bytes: &[u8]) -> Result<Vec<Avro>> {
    let mut records = Vec::new();
    for record in Reader::new(bytes)? {
        ensure!(
            records.len() < MAX_RECORDS,
            "Avro inventory exceeds fixture bound"
        );
        records.push(record?);
    }
    Ok(records)
}
fn unwrap(value: &Avro) -> &Avro {
    match value {
        Avro::Union(_, inner) => unwrap(inner),
        other => other,
    }
}
fn field<'a>(value: &'a Avro, name: &str) -> Result<&'a Avro> {
    let Avro::Record(fields) = unwrap(value) else {
        anyhow::bail!("Avro record expected");
    };
    fields
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| unwrap(value))
        .with_context(|| format!("Avro field {name} absent"))
}
fn integer(value: &Avro) -> Result<i64> {
    match unwrap(value) {
        Avro::Int(value) => Ok(i64::from(*value)),
        Avro::Long(value) => Ok(*value),
        _ => anyhow::bail!("Avro integer expected"),
    }
}
fn string(value: &Avro) -> Result<&str> {
    match unwrap(value) {
        Avro::String(value) => Ok(value),
        _ => anyhow::bail!("Avro string expected"),
    }
}

fn rewrite_one_value(
    original: &[u8],
    expected_rows: usize,
    field_id: i64,
) -> Result<Option<Vec<u8>>> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(original))?;
    let schema = Arc::clone(builder.schema());
    let metadata = Arc::clone(builder.metadata());
    let k1 = schema
        .fields()
        .iter()
        .position(|field| {
            field
                .metadata()
                .get("PARQUET:field_id")
                .and_then(|id| id.parse::<i64>().ok())
                == Some(field_id)
        })
        .context("Parquet visible field ID absent")?;
    let mut batches = Vec::new();
    let mut found = 0;
    let mut rows = 0;
    for batch in builder.with_batch_size(MAX_ROWS).build()? {
        let batch = batch?;
        rows += batch.num_rows();
        ensure!(rows <= MAX_ROWS, "Parquet rows exceed fixture bound");
        let values = batch
            .column(k1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .context("physical corruption requires an INT visible field")?;
        let changed = Int32Array::from_iter((0..values.len()).map(|row| {
            if values.is_null(row) {
                None
            } else if values.value(row) == 1 {
                found += 1;
                Some(9)
            } else {
                Some(values.value(row))
            }
        }));
        let mut columns = batch.columns().to_vec();
        columns[k1] = Arc::new(changed);
        batches.push(RecordBatch::try_new(Arc::clone(&schema), columns)?);
    }
    ensure!(
        rows == expected_rows,
        "manifest row count differs from actual Parquet rows"
    );
    if found == 0 {
        return Ok(None);
    }
    ensure!(
        found == 1,
        "physical corruption requires exactly one original visible tuple"
    );
    let file = metadata.file_metadata();
    let properties = WriterProperties::builder()
        .set_key_value_metadata(file.key_value_metadata().cloned())
        .set_created_by(
            file.created_by()
                .unwrap_or("physical-corruption-fixture")
                .to_owned(),
        )
        .build();
    let options = ArrowWriterOptions::new()
        .with_properties(properties)
        .with_skip_arrow_metadata(true);
    let mut changed = Vec::new();
    let mut writer = ArrowWriter::try_new_with_options(&mut changed, schema, options)?;
    for batch in batches {
        writer.write(&batch)?;
    }
    writer.close()?;
    Ok(Some(changed))
}

fn pad_before_footer(bytes: &mut Vec<u8>, original_size: usize) -> Result<()> {
    ensure!(
        bytes.len() >= 12 && bytes.ends_with(b"PAR1") && bytes.len() <= original_size,
        "invalid replacement Parquet footer"
    );
    let footer_length =
        u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into()?) as usize;
    let footer_start = bytes
        .len()
        .checked_sub(8 + footer_length)
        .context("invalid Parquet footer length")?;
    let padding = original_size - bytes.len();
    // Offsets point only to existing pages/indexes before this position.
    // Unreferenced bytes before FileMetaData do not move any such offset.
    bytes.splice(footer_start..footer_start, std::iter::repeat_n(0, padding));
    ensure!(
        bytes.len() == original_size,
        "Parquet padding did not retain original object size"
    );
    Ok(())
}

fn verify_replacement(
    original: &[u8],
    changed: &[u8],
    expected_rows: usize,
    field_id: i64,
) -> Result<()> {
    let original_builder =
        ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(original))?;
    let changed_builder =
        ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(changed))?;
    ensure!(
        original_builder.schema() == changed_builder.schema()
            && original_builder.metadata().file_metadata().schema()
                == changed_builder.metadata().file_metadata().schema(),
        "physical corruption changed Arrow schema or Parquet field IDs"
    );
    let k1 = changed_builder
        .schema()
        .fields()
        .iter()
        .position(|field| {
            field
                .metadata()
                .get("PARQUET:field_id")
                .and_then(|id| id.parse::<i64>().ok())
                == Some(field_id)
        })
        .context("replacement field ID absent")?;
    let original_batches = original_builder
        .with_batch_size(MAX_ROWS)
        .build()?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let changed_batches = changed_builder
        .with_batch_size(MAX_ROWS)
        .build()?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    ensure!(
        original_batches.len() == changed_batches.len(),
        "physical corruption changed batch count"
    );
    let mut rows = 0;
    let mut changes = 0;
    for (original, changed) in original_batches.iter().zip(&changed_batches) {
        ensure!(
            original.num_rows() == changed.num_rows(),
            "physical corruption changed row count"
        );
        rows += changed.num_rows();
        for column in 0..changed.num_columns() {
            if column != k1 {
                ensure!(
                    original.column(column) == changed.column(column),
                    "physical corruption changed another visible column"
                );
            }
        }
        let old = original
            .column(k1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .context("original INT absent")?;
        let new = changed
            .column(k1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .context("replacement INT absent")?;
        for row in 0..old.len() {
            ensure!(
                old.is_null(row) == new.is_null(row),
                "physical corruption changed nullness"
            );
            if !old.is_null(row) && old.value(row) != new.value(row) {
                ensure!(
                    old.value(row) == 1 && new.value(row) == 9,
                    "unexpected physical corruption value change"
                );
                changes += 1;
            }
        }
    }
    ensure!(
        rows == expected_rows && changes == 1,
        "physical corruption read-back did not prove one tuple substitution"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};

    #[test]
    fn physical_corruption_retains_size_schema_field_ids_and_row_count() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k1", DataType::Int32, false).with_metadata(
                std::collections::HashMap::from([("PARQUET:field_id".into(), "1".into())]),
            ),
            Field::new("v2", DataType::Int64, false).with_metadata(
                std::collections::HashMap::from([("PARQUET:field_id".into(), "2".into())]),
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![10, 20])),
            ],
        )
        .unwrap();
        let mut original = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut original, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let padded_size = original.len() + 512;
        pad_before_footer(&mut original, padded_size).unwrap();
        let mut replacement = rewrite_one_value(&original, 2, 1).unwrap().unwrap();
        pad_before_footer(&mut replacement, padded_size).unwrap();
        assert_eq!(replacement.len(), original.len());
        assert_ne!(replacement, original);
        verify_replacement(&original, &replacement, 2, 1).unwrap();
        assert!(rewrite_one_value(&replacement, 2, 1).unwrap().is_none());
    }

    #[test]
    fn physical_corruption_refuses_wrong_owner_and_manifest_size() {
        assert!(private_key("private", "ns/owned/", "s3://other/ns/owned/a.parquet").is_err());
        assert!(
            private_key(
                "private",
                "ns/owned/",
                "s3://private/ns/owned-other/a.parquet"
            )
            .is_err()
        );
        assert!(private_key("private", "ns/owned/", "s3://private/ns/owned/../a.parquet").is_err());
        assert!(pad_before_footer(&mut b"not-a-Parquet-file".to_vec(), 64).is_err());
    }
}
