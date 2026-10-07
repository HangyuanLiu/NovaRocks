// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Bounded, runner-owned seed for a real Equality artifact in one initial publication.
//! This module is compiled only with debug assertions. It never submits a catalog mutation.

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

use crate::commit::abort::AbortLog;
use crate::commit::{CommitOutcome, WrittenFile};
use crate::iceberg::io::FileIO;
use crate::iceberg::spec::{
    DataContentType, DataFileFormat, FormatVersion, Literal, PrimitiveType, Struct, Transform, Type,
};
use crate::iceberg::table::Table;
use crate::iceberg::transaction::Transaction;
use serde::Deserialize;
use uuid::Uuid;

const MAX_SEED_BYTES: u64 = 16 * 1024;
const MAX_ARTIFACT_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SeedRecord {
    token: Uuid,
    table_uuid: Uuid,
    namespace: String,
    table: String,
    table_location: String,
    schema_id: i32,
    spec_id: i32,
    partition_source_id: i32,
    partition_value: i64,
    equality_field_id: i32,
    no_op_value: i64,
    artifact_path: String,
    artifact_size: u64,
    artifact_record_count: u64,
}

pub(crate) struct FixtureEqualitySeed {
    record: SeedRecord,
}

impl FixtureEqualitySeed {
    /// Only a named private target's arm is observed. No arm means ordinary execution.
    /// A successfully claimed token can never be armed again, including after a failed write.
    pub(crate) fn claim(initial: &Table, root: &Path) -> Result<Option<Self>, String> {
        let metadata = initial.metadata();
        let arm = root.join(format!("mv-target-equality-seed-{}.json", metadata.uuid()));
        let file_metadata = match fs::symlink_metadata(&arm) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(format!("read MV Equality seed arm: {error}")),
        };
        if !file_metadata.file_type().is_file() || file_metadata.len() > MAX_SEED_BYTES {
            return Err("MV Equality seed must be a bounded regular file".into());
        }
        let mut bytes = Vec::new();
        fs::File::open(&arm)
            .map_err(|error| format!("open MV Equality seed arm: {error}"))?
            .take(MAX_SEED_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|error| format!("read MV Equality seed: {error}"))?;
        if bytes.len() as u64 > MAX_SEED_BYTES {
            return Err("MV Equality seed exceeds its byte limit".into());
        }
        let record: SeedRecord = serde_json::from_slice(&bytes)
            .map_err(|error| format!("decode MV Equality seed: {error}"))?;
        validate_record(initial, &record)?;
        let consumed = root.join(format!(
            "mv-target-equality-seed-{}-{}.consumed",
            metadata.uuid(),
            record.token
        ));
        let mut marker = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&consumed)
            .map_err(|error| {
                format!("claim MV Equality seed token (replay is forbidden): {error}")
            })?;
        marker
            .write_all(&bytes)
            .map_err(|error| format!("record MV Equality seed claim: {error}"))?;
        fs::remove_file(&arm).map_err(|error| format!("retire MV Equality seed arm: {error}"))?;
        Ok(Some(Self { record }))
    }

    pub(crate) fn artifact_path(&self) -> &str {
        &self.record.artifact_path
    }

    pub(crate) fn token(&self) -> Uuid {
        self.record.token
    }

    /// Add a second snapshot action to the same transaction. Original pending
    /// properties resolve CommitOutput to B2; already resolved B1 properties are refused.
    pub(crate) async fn stage(
        &self,
        transaction: Transaction,
        mut data_outcome: CommitOutcome,
        file_io: FileIO,
        abort_handle: Arc<AbortLog>,
        original_pending_properties: &BTreeMap<String, String>,
    ) -> Result<(Transaction, CommitOutcome), String> {
        if !original_pending_properties
            .contains_key(crate::document_storage::publication::PENDING_DOCUMENT_MANIFEST_PROPERTY)
        {
            return Err(
                "MV Equality seed requires the original pending publication documents".into(),
            );
        }
        let metadata = transaction.staged_table().metadata();
        if metadata.uuid() != self.record.table_uuid
            || metadata.current_schema_id() != self.record.schema_id
            || metadata.default_partition_spec_id() != self.record.spec_id
            || metadata.location() != self.record.table_location
            || metadata.current_snapshot_id() != Some(data_outcome.new_snapshot_id)
            || metadata.format_version() != FormatVersion::V3
        {
            return Err("MV Equality seed lost its exact transaction identity".into());
        }
        abort_handle.record_data_file(self.record.artifact_path.clone());
        let written = WrittenFile {
            path: self.record.artifact_path.clone(),
            format: DataFileFormat::Parquet,
            content: DataContentType::EqualityDeletes,
            partition_values: Struct::from_iter([Some(Literal::long(self.record.partition_value))]),
            partition_spec_id: self.record.spec_id,
            record_count: self.record.artifact_record_count,
            file_size_in_bytes: self.record.artifact_size,
            split_offsets: Vec::new(),
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            key_metadata: None,
            referenced_data_file: None,
            equality_ids: Some(vec![self.record.equality_field_id]),
            first_row_id: None,
            content_offset: None,
            content_size_in_bytes: None,
            cardinality: None,
        };
        let (transaction, seeded) = crate::commit::row_delta::stage_fixture_equality(
            transaction,
            written,
            file_io,
            abort_handle,
            original_pending_properties.clone(),
        )
        .await?;
        data_outcome.new_snapshot_id = seeded.new_snapshot_id;
        data_outcome
            .written_manifest_paths
            .extend(seeded.written_manifest_paths);
        Ok((transaction, data_outcome))
    }
}

fn validate_record(initial: &Table, record: &SeedRecord) -> Result<(), String> {
    let metadata = initial.metadata();
    if metadata.format_version() != FormatVersion::V3 || metadata.current_snapshot_id().is_some() {
        return Err("MV Equality seed is restricted to an initial format-v3 publication".into());
    }
    if record.token.is_nil()
        || record.table_uuid != metadata.uuid()
        || record.table_location != metadata.location()
        || record.schema_id != metadata.current_schema_id()
        || record.spec_id != metadata.default_partition_spec_id()
        || initial.identifier().namespace().as_ref().as_slice() != [record.namespace.as_str()]
        || initial.identifier().name() != record.table
        || !record.namespace.starts_with("ns_")
        || !record
            .namespace
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
        || record.table != "candidate_mv"
    {
        return Err("MV Equality seed does not bind the exact private target".into());
    }
    let fields = metadata.default_partition_spec().fields();
    let schema = metadata.current_schema();
    let partition_source = schema
        .field_by_id(record.partition_source_id)
        .ok_or("MV Equality seed partition source is absent")?;
    let equality_field = schema
        .field_by_id(record.equality_field_id)
        .ok_or("MV Equality seed Equality field is absent")?;
    if fields.len() != 1
        || fields[0].source_id != record.partition_source_id
        || fields[0].transform != Transform::Identity
        || partition_source.name != "p"
        || *partition_source.field_type != Type::Primitive(PrimitiveType::Long)
        || equality_field.name != "id"
        || *equality_field.field_type != Type::Primitive(PrimitiveType::Long)
        || record.partition_value != 2
        || record.no_op_value != -7
        || record.artifact_record_count != 1
        || record.artifact_size == 0
        || record.artifact_size > MAX_ARTIFACT_BYTES
    {
        return Err("MV Equality seed violates the narrow fixture artifact contract".into());
    }
    let expected_path = format!(
        "{}/data/uea7b3-seed-{}/equality.parquet",
        metadata.location().trim_end_matches('/'),
        record.token
    );
    if record.artifact_path != expected_path {
        return Err("MV Equality seed artifact is outside its exact owned prefix".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::TableIdent;
    use crate::iceberg::spec::{
        NestedField, PartitionSpec, Schema, SortOrder, TableMetadataBuilder,
    };

    fn initial_table(format: FormatVersion) -> Table {
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::required(
                    2,
                    "p",
                    Type::Primitive(PrimitiveType::Long),
                )),
            ])
            .build()
            .unwrap();
        let spec = PartitionSpec::builder(Arc::new(schema.clone()))
            .add_partition_field("p", "p", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();
        let metadata = TableMetadataBuilder::new(
            schema,
            spec,
            SortOrder::unsorted_order(),
            "memory:///ns_seed/candidate_mv".into(),
            format,
            HashMap::from([("write.row-lineage".into(), "true".into())]),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        Table::builder()
            .identifier(TableIdent::from_strs(["ns_seed", "candidate_mv"]).unwrap())
            .file_io(FileIO::new_with_memory())
            .metadata(metadata)
            .build()
            .unwrap()
    }

    fn arm(table: &Table, token: Uuid) -> serde_json::Value {
        let metadata = table.metadata();
        serde_json::json!({
            "token":token, "table_uuid":metadata.uuid(), "namespace":"ns_seed", "table":"candidate_mv",
            "table_location":metadata.location(), "schema_id":metadata.current_schema_id(),
            "spec_id":metadata.default_partition_spec_id(),
            "partition_source_id":metadata.current_schema().field_by_name("p").unwrap().id,
            "partition_value":2, "equality_field_id":metadata.current_schema().field_by_name("id").unwrap().id,
            "no_op_value":-7,
            "artifact_path":format!("{}/data/uea7b3-seed-{token}/equality.parquet",metadata.location()),
            "artifact_size":512, "artifact_record_count":1
        })
    }

    fn write_arm(root: &Path, table: &Table, record: &serde_json::Value) -> std::path::PathBuf {
        let path = root.join(format!(
            "mv-target-equality-seed-{}.json",
            table.metadata().uuid()
        ));
        fs::write(&path, serde_json::to_vec(record).unwrap()).unwrap();
        path
    }

    #[test]
    fn fixture_seed_claim_is_one_time_and_absent_arm_is_ordinary_execution() {
        let root = tempfile::tempdir().unwrap();
        let table = initial_table(FormatVersion::V3);
        assert!(
            FixtureEqualitySeed::claim(&table, root.path())
                .unwrap()
                .is_none()
        );
        let token = Uuid::new_v4();
        let record = arm(&table, token);
        let path = write_arm(root.path(), &table, &record);
        let claimed = FixtureEqualitySeed::claim(&table, root.path())
            .unwrap()
            .unwrap();
        assert_eq!(claimed.token(), token);
        assert_eq!(
            claimed.artifact_path(),
            record["artifact_path"].as_str().unwrap()
        );
        assert!(!path.exists());
        assert!(
            FixtureEqualitySeed::claim(&table, root.path())
                .unwrap()
                .is_none()
        );
        write_arm(root.path(), &table, &record);
        assert!(FixtureEqualitySeed::claim(&table, root.path()).is_err());
    }

    #[test]
    fn fixture_seed_refuses_wrong_identity_prefix_and_noninitial_format() {
        let root = tempfile::tempdir().unwrap();
        let table = initial_table(FormatVersion::V3);
        let token = Uuid::new_v4();
        for (field, value) in [
            ("table_uuid", serde_json::json!(Uuid::new_v4())),
            ("schema_id", serde_json::json!(999)),
            ("spec_id", serde_json::json!(999)),
            ("table_location", serde_json::json!("memory:///foreign")),
            ("namespace", serde_json::json!("ns_foreign")),
            (
                "artifact_path",
                serde_json::json!("memory:///foreign/equality.parquet"),
            ),
            ("no_op_value", serde_json::json!(1)),
            ("partition_value", serde_json::json!(1)),
            ("token", serde_json::json!(Uuid::nil())),
        ] {
            let mut record = arm(&table, token);
            record[field] = value;
            let path = write_arm(root.path(), &table, &record);
            assert!(
                FixtureEqualitySeed::claim(&table, root.path()).is_err(),
                "accepted wrong {field}"
            );
            assert!(path.exists(), "invalid arm was consumed");
        }
        let v2 = initial_table(FormatVersion::V2);
        write_arm(root.path(), &v2, &arm(&v2, token));
        assert!(FixtureEqualitySeed::claim(&v2, root.path()).is_err());
    }

    #[test]
    fn fixture_seed_refuses_oversized_and_unknown_contract_fields() {
        let root = tempfile::tempdir().unwrap();
        let table = initial_table(FormatVersion::V3);
        let mut record = arm(&table, Uuid::new_v4());
        record["unexpected"] = serde_json::json!(true);
        let path = write_arm(root.path(), &table, &record);
        assert!(FixtureEqualitySeed::claim(&table, root.path()).is_err());
        fs::write(&path, vec![b' '; MAX_SEED_BYTES as usize + 1]).unwrap();
        assert!(FixtureEqualitySeed::claim(&table, root.path()).is_err());
    }
}
