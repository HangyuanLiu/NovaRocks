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

//! Bounded diagnostics for one runner-owned target candidate fixture.

use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::PathBuf;

use crate::iceberg::spec::TableMetadata;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const MAX_ARM_BYTES: u64 = 8192;
const MAX_RECORD_BYTES: usize = 128 * 1024;
const MAX_FILES: usize = 64;

pub(crate) fn root() -> Option<PathBuf> {
    std::env::var_os("NOVAROCKS_SQL_TEST_QUERY_LIFECYCLE_FAULT_DIR").map(PathBuf::from)
}

pub(crate) fn armed(metadata: &TableMetadata) -> Result<bool, String> {
    let Some(root) = root() else { return Ok(false) };
    match fs::symlink_metadata(root.join(format!(
        "mv-target-candidate-trace-{}.json",
        metadata.uuid()
    ))) {
        Ok(stat)
            if stat.is_file() && !stat.file_type().is_symlink() && stat.len() <= MAX_ARM_BYTES =>
        {
            Ok(true)
        }
        Ok(_) => Err("target candidate trace arm must be a bounded regular file".into()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(format!("inspect target candidate trace arm: {error}")),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TraceArm {
    token: Uuid,
    table_uuid: Uuid,
    snapshot_id: i64,
    table_location: String,
}

#[derive(Serialize)]
struct CandidateRecord<'a> {
    token: Uuid,
    table_uuid: Uuid,
    snapshot_id: i64,
    phase: &'a str,
    paths: BTreeSet<String>,
}

/// Record the set actually passed to a consumer. An unarmed execution has no trace.
/// Armed records refuse overflow and replay rather than claim a truncated set is complete.
pub(crate) fn record(
    metadata: &TableMetadata,
    snapshot_id: i64,
    phase: &str,
    paths: impl IntoIterator<Item = String>,
) -> Result<(), String> {
    let Some(root) = root() else { return Ok(()) };
    record_at_root(metadata, snapshot_id, phase, paths, &root)
}

fn record_at_root(
    metadata: &TableMetadata,
    snapshot_id: i64,
    phase: &str,
    paths: impl IntoIterator<Item = String>,
    root: &std::path::Path,
) -> Result<(), String> {
    let arm_path = root.join(format!(
        "mv-target-candidate-trace-{}.json",
        metadata.uuid()
    ));
    let stat = match fs::symlink_metadata(&arm_path) {
        Ok(stat) => stat,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(format!("inspect target candidate trace arm: {error}")),
    };
    if !stat.is_file() || stat.file_type().is_symlink() || stat.len() > MAX_ARM_BYTES {
        return Err("target candidate trace arm must be a bounded regular file".into());
    }
    let mut bytes = Vec::new();
    fs::File::open(&arm_path)
        .map_err(|e| e.to_string())?
        .take(MAX_ARM_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| e.to_string())?;
    if bytes.len() as u64 > MAX_ARM_BYTES {
        return Err("target candidate trace arm exceeds its byte limit".into());
    }
    let arm: TraceArm =
        serde_json::from_slice(&bytes).map_err(|e| format!("decode candidate trace arm: {e}"))?;
    validate_arm(&arm, metadata, snapshot_id, phase)?;
    let mut selected = BTreeSet::new();
    let mut path_bytes = 0usize;
    for path in paths {
        path_bytes = path_bytes
            .checked_add(path.len())
            .ok_or("candidate trace path bytes overflow")?;
        if path.is_empty()
            || path.len() > 4096
            || path_bytes > MAX_RECORD_BYTES / 2
            || !selected.insert(path)
            || selected.len() > MAX_FILES
        {
            return Err(
                "target candidate trace requires a complete bounded unique file set".into(),
            );
        }
    }
    let record = CandidateRecord {
        token: arm.token,
        table_uuid: arm.table_uuid,
        snapshot_id,
        phase,
        paths: selected,
    };
    let encoded = serde_json::to_vec(&record).map_err(|e| e.to_string())?;
    if encoded.len() > MAX_RECORD_BYTES {
        return Err("target candidate trace record exceeds its byte limit".into());
    }
    let output = root.join(format!(
        "mv-target-candidate-{}-{}-{phase}.json",
        arm.table_uuid, arm.token
    ));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)
        .map_err(|e| format!("create exact candidate trace (replay is forbidden): {e}"))?;
    file.write_all(&encoded)
        .map_err(|e| format!("write complete candidate trace: {e}"))
}

fn validate_arm(
    arm: &TraceArm,
    metadata: &TableMetadata,
    snapshot_id: i64,
    phase: &str,
) -> Result<(), String> {
    if arm.token.is_nil()
        || arm.table_uuid != metadata.uuid()
        || arm.snapshot_id != snapshot_id
        || metadata.current_snapshot_id() != Some(snapshot_id)
        || arm.table_location != metadata.location()
        || !matches!(phase, "writer-frozen" | "reader-pinned")
    {
        return Err("target candidate trace does not bind the exact consumer baseline".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::spec::{
        FormatVersion, NestedField, Operation, PartitionSpec, PrimitiveType, Schema, Snapshot,
        SnapshotReference, SnapshotRetention, SortOrder, Summary, TableMetadataBuilder, Type,
    };
    use std::collections::HashMap;
    use std::sync::Arc;

    fn metadata() -> TableMetadata {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap();
        let metadata = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "memory:///private-candidate".into(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let snapshot = Snapshot::builder()
            .with_snapshot_id(7)
            .with_sequence_number(1)
            .with_timestamp_ms(metadata.last_updated_ms())
            .with_manifest_list("memory:///manifest")
            .with_schema_id(0)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build();
        metadata
            .into_builder(None)
            .add_snapshot(snapshot)
            .unwrap()
            .set_ref(
                "main",
                SnapshotReference {
                    snapshot_id: 7,
                    retention: SnapshotRetention::Branch {
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                },
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata
    }

    fn arm(root: &std::path::Path, metadata: &TableMetadata, token: Uuid, snapshot: i64) {
        let bytes = serde_json::to_vec(
            &serde_json::json!({"token":token, "table_uuid":metadata.uuid(),
            "snapshot_id":snapshot, "table_location":metadata.location()}),
        )
        .unwrap();
        fs::write(
            root.join(format!(
                "mv-target-candidate-trace-{}.json",
                metadata.uuid()
            )),
            bytes,
        )
        .unwrap();
    }

    #[test]
    fn candidate_fixture_records_complete_sets_and_refuses_replay() {
        let root = tempfile::tempdir().unwrap();
        let metadata = metadata();
        let token = Uuid::new_v4();
        arm(root.path(), &metadata, token, 7);
        record_at_root(
            &metadata,
            7,
            "writer-frozen",
            ["b".into(), "a".into()],
            root.path(),
        )
        .unwrap();
        let path = root.path().join(format!(
            "mv-target-candidate-{}-{}-writer-frozen.json",
            metadata.uuid(),
            token
        ));
        let bytes = fs::read(&path).unwrap();
        let record: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(record["paths"], serde_json::json!(["a", "b"]));
        assert!(record_at_root(&metadata, 7, "writer-frozen", ["c".into()], root.path()).is_err());
        assert_eq!(fs::read(path).unwrap(), bytes);
    }

    #[test]
    fn candidate_fixture_never_reports_truncated_or_wrong_snapshot_sets() {
        let root = tempfile::tempdir().unwrap();
        let metadata = metadata();
        let token = Uuid::new_v4();
        arm(root.path(), &metadata, token, 7);
        assert!(
            record_at_root(
                &metadata,
                7,
                "reader-pinned",
                (0..65).map(|i| i.to_string()),
                root.path()
            )
            .is_err()
        );
        assert!(
            record_at_root(
                &metadata,
                7,
                "reader-pinned",
                ["a".into(), "a".into()],
                root.path()
            )
            .is_err()
        );
        arm(root.path(), &metadata, token, 8);
        assert!(record_at_root(&metadata, 7, "reader-pinned", ["a".into()], root.path()).is_err());
        let path = root.path().join(format!(
            "mv-target-candidate-{}-{}-reader-pinned.json",
            metadata.uuid(),
            token
        ));
        assert!(!path.exists());
    }
}
