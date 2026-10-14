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

//! Initialization updates for a REST staged create.
//!
//! Design: ADR-0176 (docs/adr/ADR-0176-iceberg-rest-request-vocabulary-and-supported-server-floor.md)
//!
//! A staged-create response is not yet a table: its authoritative metadata
//! must travel with the first `assert-create` commit, ahead of the
//! transaction's own data updates. Only an empty initial table can be
//! represented exactly by REST metadata updates, so anything else is refused
//! rather than approximated. Nothing is derived from the original create
//! request.

use super::invalid;
use crate::iceberg::spec::{INITIAL_ROW_ID, TableMetadata};
use crate::iceberg::{Result, TableUpdate};

/// The sequence number of a table without snapshots. The format constant is
/// crate-private upstream.
const INITIAL_SEQUENCE_NUMBER: i64 = 0;

/// The partition high-water mark of an unpartitioned table: partition field
/// IDs start at 1000. The format constant is crate-private upstream.
const UNPARTITIONED_LAST_ASSIGNED_ID: i32 = 999;

/// Derive the updates that publish staged metadata exactly as the server
/// returned it.
///
/// `upgrade-format-version` is always present, also for v1: a REST server
/// selects the new table's format version from it and otherwise creates the
/// table at its default version. `add-schema` carries no `last-column-id`;
/// the server derives the high-water mark from the schema.
pub fn staged_create_updates(metadata: &TableMetadata) -> Result<Vec<TableUpdate>> {
    // Without snapshots and without a current snapshot no reference can
    // exist: metadata normalization admits only references to existing
    // snapshots, and no `main` branch without a current snapshot.
    let unrepresentable = [
        (
            metadata.last_sequence_number() != INITIAL_SEQUENCE_NUMBER,
            "a nonzero last sequence number",
        ),
        (
            metadata.current_snapshot_id().is_some() || metadata.snapshots().len() != 0,
            "snapshots",
        ),
        (!metadata.history().is_empty(), "a snapshot log"),
        (!metadata.metadata_log().is_empty(), "a metadata log"),
        (metadata.statistics_iter().len() != 0, "statistics files"),
        (
            metadata.partition_statistics_iter().len() != 0,
            "partition statistics files",
        ),
        (
            metadata.encryption_keys_iter().len() != 0,
            "encryption keys",
        ),
        (
            metadata.next_row_id() != INITIAL_ROW_ID,
            "an advanced next row ID",
        ),
        (
            metadata.schemas_iter().len() != 1,
            "other than exactly one schema",
        ),
        (
            metadata.partition_specs_iter().len() != 1,
            "other than exactly one partition spec",
        ),
        (
            metadata.sort_orders_iter().len() != 1,
            "other than exactly one sort order",
        ),
    ];
    if let Some((_, state)) = unrepresentable.iter().find(|(present, _)| *present) {
        return Err(invalid(format!(
            "Staged-create metadata contains {state}, which initialization updates cannot represent"
        )));
    }

    let partition_spec = metadata.default_partition_spec();
    let represented_last_partition_id = partition_spec
        .highest_field_id()
        .unwrap_or(UNPARTITIONED_LAST_ASSIGNED_ID);
    if metadata.last_partition_id() != represented_last_partition_id {
        return Err(invalid(format!(
            "Staged-create partition high-water mark {} cannot be represented by initialization updates, which imply {represented_last_partition_id}",
            metadata.last_partition_id()
        )));
    }

    Ok(vec![
        TableUpdate::AssignUuid {
            uuid: metadata.uuid(),
        },
        TableUpdate::UpgradeFormatVersion {
            format_version: metadata.format_version(),
        },
        TableUpdate::SetLocation {
            location: metadata.location().to_string(),
        },
        TableUpdate::AddSchema {
            schema: metadata.current_schema().as_ref().clone(),
        },
        TableUpdate::SetCurrentSchema {
            schema_id: metadata.current_schema_id(),
        },
        TableUpdate::AddSpec {
            spec: partition_spec.as_ref().clone().into_unbound(),
        },
        TableUpdate::SetDefaultSpec {
            spec_id: metadata.default_partition_spec_id(),
        },
        TableUpdate::AddSortOrder {
            sort_order: metadata.default_sort_order().as_ref().clone(),
        },
        TableUpdate::SetDefaultSortOrder {
            sort_order_id: metadata.default_sort_order_id(),
        },
        TableUpdate::SetProperties {
            updates: metadata.properties().clone(),
        },
    ])
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::iceberg::ErrorKind;
    use crate::iceberg::spec::{
        FormatVersion, NestedField, PrimitiveType, Schema, SortOrder, TableMetadataBuilder, Type,
        UnboundPartitionSpec,
    };

    fn staged_metadata(format_version: FormatVersion) -> TableMetadata {
        TableMetadataBuilder::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
            UnboundPartitionSpec::default(),
            SortOrder::unsorted_order(),
            "s3://warehouse/database/versioned".to_string(),
            format_version,
            HashMap::from([("owner".to_string(), "nova".to_string())]),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata
    }

    fn staged_metadata_json() -> serde_json::Value {
        serde_json::to_value(staged_metadata(FormatVersion::V2)).unwrap()
    }

    fn refused(metadata_json: serde_json::Value) -> crate::iceberg::Error {
        let metadata: TableMetadata = serde_json::from_value(metadata_json).unwrap();
        let error = staged_create_updates(&metadata).unwrap_err();
        assert_eq!(ErrorKind::DataInvalid, error.kind());
        error
    }

    #[test]
    fn every_format_version_selects_its_version_explicitly() {
        for expected in [FormatVersion::V1, FormatVersion::V2, FormatVersion::V3] {
            let updates = staged_create_updates(&staged_metadata(expected)).unwrap();
            assert_eq!(
                updates[1],
                TableUpdate::UpgradeFormatVersion {
                    format_version: expected,
                },
                "{expected:?}"
            );
        }
    }

    #[test]
    fn updates_restate_the_staged_metadata_in_a_fixed_order() {
        let metadata = staged_metadata(FormatVersion::V2);
        let updates = staged_create_updates(&metadata).unwrap();
        assert_eq!(
            updates,
            vec![
                TableUpdate::AssignUuid {
                    uuid: metadata.uuid()
                },
                TableUpdate::UpgradeFormatVersion {
                    format_version: FormatVersion::V2
                },
                TableUpdate::SetLocation {
                    location: "s3://warehouse/database/versioned".to_string()
                },
                TableUpdate::AddSchema {
                    schema: metadata.current_schema().as_ref().clone()
                },
                TableUpdate::SetCurrentSchema { schema_id: 0 },
                TableUpdate::AddSpec {
                    spec: metadata
                        .default_partition_spec()
                        .as_ref()
                        .clone()
                        .into_unbound()
                },
                TableUpdate::SetDefaultSpec { spec_id: 0 },
                TableUpdate::AddSortOrder {
                    sort_order: metadata.default_sort_order().as_ref().clone()
                },
                TableUpdate::SetDefaultSortOrder { sort_order_id: 0 },
                TableUpdate::SetProperties {
                    updates: metadata.properties().clone()
                },
            ]
        );
    }

    #[test]
    fn add_schema_carries_no_column_high_water_mark() {
        let updates = staged_create_updates(&staged_metadata(FormatVersion::V2)).unwrap();
        let add_schema = updates
            .iter()
            .find(|update| matches!(update, TableUpdate::AddSchema { .. }))
            .unwrap();
        let json = serde_json::to_value(add_schema).unwrap();
        assert_eq!(json["action"], "add-schema");
        assert!(json.get("last-column-id").is_none(), "{json}");
    }

    #[test]
    fn snapshot_state_is_refused() {
        let mut metadata_json = staged_metadata_json();
        let snapshot_id = 3055729675574597000i64;
        metadata_json["current-snapshot-id"] = serde_json::json!(snapshot_id);
        metadata_json["snapshots"] = serde_json::json!([{
            "snapshot-id": snapshot_id,
            "timestamp-ms": 1657810968051i64,
            "summary": {"operation": "append"},
            "manifest-list": "s3://warehouse/database/table/metadata/snap-proof.avro"
        }]);
        metadata_json["refs"] = serde_json::json!({
            "main": {"snapshot-id": snapshot_id, "type": "branch"}
        });
        assert!(refused(metadata_json).message().contains("snapshots"));
    }

    #[test]
    fn non_initial_state_is_refused() {
        type Mutation = Box<dyn Fn(&mut serde_json::Value)>;
        let cases: Vec<(&str, Mutation)> = vec![
            (
                "last sequence number",
                Box::new(|m| m["last-sequence-number"] = serde_json::json!(4)),
            ),
            (
                "snapshot log",
                Box::new(|m| {
                    m["snapshot-log"] = serde_json::json!([
                        {"snapshot-id": 1, "timestamp-ms": 1657810968051i64}
                    ])
                }),
            ),
            (
                "metadata log",
                Box::new(|m| {
                    m["metadata-log"] = serde_json::json!([{
                        "metadata-file": "s3://warehouse/database/versioned/metadata/v1.json",
                        "timestamp-ms": 1657810968051i64
                    }])
                }),
            ),
            (
                "statistics files",
                Box::new(|m| {
                    m["statistics"] = serde_json::json!([{
                        "snapshot-id": 1,
                        "statistics-path": "s3://warehouse/stats.puffin",
                        "file-size-in-bytes": 10,
                        "file-footer-size-in-bytes": 5,
                        "blob-metadata": []
                    }])
                }),
            ),
            (
                "partition statistics files",
                Box::new(|m| {
                    m["partition-statistics"] = serde_json::json!([{
                        "snapshot-id": 1,
                        "statistics-path": "s3://warehouse/partition-stats.parquet",
                        "file-size-in-bytes": 10
                    }])
                }),
            ),
            (
                "schema",
                Box::new(|m| {
                    let mut second = m["schemas"][0].clone();
                    second["schema-id"] = serde_json::json!(1);
                    m["schemas"].as_array_mut().unwrap().push(second);
                }),
            ),
            (
                "partition spec",
                Box::new(|m| {
                    m["partition-specs"]
                        .as_array_mut()
                        .unwrap()
                        .push(serde_json::json!({"spec-id": 1, "fields": []}));
                }),
            ),
            (
                "sort order",
                Box::new(|m| {
                    m["sort-orders"]
                        .as_array_mut()
                        .unwrap()
                        .push(serde_json::json!({
                            "order-id": 1,
                            "fields": [{
                                "transform": "identity",
                                "source-id": 1,
                                "direction": "asc",
                                "null-order": "nulls-first"
                            }]
                        }));
                }),
            ),
        ];
        for (state, mutate) in cases {
            let mut metadata_json = staged_metadata_json();
            mutate(&mut metadata_json);
            let error = refused(metadata_json);
            assert!(error.message().contains(state), "{state}: {error}");
        }
    }

    #[test]
    fn encryption_keys_and_advanced_row_ids_are_refused() {
        let mut metadata_json = serde_json::to_value(staged_metadata(FormatVersion::V3)).unwrap();
        metadata_json["next-row-id"] = serde_json::json!(7);
        assert!(refused(metadata_json).message().contains("next row ID"));

        let mut metadata_json = serde_json::to_value(staged_metadata(FormatVersion::V3)).unwrap();
        metadata_json["encryption-keys"] = serde_json::json!([{
            "key-id": "k1",
            "encrypted-key-metadata": "AAAA",
            "encrypted-by-id": "master"
        }]);
        assert!(refused(metadata_json).message().contains("encryption keys"));
    }

    #[test]
    fn an_unrepresented_partition_high_water_mark_is_refused() {
        let mut metadata_json = staged_metadata_json();
        metadata_json["last-partition-id"] = serde_json::json!(1000);
        assert!(
            refused(metadata_json)
                .message()
                .contains("partition high-water mark")
        );
    }
}
