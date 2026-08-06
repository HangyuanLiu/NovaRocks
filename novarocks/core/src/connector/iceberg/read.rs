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

#![allow(dead_code)]

// The shared Iceberg read-view contract is now used by catalog extraction.
// Follow-up tasks will migrate MV change planning to the same read semantics.

pub(crate) use novarocks_connector_iceberg::read_model::*;

pub(crate) fn build_read_snapshot_at(
    table: &novarocks_connector_iceberg::iceberg::table::Table,
    snapshot_id: i64,
) -> Result<IcebergReadSnapshot, String> {
    crate::connector::iceberg::catalog::registry::block_on_iceberg(
        novarocks_connector_iceberg::read_snapshot::build_read_snapshot_at(table, snapshot_id),
    )?
}

pub(crate) fn build_read_snapshot(
    table: &novarocks_connector_iceberg::iceberg::table::Table,
) -> Result<IcebergReadSnapshot, String> {
    match table.metadata().current_snapshot() {
        Some(snapshot) => build_read_snapshot_at(table, snapshot.snapshot_id()),
        None => Ok(IcebergReadSnapshot {
            snapshot_id: None,
            files: Vec::new(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data_file(
        seq: Option<i64>,
        spec_id: Option<i32>,
        partition_key: Option<&str>,
    ) -> IcebergReadFile {
        IcebergReadFile {
            path: "s3://bucket/table/data-1.parquet".to_string(),
            size: 10,
            record_count: Some(1),
            column_stats: None,
            partition_spec_id: spec_id,
            partition_key: partition_key.map(str::to_string),
            partition_values: None,
            manifest_path: None,
            first_row_id: Some(0),
            data_sequence_number: seq,
            deletes: Vec::new(),
        }
    }

    fn equality_delete(
        seq: Option<i64>,
        spec_id: Option<i32>,
        partition_key: Option<&str>,
    ) -> IcebergReadDeleteFile {
        IcebergReadDeleteFile {
            path: "s3://bucket/table/delete-1.parquet".to_string(),
            file_format: IcebergReadDeleteFormat::Parquet,
            kind: IcebergReadDeleteKind::Equality {
                equality_field_ids: vec![3],
            },
            length: Some(10),
            content_offset: None,
            content_size_in_bytes: None,
            sequence_number: seq,
            partition_spec_id: spec_id,
            partition_key: partition_key.map(str::to_string),
            referenced_data_file: None,
        }
    }

    #[test]
    fn delete_with_older_or_equal_sequence_does_not_apply() {
        let data = data_file(Some(7), None, None);
        let older = equality_delete(Some(6), None, None);
        let equal = equality_delete(Some(7), None, None);

        assert!(!delete_applies_to_data_file(&older, &data));
        assert!(!delete_applies_to_data_file(&equal, &data));
    }

    #[test]
    fn unpartitioned_newer_equality_delete_applies_globally() {
        let data = data_file(Some(7), Some(2), Some("city=A"));
        let delete = equality_delete(Some(8), None, None);

        assert!(delete_applies_to_data_file(&delete, &data));
    }

    #[test]
    fn partitioned_equality_delete_requires_matching_spec_and_partition() {
        let data = data_file(Some(7), Some(2), Some("city=A"));
        let same = equality_delete(Some(8), Some(2), Some("city=A"));
        let different_spec = equality_delete(Some(8), Some(3), Some("city=A"));
        let different_partition = equality_delete(Some(8), Some(2), Some("city=B"));

        assert!(delete_applies_to_data_file(&same, &data));
        assert!(!delete_applies_to_data_file(&different_spec, &data));
        assert!(!delete_applies_to_data_file(&different_partition, &data));
    }

    #[test]
    fn partitioned_equality_delete_requires_spec_id_on_both_sides() {
        let data_without_spec = data_file(Some(7), None, Some("city=A"));
        let data_with_spec = data_file(Some(7), Some(2), Some("city=A"));
        let delete_without_spec = equality_delete(Some(8), None, Some("city=A"));
        let delete_with_spec = equality_delete(Some(8), Some(2), Some("city=A"));

        assert!(!delete_applies_to_data_file(
            &delete_with_spec,
            &data_without_spec
        ));
        assert!(!delete_applies_to_data_file(
            &delete_without_spec,
            &data_with_spec
        ));
    }

    #[test]
    fn referenced_position_delete_requires_matching_data_file() {
        let data = data_file(Some(7), None, None);
        let delete = IcebergReadDeleteFile {
            referenced_data_file: Some(data.path.clone()),
            kind: IcebergReadDeleteKind::Position,
            sequence_number: Some(8),
            ..equality_delete(Some(8), None, None)
        };
        let other = IcebergReadDeleteFile {
            referenced_data_file: Some("s3://bucket/table/other.parquet".to_string()),
            ..delete.clone()
        };

        assert!(delete_applies_to_data_file(&delete, &data));
        assert!(!delete_applies_to_data_file(&other, &data));
    }

    #[test]
    fn read_view_attaches_only_applicable_deletes() {
        let mut data = data_file(Some(5), Some(1), Some("city=A"));
        let applicable = equality_delete(Some(6), Some(1), Some("city=A"));
        let too_old = equality_delete(Some(5), Some(1), Some("city=A"));
        let wrong_partition = equality_delete(Some(6), Some(1), Some("city=B"));

        attach_applicable_deletes(&mut data, &[applicable.clone(), too_old, wrong_partition]);

        assert_eq!(data.deletes, vec![applicable]);
    }

    #[test]
    fn data_files_matching_delete_returns_only_applicable_files() {
        let a = data_file(Some(1), Some(1), Some("city=A"));
        let b = data_file(Some(1), Some(1), Some("city=B"));
        let snapshot = IcebergReadSnapshot {
            snapshot_id: Some(10),
            files: vec![a.clone(), b],
        };
        let delete = equality_delete(Some(2), Some(1), Some("city=A"));

        let files = data_files_matching_delete(&snapshot, &delete);

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, a.path);
    }

    #[test]
    fn build_read_snapshot_at_returns_error_for_unknown_snapshot_id() {
        use novarocks_connector_iceberg::iceberg::spec::{
            FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder,
            TableMetadataBuilder, Type,
        };
        use novarocks_connector_iceberg::iceberg::table::Table;
        use novarocks_connector_iceberg::iceberg::{NamespaceIdent, TableIdent};
        use std::collections::HashMap;

        // Build a minimal TableMetadata with no snapshots.
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();

        let metadata = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            "file:///novarocks-test/table".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;

        let file_io = crate::connector::iceberg::fs_io::build_file_io_for_location(
            "file:///novarocks-test/table",
            None,
        );
        let ident = TableIdent::new(NamespaceIdent::new("db".to_string()), "table".to_string());

        let table = Table::builder()
            .file_io(file_io)
            .metadata(metadata)
            .identifier(ident)
            .build()
            .unwrap();

        let result = build_read_snapshot_at(&table, 999_i64);
        assert!(result.is_err());
        let msg = result.unwrap_err();
        assert!(
            msg.contains("snapshot 999 not found"),
            "unexpected error: {msg}"
        );
    }
}
