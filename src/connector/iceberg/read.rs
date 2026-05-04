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

// Task 1 intentionally stages the shared Iceberg read-view contract before
// follow-up tasks wire it into catalog extraction and MV change planning.
// This helper is the target shared semantics; existing applicability helpers
// in catalog/registry.rs and changes.rs are migrated in follow-up tasks.

use std::collections::HashMap;

use crate::sql::catalog::IcebergColumnStats;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum IcebergReadDeleteFormat {
    Parquet,
    Puffin,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum IcebergReadDeleteKind {
    Position,
    Equality { equality_field_ids: Vec<i32> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IcebergReadDeleteFile {
    pub(crate) path: String,
    pub(crate) file_format: IcebergReadDeleteFormat,
    pub(crate) kind: IcebergReadDeleteKind,
    pub(crate) length: Option<i64>,
    pub(crate) content_offset: Option<i64>,
    pub(crate) content_size_in_bytes: Option<i64>,
    pub(crate) sequence_number: Option<i64>,
    pub(crate) partition_spec_id: Option<i32>,
    pub(crate) partition_key: Option<String>,
    pub(crate) referenced_data_file: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct IcebergReadFile {
    pub(crate) path: String,
    pub(crate) size: i64,
    pub(crate) record_count: Option<i64>,
    pub(crate) column_stats: Option<HashMap<String, IcebergColumnStats>>,
    pub(crate) partition_spec_id: Option<i32>,
    pub(crate) partition_key: Option<String>,
    pub(crate) first_row_id: Option<i64>,
    pub(crate) data_sequence_number: Option<i64>,
    pub(crate) deletes: Vec<IcebergReadDeleteFile>,
}

#[derive(Clone, Debug)]
pub(crate) struct IcebergReadSnapshot {
    pub(crate) snapshot_id: Option<i64>,
    pub(crate) files: Vec<IcebergReadFile>,
}

pub(crate) fn delete_applies_to_data_file(
    delete_file: &IcebergReadDeleteFile,
    data_file: &IcebergReadFile,
) -> bool {
    if let (Some(delete_sequence), Some(data_sequence)) =
        (delete_file.sequence_number, data_file.data_sequence_number)
        && delete_sequence <= data_sequence
    {
        return false;
    }

    if let Some(referenced) = delete_file.referenced_data_file.as_deref()
        && referenced != data_file.path
    {
        return false;
    }

    if let Some(delete_partition) = delete_file.partition_key.as_deref() {
        let Some(delete_spec_id) = delete_file.partition_spec_id else {
            return false;
        };
        let Some(data_spec_id) = data_file.partition_spec_id else {
            return false;
        };
        if delete_spec_id != data_spec_id {
            return false;
        }
        if data_file.partition_key.as_deref() != Some(delete_partition) {
            return false;
        }
    }

    true
}

pub(crate) fn iceberg_partition_key(partition: &iceberg::spec::Struct) -> Option<String> {
    if partition.fields().is_empty() {
        None
    } else {
        Some(format!("{partition:?}"))
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
}
