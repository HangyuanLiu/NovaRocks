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

use std::collections::HashMap;

use crate::iceberg::spec::Struct;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IcebergFileFormat {
    Parquet,
    Puffin,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IcebergFileContent {
    Data,
    PositionDeletes,
    EqualityDeletes,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergDeleteFileSpec {
    pub path: String,
    pub file_format: IcebergFileFormat,
    pub file_content: IcebergFileContent,
    pub length: Option<u64>,
    pub content_offset: Option<i64>,
    pub content_size_in_bytes: Option<i64>,
}

impl IcebergDeleteFileSpec {
    pub fn parquet_position_delete(path: String, length: Option<u64>) -> Self {
        Self {
            path,
            file_format: IcebergFileFormat::Parquet,
            file_content: IcebergFileContent::PositionDeletes,
            length,
            content_offset: None,
            content_size_in_bytes: None,
        }
    }

    pub fn puffin_position_delete(
        path: String,
        length: Option<u64>,
        content_offset: i64,
        content_size_in_bytes: i64,
    ) -> Self {
        Self {
            path,
            file_format: IcebergFileFormat::Puffin,
            file_content: IcebergFileContent::PositionDeletes,
            length,
            content_offset: Some(content_offset),
            content_size_in_bytes: Some(content_size_in_bytes),
        }
    }
}

/// The partition facts needed to write a position delete against one data file.
///
/// These are extracted from the Iceberg manifest and deliberately retain the
/// provider's native partition value representation.
pub struct ReferencedDataFilePartition {
    pub partition_spec_id: i32,
    pub partition_values: Struct,
}

pub type ReferencedDataFilePartitions = HashMap<String, ReferencedDataFilePartition>;

pub fn insert_referenced_data_file_partition(
    partitions: &mut ReferencedDataFilePartitions,
    path: String,
    partition: ReferencedDataFilePartition,
) -> Result<(), String> {
    match partitions.entry(path) {
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(partition);
        }
        std::collections::hash_map::Entry::Occupied(entry) => {
            let existing = entry.get();
            if existing.partition_spec_id == partition.partition_spec_id
                && existing.partition_values == partition.partition_values
            {
                return Ok(());
            }
            return Err(format!(
                "iceberg data file `{}` has conflicting partition metadata: old partition spec id {}, new partition spec id {}",
                entry.key(),
                existing.partition_spec_id,
                partition.partition_spec_id
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::spec::Struct;

    #[test]
    fn parquet_position_delete_sets_required_content() {
        let spec = IcebergDeleteFileSpec::parquet_position_delete(
            "/tmp/delete.parquet".to_string(),
            Some(99),
        );

        assert_eq!(spec.file_format, IcebergFileFormat::Parquet);
        assert_eq!(spec.file_content, IcebergFileContent::PositionDeletes);
        assert_eq!(spec.length, Some(99));
        assert_eq!(spec.content_offset, None);
        assert_eq!(spec.content_size_in_bytes, None);
    }

    #[test]
    fn puffin_position_delete_carries_byte_range() {
        let spec = IcebergDeleteFileSpec::puffin_position_delete(
            "/tmp/delete.puffin".to_string(),
            Some(512),
            12,
            34,
        );

        assert_eq!(spec.file_format, IcebergFileFormat::Puffin);
        assert_eq!(spec.file_content, IcebergFileContent::PositionDeletes);
        assert_eq!(spec.length, Some(512));
        assert_eq!(spec.content_offset, Some(12));
        assert_eq!(spec.content_size_in_bytes, Some(34));
    }

    #[test]
    fn referenced_data_file_partition_rejects_conflicting_metadata() {
        let mut partitions = ReferencedDataFilePartitions::new();
        let partition = || ReferencedDataFilePartition {
            partition_spec_id: 7,
            partition_values: Struct::empty(),
        };
        insert_referenced_data_file_partition(
            &mut partitions,
            "s3://warehouse/data.parquet".to_string(),
            partition(),
        )
        .expect("insert partition facts");
        insert_referenced_data_file_partition(
            &mut partitions,
            "s3://warehouse/data.parquet".to_string(),
            partition(),
        )
        .expect("identical partition facts are idempotent");

        let error = insert_referenced_data_file_partition(
            &mut partitions,
            "s3://warehouse/data.parquet".to_string(),
            ReferencedDataFilePartition {
                partition_spec_id: 8,
                partition_values: Struct::empty(),
            },
        )
        .expect_err("conflicting partition facts must fail");
        assert!(error.contains("conflicting partition metadata"));
    }
}
