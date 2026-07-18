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

use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub(crate) struct ScanRangeParams {
    pub(crate) range: ScanRange,
    pub(crate) volume_id: Option<i32>,
    pub(crate) empty: Option<bool>,
    pub(crate) has_more: Option<bool>,
}

impl ScanRangeParams {
    pub(crate) fn file(file: FileScanRange) -> Self {
        Self {
            range: ScanRange::File(file),
            volume_id: None,
            empty: Some(false),
            has_more: Some(false),
        }
    }

    #[cfg_attr(not(feature = "compat"), allow(dead_code))]
    pub(crate) fn starrocks_tablet(
        tablet_id: i64,
        partition_id: i64,
        version: i64,
    ) -> Result<Self, String> {
        let range = StarRocksTabletScanRange::try_new(tablet_id, partition_id, version)?;
        Ok(Self {
            range: ScanRange::StarRocksTablet(range),
            volume_id: None,
            empty: Some(false),
            has_more: Some(false),
        })
    }
}

#[derive(Clone, Debug)]
pub(crate) enum ScanRange {
    File(FileScanRange),
    #[cfg_attr(not(feature = "compat"), allow(dead_code))]
    StarRocksTablet(StarRocksTabletScanRange),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StarRocksTabletScanRange {
    pub(crate) tablet_id: i64,
    pub(crate) partition_id: i64,
    pub(crate) version: i64,
}

impl StarRocksTabletScanRange {
    #[cfg_attr(not(feature = "compat"), allow(dead_code))]
    pub(crate) fn try_new(tablet_id: i64, partition_id: i64, version: i64) -> Result<Self, String> {
        for (field, value) in [
            ("tablet_id", tablet_id),
            ("partition_id", partition_id),
            ("version", version),
        ] {
            if value <= 0 {
                return Err(format!(
                    "StarRocks tablet scan range {field} must be positive, got {value}"
                ));
            }
        }
        Ok(Self {
            tablet_id,
            partition_id,
            version,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FileFormat {
    Parquet,
    #[allow(dead_code)]
    Orc,
}

impl FileFormat {
    pub(crate) fn as_native_name(self) -> &'static str {
        match self {
            Self::Parquet => "PARQUET",
            Self::Orc => "ORC",
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct FileScanRange {
    pub(crate) file_format: FileFormat,
    pub(crate) full_path: Option<String>,
    pub(crate) relative_path: Option<String>,
    pub(crate) table_id: Option<i64>,
    pub(crate) offset: i64,
    pub(crate) length: i64,
    pub(crate) file_length: i64,
    pub(crate) delete_files: Vec<IcebergDeleteFile>,
    pub(crate) deletion_vector_descriptor: Option<DeletionVectorDescriptor>,
    pub(crate) first_row_id: Option<i64>,
    pub(crate) data_sequence_number: Option<i64>,
    pub(crate) modification_time: Option<i64>,
    pub(crate) datacache_options: Option<DatacacheOptions>,
    pub(crate) included_positions: Vec<i64>,
    pub(crate) serialized_split: Option<String>,
    pub(crate) use_iceberg_jni_metadata_reader: bool,
    pub(crate) ivm_change_op: Option<i8>,
    pub(crate) file_pruning_min_max_values: Option<BTreeMap<i32, FilePruningMinMaxValue>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IcebergFileFormat {
    Parquet,
}

impl IcebergFileFormat {
    pub(crate) fn as_native_name(self) -> &'static str {
        match self {
            Self::Parquet => "PARQUET",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IcebergFileContent {
    PositionDeletes,
    EqualityDeletes,
}

impl IcebergFileContent {
    pub(crate) fn as_native_name(self) -> &'static str {
        match self {
            Self::PositionDeletes => "POSITION_DELETES",
            Self::EqualityDeletes => "EQUALITY_DELETES",
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct IcebergDeleteFile {
    pub(crate) full_path: Option<String>,
    pub(crate) file_format: IcebergFileFormat,
    pub(crate) file_content: IcebergFileContent,
    pub(crate) length: Option<i64>,
}

#[derive(Clone, Debug)]
pub(crate) struct DeletionVectorDescriptor {
    pub(crate) storage_type: Option<String>,
    pub(crate) path_or_inline_dv: Option<String>,
    pub(crate) offset: Option<i64>,
    pub(crate) size_in_bytes: Option<i64>,
    pub(crate) cardinality: Option<i64>,
}

#[derive(Clone, Debug)]
pub(crate) struct DatacacheOptions {
    pub(crate) enable_populate_datacache: Option<bool>,
    pub(crate) priority: Option<i32>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FilePruningValueKind {
    Bool,
    Int,
    Float,
}

#[derive(Clone, Debug)]
pub(crate) struct FilePruningMinMaxValue {
    pub(crate) value_kind: FilePruningValueKind,
    pub(crate) has_null: bool,
    pub(crate) all_null: bool,
    pub(crate) min_int_value: Option<i64>,
    pub(crate) max_int_value: Option<i64>,
    pub(crate) min_float_value: Option<f64>,
    pub(crate) max_float_value: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starrocks_tablet_range_requires_positive_identity() {
        let valid = ScanRangeParams::starrocks_tablet(300, 100, 7)
            .expect("positive StarRocks tablet range");
        let ScanRange::StarRocksTablet(range) = valid.range else {
            panic!("expected StarRocks tablet range");
        };
        assert_eq!(range.tablet_id, 300);
        assert_eq!(range.partition_id, 100);
        assert_eq!(range.version, 7);
        assert_eq!(valid.empty, Some(false));
        assert_eq!(valid.has_more, Some(false));

        for (tablet_id, partition_id, version, field) in [
            (0, 100, 7, "tablet_id"),
            (-1, 100, 7, "tablet_id"),
            (300, 0, 7, "partition_id"),
            (300, -1, 7, "partition_id"),
            (300, 100, 0, "version"),
            (300, 100, -1, "version"),
        ] {
            let err = ScanRangeParams::starrocks_tablet(tablet_id, partition_id, version)
                .expect_err("non-positive StarRocks range identity must fail");
            assert!(err.contains(field), "{err}");
            assert!(err.contains("positive"), "{err}");
        }
    }
}
