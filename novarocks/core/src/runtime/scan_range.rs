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
pub struct ScanRangeParams {
    pub range: ScanRange,
    pub volume_id: Option<i32>,
    pub empty: Option<bool>,
    pub has_more: Option<bool>,
}

impl ScanRangeParams {
    pub fn file(file: FileScanRange) -> Self {
        Self {
            range: ScanRange::File(file),
            volume_id: None,
            empty: Some(false),
            has_more: Some(false),
        }
    }

    pub fn broker_file(file: BrokerFileScanRange) -> Self {
        Self {
            range: ScanRange::BrokerFile(file),
            volume_id: None,
            empty: Some(false),
            has_more: Some(false),
        }
    }

    pub fn schema_selection(selected: bool) -> Self {
        Self {
            range: ScanRange::SchemaSelection(SchemaScanSelection { selected }),
            volume_id: None,
            empty: Some(!selected),
            has_more: Some(false),
        }
    }
}

#[derive(Clone, Debug)]
pub enum ScanRange {
    File(FileScanRange),
    BrokerFile(BrokerFileScanRange),
    SchemaSelection(SchemaScanSelection),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchemaScanSelection {
    pub selected: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrokerFileFormat {
    Csv,
    Json,
    Parquet,
    Orc,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrokerFileScanRange {
    pub path: String,
    pub file_size: i64,
    pub offset: i64,
    pub length: i64,
    pub format: BrokerFileFormat,
    pub strip_outer_array: bool,
    pub jsonpaths: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileFormat {
    Parquet,
    #[allow(dead_code)]
    Orc,
}

impl FileFormat {
    pub fn as_native_name(self) -> &'static str {
        match self {
            Self::Parquet => "PARQUET",
            Self::Orc => "ORC",
        }
    }
}

#[derive(Clone, Debug)]
pub struct FileScanRange {
    pub file_format: FileFormat,
    pub full_path: Option<String>,
    pub relative_path: Option<String>,
    pub table_id: Option<i64>,
    pub offset: i64,
    pub length: i64,
    pub file_length: i64,
    pub delete_files: Vec<IcebergDeleteFile>,
    pub deletion_vector_descriptor: Option<DeletionVectorDescriptor>,
    pub first_row_id: Option<i64>,
    pub data_sequence_number: Option<i64>,
    pub modification_time: Option<i64>,
    pub datacache_options: Option<DatacacheOptions>,
    pub candidate_node: Option<String>,
    pub included_positions: Vec<i64>,
    pub serialized_split: Option<String>,
    pub use_iceberg_jni_metadata_reader: bool,
    pub ivm_change_op: Option<i8>,
    pub file_pruning_min_max_values: Option<BTreeMap<i32, FilePruningMinMaxValue>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IcebergFileFormat {
    Parquet,
}

impl IcebergFileFormat {
    pub fn as_native_name(self) -> &'static str {
        match self {
            Self::Parquet => "PARQUET",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IcebergFileContent {
    PositionDeletes,
    EqualityDeletes,
}

impl IcebergFileContent {
    pub fn as_native_name(self) -> &'static str {
        match self {
            Self::PositionDeletes => "POSITION_DELETES",
            Self::EqualityDeletes => "EQUALITY_DELETES",
        }
    }
}

#[derive(Clone, Debug)]
pub struct IcebergDeleteFile {
    pub full_path: Option<String>,
    pub file_format: IcebergFileFormat,
    pub file_content: IcebergFileContent,
    pub length: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct DeletionVectorDescriptor {
    pub storage_type: Option<String>,
    pub path_or_inline_dv: Option<String>,
    pub offset: Option<i64>,
    pub size_in_bytes: Option<i64>,
    pub cardinality: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct DatacacheOptions {
    pub enable_populate_datacache: Option<bool>,
    pub priority: Option<i32>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilePruningValueKind {
    Bool,
    Int,
    Float,
}

#[derive(Clone, Debug)]
pub struct FilePruningMinMaxValue {
    pub value_kind: FilePruningValueKind,
    pub has_null: bool,
    pub all_null: bool,
    pub min_int_value: Option<i64>,
    pub max_int_value: Option<i64>,
    pub min_float_value: Option<f64>,
    pub max_float_value: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;
}
