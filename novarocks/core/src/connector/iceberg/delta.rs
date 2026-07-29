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

//! Provider-owned snapshot-delta split facts.
//!
//! Delta scan has a logical identity in the planner, but every physical
//! data, delete, and deletion-vector read belongs to the Iceberg provider.
//! These DTOs deliberately contain only serializable Iceberg facts; they do
//! not mention an execution node, `Chunk`, runtime filter, or core I/O type.

use std::collections::{BTreeSet, HashMap, HashSet};

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BaseDataFileLineage {
    pub first_row_id: i64,
    pub data_sequence_number: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeltaDataColumn {
    pub name: String,
    pub field_id: i32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeltaSourceFile {
    pub path: String,
    pub size: i64,
    pub role: DeltaSourceRole,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<String>,
    pub first_row_id: Option<i64>,
    pub data_sequence_number: Option<i64>,
    pub row_id_allow_list: Option<BTreeSet<i64>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum DeltaSourceRole {
    DataFile,
    PositionDelete {
        deletes: Vec<PositionDeleteSourceData>,
    },
    EqualityDelete {
        equality_field_ids: Vec<i32>,
        targets: Vec<EqualityDeleteTargetData>,
    },
    DeletedDataFile {
        previous_data_file_visibility: Option<DeletedFileVisibility>,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum PositionDeleteFileFormat {
    Parquet,
    Puffin,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PositionDeleteSourceData {
    pub delete_file_path: String,
    pub delete_file_size: i64,
    pub referenced_data_file: Option<String>,
    pub file_format: PositionDeleteFileFormat,
    pub content_offset: Option<i64>,
    pub content_size_in_bytes: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EqualityDeleteTargetData {
    pub data_file_path: String,
    pub data_file_size: i64,
    pub data_file_first_row_id: Option<i64>,
    pub data_file_sequence_number: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeletedFileVisibility {
    pub already_deleted_positions: Vec<i64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeltaScanDeleteSide {
    pub base_data_file_lineage: HashMap<String, BaseDataFileLineage>,
    pub previous_data_file_lineage: HashMap<String, BaseDataFileLineage>,
    pub previous_delete_visibility_data_files:
        Vec<super::changes::DeleteVisibilityDataFileDescriptor>,
    pub previously_deleted_positions_per_file: HashMap<String, Vec<u64>>,
    pub deleted_data_file_paths: HashSet<String>,
}
