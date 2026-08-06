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

//! Provider-owned Iceberg read-view model and delete applicability.

use std::collections::HashMap;

use crate::scan_model::IcebergColumnStats;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IcebergReadDeleteFormat {
    Parquet,
    Puffin,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IcebergReadDeleteKind {
    Position,
    Equality { equality_field_ids: Vec<i32> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IcebergReadDeleteFile {
    pub path: String,
    pub file_format: IcebergReadDeleteFormat,
    pub kind: IcebergReadDeleteKind,
    pub length: Option<i64>,
    pub content_offset: Option<i64>,
    pub content_size_in_bytes: Option<i64>,
    pub sequence_number: Option<i64>,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<String>,
    pub referenced_data_file: Option<String>,
}

#[derive(Clone, Debug)]
pub struct IcebergReadFile {
    pub path: String,
    pub size: i64,
    pub record_count: Option<i64>,
    pub column_stats: Option<HashMap<String, IcebergColumnStats>>,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<String>,
    pub partition_values: Option<crate::iceberg::spec::Struct>,
    pub manifest_path: Option<String>,
    pub first_row_id: Option<i64>,
    pub data_sequence_number: Option<i64>,
    pub deletes: Vec<IcebergReadDeleteFile>,
}

#[derive(Clone, Debug)]
pub struct IcebergReadSnapshot {
    pub snapshot_id: Option<i64>,
    pub files: Vec<IcebergReadFile>,
}

pub fn delete_applies_to_data_file(
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

pub fn attach_applicable_deletes(
    data_file: &mut IcebergReadFile,
    delete_files: &[IcebergReadDeleteFile],
) {
    let applicable = delete_files
        .iter()
        .filter(|delete_file| delete_applies_to_data_file(delete_file, data_file))
        .cloned()
        .collect::<Vec<_>>();
    data_file.deletes.extend(applicable);
}

pub fn data_files_matching_delete<'a>(
    snapshot: &'a IcebergReadSnapshot,
    delete_file: &IcebergReadDeleteFile,
) -> Vec<&'a IcebergReadFile> {
    snapshot
        .files
        .iter()
        .filter(|data_file| delete_applies_to_data_file(delete_file, data_file))
        .collect()
}

#[derive(Default)]
pub struct DeleteApplicabilityIndex {
    by_referenced_data_path: HashMap<String, Vec<IcebergReadDeleteFile>>,
    global: Vec<IcebergReadDeleteFile>,
}

impl DeleteApplicabilityIndex {
    pub fn push(&mut self, delete_file: IcebergReadDeleteFile) {
        if let Some(referenced_data_file) = delete_file.referenced_data_file.clone() {
            self.by_referenced_data_path
                .entry(referenced_data_file)
                .or_default()
                .push(delete_file);
        } else {
            self.global.push(delete_file);
        }
    }

    pub fn attach_to(&self, data_file: &mut IcebergReadFile) {
        if let Some(delete_files) = self.by_referenced_data_path.get(&data_file.path) {
            attach_applicable_deletes(data_file, delete_files);
        }
        attach_applicable_deletes(data_file, &self.global);
    }
}

pub fn iceberg_partition_key(partition: &crate::iceberg::spec::Struct) -> Option<String> {
    if partition.fields().is_empty() {
        None
    } else {
        Some(format!("{partition:?}"))
    }
}
