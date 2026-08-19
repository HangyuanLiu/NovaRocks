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

//! `crate::iceberg::spec::DataFile` re-construction shared by the
//! commit-action implementations.
//!
//! `DataFile` fields are `pub(crate)` in iceberg-rust 0.9, so every
//! reconstruction goes through `DataFileBuilder`.

use crate::iceberg::spec::{DataFile, DataFileBuilder};

/// Clone a `DataFile`, overriding `first_row_id` with the given value.
///
/// `DataFile::partition_spec_id` is private in iceberg-rust, so callers must
/// pass the manifest-level partition spec id for the source file.
pub(super) fn clone_data_file_with_first_row_id(
    src: &DataFile,
    partition_spec_id: i32,
    first_row_id: Option<i64>,
) -> Result<DataFile, String> {
    let mut builder = DataFileBuilder::default();
    builder
        .content(src.content_type())
        .file_path(src.file_path().to_string())
        .file_format(src.file_format())
        .partition(src.partition().clone())
        .partition_spec_id(partition_spec_id)
        .record_count(src.record_count())
        .file_size_in_bytes(src.file_size_in_bytes())
        .column_sizes(src.column_sizes().clone())
        .value_counts(src.value_counts().clone())
        .null_value_counts(src.null_value_counts().clone())
        .nan_value_counts(src.nan_value_counts().clone())
        .lower_bounds(src.lower_bounds().clone())
        .upper_bounds(src.upper_bounds().clone())
        .key_metadata(src.key_metadata().map(|b| b.to_vec()))
        .split_offsets(src.split_offsets().map(|s| s.to_vec()))
        .equality_ids(src.equality_ids())
        .first_row_id(first_row_id)
        .referenced_data_file(src.referenced_data_file())
        .content_offset(src.content_offset())
        .content_size_in_bytes(src.content_size_in_bytes());
    if let Some(id) = src.sort_order_id() {
        builder.sort_order_id(id);
    }
    builder
        .build()
        .map_err(|e| format!("clone_data_file_with_first_row_id failed: {e}"))
}
