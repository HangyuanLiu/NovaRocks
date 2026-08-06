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

use novarocks_connector_iceberg::scan_model::IcebergDataFileInfo;

use super::iceberg_target_apply_oracle::TargetRowPositionSet;

fn bind_target_state_file_positions(
    mut files: Vec<IcebergDataFileInfo>,
    matched_positions: &[TargetRowPositionSet],
    target: &str,
) -> Result<Vec<IcebergDataFileInfo>, String> {
    if matched_positions.is_empty() {
        files.clear();
        return Ok(files);
    }

    let mut by_file = BTreeMap::<String, Vec<i64>>::new();
    for set in matched_positions {
        if set.positions.is_empty() {
            continue;
        }
        by_file
            .entry(set.referenced_data_file.clone())
            .or_default()
            .extend(set.positions.iter().copied());
    }
    for positions in by_file.values_mut() {
        positions.sort_unstable();
        positions.dedup();
    }
    if by_file.is_empty() {
        files.clear();
        return Ok(files);
    }

    let mut bound_files = Vec::new();
    for mut file in std::mem::take(&mut files) {
        if let Some(positions) = by_file.remove(&file.path) {
            file.included_positions = Some(positions);
            bound_files.push(file);
        }
    }
    if !by_file.is_empty() {
        let missing = by_file.keys().cloned().collect::<Vec<_>>().join(", ");
        return Err(format!(
            "Iceberg target-state scan {target} locator returned positions for files not present in scan source: [{missing}]"
        ));
    }
    Ok(bound_files)
}

fn target_state_files_for_binding_test() -> Vec<IcebergDataFileInfo> {
    let file = |path: &str, size| IcebergDataFileInfo {
        path: path.to_string(),
        size,
        row_count: Some(size),
        column_stats: None,
        partition_spec_id: None,
        partition_key: None,
        first_row_id: None,
        data_sequence_number: None,
        ivm_change_op: None,
        included_positions: None,
        delete_files: Vec::new(),
        manifest_path: None,
        partition_values: Vec::new(),
    };
    vec![
        file("s3://bucket/mv/data-a.parquet", 10),
        file("s3://bucket/mv/data-b.parquet", 20),
    ]
}

#[test]
fn bind_target_state_file_positions_keeps_only_matched_files() {
    let positions = vec![TargetRowPositionSet {
        referenced_data_file: "s3://bucket/mv/data-b.parquet".to_string(),
        positions: vec![2, 8, 13],
    }];
    let files = bind_target_state_file_positions(
        target_state_files_for_binding_test(),
        &positions,
        "tgt.db.mv",
    )
    .expect("bind positions");
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].path, "s3://bucket/mv/data-b.parquet");
    assert_eq!(files[0].included_positions, Some(vec![2, 8, 13]));
}

#[test]
fn bind_target_state_file_positions_empty_matches_returns_empty_source() {
    let files =
        bind_target_state_file_positions(target_state_files_for_binding_test(), &[], "tgt.db.mv")
            .expect("bind empty positions");
    assert!(files.is_empty());
}

#[test]
fn bind_target_state_file_positions_rejects_missing_files() {
    let positions = vec![TargetRowPositionSet {
        referenced_data_file: "s3://bucket/mv/missing.parquet".to_string(),
        positions: vec![1],
    }];
    let err = bind_target_state_file_positions(
        target_state_files_for_binding_test(),
        &positions,
        "tgt.db.mv",
    )
    .expect_err("missing target file should fail");
    assert!(err.contains("locator returned positions for files not present"));
    assert!(err.contains("s3://bucket/mv/missing.parquet"));
}
