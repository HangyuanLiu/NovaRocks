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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};

use crate::connector::MinMaxPredicate;
use crate::connector::starrocks::build_native_object_store_profile_from_properties;
use crate::connector::starrocks::lake_meta::{
    LakeMetaColumnKind, LakeMetaStorageFacts, LakeMetaStorageRequest,
};
use crate::connector::starrocks::ports::StorageMetadataProvider;
use crate::connector::starrocks::schema::{StarRocksColumnSchema, StarRocksTabletSchema};
use crate::formats::starrocks::metadata::{
    StarRocksTabletSnapshot, load_bundle_segment_footers,
    load_tablet_snapshot_with_metadata_provider,
};
use crate::formats::starrocks::plan::build_native_read_plan;
use crate::formats::starrocks::reader::build_native_record_batch;

struct LoadedTabletSnapshot {
    tablet_root_path: String,
    snapshot: StarRocksTabletSnapshot,
}

/// Materializes lake-meta facts through the compat-owned storage wire codec.
/// The core kernel still owns object-store reads and Arrow materialization, but
/// does not parse StarRocks storage protobuf in this path.
pub fn materialize_lake_meta_storage_with_metadata_provider(
    request: &LakeMetaStorageRequest,
    tablet_paths: &HashMap<i64, String>,
    properties: &BTreeMap<String, String>,
    metadata_provider: &dyn StorageMetadataProvider,
) -> Result<LakeMetaStorageFacts, String> {
    let object_store_profile = build_native_object_store_profile_from_properties(&properties)?;

    let mut total_rows = 0u128;
    let mut loaded_tablets = Vec::with_capacity(request.tablets.len());
    for tablet in &request.tablets {
        let tablet_root_path = tablet_paths.get(&tablet.tablet_id).ok_or_else(|| {
            format!(
                "LAKE_META_SCAN_NODE missing tablet_root_path for tablet_id={}",
                tablet.tablet_id
            )
        })?;
        let should_load_snapshot = !request.columns.is_empty() || tablet.row_count_hint.is_none();
        if should_load_snapshot {
            let snapshot = load_tablet_snapshot_with_metadata_provider(
                tablet.tablet_id,
                tablet.version,
                tablet_root_path,
                object_store_profile.as_ref(),
                metadata_provider,
            );
            match snapshot {
                Ok(snapshot) => {
                    total_rows = total_rows
                        .checked_add(u128::from(snapshot.total_num_rows))
                        .ok_or_else(|| "LAKE_META_SCAN_NODE row count overflow".to_string())?;
                    loaded_tablets.push(LoadedTabletSnapshot {
                        tablet_root_path: tablet_root_path.clone(),
                        snapshot,
                    });
                }
                Err(error)
                    if should_treat_missing_initial_tablet_metadata_as_empty(
                        tablet.version,
                        tablet.row_count_hint,
                        &error,
                    ) =>
                {
                    let hinted_rows = u128::try_from(tablet.row_count_hint.unwrap_or_default())
                        .map_err(|_| {
                            format!(
                                "LAKE_META_SCAN_NODE row_count hint conversion failed for tablet_id={}",
                                tablet.tablet_id
                            )
                        })?;
                    total_rows = total_rows
                        .checked_add(hinted_rows)
                        .ok_or_else(|| "LAKE_META_SCAN_NODE row count overflow".to_string())?;
                }
                Err(error) => return Err(error),
            }
        } else {
            let hinted_rows =
                u128::try_from(tablet.row_count_hint.unwrap_or_default()).map_err(|_| {
                    format!(
                        "LAKE_META_SCAN_NODE row_count hint conversion failed for tablet_id={}",
                        tablet.tablet_id
                    )
                })?;
            total_rows = total_rows
                .checked_add(hinted_rows)
                .ok_or_else(|| "LAKE_META_SCAN_NODE row count overflow".to_string())?;
        }
    }
    let total_rows = i64::try_from(total_rows).map_err(|_| {
        format!(
            "LAKE_META_SCAN_NODE row count exceeds i64 range: {}",
            total_rows
        )
    })?;

    let mut column_arrays = BTreeMap::new();
    for column in &request.columns {
        let mut arrays = Vec::with_capacity(loaded_tablets.len());
        for tablet in &loaded_tablets {
            let output_type = match &column.kind {
                LakeMetaColumnKind::Dictionary => {
                    resolve_dict_scan_arrow_type(&tablet.snapshot.tablet_schema, &column.column_id)?
                }
                LakeMetaColumnKind::Value(data_type) => data_type.clone(),
            };
            let output_schema = Arc::new(Schema::new(vec![Field::new(
                column.column_id.trim(),
                output_type,
                true,
            )]));
            let segment_footers = load_bundle_segment_footers(
                &tablet.snapshot,
                &tablet.tablet_root_path,
                object_store_profile.as_ref(),
            )?;
            let plan =
                build_native_read_plan(&tablet.snapshot, &segment_footers, &output_schema, None)?;
            let no_predicates: [MinMaxPredicate; 0] = [];
            let batch = build_native_record_batch(
                &plan,
                &segment_footers,
                &tablet.tablet_root_path,
                object_store_profile.as_ref(),
                &output_schema,
                &no_predicates,
            )?;
            if batch.num_columns() != 1 {
                return Err(format!(
                    "LAKE_META_SCAN_NODE storage materialization expects one projected column, got {} (column={})",
                    batch.num_columns(),
                    column.column_id
                ));
            }
            arrays.push(Arc::clone(batch.column(0)));
        }
        column_arrays.insert(column.storage_key(), arrays);
    }

    Ok(LakeMetaStorageFacts {
        total_rows,
        column_arrays,
    })
}

fn should_treat_missing_initial_tablet_metadata_as_empty(
    version: i64,
    row_count_hint: Option<i64>,
    error: &str,
) -> bool {
    version == 1
        && !row_count_hint.is_some_and(|value| value > 0)
        && error
            .to_ascii_lowercase()
            .contains("bundle metadata does not contain tablet page")
}

fn resolve_dict_scan_arrow_type(
    tablet_schema: &StarRocksTabletSchema,
    column_id: &str,
) -> Result<DataType, String> {
    let target = column_id.trim();
    if target.contains('.') {
        return Ok(DataType::Utf8);
    }
    for column in &tablet_schema.column {
        if column
            .name
            .as_deref()
            .is_some_and(|name| name.trim().eq_ignore_ascii_case(target))
        {
            return dict_scan_arrow_type_from_column(column);
        }
    }
    Err(format!(
        "dict_merge metric references unknown schema column: {}",
        column_id
    ))
}

fn dict_scan_arrow_type_from_column(column: &StarRocksColumnSchema) -> Result<DataType, String> {
    match column.r#type.trim().to_ascii_uppercase().as_str() {
        "CHAR" | "VARCHAR" | "STRING" => Ok(DataType::Utf8),
        "ARRAY" => {
            let child = column.children_columns.first().ok_or_else(|| {
                format!(
                    "dict_merge ARRAY column missing item type: column_name={}",
                    column.name.as_deref().unwrap_or("<unknown>")
                )
            })?;
            match child.r#type.trim().to_ascii_uppercase().as_str() {
                "CHAR" | "VARCHAR" | "STRING" => Ok(DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Utf8,
                    true,
                )))),
                other => Err(format!(
                    "dict_merge ARRAY item type is not supported: column_name={}, item_type={}",
                    column.name.as_deref().unwrap_or("<unknown>"),
                    other
                )),
            }
        }
        other => Err(format!(
            "dict_merge metric only supports string/array<string> columns: column_name={}, schema_type={}",
            column.name.as_deref().unwrap_or("<unknown>"),
            other
        )),
    }
}
