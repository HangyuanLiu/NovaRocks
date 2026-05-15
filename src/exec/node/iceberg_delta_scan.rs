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

//! IVM `IcebergDeltaScan` ExecNode: snapshot-range delta source.
//!
//! Single source leaf that internally consumes Iceberg snapshot diff
//! products (data files / position-delete / equality-delete / deleted-data-file)
//! and emits a unified chunk stream tagged with the A4 transparent
//! `__change_op` column (+1 for INSERT, -1 for DELETE). Populated by
//! `lower_iceberg_delta_scan` (in `src/lower/thrift/iceberg_delta_scan.rs`)
//! when the Thrift plan carries `TPlanNodeType::ICEBERG_DELTA_SCAN_NODE`.

use std::sync::Arc;

use crate::exec::chunk::ChunkSchemaRef;
use crate::fs::object_store::ObjectStoreConfig;
use crate::fs::opendal::OpendalRangeReaderFactory;

#[derive(Clone, Debug)]
pub struct IcebergDeltaScanNode {
    pub base_table_ident: BaseTableIdent,
    pub from_snapshot_id: i64,
    pub to_snapshot_id: i64,
    pub output_chunk_schema: ChunkSchemaRef,
    pub apply_key_source: ApplyKeySource,
    pub change_files: Vec<DeltaSourceFile>,
    pub object_store_config: Option<ObjectStoreConfig>,
    pub iceberg_runtime: Arc<IcebergRuntimeHandles>,
    pub node_id: i32,
}

/// Three-part identifier of the base Iceberg table that an `IcebergDeltaScan`
/// reads from. Distinct from `iceberg::TableIdent` (which carries a richer
/// `NamespaceIdent`); this struct holds raw normalized strings for matching
/// against NovaRocks-internal MV refresh state.
#[derive(Clone, Debug)]
pub struct BaseTableIdent {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

#[derive(Clone, Debug)]
pub enum ApplyKeySource {
    /// A9 hidden apply key: base table's `_row_id` v3 row lineage column.
    BaseRowId,
}

#[derive(Clone, Debug)]
pub struct DeltaSourceFile {
    pub path: String,
    pub size: i64,
    pub role: DeltaSourceRole,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<String>,
    pub first_row_id: Option<i64>,
    pub data_sequence_number: Option<i64>,
}

#[derive(Clone, Debug)]
pub enum DeltaSourceRole {
    DataFile,
    PositionDelete {
        targets: Vec<PositionDeleteTargetData>,
    },
    EqualityDelete {
        equality_field_ids: Vec<i32>,
        targets: Vec<EqualityDeleteTargetData>,
    },
    DeletedDataFile {
        previous_data_file_visibility: Option<DeletedFileVisibility>,
    },
}

#[derive(Clone, Debug)]
pub struct PositionDeleteTargetData {
    pub data_file_path: String,
    pub data_file_first_row_id: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct EqualityDeleteTargetData {
    pub data_file_path: String,
    pub data_file_first_row_id: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct DeletedFileVisibility {
    pub already_deleted_positions: Vec<i64>,
}

/// Iceberg per-table runtime handles required by `IcebergDeltaScanOperator`
/// to open delete files and re-read target data files. Constructed by
/// `lower_iceberg_delta_scan` when lowering `ICEBERG_DELTA_SCAN_NODE`:
/// - `base_table` comes from `iceberg::Catalog::load_table`
/// - `object_store_factory` is built once via `build_factory_for_table` and
///   shared across role scanners
/// - `delete_side` is populated via `base_data_file_lineage_index` +
///   `load_existing_delete_visibility_by_data_file_at` only when the change
///   batch contains DELETE-side roles (position / equality / deleted-data-file).
#[derive(Debug)]
pub struct IcebergRuntimeHandles {
    pub base_table: iceberg::table::Table,
    pub object_store_factory: Arc<OpendalRangeReaderFactory>,
    pub delete_side: Option<DeltaScanDeleteSide>,
}

/// Per-target-data-file v3 row-lineage metadata required by the delete-side
/// scanners in `IcebergDeltaScanOperator` to synthesize the
/// `_file` / `_pos` / `_row_id` / `_last_updated_sequence_number` virtual
/// columns when reverse-projecting deleted rows. Filled in by
/// `base_data_file_lineage_index` from the previous-snapshot read view.
#[derive(Clone, Copy, Debug)]
pub struct BaseDataFileLineage {
    pub first_row_id: i64,
    pub data_sequence_number: i64,
}

#[derive(Debug)]
pub struct DeltaScanDeleteSide {
    pub base_data_file_lineage: std::collections::HashMap<String, BaseDataFileLineage>,
    pub(crate) previous_delete_visibility:
        crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
}
