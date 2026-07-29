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
//! `lower_iceberg_delta_scan` (in `src/lower/compat/node/iceberg_delta_scan.rs`)
//! when the Thrift plan carries `TPlanNodeType::ICEBERG_DELTA_SCAN_NODE`.

use std::sync::{Arc, OnceLock};

pub use crate::connector::iceberg::delta::{
    BaseDataFileLineage, DeletedFileVisibility, DeltaDataColumn as IcebergDeltaDataColumnPayload,
    DeltaScanDeleteSide as DeltaScanDeleteSidePayload, DeltaSourceFile, DeltaSourceRole,
    EqualityDeleteTargetData, PositionDeleteFileFormat, PositionDeleteSourceData,
};
use crate::exec::chunk::ChunkSchemaRef;
use crate::exec::node::runtime_filter::NativeRuntimeFilterConsumerSpec;
use novarocks_fs::ObjectStoreConfig;

#[derive(Clone, Debug)]
pub(crate) struct IcebergDeltaTablePayload {
    pub(crate) table_location: String,
    pub(crate) data_columns: Vec<IcebergDeltaDataColumnPayload>,
}

#[derive(Clone, Debug)]
pub struct IcebergDeltaScanNode {
    pub base_table_ident: BaseTableIdent,
    pub table_location: String,
    pub from_snapshot_id: i64,
    pub to_snapshot_id: i64,
    pub output_chunk_schema: ChunkSchemaRef,
    pub apply_key_source: ApplyKeySource,
    pub change_files: Vec<DeltaSourceFile>,
    pub object_store_config: Option<ObjectStoreConfig>,
    pub iceberg_runtime: Arc<IcebergRuntimeHandles>,
    pub node_id: i32,
    pub(crate) native_runtime_filter_specs: Vec<NativeRuntimeFilterConsumerSpec>,
}

impl IcebergDeltaScanNode {
    pub(crate) fn native_runtime_filter_specs(&self) -> &[NativeRuntimeFilterConsumerSpec] {
        &self.native_runtime_filter_specs
    }

    pub(crate) fn set_native_runtime_filter_specs(
        &mut self,
        specs: Vec<NativeRuntimeFilterConsumerSpec>,
    ) {
        self.native_runtime_filter_specs = specs;
    }
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

/// Iceberg per-table runtime handles required by `IcebergDeltaScanOperator`
/// to open planned data/delete files. Constructed by `lower_iceberg_delta_scan`
/// when lowering `ICEBERG_DELTA_SCAN_NODE` from the typed Thrift payload:
/// - `table` is the codegen-produced table descriptor, not full Iceberg metadata
/// - `object_store_factory` is built once from table location and shared across data,
///   delete, and Puffin deletion-vector scanners
/// - `delete_side` is populated only when the change batch contains DELETE-side roles.
#[derive(Debug)]
pub struct IcebergRuntimeHandles {
    pub(crate) table: IcebergDeltaTablePayload,
    pub(crate) delete_side_payload: Option<DeltaScanDeleteSidePayload>,
    pub(crate) resolved: OnceLock<Result<Arc<IcebergResolvedRuntime>, String>>,
}

impl IcebergRuntimeHandles {
    pub(crate) fn new(
        table: IcebergDeltaTablePayload,
        delete_side_payload: Option<DeltaScanDeleteSidePayload>,
    ) -> Self {
        Self {
            table,
            delete_side_payload,
            resolved: OnceLock::new(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct IcebergResolvedRuntime {
    pub(crate) object_store_factory: Arc<novarocks_fs::FsAccessHandle>,
    pub(crate) delete_side: Option<DeltaScanDeleteSide>,
}

/// Per-target-data-file v3 row-lineage metadata required by the delete-side
/// scanners in `IcebergDeltaScanOperator` to synthesize the
/// `_file` / `_pos` / `_row_id` / `_last_updated_sequence_number` virtual
/// columns when reverse-projecting deleted rows. Filled in from the relevant
/// snapshot read views.
#[derive(Debug)]
pub struct DeltaScanDeleteSide {
    pub base_data_file_lineage: std::collections::HashMap<String, BaseDataFileLineage>,
    pub(crate) previous_delete_visibility:
        crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
    /// Position-delete rows already visible at the previous MV-refresh
    /// snapshot, keyed by raw Iceberg data-file path. Puffin deletion vectors
    /// are cumulative replacements, so delta-scan position-delete scanners
    /// subtract this map before reverse-projecting rows.
    pub(crate) previously_deleted_positions_per_file:
        std::collections::HashMap<String, roaring::RoaringTreemap>,
    /// `first_row_id` / `data_sequence_number` index keyed by data-file
    /// path, built from the **previous** MV-refresh snapshot (i.e. the
    /// `from_snapshot_id` of the delta range). Used as a fallback by the
    /// `IcebergDeltaScanOperator`'s `DeletedDataFile` scanner when an
    /// OVERWRITE manifest's deleted entry does not carry an explicit
    /// per-file `first_row_id` (the iceberg writer may have only stamped
    /// the manifest-level `first_row_id` on the original APPEND, leaving
    /// the per-DataFile field `None`).
    pub previous_data_file_lineage: std::collections::HashMap<String, BaseDataFileLineage>,
    /// Data files removed by overwrite snapshots inside this delta range.
    /// Position deletes that target one of these files are subsumed by the
    /// deleted-data-file role and must not be emitted a second time.
    pub(crate) deleted_data_file_paths: std::collections::HashSet<String>,
}
