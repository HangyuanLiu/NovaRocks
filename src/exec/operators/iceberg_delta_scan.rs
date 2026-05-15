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
//! Streaming source operator for the `IcebergDeltaScan` ExecNode.
//!
//! Per-driver pull-driven scanner: at each `pull_chunk` call, advances
//! through `change_files`, opening a per-role scanner on demand and emitting
//! one chunk at a time. The `__change_op` column is injected as a constant
//! per-file value (`+1` for `DataFile`, `-1` for the three delete roles).
//!
//! Phase 1 (Task 4) lays only the factory + operator skeleton with all role
//! scanners stubbed out. Tasks 5-8 fill in the per-role scanner bodies.

use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int8Array};
use arrow::record_batch::RecordBatch;

use crate::exec::change_op::{CHANGE_OP_DELETE, CHANGE_OP_INSERT};
use crate::exec::chunk::Chunk;
use crate::exec::node::iceberg_delta_scan::{
    DeltaSourceFile, DeltaSourceRole, IcebergDeltaScanNode,
};
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::runtime_state::RuntimeState;

pub struct IcebergDeltaScanFactory {
    name: String,
    node: Arc<IcebergDeltaScanNode>,
}

impl IcebergDeltaScanFactory {
    pub fn new(node: IcebergDeltaScanNode) -> Self {
        let name = format!("IcebergDeltaScan (id={})", node.node_id);
        Self {
            name,
            node: Arc::new(node),
        }
    }
}

impl OperatorFactory for IcebergDeltaScanFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        // Phase 1: single-driver only. driver_id != 0 returns an empty stream.
        let pending: VecDeque<DeltaSourceFile> = if driver_id == 0 {
            self.node.change_files.iter().cloned().collect()
        } else {
            VecDeque::new()
        };
        Box::new(IcebergDeltaScanOperator {
            name: self.name.clone(),
            node: Arc::clone(&self.node),
            pending,
            current_scanner: None,
            finished: false,
        })
    }

    fn is_source(&self) -> bool {
        true
    }
}

struct IcebergDeltaScanOperator {
    name: String,
    node: Arc<IcebergDeltaScanNode>,
    pending: VecDeque<DeltaSourceFile>,
    current_scanner: Option<Box<dyn DeltaFileScanner>>,
    finished: bool,
}

pub(crate) trait DeltaFileScanner: Send {
    /// Pull the next batch from the underlying scan. Returns `None` when the
    /// scanner has been fully drained.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, String>;
    /// Constant `__change_op` value to inject on every batch this scanner
    /// produces.
    fn change_op_value(&self) -> i8;
}

impl Operator for IcebergDeltaScanOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }

    fn is_finished(&self) -> bool {
        self.finished
    }
}

impl ProcessorOperator for IcebergDeltaScanOperator {
    fn need_input(&self) -> bool {
        false
    }

    fn has_output(&self) -> bool {
        !self.finished
    }

    fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
        Err("IcebergDeltaScan does not accept input".to_string())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        loop {
            if self.current_scanner.is_none() {
                let Some(next) = self.pending.pop_front() else {
                    self.finished = true;
                    return Ok(None);
                };
                self.current_scanner = Some(open_scanner_for_role(&self.node, next)?);
            }
            match self.current_scanner.as_mut().unwrap().next_batch()? {
                Some(batch) => {
                    let op = self.current_scanner.as_ref().unwrap().change_op_value();
                    let tagged = inject_change_op_column(batch, op)?;
                    let chunk = Chunk::try_new_with_chunk_schema(
                        tagged,
                        self.node.output_chunk_schema.clone(),
                    )?;
                    return Ok(Some(chunk));
                }
                None => {
                    self.current_scanner = None;
                }
            }
        }
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        Ok(())
    }
}

fn inject_change_op_column(batch: RecordBatch, value: i8) -> Result<RecordBatch, String> {
    let rows = batch.num_rows();
    let arr: ArrayRef = Arc::new(Int8Array::from(vec![value; rows]));
    let mut fields: Vec<arrow::datatypes::Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(crate::exec::change_op::change_op_field());
    let new_schema = Arc::new(arrow::datatypes::Schema::new(fields));
    let mut columns = batch.columns().to_vec();
    columns.push(arr);
    RecordBatch::try_new(new_schema, columns).map_err(|e| format!("inject __change_op column: {e}"))
}

fn open_scanner_for_role(
    node: &IcebergDeltaScanNode,
    file: DeltaSourceFile,
) -> Result<Box<dyn DeltaFileScanner>, String> {
    match &file.role {
        DeltaSourceRole::DataFile => open_data_file_scanner(node, &file),
        DeltaSourceRole::PositionDelete { targets } => {
            open_position_delete_scanner(node, &file, targets)
        }
        DeltaSourceRole::EqualityDelete {
            equality_field_ids,
            targets: _,
        } => open_equality_delete_scanner(node, &file, equality_field_ids),
        DeltaSourceRole::DeletedDataFile {
            previous_data_file_visibility,
        } => open_deleted_data_file_scanner(node, &file, previous_data_file_visibility),
    }
}

struct DataFileScanner {
    batches: std::vec::IntoIter<RecordBatch>,
}

impl DeltaFileScanner for DataFileScanner {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, String> {
        Ok(self.batches.next())
    }

    fn change_op_value(&self) -> i8 {
        CHANGE_OP_INSERT
    }
}

fn open_data_file_scanner(
    node: &IcebergDeltaScanNode,
    file: &DeltaSourceFile,
) -> Result<Box<dyn DeltaFileScanner>, String> {
    let batches = crate::connector::iceberg::changes::scan_one_added_data_file(
        &file.path,
        file.size,
        &node.iceberg_runtime.base_table,
        node.object_store_config.as_ref(),
    )?;
    Ok(Box::new(DataFileScanner {
        batches: batches.into_iter(),
    }))
}

struct PositionDeleteScanner {
    batches: std::vec::IntoIter<RecordBatch>,
}

impl DeltaFileScanner for PositionDeleteScanner {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, String> {
        Ok(self.batches.next())
    }

    fn change_op_value(&self) -> i8 {
        CHANGE_OP_DELETE
    }
}

fn open_position_delete_scanner(
    node: &IcebergDeltaScanNode,
    file: &DeltaSourceFile,
    targets: &[crate::exec::node::iceberg_delta_scan::PositionDeleteTargetData],
) -> Result<Box<dyn DeltaFileScanner>, String> {
    // A1 Phase 2 limits position-delete sources to Parquet v2 deletes; the
    // operator does not yet plumb Puffin/DV-blob metadata through
    // `DeltaSourceFile`. Extending the data type is tracked as Phase 2
    // follow-up.
    let referenced = if targets.len() == 1 {
        Some(targets[0].data_file_path.clone())
    } else {
        None
    };
    let delete = crate::connector::iceberg::changes::PositionDeleteRef {
        delete_file_path: file.path.clone(),
        delete_file_size: file.size,
        record_count: None,
        referenced_data_file: referenced,
        file_format: iceberg::spec::DataFileFormat::Parquet,
        content_offset: None,
        content_size_in_bytes: None,
    };
    let delete_side = node.iceberg_runtime.delete_side.as_ref().ok_or_else(|| {
        format!(
            "ivm-a1 position-delete scanner: runtime.delete_side missing for {} (lower_plan \
             should have preloaded it when the change batch has DELETE-side roles)",
            file.path
        )
    })?;
    let rows = crate::connector::iceberg::changes::scan_position_delete_rows_for_targets(
        &node.iceberg_runtime.base_table,
        &delete,
        &delete_side.base_first_row_ids,
        node.iceberg_runtime.object_store_factory.as_ref(),
        node.object_store_config.as_ref(),
    )?;
    Ok(Box::new(PositionDeleteScanner {
        batches: rows.into_iter(),
    }))
}

struct EqualityDeleteScanner {
    batches: std::vec::IntoIter<RecordBatch>,
}

impl DeltaFileScanner for EqualityDeleteScanner {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, String> {
        Ok(self.batches.next())
    }

    fn change_op_value(&self) -> i8 {
        CHANGE_OP_DELETE
    }
}

fn open_equality_delete_scanner(
    node: &IcebergDeltaScanNode,
    file: &DeltaSourceFile,
    equality_field_ids: &[i32],
) -> Result<Box<dyn DeltaFileScanner>, String> {
    let delete = crate::connector::iceberg::changes::EqualityDeleteRef {
        delete_file_path: file.path.clone(),
        delete_file_size: file.size,
        record_count: None,
        equality_ids: equality_field_ids.to_vec(),
        sequence_number: file.data_sequence_number,
        partition_spec_id: file.partition_spec_id,
        partition_key: file.partition_key.clone(),
    };
    let rows = crate::connector::iceberg::changes::scan_equality_delete_rows_for_one(
        &node.iceberg_runtime.base_table,
        &delete,
        node.iceberg_runtime.object_store_factory.as_ref(),
        node.object_store_config.as_ref(),
    )?;
    Ok(Box::new(EqualityDeleteScanner {
        batches: rows.into_iter(),
    }))
}

struct DeletedDataFileScanner {
    batches: std::vec::IntoIter<RecordBatch>,
}

impl DeltaFileScanner for DeletedDataFileScanner {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, String> {
        Ok(self.batches.next())
    }

    fn change_op_value(&self) -> i8 {
        CHANGE_OP_DELETE
    }
}

fn open_deleted_data_file_scanner(
    node: &IcebergDeltaScanNode,
    file: &DeltaSourceFile,
    _visibility: &Option<crate::exec::node::iceberg_delta_scan::DeletedFileVisibility>,
) -> Result<Box<dyn DeltaFileScanner>, String> {
    let deleted_file = crate::connector::iceberg::changes::DeletedDataFileRef {
        path: file.path.clone(),
        size: file.size,
        record_count: None,
        partition_spec_id: file.partition_spec_id,
        partition_key: file.partition_key.clone(),
        first_row_id: file.first_row_id,
        data_sequence_number: file.data_sequence_number,
    };
    // Use the preloaded full-table visibility from runtime.delete_side; the
    // operator no longer rebuilds per-file visibility from
    // `DeletedFileVisibility::already_deleted_positions`.
    let delete_side = node.iceberg_runtime.delete_side.as_ref().ok_or_else(|| {
        format!(
            "ivm-a1 deleted-data-file scanner: runtime.delete_side missing for {} (lower_plan \
             should have preloaded it when the change batch has DELETE-side roles)",
            file.path
        )
    })?;
    let rows = crate::connector::iceberg::changes::scan_one_deleted_data_file(
        &node.iceberg_runtime.base_table,
        &deleted_file,
        node.object_store_config.as_ref(),
        &delete_side.previous_delete_visibility,
    )?;
    Ok(Box::new(DeletedDataFileScanner {
        batches: rows.into_iter(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Compile-only smoke test: the factory type is wired up and its trait
    /// implementations resolve. Semantic verification of empty-stream
    /// behavior happens in the SQL suite (Phase 7), where `iceberg::Table`
    /// fixtures are available.
    #[test]
    fn iceberg_delta_scan_factory_compiles_as_operator_factory() {
        fn assert_is_factory<T: OperatorFactory + ?Sized>() {}
        assert_is_factory::<IcebergDeltaScanFactory>();
    }
}
