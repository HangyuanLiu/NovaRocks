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

use crate::exec::change_op::CHANGE_OP_INSERT;
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
    RecordBatch::try_new(new_schema, columns)
        .map_err(|e| format!("inject __change_op column: {e}"))
}

fn open_scanner_for_role(
    node: &IcebergDeltaScanNode,
    file: DeltaSourceFile,
) -> Result<Box<dyn DeltaFileScanner>, String> {
    match &file.role {
        DeltaSourceRole::DataFile => open_data_file_scanner(node, &file),
        DeltaSourceRole::PositionDelete { .. } => {
            Err("position-delete scanner: TODO Task 6".to_string())
        }
        DeltaSourceRole::EqualityDelete { .. } => {
            Err("equality-delete scanner: TODO Task 7".to_string())
        }
        DeltaSourceRole::DeletedDataFile { .. } => {
            Err("deleted-data-file scanner: TODO Task 8".to_string())
        }
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
