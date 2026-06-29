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
//! IVM-A1 merge sink: routes mixed +/- chunks to the data-file writer or
//! position-delete collector. DELETE rows must already carry Iceberg `_file`
//! and `_pos` columns from the change-stream plan; this sink only groups those
//! positions into `PositionDeleteGroup`s for the shared `IcebergCommitCollector`.
//! Commit dispatch is owned by the refresh driver (not this sink) per design §3
//! / §5.

use std::sync::Arc;

use arrow::array::{Array, Int8Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use iceberg::spec::DataFile;

use crate::connector::iceberg::commit::IcebergCommitCollector;
use crate::connector::iceberg::data_writer::{
    IcebergStreamingDataFileWriter, written_file_to_sink_commit_info_for_metadata,
};
use crate::engine::iceberg_writer::data_file_to_written_file;
use crate::exec::change_op::{CHANGE_OP_COLUMN, CHANGE_OP_DELETE, CHANGE_OP_INSERT};
use crate::exec::chunk::Chunk;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::global_async_runtime::data_block_on;
use crate::runtime::runtime_state::RuntimeState;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplyKeyValueType {
    Int64,
    Utf8,
    BranchInt64,
    BranchUtf8,
}

pub struct IcebergMergeSinkPlan {
    pub target_table: iceberg::table::Table,
    pub collector: Arc<IcebergCommitCollector>,
}

pub struct IcebergMergeSinkFactory {
    name: String,
    plan: Arc<IcebergMergeSinkPlan>,
}

impl IcebergMergeSinkFactory {
    pub fn new(plan: IcebergMergeSinkPlan) -> Self {
        let ident = plan.target_table.identifier();
        Self {
            name: format!(
                "IcebergMergeSink ({}.{})",
                ident.namespace().to_url_string(),
                ident.name(),
            ),
            plan: Arc::new(plan),
        }
    }
}

impl OperatorFactory for IcebergMergeSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        let writer = match IcebergStreamingDataFileWriter::new(self.plan.target_table.clone()) {
            Ok(w) => Some(w),
            Err(e) => {
                return Box::new(FailedSinkOperator {
                    name: self.name.clone(),
                    error: e,
                });
            }
        };
        Box::new(IcebergMergeSinkOperator {
            name: self.name.clone(),
            plan: Arc::clone(&self.plan),
            writer,
            finished: false,
        })
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct IcebergMergeSinkOperator {
    name: String,
    plan: Arc<IcebergMergeSinkPlan>,
    writer: Option<IcebergStreamingDataFileWriter>,
    finished: bool,
}

impl Operator for IcebergMergeSinkOperator {
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

impl ProcessorOperator for IcebergMergeSinkOperator {
    fn need_input(&self) -> bool {
        !self.finished
    }

    fn has_output(&self) -> bool {
        false
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        let (insert_batch, delete_batch) = partition_chunk_by_change_op(&chunk)?;
        if let Some(batch) = insert_batch {
            let writer = self
                .writer
                .as_mut()
                .ok_or_else(|| "merge sink: writer missing on driver 0".to_string())?;
            data_block_on(writer.write_record_batch(strip_change_op(batch)?))??;
        }
        if let Some(batch) = delete_batch {
            self.handle_delete_batch(batch)?;
        }
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Err("merge sink does not produce output".to_string())
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        if let Some(writer) = self.writer.take() {
            let data_files: Vec<DataFile> = data_block_on(writer.finish())??;
            let metadata = self.plan.target_table.metadata();
            let partition_spec_id = metadata.default_partition_spec_id();
            let sink_commit_infos = data_files
                .into_iter()
                .map(|df| {
                    let wf = data_file_to_written_file(&df, partition_spec_id)?;
                    written_file_to_sink_commit_info_for_metadata(&wf, metadata)
                })
                .collect::<Result<Vec<_>, _>>()?;
            self.plan
                .collector
                .inject_sink_commit_infos(sink_commit_infos)?;
        }
        self.finished = true;
        Ok(())
    }
}

impl IcebergMergeSinkOperator {
    fn handle_delete_batch(&self, batch: RecordBatch) -> Result<(), String> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let snapshot_id = self
            .plan
            .target_table
            .metadata()
            .current_snapshot()
            .map(|snapshot| snapshot.snapshot_id());
        let referenced_data_file_partitions =
            crate::engine::delete_flow::load_referenced_data_file_partitions_at(
                &self.plan.target_table,
                snapshot_id,
            )
            .map_err(|err| {
                format!(
                    "merge sink: load target data-file partition metadata for DELETE positions: {err}"
                )
            })?;
        let groups = position_delete_groups_from_delete_batch_positions(
            &batch,
            &referenced_data_file_partitions,
        )?;
        for group in groups {
            self.plan.collector.inject_delete_group(group);
        }
        Ok(())
    }
}

struct FailedSinkOperator {
    name: String,
    error: String,
}

impl Operator for FailedSinkOperator {
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
        false
    }
}

impl ProcessorOperator for FailedSinkOperator {
    fn need_input(&self) -> bool {
        true
    }
    fn has_output(&self) -> bool {
        false
    }
    fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
        Err(format!("merge sink failed to initialize: {}", self.error))
    }
    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Err(format!("merge sink failed to initialize: {}", self.error))
    }
    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        Err(format!("merge sink failed to initialize: {}", self.error))
    }
}

fn partition_chunk_by_change_op(
    chunk: &Chunk,
) -> Result<(Option<RecordBatch>, Option<RecordBatch>), String> {
    let batch = &chunk.batch;
    let col_idx = batch
        .schema()
        .index_of(CHANGE_OP_COLUMN)
        .map_err(|_| format!("merge sink: chunk missing column {CHANGE_OP_COLUMN}"))?;
    let arr = batch
        .column(col_idx)
        .as_any()
        .downcast_ref::<Int8Array>()
        .ok_or_else(|| format!("merge sink: column {CHANGE_OP_COLUMN} must be Int8"))?;

    let mut insert_indices = Vec::new();
    let mut delete_indices = Vec::new();
    for (i, value) in arr.iter().enumerate() {
        match value {
            Some(CHANGE_OP_INSERT) => insert_indices.push(i),
            Some(CHANGE_OP_DELETE) => delete_indices.push(i),
            Some(other) => {
                return Err(format!(
                    "merge sink: unexpected {CHANGE_OP_COLUMN} value {other}"
                ));
            }
            None => return Err(format!("merge sink: null {CHANGE_OP_COLUMN}")),
        }
    }

    let take = |indices: &[usize]| -> Result<Option<RecordBatch>, String> {
        if indices.is_empty() {
            return Ok(None);
        }
        let index_arr =
            arrow::array::UInt32Array::from_iter_values(indices.iter().map(|&i| i as u32));
        let mut taken_columns = Vec::with_capacity(batch.num_columns());
        for col in batch.columns() {
            let taken = arrow::compute::take(col.as_ref(), &index_arr, None)
                .map_err(|e| format!("merge sink take: {e}"))?;
            taken_columns.push(taken);
        }
        let new_batch = RecordBatch::try_new(batch.schema(), taken_columns)
            .map_err(|e| format!("merge sink rebuild batch: {e}"))?;
        Ok(Some(new_batch))
    };

    Ok((take(&insert_indices)?, take(&delete_indices)?))
}

fn strip_change_op(batch: RecordBatch) -> Result<RecordBatch, String> {
    // The terminal change stream can carry refresh-internal metadata columns.
    // Only target schema columns and persisted apply keys may flow into new
    // Iceberg data files.
    let internal_names = [
        CHANGE_OP_COLUMN,
        crate::exec::row_position::ICEBERG_ROW_ID_COL,
        crate::exec::row_position::ICEBERG_FILE_PATH_COL,
        crate::exec::row_position::ICEBERG_ROW_POS_COL,
    ];
    let schema = batch.schema();
    let drop_indices: Vec<usize> = schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(idx, f)| {
            if internal_names.iter().any(|n| f.name() == *n) {
                Some(idx)
            } else {
                None
            }
        })
        .collect();
    if drop_indices.is_empty() {
        return Ok(batch);
    }
    let mut fields: Vec<arrow::datatypes::Field> =
        schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    let mut columns: Vec<arrow::array::ArrayRef> = batch.columns().to_vec();
    // Remove from highest index to lowest to keep remaining indices valid.
    for idx in drop_indices.iter().rev() {
        fields.remove(*idx);
        columns.remove(*idx);
    }
    let new_schema = Arc::new(arrow::datatypes::Schema::new(fields));
    RecordBatch::try_new(new_schema, columns)
        .map_err(|e| format!("merge sink strip internal columns: {e}"))
}

fn position_delete_groups_from_delete_batch_positions(
    batch: &RecordBatch,
    referenced_data_file_partitions: &crate::engine::delete_flow::ReferencedDataFilePartitions,
) -> Result<Vec<crate::connector::iceberg::commit::PositionDeleteGroup>, String> {
    let file_column = crate::exec::row_position::ICEBERG_FILE_PATH_COL;
    let pos_column = crate::exec::row_position::ICEBERG_ROW_POS_COL;
    let schema = batch.schema();
    let file_idx = schema.index_of(file_column).map_err(|_| {
        format!("merge sink: DELETE batch missing Iceberg row locator column {file_column}")
    })?;
    let pos_idx = schema.index_of(pos_column).map_err(|_| {
        format!("merge sink: DELETE batch missing Iceberg row locator column {pos_column}")
    })?;
    let files = batch
        .column(file_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            format!("merge sink: Iceberg row locator column {file_column} must be Utf8")
        })?;
    let positions = batch
        .column(pos_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            format!("merge sink: Iceberg row locator column {pos_column} must be Int64")
        })?;

    let mut by_file = std::collections::BTreeMap::<String, std::collections::BTreeSet<i64>>::new();
    for row in 0..batch.num_rows() {
        if files.is_null(row) {
            return Err(format!(
                "merge sink: NULL Iceberg row locator column {file_column} in DELETE batch row {row}"
            ));
        }
        if positions.is_null(row) {
            return Err(format!(
                "merge sink: NULL Iceberg row locator column {pos_column} in DELETE batch row {row}"
            ));
        }
        let file = files.value(row);
        if file.is_empty() {
            return Err(format!(
                "merge sink: empty Iceberg row locator column {file_column} in DELETE batch row {row}"
            ));
        }
        let pos = positions.value(row);
        if pos < 0 {
            return Err(format!(
                "merge sink: negative Iceberg row locator column {pos_column} value {pos} in DELETE batch row {row}"
            ));
        }
        by_file.entry(file.to_string()).or_default().insert(pos);
    }

    let mut groups = Vec::with_capacity(by_file.len());
    for (referenced_data_file, positions) in by_file {
        let partition = referenced_data_file_partitions
            .get(&referenced_data_file)
            .ok_or_else(|| {
                format!(
                    "merge sink: DELETE row locator referenced target data file `{referenced_data_file}` missing from target snapshot metadata"
                )
            })?;
        groups.push(crate::connector::iceberg::commit::PositionDeleteGroup {
            referenced_data_file,
            partition_spec_id: partition.partition_spec_id,
            partition_values: partition.partition_values.clone(),
            positions: positions.into_iter().collect(),
        });
    }
    Ok(groups)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int8Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn chunk_with(batch: RecordBatch) -> Chunk {
        let schema = batch.schema();
        let slots = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                crate::exec::chunk::ChunkSlotSchema::from_field(
                    crate::common::ids::SlotId::new(i as u32),
                    f.as_ref(),
                    None,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let chunk_schema = crate::exec::chunk::ChunkSchema::try_new(slots).unwrap();
        Chunk::try_new_with_chunk_schema(batch, Arc::new(chunk_schema)).unwrap()
    }

    #[test]
    fn partition_pure_insert_chunk() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int32, false),
            crate::exec::change_op::change_op_field(),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(Int8Array::from(vec![CHANGE_OP_INSERT; 3])) as ArrayRef,
            ],
        )
        .unwrap();
        let chunk = chunk_with(batch);
        let (ins, del) = partition_chunk_by_change_op(&chunk).unwrap();
        assert_eq!(ins.unwrap().num_rows(), 3);
        assert!(del.is_none());
    }

    #[test]
    fn partition_mixed_chunk() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int32, false),
            crate::exec::change_op::change_op_field(),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef,
                Arc::new(Int8Array::from(vec![1, -1, 1, -1])) as ArrayRef,
            ],
        )
        .unwrap();
        let chunk = chunk_with(batch);
        let (ins, del) = partition_chunk_by_change_op(&chunk).unwrap();
        assert_eq!(ins.unwrap().num_rows(), 2);
        assert_eq!(del.unwrap().num_rows(), 2);
    }

    #[test]
    fn partition_rejects_unexpected_change_op_value() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int32, false),
            crate::exec::change_op::change_op_field(),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(Int8Array::from(vec![CHANGE_OP_INSERT, 5])) as ArrayRef,
            ],
        )
        .unwrap();
        let chunk = chunk_with(batch);
        let err = partition_chunk_by_change_op(&chunk).unwrap_err();
        assert!(err.contains("unexpected"));
    }

    #[test]
    fn strip_change_op_preserves_branch_and_apply_key_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int32, false),
            crate::exec::change_op::change_op_field(),
            Field::new(
                crate::exec::row_position::ICEBERG_ROW_ID_COL,
                DataType::Int64,
                false,
            ),
            Field::new(
                crate::exec::row_position::ICEBERG_FILE_PATH_COL,
                DataType::Utf8,
                true,
            ),
            Field::new(
                crate::exec::row_position::ICEBERG_ROW_POS_COL,
                DataType::Int64,
                true,
            ),
            Field::new(
                crate::engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN,
                DataType::Int32,
                false,
            ),
            Field::new(
                crate::engine::mv::iceberg_target_apply::ICEBERG_MV_APPLY_KEY_COLUMN,
                DataType::Int64,
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![10])) as ArrayRef,
                Arc::new(Int8Array::from(vec![CHANGE_OP_INSERT])) as ArrayRef,
                Arc::new(Int64Array::from(vec![9001])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("file:///target.parquet")])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(7)])) as ArrayRef,
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
                Arc::new(Int64Array::from(vec![42])) as ArrayRef,
            ],
        )
        .unwrap();

        let stripped = strip_change_op(batch).expect("strip internal columns");
        let stripped_schema = stripped.schema();
        let names = stripped_schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            names,
            vec![
                "v",
                crate::engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN,
                crate::engine::mv::iceberg_target_apply::ICEBERG_MV_APPLY_KEY_COLUMN,
            ]
        );
    }

    fn referenced_partitions_for(
        file: &str,
    ) -> crate::engine::delete_flow::ReferencedDataFilePartitions {
        [(
            file.to_string(),
            crate::engine::delete_flow::ReferencedDataFilePartition {
                partition_spec_id: 0,
                partition_values: iceberg::spec::Struct::empty(),
            },
        )]
        .into_iter()
        .collect()
    }

    fn expect_position_group_error(
        result: Result<Vec<crate::connector::iceberg::commit::PositionDeleteGroup>, String>,
    ) -> String {
        match result {
            Ok(_) => panic!("expected position-delete group extraction to fail"),
            Err(err) => err,
        }
    }

    #[test]
    fn delete_position_groups_reject_missing_file_pos_columns() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            crate::exec::row_position::ICEBERG_FILE_PATH_COL,
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["file:///data-a.parquet"])) as ArrayRef],
        )
        .unwrap();

        let err = expect_position_group_error(position_delete_groups_from_delete_batch_positions(
            &batch,
            &referenced_partitions_for("file:///data-a.parquet"),
        ));

        assert!(
            err.contains(crate::exec::row_position::ICEBERG_ROW_POS_COL),
            "err={err}"
        );
        assert!(err.contains("missing"), "err={err}");
    }

    #[test]
    fn delete_position_groups_reject_null_file_pos_values() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                crate::exec::row_position::ICEBERG_FILE_PATH_COL,
                DataType::Utf8,
                true,
            ),
            Field::new(
                crate::exec::row_position::ICEBERG_ROW_POS_COL,
                DataType::Int64,
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    Some("file:///data-a.parquet"),
                    None,
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(0), Some(1)])) as ArrayRef,
            ],
        )
        .unwrap();

        let err = expect_position_group_error(position_delete_groups_from_delete_batch_positions(
            &batch,
            &referenced_partitions_for("file:///data-a.parquet"),
        ));

        assert!(err.contains("NULL"), "err={err}");
        assert!(
            err.contains(crate::exec::row_position::ICEBERG_FILE_PATH_COL),
            "err={err}"
        );
    }

    #[test]
    fn delete_position_groups_reject_type_mismatch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                crate::exec::row_position::ICEBERG_FILE_PATH_COL,
                DataType::Utf8,
                false,
            ),
            Field::new(
                crate::exec::row_position::ICEBERG_ROW_POS_COL,
                DataType::Utf8,
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["file:///data-a.parquet"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["0"])) as ArrayRef,
            ],
        )
        .unwrap();

        let err = expect_position_group_error(position_delete_groups_from_delete_batch_positions(
            &batch,
            &referenced_partitions_for("file:///data-a.parquet"),
        ));

        assert!(
            err.contains(crate::exec::row_position::ICEBERG_ROW_POS_COL),
            "err={err}"
        );
        assert!(err.contains("Int64"), "err={err}");
    }

    #[test]
    fn delete_position_groups_reject_unknown_target_file() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                crate::exec::row_position::ICEBERG_FILE_PATH_COL,
                DataType::Utf8,
                false,
            ),
            Field::new(
                crate::exec::row_position::ICEBERG_ROW_POS_COL,
                DataType::Int64,
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["file:///missing.parquet"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![0])) as ArrayRef,
            ],
        )
        .unwrap();

        let err = expect_position_group_error(position_delete_groups_from_delete_batch_positions(
            &batch,
            &referenced_partitions_for("file:///data-a.parquet"),
        ));

        assert!(err.contains("missing.parquet"), "err={err}");
        assert!(err.contains("target snapshot metadata"), "err={err}");
    }

    #[test]
    fn nonzero_driver_writes_insert_rows_to_collector() {
        let fixture = data_block_on(
            crate::connector::iceberg::commit::test_helpers::empty_v3_iceberg_table(),
        )
        .expect("iceberg fixture");
        let metadata = fixture.table.metadata();
        let collector = Arc::new(
            IcebergCommitCollector::new(
                crate::connector::iceberg::commit::CommitOpKind::RowDeltaDv,
                fixture.table_ident.clone(),
                metadata.current_snapshot().map(|s| s.snapshot_id()),
                metadata.last_sequence_number(),
                metadata.current_schema().clone(),
                metadata.default_partition_spec().clone(),
                format!("{}/staging", metadata.location()),
                crate::common::types::UniqueId { hi: 1, lo: 2 },
            )
            .with_table_metadata(metadata.clone()),
        );
        let plan = IcebergMergeSinkPlan {
            target_table: fixture.table.clone(),
            collector: Arc::clone(&collector),
        };
        let factory = IcebergMergeSinkFactory::new(plan);
        let mut op = factory.create(4, 2);
        let processor = op.as_processor_mut().expect("processor");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            crate::exec::change_op::change_op_field(),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![101])) as ArrayRef,
                Arc::new(Int8Array::from(vec![CHANGE_OP_INSERT])) as ArrayRef,
            ],
        )
        .unwrap();

        processor
            .push_chunk(&RuntimeState::default(), chunk_with(batch))
            .expect("push insert chunk");
        processor
            .set_finishing(&RuntimeState::default())
            .expect("finish writer");

        assert_eq!(collector.injected_data_record_count(), 1);
    }

    #[test]
    fn delete_rows_with_file_pos_inject_position_delete_groups_without_locator_state() {
        let fixture = data_block_on(
            crate::connector::iceberg::commit::test_helpers::v3_table_with_n_data_files(1),
        )
        .expect("iceberg fixture");
        let metadata = fixture.table.metadata();
        let collector = Arc::new(
            IcebergCommitCollector::new(
                crate::connector::iceberg::commit::CommitOpKind::RowDeltaDv,
                fixture.table_ident.clone(),
                metadata.current_snapshot().map(|s| s.snapshot_id()),
                metadata.last_sequence_number(),
                metadata.current_schema().clone(),
                metadata.default_partition_spec().clone(),
                format!("{}/staging", metadata.location()),
                crate::common::types::UniqueId { hi: 3, lo: 4 },
            )
            .with_table_metadata(metadata.clone()),
        );
        let plan = IcebergMergeSinkPlan {
            target_table: fixture.table.clone(),
            collector: Arc::clone(&collector),
        };
        let mut op = IcebergMergeSinkOperator {
            name: "test merge sink".to_string(),
            plan: Arc::new(plan),
            writer: None,
            finished: false,
        };
        let data_files =
            crate::connector::iceberg::catalog::registry::extract_data_files_with_stats(
                &fixture.table,
            )
            .expect("data files");
        let data_file = data_files
            .first()
            .expect("fixture must contain one data file")
            .path
            .clone();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            crate::exec::change_op::change_op_field(),
            Field::new(
                crate::exec::row_position::ICEBERG_FILE_PATH_COL,
                DataType::Utf8,
                false,
            ),
            Field::new(
                crate::exec::row_position::ICEBERG_ROW_POS_COL,
                DataType::Int64,
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![101])) as ArrayRef,
                Arc::new(Int8Array::from(vec![CHANGE_OP_DELETE])) as ArrayRef,
                Arc::new(StringArray::from(vec![data_file.as_str()])) as ArrayRef,
                Arc::new(Int64Array::from(vec![0])) as ArrayRef,
            ],
        )
        .unwrap();

        op.push_chunk(&RuntimeState::default(), chunk_with(batch))
            .expect("push delete chunk");

        let groups = collector.take_delete_groups();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].referenced_data_file, data_file);
        assert_eq!(groups[0].partition_spec_id, 0);
        assert_eq!(groups[0].partition_values, iceberg::spec::Struct::empty());
        assert_eq!(groups[0].positions, vec![0]);
    }
}
