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
//! Iceberg table sink for writing query results.
//!
//! Responsibilities:
//! - Converts output chunks into Iceberg/Parquet writer input and commits generated data files.
//! - Coordinates partitioning, file rolling, and commit metadata publication semantics.
//!
//! Key exported interfaces:
//! - Types: `IcebergTableSinkFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, Decimal128Array, Int32Array, Int64Array, RecordBatch,
    StringArray, TimestampMicrosecondArray, UInt32Array,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use base64::Engine;
use iceberg::spec::TableMetadata;
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use parquet::basic::Compression;
use parquet::data_type::AsBytes;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::WriterProperties;
use parquet::file::statistics::{Statistics, ValueStatistics};

use super::data_writer::{
    StagedWriteContext, StagedWriteOptions, staged_data_file_to_writer_report, write_record_batches,
};
use crate::common::config;
use crate::connector::iceberg::commit::{
    write_equality_delete_file, write_single_deletion_vector_puffin,
};
use crate::connector::iceberg::delete_file::IcebergFileContent;
use crate::connector::iceberg::report::{
    IcebergColumnStats, IcebergPartitionReport, IcebergWriterReport, IcebergWrittenFileReport,
    partition_path_from_struct,
};
use crate::connector::iceberg::sink_plan::{
    IcebergSinkFactoryInput, IcebergSinkMode, IcebergSinkObjectStoreConfig, IcebergSinkPlan,
};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::pipeline::async_sink::{AsyncSinkBackend, AsyncSinkOperator};
use crate::exec::pipeline::operator::Operator;
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::row_position::{
    ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
    ICEBERG_RESERVED_FIELD_ID_ROW_ID, ICEBERG_ROW_ID_COL,
};
use crate::runtime::global_async_runtime::data_block_on;
use crate::runtime::runtime_state::RuntimeState;

#[derive(Clone)]
/// Factory for Iceberg table sinks that write output chunks into committed table files.
pub struct IcebergTableSinkFactory {
    name: String,
    arena: Arc<ExprArena>,
    plan: Arc<IcebergSinkPlan>,
}

impl IcebergTableSinkFactory {
    pub(crate) fn try_new(input: IcebergSinkFactoryInput) -> Result<Self, String> {
        Ok(Self {
            name: input.name,
            arena: Arc::new(input.arena),
            plan: Arc::new(input.plan),
        })
    }
}

impl IcebergSinkPlan {
    fn build_staged_write_context(&self) -> Result<StagedWriteContext, String> {
        if self.mode != IcebergSinkMode::Data {
            return Err(format!(
                "staged data-file writer context is only valid for DATA sinks, got {:?}",
                self.mode
            ));
        }

        let writer_schema = Arc::new(iceberg_schema_from_arrow_schema(&self.target_schema)?);
        let metadata = self.build_target_table_metadata(writer_schema.as_ref())?;
        let annotated_schema = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(&writer_schema)
                .map_err(|e| format!("convert staged iceberg schema to arrow failed: {e}"))?,
        );
        let file_io = build_staged_file_io(&self.data_location, self.object_store_s3.as_ref())?;

        StagedWriteContext::from_parts_with_partition_spec_id(
            metadata,
            file_io,
            writer_schema,
            annotated_schema,
            self.target_partition_spec_id,
        )
    }

    fn build_target_table_metadata(
        &self,
        writer_schema: &iceberg::spec::Schema,
    ) -> Result<iceberg::spec::TableMetadata, String> {
        let partition_spec = build_staged_partition_spec(
            writer_schema,
            self.target_partition_spec_id,
            &self.partition_source_column_names,
            &self.partition_column_names,
            &self.transform_exprs,
        )?;
        let mut properties = self
            .target_table_metadata
            .as_ref()
            .map(|metadata| metadata.properties().clone())
            .unwrap_or_default();
        properties.insert("write.data.path".to_string(), self.data_location.clone());
        iceberg::spec::TableMetadataBuilder::new(
            writer_schema.clone(),
            iceberg::spec::PartitionSpec::unpartition_spec(),
            iceberg::spec::SortOrder::unsorted_order(),
            self.table_location.clone(),
            iceberg::spec::FormatVersion::V2,
            properties,
        )
        .map_err(|e| format!("build staged iceberg table metadata failed: {e}"))?
        .add_current_schema(writer_schema.clone())
        .map_err(|e| format!("add staged iceberg writer schema failed: {e}"))?
        .add_default_partition_spec(partition_spec)
        .map_err(|e| format!("add staged iceberg partition spec failed: {e}"))?
        .build()
        .map_err(|e| format!("finalize staged iceberg table metadata failed: {e}"))
        .and_then(|built| {
            retag_default_partition_spec_id(
                built.metadata,
                self.target_partition_spec_id,
                &self.partition_column_names,
            )
        })
    }
}

fn delete_target_snapshot_id(
    metadata: &iceberg::spec::TableMetadata,
    target_snapshot_id: Option<i64>,
) -> Option<i64> {
    target_snapshot_id.or_else(|| metadata.current_snapshot_id())
}

fn retag_default_partition_spec_id(
    metadata: iceberg::spec::TableMetadata,
    target_spec_id: i32,
    partition_column_names: &[String],
) -> Result<iceberg::spec::TableMetadata, String> {
    // iceberg-rust may assign a fresh spec id when adding a partition spec to
    // synthetic metadata. Writer reports must carry the target table's real
    // spec id so the commit collector can decode descriptors against target
    // metadata.
    let mut value = serde_json::to_value(metadata)
        .map_err(|e| format!("serialize staged iceberg table metadata failed: {e}"))?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| "staged iceberg table metadata must serialize to an object".to_string())?;
    object.insert(
        "default-spec-id".to_string(),
        serde_json::Value::from(target_spec_id),
    );

    let specs = object
        .get_mut("partition-specs")
        .and_then(serde_json::Value::as_array_mut)
        .ok_or_else(|| "staged iceberg table metadata missing partition-specs array".to_string())?;
    let desired_idx = specs
        .iter()
        .position(|spec| partition_spec_names_match(spec, partition_column_names))
        .ok_or_else(|| {
            format!(
                "staged iceberg table metadata missing partition spec for fields {:?}",
                partition_column_names
            )
        })?;
    let mut desired_spec = specs[desired_idx].clone();
    let spec_object = desired_spec
        .as_object_mut()
        .ok_or_else(|| "staged iceberg partition spec must serialize to an object".to_string())?;
    spec_object.insert(
        "spec-id".to_string(),
        serde_json::Value::from(target_spec_id),
    );
    *specs = vec![desired_spec];

    let metadata = serde_json::from_value::<iceberg::spec::TableMetadata>(value)
        .map_err(|e| format!("deserialize staged iceberg table metadata failed: {e}"))?;
    let spec = metadata.partition_spec_by_id(target_spec_id).ok_or_else(|| {
        format!(
            "staged iceberg table metadata failed to retag default partition spec id to {target_spec_id}"
        )
    })?;
    if metadata.default_partition_spec_id() != target_spec_id
        || !partition_spec_ref_names_match(spec, partition_column_names)
    {
        return Err(format!(
            "staged iceberg table metadata default partition spec does not match fields {:?}",
            partition_column_names
        ));
    }
    Ok(metadata)
}

fn partition_spec_names_match(spec: &serde_json::Value, partition_column_names: &[String]) -> bool {
    let Some(spec_object) = spec.as_object() else {
        return false;
    };
    let Some(fields) = spec_object
        .get("fields")
        .and_then(serde_json::Value::as_array)
    else {
        return partition_column_names.is_empty();
    };
    if fields.len() != partition_column_names.len() {
        return false;
    }
    fields
        .iter()
        .zip(partition_column_names.iter())
        .all(|(field, expected)| {
            field
                .as_object()
                .and_then(|object| object.get("name"))
                .and_then(serde_json::Value::as_str)
                == Some(expected.as_str())
        })
}

fn partition_spec_ref_names_match(
    spec: &iceberg::spec::PartitionSpecRef,
    partition_column_names: &[String],
) -> bool {
    spec.fields().len() == partition_column_names.len()
        && spec
            .fields()
            .iter()
            .zip(partition_column_names.iter())
            .all(|(field, expected)| field.name == *expected)
}

impl OperatorFactory for IcebergTableSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        Box::new(self.create_async_operator(driver_id))
    }

    fn is_sink(&self) -> bool {
        true
    }
}

impl IcebergTableSinkFactory {
    fn create_async_operator(&self, driver_id: i32) -> AsyncSinkOperator<IcebergTableSinkBackend> {
        AsyncSinkOperator::new(
            self.name.clone(),
            IcebergTableSinkBackend {
                arena: Arc::clone(&self.arena),
                plan: Arc::clone(&self.plan),
                driver_id,
                file_seq: 0,
                runtime_state: None,
                pending_deletion_vectors: BTreeMap::new(),
            },
            config::async_sink_queue_capacity(),
        )
    }
}

struct IcebergTableSinkBackend {
    arena: Arc<ExprArena>,
    plan: Arc<IcebergSinkPlan>,
    driver_id: i32,
    file_seq: u64,
    runtime_state: Option<RuntimeState>,
    pending_deletion_vectors: BTreeMap<String, crate::connector::iceberg::commit::DeletionVector>,
}

#[async_trait::async_trait]
impl AsyncSinkBackend for IcebergTableSinkBackend {
    type Output = ();

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> Result<(), String> {
        self.runtime_state = Some(state.clone());
        Ok(())
    }

    async fn write_chunk(&mut self, chunk: Chunk) -> Result<(), String> {
        if chunk.is_empty() {
            return Ok(());
        }
        let state = self
            .runtime_state
            .clone()
            .ok_or_else(|| "iceberg async sink backend missing runtime state".to_string())?;
        match self.plan.mode {
            IcebergSinkMode::Data => self.push_chunk_data(&state, chunk).await,
            IcebergSinkMode::PositionDeletes => self.push_chunk_position_delete(&state, chunk),
            IcebergSinkMode::DeletionVectors => self.push_chunk_deletion_vector(&state, chunk),
            IcebergSinkMode::EqualityDeletes => {
                self.push_chunk_equality_delete(&state, chunk).await
            }
        }
    }

    async fn finish(&mut self) -> Result<(), String> {
        if self.plan.mode == IcebergSinkMode::DeletionVectors {
            return self.finish_deletion_vectors();
        }
        Ok(())
    }
}

impl IcebergTableSinkBackend {
    fn build_file_path_with_prefix(
        &mut self,
        state: &RuntimeState,
        partition: &str,
        prefix: &str,
    ) -> Result<(String, String), String> {
        // Preserve the caller-supplied URI scheme in the data file path. The
        // report partition_path remains Iceberg-relative for layout/debug
        // compatibility; commit partition values are descriptor-authoritative.
        let base = self.plan.data_location.trim_end_matches('/').to_string();
        let finst = state
            .fragment_instance_id()
            .map(|id| format!("{:x}_{:x}", id.hi, id.lo))
            .unwrap_or_else(|| "finst_unknown".to_string());
        let file_name = format!(
            "{}-{}-driver{}-{}.parquet",
            prefix, finst, self.driver_id, self.file_seq
        );
        self.file_seq = self.file_seq.saturating_add(1);

        if partition.is_empty() {
            let path = format!("{base}/{file_name}");
            Ok((path, String::new()))
        } else {
            let partition_path = partition.trim_matches('/').to_string();
            let path = format!("{base}/{partition_path}/{file_name}");
            Ok((path, partition_path))
        }
    }

    async fn push_chunk_data(&mut self, state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if self.plan.row_lineage_data {
            return self.push_chunk_row_lineage_data(state, chunk).await;
        }

        let output_arrays = eval_exprs(&self.arena, &self.plan.output_exprs, &chunk)?;
        let output_arrays = align_arrays_to_schema(output_arrays, &self.plan.output_schema)?;
        let batch = RecordBatch::try_new(Arc::clone(&self.plan.output_schema), output_arrays)
            .map_err(|e| format!("iceberg sink build batch failed: {e}"))?;

        let partition_groups = self.partition_group_indices(&chunk, batch.num_rows())?;
        let staged_ctx = self.plan.build_staged_write_context()?;
        let staged_opts = StagedWriteOptions {
            collect_theta_sketches: true,
            ..StagedWriteOptions::default()
        };

        for (key, group) in partition_groups {
            let indices = UInt32Array::from(group.indices);
            let part_batch = arrow::compute::take_record_batch(&batch, &indices)
                .map_err(|e| format!("iceberg sink take batch failed: {e}"))?;
            if part_batch.num_rows() == 0 {
                continue;
            }
            let staged_files =
                write_record_batches(&staged_ctx, [part_batch], &staged_opts).await?;
            let partition_path = self.partition_path_for_key(&key);
            for staged in staged_files {
                let partition_report = IcebergPartitionReport {
                    partition_path: partition_path.clone(),
                    null_fingerprint: key.null_fingerprint.clone(),
                    partition_spec_id: staged.partition_spec_id,
                    partition_values: staged.data_file.partition().clone(),
                };
                let (report, sketch_set) = staged_data_file_to_writer_report(
                    &staged,
                    partition_report,
                    self.plan.report_file_format.clone(),
                    IcebergFileContent::Data,
                )?;
                if let Some(sketch_set) = sketch_set {
                    state.add_iceberg_sketch_set(sketch_set);
                }
                let metadata = staged.metadata.as_ref();
                state.add_iceberg_writer_report(report, metadata)?;
            }
        }

        Ok(())
    }

    async fn push_chunk_row_lineage_data(
        &mut self,
        state: &RuntimeState,
        chunk: Chunk,
    ) -> Result<(), String> {
        let output_arrays = eval_exprs(&self.arena, &self.plan.output_exprs, &chunk)?;
        let output_arrays = align_arrays_to_schema(output_arrays, &self.plan.output_schema)?;
        let batch = RecordBatch::try_new(Arc::clone(&self.plan.output_schema), output_arrays)
            .map_err(|e| format!("iceberg row-lineage sink build batch failed: {e}"))?;

        let row_id_idx = row_lineage_row_id_index(&self.plan.output_schema)?;
        let row_id_col = batch
            .column(row_id_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| "iceberg row-lineage sink _row_id column must be Int64".to_string())?;
        if row_id_col.null_count() > 0 {
            return Err("iceberg row-lineage sink rejects NULL _row_id".to_string());
        }

        let partition_groups = self.partition_group_indices(&chunk, batch.num_rows())?;
        let staged_ctx = self.plan.build_staged_write_context()?;
        let staged_opts = StagedWriteOptions::default();

        for (key, group) in partition_groups {
            let mut indices = group.indices;
            indices.sort_by_key(|idx| row_id_col.value(*idx as usize));
            let partition_path = self.partition_path_for_key(&key);
            let mut start = 0usize;
            while start < indices.len() {
                let first_idx = indices[start] as usize;
                let first_row_id = row_id_col.value(first_idx);
                if first_row_id < 0 {
                    return Err(format!(
                        "iceberg row-lineage sink _row_id must be non-negative: {first_row_id}"
                    ));
                }
                let mut end = start + 1;
                let mut previous = first_row_id;
                while end < indices.len() {
                    let row_id = row_id_col.value(indices[end] as usize);
                    if row_id < 0 {
                        return Err(format!(
                            "iceberg row-lineage sink _row_id must be non-negative: {row_id}"
                        ));
                    }
                    if row_id == previous {
                        return Err(format!(
                            "iceberg row-lineage sink encountered duplicate _row_id {row_id}"
                        ));
                    }
                    if row_id != previous + 1 {
                        break;
                    }
                    previous = row_id;
                    end += 1;
                }

                let run_indices = UInt32Array::from(indices[start..end].to_vec());
                let part_batch = arrow::compute::take_record_batch(&batch, &run_indices)
                    .map_err(|e| format!("iceberg row-lineage sink take batch failed: {e}"))?;
                if part_batch.num_rows() == 0 {
                    start = end;
                    continue;
                }
                let staged_files = write_record_batches(&staged_ctx, [part_batch], &staged_opts)
                    .await
                    .map_err(|e| format!("iceberg row-lineage sink write failed: {e}"))?;
                let mut first_row_id_cursor = first_row_id;
                for staged in staged_files {
                    let partition_report = IcebergPartitionReport {
                        partition_path: partition_path.clone(),
                        null_fingerprint: key.null_fingerprint.clone(),
                        partition_spec_id: staged.partition_spec_id,
                        partition_values: staged.data_file.partition().clone(),
                    };
                    let (mut report, sketch_set) = staged_data_file_to_writer_report(
                        &staged,
                        partition_report,
                        self.plan.report_file_format.clone(),
                        IcebergFileContent::Data,
                    )?;
                    report.file.first_row_id = Some(first_row_id_cursor);
                    first_row_id_cursor = first_row_id_cursor
                        .checked_add(report.file.record_count)
                        .ok_or_else(|| {
                            "iceberg row-lineage sink first_row_id overflow".to_string()
                        })?;
                    if let Some(sketch_set) = sketch_set {
                        state.add_iceberg_sketch_set(sketch_set);
                    }
                    let metadata = staged.metadata.as_ref();
                    state.add_iceberg_writer_report(report, metadata)?;
                }
                start = end;
            }
        }

        Ok(())
    }

    fn build_file_pos_batch(&self, chunk: &Chunk, sink_label: &str) -> Result<RecordBatch, String> {
        let all_output_arrays = eval_exprs(&self.arena, &self.plan.output_exprs, chunk)?;
        if all_output_arrays.len() < 2 {
            return Err(format!(
                "iceberg {sink_label} sink expected at least 2 output arrays, got {}",
                all_output_arrays.len()
            ));
        }
        let file_path_pos_arrays = vec![all_output_arrays[0].clone(), all_output_arrays[1].clone()];
        let delete_arrays = align_arrays_to_schema(file_path_pos_arrays, &self.plan.output_schema)?;
        RecordBatch::try_new(Arc::clone(&self.plan.output_schema), delete_arrays)
            .map_err(|e| format!("iceberg {sink_label} sink build batch failed: {e}"))
    }

    fn push_chunk_position_delete(
        &mut self,
        state: &RuntimeState,
        chunk: Chunk,
    ) -> Result<(), String> {
        // output_exprs is [file_path_expr, pos_expr, partition_source_expr_0, ...].
        // We only materialize the first two into the parquet file; the remaining
        // expressions are kept around so that `plan.partition_exprs` (which were
        // rewritten to reference the partition source SLOT_REFs in try_new) can
        // be evaluated against `chunk` on the existing DATA-sink code path.
        let batch = self.build_file_pos_batch(&chunk, "position-delete")?;

        let partition_groups = self.position_delete_partition_group_indices(&batch)?;
        let descriptor_metadata = self
            .plan
            .target_table_metadata
            .as_ref()
            .ok_or_else(|| {
                "iceberg position-delete sink missing target table metadata".to_string()
            })?
            .clone();

        for (key, group) in partition_groups {
            // Sort indices by (file_path, pos) to produce a well-ordered
            // position-delete file. Iceberg readers rely on this ordering to do
            // binary search when the referenced data file is uniform; mixed-file
            // delete files still benefit from locality.
            let mut sortable: Vec<u32> = group.indices;
            let file_path_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    "iceberg position-delete sink: file_path array expected as Utf8".to_string()
                })?;
            let pos_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    "iceberg position-delete sink: pos array expected as Int64".to_string()
                })?;
            if file_path_col.null_count() > 0 || pos_col.null_count() > 0 {
                return Err(
                    "iceberg position-delete sink rejects NULL file_path or pos".to_string()
                );
            }
            sortable.sort_by(|a, b| {
                let a = *a as usize;
                let b = *b as usize;
                match file_path_col.value(a).cmp(file_path_col.value(b)) {
                    Ordering::Equal => pos_col.value(a).cmp(&pos_col.value(b)),
                    other => other,
                }
            });

            let indices = UInt32Array::from(sortable);
            let part_batch = arrow::compute::take_record_batch(&batch, &indices)
                .map_err(|e| format!("iceberg position-delete sink take batch failed: {e}"))?;
            if part_batch.num_rows() == 0 {
                continue;
            }
            let (file_path, partition_path) =
                self.build_file_path_with_prefix(state, &key.path, "delete")?;
            let write_result = write_parquet_file(
                &file_path,
                self.plan.object_store_s3.as_ref(),
                Arc::clone(&self.plan.output_schema),
                &part_batch,
                self.plan.compression,
            )?;

            // Iceberg spec allows `referenced_data_file` to be populated only
            // when every position in this delete file points at the same data
            // file. Readers use it to prune delete files at plan time.
            let referenced_data_file = unique_file_path(&part_batch)?;
            let record_count = i64::try_from(part_batch.num_rows()).map_err(|_| {
                format!(
                    "iceberg position delete record_count {} overflows i64",
                    part_batch.num_rows()
                )
            })?;
            let file_size_in_bytes = i64::try_from(write_result.file_size).map_err(|_| {
                format!(
                    "iceberg position delete file_size_in_bytes {} overflows i64",
                    write_result.file_size
                )
            })?;

            let report = IcebergWriterReport {
                file: IcebergWrittenFileReport {
                    path: file_path,
                    format: self.plan.report_file_format.clone(),
                    content: IcebergFileContent::PositionDeletes,
                    record_count,
                    file_size_in_bytes,
                    partition: IcebergPartitionReport {
                        partition_path,
                        null_fingerprint: key.null_fingerprint.clone(),
                        partition_spec_id: group.partition_spec_id,
                        partition_values: group.partition_values.clone(),
                    },
                    split_offsets: write_result.split_offsets,
                    column_stats: write_result.column_stats,
                    referenced_data_file,
                    first_row_id: None,
                    equality_ids: None,
                    key_metadata: None,
                    content_offset: None,
                    content_size_in_bytes: None,
                    cardinality: None,
                },
                is_overwrite: None,
                is_rewrite: None,
            };
            state.add_iceberg_writer_report(report, &descriptor_metadata)?;
        }

        Ok(())
    }

    fn push_chunk_deletion_vector(
        &mut self,
        _state: &RuntimeState,
        chunk: Chunk,
    ) -> Result<(), String> {
        let batch = self.build_file_pos_batch(&chunk, "deletion-vector")?;
        let positions_by_file = group_positions_by_file(&batch, "deletion-vector")?;
        let vectors = merge_deletion_vectors_by_file(HashMap::new(), &positions_by_file)?;
        for (referenced_data_file, dv) in vectors {
            self.pending_deletion_vectors
                .entry(referenced_data_file)
                .or_default()
                .merge(&dv);
        }
        Ok(())
    }

    fn finish_deletion_vectors(&mut self) -> Result<(), String> {
        if self.pending_deletion_vectors.is_empty() {
            return Ok(());
        }
        let state = self.runtime_state.clone().ok_or_else(|| {
            "iceberg deletion-vector sink missing runtime state during finish".to_string()
        })?;
        let metadata = self
            .plan
            .target_table_metadata
            .as_ref()
            .ok_or_else(|| {
                "iceberg deletion-vector sink missing target table metadata".to_string()
            })?
            .clone();
        // Iceberg FileIO opens absolute Iceberg paths; `data_location` only
        // selects the same storage backend/credentials as staged DV writes.
        // The existing-delete read factory itself is rooted from table metadata.
        let file_io =
            build_staged_file_io(&self.plan.data_location, self.plan.object_store_s3.as_ref())?;
        let existing = self.read_existing_dv_positions(
            &metadata,
            &file_io,
            self.pending_deletion_vectors.keys().map(String::as_str),
        )?;
        let vectors =
            merge_existing_with_pending_deletion_vectors(existing, &self.pending_deletion_vectors);

        for (referenced_data_file, dv) in vectors {
            let mut partition_report = self.referenced_data_file_partition_report(
                &referenced_data_file,
                &metadata,
                "deletion-vector",
            )?;
            let (parquet_path, report_partition_path) =
                self.build_file_path_with_prefix(&state, &partition_report.partition_path, "dv")?;
            partition_report.partition_path = report_partition_path;
            let puffin_path = parquet_path
                .strip_suffix(".parquet")
                .ok_or_else(|| {
                    format!(
                        "iceberg deletion-vector sink generated non-parquet staging path `{parquet_path}`"
                    )
                })?
                .to_string()
                + ".puffin";
            let written = data_block_on(write_single_deletion_vector_puffin(
                &file_io,
                &puffin_path,
                &referenced_data_file,
                &dv,
            ))
            .map_err(|e| format!("iceberg deletion-vector sink wait for Puffin write failed: {e}"))?
            .map_err(|e| format!("iceberg deletion-vector sink write Puffin failed: {e}"))?;
            let cardinality = i64::try_from(written.cardinality).map_err(|_| {
                format!(
                    "iceberg deletion-vector sink cardinality {} overflows i64",
                    written.cardinality
                )
            })?;
            let file_size_in_bytes = i64::try_from(written.file_size_in_bytes).map_err(|_| {
                format!(
                    "iceberg deletion-vector sink Puffin file size {} overflows i64",
                    written.file_size_in_bytes
                )
            })?;

            let report = IcebergWriterReport {
                file: IcebergWrittenFileReport {
                    path: written.path,
                    format: "puffin".to_string(),
                    content: IcebergFileContent::PositionDeletes,
                    record_count: cardinality,
                    file_size_in_bytes,
                    partition: partition_report,
                    split_offsets: None,
                    column_stats: None,
                    referenced_data_file: Some(written.referenced_data_file),
                    first_row_id: None,
                    equality_ids: None,
                    key_metadata: None,
                    content_offset: Some(written.content_offset),
                    content_size_in_bytes: Some(written.content_size_in_bytes),
                    cardinality: Some(cardinality),
                },
                is_overwrite: None,
                is_rewrite: None,
            };
            state.add_iceberg_writer_report(report, &metadata)?;
        }

        self.pending_deletion_vectors.clear();
        Ok(())
    }

    async fn push_chunk_equality_delete(
        &mut self,
        state: &RuntimeState,
        chunk: Chunk,
    ) -> Result<(), String> {
        if self.plan.equality_delete_columns.is_empty() {
            return Err(
                "iceberg equality-delete sink requires at least one equality column".to_string(),
            );
        }
        if !self.plan.partition_exprs.is_empty() || !self.plan.partition_column_names.is_empty() {
            return Err(
                "iceberg equality-delete sink currently supports only unpartitioned tables"
                    .to_string(),
            );
        }

        let output_arrays = eval_exprs(&self.arena, &self.plan.output_exprs, &chunk)?;
        let output_arrays = align_arrays_to_schema(output_arrays, &self.plan.output_schema)?;
        let batch = RecordBatch::try_new(Arc::clone(&self.plan.output_schema), output_arrays)
            .map_err(|e| format!("iceberg equality-delete sink build batch failed: {e}"))?;
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let file_io =
            build_staged_file_io(&self.plan.data_location, self.plan.object_store_s3.as_ref())?;
        let staging_dir = self.plan.data_location.trim_end_matches('/');
        let written = write_equality_delete_file(
            &file_io,
            staging_dir,
            self.plan.target_partition_spec_id,
            self.plan.equality_delete_columns.clone(),
            batch,
        )
        .await?;
        let Some(written) = written else {
            return Ok(());
        };

        let metadata = self.build_equality_delete_report_metadata()?;
        let report = crate::connector::iceberg::report::writer_report_from_written_file(
            &written, &metadata,
        )?;
        state.add_iceberg_writer_report(report, &metadata)?;
        Ok(())
    }

    fn build_equality_delete_report_metadata(&self) -> Result<TableMetadata, String> {
        let writer_schema = iceberg_schema_from_arrow_schema(&self.plan.output_schema)?;
        self.plan.build_target_table_metadata(&writer_schema)
    }

    fn read_existing_dv_positions<'a>(
        &self,
        metadata: &iceberg::spec::TableMetadata,
        file_io: &iceberg::io::FileIO,
        owned_files: impl IntoIterator<Item = &'a str>,
    ) -> Result<HashMap<String, crate::connector::iceberg::commit::DeletionVector>, String> {
        let Some(snapshot_id) = delete_target_snapshot_id(metadata, self.plan.target_snapshot_id)
        else {
            return Ok(HashMap::new());
        };
        let object_store_cfg = self
            .plan
            .object_store_s3
            .as_ref()
            .map(IcebergSinkObjectStoreConfig::to_object_store_config);
        let table = iceberg::table::Table::builder()
            .file_io(file_io.clone())
            .metadata(Arc::new(metadata.clone()))
            .identifier(
                iceberg::TableIdent::from_strs(["novarocks", "sink"])
                    .map_err(|e| format!("build iceberg deletion-vector sink table ident: {e}"))?,
            )
            .build()
            .map_err(|e| format!("build iceberg deletion-vector sink table: {e}"))?;
        let factory = crate::connector::iceberg::changes::build_factory_for_table(
            &table,
            object_store_cfg.as_ref(),
        )?;
        let expected_object_store_bucket =
            crate::connector::iceberg::changes::expected_object_store_bucket_for_table(&table)?;
        let owned_files: HashSet<&str> = owned_files.into_iter().collect();
        let existing = crate::connector::iceberg::scan_deletes::previously_deleted_positions_at_snapshot(
            &table,
            snapshot_id,
            &factory,
            &|path| crate::connector::iceberg::changes::normalize_delete_projection_path(
                path,
                object_store_cfg.as_ref(),
                expected_object_store_bucket.as_deref(),
            ),
            |path| owned_files.contains(path),
        )
        .map_err(|e| {
            format!(
                "iceberg deletion-vector sink read existing delete positions at snapshot {snapshot_id} failed: {e}"
            )
        })?;

        existing
            .into_iter()
            .map(|(file, positions)| {
                let mut dv = crate::connector::iceberg::commit::DeletionVector::new();
                for pos in positions {
                    dv.insert(pos).map_err(|e| {
                        format!(
                            "iceberg deletion-vector sink convert existing position {pos} for `{file}` failed: {e}"
                        )
                    })?;
                }
                Ok((file, dv))
            })
            .collect()
    }

    fn referenced_data_file_partition_report(
        &self,
        referenced_data_file: &str,
        metadata: &iceberg::spec::TableMetadata,
        sink_label: &str,
    ) -> Result<IcebergPartitionReport, String> {
        let partition = self
            .plan
            .position_delete_data_file_partitions
            .get(referenced_data_file)
            .ok_or_else(|| {
                format!(
                    "iceberg {sink_label} sink missing partition metadata for referenced data file `{referenced_data_file}`"
                )
            })?;
        let partition_spec = metadata
            .partition_spec_by_id(partition.partition_spec_id)
            .ok_or_else(|| {
                format!(
                    "iceberg {sink_label} sink referenced data file `{referenced_data_file}` uses unknown partition spec id {}",
                    partition.partition_spec_id
                )
            })?;
        let (partition_path, null_fingerprint) =
            partition_path_from_struct(&partition.partition_values, partition_spec)?;
        Ok(IcebergPartitionReport {
            partition_path,
            null_fingerprint,
            partition_spec_id: partition.partition_spec_id,
            partition_values: partition.partition_values.clone(),
        })
    }

    fn position_delete_partition_group_indices(
        &self,
        batch: &RecordBatch,
    ) -> Result<HashMap<PartitionKey, PartitionGroup>, String> {
        let metadata = self.plan.target_table_metadata.as_ref().ok_or_else(|| {
            "iceberg position-delete sink missing target table metadata".to_string()
        })?;
        let file_path_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                "iceberg position-delete sink: file_path array expected as Utf8".to_string()
            })?;
        if file_path_col.null_count() > 0 {
            return Err("iceberg position-delete sink rejects NULL file_path".to_string());
        }

        let mut partition_groups = HashMap::new();
        for row in 0..batch.num_rows() {
            let referenced_data_file = file_path_col.value(row);
            let partition = self
                .plan
                .position_delete_data_file_partitions
                .get(referenced_data_file)
                .ok_or_else(|| {
                    format!(
                        "iceberg position-delete sink missing partition metadata for referenced data file `{referenced_data_file}`"
                    )
                })?;
            let partition_spec = metadata
                .partition_spec_by_id(partition.partition_spec_id)
                .ok_or_else(|| {
                    format!(
                        "iceberg position-delete sink referenced data file `{referenced_data_file}` uses unknown partition spec id {}",
                        partition.partition_spec_id
                    )
                })?;
            let (path, null_fingerprint) =
                partition_path_from_struct(&partition.partition_values, partition_spec)?;
            let partition_key = format!("{:?}", partition.partition_values);
            let key = PartitionKey {
                partition_spec_id: partition.partition_spec_id,
                path,
                null_fingerprint,
                partition_key,
            };
            partition_groups
                .entry(key)
                .or_insert_with(|| PartitionGroup {
                    indices: Vec::new(),
                    partition_spec_id: partition.partition_spec_id,
                    partition_values: partition.partition_values.clone(),
                })
                .indices
                .push(row as u32);
        }
        Ok(partition_groups)
    }

    fn partition_path_for_key(&self, key: &PartitionKey) -> String {
        key.path.trim_matches('/').to_string()
    }

    fn partition_group_indices(
        &self,
        chunk: &Chunk,
        num_rows: usize,
    ) -> Result<HashMap<PartitionKey, PartitionGroup>, String> {
        let mut partition_groups = HashMap::new();
        if self.plan.partition_exprs.is_empty() {
            partition_groups.insert(
                PartitionKey::default(),
                PartitionGroup {
                    indices: (0..num_rows as u32).collect(),
                    partition_spec_id: self.plan.target_partition_spec_id,
                    partition_values: iceberg::spec::Struct::empty(),
                },
            );
            return Ok(partition_groups);
        }
        let partition_arrays = eval_exprs(&self.arena, &self.plan.partition_exprs, chunk)?;
        for row in 0..num_rows {
            let (partition, fingerprint, partition_values) = iceberg_partition_key_for_row(
                &self.plan.partition_column_names,
                &self.plan.transform_exprs,
                &partition_arrays,
                row,
            )?;
            let key = PartitionKey {
                partition_spec_id: self.plan.target_partition_spec_id,
                path: partition,
                null_fingerprint: fingerprint,
                partition_key: format!("{:?}", partition_values),
            };
            partition_groups
                .entry(key)
                .or_insert_with(|| PartitionGroup {
                    indices: Vec::new(),
                    partition_spec_id: self.plan.target_partition_spec_id,
                    partition_values,
                })
                .indices
                .push(row as u32);
        }
        Ok(partition_groups)
    }
}

/// Returns the single file_path referenced by every row in a position-delete
/// batch, or `None` when multiple distinct data files are referenced.
///
/// Iceberg spec `referenced_data_file` is only safe to populate in the
/// uniform-reference case.
fn unique_file_path(batch: &RecordBatch) -> Result<Option<String>, String> {
    if batch.num_rows() == 0 {
        return Ok(None);
    }
    let file_path_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "position-delete batch missing file_path Utf8 column".to_string())?;
    let first = file_path_col.value(0);
    for row in 1..batch.num_rows() {
        if file_path_col.value(row) != first {
            return Ok(None);
        }
    }
    Ok(Some(first.to_string()))
}

fn group_positions_by_file(
    batch: &RecordBatch,
    sink_label: &str,
) -> Result<BTreeMap<String, Vec<u64>>, String> {
    let file_path_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| format!("iceberg {sink_label} sink: file_path array expected as Utf8"))?;
    let pos_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| format!("iceberg {sink_label} sink: pos array expected as Int64"))?;
    if file_path_col.null_count() > 0 || pos_col.null_count() > 0 {
        return Err(format!(
            "iceberg {sink_label} sink rejects NULL file_path or pos"
        ));
    }

    let mut out: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    for row in 0..batch.num_rows() {
        let pos = pos_col.value(row);
        if pos < 0 {
            return Err(format!(
                "iceberg {sink_label} sink pos must be non-negative: {pos}"
            ));
        }
        out.entry(file_path_col.value(row).to_string())
            .or_default()
            .push(pos as u64);
    }
    Ok(out)
}

fn merge_deletion_vectors_by_file(
    mut existing: HashMap<String, crate::connector::iceberg::commit::DeletionVector>,
    positions_by_file: &BTreeMap<String, Vec<u64>>,
) -> Result<BTreeMap<String, crate::connector::iceberg::commit::DeletionVector>, String> {
    let mut out = BTreeMap::new();
    for (file, positions) in positions_by_file {
        let mut dv = existing.remove(file).unwrap_or_default();
        for pos in positions {
            dv.insert(*pos).map_err(|e| {
                format!(
                    "iceberg deletion-vector sink insert position {pos} for `{file}` failed: {e}"
                )
            })?;
        }
        out.insert(file.clone(), dv);
    }
    Ok(out)
}

fn merge_existing_with_pending_deletion_vectors(
    mut existing: HashMap<String, crate::connector::iceberg::commit::DeletionVector>,
    pending: &BTreeMap<String, crate::connector::iceberg::commit::DeletionVector>,
) -> BTreeMap<String, crate::connector::iceberg::commit::DeletionVector> {
    let mut out = BTreeMap::new();
    for (file, pending_dv) in pending {
        let mut dv = existing.remove(file).unwrap_or_default();
        dv.merge(pending_dv);
        out.insert(file.clone(), dv);
    }
    out
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
struct PartitionKey {
    partition_spec_id: i32,
    path: String,
    null_fingerprint: String,
    partition_key: String,
}

#[derive(Debug)]
struct PartitionGroup {
    indices: Vec<u32>,
    partition_spec_id: i32,
    partition_values: iceberg::spec::Struct,
}

struct ParquetWriteResult {
    file_size: u64,
    split_offsets: Option<Vec<i64>>,
    column_stats: Option<IcebergColumnStats>,
    /// Per-primitive-column Theta sketches keyed by Iceberg field id. None
    /// when the sink could not compute sketches (e.g. schema lacks parquet
    /// field-id metadata). Wrapped in `Option` so the existing
    /// position-delete path that does not produce sketches stays cheap.
    ///
    /// NOTE: this field is populated but not yet consumed by the commit
    /// action plumbing (Phase 2.3). The `dead_code` lint is suppressed
    /// because the field is intentionally written ahead of its reader.
    #[allow(dead_code)]
    theta_sketches: Option<HashMap<i32, super::theta_sketch::ThetaSketchHandle>>,
}

#[derive(Default)]
struct ColumnStatsAccumulator {
    column_size: i64,
    value_count: i64,
    null_value_count: i64,
    has_statistics: bool,
    merged_statistics: Option<Statistics>,
}

fn eval_exprs(arena: &ExprArena, exprs: &[ExprId], chunk: &Chunk) -> Result<Vec<ArrayRef>, String> {
    let mut out = Vec::with_capacity(exprs.len());
    for expr in exprs {
        out.push(arena.eval(*expr, chunk)?);
    }
    Ok(out)
}

fn align_arrays_to_schema(
    arrays: Vec<ArrayRef>,
    schema: &SchemaRef,
) -> Result<Vec<ArrayRef>, String> {
    if arrays.len() != schema.fields().len() {
        return Err(format!(
            "iceberg sink column count mismatch while aligning arrays: arrays={} schema={}",
            arrays.len(),
            schema.fields().len()
        ));
    }

    arrays
        .into_iter()
        .zip(schema.fields().iter())
        .enumerate()
        .map(|(idx, (array, field))| {
            let target_type = field.data_type();
            if array.data_type() == target_type {
                return Ok(array);
            }

            let casted = cast(array.as_ref(), target_type).map_err(|e| {
                format!(
                    "iceberg sink cast failed at column index {} name={} from {:?} to {:?}: {}",
                    idx,
                    field.name(),
                    array.data_type(),
                    target_type,
                    e
                )
            })?;

            if !matches!(array.data_type(), DataType::Null)
                && casted.null_count() > array.null_count()
            {
                return Err(format!(
                    "iceberg sink cast introduced nulls at column index {} name={} from {:?} to {:?}",
                    idx,
                    field.name(),
                    array.data_type(),
                    target_type
                ));
            }
            Ok(casted)
        })
        .collect()
}

fn iceberg_schema_from_arrow_schema(schema: &Schema) -> Result<iceberg::spec::Schema, String> {
    let fields = schema
        .fields()
        .iter()
        .map(|field| iceberg_nested_field_from_arrow_field(field.as_ref()))
        .collect::<Result<Vec<_>, _>>()?;
    iceberg::spec::Schema::builder()
        .with_schema_id(1)
        .with_fields(fields)
        .build()
        .map_err(|e| format!("build staged iceberg writer schema failed: {e}"))
}

fn iceberg_nested_field_from_arrow_field(
    field: &Field,
) -> Result<iceberg::spec::NestedFieldRef, String> {
    let field_id = arrow_field_id(field)?;
    let field_type = iceberg_type_from_arrow_type(field.data_type())?;
    Ok(Arc::new(iceberg::spec::NestedField::new(
        field_id,
        field.name(),
        field_type,
        !field.is_nullable(),
    )))
}

fn arrow_field_id(field: &Field) -> Result<i32, String> {
    let raw = field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .ok_or_else(|| {
            format!(
                "iceberg sink field {} is missing parquet field id metadata",
                field.name()
            )
        })?;
    raw.parse::<i32>().map_err(|e| {
        format!(
            "iceberg sink field {} has invalid parquet field id {raw}: {e}",
            field.name()
        )
    })
}

fn schema_has_reserved_row_lineage_columns(schema: &Schema) -> Result<bool, String> {
    let mut has_row_id = false;
    let mut has_last_updated = false;
    for field in schema.fields() {
        if field.name().eq_ignore_ascii_case(ICEBERG_ROW_ID_COL) {
            has_row_id = matches!(arrow_field_id(field), Ok(ICEBERG_RESERVED_FIELD_ID_ROW_ID));
        } else if field
            .name()
            .eq_ignore_ascii_case(ICEBERG_LAST_UPDATED_SEQ_COL)
        {
            has_last_updated = matches!(
                arrow_field_id(field),
                Ok(ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER)
            );
        }
    }
    Ok(has_row_id && has_last_updated)
}

fn row_lineage_row_id_index(schema: &Schema) -> Result<usize, String> {
    for (idx, field) in schema.fields().iter().enumerate() {
        if field.name().eq_ignore_ascii_case(ICEBERG_ROW_ID_COL)
            && arrow_field_id(field)? == ICEBERG_RESERVED_FIELD_ID_ROW_ID
        {
            return Ok(idx);
        }
    }
    Err("iceberg row-lineage sink missing reserved _row_id field".to_string())
}

fn iceberg_type_from_arrow_type(data_type: &DataType) -> Result<iceberg::spec::Type, String> {
    use arrow::datatypes::TimeUnit;
    use iceberg::spec::{ListType, MapType, PrimitiveType, StructType, Type};

    let primitive = match data_type {
        DataType::Boolean => Some(PrimitiveType::Boolean),
        DataType::Int8 | DataType::Int16 | DataType::Int32 => Some(PrimitiveType::Int),
        DataType::Int64 => Some(PrimitiveType::Long),
        DataType::Float32 => Some(PrimitiveType::Float),
        DataType::Float64 => Some(PrimitiveType::Double),
        DataType::Decimal128(precision, scale) => Some(PrimitiveType::Decimal {
            precision: (*precision).into(),
            scale: u32::try_from(*scale)
                .map_err(|_| format!("iceberg sink decimal scale {scale} cannot convert to u32"))?,
        }),
        DataType::Date32 => Some(PrimitiveType::Date),
        DataType::Time64(TimeUnit::Microsecond) => Some(PrimitiveType::Time),
        DataType::Timestamp(TimeUnit::Microsecond, None) => Some(PrimitiveType::Timestamp),
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => Some(PrimitiveType::Timestamptz),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => Some(PrimitiveType::TimestampNs),
        DataType::Timestamp(TimeUnit::Nanosecond, Some(_)) => Some(PrimitiveType::TimestamptzNs),
        DataType::Utf8 | DataType::LargeUtf8 => Some(PrimitiveType::String),
        DataType::Binary => Some(PrimitiveType::Binary),
        DataType::LargeBinary => Some(PrimitiveType::Variant),
        DataType::FixedSizeBinary(size) => {
            Some(PrimitiveType::Fixed(u64::try_from(*size).map_err(
                |_| format!("iceberg sink fixed binary width {size} cannot convert to u64"),
            )?))
        }
        _ => None,
    };
    if let Some(primitive) = primitive {
        return Ok(Type::Primitive(primitive));
    }

    match data_type {
        DataType::Struct(fields) => {
            let fields = fields
                .iter()
                .map(|field| iceberg_nested_field_from_arrow_field(field.as_ref()))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Type::Struct(StructType::new(fields)))
        }
        DataType::List(element) => Ok(Type::List(ListType::new(
            iceberg_nested_field_from_arrow_field(element.as_ref())?,
        ))),
        DataType::Map(entries, _sorted) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(format!(
                    "iceberg sink MAP entries field must be Struct, got {:?}",
                    entries.data_type()
                ));
            };
            if fields.len() != 2 {
                return Err(format!(
                    "iceberg sink MAP entries Struct must have 2 fields, got {}",
                    fields.len()
                ));
            }
            Ok(Type::Map(MapType::new(
                iceberg_nested_field_from_arrow_field(fields[0].as_ref())?,
                iceberg_nested_field_from_arrow_field(fields[1].as_ref())?,
            )))
        }
        other => Err(format!(
            "unsupported arrow type for staged iceberg sink writer schema: {other:?}"
        )),
    }
}

fn build_staged_partition_spec(
    schema: &iceberg::spec::Schema,
    partition_spec_id: i32,
    source_column_names: &[String],
    partition_column_names: &[String],
    transform_exprs: &[String],
) -> Result<iceberg::spec::UnboundPartitionSpec, String> {
    if source_column_names.len() != partition_column_names.len()
        || source_column_names.len() != transform_exprs.len()
    {
        return Err(format!(
            "iceberg sink partition spec metadata mismatch: sources={} names={} transforms={}",
            source_column_names.len(),
            partition_column_names.len(),
            transform_exprs.len()
        ));
    }

    let mut builder =
        iceberg::spec::UnboundPartitionSpec::builder().with_spec_id(partition_spec_id);
    for ((source_name, partition_name), transform_expr) in source_column_names
        .iter()
        .zip(partition_column_names.iter())
        .zip(transform_exprs.iter())
    {
        let field = schema
            .field_by_name_case_insensitive(source_name)
            .ok_or_else(|| {
                format!("iceberg sink partition source column {source_name} missing from schema")
            })?;
        builder = builder
            .add_partition_field(
                field.id,
                partition_name,
                parse_staged_partition_transform(transform_expr)?,
            )
            .map_err(|e| format!("build staged iceberg partition field failed: {e}"))?;
    }
    Ok(builder.build())
}

fn parse_staged_partition_transform(raw: &str) -> Result<iceberg::spec::Transform, String> {
    let normalized = raw.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "identity" => Ok(iceberg::spec::Transform::Identity),
        "year" => Ok(iceberg::spec::Transform::Year),
        "month" => Ok(iceberg::spec::Transform::Month),
        "day" => Ok(iceberg::spec::Transform::Day),
        "hour" => Ok(iceberg::spec::Transform::Hour),
        "void" => Ok(iceberg::spec::Transform::Void),
        _ => {
            if let Some(width) = parse_transform_arg(&normalized, "bucket")? {
                return Ok(iceberg::spec::Transform::Bucket(width));
            }
            if let Some(width) = parse_transform_arg(&normalized, "truncate")? {
                return Ok(iceberg::spec::Transform::Truncate(width));
            }
            Err(format!(
                "unsupported iceberg partition transform for staged sink writer: {raw}"
            ))
        }
    }
}

fn parse_transform_arg(raw: &str, name: &str) -> Result<Option<u32>, String> {
    let Some(rest) = raw.strip_prefix(name) else {
        return Ok(None);
    };
    let Some(rest) = rest.strip_prefix('[').and_then(|s| s.strip_suffix(']')) else {
        return Err(format!(
            "iceberg partition transform {raw} must use {name}[N] syntax"
        ));
    };
    let value = rest.parse::<u32>().map_err(|e| {
        format!("iceberg partition transform {raw} has invalid numeric argument: {e}")
    })?;
    if value == 0 {
        return Err(format!(
            "iceberg partition transform {raw} requires a positive numeric argument"
        ));
    }
    Ok(Some(value))
}

pub(crate) fn build_staged_file_io(
    data_location: &str,
    s3_config: Option<&IcebergSinkObjectStoreConfig>,
) -> Result<iceberg::io::FileIO, String> {
    if data_location.starts_with("s3://") || data_location.starts_with("oss://") {
        let s3 = s3_config.ok_or_else(|| {
            format!(
                "iceberg sink missing S3 config for staged writer data_location={data_location}"
            )
        })?;
        let factory = s3.to_s3_storage_factory();
        return Ok(iceberg::io::FileIOBuilder::new(Arc::new(factory)).build());
    }
    Ok(iceberg::io::FileIO::new_with_fs())
}

pub(crate) fn parse_object_store_bucket_and_root(path: &str) -> Option<(String, String)> {
    for scheme in ["s3://", "oss://"] {
        if let Some(rest) = path.trim().strip_prefix(scheme) {
            let (bucket, key_prefix) = rest.split_once('/').unwrap_or((rest, ""));
            let bucket = bucket.trim();
            if bucket.is_empty() {
                return None;
            }
            return Some((bucket.to_string(), key_prefix.trim_matches('/').to_string()));
        }
    }
    None
}

fn normalize_path(path: &str) -> Result<String, String> {
    if path.starts_with("file:") {
        let url = url::Url::parse(path).map_err(|e| format!("invalid file url: {e}"))?;
        let p = url
            .to_file_path()
            .map_err(|_| "file url is not a valid local path".to_string())?;
        return Ok(p.to_string_lossy().to_string());
    }
    Ok(path.to_string())
}

fn write_parquet_file(
    path: &str,
    s3_config: Option<&IcebergSinkObjectStoreConfig>,
    schema: SchemaRef,
    batch: &RecordBatch,
    compression: Compression,
) -> Result<ParquetWriteResult, String> {
    let props = WriterProperties::builder()
        .set_compression(compression)
        .build();

    if path.starts_with("oss://") || path.starts_with("s3://") {
        let (data, write_result) = write_parquet_to_bytes(schema, batch, props)?;
        let s3 = s3_config.ok_or_else(|| {
            format!(
                "iceberg sink missing S3 config for object-store path={path}; \
                expected sink cloud_configuration to provide credentials"
            )
        })?;
        let object_store_cfg = s3.to_object_store_config();
        let op = crate::fs::oss::build_object_store_operator(&s3.bucket, &object_store_cfg)
            .map_err(|e| e.to_string())?;
        let rel = crate::fs::oss::normalize_oss_path(path, &s3.bucket, "")?;
        data_block_on(op.write(&rel, data))
            .map_err(|e| format!("run object-store write on data runtime failed: {e}"))?
            .map_err(|e| format!("opendal write failed: {e}"))?;
        return Ok(write_result);
    }

    // Local filesystem write: the path reported back to FE keeps its URI scheme
    // (e.g. "file:///tmp/..."), but ::fs APIs need a bare posix path.
    let local_path = normalize_path(path)?;
    let path_buf = PathBuf::from(&local_path);
    if let Some(parent) = path_buf.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("create parquet dir failed: {e}"))?;
    }
    let file =
        fs::File::create(&path_buf).map_err(|e| format!("create parquet file failed: {e}"))?;
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))
        .map_err(|e| format!("create parquet writer failed: {e}"))?;
    writer
        .write(batch)
        .map_err(|e| format!("write parquet batch failed: {e}"))?;
    let parquet_metadata = writer
        .close()
        .map_err(|e| format!("close parquet writer failed: {e}"))?;
    let meta = fs::metadata(&path_buf).map_err(|e| format!("stat parquet file failed: {e}"))?;
    Ok(build_parquet_write_result(
        meta.len(),
        &parquet_metadata,
        Some(batch),
    ))
}

fn write_parquet_to_bytes(
    schema: SchemaRef,
    batch: &RecordBatch,
    props: WriterProperties,
) -> Result<(Vec<u8>, ParquetWriteResult), String> {
    let mut buffer = Vec::new();
    let parquet_metadata;
    {
        let cursor = Cursor::new(&mut buffer);
        let mut writer = ArrowWriter::try_new(cursor, schema, Some(props))
            .map_err(|e| format!("create parquet writer failed: {e}"))?;
        writer
            .write(batch)
            .map_err(|e| format!("write parquet batch failed: {e}"))?;
        parquet_metadata = writer
            .close()
            .map_err(|e| format!("close parquet writer failed: {e}"))?;
    }
    let write_result =
        build_parquet_write_result(buffer.len() as u64, &parquet_metadata, Some(batch));
    Ok((buffer, write_result))
}

fn build_parquet_write_result(
    file_size: u64,
    metadata: &ParquetMetaData,
    batch: Option<&RecordBatch>,
) -> ParquetWriteResult {
    ParquetWriteResult {
        file_size,
        split_offsets: collect_split_offsets(metadata),
        column_stats: collect_iceberg_column_stats(metadata),
        theta_sketches: batch.and_then(collect_theta_sketches),
    }
}

fn collect_split_offsets(metadata: &ParquetMetaData) -> Option<Vec<i64>> {
    let mut offsets = Vec::with_capacity(metadata.row_groups().len());
    for row_group in metadata.row_groups() {
        if row_group.num_columns() == 0 {
            continue;
        }
        let first_column = row_group.column(0);
        let data_page_offset = first_column.data_page_offset();
        let split_offset = match first_column.dictionary_page_offset() {
            Some(dictionary_page_offset)
                if dictionary_page_offset > 0 && dictionary_page_offset < data_page_offset =>
            {
                dictionary_page_offset
            }
            _ => data_page_offset,
        };
        offsets.push(split_offset);
    }
    (!offsets.is_empty()).then_some(offsets)
}

fn collect_iceberg_column_stats(metadata: &ParquetMetaData) -> Option<IcebergColumnStats> {
    let mut accumulators: BTreeMap<i32, ColumnStatsAccumulator> = BTreeMap::new();

    for row_group in metadata.row_groups() {
        for column in row_group.columns() {
            let basic_info = column.column_descr().self_type().get_basic_info();
            if !basic_info.has_id() {
                continue;
            }
            let field_id = basic_info.id();
            let acc = accumulators.entry(field_id).or_default();
            acc.column_size += column.compressed_size();

            let Some(stats) = column.statistics() else {
                continue;
            };
            acc.has_statistics = true;
            acc.value_count += column.num_values();
            if let Some(null_count) = stats.null_count_opt() {
                acc.null_value_count += null_count as i64;
            }
            if let Some(merged) = acc.merged_statistics.as_mut() {
                merge_statistics(merged, stats);
            } else {
                acc.merged_statistics = Some(stats.clone());
            }
        }
    }

    if accumulators.is_empty() {
        return None;
    }

    let mut column_sizes = BTreeMap::new();
    let mut value_counts = BTreeMap::new();
    let mut null_value_counts = BTreeMap::new();
    let mut lower_bounds = BTreeMap::new();
    let mut upper_bounds = BTreeMap::new();

    for (field_id, acc) in accumulators {
        column_sizes.insert(field_id, acc.column_size);
        if !acc.has_statistics {
            continue;
        }

        value_counts.insert(field_id, acc.value_count);
        null_value_counts.insert(field_id, acc.null_value_count);

        if let Some(stats) = acc.merged_statistics.as_ref() {
            if let Some(min) = stats.min_bytes_opt() {
                lower_bounds.insert(field_id, min.to_vec());
            }
            if let Some(max) = stats.max_bytes_opt() {
                upper_bounds.insert(field_id, max.to_vec());
            }
        }
    }

    Some(IcebergColumnStats {
        column_sizes,
        value_counts,
        null_value_counts,
        nan_value_counts: BTreeMap::new(),
        lower_bounds,
        upper_bounds,
    })
}

/// Compute a Theta sketch per primitive column from the input RecordBatch.
///
/// Each Arrow `Field` whose metadata carries `PARQUET_FIELD_ID_META_KEY`
/// becomes a `field_id → ThetaSketchHandle` entry; columns without a field
/// id, or whose type is non-primitive (struct/list/map/binary), are skipped.
///
/// The hash is computed over the canonical byte representation of the value
/// (little-endian for integers/floats, raw UTF-8 bytes for strings,
/// IEEE 754 bytes for floats with NaN normalization). Nulls are not pushed
/// into the sketch.
///
/// Returns `None` when no primitive column with a parquet field id is
/// found — there is nothing to write into a Puffin blob in that case.
///
/// Public wrapper used by the standalone iceberg_writer path; the
/// pipeline-driven sink calls this through the internal Parquet write
/// helper.
pub(crate) fn compute_theta_sketches_for_batch(
    batch: &RecordBatch,
) -> Option<HashMap<i32, super::theta_sketch::ThetaSketchHandle>> {
    collect_theta_sketches(batch)
}

/// Feed every non-null value of `array` into `sketch`, dispatching by Arrow
/// type. Returns true if at least one value was fed. NaN floats are collapsed
/// to a single canonical bit pattern so independent NaN encodings count once.
/// Shared by `collect_theta_sketches` (write path, field-id from Arrow
/// metadata) and `collect_theta_sketches_by_name` (ANALYZE path, field-id from
/// an explicit name map). Unsupported/complex types feed nothing -> false.
fn feed_array_into_sketch(
    sketch: &mut super::theta_sketch::ThetaSketchHandle,
    data_type: &DataType,
    array: &arrow::array::ArrayRef,
) -> bool {
    use arrow::array::{
        BooleanArray, Date32Array, Date64Array, Decimal128Array, Float32Array, Float64Array,
        Int8Array, Int16Array, Int32Array, Int64Array, LargeStringArray, StringArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    };
    use arrow::datatypes::TimeUnit;
    let mut updated = false;
    macro_rules! feed_int {
        ($ty:ty) => {{
            if let Some(arr) = array.as_any().downcast_ref::<$ty>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        sketch.update(arr.value(i));
                        updated = true;
                    }
                }
            }
        }};
    }
    match data_type {
        DataType::Boolean => {
            if let Some(arr) = array.as_any().downcast_ref::<BooleanArray>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        let v: u8 = if arr.value(i) { 1 } else { 0 };
                        sketch.update(v);
                        updated = true;
                    }
                }
            }
        }
        DataType::Int8 => feed_int!(Int8Array),
        DataType::Int16 => feed_int!(Int16Array),
        DataType::Int32 => feed_int!(Int32Array),
        DataType::Int64 => feed_int!(Int64Array),
        DataType::Date32 => feed_int!(Date32Array),
        DataType::Date64 => feed_int!(Date64Array),
        DataType::Float32 => {
            if let Some(arr) = array.as_any().downcast_ref::<Float32Array>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        let v = arr.value(i);
                        let bits = if v.is_nan() {
                            f32::NAN.to_bits()
                        } else {
                            v.to_bits()
                        };
                        sketch.update(bits);
                        updated = true;
                    }
                }
            }
        }
        DataType::Float64 => {
            if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        let v = arr.value(i);
                        let bits = if v.is_nan() {
                            f64::NAN.to_bits()
                        } else {
                            v.to_bits()
                        };
                        sketch.update(bits);
                        updated = true;
                    }
                }
            }
        }
        DataType::Decimal128(_, _) => {
            if let Some(arr) = array.as_any().downcast_ref::<Decimal128Array>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        sketch.update(arr.value(i));
                        updated = true;
                    }
                }
            }
        }
        DataType::Utf8 => {
            if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        sketch.update(arr.value(i));
                        updated = true;
                    }
                }
            }
        }
        DataType::LargeUtf8 => {
            if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        sketch.update(arr.value(i));
                        updated = true;
                    }
                }
            }
        }
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => feed_int!(TimestampSecondArray),
            TimeUnit::Millisecond => feed_int!(TimestampMillisecondArray),
            TimeUnit::Microsecond => feed_int!(TimestampMicrosecondArray),
            TimeUnit::Nanosecond => feed_int!(TimestampNanosecondArray),
        },
        _ => {}
    }
    updated
}

fn collect_theta_sketches(
    batch: &RecordBatch,
) -> Option<HashMap<i32, super::theta_sketch::ThetaSketchHandle>> {
    use super::theta_sketch::ThetaSketchHandle;

    // Apache DataSketches Java/Spark default lg_k = 12 (k = 4096, ~1.5% error)
    // matches the spec. Kept hard-coded here; the table property override is
    // a follow-up wired through the sink plan.
    const LG_K: u8 = 12;

    let schema = batch.schema();
    let mut sketches: HashMap<i32, ThetaSketchHandle> = HashMap::new();

    for (col_idx, field) in schema.fields().iter().enumerate() {
        let Some(field_id_str) = field.metadata().get(PARQUET_FIELD_ID_META_KEY) else {
            continue;
        };
        let Ok(field_id) = field_id_str.parse::<i32>() else {
            continue;
        };
        let array = batch.column(col_idx);
        let mut sketch = ThetaSketchHandle::new(LG_K);
        let updated = feed_array_into_sketch(&mut sketch, field.data_type(), array);
        if updated {
            sketches.insert(field_id, sketch);
        }
    }

    if sketches.is_empty() {
        None
    } else {
        Some(sketches)
    }
}

/// Build per-field Theta sketches from a `RecordBatch` whose columns carry no
/// iceberg field-id metadata (e.g. an `execute_query` scan result), using an
/// explicit lowercased-column-name -> field_id map. Columns absent from the map,
/// or of unsupported type, are skipped. Sketches accumulate per call; union
/// across batches via `ThetaSketchHandle::union` at the call site.
pub(crate) fn collect_theta_sketches_by_name(
    batch: &RecordBatch,
    name_to_field_id: &HashMap<String, i32>,
) -> HashMap<i32, super::theta_sketch::ThetaSketchHandle> {
    const LG_K: u8 = 12;
    let schema = batch.schema();
    let mut sketches = HashMap::new();
    for (col_idx, field) in schema.fields().iter().enumerate() {
        let Some(&field_id) = name_to_field_id.get(&field.name().to_lowercase()) else {
            continue;
        };
        let mut sketch = super::theta_sketch::ThetaSketchHandle::new(LG_K);
        if feed_array_into_sketch(&mut sketch, field.data_type(), batch.column(col_idx)) {
            sketches.insert(field_id, sketch);
        }
    }
    sketches
}

fn merge_statistics(current: &mut Statistics, next: &Statistics) {
    match (current, next) {
        (Statistics::Boolean(cur), Statistics::Boolean(nxt)) => {
            *cur = merge_typed_statistics(cur, nxt, PartialOrd::partial_cmp);
        }
        (Statistics::Int32(cur), Statistics::Int32(nxt)) => {
            *cur = merge_typed_statistics(cur, nxt, PartialOrd::partial_cmp);
        }
        (Statistics::Int64(cur), Statistics::Int64(nxt)) => {
            *cur = merge_typed_statistics(cur, nxt, PartialOrd::partial_cmp);
        }
        (Statistics::Int96(cur), Statistics::Int96(nxt)) => {
            *cur = merge_typed_statistics(cur, nxt, PartialOrd::partial_cmp);
        }
        (Statistics::Float(cur), Statistics::Float(nxt)) => {
            *cur = merge_typed_statistics(cur, nxt, PartialOrd::partial_cmp);
        }
        (Statistics::Double(cur), Statistics::Double(nxt)) => {
            *cur = merge_typed_statistics(cur, nxt, PartialOrd::partial_cmp);
        }
        (Statistics::ByteArray(cur), Statistics::ByteArray(nxt)) => {
            *cur = merge_typed_statistics(cur, nxt, PartialOrd::partial_cmp);
        }
        (Statistics::FixedLenByteArray(cur), Statistics::FixedLenByteArray(nxt)) => {
            *cur = merge_typed_statistics(cur, nxt, PartialOrd::partial_cmp);
        }
        _ => {}
    }
}

fn merge_typed_statistics<T, F>(
    current: &ValueStatistics<T>,
    next: &ValueStatistics<T>,
    compare: F,
) -> ValueStatistics<T>
where
    T: Clone + AsBytes,
    F: Fn(&T, &T) -> Option<Ordering>,
{
    let min = choose_min(current.min_opt(), next.min_opt(), &compare);
    let max = choose_max(current.max_opt(), next.max_opt(), &compare);
    let null_count =
        Some(current.null_count_opt().unwrap_or(0) + next.null_count_opt().unwrap_or(0));
    let min_is_exact = match (current.min_opt(), next.min_opt()) {
        (Some(_), Some(_)) => current.min_is_exact() && next.min_is_exact(),
        (Some(_), None) => current.min_is_exact(),
        (None, Some(_)) => next.min_is_exact(),
        (None, None) => false,
    };
    let max_is_exact = match (current.max_opt(), next.max_opt()) {
        (Some(_), Some(_)) => current.max_is_exact() && next.max_is_exact(),
        (Some(_), None) => current.max_is_exact(),
        (None, Some(_)) => next.max_is_exact(),
        (None, None) => false,
    };

    ValueStatistics::new(min, max, None, null_count, false)
        .with_backwards_compatible_min_max(
            current.is_min_max_backwards_compatible() && next.is_min_max_backwards_compatible(),
        )
        .with_min_is_exact(min_is_exact)
        .with_max_is_exact(max_is_exact)
}

fn choose_min<T, F>(left: Option<&T>, right: Option<&T>, compare: &F) -> Option<T>
where
    T: Clone,
    F: Fn(&T, &T) -> Option<Ordering>,
{
    match (left, right) {
        (Some(left), Some(right)) => match compare(left, right) {
            Some(Ordering::Greater) => Some(right.clone()),
            _ => Some(left.clone()),
        },
        (Some(left), None) => Some(left.clone()),
        (None, Some(right)) => Some(right.clone()),
        (None, None) => None,
    }
}

fn choose_max<T, F>(left: Option<&T>, right: Option<&T>, compare: &F) -> Option<T>
where
    T: Clone,
    F: Fn(&T, &T) -> Option<Ordering>,
{
    match (left, right) {
        (Some(left), Some(right)) => match compare(left, right) {
            Some(Ordering::Less) => Some(right.clone()),
            _ => Some(left.clone()),
        },
        (Some(left), None) => Some(left.clone()),
        (None, Some(right)) => Some(right.clone()),
        (None, None) => None,
    }
}

fn iceberg_partition_key_for_row(
    partition_column_names: &[String],
    transform_exprs: &[String],
    partition_arrays: &[ArrayRef],
    row: usize,
) -> Result<(String, String, iceberg::spec::Struct), String> {
    if partition_column_names.len() != transform_exprs.len()
        || partition_arrays.len() != partition_column_names.len()
    {
        return Err("partition arrays mismatch for iceberg sink".to_string());
    }
    let mut path = String::new();
    let mut nulls = String::with_capacity(partition_column_names.len());
    let mut partition_values = Vec::with_capacity(partition_column_names.len());
    for i in 0..partition_column_names.len() {
        let transform = transform_exprs[i].to_lowercase();
        let base = transform.split('[').next().unwrap_or(transform.as_str());
        let is_null = base == "void" || partition_arrays[i].is_null(row);
        let value = iceberg_partition_value(base, &partition_arrays[i], row)?;
        let literal = if is_null {
            None
        } else {
            Some(iceberg_partition_literal(base, &partition_arrays[i], row)?)
        };
        nulls.push(if is_null { '1' } else { '0' });
        partition_values.push(literal);
        path.push_str(&partition_column_names[i]);
        path.push('=');
        path.push_str(&value);
        path.push('/');
    }
    Ok((
        path,
        nulls,
        iceberg::spec::Struct::from_iter(partition_values),
    ))
}

fn iceberg_partition_literal(
    transform: &str,
    array: &ArrayRef,
    row: usize,
) -> Result<iceberg::spec::Literal, String> {
    match transform {
        "year" | "month" | "hour" | "bucket" => {
            let value = array_value_as_i64(array, row)?;
            let value = i32::try_from(value)
                .map_err(|_| format!("{transform} transform value out of INT range"))?;
            Ok(iceberg::spec::Literal::int(value))
        }
        "day" => {
            let value = array_value_as_i64(array, row)?;
            let days =
                i32::try_from(value).map_err(|_| "day transform value out of range".to_string())?;
            Ok(iceberg::spec::Literal::date(days))
        }
        "truncate" | "identity" => column_literal(array, row),
        other => Err(format!("unsupported iceberg partition transform: {other}")),
    }
}

fn iceberg_partition_value(
    transform: &str,
    array: &ArrayRef,
    row: usize,
) -> Result<String, String> {
    if array.is_null(row) || transform == "void" {
        return Ok("null".to_string());
    }
    match transform {
        "year" => {
            let value = array_value_as_i64(array, row)?;
            Ok((value + 1970).to_string())
        }
        "month" => {
            let value = array_value_as_i64(array, row)?;
            let year = 1970 + (value / 12);
            let month = value % 12 + 1;
            Ok(format!("{:04}-{:02}", year, month))
        }
        "day" => {
            let value = array_value_as_i64(array, row)?;
            let days =
                i32::try_from(value).map_err(|_| "day transform value out of range".to_string())?;
            let date = chrono::NaiveDate::from_num_days_from_ce_opt(719_163 + days)
                .ok_or_else(|| "invalid day transform value".to_string())?;
            Ok(date.format("%Y-%m-%d").to_string())
        }
        "hour" => {
            let value = array_value_as_i64(array, row)?;
            let seconds = value * 3600;
            let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(seconds, 0)
                .ok_or_else(|| "invalid hour transform value".to_string())?
                .naive_utc();
            Ok(dt.format("%Y-%m-%d-%H").to_string())
        }
        "truncate" | "bucket" | "identity" => column_value(array, row),
        other => Err(format!("unsupported iceberg partition transform: {other}")),
    }
}

fn array_value_as_i64(array: &ArrayRef, row: usize) -> Result<i64, String> {
    match array.data_type() {
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "expected INT array".to_string())?;
            Ok(i64::from(arr.value(row)))
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "expected BIGINT array".to_string())?;
            Ok(arr.value(row))
        }
        other => Err(format!(
            "iceberg partition transform expects INT/BIGINT, got {other:?}"
        )),
    }
}

fn column_value(array: &ArrayRef, row: usize) -> Result<String, String> {
    match array.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .ok_or_else(|| "expected BOOLEAN array".to_string())?;
            Ok(if arr.value(row) { "true" } else { "false" }.to_string())
        }
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .ok_or_else(|| "expected TINYINT array".to_string())?;
            Ok(arr.value(row).to_string())
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int16Array>()
                .ok_or_else(|| "expected SMALLINT array".to_string())?;
            Ok(arr.value(row).to_string())
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "expected INT array".to_string())?;
            Ok(arr.value(row).to_string())
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "expected BIGINT array".to_string())?;
            Ok(arr.value(row).to_string())
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Date32Array>()
                .ok_or_else(|| "expected DATE array".to_string())?;
            let days = arr.value(row);
            let date = chrono::NaiveDate::from_num_days_from_ce_opt(719_163 + days)
                .ok_or_else(|| "invalid Date32 value".to_string())?;
            Ok(date.format("%Y-%m-%d").to_string())
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| "expected DATETIME array".to_string())?;
            let micros = arr.value(row);
            let secs = micros.div_euclid(1_000_000);
            let rem = micros.rem_euclid(1_000_000);
            let nanos = (rem as u32) * 1000;
            let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nanos)
                .ok_or_else(|| "invalid DATETIME value".to_string())?
                .naive_utc();
            Ok(url_encode(&format_datetime(dt)))
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "expected VARCHAR array".to_string())?;
            Ok(url_encode(arr.value(row)))
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| "expected BINARY array".to_string())?;
            let bytes = arr.value(row);
            let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
            Ok(url_encode(&encoded))
        }
        DataType::Decimal128(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "expected DECIMAL array".to_string())?;
            Ok(arr.value_as_string(row))
        }
        other => Err(format!(
            "unsupported iceberg partition column type: {other:?}"
        )),
    }
}

fn column_literal(array: &ArrayRef, row: usize) -> Result<iceberg::spec::Literal, String> {
    match array.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .ok_or_else(|| "expected BOOLEAN array".to_string())?;
            Ok(iceberg::spec::Literal::bool(arr.value(row)))
        }
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .ok_or_else(|| "expected TINYINT array".to_string())?;
            Ok(iceberg::spec::Literal::int(i32::from(arr.value(row))))
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int16Array>()
                .ok_or_else(|| "expected SMALLINT array".to_string())?;
            Ok(iceberg::spec::Literal::int(i32::from(arr.value(row))))
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "expected INT array".to_string())?;
            Ok(iceberg::spec::Literal::int(arr.value(row)))
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "expected BIGINT array".to_string())?;
            Ok(iceberg::spec::Literal::long(arr.value(row)))
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .ok_or_else(|| "expected FLOAT array".to_string())?;
            Ok(iceberg::spec::Literal::float(arr.value(row)))
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .ok_or_else(|| "expected DOUBLE array".to_string())?;
            Ok(iceberg::spec::Literal::double(arr.value(row)))
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Date32Array>()
                .ok_or_else(|| "expected DATE array".to_string())?;
            Ok(iceberg::spec::Literal::date(arr.value(row)))
        }
        DataType::Time64(arrow::datatypes::TimeUnit::Microsecond) => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Time64MicrosecondArray>()
                .ok_or_else(|| "expected TIME array".to_string())?;
            Ok(iceberg::spec::Literal::time(arr.value(row)))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| "expected DATETIME array".to_string())?;
            Ok(iceberg::spec::Literal::timestamp(arr.value(row)))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some(_)) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| "expected TIMESTAMP array".to_string())?;
            Ok(iceberg::spec::Literal::timestamptz(arr.value(row)))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                .ok_or_else(|| "expected TIMESTAMP_NS array".to_string())?;
            Ok(iceberg::spec::Literal::Primitive(
                iceberg::spec::PrimitiveLiteral::Long(arr.value(row)),
            ))
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "expected VARCHAR array".to_string())?;
            Ok(iceberg::spec::Literal::string(arr.value(row)))
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| "expected BINARY array".to_string())?;
            Ok(iceberg::spec::Literal::binary(
                arr.value(row).iter().copied(),
            ))
        }
        DataType::FixedSizeBinary(_) => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
                .ok_or_else(|| "expected FIXED array".to_string())?;
            Ok(iceberg::spec::Literal::fixed(
                arr.value(row).iter().copied(),
            ))
        }
        DataType::Decimal128(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "expected DECIMAL array".to_string())?;
            Ok(iceberg::spec::Literal::decimal(arr.value(row)))
        }
        other => Err(format!(
            "unsupported iceberg partition column type: {other:?}"
        )),
    }
}

fn url_encode(input: &str) -> String {
    url::form_urlencoded::byte_serialize(input.as_bytes()).collect()
}

fn format_datetime(dt: chrono::NaiveDateTime) -> String {
    let micros = dt.and_utc().timestamp_subsec_micros();
    if micros == 0 {
        dt.format("%Y-%m-%d %H:%M:%S").to_string()
    } else {
        dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, Int32Array, Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;

    use arrow::array::StringArray;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::reader::{FileReader, SerializedFileReader};

    use crate::exec::pipeline::operator::Operator;

    use super::{
        ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
        ICEBERG_RESERVED_FIELD_ID_ROW_ID, ICEBERG_ROW_ID_COL, IcebergSinkMode, IcebergSinkPlan,
        IcebergTableSinkBackend, IcebergTableSinkFactory, align_arrays_to_schema,
        collect_theta_sketches_by_name, iceberg_partition_key_for_row,
        iceberg_schema_from_arrow_schema, merge_deletion_vectors_by_file, row_lineage_row_id_index,
        schema_has_reserved_row_lineage_columns, unique_file_path, write_parquet_to_bytes,
    };
    use crate::connector::iceberg::commit::EqualityDeleteColumn;
    use crate::connector::iceberg::data_writer::{StagedWriteOptions, write_record_batches};
    use crate::connector::iceberg::delete_file::IcebergFileContent;
    use crate::connector::iceberg::sink_plan::PositionDeleteDataFilePartition;
    use crate::exec::chunk::{Chunk, ChunkSchema};
    use crate::exec::expr::ExprNode;
    use crate::exec::pipeline::async_sink::AsyncSinkBackend;
    use crate::runtime::runtime_state::RuntimeState;
    use crate::{common::ids::SlotId, common::types::UniqueId};

    fn position_delete_required_schema_for_tests() -> SchemaRef {
        use crate::connector::iceberg::position_delete_descriptor::{
            ICEBERG_POSITION_DELETE_FILE_PATH_COLUMN, ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID,
            ICEBERG_POSITION_DELETE_POS_COLUMN, ICEBERG_POSITION_DELETE_POS_FIELD_ID,
            PositionDeleteDescriptorInput, PositionDeleteOutputField,
            output_schema_from_descriptor,
        };

        let desc = PositionDeleteDescriptorInput {
            file_path: PositionDeleteOutputField {
                output_expr_index: 0,
                name: ICEBERG_POSITION_DELETE_FILE_PATH_COLUMN.to_string(),
                data_type: DataType::Utf8,
                field_id: ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID,
            },
            pos: PositionDeleteOutputField {
                output_expr_index: 1,
                name: ICEBERG_POSITION_DELETE_POS_COLUMN.to_string(),
                data_type: DataType::Int64,
                field_id: ICEBERG_POSITION_DELETE_POS_FIELD_ID,
            },
            partition_source_fields: Vec::new(),
            target_partition_spec_id: 0,
        };
        output_schema_from_descriptor(&desc).expect("required position delete schema")
    }

    #[test]
    fn iceberg_sink_maps_largebinary_to_variant() {
        use iceberg::spec::{PrimitiveType, Type};

        let binary = super::iceberg_type_from_arrow_type(&DataType::Binary).expect("binary");
        assert!(matches!(binary, Type::Primitive(PrimitiveType::Binary)));

        let variant = super::iceberg_type_from_arrow_type(&DataType::LargeBinary).expect("variant");
        assert!(matches!(variant, Type::Primitive(PrimitiveType::Variant)));
    }

    #[test]
    fn collect_theta_sketches_by_name_keys_by_explicit_map() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::collections::HashMap;
        use std::sync::Arc;

        // No PARQUET_FIELD_ID_META_KEY metadata on fields (mirrors a query result).
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 1, 2, 3])), // 3 distinct
                Arc::new(StringArray::from(vec!["a", "a", "b", "b"])), // 2 distinct
            ],
        )
        .unwrap();
        let mut name_to_field_id = HashMap::new();
        name_to_field_id.insert("k".to_string(), 7_i32);
        name_to_field_id.insert("s".to_string(), 9_i32);

        let sketches = collect_theta_sketches_by_name(&batch, &name_to_field_id);
        assert!((sketches[&7].estimate() - 3.0).abs() < 0.5, "k ndv ~3");
        assert!((sketches[&9].estimate() - 2.0).abs() < 0.5, "s ndv ~2");
        assert!(!sketches.contains_key(&999));
    }

    #[test]
    fn reserved_row_lineage_schema_detection_requires_field_ids() {
        let plain = Schema::new(vec![
            Field::new(ICEBERG_ROW_ID_COL, DataType::Int64, false),
            Field::new(ICEBERG_LAST_UPDATED_SEQ_COL, DataType::Int64, true),
        ]);
        assert!(
            !schema_has_reserved_row_lineage_columns(&plain).expect("plain schema check"),
            "names alone must not enable row-lineage sink behavior"
        );

        let reserved = Schema::new(vec![
            Field::new(ICEBERG_ROW_ID_COL, DataType::Int64, false).with_metadata(HashMap::from([
                (
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    ICEBERG_RESERVED_FIELD_ID_ROW_ID.to_string(),
                ),
            ])),
            Field::new(ICEBERG_LAST_UPDATED_SEQ_COL, DataType::Int64, true).with_metadata(
                HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER.to_string(),
                )]),
            ),
        ]);
        assert!(
            schema_has_reserved_row_lineage_columns(&reserved).expect("reserved schema check"),
            "reserved row-lineage field ids should enable row-lineage sink behavior"
        );
        assert_eq!(
            row_lineage_row_id_index(&reserved).expect("row id index"),
            0
        );
    }

    #[test]
    fn iceberg_table_sink_factory_creates_async_sink_operator() {
        let schema = Arc::new(Schema::new(vec![Field::new("c0", DataType::Int64, true)]));
        let factory = IcebergTableSinkFactory {
            name: "ICEBERG_TABLE_SINK".to_string(),
            arena: Arc::new(crate::exec::expr::ExprArena::default()),
            plan: Arc::new(IcebergSinkPlan {
                mode: IcebergSinkMode::Data,
                table_location: "file:///tmp/novarocks-iceberg-sink-test".to_string(),
                data_location: "file:///tmp/novarocks-iceberg-sink-test/data".to_string(),
                target_partition_spec_id: 0,
                target_table_metadata: None,
                target_snapshot_id: None,
                position_delete_data_file_partitions: HashMap::new(),
                object_store_s3: None,
                file_format: crate::connector::iceberg::delete_file::IcebergFileFormat::Parquet,
                report_file_format: "parquet".to_string(),
                compression: Compression::SNAPPY,
                output_schema: Arc::clone(&schema),
                target_schema: schema,
                equality_delete_columns: Vec::new(),
                row_lineage_data: false,
                output_exprs: Vec::new(),
                partition_exprs: Vec::new(),
                partition_source_column_names: Vec::new(),
                partition_column_names: Vec::new(),
                transform_exprs: Vec::new(),
                position_delete_binding: None,
            }),
        };
        let op = factory.create_async_operator(0);

        assert_eq!(op.name(), "ICEBERG_TABLE_SINK");
    }

    #[tokio::test]
    async fn equality_delete_sink_writes_key_projection_with_equality_ids() {
        let dir = tempfile::Builder::new()
            .prefix("novarocks-iceberg-equality-delete-sink-")
            .tempdir()
            .expect("temp dir");
        let table_location = format!("file://{}", dir.path().display());
        let data_location = format!("{table_location}/custom-data");
        let id_slot = SlotId::new(1);
        let category_slot = SlotId::new(2);
        let mut arena = crate::exec::expr::ExprArena::default();
        let id_expr = arena.push_typed(ExprNode::SlotId(id_slot), DataType::Int32);
        let category_expr = arena.push_typed(ExprNode::SlotId(category_slot), DataType::Utf8);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "11".to_string(),
            )])),
            Field::new("category", DataType::Utf8, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "12".to_string(),
            )])),
        ]));
        let plan = Arc::new(IcebergSinkPlan {
            mode: IcebergSinkMode::EqualityDeletes,
            table_location,
            data_location: data_location.clone(),
            target_partition_spec_id: 0,
            target_table_metadata: None,
            target_snapshot_id: None,
            position_delete_data_file_partitions: HashMap::new(),
            object_store_s3: None,
            file_format: crate::connector::iceberg::delete_file::IcebergFileFormat::Parquet,
            report_file_format: "parquet".to_string(),
            compression: Compression::SNAPPY,
            output_schema: Arc::clone(&schema),
            target_schema: Arc::clone(&schema),
            equality_delete_columns: vec![
                EqualityDeleteColumn {
                    name: "id".to_string(),
                    field_id: 11,
                    data_type: DataType::Int32,
                    nullable: false,
                },
                EqualityDeleteColumn {
                    name: "category".to_string(),
                    field_id: 12,
                    data_type: DataType::Utf8,
                    nullable: true,
                },
            ],
            row_lineage_data: false,
            output_exprs: vec![id_expr, category_expr],
            partition_exprs: Vec::new(),
            partition_source_column_names: Vec::new(),
            partition_column_names: Vec::new(),
            transform_exprs: Vec::new(),
            position_delete_binding: None,
        });

        let mut backend = IcebergTableSinkBackend {
            arena: Arc::new(arena),
            plan,
            driver_id: 17,
            file_seq: 0,
            runtime_state: None,
            pending_deletion_vectors: BTreeMap::new(),
        };
        let batch_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("category", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&batch_schema),
            vec![
                Arc::new(Int32Array::from(vec![2, 4])),
                Arc::new(StringArray::from(vec![Some("B"), None])),
            ],
        )
        .expect("record batch");
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch_schema.as_ref(),
            &[id_slot, category_slot],
        )
        .expect("chunk schema");
        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk");
        let finst_id = UniqueId { hi: 707, lo: 42 };
        crate::runtime::sink_commit::unregister(finst_id);
        let state = RuntimeState::new(
            None,
            None,
            None,
            None,
            Some(finst_id),
            None,
            None,
            None,
            None,
        );
        backend
            .bind_runtime_state(&state)
            .expect("bind runtime state");

        backend
            .write_chunk(chunk)
            .await
            .expect("write equality-delete chunk");

        let infos = crate::runtime::sink_commit::list(finst_id);
        crate::runtime::sink_commit::unregister(finst_id);
        let target_writer_schema =
            iceberg_schema_from_arrow_schema(&backend.plan.target_schema).expect("target schema");
        let target_metadata = backend
            .plan
            .build_target_table_metadata(&target_writer_schema)
            .expect("target metadata");
        let reports = crate::runtime::sink_commit_wire::sink_commit_infos_to_writer_reports(
            infos,
            &target_metadata,
        )
        .expect("decode writer reports");
        assert_eq!(reports.len(), 1);
        let data_file = &reports[0].file;
        assert_eq!(data_file.format, "parquet");
        assert_eq!(data_file.record_count, 2);
        assert_eq!(data_file.content, IcebergFileContent::EqualityDeletes);
        assert_eq!(data_file.equality_ids, Some(vec![11, 12]));
        assert_eq!(data_file.partition.partition_path, "");
        assert_eq!(data_file.partition.partition_spec_id, 0);
        assert!(
            data_file
                .path
                .starts_with(&format!("{data_location}/equality-delete-")),
            "unexpected equality-delete path: {}",
            data_file.path
        );
    }

    #[test]
    fn delete_target_snapshot_id_prefers_explicit_descriptor_snapshot() {
        let table_location = "file:///warehouse/dv-explicit-snapshot";
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "42".to_string(),
            )])),
        ]));
        let metadata =
            test_identity_partition_metadata(table_location, table_location, target_schema, 7);

        assert_eq!(
            super::delete_target_snapshot_id(&metadata, Some(101)),
            Some(101)
        );
        assert_eq!(
            super::delete_target_snapshot_id(&metadata, None),
            metadata.current_snapshot_id()
        );
    }

    #[test]
    fn delete_like_sinks_wire_explicit_snapshot_selection_into_call_sites() {
        let lower_source = include_str!("../../lower/sink/iceberg.rs");
        let partition_index = lower_source
            .split("fn build_position_delete_data_file_partition_index")
            .nth(1)
            .expect("partition index helper")
            .split("fn insert_position_delete_data_file_partition")
            .next()
            .expect("partition index helper body");
        assert!(
            partition_index
                .contains("target_snapshot_id.or_else(|| metadata.current_snapshot_id())"),
            "partition index builder must select the descriptor target snapshot"
        );
        assert!(
            !partition_index.contains("metadata.current_snapshot()"),
            "partition index builder must not use metadata current snapshot directly"
        );

        let source = include_str!("sink.rs");
        let dv_reader = source
            .split("fn read_existing_dv_positions")
            .nth(1)
            .expect("DV reader")
            .split("fn referenced_data_file_partition_report")
            .next()
            .expect("DV reader body");
        assert!(
            dv_reader.contains("delete_target_snapshot_id(metadata, self.plan.target_snapshot_id)"),
            "DV reader must select the descriptor target snapshot"
        );
        assert!(
            !dv_reader.contains("metadata.current_snapshot_id()"),
            "DV reader must not use metadata current snapshot id directly"
        );

        let partition_index_call_site = lower_source
            .split("let position_delete_data_file_partitions")
            .nth(1)
            .expect("partition index call site")
            .split("let file_format")
            .next()
            .expect("partition index call site body");
        assert!(
            partition_index_call_site.contains("IcebergSinkMode::DeletionVectors"),
            "DV sink factory must build referenced data file partition metadata"
        );
    }

    #[tokio::test]
    async fn data_sink_plan_builds_staged_context_with_fe_metadata() {
        let dir = tempfile::Builder::new()
            .prefix("novarocks-iceberg-fe-staged-ctx-")
            .tempdir()
            .expect("temp dir");
        let table_location = format!("file://{}", dir.path().display());
        let data_location = format!("{table_location}/custom-data");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "42".to_string(),
            )])),
        ]));
        let plan = IcebergSinkPlan {
            mode: IcebergSinkMode::Data,
            table_location: table_location.clone(),
            data_location: data_location.clone(),
            target_partition_spec_id: 7,
            target_table_metadata: None,
            target_snapshot_id: None,
            position_delete_data_file_partitions: HashMap::new(),
            object_store_s3: None,
            file_format: crate::connector::iceberg::delete_file::IcebergFileFormat::Parquet,
            report_file_format: "parquet".to_string(),
            compression: Compression::SNAPPY,
            output_schema: Arc::clone(&schema),
            target_schema: Arc::clone(&schema),
            equality_delete_columns: Vec::new(),
            row_lineage_data: false,
            output_exprs: Vec::new(),
            partition_exprs: Vec::new(),
            partition_source_column_names: vec!["id".to_string()],
            partition_column_names: vec!["id_part".to_string()],
            transform_exprs: vec!["identity".to_string()],
            position_delete_binding: None,
        };

        let ctx = plan.build_staged_write_context().expect("staged context");

        assert_eq!(ctx.schema().as_struct().fields()[0].id, 42);
        assert_eq!(
            ctx.partition_spec_id(),
            7,
            "staged sink metadata must preserve the target default partition spec id"
        );
        assert_eq!(ctx.partition_spec().fields().len(), 1);
        assert_eq!(ctx.partition_spec().fields()[0].source_id, 42);
        assert_eq!(ctx.partition_spec().fields()[0].name, "id_part");

        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![7, 7]))])
            .expect("record batch");
        let staged = write_record_batches(&ctx, vec![batch], &StagedWriteOptions::default())
            .await
            .expect("write staged file");
        assert_eq!(staged.len(), 1);
        let path = staged[0].data_file.file_path().to_string();
        assert!(
            path.starts_with(&format!("{data_location}/id_part=7/")),
            "staged sink context should write under FE data_location and partition path, got {path}"
        );
        let (report, _) =
            crate::connector::iceberg::data_writer::staged_data_file_to_writer_report(
                &staged[0],
                crate::connector::iceberg::report::IcebergPartitionReport {
                    partition_path: "id_part=7".to_string(),
                    null_fingerprint: "0".to_string(),
                    partition_spec_id: 7,
                    partition_values: staged[0].data_file.partition().clone(),
                },
                "parquet".to_string(),
                crate::connector::iceberg::delete_file::IcebergFileContent::Data,
            )
            .expect("writer report");
        assert_eq!(report.file.partition.partition_spec_id, 7);
        let target_writer_schema =
            iceberg_schema_from_arrow_schema(&plan.target_schema).expect("target writer schema");
        let target_metadata = plan
            .build_target_table_metadata(&target_writer_schema)
            .expect("target metadata");
        assert_eq!(target_metadata.default_partition_spec_id(), 7);
        assert_eq!(
            report.file.partition.partition_values,
            iceberg::spec::Struct::from_iter([Some(iceberg::spec::Literal::int(7))])
        );
    }

    #[test]
    fn data_sink_staged_metadata_preserves_target_table_properties() {
        let table_location = "file:///tmp/novarocks-iceberg-target-props".to_string();
        let data_location = format!("{table_location}/custom-data");
        let writer_schema = iceberg::spec::Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                iceberg::spec::NestedField::required(
                    1,
                    "id",
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int),
                )
                .into(),
                iceberg::spec::NestedField::optional(
                    2,
                    "v",
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Variant),
                )
                .into(),
            ])
            .build()
            .expect("schema");
        let target_metadata = iceberg::spec::TableMetadataBuilder::new(
            writer_schema.clone(),
            iceberg::spec::PartitionSpec::unpartition_spec(),
            iceberg::spec::SortOrder::unsorted_order(),
            table_location.clone(),
            iceberg::spec::FormatVersion::V3,
            HashMap::from([(
                "write.parquet.variant-shredding.v".to_string(),
                "a bigint".to_string(),
            )]),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata;
        let arrow_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let plan = IcebergSinkPlan {
            mode: IcebergSinkMode::Data,
            table_location,
            data_location: data_location.clone(),
            target_partition_spec_id: 0,
            target_table_metadata: Some(target_metadata),
            target_snapshot_id: None,
            position_delete_data_file_partitions: HashMap::new(),
            object_store_s3: None,
            file_format: crate::connector::iceberg::delete_file::IcebergFileFormat::Parquet,
            report_file_format: "parquet".to_string(),
            compression: Compression::SNAPPY,
            output_schema: Arc::clone(&arrow_schema),
            target_schema: arrow_schema,
            equality_delete_columns: Vec::new(),
            row_lineage_data: false,
            output_exprs: Vec::new(),
            partition_exprs: Vec::new(),
            partition_source_column_names: Vec::new(),
            partition_column_names: Vec::new(),
            transform_exprs: Vec::new(),
            position_delete_binding: None,
        };

        let staged_metadata = plan
            .build_target_table_metadata(&writer_schema)
            .expect("staged metadata");

        assert_eq!(
            staged_metadata
                .properties()
                .get("write.parquet.variant-shredding.v")
                .map(String::as_str),
            Some("a bigint")
        );
        assert_eq!(
            staged_metadata
                .properties()
                .get("write.data.path")
                .map(String::as_str),
            Some(data_location.as_str())
        );
    }

    #[tokio::test]
    async fn data_sink_backend_writes_data_files_through_staged_writer_kernel() {
        let dir = tempfile::Builder::new()
            .prefix("novarocks-iceberg-fe-staged-write-")
            .tempdir()
            .expect("temp dir");
        let table_location = format!("file://{}", dir.path().display());
        let data_location = format!("{table_location}/custom-data");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "42".to_string(),
            )])),
        ]));
        let slot_id = SlotId::new(1);
        let mut arena = crate::exec::expr::ExprArena::default();
        let id_expr = arena.push_typed(ExprNode::SlotId(slot_id), DataType::Int32);
        let plan = Arc::new(IcebergSinkPlan {
            mode: IcebergSinkMode::Data,
            table_location,
            data_location: data_location.clone(),
            target_partition_spec_id: 0,
            target_table_metadata: None,
            target_snapshot_id: None,
            position_delete_data_file_partitions: HashMap::new(),
            object_store_s3: None,
            file_format: crate::connector::iceberg::delete_file::IcebergFileFormat::Parquet,
            report_file_format: "parquet".to_string(),
            compression: Compression::SNAPPY,
            output_schema: Arc::clone(&schema),
            target_schema: Arc::clone(&schema),
            equality_delete_columns: Vec::new(),
            row_lineage_data: false,
            output_exprs: vec![id_expr],
            partition_exprs: vec![id_expr],
            partition_source_column_names: vec!["id".to_string()],
            partition_column_names: vec!["id_part".to_string()],
            transform_exprs: vec!["identity".to_string()],
            position_delete_binding: None,
        });
        let mut backend = IcebergTableSinkBackend {
            arena: Arc::new(arena),
            plan,
            driver_id: 3,
            file_seq: 0,
            runtime_state: None,
            pending_deletion_vectors: BTreeMap::new(),
        };
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![7, 7]))])
            .expect("record batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[slot_id])
                .expect("chunk schema");
        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk");
        let finst_id = UniqueId { hi: 701, lo: 42 };
        crate::runtime::sink_commit::unregister(finst_id);
        let state = RuntimeState::new(
            None,
            None,
            None,
            None,
            Some(finst_id),
            None,
            None,
            None,
            None,
        );

        backend
            .push_chunk_data(&state, chunk)
            .await
            .expect("write chunk");

        let infos = crate::runtime::sink_commit::list(finst_id);
        crate::runtime::sink_commit::unregister(finst_id);
        let target_writer_schema =
            iceberg_schema_from_arrow_schema(&backend.plan.target_schema).expect("target schema");
        let target_metadata = backend
            .plan
            .build_target_table_metadata(&target_writer_schema)
            .expect("target metadata");
        let reports = crate::runtime::sink_commit_wire::sink_commit_infos_to_writer_reports(
            infos,
            &target_metadata,
        )
        .expect("decode writer reports");
        assert_eq!(reports.len(), 1);
        let data_file = &reports[0].file;
        let path = data_file.path.as_str();
        assert!(
            path.starts_with(&format!("{data_location}/id_part=7/novarocks-00000-")),
            "DATA sink should use staged writer kernel file naming under FE data_location, got {path}"
        );
        assert_eq!(data_file.partition.partition_path, "id_part=7");
        assert_eq!(
            data_file.partition.partition_spec_id, 0,
            "writer report must carry the target default partition spec id"
        );
        assert_eq!(data_file.record_count, 2);
        assert_eq!(data_file.content, IcebergFileContent::Data);
    }

    fn test_identity_partition_metadata(
        table_location: &str,
        data_location: &str,
        target_schema: SchemaRef,
        partition_spec_id: i32,
    ) -> iceberg::spec::TableMetadata {
        let plan = IcebergSinkPlan {
            mode: IcebergSinkMode::Data,
            table_location: table_location.to_string(),
            data_location: data_location.to_string(),
            target_partition_spec_id: partition_spec_id,
            target_table_metadata: None,
            target_snapshot_id: None,
            position_delete_data_file_partitions: HashMap::new(),
            object_store_s3: None,
            file_format: crate::connector::iceberg::delete_file::IcebergFileFormat::Parquet,
            report_file_format: "parquet".to_string(),
            compression: Compression::SNAPPY,
            output_schema: Arc::clone(&target_schema),
            target_schema,
            equality_delete_columns: Vec::new(),
            row_lineage_data: false,
            output_exprs: Vec::new(),
            partition_exprs: Vec::new(),
            partition_source_column_names: vec!["id".to_string()],
            partition_column_names: vec!["id_part".to_string()],
            transform_exprs: vec!["identity".to_string()],
            position_delete_binding: None,
        };
        let writer_schema =
            iceberg_schema_from_arrow_schema(&plan.target_schema).expect("target writer schema");
        plan.build_target_table_metadata(&writer_schema)
            .expect("target metadata")
    }

    fn spec_id_by_partition_field(
        metadata: &iceberg::spec::TableMetadata,
        field_name: &str,
    ) -> i32 {
        metadata
            .partition_specs_iter()
            .find(|spec| spec.fields().iter().any(|field| field.name == field_name))
            .map(|spec| spec.spec_id())
            .unwrap_or_else(|| panic!("missing partition spec field {field_name}"))
    }

    fn test_bucket_evolution_metadata(
        table_location: &str,
    ) -> (iceberg::spec::TableMetadata, i32, i32) {
        let iceberg_schema = Arc::new(
            iceberg::spec::Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![Arc::new(iceberg::spec::NestedField::required(
                    1,
                    "user_id",
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
                ))])
                .build()
                .expect("iceberg schema"),
        );
        let old_spec = iceberg::spec::PartitionSpec::builder(Arc::clone(&iceberg_schema))
            .add_partition_field(
                "user_id",
                "user_bucket_4",
                iceberg::spec::Transform::Bucket(4),
            )
            .expect("old partition field")
            .build()
            .expect("old partition spec");
        let new_spec = iceberg::spec::UnboundPartitionSpec::builder()
            .add_partition_field(1, "user_bucket_8", iceberg::spec::Transform::Bucket(8))
            .expect("new partition field")
            .build();
        let metadata = iceberg::spec::TableMetadataBuilder::new(
            iceberg_schema.as_ref().clone(),
            old_spec,
            iceberg::spec::SortOrder::unsorted_order(),
            table_location.to_string(),
            iceberg::spec::FormatVersion::V2,
            HashMap::new(),
        )
        .expect("metadata builder")
        .add_default_partition_spec(new_spec)
        .expect("add evolved partition spec")
        .build()
        .expect("metadata build")
        .metadata;
        let old_spec_id = spec_id_by_partition_field(&metadata, "user_bucket_4");
        let new_spec_id = spec_id_by_partition_field(&metadata, "user_bucket_8");
        (metadata, old_spec_id, new_spec_id)
    }

    #[test]
    fn position_delete_backend_keeps_manual_delete_file_writer_path() {
        let dir = tempfile::Builder::new()
            .prefix("novarocks-iceberg-position-delete-write-")
            .tempdir()
            .expect("temp dir");
        let table_location = format!("file://{}", dir.path().display());
        let data_location = format!("{table_location}/custom-data");
        let source_schema = Arc::new(Schema::new(vec![
            Field::new("file_path", DataType::Utf8, false),
            Field::new("pos", DataType::Int64, false),
            Field::new("id", DataType::Int32, false),
        ]));
        let file_slot = SlotId::new(1);
        let pos_slot = SlotId::new(2);
        let id_slot = SlotId::new(3);
        let mut arena = crate::exec::expr::ExprArena::default();
        let file_expr = arena.push_typed(ExprNode::SlotId(file_slot), DataType::Utf8);
        let pos_expr = arena.push_typed(ExprNode::SlotId(pos_slot), DataType::Int64);
        let id_expr = arena.push_typed(ExprNode::SlotId(id_slot), DataType::Int32);
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "42".to_string(),
            )])),
        ]));
        let target_metadata = test_identity_partition_metadata(
            &table_location,
            &data_location,
            Arc::clone(&target_schema),
            7,
        );
        let referenced = "file:///tmp/base-data.parquet";
        let mut referenced_partitions = HashMap::new();
        referenced_partitions.insert(
            referenced.to_string(),
            PositionDeleteDataFilePartition {
                partition_spec_id: target_metadata.default_partition_spec_id(),
                partition_values: iceberg::spec::Struct::from_iter([Some(
                    iceberg::spec::Literal::int(7),
                )]),
            },
        );
        let plan = Arc::new(IcebergSinkPlan {
            mode: IcebergSinkMode::PositionDeletes,
            table_location,
            data_location: data_location.clone(),
            target_partition_spec_id: 7,
            target_table_metadata: Some(target_metadata.clone()),
            target_snapshot_id: None,
            position_delete_data_file_partitions: referenced_partitions,
            object_store_s3: None,
            file_format: crate::connector::iceberg::delete_file::IcebergFileFormat::Parquet,
            report_file_format: "parquet".to_string(),
            compression: Compression::SNAPPY,
            output_schema: position_delete_required_schema_for_tests(),
            target_schema,
            equality_delete_columns: Vec::new(),
            row_lineage_data: false,
            output_exprs: vec![file_expr, pos_expr, id_expr],
            partition_exprs: vec![id_expr],
            partition_source_column_names: vec!["id".to_string()],
            partition_column_names: vec!["id_part".to_string()],
            transform_exprs: vec!["identity".to_string()],
            position_delete_binding: None,
        });
        let mut backend = IcebergTableSinkBackend {
            arena: Arc::new(arena),
            plan,
            driver_id: 5,
            file_seq: 0,
            runtime_state: None,
            pending_deletion_vectors: BTreeMap::new(),
        };
        let batch = RecordBatch::try_new(
            source_schema,
            vec![
                Arc::new(StringArray::from(vec![referenced, referenced])),
                Arc::new(Int64Array::from(vec![9_i64, 2_i64])),
                Arc::new(Int32Array::from(vec![7, 7])),
            ],
        )
        .expect("record batch");
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[file_slot, pos_slot, id_slot],
        )
        .expect("chunk schema");
        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk");
        let finst_id = UniqueId { hi: 702, lo: 42 };
        crate::runtime::sink_commit::unregister(finst_id);
        let state = RuntimeState::new(
            None,
            None,
            None,
            None,
            Some(finst_id),
            None,
            None,
            None,
            None,
        );

        backend
            .push_chunk_position_delete(&state, chunk)
            .expect("write position deletes");

        let infos = crate::runtime::sink_commit::list(finst_id);
        crate::runtime::sink_commit::unregister(finst_id);
        let reports = crate::runtime::sink_commit_wire::sink_commit_infos_to_writer_reports(
            infos,
            &target_metadata,
        )
        .expect("decode writer reports");
        assert_eq!(reports.len(), 1);
        let data_file = &reports[0].file;
        let path = data_file.path.as_str();
        assert!(
            path.starts_with(&format!("{data_location}/id_part=7/delete-")),
            "position delete sink should keep manual delete file naming, got {path}"
        );
        assert_eq!(data_file.content, IcebergFileContent::PositionDeletes);
        assert_eq!(data_file.partition.partition_spec_id, 7);
        assert_eq!(target_metadata.default_partition_spec_id(), 7);
        assert_eq!(
            data_file.partition.partition_values,
            iceberg::spec::Struct::from_iter([Some(iceberg::spec::Literal::int(7))])
        );
        assert_eq!(data_file.referenced_data_file.as_deref(), Some(referenced));
    }

    #[test]
    fn position_delete_backend_groups_by_referenced_data_file_partition_spec() {
        let dir = tempfile::Builder::new()
            .prefix("novarocks-iceberg-position-delete-evolved-spec-")
            .tempdir()
            .expect("temp dir");
        let table_location = format!("file://{}", dir.path().display());
        let data_location = format!("{table_location}/custom-data");
        let (target_metadata, old_spec_id, new_spec_id) =
            test_bucket_evolution_metadata(&table_location);
        let old_file = "file:///tmp/old-spec-data.parquet";
        let new_file = "file:///tmp/new-spec-data.parquet";
        let mut referenced_partitions = HashMap::new();
        referenced_partitions.insert(
            old_file.to_string(),
            PositionDeleteDataFilePartition {
                partition_spec_id: old_spec_id,
                partition_values: iceberg::spec::Struct::from_iter([Some(
                    iceberg::spec::Literal::int(1),
                )]),
            },
        );
        referenced_partitions.insert(
            new_file.to_string(),
            PositionDeleteDataFilePartition {
                partition_spec_id: new_spec_id,
                partition_values: iceberg::spec::Struct::from_iter([Some(
                    iceberg::spec::Literal::int(2),
                )]),
            },
        );

        let source_schema = Arc::new(Schema::new(vec![
            Field::new("file_path", DataType::Utf8, false),
            Field::new("pos", DataType::Int64, false),
        ]));
        let file_slot = SlotId::new(11);
        let pos_slot = SlotId::new(12);
        let mut arena = crate::exec::expr::ExprArena::default();
        let file_expr = arena.push_typed(ExprNode::SlotId(file_slot), DataType::Utf8);
        let pos_expr = arena.push_typed(ExprNode::SlotId(pos_slot), DataType::Int64);
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Int64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));
        let plan = Arc::new(IcebergSinkPlan {
            mode: IcebergSinkMode::PositionDeletes,
            table_location,
            data_location,
            target_partition_spec_id: new_spec_id,
            target_table_metadata: Some(target_metadata.clone()),
            target_snapshot_id: None,
            position_delete_data_file_partitions: referenced_partitions,
            object_store_s3: None,
            file_format: crate::connector::iceberg::delete_file::IcebergFileFormat::Parquet,
            report_file_format: "parquet".to_string(),
            compression: Compression::SNAPPY,
            output_schema: position_delete_required_schema_for_tests(),
            target_schema,
            equality_delete_columns: Vec::new(),
            row_lineage_data: false,
            output_exprs: vec![file_expr, pos_expr],
            partition_exprs: Vec::new(),
            partition_source_column_names: Vec::new(),
            partition_column_names: Vec::new(),
            transform_exprs: Vec::new(),
            position_delete_binding: None,
        });
        let mut backend = IcebergTableSinkBackend {
            arena: Arc::new(arena),
            plan,
            driver_id: 9,
            file_seq: 0,
            runtime_state: None,
            pending_deletion_vectors: BTreeMap::new(),
        };
        let batch = RecordBatch::try_new(
            source_schema,
            vec![
                Arc::new(StringArray::from(vec![old_file, new_file])),
                Arc::new(Int64Array::from(vec![3_i64, 4_i64])),
            ],
        )
        .expect("record batch");
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[file_slot, pos_slot],
        )
        .expect("chunk schema");
        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk");
        let finst_id = UniqueId { hi: 703, lo: 42 };
        crate::runtime::sink_commit::unregister(finst_id);
        let state = RuntimeState::new(
            None,
            None,
            None,
            None,
            Some(finst_id),
            None,
            None,
            None,
            None,
        );

        backend
            .push_chunk_position_delete(&state, chunk)
            .expect("write position deletes");

        let infos = crate::runtime::sink_commit::list(finst_id);
        crate::runtime::sink_commit::unregister(finst_id);
        let reports = crate::runtime::sink_commit_wire::sink_commit_infos_to_writer_reports(
            infos,
            &target_metadata,
        )
        .expect("decode writer reports");
        assert_eq!(reports.len(), 2);
        let mut reports = reports
            .into_iter()
            .map(|report| {
                let data_file = report.file;
                (
                    data_file.referenced_data_file.expect("referenced file"),
                    data_file.partition.partition_spec_id,
                    data_file.partition.partition_values,
                )
            })
            .collect::<Vec<_>>();
        reports.sort_by(|left, right| left.0.cmp(&right.0));

        assert_eq!(reports[0].0, new_file);
        assert_eq!(reports[0].1, new_spec_id);
        assert_eq!(
            reports[0].2,
            iceberg::spec::Struct::from_iter([Some(iceberg::spec::Literal::int(2))])
        );
        assert_eq!(reports[1].0, old_file);
        assert_eq!(reports[1].1, old_spec_id);
        assert_eq!(
            reports[1].2,
            iceberg::spec::Struct::from_iter([Some(iceberg::spec::Literal::int(1))])
        );
    }

    fn build_deletion_vector_backend_chunk(
        table_location: String,
        data_location: String,
        referenced_data_file: &str,
        positions: Vec<i64>,
        finst_id: UniqueId,
    ) -> (
        IcebergTableSinkBackend,
        RuntimeState,
        Chunk,
        iceberg::spec::TableMetadata,
    ) {
        let source_schema = Arc::new(Schema::new(vec![
            Field::new("file_path", DataType::Utf8, false),
            Field::new("pos", DataType::Int64, false),
            Field::new("id", DataType::Int32, false),
        ]));
        let file_slot = SlotId::new(1);
        let pos_slot = SlotId::new(2);
        let id_slot = SlotId::new(3);
        let mut arena = crate::exec::expr::ExprArena::default();
        let file_expr = arena.push_typed(ExprNode::SlotId(file_slot), DataType::Utf8);
        let pos_expr = arena.push_typed(ExprNode::SlotId(pos_slot), DataType::Int64);
        let id_expr = arena.push_typed(ExprNode::SlotId(id_slot), DataType::Int32);
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "42".to_string(),
            )])),
        ]));
        let target_metadata = test_identity_partition_metadata(
            &table_location,
            &data_location,
            Arc::clone(&target_schema),
            7,
        );
        let mut referenced_partitions = HashMap::new();
        referenced_partitions.insert(
            referenced_data_file.to_string(),
            PositionDeleteDataFilePartition {
                partition_spec_id: target_metadata.default_partition_spec_id(),
                partition_values: iceberg::spec::Struct::from_iter([Some(
                    iceberg::spec::Literal::int(7),
                )]),
            },
        );
        let plan = Arc::new(IcebergSinkPlan {
            mode: IcebergSinkMode::DeletionVectors,
            table_location,
            data_location,
            target_partition_spec_id: 7,
            target_table_metadata: Some(target_metadata.clone()),
            target_snapshot_id: None,
            position_delete_data_file_partitions: referenced_partitions,
            object_store_s3: None,
            file_format: crate::connector::iceberg::delete_file::IcebergFileFormat::Parquet,
            report_file_format: "parquet".to_string(),
            compression: Compression::SNAPPY,
            output_schema: position_delete_required_schema_for_tests(),
            target_schema,
            equality_delete_columns: Vec::new(),
            row_lineage_data: false,
            output_exprs: vec![file_expr, pos_expr, id_expr],
            partition_exprs: vec![id_expr],
            partition_source_column_names: vec!["id".to_string()],
            partition_column_names: vec!["id_part".to_string()],
            transform_exprs: vec!["identity".to_string()],
            position_delete_binding: None,
        });
        let files = vec![referenced_data_file; positions.len()];
        let ids = vec![7_i32; positions.len()];
        let batch = RecordBatch::try_new(
            source_schema,
            vec![
                Arc::new(StringArray::from(files)),
                Arc::new(Int64Array::from(positions)),
                Arc::new(Int32Array::from(ids)),
            ],
        )
        .expect("record batch");
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[file_slot, pos_slot, id_slot],
        )
        .expect("chunk schema");
        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk");
        crate::runtime::sink_commit::unregister(finst_id);
        let state = RuntimeState::new(
            None,
            None,
            None,
            None,
            Some(finst_id),
            None,
            None,
            None,
            None,
        );
        let backend = IcebergTableSinkBackend {
            arena: Arc::new(arena),
            plan,
            driver_id: 13,
            file_seq: 0,
            runtime_state: None,
            pending_deletion_vectors: BTreeMap::new(),
        };
        (backend, state, chunk, target_metadata)
    }

    fn build_deletion_vector_chunk(referenced_data_file: &str, positions: Vec<i64>) -> Chunk {
        let source_schema = Arc::new(Schema::new(vec![
            Field::new("file_path", DataType::Utf8, false),
            Field::new("pos", DataType::Int64, false),
            Field::new("id", DataType::Int32, false),
        ]));
        let file_slot = SlotId::new(1);
        let pos_slot = SlotId::new(2);
        let id_slot = SlotId::new(3);
        let files = vec![referenced_data_file; positions.len()];
        let ids = vec![7_i32; positions.len()];
        let batch = RecordBatch::try_new(
            source_schema,
            vec![
                Arc::new(StringArray::from(files)),
                Arc::new(Int64Array::from(positions)),
                Arc::new(Int32Array::from(ids)),
            ],
        )
        .expect("record batch");
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[file_slot, pos_slot, id_slot],
        )
        .expect("chunk schema");
        Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk")
    }

    #[test]
    fn push_chunk_deletion_vector_reports_puffin_descriptor() {
        let dir = tempfile::Builder::new()
            .prefix("novarocks-iceberg-dv-write-")
            .tempdir()
            .expect("temp dir");
        let table_location = dir.path().display().to_string();
        let data_location = dir.path().join("custom-data").display().to_string();
        let referenced = "f.parquet";
        let finst_id = UniqueId { hi: 704, lo: 42 };
        let (mut backend, state, chunk, target_metadata) = build_deletion_vector_backend_chunk(
            table_location,
            data_location.clone(),
            referenced,
            vec![0, 2],
            finst_id,
        );

        backend
            .push_chunk_deletion_vector(&state, chunk)
            .expect("buffer deletion vector");

        let infos = crate::runtime::sink_commit::list(finst_id);
        assert!(
            infos.is_empty(),
            "deletion-vector sink should report only during finish"
        );
        backend
            .bind_runtime_state(&state)
            .expect("bind runtime state");
        crate::runtime::global_async_runtime::data_block_on(backend.finish())
            .expect("finish runtime")
            .expect("finish deletion vectors");

        let infos = crate::runtime::sink_commit::list(finst_id);
        crate::runtime::sink_commit::unregister(finst_id);
        let reports = crate::runtime::sink_commit_wire::sink_commit_infos_to_writer_reports(
            infos,
            &target_metadata,
        )
        .expect("decode writer reports");
        assert_eq!(reports.len(), 1);
        let data_file = &reports[0].file;
        assert_eq!(data_file.format, "puffin");
        assert_eq!(data_file.content, IcebergFileContent::PositionDeletes);
        assert_eq!(data_file.referenced_data_file.as_deref(), Some(referenced));
        assert_eq!(data_file.cardinality, Some(2));
        assert_eq!(data_file.record_count, 2);
        assert!(data_file.content_offset.is_some());
        assert!(data_file.content_size_in_bytes.is_some());
        assert!(data_file.path.ends_with(".puffin"));
        assert_eq!(data_file.partition.partition_path, "id_part=7");
        assert_eq!(
            data_file.partition.partition_values,
            iceberg::spec::Struct::from_iter([Some(iceberg::spec::Literal::int(7))])
        );
    }

    #[test]
    fn merge_deletion_vector_positions_preserves_existing_positions() {
        let mut old = crate::connector::iceberg::commit::DeletionVector::new();
        old.insert(5).expect("old position");
        let existing = HashMap::from([("f.parquet".to_string(), old)]);
        let positions = BTreeMap::from([("f.parquet".to_string(), vec![0_u64, 2])]);

        let merged =
            merge_deletion_vectors_by_file(existing, &positions).expect("merge deletion vectors");

        let dv = merged.get("f.parquet").expect("merged dv");
        assert_eq!(dv.cardinality(), 3);
        assert!(dv.contains(0));
        assert!(dv.contains(2));
        assert!(dv.contains(5));
    }

    #[test]
    fn push_chunk_deletion_vector_merges_same_file_across_chunks_at_finish() {
        let dir = tempfile::Builder::new()
            .prefix("novarocks-iceberg-dv-multi-chunk-")
            .tempdir()
            .expect("temp dir");
        let table_location = dir.path().display().to_string();
        let data_location = dir.path().join("custom-data").display().to_string();
        let referenced = "f.parquet";
        let finst_id = UniqueId { hi: 706, lo: 42 };
        let (mut backend, state, first_chunk, target_metadata) =
            build_deletion_vector_backend_chunk(
                table_location,
                data_location,
                referenced,
                vec![0, 2],
                finst_id,
            );
        let second_chunk = build_deletion_vector_chunk(referenced, vec![5]);

        backend
            .push_chunk_deletion_vector(&state, first_chunk)
            .expect("buffer first deletion-vector chunk");
        backend
            .push_chunk_deletion_vector(&state, second_chunk)
            .expect("buffer second deletion-vector chunk");
        let infos = crate::runtime::sink_commit::list(finst_id);
        assert!(
            infos.is_empty(),
            "deletion-vector sink should report only during finish"
        );
        backend
            .bind_runtime_state(&state)
            .expect("bind runtime state");
        crate::runtime::global_async_runtime::data_block_on(backend.finish())
            .expect("finish runtime")
            .expect("finish deletion vectors");

        let infos = crate::runtime::sink_commit::list(finst_id);
        crate::runtime::sink_commit::unregister(finst_id);
        assert_eq!(
            infos.len(),
            1,
            "same referenced data file should produce one Puffin DV per sink lifecycle"
        );
        let reports = crate::runtime::sink_commit_wire::sink_commit_infos_to_writer_reports(
            infos,
            &target_metadata,
        )
        .expect("decode writer reports");
        let data_file = &reports[0].file;
        assert_eq!(data_file.referenced_data_file.as_deref(), Some(referenced));
        assert_eq!(data_file.cardinality, Some(3));
        assert_eq!(data_file.record_count, 3);
    }

    #[test]
    fn push_chunk_deletion_vector_rejects_negative_position_without_commit_info() {
        let dir = tempfile::Builder::new()
            .prefix("novarocks-iceberg-dv-negative-")
            .tempdir()
            .expect("temp dir");
        let table_location = dir.path().display().to_string();
        let data_location = dir.path().join("custom-data").display().to_string();
        let referenced = "f.parquet";
        let finst_id = UniqueId { hi: 705, lo: 42 };
        let (mut backend, state, chunk, _target_metadata) = build_deletion_vector_backend_chunk(
            table_location,
            data_location,
            referenced,
            vec![-1],
            finst_id,
        );

        let err = backend
            .push_chunk_deletion_vector(&state, chunk)
            .expect_err("negative position should fail");

        assert!(
            err.contains("pos must be non-negative"),
            "unexpected error: {err}"
        );
        let infos = crate::runtime::sink_commit::list(finst_id);
        crate::runtime::sink_commit::unregister(finst_id);
        assert!(infos.is_empty());
    }

    #[test]
    fn test_align_arrays_to_schema_casts_int64_to_int32() {
        let schema = Arc::new(Schema::new(vec![Field::new("c0", DataType::Int32, true)]));
        let arrays: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(2)]))];

        let aligned = align_arrays_to_schema(arrays, &schema).expect("align arrays");
        assert_eq!(aligned.len(), 1);
        assert_eq!(aligned[0].data_type(), &DataType::Int32);

        let out = aligned[0]
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 array");
        assert_eq!(out.len(), 3);
        assert_eq!(out.value(0), 1);
        assert!(out.is_null(1));
        assert_eq!(out.value(2), 2);
    }

    #[test]
    fn test_align_arrays_to_schema_casts_null_array_to_target_type() {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
        let arrays: Vec<ArrayRef> = vec![Arc::new(arrow::array::NullArray::new(3))];

        let aligned = align_arrays_to_schema(arrays, &schema).expect("align null array");
        assert_eq!(aligned.len(), 1);
        assert_eq!(aligned[0].data_type(), &DataType::Int64);
        assert_eq!(aligned[0].len(), 3);
        assert_eq!(aligned[0].null_count(), 3);
    }

    #[test]
    fn test_align_arrays_to_schema_rejects_lossy_cast() {
        let schema = Arc::new(Schema::new(vec![Field::new("c0", DataType::Int32, true)]));
        let arrays: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![Some(i64::MAX)]))];

        let err = align_arrays_to_schema(arrays, &schema).expect_err("should fail");
        assert!(err.contains("introduced nulls"));
    }

    #[test]
    fn test_iceberg_partition_key_formats_transforms_and_null_fingerprint() {
        let partition_column_names = vec![
            "p_year".to_string(),
            "p_bucket".to_string(),
            "p_void".to_string(),
        ];
        let transform_exprs = vec![
            "year".to_string(),
            "bucket[16]".to_string(),
            "void".to_string(),
        ];
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![Some(2)])),
            Arc::new(Int32Array::from(vec![Some(7)])),
            Arc::new(Int32Array::from(vec![Some(123)])),
        ];

        let (path, fingerprint, partition_values) =
            iceberg_partition_key_for_row(&partition_column_names, &transform_exprs, &arrays, 0)
                .expect("partition key");

        assert_eq!(path, "p_year=1972/p_bucket=7/p_void=null/");
        assert_eq!(fingerprint, "001");
        assert_eq!(
            partition_values,
            iceberg::spec::Struct::from_iter([
                Some(iceberg::spec::Literal::int(2)),
                Some(iceberg::spec::Literal::int(7)),
                None,
            ])
        );
    }

    #[test]
    fn test_write_parquet_to_bytes_collects_iceberg_metrics() {
        let mut metadata = HashMap::new();
        metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string());
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true).with_metadata(metadata),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![Some(1), Some(2)]))],
        )
        .expect("record batch");
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let (_bytes, write_result) =
            write_parquet_to_bytes(Arc::clone(&schema), &batch, props).expect("write parquet");

        let split_offsets = write_result.split_offsets.expect("split offsets");
        assert_eq!(split_offsets.len(), 1);
        let column_stats = write_result.column_stats.expect("column stats");
        let value_counts = column_stats.value_counts;
        let lower_bounds = column_stats.lower_bounds;
        let upper_bounds = column_stats.upper_bounds;

        assert_eq!(value_counts.get(&1), Some(&2));
        assert_eq!(
            i64::from_le_bytes(
                lower_bounds[&1]
                    .as_slice()
                    .try_into()
                    .expect("lower bound bytes")
            ),
            1
        );
        assert_eq!(
            i64::from_le_bytes(
                upper_bounds[&1]
                    .as_slice()
                    .try_into()
                    .expect("upper bound bytes")
            ),
            2
        );
    }

    #[test]
    fn test_write_parquet_to_bytes_counts_null_rows_once() {
        let mut metadata = HashMap::new();
        metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string());
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true).with_metadata(metadata),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![Option::<i64>::None]))],
        )
        .expect("record batch");
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let (_bytes, write_result) =
            write_parquet_to_bytes(Arc::clone(&schema), &batch, props).expect("write parquet");

        let column_stats = write_result.column_stats.expect("column stats");
        let value_counts = column_stats.value_counts;
        let null_value_counts = column_stats.null_value_counts;

        assert_eq!(value_counts.get(&1), Some(&1));
        assert_eq!(null_value_counts.get(&1), Some(&1));
    }

    #[test]
    fn test_position_delete_schema_carries_iceberg_field_ids() {
        let schema = position_delete_required_schema_for_tests();
        assert_eq!(schema.fields().len(), 2);

        let file_path_field = schema.field(0);
        assert_eq!(file_path_field.name(), "file_path");
        assert_eq!(file_path_field.data_type(), &DataType::Utf8);
        assert!(!file_path_field.is_nullable());
        assert_eq!(
            file_path_field.metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&"2147483546".to_string()),
        );

        let pos_field = schema.field(1);
        assert_eq!(pos_field.name(), "pos");
        assert_eq!(pos_field.data_type(), &DataType::Int64);
        assert!(!pos_field.is_nullable());
        assert_eq!(
            pos_field.metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&"2147483545".to_string()),
        );
    }

    #[test]
    fn test_position_delete_parquet_preserves_spec_field_ids() {
        // Round-trip check: write a position-delete batch out to parquet bytes
        // and confirm the parquet file-level schema exposes the Iceberg-spec
        // field ids. External engines (Spark/Trino) key off these exact ids.
        let schema = position_delete_required_schema_for_tests();
        let file_paths = Arc::new(StringArray::from(vec![
            "s3://b/path/data-a.parquet",
            "s3://b/path/data-a.parquet",
        ]));
        let positions = Arc::new(Int64Array::from(vec![0_i64, 5]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![file_paths, positions])
            .expect("build record batch");

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let (bytes, _write_result) =
            write_parquet_to_bytes(Arc::clone(&schema), &batch, props).expect("write parquet");

        let reader =
            SerializedFileReader::new(bytes::Bytes::from(bytes)).expect("open parquet bytes");
        let schema_desc = reader.metadata().file_metadata().schema_descr();
        let columns = schema_desc.columns();
        assert_eq!(columns.len(), 2);
        // Parquet ColumnDescriptor carries the field_id from SchemaElement::field_id.
        assert_eq!(columns[0].self_type().get_basic_info().id(), 2_147_483_546);
        assert_eq!(columns[1].self_type().get_basic_info().id(), 2_147_483_545);
    }

    #[test]
    fn test_position_delete_parquet_roundtrip_values() {
        // Ensures the file_path / pos values survive write + read and that the
        // ArrowReader can reconstruct them. Ordering by (file_path, pos) is the
        // writer's responsibility; this test just validates the write plumbing.
        let schema = position_delete_required_schema_for_tests();
        let file_paths = Arc::new(StringArray::from(vec!["s3://b/a", "s3://b/a", "s3://b/b"]));
        let positions = Arc::new(Int64Array::from(vec![1_i64, 7, 0]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![file_paths, positions])
            .expect("build record batch");

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let (bytes, _) =
            write_parquet_to_bytes(Arc::clone(&schema), &batch, props).expect("write parquet");

        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
            .expect("builder")
            .build()
            .expect("reader");
        let mut out = Vec::new();
        for rb in reader {
            out.push(rb.expect("batch"));
        }
        assert_eq!(out.len(), 1);
        let rb = &out[0];
        assert_eq!(rb.num_rows(), 3);
        let fp = rb
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("fp col");
        let pos = rb
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("pos col");
        assert_eq!(fp.value(0), "s3://b/a");
        assert_eq!(pos.value(2), 0);
    }

    #[test]
    fn test_position_delete_writes_on_disk_for_external_verification() {
        // Drop a real parquet file under a known path so external tools
        // (pyarrow / pyiceberg / Spark) can cross-check the Iceberg spec
        // invariants (field_ids, required-ness, values) produced by this
        // writer. Skips cleanly when the target directory is not writable.
        let dir = std::env::var_os("NOVAROCKS_POS_DELETE_E2E_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| std::path::PathBuf::from("/tmp/novarocks-pos-delete-e2e"));
        if std::fs::create_dir_all(&dir).is_err() {
            return;
        }
        let schema = position_delete_required_schema_for_tests();
        let file_paths = Arc::new(StringArray::from(vec![
            "s3://bucket/data-a.parquet",
            "s3://bucket/data-a.parquet",
            "s3://bucket/data-b.parquet",
        ]));
        let positions = Arc::new(Int64Array::from(vec![0_i64, 5, 3]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![file_paths, positions])
            .expect("build record batch");
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let (bytes, _) = write_parquet_to_bytes(Arc::clone(&schema), &batch, props).unwrap();

        let path = dir.join("e2e_position_delete.parquet");
        std::fs::write(&path, &bytes).expect("write parquet tmp file");
        assert!(path.exists());
    }

    #[test]
    fn test_unique_file_path_uniform_and_mixed() {
        let schema = position_delete_required_schema_for_tests();

        let uniform = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["s3://b/a", "s3://b/a"])),
                Arc::new(Int64Array::from(vec![0_i64, 5])),
            ],
        )
        .expect("uniform batch");
        assert_eq!(
            unique_file_path(&uniform).expect("uniform"),
            Some("s3://b/a".to_string()),
        );

        let mixed = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["s3://b/a", "s3://b/b"])),
                Arc::new(Int64Array::from(vec![0_i64, 0])),
            ],
        )
        .expect("mixed batch");
        assert_eq!(unique_file_path(&mixed).expect("mixed"), None);

        let empty = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(Vec::<&str>::new())),
                Arc::new(Int64Array::from(Vec::<i64>::new())),
            ],
        )
        .expect("empty batch");
        assert_eq!(unique_file_path(&empty).expect("empty"), None);
    }
}
