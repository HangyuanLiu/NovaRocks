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

//! Provider-owned reader for Iceberg snapshot-delta split roles.

use std::collections::{BTreeSet, VecDeque};

use arrow::array::{Array, BooleanArray, Int64Array};
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use novarocks_fs::FileCancellation;
use novarocks_spi::connector::{
    ConnectorBatchReader, ConnectorError, ConnectorErrorKind, ConnectorOpenReaderRequest,
    ConnectorReaderMetricsSnapshot,
};

use super::delete_file::IcebergDeleteFileSpec;
use super::delta::{
    BaseDataFileLineage, DeltaScanDeleteSide, DeltaSourceFile, DeltaSourceRole,
    PositionDeleteFileFormat,
};
use super::provider::IcebergReadBinding;
use super::reader::IcebergBatchReader;
use super::scan_model::{
    IcebergDataFileInfo, IcebergDeleteFileContent, IcebergDeleteFileFormat, IcebergDeleteFileInfo,
};

struct DeltaReadJob {
    file: IcebergDataFileInfo,
    equality_match_only: bool,
    row_id_allow_list: Option<BTreeSet<i64>>,
}

pub(crate) struct IcebergDeltaBatchReader {
    binding: IcebergReadBinding,
    request: ConnectorOpenReaderRequest,
    cancellation: FileCancellation,
    pending: VecDeque<DeltaReadJob>,
    current: Option<IcebergBatchReader>,
    current_row_id_allow_list: Option<BTreeSet<i64>>,
    terminal_metrics: ConnectorReaderMetricsSnapshot,
    closed: bool,
}

impl IcebergDeltaBatchReader {
    pub(crate) fn try_new(
        source: DeltaSourceFile,
        delete_side: Option<DeltaScanDeleteSide>,
        binding: IcebergReadBinding,
        request: ConnectorOpenReaderRequest,
    ) -> Result<Self, ConnectorError> {
        let cancellation = FileCancellation::new();
        let pending = plan_jobs(
            &source,
            delete_side.as_ref(),
            &binding,
            &request,
            &cancellation,
        )?;
        Ok(Self {
            binding,
            request,
            cancellation,
            pending: pending.into(),
            current: None,
            current_row_id_allow_list: None,
            terminal_metrics: ConnectorReaderMetricsSnapshot::default(),
            closed: false,
        })
    }

    fn open_next(&mut self) -> Result<bool, ConnectorError> {
        let Some(job) = self.pending.pop_front() else {
            return Ok(false);
        };
        let access = self.binding.resolve_access(&job.file.path)?;
        let context = self
            .binding
            .file_read_context(self.cancellation.clone(), self.request.context.deadline())?;
        self.current = Some(IcebergBatchReader::try_new_delta_child(
            &job.file,
            access,
            self.request.clone(),
            context,
            job.equality_match_only,
        )?);
        self.current_row_id_allow_list = job.row_id_allow_list;
        Ok(true)
    }

    fn finish_current(&mut self) -> Result<(), ConnectorError> {
        if let Some(mut reader) = self.current.take() {
            self.terminal_metrics = self
                .terminal_metrics
                .saturating_add(reader.metrics_snapshot());
            reader.close()?;
        }
        self.current_row_id_allow_list = None;
        Ok(())
    }
}

impl ConnectorBatchReader for IcebergDeltaBatchReader {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        if self.closed {
            return Ok(None);
        }
        loop {
            if self.current.is_none() && !self.open_next()? {
                self.close()?;
                return Ok(None);
            }
            match self
                .current
                .as_mut()
                .expect("delta reader is open")
                .next_batch()?
            {
                Some(batch) => {
                    let batch =
                        filter_allowed_row_ids(batch, self.current_row_id_allow_list.as_ref())?;
                    if batch.num_rows() > 0 {
                        return Ok(Some(batch));
                    }
                }
                None => self.finish_current()?,
            }
        }
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        self.cancellation.cancel();
        self.finish_current()
    }

    fn metrics_snapshot(&self) -> ConnectorReaderMetricsSnapshot {
        self.current
            .as_ref()
            .map(|reader| {
                self.terminal_metrics
                    .saturating_add(reader.metrics_snapshot())
            })
            .unwrap_or(self.terminal_metrics)
    }
}

impl Drop for IcebergDeltaBatchReader {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

fn plan_jobs(
    source: &DeltaSourceFile,
    delete_side: Option<&DeltaScanDeleteSide>,
    binding: &IcebergReadBinding,
    request: &ConnectorOpenReaderRequest,
    cancellation: &FileCancellation,
) -> Result<Vec<DeltaReadJob>, ConnectorError> {
    match &source.role {
        DeltaSourceRole::DataFile => Ok(vec![DeltaReadJob {
            file: source_as_data_file(source, 1, Vec::new(), None),
            equality_match_only: false,
            row_id_allow_list: source.row_id_allow_list.clone(),
        }]),
        DeltaSourceRole::PositionDelete { deletes } => {
            let side = require_delete_side(delete_side, "position-delete")?;
            let mut lineage = side.base_data_file_lineage.clone();
            for (path, value) in &side.previous_data_file_lineage {
                lineage.entry(path.clone()).or_insert(*value);
            }
            let mut jobs = Vec::new();
            for (path, file_lineage) in lineage {
                if side.deleted_data_file_paths.contains(&path) {
                    continue;
                }
                let specs = deletes
                    .iter()
                    .filter(|delete| position_delete_applies_to_data_file(delete, &path))
                    .map(position_delete_spec)
                    .collect::<Result<Vec<_>, _>>()?;
                if specs.is_empty() {
                    continue;
                }
                let access = binding.resolve_access(&path)?;
                let context =
                    binding.file_read_context(cancellation.clone(), request.context.deadline())?;
                let mut positions = super::position_delete::load_position_deletes_with_context(
                    &specs, &path, &access, &context,
                )
                .map_err(corrupt)?;
                if let Some(previous) = side.previously_deleted_positions_per_file.get(&path) {
                    for position in previous {
                        positions.remove(*position);
                    }
                }
                if positions.is_empty() {
                    continue;
                }
                let size = binding.file_size(&path, &access, &context)?;
                jobs.push(DeltaReadJob {
                    file: IcebergDataFileInfo {
                        path,
                        size: i64::try_from(size).map_err(|_| {
                            ConnectorError::new(
                                ConnectorErrorKind::ResourceExhausted,
                                "Iceberg delta data file size exceeds Int64",
                            )
                        })?,
                        row_count: None,
                        column_stats: None,
                        partition_spec_id: None,
                        partition_key: None,
                        first_row_id: Some(file_lineage.first_row_id),
                        data_sequence_number: Some(file_lineage.data_sequence_number),
                        ivm_change_op: Some(-1),
                        included_positions: Some(
                            positions
                                .iter()
                                .map(|value| {
                                    i64::try_from(value).map_err(|_| {
                                        corrupt("Iceberg delta row position exceeds Int64")
                                    })
                                })
                                .collect::<Result<Vec<_>, _>>()?,
                        ),
                        delete_files: Vec::new(),
                        manifest_path: None,
                        partition_values: Vec::new(),
                    },
                    equality_match_only: false,
                    row_id_allow_list: None,
                });
            }
            Ok(jobs)
        }
        DeltaSourceRole::EqualityDelete {
            equality_field_ids,
            targets,
        } => {
            let delete = IcebergDeleteFileInfo {
                path: source.path.clone(),
                file_format: IcebergDeleteFileFormat::Parquet,
                file_content: IcebergDeleteFileContent::Equality,
                length: Some(source.size),
                content_offset: None,
                content_size_in_bytes: None,
                sequence_number: source.data_sequence_number,
                partition_spec_id: source.partition_spec_id,
                partition_key: source.partition_key.clone(),
                equality_column_names: Vec::new(),
                equality_field_ids: equality_field_ids.clone(),
            };
            Ok(targets
                .iter()
                .map(|target| DeltaReadJob {
                    file: IcebergDataFileInfo {
                        path: target.data_file_path.clone(),
                        size: target.data_file_size,
                        row_count: None,
                        column_stats: None,
                        partition_spec_id: None,
                        partition_key: None,
                        first_row_id: target.data_file_first_row_id,
                        data_sequence_number: target.data_file_sequence_number,
                        ivm_change_op: Some(-1),
                        included_positions: None,
                        delete_files: vec![delete.clone()],
                        manifest_path: None,
                        partition_values: Vec::new(),
                    },
                    equality_match_only: true,
                    row_id_allow_list: None,
                })
                .collect())
        }
        DeltaSourceRole::DeletedDataFile { .. } => {
            let side = require_delete_side(delete_side, "deleted-data-file")?;
            let lineage = side
                .previous_data_file_lineage
                .get(&source.path)
                .copied()
                .or_else(|| source_lineage(source))
                .ok_or_else(|| {
                    corrupt(format!(
                        "Iceberg delta deleted data file {} has no row-lineage facts",
                        source.path
                    ))
                })?;
            let delete_files = side
                .previous_delete_visibility_data_files
                .iter()
                .find(|file| file.path == source.path)
                .map(delete_visibility_specs)
                .transpose()?
                .unwrap_or_default();
            Ok(vec![DeltaReadJob {
                file: IcebergDataFileInfo {
                    path: source.path.clone(),
                    size: source.size,
                    row_count: None,
                    column_stats: None,
                    partition_spec_id: source.partition_spec_id,
                    partition_key: source.partition_key.clone(),
                    first_row_id: Some(lineage.first_row_id),
                    data_sequence_number: Some(lineage.data_sequence_number),
                    ivm_change_op: Some(-1),
                    included_positions: None,
                    delete_files,
                    manifest_path: None,
                    partition_values: Vec::new(),
                },
                equality_match_only: false,
                row_id_allow_list: None,
            }])
        }
    }
}

fn source_as_data_file(
    source: &DeltaSourceFile,
    change_op: i8,
    delete_files: Vec<IcebergDeleteFileInfo>,
    included_positions: Option<Vec<i64>>,
) -> IcebergDataFileInfo {
    IcebergDataFileInfo {
        path: source.path.clone(),
        size: source.size,
        row_count: None,
        column_stats: None,
        partition_spec_id: source.partition_spec_id,
        partition_key: source.partition_key.clone(),
        first_row_id: source.first_row_id,
        data_sequence_number: source.data_sequence_number,
        ivm_change_op: Some(change_op),
        included_positions,
        delete_files,
        manifest_path: None,
        partition_values: Vec::new(),
    }
}

fn position_delete_spec(
    delete: &super::delta::PositionDeleteSourceData,
) -> Result<IcebergDeleteFileSpec, ConnectorError> {
    let length = u64::try_from(delete.delete_file_size).ok();
    match delete.file_format {
        PositionDeleteFileFormat::Parquet => Ok(IcebergDeleteFileSpec::parquet_position_delete(
            delete.delete_file_path.clone(),
            length,
        )),
        PositionDeleteFileFormat::Puffin => Ok(IcebergDeleteFileSpec::puffin_position_delete(
            delete.delete_file_path.clone(),
            length,
            delete
                .content_offset
                .ok_or_else(|| corrupt("Iceberg delta Puffin delete is missing content_offset"))?,
            delete.content_size_in_bytes.ok_or_else(|| {
                corrupt("Iceberg delta Puffin delete is missing content_size_in_bytes")
            })?,
        )),
    }
}

fn position_delete_applies_to_data_file(
    delete: &super::delta::PositionDeleteSourceData,
    data_file_path: &str,
) -> bool {
    match delete.file_format {
        // Parquet position-delete files carry one target path per row, so the
        // reader must retain them and perform the row-level path filter.
        PositionDeleteFileFormat::Parquet => true,
        // A Puffin deletion vector has no per-row file-path column. Its
        // manifest-level referenced_data_file is therefore the only target
        // identity; applying it to every live file aliases positions across
        // files and corrupts v3 row lineage.
        PositionDeleteFileFormat::Puffin => delete
            .referenced_data_file
            .as_deref()
            .is_some_and(|referenced| referenced == data_file_path),
    }
}

fn delete_visibility_specs(
    file: &super::changes::DeleteVisibilityDataFileDescriptor,
) -> Result<Vec<IcebergDeleteFileInfo>, ConnectorError> {
    file.delete_files
        .iter()
        .map(|delete| {
            Ok(IcebergDeleteFileInfo {
                path: delete.path.clone(),
                file_format: match delete.file_format {
                    super::changes::DeleteVisibilityDeleteFileFormat::Parquet => {
                        IcebergDeleteFileFormat::Parquet
                    }
                    super::changes::DeleteVisibilityDeleteFileFormat::Puffin => {
                        IcebergDeleteFileFormat::Puffin
                    }
                },
                file_content: match delete.file_content {
                    super::changes::DeleteVisibilityDeleteFileContent::Position => {
                        IcebergDeleteFileContent::Position
                    }
                    super::changes::DeleteVisibilityDeleteFileContent::Equality => {
                        IcebergDeleteFileContent::Equality
                    }
                },
                length: delete.length,
                content_offset: delete.content_offset,
                content_size_in_bytes: delete.content_size_in_bytes,
                sequence_number: None,
                partition_spec_id: None,
                partition_key: None,
                equality_column_names: Vec::new(),
                equality_field_ids: Vec::new(),
            })
        })
        .collect()
}

fn source_lineage(source: &DeltaSourceFile) -> Option<BaseDataFileLineage> {
    Some(BaseDataFileLineage {
        first_row_id: source.first_row_id?,
        data_sequence_number: source.data_sequence_number?,
    })
}

fn require_delete_side<'a>(
    side: Option<&'a DeltaScanDeleteSide>,
    role: &str,
) -> Result<&'a DeltaScanDeleteSide, ConnectorError> {
    side.ok_or_else(|| {
        corrupt(format!(
            "Iceberg delta {role} split is missing delete-side facts"
        ))
    })
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message.into())
}

fn filter_allowed_row_ids(
    batch: RecordBatch,
    allowed: Option<&BTreeSet<i64>>,
) -> Result<RecordBatch, ConnectorError> {
    let Some(allowed) = allowed else {
        return Ok(batch);
    };
    let row_ids = batch
        .column_by_name("_row_id")
        .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| {
            corrupt("Iceberg delta row_id_allow_list requires provider output column _row_id")
        })?;
    let mask = BooleanArray::from(
        (0..row_ids.len())
            .map(|index| !row_ids.is_null(index) && allowed.contains(&row_ids.value(index)))
            .collect::<Vec<_>>(),
    );
    let filtered = filter_record_batch(&batch, &mask)
        .map_err(|error| corrupt(format!("filter Iceberg delta row_id_allow_list: {error}")))?;
    Ok(filtered)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn puffin_delete(
        referenced_data_file: Option<&str>,
    ) -> super::super::delta::PositionDeleteSourceData {
        super::super::delta::PositionDeleteSourceData {
            delete_file_path: "s3://bucket/table/delete.puffin".to_string(),
            delete_file_size: 1,
            referenced_data_file: referenced_data_file.map(str::to_string),
            file_format: PositionDeleteFileFormat::Puffin,
            content_offset: Some(0),
            content_size_in_bytes: Some(1),
        }
    }

    #[test]
    fn puffin_deletion_vector_only_applies_to_its_referenced_data_file() {
        let delete = puffin_delete(Some("s3://bucket/table/data-new.parquet"));

        assert!(position_delete_applies_to_data_file(
            &delete,
            "s3://bucket/table/data-new.parquet"
        ));
        assert!(!position_delete_applies_to_data_file(
            &delete,
            "s3://bucket/table/data-old.parquet"
        ));
    }

    #[test]
    fn parquet_position_delete_remains_row_filtered() {
        let mut delete = puffin_delete(None);
        delete.file_format = PositionDeleteFileFormat::Parquet;
        delete.content_offset = None;
        delete.content_size_in_bytes = None;

        assert!(position_delete_applies_to_data_file(
            &delete,
            "s3://bucket/table/any-data-file.parquet"
        ));
    }
}
