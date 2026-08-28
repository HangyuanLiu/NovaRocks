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

//! The page source of `REWRITE_POSITION_DELETE_FILES`.
//!
//! It is the one reader in this stack that never opens a data file. Its input
//! is the set of Puffin deletion vectors the frozen rewrite group selected for
//! one data file, and its output is the two columns an Iceberg position-delete
//! file holds: the data file's path, and each deleted row's absolute position
//! inside it.
//!
//! Positions are re-encoded, never re-derived. The rewrite exists to repack the
//! delete artifacts of a data file without changing which rows they remove, so
//! reading a vector the group did not select, or losing one it did, would
//! change the relation's visible rows rather than a query's answer.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use novarocks_fs::{FileReadBudget, FileReadContext};
use novarocks_spi::connector::ConnectorError;
use novarocks_spi::connector::read_stack::{ConnectorPageSource, PageSourceMetrics, SourcePage};

use crate::access_binding::IcebergReadBinding;
use crate::delete_file::{IcebergDeleteFileSpec, IcebergFileContent, IcebergFileFormat};
use crate::position_delete::load_position_deletes_with_context;

use super::column_handle::{IcebergColumnHandle, invalid};
use super::schema_binding::IcebergMetadataColumn;
use super::split::IcebergDeleteFile;
use super::table_execute::{
    IcebergRewritePositionDeleteFilesSplit, REWRITE_POSITION_DELETE_OUTPUT_COLUMNS,
};

/// The frozen facts one rewrite-position page source reads.
pub struct IcebergRewritePositionDeleteFilesPageSourceRequest<'a> {
    pub split: &'a IcebergRewritePositionDeleteFilesSplit,
    /// The scan's ordered output columns. Channel `i` is produced for
    /// `columns[i]`, so their order -- not this module's -- is the contract.
    pub columns: &'a [IcebergColumnHandle],
    pub access_binding: IcebergReadBinding,
    pub context: FileReadContext,
    pub budget: FileReadBudget,
}

/// Which of the two output columns one page channel carries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RewritePositionOutput {
    FilePath,
    Position,
}

/// Resolve the scan's ordered columns onto the two columns this reader has.
///
/// A column it cannot produce is refused here rather than filled with nulls: a
/// deletion-vector rewrite that emitted an unbound column would write a
/// position-delete file whose rows name nothing.
fn resolve_outputs(
    columns: &[IcebergColumnHandle],
) -> Result<Vec<RewritePositionOutput>, ConnectorError> {
    columns
        .iter()
        .map(|column| {
            if !column.is_base_column() {
                return Err(invalid(format!(
                    "the iceberg rewrite-position reader produces no nested field, but column '{}' dereferences one",
                    column.base_column_identity().name()
                )));
            }
            match IcebergMetadataColumn::from_field_id(column.base_field_id()) {
                Some(IcebergMetadataColumn::Path)
                    if column.base_column_identity().name()
                        == REWRITE_POSITION_DELETE_OUTPUT_COLUMNS[0].0 =>
                {
                    Ok(RewritePositionOutput::FilePath)
                }
                Some(IcebergMetadataColumn::RowPosition)
                    if column.base_column_identity().name()
                        == REWRITE_POSITION_DELETE_OUTPUT_COLUMNS[1].0 =>
                {
                    Ok(RewritePositionOutput::Position)
                }
                Some(IcebergMetadataColumn::Path | IcebergMetadataColumn::RowPosition) => {
                    Err(invalid(format!(
                        "the iceberg rewrite-position reader requires canonical output column names file_path and pos, not '{}'",
                        column.base_column_identity().name()
                    )))
                }
                Some(
                    IcebergMetadataColumn::RowId
                    | IcebergMetadataColumn::LastUpdatedSequenceNumber
                    | IcebergMetadataColumn::IsDeleted,
                )
                | None => Err(invalid(format!(
                    "the iceberg rewrite-position reader produces only the data file path and the deleted row position, not column '{}'",
                    column.base_column_identity().name()
                ))),
            }
        })
        .collect()
}

/// The Puffin content ranges one split selected, as the delete loader names
/// them.
///
/// [`IcebergRewritePositionDeleteFilesSplit`] already proved every selected
/// delete is a Puffin deletion vector with a content range, so this conversion
/// restates those facts rather than re-deciding them.
fn selected_delete_specs(
    deletes: &[IcebergDeleteFile],
) -> Result<Vec<IcebergDeleteFileSpec>, ConnectorError> {
    deletes
        .iter()
        .map(|delete| {
            let content_offset = delete.content_offset().ok_or_else(|| {
                invalid(format!(
                    "iceberg rewrite-position deletion vector {} has no puffin content offset",
                    delete.path()
                ))
            })?;
            let content_size_in_bytes = delete.content_size_in_bytes().ok_or_else(|| {
                invalid(format!(
                    "iceberg rewrite-position deletion vector {} has no puffin content size",
                    delete.path()
                ))
            })?;
            Ok(IcebergDeleteFileSpec {
                path: delete.path().to_string(),
                file_format: IcebergFileFormat::Puffin,
                file_content: IcebergFileContent::PositionDeletes,
                length: u64::try_from(delete.file_size_in_bytes()).ok(),
                content_offset: Some(content_offset),
                content_size_in_bytes: Some(content_size_in_bytes),
                referenced_data_file: delete.referenced_data_file().map(str::to_string),
            })
        })
        .collect()
}

/// Open the page source for one rewrite-position split.
///
/// Every selected vector is loaded up front: they belong to one data file, the
/// group froze how many there are, and the merged positions must be complete
/// and ordered before the first row can be emitted at all. `RoaringTreemap`
/// iteration is ascending and set-valued, so the union of the vectors is
/// sorted and deduplicated by construction -- which is what an Iceberg
/// position-delete file requires.
pub fn create_iceberg_rewrite_position_delete_files_page_source(
    request: IcebergRewritePositionDeleteFilesPageSourceRequest<'_>,
) -> Result<Box<dyn ConnectorPageSource>, ConnectorError> {
    let IcebergRewritePositionDeleteFilesPageSourceRequest {
        split,
        columns,
        access_binding,
        context,
        budget,
    } = request;

    let outputs = resolve_outputs(columns)?;
    let specs = selected_delete_specs(split.selected_position_deletes())?;
    let access =
        access_binding.resolve_access_for_locations(specs.iter().map(|spec| spec.path.as_str()))?;
    let bytes_read = specs
        .iter()
        .filter_map(|spec| spec.content_size_in_bytes)
        .map(|size| u64::try_from(size).unwrap_or_default())
        .sum::<u64>();
    let positions =
        load_position_deletes_with_context(&specs, split.data_file_path(), &access, &context)
            .map_err(invalid)?;
    let mut rows = Vec::with_capacity(usize::try_from(positions.len()).unwrap_or_default());
    for position in &positions {
        rows.push(i64::try_from(position).map_err(|_| {
            invalid(format!(
                "iceberg rewrite-position deletion vector for {} holds a position outside i64",
                split.data_file_path()
            ))
        })?);
    }

    Ok(Box::new(IcebergRewritePositionDeleteFilesPageSource {
        data_file_path: split.data_file_path().to_string(),
        outputs,
        rows,
        next_row: 0,
        max_page_rows: budget.max_rows.get(),
        bytes_read,
        closed: false,
    }))
}

/// One split's worth of re-encoded delete positions.
struct IcebergRewritePositionDeleteFilesPageSource {
    data_file_path: String,
    outputs: Vec<RewritePositionOutput>,
    /// Every selected vector's positions, merged and ascending.
    rows: Vec<i64>,
    next_row: usize,
    max_page_rows: usize,
    bytes_read: u64,
    closed: bool,
}

impl ConnectorPageSource for IcebergRewritePositionDeleteFilesPageSource {
    fn next_source_page(&mut self) -> Result<Option<SourcePage>, ConnectorError> {
        if self.closed || self.next_row >= self.rows.len() {
            self.closed = true;
            return Ok(None);
        }
        let end = (self.next_row + self.max_page_rows).min(self.rows.len());
        let chunk = &self.rows[self.next_row..end];
        self.next_row = end;
        let columns = self
            .outputs
            .iter()
            .map(|output| match output {
                RewritePositionOutput::FilePath => Arc::new(StringArray::from(vec![
                        self.data_file_path.as_str();
                        chunk.len()
                    ])) as ArrayRef,
                RewritePositionOutput::Position => {
                    Arc::new(Int64Array::from(chunk.to_vec())) as ArrayRef
                }
            })
            .collect::<Vec<_>>();
        SourcePage::try_new(chunk.len(), columns).map(Some)
    }

    fn is_finished(&self) -> bool {
        self.closed || self.next_row >= self.rows.len()
    }

    fn metrics(&self) -> PageSourceMetrics {
        PageSourceMetrics {
            completed_bytes: self.bytes_read,
            completed_positions: self.next_row as u64,
            read_time_nanos: 0,
            ..Default::default()
        }
    }

    fn memory_usage_bytes(&self) -> u64 {
        (self.rows.len() * size_of::<i64>() + self.data_file_path.len()) as u64
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        self.closed = true;
        self.rows.clear();
        self.rows.shrink_to_fit();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow::array::Array;
    use novarocks_fs::{
        FileCancellation, FileIoRuntime, FileTaskSpawner, FsAccessResolver, TokioFileIoRuntime,
        TokioFileTaskSpawner,
    };
    use novarocks_spi::connector::read_stack::SplitWeight;

    use crate::commit::DeletionVector;
    use crate::iceberg::spec::{NestedField, Type};
    use crate::typed_read::split::{
        IcebergDeleteFileContent, IcebergDeleteFileParams, IcebergFileFormat,
    };
    use crate::typed_read::table_execute::IcebergRewritePositionDeleteFilesSplitParams;

    use super::*;

    #[test]
    fn page_source_reads_selected_vectors_and_emits_canonical_output_columns() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let delete_path = directory.path().join("deletes.puffin");
        let data_file_path = directory
            .path()
            .join("data.parquet")
            .to_string_lossy()
            .to_string();

        let mut first = DeletionVector::new();
        first.insert(1).expect("position");
        first.insert(4).expect("position");
        let mut second = DeletionVector::new();
        second.insert(4).expect("position");
        second.insert(9).expect("position");
        let first_payload = first.to_iceberg_payload().expect("first payload");
        let second_payload = second.to_iceberg_payload().expect("second payload");
        let mut payloads = first_payload.clone();
        payloads.extend_from_slice(&second_payload);
        fs::write(&delete_path, &payloads).expect("write deletion vectors");

        let delete_path = delete_path.to_string_lossy().to_string();
        let delete = |offset: usize, payload: &[u8]| {
            IcebergDeleteFile::try_new(IcebergDeleteFileParams {
                content: IcebergDeleteFileContent::PositionDeletes,
                path: delete_path.clone(),
                format: IcebergFileFormat::Puffin,
                record_count: 2,
                file_size_in_bytes: payloads.len() as i64,
                equality_field_ids: Vec::new(),
                row_position_lower_bound: None,
                row_position_upper_bound: None,
                data_sequence_number: 1,
                content_offset: Some(offset as i64),
                content_size_in_bytes: Some(payload.len() as i64),
                referenced_data_file: Some(data_file_path.clone()),
                decryption_data: None,
            })
            .expect("selected deletion vector")
        };
        let split = IcebergRewritePositionDeleteFilesSplit::try_new(
            IcebergRewritePositionDeleteFilesSplitParams {
                data_file_path: data_file_path.clone(),
                data_file_size: 0,
                partition_spec_id: 0,
                partition_data_json: "{}".to_string(),
                selected_position_deletes: vec![
                    delete(0, &first_payload),
                    delete(first_payload.len(), &second_payload),
                ],
                split_weight: SplitWeight::STANDARD,
            },
        )
        .expect("rewrite split");

        let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
        let file_runtime: Arc<dyn FileIoRuntime> =
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone()));
        let task_spawner: Arc<dyn FileTaskSpawner> =
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone()));
        let binding = IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::clone(&file_runtime),
            Arc::clone(&task_spawner),
        );
        let context = FileReadContext {
            cancellation: FileCancellation::new(),
            deadline: Some(Instant::now() + Duration::from_secs(10)),
            runtime: file_runtime,
            task_spawner,
        };
        let columns = REWRITE_POSITION_DELETE_OUTPUT_COLUMNS.map(|(name, metadata)| {
            IcebergColumnHandle::base_column(&NestedField::optional(
                metadata.field_id(),
                name,
                Type::Primitive(metadata.declared_type()),
            ))
            .expect("canonical output handle")
        });
        let mut source = create_iceberg_rewrite_position_delete_files_page_source(
            IcebergRewritePositionDeleteFilesPageSourceRequest {
                split: &split,
                columns: &columns,
                access_binding: binding,
                context,
                budget: FileReadBudget {
                    max_rows: NonZeroUsize::new(16).expect("nonzero"),
                    max_bytes: NonZeroUsize::new(1024).expect("nonzero"),
                },
            },
        )
        .expect("page source");
        let page = source
            .next_source_page()
            .expect("read page")
            .expect("one output page");
        let (rows, columns) = page.into_columns().expect("materialize page");
        assert_eq!(rows, 3);
        let paths = columns[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("file_path string column");
        let positions = columns[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("pos bigint column");
        assert_eq!(
            paths.iter().collect::<Vec<_>>(),
            vec![Some(data_file_path.as_str()); 3]
        );
        assert_eq!(positions.values(), &[1, 4, 9]);
        assert!(
            source
                .next_source_page()
                .expect("finish page source")
                .is_none()
        );
        assert!(source.is_finished());
    }
}
