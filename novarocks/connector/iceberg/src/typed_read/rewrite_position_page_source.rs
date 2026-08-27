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
use super::table_execute::IcebergRewritePositionDeleteFilesSplit;

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
                Some(IcebergMetadataColumn::Path) => Ok(RewritePositionOutput::FilePath),
                Some(IcebergMetadataColumn::RowPosition) => Ok(RewritePositionOutput::Position),
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
