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

//! Bounded construction of Frontend-local results.
//!
//! SHOW, information_schema, EXPLAIN and management commands build their
//! results on the Frontend under one Local result position. A local source
//! pushes its rows here one at a time; each row is checked against the frozen
//! local bound before any of its cells is appended, so a source too large for
//! the position is refused while it grows, not materialized and then sliced.
//!
//! Logical cells and actual owned buffer capacities have separate bounds.
//! Segment tails grow only within the collector share; a copied tail plus
//! incoming source cells is checked against the conversion workspace before
//! reserve. Source scratch must retire before final Arrow consolidation.

use std::sync::Arc;

use super::local_result_buffer::{Growth, LocalBuffer};
use arrow::array::{Array, ArrayRef, StringArray};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use super::result::{QueryResult, ResultField};

/// The frozen bound of one local result: profile-v1 `local_source.entries`
/// and `local_source.collector_bytes`, and `client_rows.columns`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalResultBound {
    pub rows: usize,
    pub bytes: usize,
    pub columns: usize,
}

impl LocalResultBound {
    pub const V1: Self = Self {
        rows: 65_536,
        bytes: 32 * 1024 * 1024,
        columns: 4096,
    };

    /// Admits a result of `rows` rows and `bytes` logical cell bytes before
    /// its arrays are built. A source that can count its rows and bytes from
    /// data it already holds uses this instead of [`LocalTableBuilder`].
    pub fn admit(&self, rows: usize, bytes: usize) -> Result<(), String> {
        if rows > self.rows {
            return Err(format!(
                "local result has {rows} rows, beyond its {} row bound",
                self.rows
            ));
        }
        if bytes > self.bytes {
            return Err(format!(
                "local result exceeds its {} byte bound",
                self.bytes
            ));
        }
        Ok(())
    }

    /// Logical bytes of one cell holding `value_bytes` bytes, as
    /// [`LocalTableBuilder`] counts it.
    pub const fn cell_bytes(value_bytes: usize) -> usize {
        value_bytes.saturating_add(CELL_OVERHEAD_BYTES)
    }

    /// Checks a result whose arrays were built by a source that is itself
    /// structurally bounded (a fixed number of rows, or a registry with its
    /// own cap). It refuses an oversized result before it is published.
    pub fn check_arrays(&self, arrays: &[ArrayRef]) -> Result<(), String> {
        if arrays.len() > self.columns {
            return Err(format!(
                "local result has {} columns, beyond its {} column bound",
                arrays.len(),
                self.columns
            ));
        }
        let rows = arrays.first().map_or(0, |array| array.len());
        if rows > self.rows {
            return Err(format!(
                "local result has {rows} rows, beyond its {} row bound",
                self.rows
            ));
        }
        let bytes = arrays.iter().try_fold(0_usize, |total, array| {
            total.checked_add(array.get_array_memory_size())
        });
        match bytes {
            Some(bytes) if bytes <= self.bytes => Ok(()),
            _ => Err(format!(
                "local result exceeds its {} byte bound",
                self.bytes
            )),
        }
    }
}

/// Per-cell overhead counted with the cell's bytes: one i32 offset and its
/// validity bit, rounded up to a byte.
const CELL_OVERHEAD_BYTES: usize = size_of::<i32>() + 1;

/// Builds one local UTF-8 table under a [`LocalResultBound`].
pub struct LocalTableBuilder {
    fields: Vec<ResultField>,
    builders: Vec<LocalUtf8Column>,
    rows: usize,
    bytes: usize,
    bound: LocalResultBound,
    failed: bool,
    #[cfg(test)]
    last_row_workspace_peak: usize,
}

impl LocalTableBuilder {
    /// `columns` names each column and whether it may hold NULL.
    pub fn try_new(columns: &[(&str, bool)], bound: LocalResultBound) -> Result<Self, String> {
        if bound.rows > LocalResultBound::V1.rows
            || bound.bytes > LocalResultBound::V1.bytes
            || bound.columns > LocalResultBound::V1.columns
        {
            return Err("local result construction cannot exceed the frozen profile".into());
        }
        if columns.is_empty() {
            return Err("immediate tabular result requires at least one column".to_owned());
        }
        if columns.len() > bound.columns {
            return Err(format!(
                "local result has {} columns, beyond its {} column bound",
                columns.len(),
                bound.columns
            ));
        }
        if columns.iter().any(|(name, _)| name.is_empty()) {
            return Err("immediate tabular result column names must be nonempty".to_owned());
        }
        preflight_local_columns(columns)?;
        Ok(Self {
            fields: columns
                .iter()
                .map(|(name, nullable)| ResultField::new(*name, DataType::Utf8, *nullable, None))
                .collect(),
            builders: columns
                .iter()
                .map(|_| LocalUtf8Column::new())
                .collect::<Result<Vec<_>, _>>()?,
            rows: 0,
            bytes: 0,
            bound,
            failed: false,
            #[cfg(test)]
            last_row_workspace_peak: 0,
        })
    }

    /// Rows appended so far.
    pub const fn rows(&self) -> usize {
        self.rows
    }

    /// Logical bytes counted so far.
    pub const fn bytes(&self) -> usize {
        self.bytes
    }

    /// Appends one row after checking its width, its required cells and the
    /// whole row against the remaining bound. A refused row appends nothing.
    pub fn push_row<S: AsRef<str>>(&mut self, row: &[Option<S>]) -> Result<(), String> {
        if self.failed {
            return Err("local result builder failed before publication".into());
        }
        if row.len() != self.fields.len() {
            return Err(
                "immediate tabular result contains a row with the wrong column count".to_owned(),
            );
        }
        if row
            .iter()
            .zip(&self.fields)
            .any(|(value, field)| value.is_none() && !field.nullable())
        {
            return Err("immediate tabular result contains null in a required column".to_owned());
        }
        if self.rows >= self.bound.rows {
            return Err(format!(
                "local result exceeds its {} row bound",
                self.bound.rows
            ));
        }
        let row_bytes = row.iter().try_fold(0_usize, |total, value| {
            total
                .checked_add(value.as_ref().map_or(0, |value| value.as_ref().len()))?
                .checked_add(CELL_OVERHEAD_BYTES)
        });
        let bytes = row_bytes
            .and_then(|row_bytes| self.bytes.checked_add(row_bytes))
            .filter(|bytes| *bytes <= self.bound.bytes)
            .ok_or_else(|| format!("local result exceeds its {} byte bound", self.bound.bytes))?;
        let incoming = row_bytes.expect("whole row bytes already checked");
        let mut growth = Growth::new(0, incoming, self.bound.bytes);
        // Offset zero is fixed metadata, charged with the column headers, not
        // one of the cell buffers. No source row has been copied yet.
        let mut capacity = self.buffer_bytes();
        let mut missing = self.missing_bytes(row);
        let largest_tail = self
            .builders
            .iter()
            .map(LocalUtf8Column::maximum_tail_bytes)
            .max()
            .unwrap_or(0);
        if capacity
            .checked_add(missing)
            .is_none_or(|n| n > self.bound.bytes)
            || largest_tail
                .checked_add(incoming)
                .is_none_or(|n| n > self.bound.bytes)
        {
            for column in &mut self.builders {
                if let Err(error) = column.compact_tails(&mut growth) {
                    self.failed = true;
                    return Err(error);
                }
            }
            capacity = self.buffer_bytes();
            missing = self.missing_bytes(row);
        }
        let committed = capacity
            .checked_add(missing)
            .filter(|n| *n <= self.bound.bytes)
            .ok_or_else(|| "local result exceeds its owned collector capacity bound".to_string())?;
        growth.grant_extra(self.bound.bytes - committed);
        for (builder, value) in self.builders.iter_mut().zip(row) {
            if let Err(error) =
                builder.append(value.as_ref().map(AsRef::as_ref), self.rows, &mut growth)
            {
                self.failed = true;
                return Err(error);
            }
        }
        debug_assert!(self.buffer_bytes() <= self.bound.bytes);
        #[cfg(test)]
        {
            self.last_row_workspace_peak = incoming + growth.maximum_copy_bytes;
        }
        self.rows += 1;
        self.bytes = bytes;
        Ok(())
    }

    fn buffer_bytes(&self) -> usize {
        self.builders
            .iter()
            .map(LocalUtf8Column::capacity_bytes)
            .sum()
    }

    fn missing_bytes<S: AsRef<str>>(&self, row: &[Option<S>]) -> usize {
        self.builders
            .iter()
            .zip(row)
            .map(|(column, value)| {
                column.missing_bytes(value.as_ref().map_or(0, |v| v.as_ref().len()), self.rows)
            })
            .sum()
    }

    /// Publishes the table as one immediate result.
    pub fn finish(self) -> Result<QueryResult, String> {
        if self.failed {
            return Err("local result builder failed before publication".into());
        }
        let schema = Arc::new(Schema::new(
            self.fields
                .iter()
                .map(|field| Field::new(field.name(), DataType::Utf8, field.nullable()))
                .collect::<Vec<_>>(),
        ));
        let arrays = self
            .builders
            .into_iter()
            .map(|builder| {
                builder
                    .finish(self.rows)
                    .map(|array| Arc::new(array) as ArrayRef)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let batch = RecordBatch::try_new(schema, arrays)
            .map_err(|error| format!("build immediate tabular result failed: {error}"))?;
        Ok(QueryResult {
            columns: self.fields,
            batches: vec![batch],
        })
    }
}

/// The future String render schema is known now. Borrowed name/type/header
/// bounds must pass before ResultField/Arrow/renderer copies are allocated.
fn preflight_local_columns(columns: &[(&str, bool)]) -> Result<(), String> {
    use novarocks_result_contract::{ClientRenderSchema, RenderColumn, RootProfileV1 as P};
    let mut backing = size_of::<ClientRenderSchema>() + columns.len() * size_of::<RenderColumn>();
    let mut wire = 0usize;
    for (name, _) in columns {
        if name.len() > P::MAX_NAME_BYTES {
            return Err("local result name exceeds the frozen schema bound".into());
        }
        backing = backing
            .checked_add(name.len())
            .ok_or("local result schema size overflows")?;
        // The same name+16 and one leaf node+32 as the compiler's borrowed
        // String schema preflight; schema wire is smaller than MySQL metadata.
        wire = wire
            .checked_add(name.len())
            .and_then(|n| n.checked_add(48))
            .ok_or("local result schema size overflows")?;
        if backing > P::SCHEMA_BACKING_BYTES || wire > P::SCHEMA_WIRE_BYTES {
            return Err("local result schema exceeds the frozen backing/wire bound".into());
        }
    }
    Ok(())
}

struct LocalUtf8Column {
    values: LocalBuffer<u8>,
    offsets: LocalBuffer<i32>,
    validity: LocalBuffer<u8>,
    has_null: bool,
}

impl LocalUtf8Column {
    fn new() -> Result<Self, String> {
        let mut offsets = LocalBuffer::new(size_of::<i32>());
        // One zero offset is root metadata. Start without amortized slack so
        // an empty column consumes no collector/workspace payload capacity.
        offsets.append(&[0], &mut Growth::new(0, 0, 0))?;
        Ok(Self {
            values: LocalBuffer::new(0),
            offsets,
            validity: LocalBuffer::new(0),
            has_null: false,
        })
    }
    fn capacity_bytes(&self) -> usize {
        self.values.capacity_bytes() + self.offsets.capacity_bytes() - size_of::<i32>()
            + self.validity.capacity_bytes()
    }
    fn maximum_tail_bytes(&self) -> usize {
        self.values
            .maximum_tail_bytes()
            .max(self.offsets.maximum_tail_bytes())
            .max(self.validity.maximum_tail_bytes())
    }
    fn missing_bytes(&self, value_bytes: usize, row: usize) -> usize {
        self.values.missing_bytes(value_bytes)
            + self.offsets.missing_bytes(1)
            + self.validity.missing_bytes(usize::from(row % 8 == 0))
    }
    fn compact_tails(&mut self, growth: &mut Growth) -> Result<(), String> {
        self.values.compact_tail(growth)?;
        self.offsets.compact_tail(growth)?;
        self.validity.compact_tail(growth)
    }
    fn append(
        &mut self,
        value: Option<&str>,
        row: usize,
        growth: &mut Growth,
    ) -> Result<(), String> {
        if let Some(value) = value {
            self.values.append(value.as_bytes(), growth)?;
        }
        let offset = i32::try_from(self.values.len())
            .map_err(|_| "local UTF-8 column offset overflows".to_string())?;
        self.offsets.append(&[offset], growth)?;
        if row % 8 == 0 {
            self.validity.append(&[0], growth)?;
        }
        if value.is_some() {
            *self.validity.last_mut().expect("row validity byte") |= 1 << (row % 8);
        } else {
            self.has_null = true;
        }
        Ok(())
    }
    fn finish(self, rows: usize) -> Result<StringArray, String> {
        let values = Buffer::from(self.values.into_compact()?);
        let offsets = OffsetBuffer::new(ScalarBuffer::from(self.offsets.into_compact()?));
        let nulls = if self.has_null {
            Some(NullBuffer::new(BooleanBuffer::new(
                Buffer::from(self.validity.into_compact()?),
                0,
                rows,
            )))
        } else {
            None
        };
        StringArray::try_new(offsets, values, nulls)
            .map_err(|error| format!("build local UTF-8 column failed: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, Int64Array, StringArray};

    use super::{CELL_OVERHEAD_BYTES, LocalResultBound, LocalTableBuilder};

    fn bound(rows: usize, bytes: usize) -> LocalResultBound {
        LocalResultBound {
            rows,
            bytes,
            columns: 4,
        }
    }

    #[test]
    fn rows_are_appended_until_the_row_bound_and_refused_beyond_it() {
        let mut table = LocalTableBuilder::try_new(&[("a", false)], bound(2, 1 << 20)).unwrap();
        table.push_row(&[Some("x")]).unwrap();
        table.push_row(&[Some("y")]).unwrap();
        let error = table.push_row(&[Some("z")]).unwrap_err();
        assert!(error.contains("row bound"), "{error}");
        let result = table.finish().unwrap();
        assert_eq!(result.row_count(), 2);
    }

    #[test]
    fn a_row_beyond_the_byte_bound_appends_nothing() {
        // The first row plus one empty two-cell row fills the bound exactly.
        let exact = 3 + 4 * CELL_OVERHEAD_BYTES;
        let mut table =
            LocalTableBuilder::try_new(&[("a", false), ("b", true)], bound(16, exact)).unwrap();
        table.push_row(&[Some("abc"), None::<&str>]).unwrap();
        assert_eq!(table.bytes(), 3 + 2 * CELL_OVERHEAD_BYTES);
        let error = table.push_row(&[Some("abcd"), Some("e")]).unwrap_err();
        assert!(error.contains("byte bound"), "{error}");
        assert_eq!(table.rows(), 1);
        // A row that exactly fills the remaining bytes is admitted.
        table.push_row(&[Some(""), Some("")]).unwrap();
        let result = table.finish().unwrap();
        let first = result.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();
        assert_eq!(first.len(), 2);
        assert_eq!(first.value(0), "abc");
        assert!(result.batches[0].column(1).is_null(0));
    }

    #[test]
    fn shape_errors_are_refused_before_appending() {
        let mut table =
            LocalTableBuilder::try_new(&[("a", false), ("b", false)], bound(16, 1 << 20)).unwrap();
        assert!(table.push_row(&[Some("a")]).is_err());
        assert!(table.push_row(&[Some("a"), None]).is_err());
        assert_eq!(table.rows(), 0);
        assert!(LocalTableBuilder::try_new(&[], bound(1, 1)).is_err());
        assert!(LocalTableBuilder::try_new(&[("", false)], bound(1, 1)).is_err());
        let too_wide = [("a", false); 5];
        assert!(LocalTableBuilder::try_new(&too_wide, bound(1, 1)).is_err());
        // An empty table keeps its schema.
        let empty = LocalTableBuilder::try_new(&[("a", true)], bound(1, 1))
            .unwrap()
            .finish()
            .unwrap();
        assert_eq!(empty.row_count(), 0);
        assert_eq!(empty.columns[0].name(), "a");
    }

    #[test]
    fn prebuilt_arrays_are_checked_against_rows_bytes_and_columns() {
        let column: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let bytes = column.get_array_memory_size();
        assert!(bound(3, bytes).check_arrays(&[column.clone()]).is_ok());
        assert!(bound(2, bytes).check_arrays(&[column.clone()]).is_err());
        assert!(bound(3, bytes - 1).check_arrays(&[column.clone()]).is_err());
        let wide = vec![column; 5];
        assert!(bound(3, usize::MAX).check_arrays(&wide).is_err());
    }

    #[test]
    fn shared_helpers_refuse_a_source_beyond_the_frozen_bound() {
        let rows = (0..=LocalResultBound::V1.rows)
            .map(|index| index.to_string())
            .collect::<Vec<_>>();
        let error = crate::api::build_string_query_result("n", rows).unwrap_err();
        assert!(error.contains("row bound"), "{error}");
        let wide = "w".repeat(LocalResultBound::V1.bytes);
        let error = crate::api::build_utf8_query_result(&["w"], vec![vec![wide]]).unwrap_err();
        assert!(error.contains("byte bound"), "{error}");
    }
}

#[cfg(test)]
mod capacity_tests {
    use super::*;
    use arrow::array::StringBuilder;

    fn bound(bytes: usize) -> LocalResultBound {
        LocalResultBound {
            bytes,
            rows: LocalResultBound::V1.rows,
            columns: 4,
        }
    }

    #[test]
    fn exact_logical_limit_is_preserved_while_unused_buffer_capacities_are_compacted() {
        let mut table =
            LocalTableBuilder::try_new(&[("a", false), ("b", true)], bound(1024)).unwrap();
        for value in ["x".repeat(350), "y".repeat(350), "z".repeat(294)] {
            table.push_row(&[Some(value.as_str()), None]).unwrap();
            assert!(table.buffer_bytes() <= 1024);
        }
        assert_eq!(table.bytes(), 1024);
        let capacity = table.buffer_bytes();
        assert!(table.push_row(&[Some(""), None]).is_err());
        assert_eq!(table.rows(), 3);
        assert_eq!(table.buffer_bytes(), capacity);
        let result = table.finish().unwrap();
        let column = result.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(column.value(0), "x".repeat(350));
        assert_eq!(column.value(2), "z".repeat(294));
        assert!(result.batches[0].column(1).is_null(2));
    }

    #[test]
    fn large_incoming_row_does_not_overlap_an_amortized_tail_outside_workspace() {
        let mut table =
            LocalTableBuilder::try_new(&[("a", false), ("b", true)], bound(64)).unwrap();
        table.push_row(&[Some("x"), None]).unwrap();
        let value = "y".repeat(43);
        table.push_row(&[Some(value.as_str()), None]).unwrap();
        assert_eq!(table.bytes(), 64);
        assert!(table.buffer_bytes() <= 64);
        assert!(table.last_row_workspace_peak <= 64);
        drop(value);
        let result = table.finish().unwrap();
        assert_eq!(result.row_count(), 2);
    }

    #[test]
    fn unicode_nulls_offsets_and_validity_match_arrow_across_every_segment_boundary() {
        let mut table =
            LocalTableBuilder::try_new(&[("a", true), ("b", false)], LocalResultBound::V1).unwrap();
        let mut expected_a = StringBuilder::new();
        let mut expected_b = StringBuilder::new();
        let wide = "🦀".repeat(20_000);
        for row in 0..LocalResultBound::V1.rows {
            let a = if row % 17 == 0 {
                None
            } else if row == 1 {
                Some(wide.as_str())
            } else {
                Some("名🦀")
            };
            let b = if row % 3 == 0 { "" } else { "tiny" };
            table.push_row(&[a, Some(b)]).unwrap();
            expected_a.append_option(a);
            expected_b.append_value(b);
            assert!(table.buffer_bytes() <= LocalResultBound::V1.bytes);
        }
        let a = expected_a.finish();
        let b = expected_b.finish();
        drop(wide);
        let result = table.finish().unwrap();
        assert_eq!(
            result.batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap(),
            &a
        );
        assert_eq!(
            result.batches[0]
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap(),
            &b
        );
    }

    #[test]
    fn metadata_and_looser_limits_are_refused_before_owned_schema_construction() {
        let name = "n".repeat(novarocks_result_contract::RootProfileV1::MAX_NAME_BYTES + 1);
        assert!(
            LocalTableBuilder::try_new(&[(&name, false)], LocalResultBound::V1)
                .err()
                .unwrap()
                .contains("name")
        );
        let name = "n".repeat(novarocks_result_contract::RootProfileV1::MAX_NAME_BYTES);
        assert!(
            LocalTableBuilder::try_new(&[(name.as_str(), false); 4], LocalResultBound::V1)
                .err()
                .unwrap()
                .contains("backing/wire")
        );
        let loose = LocalResultBound {
            bytes: LocalResultBound::V1.bytes + 1,
            ..LocalResultBound::V1
        };
        assert!(
            LocalTableBuilder::try_new(&[("a", false)], loose)
                .err()
                .unwrap()
                .contains("frozen profile")
        );
    }
}
