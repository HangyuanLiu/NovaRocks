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
//! The bound counts the logical bytes of every cell plus its offset and
//! validity overhead. Arrow's amortized buffer growth can hold up to twice
//! the logical bytes; that slack belongs to the position's workspace share,
//! which the frozen profile sizes equal to the collector share.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringBuilder};
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
    builders: Vec<StringBuilder>,
    rows: usize,
    bytes: usize,
    bound: LocalResultBound,
}

impl LocalTableBuilder {
    /// `columns` names each column and whether it may hold NULL.
    pub fn try_new(columns: &[(&str, bool)], bound: LocalResultBound) -> Result<Self, String> {
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
        Ok(Self {
            fields: columns
                .iter()
                .map(|(name, nullable)| ResultField::new(*name, DataType::Utf8, *nullable, None))
                .collect(),
            builders: columns.iter().map(|_| StringBuilder::new()).collect(),
            rows: 0,
            bytes: 0,
            bound,
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
        for (builder, value) in self.builders.iter_mut().zip(row) {
            builder.append_option(value.as_ref());
        }
        self.rows += 1;
        self.bytes = bytes;
        Ok(())
    }

    /// Publishes the table as one immediate result.
    pub fn finish(self) -> Result<QueryResult, String> {
        let schema = Arc::new(Schema::new(
            self.fields
                .iter()
                .map(|field| Field::new(field.name(), DataType::Utf8, field.nullable()))
                .collect::<Vec<_>>(),
        ));
        let arrays = self
            .builders
            .into_iter()
            .map(|mut builder| Arc::new(builder.finish()) as ArrayRef)
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(schema, arrays)
            .map_err(|error| format!("build immediate tabular result failed: {error}"))?;
        Ok(QueryResult {
            columns: self.fields,
            batches: vec![batch],
        })
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
