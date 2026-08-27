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

//! Iceberg V3 row-lineage column synthesis helpers.
//!
//! The reserved field IDs and fallback rules are Iceberg physical-format
//! facts. They remain provider-private: consumers receive ordinary Arrow
//! batches after the provider reader has applied this transformation.
//!
//! # The inherit-vs-stored rule
//!
//! A row-lineage column is either *inherited* from a file-level fact or
//! *stored* as a real column in the data file. Which one applies is decided
//! per column and per row, and the two columns never decide for each other:
//!
//! * A data file that materializes the reserved column carries the lineage its
//!   rows already had, which is exactly how a rewrite preserves history. That
//!   stored value wins.
//! * A null in a materialized column, and a file that materializes no column
//!   at all, both mean "inherit": `_row_id` becomes
//!   `first_row_id + absolute row position` and
//!   `_last_updated_sequence_number` becomes the file's own data sequence
//!   number.
//!
//! A file may materialize one column and not the other, so each column asks
//! the question separately. The inherited fact is demanded only by the rows
//! that actually need it: a file whose stored column covers every row is read
//! without it rather than rejected for lacking it.

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::Schema;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

/// Iceberg v2 row-level-delete virtual column names.
pub const ICEBERG_FILE_PATH_COL: &str = "_file";
pub const ICEBERG_ROW_POS_COL: &str = "_pos";

/// Iceberg v3 row-lineage virtual column names.
pub const ICEBERG_ROW_ID_COL: &str = "_row_id";
pub const ICEBERG_LAST_UPDATED_SEQ_COL: &str = "_last_updated_sequence_number";

/// Iceberg v3 reserved field IDs from the table-format specification.
pub const ICEBERG_RESERVED_FIELD_ID_ROW_ID: i32 = i32::MAX - 107;
pub const ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER: i32 = i32::MAX - 108;

pub fn is_iceberg_row_id(name: &str) -> bool {
    name.eq_ignore_ascii_case(ICEBERG_ROW_ID_COL)
}

pub fn is_iceberg_last_updated_sequence_number(name: &str) -> bool {
    name.eq_ignore_ascii_case(ICEBERG_LAST_UPDATED_SEQ_COL)
}

/// The two Iceberg v3 row-lineage columns, as a closed set.
///
/// They are the only columns a data file may materialize under a reserved
/// field ID, so naming them as their own type keeps a caller from asking
/// whether `_file` or `_pos` was "stored" -- a question with no meaning.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum IcebergRowLineageColumn {
    RowId,
    LastUpdatedSequenceNumber,
}

impl IcebergRowLineageColumn {
    pub const fn field_id(self) -> i32 {
        match self {
            Self::RowId => ICEBERG_RESERVED_FIELD_ID_ROW_ID,
            Self::LastUpdatedSequenceNumber => {
                ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER
            }
        }
    }

    pub const fn column_name(self) -> &'static str {
        match self {
            Self::RowId => ICEBERG_ROW_ID_COL,
            Self::LastUpdatedSequenceNumber => ICEBERG_LAST_UPDATED_SEQ_COL,
        }
    }
}

/// Indices of stored row-lineage columns (`_row_id`, `_last_updated_seq`) in a
/// batch schema, if present.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct StoredRowLineageIndices {
    pub(crate) row_id: Option<usize>,
    pub(crate) last_updated_seq: Option<usize>,
}

impl StoredRowLineageIndices {
    /// Where one row-lineage column sits, when the schema materializes it.
    pub(crate) const fn index_of(self, column: IcebergRowLineageColumn) -> Option<usize> {
        match column {
            IcebergRowLineageColumn::RowId => self.row_id,
            IcebergRowLineageColumn::LastUpdatedSequenceNumber => self.last_updated_seq,
        }
    }

    /// Whether the schema materializes this column at all. The two columns are
    /// independent, so this is asked once per column.
    pub(crate) const fn contains(self, column: IcebergRowLineageColumn) -> bool {
        self.index_of(column).is_some()
    }
}

/// Locate stored row-lineage columns by their reserved Iceberg field IDs.
///
/// Field ID, never name: a table may legitimately own a user column called
/// `_row_id`, and a rewritten file's reserved column is identified by the
/// spec's reserved ID alone.
pub(crate) fn stored_row_lineage_indices(schema: &Schema) -> StoredRowLineageIndices {
    let mut out = StoredRowLineageIndices::default();
    for (idx, field) in schema.fields().iter().enumerate() {
        let Some(field_id_str) = field.metadata().get(PARQUET_FIELD_ID_META_KEY) else {
            continue;
        };
        let Ok(field_id) = field_id_str.parse::<i32>() else {
            continue;
        };
        if field_id == ICEBERG_RESERVED_FIELD_ID_ROW_ID && out.row_id.is_none() {
            out.row_id = Some(idx);
        } else if field_id == ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER
            && out.last_updated_seq.is_none()
        {
            out.last_updated_seq = Some(idx);
        }
    }
    out
}

/// Locate one stored row-lineage column's values in a batch.
fn stored_values<'a>(
    schema: &Schema,
    columns: &'a [ArrayRef],
    column: IcebergRowLineageColumn,
) -> Result<Option<&'a Int64Array>, String> {
    let name = column.column_name();
    let Some(index) = stored_row_lineage_indices(schema).index_of(column) else {
        return Ok(None);
    };
    let array = columns.get(index).ok_or_else(|| {
        format!(
            "row-lineage stored {name} column index {index} out of bounds (columns.len={})",
            columns.len()
        )
    })?;
    array
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            format!(
                "stored {name} column must be Int64, got {:?}",
                array.data_type()
            )
        })
        .map(Some)
}

/// Resolve `_row_id` for every row of a batch.
///
/// A stored non-null value wins; every other row inherits
/// `first_row_id + positions[row]`. `first_row_id` is a manifest fact, so it
/// is passed as the option it is and demanded only by a row that inherits: a
/// file whose stored column covers every row needs no such fact.
///
/// `positions` are the rows' file-level absolute zero-based positions. They
/// are required rather than optional because a batch-relative index is not a
/// file position for any batch after the first, and a wrong `_row_id` is
/// indistinguishable from a right one.
pub(crate) fn synthesize_row_id(
    schema: &Schema,
    columns: &[ArrayRef],
    num_rows: usize,
    first_row_id: Option<i64>,
    positions: &[i64],
) -> Result<Vec<i64>, String> {
    let stored = stored_values(schema, columns, IcebergRowLineageColumn::RowId)?;

    if positions.len() != num_rows {
        return Err(format!(
            "synthesize_row_id positions.len()={} does not match num_rows={num_rows}",
            positions.len()
        ));
    }

    let mut out = Vec::with_capacity(num_rows);
    for (row, position) in positions.iter().copied().enumerate().take(num_rows) {
        if let Some(stored) = stored
            && !stored.is_null(row)
        {
            out.push(stored.value(row));
            continue;
        }
        let first_row_id = first_row_id.ok_or_else(|| {
            format!(
                "row {row} inherits its {ICEBERG_ROW_ID_COL} but the data file carries no first row id"
            )
        })?;
        let computed = first_row_id.checked_add(position).ok_or_else(|| {
            format!(
                "Row ID overflow when computing fallback _row_id: first_row_id={first_row_id}, position={position}"
            )
        })?;
        out.push(computed);
    }
    Ok(out)
}

/// Resolve `_last_updated_sequence_number` for every row of a batch.
///
/// A stored non-null value wins; every other row inherits the data file's own
/// sequence number. That fact is demanded only by a row that inherits, for the
/// same reason `first_row_id` is.
pub(crate) fn synthesize_last_updated_sequence_number(
    schema: &Schema,
    columns: &[ArrayRef],
    num_rows: usize,
    data_sequence_number: Option<i64>,
) -> Result<Vec<i64>, String> {
    let stored = stored_values(
        schema,
        columns,
        IcebergRowLineageColumn::LastUpdatedSequenceNumber,
    )?;

    let mut out = Vec::with_capacity(num_rows);
    for row in 0..num_rows {
        if let Some(stored) = stored
            && !stored.is_null(row)
        {
            out.push(stored.value(row));
            continue;
        }
        out.push(data_sequence_number.ok_or_else(|| {
            format!(
                "row {row} inherits its {ICEBERG_LAST_UPDATED_SEQ_COL} but the data file carries no data sequence number"
            )
        })?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn field_with_id(name: &str, id: i32, ty: DataType, nullable: bool) -> Field {
        let mut metadata = HashMap::new();
        metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string());
        Field::new(name, ty, nullable).with_metadata(metadata)
    }

    fn schema_with_stored_row_id() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            field_with_id(
                "_row_id",
                ICEBERG_RESERVED_FIELD_ID_ROW_ID,
                DataType::Int64,
                true,
            ),
            field_with_id(
                "_last_updated_sequence_number",
                ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
                DataType::Int64,
                true,
            ),
        ])
    }

    #[test]
    fn locates_stored_row_lineage_columns_by_field_id() {
        let schema = schema_with_stored_row_id();
        let indices = stored_row_lineage_indices(&schema);
        assert_eq!(indices.row_id, Some(1));
        assert_eq!(indices.last_updated_seq, Some(2));
    }

    #[test]
    fn synthesize_row_id_uses_stored_and_fallback_values() {
        let schema = schema_with_stored_row_id();
        let columns = vec![
            Arc::new(Int64Array::from(vec![100_i64, 200, 300])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(42_i64), None, Some(7)])) as ArrayRef,
            Arc::new(Int64Array::from(vec![None, None, None])) as ArrayRef,
        ];

        assert_eq!(
            synthesize_row_id(&schema, &columns, 3, Some(1000), &[0, 1, 2])
                .expect("synthesis succeeds"),
            vec![42, 1001, 7]
        );
    }

    #[test]
    fn synthesize_row_id_honors_positions() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let columns = vec![Arc::new(Int64Array::from(vec![100_i64, 200])) as ArrayRef];

        assert_eq!(
            synthesize_row_id(&schema, &columns, 2, Some(500), &[3, 9])
                .expect("synthesis succeeds"),
            vec![503, 509]
        );
    }

    /// A file whose stored column covers every row is complete on its own, so
    /// the manifest's `first_row_id` is never consulted.
    #[test]
    fn a_fully_stored_row_id_column_needs_no_first_row_id() {
        let schema = schema_with_stored_row_id();
        let columns = vec![
            Arc::new(Int64Array::from(vec![100_i64, 200])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(42_i64), Some(7)])) as ArrayRef,
            Arc::new(Int64Array::from(vec![None, None])) as ArrayRef,
        ];

        assert_eq!(
            synthesize_row_id(&schema, &columns, 2, None, &[0, 1]).expect("synthesis succeeds"),
            vec![42, 7]
        );
    }

    /// The inherited fact is not optional for a row that actually inherits.
    #[test]
    fn an_inheriting_row_without_first_row_id_fails_closed() {
        let schema = schema_with_stored_row_id();
        let columns = vec![
            Arc::new(Int64Array::from(vec![100_i64, 200])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(42_i64), None])) as ArrayRef,
            Arc::new(Int64Array::from(vec![None, None])) as ArrayRef,
        ];

        let error = synthesize_row_id(&schema, &columns, 2, None, &[0, 1])
            .expect_err("an inheriting row needs the file's first row id");
        assert!(error.contains("row 1"), "{error}");
        assert!(error.contains("first row id"), "{error}");
    }

    #[test]
    fn synthesize_last_updated_sequence_uses_stored_and_fallback_values() {
        let schema = schema_with_stored_row_id();
        let columns = vec![
            Arc::new(Int64Array::from(vec![100_i64, 200])) as ArrayRef,
            Arc::new(Int64Array::from(vec![None, None])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(11_i64), None])) as ArrayRef,
        ];

        assert_eq!(
            synthesize_last_updated_sequence_number(&schema, &columns, 2, Some(99))
                .expect("synthesis succeeds"),
            vec![11, 99]
        );
    }

    /// The two columns decide independently: a file may materialize one and
    /// leave the other to inheritance, and neither answer may leak into the
    /// other.
    #[test]
    fn each_row_lineage_column_falls_back_on_its_own() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            field_with_id(
                "_last_updated_sequence_number",
                ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
                DataType::Int64,
                true,
            ),
        ]);
        let columns = vec![
            Arc::new(Int64Array::from(vec![100_i64, 200])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(11_i64), Some(12)])) as ArrayRef,
        ];
        let indices = stored_row_lineage_indices(&schema);
        assert!(!indices.contains(IcebergRowLineageColumn::RowId));
        assert!(indices.contains(IcebergRowLineageColumn::LastUpdatedSequenceNumber));

        // `_row_id` is absent, so every row inherits it ...
        assert_eq!(
            synthesize_row_id(&schema, &columns, 2, Some(500), &[3, 9])
                .expect("synthesis succeeds"),
            vec![503, 509]
        );
        // ... while the stored `_last_updated_sequence_number` is preserved.
        assert_eq!(
            synthesize_last_updated_sequence_number(&schema, &columns, 2, Some(99))
                .expect("synthesis succeeds"),
            vec![11, 12]
        );
    }
}
