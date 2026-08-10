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

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::Schema;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

/// Iceberg v3 reserved field IDs from the table-format specification.
pub(crate) const ICEBERG_RESERVED_FIELD_ID_ROW_ID: i32 = i32::MAX - 107;
pub(crate) const ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER: i32 = i32::MAX - 108;

/// Indices of stored row-lineage columns (`_row_id`, `_last_updated_seq`) in a
/// batch schema, if present.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct StoredRowLineageIndices {
    pub(crate) row_id: Option<usize>,
    pub(crate) last_updated_seq: Option<usize>,
}

/// Locate stored row-lineage columns by their reserved Iceberg field IDs.
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

/// Synthesize `_row_id` values for rows in `columns`.
pub(crate) fn synthesize_row_id(
    schema: &Schema,
    columns: &[ArrayRef],
    num_rows: usize,
    first_row_id: i64,
    positions: Option<&[i64]>,
) -> Result<Vec<i64>, String> {
    let idx = stored_row_lineage_indices(schema);
    let stored: Option<&Int64Array> = idx
        .row_id
        .map(|i| {
            columns
                .get(i)
                .ok_or_else(|| {
                    format!(
                        "row-lineage stored _row_id column index {i} out of bounds (columns.len={})",
                        columns.len()
                    )
                })
                .and_then(|col| {
                    col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                        format!(
                            "stored _row_id column must be Int64, got {:?}",
                            col.data_type()
                        )
                    })
                })
        })
        .transpose()?;

    if let Some(positions) = positions
        && positions.len() != num_rows
    {
        return Err(format!(
            "synthesize_row_id positions.len()={} does not match num_rows={num_rows}",
            positions.len()
        ));
    }

    let mut out = Vec::with_capacity(num_rows);
    for i in 0..num_rows {
        if let Some(stored) = stored
            && !stored.is_null(i)
        {
            out.push(stored.value(i));
            continue;
        }
        let position = positions.map_or(i as i64, |positions| positions[i]);
        let computed = first_row_id.checked_add(position).ok_or_else(|| {
            format!(
                "Row ID overflow when computing fallback _row_id: first_row_id={first_row_id}, position={position}"
            )
        })?;
        out.push(computed);
    }
    Ok(out)
}

/// Synthesize `_last_updated_sequence_number` values for rows in `columns`.
pub(crate) fn synthesize_last_updated_sequence_number(
    schema: &Schema,
    columns: &[ArrayRef],
    num_rows: usize,
    data_sequence_number: i64,
) -> Result<Vec<i64>, String> {
    let idx = stored_row_lineage_indices(schema);
    let stored: Option<&Int64Array> = idx
        .last_updated_seq
        .map(|i| {
            columns
                .get(i)
                .ok_or_else(|| {
                    format!(
                        "row-lineage stored _last_updated_sequence_number index {i} out of bounds"
                    )
                })
                .and_then(|col| {
                    col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                        format!(
                            "stored _last_updated_sequence_number column must be Int64, got {:?}",
                            col.data_type()
                        )
                    })
                })
        })
        .transpose()?;

    let mut out = Vec::with_capacity(num_rows);
    for i in 0..num_rows {
        if let Some(stored) = stored
            && !stored.is_null(i)
        {
            out.push(stored.value(i));
        } else {
            out.push(data_sequence_number);
        }
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
            synthesize_row_id(&schema, &columns, 3, 1000, None).expect("synthesis succeeds"),
            vec![42, 1001, 7]
        );
    }

    #[test]
    fn synthesize_row_id_honors_positions() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let columns = vec![Arc::new(Int64Array::from(vec![100_i64, 200])) as ArrayRef];

        assert_eq!(
            synthesize_row_id(&schema, &columns, 2, 500, Some(&[3, 9]))
                .expect("synthesis succeeds"),
            vec![503, 509]
        );
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
            synthesize_last_updated_sequence_number(&schema, &columns, 2, 99)
                .expect("synthesis succeeds"),
            vec![11, 99]
        );
    }
}
