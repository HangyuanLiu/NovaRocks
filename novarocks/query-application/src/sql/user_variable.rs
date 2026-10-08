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

//! Query-application conversion of scalar query results into SQL literals.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, LargeBinaryArray, LargeStringArray, StringArray, Time64MicrosecondArray,
    TimestampMicrosecondArray, TimestampNanosecondArray, new_null_array,
};
use arrow::datatypes::{DataType, Field, Fields, TimeUnit, i256};
use novarocks_result_contract::{
    ScalarField, ScalarRecord, ScalarSchema, ScalarTimestampUnit, ScalarValue, ScalarValueType,
};

use crate::api::QueryResult;

mod borrowed;
pub use borrowed::{
    borrowed_scalar_record_to_user_variable_literal,
    local_mv_scalar_result_to_user_variable_literal,
};
use novarocks_sql::literal::literal_from_batch;
use novarocks_sql::semantic::Literal;

/// Converts a scalar query result into SQL that can be substituted for a user
/// variable. The result contract belongs to Query Application, so adapters and
/// product command routes share one literal representation.
pub fn query_result_to_user_variable_literal(result: &QueryResult) -> Result<String, String> {
    if result.columns.len() != 1 {
        return Err(format!(
            "user variable assignment expected 1 column, got {}",
            result.columns.len()
        ));
    }
    let row_count = result.row_count();
    if row_count == 0 {
        return Ok("null".to_string());
    }
    if row_count > 1 {
        return Err("Subquery returns more than 1 row".to_string());
    }
    for batch in &result.batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let column = batch
            .columns()
            .first()
            .ok_or_else(|| "empty query chunk".to_string())?;
        let declared = result
            .columns
            .first()
            .ok_or_else(|| "user variable assignment missing column metadata".to_string())?;
        return query_result_cell_to_user_variable_sql(column, declared.data_type(), 0);
    }
    Ok("null".to_string())
}

/// Converts a relayed ScalarValueV1 record into the same SQL literal the
/// Arrow result path produces: no rows is `null`, a one-row NULL is `NULL`,
/// and containers are walked without allocating an Arrow or Literal tree.
pub fn scalar_record_to_user_variable_literal(
    schema: &ScalarSchema,
    record: &ScalarRecord,
) -> Result<String, String> {
    let ScalarRecord::Value(value) = record else {
        return Ok("null".to_string());
    };
    let mut measured = ScalarSqlOutput::new(None);
    write_scalar_sql(&mut measured, schema.field(), value, false)?;
    if measured
        .bytes
        .checked_add(measured.scratch)
        .is_none_or(|bytes| {
            bytes > novarocks_result_contract::ScalarProfileV1::ASSIGNMENT_SCRATCH_BYTES
        })
    {
        return Err("scalar SQL literal exceeds its assignment scratch bound".into());
    }
    let mut output = ScalarSqlOutput::new(Some(String::with_capacity(measured.bytes)));
    write_scalar_sql(&mut output, schema.field(), value, false)?;
    Ok(output
        .output
        .expect("the second pass owns its exact output buffer"))
}

/// Count first, then allocate once. Container traversal never creates a whole
/// Arrow value, per-item array vector, Literal tree or formatted child vector.
struct ScalarSqlOutput {
    output: Option<String>,
    bytes: usize,
    scratch: usize,
}
impl ScalarSqlOutput {
    fn new(output: Option<String>) -> Self {
        Self {
            output,
            bytes: 0,
            scratch: 0,
        }
    }
    fn append(&mut self, text: &str) -> Result<(), String> {
        self.bytes = self
            .bytes
            .checked_add(text.len())
            .filter(|bytes| {
                *bytes <= novarocks_result_contract::ScalarProfileV1::ASSIGNMENT_SCRATCH_BYTES
            })
            .ok_or_else(|| "scalar SQL literal exceeds its assignment scratch bound".to_string())?;
        if let Some(output) = &mut self.output {
            output.push_str(text);
        }
        Ok(())
    }
    fn character(&mut self, value: char) -> Result<(), String> {
        match value {
            '\'' => self.append("''"),
            '\\' => self.append(r"\\"),
            _ => self.append(value.encode_utf8(&mut [0; 4])),
        }
    }
    fn quoted(&mut self, text: &str) -> Result<(), String> {
        self.append("'")?;
        for value in text.chars() {
            self.character(value)?;
        }
        self.append("'")
    }
}

fn write_scalar_sql(
    output: &mut ScalarSqlOutput,
    field: &ScalarField,
    value: &ScalarValue,
    nested: bool,
) -> Result<(), String> {
    if matches!(field.value_type, ScalarValueType::Null) {
        return Err("literal_from_batch does not support column type Null".into());
    }
    if matches!(value, ScalarValue::Null) {
        return output.append("NULL");
    }
    match (&field.value_type, value) {
        (ScalarValueType::List(item), ScalarValue::List(values)) => {
            output.append("[")?;
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.append(", ")?;
                }
                write_scalar_sql(output, item, value, true)?;
            }
            output.append("]")
        }
        (ScalarValueType::Map { key, value }, ScalarValue::Map(entries)) => {
            output.append("map(")?;
            for (index, (k, v)) in entries.iter().enumerate() {
                if index != 0 {
                    output.append(", ")?;
                }
                write_scalar_sql(output, key, k, true)?;
                output.append(", ")?;
                write_scalar_sql(output, value, v, true)?;
            }
            output.append(")")
        }
        (ScalarValueType::Struct(fields), ScalarValue::Struct(values))
            if fields.len() == values.len() =>
        {
            output.append("row(")?;
            for (index, (field, value)) in fields.iter().zip(values).enumerate() {
                if index != 0 {
                    output.append(", ")?;
                }
                write_scalar_sql(output, &field.field, value, true)?;
            }
            output.append(")")
        }
        (ScalarValueType::String, ScalarValue::String(text))
        | (ScalarValueType::Json, ScalarValue::Json(text)) => output.quoted(text),
        (ScalarValueType::Binary, ScalarValue::Binary(bytes))
        | (ScalarValueType::Variant, ScalarValue::Variant(bytes))
        | (ScalarValueType::Opaque(_), ScalarValue::Opaque { bytes, .. }) => {
            output.append("'")?;
            if nested {
                // literal_from_batch uses Latin-1 inside containers; the
                // existing top-level text bridge deliberately uses UTF-8 loss.
                for byte in bytes {
                    output.character(char::from(*byte))?;
                }
            } else {
                for chunk in bytes.utf8_chunks() {
                    for value in chunk.valid().chars() {
                        output.character(value)?;
                    }
                    if !chunk.invalid().is_empty() {
                        output.character('\u{fffd}')?;
                    }
                }
            }
            output.append("'")
        }
        _ => {
            if matches!(
                field.value_type,
                ScalarValueType::List(_) | ScalarValueType::Map { .. } | ScalarValueType::Struct(_)
            ) {
                return Err("scalar value differs from its frozen field".into());
            }
            let timezone = match &field.value_type {
                ScalarValueType::Timestamp { timezone, .. } => {
                    timezone.as_ref().map_or(0, String::len)
                }
                _ => 0,
            };
            // Only one fixed-width leaf exists at a time. This allowance covers
            // its small Arrow bridge, Literal/text and timezone-copy peak.
            let scratch = timezone
                .checked_mul(4)
                .and_then(|bytes| bytes.checked_add(4096))
                .filter(|bytes| {
                    *bytes <= novarocks_result_contract::ScalarProfileV1::ASSIGNMENT_SCRATCH_BYTES
                })
                .ok_or_else(|| "scalar leaf exceeds its assignment scratch bound".to_string())?;
            output.scratch = output.scratch.max(scratch);
            let array = scalar_value_array(field, value)?;
            let text = if nested {
                user_variable_literal_to_sql(&literal_from_batch(&array, 0)?)?
            } else {
                query_result_cell_to_user_variable_sql(&array, &scalar_storage_type(field), 0)?
            };
            output.append(&text)
        }
    }
}

/// The exact storage carrier of a frozen scalar field.
fn scalar_storage_type(field: &ScalarField) -> DataType {
    use ScalarValueType as S;
    match &field.value_type {
        S::Null => DataType::Null,
        S::Boolean => DataType::Boolean,
        S::SignedInteger(8) => DataType::Int8,
        S::SignedInteger(16) => DataType::Int16,
        S::SignedInteger(32) => DataType::Int32,
        S::SignedInteger(_) => DataType::Int64,
        S::LargeInt => DataType::FixedSizeBinary(16),
        S::Float32 => DataType::Float32,
        S::Float64 => DataType::Float64,
        S::Decimal {
            bits: 128,
            precision,
            scale,
        } => DataType::Decimal128(*precision, *scale as i8),
        S::Decimal {
            precision, scale, ..
        } => DataType::Decimal256(*precision, *scale as i8),
        S::String | S::Json => DataType::Utf8,
        S::Binary | S::Opaque(_) => DataType::Binary,
        S::Variant => DataType::LargeBinary,
        S::Date => DataType::Date32,
        S::TimeMicros => DataType::Time64(TimeUnit::Microsecond),
        S::Timestamp { unit, timezone } => DataType::Timestamp(
            match unit {
                ScalarTimestampUnit::Microsecond => TimeUnit::Microsecond,
                ScalarTimestampUnit::Nanosecond => TimeUnit::Nanosecond,
            },
            timezone.as_deref().map(Into::into),
        ),
        S::List(item) => DataType::List(Arc::new(Field::new(
            "item",
            scalar_storage_type(item),
            item.nullable,
        ))),
        S::Map { key, value } => DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", scalar_storage_type(key), key.nullable),
                    Field::new("value", scalar_storage_type(value), value.nullable),
                ])),
                false,
            )),
            false,
        ),
        S::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|named| {
                    Field::new(
                        named.name.clone(),
                        scalar_storage_type(&named.field),
                        named.field.nullable,
                    )
                })
                .collect::<Fields>(),
        ),
    }
}

/// One value as a one-row array of its exact storage type.
fn scalar_value_array(field: &ScalarField, value: &ScalarValue) -> Result<ArrayRef, String> {
    if matches!(
        field.value_type,
        ScalarValueType::List(_) | ScalarValueType::Map { .. } | ScalarValueType::Struct(_)
    ) {
        return Err("a scalar leaf bridge cannot materialize a container".into());
    }
    let storage = scalar_storage_type(field);
    let mismatch = || format!("scalar value does not match its frozen type {storage:?}");
    if matches!(value, ScalarValue::Null) {
        return Ok(new_null_array(&storage, 1));
    }
    let array: ArrayRef = match (&field.value_type, value) {
        (ScalarValueType::Boolean, ScalarValue::Boolean(value)) => {
            Arc::new(BooleanArray::from(vec![*value]))
        }
        (ScalarValueType::SignedInteger(bits), ScalarValue::SignedInteger { value, .. }) => {
            match bits {
                8 => Arc::new(Int8Array::from(vec![
                    i8::try_from(*value).map_err(|_| mismatch())?,
                ])),
                16 => Arc::new(Int16Array::from(vec![
                    i16::try_from(*value).map_err(|_| mismatch())?,
                ])),
                32 => Arc::new(Int32Array::from(vec![
                    i32::try_from(*value).map_err(|_| mismatch())?,
                ])),
                _ => Arc::new(Int64Array::from(vec![*value])),
            }
        }
        (ScalarValueType::LargeInt, ScalarValue::LargeInt(value)) => Arc::new(
            FixedSizeBinaryArray::try_from_iter(std::iter::once(value.to_le_bytes()))
                .map_err(|error| error.to_string())?,
        ),
        (ScalarValueType::Float32, ScalarValue::Float32(bits)) => {
            Arc::new(Float32Array::from(vec![f32::from_bits(*bits)]))
        }
        (ScalarValueType::Float64, ScalarValue::Float64(bits)) => {
            Arc::new(Float64Array::from(vec![f64::from_bits(*bits)]))
        }
        (
            ScalarValueType::Decimal { bits: 128, .. },
            ScalarValue::Decimal128 {
                coefficient,
                precision,
                scale,
            },
        ) => Arc::new(
            Decimal128Array::from(vec![*coefficient])
                .with_precision_and_scale(*precision, *scale as i8)
                .map_err(|error| error.to_string())?,
        ),
        (
            ScalarValueType::Decimal { .. },
            ScalarValue::Decimal256 {
                coefficient_le,
                precision,
                scale,
            },
        ) => Arc::new(
            Decimal256Array::from(vec![i256::from_le_bytes(*coefficient_le)])
                .with_precision_and_scale(*precision, *scale as i8)
                .map_err(|error| error.to_string())?,
        ),
        (ScalarValueType::String, ScalarValue::String(text))
        | (ScalarValueType::Json, ScalarValue::Json(text)) => {
            Arc::new(StringArray::from(vec![text.as_str()]))
        }
        (ScalarValueType::Binary, ScalarValue::Binary(bytes))
        | (ScalarValueType::Opaque(_), ScalarValue::Opaque { bytes, .. }) => {
            Arc::new(BinaryArray::from(vec![bytes.as_slice()]))
        }
        (ScalarValueType::Variant, ScalarValue::Variant(bytes)) => {
            Arc::new(LargeBinaryArray::from(vec![bytes.as_slice()]))
        }
        (ScalarValueType::Date, ScalarValue::Date(days)) => {
            Arc::new(Date32Array::from(vec![*days]))
        }
        (ScalarValueType::TimeMicros, ScalarValue::TimeMicros(ticks)) => {
            Arc::new(Time64MicrosecondArray::from(vec![*ticks]))
        }
        (ScalarValueType::Timestamp { unit, timezone }, ScalarValue::Timestamp { ticks, .. }) => {
            match unit {
                ScalarTimestampUnit::Microsecond => Arc::new(
                    TimestampMicrosecondArray::from(vec![*ticks])
                        .with_timezone_opt(timezone.clone()),
                ),
                ScalarTimestampUnit::Nanosecond => Arc::new(
                    TimestampNanosecondArray::from(vec![*ticks])
                        .with_timezone_opt(timezone.clone()),
                ),
            }
        }
        _ => return Err(mismatch()),
    };
    Ok(array)
}

fn query_result_cell_to_user_variable_sql(
    column: &ArrayRef,
    declared_type: &DataType,
    row_idx: usize,
) -> Result<String, String> {
    if column.is_null(row_idx) {
        return Ok("NULL".to_string());
    }
    if let Some(text) = arrow_text_cell(column, row_idx) {
        return user_variable_text_to_sql(&text?, declared_type);
    }
    let literal = literal_from_batch(column, row_idx)?;
    user_variable_literal_to_sql(&literal)
}

fn arrow_text_cell(column: &ArrayRef, row_idx: usize) -> Option<Result<String, String>> {
    match column.data_type() {
        DataType::Utf8 => Some(
            column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "failed to downcast user variable value to StringArray".to_string())
                .map(|arr| arr.value(row_idx).to_string()),
        ),
        DataType::LargeUtf8 => Some(
            column
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    "failed to downcast user variable value to LargeStringArray".to_string()
                })
                .map(|arr| arr.value(row_idx).to_string()),
        ),
        DataType::Binary => Some(
            column
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| "failed to downcast user variable value to BinaryArray".to_string())
                .map(|arr| String::from_utf8_lossy(arr.value(row_idx)).into_owned()),
        ),
        DataType::LargeBinary => Some(
            column
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| {
                    "failed to downcast user variable value to LargeBinaryArray".to_string()
                })
                .map(|arr| String::from_utf8_lossy(arr.value(row_idx)).into_owned()),
        ),
        _ => None,
    }
}

fn user_variable_text_to_sql(text: &str, declared_type: &DataType) -> Result<String, String> {
    Ok(match declared_type {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => text.to_string(),
        DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _) | DataType::Struct(_) => {
            text.to_string()
        }
        DataType::Null => "NULL".to_string(),
        _ => single_quoted_user_variable_sql(text),
    })
}

fn user_variable_literal_to_sql(literal: &Literal) -> Result<String, String> {
    Ok(match literal {
        Literal::Null => "NULL".to_string(),
        Literal::Bool(value) => if *value { "TRUE" } else { "FALSE" }.to_string(),
        Literal::Int(value) => value.to_string(),
        Literal::Float(value) => {
            if !value.is_finite() {
                return Err(format!(
                    "non-finite floating literal is not supported: {value}"
                ));
            }
            value.to_string()
        }
        Literal::String(value) | Literal::Date(value) => single_quoted_user_variable_sql(value),
        Literal::Array(items) => format!(
            "[{}]",
            items
                .iter()
                .map(user_variable_literal_to_sql)
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        ),
        Literal::Map(entries) => {
            let mut args = Vec::with_capacity(entries.len() * 2);
            for (key, value) in entries {
                args.push(user_variable_literal_to_sql(key)?);
                args.push(user_variable_literal_to_sql(value)?);
            }
            format!("map({})", args.join(", "))
        }
        Literal::Struct(values) => format!(
            "row({})",
            values
                .iter()
                .map(user_variable_literal_to_sql)
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        ),
    })
}

fn single_quoted_user_variable_sql(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len() + 2);
    for ch in value.chars() {
        match ch {
            '\'' => escaped.push_str("''"),
            '\\' => escaped.push_str(r"\\"),
            _ => escaped.push(ch),
        }
    }
    format!("'{escaped}'")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::DataType;
    use arrow::record_batch::RecordBatch;

    use crate::api::{QueryResult, ResultField};

    use super::query_result_to_user_variable_literal;

    #[test]
    fn borrowed_record_keeps_existing_leaf_literals_and_absent_null_semantics() {
        use novarocks_result_contract::{
            BorrowedScalarLeaf as V, BorrowedScalarRecord, ScalarField, ScalarLeafCursor,
            ScalarRecord, ScalarSchema, ScalarValueType as T,
        };
        for (ty, value) in [
            (
                T::SignedInteger(32),
                V::SignedInteger {
                    bits: 32,
                    value: -7,
                },
            ),
            (T::Boolean, V::Boolean(true)),
            (T::Date, V::Date(0)),
            (T::Float64, V::Float64(2.5f64.to_bits())),
            (T::Float64, V::Float64(f64::NAN.to_bits())),
            (
                T::Decimal {
                    bits: 128,
                    precision: 18,
                    scale: 4,
                },
                V::Decimal128 {
                    coefficient: 12345,
                    precision: 18,
                    scale: 4,
                },
            ),
            (T::String, V::String("a'b\\c")),
            (T::Json, V::Json("{\"v\":1}")),
            (T::Binary, V::Binary(&[0xff, b'\''])),
            (T::SignedInteger(64), V::NoRows),
            (T::SignedInteger(64), V::Null),
        ] {
            let schema = ScalarSchema::try_new(ScalarField {
                nullable: true,
                value_type: ty,
            })
            .unwrap();
            let cursor = ScalarLeafCursor::try_new(&schema, value).unwrap();
            let mut bytes = vec![0; cursor.encoded_len()];
            cursor.copy_range(0, &mut bytes).unwrap();
            let borrowed = BorrowedScalarRecord::try_decode(&schema, &bytes).unwrap();
            let owned = ScalarRecord::decode_owned(&schema, &bytes).unwrap();
            assert_eq!(
                super::borrowed_scalar_record_to_user_variable_literal(&borrowed),
                super::scalar_record_to_user_variable_literal(&schema, &owned)
            );
        }
    }

    #[test]
    fn borrowed_map_retains_order_and_nested_binary_latin1_semantics() {
        use novarocks_result_contract::{
            BorrowedScalarLeaf as V, BorrowedScalarRecord, ScalarField, ScalarRecord,
            ScalarRecordWriter, ScalarSchema, ScalarValueType as T,
        };
        let key = ScalarField {
            nullable: false,
            value_type: T::String,
        };
        let value = ScalarField {
            nullable: true,
            value_type: T::Binary,
        };
        let schema = ScalarSchema::try_new(ScalarField {
            nullable: false,
            value_type: T::Map {
                key: Box::new(key.clone()),
                value: Box::new(value.clone()),
            },
        })
        .unwrap();
        let mut bytes = Vec::with_capacity(64 * 1024);
        let mut writer = ScalarRecordWriter::new(&schema, &mut bytes).unwrap();
        writer.count(1).unwrap();
        writer.presence(&key, false).unwrap();
        writer.leaf(&key, V::String("key")).unwrap();
        writer.presence(&value, false).unwrap();
        writer.leaf(&value, V::Binary(&[0xff, b'\''])).unwrap();
        writer.finish(&schema).unwrap();
        let borrowed = BorrowedScalarRecord::try_decode(&schema, &bytes).unwrap();
        let owned = ScalarRecord::decode_owned(&schema, &bytes).unwrap();
        let actual = super::borrowed_scalar_record_to_user_variable_literal(&borrowed).unwrap();
        assert_eq!(actual, "map('key', 'ÿ''')");
        assert_eq!(
            Ok(actual),
            super::scalar_record_to_user_variable_literal(&schema, &owned)
        );
    }

    #[test]
    fn local_mv_scalar_source_counts_the_header_before_formatting() {
        for len in [65_512, 65_513] {
            let result = QueryResult {
                columns: vec![ResultField::new("TABLE_NAME", DataType::Utf8, false, None)],
                batches: vec![
                    RecordBatch::try_from_iter(vec![(
                        "TABLE_NAME",
                        Arc::new(StringArray::from(vec!["x".repeat(len)])) as ArrayRef,
                    )])
                    .unwrap(),
                ],
            };
            let output = super::local_mv_scalar_result_to_user_variable_literal(&result);
            if len == 65_512 {
                assert_eq!(output.unwrap().len(), len + 2);
            } else {
                assert_eq!(
                    output.unwrap_err(),
                    "scalar value exceeds its admitted profile"
                );
            }
        }
        let unsupported = QueryResult {
            columns: vec![ResultField::new("outside", DataType::Int64, false, None)],
            batches: vec![
                RecordBatch::try_from_iter(vec![(
                    "outside",
                    Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                )])
                .unwrap(),
            ],
        };
        assert_eq!(
            super::local_mv_scalar_result_to_user_variable_literal(&unsupported).unwrap_err(),
            "local MV scalar field is outside its closed source contract"
        );
    }

    #[test]
    fn borrowed_nested_record_converts_without_materializing_an_owned_child_tree() {
        use novarocks_result_contract::{
            BorrowedScalarRecord, ScalarField, ScalarLeafError, ScalarRecord, ScalarRecordWriter,
            ScalarSchema, ScalarValueType,
        };
        let item = ScalarField {
            nullable: true,
            value_type: ScalarValueType::SignedInteger(64),
        };
        let schema = ScalarSchema::try_new(ScalarField {
            nullable: false,
            value_type: ScalarValueType::List(Box::new(item.clone())),
        })
        .unwrap();
        for count in [3_000, 25_000] {
            let mut bytes = Vec::with_capacity(64 * 1024);
            let mut writer = ScalarRecordWriter::new(&schema, &mut bytes).unwrap();
            writer.count(count).unwrap();
            for _ in 0..count {
                writer.presence(&item, true).unwrap();
            }
            writer.finish(&schema).unwrap();
            assert_eq!(
                ScalarRecord::decode_owned(&schema, &bytes),
                Err(ScalarLeafError::ValueLimit)
            );
            let record = BorrowedScalarRecord::try_decode(&schema, &bytes).unwrap();
            let result = super::borrowed_scalar_record_to_user_variable_literal(&record);
            if count == 3_000 {
                assert_eq!(
                    result.unwrap(),
                    format!("[{}NULL]", "NULL, ".repeat(count - 1))
                );
            } else {
                assert_eq!(
                    result.unwrap_err(),
                    "scalar SQL literal exceeds its assignment scratch bound"
                );
            }
        }
    }

    #[test]
    fn borrowed_nested_struct_preserves_exact_escaping_order_nulls_and_fixed_leaves() {
        use novarocks_result_contract::{
            BorrowedScalarLeaf as V, BorrowedScalarRecord, NamedScalarField, ScalarField,
            ScalarRecordWriter, ScalarSchema, ScalarValueType as T,
        };
        let text = ScalarField {
            nullable: true,
            value_type: T::String,
        };
        let list = ScalarField {
            nullable: false,
            value_type: T::List(Box::new(text.clone())),
        };
        let boolean = ScalarField {
            nullable: false,
            value_type: T::Boolean,
        };
        let schema = ScalarSchema::try_new(ScalarField {
            nullable: false,
            value_type: T::Struct(vec![
                NamedScalarField {
                    name: "items".into(),
                    field: list.clone(),
                },
                NamedScalarField {
                    name: "enabled".into(),
                    field: boolean.clone(),
                },
            ]),
        })
        .unwrap();
        let mut bytes = Vec::with_capacity(64 * 1024);
        let mut writer = ScalarRecordWriter::new(&schema, &mut bytes).unwrap();
        writer.presence(&list, false).unwrap();
        writer.count(2).unwrap();
        writer.presence(&text, false).unwrap();
        writer.leaf(&text, V::String("a'b\\c")).unwrap();
        writer.presence(&text, true).unwrap();
        writer.presence(&boolean, false).unwrap();
        writer.leaf(&boolean, V::Boolean(true)).unwrap();
        writer.finish(&schema).unwrap();
        let record = BorrowedScalarRecord::try_decode(&schema, &bytes).unwrap();
        assert_eq!(
            super::borrowed_scalar_record_to_user_variable_literal(&record).unwrap(),
            "row(['a''b\\\\c', NULL], TRUE)"
        );
    }

    #[test]
    fn converts_text_values_to_escaped_sql_literals() {
        let result = QueryResult {
            columns: vec![ResultField::new("v", DataType::Utf8, true, None)],
            batches: vec![
                RecordBatch::try_from_iter(vec![(
                    "v",
                    Arc::new(StringArray::from(vec!["a'b\\c"])) as _,
                )])
                .expect("batch"),
            ],
        };

        assert_eq!(
            query_result_to_user_variable_literal(&result).expect("scalar literal"),
            "'a''b\\\\c'"
        );
    }

    #[test]
    fn rejects_more_than_one_scalar_row() {
        let result = QueryResult {
            columns: vec![ResultField::new("v", DataType::Int64, false, None)],
            batches: vec![
                RecordBatch::try_from_iter(vec![(
                    "v",
                    Arc::new(Int64Array::from(vec![1, 2])) as _,
                )])
                .expect("batch"),
            ],
        };

        assert_eq!(
            query_result_to_user_variable_literal(&result).expect_err("multiple rows fail"),
            "Subquery returns more than 1 row"
        );
    }

    mod relayed_scalar {
        use std::sync::Arc;

        use arrow::array::{
            ArrayRef, BinaryArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
            Float64Array, Int32Array, Int64Array, ListArray, MapArray, StringArray, StructArray,
            TimestampMicrosecondArray,
        };
        use arrow::buffer::OffsetBuffer;
        use arrow::datatypes::{DataType, Field, Fields};
        use novarocks_result_contract::{
            NamedScalarField, ScalarField, ScalarRecord, ScalarSchema, ScalarTimestampUnit,
            ScalarValue, ScalarValueType,
        };

        use super::super::{
            query_result_cell_to_user_variable_sql, scalar_record_to_user_variable_literal,
            scalar_storage_type,
        };

        fn field(value_type: ScalarValueType) -> ScalarField {
            ScalarField {
                nullable: true,
                value_type,
            }
        }

        /// The relayed record must convert exactly as the Arrow cell it
        /// replaces, refusals included, and its rebuilt storage must be that
        /// cell's type.
        fn assert_same_literal(scalar: ScalarField, value: ScalarValue, arrow: ArrayRef) {
            assert_eq!(&scalar_storage_type(&scalar), arrow.data_type());
            let expected = query_result_cell_to_user_variable_sql(&arrow, arrow.data_type(), 0);
            let schema = ScalarSchema::try_new(scalar).expect("scalar schema");
            assert_eq!(
                scalar_record_to_user_variable_literal(&schema, &ScalarRecord::Value(value)),
                expected
            );
        }

        #[test]
        fn typed_sql_literal_checks_growth_before_output_and_keeps_nested_binary_semantics() {
            use novarocks_result_contract::ScalarProfileV1;
            let schema = ScalarSchema::try_new(field(ScalarValueType::String)).unwrap();
            let record = ScalarRecord::Value(ScalarValue::String(
                "'".repeat(ScalarProfileV1::SINGLE_VALUE_BYTES),
            ));
            assert!(
                scalar_record_to_user_variable_literal(&schema, &record)
                    .unwrap_err()
                    .contains("scratch bound")
            );
            let binary = ScalarSchema::try_new(field(ScalarValueType::Binary)).unwrap();
            let record = ScalarRecord::Value(ScalarValue::Binary(vec![
                    0xff;
                    ScalarProfileV1::SINGLE_VALUE_BYTES
                ]));
            assert!(
                scalar_record_to_user_variable_literal(&binary, &record)
                    .unwrap_err()
                    .contains("scratch bound")
            );
            let item = field(ScalarValueType::Binary);
            let bytes = [0xff, b'\''];
            assert_same_literal(
                field(ScalarValueType::List(Box::new(item))),
                ScalarValue::List(vec![ScalarValue::Binary(bytes.to_vec())]),
                Arc::new(
                    ListArray::try_new(
                        Arc::new(Field::new("item", DataType::Binary, true)),
                        OffsetBuffer::from_lengths([1]),
                        Arc::new(BinaryArray::from(vec![bytes.as_slice()])),
                        None,
                    )
                    .unwrap(),
                ),
            );
            let count = 1024;
            assert_same_literal(
                field(ScalarValueType::List(Box::new(field(
                    ScalarValueType::Null,
                )))),
                ScalarValue::List(vec![ScalarValue::Null; count]),
                Arc::new(
                    ListArray::try_new(
                        Arc::new(Field::new("item", DataType::Null, true)),
                        OffsetBuffer::from_lengths([count]),
                        arrow::array::new_null_array(&DataType::Null, count),
                        None,
                    )
                    .unwrap(),
                ),
            );
        }

        #[test]
        fn no_rows_and_null_keep_their_distinct_literals() {
            assert_same_literal(
                field(ScalarValueType::Null),
                ScalarValue::Null,
                arrow::array::new_null_array(&DataType::Null, 1),
            );
            let schema =
                ScalarSchema::try_new(field(ScalarValueType::SignedInteger(64))).expect("schema");
            assert_eq!(
                scalar_record_to_user_variable_literal(&schema, &ScalarRecord::NoRows)
                    .expect("no rows"),
                "null"
            );
            assert_eq!(
                scalar_record_to_user_variable_literal(
                    &schema,
                    &ScalarRecord::Value(ScalarValue::Null)
                )
                .expect("null"),
                "NULL"
            );
        }

        #[test]
        fn leaf_values_format_as_their_arrow_cells() {
            assert_same_literal(
                field(ScalarValueType::SignedInteger(32)),
                ScalarValue::SignedInteger {
                    bits: 32,
                    value: -7,
                },
                Arc::new(Int32Array::from(vec![-7])),
            );
            let large = -170_141_183_460_469_231_731_687_303_715_884_105_000_i128;
            assert_same_literal(
                field(ScalarValueType::LargeInt),
                ScalarValue::LargeInt(large),
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(std::iter::once(large.to_le_bytes()))
                        .expect("largeint"),
                ),
            );
            assert_same_literal(
                field(ScalarValueType::Float64),
                ScalarValue::Float64(2.5_f64.to_bits()),
                Arc::new(Float64Array::from(vec![2.5])),
            );
            assert_same_literal(
                field(ScalarValueType::Decimal {
                    bits: 128,
                    precision: 10,
                    scale: 3,
                }),
                ScalarValue::Decimal128 {
                    coefficient: -12_345,
                    precision: 10,
                    scale: 3,
                },
                Arc::new(
                    Decimal128Array::from(vec![-12_345])
                        .with_precision_and_scale(10, 3)
                        .expect("decimal"),
                ),
            );
            assert_same_literal(
                field(ScalarValueType::String),
                ScalarValue::String("a'b\\c".to_string()),
                Arc::new(StringArray::from(vec!["a'b\\c"])),
            );
            assert_same_literal(
                field(ScalarValueType::Date),
                ScalarValue::Date(19_000),
                Arc::new(Date32Array::from(vec![19_000])),
            );
            assert_same_literal(
                field(ScalarValueType::Timestamp {
                    unit: ScalarTimestampUnit::Microsecond,
                    timezone: Some("UTC".to_string()),
                }),
                ScalarValue::Timestamp {
                    ticks: 1_700_000_000_123_456,
                    unit: ScalarTimestampUnit::Microsecond,
                },
                Arc::new(
                    TimestampMicrosecondArray::from(vec![1_700_000_000_123_456])
                        .with_timezone("UTC"),
                ),
            );
        }

        #[test]
        fn nested_values_format_as_their_arrow_cells() {
            let item = field(ScalarValueType::SignedInteger(64));
            assert_same_literal(
                field(ScalarValueType::List(Box::new(item))),
                ScalarValue::List(vec![
                    ScalarValue::SignedInteger { bits: 64, value: 1 },
                    ScalarValue::Null,
                    ScalarValue::SignedInteger { bits: 64, value: 3 },
                ]),
                Arc::new(
                    ListArray::try_new(
                        Arc::new(Field::new("item", DataType::Int64, true)),
                        OffsetBuffer::from_lengths([3]),
                        Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
                        None,
                    )
                    .expect("list"),
                ),
            );

            let key = ScalarField {
                nullable: false,
                value_type: ScalarValueType::String,
            };
            let value = field(ScalarValueType::SignedInteger(32));
            let entry_fields = Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int32, true),
            ]);
            let entries = StructArray::try_new(
                entry_fields.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["x", "y"])),
                    Arc::new(Int32Array::from(vec![Some(4), None])),
                ],
                None,
            )
            .expect("entries");
            assert_same_literal(
                field(ScalarValueType::Map {
                    key: Box::new(key),
                    value: Box::new(value),
                }),
                ScalarValue::Map(vec![
                    (
                        ScalarValue::String("x".to_string()),
                        ScalarValue::SignedInteger { bits: 32, value: 4 },
                    ),
                    (ScalarValue::String("y".to_string()), ScalarValue::Null),
                ]),
                Arc::new(
                    MapArray::try_new(
                        Arc::new(Field::new("entries", DataType::Struct(entry_fields), false)),
                        OffsetBuffer::from_lengths([2]),
                        entries,
                        None,
                        false,
                    )
                    .expect("map"),
                ),
            );

            let struct_fields = Fields::from(vec![
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Utf8, true),
            ]);
            assert_same_literal(
                field(ScalarValueType::Struct(vec![
                    NamedScalarField {
                        name: "a".to_string(),
                        field: field(ScalarValueType::SignedInteger(64)),
                    },
                    NamedScalarField {
                        name: "b".to_string(),
                        field: field(ScalarValueType::String),
                    },
                ])),
                ScalarValue::Struct(vec![
                    ScalarValue::SignedInteger { bits: 64, value: 9 },
                    ScalarValue::String("z".to_string()),
                ]),
                Arc::new(
                    StructArray::try_new(
                        struct_fields,
                        vec![
                            Arc::new(Int64Array::from(vec![9])),
                            Arc::new(StringArray::from(vec!["z"])),
                        ],
                        None,
                    )
                    .expect("struct"),
                ),
            );
        }

        #[test]
        fn a_value_outside_its_frozen_type_is_refused() {
            let schema =
                ScalarSchema::try_new(field(ScalarValueType::SignedInteger(8))).expect("schema");
            let error = scalar_record_to_user_variable_literal(
                &schema,
                &ScalarRecord::Value(ScalarValue::SignedInteger {
                    bits: 8,
                    value: 300,
                }),
            )
            .expect_err("out-of-range value");
            assert!(error.contains("does not match its frozen type"), "{error}");
        }
    }
}
