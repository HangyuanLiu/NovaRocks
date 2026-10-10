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

//! Closed local source and borrowed typed-record scalar conversion.

use super::{ScalarSqlOutput, write_scalar_sql};
use crate::api::QueryResult;
use arrow::array::{BooleanArray, StringArray};
use arrow::datatypes::DataType;
use novarocks_result_contract::{ScalarField, ScalarSchema, ScalarValue, ScalarValueType};

/// Scalar projection of the admitted local MV inventory. Its closed source
/// publishes only UTF-8 and Boolean fields; it uses the same complete-record
/// and SQL scratch limits without constructing a wire record or owned tree.
pub fn local_mv_scalar_result_to_user_variable_literal(
    result: &QueryResult,
) -> Result<String, String> {
    use novarocks_result_contract::{BorrowedScalarLeaf as V, ScalarLeafCursor};
    if result.columns.len() != 1 {
        return Err(format!(
            "user variable assignment expected 1 column, got {}",
            result.columns.len()
        ));
    }
    let rows = result.row_count();
    if rows > 1 {
        return Err("Subquery returns more than 1 row".into());
    }
    if rows == 0 {
        return Ok("null".into());
    }
    let declared = &result.columns[0];
    let field = ScalarField {
        nullable: declared.nullable(),
        value_type: match declared.data_type() {
            DataType::Utf8 => ScalarValueType::String,
            DataType::Boolean => ScalarValueType::Boolean,
            _ => return Err("local MV scalar field is outside its closed source contract".into()),
        },
    };
    if declared.logical_type().is_some() {
        return Err("local MV scalar field has unexpected logical metadata".into());
    }
    let schema = ScalarSchema::try_new(field).map_err(|error| error.to_string())?;
    for batch in &result.batches {
        if batch.num_rows() == 0 {
            continue;
        }
        if batch.num_columns() != 1 || batch.column(0).data_type() != declared.data_type() {
            return Err("local MV scalar carrier differs from its field contract".into());
        }
        let array = batch.column(0);
        let value = if array.is_null(0) {
            V::Null
        } else {
            match schema.field().value_type {
                ScalarValueType::String => V::String(
                    array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or("local MV scalar UTF-8 carrier differs from its field")?
                        .value(0),
                ),
                ScalarValueType::Boolean => V::Boolean(
                    array
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or("local MV scalar Boolean carrier differs from its field")?
                        .value(0),
                ),
                _ => unreachable!("the local MV source contract is closed"),
            }
        };
        // Preflight the whole typed size before formatting or copying a value.
        ScalarLeafCursor::try_new(&schema, value).map_err(|error| error.to_string())?;
        let mut measured = ScalarSqlOutput::new(None);
        write_borrowed_leaf_sql(&mut measured, schema.field(), value, false)?;
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
        write_borrowed_leaf_sql(&mut output, schema.field(), value, false)?;
        return Ok(output
            .output
            .expect("the measured local leaf owns its exact output buffer"));
    }
    Err("local MV scalar source has no row despite its row count".into())
}

/// The production typed-record path walks borrowed values directly. A legal
/// compact record never needs a larger owned nested tree before conversion.
pub fn borrowed_scalar_record_to_user_variable_literal(
    record: &novarocks_result_contract::BorrowedScalarRecord<'_, '_>,
) -> Result<String, String> {
    let mut measured = ScalarSqlOutput::new(None);
    write_borrowed_scalar_sql(&mut measured, record)?;
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
    write_borrowed_scalar_sql(&mut output, record)?;
    Ok(output
        .output
        .expect("the second pass owns its exact output buffer"))
}

fn write_borrowed_scalar_sql(
    output: &mut ScalarSqlOutput,
    record: &novarocks_result_contract::BorrowedScalarRecord<'_, '_>,
) -> Result<(), String> {
    use novarocks_result_contract::ScalarRecordEvent as E;
    // The frozen depth bounds stack storage, independently of child count.
    let mut children = [0usize; novarocks_result_contract::RootProfileV1::MAX_DEPTH + 1];
    record
        .walk(|event| -> Result<(), String> {
            match event {
                E::NoRows => output.append("null"),
                E::End { field, .. } => output.append(match field.value_type {
                    ScalarValueType::List(_) => "]",
                    ScalarValueType::Map { .. } | ScalarValueType::Struct(_) => ")",
                    _ => unreachable!("only containers have end events"),
                }),
                E::Begin { field, depth } => {
                    if depth > 1 {
                        if children[depth - 1] != 0 {
                            output.append(", ")?;
                        }
                        children[depth - 1] += 1;
                    }
                    children[depth] = 0;
                    output.append(match field.value_type {
                        ScalarValueType::List(_) => "[",
                        ScalarValueType::Map { .. } => "map(",
                        ScalarValueType::Struct(_) => "row(",
                        _ => unreachable!("only containers have begin events"),
                    })
                }
                E::Leaf {
                    field,
                    value,
                    depth,
                } => {
                    if depth > 1 {
                        if children[depth - 1] != 0 {
                            output.append(", ")?;
                        }
                        children[depth - 1] += 1;
                    }
                    write_borrowed_leaf_sql(output, field, value, depth > 1)
                }
            }
        })
        .map_err(|error| error.to_string())
}

fn write_borrowed_leaf_sql(
    output: &mut ScalarSqlOutput,
    field: &ScalarField,
    value: novarocks_result_contract::BorrowedScalarLeaf<'_>,
    nested: bool,
) -> Result<(), String> {
    use novarocks_result_contract::BorrowedScalarLeaf as V;
    if matches!(field.value_type, ScalarValueType::Null) {
        return Err("literal_from_batch does not support column type Null".into());
    }
    match value {
        V::NoRows => output.append("null"),
        V::Null => output.append("NULL"),
        V::String(text) | V::Json(text) => output.quoted(text),
        V::Binary(bytes) | V::Variant(bytes) | V::Opaque { bytes, .. } => {
            output.append("'")?;
            if nested {
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
        fixed => {
            // Only fixed-width leaves use the existing exact Arrow/literal
            // bridge. No String, Vec or container is materialized here.
            let value = match fixed {
                V::Boolean(value) => ScalarValue::Boolean(value),
                V::SignedInteger { bits, value } => ScalarValue::SignedInteger { bits, value },
                V::LargeInt(value) => ScalarValue::LargeInt(value),
                V::Float32(bits) => ScalarValue::Float32(bits),
                V::Float64(bits) => ScalarValue::Float64(bits),
                V::Decimal128 {
                    coefficient,
                    precision,
                    scale,
                } => ScalarValue::Decimal128 {
                    coefficient,
                    precision,
                    scale,
                },
                V::Decimal256 {
                    coefficient_le,
                    precision,
                    scale,
                } => ScalarValue::Decimal256 {
                    coefficient_le,
                    precision,
                    scale,
                },
                V::Date(days) => ScalarValue::Date(days),
                V::TimeMicros(ticks) => ScalarValue::TimeMicros(ticks),
                V::Timestamp { ticks, unit } => ScalarValue::Timestamp { ticks, unit },
                _ => unreachable!("variable and absent leaves were handled without ownership"),
            };
            write_scalar_sql(output, field, &value, nested)
        }
    }
}
