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
use crate::exec::chunk::Chunk;
use crate::exec::expr::function::date::common::{extract_datetime_array, naive_to_date32};
use crate::exec::expr::{ExprArena, ExprId};
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use arrow::array::{
    Array, ArrayRef, Date32Array, Int64Array, ListArray, StringArray, TimestampMicrosecondArray,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use chrono::NaiveDateTime;
use novarocks_functions::array_generate::{
    ArrayGenerateFailure, ArrayGenerateUnit, check_list_extent, integer_range_count,
};
fn contract(detail: impl std::fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from_domain(ArrayGenerateFailure::contract(detail))
}
fn conversion(source: arrow::error::ArrowError) -> ExecutionFailure {
    ExecutionFailure::from_domain(ArrayGenerateFailure::Arrow(source))
}
use std::sync::Arc;

fn cast_to_i64(array: ArrayRef, _arg_name: &str) -> ExecutionResult<ArrayRef> {
    if array.data_type() == &DataType::Int64 {
        return Ok(array);
    }
    // LARGEINT is admitted by the frozen numeric range domain. Its binary
    // carrier needs the same exact numeric cast as other scalar consumers.
    if novarocks_type_contract::is_largeint_data_type(array.data_type()) {
        return novarocks_types::arrow_cast::cast_scalar_with_special_rules(
            &array,
            &DataType::Int64,
        )
        .map_err(ExecutionFailure::from_domain);
    }
    cast(&array, &DataType::Int64).map_err(conversion)
}

fn build_datetime_values_array(
    values: Vec<NaiveDateTime>,
    target_type: &DataType,
) -> ExecutionResult<ArrayRef> {
    match target_type {
        DataType::Date32 => {
            let out = values
                .into_iter()
                .map(|dt| naive_to_date32(dt.date()))
                .collect::<Vec<_>>();
            Ok(Arc::new(Date32Array::from(out)) as ArrayRef)
        }
        DataType::Timestamp(_, _) => {
            let micros = values
                .into_iter()
                .map(|dt| dt.and_utc().timestamp_micros())
                .collect::<Vec<_>>();
            let arr = Arc::new(TimestampMicrosecondArray::from(micros)) as ArrayRef;
            if arr.data_type() == target_type {
                Ok(arr)
            } else {
                cast(&arr, target_type).map_err(conversion)
            }
        }
        other => Err(contract(format_args!(
            "array_generate date mode unsupported output element type: {other:?}"
        ))),
    }
}

fn eval_array_generate_numeric<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    output_field: Arc<Field>,
    target_item_type: DataType,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    let stop_arr = if args.len() == 1 {
        cast_to_i64(arena.eval(args[0], chunk)?, "stop")?
    } else {
        cast_to_i64(arena.eval(args[1], chunk)?, "stop")?
    };
    let stop = stop_arr
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| contract("array_generate internal stop downcast failure"))?;

    let start_arr = if args.len() == 1 {
        None
    } else {
        Some(cast_to_i64(arena.eval(args[0], chunk)?, "start")?)
    };
    let start = start_arr
        .as_ref()
        .map(|arr| {
            arr.as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| contract("array_generate internal start downcast failure"))
        })
        .transpose()?;

    let step_arr = if args.len() == 3 {
        Some(cast_to_i64(arena.eval(args[2], chunk)?, "step")?)
    } else {
        None
    };
    let step = step_arr
        .as_ref()
        .map(|arr| {
            arr.as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| contract("array_generate internal step downcast failure"))
        })
        .transpose()?;

    let mut values = Vec::<i64>::new();
    let mut offsets = Vec::with_capacity(chunk.len() + 1);
    offsets.push(0_i32);
    let mut current_offset: i64 = 0;
    let mut null_builder = NullBufferBuilder::new(chunk.len());

    for row in 0..chunk.len() {
        let stop_row = super::common::row_index(row, stop.len());
        if stop.is_null(stop_row) {
            null_builder.append_null();
            offsets.push(current_offset as i32);
            continue;
        }
        let stop_v = stop.value(stop_row);

        let start_v = if let Some(start_arr) = start {
            let start_row = super::common::row_index(row, start_arr.len());
            if start_arr.is_null(start_row) {
                null_builder.append_null();
                offsets.push(current_offset as i32);
                continue;
            }
            start_arr.value(start_row)
        } else {
            1_i64
        };

        let step_v = if let Some(step_arr) = step {
            let step_row = super::common::row_index(row, step_arr.len());
            if step_arr.is_null(step_row) {
                null_builder.append_null();
                offsets.push(current_offset as i32);
                continue;
            }
            step_arr.value(step_row)
        } else if start_v <= stop_v {
            1_i64
        } else {
            -1_i64
        };

        if step_v == 0 {
            null_builder.append_non_null();
            offsets.push(current_offset as i32);
            continue;
        }

        let row_values = integer_range_count(start_v, stop_v, step_v);
        check_list_extent(current_offset as u128 + row_values, row_values, row)
            .map_err(ExecutionFailure::from_domain)?;
        if step_v > 0 && start_v <= stop_v {
            let mut cur = start_v;
            while cur <= stop_v {
                values.push(cur);
                current_offset += 1;
                let Some(next) = cur.checked_add(step_v) else {
                    break;
                };
                cur = next;
            }
        } else if step_v < 0 && start_v >= stop_v {
            let mut cur = start_v;
            while cur >= stop_v {
                values.push(cur);
                current_offset += 1;
                let Some(next) = cur.checked_add(step_v) else {
                    break;
                };
                cur = next;
            }
        }

        null_builder.append_non_null();
        offsets.push(current_offset as i32);
    }

    let mut values_array = Arc::new(Int64Array::from(values)) as ArrayRef;
    if values_array.data_type() != &target_item_type {
        values_array = cast(&values_array, &target_item_type).map_err(conversion)?;
    }

    let out = ListArray::new(
        output_field,
        OffsetBuffer::new(offsets.into()),
        values_array,
        null_builder.finish(),
    );
    Ok(Arc::new(out) as ArrayRef)
}

fn eval_array_generate_datetime<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    output_field: Arc<Field>,
    target_item_type: DataType,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    let start_arr = arena.eval(args[0], chunk)?;
    let stop_arr = arena.eval(args[1], chunk)?;
    let start_values = extract_datetime_array(&start_arr)?;
    let stop_values = extract_datetime_array(&stop_arr)?;

    let step_arr = if args.len() >= 3 {
        Some(cast_to_i64(arena.eval(args[2], chunk)?, "step")?)
    } else {
        None
    };
    let step_values = step_arr
        .as_ref()
        .map(|arr| {
            arr.as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| contract("array_generate internal step downcast failure"))
        })
        .transpose()?;

    let unit_arr = if args.len() == 4 {
        let unit = arena.eval(args[3], chunk)?;
        if unit.data_type() == &DataType::Utf8 {
            unit
        } else {
            return Err(contract(
                "array_generate unit differs from its frozen Utf8 carrier",
            ));
        }
    } else {
        Arc::new(StringArray::from(vec!["day"])) as ArrayRef
    };
    let units = unit_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| contract("array_generate internal unit downcast failure"))?;

    let mut values = Vec::<NaiveDateTime>::new();
    let mut offsets = Vec::with_capacity(chunk.len() + 1);
    offsets.push(0_i32);
    let mut current_offset: i64 = 0;
    let mut null_builder = NullBufferBuilder::new(chunk.len());

    for row in 0..chunk.len() {
        let start_row = super::common::row_index(row, start_values.len());
        let stop_row = super::common::row_index(row, stop_values.len());
        let Some(start) = start_values[start_row] else {
            null_builder.append_null();
            offsets.push(current_offset as i32);
            continue;
        };
        let Some(stop) = stop_values[stop_row] else {
            null_builder.append_null();
            offsets.push(current_offset as i32);
            continue;
        };

        let step_v = if let Some(steps) = step_values {
            let step_row = super::common::row_index(row, steps.len());
            if steps.is_null(step_row) {
                return Err(ExecutionFailure::from_domain(
                    ArrayGenerateFailure::NullStep,
                ));
            }
            steps.value(step_row)
        } else {
            1_i64
        };
        if step_v < 0 {
            return Err(ExecutionFailure::from_domain(
                ArrayGenerateFailure::NegativeStep,
            ));
        }

        let unit_row = super::common::row_index(row, units.len());
        if units.is_null(unit_row) {
            null_builder.append_null();
            offsets.push(current_offset as i32);
            continue;
        }
        if step_v == 0 {
            null_builder.append_non_null();
            offsets.push(current_offset as i32);
            continue;
        }
        let unit = ArrayGenerateUnit::parse(units.value(unit_row))
            .map_err(ExecutionFailure::from_domain)?;
        if target_item_type == DataType::Date32 && unit.is_subday() {
            return Err(contract(
                "array_generate DATE subday range requires frozen DATETIME bounds",
            ));
        }

        let forward = start <= stop;
        let actual_step = if forward { step_v } else { -step_v };
        let mut current = start;
        let row_begin = current_offset;
        loop {
            if forward {
                if current > stop {
                    break;
                }
            } else if current < stop {
                break;
            }
            check_list_extent(
                current_offset as u128 + 1,
                (current_offset - row_begin) as u128 + 1,
                row,
            )
            .map_err(ExecutionFailure::from_domain)?;
            values.push(current);
            current_offset += 1;
            let next = unit
                .advance(current, actual_step)
                .map_err(ExecutionFailure::from_domain)?;
            if next == current {
                break;
            }
            current = next;
        }

        null_builder.append_non_null();
        offsets.push(current_offset as i32);
    }

    let values_array = build_datetime_values_array(values, &target_item_type)?;
    let out = ListArray::new(
        output_field,
        OffsetBuffer::new(offsets.into()),
        values_array,
        null_builder.finish(),
    );
    Ok(Arc::new(out) as ArrayRef)
}

pub fn eval_array_generate<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    if args.is_empty() || args.len() > 4 {
        return Err(contract("array_generate expects 1 to 4 arguments"));
    }

    let output_field = match arena.data_type(expr) {
        Some(DataType::List(field)) => field.clone(),
        _ => {
            return Err(contract(
                "array_generate requires a frozen List output type",
            ));
        }
    };
    let target_item_type = output_field.data_type().clone();
    let argument_types = args
        .iter()
        .map(|arg| {
            arena
                .data_type(*arg)
                .cloned()
                .ok_or_else(|| contract("array_generate requires frozen argument types"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let expected = novarocks_type_contract::array_generate_item_type(&argument_types)
        .ok_or_else(|| contract("array_generate requires normalized frozen bounds"))?;
    if expected != target_item_type {
        return Err(contract(
            "array_generate frozen output differs from its bound domain",
        ));
    }
    let is_date_mode = matches!(
        target_item_type,
        DataType::Date32 | DataType::Timestamp(_, _)
    );
    if is_date_mode {
        Ok((eval_array_generate_datetime(arena, output_field, target_item_type, args, chunk))?)
    } else {
        Ok((eval_array_generate_numeric(arena, output_field, target_item_type, args, chunk))?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::function::array::eval_array_function;
    use crate::exec::expr::{ExprNode, LiteralValue};
    use arrow::datatypes::Schema;
    mod common {
        use super::*;
        pub fn chunk_len_1() -> Chunk {
            let batch = arrow::record_batch::RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "input",
                    DataType::Int64,
                    false,
                )])),
                vec![Arc::new(Int64Array::from(vec![0])) as ArrayRef],
            )
            .unwrap();
            let schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[novarocks_types::SlotId::new(1)],
            )
            .unwrap();
            Chunk::new_with_chunk_schema(batch, schema)
        }
        pub fn typed_null(arena: &mut ExprArena, ty: DataType) -> ExprId {
            arena.push_typed(ExprNode::Literal(LiteralValue::Null), ty)
        }
        pub fn literal_i64(arena: &mut ExprArena, value: i64) -> ExprId {
            arena.push_typed(
                ExprNode::Literal(LiteralValue::Int64(value)),
                DataType::Int64,
            )
        }
    }

    #[test]
    fn array_generate_wide_numeric_row_refuses_before_expansion_with_exact_facts() {
        use novarocks_failure_contract::{FailureClass, FailureProjection, RepresentationBoundary};
        let chunk = common::chunk_len_1();
        let mut arena = ExprArena::default();
        let start = common::literal_i64(&mut arena, 0);
        let stop = common::literal_i64(&mut arena, i32::MAX as i64);
        let expr = arena.push_typed(
            ExprNode::Literal(LiteralValue::Null),
            DataType::List(Arc::new(Field::new("item", DataType::Int64, false))),
        );
        let failure = eval_array_generate(&arena, expr, &[start, stop], &chunk).unwrap_err();
        let fact = failure.failure_fact();
        assert_eq!(fact.class(), FailureClass::Representation);
        let extent = fact.representation().unwrap();
        assert_eq!(extent.boundary, RepresentationBoundary::MinimumUnit);
        assert_eq!(extent.required, Some(i32::MAX as u64 + 1));
        assert_eq!(extent.extent_limit, Some(i32::MAX as u64));
        assert_eq!(extent.representable_prefix, Some(0));
    }

    #[test]
    fn array_generate_prepared_timestamp_columns_keep_clock_and_null_bounds() {
        use crate::exec::chunk::{Chunk, ChunkSchema};
        use crate::exec::expr::function::FunctionKind;
        use arrow::array::{ArrayRef, TimestampMicrosecondArray};
        use arrow::datatypes::TimeUnit;
        use arrow::record_batch::RecordBatch;
        use novarocks_types::SlotId;
        let timestamp = DataType::Timestamp(TimeUnit::Microsecond, None);
        let parse = |s: &str| {
            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                .unwrap()
                .and_utc()
                .timestamp_micros()
        };
        let start = parse("2025-10-01 14:28:31");
        let end = parse("2025-10-01 14:28:32.800000");
        let schema = Arc::new(Schema::new(vec![
            Field::new("start", timestamp.clone(), true),
            Field::new("end", timestamp.clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(start),
                    Some(end),
                    None,
                    Some(start),
                ])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(end),
                    Some(start),
                    Some(end),
                    None,
                ])) as ArrayRef,
            ],
        )
        .unwrap();
        let slots = [SlotId::new(1), SlotId::new(2)];
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, chunk_schema);
        let mut arena = ExprArena::default();
        let left = arena.push_typed(ExprNode::SlotId(slots[0]), timestamp.clone());
        let right = arena.push_typed(ExprNode::SlotId(slots[1]), timestamp.clone());
        let step = common::literal_i64(&mut arena, 500000);
        let unit = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("microsecond".to_string())),
            DataType::Utf8,
        );
        let out_type = DataType::List(Arc::new(Field::new("item", timestamp, true)));
        let expr = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Array("array_generate"),
                args: vec![left, right, step, unit],
            },
            out_type,
        );
        let frozen = arena.into_immutable().unwrap();
        let prepared =
            ExprArena::from_immutable(&frozen).expect("legacy frozen expression fixture");
        let output = prepared.eval(expr, &chunk).unwrap();
        let list = output.as_any().downcast_ref::<ListArray>().unwrap();
        for (row, values) in [
            (
                0,
                vec![start, start + 500000, start + 1000000, start + 1500000],
            ),
            (1, vec![end, end - 500000, end - 1000000, end - 1500000]),
        ] {
            let values_array = list.value(row);
            let values_array = values_array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            assert_eq!(values_array.values().to_vec(), values);
        }
        assert!(list.is_null(2));
        assert!(list.is_null(3));
    }

    #[test]
    fn array_generate_rejects_unfrozen_raw_text_and_wrong_output() {
        use arrow::datatypes::TimeUnit;
        let mut arena = ExprArena::default();
        let chunk = common::chunk_len_1();
        let timestamp = DataType::Timestamp(TimeUnit::Microsecond, None);
        let text = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("2025-10-01 14:28:31".to_string())),
            DataType::Utf8,
        );
        let stop = arena.push_typed(
            ExprNode::Cast(
                text,
                novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            ),
            timestamp.clone(),
        );
        let out = common::typed_null(
            &mut arena,
            DataType::List(Arc::new(Field::new("item", timestamp.clone(), true))),
        );
        assert!(
            eval_array_function("array_generate", &arena, out, &[text, text], &chunk)
                .unwrap_err()
                .to_string()
                .contains("normalized frozen bounds")
        );
        let wrong = common::typed_null(
            &mut arena,
            DataType::List(Arc::new(Field::new("item", DataType::Date32, true))),
        );
        assert!(
            eval_array_function("array_generate", &arena, wrong, &[stop, stop], &chunk)
                .unwrap_err()
                .to_string()
                .contains("differs from its bound domain")
        );
        let no_type = common::typed_null(&mut arena, DataType::Null);
        assert!(
            eval_array_function("array_generate", &arena, no_type, &[stop, stop], &chunk)
                .unwrap_err()
                .to_string()
                .contains("frozen List")
        );
    }

    #[test]
    fn array_generate_prepared_zero_invalid_text_and_negative_step_controls() {
        use crate::exec::expr::function::FunctionKind;
        let mut arena = ExprArena::default();
        let chunk = common::chunk_len_1();
        let start = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("2025-10-01".to_string())),
            DataType::Utf8,
        );
        let invalid = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("abc".to_string())),
            DataType::Utf8,
        );
        let start = arena.push_typed(
            ExprNode::Cast(
                start,
                novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            ),
            DataType::Date32,
        );
        let invalid = arena.push_typed(
            ExprNode::Cast(
                invalid,
                novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            ),
            DataType::Date32,
        );
        let zero = common::literal_i64(&mut arena, 0);
        let negative = common::literal_i64(&mut arena, -1);
        let unit = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("day".to_string())),
            DataType::Utf8,
        );
        let dtype = DataType::List(Arc::new(Field::new("item", DataType::Date32, true)));
        let empty = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Array("array_generate"),
                args: vec![start, start, zero, unit],
            },
            dtype.clone(),
        );
        let null = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Array("array_generate"),
                args: vec![start, invalid, zero, unit],
            },
            dtype.clone(),
        );
        let error = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Array("array_generate"),
                args: vec![start, start, negative, unit],
            },
            dtype,
        );
        let frozen = arena.into_immutable().unwrap();
        let prepared =
            ExprArena::from_immutable(&frozen).expect("legacy frozen expression fixture");
        let output = prepared.eval(empty, &chunk).unwrap();
        let list = output.as_any().downcast_ref::<ListArray>().unwrap();
        assert!(!list.is_null(0));
        assert_eq!(list.value_length(0), 0);
        let output = prepared.eval(null, &chunk).unwrap();
        assert!(output.is_null(0));
        assert!(
            prepared
                .eval(error, &chunk)
                .unwrap_err()
                .to_string()
                .contains("non-negative")
        );
    }

    #[test]
    fn array_generate_rejects_date_subday_profile_without_sql_promotion() {
        let mut arena = ExprArena::default();
        let chunk = common::chunk_len_1();
        let text = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("2025-10-01".to_string())),
            DataType::Utf8,
        );
        let date = arena.push_typed(
            ExprNode::Cast(
                text,
                novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            ),
            DataType::Date32,
        );
        let step = common::literal_i64(&mut arena, 1);
        let unit = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("hour".to_string())),
            DataType::Utf8,
        );
        let out = common::typed_null(
            &mut arena,
            DataType::List(Arc::new(Field::new("item", DataType::Date32, true))),
        );
        assert!(
            eval_array_function(
                "array_generate",
                &arena,
                out,
                &[date, date, step, unit],
                &chunk
            )
            .unwrap_err()
            .to_string()
            .contains("requires frozen DATETIME bounds")
        );
    }

    #[test]
    fn array_generate_prepared_extreme_steps_error_and_month_clamp_survives() {
        use crate::exec::chunk::{Chunk, ChunkSchema};
        use crate::exec::expr::function::FunctionKind;
        use arrow::datatypes::TimeUnit;
        use arrow::record_batch::RecordBatch;
        use novarocks_types::SlotId;
        let timestamp = DataType::Timestamp(TimeUnit::Microsecond, None);
        let parse = |s: &str| {
            NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                .unwrap()
                .and_utc()
                .timestamp_micros()
        };
        let start = parse("2025-01-31 14:28:31.250000");
        let end = parse("2025-04-30 14:28:31.250000");
        let schema = Arc::new(Schema::new(vec![
            Field::new("start", timestamp.clone(), true),
            Field::new("end", timestamp.clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![
                    None,
                    Some(start),
                    Some(end),
                ])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(end),
                    Some(end),
                    Some(start),
                ])) as ArrayRef,
            ],
        )
        .unwrap();
        let slots = [SlotId::new(1), SlotId::new(2)];
        let schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, schema);
        let mut arena = ExprArena::default();
        let left = arena.push_typed(ExprNode::SlotId(slots[0]), timestamp.clone());
        let right = arena.push_typed(ExprNode::SlotId(slots[1]), timestamp.clone());
        let dtype = DataType::List(Arc::new(Field::new("item", timestamp, true)));
        let mut errors = Vec::new();
        for (step, unit) in [
            (i64::MAX, "year"),
            (i64::MAX, "quarter"),
            (i32::MAX as i64, "month"),
            (i64::MAX, "week"),
            (i64::MAX, "day"),
            (i64::MAX, "hour"),
            (i64::MAX, "minute"),
            (i64::MAX, "second"),
            (i64::MAX, "millisecond"),
            (i64::MAX, "microsecond"),
        ] {
            let step = common::literal_i64(&mut arena, step);
            let unit = arena.push_typed(
                ExprNode::Literal(LiteralValue::Utf8(unit.to_string())),
                DataType::Utf8,
            );
            errors.push(arena.push_typed(
                ExprNode::FunctionCall {
                    kind: FunctionKind::Array("array_generate"),
                    args: vec![left, right, step, unit],
                },
                dtype.clone(),
            ));
        }
        let step = common::literal_i64(&mut arena, 1);
        let unit = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("month".to_string())),
            DataType::Utf8,
        );
        let normal = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Array("array_generate"),
                args: vec![left, right, step, unit],
            },
            dtype,
        );
        let frozen = arena.into_immutable().unwrap();
        let prepared =
            ExprArena::from_immutable(&frozen).expect("legacy frozen expression fixture");
        for expr in errors {
            // NULL row zero is visited first; overflow in the next real row
            // refuses the whole batch instead of returning partial output.
            assert!(
                prepared
                    .eval(expr, &chunk)
                    .unwrap_err()
                    .to_string()
                    .contains("temporal step is out of range")
            );
        }
        let output = prepared.eval(normal, &chunk).unwrap();
        let output = output.as_any().downcast_ref::<ListArray>().unwrap();
        assert!(output.is_null(0));
        for (row, texts) in [
            (
                1,
                vec![
                    "2025-01-31 14:28:31.250000",
                    "2025-02-28 14:28:31.250000",
                    "2025-03-28 14:28:31.250000",
                    "2025-04-28 14:28:31.250000",
                ],
            ),
            (
                2,
                vec![
                    "2025-04-30 14:28:31.250000",
                    "2025-03-30 14:28:31.250000",
                    "2025-02-28 14:28:31.250000",
                ],
            ),
        ] {
            let values = output.value(row);
            let values = values
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            assert_eq!(
                values.values().to_vec(),
                texts.into_iter().map(parse).collect::<Vec<_>>()
            );
        }
    }
}
