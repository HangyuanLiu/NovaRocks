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
//! Immutable original GenerateSeries dispatcher and processor lifecycle witnesses.
use super::*;
use arrow::array::{StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};

pub(super) fn columns(values: &[Option<i128>], ty: &DataType) -> ArrayRef {
    macro_rules! ints {
        ($array:ty, $int:ty) => {
            Arc::new(<$array>::from(
                values
                    .iter()
                    .map(|v| v.map(|v| <$int>::try_from(v).unwrap()))
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        };
    }
    match ty {
        DataType::Int8 => ints!(Int8Array, i8),
        DataType::Int16 => ints!(Int16Array, i16),
        DataType::Int32 => ints!(Int32Array, i32),
        DataType::Int64 => ints!(Int64Array, i64),
        DataType::UInt8 => ints!(UInt8Array, u8),
        DataType::UInt16 => ints!(UInt16Array, u16),
        DataType::UInt32 => ints!(UInt32Array, u32),
        DataType::UInt64 => ints!(UInt64Array, u64),
        DataType::FixedSizeBinary(16) => largeint::array_from_i128(values).unwrap(),
        _ => panic!("test source requires an original integer carrier"),
    }
}
pub(super) fn raw(
    parameters: Vec<ArrayRef>,
    target: DataType,
    left: bool,
) -> Result<ArrayRef, String> {
    let rows = parameters.first().map_or(1, |a| a.len());
    let slots = (0..parameters.len())
        .map(|i| SlotId::new(u32::try_from(i + 11).unwrap()))
        .collect::<Vec<_>>();
    let fields = parameters
        .iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("parameter-{i}"), a.data_type().clone(), true))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new_with_options(
        Arc::new(Schema::new(fields)),
        parameters.clone(),
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .unwrap();
    let schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
        batch.schema().as_ref(),
        &slots,
    )
    .unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, schema);
    let result_slot = SlotId::new(29);
    let output_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
        &Schema::new(vec![Field::new(
            "original-series-column",
            target.clone(),
            left,
        )]),
        &[result_slot],
    )
    .unwrap();
    let factory = TableFunctionProcessorFactory::new(
        43,
        "generate_series".into(),
        slots,
        vec![],
        vec![result_slot],
        true,
        left,
        parameters.iter().map(|a| a.data_type().clone()).collect(),
        vec![target.clone()],
        output_schema,
        vec![TableFunctionOutputSlot::Result { index: 0 }],
    );
    let mut operator = factory.create(7, 0);
    let state = RuntimeState::default();
    let processor = operator.as_processor_mut().unwrap();
    processor
        .push_chunk(&state, chunk)
        .map_err(|e| e.to_string())?;
    processor.set_finishing(&state).map_err(|e| e.to_string())?;
    let mut output = Vec::new();
    while processor.has_output() {
        if let Some(chunk) = processor.pull_chunk(&state).map_err(|e| e.to_string())? {
            assert_eq!(chunk.schema().field(0).name(), "original-series-column");
            output.push(chunk.column_by_slot_id(result_slot)?);
        }
    }
    if output.is_empty() {
        return Ok(new_empty_array(&target));
    }
    let borrowed = output.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
    arrow::compute::concat(&borrowed).map_err(|e| e.to_string())
}
fn i64s(values: &[Option<i128>]) -> ArrayRef {
    columns(values, &DataType::Int64)
}
fn actual(array: &ArrayRef) -> Vec<Option<i128>> {
    (0..array.len())
        .map(|row| {
            let dummy = TableFunctionProcessorFactory::new(
                1,
                "generate_series".into(),
                vec![],
                vec![],
                vec![],
                false,
                false,
                vec![],
                vec![],
                crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                    &Schema::empty(),
                    &[],
                )
                .unwrap(),
                vec![],
            );
            // The original private reader is the authority for test output projection.
            let operator = TableFunctionProcessorOperator {
                name: dummy.name,
                function_name: dummy.function_name,
                param_slots: vec![],
                outer_slots: vec![],
                fn_result_slots: vec![],
                fn_result_required: false,
                is_left_join: false,
                param_types: vec![],
                ret_types: vec![],
                output_chunk_schema: dummy.output_chunk_schema,
                output_slot_sources: vec![],
                output_chunk: None,
                output_offset: 0,
                emit_empty_once: false,
                finishing: false,
                finished: false,
            };
            operator
                .int_like_arg_to_i128(array, row, 0, novarocks_functions::generate_series_core::IntegerDiagnosticContext::GenerateSeries)
                .unwrap()
        })
        .collect()
}
#[test]
fn original_generate_series_all_integer_input_and_return_carriers() {
    for source in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::FixedSizeBinary(16),
    ] {
        for target in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::FixedSizeBinary(16),
        ] {
            let out = raw(
                vec![columns(&[Some(1)], &source), columns(&[Some(4)], &source)],
                target.clone(),
                false,
            )
            .unwrap();
            assert_eq!(out.data_type(), &target);
            assert_eq!(actual(&out), vec![Some(1), Some(2), Some(3), Some(4)]);
        }
    }
}
#[test]
fn original_generate_series_order_default_step_null_empty_and_slice() {
    let out = raw(
        vec![
            i64s(&[Some(99), Some(1), Some(5), None, Some(3)]).slice(1, 4),
            i64s(&[Some(99), Some(4), Some(1), Some(7), Some(2)]).slice(1, 4),
            i64s(&[Some(99), Some(2), Some(-2), Some(1), Some(1)]).slice(1, 4),
        ],
        DataType::Int64,
        false,
    )
    .unwrap();
    assert_eq!(
        actual(&out),
        vec![Some(1), Some(3), Some(5), Some(3), Some(1)]
    );
    let left = raw(
        vec![i64s(&[None, Some(3)]), i64s(&[Some(7), Some(2)])],
        DataType::Int64,
        true,
    )
    .unwrap();
    assert_eq!(actual(&left), vec![None, None]);
    let empty = raw(vec![i64s(&[]), i64s(&[])], DataType::Utf8, false).unwrap();
    assert_eq!(empty.len(), 0);
    assert_eq!(empty.data_type(), &DataType::Utf8);
    let two = raw(
        vec![i64s(&[Some(-2)]), i64s(&[Some(2)])],
        DataType::Int64,
        false,
    )
    .unwrap();
    let three = raw(
        vec![i64s(&[Some(-2)]), i64s(&[Some(2)]), i64s(&[Some(1)])],
        DataType::Int64,
        false,
    )
    .unwrap();
    assert_eq!(two.to_data(), three.to_data());
    assert_eq!(
        actual(&two),
        vec![Some(-2), Some(-1), Some(0), Some(1), Some(2)]
    );
}
#[test]
fn original_generate_series_zero_step_and_null_mask_keep_full_messages() {
    assert_eq!(
        raw(
            vec![i64s(&[Some(1)]), i64s(&[Some(2)]), i64s(&[Some(0)])],
            DataType::Utf8,
            false
        )
        .unwrap_err(),
        "table function generate_series step size cannot equal zero"
    );
    let null = raw(
        vec![i64s(&[None]), i64s(&[Some(2)]), i64s(&[Some(0)])],
        DataType::Utf8,
        false,
    )
    .unwrap();
    assert_eq!(null.len(), 0);
    let reverse = raw(
        vec![i64s(&[Some(2)]), i64s(&[Some(1)])],
        DataType::Utf8,
        false,
    )
    .unwrap();
    assert_eq!(reverse.len(), 0);
}
#[test]
fn original_generate_series_arity_and_typed_errors_precede_return_cast() {
    for count in [0, 1, 4] {
        let parameters = (0..count).map(|_| i64s(&[Some(1)])).collect();
        assert_eq!(
            raw(parameters, DataType::Int64, false).unwrap_err(),
            format!("table function generate_series expects 2 or 3 args, got {count}")
        );
    }
    let text = Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef;
    assert_eq!(
        raw(vec![text, i64s(&[Some(1)])], DataType::Float64, false).unwrap_err(),
        "table function generate_series arg 0 expects TINYINT/SMALLINT/INT/BIGINT/LARGEINT, got Utf8"
    );
    let bare = Arc::new(NullArray::new(1)) as ArrayRef;
    assert_eq!(
        raw(vec![i64s(&[None]), bare], DataType::Float64, false).unwrap_err(),
        "table function generate_series arg 1 expects TINYINT/SMALLINT/INT/BIGINT/LARGEINT, got Null"
    );
    assert_eq!(
        raw(
            vec![i64s(&[Some(1)]), i64s(&[Some(1)])],
            DataType::UInt64,
            false
        )
        .unwrap_err(),
        "table function generate_series return type expects TINYINT/SMALLINT/INT/BIGINT/LARGEINT, got UInt64"
    );
}
#[test]
fn original_generate_series_cap_and_count_overflow_precede_output_materialization() {
    assert_eq!(
        raw(
            vec![i64s(&[Some(0)]), i64s(&[Some(i128::from(u32::MAX))])],
            DataType::Int64,
            false
        )
        .unwrap_err(),
        "table function output too large"
    );
    let ty = DataType::FixedSizeBinary(16);
    assert_eq!(
        raw(
            vec![
                columns(&[Some(0)], &ty),
                columns(&[Some(i128::MAX - 1)], &ty)
            ],
            DataType::Int64,
            false
        )
        .unwrap_err(),
        format!(
            "table function generate_series count overflow: {}",
            i128::MAX
        )
    );
    let mut total = MAX_TABLE_FUNCTION_OUTPUT_ROWS;
    assert_eq!(
        checked_add_table_function_rows(&mut total, 1).unwrap_err(),
        "table function output too large"
    );
    assert_eq!(total, MAX_TABLE_FUNCTION_OUTPUT_ROWS + 1);
    let mut total = usize::MAX;
    assert_eq!(
        checked_add_table_function_rows(&mut total, 1).unwrap_err(),
        "table function output too large"
    );
    assert_eq!(total, usize::MAX);
}
#[test]
fn original_generate_series_last_checked_add_is_observable_even_for_one_value() {
    let ty = DataType::FixedSizeBinary(16);
    for (value, step) in [(i128::MAX, 1), (i128::MIN, -1)] {
        assert_eq!(
            raw(
                vec![
                    columns(&[Some(value)], &ty),
                    columns(&[Some(value)], &ty),
                    columns(&[Some(step)], &ty)
                ],
                ty.clone(),
                false
            )
            .unwrap_err(),
            format!("table function generate_series value overflow: current={value} step={step}")
        );
    }
    let out = raw(
        vec![
            i64s(&[Some(i128::from(i64::MAX))]),
            i64s(&[Some(i128::from(i64::MAX))]),
        ],
        DataType::Int64,
        false,
    )
    .unwrap();
    assert_eq!(actual(&out), vec![Some(i128::from(i64::MAX))]);
}
#[test]
fn original_generate_series_return_width_range_errors_are_unchanged() {
    for (target, value, label) in [
        (DataType::Int8, 128, "TINYINT"),
        (DataType::Int16, 32768, "SMALLINT"),
        (DataType::Int32, i128::from(i32::MAX) + 1, "INT"),
        (DataType::Int64, i128::from(i64::MAX) + 1, "BIGINT"),
    ] {
        let ty = DataType::FixedSizeBinary(16);
        assert_eq!(
            raw(
                vec![columns(&[Some(value)], &ty), columns(&[Some(value)], &ty)],
                target,
                false
            )
            .unwrap_err(),
            format!("table function generate_series value out of {label} range: {value}")
        );
    }
}
#[test]
fn original_generate_series_extreme_count_debug_panics_release_wrapping_remain_original() {
    let difference = std::panic::catch_unwind(|| generate_series_count(i128::MIN, i128::MAX, 1));
    let negative_abs = std::panic::catch_unwind(|| generate_series_count(0, -1, i128::MIN));
    if cfg!(debug_assertions) {
        assert!(difference.is_err());
        assert!(negative_abs.is_err());
    } else {
        assert_eq!(difference.unwrap().unwrap(), 0);
        assert_eq!(negative_abs.unwrap().unwrap(), 1);
    }
}
