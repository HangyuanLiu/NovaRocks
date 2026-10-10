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
use super::*;
use crate::exec::chunk::ChunkSchema;
use crate::exec::expr::ExprNode;
use arrow::array::{
    FixedSizeBinaryArray, Int8Array, Int16Array, Int32Array, builder::FixedSizeBinaryBuilder,
};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use novarocks_functions::{
    ArithmeticRowResult, EvaluatedArgument, KernelEvaluationControl, KernelFailure,
    PreparedArithmeticRecipe,
};
use novarocks_type_contract::{
    ArithmeticOperator, CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
    ValueLogicalType, arithmetic_result_value_type_with_op,
};
use novarocks_types::SlotId;
use std::time::Duration;

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("arithmetic must not wait")
    }
}
fn large() -> FunctionValueType {
    FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::LargeInt,
    )
    .unwrap()
}
fn integer_array(ty: &FunctionValueType, values: &[Option<i128>]) -> ArrayRef {
    macro_rules! signed {
        ($array:ty, $native:ty) => {
            Arc::new(<$array>::from(
                values
                    .iter()
                    .map(|value| value.map(|value| <$native>::try_from(value).unwrap()))
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        };
    }
    match ty.data_type {
        DataType::Int8 => signed!(Int8Array, i8),
        DataType::Int16 => signed!(Int16Array, i16),
        DataType::Int32 => signed!(Int32Array, i32),
        DataType::Int64 => signed!(Int64Array, i64),
        DataType::FixedSizeBinary(16) => {
            let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), 16);
            for value in values {
                match value {
                    Some(value) => builder.append_value(value.to_be_bytes()).unwrap(),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        _ => panic!("fixture requires an explicit integral source"),
    }
}
fn chunk(
    left_type: &FunctionValueType,
    right_type: &FunctionValueType,
    left: ArrayRef,
    right: ArrayRef,
) -> Chunk {
    let schema = Arc::new(Schema::new(vec![
        left_type.try_to_field("left").unwrap(),
        right_type.try_to_field("right").unwrap(),
    ]));
    let batch = RecordBatch::try_new(schema, vec![left, right]).unwrap();
    let schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
        batch.schema().as_ref(),
        &[SlotId::new(1), SlotId::new(2)],
    )
    .unwrap();
    Chunk::new_with_chunk_schema(batch, schema)
}
fn legacy(
    op: ArithmeticOperator,
    left_type: &FunctionValueType,
    right_type: &FunctionValueType,
    left: ArrayRef,
    right: ArrayRef,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> Result<ArrayRef, String> {
    let result = arithmetic_result_value_type_with_op(left_type, right_type, op).unwrap();
    let chunk = chunk(left_type, right_type, left, right);
    let mut arena = ExprArena::default();
    arena.set_allow_throw_exception(allow);
    // The actual old arena has carrier-only metadata. This oracle is not a
    // new nominal FVT receipt; its input types above are separately authored.
    let a = arena.push_typed(
        ExprNode::SlotId(SlotId::new(1)),
        left_type.data_type.clone(),
    );
    let b = arena.push_typed(
        ExprNode::SlotId(SlotId::new(2)),
        right_type.data_type.clone(),
    );
    let expression = match op {
        ArithmeticOperator::Add => ExprNode::Add(a, b, policy),
        ArithmeticOperator::Subtract => ExprNode::Sub(a, b, policy),
        ArithmeticOperator::Multiply => ExprNode::Mul(a, b, policy),
        ArithmeticOperator::Divide => ExprNode::Div(a, b, policy),
        ArithmeticOperator::Modulo => ExprNode::Mod(a, b, policy),
    };
    let id = arena.push_typed(expression, result.data_type);
    let frozen = arena.into_immutable().unwrap();
    ExprArena::from_immutable(&frozen).unwrap().eval(id, &chunk)
}
fn prepared(
    op: ArithmeticOperator,
    left: &FunctionValueType,
    right: &FunctionValueType,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> PreparedArithmeticRecipe {
    let mut result = arithmetic_result_value_type_with_op(left, right, op).unwrap();
    result.nullable = true;
    PreparedArithmeticRecipe::try_new(op, left, right, &result, policy, allow, &Control).unwrap()
}
fn read_large(array: &ArrayRef, row: usize) -> Option<i128> {
    if array.is_null(row) {
        return None;
    }
    let array = array
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    Some(i128::from_be_bytes(array.value(row).try_into().unwrap()))
}
fn compare_integral(
    op: ArithmeticOperator,
    left_type: &FunctionValueType,
    right_type: &FunctionValueType,
    lhs: &[Option<i128>],
    rhs: &[Option<i128>],
    expected: &[Option<i128>],
) {
    // Actual slices retain an unused prefix/suffix and independent payloads.
    let sliced = |ty: &FunctionValueType, values: &[Option<i128>]| {
        let mut padded = vec![Some(0)];
        padded.extend_from_slice(values);
        padded.push(Some(42));
        integer_array(ty, &padded).slice(1, values.len())
    };
    let left = sliced(left_type, lhs);
    let right = sliced(right_type, rhs);
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for allow in [false, true] {
            let old = legacy(
                op,
                left_type,
                right_type,
                left.clone(),
                right.clone(),
                policy,
                allow,
            )
            .unwrap();
            assert_eq!(old.data_type(), &DataType::FixedSizeBinary(16));
            let recipe = prepared(op, left_type, right_type, policy, allow);
            assert_eq!(recipe.result_type(), &large());
            for (row, expected) in expected.iter().enumerate() {
                assert_eq!(read_large(&old, row), *expected);
                let actual = recipe
                    .evaluate_row(
                        EvaluatedArgument::Column(&left),
                        row,
                        row,
                        EvaluatedArgument::Column(&right),
                        row,
                        row,
                        &Control,
                    )
                    .unwrap();
                assert_eq!(
                    actual,
                    expected.map_or(ArithmeticRowResult::Null, ArithmeticRowResult::LargeInt)
                );
            }
        }
    }
}

#[test]
fn legacy_largeint_integral_four_ops_match_prepared_nine_ordered_pairs_and_wrapping_extrema() {
    use ArithmeticOperator::*;
    let types = [
        FunctionValueType::new(DataType::Int8, true),
        FunctionValueType::new(DataType::Int16, true),
        FunctionValueType::new(DataType::Int32, true),
        FunctionValueType::new(DataType::Int64, true),
        large(),
    ];
    let lhs = [Some(-7), None, Some(7), Some(7), Some(-7), None];
    let rhs = [Some(3), Some(3), None, Some(0), Some(-3), Some(0)];
    let mut pairs = 0;
    for (li, left) in types.iter().enumerate() {
        for (ri, right) in types.iter().enumerate() {
            if li != 4 && ri != 4 {
                continue;
            }
            pairs += 1;
            for (op, expected) in [
                (Add, [Some(-4), None, None, Some(7), Some(-10), None]),
                (Subtract, [Some(-10), None, None, Some(7), Some(-4), None]),
                (Multiply, [Some(-21), None, None, Some(0), Some(21), None]),
                (Modulo, [Some(-1), None, None, None, Some(-1), None]),
            ] {
                compare_integral(op, left, right, &lhs, &rhs, &expected);
            }
        }
    }
    assert_eq!(pairs, 9);
    for (op, lhs, rhs, expected) in [
        (Add, i128::MAX, 1, i128::MIN),
        (Subtract, i128::MIN, 1, i128::MAX),
        (Multiply, i128::MIN, -1, i128::MIN),
        (Multiply, i128::MAX, 2, -2),
        (Modulo, i128::MIN, -1, 0),
    ] {
        compare_integral(
            op,
            &large(),
            &large(),
            &[Some(lhs)],
            &[Some(rhs)],
            &[Some(expected)],
        );
    }
}

#[test]
fn legacy_legal_largeint_division_and_float64_mixed_profiles_expose_the_arrow_cast_gap() {
    let ty = large();
    let signed = FunctionValueType::new(DataType::Int64, true);
    for (left_type, right_type, left, right) in [
        (
            &ty,
            &ty,
            integer_array(&ty, &[Some(7)]),
            integer_array(&ty, &[Some(2)]),
        ),
        (
            &ty,
            &signed,
            integer_array(&ty, &[Some(7)]),
            integer_array(&signed, &[Some(2)]),
        ),
        (
            &signed,
            &ty,
            integer_array(&signed, &[Some(7)]),
            integer_array(&ty, &[Some(2)]),
        ),
    ] {
        assert_eq!(
            arithmetic_result_value_type_with_op(left_type, right_type, ArithmeticOperator::Divide)
                .unwrap()
                .data_type,
            DataType::Float64
        );
        let error = legacy(
            ArithmeticOperator::Divide,
            left_type,
            right_type,
            left,
            right,
            DecimalOverflowPolicy::OutputNull,
            false,
        )
        .unwrap_err();
        assert!(
            error.contains("Casting from FixedSizeBinary(16) to Int64 not supported"),
            "{error}"
        );
    }
    let float = FunctionValueType::new(DataType::Float64, true);
    let f: ArrayRef = Arc::new(Float64Array::from(vec![Some(2.0)]));
    let l = integer_array(&ty, &[Some(7)]);
    for (left_type, right_type, left, right) in
        [(&ty, &float, l.clone(), f.clone()), (&float, &ty, f, l)]
    {
        let error = legacy(
            ArithmeticOperator::Add,
            left_type,
            right_type,
            left,
            right,
            DecimalOverflowPolicy::OutputNull,
            false,
        )
        .unwrap_err();
        assert!(
            error.contains("Casting from FixedSizeBinary(16) to Float64 not supported"),
            "{error}"
        );
    }
}

#[test]
fn legacy_explicit_largeint_float64_cast_proves_signed_ties_even_conversion_for_new_division() {
    let ty = large();
    // Literal IEEE bit oracles were calculated from integer exponent,
    // significand and ties-to-even; do not derive expected values from CAST.
    let fixtures = [
        (Some(0), Some(0x0000_0000_0000_0000)),
        (Some(1), Some(0x3ff0_0000_0000_0000)),
        (Some(-1), Some(0xbff0_0000_0000_0000)),
        (Some((1_i128 << 53) + 1), Some(0x4340_0000_0000_0000)),
        (Some((1_i128 << 53) + 3), Some(0x4340_0000_0000_0002)),
        (Some(-((1_i128 << 53) + 1)), Some(0xc340_0000_0000_0000)),
        (Some(i128::MIN), Some(0xc7e0_0000_0000_0000)),
        (Some(i128::MAX), Some(0x47e0_0000_0000_0000)),
        (None, None),
    ];
    let left = integer_array(
        &ty,
        &fixtures.iter().map(|(value, _)| *value).collect::<Vec<_>>(),
    );
    let right = integer_array(&ty, &vec![Some(1); fixtures.len()]);
    let chunk = chunk(&ty, &ty, left.clone(), right.clone());
    let mut arena = ExprArena::default();
    let source = arena.push_typed(
        ExprNode::SlotId(SlotId::new(1)),
        DataType::FixedSizeBinary(16),
    );
    let cast = arena.push_typed(
        ExprNode::Cast(source, DecimalOverflowPolicy::OutputNull),
        DataType::Float64,
    );
    let frozen = arena.into_immutable().unwrap();
    let output = ExprArena::from_immutable(&frozen)
        .unwrap()
        .eval(cast, &chunk)
        .unwrap();
    let output = output.as_any().downcast_ref::<Float64Array>().unwrap();
    let recipe = prepared(
        ArithmeticOperator::Divide,
        &ty,
        &ty,
        DecimalOverflowPolicy::OutputNull,
        false,
    );
    for (row, (_, bits)) in fixtures.into_iter().enumerate() {
        let actual = recipe
            .evaluate_row(
                EvaluatedArgument::Column(&left),
                row,
                row,
                EvaluatedArgument::Column(&right),
                row,
                row,
                &Control,
            )
            .unwrap();
        match bits {
            Some(bits) => {
                assert!(!output.is_null(row));
                assert_eq!(output.value(row).to_bits(), bits);
                let ArithmeticRowResult::Float(value) = actual else {
                    panic!("fractional result must be F64")
                };
                assert_eq!(value.to_bits(), bits);
            }
            None => {
                assert!(output.is_null(row));
                assert_eq!(actual, ArithmeticRowResult::Null);
            }
        }
    }
}
