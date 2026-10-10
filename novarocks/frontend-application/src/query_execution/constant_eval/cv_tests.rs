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

//! Checked CV fixtures exercise the real staged FE evaluator, not a second
//! evaluator or a syntax readback adapter. Policies are finite test inputs.
use super::*;
use arrow::array::{Date32Array, Float32Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::Field;
use novarocks_functions::{ConstantError, ConstantPolicy};
use novarocks_sql::compiler::FoldArg;
use novarocks_type_contract::{CompileControlError, DecimalOverflowPolicy, NR_LOGICAL_TYPE_KEY};
use std::sync::Mutex;

struct TestControl;
impl PureCompileControl for TestControl {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 1024,
        max_array_nodes: 4096,
        max_logical_elements: 1_000_000,
        max_retained_buffer_bytes: 16_777_216,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 1_048_576,
        max_library_validation_work: 67_108_864,
        max_library_validation_bytes: 67_108_864,
    }
}
fn pool(array: ArrayRef, ty: FunctionValueType) -> ConstantPool {
    ConstantPool::try_new(
        Arc::new(ty.try_to_field("literal").unwrap()),
        ty,
        array.to_data(),
        policy(),
        CompilePhase::FunctionSpecialization,
        &TestControl,
    )
    .unwrap()
}
fn arg(array: ArrayRef, nullable: bool) -> FoldArg {
    let ty = FunctionValueType::new(array.data_type().clone(), nullable);
    let value = pool(array, ty.clone()).value(0).unwrap();
    FoldArg {
        value,
        value_type: ty,
    }
}
fn i32(value: i32) -> FoldArg {
    arg(Arc::new(Int32Array::from(vec![value])), false)
}
fn i64(value: i64) -> FoldArg {
    arg(Arc::new(Int64Array::from(vec![value])), false)
}
fn text(value: &str) -> FoldArg {
    arg(Arc::new(StringArray::from(vec![value])), false)
}
fn decimal(value: i128, precision: u8, scale: i8) -> FoldArg {
    arg(
        Arc::new(
            Decimal128Array::from(vec![value])
                .with_precision_and_scale(precision, scale)
                .unwrap(),
        ),
        false,
    )
}
fn request(kind: FoldNodeKind, args: Vec<FoldArg>, result: DataType) -> FoldRequest {
    FoldRequest {
        kind,
        args,
        constant_policy: policy(),
        result_type: FunctionValueType::new(result, true),
    }
}
fn cast(args: Vec<FoldArg>, target: DataType) -> FoldRequest {
    request(
        FoldNodeKind::Cast(DecimalOverflowPolicy::OutputNull),
        args,
        target,
    )
}
fn binary(op: BinOp, args: Vec<FoldArg>, result: DataType) -> FoldRequest {
    request(
        FoldNodeKind::BinaryOp(op, DecimalOverflowPolicy::OutputNull),
        args,
        result,
    )
}
fn evaluate(request: &FoldRequest) -> ConstantValue {
    let value = constant_evaluator()
        .eval_scalar(request, &TestControl)
        .unwrap()
        .unwrap();
    assert!(
        value
            .value_type()
            .exactly_equals_observed::<ConstantError>(&request.result_type, || Ok(()))
            .unwrap()
    );
    assert_eq!(value.field().name(), "literal");
    assert_eq!(value.pool().resource_facts().rows, 1);
    value
}
fn decimal_value(value: &ConstantValue) -> i128 {
    value
        .pool()
        .array()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap()
        .value(value.ordinal() as usize)
}

#[test]
fn folds_int32_addition_without_an_integer_carrier_roundtrip() {
    let output = evaluate(&binary(BinOp::Add, vec![i32(1), i32(1)], DataType::Int32));
    assert_eq!(
        output
            .pool()
            .array()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        2
    );
}
#[test]
fn folds_date_format_and_utf8_to_date32_through_real_kernels() {
    let date = arg(Arc::new(Date32Array::from(vec![18262])), false);
    let output = evaluate(&request(
        FoldNodeKind::Function {
            name: "date_format".into(),
        },
        vec![date, text("%Y-%m-%d")],
        DataType::Utf8,
    ));
    assert_eq!(
        output
            .utf8_observed(CompilePhase::FunctionSpecialization, &TestControl)
            .unwrap(),
        Some("2020-01-01")
    );
    assert_eq!(
        evaluate(&cast(vec![text("2020-01-01")], DataType::Date32))
            .try_date32()
            .unwrap(),
        Some(18262)
    );
}
#[test]
fn folds_decimal_multiplication_and_checked_rounding_cast_with_exact_coefficients() {
    assert_eq!(
        decimal_value(&evaluate(&binary(
            BinOp::Mul,
            vec![decimal(125, 10, 2), decimal(400, 10, 2)],
            DataType::Decimal128(20, 4)
        ))),
        50000
    );
    assert_eq!(
        decimal_value(&evaluate(&cast(
            vec![decimal(99_999_999, 8, 3)],
            DataType::Decimal128(8, 2)
        ))),
        10_000_000
    );
    let overflow = evaluate(&cast(
        vec![decimal(99_999_999, 8, 3)],
        DataType::Decimal128(7, 2),
    ));
    assert!(
        overflow
            .is_null_observed(CompilePhase::FunctionSpecialization, &TestControl)
            .unwrap()
    );
}
#[test]
fn declines_unknown_and_unmapped_unary_shapes_without_a_fake_kernel() {
    for kind in [
        FoldNodeKind::Function {
            name: "no_such_novarocks_function".into(),
        },
        FoldNodeKind::UnaryOp(UnOp::Negate),
        FoldNodeKind::UnaryOp(UnOp::BitwiseNot),
    ] {
        assert!(matches!(
            constant_evaluator()
                .eval_scalar(&request(kind, vec![i32(1)], DataType::Int32), &TestControl),
            Ok(None)
        ));
    }
    let boolean = arg(
        Arc::new(arrow::array::BooleanArray::from(vec![true])),
        false,
    );
    assert_eq!(
        evaluate(&request(
            FoldNodeKind::UnaryOp(UnOp::Not),
            vec![boolean],
            DataType::Boolean
        ))
        .try_boolean()
        .unwrap(),
        Some(false)
    );
}
#[test]
fn division_by_zero_is_successful_typed_null_and_nonnullable_output_declines() {
    let mut request = binary(BinOp::Div, vec![i32(1), i32(0)], DataType::Float64);
    assert!(
        evaluate(&request)
            .is_null_observed(CompilePhase::FunctionSpecialization, &TestControl)
            .unwrap()
    );
    request.result_type.nullable = false;
    assert!(matches!(
        constant_evaluator().eval_scalar(&request, &TestControl),
        Ok(None)
    ));
}
#[test]
fn exact_source_gate_rejects_carrier_domain_and_nullable_forgery() {
    let original = i32(7);
    for ty in [
        FunctionValueType::new(DataType::Int32, true),
        FunctionValueType::new(DataType::Int64, false),
    ] {
        let mut forged = original.clone();
        forged.value_type = ty;
        assert!(matches!(
            constant_evaluator().eval_scalar(&cast(vec![forged], DataType::Int64), &TestControl),
            Err(SqlConstantEvaluationError::Constant(
                ConstantError::Invalid(_)
            ))
        ));
    }
    let json =
        FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
            .unwrap();
    let mut forged = text("{}");
    forged.value_type = json;
    assert!(matches!(
        constant_evaluator().eval_scalar(&cast(vec![forged], DataType::Utf8), &TestControl),
        Err(SqlConstantEvaluationError::Constant(
            ConstantError::Invalid(_)
        ))
    ));
}
#[test]
fn authored_nominal_and_nested_domains_decline_without_carrier_retagging() {
    for (ty, array) in [
        (
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap(),
            Arc::new(StringArray::from(vec!["{}"])) as ArrayRef,
        ),
        (
            FunctionValueType::try_with_logical_type(
                DataType::LargeBinary,
                true,
                ValueLogicalType::Variant,
            )
            .unwrap(),
            Arc::new(arrow::array::LargeBinaryArray::from(vec![b"x".as_slice()])) as ArrayRef,
        ),
        (
            FunctionValueType::try_with_logical_type(DataType::Binary, true, ValueLogicalType::Hll)
                .unwrap(),
            Arc::new(arrow::array::BinaryArray::from(vec![b"x".as_slice()])) as ArrayRef,
        ),
        (
            FunctionValueType::try_with_logical_type(
                DataType::Binary,
                true,
                ValueLogicalType::Bitmap,
            )
            .unwrap(),
            Arc::new(arrow::array::BinaryArray::from(vec![b"x".as_slice()])) as ArrayRef,
        ),
        (
            FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                true,
                ValueLogicalType::Uuid,
            )
            .unwrap(),
            arrow::array::new_null_array(&DataType::FixedSizeBinary(16), 1),
        ),
    ] {
        let value = pool(array, ty.clone()).value(0).unwrap();
        assert!(matches!(
            constant_evaluator().eval_scalar(
                &cast(
                    vec![FoldArg {
                        value,
                        value_type: ty
                    }],
                    DataType::Utf8
                ),
                &TestControl
            ),
            Ok(None)
        ));
    }
    let child = Field::new("item", DataType::Utf8, true)
        .with_metadata([(NR_LOGICAL_TYPE_KEY.into(), "json".into())].into());
    let ty = DataType::List(Arc::new(child));
    let null = arg(arrow::array::new_null_array(&ty, 1), true);
    assert!(matches!(
        constant_evaluator().eval_scalar(&cast(vec![null], ty), &TestControl),
        Ok(None)
    ));
}
#[test]
fn malformed_later_type_is_not_hidden_by_an_earlier_domain_decline() {
    let json =
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap();
    let value = pool(Arc::new(StringArray::from(vec!["{}"])), json.clone())
        .value(0)
        .unwrap();
    let mut invalid = i64(1);
    invalid.value_type.logical_type = ValueLogicalType::Json;
    let request = cast(
        vec![
            FoldArg {
                value,
                value_type: json,
            },
            invalid,
        ],
        DataType::Utf8,
    );
    assert!(matches!(
        constant_evaluator().eval_scalar(&request, &TestControl),
        Err(SqlConstantEvaluationError::InvalidType(_))
    ));
}
#[test]
fn exact_largeint_constant_and_nonzero_pool_ordinal_keep_nominal_identity() {
    let ty = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    let value = i128::from(i64::MAX) + 17;
    let bytes = [
        1_i128.to_be_bytes(),
        value.to_be_bytes(),
        (-3_i128).to_be_bytes(),
    ];
    let array = Arc::new(
        arrow::array::FixedSizeBinaryArray::try_from_iter(bytes.iter().map(|v| v.as_slice()))
            .unwrap(),
    );
    let source = pool(array, ty.clone()).value(1).unwrap();
    let mut request = cast(
        vec![FoldArg {
            value: source,
            value_type: ty.clone(),
        }],
        ty.data_type.clone(),
    );
    request.result_type = ty;
    assert_eq!(evaluate(&request).try_largeint().unwrap(), Some(value));
    request.args[0].value_type.logical_type = ValueLogicalType::Physical;
    assert!(matches!(
        constant_evaluator().eval_scalar(&request, &TestControl),
        Err(SqlConstantEvaluationError::Constant(_))
    ));
}
#[test]
fn float32_signaling_nan_and_signed_zero_never_widen_through_float64_syntax() {
    for bits in [
        0x7f80_0001,
        0xff80_0123,
        0x8000_0000,
        0x0000_0001,
        0x7f80_0000,
    ] {
        let source = arg(
            Arc::new(Float32Array::from(vec![f32::from_bits(bits)])),
            false,
        );
        let output = evaluate(&cast(vec![source], DataType::Float32));
        assert_eq!(
            output
                .pool()
                .array()
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(0)
                .to_bits(),
            bits
        );
    }
}
#[test]
fn newly_materialized_unsigned_negative_scale_and_nested_metadata_outputs_are_cv() {
    let output = evaluate(&cast(vec![i32(7)], DataType::UInt32));
    assert_eq!(
        output
            .pool()
            .array()
            .as_any()
            .downcast_ref::<arrow::array::UInt32Array>()
            .unwrap()
            .value(0),
        7
    );
    let output = evaluate(&cast(
        vec![decimal(123, 10, -2)],
        DataType::Decimal128(10, -2),
    ));
    assert_eq!(decimal_value(&output), 123);
    let field = Arc::new(
        Field::new("source", DataType::Int32, true)
            .with_metadata([("provider.fact".into(), "kept".into())].into()),
    );
    let ty = DataType::List(field);
    let output = evaluate(&cast(
        vec![arg(arrow::array::new_null_array(&ty, 1), true)],
        ty.clone(),
    ));
    assert_eq!(output.value_type().data_type, ty);
}
#[test]
fn checked_decimal_error_remains_ordinary_evaluation_while_policy_null_is_success() {
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        let request = request(
            FoldNodeKind::BinaryOp(BinOp::Add, policy),
            vec![
                decimal(99_999_999_999_999_999_999_999_999_999_999_999_999, 38, 0),
                i64(1),
            ],
            DataType::Decimal128(38, 0),
        );
        let result = constant_evaluator().eval_scalar(&request, &TestControl);
        if policy == DecimalOverflowPolicy::OutputNull {
            assert!(
                result
                    .unwrap()
                    .unwrap()
                    .is_null_observed(CompilePhase::FunctionSpecialization, &TestControl)
                    .unwrap()
            );
        } else {
            let Err(SqlConstantEvaluationError::Evaluation(message)) = result else {
                panic!("ordinary runtime error");
            };
            assert!(message.contains("'add' operation involving decimal values overflows"));
        }
    }
}

#[derive(Default)]
struct Control {
    checks: Mutex<Vec<u32>>,
    fail_at: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::FunctionSpecialization);
        assert!(units <= 256);
        let mut checks = self.checks.lock().unwrap();
        let index = checks.len();
        checks.push(units);
        if let Some((at, cause)) = self.fail_at
            && at == index
        {
            return Err(cause);
        }
        Ok(())
    }
}
fn assert_all_callbacks(
    request: &FoldRequest,
    expected: fn(&Result<Option<ConstantValue>, SqlConstantEvaluationError>),
) {
    let good = Control::default();
    let result = constant_evaluator().eval_scalar(request, &good);
    expected(&result);
    let trace = good.checks.lock().unwrap().clone();
    assert_eq!(trace[0], 0);
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 0..trace.len() {
            let refusing = Control {
                fail_at: Some((at, cause)),
                ..Default::default()
            };
            assert!(
                matches!(constant_evaluator().eval_scalar(request, &refusing), Err(SqlConstantEvaluationError::Control(actual)) if actual == cause)
            );
            assert_eq!(*refusing.checks.lock().unwrap(), trace[..=at]);
        }
    }
}
#[test]
fn real_wide_argument_decline_keeps_entry_quantum_tail_and_all_three_original_causes() {
    let request = request(
        FoldNodeKind::Function {
            name: "no_such_novarocks_function".into(),
        },
        vec![i64(7); 320],
        DataType::Int64,
    );
    assert_all_callbacks(&request, |result| assert!(matches!(result, Ok(None))));
    let good = Control::default();
    constant_evaluator().eval_scalar(&request, &good).unwrap();
    assert!(good.checks.lock().unwrap().contains(&256));
}
#[test]
fn actual_metadata_decline_and_successful_output_pool_keep_all_callback_prefixes() {
    let metadata = (0..255)
        .map(|i| (format!("provider.fact.{i}"), "value".into()))
        .chain([(NR_LOGICAL_TYPE_KEY.into(), "json".into())])
        .collect();
    let ty = DataType::List(Arc::new(
        Field::new("item", DataType::Utf8, true).with_metadata(metadata),
    ));
    let request = cast(vec![arg(arrow::array::new_null_array(&ty, 1), true)], ty);
    assert_all_callbacks(&request, |result| assert!(matches!(result, Ok(None))));
    assert_all_callbacks(
        &binary(BinOp::Add, vec![i32(1), i32(1)], DataType::Int32),
        |result| assert!(matches!(result, Ok(Some(_)))),
    );
}
#[test]
fn ordinary_kernel_error_and_nonnullable_decline_observe_exact_original_completion() {
    let request = request(
        FoldNodeKind::BinaryOp(BinOp::Add, DecimalOverflowPolicy::ReportError),
        vec![
            decimal(99_999_999_999_999_999_999_999_999_999_999_999_999, 38, 0),
            i64(1),
        ],
        DataType::Decimal128(38, 0),
    );
    assert_all_callbacks(&request, |result| {
        assert!(matches!(
            result,
            Err(SqlConstantEvaluationError::Evaluation(_))
        ))
    });
    let mut request = binary(BinOp::Div, vec![i32(1), i32(0)], DataType::Float64);
    request.result_type.nullable = false;
    assert_all_callbacks(&request, |result| assert!(matches!(result, Ok(None))));
}
#[test]
fn shared_type_resource_refusal_cannot_be_replaced_or_fail_open() {
    let field = Field::new("item", DataType::Utf8, true).with_metadata(
        [(
            "provider.fact".into(),
            "x".repeat(novarocks_type_contract::MAX_ARROW_FIELD_METADATA_VALUE_BYTES + 1),
        )]
        .into(),
    );
    let request = request(
        FoldNodeKind::Cast(DecimalOverflowPolicy::OutputNull),
        vec![],
        DataType::List(Arc::new(field)),
    );
    let refusing = Control {
        fail_at: Some((1, CompileControlError::Cancelled)),
        ..Default::default()
    };
    assert!(matches!(
        constant_evaluator().eval_scalar(&request, &refusing),
        Err(SqlConstantEvaluationError::Control(
            CompileControlError::ResourceExhausted
        ))
    ));
    assert_eq!(*refusing.checks.lock().unwrap(), [0]);
}
#[test]
fn explicit_result_policy_refusal_is_typed_preparation_and_has_no_later_callback() {
    let mut request = binary(BinOp::Add, vec![i32(1), i32(1)], DataType::Int32);
    request.constant_policy.max_rows = 0;
    let good = Control::default();
    assert!(matches!(
        constant_evaluator().eval_scalar(&request, &good),
        Err(SqlConstantEvaluationError::Constant(ConstantError::Limit(
            _
        )))
    ));
    let trace = good.checks.lock().unwrap().clone();
    let refusing = Control {
        fail_at: Some((trace.len(), CompileControlError::Cancelled)),
        ..Default::default()
    };
    assert!(matches!(
        constant_evaluator().eval_scalar(&request, &refusing),
        Err(SqlConstantEvaluationError::Constant(ConstantError::Limit(
            _
        )))
    ));
    assert_eq!(*refusing.checks.lock().unwrap(), trace);
}

#[test]
fn malformed_selected_utf8_bytes_decline_without_constructing_invalid_str() {
    use arrow::buffer::Buffer;
    for ty in [DataType::Utf8, DataType::LargeUtf8] {
        let offsets = if ty == DataType::Utf8 {
            Buffer::from_slice_ref([0i32, 1])
        } else {
            Buffer::from_slice_ref([0i64, 1])
        };
        // SAFETY: This deliberate invalid-UTF8 fixture is never read as &str.
        // The production guard examines only its validated offsets/raw bytes.
        let data = unsafe {
            arrow::array::ArrayData::builder(ty)
                .len(1)
                .add_buffer(offsets)
                .add_buffer(Buffer::from(vec![0xff]))
                .build_unchecked()
        };
        let output = arrow::array::make_array(data);
        let good = TestControl;
        let mut work =
            CompileCheckpoints::try_new(&good, CompilePhase::FunctionSpecialization).unwrap();
        assert!(!utf8_output_fits(&output, &mut work).unwrap());
        work.finish().unwrap();
    }
}
