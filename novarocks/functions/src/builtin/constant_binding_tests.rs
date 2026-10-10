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

//! Explicit finite fixtures and actual CV-dependent binder regressions.
use crate::{ConstantPolicy, ConstantPool, ConstantValue, FunctionValueType};
use arrow_array::{Array, ArrayRef, Int64Array, StringArray};
use arrow_schema::{DataType, Field};
use novarocks_type_contract::{CompilePhase, PureCompileControl};
use std::sync::Arc;

pub(crate) fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 1024,
        max_array_nodes: 4096,
        max_logical_elements: 1_000_000,
        max_retained_buffer_bytes: 4_194_304,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 1_048_576,
        max_library_validation_work: 16_777_216,
        max_library_validation_bytes: 16_777_216,
    }
}
pub(crate) fn pool(array: ArrayRef, ty: FunctionValueType) -> ConstantPool {
    ConstantPool::try_new(
        Arc::new(Field::new("constant", ty.data_type.clone(), ty.nullable)),
        ty,
        array.to_data(),
        policy(),
        CompilePhase::FunctionSpecialization,
        crate::binding_test_control(),
    )
    .unwrap()
}
pub(crate) fn i64(value: i64, nullable: bool) -> ConstantValue {
    pool(
        Arc::new(Int64Array::from(vec![value])),
        FunctionValueType::new(DataType::Int64, nullable),
    )
    .value(0)
    .unwrap()
}
pub(crate) fn i32(value: i32, nullable: bool) -> ConstantValue {
    pool(
        Arc::new(arrow_array::Int32Array::from(vec![value])),
        FunctionValueType::new(DataType::Int32, nullable),
    )
    .value(0)
    .unwrap()
}
pub(crate) fn utf8(value: &str, nullable: bool) -> ConstantValue {
    utf8_typed(value, FunctionValueType::new(DataType::Utf8, nullable))
}
pub(crate) fn utf8_typed(value: &str, ty: FunctionValueType) -> ConstantValue {
    pool(Arc::new(StringArray::from(vec![value])), ty)
        .value(0)
        .unwrap()
}
pub(crate) fn null(ty: DataType) -> ConstantValue {
    pool(
        arrow_array::new_null_array(&ty, 1),
        FunctionValueType::new(ty, true),
    )
    .value(0)
    .unwrap()
}
pub(crate) fn boolean(value: bool) -> ConstantValue {
    pool(
        Arc::new(arrow_array::BooleanArray::from(vec![value])),
        FunctionValueType::new(DataType::Boolean, false),
    )
    .value(0)
    .unwrap()
}
pub(crate) fn u64(value: u64) -> ConstantValue {
    pool(
        Arc::new(arrow_array::UInt64Array::from(vec![value])),
        FunctionValueType::new(DataType::UInt64, false),
    )
    .value(0)
    .unwrap()
}
pub(crate) fn f64(value: f64) -> ConstantValue {
    pool(
        Arc::new(arrow_array::Float64Array::from(vec![value])),
        FunctionValueType::new(DataType::Float64, false),
    )
    .value(0)
    .unwrap()
}

use crate::{
    FunctionArgument, FunctionBindingError, FunctionBindingRequest, FunctionBindingResolver,
    FunctionResultType,
};
use novarocks_type_contract::{CompileControlError, ValueLogicalType};
use std::sync::Mutex;

fn argument(value: ConstantValue) -> FunctionArgument {
    FunctionArgument::Value {
        value_type: value.value_type().clone(),
        constant: Some(value),
    }
}
fn request(arguments: &[FunctionArgument]) -> FunctionBindingRequest<'_> {
    FunctionBindingRequest {
        arguments,
        logical_argument_count: arguments.len(),
        expected_result_type: None,
    }
}

#[test]
fn selected_text_ordinal_and_typed_null_are_actual_dynamic_binding_facts() {
    let strings = pool(
        Arc::new(StringArray::from(vec![
            None,
            Some("actual"),
            Some("unused"),
        ])),
        FunctionValueType::new(DataType::Utf8, true),
    );
    let (_, resolver) = super::dynamic_definition_parts("named_struct").unwrap();
    let args = [
        argument(strings.value(1).unwrap()),
        FunctionArgument::Value {
            value_type: FunctionValueType::new(DataType::Int64, false),
            constant: None,
        },
    ];
    let selected = resolver
        .resolve(request(&args), crate::binding_test_control())
        .unwrap();
    let FunctionResultType::Scalar(result) = &selected.result_type else {
        panic!("scalar");
    };
    let DataType::Struct(fields) = &result.data_type else {
        panic!("struct");
    };
    assert_eq!(fields[0].name(), "actual");
    resolver
        .validate_selected(&selected, request(&args), crate::binding_test_control())
        .unwrap();
    let null_args = [argument(strings.value(0).unwrap()), args[1].clone()];
    assert!(matches!(
        resolver.resolve(request(&null_args), crate::binding_test_control()),
        Err(FunctionBindingError::NoMatchingOverload)
    ));
}

#[test]
fn selected_signed_digits_preserve_i8_wrapping_and_null_is_not_nonconstant() {
    let digits = pool(
        Arc::new(Int64Array::from(vec![None, Some(257), Some(2)])),
        FunctionValueType::new(DataType::Int64, true),
    );
    for name in ["round", "truncate"] {
        let (_, resolver) = super::dynamic_definition_parts(name).unwrap();
        for (value, scale) in [
            (digits.value(1).unwrap(), 1),
            (digits.value(0).unwrap(), 5),
            (i32(257, false), 1),
        ] {
            let args = [
                FunctionArgument::Value {
                    value_type: FunctionValueType::new(DataType::Decimal128(12, 5), false),
                    constant: None,
                },
                argument(value),
            ];
            let selected = resolver
                .resolve(request(&args), crate::binding_test_control())
                .unwrap();
            assert_eq!(
                selected.result_type,
                FunctionResultType::Scalar(FunctionValueType::new(
                    DataType::Decimal128(38, scale),
                    true
                ))
            );
            resolver
                .validate_selected(&selected, request(&args), crate::binding_test_control())
                .unwrap();
        }
    }
    let typed_null = null(DataType::Int64);
    assert!(
        typed_null
            .is_null_observed(
                CompilePhase::FunctionSpecialization,
                crate::binding_test_control()
            )
            .unwrap()
    );
    assert!(matches!(
        argument(typed_null),
        FunctionArgument::Value {
            constant: Some(_),
            ..
        }
    ));
}

#[test]
fn direct_family_gate_rejects_complete_source_domain_and_nullable_mismatch() {
    let (_, resolver) = super::dynamic_definition_parts("named_struct").unwrap();
    for source in [
        FunctionValueType::new(DataType::Utf8, true),
        FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
            .unwrap(),
    ] {
        let args = [
            FunctionArgument::Value {
                value_type: source,
                constant: Some(utf8("name", false)),
            },
            FunctionArgument::Value {
                value_type: FunctionValueType::new(DataType::Int64, false),
                constant: None,
            },
        ];
        assert!(matches!(
            resolver.resolve(request(&args), crate::binding_test_control()),
            Err(FunctionBindingError::InvalidBinding(_))
        ));
    }
}

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut trace = self.trace.lock().unwrap();
        let index = trace.len();
        trace.push((phase, units));
        if let Some((at, cause)) = self.refusal
            && at == index
        {
            return Err(cause);
        }
        Ok(())
    }
}

#[test]
fn actual_selected_long_text_read_preserves_every_original_callback_and_primary_cause() {
    let (_, resolver) = super::dynamic_definition_parts("variant_get").unwrap();
    let path = "x".repeat(320 * 1024);
    let values = pool(
        Arc::new(StringArray::from(vec![
            Some("unused"),
            Some(path.as_str()),
            None,
        ])),
        FunctionValueType::new(DataType::Utf8, true),
    );
    let args = [
        FunctionArgument::Value {
            value_type: FunctionValueType::try_with_logical_type(
                DataType::LargeBinary,
                false,
                ValueLogicalType::Variant,
            )
            .unwrap(),
            constant: None,
        },
        argument(values.value(1).unwrap()),
    ];
    let good = Control::default();
    let selected = resolver.resolve(request(&args), &good).unwrap();
    let baseline = good.trace.lock().unwrap().clone();
    assert!(baseline.iter().any(|(_, units)| *units == 256));
    for frozen in [false, true] {
        let good = Control::default();
        if frozen {
            resolver
                .validate_selected(&selected, request(&args), &good)
                .unwrap();
        } else {
            resolver.resolve(request(&args), &good).unwrap();
        }
        let baseline = good.trace.lock().unwrap().clone();
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for index in 0..baseline.len() {
                let refusing = Control {
                    refusal: Some((index, cause)),
                    ..Default::default()
                };
                let error = if frozen {
                    resolver
                        .validate_selected(&selected, request(&args), &refusing)
                        .unwrap_err()
                } else {
                    resolver.resolve(request(&args), &refusing).unwrap_err()
                };
                assert_eq!(error.control_error(), Some(cause));
                assert_eq!(*refusing.trace.lock().unwrap(), baseline[..=index]);
            }
        }
    }
}
