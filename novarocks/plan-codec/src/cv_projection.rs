// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Native-v1 payload projection from the sole checked constant owner.
//! This does not construct a source literal/type or authorize its allocation.

use super::{ExpressionEncodingContext, NATIVE_V1_MAX_EXPANDED_EXPR_DYNAMIC_BYTES};
use crate::PhysicalEncodeError;
use arrow::datatypes::DataType;
use novarocks_constant_contract::{ConstantError, ConstantValue};
use novarocks_physical_plan::{ConstantReference, ValueType};
use novarocks_proto_models::common;
use novarocks_type_contract::{CompileCheckpoints, CompileControlError, CompilePhase};

pub(super) fn cost(
    reference: ConstantReference,
    expected: &ValueType,
    context: ExpressionEncodingContext<'_>,
) -> Result<(usize, usize), PhysicalEncodeError> {
    with_value(reference, expected, context, |value, work| {
        if value
            .is_null_observed(CompilePhase::Encode, work.control())
            .map_err(constant_error)?
        {
            return Ok((0, 0));
        }
        let (messages, bytes) = match &expected.data_type {
            DataType::Boolean => {
                required(value.try_boolean().map_err(constant_error)?)?;
                (0, 0)
            }
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                required(value.try_i64().map_err(constant_error)?)?;
                (0, 0)
            }
            DataType::Float32 => {
                exact_wire_f32(value, work)?;
                (0, 0)
            }
            DataType::Float64 => {
                required(value.try_f64_bits().map_err(constant_error)?)?;
                (0, 0)
            }
            DataType::FixedSizeBinary(16) => {
                required(value.try_largeint().map_err(constant_error)?)?;
                (0, 16)
            }
            DataType::Decimal128(_, _) => {
                required(value.try_decimal128().map_err(constant_error)?)?;
                (1, 16)
            }
            DataType::Decimal256(_, _) => {
                required(value.try_decimal256_be().map_err(constant_error)?)?;
                (3, 32)
            }
            DataType::Utf8 => (
                0,
                required(value.try_utf8().map_err(constant_error)?)?.len(),
            ),
            DataType::Binary | DataType::LargeBinary => (
                0,
                required(value.try_binary().map_err(constant_error)?)?.len(),
            ),
            DataType::Date32 => {
                required(value.try_date32().map_err(constant_error)?)?;
                (0, 0)
            }
            _ => {
                return Err("native wire v1 cannot preserve this non-NULL constant carrier".into());
            }
        };
        work.step()?;
        if bytes > NATIVE_V1_MAX_EXPANDED_EXPR_DYNAMIC_BYTES {
            return Err("native wire v1 selected constant exceeds expression payload limit".into());
        }
        Ok((messages, bytes))
    })
}

pub(super) fn encode(
    reference: ConstantReference,
    expected: &ValueType,
    context: ExpressionEncodingContext<'_>,
) -> Result<common::LiteralValue, PhysicalEncodeError> {
    with_value(reference, expected, context, |value, work| {
        use common::literal_value::Value;
        let payload = if value
            .is_null_observed(CompilePhase::Encode, work.control())
            .map_err(constant_error)?
        {
            Value::NullValue(true)
        } else {
            match &expected.data_type {
                DataType::Boolean => {
                    Value::BoolValue(required(value.try_boolean().map_err(constant_error)?)?)
                }
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                    Value::IntValue(required(value.try_i64().map_err(constant_error)?)?)
                }
                DataType::Float32 => Value::FloatValue(f64::from(exact_wire_f32(value, work)?)),
                // Prost's double codec copies to_bits/from_bits without numeric
                // conversion; the native Float64 literal also retains f64.
                DataType::Float64 => Value::FloatValue(f64::from_bits(required(
                    value.try_f64_bits().map_err(constant_error)?,
                )?)),
                DataType::FixedSizeBinary(16) => Value::LargeintValue(copy_bytes(
                    &required(value.try_largeint().map_err(constant_error)?)?.to_be_bytes(),
                    work,
                )?),
                DataType::Decimal128(precision, scale) => {
                    Value::DecimalValue(common::DecimalLiteral {
                        value: copy_bytes(
                            &required(value.try_decimal128().map_err(constant_error)?)?
                                .to_be_bytes(),
                            work,
                        )?,
                        precision: u32::from(*precision),
                        scale: i32::from(*scale),
                    })
                }
                DataType::Decimal256(precision, scale) => {
                    Value::DecimalValue(common::DecimalLiteral {
                        value: copy_bytes(
                            &required(value.try_decimal256_be().map_err(constant_error)?)?,
                            work,
                        )?,
                        precision: u32::from(*precision),
                        scale: i32::from(*scale),
                    })
                }
                DataType::Utf8 => Value::StringValue(copy_text(
                    required(value.try_utf8().map_err(constant_error)?)?,
                    work,
                )?),
                DataType::Binary | DataType::LargeBinary => Value::BinaryValue(copy_bytes(
                    required(value.try_binary().map_err(constant_error)?)?,
                    work,
                )?),
                DataType::Date32 => {
                    Value::Date32Value(required(value.try_date32().map_err(constant_error)?)?)
                }
                _ => {
                    return Err(
                        "native wire v1 cannot preserve this non-NULL constant carrier".into(),
                    );
                }
            }
        };
        work.step()?;
        Ok(common::LiteralValue {
            value: Some(payload),
        })
    })
}

pub(super) fn window_offset(
    reference: ConstantReference,
    expected: &ValueType,
    context: ExpressionEncodingContext<'_>,
) -> Result<i64, PhysicalEncodeError> {
    with_value(reference, expected, context, |value, work| {
        if expected.logical_type != novarocks_type_contract::ValueLogicalType::Physical
            || expected.data_type != DataType::Int64
        {
            return Err(
                "native wire v1 window bound must be a non-negative exact Int64 constant".into(),
            );
        }
        let offset = required(value.try_i64().map_err(constant_error)?)?;
        work.step()?;
        if offset < 0 {
            return Err("native wire v1 window bound must be non-negative".into());
        }
        Ok(offset)
    })
}

fn with_value<T>(
    reference: ConstantReference,
    expected: &ValueType,
    context: ExpressionEncodingContext<'_>,
    consume: impl FnOnce(&ConstantValue, &mut CompileCheckpoints<'_>) -> Result<T, PhysicalEncodeError>,
) -> Result<T, PhysicalEncodeError> {
    let mut work = CompileCheckpoints::try_new(context.control, CompilePhase::Encode)?;
    let result = (|| {
        let value = context
            .constants
            .resolve_observed(reference, expected, &mut work)
            .map_err(|error| match error {
                novarocks_physical_plan::ConstantReferenceError::Control(cause) => {
                    PhysicalEncodeError::Control(cause)
                }
                error => PhysicalEncodeError::Invalid(error.to_string()),
            })?;
        consume(&value, &mut work)
    })();
    if matches!(result, Err(PhysicalEncodeError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

fn exact_wire_f32(
    value: &ConstantValue,
    work: &mut CompileCheckpoints<'_>,
) -> Result<f32, PhysicalEncodeError> {
    let bits = required(value.try_f32_bits().map_err(constant_error)?)?;
    let nan = bits & 0x7f80_0000 == 0x7f80_0000 && bits & 0x007f_ffff != 0;
    work.step()?;
    if nan {
        return Err("native wire v1 cannot guarantee Float32 NaN payload preservation".into());
    }
    // Every other IEEE32 value widens exactly and narrows back exactly,
    // including both zero signs, subnormals and infinities.
    Ok(f32::from_bits(bits))
}

fn required<T>(value: Option<T>) -> Result<T, PhysicalEncodeError> {
    value.ok_or_else(|| "non-NULL selected constant lacks its exact carrier payload".into())
}
fn constant_error(error: ConstantError) -> PhysicalEncodeError {
    match error {
        ConstantError::Control(cause) => PhysicalEncodeError::Control(cause),
        ConstantError::Limit(_) => {
            PhysicalEncodeError::Control(CompileControlError::ResourceExhausted)
        }
        error => PhysicalEncodeError::Invalid(error.to_string()),
    }
}
fn copy_bytes(
    bytes: &[u8],
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<u8>, PhysicalEncodeError> {
    if bytes.len() > NATIVE_V1_MAX_EXPANDED_EXPR_DYNAMIC_BYTES {
        return Err("native wire v1 selected constant exceeds expression payload limit".into());
    }
    work.flush()?;
    let mut output = Vec::new();
    output
        .try_reserve_exact(bytes.len())
        .map_err(|_| PhysicalEncodeError::Control(CompileControlError::ResourceExhausted))?;
    work.flush()?;
    for chunk in bytes.chunks(1024) {
        output.extend_from_slice(chunk);
        work.step()?;
    }
    Ok(output)
}
fn copy_text(text: &str, work: &mut CompileCheckpoints<'_>) -> Result<String, PhysicalEncodeError> {
    if text.len() > NATIVE_V1_MAX_EXPANDED_EXPR_DYNAMIC_BYTES {
        return Err("native wire v1 selected constant exceeds expression payload limit".into());
    }
    work.flush()?;
    let mut output = String::new();
    output
        .try_reserve_exact(text.len())
        .map_err(|_| PhysicalEncodeError::Control(CompileControlError::ResourceExhausted))?;
    work.flush()?;
    let mut remaining = text;
    while !remaining.is_empty() {
        let mut end = remaining.len().min(1024);
        while !remaining.is_char_boundary(end) {
            end -= 1;
        }
        output.push_str(&remaining[..end]);
        work.step()?;
        remaining = &remaining[end..];
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, ArrayRef, Float32Array, Float64Array, Int64Array, StringArray,
        TimestampMicrosecondArray,
    };
    use arrow::datatypes::Field;
    use novarocks_constant_contract::{ConstantPolicy, ConstantPool};
    use novarocks_physical_plan::{ConstantPoolId, ConstantPools};
    use novarocks_type_contract::{FunctionValueType, PureCompileControl};
    use prost::Message;
    use std::sync::{Arc, Mutex};

    struct Control {
        reject: Option<(usize, CompileControlError)>,
        calls: Mutex<Vec<(CompilePhase, u32)>>,
    }
    impl Control {
        fn good() -> Self {
            Self {
                reject: None,
                calls: Mutex::new(Vec::new()),
            }
        }
        fn trace(&self) -> Vec<(CompilePhase, u32)> {
            self.calls.lock().unwrap().clone()
        }
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            let mut calls = self.calls.lock().unwrap();
            let at = calls.len();
            calls.push((phase, units));
            match self.reject {
                Some((index, cause)) if index == at => Err(cause),
                _ => Ok(()),
            }
        }
    }
    fn policy() -> ConstantPolicy {
        ConstantPolicy {
            max_rows: 1024,
            max_array_nodes: 4096,
            max_logical_elements: 100_000,
            max_retained_buffer_bytes: 8 * 1024 * 1024,
            max_type_depth: 64,
            max_type_nodes: 4096,
            max_dictionary_depth: 16,
            max_metadata_bytes: 1024 * 1024,
            max_library_validation_work: 16 * 1024 * 1024,
            max_library_validation_bytes: 16 * 1024 * 1024,
        }
    }
    fn source(array: ArrayRef, nullable: bool) -> (ConstantPools, FunctionValueType) {
        let ty = FunctionValueType::new(array.data_type().clone(), nullable);
        let p = ConstantPool::try_new(
            Arc::new(Field::new("source", array.data_type().clone(), nullable)),
            ty.clone(),
            array.to_data(),
            policy(),
            CompilePhase::Validate,
            &Control::good(),
        )
        .unwrap();
        let mut pools = ConstantPools::empty();
        pools.insert(ConstantPoolId::new(u32::MAX), p).unwrap();
        (pools, ty)
    }
    fn reference(ordinal: u32) -> ConstantReference {
        ConstantReference {
            pool: ConstantPoolId::new(u32::MAX),
            ordinal,
        }
    }
    fn matrix(
        pools: &ConstantPools,
        ty: &ValueType,
        reference: ConstantReference,
        ordinary: bool,
    ) -> Vec<(CompilePhase, u32)> {
        let baseline = Control::good();
        let result = encode(
            reference,
            ty,
            ExpressionEncodingContext {
                constants: pools,
                control: &baseline,
            },
        );
        if ordinary {
            assert!(matches!(result, Err(PhysicalEncodeError::Invalid(_))));
        } else {
            result.unwrap();
        }
        let trace = baseline.trace();
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let refusing = Control {
                    reject: Some((at, cause)),
                    calls: Mutex::new(Vec::new()),
                };
                assert!(
                    matches!(encode(reference,ty,ExpressionEncodingContext{constants:pools,control:&refusing}),Err(PhysicalEncodeError::Control(actual)) if actual==cause)
                );
                assert_eq!(refusing.trace(), trace[..=at]);
            }
        }
        trace
    }

    #[test]
    fn v1_projection_selects_nonzero_ordinal_and_preserves_raw_float64_wire_bits() {
        let (pools, ty) = source(Arc::new(Int64Array::from(vec![99, i64::MIN, 42])), false);
        let control = Control::good();
        let context = ExpressionEncodingContext {
            constants: &pools,
            control: &control,
        };
        assert_eq!(
            encode(reference(1), &ty, context).unwrap().value,
            Some(common::literal_value::Value::IntValue(i64::MIN))
        );
        for bits in [
            0x8000_0000_0000_0000,
            0x7ff0_0000_0000_0001,
            0xfff8_0000_0000_1234,
        ] {
            let (pools, ty) = source(
                Arc::new(Float64Array::from(vec![1.0, f64::from_bits(bits)])),
                false,
            );
            let wire = encode(
                reference(1),
                &ty,
                ExpressionEncodingContext {
                    constants: &pools,
                    control: &control,
                },
            )
            .unwrap();
            let bytes = wire.encode_to_vec();
            let decoded = common::LiteralValue::decode(bytes.as_slice()).unwrap();
            let Some(common::literal_value::Value::FloatValue(value)) = decoded.value else {
                panic!("expected exact Float64 arm")
            };
            assert_eq!(value.to_bits(), bits);
        }
        matrix(&pools, &ty, reference(1), false);
    }

    #[test]
    fn v1_projection_float32_exact_values_preserve_legacy_boundary_and_nan_is_refused() {
        for bits in [0u32, 0x8000_0000, 0x3f80_0000, 1, 0x7f80_0000, 0xff80_0000] {
            let (pools, ty) = source(
                Arc::new(Float32Array::from(vec![1.0, f32::from_bits(bits)])),
                false,
            );
            let control = Control::good();
            let context = ExpressionEncodingContext {
                constants: &pools,
                control: &control,
            };
            assert_eq!(cost(reference(1), &ty, context).unwrap(), (0, 0));
            let wire = encode(reference(1), &ty, context).unwrap();
            let decoded = common::LiteralValue::decode(wire.encode_to_vec().as_slice()).unwrap();
            let Some(common::literal_value::Value::FloatValue(value)) = decoded.value else {
                panic!("expected Float32 legacy double arm")
            };
            assert_eq!((value as f32).to_bits(), bits);
        }
        for bits in [0x7f80_0001, 0xffc0_1234] {
            let (pools, ty) = source(
                Arc::new(Float32Array::from(vec![f32::from_bits(bits)])),
                false,
            );
            let error = encode(
                reference(0),
                &ty,
                ExpressionEncodingContext {
                    constants: &pools,
                    control: &Control::good(),
                },
            )
            .unwrap_err();
            assert!(
                matches!(error, PhysicalEncodeError::Invalid(message) if message.contains("Float32 NaN payload"))
            );
            matrix(&pools, &ty, reference(0), true);
        }
    }

    #[test]
    fn v1_projection_cost_and_copy_ignore_unselected_pool_rows_and_observe_real_quantum() {
        let unused = "unused".repeat(60_000);
        let selected = "雪☃a".repeat(50_000);
        let (pools, ty) = source(
            Arc::new(StringArray::from(vec![unused.as_str(), selected.as_str()])),
            false,
        );
        let control = Control::good();
        let context = ExpressionEncodingContext {
            constants: &pools,
            control: &control,
        };
        assert_eq!(
            cost(reference(1), &ty, context).unwrap(),
            (0, selected.len())
        );
        let baseline = Control::good();
        let wire = encode(
            reference(1),
            &ty,
            ExpressionEncodingContext {
                constants: &pools,
                control: &baseline,
            },
        )
        .unwrap();
        assert_eq!(
            wire.value,
            Some(common::literal_value::Value::StringValue(selected))
        );
        let trace = baseline.trace();
        assert!(trace.iter().any(|(_, n)| *n == 256));
        let at = trace.iter().position(|(_, n)| *n == 256).unwrap();
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                reject: Some((at, cause)),
                calls: Mutex::new(Vec::new()),
            };
            assert!(
                matches!(encode(reference(1),&ty,ExpressionEncodingContext{constants:&pools,control:&control}),Err(PhysicalEncodeError::Control(actual)) if actual==cause)
            );
            assert_eq!(control.trace(), trace[..=at]);
        }
    }

    #[test]
    fn v1_projection_preserves_typed_null_but_refuses_unrepresentable_selected_payload() {
        let (pools, ty) = source(
            Arc::new(TimestampMicrosecondArray::from(vec![None, Some(1)])),
            true,
        );
        let control = Control::good();
        let context = ExpressionEncodingContext {
            constants: &pools,
            control: &control,
        };
        assert_eq!(
            encode(reference(0), &ty, context).unwrap().value,
            Some(common::literal_value::Value::NullValue(true))
        );
        assert_eq!(cost(reference(0), &ty, context).unwrap(), (0, 0));
        matrix(&pools, &ty, reference(0), false);
        matrix(&pools, &ty, reference(1), true);
        let wrong = FunctionValueType::new(DataType::Int64, true);
        matrix(&pools, &wrong, reference(0), true);
        matrix(&pools, &ty, reference(2), true);
        matrix(&ConstantPools::empty(), &ty, reference(0), true);
    }

    #[test]
    fn v1_projection_decimal_and_largeint_payloads_use_exact_selected_signed_be() {
        let control = Control::good();
        let large = FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            false,
            novarocks_type_contract::ValueLogicalType::LargeInt,
        )
        .unwrap();
        let d128 = FunctionValueType::new(DataType::Decimal128(38, -2), false);
        let d256 = FunctionValueType::new(DataType::Decimal256(76, 76), false);
        let values = [
            ConstantValue::from_largeint(
                Arc::new(large.try_to_field("source").unwrap()),
                large,
                i128::MIN,
                policy(),
                CompilePhase::Validate,
                &control,
            )
            .unwrap(),
            ConstantValue::from_decimal128(
                Arc::new(d128.try_to_field("source").unwrap()),
                d128,
                -1,
                policy(),
                CompilePhase::Validate,
                &control,
            )
            .unwrap(),
            ConstantValue::from_decimal256_be(
                Arc::new(d256.try_to_field("source").unwrap()),
                d256,
                [0xff; 32],
                policy(),
                CompilePhase::Validate,
                &control,
            )
            .unwrap(),
        ];
        for (index, value) in values.iter().enumerate() {
            let mut pools = ConstantPools::empty();
            pools
                .insert(ConstantPoolId::new(u32::MAX), value.pool().clone())
                .unwrap();
            let context = ExpressionEncodingContext {
                constants: &pools,
                control: &control,
            };
            let wire = encode(reference(0), value.value_type(), context).unwrap();
            match (index, wire.value) {
                (0, Some(common::literal_value::Value::LargeintValue(bytes))) => {
                    assert_eq!(bytes, i128::MIN.to_be_bytes())
                }
                (1, Some(common::literal_value::Value::DecimalValue(decimal))) => {
                    assert_eq!(decimal.value, (-1i128).to_be_bytes());
                    assert_eq!((decimal.precision, decimal.scale), (38, -2));
                }
                (2, Some(common::literal_value::Value::DecimalValue(decimal))) => {
                    assert_eq!(decimal.value, [0xff; 32]);
                    assert_eq!((decimal.precision, decimal.scale), (76, 76));
                }
                _ => panic!("incorrect direct signed payload arm"),
            }
            assert_eq!(
                cost(reference(0), value.value_type(), context).unwrap(),
                [(0, 16), (1, 16), (3, 32)][index]
            );
        }
        // The leaf can spell a true LARGEINT payload. The complete native-v1
        // encoder's existing logical-type admission remains a separate gate.
        let ordinary = FunctionValueType::new(DataType::FixedSizeBinary(16), false);
        let mut pools = ConstantPools::empty();
        pools
            .insert(ConstantPoolId::new(u32::MAX), values[0].pool().clone())
            .unwrap();
        matrix(&pools, &ordinary, reference(0), true);
    }

    #[test]
    fn v1_window_offsets_use_checked_full_source_and_original_control() {
        let (pools, ty) = source(
            Arc::new(Int64Array::from(vec![Some(-1), Some(42), None])),
            true,
        );
        let control = Control::good();
        let context = ExpressionEncodingContext {
            constants: &pools,
            control: &control,
        };
        assert_eq!(window_offset(reference(1), &ty, context).unwrap(), 42);
        assert!(matches!(
            window_offset(reference(0), &ty, context),
            Err(PhysicalEncodeError::Invalid(_))
        ));
        assert!(matches!(
            window_offset(reference(2), &ty, context),
            Err(PhysicalEncodeError::Invalid(_))
        ));
        let wrong = FunctionValueType::new(DataType::Int64, false);
        assert!(matches!(
            window_offset(reference(1), &wrong, context),
            Err(PhysicalEncodeError::Invalid(_))
        ));
    }
}
