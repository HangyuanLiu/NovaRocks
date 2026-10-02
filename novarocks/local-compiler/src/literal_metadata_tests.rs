// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use arrow_schema::{DataType, TimeUnit};
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
use std::sync::Mutex;

struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl Control {
    fn good() -> Self {
        Self {
            trace: Mutex::new(vec![]),
            refusal: None,
        }
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut trace = self.trace.lock().unwrap();
        if let Some((at, _)) = self.refusal {
            assert!(
                trace.len() < at,
                "primary refusal must have no later callback"
            );
        }
        trace.push((phase, units));
        match self.refusal {
            Some((at, cause)) if trace.len() == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 1,
        max_array_nodes: 8,
        max_logical_elements: 8,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 8,
        max_type_nodes: 16,
        max_dictionary_depth: 4,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 1 << 20,
        max_library_validation_bytes: 1 << 20,
    }
}
#[test]
fn scalar_metadata_keeps_actual_payloads_instead_of_nonconstant_markers() {
    macro_rules! check {
        ($ty:expr, $factory:ident, $payload:expr, $source:expr, $expected:expr) => {{
            let ty = $ty;
            let value = ConstantValue::$factory(
                Arc::new(ty.try_to_field("literal").unwrap()),
                ty,
                $payload,
                policy(),
                CompilePhase::Validate,
                &Control::good(),
            )
            .unwrap();
            assert_eq!(
                checked_literal_metadata(&$source, &value, &Control::good()).unwrap(),
                $expected
            );
        }};
    }
    check!(
        FunctionValueType::new(DataType::UInt64, false),
        from_u64,
        u64::MAX,
        LiteralValue::UInt64(u64::MAX),
        FunctionLiteral::UInt64(u64::MAX)
    );
    check!(
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            false,
            ValueLogicalType::LargeInt
        )
        .unwrap(),
        from_largeint,
        i128::MIN,
        LiteralValue::LargeInt(i128::MIN),
        FunctionLiteral::LargeInt(i128::MIN)
    );
    check!(
        FunctionValueType::new(DataType::Decimal128(38, -2), false),
        from_decimal128,
        -123,
        LiteralValue::Decimal128(-123),
        FunctionLiteral::Decimal128(-123)
    );
    check!(
        FunctionValueType::new(DataType::Decimal256(76, 9), false),
        from_decimal256_be,
        [255; 32],
        LiteralValue::Decimal256([255; 32]),
        FunctionLiteral::Decimal256([255; 32])
    );
    check!(
        FunctionValueType::new(DataType::Date32, false),
        from_date32,
        -11,
        LiteralValue::Date32(-11),
        FunctionLiteral::Date32(-11)
    );
    check!(
        FunctionValueType::new(DataType::Time64(TimeUnit::Nanosecond), false),
        from_time64,
        -99,
        LiteralValue::Time64(-99),
        FunctionLiteral::Time64(-99)
    );
    check!(
        FunctionValueType::new(
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false
        ),
        from_timestamp,
        -99,
        LiteralValue::Timestamp(-99),
        FunctionLiteral::Timestamp(-99)
    );
    check!(
        FunctionValueType::new(
            DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano),
            false
        ),
        from_interval_month_day_nano,
        (i32::MIN, i32::MAX, i64::MIN),
        LiteralValue::IntervalMonthDayNano {
            months: i32::MIN,
            days: i32::MAX,
            nanoseconds: i64::MIN
        },
        FunctionLiteral::IntervalMonthDayNano {
            months: i32::MIN,
            days: i32::MAX,
            nanoseconds: i64::MIN
        }
    );
}
fn text_value(carrier: DataType, payload: &str) -> ConstantValue {
    let ty = FunctionValueType::new(carrier, false);
    ConstantValue::from_utf8(
        Arc::new(ty.try_to_field("literal").unwrap()),
        ty,
        payload,
        policy(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap()
}
#[test]
fn byte_metadata_compares_every_selected_byte_and_keeps_empty_constants() {
    for carrier in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        for text in [String::new(), "µ\0雪".repeat(100)] {
            let value = text_value(carrier.clone(), &text);
            let source = LiteralValue::Utf8(text.clone().into());
            assert_eq!(
                checked_literal_metadata(&source, &value, &Control::good()).unwrap(),
                FunctionLiteral::Utf8(text.clone().into())
            );
            let mut different = text;
            different.push('x');
            assert!(matches!(
                checked_literal_metadata(
                    &LiteralValue::Utf8(different.into()),
                    &value,
                    &Control::good()
                ),
                Err(ExpressionLoweringError::Invalid(_))
            ));
        }
    }
    let ty = FunctionValueType::new(DataType::BinaryView, false);
    let value = ConstantValue::from_binary(
        Arc::new(ty.try_to_field("literal").unwrap()),
        ty,
        &[0, 255, 128],
        policy(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap();
    assert_eq!(
        checked_literal_metadata(
            &LiteralValue::Binary(vec![0, 255, 128].into()),
            &value,
            &Control::good()
        )
        .unwrap(),
        FunctionLiteral::Binary(vec![0, 255, 128].into())
    );
    assert!(matches!(
        checked_literal_metadata(
            &LiteralValue::Binary(vec![0, 255, 127].into()),
            &value,
            &Control::good()
        ),
        Err(ExpressionLoweringError::Invalid(_))
    ));
}
#[test]
fn long_literal_metadata_refusal_has_one_original_callback_prefix() {
    let text = "ab\0µ雪".repeat(200);
    let value = text_value(DataType::Utf8View, &text);
    let source = LiteralValue::Utf8(text.into());
    let baseline = Control::good();
    checked_literal_metadata(&source, &value, &baseline).unwrap();
    let trace = baseline.trace.into_inner().unwrap();
    assert!(trace.iter().filter(|(_, units)| *units > 0).count() >= 4);
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 1..=trace.len() {
            let control = Control {
                trace: Mutex::new(vec![]),
                refusal: Some((at, cause)),
            };
            assert!(
                matches!(checked_literal_metadata(&source, &value, &control), Err(ExpressionLoweringError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..at]);
        }
    }
}
#[test]
fn completed_mismatch_tail_observes_control_before_returning_data_error() {
    let value = text_value(DataType::LargeUtf8, "abc");
    let source = LiteralValue::Utf8("abd".into());
    let baseline = Control::good();
    assert!(matches!(
        checked_literal_metadata(&source, &value, &baseline),
        Err(ExpressionLoweringError::Invalid(_))
    ));
    let trace = baseline.trace.into_inner().unwrap();
    assert!(trace.last().unwrap().1 > 0);
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let control = Control {
            trace: Mutex::new(vec![]),
            refusal: Some((trace.len(), cause)),
        };
        assert!(
            matches!(checked_literal_metadata(&source, &value, &control), Err(ExpressionLoweringError::Control(actual)) if actual == cause)
        );
        assert_eq!(*control.trace.lock().unwrap(), trace);
    }
}
