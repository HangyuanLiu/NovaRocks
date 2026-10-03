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
fn source(ty: FunctionValueType, kind: ExprKind) -> ExprNode {
    ExprNode {
        id: ExprId::new(17),
        owner: novarocks_physical_plan::NodeId::new(7),
        lambda_scope: None,
        ty,
        kind,
    }
}
fn project(
    value: &ConstantValue,
    source: &ExprNode,
    control: &dyn PureCompileControl,
) -> Result<Option<ConstantValue>, ExpressionLoweringError> {
    let node = StaticExprNode::new(
        StaticExprKind::Constant(value.clone()),
        value.value_type().data_type.clone(),
        None,
    );
    literal_argument(source, &node, control)
}
fn assert_shared(actual: &ConstantValue, original: &ConstantValue) {
    assert_eq!(actual.ordinal(), original.ordinal());
    assert_eq!(actual.value_type(), original.value_type());
    assert!(Arc::ptr_eq(
        actual.pool().field_ref(),
        original.pool().field_ref()
    ));
    assert!(Arc::ptr_eq(actual.pool().array(), original.pool().array()));
}
#[test]
fn scalar_metadata_keeps_actual_payloads_instead_of_nonconstant_markers() {
    macro_rules! check {
        ($ty:expr, $factory:ident, $payload:expr, $source:expr, $getter:ident, $expected:expr) => {{
            let ty = $ty;
            let value = ConstantValue::$factory(
                Arc::new(ty.try_to_field("literal").unwrap()),
                ty.clone(),
                $payload,
                policy(),
                CompilePhase::Validate,
                &Control::good(),
            )
            .unwrap();
            let actual = project(
                &value,
                &source(ty, ExprKind::Literal($source)),
                &Control::good(),
            )
            .unwrap()
            .unwrap();
            assert_shared(&actual, &value);
            assert_eq!(actual.$getter().unwrap(), Some($expected));
        }};
    }
    check!(
        FunctionValueType::new(DataType::UInt64, false),
        from_u64,
        u64::MAX,
        LiteralValue::UInt64(u64::MAX),
        try_u64,
        u64::MAX
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
        try_largeint,
        i128::MIN
    );
    check!(
        FunctionValueType::new(DataType::Decimal128(38, -2), false),
        from_decimal128,
        -123,
        LiteralValue::Decimal128(-123),
        try_decimal128,
        -123
    );
    check!(
        FunctionValueType::new(DataType::Decimal256(76, 9), false),
        from_decimal256_be,
        [255; 32],
        LiteralValue::Decimal256([255; 32]),
        try_decimal256_be,
        [255; 32]
    );
    check!(
        FunctionValueType::new(DataType::Date32, false),
        from_date32,
        -11,
        LiteralValue::Date32(-11),
        try_date32,
        -11
    );
    check!(
        FunctionValueType::new(DataType::Time64(TimeUnit::Nanosecond), false),
        from_time64,
        -99,
        LiteralValue::Time64(-99),
        try_time64,
        -99
    );
    check!(
        FunctionValueType::new(
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false
        ),
        from_timestamp,
        -99,
        LiteralValue::Timestamp(-99),
        try_timestamp,
        -99
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
        try_interval_month_day_nano,
        (i32::MIN, i32::MAX, i64::MIN)
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
    // Binding retains the admitted CV directly; byte validation belongs to its owner.
    for carrier in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        for text in [String::new(), "µ\0雪".repeat(100)] {
            let value = text_value(carrier.clone(), &text);
            let actual = project(
                &value,
                &source(
                    value.value_type().clone(),
                    ExprKind::Literal(LiteralValue::Utf8(text.clone().into())),
                ),
                &Control::good(),
            )
            .unwrap()
            .unwrap();
            assert_shared(&actual, &value);
            assert_eq!(actual.try_utf8().unwrap(), Some(text.as_str()));
        }
    }
    let ty = FunctionValueType::new(DataType::BinaryView, false);
    let value = ConstantValue::from_binary(
        Arc::new(ty.try_to_field("literal").unwrap()),
        ty.clone(),
        &[0, 255, 128],
        policy(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap();
    let actual = project(
        &value,
        &source(
            ty,
            ExprKind::Literal(LiteralValue::Binary(vec![0, 255, 128].into())),
        ),
        &Control::good(),
    )
    .unwrap()
    .unwrap();
    assert_shared(&actual, &value);
    assert_eq!(actual.try_binary().unwrap(), Some(&[0, 255, 128][..]));
}
#[test]
fn literal_metadata_preserves_nonzero_pool_ordinal_and_null_vs_nonconstant() {
    use arrow_array::{Array, Int64Array};
    let ty = FunctionValueType::new(DataType::Int64, true);
    let field = Arc::new(ty.try_to_field("source").unwrap());
    let mut pool_policy = policy();
    pool_policy.max_rows = 3;
    let pool = novarocks_functions::ConstantPool::try_new(
        field.clone(),
        ty.clone(),
        Int64Array::from(vec![Some(99), Some(42), None]).to_data(),
        pool_policy,
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap();
    let value = pool.value(1).unwrap();
    let actual = project(
        &value,
        &source(ty.clone(), ExprKind::Literal(LiteralValue::Int64(42))),
        &Control::good(),
    )
    .unwrap()
    .unwrap();
    assert_shared(&actual, &value);
    assert_eq!(actual.try_i64().unwrap(), Some(42));
    let null = pool.value(2).unwrap();
    let actual = project(
        &null,
        &source(ty.clone(), ExprKind::Literal(LiteralValue::Null)),
        &Control::good(),
    )
    .unwrap()
    .unwrap();
    assert_shared(&actual, &null);
    assert_eq!(actual.try_i64().unwrap(), None);
    let node = StaticExprNode::new(
        StaticExprKind::SlotId(novarocks_types::SlotId::new(3)),
        DataType::Int64,
        None,
    );
    assert!(
        literal_argument(
            &source(
                ty,
                ExprKind::Value(novarocks_physical_plan::ValueId::new(91))
            ),
            &node,
            &Control::good()
        )
        .unwrap()
        .is_none()
    );
}
#[test]
fn long_literal_metadata_refusal_has_one_original_callback_prefix() {
    // Full nested metadata, not payload reconstruction, drives the observed traversal.
    let mut metadata = std::collections::HashMap::new();
    for n in 0..320 {
        metadata.insert(format!("key{n}"), format!("value{n}"));
    }
    let carrier = DataType::Struct(
        vec![Arc::new(
            arrow_schema::Field::new("child", DataType::Int64, true).with_metadata(metadata),
        )]
        .into(),
    );
    let ty = FunctionValueType::new(carrier, true);
    let mut p = policy();
    p.max_type_depth = 64;
    p.max_type_nodes = 4096;
    let value = ConstantValue::null(
        Arc::new(ty.try_to_field("literal").unwrap()),
        ty.clone(),
        p,
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap();
    let source = source(ty, ExprKind::Literal(LiteralValue::Null));
    let baseline = Control::good();
    project(&value, &source, &baseline).unwrap();
    let trace = baseline.trace.into_inner().unwrap();
    assert!(trace.iter().any(|(_, units)| *units == 256));
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        // Wide input samples each real positive quantum plus the entry and tail.
        for at in
            (1..=trace.len()).filter(|at| *at == 1 || *at == trace.len() || trace[*at - 1].1 == 256)
        {
            let control = Control {
                trace: Mutex::new(vec![]),
                refusal: Some((at, cause)),
            };
            assert!(
                matches!(project(&value,&source,&control),Err(ExpressionLoweringError::Control(actual)) if actual==cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..at]);
        }
    }
}
#[test]
fn completed_mismatch_tail_observes_control_before_returning_data_error() {
    let value = text_value(DataType::LargeUtf8, "abc");
    let mut ty = value.value_type().clone();
    ty.nullable = true;
    let source = source(ty, ExprKind::Literal(LiteralValue::Utf8("abc".into())));
    let baseline = Control::good();
    assert!(matches!(
        project(&value, &source, &baseline),
        Err(ExpressionLoweringError::Invalid(_))
    ));
    let trace = baseline.trace.into_inner().unwrap();
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
                matches!(project(&value,&source,&control),Err(ExpressionLoweringError::Control(actual)) if actual==cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..at]);
        }
    }
}
