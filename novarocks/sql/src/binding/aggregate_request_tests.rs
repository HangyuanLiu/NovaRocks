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
use arrow::datatypes::DataType;
use std::sync::Mutex;

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::FunctionSpecialization);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let ordinal = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(ordinal <= stop, "callback after original refusal");
        }
        trace.push(units);
        match self.refusal {
            Some((stop, cause)) if ordinal == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn source(certified: bool, arguments: usize) -> AggregateArgumentSource<TypedExpr, SortItem> {
    let expression = TypedExpr {
        kind: crate::analysis::ExprKind::ColumnRef {
            column_id: crate::column_id::ColumnId(9),
            qualifier: None,
            column: "input".into(),
        },
        value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
    };
    let binding = crate::functions::test_resolved_aggregate("count", &[DataType::Int64], false);
    if certified {
        AggregateArgumentSource::logical_update(vec![expression; arguments], vec![], binding)
    } else {
        AggregateArgumentSource::uncertified(vec![expression; arguments], vec![], binding)
    }
}

#[test]
fn count_static_request_keeps_original_selected_result_and_policy() {
    let source = source(true, 1);
    let policy = crate::constant::test_constant_policy();
    let captured = capture_aggregate_logical_request(&source, policy, &Control::default()).unwrap();
    assert!(std::ptr::eq(
        source.binding().resolved(),
        captured.binding().resolved()
    ));
    assert_eq!(captured.constant_policy(), policy);
    let request = captured.request();
    assert_eq!(request.logical_argument_count, 1);
    assert_eq!(request.arguments.len(), 1);
    let FunctionArgument::Value {
        value_type,
        constant,
    } = &request.arguments[0]
    else {
        panic!("actual column input");
    };
    assert_eq!(value_type, &source.arguments()[0].value_type);
    assert!(constant.is_none());
    let FunctionResultType::Scalar(result) = &source.binding().resolved().selected.result_type
    else {
        panic!("actual COUNT result");
    };
    assert!(std::ptr::eq(request.expected_result_type.unwrap(), result));
}

#[test]
fn capture_refuses_uncertified_same_signature_and_observes_every_control_prefix() {
    let policy = crate::constant::test_constant_policy();
    for (certified, arguments) in [(true, 1), (false, 1), (true, 0)] {
        let source = source(certified, arguments);
        let control = Control::default();
        let result = capture_aggregate_logical_request(&source, policy, &control);
        match (certified, arguments) {
            (true, 1) => assert!(result.is_ok()),
            (false, 1) => assert!(matches!(
                result,
                Err(AggregateRequestCaptureError::MissingLogicalSource)
            )),
            (true, 0) => assert!(matches!(
                result,
                Err(AggregateRequestCaptureError::InvalidSource(_))
            )),
            _ => unreachable!(),
        }
        let baseline = control.trace.into_inner().unwrap();
        if arguments != 1 || !certified {
            assert_eq!(baseline, vec![0, u32::from(certified)]);
        }
        for stop in 0..baseline.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Control {
                    refusal: Some((stop, cause)),
                    ..Control::default()
                };
                assert!(
                    matches!(capture_aggregate_logical_request(&source, policy, &control), Err(AggregateRequestCaptureError::Control(actual)) if actual == cause)
                );
                assert_eq!(control.trace.into_inner().unwrap(), baseline[..=stop]);
            }
        }
    }
}

#[test]
fn static_capture_argument_bound_precedes_request_allocation_and_shape_check() {
    let policy = crate::constant::test_constant_policy();
    // COUNT has one installed logical channel. This source intentionally has
    // invalid arity; the boundary checks do not claim a wide installed overload.
    let near = source(true, MAX_CALL_EFFECT_ARGUMENTS);
    let control = Control::default();
    assert!(matches!(
        capture_aggregate_logical_request(&near, policy, &control),
        Err(AggregateRequestCaptureError::InvalidSource(_))
    ));
    assert_eq!(control.trace.into_inner().unwrap(), vec![0, 1]);
    let over = source(true, MAX_CALL_EFFECT_ARGUMENTS + 1);
    let control = Control::default();
    assert!(matches!(
        capture_aggregate_logical_request(&over, policy, &control),
        Err(AggregateRequestCaptureError::Control(
            CompileControlError::ResourceExhausted
        ))
    ));
    assert_eq!(control.trace.into_inner().unwrap(), vec![0]);
}
