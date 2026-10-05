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
use crate::{CallRequestError, PhysicalCallDefinition, PhysicalCallRequest};
use novarocks_constant_contract::ConstantPolicy;
use novarocks_function_contract::FunctionArgument;
use novarocks_type_contract::{CompilePhase, PureCompileControl};
use std::sync::Mutex;

const PHASE: CompilePhase = CompilePhase::Validate;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, PHASE);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        if let Some((at, _)) = self.refusal {
            assert!(trace.len() < at, "callback after original refusal");
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
        max_rows: 16,
        max_array_nodes: 32,
        max_logical_elements: 128,
        max_retained_buffer_bytes: 65536,
        max_type_depth: 16,
        max_type_nodes: 128,
        max_dictionary_depth: 8,
        max_metadata_bytes: 4096,
        max_library_validation_work: 1000000,
        max_library_validation_bytes: 1048576,
    }
}
fn integer() -> ValueType {
    ValueType::new(DataType::Int64, true)
}
fn request(arguments: Vec<FunctionArgument<crate::ConstantReference>>) -> PhysicalCallRequest {
    PhysicalCallRequest {
        logical_argument_count: arguments.len(),
        arguments: arguments.into_boxed_slice(),
        expected_result_type: None,
        constant_policy: policy(),
    }
}
fn mixed_request() -> PhysicalCallRequest {
    let mut request = request(vec![
        FunctionArgument::Value {
            value_type: integer(),
            constant: Some(crate::ConstantReference {
                pool: crate::ConstantPoolId::new(u32::MAX),
                ordinal: 1,
            }),
        },
        FunctionArgument::Lambda {
            parameter_types: Box::from([integer(), integer()]),
            result_type: integer(),
        },
    ]);
    request.expected_result_type = Some(integer());
    request
}
fn finish(
    operation: impl FnOnce(&mut CompileCheckpoints<'_>) -> Result<(), CallRequestError>,
    control: &dyn PureCompileControl,
) -> Result<(), CallRequestError> {
    let mut work = CompileCheckpoints::try_new(control, PHASE)?;
    let result = operation(&mut work);
    if matches!(result, Err(CallRequestError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn source(
    requests: &[(PhysicalCallDefinition, PhysicalCallRequest)],
    capacity: usize,
    control: &dyn PureCompileControl,
) -> Result<(), CallRequestError> {
    finish(
        |work| validate_call_request_source_observed(requests, capacity, work),
        control,
    )
}
fn checked_empty_requests(count: usize) -> crate::Fragment {
    use novarocks_type_contract::{
        FunctionArgumentEvaluation, FunctionFailureBehavior, FunctionId, FunctionIntrinsicRowError,
        FunctionKind, FunctionOverloadId, FunctionVolatility,
    };
    let mut builder = crate::FragmentBuilder::new(crate::FragmentId::new(9));
    let source = crate::NodeId::new(99);
    let owner = crate::NodeId::new(0);
    builder
        .add_values(source, Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut requests = Vec::with_capacity(count);
    let mut assignments = Vec::with_capacity(count);
    let mut outputs = Vec::with_capacity(count);
    for _ in 0..count {
        let expression = builder
            .add_expression(
                owner,
                integer(),
                ExprKind::FunctionCall {
                    function: BoundFunction {
                        function_id: FunctionId::try_new("test/resource-request").unwrap(),
                        overload: FunctionOverloadId::try_new("test/resource-request/exact")
                            .unwrap(),
                        kind: FunctionKind::Scalar,
                        argument_types: Box::default(),
                        result_type: integer(),
                        volatility: FunctionVolatility::Immutable,
                        argument_evaluation: FunctionArgumentEvaluation::Eager,
                        failure_behavior: FunctionFailureBehavior::Propagate,
                        intrinsic_row_error: FunctionIntrinsicRowError::NoRowError,
                        semantic_parameters: Box::default(),
                    },
                    args: Box::default(),
                },
            )
            .unwrap();
        let output = builder
            .add_value(
                integer(),
                crate::ValueOrigin::Expr {
                    node: owner,
                    expr: expression,
                },
            )
            .unwrap();
        assignments.push((expression, output));
        outputs.push(output);
        requests.push((
            PhysicalCallDefinition::Expression(expression),
            request(Vec::new()),
        ));
    }
    builder
        .add_project(
            owner,
            source,
            assignments.into_boxed_slice(),
            outputs.into_boxed_slice(),
        )
        .unwrap();
    builder
        .finish_definition(
            owner,
            crate::FragmentSink::Noop,
            crate::PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
        )
        .unwrap()
        .with_call_requests_observed(requests, &Control::default())
        .unwrap()
}
fn prefixes(
    operation: impl Fn(&dyn PureCompileControl) -> Result<(), CallRequestError>,
    expected: Result<(), CallRequestError>,
) {
    let baseline = Control::default();
    assert_eq!(operation(&baseline), expected);
    let trace = baseline.trace.into_inner().unwrap();
    assert_eq!(trace.first(), Some(&(PHASE, 0)));
    for at in 1..=trace.len() {
        for cause in CAUSES {
            let control = Control {
                trace: Mutex::default(),
                refusal: Some((at, cause)),
            };
            assert_eq!(operation(&control), Err(CallRequestError::Control(cause)));
            assert_eq!(*control.trace.lock().unwrap(), trace[..at]);
        }
    }
}

#[test]
fn request_owned_arguments_and_lambda_types_preserve_the_sole_resource_numerics() {
    let request = mixed_request();
    let mut expected =
        ResourceUsage::limited(MAX_FRAGMENT_DYNAMIC_ITEMS, MAX_FRAGMENT_DYNAMIC_BYTES);
    let mut errors = ValidationContext::new();
    add_call_request_usage(&request, &mut expected, &mut errors, &mut |_| {
        Ok::<(), std::convert::Infallible>(())
    })
    .unwrap();
    assert!(errors.is_empty());
    assert_eq!(expected.items, 9);
    assert_eq!(
        expected.bytes,
        2 * std::mem::size_of::<FunctionArgument<crate::ConstantReference>>()
            + 2 * std::mem::size_of::<ValueType>()
    );
    let mut actual = ResourceUsage::limited(MAX_FRAGMENT_DYNAMIC_ITEMS, MAX_FRAGMENT_DYNAMIC_BYTES);
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, PHASE).unwrap();
    add_call_request_usage(&request, &mut actual, &mut errors, &mut |opaque| {
        if opaque { work.flush() } else { work.step() }
    })
    .unwrap();
    work.finish().unwrap();
    assert_eq!(
        (actual.items, actual.bytes),
        (expected.items, expected.bytes)
    );
    assert_eq!(
        control
            .trace
            .lock()
            .unwrap()
            .iter()
            .map(|(_, units)| usize::try_from(*units).unwrap())
            .sum::<usize>(),
        9
    );
}

#[test]
fn request_source_capacity_and_owned_extents_refuse_before_the_unadmitted_walk() {
    let requests = vec![(
        PhysicalCallDefinition::Expression(crate::ExprId::new(0)),
        mixed_request(),
    )];
    let control = Control::default();
    assert_eq!(
        source(&requests, usize::MAX, &control),
        Err(CallRequestError::Control(
            CompileControlError::ResourceExhausted
        ))
    );
    assert_eq!(*control.trace.lock().unwrap(), vec![(PHASE, 0)]);
    let mut usage = ResourceUsage::limited(1, MAX_FRAGMENT_DYNAMIC_BYTES);
    let mut errors = ValidationContext::new();
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, PHASE).unwrap();
    add_call_request_usage(&requests[0].1, &mut usage, &mut errors, &mut |opaque| {
        if opaque { work.flush() } else { work.step() }
    })
    .unwrap();
    assert!(usage.exhausted());
    assert_eq!(*control.trace.lock().unwrap(), vec![(PHASE, 0)]);
    assert!(errors.is_empty());
    // The real source Vec capacity coexists with destination headers. No type
    // or backing scratch is reached after its numerical primary refusal.
}

#[test]
fn request_resource_success_and_ordinary_type_error_preserve_every_actual_control_prefix() {
    let requests = vec![(
        PhysicalCallDefinition::Expression(crate::ExprId::new(u32::MAX)),
        mixed_request(),
    )];
    prefixes(
        |control| source(&requests, requests.capacity(), control),
        Ok(()),
    );
    let bad = vec![(
        PhysicalCallDefinition::Expression(crate::ExprId::new(0)),
        request(vec![FunctionArgument::Value {
            value_type: ValueType::new(DataType::FixedSizeBinary(-1), true),
            constant: None,
        }]),
    )];
    let baseline = Control::default();
    let expected = source(&bad, bad.capacity(), &baseline);
    assert!(matches!(expected, Err(CallRequestError::Structure(_))));
    prefixes(|control| source(&bad, bad.capacity(), control), expected);
    let fragment = checked_empty_requests(2);
    prefixes(
        |control| {
            finish(
                |work| validate_call_request_table_observed(fragment.call_requests(), work),
                control,
            )
        },
        Ok(()),
    );
}

#[test]
fn request_table_and_source_wide_owned_loops_have_real_quantum_without_a_type_internal_claim() {
    let fragment = checked_empty_requests(320);
    let requests: Vec<_> = fragment
        .call_requests()
        .entries()
        .iter()
        .map(|(key, request)| (*key, request.clone()))
        .collect();
    for table in [false, true] {
        let operation = |control: &dyn PureCompileControl| {
            if table {
                finish(
                    |work| validate_call_request_table_observed(fragment.call_requests(), work),
                    control,
                )
            } else {
                source(&requests, requests.capacity(), control)
            }
        };
        let baseline = Control::default();
        operation(&baseline).unwrap();
        let trace = baseline.trace.into_inner().unwrap();
        assert_eq!(trace, vec![(PHASE, 0), (PHASE, 256), (PHASE, 64)]);
        for cause in CAUSES {
            let control = Control {
                trace: Mutex::default(),
                refusal: Some((2, cause)),
            };
            assert_eq!(operation(&control), Err(CallRequestError::Control(cause)));
            assert_eq!(*control.trace.lock().unwrap(), trace[..2]);
        }
    }
    let mut usage = ResourceUsage::limited(MAX_FRAGMENT_DYNAMIC_ITEMS, MAX_FRAGMENT_DYNAMIC_BYTES);
    let mut errors = ValidationContext::new();
    add_call_request_table_usage(fragment.call_requests(), &mut usage, &mut errors);
    assert!(errors.is_empty());
    assert_eq!(usage.items, 320);
    assert_eq!(
        usage.bytes,
        320 * std::mem::size_of::<(PhysicalCallDefinition, PhysicalCallRequest)>()
    );
}
