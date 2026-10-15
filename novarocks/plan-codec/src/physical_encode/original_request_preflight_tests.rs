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
use novarocks_physical_plan::{
    ConstantPoolId, ConstantReference, PhysicalCallDefinition, PhysicalCallRequest,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, PureCompileControl, ValueLogicalType,
};
use std::sync::Mutex;

fn conversion_fixture(
    expected: bool,
) -> (
    EngineFunctionCatalog,
    novarocks_physical_plan::ConstantPools,
    Fragment,
    ExprId,
    novarocks_physical_plan::BoundFunction,
    novarocks_constant_contract::ConstantValue,
) {
    let source =
        FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
            .unwrap();
    let target = FunctionValueType::new(DataType::Utf8, false);
    let value = novarocks_constant_contract::ConstantValue::from_utf8(
        Arc::new(arrow::datatypes::Field::new(
            "json source",
            DataType::Utf8,
            false,
        )),
        source.clone(),
        "{\"a\":1}",
        codec_constant_policy(),
        CompilePhase::Encode,
        &CodecTestControl,
    )
    .unwrap();
    let mut constants = novarocks_physical_plan::ConstantPools::empty();
    let reference = ConstantReference {
        pool: ConstantPoolId::new(17),
        ordinal: value.ordinal(),
    };
    constants
        .insert(reference.pool, value.pool().clone())
        .unwrap();
    let mut catalog = EngineFunctionCatalogBuilder::new();
    catalog
        .register(
            novarocks_functions::builtin::value_conversion::value_conversion_definition().unwrap(),
        )
        .unwrap();
    let catalog = catalog.seal_bound().unwrap();
    let arguments = [FunctionArgument::Value {
        value_type: source.clone(),
        constant: Some(value.clone()),
    }];
    let resolved = catalog
        .resolve_bound_trusted(
            novarocks_functions::builtin::value_conversion::VALUE_CONVERSION_NAME,
            FunctionKind::Scalar,
            FunctionBindingRequest {
                arguments: &arguments,
                logical_argument_count: 1,
                expected_result_type: Some(&target),
            },
            &CodecTestControl,
        )
        .unwrap();
    let function = novarocks_physical_plan::BoundFunction {
        function_id: resolved.function_id,
        overload: resolved.selected.overload,
        kind: resolved.kind,
        argument_types: resolved.selected.argument_types,
        result_type: target.clone(),
        legacy_metadata: Some(novarocks_physical_plan::LegacyBindingMetadata {
            semantic_parameters: Box::default(),
            volatility: resolved.semantics.volatility,
            argument_evaluation: resolved.semantics.argument_evaluation,
            failure_behavior: resolved.semantics.failure_behavior,
            intrinsic_row_error: resolved.semantics.intrinsic_row_error,
        }),
    };
    let mut builder = FragmentBuilder::new(FragmentId::new(23));
    let node = builder.reserve_node_id().unwrap();
    let child = builder
        .add_expression(node, source.clone(), ExprKind::Constant(reference))
        .unwrap();
    let call = builder
        .add_expression(
            node,
            target.clone(),
            ExprKind::FunctionCall {
                function: function.clone(),
                args: Box::from([child]),
            },
        )
        .unwrap();
    let output = builder
        .add_value(
            target.clone(),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: properties(),
            output: OutputPort {
                node,
                columns: Box::from([output]),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([call])]),
            },
        })
        .unwrap();
    let fragment = builder
        .finish_definition(
            node,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
        )
        .unwrap()
        .with_call_requests_observed(
            vec![(
                PhysicalCallDefinition::Expression(call),
                PhysicalCallRequest {
                    arguments: Box::from([
                        novarocks_physical_plan::StaticFunctionArgument::Value {
                            value_type: source,
                            constant: Some(reference),
                        },
                    ]),
                    logical_argument_count: 1,
                    expected_result_type: expected.then_some(target),
                    constant_policy: codec_constant_policy(),
                },
            )],
            &CodecTestControl,
        )
        .unwrap();
    (catalog, constants, fragment, call, function, value)
}

#[test]
fn v1_preflight_preserves_original_nominal_target_and_constant_source() {
    let (catalog, constants, fragment, call, function, value) = conversion_fixture(true);
    let materialized = original_call_request(
        &constants,
        &fragment,
        PhysicalCallDefinition::Expression(call),
        &CodecTestControl,
    )
    .unwrap();
    assert_eq!(
        materialized
            .request()
            .expected_result_type
            .unwrap()
            .logical_type(),
        ValueLogicalType::Physical
    );
    assert_eq!(materialized.constant_policy(), codec_constant_policy());
    let FunctionArgument::Value {
        value_type,
        constant: Some(original),
    } = &materialized.request().arguments[0]
    else {
        panic!("original constant is required")
    };
    assert_eq!(value_type.logical_type(), ValueLogicalType::Json);
    assert!(Arc::ptr_eq(
        original.pool().field_ref(),
        value.pool().field_ref()
    ));
    assert!(Arc::ptr_eq(original.pool().array(), value.pool().array()));
    validate_scalar_binding(
        &catalog,
        &constants,
        &fragment,
        &function,
        call,
        &CodecTestControl,
    )
    .unwrap();
    // The selected Physical Utf8 result cannot manufacture the missing target.
    let (catalog, constants, fragment, call, function, _) = conversion_fixture(false);
    let failure = validate_scalar_binding(
        &catalog,
        &constants,
        &fragment,
        &function,
        call,
        &CodecTestControl,
    )
    .unwrap_err();
    assert!(
        failure.to_string().contains("explicit complete target"),
        "{failure}"
    );
    let missing = original_call_request(
        &constants,
        &fragment,
        PhysicalCallDefinition::Expression(ExprId::new(u32::MAX)),
        &CodecTestControl,
    )
    .unwrap_err();
    assert!(missing.to_string().contains("lacks original call request"));
}

#[derive(Default)]
struct TraceControl {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for TraceControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut trace = self.trace.lock().unwrap();
        let index = trace.len();
        trace.push((phase, units));
        match self.stop {
            Some((at, cause)) if at == index => Err(cause),
            _ => Ok(()),
        }
    }
}

#[test]
fn v1_original_request_preflight_keeps_all_first_control_prefixes_without_tail() {
    let (catalog, constants, fragment, call, function, _) = conversion_fixture(true);
    let baseline = TraceControl::default();
    validate_scalar_binding(&catalog, &constants, &fragment, &function, call, &baseline).unwrap();
    let trace = baseline.trace.into_inner().unwrap();
    assert!(trace.len() > 1);
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for index in [0, trace.len() / 2, trace.len() - 1] {
            let control = TraceControl {
                trace: Mutex::new(Vec::new()),
                stop: Some((index, cause)),
            };
            assert_eq!(
                validate_scalar_binding(&catalog, &constants, &fragment, &function, call, &control),
                Err(PhysicalEncodeError::Control(cause))
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=index]);
        }
    }
}
