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
use crate::physical_aggregate_binding_v2;
use crate::physical_binding_v2::{self, BindingCodecError, BindingProjectionLimits};
use crate::physical_call_requests_v2::{self, CallRequestCodecError, CallRequestProjectionLimits};
use crate::physical_connector_payload_v2::{
    self, ConnectorPayloadCodecError, ConnectorPayloadProjectionLimits,
};
use crate::physical_constant_v2::{
    self, ConstantNamespaceProjectionLimits, ConstantWriteProjectionLimits,
    PhysicalConstantCodecError,
};
use crate::physical_expression_v2::{self, ExpressionCodecError, ExpressionProjectionLimits};
use crate::physical_node_v2::{NodeCodecError, NodeProjectionFacts, NodeProjectionLimits};
use crate::physical_package_v2::{
    binding_sources::{self, BindingSourceLimits},
    type_views::{TypeViewBudget, TypeViewFacts, TypeViewLimits, collect_package_type_views_in},
};
use crate::physical_properties_v2::PhysicalPropertyProjectionLimits;
use crate::physical_result_v2;
use crate::physical_type_v2::{self, PackageTypeProjectionLimits, TypeCodecError};
use crate::physical_value_origin_v2::ValueOriginProjectionLimits;
use crate::physical_value_v2::{self, ValueCodecError, ValueProjectionLimits};
use arrow::{
    array::{Array, Int64Array},
    datatypes::{DataType, Field},
};
use novarocks_connector_contract as c;
use novarocks_physical_plan as p;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::*;
use std::{
    cell::Cell,
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
const SOURCE: usize = 128 * 1024 * 1024;
const REQUEST_BYTES: usize = 512 * 1024 * 1024;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    stop: Mutex<Option<(usize, CompileControlError)>>,
    trace: Mutex<Vec<u32>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Encode);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = *self.stop.lock().unwrap() {
            assert!(at <= stop, "callback after refusal");
        }
        trace.push(units);
        if let Some((stop, cause)) = *self.stop.lock().unwrap()
            && stop == at
        {
            return Err(cause);
        }
        Ok(())
    }
}
struct Setup;
impl PureCompileControl for Setup {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
fn int() -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, false)
}
fn dictionary() -> FunctionValueType {
    FunctionValueType::new(
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        true,
    )
}
fn properties() -> p::PhysicalProperties {
    p::PhysicalProperties {
        distribution: p::Distribution::Singleton,
        row_multiplicity: p::RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}
fn function(kind: FunctionKind, args: Box<[FunctionArgumentType]>) -> p::BoundFunction {
    p::BoundFunction::from_exact_signature(
        FunctionId::try_new("fixture/binding-source").unwrap(),
        FunctionOverloadId::try_new("fixture/selected-v1").unwrap(),
        kind,
        args,
        int(),
    )
}
fn request(args: Box<[p::StaticFunctionArgument<p::ConstantReference>]>) -> p::PhysicalCallRequest {
    p::PhysicalCallRequest {
        logical_argument_count: args.len(),
        arguments: args,
        expected_result_type: None,
        constant_policy: p::ConstantPolicy {
            max_rows: 16,
            max_array_nodes: 32,
            max_logical_elements: 128,
            max_retained_buffer_bytes: 65536,
            max_type_depth: 16,
            max_type_nodes: 128,
            max_dictionary_depth: 8,
            max_metadata_bytes: 4096,
            max_library_validation_work: 1_000_000,
            max_library_validation_bytes: 1_000_000,
        },
    }
}
fn context(id: u32, domain: u32) -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(id),
        domain: EvaluationDomainId::new(domain),
        demand: EvaluationDemand::Value,
    }
}
fn effects(kind: FunctionKind, ctx: ExpressionEffectContext) -> CallEffects {
    CallEffects {
        value_stability: FunctionVolatility::Immutable,
        own_row_error: if kind == FunctionKind::Aggregate {
            FunctionIntrinsicRowError::NotRowEvaluated
        } else {
            FunctionIntrinsicRowError::NoRowError
        },
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: FunctionNullBehavior::CalledOnNull,
        argument_control: match kind {
            FunctionKind::Scalar => ArgumentControl::HigherOrder {
                body_ordinal: 0,
                body_demand: EvaluationDemand::Value,
            },
            FunctionKind::Aggregate => ArgumentControl::Aggregate,
            FunctionKind::Table => ArgumentControl::Table,
            FunctionKind::Window => unreachable!(),
        },
        instance_state: match kind {
            FunctionKind::Scalar => FunctionInstanceState::None,
            FunctionKind::Aggregate => FunctionInstanceState::AggregateInstance,
            FunctionKind::Table => FunctionInstanceState::TableInstance,
            FunctionKind::Window => unreachable!(),
        },
        observable_effects: ObservableEffects::NONE,
        environment: Box::default(),
        proof_scope: CallProofScope::Domain(ctx.domain),
    }
}
fn node(
    id: p::NodeId,
    inputs: Box<[p::NodeId]>,
    columns: Box<[p::ValueId]>,
    kind: p::NodeKind,
) -> p::PhysicalNode {
    p::PhysicalNode {
        id,
        required_inputs: vec![properties(); inputs.len()].into_boxed_slice(),
        inputs,
        output_properties: properties(),
        output: p::OutputPort { node: id, columns },
        kind,
    }
}

// Original sparse construction and every mandatory publication author are
// exercised. Exact signatures and fixture effect claims certify no installed
// function or executable implementation.
fn rich_package() -> p::FragmentPackage {
    let source = p::NodeId::new(0);
    let agg = p::NodeId::new(7);
    let table = p::NodeId::new(u32::MAX);
    let scalar = p::ExprId::new(0);
    let lambda = p::ExprId::new(1);
    let body = p::ExprId::new(u32::MAX);
    let expressions = p::ExprArena::try_from_definitions_observed(
        vec![
            p::ExprNode {
                id: scalar,
                owner: source,
                lambda_scope: None,
                ty: int(),
                kind: p::ExprKind::FunctionCall {
                    function: function(
                        FunctionKind::Scalar,
                        Box::from([FunctionArgumentType::Lambda {
                            parameter_types: Box::from([dictionary(), int()]),
                            result_type: int(),
                        }]),
                    ),
                    args: Box::from([lambda]),
                },
            },
            p::ExprNode {
                id: lambda,
                owner: source,
                lambda_scope: None,
                ty: int(),
                kind: p::ExprKind::Lambda {
                    parameter_types: Box::from([dictionary(), int()]),
                    body,
                },
            },
            p::ExprNode {
                id: body,
                owner: source,
                lambda_scope: Some(lambda),
                ty: int(),
                kind: p::ExprKind::Literal(p::LiteralValue::Int64(42)),
            },
        ]
        .into_iter(),
        &p::PlanLimits::FROZEN,
        &Setup,
    )
    .unwrap();
    let source_value = p::ValueId::new(0);
    let a = p::ValueId::new(1);
    let b = p::ValueId::new(u32::MAX);
    let t0 = p::ValueId::new(2);
    let t1 = p::ValueId::new(3);
    let calls = [0, u32::MAX]
        .into_iter()
        .zip([a, b])
        .map(|(id, output)| p::AggregateCall {
            id: p::AggregateCallId::new(id),
            binding: p::AggregateBinding {
                state_argument_contract: AggregateStateArgumentContract::ExactSignature,
                function: function(FunctionKind::Aggregate, Box::default()),
                phase: p::AggregatePhase::Single,
                logical_argument_count: 0,
                intermediate_type: FunctionValueType::new(DataType::Binary, false),
                state_format: AggregateStateFormatId::try_new("fixture/state-v1").unwrap(),
            },
            arguments: Box::default(),
            distinct: false,
            order_by: Box::default(),
            output,
        })
        .collect::<Box<[_]>>();
    let values = [
        p::ValueDef {
            id: source_value,
            ty: int(),
            origin: p::ValueOrigin::NodeOutput {
                node: source,
                output_ordinal: 0,
            },
        },
        p::ValueDef {
            id: a,
            ty: int(),
            origin: p::ValueOrigin::AggregateResult {
                call: p::AggregateCallId::new(0),
            },
        },
        p::ValueDef {
            id: b,
            ty: int(),
            origin: p::ValueOrigin::AggregateResult {
                call: p::AggregateCallId::new(u32::MAX),
            },
        },
        p::ValueDef {
            id: t0,
            ty: int(),
            origin: p::ValueOrigin::NodeOutput {
                node: table,
                output_ordinal: 2,
            },
        },
        p::ValueDef {
            id: t1,
            ty: dictionary(),
            origin: p::ValueOrigin::NodeOutput {
                node: table,
                output_ordinal: 3,
            },
        },
    ]
    .into_iter()
    .map(|v| (v.id, v))
    .collect();
    let nodes = [
        node(
            source,
            Box::default(),
            Box::from([source_value]),
            p::NodeKind::Values {
                rows: Box::from([Box::from([scalar]), Box::from([scalar])]),
            },
        ),
        node(
            agg,
            Box::from([source]),
            Box::from([a, b]),
            p::NodeKind::Aggregate {
                group_by: Box::default(),
                calls,
                grouping: p::AggregateGrouping::Complete,
            },
        ),
        node(
            table,
            Box::from([agg]),
            Box::from([a, b, t0, t1]),
            p::NodeKind::TableFunction {
                function: p::BoundTableFunction::from_exact_signature(
                    FunctionId::try_new("fixture/table-source").unwrap(),
                    FunctionOverloadId::try_new("fixture/table-v1").unwrap(),
                    Box::default(),
                    Box::from([int(), dictionary()]),
                ),
                arguments: Box::default(),
                outputs: Box::from([
                    p::TableFunctionOutput::PassThrough(a),
                    p::TableFunctionOutput::PassThrough(b),
                    p::TableFunctionOutput::FunctionResult {
                        result_ordinal: 0,
                        value: t0,
                    },
                    p::TableFunctionOutput::FunctionResult {
                        result_ordinal: 1,
                        value: t1,
                    },
                ]),
                left_outer: false,
            },
        ),
    ]
    .into_iter()
    .map(|v| (v.id, v))
    .collect();
    let fragment = p::Fragment::try_from_structure_observed(
        p::FragmentStructureInput {
            id: p::FragmentId::new(u32::MAX),
            root: table,
            values,
            expressions,
            nodes,
            sink: p::FragmentSink::Result,
            dop_domain: p::PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            runtime_filters: Box::default(),
        },
        p::PlanLimits::FROZEN,
        &Setup,
    )
    .unwrap();
    let fragment = fragment
        .with_call_requests_observed(
            vec![
                (
                    p::PhysicalCallDefinition::Expression(scalar),
                    request(Box::from([p::StaticFunctionArgument::Lambda {
                        parameter_types: Box::from([dictionary(), int()]),
                        result_type: int(),
                    }])),
                ),
                (
                    p::PhysicalCallDefinition::Relational(p::PhysicalCallSite::Aggregate {
                        node: agg,
                        call: 0,
                    }),
                    request(Box::default()),
                ),
                (
                    p::PhysicalCallDefinition::Relational(p::PhysicalCallSite::Aggregate {
                        node: agg,
                        call: 1,
                    }),
                    request(Box::default()),
                ),
                (
                    p::PhysicalCallDefinition::Relational(p::PhysicalCallSite::Table {
                        node: table,
                    }),
                    request(Box::default()),
                ),
            ],
            &Setup,
        )
        .unwrap();
    // Two actual rows invoke one definition separately; each lambda has the
    // guarded body domain required by the original HigherOrder author.
    let mut invocations = Vec::new();
    let mut domains = vec![ExpressionEvaluationDomain {
        id: EvaluationDomainId::new(0),
        parent: None,
        guard: None,
    }];
    for row in 0..2 {
        let base = row * 3;
        let domain = row + 1;
        domains.push(ExpressionEvaluationDomain {
            id: EvaluationDomainId::new(domain),
            parent: Some(EvaluationDomainId::new(0)),
            guard: Some(DomainGuard {
                owner: ExpressionUseId::new(base),
                kind: GuardKind::LambdaInvocation,
            }),
        });
        invocations.extend([
            ExpressionInvocation {
                context: context(base, 0),
                definition: scalar,
                control: ControlShape::HigherOrder {
                    body_ordinal: 0,
                    body_demand: EvaluationDemand::Value,
                },
                arguments: Box::from([ExpressionUseId::new(base + 1)]),
            },
            ExpressionInvocation {
                context: context(base + 1, domain),
                definition: lambda,
                control: ControlShape::LambdaBody,
                arguments: Box::from([ExpressionUseId::new(base + 2)]),
            },
            ExpressionInvocation {
                context: context(base + 2, domain),
                definition: body,
                control: ControlShape::Eager,
                arguments: Box::default(),
            },
        ]);
    }
    let flow = ExpressionControlFlow::try_new(
        domains,
        invocations,
        fragment.expressions(),
        CompilePhase::Validate,
        &Setup,
    )
    .unwrap();
    let uses = p::PhysicalRootUses::try_new(
        &fragment,
        flow,
        (0..2)
            .map(|row| {
                (
                    p::ExpressionRootSite {
                        node: source,
                        role: p::ExpressionRootRole::ValuesCell { row, column: 0 },
                    },
                    ExpressionUseId::new(row * 3),
                )
            })
            .collect(),
        &Setup,
    )
    .unwrap();
    let mut claims = [0, 3]
        .into_iter()
        .map(|id| {
            let ctx = context(id, 0);
            p::FrozenPhysicalCall {
                site: p::PhysicalCallSite::Expression(ctx.use_id),
                context: ctx,
                effects: effects(FunctionKind::Scalar, ctx),
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            }
        })
        .collect::<Vec<_>>();
    for (use_id, site, kind) in [
        (
            6,
            p::PhysicalCallSite::Aggregate { node: agg, call: 0 },
            FunctionKind::Aggregate,
        ),
        (
            7,
            p::PhysicalCallSite::Aggregate { node: agg, call: 1 },
            FunctionKind::Aggregate,
        ),
        (
            u32::MAX,
            p::PhysicalCallSite::Table { node: table },
            FunctionKind::Table,
        ),
    ] {
        let ctx = context(use_id, 0);
        claims.push(p::FrozenPhysicalCall {
            site,
            context: ctx,
            effects: effects(kind, ctx),
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        });
    }
    let claims = p::FrozenFragmentCalls::try_new(&fragment, &uses, claims, &Setup).unwrap();
    p::FragmentPackage::try_new(
        p::FragmentPackageInput {
            constants: p::ConstantPools::empty(),
            version: p::PlanVersionId::try_new([3; 16]).unwrap(),
            required: p::RequiredContracts {
                plan_contract_revision: p::PLAN_CONTRACT_REVISION,
            },
            pruning: p::FrozenFragmentPruning::try_new(fragment.id(), vec![], &Setup).unwrap(),
            expression_uses: uses,
            calls: claims,
            cuts: p::FragmentCuts {
                inbound: Box::default(),
                outbound: Box::default(),
                runtime_filters: Box::default(),
            },
            result: Some(p::ResultPort {
                fragment: fragment.id(),
                output: fragment.nodes()[&fragment.root()].output.clone(),
                fields: fragment.nodes()[&fragment.root()]
                    .output
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(i, value)| p::ResultField {
                        name: format!("out{i}\0").into(),
                        alias: Some(format!("alias{i}").into()),
                        value: *value,
                        ty: fragment.values()[value].ty.clone(),
                    })
                    .collect(),
            }),
            fragment,
            parameters: SemanticParameters::try_new([]).unwrap(),
            scans: BTreeMap::new(),
            writes: BTreeMap::new(),
            annotations: Box::default(),
        },
        p::FragmentPackageAdmission {
            plan_limits: p::PlanLimits::FROZEN,
            source_retained_bytes: SOURCE,
            property_projection_limits: p::PropertyProofProjectionLimits {
                max_request_bytes: 64 * 1024 * 1024,
                max_coexisting_bytes: 512 * 1024 * 1024,
                max_projection_work: 64 * 1024 * 1024,
            },
        },
        &Setup,
    )
    .unwrap()
}

fn cv_package() -> p::FragmentPackage {
    let source = p::NodeId::new(u32::MAX);
    let pool_id = p::ConstantPoolId::new(u32::MAX);
    let reference = p::ConstantReference {
        pool: pool_id,
        ordinal: 1,
    };
    let nullable = FunctionValueType::new(DataType::Int64, true);
    let make_function = || {
        p::BoundFunction::from_exact_signature(
            FunctionId::try_new("fixture/original-cv").unwrap(),
            FunctionOverloadId::try_new("fixture/value-v1").unwrap(),
            FunctionKind::Scalar,
            Box::from([FunctionArgumentType::Value(nullable.clone())]),
            int(),
        )
    };
    let none = p::ExprId::new(0);
    let constant = p::ExprId::new(1);
    let some = p::ExprId::new(u32::MAX);
    let arena = p::ExprArena::try_from_definitions_observed(
        vec![
            p::ExprNode {
                id: none,
                owner: source,
                lambda_scope: None,
                ty: int(),
                kind: p::ExprKind::FunctionCall {
                    function: make_function(),
                    args: Box::from([constant]),
                },
            },
            p::ExprNode {
                id: constant,
                owner: source,
                lambda_scope: None,
                ty: nullable.clone(),
                kind: p::ExprKind::Constant(reference),
            },
            p::ExprNode {
                id: some,
                owner: source,
                lambda_scope: None,
                ty: int(),
                kind: p::ExprKind::FunctionCall {
                    function: make_function(),
                    args: Box::from([constant]),
                },
            },
        ]
        .into_iter(),
        &p::PlanLimits::FROZEN,
        &Setup,
    )
    .unwrap();
    let values = [p::ValueId::new(0), p::ValueId::new(u32::MAX)];
    let fragment = p::Fragment::try_from_structure_observed(
        p::FragmentStructureInput {
            id: p::FragmentId::new(9),
            root: source,
            values: values
                .iter()
                .enumerate()
                .map(|(ordinal, id)| {
                    (
                        *id,
                        p::ValueDef {
                            id: *id,
                            ty: int(),
                            origin: p::ValueOrigin::NodeOutput {
                                node: source,
                                output_ordinal: ordinal as u32,
                            },
                        },
                    )
                })
                .collect(),
            expressions: arena,
            nodes: BTreeMap::from([(
                source,
                node(
                    source,
                    Box::default(),
                    Box::from(values),
                    p::NodeKind::Values {
                        rows: Box::from([Box::from([none, some])]),
                    },
                ),
            )]),
            sink: p::FragmentSink::Result,
            dop_domain: p::PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            runtime_filters: Box::default(),
        },
        p::PlanLimits::FROZEN,
        &Setup,
    )
    .unwrap();
    let mut some_request = request(Box::from([p::StaticFunctionArgument::Value {
        ty: nullable.clone(),
        constant: Some(reference),
    }]));
    some_request.expected_result_type = Some(int());
    let fragment = fragment
        .with_call_requests_observed(
            vec![
                (
                    p::PhysicalCallDefinition::Expression(none),
                    request(Box::from([p::StaticFunctionArgument::Value {
                        ty: nullable.clone(),
                        constant: None,
                    }])),
                ),
                (p::PhysicalCallDefinition::Expression(some), some_request),
            ],
            &Setup,
        )
        .unwrap();
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: EvaluationDomainId::new(0),
            parent: None,
            guard: None,
        }],
        vec![
            ExpressionInvocation {
                context: context(0, 0),
                definition: none,
                control: ControlShape::Eager,
                arguments: Box::from([ExpressionUseId::new(1)]),
            },
            ExpressionInvocation {
                context: context(1, 0),
                definition: constant,
                control: ControlShape::Eager,
                arguments: Box::default(),
            },
            ExpressionInvocation {
                context: context(2, 0),
                definition: some,
                control: ControlShape::Eager,
                arguments: Box::from([ExpressionUseId::new(3)]),
            },
            ExpressionInvocation {
                context: context(3, 0),
                definition: constant,
                control: ControlShape::Eager,
                arguments: Box::default(),
            },
        ],
        fragment.expressions(),
        CompilePhase::Validate,
        &Setup,
    )
    .unwrap();
    let uses = p::PhysicalRootUses::try_new(
        &fragment,
        flow,
        vec![
            (
                p::ExpressionRootSite {
                    node: source,
                    role: p::ExpressionRootRole::ValuesCell { row: 0, column: 0 },
                },
                ExpressionUseId::new(0),
            ),
            (
                p::ExpressionRootSite {
                    node: source,
                    role: p::ExpressionRootRole::ValuesCell { row: 0, column: 1 },
                },
                ExpressionUseId::new(2),
            ),
        ],
        &Setup,
    )
    .unwrap();
    let claims = p::FrozenFragmentCalls::try_new(
        &fragment,
        &uses,
        [0, 2]
            .into_iter()
            .map(|id| {
                let ctx = context(id, 0);
                let mut effect = effects(FunctionKind::Scalar, ctx);
                effect.argument_control = ArgumentControl::Eager;
                p::FrozenPhysicalCall {
                    site: p::PhysicalCallSite::Expression(ctx.use_id),
                    context: ctx,
                    effects: effect,
                    decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                }
            })
            .collect(),
        &Setup,
    )
    .unwrap();
    let mut pools = p::ConstantPools::empty();
    let policy = request(Box::default()).constant_policy;
    let pool = novarocks_constant_contract::ConstantPool::try_new(
        Arc::new(Field::new("original\0pool", DataType::Int64, true)),
        nullable,
        Int64Array::from(vec![Some(42), None]).to_data(),
        policy,
    )
    .unwrap();
    pools.insert(pool_id, pool).unwrap();
    p::FragmentPackage::try_new(
        p::FragmentPackageInput {
            version: p::PlanVersionId::try_new([4; 16]).unwrap(),
            required: p::RequiredContracts {
                plan_contract_revision: p::PLAN_CONTRACT_REVISION,
            },
            result: Some(p::ResultPort {
                fragment: fragment.id(),
                output: fragment.nodes()[&source].output.clone(),
                fields: values
                    .iter()
                    .enumerate()
                    .map(|(i, value)| p::ResultField {
                        name: format!("original{i}\0").into(),
                        alias: None,
                        value: *value,
                        ty: int(),
                    })
                    .collect(),
            }),
            cuts: p::FragmentCuts {
                inbound: Box::default(),
                outbound: Box::default(),
                runtime_filters: Box::default(),
            },
            pruning: p::FrozenFragmentPruning::try_new(fragment.id(), vec![], &Setup).unwrap(),
            fragment,
            expression_uses: uses,
            calls: claims,
            constants: pools,
            parameters: SemanticParameters::try_new([]).unwrap(),
            scans: BTreeMap::new(),
            writes: BTreeMap::new(),
            annotations: Box::default(),
        },
        p::FragmentPackageAdmission {
            plan_limits: p::PlanLimits::FROZEN,
            source_retained_bytes: SOURCE,
            property_projection_limits: p::PropertyProofProjectionLimits {
                max_request_bytes: REQUEST_BYTES,
                max_coexisting_bytes: SOURCE + REQUEST_BYTES,
                max_projection_work: usize::MAX / 4,
            },
        },
        &Setup,
    )
    .unwrap()
}
fn writer_package() -> p::FragmentPackage {
    let provider = c::ConnectorProviderId::parse("iceberg").unwrap();
    let instance = c::ConnectorInstanceId::try_from_canonical("lake").unwrap();
    let catalog = c::CatalogHandle::new(instance.clone(), c::CatalogVersion::from_bytes([7; 32]));
    let recipe = c::ConnectorWriteRecipeDraft::try_new(
        c::ConnectorWriteBinding::new(
            c::ConnectorInstanceDescriptor {
                provider_id: provider.clone(),
                instance_id: instance,
            },
            catalog.clone(),
        ),
        c::ConnectorEncodedPayload::new(
            c::ConnectorEnvelopeHeader::new(
                provider,
                catalog,
                c::ConnectorCodecCategory::WriteHandle,
                c::ConnectorCodecRevision::try_new(1).unwrap(),
            ),
            vec![7].into(),
        ),
        c::ConnectorWriteInputShape::Data {
            fields: vec![c::ConnectorWriteFieldBinding::new(
                c::ConnectorWriteFieldToken::from_bytes([1; 32]),
                Field::new("v", DataType::Int64, false),
            )],
        },
    )
    .unwrap();
    crate::physical_type_v2::sender_tests::checked_writer_package(recipe)
}
