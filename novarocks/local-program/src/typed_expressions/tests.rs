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
use crate::{
    BindingRequirement, BindingRequirements, CompileProfile, ControlShape, ImmutableExpressions,
    KernelAbiVersion, LocalProgram, ProgramControlFlow, ProgramEvaluationDomain,
    ProgramExpressionRootSite, ProgramExpressionUse, ProgramNode, ProgramNodeExpressionRole,
    ProgramNodeId, ProgramNodeKind, ProgramRootControlBindings, ProgramRootUseBinding,
    StaticExprNode, StaticLayout, StaticLiteral, StaticSinkProgram, StaticStreamBranch,
    StaticValues,
};
use arrow_array::{BooleanArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use novarocks_execution_contract::DataStreamPartitionType;
use novarocks_type_contract::{EvaluationDomainId, ValueLogicalType};
use novarocks_types::SlotId;
use std::{collections::HashMap, num::NonZeroUsize};

#[derive(Default)]
struct Control {
    at: Option<u32>,
    failure: Option<CompileControlError>,
}

fn shared_truth_value_fixture(ty: DataType) -> ProgramResolvedCalls {
    let nodes = arena(vec![StaticExprNode::new(
        StaticExprKind::Literal(StaticLiteral::Null),
        ty.clone(),
        None,
    )]);
    let source_schema = Arc::new(Schema::new(vec![Field::new(
        "source",
        DataType::Boolean,
        false,
    )]));
    let source_layout =
        StaticLayout::try_new(source_schema.clone(), Arc::from([SlotId::new(1)])).unwrap();
    let values = StaticValues::try_new(
        RecordBatch::try_new(
            source_schema,
            vec![Arc::new(BooleanArray::from(vec![true]))],
        )
        .unwrap(),
        source_layout.clone(),
    )
    .unwrap();
    let output = StaticLayout::try_new(
        Arc::new(Schema::new(vec![Field::new("result", ty, true)])),
        Arc::from([SlotId::new(2)]),
    )
    .unwrap();
    let program = LocalProgram::try_new(
        vec![
            ProgramNode::new(0, ProgramNodeKind::Values { values }, source_layout.clone()),
            ProgramNode::new(
                1,
                ProgramNodeKind::Filter {
                    input: ProgramNodeId::new(0),
                    predicate: ProgramExprId::new(0),
                },
                source_layout,
            ),
            ProgramNode::new(
                2,
                ProgramNodeKind::Project {
                    input: ProgramNodeId::new(1),
                    is_subordinate: false,
                    exprs: vec![ProgramExprId::new(0)],
                    expr_slot_ids: vec![SlotId::new(2)],
                    expr_slot_schemas: None,
                    output_indices: None,
                },
                output.clone(),
            ),
        ],
        ProgramNodeId::new(2),
        nodes,
        CompileProfile::new(
            NonZeroUsize::new(1).unwrap(),
            None,
            output.identity().unwrap(),
            KernelAbiVersion::CURRENT,
        ),
        BindingRequirements::try_new(vec![]).unwrap(),
    )
    .unwrap();
    let flow = ProgramControlFlow::try_new(
        vec![ProgramEvaluationDomain {
            id: EvaluationDomainId::new(0),
            parent: None,
            guard: None,
        }],
        [EvaluationDemand::TruthOnly, EvaluationDemand::Value]
            .into_iter()
            .enumerate()
            .map(|(index, demand)| ProgramExpressionUse {
                context: novarocks_type_contract::ExpressionEffectContext {
                    use_id: novarocks_type_contract::ExpressionUseId::new(index as u32),
                    domain: EvaluationDomainId::new(0),
                    demand,
                },
                definition: ProgramExprId::new(0),
                control: ControlShape::Eager,
                arguments: Box::default(),
            })
            .collect(),
        1,
        &Control::default(),
    )
    .unwrap();
    let bindings = [
        ProgramNodeExpressionRole::FilterPredicate,
        ProgramNodeExpressionRole::ProjectOutput { expression: 0 },
    ]
    .into_iter()
    .enumerate()
    .map(|(index, role)| ProgramRootUseBinding {
        site: ProgramExpressionRootSite::Node {
            node: ProgramNodeId::new(index + 1),
            role,
        },
        use_id: novarocks_type_contract::ExpressionUseId::new(index as u32),
    })
    .collect();
    let snapshot = ProgramRootControlBindings::try_new(
        program,
        BTreeMap::from([(ProgramExpressionArena::Main, flow)]),
        bindings,
        &Control::default(),
    )
    .unwrap();
    ProgramResolvedCalls::try_new(snapshot, vec![], &Control::default()).unwrap()
}

#[test]
fn actual_truth_only_uses_require_boolean_without_erasing_shared_value_nullability() {
    let typed = ProgramTypedExpressions::try_new(
        shared_truth_value_fixture(DataType::Boolean),
        main_types(vec![value(DataType::Boolean, true)]),
        &Control::default(),
    )
    .unwrap();
    assert_eq!(
        typed.definition_type(ProgramExpressionArena::Main, ProgramExprId::new(0)),
        Some(&value(DataType::Boolean, true))
    );
    let uses = typed.resolved_calls().snapshot().flows()[&ProgramExpressionArena::Main].uses();
    assert_eq!(
        uses[&novarocks_type_contract::ExpressionUseId::new(0)]
            .context
            .demand,
        EvaluationDemand::TruthOnly
    );
    assert_eq!(
        uses[&novarocks_type_contract::ExpressionUseId::new(1)]
            .context
            .demand,
        EvaluationDemand::Value
    );
    for ty in [DataType::Utf8, DataType::Int64, DataType::Null] {
        assert_eq!(
            ProgramTypedExpressions::try_new(
                shared_truth_value_fixture(ty.clone()),
                main_types(vec![value(ty, true)]),
                &Control::default()
            )
            .unwrap_err(),
            ProgramExpressionTypeError::WrongDemand
        );
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::LowerProgram);
        assert!(units <= 256);
        if self.at == Some(units) {
            Err(self.failure.unwrap())
        } else {
            Ok(())
        }
    }
}
fn arena(nodes: Vec<StaticExprNode>) -> Arc<ImmutableExpressions> {
    Arc::new(ImmutableExpressions::try_new(nodes, false, HashMap::new(), None).unwrap())
}
fn bool_node() -> StaticExprNode {
    StaticExprNode::new(
        StaticExprKind::Literal(StaticLiteral::Bool(true)),
        DataType::Boolean,
        None,
    )
}
fn value(ty: DataType, nullable: bool) -> FunctionArgumentType {
    FunctionArgumentType::Value(FunctionValueType::new(ty, nullable))
}
fn empty_flow(count: usize) -> ProgramControlFlow {
    ProgramControlFlow::try_new(
        vec![ProgramEvaluationDomain {
            id: EvaluationDomainId::new(u32::MAX),
            parent: None,
            guard: None,
        }],
        vec![],
        count,
        &Control::default(),
    )
    .unwrap()
}
fn resolved(
    nodes: Vec<StaticExprNode>,
    sink_nodes: Option<Vec<StaticExprNode>>,
) -> ProgramResolvedCalls {
    let main = arena(nodes);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "source",
        DataType::Boolean,
        false,
    )]));
    let layout = StaticLayout::try_new(schema.clone(), Arc::from([SlotId::new(1)])).unwrap();
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(BooleanArray::from(vec![true]))]).unwrap();
    let values = StaticValues::try_new(batch, layout.clone()).unwrap();
    let profile = CompileProfile::new(
        NonZeroUsize::new(1).unwrap(),
        None,
        layout.identity().unwrap(),
        KernelAbiVersion::CURRENT,
    );
    let mut flows =
        BTreeMap::from([(ProgramExpressionArena::Main, empty_flow(main.nodes().len()))]);
    let (requirements, sink) = match sink_nodes {
        Some(nodes) => {
            let sink_arena = arena(nodes);
            flows.insert(
                ProgramExpressionArena::Sink,
                empty_flow(sink_arena.nodes().len()),
            );
            let branch = StaticStreamBranch::try_new(
                10,
                DataStreamPartitionType::Unpartitioned,
                vec![],
                vec![],
                None,
            )
            .unwrap();
            (
                vec![BindingRequirement::ExchangeOutput {
                    branch: 0,
                    layout: layout.clone(),
                }],
                Some(StaticSinkProgram::try_data_stream(branch, sink_arena).unwrap()),
            )
        }
        None => (vec![], None),
    };
    let program = LocalProgram::try_new_with_sink(
        vec![ProgramNode::new(
            0,
            ProgramNodeKind::Values { values },
            layout,
        )],
        ProgramNodeId::new(0),
        main,
        profile,
        BindingRequirements::try_new(requirements).unwrap(),
        sink,
    )
    .unwrap();
    let snapshot =
        ProgramRootControlBindings::try_new(program, flows, vec![], &Control::default()).unwrap();
    ProgramResolvedCalls::try_new(snapshot, vec![], &Control::default()).unwrap()
}
fn main_types(
    types: Vec<FunctionArgumentType>,
) -> BTreeMap<ProgramExpressionArena, Vec<FunctionArgumentType>> {
    BTreeMap::from([(ProgramExpressionArena::Main, types)])
}

#[test]
fn unused_definitions_still_have_explicit_full_logical_and_nullable_types() {
    let source = resolved(
        vec![StaticExprNode::new(
            StaticExprKind::Literal(StaticLiteral::Utf8("null".into())),
            DataType::Utf8,
            None,
        )],
        None,
    );
    let json = FunctionArgumentType::Value(
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap(),
    );
    let typed = ProgramTypedExpressions::try_new(
        source,
        main_types(vec![json.clone()]),
        &Control::default(),
    )
    .unwrap();
    assert_eq!(
        typed.definition_type(ProgramExpressionArena::Main, ProgramExprId::new(0)),
        Some(&json)
    );
    assert!(
        typed
            .definition_type(ProgramExpressionArena::Sink, ProgramExprId::new(0))
            .is_none()
    );
    assert!(
        typed
            .definition_type(ProgramExpressionArena::Main, ProgramExprId::new(1))
            .is_none()
    );
    assert!(typed.resolved_calls().calls().is_empty());
}

#[test]
fn missing_extra_arena_and_definition_positions_are_not_optional() {
    let source = resolved(vec![bool_node()], None);
    for entries in [
        BTreeMap::new(),
        main_types(vec![]),
        main_types(vec![value(DataType::Boolean, true); 2]),
        BTreeMap::from([(
            ProgramExpressionArena::Sink,
            vec![value(DataType::Boolean, true)],
        )]),
    ] {
        assert_eq!(
            ProgramTypedExpressions::try_new(source.clone(), entries, &Control::default())
                .unwrap_err(),
            ProgramExpressionTypeError::IncompleteCoverage
        );
    }
}

#[test]
fn lambda_parameter_shape_and_body_type_remain_explicit() {
    let source = resolved(
        vec![
            bool_node(),
            StaticExprNode::new(
                StaticExprKind::LambdaFunction {
                    body: ProgramExprId::new(0),
                    arg_slots: vec![SlotId::new(7)],
                    common_sub_exprs: vec![],
                    is_nondeterministic: false,
                },
                DataType::Boolean,
                None,
            ),
        ],
        None,
    );
    let lambda = FunctionArgumentType::Lambda {
        parameter_types: vec![FunctionValueType::new(DataType::Int64, false)].into_boxed_slice(),
        result_type: FunctionValueType::new(DataType::Boolean, true),
    };
    ProgramTypedExpressions::try_new(
        source.clone(),
        main_types(vec![value(DataType::Boolean, true), lambda.clone()]),
        &Control::default(),
    )
    .unwrap();
    assert_eq!(
        ProgramTypedExpressions::try_new(
            source.clone(),
            main_types(vec![value(DataType::Boolean, true); 2]),
            &Control::default()
        )
        .unwrap_err(),
        ProgramExpressionTypeError::WrongKind
    );
    let FunctionArgumentType::Lambda { result_type, .. } = lambda.clone() else {
        unreachable!()
    };
    let wrong = FunctionArgumentType::Lambda {
        parameter_types: Box::default(),
        result_type,
    };
    assert_eq!(
        ProgramTypedExpressions::try_new(
            source.clone(),
            main_types(vec![value(DataType::Boolean, true), wrong]),
            &Control::default()
        )
        .unwrap_err(),
        ProgramExpressionTypeError::WrongLambda
    );
    assert_eq!(
        ProgramTypedExpressions::try_new(
            source,
            main_types(vec![value(DataType::Boolean, false), lambda]),
            &Control::default()
        )
        .unwrap_err(),
        ProgramExpressionTypeError::TypeMismatch
    );
}

#[test]
fn exact_dictionary_field_identity_and_root_logical_validation_are_observed() {
    #[allow(deprecated)]
    let field = |id| {
        Arc::new(Field::new_dict(
            "dictionary",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            false,
            id,
            true,
        ))
    };
    let left = DataType::Struct(vec![field(1)].into());
    let right = DataType::Struct(vec![field(2)].into());
    assert_eq!(left, right); // Arrow logical equality is deliberately coarser.
    let source = resolved(
        vec![StaticExprNode::new(
            StaticExprKind::Literal(StaticLiteral::Null),
            left.clone(),
            None,
        )],
        None,
    );
    ProgramTypedExpressions::try_new(
        source.clone(),
        main_types(vec![value(left, true)]),
        &Control::default(),
    )
    .unwrap();
    assert_eq!(
        ProgramTypedExpressions::try_new(
            source,
            main_types(vec![value(right, true)]),
            &Control::default()
        )
        .unwrap_err(),
        ProgramExpressionTypeError::TypeMismatch
    );
    let wrong = FunctionArgumentType::Value(FunctionValueType {
        data_type: DataType::Boolean,
        nullable: true,
        logical_type: ValueLogicalType::Json,
    });
    assert!(matches!(
        ProgramTypedExpressions::try_new(
            resolved(vec![bool_node()], None),
            main_types(vec![wrong]),
            &Control::default()
        ),
        Err(ProgramExpressionTypeError::Kernel(
            KernelFailure::InvalidProgram(_)
        ))
    ));
}

#[test]
fn combined_definition_budget_counts_distinct_arenas_even_for_unused_constants() {
    for main_count in [
        MAX_STATIC_EXPRESSIONS - 2,
        MAX_STATIC_EXPRESSIONS - 1,
        MAX_STATIC_EXPRESSIONS,
    ] {
        let source = resolved(vec![bool_node(); main_count], Some(vec![bool_node()]));
        let types = BTreeMap::from([
            (
                ProgramExpressionArena::Main,
                vec![value(DataType::Boolean, false); main_count],
            ),
            (
                ProgramExpressionArena::Sink,
                vec![value(DataType::Boolean, false)],
            ),
        ]);
        let result = ProgramTypedExpressions::try_new(source, types, &Control::default());
        if main_count == MAX_STATIC_EXPRESSIONS {
            assert_eq!(
                result.unwrap_err(),
                ProgramExpressionTypeError::TooManyDefinitions
            );
        } else {
            result.unwrap();
        }
    }
}

#[test]
fn signature_type_work_uses_one_scope_and_preserves_all_three_control_failures() {
    let source = resolved(vec![bool_node(); 300], None);
    for failure in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in [0, 256] {
            assert_eq!(
                ProgramTypedExpressions::try_new(
                    source.clone(),
                    main_types(vec![value(DataType::Boolean, false); 300]),
                    &Control {
                        at: Some(at),
                        failure: Some(failure)
                    }
                )
                .unwrap_err(),
                ProgramExpressionTypeError::Control(failure)
            );
        }
    }
}

fn cv_policy() -> novarocks_functions::ConstantPolicy {
    novarocks_functions::ConstantPolicy {
        max_rows: 1_000_000,
        max_array_nodes: 4096,
        max_logical_elements: 16_000_000,
        max_retained_buffer_bytes: 64 * 1024 * 1024,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 64,
        max_metadata_bytes: 1024 * 1024,
        max_library_validation_work: 1024 * 1024 * 1024,
        max_library_validation_bytes: 64 * 1024 * 1024,
    }
}
fn cv(
    array: arrow_array::ArrayRef,
    ty: FunctionValueType,
    ordinal: u32,
) -> novarocks_functions::ConstantValue {
    novarocks_functions::ConstantPool::try_new(
        Arc::new(ty.try_to_field("selected").unwrap()),
        ty,
        array.to_data(),
        cv_policy(),
        CompilePhase::LowerProgram,
        &Control::default(),
    )
    .unwrap()
    .value(ordinal)
    .unwrap()
}
fn cv_node(value: novarocks_functions::ConstantValue) -> StaticExprNode {
    let ty = value.value_type().data_type.clone();
    StaticExprNode::new(StaticExprKind::Constant(value), ty, None)
}

#[test]
fn shared_constants_keep_selected_rows_float_bits_and_exact_complete_types() {
    use arrow_array::{Array, Float32Array, StringArray, StructArray};
    let floats = [0x8000_0000u32, 0x7fc0_0042];
    let ty = FunctionValueType::new(DataType::Float32, false);
    let source = cv(
        Arc::new(Float32Array::from(floats.map(f32::from_bits).to_vec())),
        ty.clone(),
        1,
    );
    let typed = ProgramTypedExpressions::try_new(
        resolved(vec![cv_node(source.clone())], None),
        main_types(vec![FunctionArgumentType::Value(ty)]),
        &Control::default(),
    )
    .unwrap();
    let arena = &typed.resolved_calls().snapshot().roots().arenas()[&ProgramExpressionArena::Main];
    let StaticExprKind::Constant(stored) = arena.nodes()[0].kind() else {
        panic!("missing checked constant")
    };
    assert_eq!(stored.ordinal(), 1);
    assert!(Arc::ptr_eq(stored.pool().array(), source.pool().array()));
    assert_eq!(
        stored
            .pool()
            .array()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(stored.ordinal() as usize)
            .to_bits(),
        floats[1]
    );

    let child = Arc::new(
        Field::new("json", DataType::Utf8, true).with_metadata(HashMap::from([
            (
                novarocks_type_contract::NR_LOGICAL_TYPE_KEY.to_owned(),
                "json".to_owned(),
            ),
            ("provider".to_owned(), "source".to_owned()),
        ])),
    );
    let array = Arc::new(StructArray::new(
        vec![child.clone()].into(),
        vec![Arc::new(StringArray::from(vec![Some("null"), Some("{}")]))],
        None,
    ));
    let exact = FunctionValueType::new(array.data_type().clone(), false);
    let nested = cv(array, exact.clone(), 1);
    ProgramTypedExpressions::try_new(
        resolved(vec![cv_node(nested.clone())], None),
        main_types(vec![FunctionArgumentType::Value(exact)]),
        &Control::default(),
    )
    .unwrap();
    let wrong = FunctionValueType::new(
        DataType::Struct(
            vec![Arc::new(child.as_ref().clone().with_metadata(
                HashMap::from([
                    (
                        novarocks_type_contract::NR_LOGICAL_TYPE_KEY.to_owned(),
                        "json".to_owned(),
                    ),
                    ("provider".to_owned(), "different".to_owned()),
                ]),
            ))]
            .into(),
        ),
        false,
    );
    assert_eq!(
        ProgramTypedExpressions::try_new(
            resolved(vec![cv_node(nested)], None),
            main_types(vec![FunctionArgumentType::Value(wrong)]),
            &Control::default(),
        )
        .unwrap_err(),
        ProgramExpressionTypeError::TypeMismatch
    );
}

#[test]
fn shared_constants_reject_logical_nullable_and_carrier_retagging() {
    use arrow_array::{Float32Array, StringArray};
    let json_type =
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap();
    let json = cv(
        Arc::new(StringArray::from(vec![None::<&str>, Some("null")])),
        json_type.clone(),
        0,
    );
    assert!(
        json.is_null_observed(CompilePhase::LowerProgram, &Control::default())
            .unwrap()
    );
    ProgramTypedExpressions::try_new(
        resolved(vec![cv_node(json.clone())], None),
        main_types(vec![FunctionArgumentType::Value(json_type.clone())]),
        &Control::default(),
    )
    .unwrap();
    for wrong in [
        FunctionValueType::new(DataType::Utf8, true),
        FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
            .unwrap(),
    ] {
        assert_eq!(
            ProgramTypedExpressions::try_new(
                resolved(vec![cv_node(json.clone())], None),
                main_types(vec![FunctionArgumentType::Value(wrong)]),
                &Control::default(),
            )
            .unwrap_err(),
            ProgramExpressionTypeError::TypeMismatch
        );
    }
    let float = cv(
        Arc::new(Float32Array::from(vec![1.0])),
        FunctionValueType::new(DataType::Float32, false),
        0,
    );
    assert_eq!(
        ImmutableExpressions::try_new(
            vec![StaticExprNode::new(
                StaticExprKind::Constant(float),
                DataType::Float64,
                None
            )],
            false,
            HashMap::new(),
            None,
        )
        .unwrap_err(),
        crate::StaticExpressionError::ConstantTypeMismatch
    );
}

#[test]
fn shared_constant_backing_is_bounded_as_retained_storage_and_deduplicated() {
    use arrow_array::BinaryArray;
    let payload = vec![7u8; 9 * 1024 * 1024];
    let make = || {
        cv(
            Arc::new(BinaryArray::from(vec![payload.as_slice()])),
            FunctionValueType::new(DataType::Binary, false),
            0,
        )
    };
    let same = make();
    arena(vec![
        cv_node(same.clone()),
        cv_node(same.clone()),
        cv_node(same),
    ]);
    assert_eq!(
        ImmutableExpressions::try_new(
            vec![cv_node(make()), cv_node(make())],
            false,
            HashMap::new(),
            None,
        )
        .unwrap_err(),
        crate::StaticExpressionError::TooManyBytes
    );
}

#[test]
fn shared_constant_definition_control_never_becomes_a_partial_typed_program() {
    let constant = cv(
        Arc::new(BooleanArray::from(vec![true])),
        FunctionValueType::new(DataType::Boolean, false),
        0,
    );
    let source = resolved(vec![cv_node(constant); 320], None);
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in [0, 256] {
            assert_eq!(
                ProgramTypedExpressions::try_new(
                    source.clone(),
                    main_types(vec![value(DataType::Boolean, false); 320]),
                    &Control {
                        at: Some(at),
                        failure: Some(error)
                    },
                )
                .unwrap_err(),
                ProgramExpressionTypeError::Control(error)
            );
        }
    }
}
