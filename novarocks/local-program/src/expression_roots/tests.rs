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
    AggregateTopNFilter, BindingRequirement, BindingRequirements, CompileProfile, FilterNullOrder,
    FilterNullSemantics, FilterOrderKey, FilterProducerAtExpr, FilterProducerKind, FilterReduction,
    FilterSortDirection, JoinDistributionMode, KernelAbiVersion, LocalProgramError, ProgramNode,
    StaticExprKind, StaticExprNode, StaticFilterContract, StaticFilterProducer, StaticLayout,
    StaticLiteral, StaticSinkProgram, StaticStreamBranch, StaticValues, StaticWriterProjection,
    WriterFinalAggregatePlan, WriterGroupedUnpivotMapping, WriterGroupedUnpivotPlan,
};
use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use novarocks_connector_contract::WriteTargetOrdinal;
use novarocks_execution_contract::DataStreamPartitionType;
use novarocks_types::SlotId;
use std::{
    collections::HashMap,
    num::{NonZeroU32, NonZeroUsize},
    sync::Mutex,
};

#[derive(Default)]
struct Control {
    failure: Option<CompileControlError>,
    positive: bool,
    work: Mutex<Vec<u32>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, work: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::LowerProgram);
        assert!(work <= 256);
        self.work.lock().unwrap().push(work);
        if !self.positive || work > 0 {
            self.failure.map_or(Ok(()), Err)
        } else {
            Ok(())
        }
    }
}
fn arena(nodes: Vec<StaticExprNode>) -> Arc<ImmutableExpressions> {
    Arc::new(ImmutableExpressions::try_new(nodes, false, HashMap::new(), None).unwrap())
}
fn integer_arena(value: i64) -> Arc<ImmutableExpressions> {
    arena(vec![StaticExprNode::new(
        StaticExprKind::Literal(StaticLiteral::Int64(value)),
        DataType::Int64,
        None,
    )])
}
fn boolean_arena() -> Arc<ImmutableExpressions> {
    arena(vec![StaticExprNode::new(
        StaticExprKind::Literal(StaticLiteral::Bool(true)),
        DataType::Boolean,
        None,
    )])
}
fn values(columns: usize) -> (StaticValues, StaticLayout) {
    let schema = Arc::new(Schema::new(
        (0..columns)
            .map(|column| Field::new(format!("v{column}"), DataType::Int64, false))
            .collect::<Vec<_>>(),
    ));
    let arrays = (0..columns)
        .map(|column| Arc::new(Int64Array::from(vec![column as i64 + 1])) as ArrayRef)
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    let slots = (0..columns)
        .map(|column| SlotId::new(column as u32 + 1))
        .collect::<Vec<_>>();
    let layout = StaticLayout::try_new(schema, slots.into()).unwrap();
    (
        StaticValues::try_new(batch, layout.clone()).unwrap(),
        layout,
    )
}
fn profile(layout: &StaticLayout) -> CompileProfile {
    CompileProfile::new(
        NonZeroUsize::new(1).unwrap(),
        None,
        layout.identity().unwrap(),
        KernelAbiVersion::CURRENT,
    )
}
fn metadata_source_program(
    join: bool,
    wrong_definition: bool,
) -> Result<LocalProgramGraph, LocalProgramError> {
    let (source, layout) = values(1);
    let definitions = arena(vec![
        StaticExprNode::new(
            StaticExprKind::Literal(StaticLiteral::Int64(7)),
            DataType::Int64,
            None,
        ),
        StaticExprNode::new(
            StaticExprKind::Literal(StaticLiteral::Int64(9)),
            DataType::Int64,
            None,
        ),
    ]);
    let mut nodes = vec![ProgramNode::new(
        10,
        ProgramNodeKind::Values {
            values: source.clone(),
        },
        layout.clone(),
    )];
    let source_definition = ProgramExprId::new(usize::from(wrong_definition));
    let root = if join {
        let producer = StaticFilterProducer::try_new(
            7,
            8,
            FilterProducerKind::Membership,
            StaticFilterContract::Membership {
                data_type: DataType::Int64,
                null_semantics: FilterNullSemantics::NeverMatches,
                digest: [1; 32],
            },
            FilterReduction::SetUnion,
        )
        .unwrap();
        nodes.push(ProgramNode::new(
            11,
            ProgramNodeKind::Values { values: source },
            layout.clone(),
        ));
        nodes.push(ProgramNode::new(
            20,
            ProgramNodeKind::Join {
                left: ProgramNodeId::new(0),
                right: ProgramNodeId::new(1),
                join_type: JoinType::Inner,
                distribution_mode: JoinDistributionMode::Broadcast,
                left_layout: layout.clone(),
                right_layout: layout.clone(),
                join_scope_layout: layout.clone(),
                probe_keys: vec![ProgramExprId::new(0)],
                build_keys: vec![ProgramExprId::new(0)],
                eq_null_safe: vec![false],
                residual_predicate: None,
                runtime_filter_consumers: vec![],
                runtime_filters: vec![FilterProducerAtExpr {
                    expr_id: source_definition,
                    key_ordinal: 0,
                    producer,
                }],
            },
            layout.clone(),
        ));
        ProgramNodeId::new(2)
    } else {
        let producer = StaticFilterProducer::try_new(
            7,
            8,
            FilterProducerKind::OrderedBound,
            StaticFilterContract::Ordered {
                keys: Arc::from([FilterOrderKey {
                    data_type: DataType::Int64,
                    direction: FilterSortDirection::Ascending,
                    null_order: FilterNullOrder::Last,
                }]),
                comparator_digest: [1; 32],
                contract_digest: [2; 32],
            },
            FilterReduction::TightenOrderedBound,
        )
        .unwrap();
        nodes.push(ProgramNode::new(
            20,
            ProgramNodeKind::Aggregate {
                input: ProgramNodeId::new(0),
                group_by: vec![ProgramExprId::new(0)],
                functions: vec![],
                need_finalize: true,
                input_is_intermediate: false,
                topn_filters: vec![AggregateTopNFilter {
                    group_key_expr: source_definition,
                    group_key_ordinal: 0,
                    limit: NonZeroU32::new(3).unwrap(),
                    producer,
                }],
                streaming_preaggregation_mode: None,
            },
            layout.clone(),
        ));
        ProgramNodeId::new(1)
    };
    LocalProgramGraph::try_new(
        nodes,
        root,
        definitions,
        profile(&layout),
        BindingRequirements::try_new(vec![BindingRequirement::RuntimeFilter { binding_id: 7 }])
            .unwrap(),
    )
}

#[test]
fn group_and_build_filter_producers_reference_evaluated_arrays_without_extra_calls() {
    for (join, expected) in [(false, 1), (true, 2)] {
        let program = metadata_source_program(join, false).unwrap();
        let roots = ProgramExpressionRoots::collect(&program, &Control::default()).unwrap();
        assert_eq!(roots.sites().len(), expected);
        assert!(roots.sites().keys().all(|site| matches!(
            site,
            ProgramExpressionRootSite::Node {
                role: ProgramNodeExpressionRole::AggregateGroup { .. }
                    | ProgramNodeExpressionRole::JoinProbeKey { .. }
                    | ProgramNodeExpressionRole::JoinBuildKey { .. },
                ..
            }
        )));
        assert_eq!(
            metadata_source_program(join, true).unwrap_err(),
            LocalProgramError::InvalidNodeShape
        );
    }
}
fn branch(index: usize, keys: usize, columns: usize) -> StaticStreamBranch {
    StaticStreamBranch::try_new(
        index as i32 + 1,
        DataStreamPartitionType::HashPartitioned,
        vec![ProgramExprId::new(0); keys],
        (0..columns)
            .map(|column| SlotId::new(column as u32 + 1))
            .collect(),
        None,
    )
    .unwrap()
}
fn scoped_program(shared: bool) -> LocalProgramGraph {
    let (values, layout) = values(2);
    let main = integer_arena(7);
    let writer = if shared {
        main.clone()
    } else {
        integer_arena(8)
    };
    let sink_arena = if shared {
        main.clone()
    } else {
        boolean_arena()
    };
    let nodes = vec![
        ProgramNode::new(0, ProgramNodeKind::Values { values }, layout.clone()),
        ProgramNode::new(
            1,
            ProgramNodeKind::Project {
                input: ProgramNodeId::new(0),
                is_subordinate: false,
                validate_final_result_input: false,
                exprs: vec![ProgramExprId::new(0); 2],
                expr_slot_ids: vec![SlotId::new(1), SlotId::new(2)],
                expr_slot_schemas: None,
                output_indices: None,
            },
            layout.clone(),
        ),
        ProgramNode::new(
            2,
            ProgramNodeKind::TableWriter {
                input: ProgramNodeId::new(1),
                target: WriteTargetOrdinal::try_new(0).unwrap(),
                expected_layout: layout.clone(),
                projection: StaticWriterProjection {
                    arena: writer,
                    expressions: vec![ProgramExprId::new(0); 2],
                    layout: layout.clone(),
                },
                writer_multiplex_layout: layout.clone(),
                partial_aggregates: vec![],
            },
            layout.clone(),
        ),
    ];
    let sink = if shared {
        StaticSinkProgram::try_multicast(vec![branch(0, 1, 2), branch(1, 1, 2)], sink_arena)
            .unwrap()
    } else {
        StaticSinkProgram::try_split(
            vec![branch(0, 1, 2), branch(1, 1, 2)],
            vec![ProgramExprId::new(0); 2],
            sink_arena,
            true,
        )
        .unwrap()
    };
    let requirements = BindingRequirements::try_new(vec![
        BindingRequirement::TableWriter {
            node: ProgramNodeId::new(2),
            layout: layout.clone(),
        },
        BindingRequirement::ExchangeOutput {
            branch: 0,
            layout: layout.clone(),
        },
        BindingRequirement::ExchangeOutput {
            branch: 1,
            layout: layout.clone(),
        },
    ])
    .unwrap();
    LocalProgramGraph::try_new_with_sink(
        nodes,
        ProgramNodeId::new(2),
        main,
        profile(&layout),
        requirements,
        Some(sink),
    )
    .unwrap()
}
#[test]
fn repeated_definition_zero_keeps_every_actual_main_writer_and_sink_site_with_exact_backing() {
    let program = scoped_program(false);
    let roots = ProgramExpressionRoots::collect(&program, &Control::default()).unwrap();
    assert_eq!(roots.arenas().len(), 3);
    assert_eq!(roots.sites().len(), 8);
    assert!(Arc::ptr_eq(
        &roots.arenas()[&ProgramExpressionArena::Main],
        program.expressions()
    ));
    let ProgramNodeKind::TableWriter { projection, .. } = program.nodes()[2].kind() else {
        unreachable!()
    };
    assert!(Arc::ptr_eq(
        &roots.arenas()[&ProgramExpressionArena::WriterProjection(ProgramNodeId::new(2))],
        &projection.arena
    ));
    assert!(Arc::ptr_eq(
        &roots.arenas()[&ProgramExpressionArena::Sink],
        program.sink().unwrap().arena().unwrap()
    ));
    let literal = |scope| match roots.arenas()[&scope]
        .node(ProgramExprId::new(0))
        .unwrap()
        .kind()
    {
        StaticExprKind::Literal(value) => value.clone(),
        _ => panic!("fixture root is literal"),
    };
    assert!(matches!(
        literal(ProgramExpressionArena::Main),
        StaticLiteral::Int64(7)
    ));
    assert!(matches!(
        literal(ProgramExpressionArena::WriterProjection(
            ProgramNodeId::new(2)
        )),
        StaticLiteral::Int64(8)
    ));
    assert!(matches!(
        literal(ProgramExpressionArena::Sink),
        StaticLiteral::Bool(true)
    ));
    for expression in 0..2 {
        assert_eq!(
            roots.sites()[&ProgramExpressionRootSite::Node {
                node: ProgramNodeId::new(1),
                role: ProgramNodeExpressionRole::ProjectOutput { expression }
            }],
            ProgramExpressionRoot {
                definition: ProgramExprId::new(0),
                demand: EvaluationDemand::Value
            }
        );
        assert_eq!(
            roots.sites()[&ProgramExpressionRootSite::WriterProjection {
                node: ProgramNodeId::new(2),
                expression
            }],
            ProgramExpressionRoot {
                definition: ProgramExprId::new(0),
                demand: EvaluationDemand::Value
            }
        );
        assert_eq!(
            roots.sites()[&ProgramExpressionRootSite::SinkPartition {
                branch: expression,
                key: 0
            }]
                .demand,
            EvaluationDemand::Value
        );
        assert_eq!(
            roots.sites()[&ProgramExpressionRootSite::SinkSplitPredicate { branch: expression }]
                .demand,
            EvaluationDemand::TruthOnly
        );
    }
}
#[test]
fn shared_arc_backing_does_not_merge_arena_scope_or_root_occurrences() {
    let program = scoped_program(true);
    let roots = ProgramExpressionRoots::collect(&program, &Control::default()).unwrap();
    assert_eq!(roots.arenas().len(), 3);
    assert_eq!(roots.sites().len(), 6);
    assert!(
        roots
            .arenas()
            .values()
            .all(|arena| Arc::ptr_eq(arena, program.expressions()))
    );
    assert_eq!(
        roots
            .sites()
            .keys()
            .filter(|site| site.arena() == ProgramExpressionArena::Main)
            .count(),
        2
    );
    assert_eq!(
        roots
            .sites()
            .keys()
            .filter(|site| site.arena() == ProgramExpressionArena::Sink)
            .count(),
        2
    );
    assert_eq!(
        roots
            .sites()
            .keys()
            .filter(|site| matches!(site.arena(), ProgramExpressionArena::WriterProjection(_)))
            .count(),
        2
    );
}
fn finish_program(constants: Vec<Vec<UnpivotConstant>>) -> LocalProgramGraph {
    let (values, layout) = values(1);
    let expressions = integer_arena(7);
    let unpivot = WriterGroupedUnpivotPlan {
        grouping_input_slot_id: SlotId::new(1),
        grouping_output_slot_id: SlotId::new(2),
        passthrough_output_slot_id: SlotId::new(3),
        value_output_slot_id: SlotId::new(4),
        literal_output_slot_ids: vec![SlotId::new(5), SlotId::new(6), SlotId::new(7)],
        mappings: constants
            .into_iter()
            .enumerate()
            .map(|(mapping, constants)| WriterGroupedUnpivotMapping {
                grouping_key: mapping as u32,
                input_value_slot_id: SlotId::new(1),
                constants,
            })
            .collect(),
        max_output_rows: 16,
        max_output_bytes: 1024,
    };
    LocalProgramGraph::try_new(
        vec![
            ProgramNode::new(0, ProgramNodeKind::Values { values }, layout.clone()),
            ProgramNode::new(
                1,
                ProgramNodeKind::TableFinish {
                    inputs: vec![ProgramNodeId::new(0)],
                    expected_targets: vec![WriteTargetOrdinal::try_new(0).unwrap()],
                    writer_multiplex_layout: layout.clone(),
                    root_result_layout: layout.clone(),
                    final_aggregates: WriterFinalAggregatePlan {
                        calls: vec![],
                        unpivot: Some(unpivot),
                    },
                },
                layout.clone(),
            ),
        ],
        ProgramNodeId::new(1),
        expressions,
        profile(&layout),
        BindingRequirements::try_new(vec![BindingRequirement::TableFinish {
            node: ProgramNodeId::new(1),
            layout,
        }])
        .unwrap(),
    )
    .unwrap()
}
#[test]
fn finish_grouped_unpivot_mixed_constants_keep_scalar_mapping_and_constant_ordinals() {
    let scalar = || UnpivotConstant::Scalar {
        expr_id: ProgramExprId::new(0),
        nullable: false,
    };
    let list = || UnpivotConstant::Int32List(vec![2, 3]);
    let map = || UnpivotConstant::Utf8Map(vec![(Arc::from("k"), Arc::from("v"))]);
    let program = finish_program(vec![
        vec![list(), scalar(), map()],
        vec![scalar(), map(), scalar()],
    ]);
    let roots = ProgramExpressionRoots::collect(&program, &Control::default()).unwrap();
    assert_eq!(roots.sites().len(), 3);
    for (mapping, constant) in [(0, 1), (1, 0), (1, 2)] {
        assert_eq!(
            roots.sites()[&ProgramExpressionRootSite::Node {
                node: ProgramNodeId::new(1),
                role: ProgramNodeExpressionRole::FinishUnpivotConstant { mapping, constant }
            }],
            ProgramExpressionRoot {
                definition: ProgramExprId::new(0),
                demand: EvaluationDemand::Value
            }
        );
    }
    assert_eq!(roots.arenas().len(), 1);
}
fn sink_budget_program(extra_partition: bool, main_root: bool) -> LocalProgramGraph {
    let (values, layout) = values(1);
    let expressions = integer_arena(7);
    let mut nodes = vec![ProgramNode::new(
        0,
        ProgramNodeKind::Values { values },
        layout.clone(),
    )];
    if main_root {
        nodes.push(ProgramNode::new(
            1,
            ProgramNodeKind::Project {
                input: ProgramNodeId::new(0),
                is_subordinate: false,
                validate_final_result_input: false,
                exprs: vec![ProgramExprId::new(0)],
                expr_slot_ids: vec![SlotId::new(1)],
                expr_slot_schemas: None,
                output_indices: None,
            },
            layout.clone(),
        ));
    }
    let mut branches = (0..16)
        .map(|index| branch(index, 4096, 1))
        .collect::<Vec<_>>();
    if extra_partition {
        branches.push(branch(16, 1, 1));
    }
    let requirements = BindingRequirements::try_new(
        (0..branches.len())
            .map(|branch| BindingRequirement::ExchangeOutput {
                branch,
                layout: layout.clone(),
            })
            .collect(),
    )
    .unwrap();
    let sink = StaticSinkProgram::try_multicast(branches, integer_arena(8)).unwrap();
    let root = ProgramNodeId::new(nodes.len() - 1);
    LocalProgramGraph::try_new_with_sink(
        nodes,
        root,
        expressions,
        profile(&layout),
        requirements,
        Some(sink),
    )
    .unwrap()
}
#[test]
fn one_global_root_budget_accepts_65536_and_rejects_extra_sink_or_main_occurrence() {
    assert_eq!(MAX_CONTROL_USE_REFERENCES, 65536);
    let near = sink_budget_program(false, false);
    let roots = ProgramExpressionRoots::collect(&near, &Control::default()).unwrap();
    assert_eq!(roots.sites().len(), MAX_CONTROL_USE_REFERENCES);
    assert!(
        near.sink()
            .unwrap()
            .branches()
            .iter()
            .all(|branch| branch.partition_exprs().len() == 4096)
    );
    for program in [
        sink_budget_program(true, false),
        sink_budget_program(false, true),
    ] {
        assert_eq!(
            ProgramExpressionRoots::collect(&program, &Control::default()).unwrap_err(),
            ProgramExpressionRootError::TooManyRoots
        );
    }
}
#[test]
fn entry_and_positive_quantum_failures_keep_exact_typed_control_categories() {
    let program = sink_budget_program(false, false);
    for failure in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for positive in [false, true] {
            let control = Control {
                failure: Some(failure),
                positive,
                work: Mutex::default(),
            };
            assert_eq!(
                ProgramExpressionRoots::collect(&program, &control).unwrap_err(),
                ProgramExpressionRootError::Control(failure)
            );
            assert_eq!(
                *control.work.lock().unwrap(),
                if positive { vec![0, 256] } else { vec![0] }
            );
        }
    }
}
#[test]
fn non_expression_constants_and_zero_expression_nodes_still_observe_work() {
    let program = finish_program(
        (0..300)
            .map(|_| {
                vec![
                    UnpivotConstant::Int32List(vec![]),
                    UnpivotConstant::Utf8Map(vec![]),
                    UnpivotConstant::Int32List(vec![]),
                ]
            })
            .collect(),
    );
    let control = Control::default();
    let roots = ProgramExpressionRoots::collect(&program, &control).unwrap();
    assert!(roots.sites().is_empty());
    assert!(control.work.lock().unwrap().contains(&256));
    let (values, layout) = values(1);
    let mut nodes = (0..300)
        .map(|index| {
            ProgramNode::new(
                index,
                ProgramNodeKind::Values {
                    values: values.clone(),
                },
                layout.clone(),
            )
        })
        .collect::<Vec<_>>();
    nodes.push(ProgramNode::new(
        300,
        ProgramNodeKind::UnionAll {
            inputs: (0..300).map(ProgramNodeId::new).collect(),
        },
        layout.clone(),
    ));
    let zero = LocalProgramGraph::try_new(
        nodes,
        ProgramNodeId::new(300),
        arena(vec![]),
        profile(&layout),
        BindingRequirements::try_new(vec![BindingRequirement::ResultSink { layout }]).unwrap(),
    )
    .unwrap();
    let control = Control::default();
    assert!(
        ProgramExpressionRoots::collect(&zero, &control)
            .unwrap()
            .sites()
            .is_empty()
    );
    assert!(control.work.lock().unwrap().contains(&256));
    for program in [&program, &zero] {
        for failure in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                failure: Some(failure),
                positive: true,
                work: Mutex::default(),
            };
            assert_eq!(
                ProgramExpressionRoots::collect(program, &control).unwrap_err(),
                ProgramExpressionRootError::Control(failure)
            );
            assert_eq!(*control.work.lock().unwrap(), [0, 256]);
        }
    }
}
