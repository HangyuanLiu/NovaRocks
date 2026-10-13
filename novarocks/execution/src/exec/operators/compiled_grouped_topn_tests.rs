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

//! Actual compiled Partial -> Grouped Intermediate -> Final owners.
//! A separate complete BTreeMap/SQL key order oracle proves all contributions;
//! the N bound is checked during consumption, not inferred from final output.
//! Declared local Values/catalog/tracker inputs need no external fixture or
//! processes. Execution owns this affected-change component lane. Native SQL,
//! remaining key domains and variable state resources have separate gates.

use super::aggregate_fixture::{
    CallSpec, add_aggregate, aggregate_catalog, bind, compile, finish, packages, values,
};
use super::family_fixture::{FixtureControl, cell, int64};
use super::*;
use crate::exec::operators::compiled_aggregate::CompiledAggregateProcessorFactory;
use crate::runtime::fragment::ExecutionFailureCause;
use arrow::array::Int64Array;
use novarocks_functions::PureKernelAbi;
use novarocks_physical_plan::{
    AggregateCall, AggregateCallId, AggregateGrouping, AggregatePhase, AggregateSequenceId,
    ExprKind, FragmentBuilder, FragmentId, FragmentSink, NodeId, NodeKind, NullOrdering,
    OrderingKey, OutputPort, PhysicalNode, PlanBuilder, PlanVersionId, ResultField, ResultPort,
    SortDirection, SortExpr, TopNPhase, TopNReduction, TopNSequenceId, ValueOrigin,
};
use std::collections::BTreeMap;

type Input = (Option<i64>, Option<i64>);
type Output = (Option<i64>, i64, Option<i64>, Option<i64>, Option<i64>);
const ROWS: &[Input] = &[
    (Some(8), Some(80)),
    (Some(3), Some(30)),
    (Some(8), Some(8)),
    (Some(1), Some(10)),
    (None, Some(7)),
    (Some(2), None),
    (Some(1), Some(11)),
    (Some(9), Some(90)),
    (None, None),
    (Some(2), Some(20)),
    (Some(1), None),
    (Some(7), Some(70)),
    (None, Some(-2)),
    (Some(2), Some(22)),
    (Some(1), Some(-3)),
];

struct Fixture {
    program: Arc<LocalProgram>,
    input: Chunk,
    partial: ProgramNodeId,
    grouped: ProgramNodeId,
    final_node: ProgramNodeId,
}
fn fixture(limit: usize, asc: bool, nulls_first: bool) -> Fixture {
    let catalog = aggregate_catalog(&[
        ("count", PureKernelAbi::AggregateWindowV1),
        ("sum", PureKernelAbi::AggregateWindowV1),
        ("min", PureKernelAbi::AggregateWindowV1),
        ("max", PureKernelAbi::AggregateWindowV1),
    ]);
    let bounds = [
        bind(&catalog, "count", &[]),
        bind(&catalog, "sum", &[int64(true)]),
        bind(&catalog, "min", &[int64(true)]),
        bind(&catalog, "max", &[int64(true)]),
    ];
    let mut builder = FragmentBuilder::new(FragmentId::new(1));
    let source = builder.reserve_node_id().unwrap();
    let columns = values(
        &mut builder,
        source,
        &[int64(true), int64(true)],
        &ROWS
            .iter()
            .map(|(key, value)| vec![cell(*key), cell(*value)])
            .collect::<Vec<_>>(),
    );
    let sequence = |ordinal: usize| AggregateSequenceId::new(41 + ordinal as u32);
    let partial_calls = bounds
        .iter()
        .enumerate()
        .map(|(ordinal, bound)| CallSpec {
            bound,
            id: AggregateCallId::new(10 + ordinal as u32),
            phase: AggregatePhase::Partial {
                sequence: sequence(ordinal),
            },
            distinct: false,
            arguments: if ordinal == 0 {
                vec![]
            } else {
                vec![columns[1]]
            },
        })
        .collect::<Vec<_>>();
    let (partial, partial_output) = add_aggregate(
        &mut builder,
        source,
        &columns[..1],
        &partial_calls,
        AggregateGrouping::Partial,
    );
    let grouped = builder.reserve_node_id().unwrap();
    let key = builder
        .add_expression(grouped, int64(true), ExprKind::Value(columns[0]))
        .unwrap();
    let mut outputs = vec![columns[0]];
    let mut calls = Vec::new();
    for (ordinal, bound) in bounds.iter().enumerate() {
        let id = AggregateCallId::new(20 + ordinal as u32);
        let phase = AggregatePhase::Intermediate {
            sequence: sequence(ordinal),
        };
        let argument = builder
            .add_expression(
                grouped,
                bound.intermediate_type(),
                ExprKind::Value(partial_output[ordinal + 1]),
            )
            .unwrap();
        let output = builder
            .add_value(
                bound.output_type(phase),
                ValueOrigin::AggregateState { call: id, phase },
            )
            .unwrap();
        outputs.push(output);
        calls.push(AggregateCall {
            id,
            binding: bound.binding(phase),
            arguments: Box::from([argument]),
            distinct: false,
            order_by: Box::default(),
            output,
        });
    }
    let direction = if asc {
        SortDirection::Ascending
    } else {
        SortDirection::Descending
    };
    let null_ordering = if nulls_first {
        NullOrdering::First
    } else {
        NullOrdering::Last
    };
    let required = builder.node_output_properties(partial).unwrap().clone();
    let mut produced = required.clone();
    produced.ordering = Box::from([OrderingKey {
        value: columns[0],
        direction,
        null_ordering,
    }]);
    builder
        .insert_node_unchecked(PhysicalNode {
            id: grouped,
            inputs: Box::from([partial]),
            required_inputs: Box::from([required]),
            output_properties: produced,
            output: OutputPort {
                node: grouped,
                columns: outputs.clone().into_boxed_slice(),
            },
            kind: NodeKind::TopN {
                order_by: Box::from([SortExpr {
                    expr: key,
                    direction,
                    null_ordering,
                }]),
                limit: limit as u64,
                offset: 0,
                phase: TopNPhase::Partial {
                    sequence: TopNSequenceId::new(42),
                },
                reduction: TopNReduction::GroupedStates {
                    group_by: Box::from([(key, columns[0])]),
                    calls: calls.into_boxed_slice(),
                    comparator:
                        novarocks_type_contract::OrderedComparisonAlgorithm::NativeScalarOrderV1,
                },
            },
        })
        .unwrap();
    let final_calls = bounds
        .iter()
        .enumerate()
        .map(|(ordinal, bound)| CallSpec {
            bound,
            id: AggregateCallId::new(30 + ordinal as u32),
            phase: AggregatePhase::Final {
                sequence: sequence(ordinal),
            },
            distinct: false,
            arguments: vec![outputs[ordinal + 1]],
        })
        .collect::<Vec<_>>();
    let (final_node, _) = add_aggregate(
        &mut builder,
        grouped,
        &columns[..1],
        &final_calls,
        AggregateGrouping::Complete,
    );
    let topn = builder.reserve_node_id().unwrap();
    let key = builder
        .add_expression(topn, int64(true), ExprKind::Value(columns[0]))
        .unwrap();
    builder
        .add_top_n(
            topn,
            final_node,
            Box::from([SortExpr {
                expr: key,
                direction,
                null_ordering,
            }]),
            limit as u64,
            0,
            TopNPhase::Final {
                sequence: TopNSequenceId::new(42),
            },
        )
        .unwrap();
    let definition = finish(builder, topn, FragmentSink::Result, 2);
    let output = definition.nodes()[&topn].output.clone();
    let fields = output
        .columns
        .iter()
        .enumerate()
        .map(|(ordinal, value)| ResultField {
            domain: crate::test_result_domain::result_value_domain(&definition.values()[value].ty),
            name: format!("field-{ordinal}").into(),
            alias: None,
            value: *value,
            ty: definition.values()[value].ty.clone(),
        })
        .collect();
    let mut plan = PlanBuilder::new(PlanVersionId::try_new([121; 16]).unwrap());
    plan.add_fragment(definition).unwrap();
    plan.set_result_port(ResultPort {
        scalar_schema: None,
        fragment: FragmentId::new(1),
        output,
        fields,
    })
    .unwrap();
    let plan = plan.finish_observed(&FixtureControl).unwrap();
    let package = packages(&plan, &catalog)
        .remove(&FragmentId::new(1))
        .unwrap();
    let program = compile(package, &catalog, 2, true);
    let mut source = None;
    let mut partial = None;
    let mut grouped = None;
    let mut final_node = None;
    for (index, node) in program.graph().nodes().iter().enumerate() {
        match node.kind() {
            ProgramNodeKind::Values { values } => {
                source = Some(Chunk::new_with_chunk_schema(
                    values.batch().unwrap().clone(),
                    ChunkSchema::from_compiled_layout(node.output_layout()).unwrap(),
                ))
            }
            ProgramNodeKind::Aggregate { need_finalize, .. } => {
                if *need_finalize {
                    final_node = Some(ProgramNodeId::new(index));
                } else {
                    partial = Some(ProgramNodeId::new(index));
                }
            }
            ProgramNodeKind::GroupedTopN { .. } => grouped = Some(ProgramNodeId::new(index)),
            _ => {}
        }
    }
    Fixture {
        program,
        input: source.unwrap(),
        partial: partial.unwrap(),
        grouped: grouped.unwrap(),
        final_node: final_node.unwrap(),
    }
}

fn partial_chunks(f: &Fixture, offset: usize, len: usize) -> Vec<Chunk> {
    let factory = CompiledAggregateProcessorFactory::try_new(
        Arc::clone(&f.program),
        f.partial,
        Arc::new(RuntimeErrorState::default()),
    )
    .unwrap();
    let mut operator = factory.create(1, 0);
    operator.set_mem_tracker(MemTracker::new_root("grouped-fixture-partial"));
    let state = RuntimeState::default();
    let processor = operator.as_processor_mut().unwrap();
    processor
        .push_chunk(
            &state,
            Chunk::new_like(f.input.batch.slice(offset, len), &f.input),
        )
        .unwrap();
    processor.set_finishing(&state).unwrap();
    let mut output = Vec::new();
    while let Some(chunk) = processor.pull_chunk(&state).unwrap() {
        output.push(chunk);
    }
    output
}
fn oracle(limit: usize, asc: bool, nulls_first: bool) -> Vec<Output> {
    let mut all = BTreeMap::<Option<i64>, (i64, Option<i64>, Option<i64>, Option<i64>)>::new();
    for &(key, value) in ROWS {
        let state = all.entry(key).or_insert((0, None, None, None));
        state.0 += 1;
        if let Some(value) = value {
            state.1 = Some(state.1.unwrap_or(0) + value);
            state.2 = Some(state.2.map_or(value, |old| old.min(value)));
            state.3 = Some(state.3.map_or(value, |old| old.max(value)));
        }
    }
    let mut rows = all
        .into_iter()
        .map(|(key, (count, sum, min, max))| (key, count, sum, min, max))
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| match (a.0, b.0) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (Some(_), None) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (Some(a), Some(b)) => {
            if asc {
                a.cmp(&b)
            } else {
                b.cmp(&a)
            }
        }
    });
    rows.truncate(limit);
    rows
}
fn collect_final(f: &Fixture, chunks: Vec<Chunk>) -> Vec<Output> {
    let factory = CompiledAggregateProcessorFactory::try_new(
        Arc::clone(&f.program),
        f.final_node,
        Arc::new(RuntimeErrorState::default()),
    )
    .unwrap();
    let mut operator = factory.create(1, 0);
    operator.set_mem_tracker(MemTracker::new_root("grouped-fixture-final"));
    let state = RuntimeState::default();
    let processor = operator.as_processor_mut().unwrap();
    for chunk in chunks {
        processor.push_chunk(&state, chunk).unwrap();
    }
    processor.set_finishing(&state).unwrap();
    let mut result = Vec::new();
    while let Some(chunk) = processor.pull_chunk(&state).unwrap() {
        let columns = chunk
            .batch
            .columns()
            .iter()
            .map(|array| array.as_any().downcast_ref::<Int64Array>().unwrap())
            .collect::<Vec<_>>();
        let value = |column: usize, row: usize| {
            if columns[column].is_null(row) {
                None
            } else {
                Some(columns[column].value(row))
            }
        };
        for row in 0..chunk.len() {
            result.push((
                value(0, row),
                value(1, row).unwrap(),
                value(2, row),
                value(3, row),
                value(4, row),
            ));
        }
    }
    result
}

#[test]
fn compiled_grouped_topn_keeps_all_contributions_within_its_distinct_key_bound() {
    for (limit, asc, nulls_first) in [
        (0, true, true),
        (1, true, false),
        (2, true, true),
        (2, false, false),
        (2, false, true),
    ] {
        let f = fixture(limit, asc, nulls_first);
        let factory = CompiledGroupedTopNProcessorFactory::try_new(
            Arc::clone(&f.program),
            f.grouped,
            Arc::new(RuntimeErrorState::default()),
        )
        .unwrap();
        let mut processor = factory.processor();
        let tracker = MemTracker::new_root("grouped-fixture-consumer");
        processor.set_mem_tracker(tracker.clone());
        let state = RuntimeState::default();
        for (offset, len) in [(0, 3), (3, 2), (5, 4), (9, 2), (11, 4)] {
            for chunk in partial_chunks(&f, offset, len) {
                processor.push_chunk(&state, chunk).unwrap();
                assert!(
                    processor
                        .candidates
                        .as_ref()
                        .map_or(0, |set| set.entries.len())
                        <= limit
                );
                assert!(
                    !processor.has_output(),
                    "no unfinished state prefix escapes"
                );
            }
        }
        processor.set_finishing(&state).unwrap();
        let mut output = Vec::new();
        while let Some(chunk) = processor.pull_chunk(&state).unwrap() {
            output.push(chunk);
        }
        assert_eq!(collect_final(&f, output), oracle(limit, asc, nulls_first));
        assert!(processor.is_finished());
        assert_eq!(tracker.current(), 0);
    }
}

#[test]
fn compiled_grouped_topn_cancel_destroys_live_owners_and_keeps_the_first_cause() {
    let f = fixture(2, true, true);
    let error = Arc::new(RuntimeErrorState::default());
    let factory = CompiledGroupedTopNProcessorFactory::try_new(
        Arc::clone(&f.program),
        f.grouped,
        error.clone(),
    )
    .unwrap();
    let mut processor = factory.processor();
    let tracker = MemTracker::new_root("grouped-fixture-cancel");
    processor.set_mem_tracker(tracker.clone());
    let state = RuntimeState::default();
    for chunk in partial_chunks(&f, 0, 3) {
        processor.push_chunk(&state, chunk).unwrap();
    }
    assert!(tracker.current() > 0);
    error.set_error("stop the actual grouped consumer after accumulation");
    let first = processor.push_chunk(&state, f.input.clone()).unwrap_err();
    assert_eq!(
        first.cause(),
        &ExecutionFailureCause::Kernel(KernelFailure::Cancelled)
    );
    assert_eq!(tracker.current(), 0);
    assert_eq!(processor.pull_chunk(&state).unwrap_err(), first);
    assert_eq!(processor.set_finishing(&state).unwrap_err(), first);
    processor.cancel();
    assert_eq!(processor.close().unwrap_err(), first);
}
