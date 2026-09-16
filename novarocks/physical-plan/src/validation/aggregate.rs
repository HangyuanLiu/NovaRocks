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

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    AggregatePhase, Distribution, ExprId, Fragment, FragmentId, NodeId, NodeKind, PhysicalPlan,
    ValueId,
};

pub(crate) fn validate_aggregate_sequences(plan: &PhysicalPlan, errors: &mut ValidationContext) {
    type CallRef = (FragmentId, NodeId, crate::AggregateCallId);

    #[derive(Default)]
    struct SequenceMembers {
        partials: BTreeSet<CallRef>,
        intermediates: BTreeSet<CallRef>,
        finals: Vec<CallRef>,
    }

    let mut sequences: BTreeMap<crate::AggregateSequenceId, SequenceMembers> = BTreeMap::new();
    let mut calls_by_ref = BTreeMap::new();
    for fragment in plan.fragments().values() {
        for node in fragment.nodes().values() {
            let NodeKind::Aggregate { calls, .. } = &node.kind else {
                continue;
            };
            for call in calls {
                let call_ref = (fragment.id(), node.id, call.id);
                calls_by_ref.insert(call_ref, (fragment, node, call));
                let Some(sequence) = call.binding.phase.sequence() else {
                    continue;
                };
                let members = sequences.entry(sequence).or_default();
                match call.binding.phase {
                    AggregatePhase::Single => unreachable!("single phase has no sequence"),
                    AggregatePhase::Partial { .. } => {
                        members.partials.insert(call_ref);
                    }
                    AggregatePhase::Intermediate { .. } => {
                        members.intermediates.insert(call_ref);
                    }
                    AggregatePhase::Final { .. } => members.finals.push(call_ref),
                }
            }
        }
    }

    let mut trace_budget = SemanticTraceWorkBudget::new(errors.limits());
    let mut trace_indexes = SemanticTraceIndexes::default();
    for (sequence, members) in sequences {
        let path = format!("aggregate_sequences[{}]", sequence.get());
        if members.finals.len() != 1 {
            errors.push(ValidationError::new(
                &path,
                "aggregate sequence must have exactly one final call",
            ));
            continue;
        }
        if members.partials.is_empty() {
            errors.push(ValidationError::new(
                &path,
                "aggregate sequence final has no partial producer",
            ));
            continue;
        }
        let final_ref = members.finals[0];
        let Some(&(final_fragment, final_node, final_call)) = calls_by_ref.get(&final_ref) else {
            continue;
        };
        let NodeKind::Aggregate { group_by, .. } = &final_node.kind else {
            continue;
        };
        let Some(values) = aggregate_state_inputs(final_fragment, group_by, final_call) else {
            errors.push(ValidationError::new(
                &path,
                "aggregate final inputs are not direct grouping/state values",
            ));
            continue;
        };
        let Some(input) = final_node.inputs.first().copied() else {
            continue;
        };
        let partial_shape = members.partials.iter().find_map(|call_ref| {
            calls_by_ref.get(call_ref).map(|(_, _, call)| {
                (
                    call.distinct,
                    call.order_by
                        .iter()
                        .map(|item| (item.direction, item.null_ordering))
                        .collect::<Vec<_>>(),
                )
            })
        });
        let Some(partial_shape) = partial_shape else {
            continue;
        };
        let mut matched_partials = BTreeSet::new();
        let mut matched_intermediates = BTreeSet::new();
        let valid = trace_aggregate_sequence_inputs(
            plan,
            sequence,
            (final_fragment.id(), input),
            values,
            &final_call.binding,
            &partial_shape,
            &mut matched_partials,
            &mut matched_intermediates,
            &mut trace_budget,
            &mut trace_indexes,
        );
        if !valid
            || matched_partials != members.partials
            || matched_intermediates != members.intermediates
        {
            errors.push(ValidationError::new(
                &path,
                "aggregate state paths do not reduce exactly into their matching final",
            ));
        }
    }
}

pub(crate) fn aggregate_state_inputs(
    fragment: &Fragment,
    group_by: &[(ExprId, ValueId)],
    call: &crate::AggregateCall,
) -> Option<Vec<ValueId>> {
    let mut values = group_by
        .iter()
        .map(|(expression, _)| crate::expression_value(fragment.expressions(), *expression))
        .collect::<Option<Vec<_>>>()?;
    if call.arguments.len() != 1 || !call.order_by.is_empty() {
        return None;
    }
    values.push(crate::expression_value(
        fragment.expressions(),
        call.arguments[0],
    )?);
    Some(values)
}

pub(crate) fn aggregate_outputs(
    group_by: &[(ExprId, ValueId)],
    call: &crate::AggregateCall,
) -> Vec<ValueId> {
    group_by
        .iter()
        .map(|(_, output)| *output)
        .chain(std::iter::once(call.output))
        .collect()
}

pub(crate) fn aggregate_bindings_match(
    expected: &crate::AggregateBinding,
    actual: &crate::AggregateBinding,
) -> bool {
    expected.function == actual.function
        && expected.logical_argument_count == actual.logical_argument_count
        && expected.intermediate_type == actual.intermediate_type
        && expected.state_format == actual.state_format
        && expected.phase.sequence() == actual.phase.sequence()
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn trace_aggregate_sequence_inputs(
    plan: &PhysicalPlan,
    sequence: crate::AggregateSequenceId,
    start: (FragmentId, NodeId),
    initial_values: Vec<ValueId>,
    expected_binding: &crate::AggregateBinding,
    partial_shape: &(bool, Vec<(crate::SortDirection, crate::NullOrdering)>),
    matched_partials: &mut BTreeSet<(FragmentId, NodeId, crate::AggregateCallId)>,
    matched_intermediates: &mut BTreeSet<(FragmentId, NodeId, crate::AggregateCallId)>,
    trace_budget: &mut SemanticTraceWorkBudget,
    trace_indexes: &mut SemanticTraceIndexes,
) -> bool {
    let mut pending = vec![(start, initial_values)];
    let mut visited = BTreeSet::new();
    while let Some((node_ref, expected_values)) = pending.pop() {
        if !trace_budget.charge(expected_values.len().saturating_add(1))
            || !visited.insert((node_ref.0, node_ref.1, expected_values.clone()))
        {
            return false;
        }
        let Some(fragment) = plan.fragments().get(&node_ref.0) else {
            return false;
        };
        let Some(node) = fragment.nodes().get(&node_ref.1) else {
            return false;
        };
        match &node.kind {
            NodeKind::Aggregate {
                group_by, calls, ..
            } => {
                let Some(call) = trace_indexes.aggregate_sequence_call(
                    fragment.id(),
                    node.id,
                    calls,
                    sequence,
                    trace_budget,
                ) else {
                    return false;
                };
                if !aggregate_bindings_match(expected_binding, &call.binding)
                    || aggregate_outputs(group_by, call) != expected_values
                {
                    return false;
                }
                let call_ref = (fragment.id(), node.id, call.id);
                match call.binding.phase {
                    AggregatePhase::Partial { .. } => {
                        let shape = (
                            call.distinct,
                            call.order_by
                                .iter()
                                .map(|item| (item.direction, item.null_ordering))
                                .collect::<Vec<_>>(),
                        );
                        if shape != *partial_shape || !matched_partials.insert(call_ref) {
                            return false;
                        }
                    }
                    AggregatePhase::Intermediate { .. } => {
                        if !matched_intermediates.insert(call_ref) {
                            return false;
                        }
                        let Some(values) = aggregate_state_inputs(fragment, group_by, call) else {
                            return false;
                        };
                        let Some(input) = node.inputs.first().copied() else {
                            return false;
                        };
                        pending.push(((fragment.id(), input), values));
                    }
                    AggregatePhase::Single | AggregatePhase::Final { .. } => return false,
                }
            }
            NodeKind::ExchangeSource { edge, .. } => {
                let Some(edge) = plan.edges().get(edge) else {
                    return false;
                };
                if edge.kind != crate::EdgeKind::Stream
                    || edge.destination.fragment != node_ref.0
                    || edge.destination.node != node_ref.1
                {
                    return false;
                }
                let Some(values) = trace_indexes.map_edge_values(
                    edge.id,
                    &edge.destination.receive_mapping,
                    &expected_values,
                    false,
                    trace_budget,
                ) else {
                    return false;
                };
                let Some(source) = plan.fragments().get(&edge.source.fragment) else {
                    return false;
                };
                pending.push(((source.id(), source.root()), values));
            }
            NodeKind::Project { expressions } => {
                let Some(input) = node.inputs.first().copied() else {
                    return false;
                };
                let Some(child) = fragment.nodes().get(&input) else {
                    return false;
                };
                let Some(values) = trace_indexes.map_project_values(
                    fragment,
                    node,
                    child,
                    expressions,
                    &expected_values,
                    trace_budget,
                ) else {
                    return false;
                };
                pending.push(((fragment.id(), input), values));
            }
            // A partial top-N between two phases of an aggregate drops whole
            // groups the final would not have published anyway -- that is what
            // it is placed for, and its own sequence proves the order it prunes
            // by is the grouping. The states that survive it carry on
            // unchanged.
            NodeKind::TopN {
                phase: crate::TopNPhase::Partial { .. },
                ..
            } => {
                let Some(input) = node.inputs.first().copied() else {
                    return false;
                };
                pending.push(((fragment.id(), input), expected_values));
            }
            NodeKind::SetOp {
                kind: crate::SetOperationKind::UnionAll,
                input_mappings,
            } => {
                if node.inputs.len() != input_mappings.len() || node.inputs.is_empty() {
                    return false;
                }
                for (input_ordinal, (input, mapping)) in
                    node.inputs.iter().zip(input_mappings).enumerate()
                {
                    let Some(mapped) = trace_indexes.map_union_values(
                        fragment.id(),
                        node.id,
                        input_ordinal,
                        &node.output.columns,
                        mapping,
                        &expected_values,
                        false,
                        trace_budget,
                    ) else {
                        return false;
                    };
                    pending.push(((fragment.id(), *input), mapped));
                }
            }
            _ => return false,
        }
    }
    true
}

pub(crate) fn validate_topn_reductions(plan: &PhysicalPlan, errors: &mut ValidationContext) {
    type NodeRef = (FragmentId, NodeId);

    let mut partials: BTreeMap<crate::TopNSequenceId, BTreeSet<NodeRef>> = BTreeMap::new();
    let mut finals: BTreeMap<crate::TopNSequenceId, Vec<NodeRef>> = BTreeMap::new();
    for fragment in plan.fragments().values() {
        for node in fragment.nodes().values() {
            if let NodeKind::TopN { phase, .. } = &node.kind {
                match *phase {
                    crate::TopNPhase::Single => {}
                    crate::TopNPhase::Partial { sequence } => {
                        partials
                            .entry(sequence)
                            .or_default()
                            .insert((fragment.id(), node.id));
                    }
                    crate::TopNPhase::Final { sequence } => {
                        finals
                            .entry(sequence)
                            .or_default()
                            .push((fragment.id(), node.id));
                    }
                }
            }
        }
    }

    let sequences = partials
        .keys()
        .chain(finals.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    let mut trace_budget = SemanticTraceWorkBudget::new(errors.limits());
    let mut trace_indexes = SemanticTraceIndexes::default();
    for sequence in sequences {
        let sequence_path = format!("topn_sequences[{}]", sequence.get());
        let expected_partials = partials.get(&sequence).cloned().unwrap_or_default();
        let sequence_finals = finals.get(&sequence).map(Vec::as_slice).unwrap_or_default();
        if sequence_finals.len() != 1 {
            errors.push(ValidationError::new(
                &sequence_path,
                "TopN sequence must have exactly one final node",
            ));
            continue;
        }
        if expected_partials.is_empty() {
            errors.push(ValidationError::new(
                &sequence_path,
                "TopN sequence final has no partial producer",
            ));
            continue;
        }
        let (final_fragment_id, final_node_id) = sequence_finals[0];
        let Some(final_fragment) = plan.fragments().get(&final_fragment_id) else {
            continue;
        };
        let Some(final_node) = final_fragment.nodes().get(&final_node_id) else {
            continue;
        };
        let NodeKind::TopN {
            order_by,
            limit,
            offset,
            phase: crate::TopNPhase::Final { .. },
        } = &final_node.kind
        else {
            continue;
        };
        let Some(required_partial_limit) = limit.checked_add(*offset) else {
            continue;
        };
        let Some(expected_ordering) = derive_ordering(final_fragment, &[], order_by) else {
            continue;
        };
        let Some(input) = final_node.inputs.first().copied() else {
            continue;
        };
        let mut matched = BTreeSet::new();
        let all_paths_match = trace_topn_reduction_inputs(
            plan,
            sequence,
            (final_fragment_id, input),
            expected_ordering,
            required_partial_limit,
            &mut matched,
            &mut trace_budget,
            &mut trace_indexes,
        );
        if !all_paths_match || matched != expected_partials {
            errors.push(ValidationError::new(
                &sequence_path,
                "TopN partial paths do not reduce exactly into their matching final",
            ));
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn trace_topn_reduction_inputs(
    plan: &PhysicalPlan,
    sequence: crate::TopNSequenceId,
    start: (FragmentId, NodeId),
    initial_ordering: Vec<crate::OrderingKey>,
    required_partial_limit: u64,
    matched: &mut BTreeSet<(FragmentId, NodeId)>,
    trace_budget: &mut SemanticTraceWorkBudget,
    trace_indexes: &mut SemanticTraceIndexes,
) -> bool {
    let mut pending = vec![(start, initial_ordering)];
    let mut visited = BTreeSet::new();
    while let Some((node_ref, expected_ordering)) = pending.pop() {
        if !trace_budget.charge(expected_ordering.len().saturating_add(1))
            || !visited.insert((node_ref.0, node_ref.1, expected_ordering.clone()))
        {
            return false;
        }
        let Some(fragment) = plan.fragments().get(&node_ref.0) else {
            return false;
        };
        let Some(node) = fragment.nodes().get(&node_ref.1) else {
            return false;
        };
        match &node.kind {
            NodeKind::TopN {
                limit,
                offset,
                phase:
                    crate::TopNPhase::Partial {
                        sequence: partial_sequence,
                    },
                ..
            } => {
                if *partial_sequence != sequence
                    || *offset != 0
                    || *limit != required_partial_limit
                    || node.output_properties.ordering.as_ref() != expected_ordering.as_slice()
                {
                    return false;
                }
                if !matched.insert(node_ref) {
                    return false;
                }
            }
            NodeKind::ExchangeSource { edge, .. } => {
                let Some(edge) = plan.edges().get(edge) else {
                    return false;
                };
                if edge.kind != crate::EdgeKind::Stream
                    || edge.destination.fragment != node_ref.0
                    || edge.destination.node != node_ref.1
                    || edge.partitioning.source != Distribution::Singleton
                    || edge.partitioning.destination != Distribution::Singleton
                {
                    return false;
                }
                let Some(mapped_values) = trace_indexes.map_edge_values(
                    edge.id,
                    &edge.destination.receive_mapping,
                    &expected_ordering
                        .iter()
                        .map(|key| key.value)
                        .collect::<Vec<_>>(),
                    true,
                    trace_budget,
                ) else {
                    return false;
                };
                let mapped = expected_ordering
                    .iter()
                    .zip(mapped_values)
                    .map(|(key, value)| crate::OrderingKey {
                        value,
                        direction: key.direction,
                        null_ordering: key.null_ordering,
                    })
                    .collect();
                let Some(source) = plan.fragments().get(&edge.source.fragment) else {
                    return false;
                };
                pending.push(((source.id(), source.root()), mapped));
            }
            NodeKind::Project { .. } => {
                let Some(input) = node.inputs.first().copied() else {
                    return false;
                };
                let Some(child) = fragment.nodes().get(&input) else {
                    return false;
                };
                if !trace_indexes.port_contains_all(
                    fragment.id(),
                    child,
                    expected_ordering.iter().map(|key| key.value),
                    expected_ordering.len(),
                    trace_budget,
                ) {
                    return false;
                }
                pending.push(((fragment.id(), input), expected_ordering));
            }
            // An aggregate keeps one row per group, so pruning below it is
            // sound exactly when the order it is pruned by is the grouping
            // itself: every ordering key is one of this node's group keys and
            // every group key is ordered by. The order then continues over the
            // values those keys read.
            NodeKind::Aggregate { group_by, .. } => {
                if group_by.len() != expected_ordering.len() {
                    return false;
                }
                let Some(input) = node.inputs.first().copied() else {
                    return false;
                };
                let mut mapped = Vec::with_capacity(expected_ordering.len());
                for key in &expected_ordering {
                    let Some((expression, _)) =
                        group_by.iter().find(|(_, output)| *output == key.value)
                    else {
                        return false;
                    };
                    let Some(source) =
                        fragment
                            .expressions()
                            .get(*expression)
                            .and_then(|expression| match expression.kind {
                                crate::ExprKind::Value(source) => Some(source),
                                _ => None,
                            })
                    else {
                        return false;
                    };
                    mapped.push(crate::OrderingKey {
                        value: source,
                        direction: key.direction,
                        null_ordering: key.null_ordering,
                    });
                }
                if !trace_budget.charge(mapped.len().saturating_add(1)) {
                    return false;
                }
                pending.push(((fragment.id(), input), mapped));
            }
            NodeKind::SetOp {
                kind: crate::SetOperationKind::UnionAll,
                input_mappings,
            } => {
                if node.inputs.len() != input_mappings.len() || node.inputs.is_empty() {
                    return false;
                }
                let expected_values = expected_ordering
                    .iter()
                    .map(|key| key.value)
                    .collect::<Vec<_>>();
                for (input_ordinal, (input, mapping)) in
                    node.inputs.iter().zip(input_mappings).enumerate()
                {
                    let mapped_values = trace_indexes.map_union_values(
                        fragment.id(),
                        node.id,
                        input_ordinal,
                        &node.output.columns,
                        mapping,
                        &expected_values,
                        true,
                        trace_budget,
                    );
                    let Some(mapped_values) = mapped_values else {
                        return false;
                    };
                    let mapped = expected_ordering
                        .iter()
                        .zip(mapped_values)
                        .map(|(key, value)| crate::OrderingKey {
                            value,
                            direction: key.direction,
                            null_ordering: key.null_ordering,
                        })
                        .collect();
                    pending.push(((fragment.id(), *input), mapped));
                }
            }
            _ => return false,
        }
    }
    true
}
