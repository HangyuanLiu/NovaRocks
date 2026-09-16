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
    AggregatePhase, Distribution, ExprId, Fragment, FragmentCuts, NodeKind, PhysicalNode,
    PhysicalPlan, RowMultiplicity, ValueId,
};

pub(crate) fn validate_fragment_partition_identities(
    fragment: &Fragment,
    cuts: &FragmentCuts,
    errors: &mut ValidationContext,
) {
    let mut spaces = BTreeMap::new();
    let mut counts = BTreeMap::new();
    for node in fragment.nodes().values() {
        for (ordinal, properties) in node.required_inputs.iter().enumerate() {
            register_partition_identity(
                &properties.distribution,
                &format!("nodes[{}].required_inputs[{ordinal}]", node.id.get()),
                &mut spaces,
                &mut counts,
                errors,
            );
        }
        register_partition_identity(
            &node.output_properties.distribution,
            &format!("nodes[{}].output_properties", node.id.get()),
            &mut spaces,
            &mut counts,
            errors,
        );
        if let NodeKind::TableWriter { target } = &node.kind {
            register_partition_identity(
                &target.required_distribution,
                &format!("nodes[{}].writer.required_distribution", node.id.get()),
                &mut spaces,
                &mut counts,
                errors,
            );
        }
    }
    for (direction, ordinal, partitioning) in cuts
        .inbound
        .iter()
        .enumerate()
        .flat_map(|(ordinal, cut)| {
            [
                ("inbound.source", ordinal, &cut.partitioning.source),
                (
                    "inbound.destination",
                    ordinal,
                    &cut.partitioning.destination,
                ),
            ]
        })
        .chain(cuts.outbound.iter().enumerate().flat_map(|(ordinal, cut)| {
            [
                ("outbound.source", ordinal, &cut.partitioning.source),
                (
                    "outbound.destination",
                    ordinal,
                    &cut.partitioning.destination,
                ),
            ]
        }))
    {
        register_partition_identity(
            partitioning,
            &format!("cuts.{direction}[{ordinal}].partitioning"),
            &mut spaces,
            &mut counts,
            errors,
        );
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PartitionSpaceDefinition {
    Hash(crate::HashPartitionScheme),
    Bucket(crate::BucketPartitionScheme),
}

pub(crate) fn validate_partition_identities(plan: &PhysicalPlan, errors: &mut ValidationContext) {
    let mut spaces = BTreeMap::new();
    let mut counts = BTreeMap::new();
    for fragment in plan.fragments().values() {
        for node in fragment.nodes().values() {
            for (ordinal, properties) in node.required_inputs.iter().enumerate() {
                register_partition_identity(
                    &properties.distribution,
                    &format!(
                        "fragments[{}].nodes[{}].required_inputs[{ordinal}]",
                        fragment.id().get(),
                        node.id.get()
                    ),
                    &mut spaces,
                    &mut counts,
                    errors,
                );
            }
            register_partition_identity(
                &node.output_properties.distribution,
                &format!(
                    "fragments[{}].nodes[{}].output_properties",
                    fragment.id().get(),
                    node.id.get()
                ),
                &mut spaces,
                &mut counts,
                errors,
            );
            if let NodeKind::TableWriter { target } = &node.kind {
                register_partition_identity(
                    &target.required_distribution,
                    &format!(
                        "fragments[{}].nodes[{}].writer.required_distribution",
                        fragment.id().get(),
                        node.id.get()
                    ),
                    &mut spaces,
                    &mut counts,
                    errors,
                );
            }
        }
    }
    for edge in plan.edges().values() {
        register_partition_identity(
            &edge.partitioning.source,
            &format!("edges[{}].partitioning.source", edge.id.get()),
            &mut spaces,
            &mut counts,
            errors,
        );
        register_partition_identity(
            &edge.partitioning.destination,
            &format!("edges[{}].partitioning.destination", edge.id.get()),
            &mut spaces,
            &mut counts,
            errors,
        );
    }
}

pub(crate) fn register_partition_identity(
    distribution: &Distribution,
    path: &str,
    spaces: &mut BTreeMap<novarocks_type_contract::PartitionSpaceId, PartitionSpaceDefinition>,
    counts: &mut BTreeMap<
        novarocks_type_contract::PartitionCountParameterId,
        crate::PartitionCountDomain,
    >,
    errors: &mut ValidationContext,
) {
    let (space, definition) = match distribution {
        Distribution::Hash { scheme, .. } => {
            if counts
                .insert(scheme.count.id, scheme.count.admissible)
                .is_some_and(|existing| existing != scheme.count.admissible)
            {
                errors.push(ValidationError::new(
                    path,
                    "partition-count parameter identity has conflicting admissible domains",
                ));
            }
            (scheme.space, PartitionSpaceDefinition::Hash(scheme.clone()))
        }
        Distribution::BucketShuffle { scheme, .. } => (
            scheme.space,
            PartitionSpaceDefinition::Bucket(scheme.clone()),
        ),
        Distribution::Unconstrained
        | Distribution::Singleton
        | Distribution::RoundRobin
        | Distribution::Broadcast => return,
    };
    if spaces
        .insert(space, definition.clone())
        .is_some_and(|existing| existing != definition)
    {
        errors.push(ValidationError::new(
            path,
            "partition-space identity has conflicting definitions",
        ));
    }
}

pub(crate) fn validate_distribution(
    fragment: &Fragment,
    distribution: &Distribution,
    label: &str,
    errors: &mut ValidationContext,
) {
    let path = format!("fragments[{}].{label}", fragment.id().get());
    let (keys, algorithm) = match distribution {
        Distribution::Hash { keys, scheme } => {
            if scheme.definition.algorithm
                != novarocks_type_contract::PartitionHashAlgorithm::NativeExchangeV1
            {
                errors.push(ValidationError::new(
                    &path,
                    "ordinary hash partitioning uses the wrong hash algorithm",
                ));
            }
            validate_partition_count_domain(&scheme.count.admissible, &path, errors);
            (Some(keys), Some(scheme.definition.algorithm))
        }
        Distribution::BucketShuffle { keys, scheme } => {
            if scheme.bucket_count == 0 || scheme.bucket_count > MAX_PARTITION_COUNT {
                errors.push(ValidationError::new(
                    &path,
                    "bucket partition count must be within the supported bound",
                ));
            }
            if scheme.hash != novarocks_type_contract::PartitionHashAlgorithm::NativeBucketCrc32V1 {
                errors.push(ValidationError::new(
                    &path,
                    "bucket partitioning uses the wrong hash algorithm",
                ));
            }
            if scheme.layout != novarocks_type_contract::BucketLayoutAlgorithm::DenseZeroBasedV1
                || scheme.ordinal_domain.first_ordinal != 0
                || scheme.ordinal_domain.ordinal_count != scheme.bucket_count
                || scheme.ordinal_domain.evidence_digest == [0; 32]
            {
                errors.push(ValidationError::new(
                    &path,
                    "bucket partitioning lacks a complete dense ordinal-domain proof",
                ));
            }
            (Some(keys), Some(scheme.hash))
        }
        Distribution::Unconstrained
        | Distribution::Singleton
        | Distribution::RoundRobin
        | Distribution::Broadcast => (None, None),
    };
    if let Some(keys) = keys {
        if keys.is_empty() {
            errors.push(ValidationError::new(
                &path,
                "keyed distribution has no keys",
            ));
        }
        for value in keys.iter() {
            require_value(fragment, *value, &path, errors);
            if let (Some(algorithm), Some(value)) = (algorithm, fragment.values().get(value))
                && !algorithm.supports_partition_key(&value.ty.data_type)
            {
                errors.push(ValidationError::new(
                    &path,
                    "keyed distribution uses a data type outside its hash algorithm domain",
                ));
            }
        }
    }
}

pub(crate) fn validate_partition_count_domain(
    domain: &crate::PartitionCountDomain,
    path: &str,
    errors: &mut ValidationContext,
) {
    if domain.min == 0 || domain.min > domain.max || domain.max > MAX_PARTITION_COUNT {
        errors.push(ValidationError::new(
            path,
            "partition-count domain must be non-zero, ordered and bounded",
        ));
    }
    if domain.requires_power_of_two
        && domain
            .min
            .checked_next_power_of_two()
            .is_none_or(|first| first > domain.max)
    {
        errors.push(ValidationError::new(
            path,
            "power-of-two partition-count domain has no admissible member",
        ));
    }
}

pub(crate) fn properties_satisfy(
    actual: &crate::PhysicalProperties,
    required: &crate::PhysicalProperties,
) -> bool {
    let distribution = match &required.distribution {
        Distribution::Unconstrained => true,
        required => &actual.distribution == required,
    };
    distribution
        && actual.row_multiplicity == required.row_multiplicity
        && actual.ordering.len() >= required.ordering.len()
        && actual.ordering[..required.ordering.len()] == *required.ordering
}

pub(crate) fn nest_loop_join_output_distribution(
    fragment: &Fragment,
    node: &PhysicalNode,
    kind: crate::JoinKind,
    distribution: crate::NestLoopJoinDistribution,
    predicate: Option<ExprId>,
) -> Option<Distribution> {
    let inputs = node
        .inputs
        .iter()
        .map(|input| fragment.nodes().get(input))
        .collect::<Option<Vec<_>>>()?;
    if inputs.len() != 2 || node.required_inputs.len() != 2 {
        return None;
    }
    match distribution {
        crate::NestLoopJoinDistribution::Singleton => {
            let complete = inputs
                .iter()
                .zip(&node.required_inputs)
                .all(|(input, required)| {
                    input.output_properties.distribution == Distribution::Singleton
                        && required.distribution == Distribution::Singleton
                        && input.output_properties.row_multiplicity == RowMultiplicity::SingleCopy
                        && required.row_multiplicity == RowMultiplicity::SingleCopy
                });
            complete.then_some(Distribution::Singleton)
        }
        crate::NestLoopJoinDistribution::BroadcastRight => {
            if !matches!(
                kind,
                crate::JoinKind::Cross
                    | crate::JoinKind::Inner
                    | crate::JoinKind::LeftOuter
                    | crate::JoinKind::LeftSemi
                    | crate::JoinKind::LeftAnti
                    | crate::JoinKind::NullAwareLeftAnti
            ) || inputs[1].output_properties.distribution != Distribution::Broadcast
                || node.required_inputs[1].distribution != Distribution::Broadcast
                || inputs[1].output_properties.row_multiplicity != RowMultiplicity::Replicated
                || node.required_inputs[1].row_multiplicity != RowMultiplicity::Replicated
                || inputs[0].output_properties.row_multiplicity != RowMultiplicity::SingleCopy
                || node.required_inputs[0].row_multiplicity != RowMultiplicity::SingleCopy
                || node.required_inputs[0].distribution != inputs[0].output_properties.distribution
            {
                return None;
            }
            let mut output = inputs[0].output_properties.distribution.clone();
            if output == Distribution::Broadcast
                && predicate.is_some_and(|predicate| {
                    !fragment_expressions_are_replica_deterministic(
                        fragment,
                        std::iter::once(predicate),
                        true,
                    )
                })
            {
                output = Distribution::Unconstrained;
            }
            Some(output)
        }
    }
}

pub(crate) fn set_operation_output_distribution(
    fragment: &Fragment,
    node: &PhysicalNode,
    kind: crate::SetOperationKind,
) -> Option<Distribution> {
    let NodeKind::SetOp { input_mappings, .. } = &node.kind else {
        return None;
    };
    let inputs = node
        .inputs
        .iter()
        .map(|input| fragment.nodes().get(input))
        .collect::<Option<Vec<_>>>()?;
    if inputs.len() < 2
        || inputs.len() != input_mappings.len()
        || inputs.len() != node.required_inputs.len()
        || inputs
            .iter()
            .any(|input| input.output_properties.row_multiplicity != RowMultiplicity::SingleCopy)
        || node
            .required_inputs
            .iter()
            .any(|required| required.row_multiplicity != RowMultiplicity::SingleCopy)
    {
        return None;
    }
    if kind == crate::SetOperationKind::UnionAll {
        return Some(
            if inputs
                .iter()
                .all(|input| input.output_properties.distribution == Distribution::Singleton)
            {
                Distribution::Singleton
            } else {
                Distribution::Unconstrained
            },
        );
    }
    let exact_inputs = inputs
        .iter()
        .zip(&node.required_inputs)
        .all(|(input, required)| required.distribution == input.output_properties.distribution);
    if !exact_inputs {
        return None;
    }
    if inputs
        .iter()
        .all(|input| input.output_properties.distribution == Distribution::Singleton)
    {
        return Some(Distribution::Singleton);
    }

    let comparison_pattern = occurrence_equivalence_pattern(input_mappings.first()?);
    if input_mappings
        .iter()
        .skip(1)
        .any(|mapping| occurrence_equivalence_pattern(mapping) != comparison_pattern)
    {
        return None;
    }
    let mut output_sources = BTreeMap::new();
    for (output, source) in node.output.columns.iter().zip(input_mappings.first()?) {
        if output_sources
            .insert(*output, *source)
            .is_some_and(|existing| existing != *source)
        {
            return None;
        }
    }
    let representative_ordinals = comparison_pattern
        .iter()
        .enumerate()
        .filter_map(|(ordinal, representative)| (*representative == ordinal).then_some(ordinal))
        .collect::<Vec<_>>();

    let hash_scheme =
        inputs
            .iter()
            .zip(input_mappings)
            .try_fold(None, |expected, (input, mapping)| {
                match &input.output_properties.distribution {
                    Distribution::Hash { keys, scheme }
                        if mapping_keys_match(keys, mapping, &representative_ordinals) =>
                    {
                        match expected {
                            None => Some(Some(scheme)),
                            Some(expected) if expected == scheme => Some(Some(expected)),
                            Some(_) => None,
                        }
                    }
                    _ => None,
                }
            });
    if let Some(Some(scheme)) = hash_scheme {
        return Some(Distribution::Hash {
            keys: representative_ordinals
                .iter()
                .map(|ordinal| node.output.columns[*ordinal])
                .collect(),
            scheme: scheme.clone(),
        });
    }

    let bucket_scheme =
        inputs
            .iter()
            .zip(input_mappings)
            .try_fold(None, |expected, (input, mapping)| {
                match &input.output_properties.distribution {
                    Distribution::BucketShuffle { keys, scheme }
                        if mapping_keys_match(keys, mapping, &representative_ordinals) =>
                    {
                        match expected {
                            None => Some(Some(scheme)),
                            Some(expected) if expected == scheme => Some(Some(expected)),
                            Some(_) => None,
                        }
                    }
                    _ => None,
                }
            });
    bucket_scheme
        .flatten()
        .map(|scheme| Distribution::BucketShuffle {
            keys: representative_ordinals
                .iter()
                .map(|ordinal| node.output.columns[*ordinal])
                .collect(),
            scheme: scheme.clone(),
        })
}

pub(crate) fn occurrence_equivalence_pattern(values: &[ValueId]) -> Vec<usize> {
    let mut representatives = BTreeMap::new();
    values
        .iter()
        .enumerate()
        .map(|(ordinal, value)| *representatives.entry(*value).or_insert(ordinal))
        .collect()
}

pub(crate) fn mapping_keys_match(
    keys: &[ValueId],
    mapping: &[ValueId],
    ordinals: &[usize],
) -> bool {
    keys.iter()
        .copied()
        .eq(ordinals.iter().map(|ordinal| mapping[*ordinal]))
}

pub(crate) fn validate_node_output_properties(
    fragment: &Fragment,
    node: &PhysicalNode,
    path: &str,
    errors: &mut ValidationContext,
) {
    let empty = crate::PhysicalProperties {
        distribution: Distribution::Unconstrained,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    };
    let expected = match &node.kind {
        NodeKind::Scan { relation, .. } => {
            if relation.provided_properties().row_multiplicity != RowMultiplicity::SingleCopy
                || relation.provided_properties().distribution == Distribution::Broadcast
            {
                errors.push(ValidationError::new(
                    path,
                    "scan output requires single-copy provider work ownership",
                ));
            }
            Some(relation.provided_properties())
        }
        NodeKind::Filter { predicates } => {
            let Some(input) = node
                .inputs
                .first()
                .and_then(|input| fragment.nodes().get(input))
            else {
                return;
            };
            let expected = crate::derive_filter_output_properties(
                &input.output_properties,
                fragment_expressions_are_replica_deterministic(
                    fragment,
                    predicates.iter().copied(),
                    true,
                ),
            );
            if node.output_properties != expected {
                errors.push(ValidationError::new(
                    path,
                    "filter output properties exceed its deterministic predicate proof",
                ));
            }
            return;
        }
        NodeKind::Limit { .. } => {
            let Some(input) = node
                .inputs
                .first()
                .and_then(|input| fragment.nodes().get(input))
            else {
                return;
            };
            if input.output_properties.row_multiplicity != RowMultiplicity::SingleCopy
                || node.output_properties
                    != (crate::PhysicalProperties {
                        distribution: Distribution::Singleton,
                        row_multiplicity: RowMultiplicity::SingleCopy,
                        ordering: input.output_properties.ordering.clone(),
                    })
            {
                errors.push(ValidationError::new(
                    path,
                    "limit output properties exceed its replica-equivalence proof",
                ));
            }
            return;
        }
        NodeKind::Project { expressions } => {
            let Some(input) = node
                .inputs
                .first()
                .and_then(|input| fragment.nodes().get(input))
            else {
                return;
            };
            let expected = crate::derive_project_output_properties(
                &input.output_properties,
                &node.output.columns,
                fragment_expressions_are_replica_deterministic(
                    fragment,
                    expressions.iter().map(|(expression, _)| *expression),
                    true,
                ),
            );
            if node.output_properties != expected {
                errors.push(ValidationError::new(
                    path,
                    "project output properties differ from the guarantees preserved by its output",
                ));
            }
            return;
        }
        NodeKind::Sort { order_by, mode } => {
            let Some(input) = node
                .inputs
                .first()
                .and_then(|input| fragment.nodes().get(input))
            else {
                return;
            };
            let required = node.required_inputs.first();
            let partition_by = match mode {
                crate::SortMode::Global => &[][..],
                crate::SortMode::Analytic { partition_by }
                | crate::SortMode::PartitionTopN { partition_by, .. } => partition_by,
            };
            let partition_values = direct_order_values(fragment, partition_by);
            let expected_ordering = derive_ordering(fragment, partition_by, order_by);
            let distribution_valid = match mode {
                crate::SortMode::Global => {
                    required.is_some_and(|required| {
                        required.distribution == Distribution::Singleton
                            && required.ordering.is_empty()
                    }) && input.output_properties.distribution == Distribution::Singleton
                        && node.output_properties.distribution == Distribution::Singleton
                }
                crate::SortMode::Analytic { .. } | crate::SortMode::PartitionTopN { .. } => {
                    partition_values.is_some_and(|keys| {
                        required.is_some_and(|required| {
                            required.ordering.is_empty()
                                && required.distribution == input.output_properties.distribution
                        }) && input.output_properties.distribution
                            == node.output_properties.distribution
                            && distribution_colocates_by(
                                &input.output_properties.distribution,
                                &keys,
                            )
                    })
                }
            };
            let multiplicity_valid = match mode {
                crate::SortMode::Global => {
                    required.is_some_and(|required| {
                        required.row_multiplicity == RowMultiplicity::SingleCopy
                    }) && input.output_properties.row_multiplicity == RowMultiplicity::SingleCopy
                        && node.output_properties.row_multiplicity == RowMultiplicity::SingleCopy
                }
                crate::SortMode::Analytic { .. } | crate::SortMode::PartitionTopN { .. } => {
                    required.is_some_and(|required| {
                        required.row_multiplicity == input.output_properties.row_multiplicity
                    }) && node.output_properties.row_multiplicity
                        == input.output_properties.row_multiplicity
                }
            };
            if !distribution_valid || !multiplicity_valid {
                errors.push(ValidationError::new(
                    path,
                    "sort mode lacks its exact input and output distribution contract",
                ));
            }
            if expected_ordering.as_deref() != Some(node.output_properties.ordering.as_ref()) {
                errors.push(ValidationError::new(
                    path,
                    "sort output ordering differs from its exact partition and order keys",
                ));
            }
            return;
        }
        NodeKind::TopN {
            order_by, phase, ..
        } => {
            let Some(input) = node
                .inputs
                .first()
                .and_then(|input| fragment.nodes().get(input))
            else {
                return;
            };
            let required = node.required_inputs.first();
            let distribution_valid = match phase {
                crate::TopNPhase::Single | crate::TopNPhase::Final { .. } => {
                    required.is_some_and(|required| {
                        required.distribution == Distribution::Singleton
                            && required.ordering.is_empty()
                    }) && input.output_properties.distribution == Distribution::Singleton
                        && node.output_properties.distribution == Distribution::Singleton
                }
                crate::TopNPhase::Partial { .. } => {
                    required.is_some_and(|required| {
                        required.distribution == input.output_properties.distribution
                            && required.ordering.is_empty()
                    }) && node.output_properties.distribution
                        == input.output_properties.distribution
                }
            };
            let multiplicity_valid = match phase {
                crate::TopNPhase::Single | crate::TopNPhase::Final { .. } => {
                    required.is_some_and(|required| {
                        required.row_multiplicity == RowMultiplicity::SingleCopy
                    }) && input.output_properties.row_multiplicity == RowMultiplicity::SingleCopy
                        && node.output_properties.row_multiplicity == RowMultiplicity::SingleCopy
                }
                crate::TopNPhase::Partial { .. } => {
                    required.is_some_and(|required| {
                        required.row_multiplicity == input.output_properties.row_multiplicity
                    }) && node.output_properties.row_multiplicity
                        == input.output_properties.row_multiplicity
                }
            };
            if !distribution_valid || !multiplicity_valid {
                errors.push(ValidationError::new(
                    path,
                    "TopN phase lacks its exact distribution contract",
                ));
            }
            if derive_ordering(fragment, &[], order_by).as_deref()
                != Some(node.output_properties.ordering.as_ref())
            {
                errors.push(ValidationError::new(
                    path,
                    "TopN output ordering differs from its exact order keys",
                ));
            }
            return;
        }
        NodeKind::Window(spec) => {
            validate_window_properties(fragment, node, spec, path, errors);
            return;
        }
        NodeKind::AssertOneRow(spec) => {
            validate_assertion_properties(fragment, node, spec, path, errors);
            return;
        }
        NodeKind::TableFunction {
            function,
            arguments,
            outputs,
            ..
        } => {
            validate_table_function_properties(
                fragment, node, function, arguments, outputs, path, errors,
            );
            return;
        }
        NodeKind::ExchangeSource { .. } => return,
        NodeKind::Values { .. } => {
            let NodeKind::Values { rows } = &node.kind else {
                unreachable!();
            };
            let valid = node.output_properties.ordering.is_empty()
                && match (&node.output_properties.distribution, rows.is_empty()) {
                    (Distribution::Unconstrained, true) | (Distribution::Singleton, _) => {
                        node.output_properties.row_multiplicity == RowMultiplicity::SingleCopy
                    }
                    (Distribution::Broadcast, _) => {
                        node.output_properties.row_multiplicity == RowMultiplicity::Replicated
                            && fragment_expressions_are_replica_deterministic(
                                fragment,
                                rows.iter().flat_map(|row| row.iter().copied()),
                                false,
                            )
                    }
                    (
                        Distribution::Unconstrained
                        | Distribution::RoundRobin
                        | Distribution::Hash { .. }
                        | Distribution::BucketShuffle { .. },
                        _,
                    ) => false,
                };
            if !valid {
                errors.push(ValidationError::new(
                    path,
                    "VALUES distribution and row multiplicity lack an exact placement proof",
                ));
            }
            return;
        }
        NodeKind::Aggregate { group_by, calls } => {
            let input = node
                .inputs
                .first()
                .and_then(|input| fragment.nodes().get(input));
            let output_values = node.output.columns.iter().copied().collect::<BTreeSet<_>>();
            let distribution = input
                .map(|input| match &input.output_properties.distribution {
                    Distribution::Singleton => Distribution::Singleton,
                    Distribution::Hash { keys, .. } | Distribution::BucketShuffle { keys, .. }
                        if keys.iter().all(|key| output_values.contains(key)) =>
                    {
                        input.output_properties.distribution.clone()
                    }
                    Distribution::Unconstrained
                    | Distribution::RoundRobin
                    | Distribution::Broadcast
                    | Distribution::Hash { .. }
                    | Distribution::BucketShuffle { .. } => Distribution::Unconstrained,
                })
                .unwrap_or(Distribution::Unconstrained);
            let consumes_complete_groups = calls.is_empty()
                || calls.iter().any(|call| {
                    matches!(
                        call.binding.phase,
                        AggregatePhase::Single | AggregatePhase::Final { .. }
                    )
                });
            if consumes_complete_groups {
                let required = node.required_inputs.first();
                let grouping_values = group_by
                    .iter()
                    .map(|(expression, _)| {
                        crate::expression_value(fragment.expressions(), *expression)
                    })
                    .collect::<Option<Vec<_>>>();
                let colocated = input.is_some_and(|input| {
                    required.is_some_and(|required| {
                        required.distribution == input.output_properties.distribution
                    }) && if group_by.is_empty() {
                        input.output_properties.distribution == Distribution::Singleton
                    } else {
                        grouping_values.as_deref().is_some_and(|keys| {
                            distribution_colocates_by(&input.output_properties.distribution, keys)
                        })
                    }
                });
                if !colocated {
                    errors.push(ValidationError::new(
                        path,
                        "aggregate finalization lacks complete group co-location",
                    ));
                }
            }
            let single_copy = input.is_some_and(|input| {
                input.output_properties.row_multiplicity == RowMultiplicity::SingleCopy
            }) && node
                .required_inputs
                .first()
                .is_some_and(|required| required.row_multiplicity == RowMultiplicity::SingleCopy);
            if !single_copy
                || node.output_properties
                    != (crate::PhysicalProperties {
                        distribution,
                        row_multiplicity: RowMultiplicity::SingleCopy,
                        ordering: Box::default(),
                    })
            {
                errors.push(ValidationError::new(
                    path,
                    "aggregate output properties differ from its proven input distribution",
                ));
            }
            return;
        }
        NodeKind::HashJoin {
            kind,
            keys,
            build_side,
            distribution,
            residual,
            ..
        } => {
            let inputs = node
                .inputs
                .iter()
                .filter_map(|input| fragment.nodes().get(input))
                .collect::<Vec<_>>();
            if inputs.len() != 2 {
                return;
            }
            let mut output_distribution = match (kind, distribution, build_side) {
                (_, crate::JoinDistribution::Singleton, _) => Distribution::Singleton,
                (crate::JoinKind::Inner, _, crate::JoinSide::Left) => {
                    inputs[1].output_properties.distribution.clone()
                }
                (
                    crate::JoinKind::Inner
                    | crate::JoinKind::LeftOuter
                    | crate::JoinKind::LeftSemi
                    | crate::JoinKind::LeftAnti
                    | crate::JoinKind::NullAwareLeftAnti,
                    _,
                    _,
                ) => inputs[0].output_properties.distribution.clone(),
                (
                    crate::JoinKind::RightOuter
                    | crate::JoinKind::RightSemi
                    | crate::JoinKind::RightAnti,
                    _,
                    _,
                ) => inputs[1].output_properties.distribution.clone(),
                (crate::JoinKind::FullOuter | crate::JoinKind::Cross, _, _) => {
                    Distribution::Unconstrained
                }
            };
            if output_distribution == Distribution::Broadcast
                && (inputs
                    .iter()
                    .any(|input| input.output_properties.distribution != Distribution::Broadcast)
                    || !fragment_expressions_are_replica_deterministic(
                        fragment,
                        keys.iter()
                            .flat_map(|key| [key.left, key.right])
                            .chain(residual.iter().copied()),
                        true,
                    ))
            {
                output_distribution = Distribution::Unconstrained;
            }
            if node.output_properties
                != (crate::PhysicalProperties {
                    distribution: output_distribution,
                    row_multiplicity: RowMultiplicity::SingleCopy,
                    ordering: Box::default(),
                })
            {
                errors.push(ValidationError::new(
                    path,
                    "hash join output properties differ from its preserved partition side",
                ));
            }
            return;
        }
        NodeKind::NestLoopJoin {
            kind,
            distribution,
            predicate,
            ..
        } => {
            let Some(distribution) = nest_loop_join_output_distribution(
                fragment,
                node,
                *kind,
                *distribution,
                *predicate,
            ) else {
                return;
            };
            if node.output_properties
                != (crate::PhysicalProperties {
                    distribution,
                    row_multiplicity: RowMultiplicity::SingleCopy,
                    ordering: Box::default(),
                })
            {
                errors.push(ValidationError::new(
                    path,
                    "nested-loop join output properties differ from its execution placement",
                ));
            }
            return;
        }
        NodeKind::SetOp { kind, .. } => {
            let Some(distribution) = set_operation_output_distribution(fragment, node, *kind)
            else {
                return;
            };
            if node.output_properties
                != (crate::PhysicalProperties {
                    distribution,
                    row_multiplicity: RowMultiplicity::SingleCopy,
                    ordering: Box::default(),
                })
            {
                errors.push(ValidationError::new(
                    path,
                    "set operation output properties differ from its equality co-location proof",
                ));
            }
            return;
        }
        NodeKind::Repeat { .. } | NodeKind::Unpivot { .. } => {
            let Some(input) = node
                .inputs
                .first()
                .and_then(|input| fragment.nodes().get(input))
            else {
                return;
            };
            let value_mapping = match &node.kind {
                NodeKind::Unpivot { spec } => {
                    spec.passthrough.iter().copied().collect::<BTreeMap<_, _>>()
                }
                NodeKind::Repeat { .. } => input
                    .output
                    .columns
                    .iter()
                    .copied()
                    .filter(|value| node.output.columns.contains(value))
                    .map(|value| (value, value))
                    .collect::<BTreeMap<_, _>>(),
                _ => unreachable!(),
            };
            let expected =
                crate::remap_properties_through_values(&input.output_properties, &value_mapping);
            if node.output_properties != expected {
                errors.push(ValidationError::new(
                    path,
                    "row-expanding operator output properties differ from its exact passthrough mapping",
                ));
            }
            return;
        }
        NodeKind::ChangeEventExpand { .. } => {
            let Some(input) = node
                .inputs
                .first()
                .and_then(|input| fragment.nodes().get(input))
            else {
                return;
            };
            if node.output_properties
                != (crate::PhysicalProperties {
                    distribution: Distribution::Unconstrained,
                    row_multiplicity: input.output_properties.row_multiplicity,
                    ordering: Box::default(),
                })
            {
                errors.push(ValidationError::new(
                    path,
                    "change-event expansion must declare unconstrained output properties",
                ));
            }
            return;
        }
        NodeKind::GenerateSeries { .. } => {
            if node.output_properties
                != (crate::PhysicalProperties {
                    distribution: Distribution::Singleton,
                    row_multiplicity: RowMultiplicity::SingleCopy,
                    ordering: Box::default(),
                })
            {
                errors.push(ValidationError::new(
                    path,
                    "generate-series requires singleton placement with single-copy row ownership",
                ));
            }
            return;
        }
        NodeKind::TableWriter { .. } => Some(&empty),
        NodeKind::TableFinish(_) => {
            let finish = crate::PhysicalProperties {
                distribution: Distribution::Singleton,
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            };
            if node.output_properties != finish {
                errors.push(ValidationError::new(
                    path,
                    "table finish output requires singleton placement with single-copy ownership",
                ));
            }
            return;
        }
    };
    if expected.is_some_and(|expected| expected != &node.output_properties) {
        errors.push(ValidationError::new(
            path,
            "node output properties are not proven by its operator semantics",
        ));
    }
}

/// Fragment-scoped wrapper over the arena-level check in `expression`.
pub(crate) fn fragment_expressions_are_replica_deterministic(
    fragment: &Fragment,
    expressions: impl IntoIterator<Item = ExprId>,
    allow_values: bool,
) -> bool {
    crate::expressions_are_replica_deterministic(fragment.expressions(), expressions, allow_values)
}

pub(crate) fn direct_order_values(
    fragment: &Fragment,
    ordering: &[crate::SortExpr],
) -> Option<Vec<ValueId>> {
    ordering
        .iter()
        .map(|item| crate::expression_value(fragment.expressions(), item.expr))
        .collect()
}

pub(crate) fn derive_ordering(
    fragment: &Fragment,
    partition_by: &[crate::SortExpr],
    order_by: &[crate::SortExpr],
) -> Option<Vec<crate::OrderingKey>> {
    crate::ordering_keys(fragment.expressions(), partition_by, order_by)
}

pub(crate) fn distribution_colocates_by(distribution: &Distribution, keys: &[ValueId]) -> bool {
    let key_index = ValuePortIndex::new(keys);
    match distribution {
        Distribution::Singleton => true,
        Distribution::Hash {
            keys: distribution_keys,
            ..
        }
        | Distribution::BucketShuffle {
            keys: distribution_keys,
            ..
        } => distribution_keys.iter().all(|key| key_index.contains(key)),
        Distribution::Unconstrained | Distribution::RoundRobin | Distribution::Broadcast => false,
    }
}

pub(crate) fn validate_window_properties(
    fragment: &Fragment,
    node: &PhysicalNode,
    spec: &crate::WindowSpec,
    path: &str,
    errors: &mut ValidationContext,
) {
    let Some(input) = node
        .inputs
        .first()
        .and_then(|input| fragment.nodes().get(input))
    else {
        return;
    };
    let Some(required) = node.required_inputs.first() else {
        return;
    };
    let expected_ordering = derive_ordering(fragment, &spec.partition_by, &spec.order_by);
    let partition_values = direct_order_values(fragment, &spec.partition_by);
    let distribution_valid = if spec.partition_by.is_empty() {
        required.distribution == Distribution::Singleton
            && input.output_properties.distribution == Distribution::Singleton
            && node.output_properties.distribution == Distribution::Singleton
    } else {
        partition_values.is_some_and(|keys| {
            properties_satisfy(&input.output_properties, required)
                && input.output_properties.distribution == node.output_properties.distribution
                && distribution_colocates_by(&input.output_properties.distribution, &keys)
        })
    };
    let single_copy = required.row_multiplicity == RowMultiplicity::SingleCopy
        && input.output_properties.row_multiplicity == RowMultiplicity::SingleCopy
        && node.output_properties.row_multiplicity == RowMultiplicity::SingleCopy;
    if !distribution_valid || !single_copy {
        errors.push(ValidationError::new(
            path,
            "window partition lacks its exact input and output distribution contract",
        ));
    }
    if expected_ordering.as_deref().is_none_or(|expected| {
        required.ordering.len() < expected.len()
            || required.ordering[..expected.len()] != *expected
            || input.output_properties.ordering.len() < expected.len()
            || input.output_properties.ordering[..expected.len()] != *expected
    }) || input.output_properties != node.output_properties
    {
        errors.push(ValidationError::new(
            path,
            "window child and output properties differ from its exact partition ordering",
        ));
    }
}

pub(crate) fn validate_assertion_properties(
    fragment: &Fragment,
    node: &PhysicalNode,
    spec: &crate::RowCountAssertionSpec,
    path: &str,
    errors: &mut ValidationContext,
) {
    let Some(input) = node
        .inputs
        .first()
        .and_then(|input| fragment.nodes().get(input))
    else {
        return;
    };
    let Some(required) = node.required_inputs.first() else {
        return;
    };
    let distribution_valid = match spec {
        crate::RowCountAssertionSpec::Global { .. } => {
            required.distribution == Distribution::Singleton
                && input.output_properties.distribution == Distribution::Singleton
                && node.output_properties.distribution == Distribution::Singleton
        }
        crate::RowCountAssertionSpec::PerKeyAtMostOne { keys, .. } => {
            properties_satisfy(&input.output_properties, required)
                && input.output_properties.distribution == node.output_properties.distribution
                && distribution_colocates_by(&input.output_properties.distribution, keys)
        }
    };
    let single_copy = required.row_multiplicity == RowMultiplicity::SingleCopy
        && input.output_properties.row_multiplicity == RowMultiplicity::SingleCopy
        && node.output_properties.row_multiplicity == RowMultiplicity::SingleCopy;
    if !distribution_valid || !single_copy || !required.ordering.is_empty() {
        errors.push(ValidationError::new(
            path,
            "row-count assertion lacks its exact distribution contract",
        ));
    }
    if node.output_properties != input.output_properties {
        errors.push(ValidationError::new(
            path,
            "row-count assertion does not preserve its child properties",
        ));
    }
}

pub(crate) fn validate_table_function_properties(
    fragment: &Fragment,
    node: &PhysicalNode,
    function: &crate::BoundTableFunction,
    arguments: &[ExprId],
    outputs: &[crate::TableFunctionOutput],
    path: &str,
    errors: &mut ValidationContext,
) {
    let Some(input) = node
        .inputs
        .first()
        .and_then(|input| fragment.nodes().get(input))
    else {
        if node.output_properties
            != (crate::PhysicalProperties {
                distribution: Distribution::Singleton,
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            })
        {
            errors.push(ValidationError::new(
                path,
                "standalone table function requires singleton placement with single-copy ownership",
            ));
        }
        return;
    };
    let passthrough = outputs
        .iter()
        .filter_map(|output| match output {
            crate::TableFunctionOutput::PassThrough(value) => Some(*value),
            crate::TableFunctionOutput::FunctionResult { .. } => None,
        })
        .collect::<BTreeSet<_>>();
    let distribution = match &input.output_properties.distribution {
        Distribution::Hash { keys, .. } | Distribution::BucketShuffle { keys, .. }
            if keys.iter().all(|key| passthrough.contains(key)) =>
        {
            input.output_properties.distribution.clone()
        }
        Distribution::Hash { .. } | Distribution::BucketShuffle { .. } => {
            Distribution::Unconstrained
        }
        Distribution::Broadcast
            if function.volatility != crate::FunctionVolatility::Immutable
                || !fragment_expressions_are_replica_deterministic(
                    fragment,
                    arguments.iter().copied(),
                    true,
                ) =>
        {
            Distribution::Unconstrained
        }
        distribution => distribution.clone(),
    };
    let ordering_len = input
        .output_properties
        .ordering
        .iter()
        .take_while(|key| passthrough.contains(&key.value))
        .count();
    let ordering = Box::from(&input.output_properties.ordering[..ordering_len]);
    if node.output_properties
        != (crate::PhysicalProperties {
            distribution,
            row_multiplicity: input.output_properties.row_multiplicity,
            ordering,
        })
    {
        errors.push(ValidationError::new(
            path,
            "table function output properties differ from its explicit passthrough guarantees",
        ));
    }
}

pub(crate) fn validate_property_keys_on_port(
    fragment: &Fragment,
    properties: &crate::PhysicalProperties,
    port_values: &ValuePortIndex,
    path: &str,
    errors: &mut ValidationContext,
) {
    if properties.distribution == Distribution::Broadcast
        && properties.row_multiplicity != RowMultiplicity::Replicated
    {
        errors.push(ValidationError::new(
            path,
            "broadcast physical properties require replicated row multiplicity",
        ));
    }
    let mut keys = properties
        .ordering
        .iter()
        .map(|key| key.value)
        .collect::<Vec<_>>();
    match &properties.distribution {
        Distribution::Hash {
            keys: partition_keys,
            ..
        }
        | Distribution::BucketShuffle {
            keys: partition_keys,
            ..
        } => keys.extend(partition_keys.iter().copied()),
        Distribution::Unconstrained
        | Distribution::Singleton
        | Distribution::RoundRobin
        | Distribution::Broadcast => {}
    }
    for key in keys {
        require_value(fragment, key, path, errors);
        if !port_values.contains(&key) {
            errors.push(ValidationError::new(
                path,
                format!(
                    "physical property key {} is absent from the port",
                    key.get()
                ),
            ));
        }
    }
}
