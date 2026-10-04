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

use crate::FrozenCallError;
use crate::frozen_calls::OccurrencePropertyProof;
use crate::{
    Distribution, ExprId, Fragment, FragmentCuts, NodeId, NodeKind, PhysicalNode, PhysicalPlan,
    RowMultiplicity, ValueId,
};
use novarocks_type_contract::CompileCheckpoints;

/// Only old standalone definition validation consults migration-era binding
/// bits. The frozen package route supplies a checked proof of the same source
/// and its original meter; it cannot fall back to legacy expression facts.
pub(crate) enum PropertyEffectSource<'proof, 'work, 'control> {
    Legacy,
    Frozen {
        proof: &'proof OccurrencePropertyProof<'proof>,
        work: &'work mut CompileCheckpoints<'control>,
    },
}
impl PropertyEffectSource<'_, '_, '_> {
    fn expressions_safe(
        &mut self,
        fragment: &Fragment,
        node: NodeId,
        expressions: impl IntoIterator<Item = ExprId>,
        allow_values: bool,
    ) -> Result<bool, FrozenCallError> {
        match self {
            Self::Legacy => Ok(fragment_expressions_are_replica_deterministic(
                fragment,
                expressions,
                allow_values,
            )),
            Self::Frozen { proof, work } => {
                proof.require_fragment(fragment, work)?;
                if !allow_values {
                    // Preserve the original static closed-value prerequisite
                    // separately from actual occurrence effect authority.
                    // The original graph walker allocates opaque scratch;
                    // this boundary does not claim its internal cooperation.
                    work.flush()?;
                    let closed = crate::expression::expressions_have_closed_value_scope(
                        fragment.expressions(),
                        expressions,
                    );
                    work.step()?;
                    work.flush()?;
                    if !closed {
                        return Ok(false);
                    }
                }
                proof.replica_safe(node, work)
            }
        }
    }

    fn table_safe(
        &mut self,
        fragment: &Fragment,
        node: NodeId,
        function: &crate::BoundTableFunction,
        arguments: &[ExprId],
    ) -> Result<bool, FrozenCallError> {
        match self {
            Self::Legacy => Ok(function.volatility == crate::FunctionVolatility::Immutable
                && fragment_expressions_are_replica_deterministic(
                    fragment,
                    arguments.iter().copied(),
                    true,
                )),
            Self::Frozen { proof, work } => {
                proof.require_fragment(fragment, work)?;
                proof.replica_safe(node, work)
            }
        }
    }
}

#[derive(Debug)]
pub enum FragmentPropertyError {
    Control(novarocks_type_contract::CompileControlError),
    Calls(FrozenCallError),
    Structure(ValidationErrors),
}
impl From<FrozenCallError> for FragmentPropertyError {
    fn from(error: FrozenCallError) -> Self {
        match error {
            FrozenCallError::Control(cause) => Self::Control(cause),
            error => Self::Calls(error),
        }
    }
}
impl From<novarocks_type_contract::CompileControlError> for FragmentPropertyError {
    fn from(cause: novarocks_type_contract::CompileControlError) -> Self {
        Self::Control(cause)
    }
}
impl std::fmt::Display for FragmentPropertyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Control(cause) => cause.fmt(f),
            Self::Calls(cause) => cause.fmt(f),
            Self::Structure(cause) => cause.fmt(f),
        }
    }
}
impl std::error::Error for FragmentPropertyError {}

/// Validate the sole output-property formulas against this fragment's exact
/// invocation claims. This stage neither derives/replans required inputs nor
/// authenticates an installed kernel. The final package must still validate
/// cuts, constants, parameters, pruning and all other application-owned facts.
///
/// The explicit projection ceilings admit the occurrence index. Existing
/// structural validators and property formulas retain opaque scratch work;
/// this API does not claim a complete first-allocation model or host MEM grant.
pub fn validate_fragment_output_properties_observed(
    fragment: &Fragment,
    uses: &crate::PhysicalRootUses,
    calls: &crate::FrozenFragmentCalls,
    limits: PlanLimits,
    source_retained_bytes: usize,
    projection_limits: crate::PropertyProofProjectionLimits,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<crate::PropertyProofProjectionFacts, FragmentPropertyError> {
    let mut work =
        CompileCheckpoints::try_new(control, novarocks_type_contract::CompilePhase::Validate)?;
    let result = (|| {
        work.flush()?;
        let proof = calls.property_proof(
            fragment,
            uses,
            &limits,
            source_retained_bytes,
            projection_limits,
            control,
        )?;
        work.flush()?;
        let structure = super::validate_fragment_construction_after_admission(fragment, limits);
        work.step()?;
        work.flush()?;
        structure.map_err(FragmentPropertyError::Structure)?;
        proof.require_declared_broadcast_equivalence(&mut work)?;
        let facts = super::guarantee::validate_guarantees_observed(
            fragment,
            proof.facts(),
            limits,
            projection_limits,
            &mut work,
        )?;
        let mut errors = ValidationContext::with_limits(limits);
        let _completion = super::graph::visit_node_graph_child_first(fragment, |event| {
            match event {
                super::graph::NodeGraphEvent::Step => work.step()?,
                super::graph::NodeGraphEvent::Ready(id) => {
                    let node = fragment
                        .nodes()
                        .get(&id)
                        .ok_or(FrozenCallError::InvalidSite)?;
                    // The sole schedule never emits a parent before its actual
                    // children. Formula scratch/clone/format remains opaque.
                    work.flush()?;
                    validate_node_output_properties_from(
                        fragment,
                        node,
                        "fragment.output_properties",
                        &mut errors,
                        &mut PropertyEffectSource::Frozen {
                            proof: &proof,
                            work: &mut work,
                        },
                    )?;
                    work.step()?;
                    work.flush()?;
                    if errors.is_saturated() {
                        errors.mark_truncated();
                        return Ok(false);
                    }
                }
            }
            Ok::<_, FrozenCallError>(true)
        })?;
        if !errors.is_empty() {
            return Err(FragmentPropertyError::Structure(
                ValidationErrors::from_collector(errors),
            ));
        }
        Ok(facts)
    })();
    if matches!(&result, Err(FragmentPropertyError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

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
    source: &mut PropertyEffectSource<'_, '_, '_>,
) -> Result<Option<Distribution>, FrozenCallError> {
    let Some(mut output) =
        nest_loop_join_placement_distribution(fragment, node, kind, distribution)
    else {
        return Ok(None);
    };
    if output == Distribution::Broadcast
        && let Some(predicate) = predicate
        && !source.expressions_safe(fragment, node.id, std::iter::once(predicate), true)?
    {
        output = Distribution::Unconstrained;
    }
    Ok(Some(output))
}

/// Placement is a structural prerequisite, independently of predicate effects
/// that may relinquish an otherwise provided output distribution.
pub(crate) fn nest_loop_join_placement_distribution(
    fragment: &Fragment,
    node: &PhysicalNode,
    kind: crate::JoinKind,
    distribution: crate::NestLoopJoinDistribution,
) -> Option<Distribution> {
    let [left, right] = node.inputs.as_ref() else {
        return None;
    };
    if node.required_inputs.len() != 2 {
        return None;
    }
    let inputs = [fragment.nodes().get(left)?, fragment.nodes().get(right)?];
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
            Some(inputs[0].output_properties.distribution.clone())
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
    if let Err(error) = validate_node_output_properties_from(
        fragment,
        node,
        path,
        errors,
        &mut PropertyEffectSource::Legacy,
    ) {
        // Legacy mode has no compile owner and cannot produce Control; any
        // new invariant failure must still prevent standalone publication.
        errors.push(ValidationError::new(path, error.to_string()));
    }
}

/// Derive the original replica-sensitive operator formulas from complete
/// occurrence claims for this immutable snapshot. The child properties must
/// already be final in this same source. This does not plan placement, derive
/// the other operator families, mutate the source or publish a fragment.
///
/// `None` explicitly means this node has no formula in this port. Consumers
/// must not substitute a default. After applying a result to a new snapshot,
/// rebuild its exact roots/call proofs and perform full package admission.
///
/// This port validates claim correspondence, not installed implementation
/// ownership; the FE must prepare the original selected owner first.
///
/// Projection ceilings cover only the occurrence index. The caller must admit
/// delegated structural scratch and output property clones before entry;
/// neither these ceilings nor the returned facts grant host memory.
#[allow(clippy::too_many_arguments)]
pub fn derive_replica_sensitive_output_properties_observed(
    fragment: &Fragment,
    uses: &crate::PhysicalRootUses,
    calls: &crate::FrozenFragmentCalls,
    node: NodeId,
    limits: PlanLimits,
    source_retained_bytes: usize,
    projection_limits: crate::PropertyProofProjectionLimits,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<
    Option<(
        crate::PhysicalProperties,
        crate::PropertyProofProjectionFacts,
    )>,
    FragmentPropertyError,
> {
    let mut work =
        CompileCheckpoints::try_new(control, novarocks_type_contract::CompilePhase::Validate)?;
    let result = (|| {
        work.flush()?;
        let proof = calls.property_proof(
            fragment,
            uses,
            &limits,
            source_retained_bytes,
            projection_limits,
            control,
        )?;
        work.flush()?;
        let structure = super::validate_fragment_construction_after_admission(fragment, limits);
        work.step()?;
        work.flush()?;
        structure.map_err(FragmentPropertyError::Structure)?;
        let found = fragment.nodes().get(&node);
        work.step()?;
        let Some(node) = found else {
            let mut errors = ValidationContext::with_limits(limits);
            errors.push(ValidationError::new(
                "fragment.property_derivation.node",
                "property derivation references an absent node",
            ));
            return Err(FragmentPropertyError::Structure(
                ValidationErrors::from_collector(errors),
            ));
        };
        // Provisional output claims are not replica authority. In particular,
        // a Filter or Project may relinquish an unsafe Broadcast claim here.
        // Final package validation checks the rebuilt snapshot, not this loan.
        let mut source = PropertyEffectSource::Frozen {
            proof: &proof,
            work: &mut work,
        };
        let properties = match &node.kind {
            NodeKind::Filter { predicates } => {
                filter_output_properties_from(fragment, node, predicates, &mut source)?
            }
            NodeKind::Project { expressions } => {
                project_output_properties_from(fragment, node, expressions, &mut source)?
            }
            NodeKind::TableFunction {
                function,
                arguments,
                outputs,
                ..
            } => table_function_output_properties_from(
                fragment,
                node,
                (function, arguments, outputs),
                &mut source,
            )?,
            _ => None,
        };
        work.step()?;
        work.flush()?;
        Ok(properties.map(|properties| (properties, proof.facts())))
    })();
    if matches!(&result, Err(FragmentPropertyError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

fn filter_output_properties_from(
    fragment: &Fragment,
    node: &PhysicalNode,
    predicates: &[ExprId],
    source: &mut PropertyEffectSource<'_, '_, '_>,
) -> Result<Option<crate::PhysicalProperties>, FrozenCallError> {
    let Some(input) = node
        .inputs
        .first()
        .and_then(|input| fragment.nodes().get(input))
    else {
        return Ok(None);
    };
    Ok(Some(crate::derive_filter_output_properties(
        &input.output_properties,
        source.expressions_safe(fragment, node.id, predicates.iter().copied(), true)?,
    )))
}

fn project_output_properties_from(
    fragment: &Fragment,
    node: &PhysicalNode,
    expressions: &[(ExprId, ValueId)],
    source: &mut PropertyEffectSource<'_, '_, '_>,
) -> Result<Option<crate::PhysicalProperties>, FrozenCallError> {
    let Some(input) = node
        .inputs
        .first()
        .and_then(|input| fragment.nodes().get(input))
    else {
        return Ok(None);
    };
    Ok(Some(crate::derive_project_output_properties(
        &input.output_properties,
        &node.output.columns,
        source.expressions_safe(
            fragment,
            node.id,
            expressions.iter().map(|(expression, _)| *expression),
            true,
        )?,
    )))
}

pub(crate) fn validate_node_output_properties_from(
    fragment: &Fragment,
    node: &PhysicalNode,
    path: &str,
    errors: &mut ValidationContext,
    source: &mut PropertyEffectSource<'_, '_, '_>,
) -> Result<(), FrozenCallError> {
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
            let Some(expected) = filter_output_properties_from(fragment, node, predicates, source)?
            else {
                return Ok(());
            };
            if node.output_properties != expected {
                errors.push(ValidationError::new(
                    path,
                    "filter output properties exceed its deterministic predicate proof",
                ));
            }
            return Ok(());
        }
        NodeKind::Limit { .. } => {
            let Some(input) = node
                .inputs
                .first()
                .and_then(|input| fragment.nodes().get(input))
            else {
                return Ok(());
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
            return Ok(());
        }
        NodeKind::Project { expressions } => {
            let Some(expected) =
                project_output_properties_from(fragment, node, expressions, source)?
            else {
                return Ok(());
            };
            if node.output_properties != expected {
                errors.push(ValidationError::new(
                    path,
                    "project output properties differ from the guarantees preserved by its output",
                ));
            }
            return Ok(());
        }
        NodeKind::Sort { order_by, mode } => {
            let Some(input) = node
                .inputs
                .first()
                .and_then(|input| fragment.nodes().get(input))
            else {
                return Ok(());
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
                    required.is_some_and(|required| {
                        required.ordering.is_empty()
                            && required.distribution == input.output_properties.distribution
                    }) && input.output_properties.distribution
                        == node.output_properties.distribution
                        && partition_values.map_or(
                            // A partition key written as an expression has no
                            // value to compare a layout against, so the only
                            // layout that holds every partition whole is the
                            // one stream.
                            input.output_properties.distribution == Distribution::Singleton,
                            |keys| {
                                distribution_colocates_by(
                                    &input.output_properties.distribution,
                                    &keys,
                                )
                            },
                        )
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
            if expected_ordering.unwrap_or_default() != node.output_properties.ordering.as_ref() {
                errors.push(ValidationError::new(
                    path,
                    "sort output ordering differs from its exact partition and order keys",
                ));
            }
            return Ok(());
        }
        NodeKind::TopN {
            order_by, phase, ..
        } => {
            let Some(input) = node
                .inputs
                .first()
                .and_then(|input| fragment.nodes().get(input))
            else {
                return Ok(());
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
            return Ok(());
        }
        NodeKind::Window(spec) => {
            validate_window_properties(fragment, node, spec, path, errors);
            return Ok(());
        }
        NodeKind::AssertOneRow(spec) => {
            validate_assertion_properties(fragment, node, spec, path, errors);
            return Ok(());
        }
        NodeKind::TableFunction {
            function,
            arguments,
            outputs,
            ..
        } => {
            validate_table_function_properties(
                fragment,
                node,
                (function, arguments, outputs),
                path,
                errors,
                source,
            )?;
            return Ok(());
        }
        NodeKind::ExchangeSource { .. } => return Ok(()),
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
                            && source.expressions_safe(
                                fragment,
                                node.id,
                                rows.iter().flat_map(|row| row.iter().copied()),
                                false,
                            )?
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
            return Ok(());
        }
        NodeKind::Aggregate {
            group_by, grouping, ..
        } => {
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
            if *grouping == crate::AggregateGrouping::Complete {
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
            return Ok(());
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
                return Ok(());
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
                    || !source.expressions_safe(
                        fragment,
                        node.id,
                        keys.iter()
                            .flat_map(|key| [key.left, key.right])
                            .chain(residual.iter().copied()),
                        true,
                    )?)
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
            return Ok(());
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
                source,
            )?
            else {
                return Ok(());
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
            return Ok(());
        }
        NodeKind::SetOp { kind, .. } => {
            let Some(distribution) = set_operation_output_distribution(fragment, node, *kind)
            else {
                return Ok(());
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
            return Ok(());
        }
        NodeKind::Repeat { .. } | NodeKind::Unpivot { .. } => {
            let Some(input) = node
                .inputs
                .first()
                .and_then(|input| fragment.nodes().get(input))
            else {
                return Ok(());
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
            return Ok(());
        }
        NodeKind::ChangeEventExpand { .. } => {
            let Some(input) = node
                .inputs
                .first()
                .and_then(|input| fragment.nodes().get(input))
            else {
                return Ok(());
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
            return Ok(());
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
            return Ok(());
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
            return Ok(());
        }
    };
    if expected.is_some_and(|expected| expected != &node.output_properties) {
        errors.push(ValidationError::new(
            path,
            "node output properties are not proven by its operator semantics",
        ));
    }
    Ok(())
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
        properties_satisfy(&input.output_properties, required)
            && input.output_properties.distribution == node.output_properties.distribution
            && partition_values.map_or(
                // A partition written as an expression has no value to compare
                // a layout against, so only the one stream holds it whole.
                input.output_properties.distribution == Distribution::Singleton,
                |keys| distribution_colocates_by(&input.output_properties.distribution, &keys),
            )
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
    // A window over expressions requires an ordering the plan cannot name, so
    // there is nothing to compare its input against; the sort the planner put
    // below it is carried as written.
    if expected_ordering.as_deref().is_some_and(|expected| {
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
    table: (
        &crate::BoundTableFunction,
        &[ExprId],
        &[crate::TableFunctionOutput],
    ),
    path: &str,
    errors: &mut ValidationContext,
    source: &mut PropertyEffectSource<'_, '_, '_>,
) -> Result<(), FrozenCallError> {
    let Some(expected) = table_function_output_properties_from(fragment, node, table, source)?
    else {
        return Ok(());
    };
    if node.output_properties != expected {
        let message = if node.inputs.is_empty() {
            "standalone table function requires singleton placement with single-copy ownership"
        } else {
            "table function output properties differ from its explicit passthrough guarantees"
        };
        errors.push(ValidationError::new(path, message));
    }
    Ok(())
}

fn table_function_output_properties_from(
    fragment: &Fragment,
    node: &PhysicalNode,
    table: (
        &crate::BoundTableFunction,
        &[ExprId],
        &[crate::TableFunctionOutput],
    ),
    source: &mut PropertyEffectSource<'_, '_, '_>,
) -> Result<Option<crate::PhysicalProperties>, FrozenCallError> {
    let (function, arguments, outputs) = table;
    let Some(input) = node
        .inputs
        .first()
        .and_then(|input| fragment.nodes().get(input))
    else {
        return Ok(Some(crate::PhysicalProperties {
            distribution: Distribution::Singleton,
            row_multiplicity: RowMultiplicity::SingleCopy,
            ordering: Box::default(),
        }));
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
            if !source.table_safe(fragment, node.id, function, arguments)? =>
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
    Ok(Some(crate::PhysicalProperties {
        distribution,
        row_multiplicity: input.output_properties.row_multiplicity,
        ordering,
    }))
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
