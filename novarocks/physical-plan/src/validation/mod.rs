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

mod aggregate;
mod cuts;
mod error;
mod expr;
mod graph;
mod index;
mod limits;
mod node;
mod properties;
mod runtime_filter;

#[cfg(test)]
mod tests;

pub(crate) use aggregate::*;
pub use cuts::*;
pub use error::*;
pub(crate) use expr::*;
pub(crate) use graph::*;
pub(crate) use index::*;
pub use limits::*;
pub(crate) use node::*;
pub(crate) use properties::*;
pub(crate) use runtime_filter::*;

use std::collections::{BTreeMap, BTreeSet};

use crate::resource::{
    MAX_ANNOTATION_BYTES, MAX_ANNOTATION_KEY_BYTES, MAX_ANNOTATION_VALUE_BYTES, MAX_ANNOTATIONS,
    MAX_PLAN_DERIVED_CUT_BYTES, MAX_PLAN_DERIVED_CUT_ITEMS, validate_fragment_cut_resources,
    validate_fragment_resources, validate_plan_resources,
};
use crate::{
    AnnotationSubject, ExprId, Fragment, FragmentCuts, FragmentId, NodeId, NodeKind,
    PLAN_CONTRACT_REVISION, PhysicalPlan, RequiredContracts, ValueId,
};

// Guard rails, not operational bounds: these fence off values no
// legitimate plan produces. Counting them as structural protection would
// overstate what is actually bounded. Real bounds live in `PlanLimits`.
pub const MAX_RUNTIME_FILTER_ARTIFACT_BYTES: u64 = 1 << 30;
pub const MAX_RUNTIME_FILTER_DEADLINE_MS: u64 = 86_400_000;
pub const MAX_RUNTIME_FILTER_RETRIES: u32 = 100;
pub const MAX_PROVIDER_PRIVATE_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_METADATA_COVERAGE_EVIDENCE_BYTES: usize = 1024 * 1024;
pub const MAX_ARTIFACT_REFERENCE_BYTES: u32 = 16 * 1024 * 1024;
pub const MAX_UNPIVOT_LITERAL_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_UNPIVOT_OUTPUT_ROWS: u64 = 1 << 30;
pub const MAX_UNPIVOT_OUTPUT_BYTES: u64 = 1 << 30;
pub const MAX_PARTITION_COUNT: u32 = 1 << 20;
pub const MAX_SCAN_BATCH_ROWS: u64 = 1 << 30;
pub const MAX_SCAN_BATCH_BYTES: u64 = 1 << 30;
pub const MAX_PIPELINE_DOP: u32 = 1 << 20;

pub fn validate_fragment(fragment: &Fragment, cuts: &FragmentCuts) -> Result<(), ValidationErrors> {
    let mut errors = ValidationContext::new();
    validate_fragment_into(fragment, &mut errors);
    validate_fragment_cut_resources(fragment, cuts, &mut errors);
    if !errors.is_empty() {
        return Err(ValidationErrors::from_collector(errors));
    }
    validate_fragment_cuts_into(fragment, cuts, true, &mut errors);
    validate_fragment_partition_identities(fragment, cuts, &mut errors);
    if errors.is_empty() {
        Ok(())
    } else {
        Err(ValidationErrors::from_collector(errors))
    }
}

pub(crate) fn validate_fragment_definition(fragment: &Fragment) -> Result<(), ValidationErrors> {
    let mut errors = ValidationContext::new();
    validate_fragment_into(fragment, &mut errors);
    validate_fragment_partition_identities(fragment, &FragmentCuts::default(), &mut errors);
    if errors.is_empty() {
        Ok(())
    } else {
        Err(ValidationErrors::from_collector(errors))
    }
}

pub fn validate_plan(plan: &PhysicalPlan) -> Result<(), ValidationErrors> {
    validate_plan_with_limits(plan, PlanLimits::FROZEN)
}

/// Validates against caller-supplied bounds.
///
/// A refused plan is a user-visible refusal, so the bounds that produce one
/// are a deployment decision rather than a property of this build.
pub fn validate_plan_with_limits(
    plan: &PhysicalPlan,
    limits: PlanLimits,
) -> Result<(), ValidationErrors> {
    let mut errors = ValidationContext::with_limits(limits);
    validate_plan_resources(plan, &mut errors);
    if !errors.is_empty() {
        return Err(ValidationErrors::from_collector(errors));
    }
    if plan.required().plan_contract_revision != PLAN_CONTRACT_REVISION {
        errors.push(ValidationError::new(
            "required.plan_contract_revision",
            format!(
                "expected {PLAN_CONTRACT_REVISION}, got {}",
                plan.required().plan_contract_revision
            ),
        ));
    }
    bounded_count(
        &mut errors,
        "fragments",
        plan.fragments().len(),
        limits.plan_fragments,
    );
    bounded_count(&mut errors, "edges", plan.edges().len(), limits.plan_edges);
    bounded_count(
        &mut errors,
        "runtime_filters",
        plan.runtime_filters().len(),
        limits.plan_runtime_filters,
    );
    bounded_count(
        &mut errors,
        "artifact_refs",
        plan.artifact_refs().len(),
        limits.plan_artifact_refs,
    );
    if plan.fragments().is_empty() {
        errors.push(ValidationError::new("fragments", "plan has no fragments"));
    }

    for fragment in plan.fragments().values() {
        validate_fragment_into(fragment, &mut errors);
        if errors.is_saturated() {
            errors.mark_truncated();
            return Err(ValidationErrors::from_collector(errors));
        }
    }
    let mut root_port_indexes = BTreeMap::new();
    for edge in plan.edges().values() {
        validate_edge(plan, edge, &mut root_port_indexes, &mut errors);
        if errors.is_saturated() {
            errors.mark_truncated();
            return Err(ValidationErrors::from_collector(errors));
        }
    }
    macro_rules! run_validation_stage {
        ($stage:expr) => {{
            $stage;
            if errors.is_saturated() {
                errors.mark_truncated();
                return Err(ValidationErrors::from_collector(errors));
            }
        }};
    }
    run_validation_stage!(validate_fragment_graph(plan, &mut errors));
    run_validation_stage!(validate_sinks(plan, &mut errors));
    run_validation_stage!(validate_writer_flows(plan, &mut errors));
    run_validation_stage!(validate_result(plan, &mut errors));
    run_validation_stage!(validate_runtime_filters(plan, &mut errors));
    run_validation_stage!(validate_artifact_refs(plan, &mut errors));
    run_validation_stage!(validate_artifact_inputs(plan, &mut errors));
    run_validation_stage!(validate_annotations(plan, &mut errors));
    run_validation_stage!(validate_cross_fragment_value_origins(plan, &mut errors));
    run_validation_stage!(validate_provider_read_occurrences(plan, &mut errors));
    run_validation_stage!(validate_partition_identities(plan, &mut errors));
    run_validation_stage!(validate_aggregate_sequences(plan, &mut errors));
    run_validation_stage!(validate_topn_reductions(plan, &mut errors));

    if errors.is_empty() {
        let Some(derivation) = FragmentCutDerivation::new(plan, errors.limits()) else {
            errors.push(ValidationError::new(
                "fragments.cuts",
                "cannot index the complete fragment cut graph",
            ));
            return Err(ValidationErrors::from_collector(errors));
        };
        let mut total_cut_items = 0usize;
        let mut total_cut_bytes = 0usize;
        for fragment in plan.fragments().values() {
            let Some(usage) =
                preflight_fragment_cut_resources(plan, fragment.id(), &derivation, &mut errors)
            else {
                errors.push(ValidationError::new(
                    format!("fragments[{}].cuts", fragment.id().get()),
                    "cannot preflight the complete fragment cuts",
                ));
                return Err(ValidationErrors::from_collector(errors));
            };
            total_cut_items = total_cut_items.saturating_add(usage.items);
            total_cut_bytes = total_cut_bytes.saturating_add(usage.bytes);
            if total_cut_items > MAX_PLAN_DERIVED_CUT_ITEMS
                || total_cut_bytes > MAX_PLAN_DERIVED_CUT_BYTES
            {
                errors.push(ValidationError::resource_limit(
                    "fragments.cuts.resources",
                    "aggregate derived fragment cuts exceed the plan publication budget",
                ));
            }
            if !errors.is_empty() {
                return Err(ValidationErrors::from_collector(errors));
            }
            let Some(cuts) = derivation.derive_preflighted(plan, fragment.id()) else {
                errors.push(ValidationError::new(
                    format!("fragments[{}].cuts", fragment.id().get()),
                    "cannot derive complete fragment cuts",
                ));
                return Err(ValidationErrors::from_collector(errors));
            };
            validate_fragment_cuts_into(fragment, &cuts, false, &mut errors);
            validate_fragment_partition_identities(fragment, &cuts, &mut errors);
            if !errors.is_empty() {
                return Err(ValidationErrors::from_collector(errors));
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(ValidationErrors::from_collector(errors))
    }
}

pub(crate) fn validate_provider_read_occurrences(
    plan: &PhysicalPlan,
    errors: &mut ValidationContext,
) {
    let mut occurrences = BTreeMap::new();
    for fragment in plan.fragments().values() {
        for node in fragment.nodes().values() {
            let NodeKind::Scan { occurrence, .. } = &node.kind else {
                continue;
            };
            if let Some((first_fragment, first_node)) =
                occurrences.insert(*occurrence, (fragment.id(), node.id))
            {
                errors.push(ValidationError::new(
                    format!(
                        "fragments[{}].nodes[{}].occurrence",
                        fragment.id().get(),
                        node.id.get()
                    ),
                    format!(
                        "provider read occurrence {} is already owned by fragment {} node {}",
                        occurrence.get(),
                        first_fragment.get(),
                        first_node.get()
                    ),
                ));
            }
        }
    }
}

pub(crate) fn validate_fragment_into(fragment: &Fragment, errors: &mut ValidationContext) {
    let prefix = format!("fragments[{}]", fragment.id().get());
    let previous_errors = errors.len();
    validate_fragment_resources(fragment, errors);
    if errors.len() != previous_errors {
        return;
    }
    bounded_count(
        errors,
        &format!("{prefix}.nodes"),
        fragment.nodes().len(),
        errors.limits().fragment_nodes,
    );
    bounded_count(
        errors,
        &format!("{prefix}.values"),
        fragment.values().len(),
        errors.limits().fragment_values,
    );
    bounded_count(
        errors,
        &format!("{prefix}.expressions"),
        fragment.expressions().len(),
        errors.limits().fragment_expressions,
    );
    if !fragment.nodes().contains_key(&fragment.root()) {
        errors.push(ValidationError::new(
            format!("{prefix}.root"),
            format!("node {} is not defined", fragment.root().get()),
        ));
    }
    let dop = fragment.dop_domain();
    if dop.min == 0 || dop.min > dop.max || dop.max > MAX_PIPELINE_DOP {
        errors.push(ValidationError::new(
            format!("{prefix}.dop_domain"),
            "DOP bounds must be non-zero, ordered and bounded",
        ));
    }
    if dop.requires_power_of_two
        && dop
            .min
            .checked_next_power_of_two()
            .is_none_or(|first| first > dop.max)
    {
        errors.push(ValidationError::new(
            format!("{prefix}.dop_domain"),
            "power-of-two DOP domain has no admissible member",
        ));
    }
    let mut aggregate_calls = BTreeMap::new();
    for node in fragment.nodes().values() {
        let NodeKind::Aggregate { calls, .. } = &node.kind else {
            continue;
        };
        for call in calls {
            if aggregate_calls
                .insert(call.id, call.binding.phase)
                .is_some()
            {
                errors.push(ValidationError::new(
                    format!("{prefix}.aggregate_calls[{}]", call.id.get()),
                    "aggregate call identity must be unique within its fragment",
                ));
            }
            if errors.is_saturated() {
                errors.mark_truncated();
                return;
            }
        }
    }
    for value in fragment.values().values() {
        validate_value(fragment, value, &aggregate_calls, errors);
        if errors.is_saturated() {
            errors.mark_truncated();
            return;
        }
    }
    let indexes = FragmentValidationIndexes::new(fragment);
    let window_roots = fragment
        .nodes()
        .values()
        .filter_map(|node| match &node.kind {
            NodeKind::Window(spec) => Some(spec.expressions.iter().map(|item| item.expression)),
            _ => None,
        })
        .flatten()
        .collect::<BTreeSet<_>>();
    let operator_roots = fragment
        .nodes()
        .values()
        .flat_map(|node| {
            let mut roots = Vec::new();
            node.kind.expression_references(&mut roots);
            roots
        })
        .collect::<BTreeSet<_>>();
    let mut expression_parents: BTreeMap<ExprId, Vec<ExpressionParentReference>> = BTreeMap::new();
    for (_, parent) in fragment.expressions().iter() {
        let mut references = Vec::new();
        expression_parent_references(parent, &mut references);
        for (child, role) in references {
            expression_parents
                .entry(child)
                .or_default()
                .push(ExpressionParentReference {
                    parent: parent.id,
                    role,
                });
        }
    }
    for (_, expression) in fragment.expressions().iter() {
        validate_expression(
            fragment,
            expression,
            &indexes.visible_inputs,
            &window_roots,
            &operator_roots,
            expression_parents
                .get(&expression.id)
                .map(Vec::as_slice)
                .unwrap_or_default(),
            errors,
        );
        if errors.is_saturated() {
            errors.mark_truncated();
            return;
        }
    }
    validate_expression_acyclic(fragment, errors);
    validate_lambda_scope_acyclic(fragment, errors);
    validate_expression_reachability(fragment, errors);
    for node in fragment.nodes().values() {
        validate_node(fragment, node, &indexes, errors);
        if errors.is_saturated() {
            errors.mark_truncated();
            return;
        }
    }
    validate_node_graph(fragment, errors);
    validate_fragment_sink(fragment, errors);
}

pub(crate) fn validate_annotations(plan: &PhysicalPlan, errors: &mut ValidationContext) {
    bounded_count(
        errors,
        "annotations",
        plan.annotations().len(),
        MAX_ANNOTATIONS,
    );
    let total_bytes = plan
        .annotations()
        .iter()
        .fold(0_usize, |total, annotation| {
            total
                .saturating_add(annotation.key.len())
                .saturating_add(annotation.value.len())
        });
    if total_bytes > MAX_ANNOTATION_BYTES {
        errors.push(ValidationError::resource_limit(
            "annotations",
            format!("contains {total_bytes} bytes, exceeding {MAX_ANNOTATION_BYTES}"),
        ));
    }
    for (index, annotation) in plan.annotations().iter().enumerate() {
        let valid = match annotation.subject {
            AnnotationSubject::Plan => true,
            AnnotationSubject::Fragment(fragment) => plan.fragments().contains_key(&fragment),
            AnnotationSubject::Node(fragment, node) => plan
                .fragments()
                .get(&fragment)
                .is_some_and(|fragment| fragment.nodes().contains_key(&node)),
            AnnotationSubject::Value(fragment, value) => plan
                .fragments()
                .get(&fragment)
                .is_some_and(|fragment| fragment.values().contains_key(&value)),
        };
        // Four different faults read alike once they are one message, and an
        // annotation names a subject the reader has to go and find.
        let fault = if !valid {
            Some(format!(
                "names a subject this plan does not have: {:?}",
                annotation.subject
            ))
        } else if annotation.key.is_empty() {
            Some("has an empty key".to_string())
        } else if annotation.key.len() > MAX_ANNOTATION_KEY_BYTES {
            Some(format!(
                "key is {} bytes, exceeding {MAX_ANNOTATION_KEY_BYTES}",
                annotation.key.len()
            ))
        } else if annotation.value.len() > MAX_ANNOTATION_VALUE_BYTES {
            Some(format!(
                "value is {} bytes, exceeding {MAX_ANNOTATION_VALUE_BYTES}",
                annotation.value.len()
            ))
        } else {
            None
        };
        if let Some(fault) = fault {
            errors.push(ValidationError::new(
                format!("annotations[{index}]"),
                format!("annotation `{}` {fault}", annotation.key),
            ));
        }
    }
}

pub(crate) fn require_node(
    fragment: &Fragment,
    node: NodeId,
    path: &str,
    errors: &mut ValidationContext,
) {
    if !fragment.nodes().contains_key(&node) {
        errors.push(ValidationError::new(
            path,
            format!("node {} is not defined", node.get()),
        ));
    }
}

pub(crate) fn require_value(
    fragment: &Fragment,
    value: ValueId,
    path: &str,
    errors: &mut ValidationContext,
) {
    if !fragment.values().contains_key(&value) {
        errors.push(ValidationError::new(
            path,
            format!("value {} is not defined", value.get()),
        ));
    }
}

#[allow(dead_code)]
pub(crate) fn _assert_required_contract_is_copy(_: RequiredContracts) {}

#[allow(dead_code)]
pub(crate) fn _assert_maps_are_deterministic(_: BTreeMap<FragmentId, Fragment>) {}
