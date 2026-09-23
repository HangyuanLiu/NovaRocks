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

//! Frontend-owned native submission mapping.
//!
//! A fragment's static plan is a property of the plan alone, so it is frozen
//! once per completed plan, by [`freeze_completed_fragments`], before anything
//! is placed: every placement of every attempt of that plan -- a recovery
//! included -- is created from those same bytes.
//!
//! Placement and per-instance facts become available only after Init,
//! ControlReady and connector-install acknowledgement. The per-attempt mapper
//! consumes those frozen facts without reacquiring planning, topology, or
//! control state, and contributes only each placement's own task-local facts;
//! nothing about a placement is written into the shared plan.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::sync::Arc;

use novarocks_proto_models::plan;

use crate::query_execution::artifact::FragmentId;
use crate::query_execution::artifact::native_submission::SubmissionPlanFacts;
use crate::query_execution::artifact::{
    NativePlacementAssignment, NativeSubmissionAttachment, NativeSubmissionEncodingView,
    NativeSubmissionFragmentRole, ValidatedNativeSubmission,
};
use crate::query_execution::assembly;

use super::frozen::{FragmentArtifact, StaticFragmentHeader};
use super::instance::select_fragment_pipeline_dop;

/// Freezes every fragment of one completed plan, once, for every attempt.
///
/// Which consumers a CTE feeds and which exchange each router branch reaches
/// are read from the plan's own edges, never from where its tasks are placed,
/// so each static sink is completed here and the plan frozen with it. The
/// generated messages are consumed: what survives is bytes and facts.
#[expect(
    clippy::type_complexity,
    reason = "The CTE exchange payload follows the frozen native fragment contract."
)]
pub(crate) fn freeze_completed_fragments(
    fragments: Vec<plan::PlanFragment>,
    plan: &SubmissionPlanFacts,
    root_fragment_id: FragmentId,
) -> Result<BTreeMap<FragmentId, Arc<FragmentArtifact>>, String> {
    let router_edges_by_source: BTreeMap<FragmentId, (i32, Vec<_>)> =
        assembly::group_router_edges_by_source(plan.router_edges())
            .into_iter()
            .map(|((source_fragment_id, router_group_id), branch_edges)| {
                (source_fragment_id, (router_group_id, branch_edges))
            })
            .collect();
    let cte_consumers = plan.cte_consumers();

    let mut frozen = BTreeMap::new();
    for mut native_fragment in fragments {
        let fragment_id = native_fragment.fragment_id;
        let facts = plan
            .fragment(fragment_id)
            .ok_or_else(|| format!("encoded fragment {fragment_id} is absent from the plan"))?;
        let is_root = fragment_id == root_fragment_id;
        let has_stream_edge = plan.has_stream_edge_from(fragment_id);
        let router_edges = router_edges_by_source.get(&fragment_id);
        let is_producer = has_stream_edge || router_edges.is_some() || facts.cte_id().is_some();
        validate_fragment_output_kind(fragment_id, is_root, is_producer, facts.role())?;
        assembly::ensure_native_fragment_sink_supported(
            fragment_id,
            is_root,
            has_stream_edge,
            router_edges.is_some(),
            facts.cte_id().is_some(),
        )?;
        if !is_root && !has_stream_edge {
            if let Some((router_group_id, branch_edges)) = router_edges {
                assembly::patch_native_change_stream_router_sink(
                    &mut native_fragment,
                    fragment_id,
                    *router_group_id,
                    branch_edges,
                )?;
            } else if let Some(cte_id) = facts.cte_id() {
                let consumers = cte_consumers.get(&cte_id).cloned().unwrap_or_default();
                assembly::patch_native_cte_multicast_sink(
                    &mut native_fragment,
                    fragment_id,
                    cte_id,
                    &consumers,
                )?;
            }
        }
        let artifact = FragmentArtifact::freeze(
            native_fragment,
            StaticFragmentHeader {
                plan_version: plan.version(),
                plan_contract_revision: plan.contract_revision(),
                dop_domain: facts.dop_domain(),
            },
        )?;
        if frozen.insert(fragment_id, artifact).is_some() {
            return Err(format!(
                "completed plan encoded duplicate fragment id={fragment_id}"
            ));
        }
    }
    let expected = plan.fragment_ids();
    let actual = frozen.keys().copied().collect::<BTreeSet<_>>();
    if actual != expected {
        let missing = expected.difference(&actual).copied().collect::<Vec<_>>();
        let unknown = actual.difference(&expected).copied().collect::<Vec<_>>();
        return Err(format!(
            "completed plan froze fragments {actual:?} for plan fragments {expected:?}: \
             missing={missing:?} unknown={unknown:?}"
        ));
    }
    Ok(frozen)
}

/// Maps one attempt's placements onto the plan's frozen fragments.
///
/// Nothing here encodes a static plan: each placement shares its fragment's
/// frozen bytes and contributes only its own assignment.
pub(crate) fn encode_native_submission(
    view: &NativeSubmissionEncodingView<'_>,
) -> Result<NativeSubmissionAttachment, String> {
    let schedule = view.schedule();
    if schedule.root_fragment_id != view.native_root() {
        return Err(format!(
            "schedule roots at fragment {} but the plan was frozen with root {}",
            schedule.root_fragment_id,
            view.native_root()
        ));
    }
    let requested_dop = view.query_options().pipeline_dop.unwrap_or_default();
    let mut submissions_by_fragment = BTreeMap::new();
    for (&fragment_id, placements) in &schedule.by_fragment {
        let artifact = view
            .native_fragment(fragment_id)
            .ok_or_else(|| format!("native fragment {fragment_id} is missing"))?;
        let pipeline_dop =
            select_fragment_pipeline_dop(artifact.facts().dop_domain(), requested_dop)?;
        let pipeline_dop = usize::try_from(pipeline_dop)
            .ok()
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| {
                format!("fragment {fragment_id} selected a non-positive pipeline DOP")
            })?;
        let fragment_submissions = placements
            .iter()
            .map(|placement| {
                let instance_ordinal = u32::try_from(placement.instance_index)
                    .ok()
                    .filter(|ordinal| i32::try_from(*ordinal).is_ok())
                    .ok_or("native submission instance ordinal exceeds the kernel's width")?;
                Ok(ValidatedNativeSubmission::new(
                    placement.backend_idx,
                    placement.finst_id,
                    view.execution_id(),
                    Arc::clone(artifact),
                    NativePlacementAssignment::new(
                        instance_ordinal,
                        pipeline_dop,
                        placement.scan_ranges.clone(),
                    ),
                ))
            })
            .collect::<Result<Vec<_>, String>>()?;
        submissions_by_fragment.insert(fragment_id, fragment_submissions);
    }

    let mut submissions = Vec::new();
    for &fragment_id in view.topological_fragment_order().iter().rev() {
        let mut fragment_submissions = submissions_by_fragment
            .remove(&fragment_id)
            .ok_or_else(|| format!("assembled fragment {fragment_id} is missing"))?;
        submissions.append(&mut fragment_submissions);
    }
    if !submissions_by_fragment.is_empty() {
        return Err("assembled submissions contain unknown fragments".to_string());
    }
    view.seal(submissions)
        .map_err(|error| error.message().to_string())
}

fn validate_fragment_output_kind(
    fragment_id: FragmentId,
    is_root: bool,
    is_producer: bool,
    role: NativeSubmissionFragmentRole,
) -> Result<(), String> {
    if is_root {
        return match role {
            NativeSubmissionFragmentRole::Result => Ok(()),
            NativeSubmissionFragmentRole::NonTerminal => Err(format!(
                "root fragment {fragment_id} must have Result output kind"
            )),
        };
    }
    if is_producer && role != NativeSubmissionFragmentRole::NonTerminal {
        return Err(format!(
            "producer fragment {fragment_id} must have NonTerminal output kind, got {role:?}"
        ));
    }
    Ok(())
}
