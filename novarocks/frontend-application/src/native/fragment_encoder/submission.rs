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

//! Frontend-owned placement-local native submission mapping.
//!
//! Templates are sealed before this point, while placement, connector write
//! handles and per-instance sidecars become available only after Init,
//! ControlReady and connector-install acknowledgement.  This module consumes
//! those frozen facts without reacquiring planning, topology, or control
//! state, then seals the complete payload back into Core's neutral attachment.

use std::collections::BTreeMap;

use crate::query_execution::artifact::FragmentId;
use crate::query_execution::artifact::{
    NativeSubmissionAttachment, NativeSubmissionEncodingView, NativeSubmissionFragmentRole,
    ValidatedNativeSubmission,
};
use crate::query_execution::assembly;
use novarocks_execution::task_execution::FragmentContractVersion;
use novarocks_proto_models::novarocks as wire;

use super::instance::{encode_instance_params, select_fragment_pipeline_dop};

#[expect(
    clippy::type_complexity,
    reason = "The CTE exchange payload follows the frozen native fragment contract."
)]
pub(crate) fn encode_native_submission(
    view: &NativeSubmissionEncodingView<'_>,
) -> Result<NativeSubmissionAttachment, String> {
    let schedule = view.schedule();
    let root_fragment_id = schedule.root_fragment_id;
    let router_edges_by_source: BTreeMap<FragmentId, (i32, Vec<_>)> =
        assembly::group_router_edges_by_source(view.router_edges())
            .into_iter()
            .map(|((source_fragment_id, router_group_id), branch_edges)| {
                (source_fragment_id, (router_group_id, branch_edges))
            })
            .collect();

    let cte_consumers = view.cte_consumers();

    let mut native_by_fragment = view
        .native_fragments_in_id_order()
        .map(|(fragment_id, fragment)| (fragment_id, fragment.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut submissions_by_fragment = BTreeMap::new();
    let query_id = view.query_id();
    for (&fragment_id, placements) in &schedule.by_fragment {
        let facts = view
            .fragment(fragment_id)
            .ok_or_else(|| format!("prepared fragment {fragment_id} is missing"))?;
        let template = native_by_fragment
            .remove(&fragment_id)
            .ok_or_else(|| format!("native fragment template {fragment_id} is missing"))?;
        let is_root = fragment_id == root_fragment_id;
        let has_stream_edge = view.has_stream_edge_from(fragment_id);
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
        let mut native_fragment = template;
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
        let dop = facts.dop_domain();
        let pipeline_dop = select_fragment_pipeline_dop(
            dop,
            view.query_options().pipeline_dop.unwrap_or_default(),
        )?;
        let frozen = wire::FrozenFragment {
            plan_version: view.plan_version().as_bytes().to_vec().into(),
            plan_contract_revision: view.plan_contract_revision(),
            fragment_contract_version: u32::from(FragmentContractVersion::CURRENT.get()),
            pipeline_dop_domain: Some(wire::PipelineDopDomain {
                min: dop.min,
                max: dop.max,
                requires_power_of_two: dop.requires_power_of_two,
            }),
            plan: Some(native_fragment),
            // 5B fills requirements from the complete physical plan.
            required_providers: Vec::new(),
        };
        let fragment_submissions = placements
            .iter()
            .map(|placement| {
                let backend_num = i32::try_from(placement.instance_index)
                    .map_err(|_| "native submission backend number exceeds i32 width")?;
                let instance_params = encode_instance_params(
                    &query_id,
                    placement,
                    view.query_options(),
                    pipeline_dop,
                    backend_num,
                    is_root,
                )?;
                Ok(ValidatedNativeSubmission::new(
                    placement.backend_idx,
                    placement.finst_id,
                    view.execution_id(),
                    frozen.clone(),
                    instance_params,
                ))
            })
            .collect::<Result<Vec<_>, String>>()?;
        submissions_by_fragment.insert(fragment_id, fragment_submissions);
    }
    if !native_by_fragment.is_empty() {
        return Err(format!(
            "native templates remained after assembly: {:?}",
            native_by_fragment.keys().collect::<Vec<_>>()
        ));
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
