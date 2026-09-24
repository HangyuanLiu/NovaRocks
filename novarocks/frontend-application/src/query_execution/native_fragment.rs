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

//! Request-local native fragment facts and their consuming attachment.
//!
//! This is intentionally a query-execution capability rather than a native
//! encoder type. It holds each fragment's static plan as the frozen bytes the
//! encoder produced once for the whole plan; Core only verifies that those
//! bytes still belong to the exact prepared artifact before it finalizes the
//! distributed request.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::native::fragment_encoder::frozen::FragmentArtifact;
use crate::query_execution::artifact::FragmentId;

/// Complete native payload for one completed-plan encoding: every fragment's
/// static plan, frozen once. The attempt template retains it, and every
/// placement and every recovery attempt shares these same bytes; no attempt
/// encodes a static plan of its own.
#[derive(Clone, Debug)]
pub struct NativeFragmentAttachment {
    by_fragment: BTreeMap<FragmentId, Arc<FragmentArtifact>>,
    root: FragmentId,
    provenance: Option<u64>,
}

impl NativeFragmentAttachment {
    /// Seal the fragments one completed plan was frozen to.
    ///
    /// There is no second listing to cross-check against: the encoder froze
    /// these from the plan itself, so what can be wrong is a fragment filed
    /// under another fragment's id or a root that was never frozen, and both
    /// are checked.
    pub(crate) fn for_completed_plan(
        by_fragment: BTreeMap<FragmentId, Arc<FragmentArtifact>>,
        root: FragmentId,
        provenance: u64,
    ) -> Result<Self, String> {
        Self::sealed(by_fragment, root, Some(provenance))
    }

    fn sealed(
        by_fragment: BTreeMap<FragmentId, Arc<FragmentArtifact>>,
        root: FragmentId,
        provenance: Option<u64>,
    ) -> Result<Self, String> {
        for (&fragment_id, fragment) in &by_fragment {
            if fragment.facts().fragment_id() != fragment_id {
                return Err(format!(
                    "native fragment filed under {fragment_id} encodes fragment {}",
                    fragment.facts().fragment_id()
                ));
            }
        }
        if !by_fragment.contains_key(&root) {
            return Err(format!(
                "completed plan root fragment {root} has no frozen native plan"
            ));
        }
        Ok(Self {
            by_fragment,
            root,
            provenance,
        })
    }

    /// Whether every fragment of this payload was already encoded with its
    /// runtime-filter binding table.
    ///
    /// The backend requires the table on every fragment, empty or not, so
    /// "this plan declares no runtime filter" is not the same question.
    pub(crate) fn carries_runtime_filter_bindings(&self) -> bool {
        self.by_fragment
            .values()
            .all(|fragment| fragment.facts().carries_runtime_filter_bindings())
    }

    pub(crate) fn fragment_ids(&self) -> impl ExactSizeIterator<Item = FragmentId> + '_ {
        self.by_fragment.keys().copied()
    }

    pub(crate) fn get(&self, fragment_id: FragmentId) -> Option<&Arc<FragmentArtifact>> {
        self.by_fragment.get(&fragment_id)
    }

    /// The fragment frozen as the plan's root: the one whose completion is
    /// the execution's completion, and the only one a schedule may root at.
    pub(crate) const fn root(&self) -> FragmentId {
        self.root
    }

    pub(crate) fn matches_provenance(&self, provenance: u64) -> bool {
        self.provenance == Some(provenance)
    }
}

#[cfg(test)]
pub(crate) fn native_fragment_attachment_for_test(
    fragments: impl IntoIterator<Item = Arc<FragmentArtifact>>,
    root: FragmentId,
    provenance: Option<u64>,
) -> Result<NativeFragmentAttachment, String> {
    let mut by_fragment = BTreeMap::new();
    for fragment in fragments {
        let fragment_id = fragment.facts().fragment_id();
        if by_fragment.insert(fragment_id, fragment).is_some() {
            return Err(format!(
                "native fragment bundle encoded duplicate fragment id={fragment_id}"
            ));
        }
    }
    NativeFragmentAttachment::sealed(by_fragment, root, provenance)
}

#[cfg(test)]
mod tests {
    use novarocks_physical_plan::{PipelineDopDomain, PlanVersionId};
    use novarocks_proto_models::plan;

    use super::*;
    use crate::native::fragment_encoder::frozen::StaticFragmentHeader;

    fn fragment(fragment_id: FragmentId) -> Arc<FragmentArtifact> {
        FragmentArtifact::freeze(
            plan::PlanFragment {
                fragment_id,
                root: Some(plan::DistributedNode::default()),
                sink: Some(plan::DataSink {
                    kind: Some(plan::data_sink::Kind::Result(true)),
                }),
                ..Default::default()
            },
            StaticFragmentHeader {
                plan_version: PlanVersionId::try_new([3; 16]).expect("nonzero version"),
                plan_contract_revision: 1,
                dop_domain: PipelineDopDomain {
                    min: 1,
                    max: 4,
                    requires_power_of_two: false,
                },
            },
        )
        .expect("a frozen fragment")
    }

    #[test]
    fn test_fixture_rejects_duplicate_ids() {
        let error = native_fragment_attachment_for_test(vec![fragment(3), fragment(3)], 3, None)
            .expect_err("duplicate attachment ids must fail");
        assert_eq!(
            error,
            "native fragment bundle encoded duplicate fragment id=3"
        );
    }

    #[test]
    fn an_attachment_must_hold_its_root_under_its_own_id() {
        let error =
            NativeFragmentAttachment::for_completed_plan(BTreeMap::from([(3, fragment(3))]), 4, 7)
                .expect_err("a root that was never frozen is refused");
        assert!(error.contains("root fragment 4"), "{error}");

        let error =
            NativeFragmentAttachment::for_completed_plan(BTreeMap::from([(3, fragment(5))]), 3, 7)
                .expect_err("a fragment filed under another id is refused");
        assert!(error.contains("filed under 3"), "{error}");
    }
}
