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

//! Frontend-owned Stage/Start orchestration values.
//!
//! The participant-local Stage and Start contracts belong to
//! `novarocks_proto_codec::lifecycle`. These values retain the frozen ownership
//! binding, exact-batch assembly, and the two-barrier launch port. They are
//! deliberately separate from the lifecycle wire value family.

use std::collections::BTreeSet;

use novarocks_proto_codec::lifecycle::{
    ParticipantAttemptRef, QueryStageRequest, QueryStartRequest, StageDigest, StageFragment,
};
use novarocks_proto_codec::{FieldPath, ProtocolError, ProtocolErrorKind};
use novarocks_types::{QueryExecutionId, UniqueId};

use crate::query_execution::contract::DistributedQueryError;

use crate::query_execution::lifecycle_plan::QueryLifecycleTarget;

pub const DEFAULT_STAGE_MAX_FRAGMENTS: usize =
    novarocks_proto_codec::lifecycle::stage::DEFAULT_STAGE_MAX_FRAGMENTS;

/// Frozen Stage ownership for one Init participant. It is captured before the
/// Init plan is consumed and never re-reads live backend topology.
#[derive(Clone, Debug, PartialEq)]
pub struct StageParticipantBinding {
    target: QueryLifecycleTarget,
    participant: ParticipantAttemptRef,
    expected_fragment_instance_ids: BTreeSet<UniqueId>,
}

impl StageParticipantBinding {
    pub fn new(
        target: QueryLifecycleTarget,
        participant: ParticipantAttemptRef,
        expected_fragment_instance_ids: impl IntoIterator<Item = UniqueId>,
    ) -> Result<Self, ProtocolError> {
        if participant.backend_process_id()? != target.process_id() {
            return Err(ProtocolError::new(
                FieldPath::root("stage_participant_binding").field("participant"),
                ProtocolErrorKind::InvalidValue,
                "stage participant backend process id differs from the frozen target",
            ));
        }
        Ok(Self {
            target,
            participant,
            expected_fragment_instance_ids: expected_fragment_instance_ids.into_iter().collect(),
        })
    }

    pub const fn target(&self) -> &QueryLifecycleTarget {
        &self.target
    }

    pub const fn participant(&self) -> &ParticipantAttemptRef {
        &self.participant
    }

    pub fn expected_fragment_instance_ids(&self) -> &BTreeSet<UniqueId> {
        &self.expected_fragment_instance_ids
    }
}

/// One complete, exact participant-local Protocol Stage request and its frozen
/// Core owner.
#[derive(Clone, Debug, PartialEq)]
pub struct StageBatch {
    binding: StageParticipantBinding,
    request: QueryStageRequest,
    /// Derived once when the batch is frozen. The request no longer carries the
    /// stage identity, so the Start fence and acknowledgement comparison read
    /// this retained value instead of re-deriving it per attempt.
    digest: StageDigest,
}

impl StageBatch {
    pub fn new(
        execution_id: QueryExecutionId,
        binding: StageParticipantBinding,
        fragments: Vec<StageFragment>,
    ) -> Result<Self, ProtocolError> {
        if fragments.len() > DEFAULT_STAGE_MAX_FRAGMENTS {
            return Err(ProtocolError::new(
                FieldPath::root("stage_batch").field("fragments"),
                ProtocolErrorKind::Capacity,
                format!(
                    "stage batch contains {} fragments; limit is {DEFAULT_STAGE_MAX_FRAGMENTS}",
                    fragments.len()
                ),
            ));
        }
        let actual = fragments
            .iter()
            .map(StageFragment::fragment_instance_id)
            .collect::<BTreeSet<_>>();
        if actual != *binding.expected_fragment_instance_ids() {
            return Err(ProtocolError::new(
                FieldPath::root("stage_batch").field("fragments"),
                ProtocolErrorKind::InvalidValue,
                format!(
                    "stage batch exact fragment set differs for backend {}: expected {:?}, actual {:?}",
                    binding.target().backend_idx(),
                    binding.expected_fragment_instance_ids(),
                    actual
                ),
            ));
        }
        if binding.participant().execution_id().map_err(|error| {
            ProtocolError::new(
                FieldPath::root("stage_batch").field("participant"),
                ProtocolErrorKind::InvalidValue,
                error.to_string(),
            )
        })? != execution_id
        {
            return Err(ProtocolError::new(
                FieldPath::root("stage_batch").field("participant"),
                ProtocolErrorKind::InvalidValue,
                "stage participant execution id differs from the frozen execution id",
            ));
        }
        let digest = StageDigest::compute(binding.participant().clone(), &fragments)?;
        let request = QueryStageRequest::new(binding.participant().clone(), fragments)?;
        Ok(Self {
            binding,
            request,
            digest,
        })
    }

    pub const fn binding(&self) -> &StageParticipantBinding {
        &self.binding
    }

    pub const fn request(&self) -> &QueryStageRequest {
        &self.request
    }

    pub const fn digest(&self) -> StageDigest {
        self.digest
    }

    pub fn start_request(&self) -> QueryStartRequest {
        QueryStartRequest::new(
            self.request
                .participant()
                .execution_id()
                .expect("validated Stage participant retains its execution id"),
            self.digest,
        )
        .expect("validated Stage request contains a valid Start fence")
    }
}

/// FE-owned two-barrier launch port. Implementations must not issue a Start
/// request until `stage_all` has succeeded for every supplied batch.
pub trait QueryLaunchBarrier: Send + Sync + 'static {
    fn stage_all(&self, batches: &[StageBatch]) -> Result<(), DistributedQueryError>;

    fn start_all(&self, batches: &[StageBatch]) -> Result<(), DistributedQueryError>;
}

#[cfg(test)]
mod tests {
    use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
    use novarocks_proto_codec::lifecycle::AttemptId;
    use novarocks_proto_models::common;
    use novarocks_proto_models::{novarocks, plan};
    use novarocks_types::{BackendProcessId, QueryId};

    use super::*;

    fn execution_id() -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(7, 8),
            AttemptId::new(1).expect("nonzero attempt"),
        )
        .expect("nonzero query id")
    }

    fn binding(expected: impl IntoIterator<Item = UniqueId>) -> StageParticipantBinding {
        let process_id = BackendProcessId::new_v7();
        StageParticipantBinding::new(
            QueryLifecycleTarget::new(
                4,
                RuntimeEndpoint::parse("127.0.0.1:19040").expect("test endpoint"),
                process_id,
            ),
            ParticipantAttemptRef::new(execution_id(), process_id)
                .expect("valid participant attempt"),
            expected,
        )
        .expect("valid Stage participant binding")
    }

    fn fragment(lo: i64) -> StageFragment {
        StageFragment::new(
            plan::PlanFragment::default(),
            novarocks::InstanceParams {
                fragment_instance_id: Some(common::UniqueId { hi: 1, lo }),
                ..Default::default()
            },
        )
        .expect("valid Stage fragment")
    }

    #[test]
    fn batch_keeps_protocol_stage_and_start_carriers() {
        let first = fragment(9);
        let second = fragment(3);
        let batch = StageBatch::new(
            execution_id(),
            binding([first.fragment_instance_id(), second.fragment_instance_id()]),
            vec![first, second],
        )
        .expect("valid exact Stage batch");

        assert_eq!(
            batch.request().fragments()[0].fragment_instance_id().low(),
            3
        );
        assert_eq!(batch.request().as_proto().fragments.len(), 2);
        assert_eq!(
            batch.start_request().execution_id(),
            batch.request().execution_id()
        );
    }

    #[test]
    fn batch_rejects_a_fragment_set_outside_its_frozen_binding() {
        let error = StageBatch::new(
            execution_id(),
            binding([UniqueId::new(1, 9)]),
            vec![fragment(3)],
        )
        .expect_err("unbound fragment must not reach Protocol Stage");
        assert_eq!(error.kind(), ProtocolErrorKind::InvalidValue);
    }

    #[test]
    fn binding_rejects_a_participant_from_another_backend_process() {
        let target = QueryLifecycleTarget::new(
            4,
            RuntimeEndpoint::parse("127.0.0.1:19040").expect("test endpoint"),
            BackendProcessId::new_v7(),
        );
        let error = StageParticipantBinding::new(
            target,
            ParticipantAttemptRef::new(execution_id(), BackendProcessId::new_v7())
                .expect("valid participant attempt"),
            [],
        )
        .expect_err("stage participant must name the frozen backend process");
        assert_eq!(error.kind(), ProtocolErrorKind::InvalidValue);
    }
}
