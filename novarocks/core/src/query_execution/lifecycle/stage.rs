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
//   https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Typed query-stage contracts shared by the coordinator and backend lifecycle
//! owners.  This module deliberately does not prepare or start fragments.

use prost::Message;
use sha2::{Digest, Sha256};

use std::collections::BTreeSet;

use crate::common::types::UniqueId;
use crate::proto::{novarocks, plan};
use crate::query_execution::contract::DistributedQueryError;

use super::contract::{QueryLifecycleError, QueryLifecycleErrorCode, QueryLifecycleTarget};
use super::identity::QueryExecutionId;
use super::manifest::{ParticipantManifestDigest, ParticipantRole};

pub const DEFAULT_STAGE_MAX_ENCODED_BYTES: usize = 48 * 1024 * 1024;
pub const DEFAULT_STAGE_MAX_FRAGMENTS: usize = 256;

/// Version of the semantic StageFragments digest projection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StageDigestVersion(u32);

impl StageDigestVersion {
    pub const V1: Self = Self(1);

    pub const fn get(self) -> u32 {
        self.0
    }

    pub fn try_from_wire(value: u32) -> Result<Self, QueryLifecycleError> {
        match value {
            1 => Ok(Self::V1),
            _ => Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::InvalidManifest,
                format!("unsupported stage digest version {value}"),
            )),
        }
    }
}

/// Fixed-width SHA-256 output of the versioned semantic stage projection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StageDigest([u8; 32]);

impl StageDigest {
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn try_from_slice(bytes: &[u8]) -> Result<Self, QueryLifecycleError> {
        let bytes: [u8; 32] = bytes.try_into().map_err(|_| {
            QueryLifecycleError::new(
                QueryLifecycleErrorCode::InvalidManifest,
                "stage digest must be 32 bytes",
            )
        })?;
        Ok(Self(bytes))
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Computes the V1 digest over decoded semantic values. Native plan and
    /// instance map fields are generated as `BTreeMap` values, so Prost's
    /// encoding here is stable across the FE and BE process boundary. The
    /// outer StageFragments wire framing is deliberately not included.
    pub fn compute_v1(
        execution_id: QueryExecutionId,
        init_digest: ParticipantManifestDigest,
        fragments: &[StageFragment],
    ) -> Result<Self, QueryLifecycleError> {
        let mut ordered = fragments.iter().collect::<Vec<_>>();
        ordered.sort_by_key(|fragment| fragment.fragment_instance_id());
        for pair in ordered.windows(2) {
            if pair[0].fragment_instance_id() == pair[1].fragment_instance_id() {
                return Err(QueryLifecycleError::new(
                    QueryLifecycleErrorCode::InvalidManifest,
                    "stage digest requires unique fragment instance ids",
                ));
            }
        }

        let mut hasher = Sha256::new();
        hasher.update(b"novarocks.query-lifecycle.stage.v1\0");
        hasher.update(execution_id.query_id().high().to_be_bytes());
        hasher.update(execution_id.query_id().low().to_be_bytes());
        hasher.update(execution_id.attempt_id().get().to_be_bytes());
        hasher.update(init_digest.as_bytes());
        hasher.update(
            u64::try_from(ordered.len())
                .expect("fragment count fits u64")
                .to_be_bytes(),
        );
        for fragment in ordered {
            let finst = fragment.fragment_instance_id();
            hasher.update(finst.hi.to_be_bytes());
            hasher.update(finst.lo.to_be_bytes());
            hash_message(&mut hasher, fragment.plan());
            hash_message(&mut hasher, fragment.instance_params());
        }
        Ok(Self(hasher.finalize().into()))
    }
}

fn hash_message<M: Message>(hasher: &mut Sha256, message: &M) {
    let mut bytes = Vec::with_capacity(message.encoded_len());
    message
        .encode(&mut bytes)
        .expect("Vec has enough capacity for prost encoding");
    hasher.update(
        u64::try_from(bytes.len())
            .expect("message length fits u64")
            .to_be_bytes(),
    );
    hasher.update(bytes);
}

/// One static native plan and its per-instance dynamic parameters.
#[derive(Clone, Debug, PartialEq)]
pub struct StageFragment {
    plan: plan::PlanFragment,
    instance_params: novarocks::InstanceParams,
}

/// Frozen Stage ownership for one Init participant.  It is captured before
/// the Init plan is consumed and never re-reads live backend topology.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StageParticipantBinding {
    target: QueryLifecycleTarget,
    init_digest: ParticipantManifestDigest,
    roles: BTreeSet<ParticipantRole>,
    expected_fragment_instance_ids: BTreeSet<UniqueId>,
}

impl StageParticipantBinding {
    pub fn new(
        target: QueryLifecycleTarget,
        init_digest: ParticipantManifestDigest,
        roles: impl IntoIterator<Item = ParticipantRole>,
        expected_fragment_instance_ids: impl IntoIterator<Item = UniqueId>,
    ) -> Result<Self, QueryLifecycleError> {
        let roles = roles.into_iter().collect::<BTreeSet<_>>();
        if roles.is_empty() {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::InvalidManifest,
                "stage participant must have at least one role",
            ));
        }
        let expected_fragment_instance_ids = expected_fragment_instance_ids
            .into_iter()
            .collect::<BTreeSet<_>>();
        Ok(Self {
            target,
            init_digest,
            roles,
            expected_fragment_instance_ids,
        })
    }

    pub const fn target(&self) -> QueryLifecycleTarget {
        self.target
    }

    pub const fn init_digest(&self) -> ParticipantManifestDigest {
        self.init_digest
    }

    pub fn roles(&self) -> &BTreeSet<ParticipantRole> {
        &self.roles
    }

    pub fn expected_fragment_instance_ids(&self) -> &BTreeSet<UniqueId> {
        &self.expected_fragment_instance_ids
    }

    pub fn is_fragment_executor(&self) -> bool {
        self.roles.contains(&ParticipantRole::FragmentExecutor)
    }
}

/// One complete, exact participant-local Stage request and its frozen owner.
#[derive(Clone, Debug, PartialEq)]
pub struct StageBatch {
    binding: StageParticipantBinding,
    request: QueryStageRequest,
}

impl StageBatch {
    pub fn new(
        execution_id: QueryExecutionId,
        binding: StageParticipantBinding,
        fragments: Vec<StageFragment>,
    ) -> Result<Self, QueryLifecycleError> {
        if fragments.len() > DEFAULT_STAGE_MAX_FRAGMENTS {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::InvalidManifest,
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
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::InvalidManifest,
                format!(
                    "stage batch exact fragment set differs for backend {}: expected {:?}, actual {:?}",
                    binding.target().backend_idx(),
                    binding.expected_fragment_instance_ids(),
                    actual
                ),
            ));
        }
        let digest = StageDigest::compute_v1(execution_id, binding.init_digest(), &fragments)?;
        let request = QueryStageRequest::new(
            execution_id,
            binding.init_digest(),
            StageDigestVersion::V1,
            digest,
            fragments,
        )?;
        let encoded =
            crate::query_execution::lifecycle::contract::encode_query_stage_request(&request);
        if encoded.encoded_len() > DEFAULT_STAGE_MAX_ENCODED_BYTES {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::InvalidManifest,
                format!(
                    "stage request encoded bytes exceed {} byte limit",
                    DEFAULT_STAGE_MAX_ENCODED_BYTES
                ),
            ));
        }
        Ok(Self { binding, request })
    }

    pub const fn binding(&self) -> &StageParticipantBinding {
        &self.binding
    }

    pub const fn request(&self) -> &QueryStageRequest {
        &self.request
    }

    pub fn start_request(&self) -> QueryStartRequest {
        QueryStartRequest::new(
            self.request.execution_id(),
            self.request.digest_version(),
            self.request.digest(),
        )
    }
}

/// FE-owned two-barrier launch port. Implementations must not issue a Start
/// request until `stage_all` has succeeded for every supplied batch.
pub trait QueryLaunchBarrier: Send + Sync + 'static {
    fn stage_all(&self, batches: &[StageBatch]) -> Result<(), DistributedQueryError>;

    fn start_all(&self, batches: &[StageBatch]) -> Result<(), DistributedQueryError>;
}

impl StageFragment {
    pub fn new(
        plan: plan::PlanFragment,
        instance_params: novarocks::InstanceParams,
    ) -> Result<Self, QueryLifecycleError> {
        let _ = fragment_instance_id(&instance_params)?;
        Ok(Self {
            plan,
            instance_params,
        })
    }

    pub const fn plan(&self) -> &plan::PlanFragment {
        &self.plan
    }

    pub const fn instance_params(&self) -> &novarocks::InstanceParams {
        &self.instance_params
    }

    pub fn fragment_instance_id(&self) -> UniqueId {
        // Constructor and wire decoder validate this invariant.
        fragment_instance_id(&self.instance_params)
            .expect("StageFragment always has a nonzero fragment instance id")
    }

    pub fn into_parts(self) -> (plan::PlanFragment, novarocks::InstanceParams) {
        (self.plan, self.instance_params)
    }
}

/// Exact participant-local StageFragments payload.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryStageRequest {
    execution_id: QueryExecutionId,
    init_digest: ParticipantManifestDigest,
    digest_version: StageDigestVersion,
    digest: StageDigest,
    fragments: Vec<StageFragment>,
}

impl QueryStageRequest {
    pub fn new(
        execution_id: QueryExecutionId,
        init_digest: ParticipantManifestDigest,
        digest_version: StageDigestVersion,
        digest: StageDigest,
        mut fragments: Vec<StageFragment>,
    ) -> Result<Self, QueryLifecycleError> {
        fragments.sort_by_key(StageFragment::fragment_instance_id);
        for pair in fragments.windows(2) {
            if pair[0].fragment_instance_id() == pair[1].fragment_instance_id() {
                return Err(QueryLifecycleError::new(
                    QueryLifecycleErrorCode::InvalidManifest,
                    "stage fragment instance ids must be unique",
                ));
            }
        }
        Ok(Self {
            execution_id,
            init_digest,
            digest_version,
            digest,
            fragments,
        })
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub const fn init_digest(&self) -> ParticipantManifestDigest {
        self.init_digest
    }

    pub const fn digest_version(&self) -> StageDigestVersion {
        self.digest_version
    }

    pub const fn digest(&self) -> StageDigest {
        self.digest
    }

    pub fn fragments(&self) -> &[StageFragment] {
        &self.fragments
    }

    pub fn into_parts(
        self,
    ) -> (
        QueryExecutionId,
        ParticipantManifestDigest,
        StageDigestVersion,
        StageDigest,
        Vec<StageFragment>,
    ) {
        (
            self.execution_id,
            self.init_digest,
            self.digest_version,
            self.digest,
            self.fragments,
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryStageOutcome {
    Applied,
    AlreadyApplied,
    RejectedConflict,
    RejectedInvalidState,
    RejectedInvalidBatch,
    RejectedCapacity,
    RejectedTerminated,
    RejectedLocalFailure,
}

impl QueryStageOutcome {
    pub const fn is_staged(self) -> bool {
        matches!(self, Self::Applied | Self::AlreadyApplied)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryStageAck {
    execution_id: QueryExecutionId,
    digest_version: StageDigestVersion,
    digest: StageDigest,
    outcome: QueryStageOutcome,
    detail: String,
}

impl QueryStageAck {
    pub fn new(
        execution_id: QueryExecutionId,
        digest_version: StageDigestVersion,
        digest: StageDigest,
        outcome: QueryStageOutcome,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            execution_id,
            digest_version,
            digest,
            outcome,
            detail: detail.into(),
        }
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub const fn digest_version(&self) -> StageDigestVersion {
        self.digest_version
    }

    pub const fn digest(&self) -> StageDigest {
        self.digest
    }

    pub const fn outcome(&self) -> QueryStageOutcome {
        self.outcome
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryStartRequest {
    execution_id: QueryExecutionId,
    digest_version: StageDigestVersion,
    digest: StageDigest,
}

impl QueryStartRequest {
    pub const fn new(
        execution_id: QueryExecutionId,
        digest_version: StageDigestVersion,
        digest: StageDigest,
    ) -> Self {
        Self {
            execution_id,
            digest_version,
            digest,
        }
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub const fn digest_version(&self) -> StageDigestVersion {
        self.digest_version
    }

    pub const fn digest(&self) -> StageDigest {
        self.digest
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryStartOutcome {
    Applied,
    AlreadyStarted,
    RejectedNotStaged,
    RejectedConflict,
    RejectedTerminated,
}

impl QueryStartOutcome {
    pub const fn is_running(self) -> bool {
        matches!(self, Self::Applied | Self::AlreadyStarted)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryStartAck {
    execution_id: QueryExecutionId,
    digest_version: StageDigestVersion,
    digest: StageDigest,
    outcome: QueryStartOutcome,
    detail: String,
}

impl QueryStartAck {
    pub fn new(
        execution_id: QueryExecutionId,
        digest_version: StageDigestVersion,
        digest: StageDigest,
        outcome: QueryStartOutcome,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            execution_id,
            digest_version,
            digest,
            outcome,
            detail: detail.into(),
        }
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub const fn digest_version(&self) -> StageDigestVersion {
        self.digest_version
    }

    pub const fn digest(&self) -> StageDigest {
        self.digest
    }

    pub const fn outcome(&self) -> QueryStartOutcome {
        self.outcome
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

fn fragment_instance_id(
    instance_params: &novarocks::InstanceParams,
) -> Result<UniqueId, QueryLifecycleError> {
    let fragment_instance_id = instance_params
        .fragment_instance_id
        .as_ref()
        .ok_or_else(|| {
            QueryLifecycleError::new(
                QueryLifecycleErrorCode::InvalidManifest,
                "stage fragment instance params require fragment instance id",
            )
        })?;
    if fragment_instance_id.hi == 0 && fragment_instance_id.lo == 0 {
        return Err(QueryLifecycleError::new(
            QueryLifecycleErrorCode::InvalidManifest,
            "stage fragment instance id must be nonzero",
        ));
    }
    Ok(UniqueId {
        hi: fragment_instance_id.hi,
        lo: fragment_instance_id.lo,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_execution::contract::QueryId;
    use crate::query_execution::lifecycle::identity::AttemptId;

    fn execution_id() -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(7, 8),
            AttemptId::new(1).expect("nonzero attempt"),
        )
        .expect("nonzero query id")
    }

    fn fragment(lo: i64) -> StageFragment {
        StageFragment::new(
            plan::PlanFragment::default(),
            novarocks::InstanceParams {
                fragment_instance_id: Some(crate::proto::common::UniqueId { hi: 1, lo }),
                ..Default::default()
            },
        )
        .expect("valid fragment")
    }

    #[test]
    fn stage_request_sorts_fragment_ids_and_accepts_service_only_empty_batch() {
        let digest = StageDigest::new([9; 32]);
        let request = QueryStageRequest::new(
            execution_id(),
            ParticipantManifestDigest::new([3; 32]),
            StageDigestVersion::V1,
            digest,
            vec![fragment(9), fragment(3)],
        )
        .expect("valid batch");
        assert_eq!(request.fragments()[0].fragment_instance_id().lo, 3);
        assert_eq!(request.fragments()[1].fragment_instance_id().lo, 9);

        QueryStageRequest::new(
            execution_id(),
            ParticipantManifestDigest::new([3; 32]),
            StageDigestVersion::V1,
            digest,
            vec![],
        )
        .expect("service-only batch is valid");
    }

    #[test]
    fn stage_request_rejects_duplicate_fragment_ids_and_unknown_digest_version() {
        let error = QueryStageRequest::new(
            execution_id(),
            ParticipantManifestDigest::new([3; 32]),
            StageDigestVersion::V1,
            StageDigest::new([9; 32]),
            vec![fragment(3), fragment(3)],
        )
        .expect_err("duplicate ids cannot be staged atomically");
        assert_eq!(error.code(), QueryLifecycleErrorCode::InvalidManifest);
        assert!(StageDigestVersion::try_from_wire(2).is_err());
    }

    #[test]
    fn digest_v1_is_independent_of_fragment_input_order() {
        let first = fragment(9);
        let second = fragment(3);
        let execution_id = execution_id();
        let init_digest = ParticipantManifestDigest::new([2; 32]);

        assert_eq!(
            StageDigest::compute_v1(execution_id, init_digest, &[first.clone(), second.clone()])
                .expect("digest"),
            StageDigest::compute_v1(execution_id, init_digest, &[second, first]).expect("digest")
        );
    }
}
