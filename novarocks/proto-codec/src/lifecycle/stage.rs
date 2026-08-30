//! Validated, role-neutral query Stage and Start wire values.
//!
//! This module owns participant-local wire facts only. Coordinator batching,
//! participant bindings, and launch barriers remain application orchestration
//! concerns outside Protocol.

use prost::Message;
use sha2::{Digest, Sha256};

use novarocks_types::UniqueId;

use crate::{FieldPath, ProtocolError, ProtocolErrorKind};
use novarocks_proto_models::{novarocks, plan};

use super::{
    canonical,
    identity::{QueryExecutionId, decode_query_execution_id, encode_query_execution_id},
    manifest::ParticipantAttemptRef,
};

pub const DEFAULT_STAGE_MAX_ENCODED_BYTES: usize = 48 * 1024 * 1024;
pub const DEFAULT_STAGE_MAX_FRAGMENTS: usize = 256;

/// Fixed-width SHA-256 output of the immutable semantic Stage projection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StageDigest([u8; 32]);

impl StageDigest {
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn try_from_slice(bytes: &[u8]) -> Result<Self, ProtocolError> {
        let bytes: [u8; 32] = bytes.try_into().map_err(|_| {
            ProtocolError::new(
                FieldPath::root("stage_digest"),
                ProtocolErrorKind::InvalidValue,
                "stage digest must be 32 bytes",
            )
        })?;
        Ok(Self(bytes))
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Computes the digest over decoded semantic values. The shared
    /// descriptor-driven projection orders fields by number, sorts maps, and
    /// preserves ordinary repeated-field order. The outer
    /// `StageFragmentsRequest` framing is intentionally excluded.
    // Design: ADR-0127 (docs/adr/ADR-0127-participant-attempt-stage-fence.md)
    pub fn compute(
        participant: ParticipantAttemptRef,
        fragments: &[StageFragment],
    ) -> Result<Self, ProtocolError> {
        let mut ordered = fragments.iter().collect::<Vec<_>>();
        ordered.sort_by_key(|fragment| fragment.fragment_instance_id());
        for pair in ordered.windows(2) {
            if pair[0].fragment_instance_id() == pair[1].fragment_instance_id() {
                return Err(ProtocolError::new(
                    FieldPath::root("stage_fragments"),
                    ProtocolErrorKind::InvalidValue,
                    "stage digest requires unique fragment instance ids",
                ));
            }
        }

        let mut hasher = Sha256::new();
        hasher.update(b"novarocks.query-lifecycle.stage.v2\0");
        let execution_id = participant.execution_id()?;
        hasher.update(execution_id.query_id().high().to_be_bytes());
        hasher.update(execution_id.query_id().low().to_be_bytes());
        hasher.update(execution_id.attempt_id().get().to_be_bytes());
        hasher.update(participant.backend_process_id()?.as_uuid().as_bytes());
        hasher.update(
            u64::try_from(ordered.len())
                .expect("fragment count fits u64")
                .to_be_bytes(),
        );
        for fragment in ordered {
            let instance_id = fragment.fragment_instance_id();
            hasher.update(instance_id.high().to_be_bytes());
            hasher.update(instance_id.low().to_be_bytes());
            hash_stage_message(&mut hasher, "novarocks.plan.PlanFragment", fragment.plan())?;
            hash_stage_message(
                &mut hasher,
                "novarocks.InstanceParams",
                fragment.instance_params(),
            )?;
        }
        Ok(Self(hasher.finalize().into()))
    }
}

fn hash_stage_message<M: Message>(
    hasher: &mut Sha256,
    message_name: &str,
    message: &M,
) -> Result<(), ProtocolError> {
    canonical::hash_message(hasher, message_name, message).map_err(|error| {
        ProtocolError::new(
            FieldPath::root("stage_fragments"),
            ProtocolErrorKind::InvalidValue,
            format!("cannot canonicalize {message_name} for Stage digest: {error}"),
        )
    })
}

/// One static native plan and its dynamic parameters for one exact fragment
/// instance. The generated message is the sole stored representation.
#[derive(Clone, Debug, PartialEq)]
pub struct StageFragment {
    wire: novarocks::StageFragment,
}

impl StageFragment {
    pub fn new(
        plan: plan::PlanFragment,
        instance_params: novarocks::InstanceParams,
    ) -> Result<Self, ProtocolError> {
        Self::parse(novarocks::StageFragment {
            plan: Some(plan),
            instance_params: Some(instance_params),
        })
    }

    pub fn parse(wire: novarocks::StageFragment) -> Result<Self, ProtocolError> {
        let instance_params = wire.instance_params.as_ref().ok_or_else(|| {
            ProtocolError::new(
                FieldPath::root("stage_fragment").field("instance_params"),
                ProtocolErrorKind::InvalidValue,
                "stage fragment instance params are required",
            )
        })?;
        if wire.plan.is_none() {
            return Err(ProtocolError::new(
                FieldPath::root("stage_fragment").field("plan"),
                ProtocolErrorKind::InvalidValue,
                "stage fragment plan is required",
            ));
        }
        let _ = fragment_instance_id(instance_params)?;
        Ok(Self { wire })
    }

    pub const fn as_proto(&self) -> &novarocks::StageFragment {
        &self.wire
    }

    pub fn into_proto(self) -> novarocks::StageFragment {
        self.wire
    }

    pub fn plan(&self) -> &plan::PlanFragment {
        self.wire
            .plan
            .as_ref()
            .expect("validated StageFragment always has a plan")
    }

    pub fn instance_params(&self) -> &novarocks::InstanceParams {
        self.wire
            .instance_params
            .as_ref()
            .expect("validated StageFragment always has instance parameters")
    }

    pub fn fragment_instance_id(&self) -> UniqueId {
        fragment_instance_id(self.instance_params())
            .expect("validated StageFragment always has a nonzero fragment instance id")
    }
}

/// Exact participant-local StageFragments payload. Its generated message is
/// the sole stored representation; accessors decode facts already validated by
/// [`Self::parse`].
#[derive(Clone, Debug, PartialEq)]
pub struct QueryStageRequest {
    wire: novarocks::StageFragmentsRequest,
}

impl QueryStageRequest {
    pub fn new(
        participant: ParticipantAttemptRef,
        mut fragments: Vec<StageFragment>,
    ) -> Result<Self, ProtocolError> {
        fragments.sort_by_key(StageFragment::fragment_instance_id);
        validate_stage_fragment_ids(&fragments)?;
        Self::parse(novarocks::StageFragmentsRequest {
            fragments: fragments
                .into_iter()
                .map(StageFragment::into_proto)
                .collect(),
            participant: Some(participant.as_proto().clone()),
        })
    }

    pub fn parse(wire: novarocks::StageFragmentsRequest) -> Result<Self, ProtocolError> {
        if wire.fragments.len() > DEFAULT_STAGE_MAX_FRAGMENTS {
            return Err(ProtocolError::new(
                FieldPath::root("stage_fragments_request").field("fragments"),
                ProtocolErrorKind::Capacity,
                format!(
                    "stage request contains {} fragments; limit is {DEFAULT_STAGE_MAX_FRAGMENTS}",
                    wire.fragments.len()
                ),
            ));
        }
        if wire.encoded_len() > DEFAULT_STAGE_MAX_ENCODED_BYTES {
            return Err(ProtocolError::new(
                FieldPath::root("stage_fragments_request"),
                ProtocolErrorKind::Capacity,
                format!(
                    "stage request encoded bytes exceed {DEFAULT_STAGE_MAX_ENCODED_BYTES} byte limit"
                ),
            ));
        }

        required_participant(&wire.participant)?;
        let fragments = wire
            .fragments
            .iter()
            .cloned()
            .map(StageFragment::parse)
            .collect::<Result<Vec<_>, _>>()?;
        validate_stage_fragment_ids(&fragments)?;
        Ok(Self { wire })
    }

    pub const fn as_proto(&self) -> &novarocks::StageFragmentsRequest {
        &self.wire
    }

    pub fn into_proto(self) -> novarocks::StageFragmentsRequest {
        self.wire
    }

    pub fn participant(&self) -> ParticipantAttemptRef {
        required_participant(&self.wire.participant)
            .expect("validated QueryStageRequest always has a participant")
    }

    pub fn execution_id(&self) -> QueryExecutionId {
        self.participant()
            .execution_id()
            .expect("validated QueryStageRequest always has an execution id")
    }

    pub fn fragments(&self) -> Vec<StageFragment> {
        self.wire
            .fragments
            .iter()
            .cloned()
            .map(StageFragment::parse)
            .collect::<Result<Vec<_>, _>>()
            .expect("validated QueryStageRequest always has valid stage fragments")
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

/// Validated Stage response backed directly by its generated message.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryStageAck {
    wire: novarocks::StageFragmentsResponse,
}

impl Eq for QueryStageAck {}

impl QueryStageAck {
    pub fn new(
        execution_id: QueryExecutionId,
        digest: StageDigest,
        outcome: QueryStageOutcome,
        detail: impl Into<String>,
    ) -> Result<Self, ProtocolError> {
        Self::parse(novarocks::StageFragmentsResponse {
            execution_id: Some(encode_query_execution_id(execution_id)),
            stage_digest: digest.as_bytes().to_vec(),
            outcome: encode_stage_outcome(outcome),
            detail: detail.into(),
        })
    }

    pub fn parse(wire: novarocks::StageFragmentsResponse) -> Result<Self, ProtocolError> {
        let _ = required_execution_id(wire.execution_id.as_ref())?;
        let _ = StageDigest::try_from_slice(&wire.stage_digest)?;
        let _ = decode_stage_outcome(wire.outcome)?;
        Ok(Self { wire })
    }

    pub const fn as_proto(&self) -> &novarocks::StageFragmentsResponse {
        &self.wire
    }

    pub fn into_proto(self) -> novarocks::StageFragmentsResponse {
        self.wire
    }

    pub fn execution_id(&self) -> QueryExecutionId {
        required_execution_id(self.wire.execution_id.as_ref())
            .expect("validated QueryStageAck always has an execution id")
    }

    pub fn digest(&self) -> StageDigest {
        StageDigest::try_from_slice(&self.wire.stage_digest)
            .expect("validated QueryStageAck always has a stage digest")
    }

    pub fn outcome(&self) -> QueryStageOutcome {
        decode_stage_outcome(self.wire.outcome)
            .expect("validated QueryStageAck always has a known outcome")
    }

    pub fn detail(&self) -> &str {
        &self.wire.detail
    }
}

/// Validated Start request backed directly by its generated message.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryStartRequest {
    wire: novarocks::StartPreparedQueryRequest,
}

impl Eq for QueryStartRequest {}

impl QueryStartRequest {
    pub fn new(execution_id: QueryExecutionId, digest: StageDigest) -> Result<Self, ProtocolError> {
        Self::parse(novarocks::StartPreparedQueryRequest {
            execution_id: Some(encode_query_execution_id(execution_id)),
            stage_digest: digest.as_bytes().to_vec(),
        })
    }

    pub fn parse(wire: novarocks::StartPreparedQueryRequest) -> Result<Self, ProtocolError> {
        let _ = required_execution_id(wire.execution_id.as_ref())?;
        let _ = StageDigest::try_from_slice(&wire.stage_digest)?;
        Ok(Self { wire })
    }

    pub const fn as_proto(&self) -> &novarocks::StartPreparedQueryRequest {
        &self.wire
    }

    pub fn into_proto(self) -> novarocks::StartPreparedQueryRequest {
        self.wire
    }

    pub fn execution_id(&self) -> QueryExecutionId {
        required_execution_id(self.wire.execution_id.as_ref())
            .expect("validated QueryStartRequest always has an execution id")
    }

    pub fn digest(&self) -> StageDigest {
        StageDigest::try_from_slice(&self.wire.stage_digest)
            .expect("validated QueryStartRequest always has a stage digest")
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

/// Validated Start response backed directly by its generated message.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryStartAck {
    wire: novarocks::StartPreparedQueryResponse,
}

impl Eq for QueryStartAck {}

impl QueryStartAck {
    pub fn new(
        execution_id: QueryExecutionId,
        digest: StageDigest,
        outcome: QueryStartOutcome,
        detail: impl Into<String>,
    ) -> Result<Self, ProtocolError> {
        Self::parse(novarocks::StartPreparedQueryResponse {
            execution_id: Some(encode_query_execution_id(execution_id)),
            stage_digest: digest.as_bytes().to_vec(),
            outcome: encode_start_outcome(outcome),
            detail: detail.into(),
        })
    }

    pub fn parse(wire: novarocks::StartPreparedQueryResponse) -> Result<Self, ProtocolError> {
        let _ = required_execution_id(wire.execution_id.as_ref())?;
        let _ = StageDigest::try_from_slice(&wire.stage_digest)?;
        let _ = decode_start_outcome(wire.outcome)?;
        Ok(Self { wire })
    }

    pub const fn as_proto(&self) -> &novarocks::StartPreparedQueryResponse {
        &self.wire
    }

    pub fn into_proto(self) -> novarocks::StartPreparedQueryResponse {
        self.wire
    }

    pub fn execution_id(&self) -> QueryExecutionId {
        required_execution_id(self.wire.execution_id.as_ref())
            .expect("validated QueryStartAck always has an execution id")
    }

    pub fn digest(&self) -> StageDigest {
        StageDigest::try_from_slice(&self.wire.stage_digest)
            .expect("validated QueryStartAck always has a stage digest")
    }

    pub fn outcome(&self) -> QueryStartOutcome {
        decode_start_outcome(self.wire.outcome)
            .expect("validated QueryStartAck always has a known outcome")
    }

    pub fn detail(&self) -> &str {
        &self.wire.detail
    }
}

fn required_execution_id(
    execution_id: Option<&novarocks::QueryExecutionId>,
) -> Result<QueryExecutionId, ProtocolError> {
    let execution_id = execution_id.ok_or_else(|| {
        ProtocolError::new(
            FieldPath::root("query_execution_id"),
            ProtocolErrorKind::InvalidValue,
            "query execution id is required",
        )
    })?;
    decode_query_execution_id(execution_id)
}

fn required_participant(
    participant: &Option<novarocks::ParticipantAttemptRef>,
) -> Result<ParticipantAttemptRef, ProtocolError> {
    let participant = participant.clone().ok_or_else(|| {
        ProtocolError::new(
            FieldPath::root("stage_fragments_request").field("participant"),
            ProtocolErrorKind::MissingField,
            "stage participant is required",
        )
    })?;
    ParticipantAttemptRef::parse(participant)
}

fn fragment_instance_id(
    instance_params: &novarocks::InstanceParams,
) -> Result<UniqueId, ProtocolError> {
    let fragment_instance_id = instance_params
        .fragment_instance_id
        .as_ref()
        .ok_or_else(|| {
            ProtocolError::new(
                FieldPath::root("stage_fragment")
                    .field("instance_params")
                    .field("fragment_instance_id"),
                ProtocolErrorKind::InvalidValue,
                "stage fragment instance params require fragment instance id",
            )
        })?;
    if fragment_instance_id.hi == 0 && fragment_instance_id.lo == 0 {
        return Err(ProtocolError::new(
            FieldPath::root("stage_fragment")
                .field("instance_params")
                .field("fragment_instance_id"),
            ProtocolErrorKind::InvalidValue,
            "stage fragment instance id must be nonzero",
        ));
    }
    Ok(UniqueId::new(
        fragment_instance_id.hi,
        fragment_instance_id.lo,
    ))
}

fn validate_stage_fragment_ids(fragments: &[StageFragment]) -> Result<(), ProtocolError> {
    let mut instance_ids = fragments
        .iter()
        .map(StageFragment::fragment_instance_id)
        .collect::<Vec<_>>();
    instance_ids.sort_unstable();
    if instance_ids.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(ProtocolError::new(
            FieldPath::root("stage_fragments_request").field("fragments"),
            ProtocolErrorKind::InvalidValue,
            "stage fragment instance ids must be unique",
        ));
    }
    Ok(())
}

fn encode_stage_outcome(outcome: QueryStageOutcome) -> i32 {
    use novarocks::StageFragmentsOutcome as Wire;

    match outcome {
        QueryStageOutcome::Applied => Wire::StageFragmentsApplied,
        QueryStageOutcome::AlreadyApplied => Wire::StageFragmentsAlreadyApplied,
        QueryStageOutcome::RejectedConflict => Wire::StageFragmentsRejectedConflict,
        QueryStageOutcome::RejectedInvalidState => Wire::StageFragmentsRejectedInvalidState,
        QueryStageOutcome::RejectedInvalidBatch => Wire::StageFragmentsRejectedInvalidBatch,
        QueryStageOutcome::RejectedCapacity => Wire::StageFragmentsRejectedCapacity,
        QueryStageOutcome::RejectedTerminated => Wire::StageFragmentsRejectedTerminated,
        QueryStageOutcome::RejectedLocalFailure => Wire::StageFragmentsRejectedLocalFailure,
    }
    .into()
}

fn decode_stage_outcome(value: i32) -> Result<QueryStageOutcome, ProtocolError> {
    use novarocks::StageFragmentsOutcome as Wire;

    match Wire::try_from(value).ok() {
        Some(Wire::StageFragmentsApplied) => Ok(QueryStageOutcome::Applied),
        Some(Wire::StageFragmentsAlreadyApplied) => Ok(QueryStageOutcome::AlreadyApplied),
        Some(Wire::StageFragmentsRejectedConflict) => Ok(QueryStageOutcome::RejectedConflict),
        Some(Wire::StageFragmentsRejectedInvalidState) => {
            Ok(QueryStageOutcome::RejectedInvalidState)
        }
        Some(Wire::StageFragmentsRejectedInvalidBatch) => {
            Ok(QueryStageOutcome::RejectedInvalidBatch)
        }
        Some(Wire::StageFragmentsRejectedCapacity) => Ok(QueryStageOutcome::RejectedCapacity),
        Some(Wire::StageFragmentsRejectedTerminated) => Ok(QueryStageOutcome::RejectedTerminated),
        Some(Wire::StageFragmentsRejectedLocalFailure) => {
            Ok(QueryStageOutcome::RejectedLocalFailure)
        }
        Some(Wire::Unspecified) | None => Err(ProtocolError::new(
            FieldPath::root("stage_fragments_response").field("outcome"),
            ProtocolErrorKind::InvalidValue,
            format!("unknown stage fragments outcome {value}"),
        )),
    }
}

fn encode_start_outcome(outcome: QueryStartOutcome) -> i32 {
    use novarocks::StartPreparedQueryOutcome as Wire;

    match outcome {
        QueryStartOutcome::Applied => Wire::StartPreparedQueryApplied,
        QueryStartOutcome::AlreadyStarted => Wire::StartPreparedQueryAlreadyStarted,
        QueryStartOutcome::RejectedNotStaged => Wire::StartPreparedQueryRejectedNotStaged,
        QueryStartOutcome::RejectedConflict => Wire::StartPreparedQueryRejectedConflict,
        QueryStartOutcome::RejectedTerminated => Wire::StartPreparedQueryRejectedTerminated,
    }
    .into()
}

fn decode_start_outcome(value: i32) -> Result<QueryStartOutcome, ProtocolError> {
    use novarocks::StartPreparedQueryOutcome as Wire;

    match Wire::try_from(value).ok() {
        Some(Wire::StartPreparedQueryApplied) => Ok(QueryStartOutcome::Applied),
        Some(Wire::StartPreparedQueryAlreadyStarted) => Ok(QueryStartOutcome::AlreadyStarted),
        Some(Wire::StartPreparedQueryRejectedNotStaged) => Ok(QueryStartOutcome::RejectedNotStaged),
        Some(Wire::StartPreparedQueryRejectedConflict) => Ok(QueryStartOutcome::RejectedConflict),
        Some(Wire::StartPreparedQueryRejectedTerminated) => {
            Ok(QueryStartOutcome::RejectedTerminated)
        }
        Some(Wire::Unspecified) | None => Err(ProtocolError::new(
            FieldPath::root("start_prepared_query_response").field("outcome"),
            ProtocolErrorKind::InvalidValue,
            format!("unknown start prepared query outcome {value}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use novarocks_types::QueryId;

    use super::*;
    use crate::lifecycle::identity::AttemptId;
    use novarocks_proto_models::common;

    fn execution_id() -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(7, 8),
            AttemptId::new(1).expect("nonzero attempt"),
        )
        .expect("nonzero query id")
    }

    fn participant() -> ParticipantAttemptRef {
        ParticipantAttemptRef::new(execution_id(), novarocks_types::BackendProcessId::new_v7())
            .expect("valid participant")
    }

    fn golden_participant() -> ParticipantAttemptRef {
        ParticipantAttemptRef::new(
            QueryExecutionId::new(
                QueryId::new(0x0102_0304_0506_0708, 0x1112_1314_1516_1718),
                AttemptId::new(7).expect("nonzero attempt"),
            )
            .expect("nonzero query id"),
            novarocks_types::BackendProcessId::try_from_bytes([
                0x01, 0x9c, 0x98, 0xa9, 0x33, 0x90, 0x75, 0x76, 0x97, 0x7b, 0x33, 0xd1, 0x88, 0xad,
                0x1f, 0x06,
            ])
            .expect("fixed UUIDv7 backend process id"),
        )
        .expect("valid participant")
    }

    fn fragment(lo: i64) -> StageFragment {
        StageFragment::new(
            plan::PlanFragment::default(),
            novarocks::InstanceParams {
                fragment_instance_id: Some(common::UniqueId { hi: 1, lo }),
                ..Default::default()
            },
        )
        .expect("valid fragment")
    }

    fn fragment_with_maps(lo: i64, reverse_insert: bool) -> StageFragment {
        let mut per_node_scan_ranges = HashMap::new();
        let mut per_exch_num_senders = HashMap::new();
        let entries = if reverse_insert {
            [(9, 90), (3, 30)]
        } else {
            [(3, 30), (9, 90)]
        };
        for (key, value) in entries {
            per_node_scan_ranges.insert(
                key,
                novarocks::ScanRangeList {
                    ranges: vec![novarocks::ScanRangeParams {
                        volume_id: Some(value),
                        ..Default::default()
                    }],
                },
            );
            per_exch_num_senders.insert(key, value);
        }
        StageFragment::new(
            plan::PlanFragment::default(),
            novarocks::InstanceParams {
                fragment_instance_id: Some(common::UniqueId { hi: 1, lo }),
                per_node_scan_ranges,
                per_exch_num_senders,
                ..Default::default()
            },
        )
        .expect("valid fragment")
    }

    #[test]
    fn stage_request_sorts_fragment_ids_and_preserves_valid_wire_round_trip() {
        let first = fragment(9);
        let second = fragment(3);
        let request = QueryStageRequest::new(participant(), vec![first.clone(), second.clone()])
            .expect("valid batch");
        assert_eq!(request.fragments()[0].fragment_instance_id().low(), 3);
        assert_eq!(request.fragments()[1].fragment_instance_id().low(), 9);

        let round_tripped =
            QueryStageRequest::parse(request.as_proto().clone()).expect("valid generated request");
        assert_eq!(round_tripped.as_proto(), request.as_proto());

        // Both roles derive the same identity from the carried content.
        let participant = request.participant();
        assert_eq!(
            StageDigest::compute(participant.clone(), &request.fragments()).expect("sender digest"),
            StageDigest::compute(participant, &[second, first]).expect("receiver digest")
        );
    }

    #[test]
    fn stage_request_rejects_duplicate_fragment_ids_and_missing_participant() {
        let raw = novarocks::StageFragmentsRequest {
            fragments: vec![fragment(3).into_proto(), fragment(3).into_proto()],
            participant: Some(participant().as_proto().clone()),
        };
        let error = QueryStageRequest::parse(raw).expect_err("duplicate ids cannot be staged");
        assert_eq!(error.detail(), "stage fragment instance ids must be unique");
        assert_eq!(error.kind(), ProtocolErrorKind::InvalidValue);
        assert_eq!(
            error.path().to_string(),
            "stage_fragments_request.fragments"
        );
        let raw = novarocks::StageFragmentsRequest {
            fragments: vec![fragment(3).into_proto()],
            ..Default::default()
        };
        let error = QueryStageRequest::parse(raw).expect_err("participant is required");
        assert_eq!(
            error.path().to_string(),
            "stage_fragments_request.participant"
        );

        let legacy = novarocks::StageFragmentsRequest::decode(&[0x0a, 0x00][..])
            .expect("retired execution-id field stays decodable as unknown wire data");
        let error = QueryStageRequest::parse(legacy).expect_err("legacy request must fail closed");
        assert_eq!(
            error.path().to_string(),
            "stage_fragments_request.participant"
        );
    }

    #[test]
    fn digest_is_independent_of_fragment_input_order() {
        let first = fragment(9);
        let second = fragment(3);
        let participant = participant();

        assert_eq!(
            StageDigest::compute(participant.clone(), &[first.clone(), second.clone()])
                .expect("digest"),
            StageDigest::compute(participant, &[second, first]).expect("digest")
        );
    }

    #[test]
    fn stage_digest_matches_fixed_canonical_golden() {
        let digest = StageDigest::compute(
            golden_participant(),
            &[fragment_with_maps(3, false), fragment(9)],
        )
        .expect("digest");
        assert_eq!(
            digest
                .as_bytes()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
            "b70b8bc407f68fde43c4a25c67e6067f7596e3fd4759b7b6262dfe9ab4dcdbde"
        );
    }

    #[test]
    fn digest_binds_the_participant_process_including_empty_stages() {
        let first = participant();
        let second =
            ParticipantAttemptRef::new(execution_id(), novarocks_types::BackendProcessId::new_v7())
                .expect("second participant");

        assert_ne!(
            StageDigest::compute(first.clone(), &[]).expect("first empty stage"),
            StageDigest::compute(second.clone(), &[]).expect("second empty stage")
        );
        assert_ne!(
            StageDigest::compute(first, &[fragment(3)]).expect("first stage"),
            StageDigest::compute(second, &[fragment(3)]).expect("second stage")
        );
    }

    #[test]
    fn digest_sorts_maps_but_preserves_repeated_semantic_order() {
        let participant = participant();
        assert_eq!(
            StageDigest::compute(participant.clone(), &[fragment_with_maps(3, false)])
                .expect("digest"),
            StageDigest::compute(participant.clone(), &[fragment_with_maps(3, true)])
                .expect("digest")
        );

        let mut first = fragment(3).into_proto().instance_params.expect("params");
        first.destinations = vec![
            novarocks::Destination {
                endpoint: "be-a:9020".to_string(),
                ..Default::default()
            },
            novarocks::Destination {
                endpoint: "be-b:9020".to_string(),
                ..Default::default()
            },
        ];
        let mut second = first.clone();
        second.destinations.reverse();
        let first = StageFragment::new(plan::PlanFragment::default(), first).expect("fragment");
        let second = StageFragment::new(plan::PlanFragment::default(), second).expect("fragment");
        assert_ne!(
            StageDigest::compute(participant.clone(), &[first]).expect("digest"),
            StageDigest::compute(participant, &[second]).expect("digest")
        );
    }

    #[test]
    fn digest_preserves_optional_presence_and_rejects_non_finite_values() {
        let participant = participant();
        let mut absent = fragment(3).into_proto().instance_params.expect("params");
        absent.query_options = Some(novarocks::QueryOptions::default());
        let mut present = absent.clone();
        present
            .query_options
            .as_mut()
            .expect("query options")
            .runtime_filter_wait_timeout_ms = Some(0);
        let absent =
            StageFragment::new(plan::PlanFragment::default(), absent).expect("valid fragment");
        let present =
            StageFragment::new(plan::PlanFragment::default(), present).expect("valid fragment");
        assert_ne!(
            StageDigest::compute(participant.clone(), &[absent]).expect("digest"),
            StageDigest::compute(participant.clone(), &[present]).expect("digest")
        );

        let mut non_finite = fragment(3).into_proto().instance_params.expect("params");
        non_finite.per_node_scan_ranges.insert(
            1,
            novarocks::ScanRangeList {
                ranges: vec![novarocks::ScanRangeParams {
                    range: Some(novarocks::ScanRange {
                        kind: Some(novarocks::scan_range::Kind::File(
                            novarocks::FileScanRange {
                                file_pruning_min_max_values: HashMap::from([(
                                    1,
                                    novarocks::FilePruningMinMaxValue {
                                        min_float_value: Some(f64::NAN),
                                        ..Default::default()
                                    },
                                )]),
                                ..Default::default()
                            },
                        )),
                    }),
                    ..Default::default()
                }],
            },
        );
        let non_finite =
            StageFragment::new(plan::PlanFragment::default(), non_finite).expect("fragment");
        let error = StageDigest::compute(participant, &[non_finite])
            .expect_err("NaN must not enter a Stage digest");
        assert!(error.detail().contains("non-finite"));
    }

    #[test]
    fn stage_and_start_outcomes_reject_unspecified_and_unknown_wire_values() {
        for outcome in [
            QueryStageOutcome::Applied,
            QueryStageOutcome::AlreadyApplied,
            QueryStageOutcome::RejectedConflict,
            QueryStageOutcome::RejectedInvalidState,
            QueryStageOutcome::RejectedInvalidBatch,
            QueryStageOutcome::RejectedCapacity,
            QueryStageOutcome::RejectedTerminated,
            QueryStageOutcome::RejectedLocalFailure,
        ] {
            assert_eq!(
                decode_stage_outcome(encode_stage_outcome(outcome)),
                Ok(outcome)
            );
        }
        for outcome in [
            QueryStartOutcome::Applied,
            QueryStartOutcome::AlreadyStarted,
            QueryStartOutcome::RejectedNotStaged,
            QueryStartOutcome::RejectedConflict,
            QueryStartOutcome::RejectedTerminated,
        ] {
            assert_eq!(
                decode_start_outcome(encode_start_outcome(outcome)),
                Ok(outcome)
            );
        }

        assert!(decode_stage_outcome(0).is_err());
        assert!(decode_stage_outcome(99).is_err());
        assert!(decode_start_outcome(0).is_err());
        assert!(decode_start_outcome(99).is_err());
    }
}
