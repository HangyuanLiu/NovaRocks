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

//! Temporary BE-local bridge between the Protocol-owned wire values and the
//! legacy registry implementation.
//!
//! Native ingress validates `novarocks_protocol::lifecycle` values before it
//! reaches this module.  Keeping the conversion next to the existing registry
//! avoids a Core facade while the registry's terminal/profile representation is
//! migrated to generated Protocol carriers.

use novarocks::query_execution::lifecycle::contract::{
    decode_abort_query_request, decode_participant_manifest, decode_query_control_attach,
    decode_query_control_command, decode_query_stage_request, decode_query_start_request,
    encode_abort_query_response, encode_query_init_response, encode_query_stage_response,
    encode_query_start_response,
};
use novarocks::query_execution::lifecycle::{
    AttemptId as LegacyAttemptId, ParticipantManifestDigest as LegacyParticipantManifestDigest,
    ParticipantTerminalOutcome as LegacyParticipantTerminalOutcome,
    QueryAbortRequest as LegacyQueryAbortRequest, QueryControlAttach as LegacyQueryControlAttach,
    QueryControlCommand as LegacyQueryControlCommand, QueryExecutionId as LegacyQueryExecutionId,
    QueryInitAck as LegacyQueryInitAck, QueryInitRequest as LegacyQueryInitRequest,
    QueryLifecycleError, QueryLifecycleErrorCode, QueryStageAck as LegacyQueryStageAck,
    QueryStageRequest as LegacyQueryStageRequest, QueryStartAck as LegacyQueryStartAck,
    QueryStartRequest as LegacyQueryStartRequest, QueryTerminationAck as LegacyQueryTerminationAck,
};
use novarocks_protocol::lifecycle::{
    ParticipantTerminalOutcome, QueryAbortRequest, QueryControlAttach, QueryInitAck,
    QueryInitRequest, QueryStageAck, QueryStageRequest, QueryStartAck, QueryStartRequest,
    QueryTerminationAck,
};
use novarocks_protocol::{common as wire_common, novarocks as wire};
use novarocks_types::QueryId;

pub(crate) fn legacy_init_request(
    request: QueryInitRequest,
) -> Result<LegacyQueryInitRequest, QueryLifecycleError> {
    let manifest = request
        .as_proto()
        .manifest
        .as_ref()
        .ok_or_else(|| {
            QueryLifecycleError::new(
                QueryLifecycleErrorCode::InvalidManifest,
                "participant manifest is required",
            )
        })
        .and_then(decode_participant_manifest)?;
    let digest = request
        .digest()
        .expect("validated Protocol InitQuery request has a fixed-width digest");
    Ok(LegacyQueryInitRequest::from_validated_protocol_manifest(
        manifest,
        LegacyParticipantManifestDigest::new(*digest.as_bytes()),
    ))
}

pub(crate) fn protocol_init_ack(value: &LegacyQueryInitAck) -> QueryInitAck {
    QueryInitAck::parse(encode_query_init_response(value))
        .expect("legacy InitQuery acknowledgement remains a valid Protocol response")
}

pub(crate) fn legacy_stage_request(
    request: QueryStageRequest,
) -> Result<LegacyQueryStageRequest, QueryLifecycleError> {
    decode_query_stage_request(request.as_proto())
}

pub(crate) fn protocol_stage_ack(value: &LegacyQueryStageAck) -> QueryStageAck {
    QueryStageAck::parse(encode_query_stage_response(value))
        .expect("legacy StageFragments acknowledgement remains a valid Protocol response")
}

pub(crate) fn legacy_start_request(
    request: QueryStartRequest,
) -> Result<LegacyQueryStartRequest, QueryLifecycleError> {
    decode_query_start_request(request.as_proto())
}

pub(crate) fn protocol_start_ack(value: &LegacyQueryStartAck) -> QueryStartAck {
    QueryStartAck::parse(encode_query_start_response(value))
        .expect("legacy StartPreparedQuery acknowledgement remains a valid Protocol response")
}

pub(crate) fn legacy_abort_request(
    request: QueryAbortRequest,
) -> Result<LegacyQueryAbortRequest, QueryLifecycleError> {
    decode_abort_query_request(request.as_proto())
}

pub(crate) fn protocol_termination_ack(value: &LegacyQueryTerminationAck) -> QueryTerminationAck {
    QueryTerminationAck::parse(encode_abort_query_response(value))
        .expect("legacy AbortQuery acknowledgement remains a valid Protocol response")
}

pub(crate) fn legacy_control_attach(
    attach: QueryControlAttach,
) -> Result<LegacyQueryControlAttach, QueryLifecycleError> {
    let frame = novarocks_protocol::novarocks::QueryControlRequest {
        command: Some(
            novarocks_protocol::novarocks::query_control_request::Command::Attach(
                attach.as_proto().clone(),
            ),
        ),
    };
    decode_query_control_attach(&frame)
}

pub(crate) fn legacy_control_command(
    command: novarocks_protocol::lifecycle::QueryControlCommand,
) -> Result<LegacyQueryControlCommand, QueryLifecycleError> {
    decode_query_control_command(command.as_proto())
}

pub(crate) fn legacy_execution_id(
    execution_id: novarocks_protocol::lifecycle::QueryExecutionId,
) -> Result<LegacyQueryExecutionId, QueryLifecycleError> {
    LegacyQueryExecutionId::new(
        QueryId::new(
            execution_id.query_id().high(),
            execution_id.query_id().low(),
        ),
        LegacyAttemptId::new(execution_id.attempt_id().get())?,
    )
}

pub(crate) fn protocol_execution_id(
    execution_id: LegacyQueryExecutionId,
) -> novarocks_protocol::lifecycle::QueryExecutionId {
    novarocks_protocol::lifecycle::QueryExecutionId::new(
        QueryId::new(
            execution_id.query_id().high(),
            execution_id.query_id().low(),
        ),
        novarocks_protocol::lifecycle::AttemptId::new(execution_id.attempt_id().get())
            .expect("legacy query execution id always has a nonzero attempt"),
    )
    .expect("legacy query execution id is always valid under the Protocol contract")
}

/// Encode the BE-owned terminal report directly into the Protocol carrier.
///
/// The registry still owns the pre-CLS-R2 retention and control-event state,
/// but fallback delivery must not round-trip that state through the retired
/// Core lifecycle contract codec.  Runtime profiles and sink facts are
/// projected here, at their BE capture boundary, into their generated forms.
pub(crate) fn protocol_terminal_outcome(
    outcome: &LegacyParticipantTerminalOutcome,
) -> ParticipantTerminalOutcome {
    use wire::participant_terminal_outcome::Outcome;
    use wire::query_terminal_profile_contribution_telemetry::Telemetry as ProfileTelemetry;

    let raw = match outcome {
        LegacyParticipantTerminalOutcome::Proof { proof, snapshot } => {
            let snapshot = wire::QueryTerminalSnapshot {
                version: snapshot.version(),
                execution_id: Some(protocol_execution_id(snapshot.execution_id()).to_proto()),
                backend: Some(protocol_backend(snapshot.backend())),
                init_digest: snapshot.init_digest().as_bytes().to_vec(),
                digest: snapshot.digest().as_bytes().to_vec(),
                fragments: snapshot
                    .fragments()
                    .iter()
                    .map(protocol_terminal_fragment)
                    .collect(),
                profile_contribution: Some(match snapshot.profile_contribution_telemetry() {
                    novarocks::query_execution::lifecycle::TerminalTelemetry::Available(value) => {
                        wire::QueryTerminalProfileContributionTelemetry {
                            telemetry: Some(ProfileTelemetry::Available(
                                protocol_profile_contribution(value),
                            )),
                        }
                    }
                    novarocks::query_execution::lifecycle::TerminalTelemetry::Unavailable(value) => {
                        wire::QueryTerminalProfileContributionTelemetry {
                            telemetry: Some(ProfileTelemetry::Unavailable(
                                wire::TerminalTelemetryUnavailable {
                                    stage: value.stage().to_owned(),
                                    code: value.code().to_owned(),
                                },
                            )),
                        }
                    }
                }),
            };
            let proof = wire::TerminalizationProof {
                version: proof.version(),
                execution_id: Some(protocol_execution_id(proof.execution_id()).to_proto()),
                backend: Some(protocol_backend(proof.backend())),
                init_digest: proof.init_digest().as_bytes().to_vec(),
                digest: proof.digest().as_bytes().to_vec(),
                fragments: proof
                    .fragments()
                    .iter()
                    .map(|fragment| {
                        let (outcome, error_code, error_detail, error_detail_truncated) =
                            protocol_fragment_outcome(fragment.outcome());
                        wire::TerminalizationProofFragment {
                            fragment_instance_id: Some(protocol_unique_id(
                                fragment.fragment_instance_id(),
                            )),
                            backend_num: fragment.backend_num(),
                            outcome,
                            error_code,
                            error_detail,
                            error_detail_truncated,
                        }
                    })
                    .collect(),
            };
            wire::ParticipantTerminalOutcome {
                outcome: Some(Outcome::Proof(proof)),
                snapshot: Some(snapshot),
            }
        }
        LegacyParticipantTerminalOutcome::NegativeAttestation(attestation) => {
            wire::ParticipantTerminalOutcome {
                outcome: Some(Outcome::NegativeAttestation(wire::NegativeAttestation {
                    execution_id: Some(
                        protocol_execution_id(attestation.execution_id())
                            .to_proto(),
                    ),
                    backend: Some(protocol_backend(attestation.backend())),
                    init_digest: attestation.init_digest().as_bytes().to_vec(),
                    reason: match attestation.reason() {
                        novarocks::query_execution::lifecycle::NegativeAttestationReason::AttemptAborted => {
                            wire::NegativeAttestationReason::AttemptAborted as i32
                        }
                        novarocks::query_execution::lifecycle::NegativeAttestationReason::AttemptTombstoned => {
                            wire::NegativeAttestationReason::AttemptTombstoned as i32
                        }
                        novarocks::query_execution::lifecycle::NegativeAttestationReason::TerminalStateInvalid => {
                            wire::NegativeAttestationReason::TerminalStateInvalid as i32
                        }
                        novarocks::query_execution::lifecycle::NegativeAttestationReason::CorrectnessEvidenceEncodingFailed => {
                            wire::NegativeAttestationReason::CorrectnessEvidenceEncodingFailed as i32
                        }
                        novarocks::query_execution::lifecycle::NegativeAttestationReason::CorrectnessEvidenceRetentionExhausted => {
                            wire::NegativeAttestationReason::CorrectnessEvidenceRetentionExhausted as i32
                        }
                    },
                    detail: attestation.detail().to_owned(),
                    detail_truncated: attestation.detail_truncated(),
                    digest: attestation.digest().as_bytes().to_vec(),
                })),
                snapshot: None,
            }
        }
    };
    ParticipantTerminalOutcome::parse(raw)
        .expect("retained BE terminal outcome must satisfy the Protocol contract")
}

fn protocol_terminal_fragment(
    fragment: &novarocks::query_execution::lifecycle::FragmentTerminalSnapshot,
) -> wire::QueryTerminalFragmentSnapshot {
    use wire::fragment_terminal_profile_telemetry::Telemetry;

    let (outcome, error_code, error_detail, error_detail_truncated) =
        protocol_fragment_outcome(fragment.outcome());
    wire::QueryTerminalFragmentSnapshot {
        fragment_instance_id: Some(protocol_unique_id(fragment.fragment_instance_id())),
        backend_num: fragment.backend_num(),
        outcome,
        error_code,
        error_detail,
        error_detail_truncated,
        connector_staged_report_frames: fragment
            .sink()
            .connector_staged_report_frames
            .iter()
            .map(protocol_connector_staged_report_frame)
            .collect(),
        tablet_commit_infos: fragment
            .sink()
            .tablet_commit_infos
            .iter()
            .map(|value| wire::QueryTerminalTabletInfo {
                tablet_id: value.tablet_id,
                backend_id: value.backend_id,
            })
            .collect(),
        tablet_fail_infos: fragment
            .sink()
            .tablet_fail_infos
            .iter()
            .map(|value| wire::QueryTerminalTabletInfo {
                tablet_id: value.tablet_id,
                backend_id: value.backend_id,
            })
            .collect(),
        load_stats: Some(wire::QueryTerminalLoadStats {
            loaded_rows: fragment.sink().load_stats.loaded_rows,
            loaded_bytes: fragment.sink().load_stats.loaded_bytes,
            filtered_rows: fragment.sink().load_stats.filtered_rows,
        }),
        profile: Some(match fragment.profile_telemetry() {
            novarocks::query_execution::lifecycle::TerminalTelemetry::Available(profile) => {
                wire::FragmentTerminalProfileTelemetry {
                    telemetry: Some(Telemetry::Available(protocol_runtime_profile_tree(profile))),
                }
            }
            novarocks::query_execution::lifecycle::TerminalTelemetry::Unavailable(value) => {
                wire::FragmentTerminalProfileTelemetry {
                    telemetry: Some(Telemetry::Unavailable(wire::TerminalTelemetryUnavailable {
                        stage: value.stage().to_owned(),
                        code: value.code().to_owned(),
                    })),
                }
            }
        }),
        statistics_payload: fragment.statistics_payload().to_vec(),
    }
}

fn protocol_fragment_outcome(
    outcome: &novarocks::query_execution::lifecycle::FragmentTerminalOutcome,
) -> (i32, String, String, bool) {
    match outcome {
        novarocks::query_execution::lifecycle::FragmentTerminalOutcome::Succeeded => (
            wire::QueryTerminalFragmentOutcome::Succeeded as i32,
            String::new(),
            String::new(),
            false,
        ),
        novarocks::query_execution::lifecycle::FragmentTerminalOutcome::Failed {
            code,
            detail,
            detail_truncated,
        } => (
            wire::QueryTerminalFragmentOutcome::Failed as i32,
            code.clone(),
            detail.clone(),
            *detail_truncated,
        ),
        novarocks::query_execution::lifecycle::FragmentTerminalOutcome::Cancelled {
            detail,
            detail_truncated,
        } => (
            wire::QueryTerminalFragmentOutcome::Cancelled as i32,
            "CANCELLED".to_owned(),
            detail.clone(),
            *detail_truncated,
        ),
        novarocks::query_execution::lifecycle::FragmentTerminalOutcome::IncompleteDrain {
            detail,
            detail_truncated,
        } => (
            wire::QueryTerminalFragmentOutcome::IncompleteDrain as i32,
            "INCOMPLETE_DRAIN".to_owned(),
            detail.clone(),
            *detail_truncated,
        ),
    }
}

fn protocol_backend(
    backend: &novarocks::query_execution::lifecycle::ParticipantBackendIdentity,
) -> wire::ParticipantBackendIdentity {
    wire::ParticipantBackendIdentity {
        backend_id: backend.backend_id(),
        endpoint: Some(wire::QueryControlEndpoint {
            host: backend.endpoint().host().to_owned(),
            port: u32::from(backend.endpoint().port()),
        }),
        start_epoch: backend.start_epoch(),
    }
}

fn protocol_unique_id(value: novarocks_types::UniqueId) -> wire_common::UniqueId {
    wire_common::UniqueId {
        hi: value.high(),
        lo: value.low(),
    }
}

fn protocol_profile_contribution(
    contribution: &novarocks::query_execution::lifecycle::QueryTerminalProfileContributionV1,
) -> wire::QueryTerminalProfileContributionV1 {
    // The Protocol terminal wrapper validates sorting and all key constraints.
    // This projection intentionally stays at the BE capture boundary; the
    // legacy Core contribution remains only until the registry state itself is
    // cut over together with the BE control-event contract.
    let snapshot = novarocks::query_execution::lifecycle::QueryTerminalSnapshot::new_with_profile_contribution(
        novarocks::query_execution::lifecycle::QueryExecutionId::new(
            QueryId::new(1, 1),
            LegacyAttemptId::new(1).expect("one is a valid attempt"),
        )
        .expect("static execution identity"),
        novarocks::query_execution::lifecycle::ParticipantBackendIdentity::new(
            1,
            novarocks::query_execution::lifecycle::QueryControlEndpoint::new(
                "terminal-projection",
                1,
            )
                .expect("static endpoint"),
            1,
        )
        .expect("static backend identity"),
        LegacyParticipantManifestDigest::new([0; 32]),
        Vec::new(),
        contribution.clone(),
    )
    .expect("validated terminal contribution");
    novarocks::query_execution::lifecycle::encode_query_terminal_snapshot(&snapshot)
        .profile_contribution
        .expect("encoded terminal snapshot always contains profile telemetry")
        .telemetry
        .and_then(|telemetry| match telemetry {
            wire::query_terminal_profile_contribution_telemetry::Telemetry::Available(value) => {
                Some(value)
            }
            wire::query_terminal_profile_contribution_telemetry::Telemetry::Unavailable(_) => None,
        })
        .expect("available terminal contribution stays available")
}

fn protocol_runtime_profile_tree(
    tree: &novarocks_execution::runtime::profile::RuntimeProfileTree,
) -> wire::RuntimeProfileTree {
    wire::RuntimeProfileTree {
        root: Some(protocol_profile_node(&tree.root)),
    }
}

fn protocol_profile_node(
    node: &novarocks_execution::runtime::profile::ProfileNode,
) -> wire::ProfileNode {
    wire::ProfileNode {
        name: node.name.clone(),
        node_id: node.node_id,
        counters: node
            .counters
            .iter()
            .map(|counter| wire::Counter {
                name: counter.name.clone(),
                parent_name: counter.parent_name.clone(),
                unit: match counter.unit {
                    novarocks_execution::runtime::profile::ProfileUnit::Unit => {
                        wire::ProfileUnit::Unit as i32
                    }
                    novarocks_execution::runtime::profile::ProfileUnit::CpuTicks => {
                        wire::ProfileUnit::CpuTicks as i32
                    }
                    novarocks_execution::runtime::profile::ProfileUnit::Bytes => {
                        wire::ProfileUnit::Bytes as i32
                    }
                    novarocks_execution::runtime::profile::ProfileUnit::TimeNs => {
                        wire::ProfileUnit::TimeNs as i32
                    }
                    novarocks_execution::runtime::profile::ProfileUnit::TimeMs => {
                        wire::ProfileUnit::TimeMs as i32
                    }
                    novarocks_execution::runtime::profile::ProfileUnit::TimeS => {
                        wire::ProfileUnit::TimeS as i32
                    }
                    novarocks_execution::runtime::profile::ProfileUnit::None => {
                        wire::ProfileUnit::None as i32
                    }
                },
                value: counter.value,
                min_value: counter.min_value,
                max_value: counter.max_value,
            })
            .collect(),
        info_strings: node.info_strings.clone().into_iter().collect(),
        children: node.children.iter().map(protocol_profile_node).collect(),
    }
}

fn protocol_connector_staged_report_frame(
    frame: &novarocks_spi::connector::ConnectorStagedReportFrame,
) -> wire::ConnectorStagedReportFrame {
    use novarocks_spi::connector::ConnectorWriterTerminalState;

    let writer = frame.writer();
    let fragment_instance_id = writer.fragment_instance_id();
    wire::ConnectorStagedReportFrame {
        contract_version: frame.version(),
        writer: Some(novarocks_protocol::plan::ConnectorWriterIdentity {
            operation_id: writer.operation_id().to_bytes().to_vec(),
            cohort_id: writer.cohort_id().to_bytes().to_vec(),
            execution_query_id: writer.execution_id().query_id().to_vec(),
            execution_attempt_id: writer.execution_id().attempt_id(),
            fragment_instance_id: Some(wire_common::UniqueId {
                hi: i64::from_be_bytes(
                    fragment_instance_id[..8]
                        .try_into()
                        .expect("fixed UUID prefix"),
                ),
                lo: i64::from_be_bytes(
                    fragment_instance_id[8..]
                        .try_into()
                        .expect("fixed UUID suffix"),
                ),
            }),
            fragment_id: writer.fragment_id(),
            backend_num: writer.backend_num(),
            sink_ordinal: writer.sink_ordinal(),
            connector_instance_id: writer.binding_key().instance_id.as_str().to_string(),
            connector_incarnation: writer.binding_key().incarnation.to_bytes().to_vec(),
        }),
        terminal_state: match frame.state() {
            ConnectorWriterTerminalState::Staged => 0,
            ConnectorWriterTerminalState::Aborted => 1,
            ConnectorWriterTerminalState::Failed => 2,
        },
        input_rows: frame.summary().input_rows,
        staged_bytes: frame.summary().staged_bytes,
        artifact_count: frame.summary().artifact_count,
        part_index: frame.part_index(),
        part_count: frame.part_count(),
        logical_payload_len: frame.logical_payload_len(),
        logical_payload_sha256: frame.logical_payload_digest().to_vec(),
        frame_payload: frame.frame_payload().to_vec(),
        frame_payload_sha256: frame.frame_payload_digest().to_vec(),
    }
}
