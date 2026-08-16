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

use novarocks::query_execution::lifecycle::{
    AttemptId as LegacyAttemptId, ExchangeRouteManifest as LegacyExchangeRouteManifest,
    ParticipantBackendIdentity as LegacyParticipantBackendIdentity,
    ParticipantManifest as LegacyParticipantManifest,
    ParticipantManifestDigest as LegacyParticipantManifestDigest,
    ParticipantQueryOptions as LegacyParticipantQueryOptions,
    ParticipantRole as LegacyParticipantRole,
    ParticipantTerminalOutcome as LegacyParticipantTerminalOutcome,
    QueryAbortRequest as LegacyQueryAbortRequest, QueryControlAttach as LegacyQueryControlAttach,
    QueryControlCommand as LegacyQueryControlCommand,
    QueryControlEndpoint as LegacyQueryControlEndpoint, QueryExecutionId as LegacyQueryExecutionId,
    QueryInitAck as LegacyQueryInitAck, QueryInitRequest as LegacyQueryInitRequest,
    QueryLifecycleError, QueryLifecycleErrorCode, QueryStageAck as LegacyQueryStageAck,
    QueryStageRequest as LegacyQueryStageRequest, QueryStartAck as LegacyQueryStartAck,
    QueryStartRequest as LegacyQueryStartRequest, QueryTerminalAck as LegacyQueryTerminalAck,
    QueryTerminationAck as LegacyQueryTerminationAck,
    QueryTerminationReason as LegacyQueryTerminationReason,
    RuntimeFilterContribution as LegacyRuntimeFilterContribution, StageDigest as LegacyStageDigest,
    StageDigestVersion as LegacyStageDigestVersion, StageFragment as LegacyStageFragment,
};
use novarocks_execution::exec::spill::{SpillConfig, SpillMode};
use novarocks_execution::runtime::query_options::{QueryCacheOptions, QueryOptions};
use novarocks_protocol::lifecycle::{
    ParticipantTerminalOutcome, QueryAbortRequest, QueryControlAttach, QueryInitAck,
    QueryInitRequest, QueryStageAck, QueryStageRequest, QueryStartAck, QueryStartRequest,
    QueryTerminationAck,
};
use novarocks_protocol::{common as wire_common, novarocks as wire};
use novarocks_types::{QueryId, UniqueId};

pub(crate) fn legacy_init_request(
    request: QueryInitRequest,
) -> Result<LegacyQueryInitRequest, QueryLifecycleError> {
    let manifest = legacy_participant_manifest(request.manifest().map_err(legacy_contract_error)?)?;
    let digest = request
        .digest()
        .expect("validated Protocol InitQuery request has a fixed-width digest");
    Ok(LegacyQueryInitRequest::from_validated_protocol_manifest(
        manifest,
        LegacyParticipantManifestDigest::new(*digest.as_bytes()),
    ))
}

pub(crate) fn protocol_init_ack(value: &LegacyQueryInitAck) -> QueryInitAck {
    QueryInitAck::parse(wire::InitQueryResponse {
        execution_id: Some(protocol_execution_id(value.execution_id()).to_proto()),
        init_digest: value.digest().as_bytes().to_vec(),
        outcome: match value.outcome() {
            novarocks::query_execution::lifecycle::QueryInitOutcome::Applied => {
                wire::QueryInitOutcome::QueryInitApplied as i32
            }
            novarocks::query_execution::lifecycle::QueryInitOutcome::AlreadyApplied => {
                wire::QueryInitOutcome::QueryInitAlreadyApplied as i32
            }
            novarocks::query_execution::lifecycle::QueryInitOutcome::RejectedConflict => {
                wire::QueryInitOutcome::QueryInitRejectedConflict as i32
            }
            novarocks::query_execution::lifecycle::QueryInitOutcome::RejectedStaleBackend => {
                wire::QueryInitOutcome::QueryInitRejectedStaleBackend as i32
            }
            novarocks::query_execution::lifecycle::QueryInitOutcome::RejectedCapacity => {
                wire::QueryInitOutcome::QueryInitRejectedCapacity as i32
            }
            novarocks::query_execution::lifecycle::QueryInitOutcome::RejectedInvalidManifest => {
                wire::QueryInitOutcome::QueryInitRejectedInvalidManifest as i32
            }
            novarocks::query_execution::lifecycle::QueryInitOutcome::RejectedTerminated => {
                wire::QueryInitOutcome::QueryInitRejectedTerminated as i32
            }
        },
    })
    .expect("legacy InitQuery acknowledgement remains a valid Protocol response")
}

pub(crate) fn legacy_stage_request(
    request: QueryStageRequest,
) -> Result<LegacyQueryStageRequest, QueryLifecycleError> {
    let fragments = request
        .fragments()
        .into_iter()
        .map(|fragment| {
            LegacyStageFragment::new(fragment.plan().clone(), fragment.instance_params().clone())
        })
        .collect::<Result<Vec<_>, _>>()?;
    LegacyQueryStageRequest::new(
        legacy_execution_id(request.execution_id())?,
        LegacyParticipantManifestDigest::new(*request.init_digest().as_bytes()),
        LegacyStageDigestVersion::try_from_wire(request.digest_version().get())?,
        LegacyStageDigest::new(*request.digest().as_bytes()),
        fragments,
    )
}

pub(crate) fn protocol_stage_ack(value: &LegacyQueryStageAck) -> QueryStageAck {
    QueryStageAck::parse(wire::StageFragmentsResponse {
        execution_id: Some(protocol_execution_id(value.execution_id()).to_proto()),
        stage_digest_version: value.digest_version().get(),
        stage_digest: value.digest().as_bytes().to_vec(),
        outcome: match value.outcome() {
            novarocks::query_execution::lifecycle::QueryStageOutcome::Applied => {
                wire::StageFragmentsOutcome::StageFragmentsApplied as i32
            }
            novarocks::query_execution::lifecycle::QueryStageOutcome::AlreadyApplied => {
                wire::StageFragmentsOutcome::StageFragmentsAlreadyApplied as i32
            }
            novarocks::query_execution::lifecycle::QueryStageOutcome::RejectedConflict => {
                wire::StageFragmentsOutcome::StageFragmentsRejectedConflict as i32
            }
            novarocks::query_execution::lifecycle::QueryStageOutcome::RejectedInvalidState => {
                wire::StageFragmentsOutcome::StageFragmentsRejectedInvalidState as i32
            }
            novarocks::query_execution::lifecycle::QueryStageOutcome::RejectedInvalidBatch => {
                wire::StageFragmentsOutcome::StageFragmentsRejectedInvalidBatch as i32
            }
            novarocks::query_execution::lifecycle::QueryStageOutcome::RejectedCapacity => {
                wire::StageFragmentsOutcome::StageFragmentsRejectedCapacity as i32
            }
            novarocks::query_execution::lifecycle::QueryStageOutcome::RejectedTerminated => {
                wire::StageFragmentsOutcome::StageFragmentsRejectedTerminated as i32
            }
            novarocks::query_execution::lifecycle::QueryStageOutcome::RejectedLocalFailure => {
                wire::StageFragmentsOutcome::StageFragmentsRejectedLocalFailure as i32
            }
        },
        detail: value.detail().to_owned(),
    })
    .expect("legacy StageFragments acknowledgement remains a valid Protocol response")
}

pub(crate) fn legacy_start_request(
    request: QueryStartRequest,
) -> Result<LegacyQueryStartRequest, QueryLifecycleError> {
    Ok(LegacyQueryStartRequest::new(
        legacy_execution_id(request.execution_id())?,
        LegacyStageDigestVersion::try_from_wire(request.digest_version().get())?,
        LegacyStageDigest::new(*request.digest().as_bytes()),
    ))
}

pub(crate) fn protocol_start_ack(value: &LegacyQueryStartAck) -> QueryStartAck {
    QueryStartAck::parse(wire::StartPreparedQueryResponse {
        execution_id: Some(protocol_execution_id(value.execution_id()).to_proto()),
        stage_digest_version: value.digest_version().get(),
        stage_digest: value.digest().as_bytes().to_vec(),
        outcome: match value.outcome() {
            novarocks::query_execution::lifecycle::QueryStartOutcome::Applied => {
                wire::StartPreparedQueryOutcome::StartPreparedQueryApplied as i32
            }
            novarocks::query_execution::lifecycle::QueryStartOutcome::AlreadyStarted => {
                wire::StartPreparedQueryOutcome::StartPreparedQueryAlreadyStarted as i32
            }
            novarocks::query_execution::lifecycle::QueryStartOutcome::RejectedNotStaged => {
                wire::StartPreparedQueryOutcome::StartPreparedQueryRejectedNotStaged as i32
            }
            novarocks::query_execution::lifecycle::QueryStartOutcome::RejectedConflict => {
                wire::StartPreparedQueryOutcome::StartPreparedQueryRejectedConflict as i32
            }
            novarocks::query_execution::lifecycle::QueryStartOutcome::RejectedTerminated => {
                wire::StartPreparedQueryOutcome::StartPreparedQueryRejectedTerminated as i32
            }
        },
        detail: value.detail().to_owned(),
    })
    .expect("legacy StartPreparedQuery acknowledgement remains a valid Protocol response")
}

pub(crate) fn legacy_abort_request(
    request: QueryAbortRequest,
) -> Result<LegacyQueryAbortRequest, QueryLifecycleError> {
    LegacyQueryAbortRequest::new(
        legacy_execution_id(request.execution_id().map_err(legacy_contract_error)?)?,
        LegacyParticipantManifestDigest::new(
            *request.digest().map_err(legacy_contract_error)?.as_bytes(),
        ),
        request.reason(),
    )
}

pub(crate) fn protocol_termination_ack(value: &LegacyQueryTerminationAck) -> QueryTerminationAck {
    QueryTerminationAck::parse(wire::AbortQueryResponse {
        execution_id: Some(protocol_execution_id(value.execution_id()).to_proto()),
        accepted_reason: protocol_termination_reason(value.accepted_reason()) as i32,
    })
    .expect("legacy AbortQuery acknowledgement remains a valid Protocol response")
}

pub(crate) fn legacy_control_attach(
    attach: QueryControlAttach,
) -> Result<LegacyQueryControlAttach, QueryLifecycleError> {
    LegacyQueryControlAttach::new(
        legacy_execution_id(attach.execution_id().map_err(legacy_contract_error)?)?,
        LegacyParticipantManifestDigest::new(
            *attach.digest().map_err(legacy_contract_error)?.as_bytes(),
        ),
        attach.frontend_owner_epoch(),
    )
}

pub(crate) fn legacy_control_command(
    command: novarocks_protocol::lifecycle::QueryControlCommand,
) -> Result<LegacyQueryControlCommand, QueryLifecycleError> {
    use wire::query_control_request::Command;

    match command.as_proto().command.as_ref() {
        Some(Command::Heartbeat(heartbeat)) => Ok(LegacyQueryControlCommand::Heartbeat {
            sequence: heartbeat.sequence,
            sent_mono_ns: heartbeat.sent_mono_ns,
        }),
        Some(Command::Abort(abort)) => Ok(LegacyQueryControlCommand::Abort {
            reason: abort.reason.clone(),
        }),
        Some(Command::Finalize(_)) => Ok(LegacyQueryControlCommand::Finalize),
        Some(Command::TerminalAck(ack)) => Ok(LegacyQueryControlCommand::TerminalAck {
            ack: LegacyQueryTerminalAck::new(
                legacy_execution_id(
                    novarocks_protocol::lifecycle::QueryExecutionId::try_from_proto(
                        ack.execution_id
                            .as_ref()
                            .ok_or_else(|| invalid_manifest("query execution id is required"))?,
                    )
                    .map_err(legacy_contract_error)?,
                )?,
                LegacyParticipantManifestDigest::try_from_slice(&ack.init_digest)?,
                ack.snapshot_version,
                novarocks::query_execution::lifecycle::QueryTerminalSnapshotDigest::try_from_slice(
                    &ack.snapshot_digest,
                )?,
            ),
        }),
        Some(Command::Attach(_)) | None => Err(invalid_manifest(
            "validated Protocol control command must not contain attach or be empty",
        )),
    }
}

fn legacy_contract_error(
    error: novarocks_protocol::lifecycle::ContractError,
) -> QueryLifecycleError {
    QueryLifecycleError::new(QueryLifecycleErrorCode::InvalidManifest, error.to_string())
}

fn invalid_manifest(detail: impl Into<String>) -> QueryLifecycleError {
    QueryLifecycleError::new(QueryLifecycleErrorCode::InvalidManifest, detail)
}

fn protocol_termination_reason(
    reason: LegacyQueryTerminationReason,
) -> wire::QueryTerminationReason {
    match reason {
        LegacyQueryTerminationReason::CoordinatorAbort => {
            wire::QueryTerminationReason::QueryTerminationCoordinatorAbort
        }
        LegacyQueryTerminationReason::CoordinatorFinalize => {
            wire::QueryTerminationReason::QueryTerminationCoordinatorFinalize
        }
        LegacyQueryTerminationReason::CoordinatorStreamLost => {
            wire::QueryTerminationReason::QueryTerminationCoordinatorStreamLost
        }
        LegacyQueryTerminationReason::CoordinatorHeartbeatTimeout => {
            wire::QueryTerminationReason::QueryTerminationCoordinatorHeartbeatTimeout
        }
        LegacyQueryTerminationReason::LocalFailure => {
            wire::QueryTerminationReason::QueryTerminationLocalFailure
        }
        LegacyQueryTerminationReason::PreStartTimeout => {
            wire::QueryTerminationReason::QueryTerminationPreStartTimeout
        }
    }
}

fn legacy_participant_manifest(
    manifest: novarocks_protocol::lifecycle::ParticipantManifest,
) -> Result<LegacyParticipantManifest, QueryLifecycleError> {
    let execution_id =
        legacy_execution_id(manifest.execution_id().map_err(legacy_contract_error)?)?;
    let backend = manifest.backend().map_err(legacy_contract_error)?;
    let endpoint = backend.endpoint().map_err(legacy_contract_error)?;
    let backend = LegacyParticipantBackendIdentity::new(
        backend.backend_id(),
        LegacyQueryControlEndpoint::new(endpoint.host(), endpoint.port())?,
        backend.start_epoch(),
    )?;
    let roles = manifest
        .roles()
        .map_err(legacy_contract_error)?
        .into_iter()
        .map(|role| match role {
            wire::QueryParticipantRole::FragmentExecutor => {
                Ok(LegacyParticipantRole::FragmentExecutor)
            }
            wire::QueryParticipantRole::RuntimeFilterService => {
                Ok(LegacyParticipantRole::RuntimeFilterService)
            }
            wire::QueryParticipantRole::Unspecified => {
                Err(invalid_manifest("participant role must not be unspecified"))
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    let fragments = manifest
        .expected_fragment_instance_ids()
        .into_iter()
        .map(|id| UniqueId::new(id.hi, id.lo))
        .collect::<Vec<_>>();
    let options = legacy_query_options(
        manifest
            .query_options()
            .map_err(legacy_contract_error)?
            .as_proto(),
    )?;
    let routes = manifest
        .exchange_routes()
        .map_err(legacy_contract_error)?
        .into_iter()
        .map(|route| {
            let source = route
                .source_fragment_instance_id()
                .map_err(legacy_contract_error)?;
            let destination = route
                .destination_fragment_instance_id()
                .map_err(legacy_contract_error)?;
            LegacyExchangeRouteManifest::new(
                UniqueId::new(source.hi, source.lo),
                UniqueId::new(destination.hi, destination.lo),
                route.destination_node_id(),
                route.sender_ordinal(),
                route.sender_count(),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let runtime_filter = manifest
        .runtime_filter()
        .map_err(legacy_contract_error)?
        .map(|value| LegacyRuntimeFilterContribution::from_wire(value.as_proto().clone()))
        .transpose()?;
    let report = manifest.report_endpoint().map_err(legacy_contract_error)?;
    LegacyParticipantManifest::new(
        execution_id,
        backend,
        roles,
        fragments,
        LegacyParticipantQueryOptions::new(options),
        manifest.query_deadline_unix_ms(),
        routes,
        runtime_filter,
        std::time::Duration::from_millis(manifest.pre_start_timeout_ms()),
        LegacyQueryControlEndpoint::new(report.host(), report.port())?,
    )
}

fn legacy_query_options(raw: &wire::QueryOptions) -> Result<QueryOptions, QueryLifecycleError> {
    let spill = if raw.enable_spill {
        let spill = raw
            .spill_options
            .as_ref()
            .ok_or_else(|| invalid_manifest("enable_spill=true requires spill_options"))?;
        let spill_mode = match spill.spill_mode {
            0 => SpillMode::Auto,
            1 => SpillMode::Force,
            2 => SpillMode::None,
            3 => return Err(invalid_manifest("spill_mode RANDOM is not supported yet")),
            value => {
                return Err(invalid_manifest(format!(
                    "unknown spill_mode value {value}"
                )));
            }
        };
        if !spill.spill_mem_limit_threshold.is_finite() {
            return Err(invalid_manifest("spill_mem_limit_threshold must be finite"));
        }
        Some(SpillConfig {
            enable_spill: true,
            spill_mode,
            spill_mem_limit_threshold: (spill.spill_mem_limit_threshold > 0.0)
                .then_some(spill.spill_mem_limit_threshold),
            spill_operator_min_bytes: (spill.spill_operator_min_bytes > 0)
                .then_some(spill.spill_operator_min_bytes),
            spill_operator_max_bytes: (spill.spill_operator_max_bytes > 0)
                .then_some(spill.spill_operator_max_bytes),
            spill_encode_level: (spill.spill_encode_level > 0).then_some(spill.spill_encode_level),
            enable_spill_buffer_read: Some(spill.enable_spill_buffer_read),
            max_spill_read_buffer_bytes_per_driver: (spill.max_spill_read_buffer_bytes_per_driver
                > 0)
            .then_some(spill.max_spill_read_buffer_bytes_per_driver),
            spill_mem_table_size: (spill.spill_mem_table_size > 0)
                .then_some(spill.spill_mem_table_size),
            spill_mem_table_num: (spill.spill_mem_table_num > 0)
                .then_some(spill.spill_mem_table_num),
        })
    } else {
        None
    };
    Ok(QueryOptions {
        batch_size: (raw.batch_size > 0).then_some(raw.batch_size),
        query_timeout: (raw.query_timeout > 0).then_some(raw.query_timeout),
        query_delivery_timeout: (raw.query_delivery_timeout > 0)
            .then_some(raw.query_delivery_timeout),
        enable_profile: raw.enable_profile,
        runtime_profile_report_interval: (raw.runtime_profile_report_interval > 0)
            .then_some(raw.runtime_profile_report_interval),
        pipeline_dop: (raw.pipeline_dop > 0).then_some(raw.pipeline_dop),
        exec_mem_limit: (raw.query_mem_limit > 0).then_some(raw.query_mem_limit),
        connector_io_tasks_per_scan_operator: (raw.connector_io_tasks_per_scan_operator > 0)
            .then_some(raw.connector_io_tasks_per_scan_operator),
        orc_use_column_names: raw.orc_use_column_names,
        enable_file_metacache: raw.enable_file_metacache,
        enable_file_pagecache: raw.enable_file_pagecache,
        enable_parquet_reader_page_index: raw.enable_parquet_reader_page_index,
        runtime_filter_scan_wait_time_ms: raw.runtime_filter_scan_wait_time_ms,
        runtime_filter_wait_timeout_ms: raw.runtime_filter_wait_timeout_ms,
        allow_throw_exception: raw.allow_throw_exception,
        group_concat_max_len: raw.group_concat_max_len,
        enable_join_runtime_bitset_filter: raw.enable_join_runtime_bitset_filter,
        global_runtime_filter_build_max_size: (raw.global_runtime_filter_build_max_size > 0)
            .then_some(raw.global_runtime_filter_build_max_size),
        cache: QueryCacheOptions {
            enable_scan_datacache: raw.enable_scan_datacache,
            enable_populate_datacache: raw.enable_populate_datacache,
            enable_datacache_async_populate_mode: raw.enable_datacache_async_populate_mode,
            enable_datacache_io_adaptor: raw.enable_datacache_io_adaptor,
            enable_cache_select: raw.enable_cache_select,
            datacache_evict_probability: raw.datacache_evict_probability,
            datacache_priority: (raw.datacache_priority != 0).then_some(raw.datacache_priority),
            datacache_ttl_seconds: (raw.datacache_ttl_seconds > 0)
                .then_some(raw.datacache_ttl_seconds),
            datacache_sharing_work_period: (raw.datacache_sharing_work_period > 0)
                .then_some(raw.datacache_sharing_work_period),
        },
        spill,
    })
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
    let channels = contribution.channels().iter().map(|value| wire::QueryTerminalRuntimeFilterChannelV1 {
        channel_binding_id: value.key().channel_binding_id(), channel_id: value.key().channel_id(), install_state: 1,
        terminal_state: match value.terminal_state() {
            novarocks::query_execution::lifecycle::QueryTerminalRuntimeFilterChannelTerminalStateV1::Open => 1,
            novarocks::query_execution::lifecycle::QueryTerminalRuntimeFilterChannelTerminalStateV1::Completed => 2,
            novarocks::query_execution::lifecycle::QueryTerminalRuntimeFilterChannelTerminalStateV1::Unavailable => 3,
            novarocks::query_execution::lifecycle::QueryTerminalRuntimeFilterChannelTerminalStateV1::Cancelled => 4,
        }, latest_published_logical_version: value.latest_published_logical_version(), published_count: value.published_count(), completed_count: value.completed_count(), unavailable_count: value.unavailable_count(), cancelled_count: value.cancelled_count(),
    }).collect();
    let producer_streams = contribution
        .producer_streams()
        .iter()
        .map(|value| {
            let fragment = value.key().producer_fragment_instance_id();
            wire::QueryTerminalRuntimeFilterProducerStreamV1 {
                channel_binding_id: value.key().channel().channel_binding_id(),
                channel_id: value.key().channel().channel_id(),
                producer_fragment_instance_id: Some(protocol_unique_id(fragment)),
                partition_id: value.key().partition_id(),
                latest_accepted_sequence: value.latest_accepted_sequence(),
                accepted_count: value.accepted_count(),
                duplicate_count: value.duplicate_count(),
                stale_count: value.stale_count(),
                conflict_count: value.conflict_count(),
                resource_limit_count: value.resource_limit_count(),
            }
        })
        .collect();
    let transport_routes = contribution
        .transport_routes()
        .iter()
        .map(|value| wire::QueryTerminalRuntimeFilterTransportRouteV1 {
            channel_binding_id: value.key().channel().channel_binding_id(),
            channel_id: value.key().channel().channel_id(),
            route_edge_id: value.key().route_edge_id(),
            sent_count: value.sent_count(),
            sent_bytes: value.sent_bytes(),
            retried_count: value.retried_count(),
            retried_bytes: value.retried_bytes(),
            acked_count: value.acked_count(),
            acked_bytes: value.acked_bytes(),
            fail_open_count: value.fail_open_count(),
            fail_open_bytes: value.fail_open_bytes(),
        })
        .collect();
    let consumers = contribution.consumers().iter().map(|value| { let fragment = value.key().fragment_instance_id(); let reasons = value.scan_not_evaluated_reasons(); wire::QueryTerminalRuntimeFilterConsumerV1 {
        channel_binding_id: value.key().channel().channel_binding_id(), channel_id: value.key().channel().channel_id(), consumer_binding_id: value.key().consumer_binding_id(), fragment_instance_id: Some(protocol_unique_id(fragment)), latest_delivered_logical_version: value.latest_delivered_logical_version(), latest_applied_logical_version: value.latest_applied_logical_version(), subscription_terminal: match value.subscription_terminal() {
            novarocks::query_execution::lifecycle::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Pending => 1,
            novarocks::query_execution::lifecycle::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Acquired => 2,
            novarocks::query_execution::lifecycle::QueryTerminalRuntimeFilterSubscriptionTerminalV1::TimedOut => 3,
            novarocks::query_execution::lifecycle::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Unavailable => 4,
            novarocks::query_execution::lifecycle::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Unsupported => 5,
            novarocks::query_execution::lifecycle::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Cancelled => 6,
            novarocks::query_execution::lifecycle::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Completed => 7,
            novarocks::query_execution::lifecycle::QueryTerminalRuntimeFilterSubscriptionTerminalV1::CompletedWithoutArtifact => 8,
        }, row_evaluations: value.row_evaluations(), input_rows: value.input_rows(), output_rows: value.output_rows(), scan_evaluated: value.scan_evaluated(), scan_kept: value.scan_kept(), scan_pruned: value.scan_pruned(), scan_not_evaluated: value.scan_not_evaluated(), scan_not_evaluated_reasons: Some(wire::QueryTerminalRuntimeFilterScanNotEvaluatedV1 {
            unit_facts_missing: reasons.unit_facts_missing(), column_facts_missing: reasons.column_facts_missing(), data_type_unsupported: reasons.data_type_unsupported(), predicate_capability_unsupported: reasons.predicate_capability_unsupported(), resource_unavailable: reasons.resource_unavailable(), snapshot_unavailable: reasons.snapshot_unavailable(), snapshot_timed_out: reasons.snapshot_timed_out(), snapshot_not_published: reasons.snapshot_not_published(),
        }),
    }}).collect();
    wire::QueryTerminalProfileContributionV1 {
        version: contribution.version(),
        channels,
        producer_streams,
        transport_routes,
        consumers,
    }
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
