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

//! The frontend-only Iceberg write session control.
//!
//! `begin_write` completes admission and freezes the recipe with no external
//! side effect. `finish_write` validates a *complete* prepared write set and
//! performs exactly one external snapshot commit. `abort_write` is the
//! known-uncommitted release, and `reconcile_write` resolves an unknown
//! outcome by read-only adjudication against a durable snapshot marker.
//!
//! The commit verdicts are the provider's existing ones and are deliberately
//! not softened here:
//!
//! * a definite catalog refusal is `KnownUncommitted` and authorizes cleanup;
//! * a proven publication is `KnownCommitted`;
//! * a finalization failure after a proven publication stays `KnownCommitted`
//!   with a failed finalization, never a retryable commit;
//! * anything else — a timeout, a reset connection, a runtime-bridge failure —
//!   is `CommitUnknown`, keeps every staged object in place, and returns
//!   recovery evidence. Marker absence during reconciliation is *not* proof of
//!   non-commit, so an unresolved session stays unknown.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bytes::Bytes;
use novarocks_spi::connector::write_stack::session::{
    ConnectorWriteBeginRequest, ConnectorWriteFinishRequest, ConnectorWriteSessionAbortRequest,
    ConnectorWriteSessionPlan, ConnectorWriteSessionReconcileRequest, ConnectorWriteTargetPlan,
};
use novarocks_spi::connector::write_stack::{ConnectorPreparedWriteSet, WriteTargetOrdinal};
use novarocks_spi::connector::{
    CatalogHandle, ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor,
    ConnectorMutationFailure, ConnectorMutationFailureKind, ConnectorMutationOperationId,
    ConnectorProviderBindingKey, ConnectorRequestContext, ConnectorWriteAbortOutcome,
    ConnectorWriteFieldBinding, ConnectorWriteFieldRequest, ConnectorWriteFieldToken,
    ConnectorWriteInputRequest, ConnectorWriteInputShape, ConnectorWriteReceipt,
    ExternalMutationEffect, ExternalMutationEvidence, ExternalMutationFinalization,
    ExternalMutationOutcome, ProviderBindingEpoch,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::commit::write_stack::domain::{
    IcebergArtifactPartition, IcebergCommitArtifact, IcebergCommitFragment, IcebergCommitHandle,
    IcebergContentRange, IcebergDataBranchRecipe, IcebergWriteBranch, IcebergWriteFlavor,
    IcebergWriteSessionId, IcebergWriteSessionState, IcebergWriteTableFacts, IcebergWriterOutput,
    corrupt, invalid,
};
use crate::commit::write_stack::old_delete::{
    IcebergOldDeleteArtifactRef, IcebergOldDeleteMergeTarget, IcebergStorageRoute,
};
use crate::commit::write_stack::planning::{
    IcebergDataBranchPlan, IcebergDeleteBranchPlan, IcebergWriteSessionPlanInput,
};
use crate::commit::write_stack::runtime::IcebergWriteAdapter;
use crate::commit::{
    CommitServiceError, IcebergCommitCollector, RunInput, WrittenFile, run_iceberg_commit,
};
use crate::iceberg::spec::{DataContentType, DataFileFormat, TableMetadata};
use crate::metadata_context::IcebergMetadataContext;
use crate::write_descriptor::decode_partition_descriptor;

/// The snapshot summary property that makes a session's publication provable
/// after an unknown outcome. Without a durable marker, reconciliation could
/// only guess, and a guess would either double-commit or strand data.
pub const ICEBERG_WRITE_SESSION_MARKER_PROPERTY: &str = "novarocks.write.session.v1";

/// The evidence schema version this control produces and accepts.
pub const ICEBERG_WRITE_SESSION_EVIDENCE_VERSION: u16 = 1;

/// The operation-kind tag on the evidence envelope.
pub const ICEBERG_WRITE_SESSION_OPERATION_KIND: &str = "iceberg.connector_write_session.v1";

fn unavailable(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message.into())
        .with_retryable_before_progress()
}

fn internal(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, message.into())
}

fn failure(
    kind: ConnectorMutationFailureKind,
    message: impl Into<String>,
) -> ConnectorMutationFailure {
    ConnectorMutationFailure::new(kind, message.into())
}

/// The canonical provider payload inside a reconciliation evidence envelope.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergWriteSessionEvidenceV1 {
    version: u16,
    session_id: String,
    table_ident: String,
    target_ref: String,
    op_kind: String,
    base_snapshot_id: Option<i64>,
    base_sequence_number: i64,
    staging_dir: String,
    manifest_cleanup_token: Option<String>,
}

/// The frontend-only Iceberg write authority of one exact catalog generation.
pub struct IcebergWriteSessionControl {
    key: ConnectorProviderBindingKey,
    descriptor: ConnectorInstanceDescriptor,
    adapter: IcebergWriteAdapter,
    runtime: Arc<IcebergMetadataContext>,
}

impl IcebergWriteSessionControl {
    pub fn new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        catalog_handle: CatalogHandle,
        runtime: Arc<IcebergMetadataContext>,
    ) -> Self {
        let key = ConnectorProviderBindingKey {
            instance_id: descriptor.instance_id.clone(),
            incarnation,
        };
        let adapter = crate::commit::write_stack::runtime::build_write_adapter(
            descriptor.clone(),
            catalog_handle,
        );
        Self {
            key,
            descriptor,
            adapter,
            runtime,
        }
    }

    pub const fn binding_key(&self) -> &ConnectorProviderBindingKey {
        &self.key
    }

    pub const fn runtime(&self) -> &Arc<IcebergMetadataContext> {
        &self.runtime
    }
}

/// Reject a request whose attempt is already cancelled or past its deadline,
/// before any external work starts.
pub(crate) fn validate_context(context: &ConnectorRequestContext) -> Result<(), ConnectorError> {
    if context.cancellation().is_cancelled() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Cancelled,
            "Iceberg write request was cancelled",
        ));
    }
    if std::time::Instant::now() >= context.deadline() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "Iceberg write request deadline elapsed",
        ));
    }
    Ok(())
}

/// One typed fragment resolved against the sealed target set.
#[derive(Debug)]
pub(crate) struct ValidatedFragment<'a> {
    ordinal: WriteTargetOrdinal,
    fragment: &'a IcebergCommitFragment,
}

/// Validate a complete prepared write set against a sealed session.
///
/// Every check here is a fail-closed precondition of the single external
/// commit: a fragment that names an unsealed target, a branch its target does
/// not drive, a duplicate path, a partition spec the table does not have, or a
/// delete artifact whose data file is owned by another target would each make
/// the resulting snapshot wrong in a way no later step could detect.
pub(crate) fn validate_prepared_set<'a>(
    handle: &IcebergCommitHandle,
    adapter: &IcebergWriteAdapter,
    prepared: &'a ConnectorPreparedWriteSet,
) -> Result<Vec<ValidatedFragment<'a>>, ConnectorError> {
    let sealed = handle
        .targets()
        .iter()
        .map(|target| (target.ordinal(), target.branch()))
        .collect::<BTreeMap<_, _>>();
    let mut paths = BTreeSet::new();
    let mut delete_artifact_owner: BTreeMap<&str, WriteTargetOrdinal> = BTreeMap::new();
    let mut validated = Vec::with_capacity(prepared.fragments().len());

    for (ordinal, neutral) in prepared.fragments() {
        let fragment = adapter.commit_fragment(neutral)?;
        let branch = sealed.get(ordinal).copied().ok_or_else(|| {
            invalid(format!(
                "Iceberg commit fragment names write target {} which this session never sealed",
                ordinal.get()
            ))
        })?;
        if fragment.branch() != branch {
            return Err(invalid(format!(
                "Iceberg commit fragment describes a {} artifact but write target {} drives the {} branch",
                fragment.branch().as_str(),
                ordinal.get(),
                branch.as_str()
            )));
        }
        if !paths.insert(fragment.path()) {
            return Err(corrupt(format!(
                "Iceberg prepared write set repeats staged artifact {}",
                fragment.path()
            )));
        }
        if let Some(referenced) = fragment.referenced_data_file() {
            // Exactly one delete artifact per referenced data file: Iceberg
            // permits one deletion vector per data file, and two merged
            // position-delete files would each claim to be the whole truth.
            if delete_artifact_owner.insert(referenced, *ordinal).is_some() {
                return Err(corrupt(format!(
                    "Iceberg prepared write set stages more than one delete artifact for data file {referenced}"
                )));
            }
            let owner = handle.delete_owner().get(referenced).ok_or_else(|| {
                invalid(format!(
                    "Iceberg delete artifact references data file {referenced}, which this session never routed"
                ))
            })?;
            if owner != ordinal {
                return Err(invalid(format!(
                    "Iceberg delete artifact for data file {referenced} was staged by write target {} but the session routed it to {}",
                    ordinal.get(),
                    owner.get()
                )));
            }
        }
        if fragment.metrics().record_count() == 0
            && matches!(fragment.artifact(), IcebergCommitArtifact::DataFile(_))
        {
            return Err(invalid(format!(
                "Iceberg staged data file {} reports zero rows",
                fragment.path()
            )));
        }
        validated.push(ValidatedFragment {
            ordinal: *ordinal,
            fragment,
        });
    }
    Ok(validated)
}

/// Verify that every delete artifact superseded exactly the old references the
/// session froze for its data file.
///
/// This is the commit-side half of the D10 contract: the backend proved it read
/// the frozen references, and the frontend proves the artifact it is about to
/// commit accounts for all of them.
pub(crate) fn validate_merged_old_references(
    handle: &IcebergCommitHandle,
    plans: &BTreeMap<WriteTargetOrdinal, BTreeMap<String, Vec<String>>>,
    validated: &[ValidatedFragment<'_>],
) -> Result<(), ConnectorError> {
    for entry in validated {
        let Some(referenced) = entry.fragment.referenced_data_file() else {
            continue;
        };
        let frozen = plans
            .get(&entry.ordinal)
            .and_then(|files| files.get(referenced))
            .ok_or_else(|| {
                invalid(format!(
                    "Iceberg delete artifact references data file {referenced}, which write target {} never froze",
                    entry.ordinal.get()
                ))
            })?;
        let observed = entry.fragment.merged_old_references();
        if observed != frozen.as_slice() {
            return Err(corrupt(format!(
                "Iceberg delete artifact for {referenced} merged {} old references but the session froze {}",
                observed.len(),
                frozen.len()
            )));
        }
    }
    // A data file whose frozen references were never merged means its writer
    // never ran; committing the others would silently drop old deletes.
    let staged = validated
        .iter()
        .filter_map(|entry| entry.fragment.referenced_data_file())
        .collect::<BTreeSet<_>>();
    for (ordinal, files) in plans {
        for (data_file, references) in files {
            if !references.is_empty() && !staged.contains(data_file.as_str()) {
                return Err(corrupt(format!(
                    "Iceberg write target {} froze {} old delete references for data file {data_file} but staged no artifact that supersedes them",
                    ordinal.get(),
                    references.len()
                )));
            }
        }
    }
    let _ = handle;
    Ok(())
}

/// Turn one validated fragment into the provider's physical commit input.
fn written_file_from_fragment(
    fragment: &IcebergCommitFragment,
    metadata: &TableMetadata,
) -> Result<WrittenFile, ConnectorError> {
    let partition = fragment.partition();
    let partition_values = decode_partition_values(partition, metadata)?;
    let metrics = fragment.metrics();
    let record_count = metrics.record_count();
    let file_size_in_bytes = metrics.file_size_in_bytes();
    let stats = metrics.column_stats().cloned().unwrap_or_default();
    let (
        format,
        content,
        referenced_data_file,
        first_row_id,
        content_offset,
        content_size,
        cardinality,
    ) = match fragment.artifact() {
        IcebergCommitArtifact::DataFile(file) => (
            DataFileFormat::Parquet,
            DataContentType::Data,
            None,
            file.first_row_id(),
            None,
            None,
            None,
        ),
        IcebergCommitArtifact::PositionDeleteFile(file) => (
            DataFileFormat::Parquet,
            DataContentType::PositionDeletes,
            Some(file.referenced_data_file().to_string()),
            None,
            None,
            None,
            None,
        ),
        IcebergCommitArtifact::DeletionVector(file) => (
            DataFileFormat::Puffin,
            DataContentType::PositionDeletes,
            Some(file.referenced_data_file().to_string()),
            None,
            Some(file.content_range().offset()),
            Some(file.content_range().size_in_bytes()),
            Some(file.cardinality()),
        ),
    };
    Ok(WrittenFile {
        path: fragment.path().to_string(),
        format,
        content,
        partition_values,
        partition_spec_id: partition.partition_spec_id(),
        record_count,
        file_size_in_bytes,
        split_offsets: metrics.split_offsets().to_vec(),
        column_sizes: unsigned_counts(&stats.column_sizes)?,
        value_counts: unsigned_counts(&stats.value_counts)?,
        null_value_counts: unsigned_counts(&stats.null_value_counts)?,
        nan_value_counts: unsigned_counts(&stats.nan_value_counts)?,
        lower_bounds: decode_bounds(&stats.lower_bounds, metadata)?,
        upper_bounds: decode_bounds(&stats.upper_bounds, metadata)?,
        key_metadata: None,
        referenced_data_file,
        equality_ids: None,
        first_row_id,
        content_offset,
        content_size_in_bytes: content_size,
        cardinality,
    })
}

fn decode_partition_values(
    partition: &IcebergArtifactPartition,
    metadata: &TableMetadata,
) -> Result<crate::iceberg::spec::Struct, ConnectorError> {
    decode_partition_descriptor(
        Some(partition.descriptor().clone()),
        partition.partition_spec_id(),
        metadata,
    )
    .map_err(|error| corrupt(error.detail_message()))
}

fn unsigned_counts(
    counts: &BTreeMap<i32, i64>,
) -> Result<std::collections::HashMap<i32, u64>, ConnectorError> {
    counts
        .iter()
        .map(|(field, value)| {
            u64::try_from(*value)
                .map(|value| (*field, value))
                .map_err(|_| corrupt("Iceberg staged artifact column statistic is negative"))
        })
        .collect()
}

fn decode_bounds(
    bounds: &BTreeMap<i32, Vec<u8>>,
    metadata: &TableMetadata,
) -> Result<std::collections::HashMap<i32, crate::iceberg::spec::Datum>, ConnectorError> {
    let schema = metadata.current_schema();
    bounds
        .iter()
        .map(|(field_id, raw)| {
            let field = schema.field_by_id(*field_id).ok_or_else(|| {
                corrupt(format!(
                    "Iceberg staged artifact bound names unknown field id {field_id}"
                ))
            })?;
            let primitive = field.field_type.as_primitive_type().ok_or_else(|| {
                corrupt(format!(
                    "Iceberg staged artifact bound field id {field_id} is not a primitive"
                ))
            })?;
            let datum = crate::iceberg::spec::Datum::try_from_bytes(raw, primitive.clone())
                .map_err(|error| {
                    corrupt(format!(
                        "decode Iceberg staged artifact bound for field id {field_id}: {error}"
                    ))
                })?;
            Ok((*field_id, datum))
        })
        .collect()
}

impl IcebergWriteSessionControl {
    /// Perform the single external commit for one sealed session.
    ///
    /// The session's own state machine is the mutual exclusion: `begin_commit`
    /// latches, so a second `finish_write` cannot dispatch a second snapshot.
    pub fn commit_prepared_set(
        &self,
        handle: &IcebergCommitHandle,
        prepared: &ConnectorPreparedWriteSet,
        frozen_old_references: &BTreeMap<WriteTargetOrdinal, BTreeMap<String, Vec<String>>>,
        context: &ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        validate_context(context)?;
        let validated = validate_prepared_set(handle, &self.adapter, prepared)?;
        validate_merged_old_references(handle, frozen_old_references, &validated)?;

        handle.begin_commit()?;
        let outcome = self.dispatch_commit(handle, &validated, context);
        match &outcome {
            Ok(ExternalMutationOutcome::KnownCommitted { receipt, .. }) => {
                let snapshot_id = receipt
                    .committed_version()
                    .and_then(|version| version.snapshot_id())
                    .unwrap_or_default();
                handle.settle(IcebergWriteSessionState::KnownCommitted { snapshot_id })?;
            }
            Ok(ExternalMutationOutcome::KnownUncommitted { failure }) => {
                handle.settle(IcebergWriteSessionState::KnownUncommitted {
                    message: failure.message().to_string(),
                })?;
            }
            Ok(ExternalMutationOutcome::CommitUnknown { failure, .. }) => {
                handle.settle(IcebergWriteSessionState::CommitUnknown {
                    message: failure.message().to_string(),
                    staging_dir: handle.staging_dir(),
                })?;
            }
            Err(error) => {
                // A refusal raised before dispatch never touched the catalog,
                // so the session stays uncommitted rather than unknown.
                handle.settle(IcebergWriteSessionState::KnownUncommitted {
                    message: error.message().to_string(),
                })?;
            }
        }
        outcome
    }

    fn dispatch_commit(
        &self,
        handle: &IcebergCommitHandle,
        validated: &[ValidatedFragment<'_>],
        context: &ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        let facts = handle.table();
        let physical = self
            .runtime
            .load_table_for_request(facts.namespace(), facts.table_name(), context)
            .map_err(|error| unavailable(error.to_string()))?;
        let table = physical.into_table();
        let metadata = table.metadata().clone();
        if metadata.uuid().to_string() != facts.table_uuid()
            || metadata.current_schema_id() != facts.schema_id()
            || metadata.default_partition_spec_id() != facts.default_partition_spec_id()
            || crate::ref_snapshot::resolve_branch_head_snapshot_id(&metadata, facts.target_ref())
                .ok()
                .flatten()
                != facts.base_snapshot_id()
        {
            return Err(invalid(
                "Iceberg write target no longer matches its exact sealed session",
            ));
        }

        let files = validated
            .iter()
            .map(|entry| written_file_from_fragment(entry.fragment, &metadata))
            .collect::<Result<Vec<_>, _>>()?;

        let table_ident =
            crate::iceberg::TableIdent::from_strs([facts.namespace(), facts.table_name()])
                .map_err(|error| invalid(error.to_string()))?;
        let op_kind = handle.flavor().commit_op_kind();
        let collector = Arc::new(
            IcebergCommitCollector::new(
                op_kind,
                table_ident,
                facts.base_snapshot_id(),
                metadata.last_sequence_number(),
                metadata.current_schema().clone(),
                metadata.default_partition_spec().clone(),
                handle.staging_dir(),
            )
            .with_table_metadata(metadata.clone()),
        );
        collector.inject_written_files(files);

        let mut snapshot_properties = BTreeMap::new();
        snapshot_properties.insert(
            ICEBERG_WRITE_SESSION_MARKER_PROPERTY.to_string(),
            handle.session_id().to_string(),
        );

        let binding = self
            .runtime
            .resources()
            .planning_binding()
            .for_request(context.clone());
        let access = binding.resolve_access(metadata.location())?;
        let fs = access.operator();
        let cleanup_access = access.clone();
        let cleanup_path_mapper = Some(Arc::new(move |path: &str| {
            cleanup_access
                .bind_location(path, novarocks_fs::FileIdentity::new(path, 0, None))
                .map(|file| file.operator_relative_path().to_string())
                .unwrap_or_else(|_| path.to_string())
        }) as crate::commit::CleanupPathMapper);
        let catalog = self.runtime.novarocks_catalog().vendored_client();
        let input = RunInput {
            collector,
            catalog,
            table: table.clone(),
            fs,
            file_io: table.file_io().clone(),
            cleanup_path_mapper,
            cow_update_rewrite: None,
            selected_rewrite: None,
            target_ref: facts.target_ref().to_string(),
            snapshot_properties,
            atomic_partition_replacement: None,
        };
        // The runtime bridge wraps the commit, so a bridge failure says nothing
        // about whether the catalog request went out. Calling it uncommitted
        // would authorize deleting files a committed snapshot may reference, so
        // it is deliberately an unknown outcome carrying real evidence.
        let bridge_collector = Arc::clone(&input.collector);
        let result = match self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move { run_iceberg_commit(input).await })
        {
            Ok(Ok(outcome)) => Ok(outcome),
            Ok(Err(error)) => Err(error),
            Err(bridge) => Err(CommitServiceError::unknown(
                format!("Iceberg commit runtime bridge: {bridge}"),
                crate::commit::service::RecoveryEvidence::from_collector(&bridge_collector),
            )),
        };
        match result {
            Ok(outcome) => {
                let receipt = crate::write_codec::connector_write_receipt_with_partitioning(
                    outcome.new_snapshot_id,
                    None,
                    None,
                )
                .map_err(invalid)?;
                Ok(ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    receipt,
                    finalization: ExternalMutationFinalization::Complete,
                })
            }
            Err(CommitServiceError::InvalidInput { message }) => Err(invalid(message)),
            Err(CommitServiceError::KnownUncommitted { message, cleanup }) => {
                Ok(ExternalMutationOutcome::KnownUncommitted {
                    failure: failure(
                        ConnectorMutationFailureKind::Conflict,
                        format!(
                            "{message}; staged cleanup attempted={}, errors={}",
                            cleanup.attempted, cleanup.error_count
                        ),
                    ),
                })
            }
            Err(CommitServiceError::Unknown { message, evidence }) => {
                Ok(ExternalMutationOutcome::CommitUnknown {
                    failure: failure(ConnectorMutationFailureKind::Unavailable, message),
                    evidence: self.encode_evidence(handle, &evidence)?,
                })
            }
            Err(CommitServiceError::FinalizeFailedKnownCommitted {
                outcome,
                finalize_error,
                evidence,
            }) => match outcome {
                Some(committed) => {
                    let receipt = crate::write_codec::connector_write_receipt_with_partitioning(
                        committed.new_snapshot_id,
                        None,
                        None,
                    )
                    .map_err(invalid)?;
                    Ok(ExternalMutationOutcome::KnownCommitted {
                        effect: ExternalMutationEffect::Applied,
                        receipt,
                        finalization: ExternalMutationFinalization::Failed(failure(
                            ConnectorMutationFailureKind::Internal,
                            finalize_error,
                        )),
                    })
                }
                None => Ok(ExternalMutationOutcome::CommitUnknown {
                    failure: failure(ConnectorMutationFailureKind::Internal, finalize_error),
                    evidence: self.encode_evidence(handle, &evidence)?,
                }),
            },
        }
    }

    fn encode_evidence(
        &self,
        handle: &IcebergCommitHandle,
        recovery: &crate::commit::service::RecoveryEvidence,
    ) -> Result<ExternalMutationEvidence, ConnectorError> {
        encode_session_evidence(&self.descriptor, self.key.incarnation, handle, recovery)
    }

    fn decode_evidence(
        &self,
        handle: &IcebergCommitHandle,
        evidence: &ExternalMutationEvidence,
    ) -> Result<IcebergWriteSessionEvidenceV1, ConnectorError> {
        if evidence.schema_version() != ICEBERG_WRITE_SESSION_EVIDENCE_VERSION
            || evidence.descriptor() != &self.descriptor
            || evidence.incarnation() != self.key.incarnation
            || evidence.operation_kind() != ICEBERG_WRITE_SESSION_OPERATION_KIND
        {
            return Err(invalid(
                "Iceberg write session evidence does not belong to this exact generation",
            ));
        }
        if evidence.operation_id().to_bytes() != handle.session_id().to_bytes() {
            return Err(invalid(
                "Iceberg write session evidence names a different write session",
            ));
        }
        let payload: IcebergWriteSessionEvidenceV1 =
            serde_json::from_slice(evidence.provider_payload().as_ref()).map_err(|error| {
                corrupt(format!("decode Iceberg write session evidence: {error}"))
            })?;
        if payload.version != ICEBERG_WRITE_SESSION_EVIDENCE_VERSION
            || payload.session_id != handle.session_id().to_string()
        {
            return Err(corrupt(
                "Iceberg write session evidence payload disagrees with its envelope",
            ));
        }
        Ok(payload)
    }

    /// Release a session that never reached a complete prepared write set.
    ///
    /// Without a complete set there is no trustworthy cleanup manifest, so this
    /// path deliberately does not guess which staged objects exist. It reports
    /// what it knows and never claims a commit it did not observe.
    pub fn release_session(
        &self,
        handle: &IcebergCommitHandle,
        context: &ConnectorRequestContext,
    ) -> Result<ConnectorWriteAbortOutcome, ConnectorError> {
        validate_context(context)?;
        release_session_state(&self.descriptor, self.key.incarnation, handle)
    }
}

/// The terminal verdict a release produces, decided purely from the session's
/// own state so it can be reasoned about without a catalog.
pub(crate) fn release_session_state(
    descriptor: &ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    handle: &IcebergCommitHandle,
) -> Result<ConnectorWriteAbortOutcome, ConnectorError> {
    {
        match handle.state()? {
            IcebergWriteSessionState::Active => {
                handle.settle(IcebergWriteSessionState::KnownUncommitted {
                    message: "Iceberg write session was released before any external commit"
                        .to_string(),
                })?;
                Ok(ConnectorWriteAbortOutcome::KnownUncommitted {
                    cleanup: ExternalMutationFinalization::Complete,
                })
            }
            IcebergWriteSessionState::Committing => Err(unavailable(
                "Iceberg write abort cannot race an in-progress external commit",
            )),
            IcebergWriteSessionState::KnownUncommitted { .. } => {
                Ok(ConnectorWriteAbortOutcome::KnownUncommitted {
                    cleanup: ExternalMutationFinalization::Complete,
                })
            }
            IcebergWriteSessionState::KnownCommitted { snapshot_id } => {
                // Abort cannot undo a proven commit.
                let receipt = crate::write_codec::connector_write_receipt_with_partitioning(
                    snapshot_id,
                    None,
                    None,
                )
                .map_err(invalid)?;
                Ok(ConnectorWriteAbortOutcome::KnownCommitted {
                    receipt,
                    finalization: ExternalMutationFinalization::Complete,
                })
            }
            IcebergWriteSessionState::CommitUnknown {
                message,
                staging_dir,
            } => {
                // Abort cannot resolve ambiguity, and the staged objects stay
                // where reconciliation can still find them.
                Ok(ConnectorWriteAbortOutcome::CommitUnknown {
                    failure: failure(
                        ConnectorMutationFailureKind::Unavailable,
                        format!("{message}; staged files remain at {staging_dir}"),
                    ),
                    evidence: encode_session_evidence(
                        descriptor,
                        incarnation,
                        handle,
                        &crate::commit::service::RecoveryEvidence {
                            table_ident: format!(
                                "{}.{}",
                                handle.table().namespace(),
                                handle.table().table_name()
                            ),
                            op_kind: handle.flavor().commit_op_kind(),
                            base_snapshot_id: handle.table().base_snapshot_id(),
                            base_sequence_number: handle.table().base_sequence_number(),
                            staging_dir,
                            manifest_cleanup_token: None,
                        },
                    )?,
                })
            }
        }
    }
}

/// Seal one session's recovery facts into a reconciliation evidence envelope.
pub(crate) fn encode_session_evidence(
    descriptor: &ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    handle: &IcebergCommitHandle,
    recovery: &crate::commit::service::RecoveryEvidence,
) -> Result<ExternalMutationEvidence, ConnectorError> {
    let payload = IcebergWriteSessionEvidenceV1 {
        version: ICEBERG_WRITE_SESSION_EVIDENCE_VERSION,
        session_id: handle.session_id().to_string(),
        table_ident: recovery.table_ident.clone(),
        target_ref: handle.table().target_ref().to_string(),
        op_kind: format!("{:?}", recovery.op_kind),
        base_snapshot_id: recovery.base_snapshot_id,
        base_sequence_number: recovery.base_sequence_number,
        staging_dir: recovery.staging_dir.clone(),
        manifest_cleanup_token: recovery.manifest_cleanup_token.clone(),
    };
    let encoded = serde_json::to_vec(&payload)
        .map_err(|error| internal(format!("encode Iceberg write session evidence: {error}")))?;
    ExternalMutationEvidence::try_new(
        ICEBERG_WRITE_SESSION_EVIDENCE_VERSION,
        descriptor.clone(),
        incarnation,
        ConnectorMutationOperationId::from_bytes(handle.session_id().to_bytes()),
        ICEBERG_WRITE_SESSION_OPERATION_KIND,
        Bytes::from(encoded),
    )
}

impl IcebergWriteSessionControl {
    /// Resolve a session whose external outcome is unknown, by read-only
    /// adjudication against the durable snapshot marker.
    ///
    /// Marker absence is *not* proof of non-commit: the session stays unknown
    /// and every staged object is left untouched.
    pub fn adjudicate_session(
        &self,
        handle: &IcebergCommitHandle,
        evidence: &ExternalMutationEvidence,
        context: &ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        validate_context(context)?;
        let decoded = self.decode_evidence(handle, evidence)?;
        match handle.state()? {
            IcebergWriteSessionState::KnownCommitted { snapshot_id } => {
                let receipt = crate::write_codec::connector_write_receipt_with_partitioning(
                    snapshot_id,
                    None,
                    None,
                )
                .map_err(invalid)?;
                return Ok(ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    receipt,
                    finalization: ExternalMutationFinalization::Complete,
                });
            }
            IcebergWriteSessionState::KnownUncommitted { message } => {
                return Ok(ExternalMutationOutcome::KnownUncommitted {
                    failure: failure(ConnectorMutationFailureKind::Conflict, message),
                });
            }
            IcebergWriteSessionState::Active => {
                return Err(invalid(
                    "Iceberg write reconciliation requires a prior commit-unknown outcome",
                ));
            }
            IcebergWriteSessionState::Committing => {
                return Err(unavailable(
                    "Iceberg write reconciliation cannot race an in-progress external commit",
                ));
            }
            IcebergWriteSessionState::CommitUnknown { .. } => {}
        }

        let facts = handle.table();
        let physical = self
            .runtime
            .load_table_for_request(
                facts.namespace(),
                facts.table_name(),
                &context.clone().without_vended_credential_lease_sink(),
            )
            .map_err(|error| unavailable(error.to_string()))?;
        let table = physical.into_table();
        let metadata = table.metadata();
        if metadata.uuid().to_string() != facts.table_uuid() {
            return Err(invalid(
                "Iceberg write reconciliation loaded a different table identity",
            ));
        }
        let expected = handle.session_id().to_string();
        let mut matched: Option<i64> = None;
        for snapshot in metadata.snapshots() {
            let carried = snapshot
                .summary()
                .additional_properties
                .get(ICEBERG_WRITE_SESSION_MARKER_PROPERTY);
            if carried.is_some_and(|value| *value == expected)
                && matched.replace(snapshot.snapshot_id()).is_some()
            {
                return Err(corrupt(
                    "Iceberg write session marker matches multiple snapshots",
                ));
            }
        }
        match matched {
            Some(snapshot_id) => {
                let receipt = crate::write_codec::connector_write_receipt_with_partitioning(
                    snapshot_id,
                    None,
                    None,
                )
                .map_err(invalid)?;
                handle.settle(IcebergWriteSessionState::KnownCommitted { snapshot_id })?;
                Ok(ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    receipt,
                    finalization: ExternalMutationFinalization::Complete,
                })
            }
            None => Ok(ExternalMutationOutcome::CommitUnknown {
                failure: failure(
                    ConnectorMutationFailureKind::Unavailable,
                    format!(
                        "Iceberg write session marker is absent during read-only adjudication; staged files remain at {}",
                        decoded.staging_dir
                    ),
                ),
                evidence: evidence.clone(),
            }),
        }
    }
}

/// Assemble the neutral session plan from the provider's sealed targets.
pub(crate) fn session_plan_from_targets(
    adapter: &IcebergWriteAdapter,
    handle: IcebergCommitHandle,
    targets: Vec<crate::commit::write_stack::planning::IcebergWriteTargetPlan>,
) -> Result<ConnectorWriteSessionPlan, ConnectorError> {
    let commit = adapter.wrap_commit_handle(handle);
    let plans = targets
        .into_iter()
        .map(|target| {
            let (ordinal, writer, input) = target.into_parts();
            ConnectorWriteTargetPlan::new(ordinal, adapter.wrap_writer_handle(writer), input)
        })
        .collect::<Vec<_>>();
    ConnectorWriteSessionPlan::try_new(commit, plans)
}

impl IcebergWriteSessionControl {
    fn frozen_references_of(
        &self,
        handle: &IcebergCommitHandle,
    ) -> BTreeMap<WriteTargetOrdinal, BTreeMap<String, Vec<String>>> {
        handle.frozen_old_references()
    }

    /// Complete admission and freeze the write recipe.
    ///
    /// Everything external this method touches is a *read*: it loads the table,
    /// resolves the target ref's head, and enumerates the base snapshot's data
    /// files. It deliberately does not read a single delete artifact — that is
    /// the whole point of the NCP-6 inversion — and it performs no external
    /// mutation, so a failure here leaves nothing started.
    fn admit(
        &self,
        request: &ConnectorWriteBeginRequest,
    ) -> Result<
        (
            IcebergCommitHandle,
            Vec<crate::commit::write_stack::planning::IcebergWriteTargetPlan>,
        ),
        ConnectorError,
    > {
        let (namespace, table_name) = request.table.rsplit_once('.').ok_or_else(|| {
            invalid("Iceberg write target must be a namespace-qualified table name")
        })?;
        let physical = self
            .runtime
            .load_table_for_request(namespace, table_name, &request.context)
            .map_err(|error| unavailable(error.to_string()))?;
        let table = physical.into_table();
        let metadata = table.metadata().clone();
        let target_ref = request.target_ref.as_str();
        let base_snapshot_id =
            crate::ref_snapshot::resolve_branch_head_snapshot_id(&metadata, target_ref)
                .map_err(|error| invalid(error.to_string()))?;
        if let Some(base) = &request.base {
            base.validate()?;
        }
        let facts = IcebergWriteTableFacts::try_new(
            metadata.uuid().to_string(),
            namespace.to_string(),
            table_name.to_string(),
            metadata.location().to_string(),
            iceberg_data_location(&metadata),
            target_ref.to_string(),
            base_snapshot_id,
            metadata.last_sequence_number(),
            metadata.current_schema_id(),
            metadata.default_partition_spec_id(),
            format_version_number(&metadata),
        )?;
        let flavor = flavor_for(request)?;
        let signed = sign_input_shape(&facts, &request.input)?;
        let data = IcebergDataBranchPlan {
            output: IcebergWriterOutput::try_new(
                crate::delete_file::IcebergFileFormat::Parquet,
                parquet::basic::Compression::SNAPPY,
                crate::commit::data_writer::parquet_row_group_size_bytes(metadata.properties())
                    .map_err(invalid)?
                    .map(|size| size as u64),
            )?,
            recipe: data_branch_recipe(
                &metadata,
                matches!(signed, ConnectorWriteInputShape::RowLineage { .. }),
            )?,
            input: signed.clone(),
        };
        let deletes = match &signed {
            ConnectorWriteInputShape::PositionDelete { .. }
            | ConnectorWriteInputShape::DeletionVector { .. } => {
                let branch = if matches!(signed, ConnectorWriteInputShape::DeletionVector { .. }) {
                    IcebergWriteBranch::DeletionVector
                } else {
                    IcebergWriteBranch::PositionDelete
                };
                let output = IcebergWriterOutput::try_new(
                    match branch {
                        IcebergWriteBranch::DeletionVector => {
                            crate::delete_file::IcebergFileFormat::Puffin
                        }
                        _ => crate::delete_file::IcebergFileFormat::Parquet,
                    },
                    parquet::basic::Compression::SNAPPY,
                    None,
                )?;
                let snapshot_id = base_snapshot_id.ok_or_else(|| {
                    invalid("Iceberg row-level write requires a frozen target snapshot")
                })?;
                vec![IcebergDeleteBranchPlan {
                    branch,
                    output,
                    merge_targets: self.freeze_old_delete_references(
                        &table,
                        &metadata,
                        snapshot_id,
                    )?,
                    input: signed,
                }]
            }
            _ => Vec::new(),
        };
        crate::commit::write_stack::planning::plan_write_session(
            IcebergWriteSessionId::new(),
            IcebergWriteSessionPlanInput {
                flavor,
                purpose: request.purpose,
                table: facts,
                base_version_digest: request.base.as_ref().map(|base| base.digest()),
                data,
                deletes,
            },
        )
    }

    /// Freeze exact references to every old delete artifact attached to the
    /// base snapshot's data files.
    ///
    /// This is the frontend half of D10. It records what exists; it never opens
    /// one of those artifacts.
    fn freeze_old_delete_references(
        &self,
        table: &crate::iceberg::table::Table,
        metadata: &TableMetadata,
        snapshot_id: i64,
    ) -> Result<Vec<IcebergOldDeleteMergeTarget>, ConnectorError> {
        let owned = table.clone();
        let files = self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move {
                crate::manifest::extract_data_files_with_stats_at(&owned, snapshot_id).await
            })
            .map_err(|error| unavailable(error.to_string()))?
            .map_err(unavailable)?;
        let mut targets = Vec::with_capacity(files.len());
        for file in files {
            let partition_spec_id = file.partition_spec_id.ok_or_else(|| {
                corrupt(format!(
                    "Iceberg data file {} has no frozen partition spec ID",
                    file.path
                ))
            })?;
            let partition_values = file.partition_values.as_ref().ok_or_else(|| {
                corrupt(format!(
                    "Iceberg data file {} has no frozen partition values",
                    file.path
                ))
            })?;
            let partition_spec = metadata
                .partition_spec_by_id(partition_spec_id)
                .ok_or_else(|| {
                    corrupt(format!(
                        "Iceberg data file {} references unknown partition spec {partition_spec_id}",
                        file.path
                    ))
                })?;
            let (partition_path, null_fingerprint) =
                crate::commit::report::partition_path_from_struct(partition_values, partition_spec)
                    .map_err(corrupt)?;
            let descriptor = crate::write_descriptor::encode_partition_descriptor(
                partition_values,
                partition_spec_id,
                metadata,
            )
            .map_err(|error| corrupt(error.detail_message()))?;
            let partition = IcebergArtifactPartition::try_new(
                partition_path,
                null_fingerprint,
                partition_spec_id,
                descriptor,
            )?;
            let record_count =
                u64::try_from(file.record_count.unwrap_or_default()).map_err(|_| {
                    corrupt(format!(
                        "Iceberg data file {} has a negative record count",
                        file.path
                    ))
                })?;
            let mut references = Vec::new();
            for delete in &file.delete_files {
                if !matches!(
                    delete.file_content,
                    crate::scan_model::IcebergDeleteFileContent::Position
                ) {
                    continue;
                }
                let file_format = match delete.file_format {
                    crate::scan_model::IcebergDeleteFileFormat::Parquet => {
                        crate::delete_file::IcebergFileFormat::Parquet
                    }
                    crate::scan_model::IcebergDeleteFileFormat::Puffin => {
                        crate::delete_file::IcebergFileFormat::Puffin
                    }
                };
                let length = delete
                    .length
                    .and_then(|value| u64::try_from(value).ok())
                    .ok_or_else(|| {
                        corrupt(format!(
                            "Iceberg delete artifact {} has no usable frozen file size",
                            delete.path
                        ))
                    })?;
                let content_range = match (delete.content_offset, delete.content_size_in_bytes) {
                    (Some(offset), Some(size)) => Some(IcebergContentRange::try_new(offset, size)?),
                    (None, None) => None,
                    _ => {
                        return Err(corrupt(format!(
                            "Iceberg delete artifact {} carries a partial Puffin blob range",
                            delete.path
                        )));
                    }
                };
                let route = IcebergStorageRoute::try_for_location(&delete.path)?;
                references.push(IcebergOldDeleteArtifactRef::try_new(
                    delete.path.clone(),
                    crate::delete_file::IcebergFileContent::PositionDeletes,
                    file_format,
                    length,
                    // The Iceberg manifest carries a record count per delete
                    // file, but the provider's read-model projection does not
                    // surface it, so the reference is frozen without one rather
                    // than with a guessed value. The backend still rejects an
                    // exclusive artifact that decodes to nothing.
                    None,
                    content_range,
                    delete.referenced_data_file.clone(),
                    delete.sequence_number,
                    None,
                    delete.partition_spec_id.unwrap_or(partition_spec_id),
                    route,
                )?);
            }
            targets.push(IcebergOldDeleteMergeTarget::try_new(
                file.path,
                record_count,
                file.data_sequence_number,
                partition,
                snapshot_id,
                references,
            )?);
        }
        Ok(targets)
    }
}

fn iceberg_data_location(metadata: &TableMetadata) -> String {
    metadata
        .properties()
        .get("write.data.path")
        .cloned()
        .unwrap_or_else(|| format!("{}/data", metadata.location().trim_end_matches('/')))
}

fn format_version_number(metadata: &TableMetadata) -> u8 {
    match metadata.format_version() {
        crate::iceberg::spec::FormatVersion::V1 => 1,
        crate::iceberg::spec::FormatVersion::V2 => 2,
        _ => 3,
    }
}

/// Pick the flavor a begin request describes.
///
/// An unsupported combination fails here rather than being coerced into the
/// nearest supported one.
fn flavor_for(request: &ConnectorWriteBeginRequest) -> Result<IcebergWriteFlavor, ConnectorError> {
    use novarocks_spi::connector::{ConnectorWriteAdmissionPurpose, ConnectorWriteIntent};
    if request.purpose == ConnectorWriteAdmissionPurpose::MaterializedViewRefresh {
        return Ok(IcebergWriteFlavor::ManagedPublication);
    }
    Ok(match (request.intent, &request.input) {
        (ConnectorWriteIntent::Append, _) => IcebergWriteFlavor::Append,
        (ConnectorWriteIntent::Overwrite, _) => IcebergWriteFlavor::Overwrite,
        (ConnectorWriteIntent::PartitionOverwrite, _) => IcebergWriteFlavor::PartitionOverwrite,
        (ConnectorWriteIntent::RowDelta, ConnectorWriteInputRequest::DeletionVector { .. }) => {
            IcebergWriteFlavor::RowMutationDeletionVector
        }
        (ConnectorWriteIntent::RowDelta, ConnectorWriteInputRequest::PositionDelete { .. }) => {
            IcebergWriteFlavor::RowMutationPositionDelete
        }
        (ConnectorWriteIntent::RowDelta, ConnectorWriteInputRequest::RowLineage { .. }) => {
            IcebergWriteFlavor::RowMutationCopyOnWrite
        }
        (ConnectorWriteIntent::RowDelta, _) => {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "Iceberg row-delta write requires a position-delete, deletion-vector, or row-lineage input",
            ));
        }
    })
}

/// Sign the caller's requested fields into a provider-owned input shape.
///
/// The token is preparation-local and derived from the exact table generation,
/// so a binding minted for one session cannot be replayed into another.
fn sign_input_shape(
    facts: &IcebergWriteTableFacts,
    request: &ConnectorWriteInputRequest,
) -> Result<ConnectorWriteInputShape, ConnectorError> {
    let sign = |tag: &str, fields: &[ConnectorWriteFieldRequest]| {
        fields
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let mut hasher = Sha256::new();
                hasher.update(b"novarocks.iceberg.write-stack.field.v1\0");
                hasher.update(facts.table_uuid().as_bytes());
                hasher.update([0]);
                hasher.update(facts.target_ref().as_bytes());
                hasher.update([0]);
                hasher.update(tag.as_bytes());
                hasher.update([0]);
                hasher.update(index.to_be_bytes());
                hasher.update(field.field().name().as_bytes());
                hasher.update([0]);
                hasher.update(format!("{:?}", field.field().data_type()).as_bytes());
                hasher.update([u8::from(field.field().is_nullable())]);
                let token = ConnectorWriteFieldToken::from_bytes(hasher.finalize().into());
                ConnectorWriteFieldBinding::new(token, field.field().clone())
            })
            .collect::<Vec<_>>()
    };
    let shape = match request {
        ConnectorWriteInputRequest::Data { fields } => ConnectorWriteInputShape::Data {
            fields: sign("data", fields),
        },
        ConnectorWriteInputRequest::RowLineage {
            data_fields,
            row_identity_fields,
        } => ConnectorWriteInputShape::RowLineage {
            data_fields: sign("row-lineage-data", data_fields),
            row_identity_fields: sign("row-lineage-identity", row_identity_fields),
        },
        ConnectorWriteInputRequest::PositionDelete {
            identity_fields,
            partition_source_fields,
        } => ConnectorWriteInputShape::PositionDelete {
            identity_fields: sign("position-delete-identity", identity_fields),
            partition_source_fields: sign("position-delete-partition", partition_source_fields),
        },
        ConnectorWriteInputRequest::DeletionVector {
            identity_fields,
            partition_source_fields,
        } => ConnectorWriteInputShape::DeletionVector {
            identity_fields: sign("deletion-vector-identity", identity_fields),
            partition_source_fields: sign("deletion-vector-partition", partition_source_fields),
        },
        ConnectorWriteInputRequest::EqualityDelete { .. } => {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "Iceberg equality-delete writes are not part of the connector write stack",
            ));
        }
    };
    shape.validate()?;
    Ok(shape)
}

/// Derive the data branch's partitioning and schema recipe from the frozen
/// table metadata.
fn data_branch_recipe(
    metadata: &TableMetadata,
    row_lineage: bool,
) -> Result<IcebergDataBranchRecipe, ConnectorError> {
    let schema = metadata.current_schema();
    let spec = metadata.default_partition_spec();
    let mut sources = Vec::with_capacity(spec.fields().len());
    let mut names = Vec::with_capacity(spec.fields().len());
    let mut transforms = Vec::with_capacity(spec.fields().len());
    for field in spec.fields() {
        let source = schema.field_by_id(field.source_id).ok_or_else(|| {
            corrupt(format!(
                "Iceberg partition field {} names unknown source id {}",
                field.name, field.source_id
            ))
        })?;
        sources.push(source.name.clone());
        names.push(field.name.clone());
        transforms.push(field.transform.to_string());
    }
    IcebergDataBranchRecipe::try_new(
        Some(crate::schema_facts::iceberg_schema_def(schema)),
        sources,
        names,
        transforms,
        row_lineage,
    )
}

impl novarocks_spi::connector::write_stack::session::ConnectorWriteControl
    for IcebergWriteSessionControl
{
    fn binding_key(&self) -> &ConnectorProviderBindingKey {
        &self.key
    }

    fn begin_write(
        &self,
        request: ConnectorWriteBeginRequest,
    ) -> Result<ConnectorWriteSessionPlan, ConnectorError> {
        validate_context(&request.context)?;
        let (handle, targets) = self.admit(&request)?;
        // The frozen old-delete map is derived from the same writer handles the
        // plan carries, so `finish_write` can re-derive it without a second
        // source of truth.
        let plan = session_plan_from_targets(&self.adapter, handle, targets)?;
        Ok(plan)
    }

    fn finish_write(
        &self,
        request: ConnectorWriteFinishRequest<'_>,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        let handle = self.adapter.commit_handle(request.commit)?;
        let frozen = self.frozen_references_of(handle);
        self.commit_prepared_set(handle, &request.prepared, &frozen, &request.context)
    }

    fn abort_write(
        &self,
        request: ConnectorWriteSessionAbortRequest<'_>,
    ) -> Result<ConnectorWriteAbortOutcome, ConnectorError> {
        let handle = self.adapter.commit_handle(request.commit)?;
        self.release_session(handle, &request.context)
    }

    fn reconcile_write(
        &self,
        request: ConnectorWriteSessionReconcileRequest<'_>,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        let handle = self.adapter.commit_handle(request.commit)?;
        self.adjudicate_session(handle, &request.evidence, &request.context)
    }
}
