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

use base64::Engine;
use bytes::Bytes;
use novarocks_spi::connector::write_stack::session::{
    ConnectorWriteBeginRequest, ConnectorWriteFinishRequest, ConnectorWriteSessionAbortRequest,
    ConnectorWriteSessionFlavor, ConnectorWriteSessionPlan, ConnectorWriteSessionReconcileRequest,
    ConnectorWriteTargetPlan,
};
use novarocks_spi::connector::write_stack::{
    ConnectorManagedPublicationShape, ConnectorPreparedWriteSet, WriteTargetOrdinal,
};
use novarocks_spi::connector::{
    CatalogHandle, ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor,
    ConnectorManagedPublicationIntent, ConnectorMutationFailure, ConnectorMutationFailureKind,
    ConnectorMutationOperationId, ConnectorProviderBindingKey, ConnectorRequestContext,
    ConnectorWriteAbortOutcome, ConnectorWriteBaseVersion, ConnectorWriteFieldBinding,
    ConnectorWriteFieldRequest, ConnectorWriteFieldToken, ConnectorWriteInputRequest,
    ConnectorWriteInputShape, ConnectorWriteReceipt, ExternalMutationEffect,
    ExternalMutationEvidence, ExternalMutationFinalization, ExternalMutationOutcome,
    ProviderBindingEpoch,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::commit::write_stack::domain::{
    IcebergArtifactPartition, IcebergCommitArtifact, IcebergCommitFragment, IcebergCommitHandle,
    IcebergContentRange, IcebergDataBranchRecipe, IcebergEmptyWriteDecision,
    IcebergManagedPublicationFacts, IcebergManagedPublicationProvenance, IcebergWriteFlavor,
    IcebergWriteSessionId, IcebergWriteSessionState, IcebergWriteTableFacts, IcebergWriterOutput,
    corrupt, invalid,
};
use crate::commit::write_stack::flavor::{
    IcebergSessionFlavorPlan, IcebergSessionMaterial, plan_copy_on_write_branches,
    plan_distributed_rewrite_branches, plan_managed_publication_branches, plan_ordinary_branches,
    plan_row_mutation_branches,
};
use crate::commit::write_stack::old_delete::{
    IcebergOldDeleteArtifactRef, IcebergOldDeleteMergeTarget, IcebergStorageRoute,
};
use crate::commit::write_stack::planning::{IcebergBranchSessionPlanInput, plan_branch_session};
use crate::commit::write_stack::runtime::IcebergWriteAdapter;
use crate::commit::{
    CommitOpKind, CommitServiceError, CowUpdateRewriteSet, CowUpdateTouchedFile,
    IcebergCommitCollector, RunInput, WrittenFile, run_iceberg_commit,
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
///
/// The check runs over the *staged* artifacts, never over the frozen map. The
/// frozen map is a superset by construction: `freeze_old_delete_references`
/// walks every data file of the base snapshot, because at `begin_write` the
/// frontend cannot know which of them the statement's predicate will match. A
/// merge-on-read writer stages a delete artifact only for the data files it
/// actually deleted a row from, and the commit action carries every other data
/// file's delete manifest through verbatim, so a frozen data file with no
/// staged artifact keeps its existing deletes rather than losing them.
/// Demanding one artifact per frozen data file would therefore refuse every
/// `DELETE` that does not happen to touch the whole table.
pub(crate) fn validate_merged_old_references(
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
        equality_ids,
    ) = match fragment.artifact() {
        IcebergCommitArtifact::DataFile(file) => (
            DataFileFormat::Parquet,
            DataContentType::Data,
            None,
            file.first_row_id(),
            None,
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
            None,
        ),
        // An equality delete names no data file: Iceberg matches it against
        // every row whose equality-column values agree, so the manifest carries
        // the field ids instead of a referenced path.
        IcebergCommitArtifact::EqualityDeleteFile(file) => (
            DataFileFormat::Parquet,
            DataContentType::EqualityDeletes,
            None,
            None,
            None,
            None,
            None,
            Some(file.equality_field_ids().to_vec()),
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
        equality_ids,
        first_row_id,
        content_offset,
        content_size_in_bytes: content_size,
        cardinality,
    })
}

/// The frozen input file set a selected-rewrite commit replaces.
///
/// Present exactly when the session commits as a rewrite. The set comes off the
/// session, not off the fragments: the commit action asserts that the frozen
/// inputs are still live and retires all of them in one snapshot, and a group
/// whose rows were all compacted away stages no artifact at all while still
/// having to be retired.
///
/// The kind is `Data` because a session-planned rewrite is cut by
/// [`plan_data_file_groups`](crate::distributed_rewrite::plan_data_file_groups),
/// which groups whole data files. A position-delete rewrite is a different
/// planner and is not a write-session flavor.
/// The summary properties the single external commit stamps onto its snapshot.
///
/// Two things are recorded, and each exists because something later reads it
/// back off the snapshot rather than out of memory:
///
/// * the write-session marker, which is the only proof available to
///   reconciliation after an unknown outcome;
/// * a managed publication's durable provenance, whose publication id is what
///   the publication fence matches when it fast-forwards a staged refresh
///   (`commit::snapshot_matches_publication_marker`). A publication that
///   committed without it would strand its own snapshot.
///
/// `staged_data_rows` seeds the provenance row count. The commit action
/// replaces it with the committed snapshot's real `total-records`, so it is a
/// starting value rather than a claim.
pub(crate) fn session_snapshot_properties(
    handle: &IcebergCommitHandle,
    staged_data_rows: u64,
) -> Result<BTreeMap<String, String>, ConnectorError> {
    let mut properties = BTreeMap::new();
    properties.insert(
        ICEBERG_WRITE_SESSION_MARKER_PROPERTY.to_string(),
        handle.session_id().to_string(),
    );
    if let Some(publication) = handle.publication() {
        for (key, value) in publication
            .provenance()
            .to_summary_properties(publication.technique(), staged_data_rows)?
        {
            if properties.insert(key, value).is_some() {
                return Err(internal(
                    "Iceberg write session marker conflicts with managed snapshot properties",
                ));
            }
        }
    }
    Ok(properties)
}

/// The replacement record one copy-on-write commit applies.
///
/// The join key is the write target ordinal and nothing else. Branch `i`
/// replaces exactly the file its frozen recipe named, and the artifacts the
/// backends staged for target `i` are that file's replacement — so a
/// statement that rewrote three files produces three touched files, each with
/// its own artifacts, rather than one merged claim. No cohort, operation,
/// execution, or attempt identity takes part.
///
/// The matched row ids come off the frozen recipe rather than off anything the
/// writers reported: their minimum becomes the replacement manifest's
/// `first_row_id`, which is what stops the v3 manifest-list writer allocating
/// fresh `_row_id`s for rows that already have them.
pub(crate) fn cow_update_rewrite_set(
    handle: &IcebergCommitHandle,
    staged: &[(WriteTargetOrdinal, WrittenFile)],
) -> Result<Option<CowUpdateRewriteSet>, ConnectorError> {
    use crate::commit::write_stack::copy_on_write::IcebergCowBranchInput;

    if handle.commit_op_kind() != CommitOpKind::CowUpdate {
        return Ok(None);
    }
    let base_snapshot_id = handle
        .table()
        .base_snapshot_id()
        .ok_or_else(|| invalid("Iceberg copy-on-write commit requires a frozen base snapshot"))?;
    let staged_for = |ordinal: WriteTargetOrdinal| {
        staged
            .iter()
            .filter(move |(staged_ordinal, _)| *staged_ordinal == ordinal)
            .map(|(_, file)| file)
    };
    let mut touched_data_files = Vec::new();
    let mut appended_files = Vec::new();
    let mut updated_row_ids = BTreeSet::new();
    for (index, branch) in handle.copy_on_write().iter().enumerate() {
        let ordinal = WriteTargetOrdinal::try_new(
            u32::try_from(index)
                .map_err(|_| internal("Iceberg copy-on-write branch ordinal overflowed"))?,
        )?;
        match branch {
            IcebergCowBranchInput::Rewrite {
                old_file,
                matched_row_ids,
            } => {
                updated_row_ids.extend(matched_row_ids.iter().copied());
                touched_data_files.push(CowUpdateTouchedFile {
                    old_file: old_file.clone(),
                    new_files: staged_for(ordinal).map(|file| file.path.clone()).collect(),
                    row_ids: matched_row_ids.clone(),
                });
            }
            IcebergCowBranchInput::Append => {
                appended_files.extend(staged_for(ordinal).cloned());
            }
        }
    }
    Ok(Some(CowUpdateRewriteSet {
        base_snapshot_id,
        target_table_uuid: handle.table().table_uuid().to_string(),
        updated_row_ids: updated_row_ids.into_iter().collect(),
        touched_data_files,
        appended_files,
    }))
}

pub(crate) fn selected_rewrite_files(
    handle: &IcebergCommitHandle,
) -> Option<crate::commit::selected_rewrite::SelectedRewriteFiles> {
    if handle.commit_op_kind() != CommitOpKind::SelectedRewrite {
        return None;
    }
    let frozen = handle.frozen_rewrite_input();
    Some(crate::commit::selected_rewrite::SelectedRewriteFiles {
        kind: crate::commit::selected_rewrite::SelectedRewriteKind::Data,
        data_paths: frozen.data_paths().clone(),
        delete_paths: frozen.delete_paths().clone(),
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

/// Whether a field id is one of the row-lineage columns Iceberg reserves.
///
/// These ids live outside the table schema on purpose: `annotate_schema_from_scan_model`
/// stamps them onto the Arrow field directly so a lineage column can ride with
/// the data without being a table column.
const fn is_reserved_row_lineage_field(field_id: i32) -> bool {
    field_id == crate::row_lineage_synth::ICEBERG_RESERVED_FIELD_ID_ROW_ID
        || field_id
            == crate::row_lineage_synth::ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER
}

fn decode_bounds(
    bounds: &BTreeMap<i32, Vec<u8>>,
    metadata: &TableMetadata,
) -> Result<std::collections::HashMap<i32, crate::iceberg::spec::Datum>, ConnectorError> {
    let schema = metadata.current_schema();
    bounds
        .iter()
        // A row-lineage column is stamped with a reserved field id that is
        // deliberately absent from the table schema -- that absence is what
        // lets it travel beside the table's own columns. It therefore has no
        // schema type to decode a bound against, and Iceberg prunes on no
        // lineage column, so its bound is dropped rather than treated as a
        // corrupt artifact. Any other unknown id still is one.
        .filter(|(field_id, _)| !is_reserved_row_lineage_field(**field_id))
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
        validate_merged_old_references(frozen_old_references, &validated)?;

        if validated.is_empty()
            && handle.empty_write_decision() == IcebergEmptyWriteDecision::SkipExternalCommit
        {
            return settle_empty_write_without_commit(handle);
        }

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

    /// Seal a staged write into the receipt its publication will consume.
    ///
    /// The validation above the seal is the *same* validation the committing
    /// path runs, and deliberately so: a staged artifact that would have been
    /// rejected before a snapshot must be rejected before a publication too,
    /// or the create would carry files the ordinary path would never have
    /// admitted.
    ///
    /// What differs is everything after it. Nothing here touches the catalog,
    /// writes a manifest, or creates a snapshot; the validated artifacts are
    /// folded into the receipt payload and the session settles as sealed. The
    /// single `NotExist` assert-create that makes them visible stays with the
    /// staged-create capability that owns the target.
    ///
    /// An empty prepared write set reaches this the same way a non-empty one
    /// does. Whether an empty staged create publishes an empty table or refuses
    /// is a decision for the publication, which can see the target; it must not
    /// be inferred here from "there were no fragments".
    fn seal_staged_prepared_set(
        &self,
        handle: &IcebergCommitHandle,
        prepared: &ConnectorPreparedWriteSet,
        frozen_old_references: &BTreeMap<WriteTargetOrdinal, BTreeMap<String, Vec<String>>>,
        context: &ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        validate_context(context)?;
        let metadata = handle.staged_metadata().ok_or_else(|| {
            internal("Iceberg staged write session lost its frozen target metadata")
        })?;
        let validated = validate_prepared_set(handle, &self.adapter, prepared)?;
        validate_merged_old_references(frozen_old_references, &validated)?;

        // Claim the single terminal attempt before producing anything, so a
        // second finish cannot mint a second receipt for the same session.
        handle.begin_commit()?;
        let sealed = validated
            .iter()
            .map(|entry| {
                let file = written_file_from_fragment(entry.fragment, metadata)?;
                crate::commit::report::writer_report_from_written_file(&file, metadata)
                    .map_err(corrupt)
            })
            .collect::<Result<Vec<_>, ConnectorError>>();
        let sealed = match sealed {
            Ok(sealed) => sealed,
            Err(error) => {
                handle.settle(IcebergWriteSessionState::KnownUncommitted {
                    message: error.message().to_string(),
                })?;
                return Err(error);
            }
        };
        let payload = match crate::write_codec::encode_writer_reports(&sealed, metadata) {
            Ok(payload) => payload,
            Err(error) => {
                handle.settle(IcebergWriteSessionState::KnownUncommitted {
                    message: error.clone(),
                })?;
                return Err(internal(format!("seal staged Iceberg write set: {error}")));
            }
        };
        let receipt = match ConnectorWriteReceipt::try_new(payload) {
            Ok(receipt) => receipt,
            Err(error) => {
                handle.settle(IcebergWriteSessionState::KnownUncommitted {
                    message: error.message().to_string(),
                })?;
                return Err(error);
            }
        };
        handle.settle(IcebergWriteSessionState::Sealed)?;
        // `NoOp` is the honest effect: this session applied nothing externally,
        // and it is `KnownCommitted` only in the sense the caller needs -- the
        // sealing is finished and its result is not in doubt.
        Ok(ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::NoOp,
            receipt,
            finalization: ExternalMutationFinalization::Complete,
        })
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

        // The staged artifacts were written under the generation the *writers*
        // were built against. Under an atomic partition replacement that is the
        // prospective one, whose new spec the loaded table does not have yet, so
        // interpreting the artifacts against the loaded metadata would fail to
        // resolve their own partition spec.
        let commit_metadata = handle
            .repartition()
            .map_or(&metadata, |prepared| prepared.prospective_metadata());

        let files = validated
            .iter()
            .map(|entry| written_file_from_fragment(entry.fragment, commit_metadata))
            .collect::<Result<Vec<_>, _>>()?;

        let table_ident =
            crate::iceberg::TableIdent::from_strs([facts.namespace(), facts.table_name()])
                .map_err(|error| invalid(error.to_string()))?;
        let op_kind = handle.commit_op_kind();
        let collector = Arc::new(
            IcebergCommitCollector::new(
                op_kind,
                table_ident,
                facts.base_snapshot_id(),
                metadata.last_sequence_number(),
                commit_metadata.current_schema().clone(),
                commit_metadata.default_partition_spec().clone(),
                handle.staging_dir(),
            )
            .with_table_metadata(commit_metadata.clone()),
        );
        // Every data row this write staged, counted before the files are moved
        // into the collector. It seeds the publication provenance the commit
        // action later refines into the committed snapshot's `total-records`.
        let staged_data_rows = files
            .iter()
            .filter(|file| file.content == DataContentType::Data)
            .fold(0u64, |total, file| total.saturating_add(file.record_count));
        // Branch `i`'s replacement artifacts are the ones its own writers
        // staged, so the record is keyed by the write target ordinal the
        // fragment carried and by nothing else.
        let cow_update_rewrite = cow_update_rewrite_set(
            handle,
            &validated
                .iter()
                .map(|entry| entry.ordinal)
                .zip(files.iter().cloned())
                .collect::<Vec<_>>(),
        )?;
        collector.inject_written_files(files);

        let snapshot_properties = session_snapshot_properties(handle, staged_data_rows)?;

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
            cow_update_rewrite,
            selected_rewrite: selected_rewrite_files(handle),
            // A partition replacement is a change to the table itself, and the
            // one commit that carries it has to be the one that publishes the
            // rows written under the new spec. `main` is where a managed
            // publication's default partitioning lives, so the replacement's
            // commit targets it exactly.
            target_ref: match handle.repartition() {
                Some(_) => "main".to_string(),
                None => facts.target_ref().to_string(),
            },
            snapshot_properties,
            atomic_partition_replacement: handle
                .repartition()
                .map(|prepared| {
                    crate::commit::run::AtomicPartitionReplacement::try_new(
                        prepared.metadata_updates().to_vec(),
                    )
                })
                .transpose()
                .map_err(invalid)?,
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
                // The commit is proven. Everything below only *describes* it,
                // so a failure here degrades the finalization and never the
                // verdict: calling a published snapshot uncommitted would
                // authorize deleting files it already references.
                let (resulting_row_count, finalization) =
                    match self.publication_row_count(handle, outcome.new_snapshot_id, context) {
                        Ok(rows) => (rows, ExternalMutationFinalization::Complete),
                        Err(error) => (
                            None,
                            ExternalMutationFinalization::Failed(failure(
                                ConnectorMutationFailureKind::Internal,
                                error.message().to_string(),
                            )),
                        ),
                    };
                let receipt = crate::write_codec::connector_write_receipt_with_partitioning(
                    outcome.new_snapshot_id,
                    resulting_row_count,
                    handle
                        .repartition()
                        .map(|prepared| prepared.committed().clone()),
                )
                .map_err(invalid)?;
                Ok(ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    receipt,
                    finalization,
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

    /// Project the committed snapshot's row count onto a publication's receipt.
    ///
    /// Only a managed publication claims one: its caller records the published
    /// row count as part of the refresh, and no ordinary DML receipt carries a
    /// row count at all. Returning `None` for everything else is the honest
    /// answer, not a missing feature.
    ///
    /// The read has to reload. `dispatch_commit` loaded the table before the
    /// commit and the generation-local cache still holds that pre-commit view,
    /// which by construction cannot know the snapshot just created. The reload
    /// keeps the request's already-authorized storage resolver but drops the
    /// attempt's lease sink, so it cannot admit a vended-credential response
    /// after the attempt froze.
    pub(crate) fn publication_row_count(
        &self,
        handle: &IcebergCommitHandle,
        snapshot_id: i64,
        context: &ConnectorRequestContext,
    ) -> Result<Option<u64>, ConnectorError> {
        if handle.publication().is_none() {
            return Ok(None);
        }
        let facts = handle.table();
        self.runtime
            .control_state()
            .invalidate_table_cache(facts.namespace(), facts.table_name());
        let table = self
            .runtime
            .load_table_for_request(
                facts.namespace(),
                facts.table_name(),
                &context.clone().without_vended_credential_lease_sink(),
            )
            .map_err(|error| internal(error.to_string()))?
            .into_table();
        let snapshot = table
            .metadata()
            .snapshot_by_id(snapshot_id)
            .ok_or_else(|| {
                internal("committed Iceberg snapshot is absent during managed row-count projection")
            })?;
        snapshot
            .summary()
            .additional_properties
            .get("total-records")
            .map(|value| value.parse::<u64>())
            .transpose()
            .map_err(|error| {
                corrupt(format!(
                    "committed Iceberg snapshot has an unreadable row count: {error}"
                ))
            })
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

/// Terminate a session whose empty prepared write set means "do nothing".
///
/// This is the provider's own decision, taken from the session's flavor and —
/// for a publication — the disposition its caller declared. It is deliberately
/// not derivable from "there were no fragments": a zero-row `INSERT` reaches
/// `finish_write` exactly the same way and still publishes an empty snapshot.
///
/// Nothing external is touched here: no snapshot is created, the target ref
/// keeps the head the session froze, and the outcome names the version the
/// target already held so a caller records an unchanged result rather than a
/// failure. It needs no catalog at all, because every fact it uses was frozen
/// at `begin_write`.
///
/// A session whose target holds no snapshot has no version to report, so it
/// fails closed there instead of claiming a commit against nothing.
pub(crate) fn settle_empty_write_without_commit(
    handle: &IcebergCommitHandle,
) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
    let Some(unchanged_snapshot_id) = handle.table().base_snapshot_id() else {
        let message = format!(
            "Iceberg {} write produced no artifact and its target holds no snapshot to report",
            handle.flavor().as_str()
        );
        handle.settle(IcebergWriteSessionState::KnownUncommitted {
            message: message.clone(),
        })?;
        return Ok(ExternalMutationOutcome::KnownUncommitted {
            failure: failure(ConnectorMutationFailureKind::Conflict, message),
        });
    };
    let receipt = crate::write_codec::connector_write_receipt_with_partitioning(
        unchanged_snapshot_id,
        None,
        None,
    )
    .map_err(invalid)?;
    handle.settle(IcebergWriteSessionState::KnownCommitted {
        snapshot_id: unchanged_snapshot_id,
    })?;
    Ok(ExternalMutationOutcome::KnownCommitted {
        effect: ExternalMutationEffect::NoOp,
        receipt,
        finalization: ExternalMutationFinalization::Complete,
    })
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
            // A sealed staged write never reached the catalog, so releasing it
            // is proven-uncommitted. The staged objects it wrote belong to the
            // staged target, and the publication that owns that target is what
            // deletes them; this session has no authority over them at all.
            IcebergWriteSessionState::Sealed => Ok(ConnectorWriteAbortOutcome::KnownUncommitted {
                cleanup: ExternalMutationFinalization::Complete,
            }),
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
                            op_kind: handle.commit_op_kind(),
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
            // A seal performs no external commit, so its outcome was never in
            // doubt and there is nothing for reconciliation to resolve.
            IcebergWriteSessionState::Sealed => {
                return Err(invalid(
                    "Iceberg staged write session has no external commit to reconcile",
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
///
/// A branch's routing facts travel with it; its writer recipe stays opaque. The
/// two are deliberately separate: the recipe is what a backend executes, and the
/// route facts are what SQL needs to decide which branch a row belongs to.
pub(crate) fn session_plan_from_targets(
    adapter: &IcebergWriteAdapter,
    handle: IcebergCommitHandle,
    targets: Vec<crate::commit::write_stack::planning::IcebergWriteTargetPlan>,
) -> Result<ConnectorWriteSessionPlan, ConnectorError> {
    let commit = adapter.wrap_commit_handle(handle);
    let plans = targets
        .into_iter()
        .map(|target| {
            let (ordinal, writer, input, route, rewrite_source) = target.into_parts();
            let plan =
                ConnectorWriteTargetPlan::new(ordinal, adapter.wrap_writer_handle(writer), input);
            let plan = match route {
                Some(route) => plan.with_route(route),
                None => plan,
            };
            match rewrite_source {
                Some(source) => plan.with_rewrite_source(source),
                None => plan,
            }
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
        // A staged target is the one target that cannot be looked up: the
        // catalog will not know it until the publication that owns it commits.
        // So its frozen facts arrive with the request, and this branch reads
        // them instead of loading. It is deliberately the only place a session
        // accepts caller-supplied metadata, and it accepts it only for the
        // flavor whose whole definition is "there is nothing to load".
        let staged = match &request.flavor {
            ConnectorWriteSessionFlavor::StagedCreate(target) => Some(
                crate::metadata::staged_target_metadata(&self.key.instance_id, target)?,
            ),
            _ => None,
        };
        let (table, metadata) = match &staged {
            Some(staged) => {
                if staged.namespace != namespace || staged.table != table_name {
                    return Err(invalid(
                        "Iceberg staged write target names a different table than its frozen facts",
                    ));
                }
                (None, staged.metadata.clone())
            }
            None => {
                let physical = self
                    .runtime
                    .load_table_for_request(namespace, table_name, &request.context)
                    .map_err(|error| unavailable(error.to_string()))?;
                let table = physical.into_table();
                let metadata = table.metadata().clone();
                (Some(table), metadata)
            }
        };
        // The session is the write's admission point, so it applies the same
        // support guards the separate preparation call applies. Without this a
        // session would admit a write this table cannot encode, and only a
        // caller that happened to also prepare would find out. A staged target
        // is exempt: it is a table this statement is creating, so there is no
        // existing schema or spec history to be unsupported.
        if staged.is_none() {
            crate::commit::validation::ensure_iceberg_write_supported_from_metadata(&metadata)
                .map_err(|error| ConnectorError::new(ConnectorErrorKind::Unsupported, error))?;
        }
        let target_ref = request.target_ref.as_str();
        let base_snapshot_id = match &staged {
            // A staged target holds no snapshot, and resolving a branch head
            // against it would either invent one or fail on an absent ref.
            Some(_) => None,
            None => crate::ref_snapshot::resolve_branch_head_snapshot_id(&metadata, target_ref)
                .map_err(|error| invalid(error.to_string()))?,
        };
        if let Some(base) = &request.base {
            if staged.is_some() {
                return Err(invalid(
                    "Iceberg staged write target has no base version to compare against",
                ));
            }
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
        // A managed publication may replace the target's default partitioning
        // inside the same external commit that publishes its rows. Preparing it
        // here, before any branch is planned, is what lets the writers write
        // under the spec the commit is about to establish: a writer that used
        // the spec the table still has would stage files partitioned against a
        // spec the same commit retires.
        let repartition = match &request.flavor {
            ConnectorWriteSessionFlavor::ManagedPublication { intent, .. } => intent
                .partition_spec_replacement()
                .map(|replacement| {
                    let prepared = crate::commit::write_stack::repartition::
                        prepare_managed_repartition(
                            &metadata,
                            replacement,
                            intent.descriptor_properties(),
                        )?;
                    // The caller signed the provider's own earlier preview into
                    // the intent. Re-deriving it here and requiring equality is
                    // the optimistic-concurrency check: a table whose spec moved
                    // between preview and admission produces a different result
                    // and is refused rather than silently repartitioned another
                    // way.
                    let expected = intent.expected_committed_partitioning().ok_or_else(|| {
                        invalid(
                            "Iceberg managed partition replacement is missing its exact preview partitioning",
                        )
                    })?;
                    if prepared.committed() != expected {
                        return Err(invalid(
                            "Iceberg managed partition replacement no longer matches its exact preview partitioning",
                        ));
                    }
                    Ok(prepared)
                })
                .transpose()?,
            _ => None,
        };
        // The generation every writer is built against. Under a partition
        // replacement it is the prospective one; the session's own facts stay
        // on the generation the table currently holds, because that is what its
        // commit-time compare-and-swap has to match.
        let writer_metadata = repartition
            .as_ref()
            .map_or(&metadata, |prepared| prepared.prospective_metadata());
        let writer_facts = match &repartition {
            None => None,
            Some(_) => Some(IcebergWriteTableFacts::try_new(
                writer_metadata.uuid().to_string(),
                namespace.to_string(),
                table_name.to_string(),
                writer_metadata.location().to_string(),
                iceberg_data_location(writer_metadata),
                target_ref.to_string(),
                base_snapshot_id,
                writer_metadata.last_sequence_number(),
                writer_metadata.current_schema_id(),
                writer_metadata.default_partition_spec_id(),
                format_version_number(writer_metadata),
            )?),
        };
        let signed = sign_input_shape(&facts, &request.input)?;
        let material = IcebergSessionMaterial {
            data_output: IcebergWriterOutput::try_new(
                crate::delete_file::IcebergFileFormat::Parquet,
                parquet::basic::Compression::SNAPPY,
                crate::commit::data_writer::parquet_row_group_size_bytes(
                    writer_metadata.properties(),
                )
                .map_err(invalid)?
                .map(|size| size as u64),
            )?,
            data_recipe: data_branch_recipe(
                writer_metadata,
                matches!(signed, ConnectorWriteInputShape::RowLineage { .. }),
            )?,
            // A delete branch's frozen old-delete references are the one piece
            // of material that costs an external read, so it is taken only when
            // the request's own shape says a delete branch exists.
            merge_targets: if session_freezes_old_deletes(&request.flavor, &signed) {
                let snapshot_id = base_snapshot_id.ok_or_else(|| {
                    invalid("Iceberg row-level write requires a frozen target snapshot")
                })?;
                let table = table.as_ref().ok_or_else(|| {
                    invalid("Iceberg row-level write requires a loaded target table")
                })?;
                self.freeze_old_delete_references(table, &metadata, snapshot_id)?
            } else {
                Vec::new()
            },
            // The match key needs the frozen Iceberg schema to turn a column
            // name into a field id, so it is resolved here rather than in the
            // pure branch planner.
            equality: match &signed {
                ConnectorWriteInputShape::EqualityDelete { equality_fields } => {
                    Some(equality_delete_recipe(writer_metadata, equality_fields)?)
                }
                _ => None,
            },
            table: facts,
            input: signed,
        };

        let plan = match &request.flavor {
            // A staged create writes data into an empty target, which is the
            // same branch shape an append has -- one data branch. What differs
            // is where the commit happens, and that is the flavor's business,
            // not the branch planner's.
            ConnectorWriteSessionFlavor::Ordinary
            | ConnectorWriteSessionFlavor::StagedCreate(_) => {
                plan_ordinary_branches(flavor_for(request)?, &material)?
            }
            ConnectorWriteSessionFlavor::ManagedPublication { intent, shape } => {
                plan_managed_publication_branches(&material, publication_facts(intent, *shape)?)?
            }
            ConnectorWriteSessionFlavor::RowMutation => plan_row_mutation_branches(&material)?,
            ConnectorWriteSessionFlavor::DistributedRewrite => {
                let table = table.as_ref().ok_or_else(|| {
                    invalid("Iceberg distributed rewrite requires a loaded target table")
                })?;
                let groups = self.freeze_rewrite_groups(table, base_snapshot_id)?;
                plan_distributed_rewrite_branches(&material, &groups)?
            }
            ConnectorWriteSessionFlavor::CopyOnWrite(selection) => {
                let table = table.as_ref().ok_or_else(|| {
                    invalid("Iceberg copy-on-write mutation requires a loaded target table")
                })?;
                // Without a base snapshot there is nothing to rewrite, and a
                // copy-on-write mutation that silently became an append would
                // publish after-images while every before-image stayed live.
                let snapshot_id = base_snapshot_id.ok_or_else(|| {
                    invalid("Iceberg copy-on-write mutation requires a frozen base snapshot")
                })?;
                let base_version_digest = request
                    .base
                    .as_ref()
                    .map(ConnectorWriteBaseVersion::digest)
                    .ok_or_else(|| {
                        invalid("Iceberg copy-on-write mutation requires its signed base version")
                    })?;
                let recipes =
                    crate::commit::write_stack::copy_on_write::freeze_copy_on_write_branches(
                        selection,
                        crate::commit::write_stack::copy_on_write::IcebergCowFreezeInput {
                            catalog: &self.key.instance_id,
                            namespace,
                            table_name,
                            metadata: &metadata,
                            snapshot_id,
                            base_files: self.frozen_base_data_files(table, snapshot_id)?,
                            input: &material.input,
                            base_version_digest,
                            max_handle_payload_bytes: request.context.max_handle_payload_bytes(),
                        },
                    )?;
                plan_copy_on_write_branches(&material, &recipes)?
            }
        };
        let IcebergSessionFlavorPlan {
            flavor,
            publication,
            rewrite_inputs,
            copy_on_write,
            branches,
        } = plan;
        plan_branch_session(
            IcebergWriteSessionId::new(),
            IcebergBranchSessionPlanInput {
                flavor,
                purpose: request.purpose,
                table: material.table,
                base_version_digest: request.base.as_ref().map(|base| base.digest()),
                publication,
                staged_metadata: staged.map(|staged| Arc::new(staged.metadata)),
                rewrite_inputs,
                copy_on_write,
                repartition,
                writer_table: writer_facts,
                branches,
            },
        )
    }

    /// Every live data file of one frozen base snapshot.
    ///
    /// This is the same read a distributed rewrite performs to cut its groups:
    /// a walk of the frozen snapshot's manifests, and nothing else. It opens no
    /// delete artifact and mutates nothing.
    fn frozen_base_data_files(
        &self,
        table: &crate::iceberg::table::Table,
        snapshot_id: i64,
    ) -> Result<Vec<crate::manifest::DataFileWithStats>, ConnectorError> {
        let owned = table.clone();
        self.runtime
            .resources()
            .catalog_runtime()
            .block_on(async move {
                crate::manifest::extract_data_files_with_stats_at(&owned, snapshot_id).await
            })
            .map_err(|error| unavailable(error.to_string()))?
            .map_err(unavailable)
    }

    /// Freeze the rewrite groups a distributed rewrite seals one branch each
    /// for.
    ///
    /// Grouping is the provider's decision and reuses the existing rewrite
    /// planner, so a group here is the same group the rewrite path already
    /// cuts. Everything this touches is a read of the frozen base snapshot.
    fn freeze_rewrite_groups(
        &self,
        table: &crate::iceberg::table::Table,
        base_snapshot_id: Option<i64>,
    ) -> Result<Vec<crate::distributed_rewrite::IcebergFrozenRewriteGroupV1>, ConnectorError> {
        // Without a base snapshot there is nothing to rewrite at all, and a
        // rewrite that silently became an append would republish rows it never
        // read.
        let Some(snapshot_id) = base_snapshot_id else {
            return Ok(Vec::new());
        };
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
        if files.is_empty() {
            return Ok(Vec::new());
        }
        let live = crate::distributed_rewrite::live_delete_file_paths_at(
            &self.runtime,
            table,
            snapshot_id,
        )?;
        crate::distributed_rewrite::plan_data_file_groups(files, &live)
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

/// What a publication needs the session to know.
///
/// The boundary the publication id must respect is a *destination* boundary,
/// not a "the provider never holds it" one. The session's writer recipes, its
/// commit fragments, and every backend that executes them have no use for the
/// id, and it reaches none of them.
///
/// It does reach exactly one thing, and must: the summary of the snapshot this
/// session's own commit creates. The publication fence adjudicates a staged
/// refresh by reading `MV_PUBLICATION_ID_PROP` back off that snapshot
/// (`catalog_control::catalog_mutation` -> `snapshot_matches_publication_marker`),
/// so a commit that omitted it would publish a snapshot no publication could
/// ever claim — and would do so silently, because nothing before the fence
/// looks for the property. Converting the intent's durable facts once, here, is
/// what keeps the id off the execution path while still putting it where the
/// fence looks.
pub(crate) fn publication_facts(
    intent: &ConnectorManagedPublicationIntent,
    shape: ConnectorManagedPublicationShape,
) -> Result<IcebergManagedPublicationFacts, ConnectorError> {
    let bases = intent
        .bases()
        .iter()
        .map(provenance_base_from_staged_fact)
        .collect::<Result<Vec<_>, _>>()
        .map_err(invalid)?;
    Ok(IcebergManagedPublicationFacts::new(
        intent.technique(),
        intent.empty_input(),
        shape,
        IcebergManagedPublicationProvenance::try_new(
            intent.publication_id(),
            bases,
            intent.definition_fingerprint().to_string(),
            base64::engine::general_purpose::STANDARD
                .encode(intent.descriptor_properties().digest()),
        )?,
    ))
}

/// One publication base, in the Iceberg provenance form the snapshot records.
///
/// The neutral base object identity stays opaque to everything but this
/// conversion, which is the only place that is allowed to know it is a
/// canonical Iceberg table UUID (ADR-0097).
fn provenance_base_from_staged_fact(
    base: &novarocks_spi::connector::ConnectorStagedPublicationBaseFact,
) -> Result<crate::commit::ProvenanceBase, String> {
    let uuid = std::str::from_utf8(base.object_id.as_bytes())
        .map_err(|error| format!("Iceberg base object ID is not UTF-8: {error}"))?;
    let parsed = uuid::Uuid::parse_str(uuid)
        .map_err(|error| format!("Iceberg base object ID is not a UUID: {error}"))?;
    if parsed.to_string() != uuid {
        return Err("Iceberg base object ID is not a canonical UUID".to_string());
    }
    Ok(crate::commit::ProvenanceBase {
        table_fqn: base.table.to_string(),
        uuid: uuid.to_string(),
        from_snapshot: base.from_version,
        to_snapshot: base.to_version,
    })
}

/// Whether this session will seal a delete branch, and therefore has to freeze
/// the old delete artifacts that branch must supersede.
pub(crate) fn session_freezes_old_deletes(
    flavor: &ConnectorWriteSessionFlavor,
    signed: &ConnectorWriteInputShape,
) -> bool {
    match flavor {
        ConnectorWriteSessionFlavor::Ordinary => {
            crate::commit::write_stack::flavor::ordinary_delete_branch(signed).is_some()
        }
        // A merge-on-read mutation seals a deletion-vector branch from a
        // row-lineage input, so the delete-shaped-input test is not enough here.
        ConnectorWriteSessionFlavor::RowMutation => !matches!(
            signed,
            ConnectorWriteInputShape::Data { .. } | ConnectorWriteInputShape::EqualityDelete { .. }
        ),
        // A publication that applies a change stream seals the same delete
        // branch a DML row mutation does, so it must supersede the same old
        // artifacts. One that republishes rows wholesale seals no delete branch
        // at all and has nothing to freeze.
        ConnectorWriteSessionFlavor::ManagedPublication { shape, .. } => {
            *shape == ConnectorManagedPublicationShape::RowMutation
                && !matches!(
                    signed,
                    ConnectorWriteInputShape::Data { .. }
                        | ConnectorWriteInputShape::EqualityDelete { .. }
                )
        }
        // A staged target has no base snapshot and therefore no old delete
        // artifact to supersede: it is a table nobody has ever written to. A
        // distributed rewrite and a copy-on-write mutation both seal only data
        // branches: they replace whole files rather than staging a delete
        // beside them, so there is no artifact for one to supersede either.
        ConnectorWriteSessionFlavor::StagedCreate(_)
        | ConnectorWriteSessionFlavor::DistributedRewrite
        | ConnectorWriteSessionFlavor::CopyOnWrite(_) => false,
    }
}

/// Pick the flavor an *ordinary* begin request describes.
///
/// An unsupported combination fails here rather than being coerced into the
/// nearest supported one.
fn flavor_for(request: &ConnectorWriteBeginRequest) -> Result<IcebergWriteFlavor, ConnectorError> {
    use novarocks_spi::connector::{ConnectorWriteAdmissionPurpose, ConnectorWriteIntent};
    // A staged create is decided by the flavor alone. Its intent is an append
    // and its input is data, so reading intent and input would answer
    // `Append` -- correct as far as it goes, and wrong about the one thing
    // that matters: the target does not exist yet.
    if matches!(request.flavor, ConnectorWriteSessionFlavor::StagedCreate(_)) {
        return Ok(IcebergWriteFlavor::StagedCreate);
    }
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
        // `ALTER TABLE ... ADD EQUALITY DELETE`. It appends delete files rather
        // than superseding a data file's deletes, so it commits as an ordinary
        // row delta, the same op a Parquet position-delete mutation uses.
        (ConnectorWriteIntent::RowDelta, ConnectorWriteInputRequest::EqualityDelete { .. }) => {
            IcebergWriteFlavor::EqualityDelete
        }
        (ConnectorWriteIntent::RowDelta, _) => {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "Iceberg row-delta write requires a position-delete, deletion-vector, equality-delete, or row-lineage input",
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
        ConnectorWriteInputRequest::EqualityDelete { equality_fields } => {
            ConnectorWriteInputShape::EqualityDelete {
                equality_fields: sign("equality-delete", equality_fields),
            }
        }
    };
    shape.validate()?;
    Ok(shape)
}

/// Resolve the equality-delete match key against the frozen table generation.
///
/// A column name alone is not a match key: Iceberg records the *field ids* on
/// the manifest, and a reader applies the delete by id. Resolving them here,
/// against the same metadata the session froze, is what makes a renamed or
/// absent column a refusal instead of a delete that matches nothing.
fn equality_delete_recipe(
    metadata: &TableMetadata,
    fields: &[ConnectorWriteFieldBinding],
) -> Result<crate::commit::write_stack::domain::IcebergEqualityDeleteRecipe, ConnectorError> {
    use crate::commit::write_stack::domain::{
        IcebergEqualityDeleteColumnFacts, IcebergEqualityDeleteRecipe,
    };

    // A partitioned equality delete would have to decide which partition each
    // matched row lives in without ever reading one, so it stays refused rather
    // than written into an arbitrary partition.
    if !metadata.default_partition_spec().is_unpartitioned() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "Iceberg equality-delete writer supports only unpartitioned tables",
        ));
    }
    let schema = metadata.current_schema();
    let columns = fields
        .iter()
        .map(|binding| {
            let field = binding.field();
            let iceberg_field = schema
                .as_struct()
                .fields()
                .iter()
                .find(|candidate| candidate.name.eq_ignore_ascii_case(field.name()))
                .ok_or_else(|| {
                    invalid(format!(
                        "Iceberg equality-delete field `{}` is absent from the frozen schema",
                        field.name()
                    ))
                })?;
            IcebergEqualityDeleteColumnFacts::try_new(
                field.name().to_string(),
                iceberg_field.id,
                format!("{:?}", field.data_type()),
                field.is_nullable(),
            )
        })
        .collect::<Result<Vec<_>, ConnectorError>>()?;
    IcebergEqualityDeleteRecipe::try_new(columns)
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
        // A staged create is the one flavor that finishes without committing:
        // its single external effect belongs to the publication that owns the
        // staged target, and performing one here would create a snapshot on a
        // table the catalog does not yet have.
        if handle.flavor() == IcebergWriteFlavor::StagedCreate {
            return self.seal_staged_prepared_set(
                handle,
                &request.prepared,
                &frozen,
                &request.context,
            );
        }
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
