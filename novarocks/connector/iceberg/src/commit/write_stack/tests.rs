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

//! Cross-cutting write-stack tests.
//!
//! The D10 tests read real Parquet position-delete files off a temporary local
//! filesystem so that "missing", "corrupt", and "stale" are genuine I/O
//! outcomes rather than mocked verdicts.

use std::collections::BTreeMap;
use std::fs;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_fs::{
    FileIoRuntime, FileTaskSpawner, FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner,
};
use novarocks_spi::connector::write_stack::session::{
    ConnectorWriteRouteFacts, ConnectorWriteSessionPlan, ConnectorWriteTargetPlan,
};
use novarocks_spi::connector::write_stack::{
    ConnectorPreparedWriteSet, WriteRuntimeAdapter, WriteTargetOrdinal,
};
use novarocks_spi::connector::{
    CatalogHandle, CatalogVersion, ConnectorCancellation, ConnectorCommittedVersion,
    ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor, ConnectorInstanceId,
    ConnectorManagedPublicationEmptyInputDisposition, ConnectorManagedPublicationTechnique,
    ConnectorMutationRouteInput, ConnectorProviderId, ConnectorRequestContext,
    ConnectorRowMutationEffect, ConnectorWriteAbortOutcome, ConnectorWriteAdmissionPurpose,
    ConnectorWriteInputShape, ConnectorWriteRouteId, ExternalMutationEffect,
    ExternalMutationOutcome, ProviderBindingEpoch,
};
use parquet::arrow::ArrowWriter;

use crate::access_binding::IcebergReadBinding;
use crate::commit::CommitOpKind;
use crate::commit::write_stack::control::{
    release_session_state, session_plan_from_targets, settle_empty_write_without_commit,
    validate_prepared_set,
};
use crate::commit::write_stack::domain::{
    IcebergCommitFragment, IcebergCommitHandle, IcebergDataFileArtifact, IcebergEmptyWriteDecision,
    IcebergManagedPublicationFacts, IcebergPositionDeleteFileArtifact, IcebergWriteBranch,
    IcebergWriteFlavor, IcebergWriteSessionId, IcebergWriteSessionState,
};
use crate::commit::write_stack::flavor::{
    IcebergSessionFlavorPlan, plan_distributed_rewrite_branches, plan_managed_publication_branches,
    plan_ordinary_branches, plan_row_mutation_branches,
};
use crate::commit::write_stack::old_delete::{
    IcebergOldDeleteMergeTarget, read_and_merge_old_deletes,
};
use crate::commit::write_stack::planning::{
    IcebergBranchSessionPlanInput, IcebergWriteBranchPlan, IcebergWriteSessionPlanInput,
    IcebergWriteTargetPlan, plan_branch_session, plan_write_session,
};
use crate::commit::write_stack::runtime::{IcebergWriteAdapter, IcebergWriteRuntime};
use crate::commit::write_stack::test_support::{
    binding, copy_on_write_input_shape, data_branch_plan, data_input_shape, delete_branch_plan,
    delete_input_shape, dv_artifact, merge_on_read_input_shape, merge_target, parquet_ref,
    sample_metrics, sample_partition, session_material, table_facts,
};
use crate::delete_file::IcebergFileFormat;
use crate::manifest::DataFileWithStats;
use crate::position_delete::{FILE_PATH_COLUMN, POS_COLUMN};

struct NeverCancelled;

impl ConnectorCancellation for NeverCancelled {
    fn is_cancelled(&self) -> bool {
        false
    }
}

fn request_context() -> ConnectorRequestContext {
    ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(30),
        Arc::new(NeverCancelled),
        64 * 1024,
        1024 * 1024,
    )
    .expect("request context")
}

fn descriptor(catalog: &str) -> ConnectorInstanceDescriptor {
    ConnectorInstanceDescriptor {
        provider_id: ConnectorProviderId::parse("iceberg").expect("provider id"),
        instance_id: ConnectorInstanceId::parse(catalog).expect("instance id"),
    }
}

fn adapter(catalog: &str, version: u8) -> IcebergWriteAdapter {
    let descriptor = descriptor(catalog);
    let catalog_handle = CatalogHandle::new(
        descriptor.instance_id.clone(),
        CatalogVersion::from_bytes([version; 32]),
    );
    WriteRuntimeAdapter::new(Arc::new(IcebergWriteRuntime::new(
        descriptor,
        catalog_handle,
    )))
}

fn local_binding() -> (IcebergReadBinding, tokio::runtime::Runtime) {
    let runtime = tokio::runtime::Runtime::new().expect("build Tokio runtime");
    let file_runtime: Arc<dyn FileIoRuntime> =
        Arc::new(TokioFileIoRuntime::new(runtime.handle().clone()));
    let task_spawner: Arc<dyn FileTaskSpawner> =
        Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone()));
    let binding =
        IcebergReadBinding::new(None, FsAccessResolver::new(), file_runtime, task_spawner);
    (binding, runtime)
}

fn write_delete_parquet(path: &std::path::Path, file_paths: &[&str], positions: &[i64]) -> u64 {
    let schema = Arc::new(Schema::new(vec![
        Field::new(FILE_PATH_COLUMN, DataType::Utf8, false),
        Field::new(POS_COLUMN, DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(file_paths.to_vec())),
            Arc::new(Int64Array::from(positions.to_vec())),
        ],
    )
    .expect("build delete batch");
    let file = fs::File::create(path).expect("create delete file");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("create parquet writer");
    writer.write(&batch).expect("write delete batch");
    writer.close().expect("close parquet writer");
    fs::metadata(path).expect("stat delete file").len()
}

fn location(path: &std::path::Path) -> String {
    format!("file://{}", path.display())
}

// -------------------------------------------------------------------------
// D10: the backend reads, validates, and merges the frozen references.
// -------------------------------------------------------------------------

#[test]
fn multiple_old_delete_files_for_one_data_file_are_read_and_merged() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let data_file = "/data/a.parquet";
    let first = directory.path().join("d1.parquet");
    let second = directory.path().join("d2.parquet");
    let first_size = write_delete_parquet(&first, &[data_file, data_file], &[2, 5]);
    let second_size = write_delete_parquet(&second, &[data_file, "/data/b.parquet"], &[7, 9]);

    let target = IcebergOldDeleteMergeTarget::try_new(
        data_file.to_string(),
        100,
        Some(4),
        sample_partition(),
        77,
        vec![
            parquet_ref(&location(&first), Some(data_file), first_size, 2).expect("reference"),
            // The second file is shared with another data file, so it is frozen
            // without an exclusive reference and may contribute fewer rows.
            parquet_ref(&location(&second), None, second_size, 2).expect("reference"),
        ],
    )
    .expect("merge target");

    let (binding, _runtime) = local_binding();
    let merged = read_and_merge_old_deletes(&target, &binding, &request_context())
        .expect("read and merge old deletes");
    assert_eq!(
        merged.positions().iter().collect::<Vec<_>>(),
        vec![2, 5, 7],
        "the merge is the union of every frozen reference's positions for this data file"
    );
    assert_eq!(
        merged.merged_references(),
        &[location(&first), location(&second)],
        "the artifact records exactly which references it superseded"
    );
}

#[test]
fn a_data_file_with_no_frozen_references_merges_to_nothing_without_touching_storage() {
    let target = merge_target("s3://b/a.parquet", 100, Vec::new());
    let (binding, _runtime) = local_binding();
    let merged = read_and_merge_old_deletes(&target, &binding, &request_context())
        .expect("no references is a decision, not a read");
    assert!(merged.positions().is_empty());
    assert!(merged.merged_references().is_empty());
}

#[test]
fn a_missing_old_delete_artifact_fails_closed_instead_of_reading_as_empty() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let absent = directory.path().join("gone.parquet");
    let target = IcebergOldDeleteMergeTarget::try_new(
        "/data/a.parquet".to_string(),
        100,
        None,
        sample_partition(),
        77,
        vec![parquet_ref(&location(&absent), Some("/data/a.parquet"), 512, 2).expect("reference")],
    )
    .expect("merge target");

    let (binding, _runtime) = local_binding();
    let error = read_and_merge_old_deletes(&target, &binding, &request_context())
        .expect_err("a missing artifact must fail the writer");
    assert_ne!(
        error.kind(),
        ConnectorErrorKind::Unsupported,
        "a missing artifact must not be silently absorbed"
    );
}

#[test]
fn a_stale_old_delete_artifact_is_detected_before_it_is_parsed() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("d.parquet");
    let real_size = write_delete_parquet(&path, &["/data/a.parquet"], &[3]);
    let target = IcebergOldDeleteMergeTarget::try_new(
        "/data/a.parquet".to_string(),
        100,
        None,
        sample_partition(),
        77,
        vec![
            // The frozen size no longer matches what is in storage: the
            // artifact was replaced between planning and execution.
            parquet_ref(&location(&path), Some("/data/a.parquet"), real_size + 1, 1)
                .expect("reference"),
        ],
    )
    .expect("merge target");

    let (binding, _runtime) = local_binding();
    let error = read_and_merge_old_deletes(&target, &binding, &request_context())
        .expect_err("a stale artifact must fail the writer");
    assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    assert!(error.message().contains("is stale"), "{}", error.message());
}

#[test]
fn a_corrupt_old_delete_artifact_fails_closed() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("corrupt.parquet");
    fs::write(&path, b"this is not a parquet file").expect("write corrupt file");
    let size = fs::metadata(&path).expect("stat").len();
    let target = IcebergOldDeleteMergeTarget::try_new(
        "/data/a.parquet".to_string(),
        100,
        None,
        sample_partition(),
        77,
        vec![parquet_ref(&location(&path), Some("/data/a.parquet"), size, 1).expect("reference")],
    )
    .expect("merge target");

    let (binding, _runtime) = local_binding();
    let error = read_and_merge_old_deletes(&target, &binding, &request_context())
        .expect_err("a corrupt artifact must fail the writer");
    assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
}

#[test]
fn an_exclusive_artifact_that_reads_empty_is_corruption_not_an_empty_delete_set() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("foreign.parquet");
    // The file exists and parses, but every row belongs to another data file.
    // Frozen as exclusive to `/data/a.parquet`, that is a contradiction.
    let size = write_delete_parquet(&path, &["/data/b.parquet"], &[1]);
    let target = IcebergOldDeleteMergeTarget::try_new(
        "/data/a.parquet".to_string(),
        100,
        None,
        sample_partition(),
        77,
        vec![parquet_ref(&location(&path), None, size, 0).expect("reference")],
    )
    .expect("merge target");
    // Re-freeze the same artifact as exclusive to /data/a.parquet.
    let exclusive = IcebergOldDeleteMergeTarget::try_new(
        "/data/a.parquet".to_string(),
        100,
        None,
        sample_partition(),
        77,
        vec![parquet_ref(&location(&path), Some("/data/a.parquet"), size, 0).expect("reference")],
    )
    .expect("merge target");

    let (binding, _runtime) = local_binding();
    // The shared framing is allowed to contribute nothing.
    assert!(
        read_and_merge_old_deletes(&target, &binding, &request_context())
            .expect("shared artifact")
            .positions()
            .is_empty()
    );
    // The exclusive framing is not.
    let error = read_and_merge_old_deletes(&exclusive, &binding, &request_context())
        .expect_err("an exclusive artifact must contribute at least one position");
    assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    assert!(
        error.message().contains("decoded no positions"),
        "{}",
        error.message()
    );
}

#[test]
fn a_position_outside_the_referenced_data_file_is_rejected() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("d.parquet");
    let size = write_delete_parquet(&path, &["/data/a.parquet"], &[500]);
    let target = IcebergOldDeleteMergeTarget::try_new(
        "/data/a.parquet".to_string(),
        // The data file has only 100 rows, so position 500 cannot exist.
        100,
        None,
        sample_partition(),
        77,
        vec![parquet_ref(&location(&path), Some("/data/a.parquet"), size, 1).expect("reference")],
    )
    .expect("merge target");

    let (binding, _runtime) = local_binding();
    let error = read_and_merge_old_deletes(&target, &binding, &request_context())
        .expect_err("a position past the data file must fail");
    assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    assert!(
        error.message().contains("which has only 100 rows"),
        "{}",
        error.message()
    );
}

#[test]
fn an_artifact_frozen_for_another_data_file_is_rejected_before_any_io() {
    let error = IcebergOldDeleteMergeTarget::try_new(
        "/data/a.parquet".to_string(),
        100,
        None,
        sample_partition(),
        77,
        vec![parquet_ref("s3://b/d.parquet", Some("/data/b.parquet"), 512, 1).expect("reference")],
    )
    .expect_err("mismatched reference");
    assert!(error.message().contains("belongs to data file"));
}

// -------------------------------------------------------------------------
// Domain round-trips through the provider-private adapter.
// -------------------------------------------------------------------------

#[test]
fn every_fragment_kind_round_trips_through_its_own_adapter() {
    let adapter = adapter("unit", 1);
    let data = IcebergCommitFragment::data_file(
        IcebergDataFileArtifact::try_new(
            "s3://b/data/f.parquet".to_string(),
            IcebergFileFormat::Parquet,
            sample_partition(),
            sample_metrics(10, 2048),
            Some(64),
        )
        .expect("data artifact"),
    );
    let position_delete = IcebergCommitFragment::position_delete_file(
        IcebergPositionDeleteFileArtifact::try_new(
            "s3://b/data/d.parquet".to_string(),
            sample_partition(),
            sample_metrics(3, 512),
            "s3://b/data/f.parquet".to_string(),
            vec!["s3://b/data/old.parquet".to_string()],
        )
        .expect("position delete artifact"),
    );
    let deletion_vector = IcebergCommitFragment::deletion_vector(
        dv_artifact(
            "s3://b/data/v.puffin",
            "s3://b/data/f.parquet",
            3,
            1024,
            4,
            64,
        )
        .expect("deletion vector artifact"),
    );

    for (original, branch) in [
        (data, IcebergWriteBranch::Data),
        (position_delete, IcebergWriteBranch::PositionDelete),
        (deletion_vector, IcebergWriteBranch::DeletionVector),
    ] {
        let path = original.path().to_string();
        let wrapped = adapter.wrap_commit_fragment(original);
        let recovered = adapter.commit_fragment(&wrapped).expect("recover fragment");
        assert_eq!(recovered.branch(), branch);
        assert_eq!(recovered.path(), path);
    }
}

#[test]
fn another_catalog_generation_cannot_recover_a_fragment() {
    let mine = adapter("unit", 1);
    let replacement = adapter("unit", 2);
    let wrapped = mine.wrap_commit_fragment(IcebergCommitFragment::data_file(
        IcebergDataFileArtifact::try_new(
            "s3://b/data/f.parquet".to_string(),
            IcebergFileFormat::Parquet,
            sample_partition(),
            sample_metrics(1, 64),
            None,
        )
        .expect("data artifact"),
    ));
    assert_eq!(
        replacement
            .commit_fragment(&wrapped)
            .expect_err("foreign generation")
            .kind(),
        ConnectorErrorKind::InvalidRequest
    );
}

// -------------------------------------------------------------------------
// Prepared-set validation and the single-commit latch.
// -------------------------------------------------------------------------

fn dv_session() -> (IcebergCommitHandle, IcebergWriteAdapter) {
    let (handle, _plans) = plan_write_session(
        IcebergWriteSessionId::new(),
        IcebergWriteSessionPlanInput {
            flavor: IcebergWriteFlavor::RowMutationDeletionVector,
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            table: table_facts(),
            base_version_digest: None,
            data: data_branch_plan(),
            deletes: vec![delete_branch_plan(
                IcebergWriteBranch::DeletionVector,
                vec![merge_target(
                    "s3://b/wh/db/t/data/a.parquet",
                    100,
                    vec![
                        parquet_ref("s3://b/wh/db/t/data/old.parquet", None, 512, 2)
                            .expect("reference"),
                    ],
                )],
            )],
        },
    )
    .expect("session");
    (handle, adapter("unit", 1))
}

fn ordinal(value: u32) -> WriteTargetOrdinal {
    WriteTargetOrdinal::try_new(value).expect("ordinal")
}

fn prepared(
    adapter: &IcebergWriteAdapter,
    entries: Vec<(WriteTargetOrdinal, IcebergCommitFragment)>,
    expected: &[WriteTargetOrdinal],
) -> ConnectorPreparedWriteSet {
    let wrapped = entries
        .into_iter()
        .map(|(target, fragment)| (target, adapter.wrap_commit_fragment(fragment)))
        .collect();
    ConnectorPreparedWriteSet::try_new(0, wrapped, expected).expect("prepared set")
}

fn dv_fragment(path: &str, referenced: &str, merged: Vec<String>) -> IcebergCommitFragment {
    IcebergCommitFragment::deletion_vector(
        crate::commit::write_stack::domain::IcebergDeletionVectorArtifact::try_new(
            path.to_string(),
            sample_partition(),
            sample_metrics(3, 1024),
            referenced.to_string(),
            crate::commit::write_stack::domain::IcebergContentRange::try_new(4, 64).expect("range"),
            3,
            merged,
        )
        .expect("deletion vector artifact"),
    )
}

fn data_fragment(path: &str) -> IcebergCommitFragment {
    IcebergCommitFragment::data_file(
        IcebergDataFileArtifact::try_new(
            path.to_string(),
            IcebergFileFormat::Parquet,
            sample_partition(),
            sample_metrics(5, 4096),
            None,
        )
        .expect("data artifact"),
    )
}

#[test]
fn a_zero_row_write_still_produces_a_valid_complete_prepared_set() {
    // A statement that matched nothing still reaches finish_write, and an empty
    // complete set is legal: it commits an empty snapshot rather than being
    // mistaken for a lost result.
    let (handle, adapter) = dv_session();
    let empty = prepared(&adapter, Vec::new(), &handle.expected_targets());
    let validated =
        validate_prepared_set(&handle, &adapter, &empty).expect("an empty complete set is legal");
    assert!(validated.is_empty());
    assert_eq!(empty.row_count(), 0);
}

#[test]
fn a_valid_prepared_set_passes_every_sealed_check() {
    let (handle, adapter) = dv_session();
    let set = prepared(
        &adapter,
        vec![
            (ordinal(0), data_fragment("s3://b/wh/db/t/data/new.parquet")),
            (
                ordinal(1),
                dv_fragment(
                    "s3://b/wh/db/t/data/v.puffin",
                    "s3://b/wh/db/t/data/a.parquet",
                    vec!["s3://b/wh/db/t/data/old.parquet".to_string()],
                ),
            ),
        ],
        &handle.expected_targets(),
    );
    let validated = validate_prepared_set(&handle, &adapter, &set).expect("valid set");
    assert_eq!(validated.len(), 2);
}

#[test]
fn a_fragment_whose_branch_disagrees_with_its_target_is_rejected() {
    let (handle, adapter) = dv_session();
    let set = prepared(
        &adapter,
        vec![(
            ordinal(0),
            dv_fragment(
                "s3://b/wh/db/t/data/v.puffin",
                "s3://b/wh/db/t/data/a.parquet",
                Vec::new(),
            ),
        )],
        &handle.expected_targets(),
    );
    let error = validate_prepared_set(&handle, &adapter, &set).expect_err("branch disagreement");
    assert!(
        error.message().contains("drives the data branch"),
        "{}",
        error.message()
    );
}

#[test]
fn two_delete_artifacts_for_one_data_file_are_rejected() {
    let (handle, adapter) = dv_session();
    let set = prepared(
        &adapter,
        vec![
            (
                ordinal(1),
                dv_fragment(
                    "s3://b/wh/db/t/data/v1.puffin",
                    "s3://b/wh/db/t/data/a.parquet",
                    Vec::new(),
                ),
            ),
            (
                ordinal(1),
                dv_fragment(
                    "s3://b/wh/db/t/data/v2.puffin",
                    "s3://b/wh/db/t/data/a.parquet",
                    Vec::new(),
                ),
            ),
        ],
        &handle.expected_targets(),
    );
    let error = validate_prepared_set(&handle, &adapter, &set).expect_err("duplicate delete");
    assert!(
        error.message().contains("more than one delete artifact"),
        "{}",
        error.message()
    );
}

#[test]
fn a_delete_artifact_for_an_unrouted_data_file_is_rejected() {
    let (handle, adapter) = dv_session();
    let set = prepared(
        &adapter,
        vec![(
            ordinal(1),
            dv_fragment(
                "s3://b/wh/db/t/data/v.puffin",
                "s3://b/wh/db/t/data/never-routed.parquet",
                Vec::new(),
            ),
        )],
        &handle.expected_targets(),
    );
    let error = validate_prepared_set(&handle, &adapter, &set).expect_err("unrouted data file");
    assert!(
        error.message().contains("never routed"),
        "{}",
        error.message()
    );
}

#[test]
fn a_repeated_staged_path_is_rejected() {
    let (handle, adapter) = dv_session();
    let set = prepared(
        &adapter,
        vec![
            (
                ordinal(0),
                data_fragment("s3://b/wh/db/t/data/same.parquet"),
            ),
            (
                ordinal(0),
                data_fragment("s3://b/wh/db/t/data/same.parquet"),
            ),
        ],
        &handle.expected_targets(),
    );
    let error = validate_prepared_set(&handle, &adapter, &set).expect_err("duplicate path");
    assert!(
        error.message().contains("repeats staged artifact"),
        "{}",
        error.message()
    );
}

#[test]
fn a_delete_artifact_must_supersede_exactly_the_frozen_references() {
    let (handle, adapter) = dv_session();
    let frozen = handle.frozen_old_references();
    assert_eq!(
        frozen[&ordinal(1)]["s3://b/wh/db/t/data/a.parquet"],
        vec!["s3://b/wh/db/t/data/old.parquet".to_string()]
    );

    let matching = prepared(
        &adapter,
        vec![(
            ordinal(1),
            dv_fragment(
                "s3://b/wh/db/t/data/v.puffin",
                "s3://b/wh/db/t/data/a.parquet",
                vec!["s3://b/wh/db/t/data/old.parquet".to_string()],
            ),
        )],
        &handle.expected_targets(),
    );
    let validated = validate_prepared_set(&handle, &adapter, &matching).expect("valid");
    crate::commit::write_stack::control::validate_merged_old_references(
        &handle, &frozen, &validated,
    )
    .expect("the artifact superseded exactly the frozen references");

    // A writer that merged nothing must not be committed: the old deletes would
    // silently disappear from the table.
    let dropped = prepared(
        &adapter,
        vec![(
            ordinal(1),
            dv_fragment(
                "s3://b/wh/db/t/data/v.puffin",
                "s3://b/wh/db/t/data/a.parquet",
                Vec::new(),
            ),
        )],
        &handle.expected_targets(),
    );
    let validated = validate_prepared_set(&handle, &adapter, &dropped).expect("valid shape");
    let error = crate::commit::write_stack::control::validate_merged_old_references(
        &handle, &frozen, &validated,
    )
    .expect_err("dropped old references");
    assert!(
        error.message().contains("merged 0 old references"),
        "{}",
        error.message()
    );

    // A data file whose frozen references were never superseded at all is the
    // same loss, seen from the other side.
    let missing = prepared(&adapter, Vec::new(), &handle.expected_targets());
    let validated = validate_prepared_set(&handle, &adapter, &missing).expect("valid shape");
    let error = crate::commit::write_stack::control::validate_merged_old_references(
        &handle, &frozen, &validated,
    )
    .expect_err("no artifact supersedes the frozen references");
    assert!(
        error.message().contains("staged no artifact"),
        "{}",
        error.message()
    );
}

#[test]
fn a_session_dispatches_at_most_one_snapshot_commit() {
    let (handle, _adapter) = dv_session();
    handle.begin_commit().expect("first commit attempt");
    let second = handle
        .begin_commit()
        .expect_err("a second external commit must never be dispatched");
    assert!(second.message().contains("already has a commit in flight"));

    handle
        .settle(IcebergWriteSessionState::KnownCommitted { snapshot_id: 42 })
        .expect("settle committed");
    assert!(
        handle.begin_commit().is_err(),
        "a settled session cannot start another commit"
    );
}

#[test]
fn a_commit_unknown_session_stays_unknown_through_release() {
    let (handle, _adapter) = dv_session();
    handle.begin_commit().expect("commit attempt");
    handle
        .settle(IcebergWriteSessionState::CommitUnknown {
            message: "connection reset by peer".to_string(),
            staging_dir: handle.staging_dir(),
        })
        .expect("settle unknown");

    let outcome = release_session_state(
        &descriptor("unit"),
        ProviderBindingEpoch::from_bytes([9; 16]),
        &handle,
    )
    .expect("release");
    match outcome {
        ConnectorWriteAbortOutcome::CommitUnknown { failure, evidence } => {
            assert!(failure.message().contains("connection reset by peer"));
            assert!(failure.message().contains("staged files remain at"));
            assert_eq!(
                evidence.operation_kind(),
                crate::commit::write_stack::control::ICEBERG_WRITE_SESSION_OPERATION_KIND
            );
            assert_eq!(
                evidence.operation_id().to_bytes(),
                handle.session_id().to_bytes()
            );
        }
        other => panic!("abort must not resolve an unknown outcome: {other:?}"),
    }
}

#[test]
fn a_known_committed_session_cannot_be_aborted_into_uncommitted() {
    let (handle, _adapter) = dv_session();
    handle.begin_commit().expect("commit attempt");
    handle
        .settle(IcebergWriteSessionState::KnownCommitted { snapshot_id: 7 })
        .expect("settle committed");
    let outcome = release_session_state(
        &descriptor("unit"),
        ProviderBindingEpoch::from_bytes([9; 16]),
        &handle,
    )
    .expect("release");
    assert!(matches!(
        outcome,
        ConnectorWriteAbortOutcome::KnownCommitted { .. }
    ));
}

#[test]
fn a_release_cannot_race_an_in_flight_commit() {
    let (handle, _adapter) = dv_session();
    handle.begin_commit().expect("commit attempt");
    let error = release_session_state(
        &descriptor("unit"),
        ProviderBindingEpoch::from_bytes([9; 16]),
        &handle,
    )
    .expect_err("release during commit");
    assert_eq!(error.kind(), ConnectorErrorKind::Unavailable);
}

#[test]
fn every_flavor_maps_onto_a_dense_logical_target_map() {
    let mut seen: BTreeMap<IcebergWriteFlavor, usize> = BTreeMap::new();
    for flavor in [
        IcebergWriteFlavor::Append,
        IcebergWriteFlavor::Overwrite,
        IcebergWriteFlavor::PartitionOverwrite,
        IcebergWriteFlavor::RowMutationCopyOnWrite,
        IcebergWriteFlavor::StagedCreate,
        IcebergWriteFlavor::ManagedPublication,
        IcebergWriteFlavor::DistributedRewrite,
        IcebergWriteFlavor::TableMaintenance,
    ] {
        let (handle, plans) = plan_write_session(
            IcebergWriteSessionId::new(),
            IcebergWriteSessionPlanInput {
                flavor,
                purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
                table: table_facts(),
                base_version_digest: None,
                data: data_branch_plan(),
                deletes: Vec::new(),
            },
        )
        .unwrap_or_else(|error| panic!("{} must plan: {error:?}", flavor.as_str()));
        assert_eq!(plans.len(), 1);
        assert_eq!(handle.expected_targets(), vec![ordinal(0)]);
        seen.insert(flavor, plans.len());
    }
    for (flavor, branch) in [
        (
            IcebergWriteFlavor::RowMutationPositionDelete,
            IcebergWriteBranch::PositionDelete,
        ),
        (
            IcebergWriteFlavor::RowMutationDeletionVector,
            IcebergWriteBranch::DeletionVector,
        ),
    ] {
        let (handle, plans) = plan_write_session(
            IcebergWriteSessionId::new(),
            IcebergWriteSessionPlanInput {
                flavor,
                purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
                table: table_facts(),
                base_version_digest: None,
                data: data_branch_plan(),
                deletes: vec![delete_branch_plan(
                    branch,
                    vec![merge_target(
                        "s3://b/wh/db/t/data/a.parquet",
                        10,
                        Vec::new(),
                    )],
                )],
            },
        )
        .unwrap_or_else(|error| panic!("{} must plan: {error:?}", flavor.as_str()));
        assert_eq!(plans.len(), 2);
        assert_eq!(handle.branch_of(ordinal(1)), Some(branch));
        seen.insert(flavor, plans.len());
    }
    assert_eq!(seen.len(), 10, "every flavor must be covered");
}

// -------------------------------------------------------------------------
// Session flavors: how each one plans its logical branches.
// -------------------------------------------------------------------------

fn flavor_session(
    plan: IcebergSessionFlavorPlan,
) -> (IcebergCommitHandle, Vec<IcebergWriteTargetPlan>) {
    plan_branch_session(
        IcebergWriteSessionId::new(),
        IcebergBranchSessionPlanInput {
            flavor: plan.flavor,
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            table: table_facts(),
            base_version_digest: None,
            publication: plan.publication,
            branches: plan.branches,
        },
    )
    .expect("seal the flavor's branches")
}

/// Take the neutral session plan the frontend actually receives, so a route the
/// provider attached is asserted where SQL would read it.
fn neutral_plan(
    adapter: &IcebergWriteAdapter,
    sealed: (IcebergCommitHandle, Vec<IcebergWriteTargetPlan>),
) -> Result<ConnectorWriteSessionPlan, ConnectorError> {
    let (handle, targets) = sealed;
    session_plan_from_targets(adapter, handle, targets)
}

fn effects(target: &ConnectorWriteTargetPlan) -> Vec<ConnectorRowMutationEffect> {
    target
        .route()
        .expect("branch is routed")
        .accepted_effects()
        .to_vec()
}

#[test]
fn an_ordinary_session_yields_unrouted_targets() {
    // An ordinary write has exactly one thing to do with every row it is
    // given, so nothing needs routing, and the branch structure is the one this
    // stack has always sealed.
    let adapter = adapter("ordinary", 1);
    let plan = plan_ordinary_branches(
        IcebergWriteFlavor::Append,
        &session_material(data_input_shape()),
    )
    .expect("plan an ordinary append");
    let sealed = neutral_plan(&adapter, flavor_session(plan)).expect("neutral plan");
    assert_eq!(sealed.targets().len(), 1);
    assert!(
        sealed
            .targets()
            .iter()
            .all(|target| target.route().is_none())
    );

    let mut material = session_material(delete_input_shape(IcebergWriteBranch::DeletionVector));
    material.merge_targets = vec![merge_target(
        "s3://b/wh/db/t/data/a.parquet",
        10,
        Vec::new(),
    )];
    let plan = plan_ordinary_branches(IcebergWriteFlavor::RowMutationDeletionVector, &material)
        .expect("plan an ordinary row-level write");
    let sealed = neutral_plan(&adapter, flavor_session(plan)).expect("neutral plan");
    assert_eq!(sealed.targets().len(), 2);
    assert!(
        sealed
            .targets()
            .iter()
            .all(|target| target.route().is_none())
    );
}

#[test]
fn a_row_mutation_yields_one_routed_target_per_branch() {
    // A merge-on-read mutation carries both halves of a change event in one
    // row: the delete branch consumes the before-image identity and the data
    // branch consumes the after-image values, so a Replace reaches both.
    let adapter = adapter("row_mutation", 2);
    let mut material = session_material(merge_on_read_input_shape());
    material.merge_targets = vec![merge_target(
        "s3://b/wh/db/t/data/a.parquet",
        10,
        Vec::new(),
    )];
    let plan = plan_row_mutation_branches(&material).expect("plan a merge-on-read mutation");
    assert_eq!(plan.flavor, IcebergWriteFlavor::RowMutationDeletionVector);
    let (handle, targets) = flavor_session(plan);
    assert_eq!(handle.branch_of(ordinal(0)), Some(IcebergWriteBranch::Data));
    assert_eq!(
        handle.branch_of(ordinal(1)),
        Some(IcebergWriteBranch::DeletionVector)
    );

    let sealed = neutral_plan(&adapter, (handle, targets)).expect("neutral plan");
    assert_eq!(sealed.expected_targets(), vec![ordinal(0), ordinal(1)]);
    assert!(
        sealed
            .targets()
            .iter()
            .all(|target| target.route().is_some())
    );
    assert_eq!(
        effects(&sealed.targets()[0]),
        vec![
            ConnectorRowMutationEffect::Replace,
            ConnectorRowMutationEffect::Insert
        ]
    );
    assert_eq!(
        effects(&sealed.targets()[1]),
        vec![
            ConnectorRowMutationEffect::Delete,
            ConnectorRowMutationEffect::Replace
        ]
    );
    // Two branches cannot share a route key, and the ordinals are dense.
    assert_ne!(
        sealed.targets()[0].route().expect("routed").route_id(),
        sealed.targets()[1].route().expect("routed").route_id()
    );
    // Each branch reads its own columns out of the one input row: the data
    // branch the two after-image values, the delete branch the two identity
    // columns that follow them.
    let positions = |target: &ConnectorWriteTargetPlan| {
        target
            .route()
            .expect("routed")
            .input_ordinals()
            .iter()
            .map(ConnectorMutationRouteInput::input_ordinal)
            .collect::<Vec<_>>()
    };
    assert_eq!(positions(&sealed.targets()[0]), vec![0, 1]);
    assert_eq!(positions(&sealed.targets()[1]), vec![2, 3]);
}

#[test]
fn a_delete_only_row_mutation_seals_one_routed_delete_branch() {
    // A deletion-vector input carries no after-image half, so the mutation
    // needs exactly one branch, and that branch accepts only a delete. Its
    // partition source fields travel as routing facts because SQL, not the
    // writer, supplies them.
    let adapter = adapter("delete_only", 3);
    let mut material = session_material(ConnectorWriteInputShape::DeletionVector {
        identity_fields: vec![
            binding("_file", 2, DataType::Utf8),
            binding("_pos", 3, DataType::Int64),
        ],
        partition_source_fields: vec![binding("k1", 1, DataType::Int64)],
    });
    material.merge_targets = vec![merge_target(
        "s3://b/wh/db/t/data/a.parquet",
        10,
        Vec::new(),
    )];
    let plan = plan_row_mutation_branches(&material).expect("plan a delete-only mutation");
    let sealed = neutral_plan(&adapter, flavor_session(plan)).expect("neutral plan");
    assert_eq!(sealed.expected_targets(), vec![ordinal(0)]);
    assert_eq!(
        effects(&sealed.targets()[0]),
        vec![ConnectorRowMutationEffect::Delete]
    );
    assert_eq!(
        sealed.targets()[0]
            .route()
            .expect("routed")
            .partition_fields()
            .len(),
        1
    );
}

#[test]
fn a_copy_on_write_row_mutation_seals_one_rewriting_data_branch() {
    // A copy-on-write mutation rewrites whole files, so its one branch sees
    // every change event that touches a file it rewrites.
    let adapter = adapter("copy_on_write", 4);
    let plan = plan_row_mutation_branches(&session_material(copy_on_write_input_shape()))
        .expect("plan a copy-on-write mutation");
    assert_eq!(plan.flavor, IcebergWriteFlavor::RowMutationCopyOnWrite);
    let sealed = neutral_plan(&adapter, flavor_session(plan)).expect("neutral plan");
    assert_eq!(sealed.expected_targets(), vec![ordinal(0)]);
    assert_eq!(
        effects(&sealed.targets()[0]),
        vec![
            ConnectorRowMutationEffect::Delete,
            ConnectorRowMutationEffect::Replace,
            ConnectorRowMutationEffect::Insert
        ]
    );
}

#[test]
fn a_row_mutation_whose_branches_share_a_route_key_is_refused() {
    // Two branches sharing a route key would make the router's choice
    // ambiguous, and the loser's rows would vanish. The provider's own route
    // key is derived per branch and never collides, so the refusal is proven by
    // sealing a mutation whose two branches were given the same key.
    let adapter = adapter("route_collision", 5);
    let collided = ConnectorWriteRouteFacts::try_new(
        ConnectorWriteRouteId::from_bytes([9; 32]),
        vec![ConnectorRowMutationEffect::Delete],
        Vec::new(),
        Vec::new(),
    )
    .expect("route facts");
    let sealed = plan_branch_session(
        IcebergWriteSessionId::new(),
        IcebergBranchSessionPlanInput {
            flavor: IcebergWriteFlavor::RowMutationDeletionVector,
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            table: table_facts(),
            base_version_digest: None,
            publication: None,
            branches: vec![
                IcebergWriteBranchPlan::Data {
                    plan: data_branch_plan(),
                    route: Some(collided.clone()),
                },
                IcebergWriteBranchPlan::Delete {
                    plan: delete_branch_plan(
                        IcebergWriteBranch::DeletionVector,
                        vec![merge_target(
                            "s3://b/wh/db/t/data/a.parquet",
                            10,
                            Vec::new(),
                        )],
                    ),
                    route: Some(collided),
                },
            ],
        },
    )
    .expect("seal two branches");
    let error = neutral_plan(&adapter, sealed).expect_err("duplicate route key");
    assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    assert!(
        error.message().contains("repeats a row-mutation route key"),
        "unexpected message: {}",
        error.message()
    );
}

/// Cut real rewrite groups, one per partition key, through the rewrite
/// planner the rewrite path already uses.
fn rewrite_groups(
    partitions: &[&str],
) -> Vec<crate::distributed_rewrite::IcebergFrozenRewriteGroupV1> {
    let files = partitions
        .iter()
        .map(|partition| DataFileWithStats {
            path: format!("s3://b/wh/db/t/data/{partition}/f.parquet"),
            size: 1,
            record_count: Some(1),
            column_stats: None,
            partition_spec_id: Some(0),
            partition_key: Some((*partition).to_string()),
            partition_values: None,
            manifest_path: None,
            partition_field_values: Vec::new(),
            first_row_id: None,
            data_sequence_number: Some(4),
            delete_files: Vec::new(),
        })
        .collect::<Vec<_>>();
    crate::distributed_rewrite::plan_data_file_groups(files, &std::collections::BTreeSet::new())
        .expect("plan rewrite groups")
}

#[test]
fn a_distributed_rewrite_yields_one_target_per_rewrite_group() {
    let adapter = adapter("rewrite", 6);
    let groups = rewrite_groups(&["a", "b", "c"]);
    assert_eq!(groups.len(), 3);
    let plan = plan_distributed_rewrite_branches(&session_material(data_input_shape()), &groups)
        .expect("plan a distributed rewrite");
    assert_eq!(plan.flavor, IcebergWriteFlavor::DistributedRewrite);
    let (handle, targets) = flavor_session(plan);
    assert_eq!(
        handle.expected_targets(),
        vec![ordinal(0), ordinal(1), ordinal(2)]
    );
    assert!(
        handle
            .targets()
            .iter()
            .all(|target| target.branch() == IcebergWriteBranch::Data)
    );
    let sealed = neutral_plan(&adapter, (handle, targets)).expect("neutral plan");
    assert_eq!(sealed.targets().len(), 3);
    // A rewrite routes nothing: it does not split rows by change event.
    assert!(
        sealed
            .targets()
            .iter()
            .all(|target| target.route().is_none())
    );
}

#[test]
fn a_rewrite_is_not_gated_by_the_external_write_fence() {
    // The rewrite is arbitrated by the ordinary Iceberg base-state compare and
    // swap `dispatch_commit` already performs against the frozen snapshot, so
    // it must not also take the distributed external write fence. Every other
    // flavor keeps it.
    for flavor in [
        IcebergWriteFlavor::Append,
        IcebergWriteFlavor::Overwrite,
        IcebergWriteFlavor::PartitionOverwrite,
        IcebergWriteFlavor::RowMutationPositionDelete,
        IcebergWriteFlavor::RowMutationDeletionVector,
        IcebergWriteFlavor::RowMutationCopyOnWrite,
        IcebergWriteFlavor::StagedCreate,
        IcebergWriteFlavor::ManagedPublication,
        IcebergWriteFlavor::TableMaintenance,
    ] {
        assert!(
            flavor.requires_external_write_fence(),
            "{} must keep the fence",
            flavor.as_str()
        );
    }
    assert!(!IcebergWriteFlavor::DistributedRewrite.requires_external_write_fence());

    let plan = plan_distributed_rewrite_branches(
        &session_material(data_input_shape()),
        &rewrite_groups(&["a"]),
    )
    .expect("plan a distributed rewrite");
    let (handle, _) = flavor_session(plan);
    assert!(!handle.requires_external_write_fence());
    // The compare-and-swap input the commit dispatch checks the loaded table
    // against, and the rewrite commit action it dispatches.
    assert_eq!(handle.table().base_snapshot_id(), Some(77));
    assert_eq!(handle.commit_op_kind(), CommitOpKind::SelectedRewrite);
}

#[test]
fn a_managed_publication_carries_its_technique_and_disposition_into_finish() {
    // A full refresh republishes the whole target, so it must commit as a
    // replacement; an incremental one adds to what is live.
    for (technique, op_kind) in [
        (
            ConnectorManagedPublicationTechnique::Full,
            CommitOpKind::Overwrite,
        ),
        (
            ConnectorManagedPublicationTechnique::Incremental,
            CommitOpKind::FastAppend,
        ),
    ] {
        let plan = plan_managed_publication_branches(
            &session_material(data_input_shape()),
            IcebergManagedPublicationFacts::new(
                technique,
                ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
            ),
        )
        .expect("plan a managed publication");
        let (handle, _) = flavor_session(plan);
        let publication = handle
            .publication()
            .expect("publication facts reach finish");
        assert_eq!(publication.technique(), technique);
        assert_eq!(
            publication.empty_input(),
            ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite
        );
        assert_eq!(handle.commit_op_kind(), op_kind);
    }
}

#[test]
fn an_empty_prepared_set_commits_or_aborts_by_the_publication_disposition() {
    // The decision is the caller's declared disposition, not an inference from
    // "there were no fragments": a zero-row INSERT reaches finish exactly the
    // same way and still publishes.
    let (append, _) = flavor_session(
        plan_ordinary_branches(
            IcebergWriteFlavor::Append,
            &session_material(data_input_shape()),
        )
        .expect("plan an ordinary append"),
    );
    assert_eq!(
        append.empty_write_decision(),
        IcebergEmptyWriteDecision::Commit
    );

    let publication = |empty_input| {
        flavor_session(
            plan_managed_publication_branches(
                &session_material(data_input_shape()),
                IcebergManagedPublicationFacts::new(
                    ConnectorManagedPublicationTechnique::Full,
                    empty_input,
                ),
            )
            .expect("plan a managed publication"),
        )
        .0
    };

    let commits = publication(ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite);
    assert_eq!(
        commits.empty_write_decision(),
        IcebergEmptyWriteDecision::Commit
    );

    let aborts =
        publication(ConnectorManagedPublicationEmptyInputDisposition::AbortWithoutExternalCommit);
    assert_eq!(
        aborts.empty_write_decision(),
        IcebergEmptyWriteDecision::SkipExternalCommit
    );
    // Terminating without an external commit needs no catalog at all: every
    // fact it reports was frozen at begin.
    let outcome = settle_empty_write_without_commit(&aborts).expect("settle the empty write");
    match outcome {
        ExternalMutationOutcome::KnownCommitted {
            effect, receipt, ..
        } => {
            assert_eq!(effect, ExternalMutationEffect::NoOp);
            assert_eq!(
                receipt
                    .committed_version()
                    .and_then(ConnectorCommittedVersion::snapshot_id),
                Some(77),
                "the target keeps the head the session froze"
            );
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    // A rewrite with nothing to rewrite is the same no-op, and it is decided by
    // the flavor rather than by a publication disposition.
    let (rewrite, _) = flavor_session(
        plan_distributed_rewrite_branches(&session_material(data_input_shape()), &[])
            .expect("plan an empty distributed rewrite"),
    );
    assert_eq!(
        rewrite.empty_write_decision(),
        IcebergEmptyWriteDecision::SkipExternalCommit
    );
}

fn _assert_error_is_send_sync(error: ConnectorError) -> impl Send + Sync {
    error
}
