// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! One immutable operation, fresh preparation per definite catalog conflict.

pub(crate) mod observation;
pub(crate) mod prepared;

use std::collections::{BTreeSet, HashMap};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorMutationFailure, ConnectorMutationFailureKind,
};

use super::dependency::{self, ValidationInputs, Verdict};
use super::model::{
    ArtifactClass, ArtifactWriter, CleanupScope, FrozenRequest, IcebergCleanupReport,
    ObjectIdentity, OperationIntent, PublicationOutcome, PublicationReport, RequestShape,
};
use super::operation::{IcebergCommitAttempt, IcebergCommitOperation};
use super::staging::{PreparedChange, Preparer, StagingBase, StagingEngine};
use crate::catalog::error::{CatalogCommitEvidence, CatalogOutcome, uncommitted_failure_kind};
use crate::catalog::transaction::CommitProof;
use crate::iceberg::spec::TableProperties;
use crate::iceberg::{Error, ErrorKind, Result};

pub(crate) type Report = PublicationReport<CommitProof, CatalogCommitEvidence>;

#[async_trait]
pub(crate) trait Publisher: Send + Sync {
    /// Read the authoritative target; no cached source snapshot is a retry base.
    async fn load_target(&self, attempt: &IcebergCommitAttempt) -> Result<StagingBase>;
    /// Encode and bound the complete recovery ledger before publication becomes
    /// possible. Failure here is definitely undispatched.
    fn preflight_recovery(
        &self,
        request: &FrozenRequest,
        operation: &IcebergCommitOperation,
    ) -> Result<()>;
    /// Consume one complete request and issue at most one external mutation.
    async fn dispatch_once(&self, request: FrozenRequest) -> CatalogOutcome<CommitProof>;
}

/// The retained OCC entrance has one direct catalog dispatch per attempt.
/// It shares staging and retry with owner publication without changing topology.
pub(crate) type RecoveryPreflight =
    std::sync::Arc<dyn Fn(&FrozenRequest, &IcebergCommitOperation) -> Result<()> + Send + Sync>;

#[derive(Clone, Copy, Debug)]
pub(crate) struct RetryPolicy {
    retries: u32,
    min_wait: Duration,
    max_wait: Duration,
    total_timeout: Duration,
}

impl RetryPolicy {
    /// Do not parse unrelated table properties: e.g. file-format options do not own retry.
    pub(crate) fn from_properties(properties: &HashMap<String, String>) -> Result<Self> {
        fn parse<T: std::str::FromStr>(
            p: &HashMap<String, String>,
            key: &str,
            default: T,
        ) -> Result<T> {
            p.get(key).map_or(Ok(default), |value| {
                value.parse().map_err(|_| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid value for {key}: {value}"),
                    )
                })
            })
        }
        let retries = parse(
            properties,
            TableProperties::PROPERTY_COMMIT_NUM_RETRIES,
            TableProperties::PROPERTY_COMMIT_NUM_RETRIES_DEFAULT as u32,
        )?;
        let min_wait = Duration::from_millis(parse(
            properties,
            TableProperties::PROPERTY_COMMIT_MIN_RETRY_WAIT_MS,
            TableProperties::PROPERTY_COMMIT_MIN_RETRY_WAIT_MS_DEFAULT,
        )?);
        let max_wait = Duration::from_millis(parse(
            properties,
            TableProperties::PROPERTY_COMMIT_MAX_RETRY_WAIT_MS,
            TableProperties::PROPERTY_COMMIT_MAX_RETRY_WAIT_MS_DEFAULT,
        )?);
        let total_timeout = Duration::from_millis(parse(
            properties,
            TableProperties::PROPERTY_COMMIT_TOTAL_RETRY_TIME_MS,
            TableProperties::PROPERTY_COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT,
        )?);
        if min_wait > max_wait || total_timeout.is_zero() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Invalid Iceberg commit retry time bounds",
            ));
        }
        Ok(Self {
            retries,
            min_wait,
            max_wait,
            total_timeout,
        })
    }

    pub(crate) const fn retries(self) -> u32 {
        self.retries
    }
    pub(crate) const fn total_timeout(self) -> Duration {
        self.total_timeout
    }
    pub(crate) fn backoff(self, retry: u32) -> Duration {
        let ceiling = self.backoff_ceiling(retry);
        // The property lower/upper bounds remain hard bounds. Jitter does not
        // grant extra retry time or affect an already-dispatched request.
        let millis = rand::Rng::gen_range(
            &mut rand::thread_rng(),
            self.min_wait.as_millis()..=ceiling.as_millis(),
        );
        Duration::from_millis(
            u64::try_from(millis).expect("retry bounds originated as u64 milliseconds"),
        )
    }
    fn backoff_ceiling(self, retry: u32) -> Duration {
        self.min_wait
            .saturating_mul(1u32.checked_shl(retry).unwrap_or(u32::MAX))
            .min(self.max_wait)
    }
}

/// Neither cancellation nor elapsed retry time interrupts an already-issued dispatch.
/// Once publication is possible, its typed outcome alone decides cleanup and retry.
// Design: ADR-0171 (docs/adr/ADR-0171-commit-operation-model.md)
pub(crate) async fn run(
    operation: &IcebergCommitOperation,
    intent: &OperationIntent,
    prefix: &PreparedChange,
    preparers: &[&dyn Preparer],
    publisher: &dyn Publisher,
    policy: RetryPolicy,
) -> Report {
    if operation.token() != intent.token() {
        return rejected(
            operation,
            ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::InvalidRequest,
                "Commit operation and intent identities differ",
            ),
        )
        .await;
    }
    let started = Instant::now();
    let deadline = started
        .checked_add(policy.total_timeout)
        .map_or(operation.request_context().deadline(), |deadline| {
            deadline.min(operation.request_context().deadline())
        });
    if let Err(error) = operation.bind_preparation_deadline(deadline) {
        return rejected(operation, before_dispatch_failure(&error)).await;
    }
    let mut attempt_number = 0;
    loop {
        // This segment covers only preparation. Returning from the helper
        // releases its observation guard before dispatch or rejected cleanup.
        let prepared = operation
            .runtime()
            .observe_preparation_async(
                async {
                    check_window(operation, started, policy)
                        .map_err(|error| before_dispatch_failure(&error))?;
                    let attempt = operation
                        .begin_attempt()
                        .map_err(|error| before_dispatch_failure(&error))?;
                    let base = publisher
                        .load_target(&attempt)
                        .await
                        .map_err(|error| before_dispatch_failure(&error))?;
                    check_window(operation, started, policy)
                        .map_err(|error| before_dispatch_failure(&error))?;
                    let (request, retained) =
                        prepare(&attempt, intent, base, prefix, preparers).await?;
                    check_window(operation, started, policy)
                        .map_err(|error| before_dispatch_failure(&error))?;
                    publisher
                        .preflight_recovery(&request, operation)
                        .map_err(|error| before_dispatch_failure(&error))?;
                    check_window(operation, started, policy)
                        .map_err(|error| before_dispatch_failure(&error))?;
                    Ok::<_, ConnectorMutationFailure>((attempt, request, retained))
                },
                |failure| failure.clone(),
            )
            .await;
        let (attempt, request, retained) = match prepared {
            Ok(prepared) => prepared,
            Err(failure) => return rejected(operation, failure).await,
        };
        match publisher.dispatch_once(request).await {
            CatalogOutcome::KnownCommitted { receipt, .. } => {
                return Report {
                    publication: PublicationOutcome::Committed(receipt),
                    cleanup: operation.cleanup_after_commit(&retained).await,
                };
            }
            CatalogOutcome::CommitUnknown { failure, evidence } => {
                // Preserve every owned object, including prior attempts with partial cleanup.
                return Report {
                    publication: PublicationOutcome::Unknown {
                        failure,
                        evidence,
                        operation: operation.token(),
                        artifacts: operation.recovery_artifacts(),
                    },
                    cleanup: IcebergCleanupReport::NotAttempted,
                };
            }
            CatalogOutcome::KnownUncommitted { failure }
                if failure.kind() == ConnectorMutationFailureKind::Conflict
                    && intent.shape() != RequestShape::Create
                    && attempt_number < policy.retries =>
            {
                // An unsuccessful bounded cleanup remains in the ledger; it never permits path reuse.
                let _ = operation
                    .cleanup(CleanupScope::ThisAttempt(attempt.attempt_token()))
                    .await;
                let wait = policy.backoff(attempt_number);
                let left = policy.total_timeout.saturating_sub(started.elapsed());
                if wait >= left {
                    return rejected(operation, failure).await;
                }
                // Conflict cleanup is complete before this waiting segment;
                // a stopped backoff remains a typed pre-dispatch failure.
                if let Err(error) = operation
                    .runtime()
                    .observe_preparation_async(operation.pause(wait), before_dispatch_failure)
                    .await
                {
                    return rejected(operation, before_dispatch_failure(&error)).await;
                }
                attempt_number += 1;
            }
            CatalogOutcome::KnownUncommitted { failure } => {
                return rejected(operation, failure).await;
            }
            CatalogOutcome::Unsupported(error) => {
                return rejected(
                    operation,
                    ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unsupported,
                        error.message(),
                    ),
                )
                .await;
            }
        }
    }
}

fn check_window(
    operation: &IcebergCommitOperation,
    start: Instant,
    policy: RetryPolicy,
) -> Result<()> {
    operation.check_active()?;
    if start.elapsed() >= policy.total_timeout {
        return Err(Error::new(
            ErrorKind::Unexpected,
            "Iceberg commit retry deadline exceeded",
        )
        .with_source(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "Iceberg commit retry deadline exceeded",
        )));
    }
    Ok(())
}

async fn prepare(
    attempt: &IcebergCommitAttempt,
    intent: &OperationIntent,
    base: StagingBase,
    prefix: &PreparedChange,
    preparers: &[&dyn Preparer],
) -> std::result::Result<(FrozenRequest, Vec<ObjectIdentity>), ConnectorMutationFailure> {
    if let StagingBase::Existing { metadata, .. } = &base {
        if intent.target().uuid != Some(metadata.uuid()) {
            return Err(ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::Conflict,
                "Iceberg commit target table UUID was replaced",
            ));
        }
        let parent = metadata
            .snapshot_for_ref(intent.target_ref())
            .map(|s| s.snapshot_id());
        match dependency::validate(intent, metadata, parent, attempt)
            .await
            .map_err(|e| before_dispatch_failure(&e))?
        {
            Verdict::Holds => {}
            verdict => {
                return Err(ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Conflict,
                    format!("Iceberg commit dependency failed: {verdict:?}"),
                ));
            }
        }
    }
    let mut engine =
        StagingEngine::begin(base, intent, attempt).map_err(|e| before_dispatch_failure(&e))?;
    engine
        .stage_change(prefix.clone())
        .map_err(|e| before_dispatch_failure(&e))?;
    for preparer in preparers {
        engine
            .stage(*preparer)
            .await
            .map_err(|e| before_dispatch_failure(&e))?;
    }
    // New DV coalescing may supersede a BE Puffin. Retain physical reachability,
    // not all original added paths; a carried blob can keep a shared object live.
    let ledger = attempt
        .operation_artifacts()
        .map_err(|e| before_dispatch_failure(&e))?;
    let cross_attempt: BTreeSet<_> = ledger
        .iter()
        .filter(|r| {
            matches!(
                r.class,
                ArtifactClass::Operation | ArtifactClass::SessionData
            )
        })
        .map(|r| r.object.clone())
        .collect();
    let mut references: BTreeSet<_> = attempt
        .publication_references()
        .map_err(|e| before_dispatch_failure(&e))?
        .into_iter()
        .collect();
    if !cross_attempt.is_empty() {
        // Every snapshot published in this request remains readable by time travel,
        // even when a later stage removes its files from the final head.
        let mut snapshots: BTreeSet<_> = engine
            .updates()
            .iter()
            .filter_map(|update| match update {
                crate::iceberg::TableUpdate::AddSnapshot { snapshot } => {
                    Some(snapshot.snapshot_id())
                }
                _ => None,
            })
            .collect();
        snapshots.extend(
            engine
                .metadata()
                .snapshot_for_ref(intent.target_ref())
                .map(|s| s.snapshot_id()),
        );
        for snapshot in snapshots {
            // Data live sets omit manifest objects. Freeze the exact physical
            // roots of every AddSnapshot, including intermediate snapshots.
            let snapshot_ref = engine.metadata().snapshot_by_id(snapshot).ok_or_else(|| {
                ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::InvalidRequest,
                    "Staged publication snapshot is missing",
                )
            })?;
            let list = crate::manifest::load_manifest_list(
                snapshot_ref,
                attempt.file_io(),
                engine.metadata(),
            )
            .await
            .map_err(|error| before_dispatch_failure(&error))?;
            for path in std::iter::once(snapshot_ref.manifest_list()).chain(
                list.entries()
                    .iter()
                    .map(|manifest| manifest.manifest_path.as_str()),
            ) {
                let object =
                    ObjectIdentity::new(path).map_err(|error| before_dispatch_failure(&error))?;
                if cross_attempt.contains(&object) {
                    references.insert(object);
                }
            }
            let mut inputs = ValidationInputs::new(engine.metadata(), Some(snapshot), attempt);
            for identity in inputs
                .live_set()
                .await
                .map_err(|e| before_dispatch_failure(&e))?
                .keys()
            {
                let object = identity.object();
                if cross_attempt.contains(&object) {
                    references.insert(object);
                }
            }
        }
    }
    let request = engine
        .freeze(&references.into_iter().collect::<Vec<_>>())
        .map_err(|e| before_dispatch_failure(&e))?;
    let mut retained = request.artifacts().attempt_owned().to_vec();
    retained.extend_from_slice(request.artifacts().operation_references());
    retained.extend_from_slice(request.artifacts().session_references());
    Ok((request, retained))
}

pub(crate) async fn rejected(
    operation: &IcebergCommitOperation,
    failure: ConnectorMutationFailure,
) -> Report {
    Report {
        publication: PublicationOutcome::KnownUncommitted(failure),
        cleanup: operation.cleanup(CleanupScope::EntireOperation).await,
    }
}

/// The error's typed source preserves cancellation/deadline through the format adapter.
/// Preparation and load failures are proven undispatched regardless of their wording.
pub(crate) fn before_dispatch_failure(error: &Error) -> ConnectorMutationFailure {
    use std::error::Error as _;
    let mut source = error.source();
    while let Some(cause) = source {
        if let Some(error) = cause.downcast_ref::<ConnectorError>() {
            let kind = match error.kind() {
                ConnectorErrorKind::Cancelled => ConnectorMutationFailureKind::Cancelled,
                ConnectorErrorKind::DeadlineExceeded => {
                    ConnectorMutationFailureKind::DeadlineExceeded
                }
                ConnectorErrorKind::InvalidRequest => ConnectorMutationFailureKind::InvalidRequest,
                ConnectorErrorKind::NotFound => ConnectorMutationFailureKind::NotFound,
                ConnectorErrorKind::PermissionDenied => {
                    ConnectorMutationFailureKind::PermissionDenied
                }
                ConnectorErrorKind::Unsupported => ConnectorMutationFailureKind::Unsupported,
                ConnectorErrorKind::ResourceExhausted => {
                    ConnectorMutationFailureKind::ResourceExhausted
                }
                ConnectorErrorKind::Unavailable => ConnectorMutationFailureKind::Unavailable,
                ConnectorErrorKind::CorruptData => ConnectorMutationFailureKind::CorruptData,
                ConnectorErrorKind::Internal => ConnectorMutationFailureKind::Internal,
            };
            return ConnectorMutationFailure::new(kind, error.to_string());
        }
        source = cause.source();
    }
    ConnectorMutationFailure::new(uncommitted_failure_kind(error), error.to_string())
}

#[cfg(test)]
mod tests {
    use super::super::fast_append::FastAppendPreparer;
    use super::super::model::{
        AddedContent, Dependency, FileChanges, IsolationLevel, OperationIntentParts, TableTarget,
    };
    use super::super::overwrite::preparer_tests::{Fixture, data};
    use super::*;
    use crate::iceberg::TableUpdate;
    use crate::iceberg::spec::{FormatVersion, Struct, TableMetadata};
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    enum Dispatch {
        Commit,
        ConflictThen(TableMetadata),
        Unknown,
        AlwaysConflict,
        CancelAfterCommit,
    }
    struct ControlledPublisher<'a> {
        fixture: &'a Fixture,
        metadata: Mutex<TableMetadata>,
        dispatch: Dispatch,
        cancel_after_load: bool,
        load_delay: Duration,
        recovery_refused: bool,
        loads: AtomicUsize,
        requests: Mutex<Vec<serde_json::Value>>,
    }
    impl<'a> ControlledPublisher<'a> {
        fn new(fixture: &'a Fixture, metadata: TableMetadata, dispatch: Dispatch) -> Self {
            Self {
                fixture,
                metadata: Mutex::new(metadata),
                dispatch,
                cancel_after_load: false,
                load_delay: Duration::ZERO,
                recovery_refused: false,
                loads: AtomicUsize::new(0),
                requests: Mutex::new(Vec::new()),
            }
        }
    }
    #[async_trait]
    impl Publisher for ControlledPublisher<'_> {
        async fn load_target(&self, _: &IcebergCommitAttempt) -> Result<StagingBase> {
            self.loads.fetch_add(1, Ordering::SeqCst);
            if !self.load_delay.is_zero() {
                tokio::time::sleep(self.load_delay).await;
            }
            if self.cancel_after_load {
                self.fixture.cancel();
            }
            Ok(StagingBase::Existing {
                metadata: self.metadata.lock().unwrap().clone(),
                metadata_location: format!(
                    "file://{}/00000-00000000-0000-0000-0000-000000000001.metadata.json",
                    self.fixture.directory.path().display()
                ),
            })
        }
        fn preflight_recovery(
            &self,
            request: &FrozenRequest,
            operation: &IcebergCommitOperation,
        ) -> Result<()> {
            assert_eq!(request.artifacts().attempt().operation(), operation.token());
            if self.recovery_refused {
                assert_eq!(operation.artifacts()?.len(), 3);
                return Err(
                    Error::new(ErrorKind::Unexpected, "Recovery envelope was refused").with_source(
                        ConnectorError::new(
                            ConnectorErrorKind::ResourceExhausted,
                            "complete recovery evidence exceeds its capacity",
                        ),
                    ),
                );
            }
            Ok(())
        }

        async fn dispatch_once(&self, request: FrozenRequest) -> CatalogOutcome<CommitProof> {
            let call = {
                let mut requests = self.requests.lock().unwrap();
                requests.push(request.to_rest_json().unwrap());
                requests.len()
            };
            if let Dispatch::ConflictThen(metadata) = &self.dispatch {
                if call == 1 {
                    *self.metadata.lock().unwrap() = metadata.clone();
                    return CatalogOutcome::uncommitted(
                        ConnectorMutationFailureKind::Conflict,
                        "Injected concurrent catalog commit",
                    );
                }
            }
            match self.dispatch {
                Dispatch::Unknown => {
                    return CatalogOutcome::unknown(
                        "Lost dispatch response",
                        CatalogCommitEvidence::for_target("db.t"),
                    );
                }
                Dispatch::AlwaysConflict => {
                    self.fixture.cancel();
                    return CatalogOutcome::uncommitted(
                        ConnectorMutationFailureKind::Conflict,
                        "Injected conflict followed by cancellation",
                    );
                }
                _ => {}
            }
            let metadata = self.metadata.lock().unwrap().clone();
            for requirement in request.requirements() {
                if let Err(error) = requirement.check(Some(&metadata)) {
                    return CatalogOutcome::uncommitted(
                        ConnectorMutationFailureKind::Conflict,
                        error.to_string(),
                    );
                }
            }
            let mut builder = metadata.into_builder(None);
            for update in request.updates() {
                builder = update.clone().apply(builder).unwrap();
            }
            let after = builder.build().unwrap().metadata;
            let id = request.ref_snapshot_after(request.target_ref());
            *self.metadata.lock().unwrap() = after;
            if matches!(self.dispatch, Dispatch::CancelAfterCommit) {
                self.fixture.cancel();
            }
            CatalogOutcome::committed(
                CommitProof::applied(id),
                novarocks_spi::connector::ExternalMutationEffect::Applied,
            )
        }
    }
    fn policy() -> RetryPolicy {
        RetryPolicy::from_properties(&HashMap::from([
            ("commit.retry.min-wait-ms".into(), "0".into()),
            ("commit.retry.max-wait-ms".into(), "0".into()),
        ]))
        .unwrap()
    }
    fn append_intent(
        fixture: &Fixture,
        metadata: &TableMetadata,
        dependencies: Vec<Dependency>,
    ) -> OperationIntent {
        let added = AddedContent::new_logical_data(
            data(
                &format!("file://{}/new.parquet", fixture.directory.path().display()),
                3,
                Struct::empty(),
            ),
            0,
        )
        .unwrap();
        let mut parts = OperationIntentParts {
            target: TableTarget {
                ident: crate::iceberg::TableIdent::from_strs(["db", "t"]).unwrap(),
                uuid: Some(metadata.uuid()),
            },
            target_ref: "main".into(),
            start: metadata
                .snapshot_for_ref("main")
                .map(|s| super::super::model::StartSnapshot {
                    snapshot_id: s.snapshot_id(),
                    sequence_number: s.sequence_number(),
                }),
            changes: FileChanges {
                added: vec![added],
                removed: Vec::new(),
            },
            dependencies,
            isolation: IsolationLevel::Snapshot,
            shape: RequestShape::SnapshotProducing,
            summary: Default::default(),
            token: fixture.operation.token(),
        };
        parts.summary.insert(
            "iru5-operation".into(),
            fixture.operation.token().to_string(),
        );
        OperationIntent::new(parts).unwrap()
    }
    async fn append_state(fixture: &Fixture, metadata: TableMetadata, count: u64) -> TableMetadata {
        let intent = fixture.intent(
            &metadata,
            "main",
            vec![
                AddedContent::new_logical_data(
                    data(
                        &format!(
                            "file://{}/race-{count}.parquet",
                            fixture.directory.path().display()
                        ),
                        count,
                        Struct::empty(),
                    ),
                    0,
                )
                .unwrap(),
            ],
        );
        fixture
            .stage(metadata, &intent, &FastAppendPreparer)
            .await
            .0
    }
    async fn invoke(
        fixture: &Fixture,
        intent: &OperationIntent,
        publisher: &dyn Publisher,
    ) -> Report {
        run(
            &fixture.operation,
            intent,
            &PreparedChange::default(),
            &[&FastAppendPreparer],
            publisher,
            policy(),
        )
        .await
    }

    #[tokio::test]
    async fn mor_frozen_before_another_branch_commit_inherits_actual_publication_sequence() {
        use super::super::model::{ArtifactClass, ArtifactKind, ArtifactWriter, StartSnapshot};
        use super::super::row_delta_dv_metadata::{LiveFile, WrittenDvFile, dv_data_file};
        use crate::iceberg::spec::{SnapshotReference, SnapshotRetention};
        let fixture = Fixture::new();
        let source = append_state(&fixture, fixture.metadata(FormatVersion::V3), 4).await;
        let source_id = source.current_snapshot_id().unwrap();
        let source_sequence = source.snapshot_by_id(source_id).unwrap().sequence_number();
        let source_data = data(
            &format!(
                "file://{}/race-4.parquet",
                fixture.directory.path().display()
            ),
            4,
            Struct::empty(),
        );
        let writer = fixture.operation.begin_attempt().unwrap();
        let object = writer
            .allocate(ArtifactClass::Operation, ArtifactKind::DeletionVector)
            .unwrap();
        let mut vector = super::super::DeletionVector::new();
        vector.insert(1).unwrap();
        let written = super::super::write_single_deletion_vector_puffin(
            writer.file_io(),
            object.path(),
            source_data.file_path(),
            &vector,
        )
        .await
        .unwrap();
        let dv = dv_data_file(
            &WrittenDvFile::from(written),
            &LiveFile {
                data_file: source_data,
                partition_spec_id: 0,
                snapshot_id: source_id,
                sequence_number: source_sequence,
                file_sequence_number: Some(source_sequence),
            },
        )
        .unwrap();
        let updated_path = format!(
            "file://{}/updated.parquet",
            fixture.directory.path().display()
        );
        let intent = OperationIntent::new(OperationIntentParts {
            target: TableTarget {
                ident: crate::iceberg::TableIdent::from_strs(["db", "t"]).unwrap(),
                uuid: Some(source.uuid()),
            },
            target_ref: "main".into(),
            start: Some(StartSnapshot {
                snapshot_id: source_id,
                sequence_number: source_sequence,
            }),
            changes: FileChanges {
                added: vec![
                    AddedContent::new_logical_data(data(&updated_path, 1, Struct::empty()), 0)
                        .unwrap(),
                    AddedContent::new_logical_data(dv, 0).unwrap(),
                ],
                removed: vec![],
            },
            dependencies: vec![Dependency::RefUnchanged],
            isolation: IsolationLevel::Serializable,
            shape: RequestShape::SnapshotProducing,
            summary: Default::default(),
            token: fixture.operation.token(),
        })
        .unwrap();
        // This branch commits only after the statement's source facts and
        // outputs are frozen, but before the publication's authoritative load.
        let branched = source
            .into_builder(None)
            .set_ref(
                "audit",
                SnapshotReference::new(source_id, SnapshotRetention::branch(None, None, None)),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let branch_intent = fixture.intent(
            &branched,
            "audit",
            vec![
                AddedContent::new_logical_data(
                    data(
                        &format!(
                            "file://{}/audit.parquet",
                            fixture.directory.path().display()
                        ),
                        2,
                        Struct::empty(),
                    ),
                    0,
                )
                .unwrap(),
            ],
        );
        let after_branch = fixture
            .stage(branched, &branch_intent, &FastAppendPreparer)
            .await
            .0;
        assert_eq!(after_branch.current_snapshot_id(), Some(source_id));
        assert!(after_branch.last_sequence_number() > source_sequence);
        let expected = after_branch.next_sequence_number();
        let publisher = ControlledPublisher::new(&fixture, after_branch, Dispatch::Commit);
        let report = run(
            &fixture.operation,
            &intent,
            &PreparedChange::default(),
            &[&super::super::row_delta_dv_from_files::RowDeltaDvFromFilesPreparer],
            &publisher,
            policy(),
        )
        .await;
        assert!(
            matches!(report.publication, PublicationOutcome::Committed(_)),
            "{report:?}"
        );
        assert_eq!(publisher.requests.lock().unwrap().len(), 1);
        let committed = publisher.metadata.lock().unwrap().clone();
        assert_eq!(
            committed.current_snapshot().unwrap().sequence_number(),
            expected
        );
        let read = fixture.operation.begin_attempt().unwrap();
        let mut inputs = super::super::dependency::ValidationInputs::new(
            &committed,
            committed.current_snapshot_id(),
            &read,
        );
        let live = inputs.live_set().await.unwrap();
        let new_entries: Vec<_> = live
            .values()
            .filter(|e| e.file.file_path() == updated_path || e.file.file_path() == object.path())
            .collect();
        assert_eq!(new_entries.len(), 2);
        for entry in new_entries {
            assert_eq!(entry.frozen.facts().data_sequence, Some(expected));
            assert_eq!(entry.frozen.facts().file_sequence, Some(expected));
        }
    }

    #[test]
    fn retry_policy_parses_only_its_four_keys_and_rejects_invalid_bounds() {
        let mut properties =
            HashMap::from([("write.target-file-size-bytes".into(), "malformed".into())]);
        let defaults = RetryPolicy::from_properties(&properties).unwrap();
        assert_eq!(defaults.retries, 4);
        assert_eq!(defaults.backoff_ceiling(30), Duration::from_secs(60));
        for retry in [0, 1, 4, 30] {
            for _ in 0..32 {
                let wait = defaults.backoff(retry);
                assert!(wait >= defaults.min_wait);
                assert!(wait <= defaults.backoff_ceiling(retry));
            }
        }
        for (key, value) in [
            ("commit.retry.num-retries", "-1"),
            ("commit.retry.min-wait-ms", "bad"),
            ("commit.retry.max-wait-ms", "1"),
            ("commit.retry.total-timeout-ms", "0"),
        ] {
            properties.insert(key.into(), value.into());
            assert!(RetryPolicy::from_properties(&properties).is_err());
            properties.remove(key);
        }
    }

    #[tokio::test]
    async fn pure_append_reprepares_after_conflict_from_new_sequence_and_row_range() {
        let baseline = Fixture::new();
        let metadata = append_state(&baseline, baseline.metadata(FormatVersion::V3), 2).await;
        let competitor = Fixture::new();
        let raced = append_state(&competitor, metadata.clone(), 4).await;
        let fixture = Fixture::new();
        let intent = append_intent(&fixture, &metadata, vec![Dependency::NoReadDependency]);
        let publisher =
            ControlledPublisher::new(&fixture, metadata, Dispatch::ConflictThen(raced.clone()));
        let report = invoke(&fixture, &intent, &publisher).await;
        assert!(matches!(
            report.publication,
            PublicationOutcome::Committed(_)
        ));
        assert_eq!(publisher.loads.load(Ordering::SeqCst), 2);
        let requests = publisher.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        let snapshots: Vec<_> = requests
            .iter()
            .map(|r| {
                r["updates"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|u| u["action"] == "add-snapshot")
                    .unwrap()["snapshot"]
                    .clone()
            })
            .collect();
        assert_eq!(
            snapshots[1]["sequence-number"],
            raced.next_sequence_number()
        );
        assert_eq!(snapshots[1]["first-row-id"], raced.next_row_id());
        assert_eq!(snapshots[1]["added-rows"], 3);
        assert_ne!(snapshots[0]["manifest-list"], snapshots[1]["manifest-list"]);
        assert!(
            !std::path::Path::new(
                snapshots[0]["manifest-list"]
                    .as_str()
                    .unwrap()
                    .strip_prefix("file://")
                    .unwrap()
            )
            .exists()
        );
        assert!(
            std::path::Path::new(
                snapshots[1]["manifest-list"]
                    .as_str()
                    .unwrap()
                    .strip_prefix("file://")
                    .unwrap()
            )
            .exists()
        );
    }

    #[tokio::test]
    async fn read_dependency_conflict_stops_before_a_second_dispatch() {
        let baseline = Fixture::new();
        let metadata = append_state(&baseline, baseline.metadata(FormatVersion::V3), 2).await;
        let competitor = Fixture::new();
        let raced = append_state(&competitor, metadata.clone(), 4).await;
        let fixture = Fixture::new();
        let intent = append_intent(&fixture, &metadata, vec![Dependency::RefUnchanged]);
        let publisher = ControlledPublisher::new(&fixture, metadata, Dispatch::ConflictThen(raced));
        let report = invoke(&fixture, &intent, &publisher).await;
        let PublicationOutcome::KnownUncommitted(failure) = report.publication else {
            panic!("dependency must reject")
        };
        assert_eq!(failure.kind(), ConnectorMutationFailureKind::Conflict);
        assert!(failure.message().contains("RefUnchanged"));
        assert_eq!(publisher.requests.lock().unwrap().len(), 1);
        assert_eq!(publisher.loads.load(Ordering::SeqCst), 2);
        assert!(fixture.operation.artifacts().unwrap().is_empty());
    }

    #[tokio::test]
    async fn unknown_dispatch_retains_every_owned_object_without_retry_or_cleanup() {
        let fixture = Fixture::new();
        let metadata = fixture.metadata(FormatVersion::V3);
        let intent = append_intent(&fixture, &metadata, vec![Dependency::NoReadDependency]);
        let file = intent.changes().added[0].file().file_path();
        std::fs::write(file.strip_prefix("file://").unwrap(), b"session data").unwrap();
        fixture
            .operation
            .adopt_session_data(ObjectIdentity::new(file).unwrap())
            .unwrap();
        let publisher = ControlledPublisher::new(&fixture, metadata, Dispatch::Unknown);
        let report = invoke(&fixture, &intent, &publisher).await;
        let PublicationOutcome::Unknown {
            artifacts,
            operation,
            ..
        } = report.publication
        else {
            panic!("unknown")
        };
        assert_eq!(operation, fixture.operation.token());
        assert_eq!(artifacts.len(), 3);
        assert_eq!(report.cleanup, IcebergCleanupReport::NotAttempted);
        assert_eq!(publisher.requests.lock().unwrap().len(), 1);
        assert_eq!(publisher.loads.load(Ordering::SeqCst), 1);
        for artifact in artifacts {
            assert!(
                std::path::Path::new(artifact.object.path().strip_prefix("file://").unwrap())
                    .exists()
            );
        }
    }

    #[tokio::test]
    async fn cancellation_after_load_or_conflict_cannot_start_another_prepare_or_dispatch() {
        for after_load in [true, false] {
            let fixture = Fixture::new();
            let metadata = fixture.metadata(FormatVersion::V3);
            let intent = append_intent(&fixture, &metadata, vec![Dependency::NoReadDependency]);
            let mut publisher =
                ControlledPublisher::new(&fixture, metadata, Dispatch::AlwaysConflict);
            publisher.cancel_after_load = after_load;
            let report = invoke(&fixture, &intent, &publisher).await;
            let PublicationOutcome::KnownUncommitted(failure) = report.publication else {
                panic!("cancelled")
            };
            assert_eq!(failure.kind(), ConnectorMutationFailureKind::Cancelled);
            assert_eq!(
                publisher.requests.lock().unwrap().len(),
                usize::from(!after_load)
            );
            assert_eq!(publisher.loads.load(Ordering::SeqCst), 1);
            assert!(fixture.operation.artifacts().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn cancellation_after_dispatch_does_not_reclassify_known_commit_or_delete_its_artifacts()
    {
        let fixture = Fixture::new();
        let metadata = fixture.metadata(FormatVersion::V3);
        let intent = append_intent(&fixture, &metadata, vec![Dependency::NoReadDependency]);
        let publisher = ControlledPublisher::new(&fixture, metadata, Dispatch::CancelAfterCommit);
        let report = invoke(&fixture, &intent, &publisher).await;
        assert!(matches!(
            report.publication,
            PublicationOutcome::Committed(_)
        ));
        assert_eq!(
            report.cleanup,
            IcebergCleanupReport::Complete { deleted: 0 }
        );
        assert_eq!(fixture.operation.artifacts().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn committed_cleanup_uses_physical_reachability_for_shared_puffin() {
        let fixture = Fixture::new();
        let mut paths = Vec::new();
        for file in ["shared.puffin", "superseded.puffin"] {
            let path = format!("file://{}/{file}", fixture.directory.path().display());
            std::fs::write(path.strip_prefix("file://").unwrap(), b"owned puffin").unwrap();
            let object = ObjectIdentity::new(path).unwrap();
            fixture
                .operation
                .adopt_session_data(object.clone())
                .unwrap();
            paths.push(object);
        }
        let report = fixture.operation.cleanup_after_commit(&paths[..1]).await;
        assert_eq!(report, IcebergCleanupReport::Complete { deleted: 1 });
        assert!(std::path::Path::new(paths[0].path().strip_prefix("file://").unwrap()).exists());
        assert!(!std::path::Path::new(paths[1].path().strip_prefix("file://").unwrap()).exists());
        assert_eq!(fixture.operation.artifacts().unwrap()[0].object, paths[0]);
    }

    #[tokio::test]
    async fn pure_append_accepts_expired_start_and_rolled_back_ref_without_history() {
        for rollback in [false, true] {
            let baseline = Fixture::new();
            let first = append_state(&baseline, baseline.metadata(FormatVersion::V3), 2).await;
            let competitor = Fixture::new();
            let latest = append_state(&competitor, first.clone(), 4).await;
            let first_id = first.snapshot_for_ref("main").unwrap().snapshot_id();
            let latest_id = latest.snapshot_for_ref("main").unwrap().snapshot_id();
            let mut builder = latest.clone().into_builder(None);
            if rollback {
                let mut reference = latest.refs()["main"].clone();
                reference.snapshot_id = first_id;
                builder = TableUpdate::SetSnapshotRef {
                    ref_name: "main".into(),
                    reference,
                }
                .apply(builder)
                .unwrap();
            }
            builder = TableUpdate::RemoveSnapshots {
                snapshot_ids: vec![if rollback { latest_id } else { first_id }],
            }
            .apply(builder)
            .unwrap();
            let current = builder.build().unwrap().metadata;
            let fixture = Fixture::new();
            let intent = append_intent(
                &fixture,
                if rollback { &latest } else { &first },
                vec![Dependency::NoReadDependency],
            );
            assert!(
                current
                    .snapshot_by_id(intent.start().unwrap().snapshot_id)
                    .is_none()
            );
            let publisher = ControlledPublisher::new(&fixture, current, Dispatch::Commit);
            let report = invoke(&fixture, &intent, &publisher).await;
            assert!(matches!(
                report.publication,
                PublicationOutcome::Committed(_)
            ));
            let after = publisher.metadata.lock().unwrap();
            let snapshot = after.snapshot_for_ref("main").unwrap();
            assert_eq!(snapshot.sequence_number(), latest.next_sequence_number());
            assert_eq!(snapshot.first_row_id(), Some(latest.next_row_id()));
            assert_eq!(publisher.requests.lock().unwrap().len(), 1);
        }
    }

    struct CancelAfterPreparation<'a>(&'a Fixture, &'a crate::activity::CatalogActivity);
    #[async_trait]
    impl Preparer for CancelAfterPreparation<'_> {
        async fn prepare(
            &self,
            view: &super::super::staging::StagedView<'_>,
            intent: &OperationIntent,
        ) -> Result<PreparedChange> {
            let change = FastAppendPreparer.prepare(view, intent).await?;
            use novarocks_spi::connector::{CatalogActivityPhase as P, CatalogActivityReader};
            let snapshot = self.1.snapshot();
            assert_eq!(snapshot.phases[P::PreDispatch as usize].inflight, 1);
            assert_eq!(snapshot.phases[P::Evidence as usize].started, 0);
            self.0.cancel();
            Ok(change)
        }
    }

    #[tokio::test]
    async fn cancellation_after_artifact_exit_prevents_dispatch_and_cleans_preparation() {
        use novarocks_spi::connector::{
            CatalogActivityPhase as P, CatalogActivityReader, CatalogScopeIdentity,
            ConnectorInstanceId, ConnectorProviderId,
        };
        let root = crate::activity::CatalogActivity::new(CatalogScopeIdentity::new(
            ConnectorProviderId::parse("iceberg").unwrap(),
            ConnectorInstanceId::parse("preparer.actual").unwrap(),
            uuid::Uuid::now_v7(),
        ));
        let fixture = Fixture::new_observed(root.generation());
        let metadata = fixture.metadata(FormatVersion::V3);
        let intent = append_intent(&fixture, &metadata, vec![Dependency::NoReadDependency]);
        let publisher = ControlledPublisher::new(&fixture, metadata, Dispatch::Commit);
        let report = run(
            &fixture.operation,
            &intent,
            &PreparedChange::default(),
            &[&CancelAfterPreparation(&fixture, &root)],
            &publisher,
            policy(),
        )
        .await;
        let PublicationOutcome::KnownUncommitted(failure) = report.publication else {
            panic!("cancelled")
        };
        assert_eq!(failure.kind(), ConnectorMutationFailureKind::Cancelled);
        assert_eq!(
            report.cleanup,
            IcebergCleanupReport::Complete { deleted: 2 }
        );
        assert!(publisher.requests.lock().unwrap().is_empty());
        assert!(fixture.operation.artifacts().unwrap().is_empty());
        let snapshot = root.snapshot();
        assert_eq!(snapshot.phases[P::PreDispatch as usize].inflight, 0);
        assert_eq!(snapshot.phases[P::PreDispatch as usize].exited, 1);
        assert_eq!(snapshot.events.pre_dispatch_cancelled, 1);
    }

    #[tokio::test]
    async fn elapsed_retry_budget_after_load_prevents_preparation_and_dispatch() {
        let fixture = Fixture::new();
        let metadata = fixture.metadata(FormatVersion::V3);
        let intent = append_intent(&fixture, &metadata, vec![Dependency::NoReadDependency]);
        let mut publisher = ControlledPublisher::new(&fixture, metadata, Dispatch::Commit);
        publisher.load_delay = Duration::from_millis(10);
        let policy = RetryPolicy {
            total_timeout: Duration::from_millis(1),
            ..policy()
        };
        let report = run(
            &fixture.operation,
            &intent,
            &PreparedChange::default(),
            &[&FastAppendPreparer],
            &publisher,
            policy,
        )
        .await;
        let PublicationOutcome::KnownUncommitted(failure) = report.publication else {
            panic!("deadline")
        };
        assert_eq!(
            failure.kind(),
            ConnectorMutationFailureKind::DeadlineExceeded
        );
        assert!(publisher.requests.lock().unwrap().is_empty());
        assert!(fixture.operation.artifacts().unwrap().is_empty());
    }

    #[tokio::test]
    async fn recovery_preflight_refusal_is_undispatched_and_cleans_all_owned_artifacts() {
        let fixture = Fixture::new();
        let metadata = fixture.metadata(FormatVersion::V3);
        let intent = append_intent(&fixture, &metadata, vec![Dependency::NoReadDependency]);
        let file = intent.changes().added[0].file().file_path();
        std::fs::write(file.strip_prefix("file://").unwrap(), b"session data").unwrap();
        fixture
            .operation
            .adopt_session_data(ObjectIdentity::new(file).unwrap())
            .unwrap();
        let mut publisher = ControlledPublisher::new(&fixture, metadata, Dispatch::Commit);
        publisher.recovery_refused = true;
        let report = run(
            &fixture.operation,
            &intent,
            &PreparedChange::default(),
            &[&FastAppendPreparer],
            &publisher,
            policy(),
        )
        .await;
        let PublicationOutcome::KnownUncommitted(failure) = report.publication else {
            panic!("preflight refusal is not an unknown commit")
        };
        assert_eq!(
            failure.kind(),
            ConnectorMutationFailureKind::ResourceExhausted
        );
        assert_eq!(
            report.cleanup,
            IcebergCleanupReport::Complete { deleted: 3 }
        );
        assert!(publisher.requests.lock().unwrap().is_empty());
        assert!(fixture.operation.artifacts().unwrap().is_empty());
        assert!(!std::path::Path::new(file.strip_prefix("file://").unwrap()).exists());
    }

    struct RemoveAfterAppend;

    #[async_trait]
    impl Preparer for RemoveAfterAppend {
        async fn prepare(
            &self,
            view: &super::super::staging::StagedView<'_>,
            intent: &OperationIntent,
        ) -> Result<PreparedChange> {
            let remove_intent = OperationIntent::new(OperationIntentParts {
                target: intent.target().clone(),
                target_ref: intent.target_ref().to_owned(),
                start: intent.start(),
                changes: FileChanges::default(),
                dependencies: intent.dependencies().to_vec(),
                isolation: intent.isolation(),
                shape: intent.shape(),
                summary: intent.summary().clone(),
                token: intent.token(),
            })?;
            super::super::truncate::TruncatePreparer
                .prepare(view, &remove_intent)
                .await
        }
    }

    #[tokio::test]
    async fn earlier_published_snapshot_retains_owned_data_after_a_later_stage_removes_it() {
        let fixture = Fixture::new();
        let metadata = fixture.metadata(FormatVersion::V3);
        let intent = append_intent(&fixture, &metadata, vec![Dependency::RefUnchanged]);
        let file = intent.changes().added[0].file().file_path();
        std::fs::write(file.strip_prefix("file://").unwrap(), b"session data").unwrap();
        fixture
            .operation
            .adopt_session_data(ObjectIdentity::new(file).unwrap())
            .unwrap();
        let publisher = ControlledPublisher::new(&fixture, metadata, Dispatch::Commit);
        let report = run(
            &fixture.operation,
            &intent,
            &PreparedChange::default(),
            &[&FastAppendPreparer, &RemoveAfterAppend],
            &publisher,
            policy(),
        )
        .await;
        assert!(matches!(
            report.publication,
            PublicationOutcome::Committed(_)
        ));
        assert_eq!(
            report.cleanup,
            IcebergCleanupReport::Complete { deleted: 0 }
        );
        assert!(std::path::Path::new(file.strip_prefix("file://").unwrap()).exists());
        let after = publisher.metadata.lock().unwrap().clone();
        assert_eq!(after.snapshots().count(), 2);
        let attempt = fixture.operation.begin_attempt().unwrap();
        let head = after.snapshot_for_ref("main").unwrap().snapshot_id();
        let mut final_live = ValidationInputs::new(&after, Some(head), &attempt);
        assert!(final_live.live_set().await.unwrap().is_empty());
        let earlier = after
            .snapshots()
            .find(|s| s.snapshot_id() != head)
            .unwrap()
            .snapshot_id();
        let mut historical_live = ValidationInputs::new(&after, Some(earlier), &attempt);
        assert!(
            historical_live
                .live_set()
                .await
                .unwrap()
                .keys()
                .any(|identity| identity.object().path() == file)
        );
    }
    struct ReusedManifest(crate::iceberg::spec::ManifestFile);
    #[async_trait]
    impl Preparer for ReusedManifest {
        async fn prepare(
            &self,
            view: &crate::commit::staging::StagedView<'_>,
            intent: &OperationIntent,
        ) -> Result<PreparedChange> {
            crate::commit::overwrite::prepare_snapshot_change(
                view,
                intent,
                10000 + view.metadata().next_sequence_number(),
                crate::iceberg::spec::Operation::Replace,
                vec![self.0.clone()],
                HashMap::new(),
                false,
            )
            .await
        }
    }
    async fn operation_manifest_fixture(
        fixture: &Fixture,
    ) -> (
        TableMetadata,
        crate::iceberg::spec::ManifestFile,
        ObjectIdentity,
    ) {
        use crate::commit::model::ArtifactKind;
        let metadata = fixture.metadata(FormatVersion::V2);
        let added = AddedContent::new_logical_data(
            data("file:///external-data.parquet", 3, Struct::empty()),
            0,
        )
        .unwrap();
        let intent = fixture.intent(&metadata, "main", vec![added]);
        let (metadata, _) = fixture.stage(metadata, &intent, &FastAppendPreparer).await;
        let attempt = fixture.operation.begin_attempt().unwrap();
        let snapshot = metadata.snapshot_for_ref("main").unwrap();
        let list = crate::manifest::load_manifest_list(snapshot, attempt.file_io(), &metadata)
            .await
            .unwrap();
        let mut selected = list.entries()[0].clone();
        let bytes = attempt
            .file_io()
            .new_input(&selected.manifest_path)
            .unwrap()
            .read()
            .await
            .unwrap();
        let mut objects = Vec::new();
        for _ in 0..2 {
            let object = attempt
                .allocate(ArtifactClass::Operation, ArtifactKind::Manifest)
                .unwrap();
            let mut writer = attempt
                .file_io()
                .new_output(object.path())
                .unwrap()
                .writer()
                .await
                .unwrap();
            writer.write(bytes.clone()).await.unwrap();
            writer.close().await.unwrap();
            objects.push(object);
        }
        selected.manifest_path = objects[0].path().to_string();
        (metadata, selected, objects[1].clone())
    }
    #[tokio::test]
    async fn operation_manifest_roots_survive_rejected_attempt_and_only_selected_roots_survive_success()
     {
        let fixture = Fixture::new();
        let (metadata, selected, unused) = operation_manifest_fixture(&fixture).await;
        let selected_object = ObjectIdentity::new(&selected.manifest_path).unwrap();
        let intent = fixture.intent(&metadata, "main", vec![]);
        let preparer = ReusedManifest(selected);
        let first = fixture.operation.begin_attempt().unwrap();
        let (request, _) = prepare(
            &first,
            &intent,
            StagingBase::Existing {
                metadata: metadata.clone(),
                metadata_location: "file:///base.json".into(),
            },
            &PreparedChange::default(),
            &[&preparer],
        )
        .await
        .unwrap();
        assert_eq!(
            request.artifacts().operation_references(),
            [selected_object.clone()]
        );
        fixture
            .operation
            .cleanup(CleanupScope::ThisAttempt(first.attempt_token()))
            .await;
        assert!(
            first
                .file_io()
                .exists(selected_object.path())
                .await
                .unwrap()
        );
        let next = fixture.operation.begin_attempt().unwrap();
        let (request, retained) = prepare(
            &next,
            &intent,
            StagingBase::Existing {
                metadata,
                metadata_location: "file:///base.json".into(),
            },
            &PreparedChange::default(),
            &[&preparer],
        )
        .await
        .unwrap();
        assert_eq!(
            request.artifacts().operation_references(),
            [selected_object.clone()]
        );
        assert!(!retained.contains(&unused));
        fixture.operation.cleanup_after_commit(&retained).await;
        assert!(next.file_io().exists(selected_object.path()).await.unwrap());
        assert!(!next.file_io().exists(unused.path()).await.unwrap());
    }
    #[tokio::test]
    async fn unknown_reused_manifest_request_retains_all_possible_operation_outputs() {
        let fixture = Fixture::new();
        let (metadata, selected, unused) = operation_manifest_fixture(&fixture).await;
        let selected_path = selected.manifest_path.clone();
        let intent = fixture.intent(&metadata, "main", vec![]);
        let publisher = ControlledPublisher::new(&fixture, metadata, Dispatch::Unknown);
        let report = run(
            &fixture.operation,
            &intent,
            &PreparedChange::default(),
            &[&ReusedManifest(selected)],
            &publisher,
            policy(),
        )
        .await;
        assert!(matches!(
            report.publication,
            PublicationOutcome::Unknown { .. }
        ));
        let attempt = fixture.operation.begin_attempt().unwrap();
        assert!(attempt.file_io().exists(&selected_path).await.unwrap());
        assert!(attempt.file_io().exists(unused.path()).await.unwrap());
        assert_eq!(publisher.requests.lock().unwrap().len(), 1);
    }
}
