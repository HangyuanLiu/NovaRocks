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

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::meta::repository::mv::MvMetaRepository;
use crate::meta::repository::{RepositoryError, RepositoryErrorKind, RepositoryResult};
use crate::meta::{MetaError, MetaErrorKind, MetaReadTxn, MetaStoreProvider, MetaWriteTxn};
use crate::mv::dependency::model::MvDependencyObjectRef;
use crate::mv::persistence::definition::{StoredMvDefinition, UpdateMvRefreshMetadataRequest};
use crate::mv::persistence::dependency::StoredMvDependency;
use crate::mv::persistence::partition::{
    RecordFailedMvPartitionStatesRequest, ReplaceMvPartitionStatesRequest, StoredMvPartitionState,
    UpdateMvPartitionContractRequest,
};
use crate::mv::persistence::refresh::{
    BeginIcebergMvRefreshRequest, MvRefreshFinalizeRequest, RecordPublishCommitRequest,
    RecordStagingCommitRequest, RefreshExternalOutcome, StoredMvRefresh,
    UpdateStarRocksMvRefreshSummaryRequest,
};
use crate::mv::repository::{
    CreateMvDependencyRequest, CreateMvRepositoryRequest, CreateMvRepositoryWithIdRequest,
    FinalizeMvRefreshWithPartitionsRequest, MvRepository, MvRepositoryAvailability,
    MvRepositoryError, MvRepositoryErrorKind, MvTarget, RebuildMvRepositoryRequest,
    RecordExternalCommitAndFinalizeRequest,
};
use uuid::Uuid;

/// Narrow test-only fault points for post-visible StarRocks/MV split tests.
///
/// The production StateStore implementation never observes these hooks. They
/// are thread-local so parallel tests cannot accidentally fail another
/// state's repository command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TestMvRepositoryFailurePoint {
    CreateWithId,
    DropById,
    UpdateStarRocksRefreshSummary,
}

thread_local! {
    static FAILURE_POINT: RefCell<Option<TestMvRepositoryFailurePoint>> = const { RefCell::new(None) };
}

pub(crate) struct TestMvRepositoryFailureGuard {
    previous: Option<TestMvRepositoryFailurePoint>,
}

impl Drop for TestMvRepositoryFailureGuard {
    fn drop(&mut self) {
        FAILURE_POINT.with(|slot| *slot.borrow_mut() = self.previous.take());
    }
}

pub(crate) fn fail_next_mv_repository_command(
    point: TestMvRepositoryFailurePoint,
) -> TestMvRepositoryFailureGuard {
    let previous = FAILURE_POINT.with(|slot| slot.borrow_mut().replace(point));
    TestMvRepositoryFailureGuard { previous }
}

fn fail_if_requested(point: TestMvRepositoryFailurePoint) -> Result<(), MvRepositoryError> {
    let requested = FAILURE_POINT.with(|slot| slot.borrow_mut().take());
    if requested == Some(point) {
        return Err(MvRepositoryError::new(
            MvRepositoryErrorKind::Unavailable,
            format!("test-only injected MV repository failure at {point:?}"),
        ));
    }
    if let Some(other) = requested {
        FAILURE_POINT.with(|slot| *slot.borrow_mut() = Some(other));
    }
    Ok(())
}

/// Test-only bridge used while the characterized MetaStore implementation
/// remains in place. Production code never constructs or exports this type.
pub struct LegacyMvRepositoryAdapter {
    provider: Arc<dyn MetaStoreProvider>,
    legacy: MvMetaRepository,
}

impl LegacyMvRepositoryAdapter {
    pub(crate) fn new(provider: Arc<dyn MetaStoreProvider>) -> Self {
        Self {
            provider,
            legacy: MvMetaRepository,
        }
    }

    fn read<T>(
        &self,
        action: impl FnOnce(&MvMetaRepository, &dyn MetaReadTxn) -> RepositoryResult<T>,
    ) -> Result<T, MvRepositoryError> {
        let txn = self.provider.begin_read().map_err(map_meta_error)?;
        action(&self.legacy, txn.as_ref()).map_err(map_repository_error)
    }

    fn write<T>(
        &self,
        description: &str,
        action: impl FnOnce(&MvMetaRepository, &mut dyn MetaWriteTxn) -> RepositoryResult<T>,
    ) -> Result<T, MvRepositoryError> {
        let mut txn = self
            .provider
            .begin_write(description)
            .map_err(map_meta_error)?;
        let value = action(&self.legacy, txn.as_mut()).map_err(map_repository_error)?;
        txn.commit().map_err(map_meta_error)?;
        Ok(value)
    }
}

fn map_repository_error(error: RepositoryError) -> MvRepositoryError {
    let kind = match error.kind() {
        RepositoryErrorKind::Conflict => MvRepositoryErrorKind::Conflict,
        RepositoryErrorKind::NotFound => MvRepositoryErrorKind::NotFound,
        RepositoryErrorKind::InvalidRequest => MvRepositoryErrorKind::InvalidRequest,
        RepositoryErrorKind::Provider => MvRepositoryErrorKind::Corruption,
    };
    MvRepositoryError::new(kind, error.to_string())
}

fn map_meta_error(error: MetaError) -> MvRepositoryError {
    let kind = match error.kind() {
        MetaErrorKind::Conflict | MetaErrorKind::AlreadyExists => MvRepositoryErrorKind::Conflict,
        MetaErrorKind::NotFound => MvRepositoryErrorKind::NotFound,
        MetaErrorKind::InvalidRequest | MetaErrorKind::Unsupported => {
            MvRepositoryErrorKind::InvalidRequest
        }
        MetaErrorKind::CommitUnknown => MvRepositoryErrorKind::CommitUnknown,
        MetaErrorKind::Transient => MvRepositoryErrorKind::Unavailable,
        MetaErrorKind::DefiniteCommitFailure | MetaErrorKind::ProviderCorruption => {
            MvRepositoryErrorKind::Corruption
        }
    };
    MvRepositoryError::new(kind, error.to_string())
}

fn create_definition(
    legacy: &MvMetaRepository,
    txn: &mut dyn MetaWriteTxn,
    request: CreateMvRepositoryRequest,
    mv_id: Option<i64>,
) -> RepositoryResult<StoredMvDefinition> {
    let CreateMvRepositoryRequest {
        definition,
        refresh,
        dependencies,
    } = request;
    let definition = match mv_id {
        Some(mv_id) => legacy.create_definition_with_id(txn, mv_id, definition)?,
        None => legacy.create_definition(txn, definition)?,
    };
    let definition = legacy.update_refresh_metadata(
        txn,
        UpdateMvRefreshMetadataRequest {
            mv_id: definition.mv_id,
            refresh_policy: refresh.policy,
            refresh_paused: refresh.paused,
            refresh_interval_ms: refresh.interval_ms,
            max_staleness_ms: refresh.max_staleness_ms,
            last_scheduler_error: None,
            next_refresh_after_ms: refresh.next_refresh_after_ms,
        },
    )?;
    legacy.replace_dependencies_for_mv(txn, definition.mv_id, dependencies)?;
    Ok(definition)
}

impl MvRepository for LegacyMvRepositoryAdapter {
    fn availability(&self) -> MvRepositoryAvailability {
        MvRepositoryAvailability::Available
    }

    fn create(
        &self,
        _operation_id: Uuid,
        request: CreateMvRepositoryRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        self.write("legacy adapter create MV", |legacy, txn| {
            create_definition(legacy, txn, request, None)
        })
    }

    fn create_with_id(
        &self,
        _operation_id: Uuid,
        request: CreateMvRepositoryWithIdRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        fail_if_requested(TestMvRepositoryFailurePoint::CreateWithId)?;
        self.write("legacy adapter create MV with id", |legacy, txn| {
            legacy.reserve_definition_id(txn, request.mv_id)?;
            create_definition(legacy, txn, request.create, Some(request.mv_id))
        })
    }

    fn rebuild(
        &self,
        _operation_id: Uuid,
        request: RebuildMvRepositoryRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        self.write("legacy adapter rebuild MV", |legacy, txn| {
            let definition = create_definition(legacy, txn, request.create, None)?;
            legacy.set_rebuilt_refresh_watermark(
                txn,
                definition.mv_id,
                request.base_snapshots,
                request.base_table_uuids,
            )
        })
    }

    fn reserve_definition_id(&self, mv_id: i64) -> Result<(), MvRepositoryError> {
        self.write("legacy adapter reserve MV id", |legacy, txn| {
            legacy.reserve_definition_id(txn, mv_id)
        })
    }

    fn load_by_id(&self, mv_id: i64) -> Result<Option<StoredMvDefinition>, MvRepositoryError> {
        self.read(|legacy, txn| legacy.load_by_id(txn, mv_id))
    }

    fn find_by_target(
        &self,
        target: &MvTarget,
    ) -> Result<Option<StoredMvDefinition>, MvRepositoryError> {
        let Some(catalog) = target.catalog.as_deref() else {
            return Ok(None);
        };
        self.read(|legacy, txn| legacy.find_by_target(txn, catalog, &target.database, &target.name))
    }

    fn list_definitions(&self) -> Result<Vec<StoredMvDefinition>, MvRepositoryError> {
        self.read(|legacy, txn| legacy.list_definitions(txn))
    }

    fn drop_by_id(&self, mv_id: i64) -> Result<bool, MvRepositoryError> {
        fail_if_requested(TestMvRepositoryFailurePoint::DropById)?;
        self.write("legacy adapter drop MV by id", |legacy, txn| {
            legacy.drop_by_id(txn, mv_id)
        })
    }

    fn drop_by_target(&self, target: &MvTarget) -> Result<bool, MvRepositoryError> {
        let Some(catalog) = target.catalog.as_deref() else {
            return Ok(false);
        };
        self.write("legacy adapter drop MV by target", |legacy, txn| {
            legacy.drop_by_target(txn, catalog, &target.database, &target.name)
        })
    }

    fn set_rebuilt_refresh_watermark(
        &self,
        mv_id: i64,
        base_snapshots: BTreeMap<String, i64>,
        base_table_uuids: BTreeMap<String, String>,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        self.write("legacy adapter set MV watermark", |legacy, txn| {
            legacy.set_rebuilt_refresh_watermark(txn, mv_id, base_snapshots, base_table_uuids)
        })
    }

    fn update_refresh_metadata(
        &self,
        request: UpdateMvRefreshMetadataRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        self.write(
            "legacy adapter update MV refresh metadata",
            |legacy, txn| legacy.update_refresh_metadata(txn, request),
        )
    }

    fn update_partition_contract(
        &self,
        request: UpdateMvPartitionContractRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        self.write(
            "legacy adapter update MV partition contract",
            |legacy, txn| legacy.update_partition_contract(txn, request),
        )
    }

    fn begin_refresh_intent(
        &self,
        mv_id: i64,
        target_snapshots: BTreeMap<String, i64>,
    ) -> Result<StoredMvRefresh, MvRepositoryError> {
        self.write("legacy adapter begin MV refresh", |legacy, txn| {
            legacy.begin_refresh_intent(txn, mv_id, target_snapshots)
        })
    }

    fn begin_iceberg_refresh_intent(
        &self,
        request: BeginIcebergMvRefreshRequest,
    ) -> Result<StoredMvRefresh, MvRepositoryError> {
        self.write("legacy adapter begin Iceberg MV refresh", |legacy, txn| {
            legacy.begin_iceberg_refresh_intent(txn, request)
        })
    }

    fn record_staging_commit(
        &self,
        request: RecordStagingCommitRequest,
    ) -> Result<(), MvRepositoryError> {
        self.write("legacy adapter record MV staging commit", |legacy, txn| {
            legacy.record_staging_commit(txn, request)
        })
    }

    fn record_publish_commit(
        &self,
        request: RecordPublishCommitRequest,
    ) -> Result<(), MvRepositoryError> {
        self.write("legacy adapter record MV publish commit", |legacy, txn| {
            legacy.record_publish_commit(txn, request)
        })
    }

    fn mark_refresh_commit_unknown(&self, refresh_id: i64) -> Result<(), MvRepositoryError> {
        self.write(
            "legacy adapter mark MV refresh commit unknown",
            |legacy, txn| legacy.mark_refresh_commit_unknown(txn, refresh_id),
        )
    }

    fn record_external_commit_outcome(
        &self,
        refresh_id: i64,
        outcome: RefreshExternalOutcome,
    ) -> Result<(), MvRepositoryError> {
        self.write("legacy adapter record MV external commit", |legacy, txn| {
            legacy.record_external_commit_outcome(txn, refresh_id, outcome)
        })
    }

    fn finalize_refresh(&self, request: MvRefreshFinalizeRequest) -> Result<(), MvRepositoryError> {
        self.write("legacy adapter finalize MV refresh", |legacy, txn| {
            legacy.finalize_refresh(txn, request)
        })
    }

    fn finalize_refresh_with_partitions(
        &self,
        request: FinalizeMvRefreshWithPartitionsRequest,
    ) -> Result<(), MvRepositoryError> {
        self.write(
            "legacy adapter finalize MV refresh and partitions",
            |legacy, txn| {
                legacy.finalize_refresh(txn, request.refresh)?;
                if let Some(partitions) = request.partitions {
                    legacy.replace_partition_states(txn, partitions)?;
                }
                Ok(())
            },
        )
    }

    fn record_external_commit_and_finalize(
        &self,
        request: RecordExternalCommitAndFinalizeRequest,
    ) -> Result<(), MvRepositoryError> {
        self.write(
            "legacy adapter record and finalize MV commit",
            |legacy, txn| {
                legacy.record_external_commit_outcome(
                    txn,
                    request.refresh_id,
                    request.external_outcome,
                )?;
                legacy.finalize_refresh(txn, request.finalize)
            },
        )
    }

    fn clear_refresh_progress(&self, mv_id: i64) -> Result<bool, MvRepositoryError> {
        self.write("legacy adapter clear MV refresh progress", |legacy, txn| {
            legacy.clear_refresh_progress(txn, mv_id)
        })
    }

    fn load_refresh(&self, refresh_id: i64) -> Result<Option<StoredMvRefresh>, MvRepositoryError> {
        self.read(|legacy, txn| legacy.load_refresh(txn, refresh_id))
    }

    fn list_unfinished_refreshes(&self) -> Result<Vec<StoredMvRefresh>, MvRepositoryError> {
        self.read(|legacy, txn| legacy.list_unfinished_refreshes(txn))
    }

    fn list_unfinished_branch_staged_iceberg_refreshes(
        &self,
    ) -> Result<Vec<StoredMvRefresh>, MvRepositoryError> {
        self.read(|legacy, txn| legacy.list_unfinished_branch_staged_iceberg_refreshes(txn))
    }

    fn update_starrocks_refresh_summary_if_present(
        &self,
        request: UpdateStarRocksMvRefreshSummaryRequest,
    ) -> Result<bool, MvRepositoryError> {
        fail_if_requested(TestMvRepositoryFailurePoint::UpdateStarRocksRefreshSummary)?;
        self.write(
            "legacy adapter update StarRocks MV refresh summary",
            |legacy, txn| legacy.update_starrocks_refresh_summary_if_present(txn, request),
        )
    }

    fn replace_partition_states(
        &self,
        request: ReplaceMvPartitionStatesRequest,
    ) -> Result<(), MvRepositoryError> {
        self.write(
            "legacy adapter replace MV partition states",
            |legacy, txn| {
                legacy.replace_partition_states(txn, request)?;
                Ok(())
            },
        )
    }

    fn record_failed_partition_states(
        &self,
        request: RecordFailedMvPartitionStatesRequest,
    ) -> Result<(), MvRepositoryError> {
        self.write(
            "legacy adapter record failed MV partitions",
            |legacy, txn| {
                legacy.record_failed_partition_states(txn, request)?;
                Ok(())
            },
        )
    }

    fn clear_partition_states(&self, mv_id: i64) -> Result<bool, MvRepositoryError> {
        self.write("legacy adapter clear MV partition states", |legacy, txn| {
            legacy.clear_partition_states(txn, mv_id)
        })
    }

    fn list_partition_states(
        &self,
        mv_id: i64,
    ) -> Result<Vec<StoredMvPartitionState>, MvRepositoryError> {
        self.read(|legacy, txn| legacy.list_partition_states(txn, mv_id))
    }

    fn adopt_target_compaction_snapshot(
        &self,
        target: &MvTarget,
        expected_snapshot_id: i64,
        adopted_snapshot_id: i64,
    ) -> Result<bool, MvRepositoryError> {
        let Some(catalog) = target.catalog.as_deref() else {
            return Ok(false);
        };
        self.write(
            "legacy adapter adopt MV compaction snapshot",
            |legacy, txn| {
                legacy.adopt_target_compaction_snapshot(
                    txn,
                    catalog,
                    &target.database,
                    &target.name,
                    expected_snapshot_id,
                    adopted_snapshot_id,
                )
            },
        )
    }

    fn replace_dependencies_for_mv(
        &self,
        mv_id: i64,
        dependencies: Vec<CreateMvDependencyRequest>,
    ) -> Result<(), MvRepositoryError> {
        self.write("legacy adapter replace MV dependencies", |legacy, txn| {
            legacy.replace_dependencies_for_mv(txn, mv_id, dependencies)?;
            Ok(())
        })
    }

    fn delete_dependencies_for_mv(&self, mv_id: i64) -> Result<(), MvRepositoryError> {
        self.write("legacy adapter delete MV dependencies", |legacy, txn| {
            legacy.delete_dependencies_for_mv(txn, mv_id)
        })
    }

    fn ensure_no_downstream_dependencies(
        &self,
        upstream: &MvDependencyObjectRef,
    ) -> Result<(), MvRepositoryError> {
        self.read(|legacy, txn| legacy.ensure_no_downstream_dependencies(txn, upstream))
    }

    fn list_dependencies_by_downstream(
        &self,
        mv_id: i64,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        self.read(|legacy, txn| legacy.list_dependencies_by_downstream(txn, mv_id))
    }

    fn list_downstream_dependencies(
        &self,
        upstream: &MvDependencyObjectRef,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        self.read(|legacy, txn| legacy.list_downstream_dependencies(txn, upstream))
    }
}
