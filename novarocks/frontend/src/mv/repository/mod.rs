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

pub mod catalog;
pub mod codec;
pub mod key;
mod operation;

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::Arc;

use novarocks::mv::dependency::model::MvDependencyObjectRef;
use novarocks::mv::persistence::definition::{StoredMvDefinition, UpdateMvRefreshMetadataRequest};
use novarocks::mv::persistence::dependency::{CreateMvDependencyRequest, StoredMvDependency};
use novarocks::mv::persistence::partition::{
    RecordFailedMvPartitionStatesRequest, ReplaceMvPartitionStatesRequest, StoredMvPartitionState,
    UpdateMvPartitionContractRequest,
};
use novarocks::mv::persistence::refresh::{
    BeginIcebergMvRefreshRequest, MvRefreshFinalizeRequest, RecordPublishCommitRequest,
    RecordStagingCommitRequest, RefreshExternalOutcome, StoredMvRefresh,
    UpdateStarRocksMvRefreshSummaryRequest,
};
use novarocks::mv::repository::{
    CreateMvRepositoryRequest, CreateMvRepositoryWithIdRequest,
    FinalizeMvRefreshWithPartitionsRequest, MvRepository, MvRepositoryAvailability,
    MvRepositoryError, MvRepositoryErrorKind, MvTarget, MvTargetLookup, RebuildMvRepositoryRequest,
    RecordExternalCommitAndFinalizeRequest,
};
use novarocks_spi::state_store::{
    Direction, Key, KeyRange, Precondition, RangeRequest, StateRecord, StateStore, WriteTransaction,
};
use novarocks_state_store::metrics::StateStoreMetrics;
use uuid::Uuid;

use self::codec::{
    DecodedMvRecord, MvRecordKind, MvSequence, decode_definition, decode_record, encode_definition,
    encode_record,
};
use self::key::{
    definition_by_id_key, definition_prefix, dependency_by_downstream_key,
    dependency_by_downstream_prefix, dependency_by_upstream_key, dependency_by_upstream_prefix,
    mv_prefix, sequence_key, target_lookup_key,
};

/// The sole MV StateStore repository. It keeps provider transactions private
/// and exposes only the provider-neutral core MV port.
pub struct StateStoreMvRepository {
    store: Arc<dyn StateStore>,
    runtime: tokio::runtime::Handle,
    runner_metrics: StateStoreMetrics,
}

impl StateStoreMvRepository {
    pub async fn open(
        store: Arc<dyn StateStore>,
        runtime: tokio::runtime::Handle,
    ) -> Result<Arc<Self>, MvRepositoryError> {
        let repository = Arc::new(Self {
            runner_metrics: StateStoreMetrics::new(
                novarocks_spi::state_store::StateStoreProviderId::new("frontend-mv"),
            ),
            store,
            runtime,
        });
        repository.validate_open_state().await?;
        Ok(repository)
    }

    fn blocking<T>(
        &self,
        future: impl Future<Output = Result<T, MvRepositoryError>>,
    ) -> Result<T, MvRepositoryError> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(MvRepositoryError::new(
                MvRepositoryErrorKind::InvalidRequest,
                "MV repository synchronous commands must not run on a Tokio runtime worker",
            ));
        }
        self.runtime.block_on(future)
    }

    async fn validate_open_state(&self) -> Result<(), MvRepositoryError> {
        let records = self.scan_prefix(mv_prefix().map_err(corruption)?).await?;
        let mut definitions = BTreeMap::new();
        let mut target_records = Vec::new();
        let mut downstream = BTreeMap::new();
        let mut upstream = BTreeMap::new();
        for record in records {
            match key::decode_key(&record.key).map_err(corruption)?.kind {
                key::MvKeyKind::Sequence => {
                    let sequence: DecodedMvRecord<MvSequence> =
                        decode_record(&record.key, &record.value).map_err(corruption)?;
                    if sequence.value.last_allocated_id < 0 {
                        return Err(corruption("MV sequence must not be negative"));
                    }
                }
                key::MvKeyKind::Definition => {
                    let definition =
                        decode_definition(&record.key, &record.value).map_err(corruption)?;
                    if definition_by_id_key(definition.value.mv_id).map_err(corruption)?
                        != record.key
                    {
                        return Err(corruption("MV definition key does not match its stored ID"));
                    }
                    definitions.insert(definition.value.mv_id, definition.value);
                }
                key::MvKeyKind::TargetLookup => {
                    let lookup: DecodedMvRecord<MvTargetLookup> =
                        decode_record(&record.key, &record.value).map_err(corruption)?;
                    target_records.push((record.key, lookup.value));
                }
                key::MvKeyKind::DependencyDownstream => {
                    let dependency: DecodedMvRecord<StoredMvDependency> =
                        decode_record(&record.key, &record.value).map_err(corruption)?;
                    if dependency_by_downstream_key(
                        dependency.value.downstream_mv_id,
                        &dependency.value.upstream,
                    )
                    .map_err(corruption)?
                        != record.key
                    {
                        return Err(corruption(
                            "MV downstream dependency key does not match its record",
                        ));
                    }
                    downstream.insert(record.key, dependency.value);
                }
                key::MvKeyKind::DependencyUpstream => {
                    let dependency: DecodedMvRecord<StoredMvDependency> =
                        decode_record(&record.key, &record.value).map_err(corruption)?;
                    if dependency_by_upstream_key(
                        &dependency.value.upstream,
                        dependency.value.downstream_mv_id,
                    )
                    .map_err(corruption)?
                        != record.key
                    {
                        return Err(corruption(
                            "MV upstream dependency key does not match its record",
                        ));
                    }
                    upstream.insert(record.key, dependency.value);
                }
                key::MvKeyKind::Refresh | key::MvKeyKind::Partition => {
                    // Task 6 owns the operational validation for these record kinds.
                }
            }
        }
        for (key, lookup) in target_records {
            let definition = definitions
                .get(&lookup.mv_id)
                .ok_or_else(|| corruption("MV target lookup references a missing definition"))?;
            let target = definition_target(definition)?.ok_or_else(|| {
                corruption("MV target lookup references a definition without a target")
            })?;
            if target_lookup_key(
                &target.catalog.unwrap_or_default(),
                &target.database,
                &target.name,
            )
            .map_err(corruption)?
                != key
            {
                return Err(corruption(
                    "MV target lookup key does not match its definition target",
                ));
            }
        }
        for (key, dependency) in &downstream {
            let peer =
                dependency_by_upstream_key(&dependency.upstream, dependency.downstream_mv_id)
                    .map_err(corruption)?;
            if upstream.get(&peer) != Some(dependency) {
                return Err(corruption(format!(
                    "MV dependency index {key:?} has no symmetric upstream record"
                )));
            }
        }
        for (key, dependency) in &upstream {
            let peer =
                dependency_by_downstream_key(dependency.downstream_mv_id, &dependency.upstream)
                    .map_err(corruption)?;
            if downstream.get(&peer) != Some(dependency) {
                return Err(corruption(format!(
                    "MV dependency index {key:?} has no symmetric downstream record"
                )));
            }
        }
        Ok(())
    }

    async fn scan_prefix(&self, prefix: Key) -> Result<Vec<StateRecord>, MvRepositoryError> {
        let range = KeyRange::for_prefix(prefix).map_err(operation::state_store_error)?;
        let mut continuation = None;
        let mut records = Vec::new();
        loop {
            let mut transaction = self
                .store
                .begin_read()
                .await
                .map_err(operation::state_store_error)?;
            let page = transaction
                .range(&RangeRequest {
                    range: range.clone(),
                    direction: Direction::Forward,
                    page_size: self.store.limits().max_page_size,
                    continuation: continuation.clone(),
                })
                .await
                .map_err(operation::state_store_error)?;
            transaction
                .abort()
                .await
                .map_err(operation::state_store_error)?;
            continuation = page.continuation;
            records.extend(page.records);
            if continuation.is_none() {
                return Ok(records);
            }
        }
    }

    async fn read_record(&self, key: &Key) -> Result<Option<StateRecord>, MvRepositoryError> {
        let mut transaction = self
            .store
            .begin_read()
            .await
            .map_err(operation::state_store_error)?;
        let record = transaction
            .get(key)
            .await
            .map_err(operation::state_store_error)?;
        transaction
            .abort()
            .await
            .map_err(operation::state_store_error)?;
        Ok(record)
    }

    async fn create_async(
        &self,
        operation_id: Uuid,
        request: CreateMvRepositoryRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        self.create_with_optional_id_async(operation_id, None, request)
            .await
    }

    async fn create_with_optional_id_async(
        &self,
        operation_id: Uuid,
        explicit_id: Option<i64>,
        request: CreateMvRepositoryRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        validate_create_request(&request)?;
        let recovery_request = request.clone();
        let store = Arc::clone(&self.store);
        let metrics = &self.runner_metrics;
        let outcome =
            operation::run_raw(
                store.as_ref(),
                metrics,
                operation_id,
                "create materialized view",
                move |transaction| {
                    let request = request.clone();
                    Box::pin(async move {
                        let sequence_key = sequence_key().map_err(invalid_state_store)?;
                        let sequence_record = transaction.get(&sequence_key).await?;
                        let last = match &sequence_record {
                            Some(record) => {
                                decode_record::<MvSequence>(&sequence_key, &record.value)
                                    .map_err(invalid_state_store)?
                                    .value
                                    .last_allocated_id
                            }
                            None => 0,
                        };
                        let mv_id =
                            match explicit_id {
                                Some(value) => value,
                                None => last.checked_add(1).filter(|value| *value > 0).ok_or_else(
                                    || invalid_state_store("MV definition ID sequence overflow"),
                                )?,
                            };
                        if mv_id <= 0 {
                            return Err(invalid_state_store("MV definition ID must be positive"));
                        }
                        let definition_key =
                            definition_by_id_key(mv_id).map_err(invalid_state_store)?;
                        if transaction.get(&definition_key).await?.is_some() {
                            return Err(conflict_state_store(format!(
                                "mv definition {mv_id} already exists"
                            )));
                        }
                        let definition = definition_from_request(mv_id, &request);
                        let sequence_value = encode_record(
                            MvRecordKind::Sequence,
                            operation_id,
                            &MvSequence {
                                last_allocated_id: last.max(mv_id),
                            },
                        )
                        .map_err(invalid_state_store)?;
                        let definition_value = encode_definition(operation_id, &definition)
                            .map_err(invalid_state_store)?;
                        if mv_id > last {
                            transaction
                                .put(
                                    sequence_key,
                                    sequence_value,
                                    sequence_record
                                        .map(|record| Precondition::Version(record.version))
                                        .unwrap_or(Precondition::Absent),
                                )
                                .await?;
                        }
                        transaction
                            .put(definition_key, definition_value, Precondition::Absent)
                            .await?;
                        if let Some(target) = definition_target(&definition).map_err(|_| {
                            invalid_state_store("MV definition has an invalid target")
                        })? {
                            let target_key = target_lookup_key(
                                &target.catalog.unwrap_or_default(),
                                &target.database,
                                &target.name,
                            )
                            .map_err(invalid_state_store)?;
                            let target_value = encode_record(
                                MvRecordKind::TargetLookup,
                                operation_id,
                                &MvTargetLookup { mv_id },
                            )
                            .map_err(invalid_state_store)?;
                            transaction
                                .put(target_key, target_value, Precondition::Absent)
                                .await?;
                        }
                        for dependency in deduplicate_dependencies(mv_id, &request.dependencies)
                            .map_err(invalid_state_store)?
                        {
                            put_dependency(
                                transaction,
                                operation_id,
                                &dependency,
                                Precondition::Absent,
                            )
                            .await?;
                        }
                        Ok(definition)
                    })
                },
            )
            .await;
        match outcome {
            Ok(value) => Ok(value),
            Err(novarocks_state_store::RunFailure::CommitUnknown {
                transaction_id,
                error,
            }) => {
                let original = MvRepositoryError::new(
                    MvRepositoryErrorKind::CommitUnknown,
                    format!("MV CREATE commit outcome is unknown: {error}"),
                );
                match operation::resolve_commit(self.store.as_ref(), &transaction_id).await? {
                    novarocks_spi::state_store::CommitResolution::Committed(_) => self
                        .recover_create(operation_id, &recovery_request, original)
                        .await
                        .map_err(|recovery| {
                            if recovery.kind() == MvRepositoryErrorKind::CommitUnknown {
                                corruption(
                                    "MV CREATE committed but its authoritative records are missing",
                                )
                            } else {
                                recovery
                            }
                        }),
                    novarocks_spi::state_store::CommitResolution::NotCommitted => Err(original),
                    novarocks_spi::state_store::CommitResolution::Unresolved => {
                        self.recover_create(operation_id, &recovery_request, original)
                            .await
                    }
                }
            }
            Err(error) => Err(operation::run_failure(error)),
        }
    }

    async fn recover_create(
        &self,
        operation_id: Uuid,
        request: &CreateMvRepositoryRequest,
        original: MvRepositoryError,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        // A durable, exact operation-ID match is authoritative even if the provider
        // cannot yet resolve its commit ledger.
        for definition in self.list_definitions_async().await? {
            let key = definition_by_id_key(definition.mv_id).map_err(corruption)?;
            let Some(record) = self.read_record(&key).await? else {
                continue;
            };
            let matches_definition = decode_definition(&key, &record.value)
                .map(|decoded| decoded.operation_id == operation_id)
                .unwrap_or(false)
                && definition_matches_request(&definition, request);
            if !matches_definition {
                continue;
            }
            if let Some(target) = definition_target(&definition)? {
                let target_key = target_lookup_key(
                    &target.catalog.unwrap_or_default(),
                    &target.database,
                    &target.name,
                )
                .map_err(corruption)?;
                let Some(target_record) = self.read_record(&target_key).await? else {
                    continue;
                };
                let target_matches =
                    decode_record::<MvTargetLookup>(&target_key, &target_record.value)
                        .map(|decoded| {
                            decoded.operation_id == operation_id
                                && decoded.value.mv_id == definition.mv_id
                        })
                        .unwrap_or(false);
                if !target_matches {
                    continue;
                }
            }
            let expected_dependencies =
                deduplicate_dependencies(definition.mv_id, &request.dependencies)
                    .map_err(corruption)?;
            let actual_dependencies = self
                .list_dependencies_downstream_async(definition.mv_id)
                .await?;
            if actual_dependencies.len() != expected_dependencies.len() {
                continue;
            }
            let dependencies_match = expected_dependencies.iter().all(|expected| {
                actual_dependencies.iter().any(|(record, actual)| {
                    actual == expected
                        && decode_record::<StoredMvDependency>(&record.key, &record.value)
                            .map(|decoded| decoded.operation_id == operation_id)
                            .unwrap_or(false)
                })
            });
            if dependencies_match {
                return Ok(definition);
            }
        }
        Err(original)
    }

    async fn reserve_definition_id_async(&self, mv_id: i64) -> Result<(), MvRepositoryError> {
        if mv_id <= 0 {
            return Err(invalid("mv definition id must be positive"));
        }
        let operation_id = Uuid::now_v7();
        let store = Arc::clone(&self.store);
        operation::run(
            store.as_ref(),
            &self.runner_metrics,
            operation_id,
            "reserve materialized view ID",
            move |transaction| {
                Box::pin(async move {
                    let definition_key =
                        definition_by_id_key(mv_id).map_err(invalid_state_store)?;
                    if transaction.get(&definition_key).await?.is_some() {
                        return Err(conflict_state_store(format!(
                            "mv definition {mv_id} already exists"
                        )));
                    }
                    let key = sequence_key().map_err(invalid_state_store)?;
                    let current = transaction.get(&key).await?;
                    let last = match &current {
                        Some(record) => {
                            decode_record::<MvSequence>(&key, &record.value)
                                .map_err(invalid_state_store)?
                                .value
                                .last_allocated_id
                        }
                        None => 0,
                    };
                    if last < mv_id {
                        let value = encode_record(
                            MvRecordKind::Sequence,
                            operation_id,
                            &MvSequence {
                                last_allocated_id: mv_id,
                            },
                        )
                        .map_err(invalid_state_store)?;
                        transaction
                            .put(
                                key,
                                value,
                                current
                                    .map(|record| Precondition::Version(record.version))
                                    .unwrap_or(Precondition::Absent),
                            )
                            .await?;
                    }
                    Ok(())
                })
            },
        )
        .await
    }

    async fn load_by_id_async(
        &self,
        mv_id: i64,
    ) -> Result<Option<StoredMvDefinition>, MvRepositoryError> {
        if mv_id <= 0 {
            return Err(invalid("mv definition id must be positive"));
        }
        let key = definition_by_id_key(mv_id).map_err(invalid)?;
        self.read_record(&key)
            .await?
            .map(|record| {
                decode_definition(&key, &record.value)
                    .map(|decoded| decoded.value)
                    .map_err(corruption)
            })
            .transpose()
    }

    async fn list_definitions_async(&self) -> Result<Vec<StoredMvDefinition>, MvRepositoryError> {
        let mut definitions = self
            .scan_prefix(definition_prefix().map_err(corruption)?)
            .await?
            .into_iter()
            .map(|record| {
                decode_definition(&record.key, &record.value)
                    .map(|decoded| decoded.value)
                    .map_err(corruption)
            })
            .collect::<Result<Vec<_>, _>>()?;
        definitions.sort_by_key(|definition| definition.mv_id);
        Ok(definitions)
    }

    async fn find_by_target_async(
        &self,
        target: &MvTarget,
    ) -> Result<Option<StoredMvDefinition>, MvRepositoryError> {
        let key = target_lookup_key(
            &target.catalog.clone().unwrap_or_default(),
            &target.database,
            &target.name,
        )
        .map_err(invalid)?;
        let Some(record) = self.read_record(&key).await? else {
            return Ok(None);
        };
        let lookup: DecodedMvRecord<MvTargetLookup> =
            decode_record(&key, &record.value).map_err(corruption)?;
        let definition = self
            .load_by_id_async(lookup.value.mv_id)
            .await?
            .ok_or_else(|| corruption("MV target lookup references a missing definition"))?;
        let definition_target = definition_target(&definition)?.ok_or_else(|| {
            corruption("MV target lookup references a definition without a target")
        })?;
        let definition_key = target_lookup_key(
            &definition_target.catalog.unwrap_or_default(),
            &definition_target.database,
            &definition_target.name,
        )
        .map_err(corruption)?;
        if definition_key != key {
            return Err(corruption("MV target lookup does not match its definition"));
        }
        Ok(Some(definition))
    }

    async fn list_dependencies_downstream_async(
        &self,
        mv_id: i64,
    ) -> Result<Vec<(StateRecord, StoredMvDependency)>, MvRepositoryError> {
        let mut dependencies = self
            .scan_prefix(dependency_by_downstream_prefix(mv_id).map_err(corruption)?)
            .await?
            .into_iter()
            .map(|record| {
                let dependency: DecodedMvRecord<StoredMvDependency> =
                    decode_record(&record.key, &record.value).map_err(corruption)?;
                Ok((record, dependency.value))
            })
            .collect::<Result<Vec<_>, MvRepositoryError>>()?;
        dependencies.sort_by(|left, right| {
            dependency_sort_key(&left.1).cmp(&dependency_sort_key(&right.1))
        });
        Ok(dependencies)
    }

    async fn list_dependencies_upstream_async(
        &self,
        upstream: &MvDependencyObjectRef,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        let mut dependencies = self
            .scan_prefix(dependency_by_upstream_prefix(upstream).map_err(corruption)?)
            .await?
            .into_iter()
            .map(|record| {
                decode_record::<StoredMvDependency>(&record.key, &record.value)
                    .map(|decoded| decoded.value)
                    .map_err(corruption)
            })
            .collect::<Result<Vec<_>, _>>()?;
        dependencies.sort_by_key(|dependency| dependency.downstream_mv_id);
        Ok(dependencies)
    }

    async fn replace_dependencies_async(
        &self,
        mv_id: i64,
        requests: Vec<CreateMvDependencyRequest>,
    ) -> Result<(), MvRepositoryError> {
        if mv_id <= 0 {
            return Err(invalid("mv definition id must be positive"));
        }
        let operation_id = Uuid::now_v7();
        let store = Arc::clone(&self.store);
        operation::run(
            store.as_ref(),
            &self.runner_metrics,
            operation_id,
            "replace materialized view dependencies",
            move |transaction| {
                let requests = requests.clone();
                Box::pin(async move {
                    let prefix =
                        dependency_by_downstream_prefix(mv_id).map_err(invalid_state_store)?;
                    let existing = range_transaction(transaction, prefix).await?;
                    let desired =
                        deduplicate_dependencies(mv_id, &requests).map_err(invalid_state_store)?;
                    let desired_by_key = desired
                        .into_iter()
                        .map(|dependency| {
                            dependency_by_downstream_key(mv_id, &dependency.upstream)
                                .map(|key| (key, dependency))
                                .map_err(invalid_state_store)
                        })
                        .collect::<Result<BTreeMap<_, _>, _>>()?;
                    for record in existing {
                        let dependency: DecodedMvRecord<StoredMvDependency> =
                            decode_record(&record.key, &record.value)
                                .map_err(invalid_state_store)?;
                        let upstream_key =
                            dependency_by_upstream_key(&dependency.value.upstream, mv_id)
                                .map_err(invalid_state_store)?;
                        if let Some(replacement) = desired_by_key.get(&record.key) {
                            let upstream =
                                transaction.get(&upstream_key).await?.ok_or_else(|| {
                                    invalid_state_store("MV dependency index is asymmetric")
                                })?;
                            let payload =
                                encode_record(MvRecordKind::Dependency, operation_id, replacement)
                                    .map_err(invalid_state_store)?;
                            transaction
                                .put(
                                    record.key,
                                    payload.clone(),
                                    Precondition::Version(record.version),
                                )
                                .await?;
                            transaction
                                .put(
                                    upstream_key,
                                    payload,
                                    Precondition::Version(upstream.version),
                                )
                                .await?;
                        } else {
                            transaction
                                .delete(record.key, Precondition::Version(record.version))
                                .await?;
                            let upstream =
                                transaction.get(&upstream_key).await?.ok_or_else(|| {
                                    invalid_state_store("MV dependency index is asymmetric")
                                })?;
                            transaction
                                .delete(upstream_key, Precondition::Version(upstream.version))
                                .await?;
                        }
                    }
                    for (key, dependency) in desired_by_key {
                        if transaction.get(&key).await?.is_none() {
                            put_dependency(
                                transaction,
                                operation_id,
                                &dependency,
                                Precondition::Absent,
                            )
                            .await?;
                        }
                    }
                    Ok(())
                })
            },
        )
        .await
    }

    async fn delete_dependencies_async(&self, mv_id: i64) -> Result<(), MvRepositoryError> {
        self.replace_dependencies_async(mv_id, Vec::new()).await
    }

    async fn drop_by_id_async(&self, mv_id: i64) -> Result<bool, MvRepositoryError> {
        if mv_id <= 0 {
            return Err(invalid("mv definition id must be positive"));
        }
        let operation_id = Uuid::now_v7();
        let store = Arc::clone(&self.store);
        operation::run(
            store.as_ref(),
            &self.runner_metrics,
            operation_id,
            "drop materialized view definition",
            move |transaction| {
                Box::pin(async move {
                    let definition_key =
                        definition_by_id_key(mv_id).map_err(invalid_state_store)?;
                    let Some(record) = transaction.get(&definition_key).await? else {
                        return Ok(false);
                    };
                    let definition = decode_definition(&definition_key, &record.value)
                        .map_err(invalid_state_store)?
                        .value;
                    if definition.refresh_in_progress || definition.active_refresh_id.is_some() {
                        return Err(conflict_state_store(format!(
                            "mv definition {mv_id} has refresh in progress"
                        )));
                    }
                    if let Some(target) = definition_target(&definition)
                        .map_err(|_| invalid_state_store("MV definition has an invalid target"))?
                    {
                        let target_key = target_lookup_key(
                            &target.catalog.unwrap_or_default(),
                            &target.database,
                            &target.name,
                        )
                        .map_err(invalid_state_store)?;
                        let target_record =
                            transaction.get(&target_key).await?.ok_or_else(|| {
                                invalid_state_store("MV definition target lookup is missing")
                            })?;
                        transaction
                            .delete(target_key, Precondition::Version(target_record.version))
                            .await?;
                    }
                    let prefix =
                        dependency_by_downstream_prefix(mv_id).map_err(invalid_state_store)?;
                    for dependency_record in range_transaction(transaction, prefix).await? {
                        let dependency: DecodedMvRecord<StoredMvDependency> =
                            decode_record(&dependency_record.key, &dependency_record.value)
                                .map_err(invalid_state_store)?;
                        let upstream_key =
                            dependency_by_upstream_key(&dependency.value.upstream, mv_id)
                                .map_err(invalid_state_store)?;
                        let upstream_record =
                            transaction.get(&upstream_key).await?.ok_or_else(|| {
                                invalid_state_store("MV dependency index is asymmetric")
                            })?;
                        transaction
                            .delete(
                                dependency_record.key,
                                Precondition::Version(dependency_record.version),
                            )
                            .await?;
                        transaction
                            .delete(upstream_key, Precondition::Version(upstream_record.version))
                            .await?;
                    }
                    transaction
                        .delete(definition_key, Precondition::Version(record.version))
                        .await?;
                    Ok(true)
                })
            },
        )
        .await
    }
}

impl MvRepository for StateStoreMvRepository {
    fn availability(&self) -> MvRepositoryAvailability {
        MvRepositoryAvailability::Available
    }
    fn create(
        &self,
        operation_id: Uuid,
        request: CreateMvRepositoryRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        self.blocking(self.create_async(operation_id, request))
    }
    fn create_with_id(
        &self,
        operation_id: Uuid,
        request: CreateMvRepositoryWithIdRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        self.blocking(self.create_with_optional_id_async(
            operation_id,
            Some(request.mv_id),
            request.create,
        ))
    }
    fn rebuild(
        &self,
        operation_id: Uuid,
        request: RebuildMvRepositoryRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        self.blocking(self.create_async(operation_id, request.create))
    }
    fn reserve_definition_id(&self, mv_id: i64) -> Result<(), MvRepositoryError> {
        self.blocking(self.reserve_definition_id_async(mv_id))
    }
    fn load_by_id(&self, mv_id: i64) -> Result<Option<StoredMvDefinition>, MvRepositoryError> {
        self.blocking(self.load_by_id_async(mv_id))
    }
    fn find_by_target(
        &self,
        target: &MvTarget,
    ) -> Result<Option<StoredMvDefinition>, MvRepositoryError> {
        self.blocking(self.find_by_target_async(target))
    }
    fn list_definitions(&self) -> Result<Vec<StoredMvDefinition>, MvRepositoryError> {
        self.blocking(self.list_definitions_async())
    }
    fn drop_by_id(&self, mv_id: i64) -> Result<bool, MvRepositoryError> {
        self.blocking(self.drop_by_id_async(mv_id))
    }
    fn drop_by_target(&self, target: &MvTarget) -> Result<bool, MvRepositoryError> {
        match self.find_by_target(target)? {
            Some(definition) => self.drop_by_id(definition.mv_id),
            None => Ok(false),
        }
    }
    fn replace_dependencies_for_mv(
        &self,
        mv_id: i64,
        dependencies: Vec<CreateMvDependencyRequest>,
    ) -> Result<(), MvRepositoryError> {
        self.blocking(self.replace_dependencies_async(mv_id, dependencies))
    }
    fn delete_dependencies_for_mv(&self, mv_id: i64) -> Result<(), MvRepositoryError> {
        self.blocking(self.delete_dependencies_async(mv_id))
    }
    fn ensure_no_downstream_dependencies(
        &self,
        upstream: &MvDependencyObjectRef,
    ) -> Result<(), MvRepositoryError> {
        let dependencies = self.blocking(self.list_dependencies_upstream_async(upstream))?;
        if dependencies.is_empty() {
            Ok(())
        } else {
            Err(MvRepositoryError::new(
                MvRepositoryErrorKind::Conflict,
                format!(
                    "{} has downstream materialized views: {}",
                    upstream.display_name(),
                    dependencies
                        .iter()
                        .map(|dependency| dependency.downstream_mv_id.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            ))
        }
    }
    fn list_dependencies_by_downstream(
        &self,
        mv_id: i64,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        self.blocking(async {
            Ok(self
                .list_dependencies_downstream_async(mv_id)
                .await?
                .into_iter()
                .map(|(_, dependency)| dependency)
                .collect())
        })
    }
    fn list_downstream_dependencies(
        &self,
        upstream: &MvDependencyObjectRef,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        self.blocking(self.list_dependencies_upstream_async(upstream))
    }
    fn set_rebuilt_refresh_watermark(
        &self,
        _mv_id: i64,
        _base_snapshots: BTreeMap<String, i64>,
        _base_table_uuids: BTreeMap<String, String>,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        task_six()
    }
    fn update_refresh_metadata(
        &self,
        _request: UpdateMvRefreshMetadataRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        task_six()
    }
    fn update_partition_contract(
        &self,
        _request: UpdateMvPartitionContractRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        task_six()
    }
    fn begin_refresh_intent(
        &self,
        _mv_id: i64,
        _target_snapshots: BTreeMap<String, i64>,
    ) -> Result<StoredMvRefresh, MvRepositoryError> {
        task_six()
    }
    fn begin_iceberg_refresh_intent(
        &self,
        _request: BeginIcebergMvRefreshRequest,
    ) -> Result<StoredMvRefresh, MvRepositoryError> {
        task_six()
    }
    fn record_staging_commit(
        &self,
        _request: RecordStagingCommitRequest,
    ) -> Result<(), MvRepositoryError> {
        task_six()
    }
    fn record_publish_commit(
        &self,
        _request: RecordPublishCommitRequest,
    ) -> Result<(), MvRepositoryError> {
        task_six()
    }
    fn mark_refresh_commit_unknown(&self, _refresh_id: i64) -> Result<(), MvRepositoryError> {
        task_six()
    }
    fn record_external_commit_outcome(
        &self,
        _refresh_id: i64,
        _outcome: RefreshExternalOutcome,
    ) -> Result<(), MvRepositoryError> {
        task_six()
    }
    fn finalize_refresh(
        &self,
        _request: MvRefreshFinalizeRequest,
    ) -> Result<(), MvRepositoryError> {
        task_six()
    }
    fn finalize_refresh_with_partitions(
        &self,
        _request: FinalizeMvRefreshWithPartitionsRequest,
    ) -> Result<(), MvRepositoryError> {
        task_six()
    }
    fn record_external_commit_and_finalize(
        &self,
        _request: RecordExternalCommitAndFinalizeRequest,
    ) -> Result<(), MvRepositoryError> {
        task_six()
    }
    fn clear_refresh_progress(&self, _mv_id: i64) -> Result<bool, MvRepositoryError> {
        task_six()
    }
    fn load_refresh(&self, _refresh_id: i64) -> Result<Option<StoredMvRefresh>, MvRepositoryError> {
        task_six()
    }
    fn list_unfinished_refreshes(&self) -> Result<Vec<StoredMvRefresh>, MvRepositoryError> {
        task_six()
    }
    fn list_unfinished_branch_staged_iceberg_refreshes(
        &self,
    ) -> Result<Vec<StoredMvRefresh>, MvRepositoryError> {
        task_six()
    }
    fn update_starrocks_refresh_summary_if_present(
        &self,
        _request: UpdateStarRocksMvRefreshSummaryRequest,
    ) -> Result<bool, MvRepositoryError> {
        task_six()
    }
    fn replace_partition_states(
        &self,
        _request: ReplaceMvPartitionStatesRequest,
    ) -> Result<(), MvRepositoryError> {
        task_six()
    }
    fn record_failed_partition_states(
        &self,
        _request: RecordFailedMvPartitionStatesRequest,
    ) -> Result<(), MvRepositoryError> {
        task_six()
    }
    fn clear_partition_states(&self, _mv_id: i64) -> Result<bool, MvRepositoryError> {
        task_six()
    }
    fn list_partition_states(
        &self,
        _mv_id: i64,
    ) -> Result<Vec<StoredMvPartitionState>, MvRepositoryError> {
        task_six()
    }
    fn adopt_target_compaction_snapshot(
        &self,
        _target: &MvTarget,
        _expected_snapshot_id: i64,
        _adopted_snapshot_id: i64,
    ) -> Result<bool, MvRepositoryError> {
        task_six()
    }
}

fn definition_from_request(mv_id: i64, request: &CreateMvRepositoryRequest) -> StoredMvDefinition {
    StoredMvDefinition {
        mv_id,
        select_sql: request.definition.select_sql.clone(),
        base_table_refs: request.definition.base_table_refs.clone(),
        primary_key_columns: request.definition.primary_key_columns.clone(),
        storage_engine: request.definition.storage_engine.clone(),
        target_catalog: request.definition.target_catalog.clone(),
        target_namespace: request.definition.target_namespace.clone(),
        target_table: request.definition.target_table.clone(),
        schema_contract: request.definition.schema_contract.clone(),
        partition_spec: request.definition.partition_spec.clone(),
        partition_state_complete: false,
        last_refresh_ms: None,
        last_refresh_rows: None,
        last_refresh_snapshots: BTreeMap::new(),
        last_refresh_table_uuids: BTreeMap::new(),
        last_refreshed_iceberg_snapshot_id: None,
        refresh_in_progress: false,
        active_refresh_id: None,
        refresh_target_snapshots: BTreeMap::new(),
        refresh_policy: request.refresh.policy.clone(),
        refresh_paused: request.refresh.paused,
        refresh_interval_ms: request.refresh.interval_ms,
        max_staleness_ms: request.refresh.max_staleness_ms,
        last_scheduler_error: None,
        next_refresh_after_ms: request.refresh.next_refresh_after_ms,
        created_at_ms: request.definition.created_at_ms,
    }
}

fn definition_matches_request(
    definition: &StoredMvDefinition,
    request: &CreateMvRepositoryRequest,
) -> bool {
    definition.select_sql == request.definition.select_sql
        && definition.base_table_refs == request.definition.base_table_refs
        && definition.primary_key_columns == request.definition.primary_key_columns
        && definition.storage_engine == request.definition.storage_engine
        && definition.target_catalog == request.definition.target_catalog
        && definition.target_namespace == request.definition.target_namespace
        && definition.target_table == request.definition.target_table
        && definition.schema_contract == request.definition.schema_contract
        && definition.partition_spec == request.definition.partition_spec
        && definition.created_at_ms == request.definition.created_at_ms
        && definition.refresh_policy == request.refresh.policy
        && definition.refresh_paused == request.refresh.paused
        && definition.refresh_interval_ms == request.refresh.interval_ms
        && definition.max_staleness_ms == request.refresh.max_staleness_ms
        && definition.next_refresh_after_ms == request.refresh.next_refresh_after_ms
}

fn validate_create_request(request: &CreateMvRepositoryRequest) -> Result<(), MvRepositoryError> {
    let target_fields = [
        &request.definition.target_catalog,
        &request.definition.target_namespace,
        &request.definition.target_table,
    ];
    if target_fields.iter().any(|field| field.is_some())
        && target_fields.iter().any(|field| field.is_none())
    {
        return Err(invalid(
            "MV definition target catalog, namespace, and table must be set together",
        ));
    }
    Ok(())
}

fn definition_target(
    definition: &StoredMvDefinition,
) -> Result<Option<MvTarget>, MvRepositoryError> {
    match (
        &definition.target_catalog,
        &definition.target_namespace,
        &definition.target_table,
    ) {
        (None, None, None) => Ok(None),
        (Some(catalog), Some(database), Some(name)) => Ok(Some(MvTarget {
            catalog: Some(catalog.clone()),
            database: database.clone(),
            name: name.clone(),
        })),
        _ => Err(corruption("MV definition has a partial target identity")),
    }
}

fn deduplicate_dependencies(
    mv_id: i64,
    requests: &[CreateMvDependencyRequest],
) -> Result<Vec<StoredMvDependency>, String> {
    let mut seen = BTreeSet::new();
    let mut dependencies = Vec::new();
    for request in requests {
        let key = dependency_by_downstream_key(mv_id, &request.upstream)?;
        if seen.insert(key) {
            dependencies.push(StoredMvDependency {
                downstream_mv_id: mv_id,
                upstream: request.upstream.clone(),
                created_at_ms: request.created_at_ms,
            });
        }
    }
    Ok(dependencies)
}

async fn put_dependency(
    transaction: &mut dyn WriteTransaction,
    operation_id: Uuid,
    dependency: &StoredMvDependency,
    precondition: Precondition,
) -> Result<(), novarocks_spi::state_store::StateStoreError> {
    let payload = encode_record(MvRecordKind::Dependency, operation_id, dependency)
        .map_err(invalid_state_store)?;
    let downstream =
        dependency_by_downstream_key(dependency.downstream_mv_id, &dependency.upstream)
            .map_err(invalid_state_store)?;
    let upstream = dependency_by_upstream_key(&dependency.upstream, dependency.downstream_mv_id)
        .map_err(invalid_state_store)?;
    transaction
        .put(downstream, payload.clone(), precondition.clone())
        .await?;
    transaction.put(upstream, payload, precondition).await
}

async fn range_transaction(
    transaction: &mut dyn WriteTransaction,
    prefix: Key,
) -> Result<Vec<StateRecord>, novarocks_spi::state_store::StateStoreError> {
    let range = KeyRange::for_prefix(prefix)?;
    let page_size = 256;
    let mut continuation = None;
    let mut records = Vec::new();
    loop {
        let page = transaction
            .range(&RangeRequest {
                range: range.clone(),
                direction: Direction::Forward,
                page_size,
                continuation: continuation.clone(),
            })
            .await?;
        continuation = page.continuation;
        records.extend(page.records);
        if continuation.is_none() {
            return Ok(records);
        }
    }
}

fn dependency_sort_key(
    dependency: &StoredMvDependency,
) -> (
    &Option<String>,
    &String,
    &String,
    &novarocks::mv::dependency::model::MvDependencyObjectType,
    &novarocks::mv::dependency::model::MvDependencyStorageEngine,
) {
    (
        &dependency.upstream.catalog,
        &dependency.upstream.database_or_namespace,
        &dependency.upstream.name,
        &dependency.upstream.object_type,
        &dependency.upstream.storage_engine,
    )
}
fn invalid(message: impl Into<String>) -> MvRepositoryError {
    MvRepositoryError::new(MvRepositoryErrorKind::InvalidRequest, message)
}
fn corruption(message: impl Into<String>) -> MvRepositoryError {
    MvRepositoryError::new(MvRepositoryErrorKind::Corruption, message)
}
fn invalid_state_store(_message: impl Into<String>) -> novarocks_spi::state_store::StateStoreError {
    novarocks_spi::state_store::StateStoreError::new(
        novarocks_spi::state_store::StateStoreErrorKind::InvalidRequest,
        "invalid MV StateStore request",
    )
}
fn conflict_state_store(
    _message: impl Into<String>,
) -> novarocks_spi::state_store::StateStoreError {
    novarocks_spi::state_store::StateStoreError::new(
        novarocks_spi::state_store::StateStoreErrorKind::Conflict,
        "MV StateStore transaction conflict",
    )
}
fn task_six<T>() -> Result<T, MvRepositoryError> {
    Err(MvRepositoryError::new(
        MvRepositoryErrorKind::Unavailable,
        "MV refresh and partition repository commands are not available until Task 6",
    ))
}
