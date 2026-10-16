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
pub mod keyed;
mod operation;

#[cfg(test)]
#[path = "tests_definition.rs"]
mod tests_definition;
#[cfg(test)]
#[path = "tests_dependency.rs"]
mod tests_dependency;
#[cfg(test)]
mod tests_inventory;
#[cfg(test)]
#[path = "tests_port.rs"]
mod tests_port;

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks_state_store_api::{
    Direction, Key, KeyRange, Precondition, RangeRequest, ReadTransaction, StateRecord, StateStore,
    StateStoreError, StateStoreErrorKind, WriteTransaction,
};
use uuid::Uuid;

use crate::dependency::MvDependencyObjectRef;
use crate::persistence::dependency::StoredMvDependency;
use crate::product::MvTarget;
use crate::repository::{
    MvDependencyReadBound, MvProjectionInventoryBound, MvProjectionInventoryBuilder,
    MvProjectionInventoryEntry, MvRepositoryError, MvRepositoryErrorKind, MvTargetLookup,
};
use crate::repository_metrics::MvRepositoryMetrics;
use novarocks_state_store_runtime::StateStoreRunPolicy;

use self::codec::{
    DecodedMvRecord, MvRecordKind, MvSequence, decode_dependency_with_budget,
    decode_keyed_projection, decode_record, encode_record,
};
use self::key::{
    dependency_by_downstream_key, dependency_by_downstream_prefix,
    dependency_by_upstream_catalog_prefixes, dependency_by_upstream_key,
    dependency_by_upstream_prefix, projection_by_id_key, projection_prefix, sequence_key,
    target_lookup_catalog_prefix, target_lookup_key,
};

/// StateStore owner for the single current MV Accelerator family.
pub struct StateStoreMvRepository {
    store: Arc<dyn StateStore>,
    run_policy: StateStoreRunPolicy,
    runner_metrics: MvRepositoryMetrics,
}

impl StateStoreMvRepository {
    /// Opens the repository against one store instance under one application
    /// run policy.
    ///
    /// The policy is supplied rather than read from the store: how many
    /// attempts an MV write is worth and how long the caller will wait are
    /// application decisions, and a provider no longer reports an attempt
    /// ceiling for anyone to borrow.
    pub async fn open(
        store: Arc<dyn StateStore>,
        run_policy: StateStoreRunPolicy,
    ) -> Result<Arc<Self>, MvRepositoryError> {
        Ok(Arc::new(Self {
            runner_metrics: MvRepositoryMetrics::default(),
            store,
            run_policy,
        }))
    }

    async fn scan_prefix(&self, prefix: Key) -> Result<Vec<StateRecord>, MvRepositoryError> {
        scan_read_prefix(
            self.store.as_ref(),
            prefix,
            self.store.limits().max_page_size,
        )
        .await
        .map_err(operation::state_store_error)
    }

    async fn classified_dependencies(
        &self,
        prefix: Key,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        let mut transaction = self
            .store
            .begin_read()
            .await
            .map_err(operation::state_store_error)?;
        let page_size = self.store.limits().max_page_size;
        let rows = scan_transaction_range(
            transaction.as_mut(),
            KeyRange::for_prefix(prefix.clone()).map_err(operation::state_store_error)?,
            page_size,
        )
        .await
        .map_err(operation::state_store_error)?;
        let roots = scan_transaction_range(
            transaction.as_mut(),
            KeyRange::for_prefix(projection_prefix().map_err(corruption)?)
                .map_err(operation::state_store_error)?,
            page_size,
        )
        .await
        .map_err(operation::state_store_error)?;
        // A failed or partial inventory is not evidence that an occurrence is external.
        let inventory = roots
            .into_iter()
            .map(|record| {
                decode_keyed_projection(
                    &record.key,
                    &record.value,
                    crate::persistence::validation::PersistenceDecodeBudget::default(),
                )
                .map(|decoded| decoded.value)
                .map_err(corruption)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut dependencies = decode_dependencies(rows)?;
        let mut expected = Vec::new();
        for root in &inventory {
            for dependency in keyed::keyed_dependencies(root.mv_id, &root.facts) {
                let downstream_key = dependency_by_downstream_key(
                    root.mv_id,
                    &dependency.upstream,
                    dependency.occurrence_id,
                )
                .map_err(corruption)?;
                let upstream_key = dependency_by_upstream_key(
                    &dependency.upstream,
                    root.mv_id,
                    dependency.occurrence_id,
                )
                .map_err(corruption)?;
                if downstream_key.as_bytes().starts_with(prefix.as_bytes())
                    || upstream_key.as_bytes().starts_with(prefix.as_bytes())
                {
                    expected.push(dependency);
                }
            }
        }
        if dependencies.len() != expected.len()
            || dependencies
                .iter()
                .any(|dependency| !expected.contains(dependency))
        {
            return Err(corruption(
                "MV dependency index is incomplete or differs from its canonical occurrences",
            ));
        }
        let inventory = inventory
            .into_iter()
            .map(|root| MvProjectionInventoryEntry {
                mv_id: root.mv_id,
                target: root.facts.target(),
                object_id: root.facts.source_revision().target_object_id.clone(),
            })
            .collect::<Vec<_>>();
        crate::bounded_dependencies::classify(&mut dependencies, &inventory);
        transaction
            .abort()
            .await
            .map_err(operation::state_store_error)?;
        Ok(dependencies)
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
}

async fn scan_transaction_range(
    transaction: &mut dyn ReadTransaction,
    range: KeyRange,
    page_size: usize,
) -> Result<Vec<StateRecord>, StateStoreError> {
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

async fn scan_read_prefix(
    store: &dyn StateStore,
    prefix: Key,
    page_size: usize,
) -> Result<Vec<StateRecord>, StateStoreError> {
    let range = KeyRange::for_prefix(prefix)?;
    let mut transaction = store.begin_read().await?;
    let records = scan_transaction_range(transaction.as_mut(), range, page_size).await?;
    transaction.abort().await?;
    Ok(records)
}

async fn scan_write_prefix(
    transaction: &mut dyn WriteTransaction,
    prefix: Key,
    page_size: usize,
) -> Result<Vec<StateRecord>, StateStoreError> {
    let range = KeyRange::for_prefix(prefix)?;
    scan_transaction_range(transaction, range, page_size).await
}

/// How the current Accelerator names a catalog.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvCatalogReference {
    /// A current MV writes its projection into this catalog.
    Target,
    /// A current MV reads an upstream object out of this catalog.
    UpstreamDependency,
}

impl MvCatalogReference {
    pub const fn describe(self) -> &'static str {
        match self {
            Self::Target => "a materialized view target",
            Self::UpstreamDependency => "materialized view upstream dependencies",
        }
    }
}

/// Reads whether the current Accelerator still names `catalog` as an MV target
/// or upstream dependency.  Only current keys are consulted; historical MV
/// families are intentionally not compatibility data.
///
/// This is a plain read path, and deliberately so.  The same two prefixes used
/// to be scanned inside the catalog attachment delete transaction, which read
/// like a cross-family serializability fence between `DROP CATALOG` and MV DDL.
/// It never was one: the family scanned here is a rebuildable Accelerator that
/// may legitimately be wiped in whole, so the "guarantee" evaporated exactly
/// when the cache was cleared, and the parties that actually race — concurrent
/// MV DDL and an external catalog desired-state controller — were never
/// participants in that transaction anyway.  Keeping the scan transactional
/// only advertised a tool that does not exist.  The caller therefore treats
/// this as an observation, not a fence; see
/// `CatalogAttachmentRepository::observe_materialized_view_references`.
pub async fn observe_catalog_references(
    store: &dyn StateStore,
    catalog: &str,
    page_size: usize,
) -> Result<Option<MvCatalogReference>, StateStoreError> {
    if !scan_read_prefix(
        store,
        target_lookup_catalog_prefix(catalog).map_err(invalid_state_store)?,
        page_size,
    )
    .await?
    .is_empty()
    {
        return Ok(Some(MvCatalogReference::Target));
    }
    for prefix in dependency_by_upstream_catalog_prefixes(catalog).map_err(invalid_state_store)? {
        if !scan_read_prefix(store, prefix, page_size).await?.is_empty() {
            return Ok(Some(MvCatalogReference::UpstreamDependency));
        }
    }
    Ok(None)
}

/// One record per provider call, one continuation, no retained raw stream.
struct BoundedRecordRange {
    range: KeyRange,
    continuation: Option<novarocks_state_store_api::ContinuationToken>,
    previous: Option<Key>,
    exhausted: bool,
}

impl BoundedRecordRange {
    fn new(prefix: Key) -> Result<Self, MvRepositoryError> {
        Ok(Self {
            range: KeyRange::for_prefix(prefix).map_err(operation::state_store_error)?,
            continuation: None,
            previous: None,
            exhausted: false,
        })
    }

    async fn next(
        &mut self,
        transaction: &mut dyn ReadTransaction,
        bound: MvProjectionInventoryBound,
    ) -> Result<Option<StateRecord>, MvRepositoryError> {
        if self.exhausted {
            return Ok(None);
        }
        let request = RangeRequest {
            range: self.range.clone(),
            direction: Direction::Forward,
            page_size: 1,
            continuation: self.continuation.clone(),
        };
        let page = transaction
            .range(&request)
            .await
            .map_err(operation::state_store_error)?;
        if page.records.len() > 1 || (page.records.is_empty() && page.continuation.is_some()) {
            return Err(corruption(
                "MV inventory provider violated the exact page bound",
            ));
        }
        if page
            .continuation
            .as_ref()
            .is_some_and(|token| token.as_bytes().len() > bound.continuation_token_bytes)
        {
            return Err(invalid(
                "MV inventory exceeds its continuation token byte bound",
            ));
        }
        if page.continuation.is_some() && page.continuation == self.continuation {
            return Err(corruption("MV inventory continuation made no progress"));
        }
        let cursor_bytes = self
            .previous
            .as_ref()
            .map_or(0, |key| key.as_bytes().len())
            .saturating_add(
                self.continuation
                    .as_ref()
                    .map_or(0, |token| token.as_bytes().len()),
            );
        let Some(record) = page.records.into_iter().next() else {
            self.exhausted = true;
            return Ok(None);
        };
        // Account old cursor, provider page and exact cursor copies together.
        let raw_peak = record
            .value
            .as_bytes()
            .len()
            .saturating_add(record.key.as_bytes().len().saturating_mul(2))
            .saturating_add(
                page.continuation
                    .as_ref()
                    .map_or(0, |token| token.as_bytes().len())
                    .saturating_mul(2),
            )
            .saturating_add(cursor_bytes);
        if raw_peak > bound.raw_page_bytes {
            return Err(invalid("MV inventory exceeds its raw page byte bound"));
        }
        if let Some(token) = &page.continuation {
            if token
                .resume_after(&request)
                .map_err(operation::state_store_error)?
                != record.key
            {
                return Err(corruption(
                    "MV inventory continuation differs from its last page key",
                ));
            }
        }
        self.exhausted = page.continuation.is_none();
        self.continuation = page
            .continuation
            .map(|token| {
                novarocks_state_store_api::ContinuationToken::try_from(
                    bytes::Bytes::copy_from_slice(token.as_bytes()),
                )
            })
            .transpose()
            .map_err(operation::state_store_error)?;
        check_inventory_raw_record(&record, bound)?;
        if record.key < self.range.start
            || record.key >= self.range.end
            || self
                .previous
                .as_ref()
                .is_some_and(|previous| previous >= &record.key)
        {
            return Err(corruption(
                "MV inventory page key is out of range or made no progress",
            ));
        }
        self.previous = Some(
            Key::try_from(bytes::Bytes::copy_from_slice(record.key.as_bytes()))
                .map_err(operation::state_store_error)?,
        );
        Ok(Some(record))
    }
}

fn check_inventory_raw_record(
    record: &StateRecord,
    bound: MvProjectionInventoryBound,
) -> Result<(), MvRepositoryError> {
    if record
        .key
        .as_bytes()
        .len()
        .checked_add(record.value.as_bytes().len())
        .is_none_or(|bytes| bytes > bound.raw_page_bytes)
    {
        return Err(MvRepositoryError::new(
            MvRepositoryErrorKind::InvalidRequest,
            "MV inventory exceeds its raw page byte bound",
        ));
    }
    Ok(())
}

fn target_key(target: &MvTarget) -> Result<Key, String> {
    target_lookup_key(
        target.catalog().unwrap_or_default(),
        target.namespace(),
        target.name(),
    )
}

fn dependencies_for_mv(mv_id: i64, requests: &[StoredMvDependency]) -> Vec<StoredMvDependency> {
    requests
        .iter()
        .cloned()
        .map(|mut dependency| {
            dependency.downstream_mv_id = mv_id;
            dependency
        })
        .collect()
}

async fn put_sequence(
    transaction: &mut dyn WriteTransaction,
    operation_id: Uuid,
    sequence: MvSequence,
    precondition: Precondition,
) -> Result<(), StateStoreError> {
    let key = sequence_key().map_err(invalid_state_store)?;
    let value = encode_record(MvRecordKind::Sequence, operation_id, &sequence)
        .map_err(invalid_state_store)?;
    transaction.put(key, value, precondition).await
}

async fn put_target_lookup(
    transaction: &mut dyn WriteTransaction,
    operation_id: Uuid,
    key: Key,
    mv_id: i64,
    precondition: Precondition,
) -> Result<(), StateStoreError> {
    let value = encode_record(
        MvRecordKind::TargetLookup,
        operation_id,
        &MvTargetLookup { mv_id },
    )
    .map_err(invalid_state_store)?;
    transaction.put(key, value, precondition).await
}

async fn put_dependency_pair(
    transaction: &mut dyn WriteTransaction,
    operation_id: Uuid,
    dependency: &StoredMvDependency,
) -> Result<(), StateStoreError> {
    let downstream = dependency_by_downstream_key(
        dependency.downstream_mv_id,
        &dependency.upstream,
        dependency.occurrence_id,
    )
    .map_err(invalid_state_store)?;
    let upstream = dependency_by_upstream_key(
        &dependency.upstream,
        dependency.downstream_mv_id,
        dependency.occurrence_id,
    )
    .map_err(invalid_state_store)?;
    if transaction.get(&downstream).await?.is_some() || transaction.get(&upstream).await?.is_some()
    {
        return Err(conflict_state_store("MV dependency index already exists"));
    }
    let value = encode_record(MvRecordKind::Dependency, operation_id, dependency)
        .map_err(invalid_state_store)?;
    transaction
        .put(downstream, value.clone(), Precondition::Absent)
        .await?;
    transaction.put(upstream, value, Precondition::Absent).await
}

async fn replace_dependency_indexes(
    transaction: &mut dyn WriteTransaction,
    operation_id: Uuid,
    mv_id: i64,
    desired: &[StoredMvDependency],
    page_size: usize,
) -> Result<(), StateStoreError> {
    let records = scan_write_prefix(
        transaction,
        dependency_by_downstream_prefix(mv_id).map_err(invalid_state_store)?,
        page_size,
    )
    .await?;
    let mut current = BTreeMap::new();
    for downstream in records {
        let dependency: DecodedMvRecord<StoredMvDependency> =
            decode_record(&downstream.key, &downstream.value).map_err(invalid_state_store)?;
        let upstream_key = dependency_by_upstream_key(
            &dependency.value.upstream,
            mv_id,
            dependency.value.occurrence_id,
        )
        .map_err(invalid_state_store)?;
        let upstream = transaction
            .get(&upstream_key)
            .await?
            .ok_or_else(|| invalid_state_store("MV dependency index is asymmetric"))?;
        current.insert(downstream.key.clone(), (downstream, upstream_key, upstream));
    }
    let desired = dependencies_for_mv(mv_id, desired)
        .into_iter()
        .map(|dependency| {
            let key = dependency_by_downstream_key(
                mv_id,
                &dependency.upstream,
                dependency.occurrence_id,
            )?;
            Ok((key, dependency))
        })
        .collect::<Result<BTreeMap<_, _>, String>>()
        .map_err(invalid_state_store)?;
    for (key, (downstream, upstream_key, upstream)) in current {
        if let Some(dependency) = desired.get(&key) {
            let value = encode_record(MvRecordKind::Dependency, operation_id, dependency)
                .map_err(invalid_state_store)?;
            transaction
                .put(
                    key,
                    value.clone(),
                    Precondition::Version(downstream.version),
                )
                .await?;
            transaction
                .put(upstream_key, value, Precondition::Version(upstream.version))
                .await?;
        } else {
            transaction
                .delete(key, Precondition::Version(downstream.version))
                .await?;
            transaction
                .delete(upstream_key, Precondition::Version(upstream.version))
                .await?;
        }
    }
    for (key, dependency) in desired {
        if transaction.get(&key).await?.is_none() {
            put_dependency_pair(transaction, operation_id, &dependency).await?;
        }
    }
    Ok(())
}

async fn delete_indexes_for_target(
    transaction: &mut dyn WriteTransaction,
    target: &MvTarget,
    mv_id: i64,
    page_size: usize,
) -> Result<(), StateStoreError> {
    let target_key = target_key(target).map_err(invalid_state_store)?;
    let target = transaction
        .get(&target_key)
        .await?
        .ok_or_else(|| invalid_state_store("MV target lookup is missing"))?;
    let lookup: DecodedMvRecord<MvTargetLookup> =
        decode_record(&target_key, &target.value).map_err(invalid_state_store)?;
    if lookup.value.mv_id != mv_id {
        return Err(invalid_state_store(
            "MV target lookup references a different projection",
        ));
    }
    transaction
        .delete(target_key, Precondition::Version(target.version))
        .await?;
    let downstream = scan_write_prefix(
        transaction,
        dependency_by_downstream_prefix(mv_id).map_err(invalid_state_store)?,
        page_size,
    )
    .await?;
    for record in downstream {
        let dependency: DecodedMvRecord<StoredMvDependency> =
            decode_record(&record.key, &record.value).map_err(invalid_state_store)?;
        let upstream_key = dependency_by_upstream_key(
            &dependency.value.upstream,
            mv_id,
            dependency.value.occurrence_id,
        )
        .map_err(invalid_state_store)?;
        let upstream = transaction
            .get(&upstream_key)
            .await?
            .ok_or_else(|| invalid_state_store("MV dependency index is asymmetric"))?;
        transaction
            .delete(record.key, Precondition::Version(record.version))
            .await?;
        transaction
            .delete(upstream_key, Precondition::Version(upstream.version))
            .await?;
    }
    Ok(())
}

fn decode_dependencies(
    records: Vec<StateRecord>,
) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
    let mut dependencies = records
        .into_iter()
        .map(|record| {
            let dependency = decode_record::<StoredMvDependency>(&record.key, &record.value)
                .map_err(corruption)?
                .value;
            let downstream_key = dependency_by_downstream_key(
                dependency.downstream_mv_id,
                &dependency.upstream,
                dependency.occurrence_id,
            )
            .map_err(corruption)?;
            let upstream_key = dependency_by_upstream_key(
                &dependency.upstream,
                dependency.downstream_mv_id,
                dependency.occurrence_id,
            )
            .map_err(corruption)?;
            if record.key != downstream_key && record.key != upstream_key {
                return Err(corruption(
                    "MV dependency key differs from its canonical occurrence",
                ));
            }
            Ok(dependency)
        })
        .collect::<Result<Vec<_>, _>>()?;
    dependencies.sort_by(|left, right| {
        left.upstream
            .display_name()
            .cmp(&right.upstream.display_name())
            .then(left.occurrence_id.cmp(&right.occurrence_id))
    });
    Ok(dependencies)
}

fn invalid(message: impl Into<String>) -> MvRepositoryError {
    MvRepositoryError::new(MvRepositoryErrorKind::InvalidRequest, message)
}

fn corruption(message: impl Into<String>) -> MvRepositoryError {
    MvRepositoryError::new(MvRepositoryErrorKind::Corruption, message)
}

fn invalid_state_store(_message: impl Into<String>) -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::Corruption,
        "MV Accelerator StateStore key or record is invalid",
    )
}

fn conflict_state_store(_message: impl Into<String>) -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::PreconditionFailed,
        "MV Accelerator StateStore transaction precondition failed",
    )
}

impl StateStoreMvRepository {
    pub async fn list_downstream_dependencies(
        &self,
        upstream: &MvDependencyObjectRef,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        self.classified_dependencies(dependency_by_upstream_prefix(upstream).map_err(corruption)?)
            .await
    }

    pub async fn ensure_no_downstream_dependencies(
        &self,
        upstream: &MvDependencyObjectRef,
    ) -> Result<(), MvRepositoryError> {
        let dependencies = self.list_downstream_dependencies(upstream).await?;
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
}
impl StateStoreMvRepository {
    #[cfg(any(test, feature = "test-controls"))]
    pub async fn wipe_accelerator(&self, _operation_id: Uuid) -> Result<(), MvRepositoryError> {
        let records = self
            .scan_prefix(accelerator_prefix().map_err(corruption)?)
            .await?;
        let store = Arc::clone(&self.store);
        operation::run(
            store.as_ref(),
            &self.runner_metrics,
            self.run_policy,
            "wipe MV Accelerator family",
            move |transaction| {
                let records = records.clone();
                Box::pin(async move {
                    for record in &records {
                        let current = transaction.get(&record.key).await?.ok_or_else(|| {
                            conflict_state_store("MV Accelerator changed before wipe")
                        })?;
                        if current.version != record.version {
                            return Err(conflict_state_store("MV Accelerator changed before wipe"));
                        }
                        transaction
                            .delete(
                                record.key.clone(),
                                Precondition::Version(record.version.clone()),
                            )
                            .await?;
                    }
                    Ok(())
                })
            },
        )
        .await
    }
}

#[cfg(any(test, feature = "test-controls"))]
use self::key::accelerator_prefix;
