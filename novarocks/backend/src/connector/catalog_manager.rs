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

//! Backend-local catalog materialization and retention state.
//!
//! This module deliberately has no RPC, provider discovery, or query-lifecycle
//! ownership.  Its caller supplies the immutable `CatalogProperties` frozen by
//! the frontend, and its only liveness input is the set of admitted query
//! execution ids.  A later integration layer owns converting provider-specific
//! materialization failures into protocol responses.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, Condvar, Mutex};

use novarocks_spi::connector::{
    CatalogHandle, CatalogProperties, CatalogRuntime, CatalogRuntimeMaterializer,
};
use novarocks_types::QueryExecutionId;

/// The bounded number of unleased materialized catalogs retained by default.
pub const DEFAULT_MAX_RETAINED_CATALOGS: usize = 64;

/// A BE-local provider runtime selected for one exact immutable catalog.
///
/// The wrapper keeps the trait object out of the generic lifecycle manager's
/// public surface while preserving the provider runtime for the native decode
/// resolver that will consume the same catalog lease.
#[derive(Clone)]
pub struct MaterializedCatalogRuntime {
    runtime: Arc<dyn CatalogRuntime>,
    read_execution: Option<super::typed_registry::InstalledReadExecution>,
    write_execution: Option<super::typed_registry::InstalledWriteExecution>,
}

impl MaterializedCatalogRuntime {
    pub fn new(runtime: Arc<dyn CatalogRuntime>) -> Self {
        Self {
            runtime,
            read_execution: None,
            write_execution: None,
        }
    }

    fn with_executions(
        runtime: Arc<dyn CatalogRuntime>,
        read_execution: Option<super::typed_registry::InstalledReadExecution>,
        write_execution: Option<super::typed_registry::InstalledWriteExecution>,
    ) -> Self {
        Self {
            runtime,
            read_execution,
            write_execution,
        }
    }

    pub fn runtime(&self) -> Arc<dyn CatalogRuntime> {
        Arc::clone(&self.runtime)
    }

    pub fn read_execution(&self) -> Option<super::typed_registry::InstalledReadExecution> {
        self.read_execution.clone()
    }

    pub fn write_execution(&self) -> Option<super::typed_registry::InstalledWriteExecution> {
        self.write_execution.clone()
    }
}

/// Startup-sealed provider materializers keyed by the closed catalog family.
#[derive(Clone)]
pub struct CatalogRuntimeMaterializerSet {
    materializers: Arc<
        BTreeMap<
            novarocks_spi::connector::CatalogProviderKind,
            Arc<dyn CatalogRuntimeMaterializer>,
        >,
    >,
    read_bundle_factories: Arc<
        BTreeMap<
            novarocks_spi::connector::CatalogProviderKind,
            Arc<dyn novarocks_proto_codec::connector_read::ConnectorReadExecutionBundleFactory>,
        >,
    >,
    write_bundle_factories: Arc<
        BTreeMap<
            novarocks_spi::connector::CatalogProviderKind,
            Arc<dyn novarocks_spi::connector::CatalogWriteExecutionBundleFactory>,
        >,
    >,
}

impl CatalogRuntimeMaterializerSet {
    pub fn try_new(
        materializers: impl IntoIterator<Item = Arc<dyn CatalogRuntimeMaterializer>>,
    ) -> Result<Self, CatalogManagerError> {
        let mut sealed = BTreeMap::new();
        for materializer in materializers {
            let provider_kind = materializer.provider_kind();
            if sealed.insert(provider_kind, materializer).is_some() {
                return Err(CatalogManagerError::InvalidConfiguration(
                    "duplicate catalog runtime materializer provider kind",
                ));
            }
        }
        Ok(Self {
            materializers: Arc::new(sealed),
            read_bundle_factories: Arc::new(BTreeMap::new()),
            write_bundle_factories: Arc::new(BTreeMap::new()),
        })
    }

    /// Seal the provider read factories alongside catalog materializers.  The
    /// factory is invoked only after the exact immutable catalog has been
    /// materialized and identity-checked below.
    pub fn try_new_with_read_execution_factories(
        materializers: impl IntoIterator<Item = Arc<dyn CatalogRuntimeMaterializer>>,
        factories: impl IntoIterator<
            Item = (
                novarocks_spi::connector::CatalogProviderKind,
                Arc<dyn novarocks_proto_codec::connector_read::ConnectorReadExecutionBundleFactory>,
            ),
        >,
    ) -> Result<Self, CatalogManagerError> {
        let mut result = Self::try_new(materializers)?;
        let mut sealed = BTreeMap::new();
        for (provider_kind, factory) in factories {
            if sealed.insert(provider_kind, factory).is_some() {
                return Err(CatalogManagerError::InvalidConfiguration(
                    "duplicate catalog read execution provider kind",
                ));
            }
        }
        result.read_bundle_factories = Arc::new(sealed);
        Ok(result)
    }

    /// Seal catalog-scoped writer factories alongside the immutable catalog
    /// materializers and typed-read factories. Each factory is selected only
    /// by the closed catalog provider kind and receives the exact handle that
    /// materialization has just verified.
    pub fn try_new_with_execution_factories(
        materializers: impl IntoIterator<Item = Arc<dyn CatalogRuntimeMaterializer>>,
        read_factories: impl IntoIterator<
            Item = (
                novarocks_spi::connector::CatalogProviderKind,
                Arc<dyn novarocks_proto_codec::connector_read::ConnectorReadExecutionBundleFactory>,
            ),
        >,
        write_factories: impl IntoIterator<
            Item = (
                novarocks_spi::connector::CatalogProviderKind,
                Arc<dyn novarocks_spi::connector::CatalogWriteExecutionBundleFactory>,
            ),
        >,
    ) -> Result<Self, CatalogManagerError> {
        let mut result =
            Self::try_new_with_read_execution_factories(materializers, read_factories)?;
        let mut sealed = BTreeMap::new();
        for (provider_kind, factory) in write_factories {
            if sealed.insert(provider_kind, factory).is_some() {
                return Err(CatalogManagerError::InvalidConfiguration(
                    "duplicate catalog write execution provider kind",
                ));
            }
        }
        result.write_bundle_factories = Arc::new(sealed);
        Ok(result)
    }

    pub fn materialize(
        &self,
        properties: &CatalogProperties,
    ) -> Result<MaterializedCatalogRuntime, CatalogManagerError> {
        let Some(materializer) = self.materializers.get(&properties.provider_kind()) else {
            return Err(CatalogManagerError::materialization_failed(
                "catalog runtime provider is not installed",
            ));
        };
        let runtime = materializer
            .materialize(properties)
            .map_err(|error| CatalogManagerError::materialization_failed(error.to_string()))?;
        if runtime.handle() != properties.handle()
            || runtime.provider_kind() != properties.provider_kind()
        {
            return Err(CatalogManagerError::materialization_failed(
                "catalog runtime materializer returned an incompatible runtime",
            ));
        }
        let read_execution = self
            .read_bundle_factories
            .get(&properties.provider_kind())
            .map(|factory| {
                factory
                    .build(properties.handle())
                    .map(|bundle| {
                        super::typed_registry::InstalledReadExecution::new(
                            bundle.provider_factory(),
                            bundle.codec(),
                        )
                    })
                    .map_err(|error| CatalogManagerError::materialization_failed(error.to_string()))
            })
            .transpose()?;
        let write_execution =
            self.write_bundle_factories
                .get(&properties.provider_kind())
                .map(|factory| {
                    factory
                    .build(properties.handle())
                    .and_then(|bundle| {
                        let execution = bundle.execution();
                        if execution.catalog_handle() != properties.handle() {
                            return Err(novarocks_spi::connector::ConnectorError::new(
                                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                                "catalog writer factory returned an incompatible catalog handle",
                            ));
                        }
                        Ok(super::typed_registry::InstalledWriteExecution::new(execution))
                    })
                    .map_err(|error| CatalogManagerError::materialization_failed(error.to_string()))
                })
                .transpose()?;
        Ok(if read_execution.is_some() || write_execution.is_some() {
            MaterializedCatalogRuntime::with_executions(runtime, read_execution, write_execution)
        } else {
            MaterializedCatalogRuntime::new(runtime)
        })
    }
}

/// Catalog-manager configuration that is independent of any provider runtime.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CatalogManagerConfig {
    pub max_retained_catalogs: usize,
}

impl Default for CatalogManagerConfig {
    fn default() -> Self {
        Self {
            max_retained_catalogs: DEFAULT_MAX_RETAINED_CATALOGS,
        }
    }
}

/// A stable, provider-neutral failure returned by the materialization owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogManagerError {
    InvalidConfiguration(&'static str),
    ConflictingProperties { handle: CatalogHandle },
    MaterializationFailed { message: Arc<str> },
}

impl CatalogManagerError {
    pub fn materialization_failed(message: impl Into<Arc<str>>) -> Self {
        Self::MaterializationFailed {
            message: message.into(),
        }
    }
}

impl fmt::Display for CatalogManagerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration(message) => formatter.write_str(message),
            Self::ConflictingProperties { handle } => write!(
                formatter,
                "catalog handle {} has conflicting materialization properties",
                handle.catalog_name().as_str()
            ),
            Self::MaterializationFailed { message } => {
                write!(formatter, "catalog materialization failed: {message}")
            }
        }
    }
}

impl std::error::Error for CatalogManagerError {}

/// Result of reconciling backend retention against a frontend reachability
/// snapshot.  A rejection is atomic: no catalog was evicted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogPruneResult {
    Pruned {
        handles: BTreeSet<CatalogHandle>,
    },
    Rejected {
        missing_live_handles: BTreeSet<CatalogHandle>,
    },
}

impl CatalogPruneResult {
    pub fn pruned_handles(&self) -> Option<&BTreeSet<CatalogHandle>> {
        match self {
            Self::Pruned { handles } => Some(handles),
            Self::Rejected { .. } => None,
        }
    }

    pub fn missing_live_handles(&self) -> Option<&BTreeSet<CatalogHandle>> {
        match self {
            Self::Pruned { .. } => None,
            Self::Rejected {
                missing_live_handles,
            } => Some(missing_live_handles),
        }
    }
}

/// Handle-keyed BE-local materializations.  Multiple versions of one catalog
/// name coexist; only an exact `CatalogHandle` can resolve a runtime.
#[derive(Clone)]
pub struct CatalogManager<T> {
    state: Arc<Mutex<CatalogManagerState<T>>>,
    max_retained_catalogs: usize,
}

struct CatalogManagerState<T> {
    entries: BTreeMap<CatalogHandle, Arc<CatalogCell<T>>>,
    query_reachability: BTreeMap<QueryExecutionId, BTreeSet<CatalogHandle>>,
    next_registration_token: u64,
}

struct CatalogCell<T> {
    properties: CatalogProperties,
    state: Mutex<CatalogCellState<T>>,
    changed: Condvar,
}

enum CatalogCellState<T> {
    Materializing,
    Ready {
        runtime: Arc<T>,
        registration_token: RegistrationToken,
    },
    Failed(CatalogManagerError),
}

/// BE-private registration identity.  It is never supplied by FE input or
/// carried on the wire: it only proves an eviction still targets the exact
/// local installation that was selected earlier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RegistrationToken(u64);

#[derive(Clone)]
struct ReadyCandidate<T> {
    handle: CatalogHandle,
    cell: Arc<CatalogCell<T>>,
    registration_token: RegistrationToken,
}

impl<T> CatalogCell<T> {
    fn materializing(properties: CatalogProperties) -> Self {
        Self {
            properties,
            state: Mutex::new(CatalogCellState::Materializing),
            changed: Condvar::new(),
        }
    }

    fn wait_for_result(&self) -> Result<Arc<T>, CatalogManagerError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        loop {
            match &*state {
                CatalogCellState::Materializing => {
                    state = self
                        .changed
                        .wait(state)
                        .unwrap_or_else(|error| error.into_inner());
                }
                CatalogCellState::Ready { runtime, .. } => return Ok(Arc::clone(runtime)),
                CatalogCellState::Failed(error) => return Err(error.clone()),
            }
        }
    }

    fn complete(
        &self,
        result: Result<T, CatalogManagerError>,
        registration_token: Option<RegistrationToken>,
    ) -> Result<Arc<T>, CatalogManagerError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let result = match result {
            Ok(runtime) => {
                let registration_token = registration_token
                    .expect("successful catalog materialization requires a registration token");
                let runtime = Arc::new(runtime);
                *state = CatalogCellState::Ready {
                    runtime: Arc::clone(&runtime),
                    registration_token,
                };
                Ok(runtime)
            }
            Err(error) => {
                *state = CatalogCellState::Failed(error.clone());
                Err(error)
            }
        };
        self.changed.notify_all();
        result
    }

    fn ready_candidate(self: &Arc<Self>, handle: &CatalogHandle) -> Option<ReadyCandidate<T>> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let CatalogCellState::Ready {
            registration_token, ..
        } = &*state
        else {
            return None;
        };
        Some(ReadyCandidate {
            handle: handle.clone(),
            cell: Arc::clone(self),
            registration_token: *registration_token,
        })
    }
}

impl<T> CatalogManager<T> {
    pub fn try_new(config: CatalogManagerConfig) -> Result<Self, CatalogManagerError> {
        if config.max_retained_catalogs == 0 {
            return Err(CatalogManagerError::InvalidConfiguration(
                "catalog manager must retain at least one catalog",
            ));
        }
        Ok(Self {
            state: Arc::new(Mutex::new(CatalogManagerState {
                entries: BTreeMap::new(),
                query_reachability: BTreeMap::new(),
                next_registration_token: 0,
            })),
            max_retained_catalogs: config.max_retained_catalogs,
        })
    }

    /// Materialize one exact catalog, lease it to `query`, and return the
    /// shared local runtime.  Concurrent callers for matching properties wait
    /// on one installation.  A failed installation is rolled back atomically:
    /// it never becomes a retained entry or a query-reachable handle.
    pub fn ensure(
        &self,
        query: QueryExecutionId,
        properties: CatalogProperties,
        materialize: impl FnOnce(&CatalogProperties) -> Result<T, CatalogManagerError>,
    ) -> Result<Arc<T>, CatalogManagerError> {
        let handle = properties.handle().clone();
        let (cell, installer) = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if let Some(cell) = state.entries.get(&handle).cloned() {
                if cell.properties != properties {
                    return Err(CatalogManagerError::ConflictingProperties { handle });
                }
                state
                    .query_reachability
                    .entry(query)
                    .or_default()
                    .insert(properties.handle().clone());
                (cell, false)
            } else {
                let cell = Arc::new(CatalogCell::materializing(properties));
                state.entries.insert(handle.clone(), Arc::clone(&cell));
                state
                    .query_reachability
                    .entry(query)
                    .or_default()
                    .insert(handle.clone());
                (cell, true)
            }
        };

        if !installer {
            let result = cell.wait_for_result();
            if result.is_err() {
                self.remove_query_handle(query, &handle);
            }
            return result;
        }

        let materialized = materialize(&cell.properties);
        let registration_token = materialized
            .as_ref()
            .ok()
            .map(|_| self.allocate_registration_token());
        let result = cell.complete(materialized, registration_token);
        if result.is_err() {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state
                .entries
                .get(&handle)
                .is_some_and(|current| Arc::ptr_eq(current, &cell))
            {
                state.entries.remove(&handle);
            }
            remove_handle_from_all_queries(&mut state.query_reachability, &handle);
        }
        result
    }

    /// Release every catalog lease held by a terminal query.  The configured
    /// retention bound is then enforced without evicting a still-reachable
    /// catalog or a materialization that is still in progress.
    pub fn release_query(&self, query: QueryExecutionId) -> BTreeSet<CatalogHandle> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.query_reachability.remove(&query);
        trim_unreachable_ready_entries(&mut state, self.max_retained_catalogs, &BTreeSet::new())
    }

    /// Reconcile retained entries with the frontend's complete view of live
    /// query reachability.  Omitting a BE-live handle rejects the whole prune
    /// request before mutation; this prevents stale control traffic from
    /// tearing down a runtime needed by an admitted query.
    pub fn prune_unreachable(&self, reachable: &BTreeSet<CatalogHandle>) -> CatalogPruneResult {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let locally_live = all_query_handles(&state.query_reachability);
        let missing_live_handles = locally_live
            .difference(reachable)
            .cloned()
            .collect::<BTreeSet<_>>();
        if !missing_live_handles.is_empty() {
            return CatalogPruneResult::Rejected {
                missing_live_handles,
            };
        }
        let handles = trim_unreachable_ready_entries(&mut state, 0, reachable);
        CatalogPruneResult::Pruned { handles }
    }

    pub fn resolve(&self, handle: &CatalogHandle) -> Option<Arc<T>> {
        let cell = self
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entries
            .get(handle)
            .cloned()?;
        let state = cell.state.lock().unwrap_or_else(|error| error.into_inner());
        match &*state {
            CatalogCellState::Ready { runtime, .. } => Some(Arc::clone(runtime)),
            CatalogCellState::Materializing | CatalogCellState::Failed(_) => None,
        }
    }

    /// Resolve a runtime only when the exact handle remains leased by the
    /// admitted query. Decode uses this rather than the retained-cache lookup,
    /// so retention can never become an authority path.
    pub fn resolve_for_query(
        &self,
        query: QueryExecutionId,
        handle: &CatalogHandle,
    ) -> Option<Arc<T>> {
        let cell = {
            let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state
                .query_reachability
                .get(&query)
                .filter(|handles| handles.contains(handle))
                .and_then(|_| state.entries.get(handle))
                .cloned()
        }?;
        let state = cell.state.lock().unwrap_or_else(|error| error.into_inner());
        match &*state {
            CatalogCellState::Ready { runtime, .. } => Some(Arc::clone(runtime)),
            CatalogCellState::Materializing | CatalogCellState::Failed(_) => None,
        }
    }

    pub fn retained_handles(&self) -> BTreeSet<CatalogHandle> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entries
            .keys()
            .cloned()
            .collect()
    }

    pub fn query_handles(&self, query: QueryExecutionId) -> BTreeSet<CatalogHandle> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .query_reachability
            .get(&query)
            .cloned()
            .unwrap_or_default()
    }

    fn remove_query_handle(&self, query: QueryExecutionId, handle: &CatalogHandle) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let remove_query = state
            .query_reachability
            .get_mut(&query)
            .is_some_and(|handles| {
                handles.remove(handle);
                handles.is_empty()
            });
        if remove_query {
            state.query_reachability.remove(&query);
        }
    }

    fn allocate_registration_token(&self) -> RegistrationToken {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.next_registration_token = state
            .next_registration_token
            .checked_add(1)
            .expect("catalog registration token exhausted");
        RegistrationToken(state.next_registration_token)
    }
}

impl<T> Default for CatalogManager<T> {
    fn default() -> Self {
        Self::try_new(CatalogManagerConfig::default()).expect("default catalog manager is valid")
    }
}

fn all_query_handles(
    query_reachability: &BTreeMap<QueryExecutionId, BTreeSet<CatalogHandle>>,
) -> BTreeSet<CatalogHandle> {
    query_reachability
        .values()
        .flat_map(|handles| handles.iter().cloned())
        .collect()
}

fn remove_handle_from_all_queries(
    query_reachability: &mut BTreeMap<QueryExecutionId, BTreeSet<CatalogHandle>>,
    handle: &CatalogHandle,
) {
    query_reachability.retain(|_, handles| {
        handles.remove(handle);
        !handles.is_empty()
    });
}

fn trim_unreachable_ready_entries<T>(
    state: &mut CatalogManagerState<T>,
    retain_at_most: usize,
    protected: &BTreeSet<CatalogHandle>,
) -> BTreeSet<CatalogHandle> {
    let live = all_query_handles(&state.query_reachability);
    let candidates = state
        .entries
        .iter()
        .filter_map(|(handle, cell)| {
            if live.contains(handle) || protected.contains(handle) {
                return None;
            }
            cell.ready_candidate(handle)
        })
        .collect::<Vec<_>>();
    let excess = state.entries.len().saturating_sub(retain_at_most);
    let remove_count = if retain_at_most == 0 {
        candidates.len()
    } else {
        candidates.len().min(excess)
    };
    let candidates = candidates
        .into_iter()
        .take(remove_count)
        .collect::<Vec<_>>();
    let mut removed = BTreeSet::new();
    for candidate in candidates {
        if remove_ready_candidate_if_current(state, &candidate) {
            removed.insert(candidate.handle);
        }
    }
    removed
}

/// A stale candidate is harmless: the current map entry must still be the
/// same allocation and must retain the registration token observed at
/// candidate creation.  This closes an ABA gap when a handle is pruned and
/// subsequently materialized again before a delayed cleanup runs.
fn remove_ready_candidate_if_current<T>(
    state: &mut CatalogManagerState<T>,
    candidate: &ReadyCandidate<T>,
) -> bool {
    let Some(current) = state.entries.get(&candidate.handle) else {
        return false;
    };
    if !Arc::ptr_eq(current, &candidate.cell) {
        return false;
    }
    let current_state = current
        .state
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let is_current_registration = matches!(
        &*current_state,
        CatalogCellState::Ready {
            registration_token,
            ..
        } if *registration_token == candidate.registration_token
    );
    drop(current_state);
    if !is_current_registration {
        return false;
    }
    state.entries.remove(&candidate.handle);
    true
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;

    use novarocks_spi::connector::{
        CatalogHandle, CatalogProperties, CatalogProviderKind, CatalogRuntime,
        CatalogRuntimeMaterializer, CatalogVersion, ConnectorError, ConnectorInstanceId,
    };
    use novarocks_types::{AttemptId, QueryExecutionId, QueryId};

    use super::{
        CatalogManager, CatalogManagerConfig, CatalogManagerError, CatalogPruneResult,
        CatalogRuntimeMaterializerSet, remove_ready_candidate_if_current,
    };

    struct TestRuntime {
        handle: CatalogHandle,
        provider_kind: CatalogProviderKind,
    }

    impl CatalogRuntime for TestRuntime {
        fn handle(&self) -> &CatalogHandle {
            &self.handle
        }

        fn provider_kind(&self) -> CatalogProviderKind {
            self.provider_kind
        }
    }

    struct TestMaterializer {
        provider_kind: CatalogProviderKind,
        returned_kind: CatalogProviderKind,
    }

    impl CatalogRuntimeMaterializer for TestMaterializer {
        fn provider_kind(&self) -> CatalogProviderKind {
            self.provider_kind
        }

        fn materialize(
            &self,
            properties: &CatalogProperties,
        ) -> Result<Arc<dyn CatalogRuntime>, ConnectorError> {
            Ok(Arc::new(TestRuntime {
                handle: properties.handle().clone(),
                provider_kind: self.returned_kind,
            }))
        }
    }

    fn query(value: i64) -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(7, value), AttemptId::new(1).expect("attempt"))
            .expect("query execution id")
    }

    fn properties(version: u8) -> CatalogProperties {
        let handle = CatalogHandle::new(
            ConnectorInstanceId::try_from_canonical("catalog.analytics").expect("catalog id"),
            CatalogVersion::from_bytes([version; 32]),
        );
        CatalogProperties::new(handle, CatalogProviderKind::Iceberg, 1, vec![], vec![])
            .expect("catalog properties")
    }

    #[test]
    fn sealed_materializer_set_requires_the_exact_provider_and_runtime_identity() {
        let properties = properties(1);
        let set = CatalogRuntimeMaterializerSet::try_new([Arc::new(TestMaterializer {
            provider_kind: CatalogProviderKind::Iceberg,
            returned_kind: CatalogProviderKind::Iceberg,
        })
            as Arc<dyn CatalogRuntimeMaterializer>])
        .expect("seal materializer set");
        let runtime = set.materialize(&properties).expect("exact materialization");
        assert_eq!(runtime.runtime().handle(), properties.handle());

        let mismatched = CatalogRuntimeMaterializerSet::try_new([Arc::new(TestMaterializer {
            provider_kind: CatalogProviderKind::Iceberg,
            returned_kind: CatalogProviderKind::StarRocks,
        })
            as Arc<dyn CatalogRuntimeMaterializer>])
        .expect("seal materializer set");
        assert!(matches!(
            mismatched.materialize(&properties),
            Err(CatalogManagerError::MaterializationFailed { .. })
        ));

        let missing = CatalogRuntimeMaterializerSet::try_new([]).expect("empty set is valid");
        assert!(matches!(
            missing.materialize(&properties),
            Err(CatalogManagerError::MaterializationFailed { .. })
        ));
    }

    #[test]
    fn concurrent_ensure_deduplicates_materialization_and_shares_runtime() {
        let manager = Arc::new(CatalogManager::<usize>::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let first_manager = Arc::clone(&manager);
        let first_calls = Arc::clone(&calls);
        let first_entered = Arc::clone(&entered);
        let first_release = Arc::clone(&release);
        let first = thread::spawn(move || {
            first_manager.ensure(query(1), properties(1), |_| {
                first_calls.fetch_add(1, Ordering::SeqCst);
                first_entered.wait();
                first_release.wait();
                Ok(42)
            })
        });
        entered.wait();
        let second_manager = Arc::clone(&manager);
        let second_calls = Arc::clone(&calls);
        let second = thread::spawn(move || {
            second_manager.ensure(query(2), properties(1), |_| {
                second_calls.fetch_add(1, Ordering::SeqCst);
                Ok(7)
            })
        });
        release.wait();
        assert_eq!(*first.join().expect("first ensure").expect("runtime"), 42);
        assert_eq!(*second.join().expect("second ensure").expect("runtime"), 42);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn failed_materialization_rolls_back_entry_and_all_query_reachability() {
        let manager = CatalogManager::<usize>::default();
        let catalog = properties(3);
        let handle = catalog.handle().clone();
        let error = manager
            .ensure(query(1), catalog, |_| {
                Err(CatalogManagerError::materialization_failed(
                    "credentials unavailable",
                ))
            })
            .expect_err("materialization must fail");
        assert_eq!(
            error,
            CatalogManagerError::materialization_failed("credentials unavailable")
        );
        assert!(manager.resolve(&handle).is_none());
        assert!(manager.retained_handles().is_empty());
        assert!(manager.query_handles(query(1)).is_empty());
    }

    #[test]
    fn retained_runtime_does_not_bypass_the_query_catalog_lease() {
        let manager = CatalogManager::<usize>::default();
        let catalog = properties(1);
        let handle = catalog.handle().clone();
        manager
            .ensure(query(1), catalog, |_| Ok(7))
            .expect("materialize runtime");
        assert_eq!(
            *manager
                .resolve_for_query(query(1), &handle)
                .expect("query owns exact handle"),
            7
        );
        manager.release_query(query(1));
        assert!(
            manager.resolve(&handle).is_some(),
            "retention keeps the runtime"
        );
        assert!(
            manager.resolve_for_query(query(1), &handle).is_none(),
            "retention must not grant decode authority after terminal release"
        );
    }

    #[test]
    fn retry_after_failed_materialization_creates_a_fresh_runtime() {
        let manager = CatalogManager::<usize>::default();
        let catalog = properties(3);
        manager
            .ensure(query(1), catalog.clone(), |_| {
                Err(CatalogManagerError::materialization_failed(
                    "transient failure",
                ))
            })
            .expect_err("first materialization fails");
        assert_eq!(
            *manager
                .ensure(query(2), catalog.clone(), |_| Ok(9))
                .expect("retry materializes a fresh entry"),
            9
        );
        assert_eq!(
            *manager.resolve(catalog.handle()).expect("retry resolves"),
            9
        );
    }

    #[test]
    fn stale_prune_candidate_cannot_remove_a_reinstalled_handle() {
        let manager = CatalogManager::<usize>::default();
        let catalog = properties(4);
        let handle = catalog.handle().clone();
        manager
            .ensure(query(1), catalog.clone(), |_| Ok(1))
            .expect("first runtime");
        let stale_candidate = {
            let state = manager.state.lock().expect("manager state");
            state
                .entries
                .get(&handle)
                .expect("first entry")
                .ready_candidate(&handle)
                .expect("first entry is registered")
        };
        manager.release_query(query(1));
        manager.prune_unreachable(&BTreeSet::new());
        assert!(manager.resolve(&handle).is_none());

        manager
            .ensure(query(2), catalog, |_| Ok(2))
            .expect("second runtime");
        let removed = {
            let mut state = manager.state.lock().expect("manager state");
            remove_ready_candidate_if_current(&mut state, &stale_candidate)
        };
        assert!(!removed);
        assert_eq!(*manager.resolve(&handle).expect("new runtime remains"), 2);
    }

    #[test]
    fn old_and_new_catalog_versions_coexist_while_queries_hold_leases() {
        let manager = CatalogManager::<usize>::default();
        let old = properties(1);
        let new = properties(2);
        manager
            .ensure(query(1), old.clone(), |_| Ok(1))
            .expect("old runtime");
        manager
            .ensure(query(2), new.clone(), |_| Ok(2))
            .expect("new runtime");
        assert_eq!(*manager.resolve(old.handle()).expect("old resolves"), 1);
        assert_eq!(*manager.resolve(new.handle()).expect("new resolves"), 2);
        assert_eq!(manager.release_query(query(1)), BTreeSet::new());
        assert!(manager.resolve(old.handle()).is_some());
        assert!(manager.resolve(new.handle()).is_some());
    }

    #[test]
    fn prune_rejects_stale_reachability_without_evicting_live_catalogs() {
        let manager = CatalogManager::<usize>::default();
        let catalog = properties(1);
        let handle = catalog.handle().clone();
        manager
            .ensure(query(1), catalog, |_| Ok(1))
            .expect("runtime");
        let result = manager.prune_unreachable(&BTreeSet::new());
        assert_eq!(
            result,
            CatalogPruneResult::Rejected {
                missing_live_handles: BTreeSet::from([handle.clone()]),
            }
        );
        assert_eq!(*manager.resolve(&handle).expect("still retained"), 1);
    }

    #[test]
    fn prune_removes_unreachable_ready_entries_but_not_live_or_in_progress_entries() {
        let manager = Arc::new(CatalogManager::<usize>::default());
        let ready = properties(1);
        let ready_handle = ready.handle().clone();
        manager
            .ensure(query(1), ready, |_| Ok(1))
            .expect("ready runtime");
        manager.release_query(query(1));

        let loading = properties(2);
        let loading_handle = loading.handle().clone();
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let loader_manager = Arc::clone(&manager);
        let loader_entered = Arc::clone(&entered);
        let loader_release = Arc::clone(&release);
        let thread = thread::spawn(move || {
            loader_manager.ensure(query(2), loading, |_| {
                loader_entered.wait();
                loader_release.wait();
                Ok(2)
            })
        });
        entered.wait();
        let result = manager.prune_unreachable(&BTreeSet::from([loading_handle.clone()]));
        assert_eq!(
            result,
            CatalogPruneResult::Pruned {
                handles: BTreeSet::from([ready_handle.clone()]),
            }
        );
        assert!(manager.resolve(&ready_handle).is_none());
        assert!(manager.retained_handles().contains(&loading_handle));
        release.wait();
        thread
            .join()
            .expect("loading ensure")
            .expect("loaded runtime");
    }

    #[test]
    fn release_enforces_bounded_retention_after_queries_finish() {
        let manager = CatalogManager::<usize>::try_new(CatalogManagerConfig {
            max_retained_catalogs: 1,
        })
        .expect("valid manager");
        let first = properties(1);
        let second = properties(2);
        manager
            .ensure(query(1), first.clone(), |_| Ok(1))
            .expect("first runtime");
        manager
            .ensure(query(2), second.clone(), |_| Ok(2))
            .expect("second runtime");
        manager.release_query(query(1));
        assert!(manager.resolve(first.handle()).is_none());
        assert!(manager.resolve(second.handle()).is_some());
        manager.release_query(query(2));
        assert_eq!(manager.retained_handles().len(), 1);
        assert!(manager.resolve(first.handle()).is_none());
        assert_eq!(
            *manager
                .resolve(second.handle())
                .expect("newest stays by key order"),
            2
        );
    }

    #[test]
    fn zero_retention_limit_is_rejected() {
        assert!(matches!(
            CatalogManager::<()>::try_new(CatalogManagerConfig {
                max_retained_catalogs: 0,
            }),
            Err(CatalogManagerError::InvalidConfiguration(
                "catalog manager must retain at least one catalog"
            ))
        ));
    }
}
