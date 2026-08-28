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

//! FE-local serving lifecycle and workload admission ownership.
//!
//! Design: ADR-0119. This owner deliberately has no knowledge of MySQL,
//! Native transport, or a particular background scheduler. Its mutex is the
//! single linearization point for serving-state transitions and admission.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::sync::Notify;

use crate::common::query_cancellation::{QueryCancellationReason, QueryCancellationSource};

/// Monotonic, FE-local state for workload admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FrontendServingState {
    Starting,
    Ready,
    Draining,
    Stopping,
}

impl FrontendServingState {
    pub const fn as_metric_label(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Ready => "ready",
            Self::Draining => "draining",
            Self::Stopping => "stopping",
        }
    }
}

/// The closed set of admission sites visible in lifecycle observations.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FrontendWorkloadKind {
    Session,
    Statement,
    Background,
}

impl FrontendWorkloadKind {
    pub const fn as_metric_label(self) -> &'static str {
        match self {
            Self::Session => "session",
            Self::Statement => "statement",
            Self::Background => "background",
        }
    }

    const fn is_active_attempt(self) -> bool {
        matches!(self, Self::Statement | Self::Background)
    }
}

/// Closed source labels exposed by the sanitized FE management surface.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum FrontendCatalogSourceMode {
    StaticFile,
    DynamicStateStore,
    ManagedController,
}

impl FrontendCatalogSourceMode {
    pub const fn as_metric_label(self) -> &'static str {
        match self {
            Self::StaticFile => "static_file",
            Self::DynamicStateStore => "dynamic_state_store",
            Self::ManagedController => "managed_controller",
        }
    }
}

/// Typed rejection returned before a workload can mutate session or execution state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrontendAdmissionError {
    NotReady { state: FrontendServingState },
    Draining,
    Stopping,
    SessionRequiresRegistration,
}

/// A sanitized snapshot identity. Catalog names, properties, credential references,
/// and physical attachment identities never enter this type.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct FrontendCatalogSnapshotIdentity {
    pub catalog_count: usize,
    pub digest: String,
}

impl FrontendCatalogSnapshotIdentity {
    pub fn try_new(catalog_count: usize, digest: impl Into<String>) -> Result<Self, String> {
        let digest = digest.into();
        if digest.len() != 16 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(
                "frontend catalog snapshot digest must be a 16-hex short digest".to_string(),
            );
        }
        Ok(Self {
            catalog_count,
            digest,
        })
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct FrontendCatalogCounts {
    pub desired: usize,
    pub ready: usize,
    pub unavailable: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct FrontendCatalogServingSnapshot {
    pub source_mode: Option<FrontendCatalogSourceMode>,
    pub bootstrap_complete: bool,
    pub snapshot: Option<FrontendCatalogSnapshotIdentity>,
    pub counts: FrontendCatalogCounts,
}

impl Default for FrontendCatalogServingSnapshot {
    fn default() -> Self {
        Self {
            source_mode: None,
            bootstrap_complete: false,
            snapshot: None,
            counts: FrontendCatalogCounts::default(),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct FrontendActiveWorkloads {
    pub statement: usize,
    pub background: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct FrontendWorkloadTotals {
    pub session: u64,
    pub statement: u64,
    pub background: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct FrontendWorkloadServingSnapshot {
    pub active: FrontendActiveWorkloads,
    pub rejected_admissions: FrontendWorkloadTotals,
    pub completed_during_drain: FrontendWorkloadTotals,
    pub deadline_cancelled: FrontendWorkloadTotals,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct FrontendDrainServingSnapshot {
    pub started_at_unix_ms: Option<u64>,
    pub deadline_unix_ms: Option<u64>,
    pub elapsed_ms: u64,
}

/// The exact sanitized document returned by `/v1/frontend/state`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct FrontendServingSnapshot {
    pub schema_version: u8,
    pub serving_state: FrontendServingState,
    pub catalog: FrontendCatalogServingSnapshot,
    pub workload: FrontendWorkloadServingSnapshot,
    pub drain: FrontendDrainServingSnapshot,
}

impl FrontendServingSnapshot {
    fn starting() -> Self {
        Self {
            schema_version: 1,
            serving_state: FrontendServingState::Starting,
            catalog: FrontendCatalogServingSnapshot::default(),
            workload: FrontendWorkloadServingSnapshot::default(),
            drain: FrontendDrainServingSnapshot::default(),
        }
    }

    pub fn base_ready(&self) -> bool {
        self.serving_state == FrontendServingState::Ready && self.catalog.bootstrap_complete
    }
}

/// Read-only capability for the FE serving signal surface.
pub trait FrontendServingSnapshotReader: Send + Sync {
    fn frontend_serving_snapshot(&self) -> FrontendServingSnapshot;
}

/// Late-bound reader used to start management observability before the full FE
/// application has been opened. Installing an owner is one-way for the listener.
#[derive(Default)]
pub struct LateBoundFrontendServingSnapshotReader {
    reader: RwLock<Option<Arc<dyn FrontendServingSnapshotReader>>>,
}

impl LateBoundFrontendServingSnapshotReader {
    pub fn install(&self, reader: Arc<dyn FrontendServingSnapshotReader>) -> Result<(), String> {
        let mut slot = self
            .reader
            .write()
            .expect("frontend serving reader lock poisoned");
        if slot.is_some() {
            return Err("frontend serving snapshot reader is already installed".to_string());
        }
        *slot = Some(reader);
        Ok(())
    }
}

impl FrontendServingSnapshotReader for LateBoundFrontendServingSnapshotReader {
    fn frontend_serving_snapshot(&self) -> FrontendServingSnapshot {
        self.reader
            .read()
            .expect("frontend serving reader lock poisoned")
            .as_ref()
            .map_or_else(FrontendServingSnapshot::starting, |reader| {
                reader.frontend_serving_snapshot()
            })
    }
}

#[derive(Clone)]
struct ActiveLease {
    kind: FrontendWorkloadKind,
    cancellation: QueryCancellationSource,
}

struct Inner {
    state: FrontendServingState,
    next_lease_id: u64,
    active: BTreeMap<u64, ActiveLease>,
    catalog: FrontendCatalogServingSnapshot,
    rejected: FrontendWorkloadTotals,
    completed_during_drain: FrontendWorkloadTotals,
    deadline_cancelled: FrontendWorkloadTotals,
    drain_started_at: Option<SystemTime>,
    drain_deadline: Option<SystemTime>,
}

impl Default for Inner {
    fn default() -> Self {
        Self {
            state: FrontendServingState::Starting,
            next_lease_id: 0,
            active: BTreeMap::new(),
            catalog: FrontendCatalogServingSnapshot::default(),
            rejected: FrontendWorkloadTotals::default(),
            completed_during_drain: FrontendWorkloadTotals::default(),
            deadline_cancelled: FrontendWorkloadTotals::default(),
            drain_started_at: None,
            drain_deadline: None,
        }
    }
}

struct LifecycleShared {
    inner: Mutex<Inner>,
    active_changed: Notify,
}

/// Process-runtime authority for serving state and admission leases.
// Design: ADR-0119 (docs/adr/ADR-0119-frontend-serving-lifecycle-and-admission-drain.md)
#[derive(Clone)]
pub struct FrontendServingLifecycle {
    shared: Arc<LifecycleShared>,
}

impl Default for FrontendServingLifecycle {
    fn default() -> Self {
        Self::new()
    }
}

impl FrontendServingLifecycle {
    pub fn new() -> Self {
        let lifecycle = Self {
            shared: Arc::new(LifecycleShared {
                inner: Mutex::new(Inner::default()),
                active_changed: Notify::new(),
            }),
        };
        lifecycle.publish_metrics();
        lifecycle
    }

    /// Publishes sanitized catalog bootstrap facts. The caller owns catalog
    /// materialization; this lifecycle merely owns aggregate observation.
    pub fn publish_catalog_bootstrap(
        &self,
        source_mode: FrontendCatalogSourceMode,
        bootstrap_complete: bool,
        snapshot: Option<FrontendCatalogSnapshotIdentity>,
        counts: FrontendCatalogCounts,
    ) {
        let mut inner = self
            .shared
            .inner
            .lock()
            .expect("frontend lifecycle lock poisoned");
        inner.catalog = FrontendCatalogServingSnapshot {
            source_mode: Some(source_mode),
            bootstrap_complete,
            snapshot,
            counts,
        };
        drop(inner);
        self.publish_metrics();
    }

    /// Transitions only from Starting to Ready after the bootstrap owner has
    /// completed its exact snapshot/materialization barrier.
    pub fn mark_ready(&self) -> Result<(), FrontendAdmissionError> {
        let mut inner = self
            .shared
            .inner
            .lock()
            .expect("frontend lifecycle lock poisoned");
        match inner.state {
            FrontendServingState::Starting => inner.state = FrontendServingState::Ready,
            FrontendServingState::Ready => return Ok(()),
            FrontendServingState::Draining => return Err(FrontendAdmissionError::Draining),
            FrontendServingState::Stopping => return Err(FrontendAdmissionError::Stopping),
        }
        drop(inner);
        self.publish_metrics();
        Ok(())
    }

    /// Atomically closes admission and records the one-way drain deadline.
    /// Repeated calls preserve the original deadline and are idempotent.
    pub fn begin_drain(&self, timeout: Duration) -> FrontendServingState {
        let mut inner = self
            .shared
            .inner
            .lock()
            .expect("frontend lifecycle lock poisoned");
        if inner.state == FrontendServingState::Ready {
            let started = SystemTime::now();
            inner.state = FrontendServingState::Draining;
            inner.drain_started_at = Some(started);
            inner.drain_deadline = Some(started + timeout);
        }
        let state = inner.state;
        drop(inner);
        self.publish_metrics();
        state
    }

    /// Marks final teardown without creating a path back to Ready.
    pub fn mark_stopping(&self) {
        let mut inner = self
            .shared
            .inner
            .lock()
            .expect("frontend lifecycle lock poisoned");
        if inner.state != FrontendServingState::Stopping {
            inner.state = FrontendServingState::Stopping;
        }
        drop(inner);
        self.publish_metrics();
    }

    /// Runs session registration in the same mutex domain as drain. Sessions
    /// are not active attempts and do not receive a lease.
    pub fn register_session<T>(
        &self,
        registration: impl FnOnce() -> T,
    ) -> Result<T, FrontendAdmissionError> {
        let mut inner = self
            .shared
            .inner
            .lock()
            .expect("frontend lifecycle lock poisoned");
        let result = match inner.state {
            FrontendServingState::Ready => Ok(registration()),
            state => {
                increment_total(&mut inner.rejected, FrontendWorkloadKind::Session);
                Err(admission_error(state))
            }
        };
        drop(inner);
        self.publish_metrics();
        result
    }

    /// Acquires a statement or background attempt lease. `Session` must use
    /// [`Self::register_session`] so it cannot accidentally become active work.
    pub fn try_admit(
        &self,
        kind: FrontendWorkloadKind,
    ) -> Result<FrontendWorkloadLease, FrontendAdmissionError> {
        if !kind.is_active_attempt() {
            return Err(FrontendAdmissionError::SessionRequiresRegistration);
        }
        let mut inner = self
            .shared
            .inner
            .lock()
            .expect("frontend lifecycle lock poisoned");
        let result = match inner.state {
            FrontendServingState::Ready => {
                let id = inner.next_lease_id;
                inner.next_lease_id = inner.next_lease_id.wrapping_add(1);
                let cancellation = QueryCancellationSource::new();
                inner.active.insert(
                    id,
                    ActiveLease {
                        kind,
                        cancellation: cancellation.clone(),
                    },
                );
                Ok(FrontendWorkloadLease {
                    shared: Arc::clone(&self.shared),
                    id,
                    kind,
                    cancellation,
                    released: AtomicBool::new(false),
                })
            }
            state => {
                increment_total(&mut inner.rejected, kind);
                Err(admission_error(state))
            }
        };
        drop(inner);
        self.publish_metrics();
        result
    }

    /// First-wins cancellation for all leases still held at the drain deadline.
    pub fn cancel_active_at_drain_deadline(&self, timeout_ms: u64) -> usize {
        let (sources, cancelled) = {
            let mut inner = self
                .shared
                .inner
                .lock()
                .expect("frontend lifecycle lock poisoned");
            let sources = inner.active.values().cloned().collect::<Vec<_>>();
            let mut cancelled = 0;
            for lease in &sources {
                if matches!(
                    lease.cancellation.request(
                        QueryCancellationReason::FrontendDrainDeadlineExceeded { timeout_ms },
                    ),
                    crate::common::query_cancellation::QueryCancellationRequestResult::Requested
                ) {
                    increment_total(&mut inner.deadline_cancelled, lease.kind);
                    cancelled += 1;
                }
            }
            (sources, cancelled)
        };
        drop(sources);
        self.publish_metrics();
        cancelled
    }

    /// Waits for all active statement/background work to finish. Notify only
    /// wakes waiters; the mutex-protected count decides the result.
    pub async fn wait_for_no_active_work(&self) {
        loop {
            let notified = self.shared.active_changed.notified();
            if self
                .shared
                .inner
                .lock()
                .expect("frontend lifecycle lock poisoned")
                .active
                .is_empty()
            {
                return;
            }
            notified.await;
        }
    }

    fn release(&self, id: u64, kind: FrontendWorkloadKind) {
        let mut inner = self
            .shared
            .inner
            .lock()
            .expect("frontend lifecycle lock poisoned");
        if inner.active.remove(&id).is_some() {
            if inner.state == FrontendServingState::Draining {
                increment_total(&mut inner.completed_during_drain, kind);
            }
            self.shared.active_changed.notify_waiters();
        }
        drop(inner);
        self.publish_metrics();
    }

    fn publish_metrics(&self) {
        crate::metrics::publish_frontend_serving_metrics(self.frontend_serving_snapshot());
    }
}

impl FrontendServingSnapshotReader for FrontendServingLifecycle {
    fn frontend_serving_snapshot(&self) -> FrontendServingSnapshot {
        let inner = self
            .shared
            .inner
            .lock()
            .expect("frontend lifecycle lock poisoned");
        let active = FrontendActiveWorkloads {
            statement: inner
                .active
                .values()
                .filter(|lease| lease.kind == FrontendWorkloadKind::Statement)
                .count(),
            background: inner
                .active
                .values()
                .filter(|lease| lease.kind == FrontendWorkloadKind::Background)
                .count(),
        };
        let now = SystemTime::now();
        FrontendServingSnapshot {
            schema_version: 1,
            serving_state: inner.state,
            catalog: inner.catalog.clone(),
            workload: FrontendWorkloadServingSnapshot {
                active,
                rejected_admissions: inner.rejected.clone(),
                completed_during_drain: inner.completed_during_drain.clone(),
                deadline_cancelled: inner.deadline_cancelled.clone(),
            },
            drain: FrontendDrainServingSnapshot {
                started_at_unix_ms: inner.drain_started_at.and_then(unix_millis),
                deadline_unix_ms: inner.drain_deadline.and_then(unix_millis),
                elapsed_ms: inner
                    .drain_started_at
                    .and_then(|started| now.duration_since(started).ok())
                    .map_or(0, |elapsed| {
                        elapsed.as_millis().min(u64::MAX as u128) as u64
                    }),
            },
        }
    }
}

/// RAII ownership of one admitted attempt and its cancellation source.
pub struct FrontendWorkloadLease {
    shared: Arc<LifecycleShared>,
    id: u64,
    kind: FrontendWorkloadKind,
    cancellation: QueryCancellationSource,
    released: AtomicBool,
}

impl FrontendWorkloadLease {
    pub fn cancellation_source(&self) -> QueryCancellationSource {
        self.cancellation.clone()
    }

    pub fn release(&self) {
        if self.released.swap(true, Ordering::AcqRel) {
            return;
        }
        let lifecycle = FrontendServingLifecycle {
            shared: Arc::clone(&self.shared),
        };
        lifecycle.release(self.id, self.kind);
    }
}

impl Drop for FrontendWorkloadLease {
    fn drop(&mut self) {
        self.release();
    }
}

fn admission_error(state: FrontendServingState) -> FrontendAdmissionError {
    match state {
        FrontendServingState::Draining => FrontendAdmissionError::Draining,
        FrontendServingState::Stopping => FrontendAdmissionError::Stopping,
        state => FrontendAdmissionError::NotReady { state },
    }
}

fn increment_total(totals: &mut FrontendWorkloadTotals, kind: FrontendWorkloadKind) {
    match kind {
        FrontendWorkloadKind::Session => totals.session += 1,
        FrontendWorkloadKind::Statement => totals.statement += 1,
        FrontendWorkloadKind::Background => totals.background += 1,
    }
}

fn unix_millis(time: SystemTime) -> Option<u64> {
    time.duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;

    #[test]
    fn admission_and_drain_are_linearly_ordered() {
        let lifecycle = FrontendServingLifecycle::new();
        lifecycle.mark_ready().expect("mark ready");
        let lease = lifecycle
            .try_admit(FrontendWorkloadKind::Statement)
            .expect("admit before drain");
        assert_eq!(
            lifecycle.begin_drain(Duration::from_secs(1)),
            FrontendServingState::Draining
        );
        assert!(matches!(
            lifecycle.try_admit(FrontendWorkloadKind::Statement),
            Err(FrontendAdmissionError::Draining)
        ));
        assert_eq!(
            lifecycle
                .frontend_serving_snapshot()
                .workload
                .active
                .statement,
            1
        );
        drop(lease);
        assert_eq!(
            lifecycle
                .frontend_serving_snapshot()
                .workload
                .active
                .statement,
            0
        );
    }

    #[test]
    fn session_registration_rejects_after_drain() {
        let lifecycle = FrontendServingLifecycle::new();
        lifecycle.mark_ready().expect("mark ready");
        lifecycle.begin_drain(Duration::from_secs(1));
        assert_eq!(
            lifecycle.register_session(|| 7),
            Err(FrontendAdmissionError::Draining)
        );
    }

    #[test]
    fn only_starting_can_become_ready_and_drain_is_idempotent() {
        let lifecycle = FrontendServingLifecycle::new();
        assert!(matches!(
            lifecycle.try_admit(FrontendWorkloadKind::Statement),
            Err(FrontendAdmissionError::NotReady {
                state: FrontendServingState::Starting
            })
        ));
        lifecycle.mark_ready().expect("mark ready");
        lifecycle.begin_drain(Duration::from_secs(2));
        lifecycle.begin_drain(Duration::from_secs(30));
        assert_eq!(
            lifecycle.mark_ready(),
            Err(FrontendAdmissionError::Draining)
        );
        let snapshot = lifecycle.frontend_serving_snapshot();
        assert_eq!(snapshot.serving_state, FrontendServingState::Draining);
        assert!(snapshot.drain.started_at_unix_ms.is_some());
        assert!(snapshot.drain.deadline_unix_ms.is_some());
    }

    #[tokio::test]
    async fn deadline_cancellation_is_first_wins_and_drop_wakes_waiters() {
        let lifecycle = FrontendServingLifecycle::new();
        lifecycle.mark_ready().expect("mark ready");
        let lease = lifecycle
            .try_admit(FrontendWorkloadKind::Background)
            .expect("admit background");
        lifecycle.begin_drain(Duration::from_secs(1));
        assert_eq!(lifecycle.cancel_active_at_drain_deadline(1_000), 1);
        assert_eq!(lifecycle.cancel_active_at_drain_deadline(1_000), 0);
        assert_eq!(
            lease.cancellation_source().view().reason(),
            Some(QueryCancellationReason::FrontendDrainDeadlineExceeded { timeout_ms: 1_000 })
        );
        lease.release();
        lifecycle.wait_for_no_active_work().await;
        let snapshot = lifecycle.frontend_serving_snapshot();
        assert_eq!(snapshot.workload.completed_during_drain.background, 1);
        assert_eq!(snapshot.workload.deadline_cancelled.background, 1);
    }

    #[test]
    fn snapshot_identity_rejects_non_sanitized_digest() {
        assert!(FrontendCatalogSnapshotIdentity::try_new(1, "0123456789abcdef").is_ok());
        assert!(FrontendCatalogSnapshotIdentity::try_new(1, "catalog-name").is_err());
    }

    #[test]
    fn late_bound_reader_stays_starting_until_the_owner_is_installed() {
        let reader = LateBoundFrontendServingSnapshotReader::default();
        assert_eq!(
            reader.frontend_serving_snapshot().serving_state,
            FrontendServingState::Starting
        );
        let lifecycle = Arc::new(FrontendServingLifecycle::new());
        lifecycle.mark_ready().expect("mark ready");
        reader.install(lifecycle).expect("install lifecycle reader");
        assert_eq!(
            reader.frontend_serving_snapshot().serving_state,
            FrontendServingState::Ready
        );
        assert!(
            reader
                .install(Arc::new(FrontendServingLifecycle::new()))
                .is_err()
        );
    }
}
