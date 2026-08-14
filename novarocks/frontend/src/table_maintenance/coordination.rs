// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.  The ASF
// licenses this file to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// License for the specific language governing permissions and limitations
// under the License.

//! Frontend-owned coordination for table-maintenance attempts.
//!
//! Application composition owns StateStore opening, control-plane bootstrap,
//! restore, and construction of the process-scoped lease manager. This facade
//! only admits writes and acquires per-table leases from those injected
//! primitives. Every repository transaction receives a validator backed by
//! the latest fence published after lease renewal.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use novarocks::maintenance::MaintenanceTarget;
use novarocks_spi::connector::ConnectorInstanceId;
use novarocks_spi::state_store::WriteTransaction;
use novarocks_state_store::OperationId;
use novarocks_state_store::coordination::{
    AcquireOutcome, AttemptId, CoordinationError, CoordinationErrorKind, HolderId, IncarnationGate,
    LeaseCancellationReason, LeaseFence, LeaseManager, LeaseObservation, LeaseSettings,
    ResourceKey, WriteAdmission,
};
use tokio::runtime::Handle;
use tokio::sync::{Mutex as AsyncMutex, watch};
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::model::MaintenanceAuthorityV1;

const RESOURCE_KEY_DOMAIN_V1: &[u8] = b"frontend/table-maintenance/table/v1\0";

pub const MAINTENANCE_LEASE_DURATION: Duration = Duration::from_secs(15);
pub const MAINTENANCE_LEASE_RENEW_INTERVAL: Duration = Duration::from_secs(5);
pub const MAINTENANCE_MAX_CLOCK_SKEW: Duration = Duration::from_secs(1);
pub const MAINTENANCE_TAKEOVER_OBSERVATION: Duration = Duration::from_secs(2);

/// How many definite acquire conflicts to absorb before giving up. Each one
/// proves the acquire did not happen, so retrying is safe; the bound keeps a
/// genuinely contended record from spinning.
const ACQUIRE_CONFLICT_RETRIES: u8 = 3;

/// How many definite release conflicts to absorb. A swallowed release failure
/// strands the table for a whole lease duration, so this retries rather than
/// leaving the next statement to wait it out.
const RELEASE_CONFLICT_RETRIES: u8 = 3;

pub type MaintenanceFenceValidator = Arc<
    dyn for<'txn> Fn(
            &'txn mut dyn WriteTransaction,
        ) -> Pin<
            Box<dyn Future<Output = Result<(), MaintenanceAuthorityFailure>> + Send + 'txn>,
        > + Send
        + Sync,
>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MaintenanceCoordinationError {
    InvalidTarget(String),
    Coordination(CoordinationError),
    RenewalTask(String),
}

impl fmt::Display for MaintenanceCoordinationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTarget(message) => {
                write!(formatter, "invalid maintenance target: {message}")
            }
            Self::Coordination(error) => write!(formatter, "{error}"),
            Self::RenewalTask(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for MaintenanceCoordinationError {}

impl From<CoordinationError> for MaintenanceCoordinationError {
    fn from(error: CoordinationError) -> Self {
        Self::Coordination(error)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MaintenanceAuthorityFailure {
    Coordination(CoordinationError),
    Cancelled(LeaseCancellationReason),
}

impl fmt::Display for MaintenanceAuthorityFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Coordination(error) => write!(formatter, "{error}"),
            Self::Cancelled(reason) => {
                write!(formatter, "maintenance authority cancelled: {reason:?}")
            }
        }
    }
}

impl std::error::Error for MaintenanceAuthorityFailure {}

pub fn maintenance_lease_settings() -> Result<LeaseSettings, CoordinationError> {
    LeaseSettings::new(
        MAINTENANCE_LEASE_DURATION,
        MAINTENANCE_LEASE_RENEW_INTERVAL,
        MAINTENANCE_MAX_CLOCK_SKEW,
        MAINTENANCE_TAKEOVER_OBSERVATION,
    )
}

pub fn new_maintenance_holder_id() -> Result<HolderId, CoordinationError> {
    HolderId::try_from(Bytes::from(format!(
        "frontend-table-maintenance-{}",
        Uuid::now_v7()
    )))
}

/// Encodes one canonical logical table resource. The length prefixes make the
/// identity unambiguous even when future identifier rules admit separators.
pub fn maintenance_resource_key_v1(
    target: &MaintenanceTarget,
) -> Result<ResourceKey, MaintenanceCoordinationError> {
    ResourceKey::try_from(resource_key_bytes_v1(target)?).map_err(Into::into)
}

/// Canonicalize one namespace or table segment of a coordination resource key.
///
/// This is deliberately not SQL-identifier validation. The key is a concurrency
/// identity for an external table that already exists, and lake table names may
/// legally contain characters no SQL identifier allows (`sales-2026`). Applying
/// identifier rules here would make every maintenance action on such a table
/// fail closed at acquire, which is a worse outcome than coordinating on its
/// canonical name. Only genuinely unusable segments are rejected: empty after
/// trimming, or carrying control characters that no canonical form can express.
fn normalize_resource_component(
    raw: &str,
    what: &str,
) -> Result<String, MaintenanceCoordinationError> {
    let trimmed = raw.trim();
    let trimmed = trimmed
        .strip_prefix('`')
        .and_then(|value| value.strip_suffix('`'))
        .unwrap_or(trimmed)
        .trim();
    if trimmed.is_empty() {
        return Err(MaintenanceCoordinationError::InvalidTarget(format!(
            "maintenance {what} is empty"
        )));
    }
    if trimmed.chars().any(char::is_control) {
        return Err(MaintenanceCoordinationError::InvalidTarget(format!(
            "maintenance {what} contains control characters"
        )));
    }
    Ok(trimmed.to_lowercase())
}

fn resource_key_bytes_v1(
    target: &MaintenanceTarget,
) -> Result<Bytes, MaintenanceCoordinationError> {
    let instance = ConnectorInstanceId::parse(&target.catalog)
        .map_err(|error| MaintenanceCoordinationError::InvalidTarget(error.to_string()))?;
    let namespace = normalize_resource_component(&target.namespace, "namespace")?;
    let table = normalize_resource_component(&target.table, "table")?;

    let components = [
        instance.as_str().as_bytes(),
        namespace.as_bytes(),
        table.as_bytes(),
    ];
    let encoded_capacity =
        components
            .iter()
            .try_fold(RESOURCE_KEY_DOMAIN_V1.len(), |capacity, component| {
                let _ = u16::try_from(component.len()).map_err(|_| {
                    MaintenanceCoordinationError::InvalidTarget(
                        "a normalized resource component exceeds 65535 bytes".to_string(),
                    )
                })?;
                capacity.checked_add(2 + component.len()).ok_or_else(|| {
                    MaintenanceCoordinationError::InvalidTarget(
                        "the canonical resource key length overflows".to_string(),
                    )
                })
            })?;
    let mut encoded = BytesMut::with_capacity(encoded_capacity);
    encoded.extend_from_slice(RESOURCE_KEY_DOMAIN_V1);
    for component in components {
        encoded.put_u16(u16::try_from(component.len()).expect("component length was validated"));
        encoded.extend_from_slice(component);
    }
    Ok(encoded.freeze())
}

pub enum MaintenanceAcquireOutcome {
    Acquired(MaintenanceLeaseAttempt),
    Contended(LeaseObservation),
    AwaitingTakeover(LeaseObservation),
}

// Design: ADR-0065 (docs/adr/ADR-0065-per-table-maintenance-lease-attempt-authority.md)
#[derive(Clone)]
pub struct MaintenanceCoordination {
    gate: Arc<IncarnationGate>,
    manager: LeaseManager,
    runtime: Handle,
}

impl MaintenanceCoordination {
    pub fn new(gate: IncarnationGate, manager: LeaseManager, runtime: Handle) -> Self {
        Self {
            gate: Arc::new(gate),
            manager,
            runtime,
        }
    }

    /// Build the production facade from the host-owned coordination runtime.
    ///
    /// The gate handle and the process-scoped `HolderId` are shared with every
    /// other frontend domain, so table maintenance neither opens a second
    /// StateStore nor creates a second control incarnation.
    pub(crate) fn from_frontend(
        frontend: &crate::coordination::FrontendCoordinationRuntime,
        runtime: Handle,
    ) -> Self {
        Self {
            gate: frontend.gate(),
            manager: frontend.lease_manager(),
            runtime,
        }
    }

    pub async fn admit_writes(&self) -> Result<WriteAdmission, CoordinationError> {
        self.gate.admit_writes().await
    }

    pub async fn acquire(
        &self,
        target: &MaintenanceTarget,
    ) -> Result<MaintenanceAcquireOutcome, MaintenanceCoordinationError> {
        let resource = maintenance_resource_key_v1(target)?;
        let mut remaining = ACQUIRE_CONFLICT_RETRIES;
        let (attempt_uuid, outcome) = loop {
            let attempt_uuid = Uuid::now_v7();
            let attempt = AttemptId::try_from(attempt_uuid)?;
            let operation_id = OperationId::new_v7();
            match self
                .manager
                .acquire(resource.clone(), attempt, operation_id)
                .await
            {
                Ok(outcome) => break (attempt_uuid, outcome),
                Err(error) if error.kind() == CoordinationErrorKind::CommitUncertain => {
                    break (
                        attempt_uuid,
                        self.manager
                            .recover_acquire(resource.clone(), attempt, operation_id)
                            .await?,
                    );
                }
                // A definite transaction conflict: the acquire provably did not
                // take effect, so nothing is half-done and a fresh attempt and
                // operation ID may simply try again. Statements against one
                // table arrive back to back, and each release races the next
                // acquire on the same record.
                Err(error)
                    if error.kind() == CoordinationErrorKind::OperationNotCommitted
                        && remaining > 0 =>
                {
                    remaining -= 1;
                }
                Err(error) => return Err(error.into()),
            }
        };
        Ok(match outcome {
            AcquireOutcome::Acquired(guard) => MaintenanceAcquireOutcome::Acquired(
                MaintenanceLeaseAttempt::start(attempt_uuid, guard, &self.runtime),
            ),
            AcquireOutcome::Contended(observation) => {
                MaintenanceAcquireOutcome::Contended(observation)
            }
            AcquireOutcome::AwaitingTakeover(observation) => {
                MaintenanceAcquireOutcome::AwaitingTakeover(observation)
            }
        })
    }
}

struct MaintenanceLeaseAttemptInner {
    attempt_id: Uuid,
    guard: Arc<AsyncMutex<novarocks_state_store::coordination::LeaseGuard>>,
    fence_rx: watch::Receiver<LeaseFence>,
    failure_rx: watch::Receiver<Option<CoordinationError>>,
    cancellation_rx: watch::Receiver<Option<LeaseCancellationReason>>,
    stop_tx: watch::Sender<bool>,
    renew_task: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for MaintenanceLeaseAttemptInner {
    fn drop(&mut self) {
        self.stop_tx.send_replace(true);
        if let Some(task) = self
            .renew_task
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            task.abort();
        }
    }
}

#[derive(Clone)]
pub struct MaintenanceLeaseAttempt {
    inner: Arc<MaintenanceLeaseAttemptInner>,
}

impl MaintenanceLeaseAttempt {
    fn start(
        attempt_id: Uuid,
        guard: novarocks_state_store::coordination::LeaseGuard,
        runtime: &Handle,
    ) -> Self {
        let initial_fence = guard.fence();
        let cancellation_rx = guard.cancellation();
        let renew_after = guard.renew_after();
        let guard = Arc::new(AsyncMutex::new(guard));
        let (fence_tx, fence_rx) = watch::channel(initial_fence);
        let (failure_tx, failure_rx) = watch::channel(None);
        let (stop_tx, stop_rx) = watch::channel(false);
        let renew_task = runtime.spawn(run_renewal_loop(
            Arc::clone(&guard),
            renew_after,
            fence_tx,
            failure_tx,
            stop_rx,
        ));
        Self {
            inner: Arc::new(MaintenanceLeaseAttemptInner {
                attempt_id,
                guard,
                fence_rx,
                failure_rx,
                cancellation_rx,
                stop_tx,
                renew_task: Mutex::new(Some(renew_task)),
            }),
        }
    }

    pub fn attempt_id(&self) -> Uuid {
        self.inner.attempt_id
    }

    pub fn fence(&self) -> LeaseFence {
        self.inner.fence_rx.borrow().clone()
    }

    /// Returns the canonical provenance that must be persisted with a fenced
    /// repository transition. It is read from the held guard so the attempt id
    /// and token always describe the same lease epoch.
    pub async fn durable_authority(
        &self,
    ) -> Result<MaintenanceAuthorityV1, MaintenanceCoordinationError> {
        self.ensure_active()
            .map_err(|error| MaintenanceCoordinationError::RenewalTask(error.to_string()))?;
        let guard = self.inner.guard.lock().await;
        let token = guard.token().encode_v1()?;
        MaintenanceAuthorityV1::try_new(self.attempt_id(), token.to_vec())
            .map_err(MaintenanceCoordinationError::RenewalTask)
    }

    pub fn fence_validator(&self) -> MaintenanceFenceValidator {
        let fence_rx = self.inner.fence_rx.clone();
        let failure_rx = self.inner.failure_rx.clone();
        let cancellation_rx = self.inner.cancellation_rx.clone();
        Arc::new(move |transaction| {
            let failure_rx = failure_rx.clone();
            let cancellation_rx = cancellation_rx.clone();
            let fence = fence_rx.borrow().clone();
            Box::pin(async move {
                if let Some(error) = failure_rx.borrow().clone() {
                    return Err(MaintenanceAuthorityFailure::Coordination(error));
                }
                if let Some(reason) = *cancellation_rx.borrow() {
                    return Err(MaintenanceAuthorityFailure::Cancelled(reason));
                }
                fence
                    .validate_in(transaction)
                    .await
                    .map_err(MaintenanceAuthorityFailure::Coordination)?;
                if let Some(error) = failure_rx.borrow().clone() {
                    return Err(MaintenanceAuthorityFailure::Coordination(error));
                }
                if let Some(reason) = *cancellation_rx.borrow() {
                    return Err(MaintenanceAuthorityFailure::Cancelled(reason));
                }
                Ok(())
            })
        })
    }

    pub fn cancellation(&self) -> watch::Receiver<Option<LeaseCancellationReason>> {
        self.inner.cancellation_rx.clone()
    }

    pub fn authority_failure(&self) -> Option<MaintenanceAuthorityFailure> {
        if let Some(error) = self.inner.failure_rx.borrow().clone() {
            return Some(MaintenanceAuthorityFailure::Coordination(error));
        }
        self.inner
            .cancellation_rx
            .borrow()
            .map(MaintenanceAuthorityFailure::Cancelled)
    }

    pub fn ensure_active(&self) -> Result<(), MaintenanceAuthorityFailure> {
        self.authority_failure().map_or(Ok(()), Err)
    }

    pub async fn release(&self) -> Result<(), MaintenanceCoordinationError> {
        self.stop_renewal().await?;
        let mut guard = self.inner.guard.lock().await;
        let mut remaining = RELEASE_CONFLICT_RETRIES;
        loop {
            let operation_id = OperationId::new_v7();
            let result = match guard.release(operation_id).await {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == CoordinationErrorKind::CommitUncertain => {
                    guard.recover_release(operation_id).await
                }
                Err(error) => Err(error),
            };
            match result {
                Ok(()) => return Ok(()),
                // A definite transaction conflict leaves the guard active and
                // clears its recovery state, so releasing under a fresh
                // operation ID is safe. Giving up here instead would strand the
                // table until the lease expires -- fifteen seconds during which
                // the next statement on it is refused for no reason.
                Err(error)
                    if error.kind() == CoordinationErrorKind::OperationNotCommitted
                        && remaining > 0 =>
                {
                    remaining -= 1;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    async fn stop_renewal(&self) -> Result<(), MaintenanceCoordinationError> {
        self.inner.stop_tx.send_replace(true);
        let task = self
            .inner
            .renew_task
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(task) = task {
            task.await.map_err(|error| {
                MaintenanceCoordinationError::RenewalTask(format!(
                    "table-maintenance renewal task failed: {error}"
                ))
            })?;
        }
        Ok(())
    }
}

async fn run_renewal_loop(
    guard: Arc<AsyncMutex<novarocks_state_store::coordination::LeaseGuard>>,
    renew_after: Duration,
    fence_tx: watch::Sender<LeaseFence>,
    failure_tx: watch::Sender<Option<CoordinationError>>,
    mut stop_rx: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(renew_after) => {
                let operation_id = OperationId::new_v7();
                let mut guard = guard.lock().await;
                let result = match guard.renew(operation_id).await {
                    Ok(()) => Ok(()),
                    Err(error) if error.kind() == CoordinationErrorKind::CommitUncertain => {
                        guard.recover_renew(operation_id).await
                    }
                    Err(error) => Err(error),
                };
                match result {
                    Ok(()) => {
                        fence_tx.send_replace(guard.fence());
                    }
                    Err(error) => {
                        failure_tx.send_replace(Some(error));
                        return;
                    }
                }
            }
            changed = stop_rx.changed() => {
                if changed.is_err() || *stop_rx.borrow() {
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use novarocks::maintenance::MaintenanceTarget;
    use novarocks_spi::state_store::{FeDeploymentView, StateStore, TransactionId};
    use novarocks_state_store::coordination::{
        ClockHealth, CoordinationError, CoordinationErrorKind, IncarnationGate, LeaseClock,
        LeaseManager, LeaseSettings,
    };
    use novarocks_state_store::{
        OperationId, StateStoreAppConfig, StateStoreConfig, StateStoreHost, StateStoreHostConfig,
        StateStoreLimitOverrides, StateStoreProviderConfig, builtin_state_store_provider_registry,
    };
    use tempfile::TempDir;
    use tokio::runtime::Handle;
    use tokio::time::timeout;
    use uuid::Uuid;

    use super::{
        MAINTENANCE_LEASE_DURATION, MAINTENANCE_LEASE_RENEW_INTERVAL, MAINTENANCE_MAX_CLOCK_SKEW,
        MAINTENANCE_TAKEOVER_OBSERVATION, MaintenanceAcquireOutcome, MaintenanceAuthorityFailure,
        MaintenanceCoordination, maintenance_lease_settings, maintenance_resource_key_v1,
        new_maintenance_holder_id, resource_key_bytes_v1,
    };

    struct AdvancingClock {
        now_ms: AtomicU64,
    }

    impl AdvancingClock {
        fn new(now_ms: u64) -> Self {
            Self {
                now_ms: AtomicU64::new(now_ms),
            }
        }
    }

    impl LeaseClock for AdvancingClock {
        fn wall_time_millis(&self) -> Result<u64, CoordinationError> {
            Ok(self.now_ms.fetch_add(10, Ordering::SeqCst))
        }

        fn monotonic_time_millis(&self) -> u64 {
            self.now_ms.load(Ordering::SeqCst)
        }

        fn health(&self) -> ClockHealth {
            ClockHealth::Healthy
        }
    }

    struct SwitchableClock {
        now_ms: AtomicU64,
        unsafe_clock: AtomicBool,
    }

    impl SwitchableClock {
        fn new(now_ms: u64) -> Self {
            Self {
                now_ms: AtomicU64::new(now_ms),
                unsafe_clock: AtomicBool::new(false),
            }
        }

        fn make_unsafe(&self) {
            self.unsafe_clock.store(true, Ordering::SeqCst);
        }
    }

    impl LeaseClock for SwitchableClock {
        fn wall_time_millis(&self) -> Result<u64, CoordinationError> {
            if self.unsafe_clock.load(Ordering::SeqCst) {
                Err(CoordinationError::clock_unsafe())
            } else {
                Ok(self.now_ms.fetch_add(10, Ordering::SeqCst))
            }
        }

        fn monotonic_time_millis(&self) -> u64 {
            self.now_ms.load(Ordering::SeqCst)
        }

        fn health(&self) -> ClockHealth {
            if self.unsafe_clock.load(Ordering::SeqCst) {
                ClockHealth::Unsafe
            } else {
                ClockHealth::Healthy
            }
        }
    }

    fn target(catalog: &str, namespace: &str, table: &str) -> MaintenanceTarget {
        MaintenanceTarget {
            catalog: catalog.to_string(),
            namespace: namespace.to_string(),
            table: table.to_string(),
        }
    }

    async fn open_sqlite(path: &Path) -> (StateStoreHost, Arc<dyn StateStore>) {
        let host = StateStoreHost::open(
            &builtin_state_store_provider_registry().expect("built-in StateStore providers"),
            StateStoreHostConfig {
                state_store: StateStoreAppConfig {
                    store: StateStoreConfig {
                        cluster_id: "maintenance-coordination-test".to_string(),
                        limits: StateStoreLimitOverrides::default(),
                        provider: StateStoreProviderConfig::Sqlite {
                            path: path.to_path_buf(),
                            deployment_owner: "maintenance-coordination-fe".to_string(),
                        },
                    },
                    mysql_client: None,
                },
                foundationdb_client: None,
            },
            FeDeploymentView {
                active_fe_count: NonZeroUsize::new(1).expect("one FE"),
                topology_revision: Bytes::from_static(b"maintenance-coordination-topology"),
            },
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .expect("open SQLite StateStore");
        let store = host.state_store().expect("opened StateStore");
        (host, store)
    }

    fn manager(
        store: Arc<dyn StateStore>,
        lease_duration: Duration,
        renew_interval: Duration,
    ) -> LeaseManager {
        manager_with_clock(
            store,
            Arc::new(AdvancingClock::new(10_000)),
            lease_duration,
            renew_interval,
        )
    }

    fn manager_with_clock(
        store: Arc<dyn StateStore>,
        clock: Arc<dyn LeaseClock>,
        lease_duration: Duration,
        renew_interval: Duration,
    ) -> LeaseManager {
        LeaseManager::new(
            store,
            new_maintenance_holder_id().expect("process holder"),
            clock,
            LeaseSettings::new(
                lease_duration,
                renew_interval,
                Duration::ZERO,
                Duration::from_millis(10),
            )
            .expect("test lease settings"),
        )
        .expect("lease manager")
    }

    #[test]
    fn maintenance_defaults_and_process_holders_are_explicit() {
        let settings = maintenance_lease_settings().expect("maintenance lease settings");
        assert_eq!(settings.lease_duration(), MAINTENANCE_LEASE_DURATION);
        assert_eq!(settings.renew_interval(), MAINTENANCE_LEASE_RENEW_INTERVAL);
        assert_eq!(settings.max_clock_skew(), MAINTENANCE_MAX_CLOCK_SKEW);
        assert_eq!(
            settings.observation_window(),
            MAINTENANCE_TAKEOVER_OBSERVATION
        );
        assert_ne!(
            new_maintenance_holder_id().expect("first process holder"),
            new_maintenance_holder_id().expect("second process holder")
        );
    }

    #[test]
    fn resource_codec_coordinates_lake_names_that_are_not_sql_identifiers() {
        // A hyphenated or dotted lake table is legal and must still be
        // coordinable; rejecting it here would make every maintenance action on
        // that table fail closed at acquire.
        let hyphenated = target("analytics", "sales-eu", "orders-2026");
        let key = maintenance_resource_key_v1(&hyphenated).expect("hyphenated target key");
        assert_eq!(
            key,
            maintenance_resource_key_v1(&target("analytics", " Sales-EU ", "`ORDERS-2026`"))
                .expect("alias key")
        );
        assert_ne!(
            key,
            maintenance_resource_key_v1(&target("analytics", "sales-eu", "orders-2025"))
                .expect("different table key")
        );
        assert!(maintenance_resource_key_v1(&target("analytics", "sales", "  ")).is_err());
        assert!(maintenance_resource_key_v1(&target("analytics", "sales", "or\u{7}ders")).is_err());
    }

    #[test]
    fn resource_codec_is_versioned_canonical_and_bounded() {
        let canonical = target("analytics", "sales", "orders");
        let aliases = target("ANALYTICS", " Sales ", "`ORDERS`");
        assert_eq!(
            maintenance_resource_key_v1(&canonical).expect("canonical key"),
            maintenance_resource_key_v1(&aliases).expect("alias key")
        );
        assert_ne!(
            maintenance_resource_key_v1(&canonical).expect("canonical key"),
            maintenance_resource_key_v1(&target("analytics", "sales", "customers"))
                .expect("different table key")
        );
        assert!(
            resource_key_bytes_v1(&canonical)
                .expect("encoded key")
                .starts_with(b"frontend/table-maintenance/table/v1\0")
        );
        assert!(maintenance_resource_key_v1(&target("", "sales", "orders")).is_err());
        assert!(
            maintenance_resource_key_v1(&target(
                "analytics",
                &format!("a{}", "x".repeat(70_000)),
                "orders"
            ))
            .is_err()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn facade_does_not_bootstrap_control_plane() {
        let temp = TempDir::new().expect("temporary directory");
        let (mut host, store) = open_sqlite(&temp.path().join("state.sqlite")).await;
        let coordination = MaintenanceCoordination::new(
            IncarnationGate::new(Arc::clone(&store)),
            manager(
                Arc::clone(&store),
                Duration::from_millis(100),
                Duration::from_millis(20),
            ),
            Handle::current(),
        );

        assert_eq!(
            coordination
                .admit_writes()
                .await
                .expect_err("unbootstrapped gate must fail closed")
                .kind(),
            CoordinationErrorKind::NotBootstrapped
        );
        let error = coordination
            .acquire(&target("analytics", "sales", "orders"))
            .await
            .err()
            .expect("unbootstrapped acquire must fail closed");
        assert!(error.to_string().contains("NotBootstrapped"));

        drop(coordination);
        drop(store);
        host.shutdown(Instant::now() + Duration::from_secs(5))
            .await
            .expect("shutdown StateStore");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_holders_contend_only_for_the_same_canonical_table() {
        let temp = TempDir::new().expect("temporary directory");
        let (mut host, store) = open_sqlite(&temp.path().join("state.sqlite")).await;
        IncarnationGate::new(Arc::clone(&store))
            .bootstrap(OperationId::new_v7())
            .await
            .expect("bootstrap control plane");
        let first = MaintenanceCoordination::new(
            IncarnationGate::new(Arc::clone(&store)),
            manager(
                Arc::clone(&store),
                Duration::from_secs(5),
                Duration::from_secs(1),
            ),
            Handle::current(),
        );
        let second = MaintenanceCoordination::new(
            IncarnationGate::new(Arc::clone(&store)),
            manager(
                Arc::clone(&store),
                Duration::from_secs(5),
                Duration::from_secs(1),
            ),
            Handle::current(),
        );

        let first_attempt = match first
            .acquire(&target("analytics", "sales", "orders"))
            .await
            .expect("first holder acquires")
        {
            MaintenanceAcquireOutcome::Acquired(attempt) => attempt,
            _ => panic!("first holder must acquire"),
        };
        assert!(matches!(
            second
                .acquire(&target("ANALYTICS", " Sales ", "`ORDERS`"))
                .await
                .expect("second holder observes contention"),
            MaintenanceAcquireOutcome::Contended(_)
        ));
        let independent_attempt = match second
            .acquire(&target("analytics", "sales", "customers"))
            .await
            .expect("different table acquires independently")
        {
            MaintenanceAcquireOutcome::Acquired(attempt) => attempt,
            _ => panic!("different table must not contend"),
        };

        independent_attempt
            .release()
            .await
            .expect("release independent lease");
        first_attempt.release().await.expect("release first lease");
        drop(independent_attempt);
        drop(first_attempt);
        drop(second);
        drop(first);
        drop(store);
        host.shutdown(Instant::now() + Duration::from_secs(5))
            .await
            .expect("shutdown StateStore");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn renewal_failure_revokes_transaction_authority() {
        let temp = TempDir::new().expect("temporary directory");
        let (mut host, store) = open_sqlite(&temp.path().join("state.sqlite")).await;
        IncarnationGate::new(Arc::clone(&store))
            .bootstrap(OperationId::new_v7())
            .await
            .expect("bootstrap control plane");
        let clock = Arc::new(SwitchableClock::new(10_000));
        let lease_clock: Arc<dyn LeaseClock> = clock.clone();
        let coordination = MaintenanceCoordination::new(
            IncarnationGate::new(Arc::clone(&store)),
            manager_with_clock(
                Arc::clone(&store),
                lease_clock,
                Duration::from_millis(100),
                Duration::from_millis(20),
            ),
            Handle::current(),
        );
        let attempt = match coordination
            .acquire(&target("analytics", "sales", "orders"))
            .await
            .expect("acquire maintenance lease")
        {
            MaintenanceAcquireOutcome::Acquired(attempt) => attempt,
            _ => panic!("first maintenance attempt must acquire"),
        };
        clock.make_unsafe();

        let failure = timeout(Duration::from_secs(2), async {
            loop {
                if let Some(failure) = attempt.authority_failure() {
                    return failure;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("unsafe clock must fail the renewal loop");
        assert!(
            matches!(
                &failure,
                MaintenanceAuthorityFailure::Coordination(error)
                    if error.kind() == CoordinationErrorKind::ClockUnsafe
            ) || matches!(
                &failure,
                MaintenanceAuthorityFailure::Cancelled(
                    novarocks_state_store::coordination::LeaseCancellationReason::ClockUnsafe
                )
            )
        );
        assert_eq!(attempt.ensure_active(), Err(failure));

        let validator = attempt.fence_validator();
        let mut transaction = store
            .begin_write(
                TransactionId::from(Uuid::now_v7()),
                "reject write after maintenance authority failure",
            )
            .await
            .expect("begin validation transaction");
        let validation_failure = validator(transaction.as_mut())
            .await
            .expect_err("failed renewal must revoke transaction authority");
        assert!(
            matches!(
                validation_failure,
                MaintenanceAuthorityFailure::Coordination(ref error)
                    if error.kind() == CoordinationErrorKind::ClockUnsafe
            ) || matches!(
                validation_failure,
                MaintenanceAuthorityFailure::Cancelled(
                    novarocks_state_store::coordination::LeaseCancellationReason::ClockUnsafe
                )
            )
        );
        transaction
            .abort()
            .await
            .expect("abort rejected transaction");

        drop(attempt);
        drop(coordination);
        drop(store);
        host.shutdown(Instant::now() + Duration::from_secs(5))
            .await
            .expect("shutdown StateStore");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn renewal_publishes_latest_fence_for_transaction_validation() {
        let temp = TempDir::new().expect("temporary directory");
        let (mut host, store) = open_sqlite(&temp.path().join("state.sqlite")).await;
        IncarnationGate::new(Arc::clone(&store))
            .bootstrap(OperationId::new_v7())
            .await
            .expect("bootstrap control plane");
        let coordination = MaintenanceCoordination::new(
            IncarnationGate::new(Arc::clone(&store)),
            manager(
                Arc::clone(&store),
                Duration::from_millis(100),
                Duration::from_millis(20),
            ),
            Handle::current(),
        );
        let attempt = match coordination
            .acquire(&target("analytics", "sales", "orders"))
            .await
            .expect("acquire maintenance lease")
        {
            MaintenanceAcquireOutcome::Acquired(attempt) => attempt,
            _ => panic!("first maintenance attempt must acquire"),
        };
        let initial_fence = attempt.fence();

        timeout(Duration::from_secs(2), async {
            loop {
                if attempt.fence() != initial_fence {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("renewal must publish a new exact fence");

        let mut stale_transaction = store
            .begin_write(
                TransactionId::from(Uuid::now_v7()),
                "validate stale maintenance fence",
            )
            .await
            .expect("begin stale validation transaction");
        assert_eq!(
            initial_fence
                .validate_in(stale_transaction.as_mut())
                .await
                .expect_err("pre-renew fence must be stale")
                .kind(),
            CoordinationErrorKind::FenceLost
        );
        stale_transaction
            .abort()
            .await
            .expect("abort stale transaction");

        let validator = attempt.fence_validator();
        let mut current_transaction = store
            .begin_write(
                TransactionId::from(Uuid::now_v7()),
                "validate current maintenance fence",
            )
            .await
            .expect("begin current validation transaction");
        validator(current_transaction.as_mut())
            .await
            .expect("dynamic validator must use latest fence");
        current_transaction
            .abort()
            .await
            .expect("abort current transaction");
        attempt
            .ensure_active()
            .expect("renewed attempt remains active");
        attempt.release().await.expect("release maintenance lease");

        drop(attempt);
        drop(coordination);
        drop(store);
        host.shutdown(Instant::now() + Duration::from_secs(5))
            .await
            .expect("shutdown StateStore");
    }
}
