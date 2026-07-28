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

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use novarocks_spi::state_store::{
    ChangePage, ChangePollRequest, CommitResolution, ReadTransaction, StateStore, StateStoreError,
    StateStoreErrorKind, StateStoreLimits, StateStoreMetricsSnapshot, StateStoreOpenRequest,
    StateStoreProviderDescriptor, StateStoreProviderFactory, StateStoreProviderId,
    StateStoreProviderInstance, StateStoreProviderLifecycle, StoreIdentity, TransactionId,
    WriteTransaction,
};
use novarocks_state_store::{
    FeDeploymentView, SQLITE_STATE_STORE_PROVIDER_ID, StateStoreAppConfig, StateStoreConfig,
    StateStoreHost, StateStoreHostConfig, StateStoreHostErrorKind, StateStoreHostLifecycle,
    StateStoreLimitOverrides, StateStoreProviderConfig, StateStoreProviderRegistration,
    StateStoreProviderRegistry, builtin_state_store_provider_registry,
};

const OTHER_PROVIDER_ID: StateStoreProviderId = StateStoreProviderId::new("other");

#[derive(Clone, Copy)]
enum FakeOpen {
    Ready,
    Fail,
    MismatchedInstance { cleanup_fails: bool },
}

struct FakeFactory {
    descriptor: StateStoreProviderDescriptor,
    mode: FakeOpen,
    events: Arc<Mutex<Vec<&'static str>>>,
    open_deadline: Arc<Mutex<Option<Instant>>>,
    shutdown_deadline: Arc<Mutex<Option<Instant>>>,
}

#[async_trait]
impl StateStoreProviderFactory for FakeFactory {
    fn descriptor(&self) -> &StateStoreProviderDescriptor {
        &self.descriptor
    }

    async fn open(
        self: Box<Self>,
        request: StateStoreOpenRequest,
    ) -> Result<Box<dyn StateStoreProviderInstance>, StateStoreError> {
        self.events.lock().unwrap().push("factory_open");
        *self.open_deadline.lock().unwrap() = Some(request.deadline);
        if matches!(self.mode, FakeOpen::Fail) {
            return Err(StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "fake open failed",
            ));
        }
        let (descriptor, cleanup_fails) = match self.mode {
            FakeOpen::Ready => (self.descriptor, false),
            FakeOpen::MismatchedInstance { cleanup_fails } => (
                StateStoreProviderDescriptor::new(OTHER_PROVIDER_ID),
                cleanup_fails,
            ),
            FakeOpen::Fail => unreachable!(),
        };
        Ok(Box::new(FakeInstance {
            descriptor,
            lifecycle: StateStoreProviderLifecycle::Ready,
            state_store: Some(Arc::new(FakeStore)),
            events: Arc::clone(&self.events),
            cleanup_fails,
            shutdown_deadline: Arc::clone(&self.shutdown_deadline),
        }))
    }
}

struct FakeInstance {
    descriptor: StateStoreProviderDescriptor,
    lifecycle: StateStoreProviderLifecycle,
    state_store: Option<Arc<dyn StateStore>>,
    events: Arc<Mutex<Vec<&'static str>>>,
    cleanup_fails: bool,
    shutdown_deadline: Arc<Mutex<Option<Instant>>>,
}

#[async_trait]
impl StateStoreProviderInstance for FakeInstance {
    fn descriptor(&self) -> &StateStoreProviderDescriptor {
        &self.descriptor
    }

    fn lifecycle(&self) -> StateStoreProviderLifecycle {
        self.lifecycle
    }

    fn state_store(&self) -> Option<Arc<dyn StateStore>> {
        self.state_store.clone()
    }

    async fn shutdown(&mut self, deadline: Instant) -> Result<(), StateStoreError> {
        *self.shutdown_deadline.lock().unwrap() = Some(deadline);
        self.lifecycle = StateStoreProviderLifecycle::Draining;
        if deadline <= Instant::now() {
            return Err(StateStoreError::new(
                StateStoreErrorKind::DeadlineExceeded,
                "fake shutdown deadline exceeded",
            ));
        }
        if Arc::strong_count(self.state_store.as_ref().unwrap()) == 1 {
            self.events.lock().unwrap().push("host_exposure_dropped");
        }
        self.events.lock().unwrap().push("instance_shutdown");
        if self.cleanup_fails {
            return Err(StateStoreError::new(
                StateStoreErrorKind::Internal,
                "fake cleanup failed",
            ));
        }
        self.state_store.take();
        self.lifecycle = StateStoreProviderLifecycle::Stopped;
        Ok(())
    }
}

struct FakeStore;

#[async_trait]
impl StateStore for FakeStore {
    fn provider_name(&self) -> &'static str {
        "fake"
    }

    fn limits(&self) -> &StateStoreLimits {
        static LIMITS: std::sync::LazyLock<StateStoreLimits> =
            std::sync::LazyLock::new(StateStoreLimits::default);
        &LIMITS
    }

    fn metrics_snapshot(&self) -> StateStoreMetricsSnapshot {
        panic!("unused fake store operation")
    }

    async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
        panic!("unused fake store operation")
    }

    async fn begin_write(
        &self,
        _transaction_id: TransactionId,
        _purpose: &str,
    ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
        panic!("unused fake store operation")
    }

    async fn poll_changes(
        &self,
        _request: &ChangePollRequest,
    ) -> Result<ChangePage, StateStoreError> {
        panic!("unused fake store operation")
    }

    async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
        panic!("unused fake store operation")
    }

    async fn resolve_commit(
        &self,
        _transaction_id: &TransactionId,
    ) -> Result<CommitResolution, StateStoreError> {
        panic!("unused fake store operation")
    }
}

fn sqlite_host_config(path: std::path::PathBuf) -> StateStoreHostConfig {
    StateStoreHostConfig {
        state_store: StateStoreAppConfig {
            store: StateStoreConfig {
                cluster_id: "cluster-a".to_owned(),
                limits: StateStoreLimitOverrides::default(),
                provider: StateStoreProviderConfig::Sqlite {
                    path,
                    deployment_owner: "fe-a".to_owned(),
                },
            },
            mysql_client: None,
        },
        foundationdb_client: None,
    }
}

fn single_fe_view() -> FeDeploymentView {
    FeDeploymentView {
        active_fe_count: NonZeroUsize::new(1).unwrap(),
        topology_revision: Bytes::from_static(b"topology-r1"),
    }
}

type FakeControls = (
    Arc<Mutex<Vec<&'static str>>>,
    Arc<Mutex<Option<Instant>>>,
    Arc<Mutex<Option<Instant>>>,
);

fn fake_registry(mode: FakeOpen) -> (StateStoreProviderRegistry, FakeControls) {
    let events = Arc::new(Mutex::new(Vec::new()));
    let open_deadline = Arc::new(Mutex::new(None));
    let shutdown_deadline = Arc::new(Mutex::new(None));
    let mut registry = StateStoreProviderRegistry::new();
    let binder_events = Arc::clone(&events);
    let binder_open_deadline = Arc::clone(&open_deadline);
    let binder_shutdown_deadline = Arc::clone(&shutdown_deadline);
    registry
        .register(StateStoreProviderRegistration::available(
            SQLITE_STATE_STORE_PROVIDER_ID,
            novarocks_spi::state_store::MAX_KEY_BYTES,
            move |_| {
                Ok(Box::new(FakeFactory {
                    descriptor: StateStoreProviderDescriptor::new(SQLITE_STATE_STORE_PROVIDER_ID),
                    mode,
                    events: Arc::clone(&binder_events),
                    open_deadline: Arc::clone(&binder_open_deadline),
                    shutdown_deadline: Arc::clone(&binder_shutdown_deadline),
                }))
            },
        ))
        .unwrap();
    (registry, (events, open_deadline, shutdown_deadline))
}

#[tokio::test]
async fn host_stops_exposure_before_instance_shutdown() {
    let (registry, (events, _, _)) = fake_registry(FakeOpen::Ready);
    let mut host = StateStoreHost::open(
        &registry,
        sqlite_host_config("unused.sqlite".into()),
        single_fe_view(),
        Instant::now() + Duration::from_secs(1),
    )
    .await
    .unwrap();
    assert!(host.state_store().is_some());

    host.shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();

    assert_eq!(
        *events.lock().unwrap(),
        vec!["factory_open", "host_exposure_dropped", "instance_shutdown"]
    );
    assert!(host.state_store().is_none());
    assert_eq!(host.lifecycle(), StateStoreHostLifecycle::Stopped);
}

#[tokio::test]
async fn shutdown_deadline_keeps_host_draining_and_allows_retry() {
    let (registry, _) = fake_registry(FakeOpen::Ready);
    let mut host = StateStoreHost::open(
        &registry,
        sqlite_host_config("unused.sqlite".into()),
        single_fe_view(),
        Instant::now() + Duration::from_secs(1),
    )
    .await
    .unwrap();

    let error = host.shutdown(Instant::now()).await.unwrap_err();

    assert_eq!(
        error.kind(),
        StateStoreHostErrorKind::ShutdownDeadlineExceeded
    );
    assert_eq!(host.lifecycle(), StateStoreHostLifecycle::Draining);
    assert!(host.state_store().is_none());
    host.shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(host.lifecycle(), StateStoreHostLifecycle::Stopped);
}

#[tokio::test]
async fn factory_open_failure_never_exposes_a_store() {
    let (registry, _) = fake_registry(FakeOpen::Fail);
    let error = match StateStoreHost::open(
        &registry,
        sqlite_host_config("unused.sqlite".into()),
        single_fe_view(),
        Instant::now() + Duration::from_secs(1),
    )
    .await
    {
        Ok(_) => panic!("factory open failure must fail host open"),
        Err(error) => error,
    };

    assert_eq!(error.kind(), StateStoreHostErrorKind::Open);
    assert_eq!(
        error.primary().unwrap().kind(),
        StateStoreErrorKind::ProviderUnavailable
    );
    assert!(error.cleanup().is_none());
}

#[tokio::test]
async fn post_open_validation_uses_same_deadline_and_retains_primary_and_cleanup() {
    let (registry, (_, open_deadline, shutdown_deadline)) =
        fake_registry(FakeOpen::MismatchedInstance {
            cleanup_fails: true,
        });
    let deadline = Instant::now() + Duration::from_secs(1);

    let error = match StateStoreHost::open(
        &registry,
        sqlite_host_config("unused.sqlite".into()),
        single_fe_view(),
        deadline,
    )
    .await
    {
        Ok(_) => panic!("post-open descriptor mismatch must fail host open"),
        Err(error) => error,
    };

    assert_eq!(error.kind(), StateStoreHostErrorKind::DescriptorMismatch);
    assert!(error.primary().is_some());
    assert_eq!(
        error.cleanup().unwrap().kind(),
        StateStoreErrorKind::Internal
    );
    assert_eq!(*open_deadline.lock().unwrap(), Some(deadline));
    assert_eq!(*shutdown_deadline.lock().unwrap(), Some(deadline));
}

#[tokio::test]
async fn sqlite_instance_waits_for_external_store_handles_before_stopping() {
    let temp = tempfile::tempdir().unwrap();
    let registry = builtin_state_store_provider_registry().unwrap();
    let mut host = StateStoreHost::open(
        &registry,
        sqlite_host_config(temp.path().join("state-store.sqlite")),
        single_fe_view(),
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .unwrap();
    let held_store = host.state_store().unwrap();

    let error = host.shutdown(Instant::now()).await.unwrap_err();

    assert_eq!(
        error.kind(),
        StateStoreHostErrorKind::ShutdownDeadlineExceeded
    );
    assert_eq!(host.lifecycle(), StateStoreHostLifecycle::Draining);
    drop(held_store);
    host.shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(host.lifecycle(), StateStoreHostLifecycle::Stopped);
}

#[tokio::test]
async fn sqlite_host_shutdown_releases_lock_for_immediate_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let config = sqlite_host_config(temp.path().join("state-store.sqlite"));
    let registry = builtin_state_store_provider_registry().unwrap();
    let mut first = StateStoreHost::open(
        &registry,
        config.clone(),
        single_fe_view(),
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .unwrap();
    first
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();

    let mut second = StateStoreHost::open(
        &registry,
        config,
        single_fe_view(),
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .unwrap();
    second
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
}
