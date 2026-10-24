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

//! Current-process MV target readiness and publication ownership.

use std::{
    collections::BTreeMap,
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

/// A product worker callback.  The callback is one process-global event loop,
/// never a per-MV worker.  Native/provider hosts supply only the adapter work
/// performed for an event.
pub type MvRefreshBackgroundTask =
    Box<dyn FnMut(&MvBackgroundStop, &mpsc::SyncSender<()>) + Send + 'static>;
pub type MvMaintenanceBackgroundTask = Box<dyn FnMut(&MvBackgroundStop) + Send + 'static>;

/// Host adapters required to run the product's two bounded event loops.
pub struct MvBackgroundTasks {
    refresh: MvRefreshBackgroundTask,
    maintenance: MvMaintenanceBackgroundTask,
}

impl MvBackgroundTasks {
    pub fn new(refresh: MvRefreshBackgroundTask, maintenance: MvMaintenanceBackgroundTask) -> Self {
        Self {
            refresh,
            maintenance,
        }
    }
}

/// Read-only stop observation passed into a product event callback.
#[derive(Clone)]
pub struct MvBackgroundStop {
    requested: Arc<AtomicBool>,
}

impl MvBackgroundStop {
    pub fn is_requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }
}

type OriginalMaintenanceTask = Arc<Mutex<Option<MvMaintenanceBackgroundTask>>>;
struct MaintenanceTaskLoan {
    source: OriginalMaintenanceTask,
    task: Option<MvMaintenanceBackgroundTask>,
}
impl MaintenanceTaskLoan {
    fn take(source: &OriginalMaintenanceTask) -> Self {
        let task = source.lock().unwrap_or_else(|p| p.into_inner()).take();
        assert!(task.is_some(), "one original maintenance event callback");
        Self {
            source: Arc::clone(source),
            task,
        }
    }
}
impl Drop for MaintenanceTaskLoan {
    fn drop(&mut self) {
        let mut target = self.source.lock().unwrap_or_else(|p| p.into_inner());
        assert!(target.is_none(), "same callback loan returns once");
        *target = self.task.take();
    }
}

/// Product-owned lifecycle and event runtime for the two process-local MV
/// workers.  It owns worker creation, timer/wakeup waits, stop signals and
/// joins. Frontend supplies only repository/provider/native adapters.
pub struct MvBackgroundRuntime {
    refresh_stop_tx: mpsc::Sender<()>,
    refresh_worker: Option<JoinHandle<()>>,
    maintenance_stop_tx: mpsc::Sender<()>,
    #[allow(
        dead_code,
        reason = "The sender keeps the bounded maintenance wake channel alive until the product runtime joins its worker."
    )]
    maintenance_wakeup_tx: mpsc::SyncSender<()>,
    maintenance_worker: Option<JoinHandle<()>>,
    stop: MvBackgroundStop,
    // Host keeps the original callback/source beyond a worker unwind.
    maintenance_task: Option<OriginalMaintenanceTask>,
    refresh_failure: Mutex<Option<Box<dyn std::any::Any + Send>>>,
    maintenance_failure: Mutex<Option<Box<dyn std::any::Any + Send>>>,
}

impl MvBackgroundRuntime {
    pub fn new(
        refresh_stop_tx: mpsc::Sender<()>,
        refresh_worker: JoinHandle<()>,
        maintenance_stop_tx: mpsc::Sender<()>,
        maintenance_wakeup_tx: mpsc::SyncSender<()>,
        maintenance_worker: JoinHandle<()>,
    ) -> Self {
        Self {
            refresh_stop_tx,
            refresh_worker: Some(refresh_worker),
            maintenance_stop_tx,
            maintenance_wakeup_tx,
            maintenance_worker: Some(maintenance_worker),
            stop: MvBackgroundStop {
                requested: Arc::new(AtomicBool::new(false)),
            },
            maintenance_task: None,
            refresh_failure: Mutex::new(None),
            maintenance_failure: Mutex::new(None),
        }
    }

    /// Start both fixed process-wide event loops.  Timer waits block on the
    /// control channels; there is no sleep/poll loop and no worker is created
    /// for an individual materialized view.
    pub fn start(
        refresh_interval: Duration,
        maintenance_interval: Duration,
        tasks: MvBackgroundTasks,
    ) -> Result<Self, String> {
        let (refresh_stop_tx, refresh_stop_rx) = mpsc::channel();
        let (maintenance_stop_tx, maintenance_stop_rx) = mpsc::channel();
        let (maintenance_wakeup_tx, maintenance_wakeup_rx) = mpsc::sync_channel(1);
        let stop = MvBackgroundStop {
            requested: Arc::new(AtomicBool::new(false)),
        };
        let refresh_stop = stop.clone();
        let maintenance_stop = stop.clone();
        let refresh_wakeup_tx = maintenance_wakeup_tx.clone();
        let mut refresh = tasks.refresh;
        let refresh_worker = thread::Builder::new()
            .name("novarocks-mv-refresh".to_string())
            .spawn(move || {
                while !refresh_stop.is_requested() {
                    refresh(&refresh_stop, &refresh_wakeup_tx);
                    match refresh_stop_rx
                        .recv_timeout(refresh_interval.max(Duration::from_millis(1)))
                    {
                        Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => return,
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                    }
                }
            })
            .map_err(|error| format!("start MV refresh product worker: {error}"))?;
        let maintenance_task = Arc::new(Mutex::new(Some(tasks.maintenance)));
        let original_task = Arc::clone(&maintenance_task);
        let maintenance_worker = match thread::Builder::new()
            .name("novarocks-mv-maintenance".to_string())
            .spawn(move || {
                while !maintenance_stop.is_requested() {
                    // No lock is held across callback, physical poll or Drop.
                    // Unwind returns the SAME callback and its fixed source.
                    let mut original = MaintenanceTaskLoan::take(&original_task);
                    original.task.as_mut().unwrap()(&maintenance_stop);
                    drop(original);
                    match maintenance_wakeup_rx
                        .recv_timeout(maintenance_interval.max(Duration::from_millis(1)))
                    {
                        Ok(()) | Err(mpsc::RecvTimeoutError::Timeout) => {}
                        Err(mpsc::RecvTimeoutError::Disconnected) => return,
                    }
                    if maintenance_stop_rx.try_recv().is_ok() {
                        return;
                    }
                }
            }) {
            Ok(worker) => worker,
            Err(error) => {
                let _ = refresh_stop_tx.send(());
                let _ = refresh_worker.join();
                return Err(format!("start MV maintenance product worker: {error}"));
            }
        };
        Ok(Self {
            refresh_stop_tx,
            refresh_worker: Some(refresh_worker),
            maintenance_stop_tx,
            maintenance_wakeup_tx,
            maintenance_worker: Some(maintenance_worker),
            stop,
            maintenance_task: Some(maintenance_task),
            refresh_failure: Mutex::new(None),
            maintenance_failure: Mutex::new(None),
        })
    }

    pub fn maintenance_wakeup(&self) -> mpsc::SyncSender<()> {
        self.maintenance_wakeup_tx.clone()
    }

    pub fn request_stop(&self) {
        self.stop.requested.store(true, Ordering::Release);
        let _ = self.refresh_stop_tx.send(());
        let _ = self.maintenance_stop_tx.send(());
        let _ = self.maintenance_wakeup_tx.try_send(());
    }

    pub async fn stop_and_join_until(&mut self, deadline: Instant) -> Result<(), String> {
        self.request_stop();
        let refresh = wait_and_join(
            &mut self.refresh_worker,
            &self.refresh_failure,
            deadline,
            "MV refresh worker did not stop before the shared shutdown deadline",
            "MV refresh worker panicked during shutdown",
        )
        .await;
        let maintenance = wait_and_join(
            &mut self.maintenance_worker,
            &self.maintenance_failure,
            deadline,
            "MV maintenance worker did not stop before the shared shutdown deadline",
            "MV maintenance worker panicked during shutdown",
        )
        .await;
        // A sticky first worker failure never prevents observation of the
        // second original JH. Both use the SAME absolute deadline. The actual
        // opaque causes remain in their original two fixed fields.
        match (refresh, maintenance) {
            (Err(primary), _) => return Err(primary),
            (Ok(()), Err(secondary)) => return Err(secondary),
            (Ok(()), Ok(())) => {}
        }
        // Both joins succeeding is required to release the callback. A joined
        // failed worker can still own a poisoned action, so Err retains it.
        let retired = self
            .maintenance_task
            .as_ref()
            .and_then(|source| source.lock().unwrap_or_else(|p| p.into_inner()).take());
        drop(retired);
        Ok(())
    }

    #[cfg(test)]
    fn has_refresh_worker(&self) -> bool {
        self.refresh_worker.is_some()
    }

    #[cfg(test)]
    fn has_maintenance_worker(&self) -> bool {
        self.maintenance_worker.is_some()
    }
}

async fn wait_and_join(
    worker: &mut Option<JoinHandle<()>>,
    original_failure: &Mutex<Option<Box<dyn std::any::Any + Send>>>,
    deadline: Instant,
    deadline_error: &'static str,
    panic_error: &'static str,
) -> Result<(), String> {
    let Some(handle) = worker.as_ref() else {
        return if original_failure
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_some()
        {
            Err(panic_error.to_string())
        } else {
            Ok(())
        };
    };
    while !handle.is_finished() {
        if Instant::now() >= deadline {
            return Err(deadline_error.to_string());
        }
        tokio::time::sleep(
            Duration::from_millis(10).min(deadline.saturating_duration_since(Instant::now())),
        )
        .await;
    }
    match worker
        .take()
        .expect("finished MV worker is retained")
        .join()
    {
        Ok(()) => Ok(()),
        Err(primary) => {
            let mut original = original_failure.lock().unwrap_or_else(|p| p.into_inner());
            assert!(original.is_none(), "same original worker is joined once");
            *original = Some(primary);
            drop(original);
            Err(panic_error.to_string())
        }
    }
}

#[derive(Default)]
pub struct MvBackgroundRuntimeOwner {
    lifecycle: Mutex<BackgroundLifecycle>,
}

#[derive(Default)]
enum BackgroundLifecycle {
    #[default]
    Idle,
    Starting,
    // The same runtime is temporarily loaned outside the mutex for actual
    // joins. Cancellation restores it; nobody may install a second runtime.
    Stopping,
    Running(MvBackgroundRuntime),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvBackgroundRuntimeLifecycleError {
    AlreadyStarting,
    AlreadyRunning,
    StartupInterrupted,
    OwnerChangedDuringShutdown,
}

impl fmt::Display for MvBackgroundRuntimeLifecycleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyStarting => "MV background runtime is already starting",
            Self::AlreadyRunning => "MV background runtime was bound more than once",
            Self::StartupInterrupted => "MV background runtime is still starting during shutdown",
            Self::OwnerChangedDuringShutdown => {
                "MV background runtime owner changed during shutdown"
            }
        })
    }
}

impl std::error::Error for MvBackgroundRuntimeLifecycleError {}

impl MvBackgroundRuntimeOwner {
    pub fn begin_start(
        &self,
    ) -> Result<MvBackgroundRuntimeStart<'_>, MvBackgroundRuntimeLifecycleError> {
        let mut lifecycle = lock_lifecycle(&self.lifecycle)?;
        match &*lifecycle {
            BackgroundLifecycle::Idle => *lifecycle = BackgroundLifecycle::Starting,
            BackgroundLifecycle::Starting => {
                return Err(MvBackgroundRuntimeLifecycleError::AlreadyStarting);
            }
            BackgroundLifecycle::Running(_) | BackgroundLifecycle::Stopping => {
                return Err(MvBackgroundRuntimeLifecycleError::AlreadyRunning);
            }
        }
        Ok(MvBackgroundRuntimeStart {
            owner: self,
            completed: false,
        })
    }

    pub async fn shutdown_until(&self, deadline: Instant) -> Result<(), String> {
        let runtime = {
            let mut lifecycle = self
                .lifecycle
                .lock()
                .map_err(|error| format!("lock MV background runtime lifecycle: {error}"))?;
            match std::mem::replace(&mut *lifecycle, BackgroundLifecycle::Stopping) {
                BackgroundLifecycle::Idle => {
                    *lifecycle = BackgroundLifecycle::Idle;
                    return Ok(());
                }
                BackgroundLifecycle::Starting => {
                    *lifecycle = BackgroundLifecycle::Starting;
                    return Err(MvBackgroundRuntimeLifecycleError::StartupInterrupted.to_string());
                }
                BackgroundLifecycle::Stopping => {
                    return Err(
                        MvBackgroundRuntimeLifecycleError::OwnerChangedDuringShutdown.to_string(),
                    );
                }
                BackgroundLifecycle::Running(runtime) => runtime,
            }
        };
        // Constructed before the first join await; its Drop is a pure move
        // back to this owner, never a detached-worker cleanup approximation.
        let mut original = BackgroundRuntimeLoan {
            owner: self,
            runtime: Some(runtime),
        };
        let result = original
            .runtime
            .as_mut()
            .unwrap()
            .stop_and_join_until(deadline)
            .await;
        if result.is_ok() {
            original.finish();
        }
        result
    }

    pub fn request_stop_for_process_exit(&self) {
        if let Ok(lifecycle) = self.lifecycle.lock()
            && let BackgroundLifecycle::Running(runtime) = &*lifecycle
        {
            runtime.request_stop();
        }
    }
}

struct BackgroundRuntimeLoan<'a> {
    owner: &'a MvBackgroundRuntimeOwner,
    runtime: Option<MvBackgroundRuntime>,
}
impl BackgroundRuntimeLoan<'_> {
    fn finish(&mut self) {
        {
            let mut lifecycle = self
                .owner
                .lifecycle
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            assert!(
                matches!(*lifecycle, BackgroundLifecycle::Stopping),
                "same original runtime shutdown"
            );
            *lifecycle = BackgroundLifecycle::Idle;
        }
        // Actual joins and callback/source destruction have completed. All
        // remaining owner fields are destroyed outside the lifecycle mutex.
        drop(self.runtime.take());
    }
}
impl Drop for BackgroundRuntimeLoan<'_> {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            let mut lifecycle = self
                .owner
                .lifecycle
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            assert!(
                matches!(*lifecycle, BackgroundLifecycle::Stopping),
                "same original runtime loan"
            );
            *lifecycle = BackgroundLifecycle::Running(runtime);
        }
    }
}

pub struct MvBackgroundRuntimeStart<'a> {
    owner: &'a MvBackgroundRuntimeOwner,
    completed: bool,
}

impl MvBackgroundRuntimeStart<'_> {
    pub fn install(
        mut self,
        runtime: MvBackgroundRuntime,
    ) -> Result<(), MvBackgroundRuntimeLifecycleError> {
        let mut lifecycle = lock_lifecycle(&self.owner.lifecycle)?;
        if !matches!(*lifecycle, BackgroundLifecycle::Starting) {
            return Err(MvBackgroundRuntimeLifecycleError::OwnerChangedDuringShutdown);
        }
        *lifecycle = BackgroundLifecycle::Running(runtime);
        self.completed = true;
        Ok(())
    }
}

impl Drop for MvBackgroundRuntimeStart<'_> {
    fn drop(&mut self) {
        if !self.completed
            && let Ok(mut lifecycle) = self.owner.lifecycle.lock()
            && matches!(*lifecycle, BackgroundLifecycle::Starting)
        {
            *lifecycle = BackgroundLifecycle::Idle;
        }
    }
}

fn lock_lifecycle(
    lifecycle: &Mutex<BackgroundLifecycle>,
) -> Result<std::sync::MutexGuard<'_, BackgroundLifecycle>, MvBackgroundRuntimeLifecycleError> {
    lifecycle
        .lock()
        .map_err(|_| MvBackgroundRuntimeLifecycleError::OwnerChangedDuringShutdown)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TargetReadiness {
    Unobserved,
    Ready,
    /// The projection is a sound query candidate, but this process may not
    /// manage the target until its readmission completes. Reading an MV is not
    /// a management operation, so this is deliberately distinct from
    /// `Unavailable`, where the projection itself is in doubt.
    ReadOnly(String),
    Unavailable(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeAttempt<P> {
    pub publication_id: P,
}

pub struct ProcessRuntime<T, P> {
    inner: Mutex<BTreeMap<T, RuntimeEntry<P>>>,
}

impl<T, P> Default for ProcessRuntime<T, P> {
    fn default() -> Self {
        Self {
            inner: Mutex::new(BTreeMap::new()),
        }
    }
}

/// Ordering is process-local and shared by every logical target spelling owner.
/// It is not a durable fence or evidence that a remote effect has completed.
#[derive(Default, Debug)]
pub(crate) struct ProjectionOrder {
    pub generation: u64,
    pub installed: Option<crate::repository::MvProjectionVersion>,
    /// The generation of the observation currently in flight, if any.
    ///
    /// A reservation clears `installed` before it reads the provider, because
    /// what the process knows about the target stops being current the moment
    /// a new observation starts. It then releases this cell for the duration
    /// of that read, so a reader arriving in between would otherwise conclude
    /// that no successful observation exists -- which is true only in the same
    /// sense that a letter in the post has not arrived. This says which it is,
    /// so a reader can wait for the observation it would otherwise misreport.
    pub pending: Option<u64>,
    /// Woken when a reservation settles, so waiters do not poll.
    pub settled: std::sync::Arc<tokio::sync::Notify>,
}

impl ProjectionOrder {
    /// Release an in-flight observation, if this generation still owns it.
    ///
    /// A superseded reservation must not clear the marker: the generation that
    /// replaced it owns the in-flight state, and its waiters are waiting for
    /// that one.
    pub fn settle(&mut self, generation: u64) {
        if self.pending == Some(generation) {
            self.pending = None;
            self.settled.notify_waiters();
        }
    }

    /// Discard an in-flight observation because a new ordered event replaced
    /// it. Its own completion will find itself superseded and settle nothing,
    /// so the marker is released here instead.
    pub fn supersede(&mut self) {
        if self.pending.take().is_some() {
            self.settled.notify_waiters();
        }
    }
}

impl ProjectionOrder {
    pub fn advance(&mut self) -> Result<u64, crate::repository::MvRepositoryError> {
        self.generation = self.generation.checked_add(1).ok_or_else(|| {
            crate::repository::MvRepositoryError::new(
                crate::repository::MvRepositoryErrorKind::Unavailable,
                "MV projection generation exhausted",
            )
        })?;
        Ok(self.generation)
    }
}

#[derive(Clone, Debug)]
struct RuntimeEntry<P> {
    projection_order: Arc<tokio::sync::Mutex<ProjectionOrder>>,
    readiness: TargetReadiness,
    active: Option<RuntimeAttempt<P>>,
}

impl<P> Default for RuntimeEntry<P> {
    fn default() -> Self {
        Self {
            projection_order: Arc::new(tokio::sync::Mutex::new(ProjectionOrder::default())),
            readiness: TargetReadiness::Unobserved,
            active: None,
        }
    }
}

impl<T, P> ProcessRuntime<T, P>
where
    T: Clone + Ord,
    P: Copy + Eq,
{
    pub(crate) fn projection_order(&self, target: T) -> Arc<tokio::sync::Mutex<ProjectionOrder>> {
        Arc::clone(
            &self
                .inner
                .lock()
                .expect("MV application runtime lock poisoned")
                .entry(target)
                .or_default()
                .projection_order,
        )
    }

    pub(crate) fn projection_targets(&self) -> Vec<T> {
        self.inner
            .lock()
            .expect("MV application runtime lock poisoned")
            .keys()
            .cloned()
            .collect()
    }

    pub fn readiness(&self, target: &T) -> TargetReadiness {
        self.inner
            .lock()
            .expect("MV application runtime lock poisoned")
            .get(target)
            .map(|entry| entry.readiness.clone())
            .unwrap_or(TargetReadiness::Unobserved)
    }

    /// Inspect a readiness value while its runtime owner still lends it.
    /// A bounded listing can refuse a large reason before cloning it.
    pub(crate) fn with_readiness<R>(
        &self,
        target: &T,
        inspect: impl FnOnce(&TargetReadiness) -> R,
    ) -> R {
        let entries = self
            .inner
            .lock()
            .expect("MV application runtime lock poisoned");
        inspect(
            entries
                .get(target)
                .map(|entry| &entry.readiness)
                .unwrap_or(&TargetReadiness::Unobserved),
        )
    }

    pub(crate) fn set_unavailable(&self, target: T, reason: String) {
        self.inner
            .lock()
            .expect("MV application runtime lock poisoned")
            .entry(target)
            .or_default()
            .readiness = TargetReadiness::Unavailable(reason);
    }

    pub(crate) fn set_read_only(&self, target: T, reason: String) {
        self.inner
            .lock()
            .expect("MV application runtime lock poisoned")
            .entry(target)
            .or_default()
            .readiness = TargetReadiness::ReadOnly(reason);
    }

    pub(crate) fn set_ready(&self, target: T) {
        self.inner
            .lock()
            .expect("MV application runtime lock poisoned")
            .entry(target)
            .or_default()
            .readiness = TargetReadiness::Ready;
    }

    pub fn begin(&self, target: T, publication_id: P) -> bool {
        let mut entries = self
            .inner
            .lock()
            .expect("MV application runtime lock poisoned");
        let entry = entries.entry(target).or_default();
        if entry.active.is_some() {
            return false;
        }
        entry.active = Some(RuntimeAttempt { publication_id });
        true
    }

    pub fn finish(&self, target: &T, publication_id: P) {
        let mut entries = self
            .inner
            .lock()
            .expect("MV application runtime lock poisoned");
        if let Some(entry) = entries.get_mut(target)
            && entry
                .active
                .as_ref()
                .is_some_and(|active| active.publication_id == publication_id)
        {
            entry.active = None;
        }
    }

    pub fn has_active_publications(&self) -> bool {
        self.inner
            .lock()
            .expect("MV application runtime lock poisoned")
            .values()
            .any(|entry| entry.active.is_some())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::mpsc,
        thread,
        time::{Duration, Instant},
    };

    use super::{MvBackgroundRuntime, MvBackgroundRuntimeOwner, ProcessRuntime, TargetReadiness};

    #[test]
    fn runtime_tracks_one_publication_and_readiness_per_target() {
        let runtime = ProcessRuntime::<String, u64>::default();
        let target = "ice.analytics.orders_mv".to_string();
        assert!(runtime.begin(target.clone(), 7));
        assert!(!runtime.begin(target.clone(), 8));
        runtime.set_unavailable(target.clone(), "accelerator projection failed".into());
        assert!(matches!(
            runtime.readiness(&target),
            TargetReadiness::Unavailable(_)
        ));
        runtime.finish(&target, 7);
        assert!(!runtime.has_active_publications());
        runtime.set_ready(target.clone());
        assert_eq!(runtime.readiness(&target), TargetReadiness::Ready);
    }

    #[tokio::test]
    async fn product_runtime_retains_blocked_worker_join_for_retry() {
        let (refresh_stop_tx, _refresh_stop_rx) = mpsc::channel();
        let (maintenance_stop_tx, _maintenance_stop_rx) = mpsc::channel();
        let (maintenance_wakeup_tx, _maintenance_wakeup_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::channel();
        let refresh_worker = thread::spawn(move || {
            let _ = release_rx.recv();
        });
        let maintenance_worker = thread::spawn(|| {});
        let mut runtime = MvBackgroundRuntime::new(
            refresh_stop_tx,
            refresh_worker,
            maintenance_stop_tx,
            maintenance_wakeup_tx,
            maintenance_worker,
        );

        let error = runtime
            .stop_and_join_until(Instant::now() + Duration::from_millis(10))
            .await
            .expect_err("blocked MV worker must respect the shared deadline");
        assert!(error.contains("shared shutdown deadline"));
        assert!(runtime.has_refresh_worker());

        release_tx.send(()).unwrap();
        runtime
            .stop_and_join_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("the retained MV worker join remains retryable");
        assert!(!runtime.has_refresh_worker());
        assert!(!runtime.has_maintenance_worker());
    }

    #[tokio::test]
    async fn owner_exclusively_installs_and_joins_product_runtime() {
        let owner = MvBackgroundRuntimeOwner::default();
        let start = owner.begin_start().expect("first start reserves the owner");
        assert!(owner.begin_start().is_err());
        let (refresh_stop_tx, _refresh_stop_rx) = mpsc::channel();
        let (maintenance_stop_tx, _maintenance_stop_rx) = mpsc::channel();
        let (maintenance_wakeup_tx, _maintenance_wakeup_rx) = mpsc::sync_channel(1);
        let runtime = MvBackgroundRuntime::new(
            refresh_stop_tx,
            thread::spawn(|| {}),
            maintenance_stop_tx,
            maintenance_wakeup_tx,
            thread::spawn(|| {}),
        );
        start.install(runtime).expect("reservation installs once");
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("product owner joins both workers");
    }

    #[tokio::test]
    async fn product_runtime_wakes_maintenance_without_a_poll_sleep() {
        let (refresh_seen_tx, refresh_seen_rx) = mpsc::channel();
        let (maintenance_seen_tx, maintenance_seen_rx) = mpsc::channel();
        let runtime = MvBackgroundRuntime::start(
            Duration::from_secs(60),
            Duration::from_secs(60),
            super::MvBackgroundTasks::new(
                Box::new(move |_, _| {
                    let _ = refresh_seen_tx.send(());
                }),
                Box::new(move |_| {
                    let _ = maintenance_seen_tx.send(());
                }),
            ),
        )
        .expect("product workers start");
        refresh_seen_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("refresh product event runs immediately");
        maintenance_seen_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("maintenance product event runs immediately");
        runtime
            .maintenance_wakeup()
            .send(())
            .expect("product wake channel remains owned");
        maintenance_seen_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("maintenance wake runs without waiting for its timer");
        let mut runtime = runtime;
        runtime
            .stop_and_join_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("woken workers observe the product stop signal");
    }
    #[tokio::test]
    async fn failed_original_worker_keeps_callback_and_raw_cause_after_actual_join() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        struct DropProbe(Arc<AtomicUsize>);
        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let source_drops = Arc::new(AtomicUsize::new(0));
        let cause_drops = Arc::new(AtomicUsize::new(0));
        let source = Arc::new(DropProbe(Arc::clone(&source_drops)));
        let source_view = Arc::downgrade(&source);
        let mut cause = Some(DropProbe(Arc::clone(&cause_drops)));
        let (entered, receive) = mpsc::channel();
        let mut runtime = MvBackgroundRuntime::start(
            Duration::from_secs(60),
            Duration::from_secs(60),
            super::MvBackgroundTasks::new(
                Box::new(|_, _| {}),
                Box::new(move |_| {
                    let _same_original = &source;
                    entered.send(()).unwrap();
                    std::panic::panic_any(cause.take().expect("same worker only runs once"));
                }),
            ),
        )
        .unwrap();
        let entered = receive.recv_timeout(Duration::from_secs(1)).is_ok();
        let exit = runtime
            .stop_and_join_until(Instant::now() + Duration::from_secs(1))
            .await;
        let first_cause = runtime
            .maintenance_failure
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|p| p.is::<DropProbe>());
        let repeat = runtime
            .stop_and_join_until(Instant::now() + Duration::from_secs(1))
            .await;
        let success = entered
            && exit.is_err()
            && repeat.is_err()
            && first_cause
            && !runtime.has_maintenance_worker()
            && source_view.upgrade().is_some()
            && source_drops.load(Ordering::SeqCst) == 0
            && cause_drops.load(Ordering::SeqCst) == 0;
        if !success {
            std::panic::panic_any(runtime)
        }
        drop(runtime); // Explicit original process-owner disposition after real join.
        assert_eq!(source_drops.load(Ordering::SeqCst), 1);
        assert_eq!(cause_drops.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn refresh_panic_does_not_skip_the_finished_maintenance_join() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        struct Cause(Arc<AtomicUsize>);
        impl Drop for Cause {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let cause_drops = Arc::new(AtomicUsize::new(0));
        let callback_drops = Arc::new(AtomicUsize::new(0));
        let source = Arc::new(Cause(Arc::clone(&callback_drops)));
        let source_view = Arc::downgrade(&source);
        let mut primary = Some(Cause(Arc::clone(&cause_drops)));
        let (finished, receive) = mpsc::channel();
        let mut runtime = MvBackgroundRuntime::start(
            Duration::from_secs(60),
            Duration::from_secs(60),
            super::MvBackgroundTasks::new(
                Box::new(move |_, _| std::panic::panic_any(primary.take().unwrap())),
                Box::new(move |_| {
                    let _original_callback = &source;
                    let _ = finished.send(());
                }),
            ),
        )
        .unwrap();
        let entered = receive.recv_timeout(Duration::from_secs(1));
        let exit = runtime
            .stop_and_join_until(Instant::now() + Duration::from_secs(1))
            .await;
        let both_joined = !runtime.has_refresh_worker() && !runtime.has_maintenance_worker();
        let cause_retained = runtime
            .refresh_failure
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|cause| cause.is::<Cause>());
        let repeat = runtime
            .stop_and_join_until(Instant::now() + Duration::from_secs(1))
            .await;
        if !(entered.is_ok()
            && exit.is_err()
            && repeat.is_err()
            && both_joined
            && cause_retained
            && source_view.upgrade().is_some()
            && cause_drops.load(Ordering::SeqCst) == 0
            && callback_drops.load(Ordering::SeqCst) == 0)
        {
            // Both original causes and callback remain in the whole packet.
            std::panic::panic_any(runtime)
        }
        drop(runtime); // Explicit original owner disposition, not a Source exit receipt.
        assert_eq!(cause_drops.load(Ordering::SeqCst), 1);
        assert_eq!(callback_drops.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn cancelled_shutdown_restores_the_same_two_joins_and_callback() {
        use std::future::Future;
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        struct Source(Arc<AtomicUsize>);
        impl Drop for Source {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let drops = Arc::new(AtomicUsize::new(0));
        let source = Arc::new(Source(Arc::clone(&drops)));
        let source_view = Arc::downgrade(&source);
        let (refresh_entered, refresh_seen) = mpsc::channel();
        let (maintenance_entered, maintenance_seen) = mpsc::channel();
        let (release_refresh, refresh_release) = mpsc::channel();
        let (release_maintenance, maintenance_release) = mpsc::channel();
        let runtime = MvBackgroundRuntime::start(
            Duration::from_secs(60),
            Duration::from_secs(60),
            super::MvBackgroundTasks::new(
                Box::new(move |_, _| {
                    let _ = refresh_entered.send(());
                    let _ = refresh_release.recv();
                }),
                Box::new(move |_| {
                    let _original = &source;
                    let _ = maintenance_entered.send(());
                    let _ = maintenance_release.recv();
                }),
            ),
        )
        .unwrap();
        let identities = (
            runtime.refresh_worker.as_ref().unwrap().thread().id(),
            runtime.maintenance_worker.as_ref().unwrap().thread().id(),
        );
        let owner = MvBackgroundRuntimeOwner::default();
        owner.begin_start().unwrap().install(runtime).unwrap();
        let entered = refresh_seen.recv_timeout(Duration::from_secs(1)).is_ok()
            && maintenance_seen
                .recv_timeout(Duration::from_secs(1))
                .is_ok();
        let mut wait = Box::pin(owner.shutdown_until(Instant::now() + Duration::from_secs(1)));
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        let pending = wait.as_mut().poll(&mut cx).is_pending();
        drop(wait); // Borrowed cancellation is not an actual worker exit.
        let restored = {
            let lifecycle = owner.lifecycle.lock().unwrap();
            match &*lifecycle {
                super::BackgroundLifecycle::Running(runtime) => {
                    runtime.refresh_worker.as_ref().map(|h| h.thread().id()) == Some(identities.0)
                        && runtime.maintenance_worker.as_ref().map(|h| h.thread().id())
                            == Some(identities.1)
                        && runtime.maintenance_task.is_some()
                }
                _ => false,
            }
        };
        let retained = source_view.upgrade().is_some() && drops.load(Ordering::SeqCst) == 0;
        let refresh_released = release_refresh.send(()).is_ok();
        let maintenance_released = release_maintenance.send(()).is_ok();
        let joined = owner
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await;
        let closed = matches!(
            *owner.lifecycle.lock().unwrap(),
            super::BackgroundLifecycle::Idle
        );
        if !(entered
            && pending
            && restored
            && retained
            && refresh_released
            && maintenance_released
            && joined.is_ok()
            && closed
            && drops.load(Ordering::SeqCst) == 1)
        {
            std::panic::panic_any(owner)
        }
    }
}
