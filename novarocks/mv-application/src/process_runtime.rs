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
    sync::{Mutex, mpsc},
    thread::JoinHandle,
    time::{Duration, Instant},
};

/// Product-owned lifecycle for the two process-local MV background workers.
///
/// Frontend constructs the concrete workers because they consume Native,
/// provider, and query adapters.  Once constructed, their stop and join state
/// belongs here so a host cannot independently advance or retire MV work.
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
        }
    }

    pub fn request_stop(&self) {
        let _ = self.refresh_stop_tx.send(());
        let _ = self.maintenance_stop_tx.send(());
    }

    pub async fn stop_and_join_until(&mut self, deadline: Instant) -> Result<(), String> {
        self.request_stop();
        wait_and_join(
            &mut self.refresh_worker,
            deadline,
            "MV refresh worker did not stop before the shared shutdown deadline",
            "MV refresh worker panicked during shutdown",
        )
        .await?;
        wait_and_join(
            &mut self.maintenance_worker,
            deadline,
            "MV maintenance worker did not stop before the shared shutdown deadline",
            "MV maintenance worker panicked during shutdown",
        )
        .await
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
    deadline: Instant,
    deadline_error: &'static str,
    panic_error: &'static str,
) -> Result<(), String> {
    let Some(handle) = worker.as_ref() else {
        return Ok(());
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
    worker
        .take()
        .expect("finished MV worker is retained")
        .join()
        .map_err(|_| panic_error.to_string())
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
            BackgroundLifecycle::Running(_) => {
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
            match std::mem::replace(&mut *lifecycle, BackgroundLifecycle::Idle) {
                BackgroundLifecycle::Idle => return Ok(()),
                BackgroundLifecycle::Starting => {
                    *lifecycle = BackgroundLifecycle::Starting;
                    return Err(MvBackgroundRuntimeLifecycleError::StartupInterrupted.to_string());
                }
                BackgroundLifecycle::Running(runtime) => runtime,
            }
        };
        let mut runtime = runtime;
        let result = runtime.stop_and_join_until(deadline).await;
        if result.is_err() {
            let mut lifecycle = self
                .lifecycle
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if !matches!(*lifecycle, BackgroundLifecycle::Idle) {
                return Err(
                    MvBackgroundRuntimeLifecycleError::OwnerChangedDuringShutdown.to_string(),
                );
            }
            *lifecycle = BackgroundLifecycle::Running(runtime);
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
    Ready,
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

#[derive(Clone, Debug)]
struct RuntimeEntry<P> {
    readiness: TargetReadiness,
    active: Option<RuntimeAttempt<P>>,
}

impl<P> Default for RuntimeEntry<P> {
    fn default() -> Self {
        Self {
            readiness: TargetReadiness::Ready,
            active: None,
        }
    }
}

impl<T, P> ProcessRuntime<T, P>
where
    T: Clone + Ord,
    P: Copy + Eq,
{
    pub fn readiness(&self, target: &T) -> TargetReadiness {
        self.inner
            .lock()
            .expect("MV application runtime lock poisoned")
            .get(target)
            .map(|entry| entry.readiness.clone())
            .unwrap_or(TargetReadiness::Ready)
    }

    pub fn set_unavailable(&self, target: T, reason: String) {
        self.inner
            .lock()
            .expect("MV application runtime lock poisoned")
            .entry(target)
            .or_default()
            .readiness = TargetReadiness::Unavailable(reason);
    }

    pub fn set_ready(&self, target: T) {
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
}
