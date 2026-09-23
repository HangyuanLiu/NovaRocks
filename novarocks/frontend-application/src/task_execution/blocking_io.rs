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

//! Supervision for blocking Connector calls made by the frontend.
//!
//! Connector implementations may expose synchronous split enumeration and
//! credential vending. A submitted call keeps running on Tokio's blocking pool
//! when its query waiter is dropped. The worker publishes its actual outcome
//! only after the synchronous call returns. Query admission, rather than a
//! second Connector permit, controls whether the call may be submitted.

use std::fmt;
use std::sync::{Arc, Mutex};

use tokio::runtime::Handle;

/// Why a submitted blocking call produced no Connector outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ConnectorBlockingIoError {
    detail: String,
}

impl ConnectorBlockingIoError {}

impl fmt::Display for ConnectorBlockingIoError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl std::error::Error for ConnectorBlockingIoError {}

/// A submitted call whose result can be polled by an existing serial owner.
pub(crate) struct ConnectorBlockingIoJob<T> {
    outcome: Arc<Mutex<Option<Result<T, ConnectorBlockingIoError>>>>,
    ready: Arc<tokio::sync::Notify>,
}

impl<T> ConnectorBlockingIoJob<T> {
    pub(crate) fn try_take(&self) -> Option<Result<T, ConnectorBlockingIoError>> {
        self.outcome
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
    }

    /// Waits asynchronously for the submitted call to publish its outcome.
    ///
    /// The polling accessor remains useful to serial owners such as credential
    /// rotation. Split assignment uses this form to wake its serial round only
    /// after the blocking worker has really returned.
    pub(crate) async fn finish(self) -> Result<T, ConnectorBlockingIoError> {
        loop {
            let notified = self.ready.notified();
            if let Some(outcome) = self.try_take() {
                return outcome;
            }
            notified.await;
        }
    }
}

/// The one process owner that supervises frontend Connector blocking calls.
#[derive(Clone)]
pub(crate) struct ConnectorBlockingIoSupervisor {
    runtime: Handle,
}

impl ConnectorBlockingIoSupervisor {
    pub(crate) fn new(runtime: Handle) -> Self {
        Self { runtime }
    }

    /// The runtime this lane's work is admitted onto.
    ///
    /// Statement preparation runs on its own threads, not on this runtime, so
    /// it needs the handle to await anything that reaches the lane.
    pub(crate) const fn runtime(&self) -> &Handle {
        &self.runtime
    }

    /// Submit credential or lifecycle work and retain its actual completion.
    pub(crate) fn spawn_protected<T, F>(&self, call: F) -> ConnectorBlockingIoJob<T>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        self.spawn(call)
    }

    /// Submit ordinary split-source work.
    ///
    /// Only the synchronous Connector call belongs inside `call`; transport
    /// acknowledgement and retry waits must run after this job has finished so
    /// the blocking worker owns only the actual Connector call.
    pub(crate) fn spawn_ordinary<T, F>(&self, call: F) -> ConnectorBlockingIoJob<T>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        self.spawn(call)
    }

    fn spawn<T, F>(&self, call: F) -> ConnectorBlockingIoJob<T>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        let runtime = self.runtime.clone();
        let outcome = Arc::new(Mutex::new(None));
        let published = Arc::clone(&outcome);
        let ready = Arc::new(tokio::sync::Notify::new());
        let publish_ready = Arc::clone(&ready);
        self.runtime.spawn(async move {
            let completed =
                runtime
                    .spawn_blocking(call)
                    .await
                    .map_err(|error| ConnectorBlockingIoError {
                        detail: format!("connector blocking-I/O worker failed: {error}"),
                    });
            *published.lock().unwrap_or_else(|error| error.into_inner()) = Some(completed);
            // Each job has exactly one consuming waiter. A stored single
            // notification token also covers completion before `finish` registers, while
            // `notify_waiters` would lose that notification.
            publish_ready.notify_one();
        });
        ConnectorBlockingIoJob { outcome, ready }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    use super::*;

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(4)
            .enable_all()
            .build()
            .expect("runtime")
    }

    fn wait<T>(job: &ConnectorBlockingIoJob<T>) -> Result<T, ConnectorBlockingIoError> {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if let Some(outcome) = job.try_take() {
                return outcome;
            }
            assert!(Instant::now() < deadline, "blocking-I/O job did not finish");
            std::thread::yield_now();
        }
    }

    #[test]
    fn another_admitted_call_starts_while_the_first_is_held() {
        let runtime = runtime();
        let supervisor = ConnectorBlockingIoSupervisor::new(runtime.handle().clone());
        let (release, released) = mpsc::channel();
        let (started, first_started) = mpsc::channel();
        let first = supervisor.spawn_ordinary(move || {
            started.send(()).expect("publish first start");
            released.recv().expect("release first call");
        });
        first_started
            .recv_timeout(Duration::from_secs(2))
            .expect("first call did not start");

        let second = supervisor.spawn_ordinary(|| 7_u8);
        assert_eq!(wait(&second).expect("second outcome"), 7);
        assert!(
            first.try_take().is_none(),
            "held call must still be running"
        );
        release.send(()).expect("release first call");
        wait(&first).expect("first outcome");
    }

    #[test]
    fn dropping_waiter_does_not_end_the_blocking_call() {
        let runtime = runtime();
        let supervisor = ConnectorBlockingIoSupervisor::new(runtime.handle().clone());
        let (release, released) = mpsc::channel();
        let (started, first_started) = mpsc::channel();
        let (exited, observed_exit) = mpsc::channel();
        let job = supervisor.spawn_protected(move || {
            started.send(()).expect("publish start");
            released.recv().expect("release call");
            exited.send(()).expect("publish actual exit");
        });
        first_started
            .recv_timeout(Duration::from_secs(2))
            .expect("call did not start");
        drop(job);
        assert_eq!(observed_exit.try_recv(), Err(mpsc::TryRecvError::Empty));
        release.send(()).expect("release call");
        observed_exit
            .recv_timeout(Duration::from_secs(2))
            .expect("blocking call did not really exit");
    }

    #[test]
    fn async_finish_observes_completion_published_before_it_waits() {
        let runtime = runtime();
        let supervisor = ConnectorBlockingIoSupervisor::new(runtime.handle().clone());
        let job = supervisor.spawn_ordinary(|| 17_u8);
        let deadline = Instant::now() + Duration::from_secs(2);
        while job
            .outcome
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .is_none()
        {
            assert!(Instant::now() < deadline, "job did not publish completion");
            std::thread::yield_now();
        }

        assert_eq!(runtime.block_on(job.finish()).expect("finished job"), 17);
    }
}
