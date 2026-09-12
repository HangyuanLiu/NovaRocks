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

//! Worker-owned supervision for advancing local task lifecycle deadlines.

use std::sync::{Arc, mpsc};
use std::time::Duration;

use tokio::runtime::Handle;
use tokio::sync::watch;

/// The mutable task owner that alone can turn an elapsed deadline into a
/// lifecycle decision. The supervisor owns cadence, never those decisions.
pub trait WorkerDeadlineAuthority: Send + Sync {
    fn advance_deadlines(&self);
}

/// One process-local maintenance loop for a Worker deadline authority.
pub struct WorkerDeadlineSupervisor {
    stop: watch::Sender<bool>,
    failure_rx: mpsc::Receiver<String>,
    join: Option<tokio::task::JoinHandle<()>>,
}

impl WorkerDeadlineSupervisor {
    pub fn start(
        runtime: &Handle,
        authority: Arc<dyn WorkerDeadlineAuthority>,
        interval: Duration,
    ) -> Self {
        let (stop, mut stopped) = watch::channel(false);
        let (failure_tx, failure_rx) = mpsc::channel();
        let join = runtime.spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // The first Tokio tick is immediate. At construction there is no
            // deadline whose authority needs advancing yet.
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = stopped.changed() => return,
                    _ = ticker.tick() => {}
                }
                let authority = Arc::clone(&authority);
                if let Err(error) = tokio::task::spawn_blocking(move || {
                    authority.advance_deadlines();
                })
                .await
                {
                    let _ =
                        failure_tx.send(format!("worker deadline sweep stopped running: {error}"));
                    return;
                }
            }
        });
        Self {
            stop,
            failure_rx,
            join: Some(join),
        }
    }

    pub fn poll_failure(&mut self) -> Option<String> {
        self.failure_rx.try_recv().ok()
    }

    pub fn stop(&mut self) {
        let _ = self.stop.send(true);
        if let Some(join) = self.join.take() {
            join.abort();
        }
    }
}

impl Drop for WorkerDeadlineSupervisor {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::sync::Notify;

    use super::*;

    #[derive(Default)]
    struct CountingAuthority {
        advances: AtomicUsize,
        advanced: Notify,
    }

    impl WorkerDeadlineAuthority for CountingAuthority {
        fn advance_deadlines(&self) {
            self.advances.fetch_add(1, Ordering::AcqRel);
            self.advanced.notify_one();
        }
    }

    #[tokio::test]
    async fn supervisor_drives_the_injected_authority_and_stops_cleanly() {
        let authority = Arc::new(CountingAuthority::default());
        let mut supervisor = WorkerDeadlineSupervisor::start(
            &Handle::current(),
            Arc::clone(&authority) as Arc<dyn WorkerDeadlineAuthority>,
            Duration::from_millis(1),
        );

        tokio::time::timeout(Duration::from_secs(1), authority.advanced.notified())
            .await
            .expect("deadline authority was not driven");
        supervisor.stop();

        assert!(authority.advances.load(Ordering::Acquire) >= 1);
        assert_eq!(supervisor.poll_failure(), None);
    }
}
