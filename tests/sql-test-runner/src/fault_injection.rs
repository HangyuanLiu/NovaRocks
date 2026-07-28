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

use crate::cluster::ServerHandle;
use crate::types::QueryMeta;
use anyhow::{Result, bail};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, sleep};
use std::time::{Duration, Instant};

#[cfg(not(test))]
const POST_FRAGMENT_START_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(test)]
const POST_FRAGMENT_START_TIMEOUT: Duration = Duration::from_secs(1);
const POST_FRAGMENT_START_POLL_INTERVAL: Duration = Duration::from_millis(10);
const QUERY_RUNNING: u8 = 0;
const FAULT_CLAIMED: u8 = 1;
const QUERY_DONE: u8 = 2;

struct ActiveQueryFaultState {
    state: AtomicU8,
}

impl ActiveQueryFaultState {
    fn new() -> Self {
        Self {
            state: AtomicU8::new(QUERY_RUNNING),
        }
    }

    fn claim_fault(&self) -> bool {
        self.state
            .compare_exchange(
                QUERY_RUNNING,
                FAULT_CLAIMED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn mark_query_done(&self) -> bool {
        self.state
            .compare_exchange(
                QUERY_RUNNING,
                QUERY_DONE,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn query_is_done(&self) -> bool {
        self.state.load(Ordering::Acquire) == QUERY_DONE
    }
}

pub(crate) struct FragmentFailureStepGuard {
    target: Option<(Arc<Mutex<Box<dyn ServerHandle>>>, usize)>,
}

pub(crate) fn fragment_failure_step_guard(
    meta: &QueryMeta,
    server: Arc<Mutex<Box<dyn ServerHandle>>>,
) -> FragmentFailureStepGuard {
    FragmentFailureStepGuard {
        target: meta
            .fail_fragment_after_start_be_index
            .map(|index| (server, index)),
    }
}

impl Drop for FragmentFailureStepGuard {
    fn drop(&mut self) {
        let Some((server, index)) = self.target.take() else {
            return;
        };
        let mut server = match server.lock() {
            Ok(server) => server,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Err(error) = server.disarm_fragment_executor_failure(index) {
            eprintln!(
                "failed to disarm fragment executor failure for BE[{index}] after SQL step: {error:#}"
            );
        }
    }
}

pub(crate) fn has_fault(meta: &QueryMeta) -> bool {
    meta.kill_be_index.is_some()
        || meta.kill_be_after_fragment_start.is_some()
        || meta.fail_fragment_after_start_be_index.is_some()
        || meta.network_partition_be.is_some()
        || meta.heartbeat_delay_ms.is_some()
        || meta.restart_be_delay_ms.is_some()
}

pub(crate) fn apply_pre_query(meta: &QueryMeta, server: &mut dyn ServerHandle) -> Result<()> {
    let fragment_fault_count = [
        meta.kill_be_index.is_some(),
        meta.kill_be_after_fragment_start.is_some(),
        meta.fail_fragment_after_start_be_index.is_some(),
    ]
    .into_iter()
    .filter(|configured| *configured)
    .count();
    if fragment_fault_count > 1 {
        bail!(
            "a SQL step may configure at most one fragment fault directive: kill_be_index, kill_be_after_fragment_start, or fail_fragment_after_start_be_index"
        );
    }

    if let Some(index) = meta.network_partition_be {
        bail!(
            "network_partition_be is unsupported by the SQL test runner in Task 7.1 (index={index})"
        );
    }

    if meta.restart_be_delay_ms.is_some() && meta.kill_be_index.is_none() {
        bail!("restart_be_delay_ms requires kill_be_index so the runner knows which BE to restart");
    }

    if has_fault(meta) && !server.supports_fault_injection() {
        bail!(
            "fault injection directives require a mutable cross-process server handle; current server mode does not support fault injection"
        );
    }

    if let Some(index) = meta.fail_fragment_after_start_be_index {
        server.arm_fragment_executor_failure(index)?;
    }

    if let Some(index) = meta.kill_be_index {
        server.kill_be(index)?;
        if let Some(delay_ms) = meta.restart_be_delay_ms {
            sleep(Duration::from_millis(delay_ms));
            server.restart_be(index)?;
        }
    }

    if let Some(delay_ms) = meta.heartbeat_delay_ms {
        sleep(Duration::from_millis(delay_ms));
    }

    Ok(())
}

pub(crate) fn execute_with_post_fragment_start_fault<T, F>(
    meta: &QueryMeta,
    server: &Arc<Mutex<Box<dyn ServerHandle>>>,
    execute_query: F,
) -> Result<T>
where
    F: FnOnce() -> T,
{
    let Some(index) = meta.kill_be_after_fragment_start else {
        return Ok(execute_query());
    };
    let baseline = {
        let server = server
            .lock()
            .map_err(|_| anyhow::anyhow!("server handle mutex is poisoned"))?;
        if !server.supports_fault_injection() {
            bail!("kill_be_after_fragment_start requires a mutable cross-process server handle");
        }
        if index >= server.be_count() {
            bail!(
                "kill_be_after_fragment_start index {index} is out of bounds for {} BE(s)",
                server.be_count()
            );
        }
        server.scheduled_fragment_count(index)?
    };
    let fault_state = Arc::new(ActiveQueryFaultState::new());
    let worker_server = Arc::clone(server);
    let worker_fault_state = Arc::clone(&fault_state);
    let worker = thread::spawn(move || -> Result<()> {
        let deadline = Instant::now() + POST_FRAGMENT_START_TIMEOUT;
        loop {
            let current = worker_server
                .lock()
                .map_err(|_| anyhow::anyhow!("server handle mutex is poisoned"))?
                .scheduled_fragment_count(index)?;
            if current < baseline {
                bail!(
                    "BE[{index}] fragment-start marker count decreased from {baseline} to {current}"
                );
            }
            if current > baseline {
                if !worker_fault_state.claim_fault() {
                    bail!(
                        "query completed before BE[{index}] fault could claim the fresh ScheduledFragments observation"
                    );
                }
                worker_server
                    .lock()
                    .map_err(|_| anyhow::anyhow!("server handle mutex is poisoned"))?
                    .kill_be(index)?;
                return Ok(());
            }
            if worker_fault_state.query_is_done() {
                bail!(
                    "query completed before BE[{index}] ScheduledFragments advanced past {baseline}"
                );
            }
            if Instant::now() >= deadline {
                bail!(
                    "timed out waiting for BE[{index}] ScheduledFragments to advance past {baseline}"
                );
            }
            sleep(POST_FRAGMENT_START_POLL_INTERVAL);
        }
    });

    let query_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(execute_query));
    fault_state.mark_query_done();
    let worker_result = worker
        .join()
        .map_err(|_| anyhow::anyhow!("post-fragment-start fault worker panicked"));
    match query_result {
        Ok(query_result) => {
            worker_result??;
            Ok(query_result)
        }
        Err(panic) => {
            if let Ok(Err(error)) = worker_result {
                eprintln!(
                    "post-fragment-start fault worker stopped while query panicked: {error:#}"
                );
            }
            std::panic::resume_unwind(panic)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Condvar, Mutex};

    #[derive(Default)]
    struct RecordingServerHandle {
        events: Vec<String>,
    }

    impl ServerHandle for RecordingServerHandle {
        fn target_host(&self) -> Option<&str> {
            None
        }

        fn target_port(&self) -> Option<u16> {
            None
        }

        fn supports_fault_injection(&self) -> bool {
            true
        }

        fn kill_be(&mut self, index: usize) -> Result<()> {
            self.events.push(format!("kill:{index}"));
            Ok(())
        }

        fn restart_be(&mut self, index: usize) -> Result<()> {
            self.events.push(format!("restart:{index}"));
            Ok(())
        }

        fn arm_fragment_executor_failure(&mut self, index: usize) -> Result<()> {
            self.events.push(format!("arm-failure:{index}"));
            Ok(())
        }

        fn disarm_fragment_executor_failure(&mut self, index: usize) -> Result<()> {
            self.events.push(format!("disarm-failure:{index}"));
            Ok(())
        }
    }

    #[test]
    fn has_fault_detects_any_fault_directive() {
        assert!(!has_fault(&QueryMeta::default()));
        assert!(has_fault(&QueryMeta {
            heartbeat_delay_ms: Some(0),
            ..QueryMeta::default()
        }));
    }

    #[test]
    fn restart_delay_without_kill_is_rejected() {
        let meta = QueryMeta {
            restart_be_delay_ms: Some(0),
            ..QueryMeta::default()
        };
        let mut server = RecordingServerHandle::default();

        let err = apply_pre_query(&meta, &mut server).expect_err("restart without kill");

        assert!(
            err.to_string()
                .contains("restart_be_delay_ms requires kill_be_index"),
            "unexpected error: {err}"
        );
        assert!(server.events.is_empty());
    }

    #[test]
    fn unsupported_server_mode_rejects_fault_directives() {
        struct UnsupportedServerHandle;

        impl ServerHandle for UnsupportedServerHandle {
            fn target_host(&self) -> Option<&str> {
                None
            }

            fn target_port(&self) -> Option<u16> {
                None
            }
        }

        let meta = QueryMeta {
            heartbeat_delay_ms: Some(0),
            ..QueryMeta::default()
        };
        let mut server = UnsupportedServerHandle;

        let err = apply_pre_query(&meta, &mut server).expect_err("unsupported server mode");

        assert!(
            err.to_string()
                .contains("require a mutable cross-process server handle"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn network_partition_is_explicitly_unsupported() {
        let meta = QueryMeta {
            network_partition_be: Some(1),
            ..QueryMeta::default()
        };
        let mut server = RecordingServerHandle::default();

        let err = apply_pre_query(&meta, &mut server).expect_err("unsupported partition");

        assert!(
            err.to_string()
                .contains("network_partition_be is unsupported"),
            "unexpected error: {err}"
        );
        assert!(server.events.is_empty());
    }

    #[test]
    fn kill_and_restart_target_same_be() {
        let meta = QueryMeta {
            kill_be_index: Some(2),
            restart_be_delay_ms: Some(0),
            ..QueryMeta::default()
        };
        let mut server = RecordingServerHandle::default();

        apply_pre_query(&meta, &mut server).expect("apply fault");

        assert_eq!(server.events, vec!["kill:2", "restart:2"]);
    }

    #[test]
    fn fragment_executor_failure_is_armed_before_query_but_fires_after_start() {
        let meta = QueryMeta {
            fail_fragment_after_start_be_index: Some(2),
            ..QueryMeta::default()
        };
        let mut server = RecordingServerHandle::default();

        apply_pre_query(&meta, &mut server).expect("arm fragment executor failure");

        assert_eq!(server.events, vec!["arm-failure:2"]);
    }

    #[test]
    fn multiple_fragment_faults_are_rejected_before_mutating_the_cluster() {
        for meta in [
            QueryMeta {
                kill_be_index: Some(0),
                kill_be_after_fragment_start: Some(1),
                ..QueryMeta::default()
            },
            QueryMeta {
                kill_be_after_fragment_start: Some(1),
                fail_fragment_after_start_be_index: Some(2),
                ..QueryMeta::default()
            },
        ] {
            let mut server = RecordingServerHandle::default();
            let error = apply_pre_query(&meta, &mut server)
                .expect_err("a step must select exactly one fragment fault");
            assert!(
                error
                    .to_string()
                    .contains("at most one fragment fault directive"),
                "{error}"
            );
            assert!(server.events.is_empty());
        }
    }

    #[test]
    fn completed_query_wins_before_fault_claim() {
        let state = ActiveQueryFaultState::new();

        assert!(state.mark_query_done());
        assert!(
            !state.claim_fault(),
            "a fault worker must not claim permission to kill after query completion"
        );
    }

    struct SharedCleanupServerHandle {
        events: Arc<Mutex<Vec<String>>>,
    }

    impl ServerHandle for SharedCleanupServerHandle {
        fn target_host(&self) -> Option<&str> {
            None
        }

        fn target_port(&self) -> Option<u16> {
            None
        }

        fn disarm_fragment_executor_failure(&mut self, index: usize) -> Result<()> {
            self.events
                .lock()
                .expect("cleanup events")
                .push(format!("disarm-failure:{index}"));
            Ok(())
        }
    }

    #[test]
    fn fragment_failure_step_guard_disarms_during_unwind() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let server: Arc<Mutex<Box<dyn ServerHandle>>> =
            Arc::new(Mutex::new(Box::new(SharedCleanupServerHandle {
                events: Arc::clone(&events),
            })));
        let meta = QueryMeta {
            fail_fragment_after_start_be_index: Some(2),
            ..QueryMeta::default()
        };

        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = fragment_failure_step_guard(&meta, Arc::clone(&server));
            panic!("simulated step panic");
        }));

        assert!(panic.is_err());
        assert_eq!(
            *events.lock().expect("cleanup events"),
            vec!["disarm-failure:2"]
        );
    }

    struct ActiveQueryServerHandle {
        state: Arc<(Mutex<ActiveQueryState>, Condvar)>,
    }

    #[derive(Default)]
    struct ActiveQueryState {
        events: Vec<&'static str>,
        fragment_started: bool,
        killed: bool,
    }

    impl ServerHandle for ActiveQueryServerHandle {
        fn target_host(&self) -> Option<&str> {
            None
        }

        fn target_port(&self) -> Option<u16> {
            None
        }

        fn supports_fault_injection(&self) -> bool {
            true
        }

        fn be_count(&self) -> usize {
            2
        }

        fn scheduled_fragment_count(&self, index: usize) -> Result<u64> {
            assert_eq!(index, 1);
            let (lock, _) = self.state.as_ref();
            let mut state = lock.lock().expect("active query state");
            let event = if state.fragment_started {
                "scheduled:fresh"
            } else {
                "scheduled:baseline"
            };
            state.events.push(event);
            Ok(u64::from(state.fragment_started))
        }

        fn kill_be(&mut self, index: usize) -> Result<()> {
            assert_eq!(index, 1);
            let (lock, wake) = self.state.as_ref();
            let mut state = lock.lock().expect("active query state");
            state.events.push("kill");
            state.killed = true;
            wake.notify_all();
            Ok(())
        }
    }

    #[test]
    fn active_query_kill_waits_for_fresh_scheduled_fragment_count() {
        let state = Arc::new((Mutex::new(ActiveQueryState::default()), Condvar::new()));
        let server_handle: Arc<Mutex<Box<dyn ServerHandle>>> =
            Arc::new(Mutex::new(Box::new(ActiveQueryServerHandle {
                state: Arc::clone(&state),
            })));
        let meta = QueryMeta {
            kill_be_after_fragment_start: Some(1),
            ..QueryMeta::default()
        };

        let result = execute_with_post_fragment_start_fault(&meta, &server_handle, || {
            let (lock, wake) = state.as_ref();
            let mut query = lock.lock().expect("active query state");
            query.events.push("query:start");
            query.fragment_started = true;
            wake.notify_all();
            query = wake
                .wait_while(query, |state| !state.killed)
                .expect("wait for runner kill");
            query.events.push("query:end");
            42
        })
        .expect("active-query fault execution");

        assert_eq!(result, 42);
        assert_eq!(
            state.0.lock().expect("active query state").events,
            vec![
                "scheduled:baseline",
                "query:start",
                "scheduled:fresh",
                "kill",
                "query:end",
            ]
        );
    }

    #[test]
    fn query_panic_joins_fault_worker_without_a_late_kill() {
        let state = Arc::new((Mutex::new(ActiveQueryState::default()), Condvar::new()));
        let server_handle: Arc<Mutex<Box<dyn ServerHandle>>> =
            Arc::new(Mutex::new(Box::new(ActiveQueryServerHandle {
                state: Arc::clone(&state),
            })));
        let meta = QueryMeta {
            kill_be_after_fragment_start: Some(1),
            ..QueryMeta::default()
        };

        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = execute_with_post_fragment_start_fault(&meta, &server_handle, || -> () {
                panic!("simulated query panic");
            });
        }));

        assert!(panic.is_err());
        std::thread::sleep(POST_FRAGMENT_START_POLL_INTERVAL * 2);
        let state = state.0.lock().expect("active query state");
        assert!(
            !state.killed,
            "the joined fault worker must not kill a BE after query unwind"
        );
        assert!(
            !state.events.contains(&"kill"),
            "the fault worker must be quiescent before panic resumes"
        );
    }
}
