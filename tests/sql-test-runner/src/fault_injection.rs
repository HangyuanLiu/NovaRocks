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
use anyhow::{Context, Result, bail};
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
        self.state.swap(QUERY_DONE, Ordering::AcqRel) != QUERY_DONE
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

pub(crate) struct QueryLifecycleFaultStepGuard {
    server: Option<Arc<Mutex<Box<dyn ServerHandle>>>>,
}

pub(crate) fn query_lifecycle_fault_step_guard(
    meta: &QueryMeta,
    server: Arc<Mutex<Box<dyn ServerHandle>>>,
) -> QueryLifecycleFaultStepGuard {
    let armed = meta.drop_next_init_ack_be_index.is_some()
        || meta.stop_query_control_heartbeat_be_index.is_some()
        || meta.kill_fe_after_control_ready_count.is_some()
        || meta.restart_be_after_init_ack_index.is_some()
        || meta.kill_query_after_control_ready_count.is_some()
        || meta.fail_stage_prepare_ordinal.is_some()
        || meta.drop_next_stage_ack_be_index.is_some()
        || meta.drop_next_start_ack_be_index.is_some()
        || meta.suppress_start_ack_be_index.is_some()
        || meta.drop_next_terminal_ack_be_index.is_some()
        || meta.kill_query_at_lifecycle_phase.is_some()
        || meta.kill_fe_at_lifecycle_phase.is_some()
        || meta
            .stop_query_control_heartbeat_after_stage_be_index
            .is_some()
        || meta.hold_start_until_early_ingress
        || meta.query_control_fragment_backend_limit.is_some();
    QueryLifecycleFaultStepGuard {
        server: armed.then_some(server),
    }
}

impl Drop for QueryLifecycleFaultStepGuard {
    fn drop(&mut self) {
        let Some(server) = self.server.take() else {
            return;
        };
        let mut server = match server.lock() {
            Ok(server) => server,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Err(error) = server.clear_query_lifecycle_faults() {
            eprintln!("failed to clear query lifecycle fault triggers after SQL step: {error:#}");
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
        || meta.drop_next_init_ack_be_index.is_some()
        || meta.stop_query_control_heartbeat_be_index.is_some()
        || meta.kill_fe_after_control_ready_count.is_some()
        || meta.restart_be_after_init_ack_index.is_some()
        || meta.kill_query_after_control_ready_count.is_some()
        || meta.fail_stage_prepare_ordinal.is_some()
        || meta.drop_next_stage_ack_be_index.is_some()
        || meta.drop_next_start_ack_be_index.is_some()
        || meta.suppress_start_ack_be_index.is_some()
        || meta.drop_next_terminal_ack_be_index.is_some()
        || meta.kill_query_at_lifecycle_phase.is_some()
        || meta.kill_fe_at_lifecycle_phase.is_some()
        || meta
            .stop_query_control_heartbeat_after_stage_be_index
            .is_some()
        || meta.hold_start_until_early_ingress
        || meta.query_control_fragment_backend_limit.is_some()
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

    let lifecycle_fault_count = [
        meta.drop_next_init_ack_be_index.is_some(),
        meta.stop_query_control_heartbeat_be_index.is_some(),
        meta.kill_fe_after_control_ready_count.is_some(),
        meta.restart_be_after_init_ack_index.is_some(),
        meta.kill_query_after_control_ready_count.is_some(),
        meta.fail_stage_prepare_ordinal.is_some(),
        meta.drop_next_stage_ack_be_index.is_some(),
        meta.drop_next_start_ack_be_index.is_some(),
        meta.suppress_start_ack_be_index.is_some(),
        meta.drop_next_terminal_ack_be_index.is_some(),
        meta.kill_query_at_lifecycle_phase.is_some(),
        meta.kill_fe_at_lifecycle_phase.is_some(),
        meta.stop_query_control_heartbeat_after_stage_be_index
            .is_some(),
    ]
    .into_iter()
    .filter(|configured| *configured)
    .count();
    if lifecycle_fault_count > 1 {
        bail!(
            "a SQL step may configure at most one query lifecycle fault directive; hold_start_until_early_ingress is schedule-shaping and may be combined"
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

    let be_count = server.be_count();
    for (name, index) in [
        (
            "drop_next_init_ack_be_index",
            meta.drop_next_init_ack_be_index,
        ),
        (
            "stop_query_control_heartbeat_be_index",
            meta.stop_query_control_heartbeat_be_index,
        ),
        (
            "restart_be_after_init_ack_index",
            meta.restart_be_after_init_ack_index,
        ),
        (
            "drop_next_stage_ack_be_index",
            meta.drop_next_stage_ack_be_index,
        ),
        (
            "drop_next_start_ack_be_index",
            meta.drop_next_start_ack_be_index,
        ),
        (
            "suppress_start_ack_be_index",
            meta.suppress_start_ack_be_index,
        ),
        (
            "drop_next_terminal_ack_be_index",
            meta.drop_next_terminal_ack_be_index,
        ),
        (
            "stop_query_control_heartbeat_after_stage_be_index",
            meta.stop_query_control_heartbeat_after_stage_be_index,
        ),
    ] {
        if let Some(index) = index
            && index >= be_count
        {
            bail!("{name} {index} is out of bounds for {be_count} BE(s)");
        }
    }
    if let Some(count) = meta.kill_fe_after_control_ready_count
        && !(1..=be_count).contains(&count)
    {
        bail!("kill_fe_after_control_ready_count must be between 1 and {be_count}, got {count}");
    }
    if let Some(count) = meta.kill_query_after_control_ready_count
        && !(1..=be_count).contains(&count)
    {
        bail!("kill_query_after_control_ready_count must be between 1 and {be_count}, got {count}");
    }
    if let Some(ordinal) = meta.fail_stage_prepare_ordinal
        && ordinal == 0
    {
        bail!("fail_stage_prepare_ordinal must be at least 1, got {ordinal}");
    }
    if let Some(limit) = meta.query_control_fragment_backend_limit
        && !(1..=be_count).contains(&limit)
    {
        bail!("query_control_fragment_backend_limit must be between 1 and {be_count}, got {limit}");
    }

    if let Some(index) = meta.drop_next_init_ack_be_index {
        server.arm_init_ack_drop(index)?;
    }
    if let Some(index) = meta.stop_query_control_heartbeat_be_index {
        server.arm_query_control_heartbeat_stop(index)?;
    }
    if let Some(count) = meta.kill_fe_after_control_ready_count {
        server.arm_fe_crash_after_control_ready(count)?;
    }
    if let Some(index) = meta.restart_be_after_init_ack_index {
        server.arm_be_restart_after_init_ack(index)?;
    }
    if let Some(ordinal) = meta.fail_stage_prepare_ordinal {
        server.arm_stage_prepare_failure(ordinal)?;
    }
    if let Some(index) = meta.drop_next_stage_ack_be_index {
        server.arm_stage_ack_drop(index)?;
    }
    if let Some(index) = meta.drop_next_start_ack_be_index {
        server.arm_start_ack_drop(index)?;
    }
    if let Some(index) = meta.suppress_start_ack_be_index {
        server.arm_start_ack_suppress(index)?;
    }
    if let Some(index) = meta.drop_next_terminal_ack_be_index {
        server.arm_terminal_ack_drop(index)?;
    }
    if let Some(index) = meta.drop_terminal_snapshot_stream_be_index {
        server.arm_terminal_snapshot_stream_drop(index)?;
    }
    if let Some(phase) = meta.kill_query_at_lifecycle_phase {
        server.arm_kill_query_at_lifecycle_phase(phase)?;
    }
    if let Some(phase) = meta.kill_fe_at_lifecycle_phase {
        server.arm_fe_crash_at_lifecycle_phase(phase)?;
    }
    if let Some(index) = meta.stop_query_control_heartbeat_after_stage_be_index {
        server.arm_query_control_heartbeat_stop_after_stage(index)?;
    }
    if meta.hold_start_until_early_ingress {
        server.arm_hold_start_until_early_ingress()?;
    }
    if let Some(limit) = meta.query_control_fragment_backend_limit {
        server.arm_query_control_fragment_backend_limit(limit)?;
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
    query_connection_id: Option<u32>,
    shared_deadline: Option<Instant>,
    execute_query: F,
) -> Result<T>
where
    F: FnOnce() -> T,
{
    #[derive(Clone, Copy)]
    enum PostQueryFault {
        KillBackend(usize),
        ReleaseFragmentFailure(usize),
        KillFrontendAfterControlReady(usize),
        RestartBackendAfterInitAck(usize),
        KillQueryAfterControlReady {
            ready_count: usize,
            connection_id: u32,
        },
        KillQueryAtLifecyclePhase {
            phase: crate::types::QueryLifecyclePhase,
            connection_id: u32,
        },
        KillFrontendAtLifecyclePhase(crate::types::QueryLifecyclePhase),
    }

    enum FaultBaseline {
        ScheduledFragments(Vec<(usize, u64)>),
        FrontendStage {
            marker_count: u64,
        },
        FrontendReady {
            ready_count: u64,
            coordinator_lost: Vec<u64>,
        },
        BackendInit {
            index: usize,
            token: String,
            start_epoch: u64,
        },
        FrontendPhase {
            phase: crate::types::QueryLifecyclePhase,
            fe_crash: bool,
            marker_count: u64,
        },
    }

    let faults = [
        meta.kill_be_after_fragment_start
            .map(PostQueryFault::KillBackend),
        meta.fail_fragment_after_start_be_index
            .map(PostQueryFault::ReleaseFragmentFailure),
        meta.kill_fe_after_control_ready_count
            .map(PostQueryFault::KillFrontendAfterControlReady),
        meta.restart_be_after_init_ack_index
            .map(PostQueryFault::RestartBackendAfterInitAck),
        meta.kill_query_after_control_ready_count
            .map(|ready_count| {
                query_connection_id
                    .map(|connection_id| PostQueryFault::KillQueryAfterControlReady {
                        ready_count,
                        connection_id,
                    })
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "kill_query_after_control_ready_count requires the target query connection id"
                        )
                    })
            })
            .transpose()?,
        meta.kill_query_at_lifecycle_phase
            .map(|phase| {
                query_connection_id
                    .map(|connection_id| PostQueryFault::KillQueryAtLifecyclePhase {
                        phase,
                        connection_id,
                    })
                    .ok_or_else(|| anyhow::anyhow!(
                        "kill_query_at_lifecycle_phase requires the target query connection id"
                    ))
            })
            .transpose()?,
        meta.kill_fe_at_lifecycle_phase
            .map(PostQueryFault::KillFrontendAtLifecyclePhase),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();
    let [fault] = faults.as_slice() else {
        if faults.is_empty() {
            return Ok(execute_query());
        }
        bail!("a SQL step may configure at most one post-query lifecycle fault");
    };
    let fault = *fault;
    let baseline = {
        let server = server
            .lock()
            .map_err(|_| anyhow::anyhow!("server handle mutex is poisoned"))?;
        if !server.supports_fault_injection() {
            bail!("post-query faults require a mutable cross-process server handle");
        }
        match fault {
            PostQueryFault::KillBackend(index) => {
                if index >= server.be_count() {
                    bail!(
                        "post-query fault index {index} is out of bounds for {} BE(s)",
                        server.be_count()
                    );
                }
                FaultBaseline::ScheduledFragments(vec![(
                    index,
                    server.scheduled_fragment_count(index)?,
                )])
            }
            PostQueryFault::ReleaseFragmentFailure(index) => {
                if index >= server.be_count() {
                    bail!(
                        "post-query fault index {index} is out of bounds for {} BE(s)",
                        server.be_count()
                    );
                }
                FaultBaseline::FrontendStage {
                    marker_count: server.fe_log_count("NOVAROCKS_QUERY_STAGE_BARRIER")? as u64,
                }
            }
            PostQueryFault::KillFrontendAfterControlReady(_) => FaultBaseline::FrontendReady {
                ready_count: server.fe_log_count("NOVAROCKS_QUERY_CONTROL_READY")? as u64,
                coordinator_lost: (0..server.be_count())
                    .map(|index| {
                        server.be_log_count(index, "NOVAROCKS_QUERY_CONTROL_COORDINATOR_LOST")
                    })
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .map(|count| count as u64)
                    .collect(),
            },
            PostQueryFault::KillQueryAfterControlReady { .. } => FaultBaseline::FrontendReady {
                ready_count: server.fe_log_count("NOVAROCKS_QUERY_CONTROL_READY")? as u64,
                coordinator_lost: Vec::new(),
            },
            PostQueryFault::RestartBackendAfterInitAck(index) => {
                if index >= server.be_count() {
                    bail!(
                        "post-query fault index {index} is out of bounds for {} BE(s)",
                        server.be_count()
                    );
                }
                FaultBaseline::BackendInit {
                    index,
                    token: server
                        .armed_query_lifecycle_fault_token(index, "restart-after-init-ack")?
                        .context("restart-after-InitAck fault has no armed token")?,
                    start_epoch: server.backend_start_epoch(index)?,
                }
            }
            PostQueryFault::KillQueryAtLifecyclePhase { phase, .. }
            | PostQueryFault::KillFrontendAtLifecyclePhase(phase) => FaultBaseline::FrontendPhase {
                phase,
                fe_crash: matches!(fault, PostQueryFault::KillFrontendAtLifecyclePhase(_)),
                marker_count: lifecycle_phase_marker_count(
                    &server.fe_log_contents()?,
                    phase,
                    matches!(fault, PostQueryFault::KillFrontendAtLifecyclePhase(_)),
                )? as u64,
            },
        }
    };
    let fault_state = Arc::new(ActiveQueryFaultState::new());
    let worker_server = Arc::clone(server);
    let worker_fault_state = Arc::clone(&fault_state);
    let worker = thread::spawn(move || -> Result<()> {
        let deadline =
            shared_deadline.unwrap_or_else(|| Instant::now() + POST_FRAGMENT_START_TIMEOUT);
        let mut deadline_cancel_sent = false;
        loop {
            if Instant::now() >= deadline {
                let server = worker_server
                    .lock()
                    .map_err(|_| anyhow::anyhow!("server handle mutex is poisoned"))?;
                let fe = server.fe_log_contents().unwrap_or_default();
                let bes = (0..server.be_count())
                    .map(|index| server.be_log_contents(index).unwrap_or_default())
                    .collect::<Vec<_>>();
                bail!(
                    "timed out waiting for post-query fault marker; fe_tail={:?}; be_tails={:?}",
                    log_tail(&fe),
                    bes.iter().map(|log| log_tail(log)).collect::<Vec<_>>()
                );
            }
            maybe_cancel_query_near_deadline(
                &worker_server,
                &worker_fault_state,
                query_connection_id,
                deadline,
                &mut deadline_cancel_sent,
            )?;
            let ready = {
                let server = worker_server
                    .lock()
                    .map_err(|_| anyhow::anyhow!("server handle mutex is poisoned"))?;
                match &baseline {
                    FaultBaseline::ScheduledFragments(baselines) => {
                        let mut all_fresh = true;
                        for &(index, baseline) in baselines {
                            let current = server.scheduled_fragment_count(index)?;
                            if current < baseline {
                                bail!(
                                    "BE[{index}] fragment-start marker count decreased from {baseline} to {current}"
                                );
                            }
                            all_fresh &= current > baseline;
                        }
                        all_fresh
                    }
                    FaultBaseline::FrontendStage { marker_count } => {
                        server.fe_log_count("NOVAROCKS_QUERY_STAGE_BARRIER")?
                            > *marker_count as usize
                    }
                    FaultBaseline::FrontendReady { ready_count, .. } => {
                        let target = match fault {
                            PostQueryFault::KillFrontendAfterControlReady(target) => target,
                            PostQueryFault::KillQueryAfterControlReady { ready_count, .. } => {
                                ready_count
                            }
                            _ => unreachable!(
                                "frontend baseline pairs with ControlReady-driven fault"
                            ),
                        };
                        server.fe_log_count("NOVAROCKS_QUERY_CONTROL_READY")?
                            >= (*ready_count as usize).saturating_add(target)
                    }
                    FaultBaseline::BackendInit { index, token, .. } => {
                        server.be_log_contents(*index)?.lines().any(|line| {
                            line.contains("NOVAROCKS_QUERY_INIT_ACK_OBSERVED")
                                && line.contains(&format!("token={token}"))
                        })
                    }
                    FaultBaseline::FrontendPhase {
                        phase,
                        fe_crash,
                        marker_count,
                    } => {
                        lifecycle_phase_marker_count(&server.fe_log_contents()?, *phase, *fe_crash)?
                            > *marker_count as usize
                    }
                }
            };
            if ready {
                if !worker_fault_state.claim_fault() {
                    bail!(
                        "query completed before the post-query fault could claim its marker barrier"
                    );
                }
                let mut server = worker_server
                    .lock()
                    .map_err(|_| anyhow::anyhow!("server handle mutex is poisoned"))?;
                let mut evidence_execution = match (&baseline, fault) {
                    (
                        FaultBaseline::FrontendReady { ready_count, .. },
                        PostQueryFault::KillFrontendAfterControlReady(_)
                        | PostQueryFault::KillQueryAfterControlReady { .. },
                    ) => fresh_fe_control_ready_execution(
                        &server.fe_log_contents()?,
                        *ready_count as usize,
                    )?,
                    (
                        FaultBaseline::FrontendPhase {
                            phase,
                            fe_crash,
                            marker_count,
                        },
                        PostQueryFault::KillQueryAtLifecyclePhase { .. }
                        | PostQueryFault::KillFrontendAtLifecyclePhase(_),
                    ) => fresh_lifecycle_phase_execution(
                        &server.fe_log_contents()?,
                        *marker_count as usize,
                        *phase,
                        *fe_crash,
                    )?,
                    _ => None,
                };
                let action_result = (|| -> Result<()> {
                    match fault {
                        PostQueryFault::KillBackend(index) => server.kill_be(index)?,
                        PostQueryFault::ReleaseFragmentFailure(index) => {
                            server.release_fragment_executor_failure(index)?
                        }
                        PostQueryFault::RestartBackendAfterInitAck(index) => {
                            let FaultBaseline::BackendInit {
                                token, start_epoch, ..
                            } = &baseline
                            else {
                                unreachable!("BE restart fault has BackendInit baseline")
                            };
                            let old_log = server.be_log_contents(index)?;
                            let old_execution = old_log
                                .lines()
                                .rev()
                                .find(|line| {
                                    line.contains("NOVAROCKS_QUERY_INIT_ACK_OBSERVED")
                                        && line.contains(&format!("token={token}"))
                                })
                                .and_then(|line| marker_field(line, "execution_id"))
                                .context("restart marker is missing execution_id")?;
                            let backend_id = old_log
                                .lines()
                                .rev()
                                .find(|line| {
                                    line.contains("NOVAROCKS_QUERY_INIT_ACK_OBSERVED")
                                        && line.contains(&format!("token={token}"))
                                })
                                .and_then(|line| marker_field(line, "backend_id"))
                                .context("restart marker is missing backend_id")?
                                .parse::<u64>()
                                .context("restart marker has invalid backend_id")?;
                            server.restart_be_until(index, deadline)?;
                            let new_epoch = server.backend_start_epoch(index)?;
                            if new_epoch == *start_epoch {
                                bail!(
                                    "BE[{index}] restart did not change start epoch: old={start_epoch} new={new_epoch}"
                                );
                            }
                            let new_log = server.be_current_log_contents(index)?;
                            validate_restart_nonrestore_status(
                                &new_log,
                                &old_execution,
                                backend_id,
                                new_epoch,
                            )?;
                            evidence_execution = Some(old_execution.clone());
                            println!(
                                "query lifecycle BE restart proof PASS: backend_index={index} backend_id={backend_id} token={token} old_execution={old_execution} old_epoch={start_epoch} new_epoch={new_epoch} control_ready=0 active_lifecycle=0 fragment_admissions=0 fragment_acceptances=0 lifecycle_entries=0 lifecycle_tombstones=0 pre_init_tombstones=0 tombstone_index=0 restored=false"
                            );
                        }
                        PostQueryFault::KillQueryAfterControlReady { connection_id, .. } => {
                            server.kill_query_until(connection_id, deadline)?
                        }
                        PostQueryFault::KillQueryAtLifecyclePhase {
                            phase,
                            connection_id,
                        } => {
                            server.kill_query_until(connection_id, deadline)?;
                            server.release_query_lifecycle_phase_fault(phase, false)?;
                        }
                        PostQueryFault::KillFrontendAtLifecyclePhase(phase) => {
                            server.kill_fe()?;
                            server.release_query_lifecycle_phase_fault(phase, true)?;
                            server.restart_fe_until(deadline)?;
                        }
                        PostQueryFault::KillFrontendAfterControlReady(_) => {
                            server.kill_fe()?;
                            let FaultBaseline::FrontendReady {
                                coordinator_lost, ..
                            } = &baseline
                            else {
                                unreachable!("FE crash fault has frontend baseline");
                            };
                            loop {
                                if Instant::now() >= deadline {
                                    let fe = server.fe_log_contents().unwrap_or_default();
                                    let bes = (0..server.be_count())
                                        .map(|index| {
                                            server.be_log_contents(index).unwrap_or_default()
                                        })
                                        .collect::<Vec<_>>();
                                    bail!(
                                        "timed out waiting for coordinator-lost marker on every BE after FE crash; fe_tail={:?}; be_tails={:?}",
                                        log_tail(&fe),
                                        bes.iter().map(|log| log_tail(log)).collect::<Vec<_>>()
                                    );
                                }
                                let lost_executions = coordinator_lost
                                    .iter()
                                    .enumerate()
                                    .map(|(index, baseline)| {
                                        let log = server.be_log_contents(index)?;
                                        let execution = log
                                            .lines()
                                            .filter(|line| {
                                                line.contains(
                                                    "NOVAROCKS_QUERY_CONTROL_COORDINATOR_LOST",
                                                )
                                            })
                                            .skip(*baseline as usize)
                                            .find_map(|line| marker_field(line, "execution_id"));
                                        Ok(execution)
                                    })
                                    .collect::<Result<Vec<_>>>()?;
                                let all_lost = lost_executions.iter().all(Option::is_some);
                                let same_execution = all_lost
                                    && lost_executions.iter().flatten().all(|execution| {
                                        Some(execution) == evidence_execution.as_ref()
                                    });
                                if same_execution {
                                    break;
                                }
                                sleep(POST_FRAGMENT_START_POLL_INTERVAL);
                            }
                            server.clear_query_lifecycle_faults()?;
                            server.restart_fe_until(deadline)?;
                        }
                    }
                    Ok(())
                })();
                if let Err(error) = action_result {
                    let fe = server.fe_log_contents().unwrap_or_default();
                    let bes = (0..server.be_count())
                        .map(|index| server.be_log_contents(index).unwrap_or_default())
                        .collect::<Vec<_>>();
                    bail!(
                        "post-query fault action failed within 30s deadline: {error:#}; fe_tail={:?}; be_tails={:?}",
                        log_tail(&fe),
                        bes.iter().map(|log| log_tail(log)).collect::<Vec<_>>()
                    );
                }
                if matches!(
                    fault,
                    PostQueryFault::KillFrontendAfterControlReady(_)
                        | PostQueryFault::KillQueryAfterControlReady { .. }
                ) {
                    deadline_cancel_sent = true;
                }
                drop(server);
                loop {
                    if Instant::now() >= deadline {
                        let server = worker_server
                            .lock()
                            .map_err(|_| anyhow::anyhow!("server handle mutex is poisoned"))?;
                        let fe = server.fe_log_contents().unwrap_or_default();
                        let bes = (0..server.be_count())
                            .map(|index| server.be_log_contents(index).unwrap_or_default())
                            .collect::<Vec<_>>();
                        bail!(
                            "post-query lifecycle deadline expired before query completion and required terminal cleanup evidence: query_done={} execution_id={:?}; fe_tail={:?}; be_tails={:?}",
                            worker_fault_state.query_is_done(),
                            evidence_execution,
                            log_tail(&fe),
                            bes.iter().map(|log| log_tail(log)).collect::<Vec<_>>()
                        );
                    }
                    maybe_cancel_query_near_deadline(
                        &worker_server,
                        &worker_fault_state,
                        query_connection_id,
                        deadline,
                        &mut deadline_cancel_sent,
                    )?;
                    let evidence_ready = match fault {
                        PostQueryFault::KillFrontendAfterControlReady(_)
                        | PostQueryFault::KillQueryAfterControlReady { .. } => {
                            let execution = evidence_execution
                                .as_deref()
                                .context("post-query lifecycle fault has no execution anchor")?;
                            let server = worker_server
                                .lock()
                                .map_err(|_| anyhow::anyhow!("server handle mutex is poisoned"))?;
                            terminal_cleanup_on_all_backends(server.as_ref(), execution)?
                        }
                        _ => true,
                    };
                    if worker_fault_state.query_is_done() && evidence_ready {
                        return Ok(());
                    }
                    sleep(POST_FRAGMENT_START_POLL_INTERVAL);
                }
            }
            if worker_fault_state.query_is_done() {
                bail!("query completed before the post-query marker barrier was reached");
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

fn maybe_cancel_query_near_deadline(
    server: &Arc<Mutex<Box<dyn ServerHandle>>>,
    fault_state: &ActiveQueryFaultState,
    query_connection_id: Option<u32>,
    deadline: Instant,
    cancel_sent: &mut bool,
) -> Result<()> {
    #[cfg(not(test))]
    const DEADLINE_CANCEL_RESERVE: Duration = Duration::from_secs(1);
    #[cfg(test)]
    const DEADLINE_CANCEL_RESERVE: Duration = Duration::from_millis(100);
    if *cancel_sent
        || fault_state.query_is_done()
        || deadline.saturating_duration_since(Instant::now()) > DEADLINE_CANCEL_RESERVE
    {
        return Ok(());
    }
    let Some(connection_id) = query_connection_id else {
        return Ok(());
    };
    let mut server = server
        .lock()
        .map_err(|_| anyhow::anyhow!("server handle mutex is poisoned"))?;
    if let Err(error) = server.kill_query_until(connection_id, deadline) {
        let benign_completion_race = error.to_string().contains("has no active query")
            || error.to_string().contains("ER_NO_SUCH_THREAD")
            || error.to_string().contains("ERROR 1094");
        if !benign_completion_race {
            let fe = server.fe_log_contents().unwrap_or_default();
            let bes = (0..server.be_count())
                .map(|index| server.be_log_contents(index).unwrap_or_default())
                .collect::<Vec<_>>();
            bail!(
                "cancel target query connection {connection_id} before shared fault deadline failed: {error:#}; fe_tail={:?}; be_tails={:?}",
                log_tail(&fe),
                bes.iter().map(|log| log_tail(log)).collect::<Vec<_>>()
            );
        }
    }
    *cancel_sent = true;
    Ok(())
}

fn fresh_fe_control_ready_execution(log: &str, baseline: usize) -> Result<Option<String>> {
    let executions = log
        .lines()
        .filter(|line| line.contains("NOVAROCKS_QUERY_CONTROL_READY"))
        .skip(baseline)
        .filter_map(|line| marker_field(line, "execution_id"))
        .collect::<Vec<_>>();
    let Some(first) = executions.first() else {
        return Ok(None);
    };
    if executions.iter().any(|execution| execution != first) {
        bail!("fresh FE ControlReady markers span multiple executions: {executions:?}");
    }
    Ok(Some(first.clone()))
}

fn lifecycle_phase_marker_count(
    log: &str,
    phase: crate::types::QueryLifecyclePhase,
    fe_crash: bool,
) -> Result<usize> {
    let action = if fe_crash { "kill_fe" } else { "kill_query" };
    let markers = log
        .lines()
        .filter(|line| line.contains("NOVAROCKS_QUERY_LIFECYCLE_PHASE"))
        .filter(|line| {
            marker_field(line, "phase").as_deref() == Some(phase.as_str())
                && marker_field(line, "action").as_deref() == Some(action)
        })
        .collect::<Vec<_>>();
    if markers
        .iter()
        .any(|line| marker_field(line, "token").is_none())
    {
        bail!("lifecycle phase marker has no token: {markers:?}");
    }
    Ok(markers.len())
}

fn fresh_lifecycle_phase_execution(
    log: &str,
    baseline: usize,
    phase: crate::types::QueryLifecyclePhase,
    fe_crash: bool,
) -> Result<Option<String>> {
    let action = if fe_crash { "kill_fe" } else { "kill_query" };
    let executions = log
        .lines()
        .filter(|line| line.contains("NOVAROCKS_QUERY_LIFECYCLE_PHASE"))
        .filter(|line| {
            marker_field(line, "phase").as_deref() == Some(phase.as_str())
                && marker_field(line, "action").as_deref() == Some(action)
        })
        .skip(baseline)
        .filter_map(|line| marker_field(line, "execution_id"))
        .collect::<Vec<_>>();
    let Some(first) = executions.first() else {
        return Ok(None);
    };
    if executions.iter().any(|execution| execution != first) {
        bail!("fresh lifecycle phase markers span multiple executions: {executions:?}");
    }
    Ok(Some(first.clone()))
}

fn terminal_cleanup_on_all_backends(server: &dyn ServerHandle, execution_id: &str) -> Result<bool> {
    if server.be_count() != 3 {
        bail!(
            "post-query lifecycle terminal cleanup evidence requires exactly 3 BEs, found {}",
            server.be_count()
        );
    }
    for index in 0..3 {
        let log = server.be_log_contents(index)?;
        let terminated = log.lines().any(|line| {
            line.contains("NOVAROCKS_QUERY_LIFECYCLE_TERMINATED")
                && marker_field(line, "execution_id").as_deref() == Some(execution_id)
        });
        let cleaned = log.lines().any(|line| {
            line.contains("NOVAROCKS_QUERY_LIFECYCLE_CLEANUP")
                && marker_field(line, "execution_id").as_deref() == Some(execution_id)
                && marker_field(line, "active").as_deref() == Some("false")
                && marker_field(line, "tombstone").as_deref() == Some("true")
        });
        if !terminated || !cleaned {
            return Ok(false);
        }
    }
    Ok(true)
}

fn validate_restart_nonrestore_status(
    new_log: &str,
    old_execution: &str,
    backend_id: u64,
    new_epoch: u64,
) -> Result<()> {
    for forbidden in [
        "NOVAROCKS_QUERY_CONTROL_READY",
        "NOVAROCKS_QUERY_FRAGMENT_ACCEPTED",
        "NOVAROCKS_QUERY_INIT_APPLIED",
    ] {
        if new_log.lines().any(|line| {
            line.contains(forbidden)
                && marker_field(line, "execution_id").as_deref() == Some(old_execution)
        }) {
            bail!(
                "BE backend_id={backend_id} epoch={new_epoch} restored old execution {old_execution}: found {forbidden}"
            );
        }
    }
    let marker = new_log
        .lines()
        .find(|line| {
            line.contains("NOVAROCKS_QUERY_LIFECYCLE_RESTORE_STATUS")
                && marker_field(line, "backend_id")
                    .and_then(|value| value.parse::<u64>().ok())
                    == Some(backend_id)
                && marker_field(line, "start_epoch")
                    .and_then(|value| value.parse::<u64>().ok())
                    == Some(new_epoch)
        })
        .with_context(|| {
            format!(
                "new BE has no restoration-status marker for backend_id={backend_id} start_epoch={new_epoch}"
            )
        })?;
    for (field, expected) in [
        ("control_ready", "0"),
        ("active_lifecycle", "0"),
        ("fragment_admissions", "0"),
        ("fragment_acceptances", "0"),
        ("lifecycle_entries", "0"),
        ("lifecycle_tombstones", "0"),
        ("pre_init_tombstones", "0"),
        ("tombstone_index", "0"),
        ("restored", "false"),
    ] {
        let actual = marker_field(marker, field);
        if actual.as_deref() != Some(expected) {
            bail!(
                "new BE restoration status for old execution {old_execution} backend_id={backend_id} start_epoch={new_epoch} requires {field}={expected}, found {actual:?}"
            );
        }
    }
    Ok(())
}

fn log_tail(log: &str) -> String {
    log.lines()
        .rev()
        .take(40)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>()
        .join("\n")
}

fn marker_field(line: &str, name: &str) -> Option<String> {
    let prefix = format!("{name}=");
    line.split_ascii_whitespace()
        .find_map(|field| field.strip_prefix(&prefix).map(ToOwned::to_owned))
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

        fn be_count(&self) -> usize {
            3
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

        fn arm_init_ack_drop(&mut self, index: usize) -> Result<()> {
            self.events.push(format!("arm-init-ack-drop:{index}"));
            Ok(())
        }

        fn arm_query_control_heartbeat_stop(&mut self, index: usize) -> Result<()> {
            self.events.push(format!("arm-heartbeat-stop:{index}"));
            Ok(())
        }

        fn arm_fe_crash_after_control_ready(&mut self, count: usize) -> Result<()> {
            self.events.push(format!("arm-fe-crash:{count}"));
            Ok(())
        }

        fn arm_be_restart_after_init_ack(&mut self, index: usize) -> Result<()> {
            self.events.push(format!("arm-be-restart:{index}"));
            Ok(())
        }

        fn arm_stage_prepare_failure(&mut self, ordinal: usize) -> Result<()> {
            self.events
                .push(format!("arm-stage-prepare-failure:{ordinal}"));
            Ok(())
        }

        fn arm_stage_ack_drop(&mut self, index: usize) -> Result<()> {
            self.events.push(format!("arm-stage-ack-drop:{index}"));
            Ok(())
        }

        fn arm_start_ack_drop(&mut self, index: usize) -> Result<()> {
            self.events.push(format!("arm-start-ack-drop:{index}"));
            Ok(())
        }

        fn arm_start_ack_suppress(&mut self, index: usize) -> Result<()> {
            self.events.push(format!("arm-start-ack-suppress:{index}"));
            Ok(())
        }

        fn arm_terminal_ack_drop(&mut self, index: usize) -> Result<()> {
            self.events.push(format!("arm-terminal-ack-drop:{index}"));
            Ok(())
        }

        fn arm_kill_query_at_lifecycle_phase(
            &mut self,
            phase: crate::types::QueryLifecyclePhase,
        ) -> Result<()> {
            self.events
                .push(format!("arm-kill-query-phase:{}", phase.as_str()));
            Ok(())
        }

        fn arm_fe_crash_at_lifecycle_phase(
            &mut self,
            phase: crate::types::QueryLifecyclePhase,
        ) -> Result<()> {
            self.events
                .push(format!("arm-fe-crash-phase:{}", phase.as_str()));
            Ok(())
        }

        fn arm_query_control_heartbeat_stop_after_stage(&mut self, index: usize) -> Result<()> {
            self.events
                .push(format!("arm-heartbeat-stop-after-stage:{index}"));
            Ok(())
        }

        fn arm_hold_start_until_early_ingress(&mut self) -> Result<()> {
            self.events
                .push("arm-hold-start-until-early-ingress".to_string());
            Ok(())
        }

        fn arm_query_control_fragment_backend_limit(&mut self, limit: usize) -> Result<()> {
            self.events.push(format!("arm-fragment-limit:{limit}"));
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
    fn lifecycle_fault_directives_require_cross_process_mode() {
        struct UnsupportedServerHandle;

        impl ServerHandle for UnsupportedServerHandle {
            fn target_host(&self) -> Option<&str> {
                None
            }

            fn target_port(&self) -> Option<u16> {
                None
            }
        }

        for meta in [
            QueryMeta {
                drop_next_init_ack_be_index: Some(0),
                ..QueryMeta::default()
            },
            QueryMeta {
                stop_query_control_heartbeat_be_index: Some(0),
                ..QueryMeta::default()
            },
            QueryMeta {
                kill_fe_after_control_ready_count: Some(1),
                ..QueryMeta::default()
            },
            QueryMeta {
                restart_be_after_init_ack_index: Some(0),
                ..QueryMeta::default()
            },
            QueryMeta {
                kill_query_after_control_ready_count: Some(1),
                ..QueryMeta::default()
            },
            QueryMeta {
                query_control_fragment_backend_limit: Some(1),
                ..QueryMeta::default()
            },
            QueryMeta {
                fail_stage_prepare_ordinal: Some(1),
                ..QueryMeta::default()
            },
            QueryMeta {
                drop_next_stage_ack_be_index: Some(0),
                ..QueryMeta::default()
            },
            QueryMeta {
                drop_next_start_ack_be_index: Some(0),
                ..QueryMeta::default()
            },
            QueryMeta {
                suppress_start_ack_be_index: Some(0),
                ..QueryMeta::default()
            },
            QueryMeta {
                kill_query_at_lifecycle_phase: Some(crate::types::QueryLifecyclePhase::Staged),
                ..QueryMeta::default()
            },
            QueryMeta {
                kill_fe_at_lifecycle_phase: Some(crate::types::QueryLifecyclePhase::Staged),
                ..QueryMeta::default()
            },
            QueryMeta {
                stop_query_control_heartbeat_after_stage_be_index: Some(0),
                ..QueryMeta::default()
            },
            QueryMeta {
                hold_start_until_early_ingress: true,
                ..QueryMeta::default()
            },
        ] {
            let mut server = UnsupportedServerHandle;
            let error = apply_pre_query(&meta, &mut server)
                .expect_err("lifecycle faults must reject non-cross-process mode");
            assert!(
                error
                    .to_string()
                    .contains("require a mutable cross-process server handle"),
                "unexpected error: {error}"
            );
        }
    }

    #[test]
    fn lifecycle_fault_directives_reject_mutually_exclusive_faults_before_mutation() {
        let mut server = RecordingServerHandle::default();
        let meta = QueryMeta {
            drop_next_init_ack_be_index: Some(0),
            stop_query_control_heartbeat_be_index: Some(1),
            ..QueryMeta::default()
        };

        let error = apply_pre_query(&meta, &mut server)
            .expect_err("one step may arm only one lifecycle failure");

        assert!(
            error
                .to_string()
                .contains("at most one query lifecycle fault directive"),
            "unexpected error: {error}"
        );
        assert!(server.events.is_empty());
    }

    #[test]
    fn lifecycle_start_hold_may_combine_with_one_primary_fault() {
        let mut server = RecordingServerHandle::default();
        let meta = QueryMeta {
            drop_next_start_ack_be_index: Some(1),
            hold_start_until_early_ingress: true,
            ..QueryMeta::default()
        };

        apply_pre_query(&meta, &mut server).expect("schedule-shaping hold may compose");

        assert_eq!(
            server.events,
            vec![
                "arm-start-ack-drop:1".to_string(),
                "arm-hold-start-until-early-ingress".to_string(),
            ]
        );
    }

    #[test]
    fn lifecycle_fault_directives_validate_counts_and_backend_indices() {
        struct ThreeBackendServer;

        impl ServerHandle for ThreeBackendServer {
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
                3
            }
        }

        for (meta, expected) in [
            (
                QueryMeta {
                    drop_next_init_ack_be_index: Some(3),
                    ..QueryMeta::default()
                },
                "drop_next_init_ack_be_index 3 is out of bounds for 3 BE(s)",
            ),
            (
                QueryMeta {
                    stop_query_control_heartbeat_be_index: Some(4),
                    ..QueryMeta::default()
                },
                "stop_query_control_heartbeat_be_index 4 is out of bounds for 3 BE(s)",
            ),
            (
                QueryMeta {
                    restart_be_after_init_ack_index: Some(5),
                    ..QueryMeta::default()
                },
                "restart_be_after_init_ack_index 5 is out of bounds for 3 BE(s)",
            ),
            (
                QueryMeta {
                    kill_fe_after_control_ready_count: Some(0),
                    ..QueryMeta::default()
                },
                "kill_fe_after_control_ready_count must be between 1 and 3",
            ),
            (
                QueryMeta {
                    kill_query_after_control_ready_count: Some(4),
                    ..QueryMeta::default()
                },
                "kill_query_after_control_ready_count must be between 1 and 3",
            ),
            (
                QueryMeta {
                    query_control_fragment_backend_limit: Some(0),
                    ..QueryMeta::default()
                },
                "query_control_fragment_backend_limit must be between 1 and 3",
            ),
            (
                QueryMeta {
                    query_control_fragment_backend_limit: Some(4),
                    ..QueryMeta::default()
                },
                "query_control_fragment_backend_limit must be between 1 and 3",
            ),
            (
                QueryMeta {
                    fail_stage_prepare_ordinal: Some(0),
                    ..QueryMeta::default()
                },
                "fail_stage_prepare_ordinal must be at least 1",
            ),
            (
                QueryMeta {
                    drop_next_stage_ack_be_index: Some(3),
                    ..QueryMeta::default()
                },
                "drop_next_stage_ack_be_index 3 is out of bounds for 3 BE(s)",
            ),
            (
                QueryMeta {
                    drop_next_start_ack_be_index: Some(3),
                    ..QueryMeta::default()
                },
                "drop_next_start_ack_be_index 3 is out of bounds for 3 BE(s)",
            ),
            (
                QueryMeta {
                    suppress_start_ack_be_index: Some(3),
                    ..QueryMeta::default()
                },
                "suppress_start_ack_be_index 3 is out of bounds for 3 BE(s)",
            ),
            (
                QueryMeta {
                    stop_query_control_heartbeat_after_stage_be_index: Some(3),
                    ..QueryMeta::default()
                },
                "stop_query_control_heartbeat_after_stage_be_index 3 is out of bounds for 3 BE(s)",
            ),
        ] {
            let mut server = ThreeBackendServer;
            let error = apply_pre_query(&meta, &mut server)
                .expect_err("invalid lifecycle fault target must fail");
            assert!(
                error.to_string().contains(expected),
                "expected {expected:?}, got {error:#}"
            );
        }
    }

    #[test]
    fn lifecycle_fault_directives_arm_tokenized_cluster_hooks() {
        for (meta, expected_event) in [
            (
                QueryMeta {
                    drop_next_init_ack_be_index: Some(1),
                    ..QueryMeta::default()
                },
                "arm-init-ack-drop:1",
            ),
            (
                QueryMeta {
                    stop_query_control_heartbeat_be_index: Some(2),
                    ..QueryMeta::default()
                },
                "arm-heartbeat-stop:2",
            ),
            (
                QueryMeta {
                    kill_fe_after_control_ready_count: Some(3),
                    ..QueryMeta::default()
                },
                "arm-fe-crash:3",
            ),
            (
                QueryMeta {
                    restart_be_after_init_ack_index: Some(0),
                    ..QueryMeta::default()
                },
                "arm-be-restart:0",
            ),
            (
                QueryMeta {
                    query_control_fragment_backend_limit: Some(2),
                    ..QueryMeta::default()
                },
                "arm-fragment-limit:2",
            ),
            (
                QueryMeta {
                    fail_stage_prepare_ordinal: Some(2),
                    ..QueryMeta::default()
                },
                "arm-stage-prepare-failure:2",
            ),
            (
                QueryMeta {
                    drop_next_stage_ack_be_index: Some(1),
                    ..QueryMeta::default()
                },
                "arm-stage-ack-drop:1",
            ),
            (
                QueryMeta {
                    drop_next_start_ack_be_index: Some(1),
                    ..QueryMeta::default()
                },
                "arm-start-ack-drop:1",
            ),
            (
                QueryMeta {
                    suppress_start_ack_be_index: Some(1),
                    ..QueryMeta::default()
                },
                "arm-start-ack-suppress:1",
            ),
            (
                QueryMeta {
                    drop_next_terminal_ack_be_index: Some(1),
                    ..QueryMeta::default()
                },
                "arm-terminal-ack-drop:1",
            ),
            (
                QueryMeta {
                    kill_query_at_lifecycle_phase: Some(
                        crate::types::QueryLifecyclePhase::Starting,
                    ),
                    ..QueryMeta::default()
                },
                "arm-kill-query-phase:starting",
            ),
            (
                QueryMeta {
                    kill_fe_at_lifecycle_phase: Some(crate::types::QueryLifecyclePhase::Staged),
                    ..QueryMeta::default()
                },
                "arm-fe-crash-phase:staged",
            ),
            (
                QueryMeta {
                    stop_query_control_heartbeat_after_stage_be_index: Some(1),
                    ..QueryMeta::default()
                },
                "arm-heartbeat-stop-after-stage:1",
            ),
            (
                QueryMeta {
                    hold_start_until_early_ingress: true,
                    ..QueryMeta::default()
                },
                "arm-hold-start-until-early-ingress",
            ),
        ] {
            let mut server = RecordingServerHandle::default();
            apply_pre_query(&meta, &mut server).expect("arm lifecycle hook");
            assert_eq!(server.events, vec![expected_event]);
        }
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

        let result =
            execute_with_post_fragment_start_fault(&meta, &server_handle, None, None, || {
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

    struct AllBackendsReleaseServerHandle {
        state: Arc<(Mutex<AllBackendsReleaseState>, Condvar)>,
    }

    #[derive(Default)]
    struct AllBackendsReleaseState {
        baseline_reads: Vec<usize>,
        fresh_reads: Vec<usize>,
        query_started: bool,
        released_index: Option<usize>,
        events: Vec<&'static str>,
    }

    impl ServerHandle for AllBackendsReleaseServerHandle {
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
            3
        }

        fn scheduled_fragment_count(&self, index: usize) -> Result<u64> {
            let (lock, _) = self.state.as_ref();
            let mut state = lock.lock().expect("all-backend release state");
            if state.query_started {
                state.fresh_reads.push(index);
                Ok(11)
            } else {
                state.baseline_reads.push(index);
                Ok(10)
            }
        }

        fn fe_log_count(&self, marker: &str) -> Result<usize> {
            assert_eq!(marker, "NOVAROCKS_QUERY_STAGE_BARRIER");
            let (lock, _) = self.state.as_ref();
            let state = lock.lock().expect("all-backend release state");
            Ok(usize::from(state.query_started))
        }

        fn release_fragment_executor_failure(&mut self, index: usize) -> Result<()> {
            let (lock, wake) = self.state.as_ref();
            let mut state = lock.lock().expect("all-backend release state");
            state.released_index = Some(index);
            state.events.push("release");
            wake.notify_all();
            Ok(())
        }
    }

    #[test]
    fn fragment_failure_release_waits_for_the_stage_barrier() {
        let state = Arc::new((
            Mutex::new(AllBackendsReleaseState::default()),
            Condvar::new(),
        ));
        let server_handle: Arc<Mutex<Box<dyn ServerHandle>>> =
            Arc::new(Mutex::new(Box::new(AllBackendsReleaseServerHandle {
                state: Arc::clone(&state),
            })));
        let meta = QueryMeta {
            fail_fragment_after_start_be_index: Some(1),
            ..QueryMeta::default()
        };

        let result =
            execute_with_post_fragment_start_fault(&meta, &server_handle, None, None, || {
                let (lock, wake) = state.as_ref();
                let mut query = lock.lock().expect("all-backend release state");
                query.events.push("query:start");
                query.query_started = true;
                wake.notify_all();
                query = wake
                    .wait_while(query, |state| state.released_index.is_none())
                    .expect("wait for runner release");
                query.events.push("query:end");
                42
            })
            .expect("active-query fragment failure release");

        assert_eq!(result, 42);
        let state = state.0.lock().expect("all-backend release state");
        assert!(state.baseline_reads.is_empty());
        assert!(state.fresh_reads.is_empty());
        assert_eq!(state.released_index, Some(1));
        assert_eq!(state.events, vec!["query:start", "release", "query:end"]);
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
            let _ = execute_with_post_fragment_start_fault(
                &meta,
                &server_handle,
                None,
                None,
                || -> () {
                    panic!("simulated query panic");
                },
            );
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

    struct KillQueryServerHandle {
        state: Arc<(Mutex<KillQueryState>, Condvar)>,
    }

    #[derive(Default)]
    struct KillQueryState {
        control_ready_count: usize,
        killed_connection_id: Option<u32>,
    }

    impl ServerHandle for KillQueryServerHandle {
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
            3
        }

        fn fe_log_count(&self, needle: &str) -> Result<usize> {
            assert_eq!(needle, "NOVAROCKS_QUERY_CONTROL_READY");
            Ok(self
                .state
                .0
                .lock()
                .expect("kill-query state")
                .control_ready_count)
        }

        fn fe_log_contents(&self) -> Result<String> {
            let count = self
                .state
                .0
                .lock()
                .expect("kill-query state")
                .control_ready_count;
            Ok((0..count)
                .map(|_| "NOVAROCKS_QUERY_CONTROL_READY execution_id=10:20:1 backend_id=1\n")
                .collect())
        }

        fn be_log_contents(&self, index: usize) -> Result<String> {
            let killed = self
                .state
                .0
                .lock()
                .expect("kill-query state")
                .killed_connection_id
                .is_some();
            Ok(if killed {
                format!(
                    "NOVAROCKS_QUERY_LIFECYCLE_TERMINATED execution_id=10:20:1 backend_id={index} reason=CoordinatorAbort\nNOVAROCKS_QUERY_LIFECYCLE_CLEANUP execution_id=10:20:1 backend_id={index} active=false tombstone=true reason=CoordinatorAbort\n"
                )
            } else {
                String::new()
            })
        }

        fn kill_query(&mut self, connection_id: u32) -> Result<()> {
            let (lock, wake) = self.state.as_ref();
            let mut state = lock.lock().expect("kill-query state");
            state.killed_connection_id = Some(connection_id);
            wake.notify_all();
            Ok(())
        }
    }

    #[test]
    fn kill_query_waits_for_control_ready_and_uses_separate_connection_id() {
        let state = Arc::new((Mutex::new(KillQueryState::default()), Condvar::new()));
        let server_handle: Arc<Mutex<Box<dyn ServerHandle>>> =
            Arc::new(Mutex::new(Box::new(KillQueryServerHandle {
                state: Arc::clone(&state),
            })));
        let meta = QueryMeta {
            kill_query_after_control_ready_count: Some(3),
            ..QueryMeta::default()
        };

        let result =
            execute_with_post_fragment_start_fault(&meta, &server_handle, Some(41), None, || {
                let (lock, wake) = state.as_ref();
                let mut query = lock.lock().expect("kill-query state");
                query.control_ready_count = 3;
                wake.notify_all();
                query = wake
                    .wait_while(query, |state| state.killed_connection_id.is_none())
                    .expect("wait for KILL QUERY");
                query.killed_connection_id
            })
            .expect("KILL QUERY orchestration");

        assert_eq!(result, Some(41));
    }

    #[test]
    fn expired_shared_deadline_never_claims_a_post_query_fault() {
        let state = Arc::new((Mutex::new(KillQueryState::default()), Condvar::new()));
        let server_handle: Arc<Mutex<Box<dyn ServerHandle>>> =
            Arc::new(Mutex::new(Box::new(KillQueryServerHandle {
                state: Arc::clone(&state),
            })));
        let meta = QueryMeta {
            kill_query_after_control_ready_count: Some(3),
            ..QueryMeta::default()
        };

        let error = execute_with_post_fragment_start_fault(
            &meta,
            &server_handle,
            Some(41),
            Some(Instant::now()),
            || None::<u32>,
        )
        .expect_err("expired deadline must fail before claiming the fault");

        assert!(error.to_string().contains("timed out"));
        assert_eq!(
            state
                .0
                .lock()
                .expect("kill-query state")
                .killed_connection_id,
            None
        );
    }

    #[test]
    fn deadline_cancel_accepts_no_active_query_after_concurrent_completion() {
        struct CompletionRaceHandle;

        impl ServerHandle for CompletionRaceHandle {
            fn target_host(&self) -> Option<&str> {
                None
            }

            fn target_port(&self) -> Option<u16> {
                None
            }

            fn supports_fault_injection(&self) -> bool {
                true
            }

            fn kill_query(&mut self, _connection_id: u32) -> Result<()> {
                bail!("ERROR 1094 (HY000): connection has no active query")
            }
        }

        let fault_state = Arc::new(ActiveQueryFaultState::new());
        let server: Arc<Mutex<Box<dyn ServerHandle>>> =
            Arc::new(Mutex::new(Box::new(CompletionRaceHandle)));
        let mut cancel_sent = false;

        maybe_cancel_query_near_deadline(
            &server,
            fault_state.as_ref(),
            Some(41),
            Instant::now() + Duration::from_millis(50),
            &mut cancel_sent,
        )
        .expect("concurrent query completion makes no-active-query benign");

        assert!(cancel_sent);
        assert!(
            !fault_state.query_is_done(),
            "the benign decision must not depend on the client setting query_done first"
        );
    }

    #[test]
    fn restart_nonrestore_proof_requires_all_restoration_relevant_state_fields() {
        let complete = "NOVAROCKS_QUERY_LIFECYCLE_RESTORE_STATUS backend_id=7 start_epoch=42 control_ready=0 active_lifecycle=0 fragment_admissions=0 fragment_acceptances=0 lifecycle_entries=0 lifecycle_tombstones=0 pre_init_tombstones=0 tombstone_index=0 restored=false\n";
        validate_restart_nonrestore_status(complete, "10:20:1", 7, 42)
            .expect("complete fresh-process state proves non-restoration");

        for field in [
            "control_ready=0",
            "active_lifecycle=0",
            "fragment_admissions=0",
            "fragment_acceptances=0",
            "lifecycle_entries=0",
            "lifecycle_tombstones=0",
            "pre_init_tombstones=0",
            "tombstone_index=0",
            "restored=false",
        ] {
            let incomplete = complete.replace(field, "");
            let error = validate_restart_nonrestore_status(&incomplete, "10:20:1", 7, 42)
                .expect_err("missing restoration field must fail");
            assert!(error.to_string().contains(field.split('=').next().unwrap()));
        }
    }

    #[test]
    fn restart_nonrestore_proof_rejects_nonzero_retained_execution_indexes() {
        let complete = "NOVAROCKS_QUERY_LIFECYCLE_RESTORE_STATUS backend_id=7 start_epoch=42 control_ready=0 active_lifecycle=0 fragment_admissions=0 fragment_acceptances=0 lifecycle_entries=0 lifecycle_tombstones=0 pre_init_tombstones=0 tombstone_index=0 restored=false\n";

        for field in [
            "lifecycle_entries",
            "lifecycle_tombstones",
            "pre_init_tombstones",
            "tombstone_index",
        ] {
            let retained = complete.replace(&format!("{field}=0"), &format!("{field}=1"));
            let error = validate_restart_nonrestore_status(&retained, "10:20:1", 7, 42)
                .expect_err("nonzero retained execution index must reject restart proof");
            assert!(
                error.to_string().contains(field),
                "error must identify retained field {field}: {error:#}"
            );
        }
    }

    #[test]
    fn restart_nonrestore_proof_rejects_old_execution_control_or_fragment_state() {
        let status = "NOVAROCKS_QUERY_LIFECYCLE_RESTORE_STATUS backend_id=7 start_epoch=42 control_ready=0 active_lifecycle=0 fragment_admissions=0 fragment_acceptances=0 lifecycle_entries=0 lifecycle_tombstones=0 pre_init_tombstones=0 tombstone_index=0 restored=false\n";
        for marker in [
            "NOVAROCKS_QUERY_CONTROL_READY",
            "NOVAROCKS_QUERY_FRAGMENT_ACCEPTED",
            "NOVAROCKS_QUERY_INIT_APPLIED",
        ] {
            let log = format!("{status}{marker} execution_id=10:20:1 backend_id=7\n");
            let error = validate_restart_nonrestore_status(&log, "10:20:1", 7, 42)
                .expect_err("old execution state must fail");
            assert!(error.to_string().contains(marker));
        }
    }
}
