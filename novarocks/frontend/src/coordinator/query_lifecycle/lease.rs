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

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use novarocks::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};
use novarocks::query_execution::lifecycle::metrics::FrontendQueryLifecycleMetricsSnapshot;
use novarocks::query_execution::lifecycle::{
    ParticipantManifestDigest, QueryAbortRequest, QueryControlCommand, QueryControlEvent,
    QueryExecutionId, QueryLifecycleLease, QueryLifecycleLeaseGuard, QueryTerminationReason,
};

use super::barrier::FrontendQueryLifecycleConfig;
use super::manifest::MaterializedParticipant;
use super::{QueryControlSession, QueryLifecycleTarget, QueryLifecycleTransport};
use crate::coordinator::query_registry::ActiveQueryAttemptBinding;
use crate::coordinator::query_registry::{ActiveQueryAttemptControl, FrontendQueryRegistry};

const ACTIVE: u8 = 0;
const ABORTED: u8 = 1;
const FINALIZING: u8 = 2;
const FINALIZED: u8 = 3;

#[derive(Clone)]
pub(super) struct ActiveSession {
    pub target: QueryLifecycleTarget,
    pub digest: ParticipantManifestDigest,
    pub session: Arc<dyn QueryControlSession>,
    recv_gate: Arc<Mutex<()>>,
}

impl ActiveSession {
    pub fn new(
        target: QueryLifecycleTarget,
        digest: ParticipantManifestDigest,
        session: Arc<dyn QueryControlSession>,
    ) -> Self {
        Self {
            target,
            digest,
            session,
            recv_gate: Arc::new(Mutex::new(())),
        }
    }

    pub(super) fn recv(
        &self,
        timeout: Duration,
    ) -> Result<QueryControlEvent, super::QueryLifecycleTransportError> {
        let _recv = self.recv_gate.lock().expect("query control receive gate");
        self.session.recv_timeout(timeout)
    }
}

#[derive(Default)]
pub(super) struct FrontendLifecycleMetrics {
    snapshot: Mutex<FrontendQueryLifecycleMetricsSnapshot>,
}

impl FrontendLifecycleMetrics {
    pub fn attempt_created(&self) {
        self.update(|snapshot| snapshot.active_attempts += 1);
    }

    pub fn attempt_terminated(&self) {
        self.update(|snapshot| {
            snapshot.active_attempts = snapshot.active_attempts.saturating_sub(1);
        });
    }

    pub fn observe_init(&self, applied: bool, idempotent: bool, latency: Duration) {
        self.update(|snapshot| {
            if applied {
                snapshot.init_applied += 1;
            } else if idempotent {
                snapshot.init_idempotent += 1;
            } else {
                snapshot.init_failed += 1;
            }
            snapshot.init_latency_micros_total += latency.as_micros() as u64;
            snapshot.init_latency_samples += 1;
        });
    }

    pub fn observe_attach(&self, ready: bool, latency: Duration) {
        self.update(|snapshot| {
            snapshot.control_ready += u64::from(ready);
            snapshot.attach_latency_micros_total += latency.as_micros() as u64;
            snapshot.attach_latency_samples += 1;
        });
    }

    pub fn heartbeat_timeout(&self) {
        self.update(|snapshot| snapshot.heartbeat_timeouts += 1);
    }

    pub fn coordinator_lost(&self) {
        self.update(|snapshot| snapshot.coordinator_lost += 1);
    }

    fn update(&self, update: impl FnOnce(&mut FrontendQueryLifecycleMetricsSnapshot)) {
        let snapshot = {
            let mut snapshot = self.snapshot.lock().expect("frontend lifecycle metrics");
            update(&mut snapshot);
            *snapshot
        };
        novarocks::service::publish_frontend_query_lifecycle_metrics(snapshot);
    }
}

pub(super) struct AttemptControl {
    execution_id: QueryExecutionId,
    transport: Arc<dyn QueryLifecycleTransport>,
    registry: Weak<FrontendQueryRegistry>,
    config: FrontendQueryLifecycleConfig,
    attempted: Mutex<BTreeMap<usize, MaterializedParticipant>>,
    sessions: Mutex<BTreeMap<usize, ActiveSession>>,
    state: AtomicU8,
    primary_error: Mutex<Option<String>>,
    stop: (Mutex<bool>, Condvar),
    metrics: Arc<FrontendLifecycleMetrics>,
}

impl AttemptControl {
    pub fn new(
        execution_id: QueryExecutionId,
        transport: Arc<dyn QueryLifecycleTransport>,
        registry: Weak<FrontendQueryRegistry>,
        config: FrontendQueryLifecycleConfig,
        metrics: Arc<FrontendLifecycleMetrics>,
    ) -> Arc<Self> {
        metrics.attempt_created();
        Arc::new(Self {
            execution_id,
            transport,
            registry,
            config,
            attempted: Mutex::new(BTreeMap::new()),
            sessions: Mutex::new(BTreeMap::new()),
            state: AtomicU8::new(ACTIVE),
            primary_error: Mutex::new(None),
            stop: (Mutex::new(false), Condvar::new()),
            metrics,
        })
    }

    pub fn is_active(&self) -> bool {
        self.state.load(Ordering::Acquire) == ACTIVE
    }

    pub fn set_attempted(&self, participants: &[MaterializedParticipant]) {
        let mut attempted = self.attempted.lock().expect("attempted participant set");
        attempted.extend(
            participants
                .iter()
                .cloned()
                .map(|participant| (participant.target.backend_idx(), participant)),
        );
    }

    pub fn add_session(&self, session: ActiveSession) {
        self.sessions
            .lock()
            .expect("active query control sessions")
            .insert(session.target.backend_idx(), session);
    }

    pub fn sessions(&self) -> Vec<ActiveSession> {
        self.sessions
            .lock()
            .expect("active query control sessions")
            .values()
            .cloned()
            .collect()
    }

    pub fn abort_before_ready(&self, primary_error: String) -> String {
        self.abort(primary_error, true)
    }

    pub fn abort_preserving(&self, primary_error: String) -> String {
        self.abort(primary_error, false)
    }

    fn abort(&self, primary_error: String, force_unary: bool) -> String {
        if self
            .state
            .compare_exchange(ACTIVE, ABORTED, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return self
                .primary_error
                .lock()
                .expect("query lifecycle primary error")
                .clone()
                .unwrap_or(primary_error);
        }
        *self
            .primary_error
            .lock()
            .expect("query lifecycle primary error") = Some(primary_error.clone());
        self.stop_supervisor();
        self.metrics.attempt_terminated();
        tracing::warn!(
            query_id_high = self.execution_id.query_id().high(),
            query_id_low = self.execution_id.query_id().low(),
            attempt_id = self.execution_id.attempt_id().get(),
            reason = %primary_error,
            "frontend query lifecycle abort"
        );
        let errors = self.abort_targets(force_unary, &primary_error);
        let enriched = if errors.is_empty() {
            primary_error
        } else {
            format!(
                "{primary_error}; query lifecycle rollback failed: {}",
                errors.join("; ")
            )
        };
        *self
            .primary_error
            .lock()
            .expect("query lifecycle primary error") = Some(enriched.clone());
        enriched
    }

    fn abort_targets(&self, force_unary: bool, reason: &str) -> Vec<String> {
        let attempted = self
            .attempted
            .lock()
            .expect("attempted participant set")
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let sessions = self
            .sessions
            .lock()
            .expect("active query control sessions")
            .clone();
        std::thread::scope(|scope| {
            let handles = attempted
                .into_iter()
                .map(|participant| {
                    let session = sessions.get(&participant.target.backend_idx()).cloned();
                    scope.spawn(move || {
                        self.abort_target(&participant, session.as_ref(), force_unary, reason)
                    })
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .filter_map(|handle| match handle.join() {
                    Ok(Ok(())) => None,
                    Ok(Err(error)) => Some(error),
                    Err(_) => Some("query lifecycle abort worker panicked".to_string()),
                })
                .collect()
        })
    }

    fn abort_target(
        &self,
        participant: &MaterializedParticipant,
        session: Option<&ActiveSession>,
        force_unary: bool,
        reason: &str,
    ) -> Result<(), String> {
        if !force_unary && let Some(session) = session {
            let stream_result = (|| {
                session
                    .session
                    .send(QueryControlCommand::Abort {
                        reason: reason.to_string(),
                    })
                    .map_err(|error| error.to_string())?;
                match session
                    .recv(self.config.attach_timeout())
                    .map_err(|error| error.to_string())?
                {
                    QueryControlEvent::TerminationAccepted { .. } => Ok(()),
                    event => Err(format!(
                        "backend {} returned {event:?} after stream abort",
                        participant.target.backend_idx()
                    )),
                }
            })();
            if stream_result.is_ok() {
                return Ok(());
            }
        }

        let request =
            QueryAbortRequest::new(self.execution_id, participant.digest, reason.to_string())
                .map_err(|error| error.to_string())?;
        let ack = self
            .transport
            .abort_query(participant.target, request, self.config.attach_timeout())
            .map_err(|error| {
                format!(
                    "backend {} unary abort: {error}",
                    participant.target.backend_idx()
                )
            })?;
        if ack.execution_id() != self.execution_id {
            return Err(format!(
                "backend {} unary abort acknowledgement execution id mismatch",
                participant.target.backend_idx()
            ));
        }
        if ack.accepted_reason() != QueryTerminationReason::CoordinatorAbort {
            return Err(format!(
                "backend {} unary abort acknowledgement accepted {:?}",
                participant.target.backend_idx(),
                ack.accepted_reason()
            ));
        }
        Ok(())
    }

    pub fn stop_supervisor(&self) {
        let mut stopped = self.stop.0.lock().expect("query lifecycle stop lock");
        *stopped = true;
        self.stop.1.notify_all();
    }

    fn wait_heartbeat_interval(&self) -> bool {
        let stopped = self.stop.0.lock().expect("query lifecycle stop lock");
        if *stopped {
            return false;
        }
        let (stopped, _) = self
            .stop
            .1
            .wait_timeout(stopped, self.config.heartbeat_interval())
            .expect("query lifecycle heartbeat wait");
        !*stopped
    }

    fn supervisor_failed(&self, reason: String, heartbeat_timeout: bool) {
        if heartbeat_timeout {
            self.metrics.heartbeat_timeout();
        } else {
            self.metrics.coordinator_lost();
        }
        if let Some(registry) = self.registry.upgrade() {
            let _ = registry.latch_failure_and_cancel(self.execution_id.query_id(), reason);
        } else {
            let _ = self.abort_preserving(reason);
        }
    }

    pub fn finalize(&self) -> Result<(), DistributedQueryError> {
        self.state
            .compare_exchange(ACTIVE, FINALIZING, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                failed(
                    self.primary_error
                        .lock()
                        .expect("query lifecycle primary error")
                        .clone()
                        .unwrap_or_else(|| {
                            "query lifecycle attempt is already terminal".to_string()
                        }),
                )
            })?;
        self.stop_supervisor();
        let sessions = self.sessions();
        let errors = std::thread::scope(|scope| {
            let handles = sessions
                .into_iter()
                .map(|session| {
                    scope.spawn(move || {
                        session
                            .session
                            .send(QueryControlCommand::Finalize)
                            .map_err(|error| error.to_string())?;
                        match session
                            .recv(self.config.attach_timeout())
                            .map_err(|error| error.to_string())?
                        {
                            QueryControlEvent::TerminationAccepted {
                                reason: QueryTerminationReason::CoordinatorFinalize,
                            } => Ok(()),
                            event => Err(format!(
                                "backend {} returned {event:?} after finalize",
                                session.target.backend_idx()
                            )),
                        }
                    })
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .filter_map(|handle| match handle.join() {
                    Ok(Ok(())) => None,
                    Ok(Err(error)) => Some(error),
                    Err(_) => Some("query lifecycle finalize worker panicked".to_string()),
                })
                .collect::<Vec<_>>()
        });
        self.metrics.attempt_terminated();
        if errors.is_empty() {
            self.state.store(FINALIZED, Ordering::Release);
            tracing::info!(
                query_id_high = self.execution_id.query_id().high(),
                query_id_low = self.execution_id.query_id().low(),
                attempt_id = self.execution_id.attempt_id().get(),
                "frontend query lifecycle finalized"
            );
            Ok(())
        } else {
            self.state.store(ABORTED, Ordering::Release);
            let primary = format!("query lifecycle finalize failed: {}", errors.join("; "));
            let cleanup = self.abort_targets(true, &primary);
            let message = if cleanup.is_empty() {
                primary
            } else {
                format!(
                    "{primary}; query lifecycle rollback failed: {}",
                    cleanup.join("; ")
                )
            };
            *self
                .primary_error
                .lock()
                .expect("query lifecycle primary error") = Some(message.clone());
            Err(failed(message))
        }
    }
}

impl ActiveQueryAttemptControl for AttemptControl {
    fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    fn request_abort(&self, reason: String) {
        let enriched = self.abort_preserving(reason);
        if let Some(registry) = self.registry.upgrade() {
            let _ = registry.preserve_failure_context(self.execution_id.query_id(), enriched);
        }
    }
}

pub(super) fn spawn_supervisor(control: &Arc<AttemptControl>) -> JoinHandle<()> {
    let weak = Arc::downgrade(control);
    std::thread::Builder::new()
        .name(format!(
            "query-control-{}/{}-{}",
            control.execution_id.query_id().high(),
            control.execution_id.query_id().low(),
            control.execution_id.attempt_id().get()
        ))
        .spawn(move || heartbeat_supervisor(weak))
        .expect("spawn frontend query lifecycle supervisor")
}

fn heartbeat_supervisor(control: Weak<AttemptControl>) {
    let Some(control) = control.upgrade() else {
        return;
    };
    let started = Instant::now();
    let mut sequence = 0u64;
    while control.wait_heartbeat_interval() {
        sequence = match sequence.checked_add(1) {
            Some(sequence) => sequence,
            None => {
                control.supervisor_failed(
                    "query lifecycle heartbeat sequence exhausted".to_string(),
                    false,
                );
                return;
            }
        };
        let sessions = control.sessions();
        for session in &sessions {
            if let Err(error) = session.session.send(QueryControlCommand::Heartbeat {
                sequence,
                sent_mono_ns: started.elapsed().as_nanos() as u64,
            }) {
                control.supervisor_failed(
                    format!(
                        "query lifecycle control stream failed for backend {} digest {}: {error}",
                        session.target.backend_idx(),
                        hex::encode(session.digest.as_bytes())
                    ),
                    false,
                );
                return;
            }
        }
        for session in &sessions {
            match session.recv(control.config.heartbeat_timeout()) {
                Ok(QueryControlEvent::HeartbeatAck {
                    sequence: ack_sequence,
                }) if ack_sequence == sequence => {}
                Ok(QueryControlEvent::LocalFailure { code, detail }) => {
                    control.supervisor_failed(
                        format!(
                            "query lifecycle local failure on backend {} ({code}): {detail}",
                            session.target.backend_idx()
                        ),
                        false,
                    );
                    return;
                }
                Ok(event) => {
                    control.supervisor_failed(
                        format!(
                            "query lifecycle invalid heartbeat event from backend {}: {event:?}",
                            session.target.backend_idx()
                        ),
                        false,
                    );
                    return;
                }
                Err(error) => {
                    let timeout = matches!(
                        error.kind(),
                        super::QueryLifecycleTransportErrorKind::DeadlineExceeded
                    );
                    let failure = if timeout {
                        format!(
                            "query lifecycle heartbeat timeout on backend {} digest {}",
                            session.target.backend_idx(),
                            hex::encode(session.digest.as_bytes())
                        )
                    } else {
                        format!(
                            "query lifecycle control stream lost on backend {} digest {}: {error}",
                            session.target.backend_idx(),
                            hex::encode(session.digest.as_bytes())
                        )
                    };
                    control.supervisor_failed(failure, timeout);
                    return;
                }
            }
        }
    }
}

pub(super) struct FrontendQueryLifecycleLeaseGuard {
    control: Arc<AttemptControl>,
    supervisor: Option<JoinHandle<()>>,
    _registry_binding: ActiveQueryAttemptBinding,
}

impl FrontendQueryLifecycleLeaseGuard {
    pub fn lease(
        control: Arc<AttemptControl>,
        supervisor: JoinHandle<()>,
        registry_binding: ActiveQueryAttemptBinding,
    ) -> QueryLifecycleLease {
        QueryLifecycleLease::new(Box::new(Self {
            control,
            supervisor: Some(supervisor),
            _registry_binding: registry_binding,
        }))
    }

    fn stop_and_join(&mut self) {
        self.control.stop_supervisor();
        let Some(supervisor) = self.supervisor.take() else {
            return;
        };
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        std::thread::spawn(move || {
            let _ = supervisor.join();
            let _ = done_tx.send(());
        });
        let bound = self
            .control
            .config
            .heartbeat_timeout()
            .saturating_add(self.control.config.attach_timeout());
        let _ = done_rx.recv_timeout(bound.max(Duration::from_millis(1)));
    }
}

impl QueryLifecycleLeaseGuard for FrontendQueryLifecycleLeaseGuard {
    fn finalize(mut self: Box<Self>) -> Result<(), DistributedQueryError> {
        self.stop_and_join();
        self.control.finalize()
    }

    fn abort_preserving(mut self: Box<Self>, primary_error: String) -> String {
        self.stop_and_join();
        self.control.abort_preserving(primary_error)
    }
}

impl Drop for FrontendQueryLifecycleLeaseGuard {
    fn drop(&mut self) {
        self.stop_and_join();
        if self.control.is_active() {
            let _ = self
                .control
                .abort_preserving("query lifecycle lease dropped before finalize".to_string());
        }
    }
}

fn failed(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::Failed, message)
}
