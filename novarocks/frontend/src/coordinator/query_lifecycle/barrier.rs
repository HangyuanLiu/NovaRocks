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

use std::sync::Arc;
use std::time::{Duration, Instant};

use novarocks::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};
use novarocks::query_execution::lifecycle::{
    QueryControlAttach, QueryControlEvent, QueryInitBarrier, QueryInitOutcome, QueryInitPlan,
    QueryLifecycleLease,
};

use super::QueryLifecycleTransport;
use super::lease::{
    ActiveSession, AttemptControl, FrontendLifecycleMetrics, FrontendQueryLifecycleLeaseGuard,
    spawn_supervisor,
};
use super::manifest::{MaterializedParticipant, materialize};
use crate::coordinator::query_registry::{ActiveQueryAttemptControl, FrontendQueryRegistry};

#[derive(Clone, Copy)]
pub(crate) struct FrontendQueryLifecycleConfig {
    heartbeat_interval: Duration,
    heartbeat_timeout: Duration,
    init_rpc_timeout: Duration,
    attach_timeout: Duration,
}

impl FrontendQueryLifecycleConfig {
    pub(crate) fn new(
        heartbeat_interval: Duration,
        heartbeat_timeout: Duration,
        init_rpc_timeout: Duration,
        attach_timeout: Duration,
    ) -> Result<Self, DistributedQueryError> {
        if heartbeat_interval.is_zero()
            || heartbeat_timeout.is_zero()
            || init_rpc_timeout.is_zero()
            || attach_timeout.is_zero()
        {
            return Err(contract_error(
                "frontend query lifecycle timeouts must be nonzero",
            ));
        }
        if heartbeat_timeout <= heartbeat_interval {
            return Err(contract_error(
                "frontend query lifecycle heartbeat timeout must exceed its interval",
            ));
        }
        Ok(Self {
            heartbeat_interval,
            heartbeat_timeout,
            init_rpc_timeout,
            attach_timeout,
        })
    }

    pub(super) const fn heartbeat_interval(self) -> Duration {
        self.heartbeat_interval
    }

    pub(super) const fn heartbeat_timeout(self) -> Duration {
        self.heartbeat_timeout
    }

    pub(super) const fn init_rpc_timeout(self) -> Duration {
        self.init_rpc_timeout
    }

    pub(super) const fn attach_timeout(self) -> Duration {
        self.attach_timeout
    }
}

pub(crate) struct FrontendQueryLifecycleBarrier {
    transport: Arc<dyn QueryLifecycleTransport>,
    registry: Arc<FrontendQueryRegistry>,
    config: FrontendQueryLifecycleConfig,
    metrics: Arc<FrontendLifecycleMetrics>,
}

impl FrontendQueryLifecycleBarrier {
    pub(crate) fn new(
        transport: Arc<dyn QueryLifecycleTransport>,
        registry: Arc<FrontendQueryRegistry>,
        config: FrontendQueryLifecycleConfig,
    ) -> Self {
        Self {
            transport,
            registry,
            config,
            metrics: Arc::new(FrontendLifecycleMetrics::default()),
        }
    }
}

impl QueryInitBarrier for FrontendQueryLifecycleBarrier {
    fn initialize_all(
        &self,
        plan: QueryInitPlan,
    ) -> Result<QueryLifecycleLease, DistributedQueryError> {
        let materialized = materialize(plan)?;
        let execution_id = materialized.execution_id;
        let fragment_participants = materialized
            .participants
            .iter()
            .filter(|participant| participant.fragment_participant)
            .count();
        tracing::info!(
            query_id_high = execution_id.query_id().high(),
            query_id_low = execution_id.query_id().low(),
            attempt_id = execution_id.attempt_id().get(),
            participants = materialized.participants.len(),
            fragment_participants,
            service_only_participants = materialized.participants.len() - fragment_participants,
            "frontend query lifecycle attempt created"
        );

        let control = AttemptControl::new(
            execution_id,
            Arc::clone(&self.transport),
            Arc::downgrade(&self.registry),
            self.config,
            Arc::clone(&self.metrics),
        );
        let ownership = materialized
            .participants
            .iter()
            .map(|participant| {
                (
                    participant.target.backend_idx(),
                    participant.target.start_epoch(),
                )
            })
            .collect::<Vec<_>>();
        if let Err(error) = self
            .registry
            .extend_attempt_backend_ownership(execution_id.query_id(), &ownership)
        {
            let message = control.abort_before_ready(error.message().to_string());
            return Err(DistributedQueryError::new(error.kind(), message));
        }
        let active_control: Arc<dyn ActiveQueryAttemptControl> = control.clone();
        let registry_binding = match self
            .registry
            .bind_active_attempt(execution_id, active_control)
        {
            Ok(binding) => binding,
            Err(error) => {
                let message = control.abort_before_ready(error.message().to_string());
                return Err(DistributedQueryError::new(error.kind(), message));
            }
        };
        if !control.is_active() {
            return Err(failed(
                "query lifecycle attempt was cancelled before InitQuery",
            ));
        }
        control.set_attempted(&materialized.participants);
        if !control.is_active() {
            return Err(failed(
                "query lifecycle attempt was cancelled before InitQuery",
            ));
        }

        let init_errors = init_all(
            self.transport.as_ref(),
            &materialized.participants,
            self.config,
            self.metrics.as_ref(),
        );
        if let Some(primary) = init_errors.into_iter().next() {
            let message = control.abort_before_ready(primary);
            return Err(failed(message));
        }
        if !control.is_active() {
            return Err(failed(control.abort_before_ready(
                "query lifecycle attempt was cancelled during InitQuery".to_string(),
            )));
        }

        let attach_errors = attach_all(
            self.transport.as_ref(),
            &materialized.participants,
            execution_id.attempt_id().get(),
            self.config,
            self.metrics.as_ref(),
            control.as_ref(),
        );
        if let Some(primary) = attach_errors.into_iter().next() {
            let message = control.abort_before_ready(primary);
            return Err(failed(message));
        }
        if !control.is_active() {
            return Err(failed(control.abort_before_ready(
                "query lifecycle attempt was cancelled during control attach".to_string(),
            )));
        }

        let supervisor = spawn_supervisor(&control);
        Ok(FrontendQueryLifecycleLeaseGuard::lease(
            control,
            supervisor,
            registry_binding,
        ))
    }
}

fn init_all(
    transport: &dyn QueryLifecycleTransport,
    participants: &[MaterializedParticipant],
    config: FrontendQueryLifecycleConfig,
    metrics: &FrontendLifecycleMetrics,
) -> Vec<String> {
    std::thread::scope(|scope| {
        let handles = participants
            .iter()
            .map(|participant| {
                scope.spawn(move || {
                    let started = Instant::now();
                    let result = init_one(transport, participant, config.init_rpc_timeout());
                    let latency = started.elapsed();
                    match &result {
                        Ok(QueryInitOutcome::Applied) => metrics.observe_init(true, false, latency),
                        Ok(QueryInitOutcome::AlreadyApplied) => {
                            metrics.observe_init(false, true, latency)
                        }
                        Ok(_) | Err(_) => metrics.observe_init(false, false, latency),
                    }
                    tracing::info!(
                        query_id_high = participant.request.manifest().execution_id().query_id().high(),
                        query_id_low = participant.request.manifest().execution_id().query_id().low(),
                        attempt_id = participant.request.manifest().execution_id().attempt_id().get(),
                        backend_id = participant.target.backend_idx(),
                        backend_start_epoch = participant.target.start_epoch(),
                        participant_digest = %hex::encode(participant.digest.as_bytes()),
                        outcome = ?result,
                        latency_micros = latency.as_micros() as u64,
                        "frontend query lifecycle InitQuery completed"
                    );
                    result.map(|_| ())
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .filter_map(|handle| match handle.join() {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error),
                Err(_) => Some("query lifecycle InitQuery worker panicked".to_string()),
            })
            .collect()
    })
}

fn init_one(
    transport: &dyn QueryLifecycleTransport,
    participant: &MaterializedParticipant,
    timeout: Duration,
) -> Result<QueryInitOutcome, String> {
    let first = transport.init_query(participant.target, participant.request.clone(), timeout);
    let ack = match first {
        Ok(ack) => ack,
        Err(error) if error.is_unknown_init_outcome() => transport
            .init_query(participant.target, participant.request.clone(), timeout)
            .map_err(|retry| {
                format!(
                    "backend {} InitQuery retry failed after unknown outcome ({error}): {retry}",
                    participant.target.backend_idx()
                )
            })?,
        Err(error) => {
            return Err(format!(
                "backend {} InitQuery failed: {error}",
                participant.target.backend_idx()
            ));
        }
    };
    if ack.execution_id() != participant.request.manifest().execution_id() {
        return Err(format!(
            "backend {} InitAck execution id mismatch",
            participant.target.backend_idx()
        ));
    }
    if ack.digest() != participant.digest {
        return Err(format!(
            "backend {} InitAck digest mismatch",
            participant.target.backend_idx()
        ));
    }
    if !ack.outcome().is_ready() {
        return Err(format!(
            "backend {} InitQuery rejected with {:?}",
            participant.target.backend_idx(),
            ack.outcome()
        ));
    }
    Ok(ack.outcome())
}

fn attach_all(
    transport: &dyn QueryLifecycleTransport,
    participants: &[MaterializedParticipant],
    frontend_owner_epoch: u64,
    config: FrontendQueryLifecycleConfig,
    metrics: &FrontendLifecycleMetrics,
    control: &AttemptControl,
) -> Vec<String> {
    let outcomes = std::thread::scope(|scope| {
        let handles = participants
            .iter()
            .map(|participant| {
                scope.spawn(move || {
                    let started = Instant::now();
                    let outcome =
                        attach_one(transport, participant, frontend_owner_epoch, config);
                    let latency = started.elapsed();
                    metrics.observe_attach(outcome.is_ok(), latency);
                    tracing::info!(
                        query_id_high = participant.request.manifest().execution_id().query_id().high(),
                        query_id_low = participant.request.manifest().execution_id().query_id().low(),
                        attempt_id = participant.request.manifest().execution_id().attempt_id().get(),
                        backend_id = participant.target.backend_idx(),
                        backend_start_epoch = participant.target.start_epoch(),
                        participant_digest = %hex::encode(participant.digest.as_bytes()),
                        ready = outcome.is_ok(),
                        latency_micros = latency.as_micros() as u64,
                        "frontend query lifecycle control attach completed"
                    );
                    (participant.target.backend_idx(), outcome)
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| {
                handle.join().unwrap_or_else(|_| {
                    (
                        usize::MAX,
                        Err((
                            None,
                            "query lifecycle control attach worker panicked".to_string(),
                        )),
                    )
                })
            })
            .collect::<Vec<_>>()
    });

    let mut errors = Vec::new();
    for (_, outcome) in outcomes {
        match outcome {
            Ok(session) => control.add_session(session),
            Err((session, error)) => {
                if let Some(session) = session {
                    control.add_session(session);
                }
                errors.push(error);
            }
        }
    }
    errors
}

fn attach_one(
    transport: &dyn QueryLifecycleTransport,
    participant: &MaterializedParticipant,
    frontend_owner_epoch: u64,
    config: FrontendQueryLifecycleConfig,
) -> Result<ActiveSession, (Option<ActiveSession>, String)> {
    let attach = QueryControlAttach::new(
        participant.request.manifest().execution_id(),
        participant.digest,
        frontend_owner_epoch,
    )
    .map_err(|error| (None, error.to_string()))?;
    let session = transport
        .attach_control(participant.target, attach, config.attach_timeout())
        .map_err(|error| {
            (
                None,
                format!(
                    "backend {} control attach failed: {error}",
                    participant.target.backend_idx()
                ),
            )
        })?;
    let active = ActiveSession::new(participant.target, participant.digest, session);
    match active.recv(config.attach_timeout()) {
        Ok(QueryControlEvent::ControlReady) => Ok(active),
        Ok(event) => Err((
            Some(active),
            format!(
                "backend {} returned {event:?} before ControlReady",
                participant.target.backend_idx()
            ),
        )),
        Err(error) => Err((
            Some(active),
            format!(
                "backend {} ControlReady failed: {error}",
                participant.target.backend_idx()
            ),
        )),
    }
}

fn contract_error(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, message)
}

fn failed(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::Failed, message)
}
