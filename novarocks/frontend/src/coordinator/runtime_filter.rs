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

use std::net::SocketAddr;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use novarocks::query_execution::artifact::{
    RuntimeFilterAbortEnvelope, RuntimeFilterDeploymentDispatcher, RuntimeFilterInstallBarrier,
    RuntimeFilterInstallLease, RuntimeFilterInstallLeaseGuard, RuntimeFilterInstallPlan,
};
use novarocks::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};

pub(crate) struct FrontendRuntimeFilterDeployment {
    dispatcher: Arc<dyn RuntimeFilterDeploymentDispatcher>,
    #[cfg(test)]
    barrier_calls: Arc<AtomicU64>,
}

impl FrontendRuntimeFilterDeployment {
    #[cfg(not(test))]
    pub(crate) fn new(dispatcher: Arc<dyn RuntimeFilterDeploymentDispatcher>) -> Self {
        Self {
            dispatcher,
            #[cfg(test)]
            barrier_calls: Arc::new(AtomicU64::new(0)),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_barrier_counter(
        dispatcher: Arc<dyn RuntimeFilterDeploymentDispatcher>,
        barrier_calls: Arc<AtomicU64>,
    ) -> Self {
        Self {
            dispatcher,
            barrier_calls,
        }
    }
}

impl RuntimeFilterInstallBarrier for FrontendRuntimeFilterDeployment {
    fn install_all(
        &self,
        plan: RuntimeFilterInstallPlan,
    ) -> Result<RuntimeFilterInstallLease, DistributedQueryError> {
        #[cfg(test)]
        self.barrier_calls.fetch_add(1, Ordering::SeqCst);
        let mut installs = Vec::with_capacity(plan.participant_count());
        let mut rollback = Vec::with_capacity(plan.participant_count());
        for participant in plan.into_participants() {
            let backend_idx = participant.backend_idx();
            let endpoint = participant.endpoint();
            let participant_id = participant.participant_id();
            let deadline = participant.deadline();
            let (install, abort) = participant.into_envelopes();
            installs.push(InstallTarget {
                backend_idx,
                endpoint,
                participant_id,
                deadline,
                envelope: install,
            });
            rollback.push(AbortTarget {
                backend_idx,
                endpoint,
                participant_id,
                deadline,
                envelope: abort,
            });
        }

        let install_errors = install_targets(self.dispatcher.as_ref(), installs);
        if let Some((participant_id, backend_idx, error)) = install_errors.first() {
            let mut primary = format!(
                "runtime-filter install failed for participant {participant_id} on backend \
                 {backend_idx}: {error}"
            );
            if install_errors.len() > 1 {
                let additional = install_errors[1..]
                    .iter()
                    .map(|(participant_id, backend_idx, error)| {
                        format!("participant {participant_id} on backend {backend_idx}: {error}")
                    })
                    .collect::<Vec<_>>()
                    .join("; ");
                primary.push_str("; additional runtime-filter install failures: ");
                primary.push_str(&additional);
            }
            let message = abort_targets(self.dispatcher.as_ref(), rollback, primary);
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::Failed,
                message,
            ));
        }

        Ok(RuntimeFilterInstallLease::new(Box::new(
            FrontendRuntimeFilterLeaseGuard {
                dispatcher: Arc::clone(&self.dispatcher),
                rollback: Some(rollback),
            },
        )))
    }
}

struct InstallTarget {
    backend_idx: usize,
    endpoint: SocketAddr,
    participant_id: u32,
    deadline: Duration,
    envelope: novarocks::query_execution::artifact::RuntimeFilterInstallEnvelope,
}

struct AbortTarget {
    backend_idx: usize,
    endpoint: SocketAddr,
    participant_id: u32,
    deadline: Duration,
    envelope: RuntimeFilterAbortEnvelope,
}

struct FrontendRuntimeFilterLeaseGuard {
    dispatcher: Arc<dyn RuntimeFilterDeploymentDispatcher>,
    rollback: Option<Vec<AbortTarget>>,
}

impl RuntimeFilterInstallLeaseGuard for FrontendRuntimeFilterLeaseGuard {
    fn release(mut self: Box<Self>) {
        self.rollback.take();
    }

    fn abort_preserving(mut self: Box<Self>, primary_error: String) -> String {
        abort_targets(
            self.dispatcher.as_ref(),
            self.rollback.take().unwrap_or_default(),
            primary_error,
        )
    }
}

impl Drop for FrontendRuntimeFilterLeaseGuard {
    fn drop(&mut self) {
        if let Some(rollback) = self.rollback.take() {
            let _ = abort_targets(
                self.dispatcher.as_ref(),
                rollback,
                "runtime-filter deployment lease dropped before release".to_string(),
            );
        }
    }
}

fn install_targets(
    dispatcher: &dyn RuntimeFilterDeploymentDispatcher,
    installs: Vec<InstallTarget>,
) -> Vec<(u32, usize, String)> {
    std::thread::scope(|scope| {
        let handles = installs
            .into_iter()
            .map(|target| {
                let participant_id = target.participant_id;
                let backend_idx = target.backend_idx;
                let handle = scope.spawn(move || {
                    dispatcher.install(
                        target.backend_idx,
                        target.endpoint,
                        target.participant_id,
                        target.deadline,
                        target.envelope,
                    )
                });
                (participant_id, backend_idx, handle)
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .filter_map(|(participant_id, backend_idx, handle)| {
                let result = handle
                    .join()
                    .unwrap_or_else(|_| Err("runtime-filter install worker panicked".to_string()));
                result
                    .err()
                    .map(|error| (participant_id, backend_idx, error))
            })
            .collect()
    })
}

fn abort_targets(
    dispatcher: &dyn RuntimeFilterDeploymentDispatcher,
    rollback: Vec<AbortTarget>,
    primary_error: String,
) -> String {
    let abort_errors = std::thread::scope(|scope| {
        let handles = rollback
            .into_iter()
            .map(|target| {
                let participant_id = target.participant_id;
                let backend_idx = target.backend_idx;
                let handle = scope.spawn(move || {
                    dispatcher.abort(
                        target.backend_idx,
                        target.endpoint,
                        target.participant_id,
                        target.deadline,
                        target.envelope,
                    )
                });
                (participant_id, backend_idx, handle)
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .filter_map(|(participant_id, backend_idx, handle)| {
                let result = handle
                    .join()
                    .unwrap_or_else(|_| Err("runtime-filter abort worker panicked".to_string()));
                result.err().map(|error| {
                    format!("participant {participant_id} on backend {backend_idx}: {error}")
                })
            })
            .collect::<Vec<_>>()
    });
    if abort_errors.is_empty() {
        primary_error
    } else {
        format!(
            "{primary_error}; runtime-filter rollback failed: {}",
            abort_errors.join("; ")
        )
    }
}
