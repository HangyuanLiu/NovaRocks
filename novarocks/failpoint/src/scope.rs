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

//! Role-bound, finite once grants. Domain owners validate their own targets.

use crate::session::{SessionDeclaration, SessionRole, invalid};
use novarocks_types::{BackendProcessId, FrontendProcessId};
use serde::Serialize;
use std::io;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;
use tokio::sync::Notify;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessBinding {
    Frontend(FrontendProcessId),
    Backend(BackendProcessId),
}

impl ProcessBinding {
    pub fn role(self) -> SessionRole {
        match self {
            Self::Frontend(_) => SessionRole::Fe,
            Self::Backend(_) => SessionRole::Be,
        }
    }
    pub fn process_id(self) -> String {
        match self {
            Self::Frontend(id) => id.to_string(),
            Self::Backend(id) => id.to_string(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CloseCause {
    RoleStopped,
    ExplicitEnd,
    Disconnected,
    HeartbeatLost,
    Deadline,
}

#[derive(Clone, Debug, Serialize)]
pub struct GrantObservation {
    pub domain: String,
    pub grant_id: String,
    pub event: &'static str,
}

struct State {
    closed: Option<CloseCause>,
    claimed: Vec<bool>,
    observations: Vec<GrantObservation>,
}

struct Inner {
    declaration: SessionDeclaration,
    process: ProcessBinding,
    deadline: Instant,
    state: Mutex<State>,
    changed: Condvar,
    notified: Notify,
}

#[derive(Clone)]
pub struct SessionScope {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for SessionScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionScope")
            .field("session_id", &self.inner.declaration.session_id)
            .field("process", &self.inner.process)
            .finish_non_exhaustive()
    }
}

impl SessionScope {
    pub(crate) fn bind(
        declaration: SessionDeclaration,
        process: ProcessBinding,
        deadline: Instant,
    ) -> io::Result<Self> {
        declaration.validate_envelope()?;
        if declaration.role != process.role() {
            return Err(invalid(
                "test-session role does not match the actual Native process",
            ));
        }
        if Instant::now() >= deadline {
            return Err(invalid("test-session expired before role binding"));
        }
        let claimed = vec![false; declaration.domain_grants.len()];
        Ok(Self {
            inner: Arc::new(Inner {
                declaration,
                process,
                deadline,
                state: Mutex::new(State {
                    closed: None,
                    claimed,
                    observations: Vec::new(),
                }),
                changed: Condvar::new(),
                notified: Notify::new(),
            }),
        })
    }
    pub fn process(&self) -> ProcessBinding {
        self.inner.process
    }
    pub fn session_id(&self) -> &str {
        &self.inner.declaration.session_id
    }
    pub fn launch_id(&self) -> &str {
        &self.inner.declaration.launch_id
    }
    pub fn deadline(&self) -> Instant {
        self.inner.deadline
    }
    /// Composition uses names only to reject unrecognized owner domains.
    pub fn domain_names(&self) -> impl Iterator<Item = &str> {
        self.inner
            .declaration
            .domain_grants
            .iter()
            .map(|grant| grant.domain.as_str())
    }
    pub fn grants_for(&self, domain: &str) -> Vec<GrantHandle> {
        self.inner
            .declaration
            .domain_grants
            .iter()
            .enumerate()
            .filter(|(_, grant)| grant.domain == domain)
            .map(|(index, _)| GrantHandle {
                scope: self.clone(),
                index,
            })
            .collect()
    }
    pub fn close(&self, cause: CloseCause) {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.closed.is_none() {
            state.closed = Some(cause);
        }
        drop(state);
        self.inner.changed.notify_all();
        self.inner.notified.notify_waiters();
    }
    pub fn is_active_at(&self, now: Instant) -> bool {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.closed.is_none() && now >= self.inner.deadline {
            state.closed = Some(CloseCause::Deadline);
            self.inner.changed.notify_all();
            self.inner.notified.notify_waiters();
        }
        state.closed.is_none()
    }
    pub fn is_active(&self) -> bool {
        self.is_active_at(Instant::now())
    }
    pub fn close_cause(&self) -> Option<CloseCause> {
        let _ = self.is_active();
        self.inner
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .closed
    }
    pub async fn cancelled(&self) {
        loop {
            let notified = self.inner.notified.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.is_active() {
                return;
            }
            tokio::select! {
                _ = notified => {},
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(self.inner.deadline)) => {
                    self.close(CloseCause::Deadline);
                }
            }
        }
    }
    pub fn wait_closed(&self) {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        while state.closed.is_none() {
            let remaining = self
                .inner
                .deadline
                .saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                state.closed = Some(CloseCause::Deadline);
                break;
            }
            state = self
                .inner
                .changed
                .wait_timeout(state, remaining)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
    }
    pub fn observations(&self) -> Vec<GrantObservation> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .observations
            .clone()
    }
}

/// Read-only cancellation and identity; it exposes no other domain grants.
#[derive(Clone, Debug)]
pub struct ControlCancellation {
    scope: SessionScope,
}

impl ControlCancellation {
    pub fn process(&self) -> ProcessBinding {
        self.scope.process()
    }
    pub fn session_id(&self) -> &str {
        self.scope.session_id()
    }
    pub fn launch_id(&self) -> &str {
        self.scope.launch_id()
    }
    pub fn deadline(&self) -> Instant {
        self.scope.deadline()
    }
    pub fn is_active(&self) -> bool {
        self.scope.is_active()
    }
    pub async fn cancelled(&self) {
        self.scope.cancelled().await
    }
    pub fn wait_closed(&self) {
        self.scope.wait_closed()
    }
}

#[derive(Clone, Debug)]
pub struct GrantHandle {
    scope: SessionScope,
    index: usize,
}

impl GrantHandle {
    pub fn parameters(&self) -> &serde_json::Value {
        &self.scope.inner.declaration.domain_grants[self.index].parameters
    }
    pub fn grant_id(&self) -> &str {
        &self.scope.inner.declaration.domain_grants[self.index].grant_id
    }
    pub fn cancellation(&self) -> ControlCancellation {
        ControlCancellation {
            scope: self.scope.clone(),
        }
    }
    /// Domain target validation precedes this claim. Closure and claim linearize together.
    pub fn claim_once(&self, exact_process: ProcessBinding) -> io::Result<ActionClaim> {
        if exact_process != self.scope.process() {
            return Err(invalid("test grant belongs to another Native process"));
        }
        let mut state = self
            .scope
            .inner
            .state
            .lock()
            .map_err(|_| invalid("test-session state lock failed"))?;
        if state.closed.is_none() && Instant::now() >= self.scope.deadline() {
            state.closed = Some(CloseCause::Deadline);
        }
        if state.closed.is_some() {
            return Err(invalid("test-session is closed"));
        }
        if std::mem::replace(&mut state.claimed[self.index], true) {
            return Err(invalid("test-session grant was already claimed"));
        }
        let grant = &self.scope.inner.declaration.domain_grants[self.index];
        state.observations.push(GrantObservation {
            domain: grant.domain.clone(),
            grant_id: grant.grant_id.clone(),
            event: "claimed",
        });
        Ok(ActionClaim {
            grant: Some(self.clone()),
            hit_recorded: false,
        })
    }
}

#[derive(Debug)]
pub struct ActionClaim {
    grant: Option<GrantHandle>,
    hit_recorded: bool,
}

impl ActionClaim {
    /// Observation only: the domain records this after its actual action is reached.
    pub fn record_hit(&mut self) -> io::Result<()> {
        if self.hit_recorded {
            return Err(invalid("test action hit was already recorded"));
        }
        let grant = self.grant.as_ref().expect("live action claim");
        let declaration = &grant.scope.inner.declaration.domain_grants[grant.index];
        grant
            .scope
            .inner
            .state
            .lock()
            .map_err(|_| invalid("test-session state lock failed"))?
            .observations
            .push(GrantObservation {
                domain: declaration.domain.clone(),
                grant_id: declaration.grant_id.clone(),
                event: "hit",
            });
        self.hit_recorded = true;
        Ok(())
    }
    pub fn cancellation(&self) -> ControlCancellation {
        self.grant
            .as_ref()
            .expect("live action claim")
            .cancellation()
    }
}

impl Drop for ActionClaim {
    fn drop(&mut self) {
        if let Some(grant) = self.grant.take() {
            let declaration = &grant.scope.inner.declaration.domain_grants[grant.index];
            let mut state = grant
                .scope
                .inner
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            state.observations.push(GrantObservation {
                domain: declaration.domain.clone(),
                grant_id: declaration.grant_id.clone(),
                event: "claim-released",
            });
        }
    }
}
