// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Fixed result-owner capabilities. A position covers one complete declared
//! backing envelope; it does not reserve allocator bytes or imply reclamation.
//! Product admission commits this grant together with the computation permit.

use crate::{
    WorkError, WorkId, WorkScope, WorkloadControl,
    scope::{Inner, State},
};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResultWindowClass {
    Client,
    Local,
    Internal,
    Closing,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResultClosingCut {
    AcceptedCancellation,
    OriginatingFailure,
}
impl ResultWindowClass {
    const fn index(self) -> usize {
        match self {
            Self::Client => 0,
            Self::Local => 1,
            Self::Internal => 2,
            Self::Closing => 3,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResultCapacityConfig {
    pub client_compute_positions: usize,
    pub client_short_tail_positions: usize,
    pub supported_cancel_burst: u64,
    pub sustained_cancels_per_second: u64,
    pub short_tail_exit_millis: u64,
    pub positions: [usize; 4],
    pub all_objects_bytes: [u64; 4],
}
impl ResultCapacityConfig {
    pub const V1: Self = Self {
        client_compute_positions: 256,
        client_short_tail_positions: 64,
        supported_cancel_burst: 32,
        sustained_cancels_per_second: 16,
        short_tail_exit_millis: 2000,
        positions: [320, 16, 4, 64],
        all_objects_bytes: [
            8 * 1024 * 1024,
            96 * 1024 * 1024,
            1024 * 1024 * 1024,
            8 * 1024 * 1024,
        ],
    };
    pub fn validate(self) -> Result<Self, WorkError> {
        let tail = self
            .sustained_cancels_per_second
            .checked_mul(self.short_tail_exit_millis)
            .and_then(|value| value.checked_add(999))
            .map(|value| value / 1000)
            .and_then(|value| value.checked_add(self.supported_cancel_burst))
            .ok_or(WorkError::ArithmeticOverflow)?;
        let full = self
            .client_compute_positions
            .checked_add(self.client_short_tail_positions)
            .ok_or(WorkError::ArithmeticOverflow)?;
        if self.client_compute_positions == 0
            || self.short_tail_exit_millis == 0
            || tail > self.client_short_tail_positions as u64
            || full > self.positions[0]
        {
            return Err(WorkError::InvalidConfig(
                "client result computation and short-tail capacity",
            ));
        }
        let mut total = 0u64;
        for (positions, bytes) in self.positions.into_iter().zip(self.all_objects_bytes) {
            if positions == 0 || positions > 1_000_000 || bytes == 0 {
                return Err(WorkError::InvalidConfig("result owner capacity"));
            }
            total = total
                .checked_add(
                    bytes
                        .checked_mul(positions as u64)
                        .ok_or(WorkError::ArithmeticOverflow)?,
                )
                .ok_or(WorkError::ArithmeticOverflow)?;
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ResultCapacitySnapshot {
    pub held_positions: [usize; 4],
}

#[derive(Clone)]
pub struct ResultCapacityHandle {
    inner: Arc<Inner>,
}
impl WorkloadControl {
    /// Host composition installs the complete profile once, before readiness.
    /// Existing callers remain unsupported until they explicitly install it.
    pub fn configure_result_capacity(
        &self,
        config: ResultCapacityConfig,
    ) -> Result<ResultCapacityHandle, WorkError> {
        let config = config.validate()?;
        if self.inner.config.query_concurrency_limit > config.client_compute_positions {
            return Err(WorkError::InvalidConfig(
                "result capacity does not cover computation admission",
            ));
        }
        self.inner.update(|state| {
            if state.ready
                || state.closed
                || !state.nodes.is_empty()
                || state.result_capacity.is_some()
            {
                return Err(WorkError::Conflict);
            }
            state.result_capacity = Some(config);
            Ok(ResultCapacityHandle {
                inner: Arc::clone(&self.inner),
            })
        })
    }
}
impl ResultCapacityHandle {
    pub fn snapshot(&self) -> ResultCapacitySnapshot {
        self.inner.state.lock().unwrap().result_windows
    }

    /// Nonblocking complete-position acquisition. Queue admission invokes the
    /// same reservation operation under its one authority transaction. A
    /// Closing grant may only be requested after an accepted cancellation or
    /// originating failure cut; it must never host a normal slow response.
    pub fn try_acquire(
        &self,
        scope: &WorkScope,
        class: ResultWindowClass,
    ) -> Result<ResultWindowGrant, WorkError> {
        if !Arc::ptr_eq(&self.inner, &scope.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        if class == ResultWindowClass::Closing {
            return Err(WorkError::Conflict);
        }
        self.inner
            .update(|state| reserve_window(state, scope, class))
    }
    pub fn try_acquire_closing(
        &self,
        scope: &WorkScope,
        cut: ResultClosingCut,
    ) -> Result<ResultWindowGrant, WorkError> {
        if !Arc::ptr_eq(&self.inner, &scope.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        self.inner.update(|state| {
            let node = state.nodes.get(&scope.id).ok_or(WorkError::Released)?;
            if cut == ResultClosingCut::AcceptedCancellation && node.cancellation.reason().is_none()
            {
                return Err(WorkError::Conflict);
            }
            let mut grant = reserve_window(state, scope, ResultWindowClass::Closing)?;
            grant.closing_cut = Some(cut);
            Ok(grant)
        })
    }
}

pub(crate) fn reserve_window(
    state: &mut State,
    scope: &WorkScope,
    class: ResultWindowClass,
) -> Result<ResultWindowGrant, WorkError> {
    if state.closed {
        return Err(WorkError::Closed);
    }
    let config = state.result_capacity.ok_or(WorkError::NotReady)?;
    let index = class.index();
    let node = state.nodes.get(&scope.id).ok_or(WorkError::Released)?;
    // Closing keeps the original responsibility after cancellation. It does
    // not admit new computation and must not clear its cancellation reason.
    if class != ResultWindowClass::Closing {
        node.check()?;
    }
    if state.result_windows.held_positions[index] >= config.positions[index] {
        return Err(WorkError::Capacity("complete result window"));
    }
    let holders = node
        .resource_holders
        .checked_add(1)
        .ok_or(WorkError::ArithmeticOverflow)?;
    state.nodes.get_mut(&scope.id).unwrap().resource_holders = holders;
    state
        .nodes
        .get_mut(&scope.id)
        .unwrap()
        .result_windows
        .held_positions[index] += 1;
    state.result_windows.held_positions[index] += 1;
    Ok(ResultWindowGrant {
        holder: Arc::new(WindowHolder {
            scope: scope.clone(),
            class,
            all_objects_bytes: config.all_objects_bytes[index],
        }),
        closing_cut: None,
    })
}

struct WindowHolder {
    scope: WorkScope,
    class: ResultWindowClass,
    all_objects_bytes: u64,
}
impl Drop for WindowHolder {
    fn drop(&mut self) {
        self.scope.inner.update(|state| {
            state.result_windows.held_positions[self.class.index()] -= 1;
            state
                .nodes
                .get_mut(&self.scope.id)
                .expect("window retains its responsibility")
                .resource_holders -= 1;
            state
                .nodes
                .get_mut(&self.scope.id)
                .unwrap()
                .result_windows
                .held_positions[self.class.index()] -= 1;
            state.collect(self.scope.id);
        });
    }
}

/// A unique owning grant, retained through fetch/codec/transport actual exit.
/// Moving a writer into closing requires a separately acquired Closing grant.
/// Dropping a timeout or JoinHandle is not evidence that its aliases exited.
#[must_use = "every result backing and physical alias must retain its grant"]
pub struct ResultWindowGrant {
    holder: Arc<WindowHolder>,
    closing_cut: Option<ResultClosingCut>,
}
impl ResultWindowGrant {
    pub fn scope_id(&self) -> WorkId {
        self.holder.scope.id()
    }
    pub fn class(&self) -> ResultWindowClass {
        self.holder.class
    }
    pub fn closing_cut(&self) -> Option<ResultClosingCut> {
        self.closing_cut
    }
    pub fn has_retained_aliases(&self) -> bool {
        Arc::strong_count(&self.holder) != 1
    }
    pub fn is_for_scope(&self, scope: &WorkScope) -> bool {
        self.holder.scope.id == scope.id && Arc::ptr_eq(&self.holder.scope.inner, &scope.inner)
    }
    pub fn all_objects_bytes(&self) -> u64 {
        self.holder.all_objects_bytes
    }
    pub fn check_backing_total(&self, simultaneously_live_bytes: u64) -> Result<(), WorkError> {
        if simultaneously_live_bytes > self.all_objects_bytes() {
            Err(WorkError::Capacity("result backing envelope"))
        } else {
            Ok(())
        }
    }
    /// Transfer/copy only after checking all simultaneously live backing
    /// capacities, including old + new. This guard covers the original grant
    /// and keeps its position live until the last physical alias exits.
    pub fn retain_alias(&self) -> ResultWindowAlias {
        ResultWindowAlias {
            holder: Arc::clone(&self.holder),
        }
    }
}
#[derive(Clone)]
pub struct ResultWindowAlias {
    holder: Arc<WindowHolder>,
}
impl ResultWindowAlias {
    pub fn scope_id(&self) -> WorkId {
        self.holder.scope.id()
    }
    pub fn class(&self) -> ResultWindowClass {
        self.holder.class
    }
    pub fn check_backing_total(&self, simultaneously_live_bytes: u64) -> Result<(), WorkError> {
        if simultaneously_live_bytes > self.holder.all_objects_bytes {
            Err(WorkError::Capacity("result backing envelope"))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ResourceConfig, WorkClass, WorkRequest, WorkloadConfig};
    fn control() -> (WorkloadControl, ResultCapacityHandle) {
        let control = WorkloadControl::try_new(
            WorkloadConfig {
                query_concurrency_limit: 1,
                ..WorkloadConfig::default()
            },
            ResourceConfig {
                total_bytes: 1024 * 1024,
                control_bytes: 1024,
                per_scope_bytes: 1024 * 1024 - 1024,
            },
        )
        .unwrap();
        let handle = control
            .configure_result_capacity(ResultCapacityConfig {
                client_compute_positions: 1,
                client_short_tail_positions: 0,
                supported_cancel_burst: 0,
                sustained_cancels_per_second: 0,
                positions: [1; 4],
                ..ResultCapacityConfig::V1
            })
            .unwrap();
        control.mark_ready().unwrap();
        (control, handle)
    }
    #[test]
    fn position_and_scope_survive_timeout_and_late_alias_exit() {
        let (control, capacity) = control();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = root.owner.scope().clone();
        let window = capacity
            .try_acquire(&scope, ResultWindowClass::Client)
            .unwrap();
        assert!(window.check_backing_total(8 * 1024 * 1024 + 1).is_err());
        let alias = window.retain_alias();
        drop(window);
        assert!(
            capacity
                .try_acquire(&scope, ResultWindowClass::Client)
                .is_err()
        );
        // A separate protocol closing position does not borrow the ordinary
        // window; its release cannot settle the fetch/transport alias.
        assert!(
            capacity
                .try_acquire(&scope, ResultWindowClass::Closing)
                .is_err()
        );
        assert!(
            capacity
                .try_acquire_closing(&scope, ResultClosingCut::AcceptedCancellation)
                .is_err()
        );
        drop(root);
        drop(
            capacity
                .try_acquire_closing(&scope, ResultClosingCut::AcceptedCancellation)
                .unwrap(),
        );
        assert_eq!(capacity.snapshot().held_positions, [1, 0, 0, 0]);
        assert!(
            scope
                .inner
                .state
                .lock()
                .unwrap()
                .nodes
                .contains_key(&scope.id)
        );
        drop(alias);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }
    #[test]
    fn capacity_is_explicit_startup_only_and_foreign_scopes_reject() {
        let (owner, capacity) = control();
        assert!(
            owner
                .configure_result_capacity(ResultCapacityConfig::V1)
                .is_err()
        );
        let (other, _) = control();
        let root = other
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        assert!(matches!(
            capacity.try_acquire(&root.owner.scope(), ResultWindowClass::Client),
            Err(WorkError::ForeignAuthority)
        ));
        assert!(
            ResultCapacityConfig {
                all_objects_bytes: [u64::MAX; 4],
                ..ResultCapacityConfig::V1
            }
            .validate()
            .is_err()
        );
    }
}
