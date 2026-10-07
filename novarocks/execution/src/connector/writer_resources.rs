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

//! Exact operator-owned memory reservations for connector writers.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorExecutionResources, ConnectorResourceCheckpoint,
    ConnectorResourceClass, ConnectorResourceLease, ConnectorResourceLedger,
};

use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};

/// One writer's resource capability, installed with its actual task tracker.
/// Constructing the request does not authorize reservations before installation.
#[derive(Default)]
pub struct WriterResourceLedger {
    tracker: OnceLock<Arc<MemTracker>>,
    checkpoint: AtomicU64,
    error_state: OnceLock<Arc<RuntimeErrorState>>,
}

impl WriterResourceLedger {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn install(&self, tracker: Arc<MemTracker>) -> Result<(), String> {
        if let Some(existing) = self.tracker.get() {
            return if Arc::ptr_eq(existing, &tracker) {
                Ok(())
            } else {
                Err("writer resources cannot be rebound to another task tracker".to_string())
            };
        }
        self.tracker
            .set(tracker)
            .map_err(|_| "writer resource tracker installation raced".to_string())
    }

    pub fn resources(self: &Arc<Self>) -> ConnectorExecutionResources {
        ConnectorExecutionResources::from_admitted_ledger(
            Arc::clone(self) as Arc<dyn ConnectorResourceLedger>
        )
    }

    pub(crate) fn bind_runtime_state(&self, state: &RuntimeState) -> Result<(), String> {
        let error_state = state.error_state();
        if let Some(existing) = self.error_state.get() {
            return if Arc::ptr_eq(existing, &error_state) {
                Ok(())
            } else {
                Err("writer resources cannot be rebound to another runtime state".to_string())
            };
        }
        self.error_state
            .set(error_state)
            .map_err(|_| "writer resource runtime binding raced".to_string())
    }

    pub fn ensure_installed(&self) -> Result<(), String> {
        self.tracker
            .get()
            .map(|_| ())
            .ok_or_else(|| "writer resources require the admitted task memory tracker".to_string())
    }
}

struct WriterResourceLease {
    tracker: Arc<MemTracker>,
    bytes: i64,
    error_state: Option<Arc<RuntimeErrorState>>,
}

fn reservation_bytes(
    bytes: u64,
    error_state: Option<&Arc<RuntimeErrorState>>,
) -> Result<i64, ConnectorError> {
    i64::try_from(bytes).map_err(|_| {
        publish_range_refusal(error_state, bytes);
        ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "writer reservation exceeds the native memory tracker range",
        )
    })
}

fn publish_range_refusal(error_state: Option<&Arc<RuntimeErrorState>>, requested: u64) {
    if let Some(error_state) = error_state {
        error_state.set_failure(novarocks_execution_contract::TaskFailure::capacity_refused(
            novarocks_execution_contract::SafeDetail::new("connector writer reservation")
                .expect("bounded static resource name"),
            requested,
            i64::MAX as u64,
        ));
    }
}

fn charge(
    tracker: &MemTracker,
    bytes: i64,
    error_state: Option<&Arc<RuntimeErrorState>>,
) -> Result<(), ConnectorError> {
    let result = if let Some(error_state) = error_state {
        tracker.consume_checked(bytes).map_err(|failure| {
            let detail = failure.to_string();
            error_state.set_failure(failure);
            detail
        })
    } else {
        tracker.consume_and_check_limit(bytes)
    };
    // A refused reservation owns no allocation: undo the provisional charge.
    result.map_err(|error| {
        tracker.release(bytes);
        ConnectorError::new(ConnectorErrorKind::ResourceExhausted, error)
    })
}

impl ConnectorResourceLease for WriterResourceLease {
    fn bytes(&self) -> u64 {
        self.bytes as u64
    }

    fn try_grow(&mut self, additional: u64) -> Result<(), ConnectorError> {
        let additional = reservation_bytes(additional, self.error_state.as_ref())?;
        let total = self.bytes.checked_add(additional).ok_or_else(|| {
            publish_range_refusal(
                self.error_state.as_ref(),
                (self.bytes as u64).saturating_add(additional as u64),
            );
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "writer resource lease exceeds the native memory tracker range",
            )
        })?;
        charge(&self.tracker, additional, self.error_state.as_ref())?;
        self.bytes = total;
        Ok(())
    }

    fn shrink_to(&mut self, bytes: u64) -> Result<(), ConnectorError> {
        let bytes = reservation_bytes(bytes, self.error_state.as_ref())?;
        if bytes > self.bytes {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "writer resource lease cannot grow through shrink_to",
            ));
        }
        self.tracker.release(self.bytes - bytes);
        self.bytes = bytes;
        Ok(())
    }
}

impl Drop for WriterResourceLease {
    fn drop(&mut self) {
        self.tracker.release(self.bytes);
    }
}

impl ConnectorResourceLedger for WriterResourceLedger {
    fn checkpoint(&self) -> Result<ConnectorResourceCheckpoint, ConnectorError> {
        self.ensure_installed()
            .map_err(|error| ConnectorError::new(ConnectorErrorKind::InvalidRequest, error))?;
        Ok(ConnectorResourceCheckpoint::new(
            self.checkpoint.fetch_add(1, Ordering::Relaxed),
        ))
    }

    fn try_reserve(
        &self,
        _class: ConnectorResourceClass,
        bytes: u64,
    ) -> Result<Box<dyn ConnectorResourceLease>, ConnectorError> {
        let tracker = self.tracker.get().cloned().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector writer reserved memory before task tracker installation",
            )
        })?;
        let bytes = reservation_bytes(bytes, self.error_state.get())?;
        charge(&tracker, bytes, self.error_state.get())?;
        Ok(Box::new(WriterResourceLease {
            tracker,
            bytes,
            error_state: self.error_state.get().cloned(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_execution_contract::TaskFailureCategory;

    #[test]
    fn writer_resources_refuse_uninstalled_and_rebound_trackers() {
        let ledger = Arc::new(WriterResourceLedger::new());
        let resources = ledger.resources();
        assert!(resources.checkpoint().is_err());
        assert!(
            resources
                .try_reserve(ConnectorResourceClass::WriterState, 1)
                .is_err()
        );
        let tracker = MemTracker::new_root("admitted writer");
        ledger.install(Arc::clone(&tracker)).expect("install");
        ledger.install(Arc::clone(&tracker)).expect("idempotent");
        assert!(ledger.install(MemTracker::new_root("other task")).is_err());
        assert_eq!(resources.checkpoint().expect("checkpoint").sequence(), 0);
    }

    #[test]
    fn writer_state_and_delete_positions_remain_charged_until_release() {
        let tracker = MemTracker::new_root("writer task");
        let ledger = Arc::new(WriterResourceLedger::new());
        ledger.install(Arc::clone(&tracker)).expect("install");
        let resources = ledger.resources();
        let state = resources
            .try_reserve(ConnectorResourceClass::WriterState, 32)
            .expect("state");
        let mut positions = resources
            .try_reserve(ConnectorResourceClass::WriterState, 16)
            .expect("DV positions");
        positions.try_grow(16).expect("grow");
        assert_eq!(tracker.current(), 64);
        positions.shrink_to(8).expect("shrink");
        assert_eq!(tracker.current(), 40);
        drop(state);
        assert_eq!(tracker.current(), 8);
        positions.release();
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn writer_reservation_range_refusal_is_typed_without_a_charge() {
        let tracker = MemTracker::new_root("writer task");
        let state = RuntimeState::new(
            None,
            None,
            None,
            None,
            None,
            Some(Arc::clone(&tracker)),
            None,
        );
        let ledger = Arc::new(WriterResourceLedger::new());
        ledger.install(Arc::clone(&tracker)).expect("install");
        ledger.bind_runtime_state(&state).expect("bind");
        assert!(
            ledger
                .resources()
                .try_reserve(ConnectorResourceClass::WriterState, u64::MAX)
                .is_err()
        );
        assert_eq!(tracker.current(), 0);
        assert!(matches!(
            state
                .error_state()
                .task_failure()
                .expect("typed failure")
                .category(),
            TaskFailureCategory::CapacityRefused {
                requested: u64::MAX,
                ..
            }
        ));
    }

    #[test]
    fn refused_writer_reservations_roll_back_and_preserve_typed_capacity_failure() {
        let tracker = MemTracker::new_root("writer task");
        tracker.install_limit_once(64).expect("limit");
        let state = RuntimeState::new(
            None,
            None,
            None,
            None,
            None,
            Some(Arc::clone(&tracker)),
            None,
        );
        let ledger = Arc::new(WriterResourceLedger::new());
        ledger.install(Arc::clone(&tracker)).expect("install");
        let resources = ledger.resources();
        let mut held = resources
            .try_reserve(ConnectorResourceClass::WriterState, 48)
            .expect("state");
        assert!(held.try_grow(32).is_err());
        assert_eq!(held.bytes(), 48);
        assert_eq!(tracker.current(), 48);
        assert!(
            resources
                .try_reserve(ConnectorResourceClass::WriterState, 24)
                .is_err()
        );
        assert_eq!(tracker.current(), 48);
        assert!(matches!(
            state
                .error_state()
                .task_failure()
                .expect("typed failure")
                .category(),
            TaskFailureCategory::CapacityRefused {
                requested: 80,
                limit: 64,
                ..
            }
        ));
        drop(held);
        assert_eq!(tracker.current(), 0);
    }
}
