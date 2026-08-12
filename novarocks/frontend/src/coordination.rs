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

//! Process-lifetime coordination dependencies owned by the frontend host.
//!
//! Domain services consume this runtime. They never bootstrap a second holder
//! or change the global restore/write-open state themselves.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use novarocks_spi::state_store::{StateStore, WriteTransaction};
use novarocks_state_store::OperationId;
use novarocks_state_store::coordination::{
    ClockHealth, CoordinationError, CoordinationErrorKind, HolderId, IncarnationGate, LeaseClock,
    LeaseFence, LeaseManager, LeaseSettings, WriteAdmission,
};
use uuid::Uuid;

/// Closure a fenced owner hands to a repository so the exact lease fence is
/// validated **inside the same write transaction** as the writes it guards.
///
/// Checking a lease outside the transaction is not fencing: between the check
/// and the commit, another host can take the lease over. Passing the validation
/// in as a closure is what makes "stale owner cannot write" a property of the
/// commit rather than of the caller's timing.
pub type FenceValidator = Arc<
    dyn for<'txn> Fn(
            &'txn mut dyn WriteTransaction,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'txn>>
        + Send
        + Sync,
>;

/// Holds the lease fence an owner currently possesses.
///
/// Renewal advances the fence in place, so validators already handed to a
/// repository keep validating against the *current* fence instead of a snapshot
/// taken when the validator was built.
pub(crate) struct CurrentLeaseFence {
    fence: Arc<RwLock<LeaseFence>>,
}

impl CurrentLeaseFence {
    pub(crate) fn new(fence: LeaseFence) -> Self {
        Self {
            fence: Arc::new(RwLock::new(fence)),
        }
    }

    pub(crate) fn validator(&self) -> FenceValidator {
        let current = Arc::clone(&self.fence);
        Arc::new(move |transaction| {
            let fence = match current.read() {
                Ok(fence) => fence.clone(),
                Err(_) => {
                    return Box::pin(async { Err(FENCE_LOCK_POISONED.to_string()) });
                }
            };
            Box::pin(async move {
                fence
                    .validate_in(transaction)
                    .await
                    .map_err(|error| error.to_string())
            })
        })
    }

    /// Composes a global write-admission assertion with the lease fence so both
    /// land in one commit. Used where a caller must prove it is both inside an
    /// open write epoch and still the resource's owner.
    pub(crate) fn validator_with_admission(&self, admission: WriteAdmission) -> FenceValidator {
        let current = Arc::clone(&self.fence);
        Arc::new(move |transaction| {
            let admission = admission.clone();
            let fence = match current.read() {
                Ok(fence) => fence.clone(),
                Err(_) => {
                    return Box::pin(async { Err(FENCE_LOCK_POISONED.to_string()) });
                }
            };
            Box::pin(async move {
                admission
                    .validate_in(transaction)
                    .await
                    .map_err(|error| error.to_string())?;
                fence
                    .validate_in(transaction)
                    .await
                    .map_err(|error| error.to_string())
            })
        })
    }

    pub(crate) fn replace(&self, fence: LeaseFence) -> Result<(), String> {
        *self
            .fence
            .write()
            .map_err(|_| FENCE_LOCK_POISONED.to_string())? = fence;
        Ok(())
    }

    pub(crate) fn fence(&self) -> Result<LeaseFence, String> {
        self.fence
            .read()
            .map(|fence| fence.clone())
            .map_err(|_| FENCE_LOCK_POISONED.to_string())
    }
}

pub(crate) const FENCE_LOCK_POISONED: &str = "frontend lease fence lock poisoned";

pub(crate) const FRONTEND_LEASE_DURATION: Duration = Duration::from_secs(15);
pub(crate) const FRONTEND_LEASE_RENEW_INTERVAL: Duration = Duration::from_secs(5);
pub(crate) const FRONTEND_MAX_CLOCK_SKEW: Duration = Duration::from_secs(1);
pub(crate) const FRONTEND_TAKEOVER_OBSERVATION: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub(crate) struct FrontendCoordinationRuntime {
    store: Arc<dyn StateStore>,
    gate: Arc<IncarnationGate>,
    manager: LeaseManager,
    holder_uuid: Uuid,
}

impl FrontendCoordinationRuntime {
    pub(crate) async fn open(store: Arc<dyn StateStore>) -> Result<Self, CoordinationError> {
        Self::open_with_clock(store, Arc::new(SystemFrontendLeaseClock::default())).await
    }

    pub(crate) async fn open_with_clock(
        store: Arc<dyn StateStore>,
        clock: Arc<dyn LeaseClock>,
    ) -> Result<Self, CoordinationError> {
        let gate = Arc::new(IncarnationGate::new(Arc::clone(&store)));
        match gate.load().await {
            Ok(_) => {}
            Err(error) if error.kind() == CoordinationErrorKind::NotBootstrapped => {
                let operation_id = OperationId::new_v7();
                match gate.bootstrap(operation_id).await {
                    Ok(_) => {}
                    Err(error) if error.kind() == CoordinationErrorKind::CommitUncertain => {
                        gate.recover_bootstrap(operation_id).await?;
                    }
                    Err(error) => return Err(error),
                }
            }
            Err(error) => return Err(error),
        }

        let holder_uuid = Uuid::now_v7();
        let holder = HolderId::try_from(Bytes::from(format!(
            "novarocks/frontend/process/v1/{holder_uuid}"
        )))?;
        let settings = LeaseSettings::new(
            FRONTEND_LEASE_DURATION,
            FRONTEND_LEASE_RENEW_INTERVAL,
            FRONTEND_MAX_CLOCK_SKEW,
            FRONTEND_TAKEOVER_OBSERVATION,
        )?;
        let manager = LeaseManager::new(Arc::clone(&store), holder, clock, settings)?;
        Ok(Self {
            store,
            gate,
            manager,
            holder_uuid,
        })
    }

    pub(crate) async fn admit_writes(&self) -> Result<WriteAdmission, CoordinationError> {
        self.gate.admit_writes().await
    }

    pub(crate) fn lease_manager(&self) -> LeaseManager {
        self.manager.clone()
    }

    pub(crate) fn store(&self) -> Arc<dyn StateStore> {
        Arc::clone(&self.store)
    }

    pub(crate) const fn holder_uuid(&self) -> Uuid {
        self.holder_uuid
    }
}

#[derive(Debug)]
pub(crate) struct SystemFrontendLeaseClock {
    monotonic_origin: Instant,
}

impl Default for SystemFrontendLeaseClock {
    fn default() -> Self {
        Self {
            monotonic_origin: Instant::now(),
        }
    }
}

impl LeaseClock for SystemFrontendLeaseClock {
    fn wall_time_millis(&self) -> Result<u64, CoordinationError> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| CoordinationError::clock_unsafe())
            .and_then(|duration| {
                u64::try_from(duration.as_millis()).map_err(|_| CoordinationError::clock_unsafe())
            })
    }

    fn monotonic_time_millis(&self) -> u64 {
        u64::try_from(self.monotonic_origin.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    fn health(&self) -> ClockHealth {
        ClockHealth::Healthy
    }
}
