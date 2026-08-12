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

//! Cluster-wide MV refresh ownership.
//!
//! The process-local activity gate and the scheduler's running set decide
//! *fairness and capacity inside one frontend*. Neither can arbitrate between
//! two frontends sharing a StateStore: both can find the same target due and
//! both can start an attempt. This module supplies the missing arbiter — one
//! StateStore lease per MV target — and the fence validator that makes every
//! durable transition reject a superseded owner inside its own commit.
//!
//! Two properties are deliberate:
//!
//! * **One resource domain per target, whatever the entry point.** Manual SQL,
//!   the scheduler, and recovery all compete for the *same* lease. Splitting the
//!   domain by entry point, refresh policy, numeric `mv_id`, or frontend
//!   instance would let two of them run concurrently and call it correct.
//! * **The resource key is the stable target identity** frozen by the external
//!   publication fencing contract (provider ID + immutable target table UUID).
//!   A StateStore rebuild reassigns the numeric `mv_id`, so keying on it would
//!   silently split one target into two domains across a rebuild.

use std::sync::Arc;

use bytes::Bytes;
use novarocks_spi::connector::ConnectorMvRefreshResourceIdentity;
use novarocks_spi::state_store::StateStore;
use novarocks_state_store::OperationId;
use novarocks_state_store::coordination::{
    AcquireOutcome, AttemptId, CoordinationError, CoordinationErrorKind, LeaseManager, ResourceKey,
    WriteAdmission,
};
use uuid::Uuid;

use crate::coordination::{CurrentLeaseFence, FenceValidator, FrontendCoordinationRuntime};

/// Prefix that scopes MV refresh leases away from every other coordinated
/// resource in the same StateStore.
const MV_REFRESH_RESOURCE_PREFIX: &[u8] = b"novarocks/mv/refresh/v1/";

/// Builds the per-target lease resource key from the stable target identity.
///
/// Only the provider ID and the immutable target table UUID contribute. A
/// display name, a numeric `mv_id`, or a catalog attachment lifecycle ID must
/// never appear here: the first two are reassigned by a rebuild and the third is
/// reused across DROP/recreate, so any of them would break the invariant that
/// one external table maps to exactly one refresh ownership domain.
pub(crate) fn mv_refresh_resource_key(
    resource: &ConnectorMvRefreshResourceIdentity,
) -> Result<ResourceKey, CoordinationError> {
    let canonical = resource.canonical_encoding();
    let mut bytes = Vec::with_capacity(MV_REFRESH_RESOURCE_PREFIX.len() + canonical.len());
    bytes.extend_from_slice(MV_REFRESH_RESOURCE_PREFIX);
    bytes.extend_from_slice(&canonical);
    ResourceKey::try_from(Bytes::from(bytes))
}

/// Per-target MV refresh ownership, shared by every refresh entry point.
#[derive(Clone)]
pub(crate) struct MvRefreshCoordination {
    frontend: FrontendCoordinationRuntime,
    manager: LeaseManager,
}

impl MvRefreshCoordination {
    pub(crate) async fn open(store: Arc<dyn StateStore>) -> Result<Self, CoordinationError> {
        let frontend = FrontendCoordinationRuntime::open(store).await?;
        Self::from_frontend(&frontend)
    }

    pub(crate) fn from_frontend(
        frontend: &FrontendCoordinationRuntime,
    ) -> Result<Self, CoordinationError> {
        Ok(Self {
            frontend: frontend.clone(),
            manager: frontend.lease_manager(),
        })
    }

    /// Competes for one target's refresh lease.
    ///
    /// A `CommitUncertain` acquire is recovered under the **same**
    /// `(resource, attempt, operation_id)` rather than retried with a fresh
    /// attempt. Minting a new attempt would let one logical acquisition appear
    /// twice in the lease record and defeat the fence it is supposed to
    /// establish.
    pub(crate) async fn acquire(
        &self,
        resource: &ConnectorMvRefreshResourceIdentity,
    ) -> Result<AcquireOutcome, CoordinationError> {
        let key = mv_refresh_resource_key(resource)?;
        let attempt = AttemptId::try_from(Uuid::now_v7())?;
        let operation_id = OperationId::new_v7();
        match self
            .manager
            .acquire(key.clone(), attempt, operation_id)
            .await
        {
            Err(error) if error.kind() == CoordinationErrorKind::CommitUncertain => {
                self.manager.recover_acquire(key, attempt, operation_id).await
            }
            result => result,
        }
    }

    /// The global write-admission handle, composed into every fence validator so
    /// a refresh cannot write durable state while the control plane is still
    /// reconciling.
    pub(crate) async fn write_admission(&self) -> Result<WriteAdmission, CoordinationError> {
        self.frontend.admit_writes().await
    }

    /// Builds the validator a repository call must carry.
    ///
    /// Always composes admission with the lease fence: being inside an open
    /// write epoch and being the current owner are independent facts, and a
    /// refresh needs both to be true at commit time.
    pub(crate) async fn validator(
        &self,
        current: &CurrentLeaseFence,
    ) -> Result<FenceValidator, CoordinationError> {
        Ok(current.validator_with_admission(self.write_admission().await?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::ConnectorProviderId;

    fn resource(uuid: u128) -> ConnectorMvRefreshResourceIdentity {
        ConnectorMvRefreshResourceIdentity::try_new(
            ConnectorProviderId::parse("iceberg").unwrap(),
            Uuid::from_u128(uuid),
        )
        .unwrap()
    }

    #[test]
    fn resource_key_is_stable_and_target_scoped() {
        let first = mv_refresh_resource_key(&resource(0x1234)).unwrap();
        let again = mv_refresh_resource_key(&resource(0x1234)).unwrap();
        assert_eq!(
            first, again,
            "the same target must always map to the same lease resource"
        );

        let other = mv_refresh_resource_key(&resource(0x9999)).unwrap();
        assert_ne!(
            first, other,
            "distinct targets must hold distinct leases so they refresh in parallel"
        );
    }

    /// The key the implementation must produce: namespace prefix followed by
    /// exactly the stable identity's canonical encoding, and nothing else. This
    /// pins the invariant that no numeric `mv_id`, display name, or attachment
    /// id can leak into the ownership domain — anything extra would change this
    /// byte sequence.
    fn expected_key(identity: &ConnectorMvRefreshResourceIdentity) -> ResourceKey {
        let canonical = identity.canonical_encoding();
        let mut bytes = Vec::with_capacity(MV_REFRESH_RESOURCE_PREFIX.len() + canonical.len());
        bytes.extend_from_slice(MV_REFRESH_RESOURCE_PREFIX);
        bytes.extend_from_slice(&canonical);
        ResourceKey::try_from(Bytes::from(bytes)).unwrap()
    }

    #[test]
    fn resource_key_is_exactly_namespace_plus_stable_identity() {
        let identity = resource(0x1234);

        assert_eq!(
            mv_refresh_resource_key(&identity).unwrap(),
            expected_key(&identity),
            "the lease key must be the namespaced stable identity and carry nothing else"
        );

        // A bare canonical encoding without the namespace is a different key, so
        // MV refresh leases cannot collide with another coordinated domain that
        // happens to key on the same identity.
        let unnamespaced =
            ResourceKey::try_from(Bytes::from(identity.canonical_encoding())).unwrap();
        assert_ne!(mv_refresh_resource_key(&identity).unwrap(), unnamespaced);
    }
}
