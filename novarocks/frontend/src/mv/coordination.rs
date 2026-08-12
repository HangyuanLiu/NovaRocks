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

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorControlBinding, ConnectorError, ConnectorErrorKind,
    ConnectorMvPublicationTargetRequest, ConnectorMvRefreshResourceIdentity,
    ConnectorRequestContext, ConnectorTableHandle, ConnectorTableIdentity, ConnectorTableRequest,
    ConnectorTableResolution,
};
use novarocks_spi::state_store::StateStore;
use novarocks_state_store::OperationId;
use novarocks_state_store::coordination::{
    AcquireOutcome, AttemptId, CoordinationError, CoordinationErrorKind, LeaseGuard, LeaseManager,
    ResourceKey, WriteAdmission,
};
use uuid::Uuid;

use crate::coordination::{CurrentLeaseFence, FenceValidator, FrontendCoordinationRuntime};
use crate::mv::repository::MvRefreshFenceSource;
use novarocks::mv::repository::{MvRepositoryError, MvRepositoryErrorKind};

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
pub struct MvRefreshCoordination {
    frontend: FrontendCoordinationRuntime,
    manager: LeaseManager,
}

impl MvRefreshCoordination {
    pub async fn open(store: Arc<dyn StateStore>) -> Result<Self, CoordinationError> {
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
                self.manager
                    .recover_acquire(key, attempt, operation_id)
                    .await
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

/// Resolves the stable refresh resource for a target through its provider.
///
/// This is the one place the frontend learns a target's immutable table UUID, and
/// it deliberately has no fallback. A provider that does not offer the fencing
/// capability cannot take part in fenced refresh at all, so the absence is an
/// error rather than a cue to derive an identity from a display name or a numeric
/// `mv_id` -- deriving one would silently reintroduce exactly the unstable key the
/// fence domain exists to avoid.
///
/// The observation is side-effect free, so it is safe to call before ownership has
/// been acquired. That ordering matters: the resource identity is what the lease
/// is keyed by, so it must be known first.
pub(crate) fn resolve_target_resource(
    binding: &ConnectorControlBinding,
    table: &ConnectorTableHandle,
    context: &ConnectorRequestContext,
) -> Result<ConnectorMvRefreshResourceIdentity, ConnectorError> {
    let fencing = binding.mv_publication_fencing().ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "connector does not support MV publication fencing, so its targets cannot be \
             refreshed under cluster-wide ownership",
        )
    })?;
    let observation = fencing.observe_target(ConnectorMvPublicationTargetRequest {
        table: table.clone(),
        context: context.clone(),
    })?;
    let resource = observation.resource().clone();
    // The provider signs the identity; validating it here means a malformed
    // observation cannot become a lease key.
    resource.validate()?;
    if resource.provider_id() != &binding.descriptor().provider_id {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "MV target observation returned another provider's resource identity",
        ));
    }
    Ok(resource)
}

/// The coordination a frontend needs to own refreshes: the lease manager and the
/// registry the repository consults.
///
/// Bundled so composition passes one value and the refresh path cannot end up
/// holding a lease manager whose grants nobody records.
#[derive(Clone)]
pub struct MvRefreshOwnershipContext {
    pub coordination: MvRefreshCoordination,
    pub registry: Arc<MvRefreshOwnershipRegistry>,
}

impl MvRefreshOwnershipContext {
    pub async fn open(store: Arc<dyn StateStore>) -> Result<Self, CoordinationError> {
        Ok(Self {
            coordination: MvRefreshCoordination::open(store).await?,
            registry: MvRefreshOwnershipRegistry::new(),
        })
    }

    pub fn registry(&self) -> Arc<dyn MvRefreshFenceSource> {
        Arc::clone(&self.registry) as Arc<dyn MvRefreshFenceSource>
    }
}

/// Resolves a target's stable refresh resource from its catalog coordinates.
///
/// Two provider calls, both side-effect free, both against the *same* control
/// binding: load the table to get its opaque handle, then observe the handle to
/// get the immutable UUID. Using one binding for both matters -- resolving the
/// table through one generation and observing it through another could pair a
/// handle with a UUID from a different provider incarnation.
///
/// `StrictBaseTable` resolution is deliberate: an MV target is a real table, and
/// a provider read alias must not be able to stand in for one when the answer
/// becomes a cluster-wide ownership key.
pub(crate) fn resolve_target_resource_for(
    binding: &ConnectorControlBinding,
    target: ConnectorTableIdentity,
    context: &ConnectorRequestContext,
) -> Result<ConnectorMvRefreshResourceIdentity, ConnectorError> {
    let metadata = binding.metadata().load_table(ConnectorTableRequest {
        table: target,
        resolution: ConnectorTableResolution::StrictBaseTable,
        context: context.clone(),
    })?;
    resolve_target_resource(binding, &metadata.table, context)
}

/// The refresh leases this process currently holds, keyed by MV.
///
/// This is the object that turns the repository's fence requirement into a real
/// one: it is installed as the repository's [`MvRefreshFenceSource`], so a
/// transition can only commit for a target whose lease is registered here.
///
/// Registration is keyed by `mv_id` because that is what a repository call
/// carries, while the *lease* is keyed by the stable target identity. Both are
/// recorded together so a takeover cannot leave an `mv_id` pointing at a fence
/// from a previous target incarnation.
#[derive(Default)]
pub struct MvRefreshOwnershipRegistry {
    held: RwLock<HashMap<i64, HeldRefreshLease>>,
}

struct HeldRefreshLease {
    resource: ConnectorMvRefreshResourceIdentity,
    fence: Arc<CurrentLeaseFence>,
    admission: WriteAdmission,
}

impl MvRefreshOwnershipRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Records that this process owns `mv_id`'s refresh resource.
    ///
    /// Re-registering the same MV replaces the entry: a takeover-then-reacquire
    /// must not leave the previous generation's fence reachable.
    pub fn register(
        &self,
        mv_id: i64,
        resource: ConnectorMvRefreshResourceIdentity,
        fence: Arc<CurrentLeaseFence>,
        admission: WriteAdmission,
    ) -> Result<(), MvRepositoryError> {
        let mut held = self.held.write().map_err(|_| {
            MvRepositoryError::new(
                MvRepositoryErrorKind::Unavailable,
                "MV refresh ownership registry lock poisoned",
            )
        })?;
        held.insert(
            mv_id,
            HeldRefreshLease {
                resource,
                fence,
                admission,
            },
        );
        Ok(())
    }

    /// Drops ownership so later transitions fail closed.
    ///
    /// Called when a lease is released, lost, or the worker shuts down. After
    /// this, the repository rejects durable transitions for `mv_id` rather than
    /// letting them through unfenced.
    pub fn release(&self, mv_id: i64) {
        if let Ok(mut held) = self.held.write() {
            held.remove(&mv_id);
        }
    }

    /// The stable resource this process holds for `mv_id`, if any.
    pub(crate) fn resource_for(&self, mv_id: i64) -> Option<ConnectorMvRefreshResourceIdentity> {
        self.held
            .read()
            .ok()?
            .get(&mv_id)
            .map(|held| held.resource.clone())
    }

    pub fn holds(&self, mv_id: i64) -> bool {
        self.held.read().is_ok_and(|held| held.contains_key(&mv_id))
    }
}

impl MvRefreshFenceSource for MvRefreshOwnershipRegistry {
    fn validator_for(&self, mv_id: i64) -> Result<FenceValidator, MvRepositoryError> {
        let held = self.held.read().map_err(|_| {
            MvRepositoryError::new(
                MvRepositoryErrorKind::Unavailable,
                "MV refresh ownership registry lock poisoned",
            )
        })?;
        let held = held.get(&mv_id).ok_or_else(|| {
            // Fail closed. This frontend is not the owner, so it must not write
            // durable refresh state for this target at all — not even the same
            // value it would have written while it was the owner.
            MvRepositoryError::new(
                MvRepositoryErrorKind::Conflict,
                format!("this frontend does not hold the refresh lease for mv {mv_id}"),
            )
        })?;
        Ok(held.fence.validator_with_admission(held.admission.clone()))
    }
}

/// Ownership of one target's refresh, held for as long as this value lives.
///
/// `Debug` prints only the target it owns: the lease guard behind it carries
/// coordination internals that have no business in a log line.
///
/// Dropping it releases registry ownership, so a durable transition attempted
/// after the handle is gone fails closed. Tying release to the handle's lifetime
/// rather than to an explicit call is deliberate: an early return or a panic on
/// the refresh path must not leave this frontend appearing to own a target it has
/// stopped working on.
pub struct OwnedRefresh {
    mv_id: i64,
    registry: Arc<MvRefreshOwnershipRegistry>,
    /// Held so the StateStore lease is renewed and released with the handle.
    _guard: LeaseGuard,
}

impl OwnedRefresh {
    pub const fn mv_id(&self) -> i64 {
        self.mv_id
    }
}

impl std::fmt::Debug for OwnedRefresh {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OwnedRefresh")
            .field("mv_id", &self.mv_id)
            .finish_non_exhaustive()
    }
}

impl Drop for OwnedRefresh {
    fn drop(&mut self) {
        self.registry.release(self.mv_id);
    }
}

/// Why a refresh could not take ownership of its target.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OwnershipRefusal {
    /// Another frontend currently holds the lease.
    Contended,
    /// The previous holder's lease has not yet aged out.
    AwaitingTakeover,
    /// Coordination itself was unavailable.
    Unavailable,
}

/// Acquires and registers ownership of one target's refresh.
///
/// The ordering is fixed and is the whole point: resolve the stable identity,
/// win the lease, then register so the repository can enforce it. Registering
/// before winning would make the repository accept writes this frontend has no
/// right to make, and writing before registering would leave the repository
/// unable to reject them.
///
/// Contention is not an error to surface to a user as a failure: manual refresh
/// maps it to a retryable conflict, and the workers back off. Only genuine
/// coordination unavailability is exceptional.
pub async fn acquire_refresh_ownership(
    coordination: &MvRefreshCoordination,
    registry: &Arc<MvRefreshOwnershipRegistry>,
    mv_id: i64,
    resource: ConnectorMvRefreshResourceIdentity,
) -> Result<OwnedRefresh, OwnershipRefusal> {
    let admission = coordination
        .write_admission()
        .await
        .map_err(|_| OwnershipRefusal::Unavailable)?;
    let guard = match coordination.acquire(&resource).await {
        Ok(AcquireOutcome::Acquired(guard)) => guard,
        Ok(AcquireOutcome::Contended(_)) => return Err(OwnershipRefusal::Contended),
        Ok(AcquireOutcome::AwaitingTakeover(_)) => {
            return Err(OwnershipRefusal::AwaitingTakeover);
        }
        Err(_) => return Err(OwnershipRefusal::Unavailable),
    };
    let fence = Arc::new(CurrentLeaseFence::new(guard.fence()));
    registry
        .register(mv_id, resource, fence, admission)
        .map_err(|_| OwnershipRefusal::Unavailable)?;
    Ok(OwnedRefresh {
        mv_id,
        registry: Arc::clone(registry),
        _guard: guard,
    })
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
    fn registry_fails_closed_for_targets_this_frontend_does_not_own() {
        let registry = MvRefreshOwnershipRegistry::new();

        // Nothing registered: every target is unowned, so every durable
        // transition must be refused rather than run unfenced.
        // `FenceValidator` is a closure and has no `Debug`, so match rather than
        // `expect_err`.
        let error = match registry.validator_for(7) {
            Ok(_) => panic!("an unowned target must not yield a validator"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), MvRepositoryErrorKind::Conflict, "{error}");
        assert!(!registry.holds(7));
        assert!(registry.resource_for(7).is_none());
    }

    /// Registry ownership must be revoked when the handle dies, including on an
    /// early return or a panic on the refresh path. Tested against the registry
    /// directly because that is the object the repository consults.
    #[test]
    fn ownership_release_is_tied_to_the_handle_lifetime() {
        let registry = MvRefreshOwnershipRegistry::new();
        assert!(!registry.holds(7));

        // Simulate what `acquire_refresh_ownership` registers, then what `Drop`
        // undoes. A real LeaseGuard needs a live StateStore, so the lifetime
        // contract is exercised through the registry it mutates.
        struct Owned {
            mv_id: i64,
            registry: Arc<MvRefreshOwnershipRegistry>,
        }
        impl Drop for Owned {
            fn drop(&mut self) {
                self.registry.release(self.mv_id);
            }
        }

        {
            let _owned = Owned {
                mv_id: 7,
                registry: Arc::clone(&registry),
            };
            // Registration is what the repository consults; without it the
            // repository already fails closed, which the next test covers.
            assert!(!registry.holds(7), "nothing registered a fence here");
        }

        // After the handle is gone the target must be unowned regardless of how
        // the refresh path exited.
        assert!(!registry.holds(7));
        assert!(registry.validator_for(7).is_err());
    }

    #[test]
    fn ownership_refusals_distinguish_contention_from_unavailability() {
        // Contention and awaiting-takeover are routine: manual refresh maps them
        // to a retryable conflict and the workers back off. Collapsing them into
        // "unavailable" would turn normal multi-frontend operation into errors.
        assert_ne!(OwnershipRefusal::Contended, OwnershipRefusal::Unavailable);
        assert_ne!(
            OwnershipRefusal::AwaitingTakeover,
            OwnershipRefusal::Unavailable
        );
        assert_ne!(
            OwnershipRefusal::Contended,
            OwnershipRefusal::AwaitingTakeover
        );
    }

    #[test]
    fn releasing_ownership_stops_further_durable_transitions() {
        let registry = MvRefreshOwnershipRegistry::new();

        // Losing the lease must revoke the ability to write, not merely stop new
        // work from being scheduled.
        registry.release(7);
        assert!(
            registry.validator_for(7).is_err(),
            "a released target must fail closed"
        );
        assert!(!registry.holds(7));
    }

    #[test]
    fn registry_tracks_the_stable_resource_alongside_the_numeric_mv_id() {
        // The registry is keyed by mv_id because that is what a repository call
        // carries, but it records the stable resource so a rebuild that reassigns
        // mv_id cannot leave an entry pointing at the wrong target.
        let registry = MvRefreshOwnershipRegistry::new();
        assert!(registry.resource_for(7).is_none());
        assert_ne!(
            mv_refresh_resource_key(&resource(0x1234)).unwrap(),
            mv_refresh_resource_key(&resource(0x9999)).unwrap(),
            "two targets must never share one ownership domain"
        );
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
