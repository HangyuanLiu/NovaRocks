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

//! Product-owned Current observation, ordered installation, and candidate inventory.

use crate::dependency::MvDependencyObjectIdentity;
use crate::management::MvCurrentManagementAdmission;
use crate::persistence::documents::{MvDocumentError, MvObservedCurrentDocuments};
use crate::persistence::projection::{
    MvDocumentProjection, MvOutputStatistics, StoredMvProjection,
};
use crate::persistence::validation::PersistenceDecodeBudget;
use crate::process_runtime::{ProcessRuntime, ProjectionOrder, TargetReadiness};
use crate::product::MvTarget;
use crate::repository::{
    DeleteMvProjectionRequest, LoadedMvProjection, MvRepository, MvRepositoryError,
    MvRepositoryErrorKind, ReplaceMvProjectionRequest,
};
use novarocks_spi::connector::{CatalogHandle, ConnectorRequestContext, LakePublicationId};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Clone)]
pub struct MvReadinessService {
    repository: Arc<dyn MvRepository>,
    runtime: Arc<ProcessRuntime<MvTarget, LakePublicationId>>,
}

#[derive(Clone)]
pub struct MvCandidateReader {
    repository: Arc<dyn MvRepository>,
}

pub struct MvRuntimePublicationLease {
    runtime: Arc<ProcessRuntime<MvTarget, LakePublicationId>>,
    target: MvTarget,
    publication_id: LakePublicationId,
}
impl Drop for MvRuntimePublicationLease {
    fn drop(&mut self) {
        self.runtime.finish(&self.target, self.publication_id);
    }
}

#[derive(Clone)]
pub struct MvCurrentProjectionRequest {
    catalog: CatalogHandle,
    target: MvTarget,
    context: ConnectorRequestContext,
    decode_budget: PersistenceDecodeBudget,
}
impl MvCurrentProjectionRequest {
    pub fn try_new(
        catalog: CatalogHandle,
        target: MvTarget,
        context: ConnectorRequestContext,
        decode_budget: PersistenceDecodeBudget,
    ) -> Result<Self, MvProjectionError> {
        if target.catalog() != Some(catalog.catalog_name().as_str()) {
            return Err(MvProjectionError::new(
                MvProjectionErrorKind::SourceConflict,
                "MV projection target and catalog binding disagree",
            ));
        }
        Ok(Self {
            catalog,
            target,
            context,
            decode_budget,
        })
    }
    pub fn catalog(&self) -> &CatalogHandle {
        &self.catalog
    }
    pub fn target(&self) -> &MvTarget {
        &self.target
    }
    pub fn context(&self) -> &ConnectorRequestContext {
        &self.context
    }
    pub fn decode_budget(&self) -> PersistenceDecodeBudget {
        self.decode_budget
    }
}

/// Whether plain SQL may read one MV target's storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MvQueryAdmission {
    /// Nothing in this process claims this table as an MV.
    NotAnMv,
    /// The projection is sound, whatever its management state.
    Admitted,
    /// The projection itself is in doubt, so its storage must not be read as
    /// though it were a published materialization.
    Quarantined(String),
}

pub struct MvCurrentProjectionObservation {
    pub documents: MvObservedCurrentDocuments,
    pub management_admission: MvCurrentManagementAdmission,
    pub output_statistics: Option<MvOutputStatistics>,
}

/// A validated Current document set that may populate the rebuildable
/// Accelerator inventory but carries no effect authority. Foreign ownership
/// and an incomplete restart readmission are both valid read-only states.
pub struct MvReadOnlyCurrentProjectionObservation {
    pub documents: MvObservedCurrentDocuments,
    pub output_statistics: Option<MvOutputStatistics>,
}

/// Called only after product reservation. Implementations resolve the exact
/// provider source and use the MV-owned sealed document reader.
#[async_trait::async_trait]
pub trait MvCurrentProjectionSource: Send + Sync {
    async fn observe(
        &self,
        request: &MvCurrentProjectionRequest,
    ) -> Result<MvCurrentProjectionObservation, MvProjectionError>;
}

/// Read-only Current observation. Implementations must use the same sealed
/// document reader as management admission, but must not manufacture a
/// management token or recovery barrier.
#[async_trait::async_trait]
pub trait MvReadOnlyCurrentProjectionSource: Send + Sync {
    async fn observe_read_only(
        &self,
        request: &MvCurrentProjectionRequest,
    ) -> Result<MvReadOnlyCurrentProjectionObservation, MvProjectionError>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MvProjectionErrorKind {
    SourceConflict,
    Unsupported,
    CorruptDocument,
    Unavailable,
    BudgetExceeded,
    Cancelled,
    DeadlineExceeded,
    Repository,
}

#[derive(Debug)]
pub struct MvProjectionError {
    kind: MvProjectionErrorKind,
    message: String,
}
impl MvProjectionError {
    pub fn new(kind: MvProjectionErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
    pub fn kind(&self) -> MvProjectionErrorKind {
        self.kind
    }
}
impl std::fmt::Display for MvProjectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for MvProjectionError {}
impl From<MvRepositoryError> for MvProjectionError {
    fn from(error: MvRepositoryError) -> Self {
        Self::new(MvProjectionErrorKind::Repository, error.to_string())
    }
}
impl From<MvDocumentError> for MvProjectionError {
    fn from(error: MvDocumentError) -> Self {
        use novarocks_spi::connector::ConnectorErrorKind as K;
        let kind = match &error {
            MvDocumentError::Connector(error) => match error.kind() {
                K::Unsupported => MvProjectionErrorKind::Unsupported,
                K::Cancelled => MvProjectionErrorKind::Cancelled,
                K::DeadlineExceeded => MvProjectionErrorKind::DeadlineExceeded,
                K::ResourceExhausted => MvProjectionErrorKind::BudgetExceeded,
                K::NotFound | K::InvalidRequest => MvProjectionErrorKind::SourceConflict,
                K::CorruptData => MvProjectionErrorKind::CorruptDocument,
                _ => MvProjectionErrorKind::Unavailable,
            },
            MvDocumentError::Codec(
                crate::persistence::codec::PersistenceCodecError::ResourceBudget { .. },
            ) => MvProjectionErrorKind::BudgetExceeded,
            _ => MvProjectionErrorKind::CorruptDocument,
        };
        Self::new(kind, error.to_string())
    }
}

#[derive(Debug)]
pub enum MvProjectionInstallOutcome {
    Installed(LoadedMvProjection),
    Unchanged(LoadedMvProjection),
    Removed,
    AlreadyAbsent,
    Superseded,
    /// The observation reached a target object this process already holds a
    /// projection for, under a different catalog attachment.
    ///
    /// A materialized view is the object it publishes into, not the attachment
    /// a discovery happened to see it through, so nothing is installed: the
    /// existing projection is the one. Two attachments over one catalog are an
    /// ordinary deployment, and registering the view once per attachment would
    /// make its own `DROP CATALOG` refuse and leave the real target competing
    /// with its own aliases.
    AlreadyProjectedElsewhere(MvTarget),
}

struct ProjectionReservation {
    target: MvTarget,
    generation: u64,
    expected: Option<LoadedMvProjection>,
    order: Arc<tokio::sync::Mutex<ProjectionOrder>>,
    /// This process managed the target when the reservation was taken.
    managed: bool,
    /// The version it managed, so management is restored only for that exact
    /// projection and never for one the observation replaced.
    installed_before: Option<crate::repository::MvProjectionVersion>,
}

/// Single-use deletion expectation captured before the provider effect.
pub struct MvProjectionDeleteGuard {
    reservation: ProjectionReservation,
}
impl MvProjectionDeleteGuard {
    pub fn has_projection(&self) -> bool {
        self.reservation.expected.is_some()
    }

    #[cfg(test)]
    pub(crate) fn for_test(target: MvTarget) -> Self {
        Self {
            reservation: ProjectionReservation {
                target,
                generation: 0,
                expected: None,
                order: Arc::new(tokio::sync::Mutex::new(ProjectionOrder::default())),
                managed: false,
                installed_before: None,
            },
        }
    }
}
pub enum MvDropReadiness {
    ReadyToDrop(MvProjectionDeleteGuard),
    AlreadyAbsent,
}

impl MvReadinessService {
    pub fn new(
        repository: Arc<dyn MvRepository>,
        runtime: Arc<ProcessRuntime<MvTarget, LakePublicationId>>,
    ) -> Self {
        Self {
            repository,
            runtime,
        }
    }
    pub fn candidate_reader(&self) -> MvCandidateReader {
        MvCandidateReader {
            repository: Arc::clone(&self.repository),
        }
    }
    async fn reserve(&self, target: MvTarget) -> Result<ProjectionReservation, MvProjectionError> {
        let order = self.runtime.projection_order(target.clone());
        let mut cell = order.lock().await;
        let generation = cell.advance()?;
        // What this process held before the reservation cleared it. A
        // read-only observation of a target this process already manages must
        // not revoke that management: the read-only path exists to populate
        // the inventory for targets whose management has not been
        // established, not to take it away from one that has.
        let managed = matches!(self.runtime.readiness(&target), TargetReadiness::Ready)
            && cell.installed.is_some();
        let installed_before = cell.installed.clone();
        cell.installed = None;
        cell.pending = Some(generation);
        self.runtime
            .set_unavailable(target.clone(), "fresh Current observation pending".into());
        // Captured before the provider source is invoked, under the same gate
        // used by installers and invalidators.
        let expected = self.repository.find_by_target(&target).await?;
        drop(cell);
        Ok(ProjectionReservation {
            target,
            generation,
            expected,
            order,
            managed,
            installed_before,
        })
    }
    async fn matches_repository(
        &self,
        reservation: &ProjectionReservation,
    ) -> Result<bool, MvProjectionError> {
        let current = self.repository.find_by_target(&reservation.target).await?;
        Ok(match (&reservation.expected, current) {
            (None, None) => true,
            (Some(expected), Some(current)) => {
                expected.projection.mv_id == current.projection.mv_id
                    && expected.version == current.version
                    && expected.projection.facts.source_revision().target_object_id
                        == current.projection.facts.source_revision().target_object_id
            }
            _ => false,
        })
    }
    pub async fn observe_current_and_install(
        &self,
        operation_id: Uuid,
        request: MvCurrentProjectionRequest,
        source: &dyn MvCurrentProjectionSource,
    ) -> Result<MvProjectionInstallOutcome, MvProjectionError> {
        let reservation = self.reserve(request.target.clone()).await?;
        let result = async {
            check_context(&request.context)?;
            let observation = source.observe(&request).await?;
            check_context(&request.context)?;
            if observation.documents.management_target.catalog() != request.catalog()
                || observation.documents.target.instance_id.as_str()
                    != request.target.catalog().unwrap_or_default()
                || observation.documents.target.namespace.as_ref() != request.target.namespace()
                || observation.documents.target.table.as_ref() != request.target.name()
            {
                return Err(MvProjectionError::new(
                    MvProjectionErrorKind::SourceConflict,
                    "MV observation belongs to another catalog binding or logical target",
                ));
            }
            if !observation
                .management_admission
                .matches(&observation.documents)
            {
                return Err(MvProjectionError::new(
                    MvProjectionErrorKind::SourceConflict,
                    "MV Current observation is not admitted for management",
                ));
            }
            let facts = MvDocumentProjection::try_from_current(
                observation.documents,
                observation.output_statistics,
            )
            .map_err(|error| {
                MvProjectionError::new(MvProjectionErrorKind::CorruptDocument, error)
            })?;
            Ok((facts, Some(observation.management_admission)))
        }
        .await;
        self.finish_observation(
            Some(request.context()),
            operation_id,
            reservation,
            result,
            true,
        )
        .await
    }

    /// Populate the canonical candidate inventory from sealed Current facts
    /// without granting refresh, DDL, dependency-guard, or scheduler
    /// readiness. A later successful management readmission must perform a
    /// fresh ordered observation before effect-capable consumers may proceed.
    pub async fn observe_current_read_only_and_install(
        &self,
        operation_id: Uuid,
        request: MvCurrentProjectionRequest,
        source: &dyn MvReadOnlyCurrentProjectionSource,
    ) -> Result<MvProjectionInstallOutcome, MvProjectionError> {
        let reservation = self.reserve(request.target.clone()).await?;
        let result = async {
            check_context(&request.context)?;
            let observation = source.observe_read_only(&request).await?;
            check_context(&request.context)?;
            if observation.documents.management_target.catalog() != request.catalog()
                || observation.documents.target.instance_id.as_str()
                    != request.target.catalog().unwrap_or_default()
                || observation.documents.target.namespace.as_ref() != request.target.namespace()
                || observation.documents.target.table.as_ref() != request.target.name()
            {
                return Err(MvProjectionError::new(
                    MvProjectionErrorKind::SourceConflict,
                    "MV read-only observation belongs to another catalog binding or logical target",
                ));
            }
            let facts = MvDocumentProjection::try_from_current(
                observation.documents,
                observation.output_statistics,
            )
            .map_err(|error| {
                MvProjectionError::new(MvProjectionErrorKind::CorruptDocument, error)
            })?;
            Ok((facts, None))
        }
        .await;
        self.finish_observation(
            Some(request.context()),
            operation_id,
            reservation,
            result,
            false,
        )
        .await
    }

    async fn finish_observation(
        &self,
        context: Option<&ConnectorRequestContext>,
        operation_id: Uuid,
        reservation: ProjectionReservation,
        result: Result<
            (MvDocumentProjection, Option<MvCurrentManagementAdmission>),
            MvProjectionError,
        >,
        publish_management_readiness: bool,
    ) -> Result<MvProjectionInstallOutcome, MvProjectionError> {
        let mut cell = reservation.order.lock().await;
        if cell.generation != reservation.generation {
            return Ok(MvProjectionInstallOutcome::Superseded);
        }
        // From here the cell is held to the end, so a waiter released now sees
        // this observation's outcome rather than its midpoint.
        cell.settle(reservation.generation);
        if !self.matches_repository(&reservation).await? {
            return Ok(MvProjectionInstallOutcome::Superseded);
        }
        let result = result.and_then(|facts| {
            if let Some(context) = context {
                check_context(context)?;
            }
            Ok(facts)
        });
        let (facts, management_admission) = match result {
            Ok(facts) => facts,
            Err(error) => {
                self.runtime
                    .set_unavailable(reservation.target, error.to_string());
                return Err(error);
            }
        };
        if facts.target() != &reservation.target {
            return Err(MvProjectionError::new(
                MvProjectionErrorKind::SourceConflict,
                "MV installation target changed",
            ));
        }
        if management_admission
            .as_ref()
            .is_some_and(|admission| !admission.is_open())
        {
            return Err(MvProjectionError::new(
                MvProjectionErrorKind::SourceConflict,
                "MV management admission closed before projection installation",
            ));
        }
        let (loaded, unchanged) = match reservation.expected {
            Some(expected)
                if expected.projection.facts.source_revision() == facts.source_revision() =>
            {
                (expected, true)
            }
            Some(expected) => {
                let result = self
                    .repository
                    .replace_projection(
                        operation_id,
                        ReplaceMvProjectionRequest {
                            mv_id: expected.projection.mv_id,
                            expected_version: expected.version,
                            projection: facts.into(),
                        },
                    )
                    .await;
                match result {
                    Ok(loaded) => (loaded, false),
                    Err(error) => {
                        self.runtime
                            .set_unavailable(reservation.target, error.to_string());
                        return Err(error.into());
                    }
                }
            }
            None => {
                // A materialized view is the target object it publishes into.
                // The same object is reachable through every catalog
                // attachment over its catalog, and a discovery through a
                // second attachment observes the same documents under a
                // different name -- so what it found is the projection that
                // already exists, not a new one.
                if let Some(existing) = self
                    .repository
                    .find_by_target_object(&facts.source_revision().target_object_id)
                    .await?
                {
                    let owner = existing.projection.facts.target().clone();
                    self.runtime.set_unavailable(
                        reservation.target,
                        format!(
                            "MV target object is already projected as {}.{}.{}",
                            owner.catalog().unwrap_or(""),
                            owner.namespace(),
                            owner.name()
                        ),
                    );
                    return Ok(MvProjectionInstallOutcome::AlreadyProjectedElsewhere(owner));
                }
                match self
                    .repository
                    .create_projection(operation_id, facts.into())
                    .await
                {
                    Ok(loaded) => (loaded, false),
                    Err(error) => {
                        self.runtime
                            .set_unavailable(reservation.target, error.to_string());
                        return Err(error.into());
                    }
                }
            }
        };
        // The provider observation and repository effect may both suspend.
        // A cancelled installer may leave a rebuildable cache record but must
        // never publish management readiness after its caller has stopped.
        if let Some(context) = context {
            if let Err(error) = check_context(context) {
                self.runtime
                    .set_unavailable(reservation.target, error.to_string());
                return Err(error);
            }
        }
        if management_admission
            .as_ref()
            .is_some_and(|admission| !admission.is_open())
        {
            self.runtime.set_unavailable(
                reservation.target,
                "MV management admission closed during projection installation".into(),
            );
            return Err(MvProjectionError::new(
                MvProjectionErrorKind::SourceConflict,
                "MV management admission closed during projection installation",
            ));
        }
        // A read-only observation that found the very projection this process
        // was already managing leaves that management where it was. Revoking
        // it would make a rediscovery -- which runs whenever a catalog is
        // admitted, not only at startup -- close management on a view this
        // process created and refreshes, and the next REFRESH or DROP would be
        // told the target has no successful fresh observation. A restart still
        // closes management, because a fresh process manages nothing yet.
        let keeps_established_management = !publish_management_readiness
            && unchanged
            && reservation.managed
            && reservation.installed_before.as_ref() == Some(&loaded.version);
        if publish_management_readiness || keeps_established_management {
            cell.installed = Some(loaded.version.clone());
            self.runtime.set_ready(reservation.target);
        } else {
            cell.installed = None;
            self.runtime.set_read_only(
                reservation.target,
                "MV projection is read-only until management readmission completes".into(),
            );
        }
        Ok(if unchanged {
            MvProjectionInstallOutcome::Unchanged(loaded)
        } else {
            MvProjectionInstallOutcome::Installed(loaded)
        })
    }

    /// Explicit management/catalog invalidation is a new ordered event, never
    /// the completion callback of an older observation.
    pub async fn invalidate_current(
        &self,
        target: MvTarget,
        reason: String,
    ) -> Result<(), MvProjectionError> {
        let order = self.runtime.projection_order(target.clone());
        let mut cell = order.lock().await;
        cell.advance()?;
        cell.installed = None;
        cell.supersede();
        self.runtime.set_unavailable(target, reason);
        Ok(())
    }
    pub async fn reserve_projection_delete(
        &self,
        target: MvTarget,
    ) -> Result<MvProjectionDeleteGuard, MvProjectionError> {
        Ok(MvProjectionDeleteGuard {
            reservation: self.reserve(target).await?,
        })
    }
    pub async fn delete_after_provider_drop(
        &self,
        operation_id: Uuid,
        guard: MvProjectionDeleteGuard,
    ) -> Result<MvProjectionInstallOutcome, MvProjectionError> {
        let reservation = guard.reservation;
        let mut cell = reservation.order.lock().await;
        if cell.generation != reservation.generation {
            return Ok(MvProjectionInstallOutcome::Superseded);
        }
        cell.settle(reservation.generation);
        if !self.matches_repository(&reservation).await? {
            return Ok(MvProjectionInstallOutcome::Superseded);
        }
        let Some(expected) = reservation.expected else {
            return Ok(MvProjectionInstallOutcome::AlreadyAbsent);
        };
        let removed = self
            .repository
            .delete_projection(
                operation_id,
                DeleteMvProjectionRequest {
                    mv_id: expected.projection.mv_id,
                    expected_version: expected.version,
                    expected_source_revision: expected.projection.facts.source_revision().clone(),
                },
            )
            .await?;
        cell.installed = None;
        self.runtime
            .set_unavailable(reservation.target, "MV target was removed".into());
        Ok(if removed {
            MvProjectionInstallOutcome::Removed
        } else {
            MvProjectionInstallOutcome::AlreadyAbsent
        })
    }
    /// Whether plain SQL may read one MV target's storage.
    ///
    /// Reading an MV is not a management operation, so a target whose
    /// management is closed pending readmission remains readable: its
    /// publication is exactly what the lake says it is. A target whose
    /// projection is itself in doubt is not readable, which is the case this
    /// answer exists to separate out.
    pub async fn query_admission(
        &self,
        target: &MvTarget,
    ) -> Result<MvQueryAdmission, MvRepositoryError> {
        if self.repository.find_by_target(target).await?.is_none() {
            return Ok(MvQueryAdmission::NotAnMv);
        }
        Ok(match self.runtime.readiness(target) {
            TargetReadiness::Ready | TargetReadiness::ReadOnly(_) => MvQueryAdmission::Admitted,
            TargetReadiness::Unavailable(reason) => MvQueryAdmission::Quarantined(reason),
            TargetReadiness::Unobserved => MvQueryAdmission::Quarantined(
                "MV target has not been observed in this process".to_string(),
            ),
        })
    }

    /// The management-facing read: the same answer as [`Self::load_ready`],
    /// except that an observation already in flight is waited for rather than
    /// reported as the absence of one.
    ///
    /// A statement that reaches this while a background refresh is mid-read
    /// would otherwise be told the target has no successful fresh observation
    /// -- about a target whose observation is succeeding as it asks. Waiting
    /// is what that sentence already means; it was simply not being done.
    /// Inventory scans deliberately do not use this: they visit every target
    /// and want whatever is known now.
    pub async fn load_ready_settled(
        &self,
        target: &MvTarget,
    ) -> Result<Option<LoadedMvProjection>, MvRepositoryError> {
        let order = self.runtime.projection_order(target.clone());
        let deadline = tokio::time::Instant::now() + OBSERVATION_SETTLE_WAIT;
        loop {
            let cell = order.lock().await;
            if cell.pending.is_none() {
                break;
            }
            // Register as a waiter before releasing the cell. `notify_waiters`
            // wakes only the waiters registered when it runs and leaves no
            // permit behind, and the settler holds this cell while it calls it
            // -- so enabling here, under the cell, is what makes the wake-up
            // unmissable. Merely constructing the future would not: it
            // registers nothing until first polled.
            let settled = std::sync::Arc::clone(&cell.settled);
            let notified = settled.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            drop(cell);
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                break;
            }
        }
        self.load_ready(target).await
    }

    pub async fn load_ready(
        &self,
        target: &MvTarget,
    ) -> Result<Option<LoadedMvProjection>, MvRepositoryError> {
        let order = self.runtime.projection_order(target.clone());
        let cell = order.lock().await;
        let Some(loaded) = self.repository.find_by_target(target).await? else {
            return Ok(None);
        };
        if !matches!(self.runtime.readiness(target), TargetReadiness::Ready)
            || cell.installed.as_ref() != Some(&loaded.version)
        {
            return Err(MvRepositoryError::new(
                MvRepositoryErrorKind::Unavailable,
                "MV target requires a successful fresh Current observation",
            ));
        }
        Ok(Some(loaded))
    }
    pub async fn list_ready_projections(
        &self,
    ) -> Result<Vec<LoadedMvProjection>, MvRepositoryError> {
        let mut result = Vec::new();
        for projection in self.repository.list_projections().await? {
            match self.load_ready(projection.projection.facts.target()).await {
                Ok(Some(loaded)) => result.push(loaded),
                Ok(None) => {}
                Err(error) if error.kind() == MvRepositoryErrorKind::Unavailable => {}
                Err(error) => return Err(error),
            }
        }
        Ok(result)
    }
    pub async fn list_ready_dependencies_by_downstream(
        &self,
        projection: &LoadedMvProjection,
    ) -> Result<Vec<crate::persistence::dependency::StoredMvDependency>, MvRepositoryError> {
        let current = self
            .load_ready(projection.projection.facts.target())
            .await?;
        if current.as_ref().map(|current| &current.version) != Some(&projection.version) {
            return Err(MvRepositoryError::new(
                MvRepositoryErrorKind::Conflict,
                "MV dependency root changed",
            ));
        }
        self.repository
            .list_dependencies_by_downstream(projection.projection.mv_id)
            .await
    }
    pub async fn ensure_no_ready_downstream_dependencies(
        &self,
        upstream: &MvDependencyObjectIdentity,
    ) -> Result<(), MvRepositoryError> {
        for projection in self.list_ready_projections().await? {
            if self
                .list_ready_dependencies_by_downstream(&projection)
                .await?
                .iter()
                .any(|dependency| {
                    dependency.upstream.catalog.as_deref()
                        == Some(upstream.catalog_instance.as_str())
                        && dependency.upstream_object_id.as_slice() == upstream.object_id.as_bytes()
                })
            {
                return Err(MvRepositoryError::new(
                    MvRepositoryErrorKind::Conflict,
                    "exact object has downstream materialized views",
                ));
            }
        }
        Ok(())
    }
    pub async fn prepare_drop(
        &self,
        target: &MvTarget,
        if_exists: bool,
    ) -> Result<MvDropReadiness, MvProjectionError> {
        let Some(loaded) = self.load_ready(target).await? else {
            return if if_exists {
                Ok(MvDropReadiness::AlreadyAbsent)
            } else {
                Err(MvProjectionError::new(
                    MvProjectionErrorKind::SourceConflict,
                    "materialized view does not exist",
                ))
            };
        };
        let source = loaded.projection.facts.source_revision();
        self.ensure_no_ready_downstream_dependencies(&MvDependencyObjectIdentity::new(
            source.target.instance_id.as_str(),
            source.target_object_id.clone(),
        ))
        .await?;
        Ok(MvDropReadiness::ReadyToDrop(
            self.reserve_projection_delete(target.clone()).await?,
        ))
    }
    pub fn begin_publication(
        &self,
        target: MvTarget,
        publication_id: LakePublicationId,
    ) -> Result<MvRuntimePublicationLease, MvRepositoryError> {
        if !self.runtime.begin(target.clone(), publication_id) {
            return Err(MvRepositoryError::new(
                MvRepositoryErrorKind::Conflict,
                "an MV publication is already active for this target",
            ));
        }
        Ok(MvRuntimePublicationLease {
            runtime: Arc::clone(&self.runtime),
            target,
            publication_id,
        })
    }
    pub fn ensure_no_active_publications(&self) -> Result<(), MvRepositoryError> {
        if self.runtime.has_active_publications() {
            Err(MvRepositoryError::new(
                MvRepositoryErrorKind::Conflict,
                "cannot wipe MV Accelerator while an MV publication is active",
            ))
        } else {
            Ok(())
        }
    }
    pub async fn wipe_accelerator(&self, operation_id: Uuid) -> Result<(), MvProjectionError> {
        self.ensure_no_active_publications()?;
        for target in self.runtime.projection_targets() {
            self.invalidate_current(target, "MV Accelerator wipe".into())
                .await?;
        }
        self.repository.wipe_accelerator(operation_id).await?;
        Ok(())
    }
    pub async fn wipe_projection(
        &self,
        operation_id: Uuid,
        target: &MvTarget,
    ) -> Result<bool, MvProjectionError> {
        let guard = self.reserve_projection_delete(target.clone()).await?;
        Ok(matches!(
            self.delete_after_provider_drop(operation_id, guard).await?,
            MvProjectionInstallOutcome::Removed
        ))
    }

    #[cfg(any(test, feature = "test-support"))]
    #[doc(hidden)]
    pub async fn seed_projection(
        &self,
        operation_id: Uuid,
        facts: MvDocumentProjection,
    ) -> Result<MvProjectionInstallOutcome, MvProjectionError> {
        let reservation = self.reserve(facts.target().clone()).await?;
        self.finish_observation(None, operation_id, reservation, Ok((facts, None)), true)
            .await
    }
}
impl MvCandidateReader {
    /// Inventory only. Consumers independently freeze and prove exact historical reads.
    pub async fn list_candidate_definitions(
        &self,
    ) -> Result<Vec<StoredMvProjection>, MvRepositoryError> {
        Ok(self
            .repository
            .list_projections()
            .await?
            .into_iter()
            .map(|value| value.projection)
            .collect())
    }
}
fn check_context(context: &ConnectorRequestContext) -> Result<(), MvProjectionError> {
    if context.cancellation().is_cancelled() {
        return Err(MvProjectionError::new(
            MvProjectionErrorKind::Cancelled,
            "MV Current observation cancelled",
        ));
    }
    if std::time::Instant::now() >= context.deadline() {
        return Err(MvProjectionError::new(
            MvProjectionErrorKind::DeadlineExceeded,
            "MV Current observation deadline elapsed",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "readiness_tests.rs"]
mod tests;

/// How long a management read waits for an observation that is already in
/// flight before answering from what the process knows now.
///
/// The wait exists because a management read that arrives mid-observation
/// would otherwise report "no successful fresh observation" about a target
/// whose observation is succeeding as it asks -- which is what a background
/// refresh running beside a user statement makes routine. The bound exists
/// because a reservation whose owner was dropped without settling would
/// otherwise hold the reader forever: after it, the reader answers exactly as
/// it did before this wait existed.
const OBSERVATION_SETTLE_WAIT: std::time::Duration = std::time::Duration::from_secs(10);
