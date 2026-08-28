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

//! Frontend implementation of the catalog application boundary.
//!
//! Every reconcile runs one path: enumerate a complete
//! [`CatalogDesiredStateSnapshot`] from the selected source, validate it, then
//! materialize each located entry on its own. The two failure scopes that path
//! produces are carried by the error type rather than by which call happened to
//! propagate — see [`CatalogApplicationErrorKind::DesiredStateEnumerationIncomplete`]
//! for the global one and [`FrontendCatalogApplicationPort::materialize_entry`]
//! for the per-catalog one.
//!
//! Desired state is committed before a local control generation is registered.
//! A registration failure therefore leaves the source's truth intact and is
//! reported as `Unavailable`; reconciliation can retry installation.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use super::desired_state::{
    CatalogDesiredStateEntry, CatalogDesiredStateSnapshot, CatalogDesiredStateSource,
    CatalogDesiredStateSourceMode,
};
use super::{
    CatalogAdmission, CatalogApplicationError, CatalogApplicationErrorKind, CatalogApplicationPort,
    CatalogCreateCommand, CatalogDropCommand, CatalogRuntimeObservation,
    CatalogRuntimePublisherSink,
};
use crate::mv::domain::repository::{MvRepositoryError, MvRepositoryErrorKind};
use novarocks_spi::connector::{
    CatalogCredentialBinding, CatalogCredentialMode, CatalogCredentialPurpose, CatalogHandle,
    ConnectorControlFactoryRequest, ConnectorControlFactoryResolver, ConnectorControlResolver,
    ConnectorInstanceId, ConnectorProviderId, CredentialConsumerRole, StaticCredentialReference,
    canonicalize_catalog_credential_bindings,
};
use tokio::runtime::{Handle, RuntimeFlavor};
use uuid::Uuid;

use crate::catalog_attachment::{
    CatalogAttachment, CatalogAttachmentError, CatalogAttachmentErrorKind,
    CatalogAttachmentRepository,
};
use crate::connector::ConnectorControlHost;

#[derive(Clone, Debug, Eq, PartialEq)]
enum LocalProjection {
    Unavailable {
        attachment_id: Uuid,
        provider_id: ConnectorProviderId,
        reason: String,
    },
    Ready {
        attachment_id: Uuid,
        provider_id: ConnectorProviderId,
        generation: u64,
    },
}

/// Aggregate local materialization result for one exact desired-state snapshot.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct CatalogProjectionCounts {
    pub(crate) ready: usize,
    pub(crate) unavailable: usize,
}

impl LocalProjection {
    fn attachment_id(&self) -> Uuid {
        match self {
            Self::Unavailable { attachment_id, .. } | Self::Ready { attachment_id, .. } => {
                *attachment_id
            }
        }
    }

    fn ready_generation(&self) -> Option<u64> {
        match self {
            Self::Ready { generation, .. } => Some(*generation),
            Self::Unavailable { .. } => None,
        }
    }
}

/// Owns catalog desired-state mutation and the local Connector control
/// projection.
///
/// The source is taken by value at construction and never replaced, so which
/// authority owns catalog desired state is a composition-time fact of this
/// process. `None` means this frontend was composed without any source at all
/// — a role that never serves external catalogs — which is a different thing
/// from a source that exists and is failing.
// Design: ADR-0115 (docs/adr/ADR-0115-catalog-desired-state-source-modes.md)
pub struct FrontendCatalogApplicationPort {
    source: Option<CatalogDesiredStateSource>,
    control: Arc<ConnectorControlHost>,
    runtime_publisher: Arc<dyn CatalogRuntimePublisherSink>,
    runtime: Handle,
    projections: Mutex<BTreeMap<ConnectorInstanceId, LocalProjection>>,
    complete_reachable_catalogs: Mutex<Option<BTreeSet<CatalogHandle>>>,
    next_generation: AtomicU64,
}

impl FrontendCatalogApplicationPort {
    pub fn unavailable(
        control: Arc<ConnectorControlHost>,
        runtime_publisher: Arc<dyn CatalogRuntimePublisherSink>,
        runtime: Handle,
    ) -> Self {
        Self {
            source: None,
            control,
            runtime_publisher,
            runtime,
            projections: Mutex::new(BTreeMap::new()),
            complete_reachable_catalogs: Mutex::new(None),
            next_generation: AtomicU64::new(1),
        }
    }

    pub fn new(
        source: CatalogDesiredStateSource,
        control: Arc<ConnectorControlHost>,
        runtime_publisher: Arc<dyn CatalogRuntimePublisherSink>,
        runtime: Handle,
    ) -> Self {
        Self {
            source: Some(source),
            control,
            runtime_publisher,
            runtime,
            projections: Mutex::new(BTreeMap::new()),
            complete_reachable_catalogs: Mutex::new(None),
            next_generation: AtomicU64::new(1),
        }
    }

    fn source(&self) -> Result<&CatalogDesiredStateSource, CatalogApplicationError> {
        self.source.as_ref().ok_or_else(|| {
            CatalogApplicationError::new(
                CatalogApplicationErrorKind::Unavailable,
                "this frontend has no configured catalog desired-state source",
            )
        })
    }

    /// A trustworthy desired-state set plus every still-draining local control
    /// generation. `None` means this FE has never completed a source
    /// enumeration, so a prune sender must skip its round rather than send an
    /// empty or partial snapshot.
    pub(crate) fn reachable_catalog_handles(&self) -> Option<BTreeSet<CatalogHandle>> {
        let mut reachable = self.complete_reachable_catalogs.lock().ok()?.clone()?;
        reachable.extend(self.control.reachable_catalog_handles().ok()?);
        Some(reachable)
    }

    /// The authority a SQL `CREATE`/`DROP CATALOG` writes through.
    ///
    /// Admission is a function of the selected source mode, so a deployment
    /// whose desired state comes from a file or a controller never reaches a
    /// repository here: it is refused with
    /// [`CatalogApplicationErrorKind::UnsupportedSourceMode`] instead, which is
    /// what keeps one truth from having two writers.
    fn sql_mutation_authority(
        &self,
    ) -> Result<&CatalogAttachmentRepository, CatalogApplicationError> {
        self.source()?.sql_mutation_authority()
    }

    fn block_on<T>(
        &self,
        future: impl Future<Output = Result<T, CatalogAttachmentError>>,
    ) -> Result<T, CatalogApplicationError> {
        let result = match Handle::try_current() {
            Ok(_) if self.runtime.runtime_flavor() == RuntimeFlavor::CurrentThread => {
                return Err(CatalogApplicationError::new(
                    CatalogApplicationErrorKind::Unavailable,
                    "catalog attachment StateStore access is unavailable on a current-thread Tokio runtime",
                ));
            }
            Ok(_) => tokio::task::block_in_place(|| self.runtime.block_on(future)),
            Err(_) => self.runtime.block_on(future),
        };
        result.map_err(repository_error)
    }

    fn next_projection_generation(&self) -> u64 {
        self.next_generation.fetch_add(1, Ordering::Relaxed)
    }

    fn observation(&self, instance_id: &ConnectorInstanceId) -> CatalogAdmission {
        let projection = match self.projections.lock() {
            Ok(projections) => projections.get(instance_id).cloned(),
            Err(_) => {
                return CatalogAdmission::Unavailable {
                    reason: "catalog projection lock is poisoned".to_string(),
                };
            }
        };
        let Some(projection) = projection else {
            return CatalogAdmission::Absent;
        };
        match projection {
            LocalProjection::Unavailable { reason, .. } => CatalogAdmission::Unavailable { reason },
            LocalProjection::Ready {
                attachment_id,
                provider_id,
                generation,
            } => match self.control.observe_current_binding(instance_id) {
                Ok(_) => CatalogAdmission::Ready(CatalogRuntimeObservation {
                    attachment_id,
                    instance_id: instance_id.clone(),
                    provider_id,
                    generation,
                }),
                Err(error) => CatalogAdmission::Unavailable {
                    reason: error.to_string(),
                },
            },
        }
    }

    fn mark_unavailable(
        &self,
        instance_id: &ConnectorInstanceId,
        attachment_id: Uuid,
        provider_id: &ConnectorProviderId,
        reason: impl Into<String>,
    ) {
        let previous = self.projections.lock().ok().and_then(|mut projections| {
            projections.insert(
                instance_id.clone(),
                LocalProjection::Unavailable {
                    attachment_id,
                    provider_id: provider_id.clone(),
                    reason: reason.into(),
                },
            )
        });
        if let Some(generation) = previous
            .as_ref()
            .and_then(LocalProjection::ready_generation)
            && let Err(error) = self
                .runtime_publisher
                .unpublish_catalog_runtime(instance_id, generation)
        {
            tracing::warn!(%error, catalog = instance_id.as_str(), "catalog runtime unpublish failed while marking projection unavailable");
        }
        if previous.is_some()
            && let Err(error) = self.control.retire_current(instance_id)
        {
            tracing::debug!(%error, catalog = instance_id.as_str(), "catalog runtime was not locally active while marking projection unavailable");
        }
    }

    fn install_created(
        &self,
        entry: &CatalogDesiredStateEntry,
        binding: novarocks_spi::connector::ConnectorControlBinding,
    ) -> Result<CatalogRuntimeObservation, CatalogApplicationError> {
        let attachment_id = entry.identity().as_uuid();
        let instance_id = entry.config().instance_id();
        let provider_id = entry.config().provider_id();
        self.control.register(binding).map_err(connector_error)?;
        let generation = self.next_projection_generation();
        let observation = CatalogRuntimeObservation {
            attachment_id,
            instance_id: instance_id.clone(),
            provider_id: provider_id.clone(),
            generation,
        };
        if let Err(error) = self
            .runtime_publisher
            .publish_catalog_runtime(observation.clone())
        {
            let _ = self.control.retire_current(instance_id);
            return Err(error);
        }
        let projection = LocalProjection::Ready {
            attachment_id,
            provider_id: provider_id.clone(),
            generation,
        };
        let publish_result = self
            .projections
            .lock()
            .map_err(|_| {
                CatalogApplicationError::new(
                    CatalogApplicationErrorKind::Internal,
                    "catalog projection lock is poisoned",
                )
            })
            .and_then(|mut projections| match projections.get(instance_id) {
                Some(LocalProjection::Unavailable {
                    attachment_id: installed_id,
                    provider_id: installed_provider,
                    ..
                }) if *installed_id == attachment_id && installed_provider == provider_id => {
                    projections.insert(instance_id.clone(), projection);
                    Ok(())
                }
                _ => Err(CatalogApplicationError::new(
                    CatalogApplicationErrorKind::Conflict,
                    "catalog projection changed before its runtime became ready",
                )),
            });
        if let Err(error) = publish_result {
            let _ = self
                .runtime_publisher
                .unpublish_catalog_runtime(instance_id, generation);
            let _ = self.control.retire_current(instance_id);
            if let Ok(mut projections) = self.projections.lock()
                && projections
                    .get(instance_id)
                    .is_some_and(|projection| projection.attachment_id() == attachment_id)
            {
                projections.insert(
                    instance_id.clone(),
                    LocalProjection::Unavailable {
                        attachment_id,
                        provider_id: provider_id.clone(),
                        reason: error.to_string(),
                    },
                );
            }
            return Err(error);
        }
        Ok(observation)
    }

    /// Rebuilds this process's control projection from the selected source's
    /// desired state, as `enumerate -> validate -> per-catalog materialize`.
    ///
    /// A change hint never carries desired state; callers always invoke this
    /// method after rereading the source. Factory and registration work is
    /// bounded because provider materialization can synchronously perform
    /// remote validation.
    ///
    /// The two failure scopes are expressed by the type of what fails, not by
    /// which statement happens to use `?`:
    ///
    /// * `enumerate` returns a whole snapshot or
    ///   [`CatalogApplicationErrorKind::DesiredStateEnumerationIncomplete`], so
    ///   an incomplete enumeration propagates and fails frontend bootstrap. It
    ///   cannot arrive here as a valid snapshot holding fewer catalogs, which
    ///   would retire the ones it lost.
    /// * [`FrontendCatalogApplicationPort::materialize_entry`] returns `()`, so
    ///   one catalog's provider failure cannot reach this function's `Result`
    ///   at all — it marks that catalog `Unavailable` and leaves the rest
    ///   serving.
    pub(crate) async fn reconcile_with_page_size(
        self: &Arc<Self>,
        page_size: usize,
        worker_count: usize,
    ) -> Result<(), CatalogApplicationError> {
        self.reconcile_snapshot_with_page_size(page_size, worker_count)
            .await
            .map(|_| ())
    }

    /// Reconciles one complete source snapshot and returns its exact identity
    /// together with this process's materialization counts.  The source is
    /// enumerated exactly once; callers must not reread it merely to publish
    /// bootstrap observability.
    pub(crate) async fn reconcile_snapshot_with_page_size(
        self: &Arc<Self>,
        page_size: usize,
        worker_count: usize,
    ) -> Result<(CatalogDesiredStateSnapshot, CatalogProjectionCounts), CatalogApplicationError>
    {
        if worker_count == 0 {
            return Err(CatalogApplicationError::new(
                CatalogApplicationErrorKind::InvalidRequest,
                "catalog projection worker count must be positive",
            ));
        }
        let source = self.source()?;
        let snapshot = source.enumerate(page_size).await?;
        let reachable = snapshot
            .catalog_properties()?
            .into_iter()
            .map(|properties| properties.handle().clone())
            .collect::<BTreeSet<_>>();
        tracing::debug!(
            source_mode = snapshot.mode().as_str(),
            snapshot = snapshot.identity().short_digest(),
            catalogs = snapshot.identity().catalog_count(),
            "catalog desired-state snapshot enumerated"
        );
        *self.complete_reachable_catalogs.lock().map_err(|_| {
            CatalogApplicationError::new(
                CatalogApplicationErrorKind::Internal,
                "catalog reachable snapshot lock is poisoned",
            )
        })? = Some(reachable);
        self.retire_projections_absent_from(source, &snapshot)
            .await?;

        let mut workers = tokio::task::JoinSet::new();
        let mode = snapshot.mode();
        for entry in snapshot.clone().into_entries() {
            if workers.len() >= worker_count {
                let completed = workers.join_next().await.ok_or_else(|| {
                    CatalogApplicationError::new(
                        CatalogApplicationErrorKind::Internal,
                        "catalog projection worker exited unexpectedly",
                    )
                })?;
                completed.map_err(|error| {
                    CatalogApplicationError::new(
                        CatalogApplicationErrorKind::Internal,
                        format!("catalog projection worker failed: {error}"),
                    )
                })?;
            }
            let projection = Arc::clone(self);
            workers.spawn_blocking(move || projection.materialize_entry(entry, mode));
        }
        while let Some(completed) = workers.join_next().await {
            completed.map_err(|error| {
                CatalogApplicationError::new(
                    CatalogApplicationErrorKind::Internal,
                    format!("catalog projection worker failed: {error}"),
                )
            })?;
        }
        Ok((snapshot, self.projection_counts()))
    }

    /// Retires every local projection the snapshot no longer declares.
    ///
    /// This is the step that makes a snapshot total truth rather than additive
    /// seeds: a catalog absent from the source is not unmentioned, it is not
    /// wanted, so its projection has to go.
    ///
    /// A projection missing from the snapshot is not proof that its catalog is
    /// gone, though: `create_catalog` commits desired state and only then
    /// installs the projection, so a catalog created after the enumeration
    /// began is present locally and absent from the snapshot. Retiring on that
    /// alone made the statement right after CREATE EXTERNAL CATALOG fail with
    /// "unknown catalog" whenever a reconcile cycle straddled it — after a
    /// create that reported success.
    ///
    /// Re-reading each candidate closes the window rather than narrowing it:
    /// the projection can only exist because desired state was already
    /// committed, so a read issued after observing the projection sees it.
    async fn retire_projections_absent_from(
        &self,
        source: &CatalogDesiredStateSource,
        snapshot: &CatalogDesiredStateSnapshot,
    ) -> Result<(), CatalogApplicationError> {
        let candidates = self
            .projections
            .lock()
            .map_err(|_| {
                CatalogApplicationError::new(
                    CatalogApplicationErrorKind::Internal,
                    "catalog projection lock is poisoned",
                )
            })?
            .iter()
            .filter(|(instance_id, _)| !snapshot.wants(instance_id))
            .map(|(instance_id, projection)| (instance_id.clone(), projection.attachment_id()))
            .collect::<Vec<_>>();
        for (instance_id, attachment_id) in candidates {
            match source.locate(&instance_id).await {
                Ok(Some(entry)) if entry.identity().as_uuid() == attachment_id => {}
                Ok(_) => self.retire_projection(&instance_id),
                // Keep serving and retry next cycle: the read failed, so
                // nothing was proven about desired state either way.
                Err(error) => tracing::warn!(
                    %error,
                    catalog = instance_id.as_str(),
                    "catalog desired-state re-read failed while retiring a projection absent from the snapshot",
                ),
            }
        }
        Ok(())
    }

    /// Materializes one located entry into a local runtime generation.
    ///
    /// Returns nothing on purpose. The entry exists only because a complete
    /// enumeration produced it, so its provider failing says nothing about the
    /// snapshot; giving this function a `Result` would let one broken catalog
    /// abort the reconcile of every healthy one, which is the failure scope
    /// this design exists to keep separate.
    fn materialize_entry(
        &self,
        entry: CatalogDesiredStateEntry,
        mode: CatalogDesiredStateSourceMode,
    ) {
        let attachment_id = entry.identity().as_uuid();
        let instance_id = entry.config().instance_id().clone();
        let provider_id = entry.config().provider_id().clone();
        let installed = self
            .projections
            .lock()
            .map(|projections| {
                projections.get(&instance_id).is_some_and(|projection| {
                    matches!(
                        projection,
                        LocalProjection::Ready { attachment_id: installed_id, .. }
                            if *installed_id == attachment_id
                    )
                })
            })
            .unwrap_or(false)
            && self.control.observe_current_binding(&instance_id).is_ok();
        if installed {
            return;
        }

        self.mark_unavailable(
            &instance_id,
            attachment_id,
            &provider_id,
            "catalog desired-state runtime is being materialized",
        );
        let installed = (|| {
            let request = ConnectorControlFactoryRequest::try_new(
                provider_id.clone(),
                instance_id.clone(),
                entry.config().durable_properties().to_vec(),
            )
            .map_err(connector_error)?;
            let creation = self
                .control
                .create_control(request)
                .map_err(connector_error)?;
            let (binding, _) = creation.into_parts();
            let binding = binding
                .with_catalog_properties(entry.catalog_properties(mode)?)
                .map_err(connector_error)?;
            self.install_created(&entry, binding).map(|_| ())
        })();
        if let Err(error) = installed {
            self.mark_unavailable(&instance_id, attachment_id, &provider_id, error.to_string());
            // A single provider failure must not make the source's truth
            // disappear or prevent unrelated catalog projections. Its admission
            // remains Unavailable until a later resync works, and that retry
            // needs nothing beyond another successful global enumeration.
            tracing::warn!(%error, catalog = instance_id.as_str(), "catalog remains unavailable after projection attempt");
        }
    }

    /// Stops all local admission before retiring existing leases. Durable
    /// attachments remain unchanged, so a later authoritative reconcile can
    /// construct fresh generations after a freshness outage.
    pub(crate) fn unpublish_all(&self) {
        let attachments = self
            .projections
            .lock()
            .map(|projections| {
                projections
                    .iter()
                    .map(|(instance_id, projection)| match projection {
                        LocalProjection::Unavailable {
                            attachment_id,
                            provider_id,
                            ..
                        }
                        | LocalProjection::Ready {
                            attachment_id,
                            provider_id,
                            ..
                        } => (instance_id.clone(), *attachment_id, provider_id.clone()),
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        for (instance_id, attachment_id, provider_id) in attachments {
            self.mark_unavailable(
                &instance_id,
                attachment_id,
                &provider_id,
                "catalog attachment projection freshness expired",
            );
        }
    }

    pub(crate) fn projection_count(&self) -> usize {
        self.projections
            .lock()
            .map(|projections| {
                projections
                    .values()
                    .filter(|projection| matches!(projection, LocalProjection::Ready { .. }))
                    .count()
            })
            .unwrap_or_default()
    }

    pub(crate) fn projection_counts(&self) -> CatalogProjectionCounts {
        self.projections
            .lock()
            .map(|projections| {
                projections.values().fold(
                    CatalogProjectionCounts::default(),
                    |mut counts, projection| {
                        match projection {
                            LocalProjection::Ready { .. } => counts.ready += 1,
                            LocalProjection::Unavailable { .. } => counts.unavailable += 1,
                        }
                        counts
                    },
                )
            })
            .unwrap_or_default()
    }

    fn retire_projection(&self, instance_id: &ConnectorInstanceId) {
        let projection = self
            .projections
            .lock()
            .ok()
            .and_then(|mut projections| projections.remove(instance_id));
        if let Some(generation) = projection
            .as_ref()
            .and_then(LocalProjection::ready_generation)
            && let Err(error) = self
                .runtime_publisher
                .unpublish_catalog_runtime(instance_id, generation)
        {
            tracing::warn!(%error, catalog = instance_id.as_str(), "catalog runtime unpublish failed during retirement");
        }
        if let Err(error) = self.control.retire_current(instance_id) {
            tracing::debug!(%error, catalog = instance_id.as_str(), "catalog runtime was not locally active during retirement");
        }
    }
}

impl CatalogApplicationPort for FrontendCatalogApplicationPort {
    fn create_catalog(
        &self,
        command: CatalogCreateCommand,
    ) -> Result<CatalogRuntimeObservation, CatalogApplicationError> {
        let repository = self.sql_mutation_authority()?;
        if self
            .block_on(repository.get(&command.instance_id))?
            .is_some()
        {
            if !command.if_not_exists {
                return Err(CatalogApplicationError::new(
                    CatalogApplicationErrorKind::AlreadyExists,
                    "catalog attachment already exists",
                ));
            }
            return self
                .admit_catalog(&command.instance_id)
                .require_ready(&command.instance_id);
        }

        let provider_id = provider_id_from_properties(&command.properties)?;
        let request = ConnectorControlFactoryRequest::try_new(
            provider_id.clone(),
            command.instance_id.clone(),
            command.properties,
        )
        .map_err(connector_error)?;
        // The factory may validate provider configuration, but it does not
        // become live until after the attachment CAS succeeds below.
        let creation = self
            .control
            .create_control(request)
            .map_err(connector_error)?;
        let (binding, mut durable_properties) = creation.into_parts();
        durable_properties.sort_by(|left, right| left.0.cmp(&right.0));
        let attachment = CatalogAttachment {
            attachment_id: Uuid::now_v7(),
            instance_id: command.instance_id,
            provider_id,
            display_name: command.display_name,
            durable_properties,
            created_at_ms: chrono::Utc::now().timestamp_millis(),
        };
        let created = self.block_on(repository.create(attachment))?;
        // CREATE materializes through the same located-entry type a reconcile
        // uses, so a freshly created catalog and a rediscovered one install
        // through one code path instead of two that can drift.
        let entry = CatalogDesiredStateEntry::from_attachment(&created.attachment);
        self.mark_unavailable(
            entry.config().instance_id(),
            entry.identity().as_uuid(),
            entry.config().provider_id(),
            "catalog desired-state runtime is being installed",
        );
        let binding = binding
            .with_catalog_properties(
                entry.catalog_properties(CatalogDesiredStateSourceMode::DynamicStateStore)?,
            )
            .map_err(connector_error)?;
        self.install_created(&entry, binding)
    }

    fn drop_catalog(&self, command: CatalogDropCommand) -> Result<(), CatalogApplicationError> {
        let repository = self.sql_mutation_authority()?;
        let Some(existing) = self.block_on(repository.get(&command.instance_id))? else {
            return if command.if_exists {
                Ok(())
            } else {
                Err(CatalogApplicationError::new(
                    CatalogApplicationErrorKind::NotFound,
                    "catalog attachment was not found",
                ))
            };
        };
        // Ordering, not atomicity: the reference check runs here, before the
        // delete and outside it, and the delete below is a single-family
        // transaction on the catalog attachment record alone.
        //
        // The check used to be a scan inside that transaction, which read as a
        // cross-family serializability fence against MV DDL. It is now an
        // operational check that can miss — a wiped or unreadable MV
        // Accelerator observes nothing, and MV DDL elsewhere can land right
        // after the observation. What escapes it is bounded to an MV whose
        // catalog is gone, which the MV side already refuses through its
        // unavailable/fail-closed paths rather than publishing anything wrong
        // to the lake.
        self.block_on(repository.observe_materialized_view_references(&command.instance_id, 256))?;
        self.block_on(repository.drop_exact(existing))?;
        self.retire_projection(&command.instance_id);
        // Durable deletion is authoritative. A local generation can be absent
        // or already retiring; either case converges through reconciliation.
        Ok(())
    }

    fn admit_catalog(&self, instance_id: &ConnectorInstanceId) -> CatalogAdmission {
        if self.source.is_none() {
            return CatalogAdmission::Unavailable {
                reason: "this frontend has no configured catalog desired-state source".to_string(),
            };
        }
        self.observation(instance_id)
    }
}

fn provider_id_from_properties(
    properties: &[(String, String)],
) -> Result<ConnectorProviderId, CatalogApplicationError> {
    let mut providers = properties
        .iter()
        .filter(|(key, _)| key.eq_ignore_ascii_case("type"))
        .map(|(_, value)| value.as_str());
    let Some(provider) = providers.next() else {
        return Err(CatalogApplicationError::new(
            CatalogApplicationErrorKind::InvalidRequest,
            "CREATE CATALOG requires exactly one type property",
        ));
    };
    if providers.next().is_some() {
        return Err(CatalogApplicationError::new(
            CatalogApplicationErrorKind::InvalidRequest,
            "CREATE CATALOG requires exactly one type property",
        ));
    }
    ConnectorProviderId::parse(provider).map_err(|error| {
        CatalogApplicationError::new(
            CatalogApplicationErrorKind::InvalidRequest,
            error.to_string(),
        )
    })
}

fn repository_error(error: CatalogAttachmentError) -> CatalogApplicationError {
    let kind = match error.kind() {
        CatalogAttachmentErrorKind::InvalidRequest => CatalogApplicationErrorKind::InvalidRequest,
        CatalogAttachmentErrorKind::NotFound => CatalogApplicationErrorKind::NotFound,
        CatalogAttachmentErrorKind::AlreadyExists => CatalogApplicationErrorKind::AlreadyExists,
        CatalogAttachmentErrorKind::Conflict => CatalogApplicationErrorKind::Conflict,
        CatalogAttachmentErrorKind::Unavailable | CatalogAttachmentErrorKind::CommitUnknown => {
            CatalogApplicationErrorKind::Unavailable
        }
        CatalogAttachmentErrorKind::Corruption => CatalogApplicationErrorKind::Internal,
    };
    CatalogApplicationError::new(kind, error.to_string())
}

fn connector_error(error: novarocks_spi::connector::ConnectorError) -> CatalogApplicationError {
    use novarocks_spi::connector::ConnectorErrorKind;

    let kind = match error.kind() {
        ConnectorErrorKind::InvalidRequest => CatalogApplicationErrorKind::InvalidRequest,
        ConnectorErrorKind::NotFound => CatalogApplicationErrorKind::Unavailable,
        ConnectorErrorKind::Unavailable
        | ConnectorErrorKind::ResourceExhausted
        | ConnectorErrorKind::DeadlineExceeded
        | ConnectorErrorKind::Cancelled => CatalogApplicationErrorKind::Unavailable,
        ConnectorErrorKind::PermissionDenied
        | ConnectorErrorKind::Unsupported
        | ConnectorErrorKind::CorruptData
        | ConnectorErrorKind::Internal => CatalogApplicationErrorKind::Internal,
    };
    CatalogApplicationError::new(kind, error.to_string())
}

fn mv_repository_error(error: CatalogApplicationError) -> MvRepositoryError {
    let kind = match error.kind() {
        // A source mode that forbids the operation refuses it permanently, so
        // it is a request-level rejection rather than an outage to retry.
        CatalogApplicationErrorKind::InvalidRequest
        | CatalogApplicationErrorKind::UnsupportedSourceMode => {
            MvRepositoryErrorKind::InvalidRequest
        }
        CatalogApplicationErrorKind::NotFound
        | CatalogApplicationErrorKind::AlreadyExists
        | CatalogApplicationErrorKind::Conflict => MvRepositoryErrorKind::Conflict,
        // The source could not be read completely; a later attempt may succeed,
        // and nothing about desired state was proven either way.
        CatalogApplicationErrorKind::Unavailable
        | CatalogApplicationErrorKind::DesiredStateEnumerationIncomplete => {
            MvRepositoryErrorKind::Unavailable
        }
        CatalogApplicationErrorKind::Internal => MvRepositoryErrorKind::Corruption,
    };
    MvRepositoryError::new(kind, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_catalog_requires_one_type_property() {
        assert_eq!(
            provider_id_from_properties(&[])
                .expect_err("missing type must fail")
                .kind(),
            CatalogApplicationErrorKind::InvalidRequest
        );
        assert_eq!(
            provider_id_from_properties(&[
                ("type".to_string(), "iceberg".to_string()),
                ("TYPE".to_string(), "starrocks".to_string()),
            ])
            .expect_err("duplicate type must fail")
            .kind(),
            CatalogApplicationErrorKind::InvalidRequest
        );
        assert_eq!(
            provider_id_from_properties(&[("type".to_string(), "iceberg".to_string())])
                .expect("one type")
                .as_str(),
            "iceberg"
        );
    }

    #[tokio::test]
    async fn prune_reachability_requires_a_complete_snapshot_but_allows_complete_empty() {
        let projection = crate::catalog_application::CatalogRuntimeProjection::new();
        let port = Arc::new(FrontendCatalogApplicationPort::new(
            CatalogDesiredStateSource::static_file(
                CatalogDesiredStateSnapshot::try_new(CatalogDesiredStateSourceMode::StaticFile, [])
                    .expect("empty static snapshot"),
            )
            .expect("static desired-state source"),
            Arc::new(ConnectorControlHost::new()),
            projection.publisher(),
            Handle::current(),
        ));

        assert_eq!(port.reachable_catalog_handles(), None);
        port.reconcile_snapshot_with_page_size(1, 1)
            .await
            .expect("complete empty snapshot reconciles");
        assert_eq!(port.reachable_catalog_handles(), Some(BTreeSet::new()));
    }
}
