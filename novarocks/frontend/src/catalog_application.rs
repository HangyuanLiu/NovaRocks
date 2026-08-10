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

//! Frontend implementation of Core's catalog application boundary.
//!
//! The StateStore attachment is committed before a local control generation is
//! registered. A registration failure therefore leaves durable truth intact
//! and is reported as `Unavailable`; reconciliation can retry installation.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use novarocks::catalog_application::{
    CatalogAdmission, CatalogApplicationError, CatalogApplicationErrorKind, CatalogApplicationPort,
    CatalogCreateCommand, CatalogDropCommand, CatalogRuntimeObservation,
};
use novarocks::mv::repository::{MvRepositoryError, MvRepositoryErrorKind};
use novarocks_spi::connector::{
    ConnectorControlFactoryRequest, ConnectorControlFactoryResolver, ConnectorControlResolver,
    ConnectorInstanceId, ConnectorProviderId,
};
use tokio::runtime::{Handle, RuntimeFlavor};
use uuid::Uuid;

use crate::catalog_attachment::{
    CatalogAttachment, CatalogAttachmentError, CatalogAttachmentErrorKind,
    CatalogAttachmentRepository,
};
use crate::connector::ConnectorControlHost;
use crate::mv::repository::CatalogAttachmentObservationSource;

#[derive(Clone, Debug, Eq, PartialEq)]
struct LocalProjection {
    attachment_id: Uuid,
    provider_id: ConnectorProviderId,
    generation: u64,
}

/// Owns durable attachment mutation and the local Connector control projection.
pub struct FrontendCatalogApplicationPort {
    repository: Option<CatalogAttachmentRepository>,
    control: Arc<ConnectorControlHost>,
    runtime: Handle,
    projections: Mutex<BTreeMap<ConnectorInstanceId, LocalProjection>>,
    next_generation: AtomicU64,
}

impl FrontendCatalogApplicationPort {
    pub fn unavailable(control: Arc<ConnectorControlHost>, runtime: Handle) -> Self {
        Self {
            repository: None,
            control,
            runtime,
            projections: Mutex::new(BTreeMap::new()),
            next_generation: AtomicU64::new(1),
        }
    }

    pub fn new(
        repository: CatalogAttachmentRepository,
        control: Arc<ConnectorControlHost>,
        runtime: Handle,
    ) -> Self {
        Self {
            repository: Some(repository),
            control,
            runtime,
            projections: Mutex::new(BTreeMap::new()),
            next_generation: AtomicU64::new(1),
        }
    }

    fn repository(&self) -> Result<&CatalogAttachmentRepository, CatalogApplicationError> {
        self.repository.as_ref().ok_or_else(|| {
            CatalogApplicationError::new(
                CatalogApplicationErrorKind::Unavailable,
                "catalog attachments require a configured Frontend StateStore",
            )
        })
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
            return CatalogAdmission::Unavailable {
                reason: "catalog attachment is not locally projected".to_string(),
            };
        };
        match self.control.observe_current_binding(instance_id) {
            Ok(_) => CatalogAdmission::Ready(CatalogRuntimeObservation {
                attachment_id: projection.attachment_id,
                instance_id: instance_id.clone(),
                provider_id: projection.provider_id,
                generation: projection.generation,
            }),
            Err(error) => CatalogAdmission::Unavailable {
                reason: error.to_string(),
            },
        }
    }

    fn install_created(
        &self,
        attachment: &CatalogAttachment,
        binding: novarocks_spi::connector::ConnectorControlBinding,
    ) -> Result<CatalogRuntimeObservation, CatalogApplicationError> {
        self.control.register(binding).map_err(connector_error)?;
        let generation = self.next_projection_generation();
        let projection = LocalProjection {
            attachment_id: attachment.attachment_id,
            provider_id: attachment.provider_id.clone(),
            generation,
        };
        self.projections
            .lock()
            .map_err(|_| {
                CatalogApplicationError::new(
                    CatalogApplicationErrorKind::Internal,
                    "catalog projection lock is poisoned",
                )
            })?
            .insert(attachment.instance_id.clone(), projection);
        Ok(CatalogRuntimeObservation {
            attachment_id: attachment.attachment_id,
            instance_id: attachment.instance_id.clone(),
            provider_id: attachment.provider_id.clone(),
            generation,
        })
    }

    /// Rebuild this process's control projection from the authoritative
    /// attachment scan. A change hint never carries attachment state; callers
    /// always invoke this method after rereading StateStore.
    pub(crate) async fn reconcile(&self) -> Result<(), CatalogApplicationError> {
        self.reconcile_with_page_size(256).await
    }

    pub(crate) async fn reconcile_with_page_size(
        &self,
        page_size: usize,
    ) -> Result<(), CatalogApplicationError> {
        let repository = self.repository()?;
        let attachments = repository
            .list_with_page_size(page_size)
            .await
            .map_err(repository_error)?;
        let desired = attachments
            .iter()
            .map(|versioned| {
                (
                    versioned.attachment.instance_id.clone(),
                    versioned.attachment.clone(),
                )
            })
            .collect::<BTreeMap<_, _>>();

        let stale = self
            .projections
            .lock()
            .map_err(|_| {
                CatalogApplicationError::new(
                    CatalogApplicationErrorKind::Internal,
                    "catalog projection lock is poisoned",
                )
            })?
            .keys()
            .filter(|instance_id| !desired.contains_key(*instance_id))
            .cloned()
            .collect::<Vec<_>>();
        for instance_id in stale {
            self.retire_projection(&instance_id);
        }

        for attachment in desired.values() {
            let installed = self
                .projections
                .lock()
                .map_err(|_| {
                    CatalogApplicationError::new(
                        CatalogApplicationErrorKind::Internal,
                        "catalog projection lock is poisoned",
                    )
                })?
                .get(&attachment.instance_id)
                .is_some_and(|projection| projection.attachment_id == attachment.attachment_id)
                && self
                    .control
                    .observe_current_binding(&attachment.instance_id)
                    .is_ok();
            if installed {
                continue;
            }

            self.retire_projection(&attachment.instance_id);
            let installed = (|| {
                let request = ConnectorControlFactoryRequest::try_new(
                    attachment.provider_id.clone(),
                    attachment.instance_id.clone(),
                    attachment.durable_properties.clone(),
                )
                .map_err(connector_error)?;
                let creation = self
                    .control
                    .create_control(request)
                    .map_err(connector_error)?;
                let (binding, _) = creation.into_parts();
                self.install_created(attachment, binding).map(|_| ())
            })();
            if let Err(error) = installed {
                // A single provider failure must not make durable truth
                // disappear or prevent unrelated catalog projections. Its
                // admission remains Unavailable until a later resync works.
                tracing::warn!(%error, catalog = attachment.instance_id.as_str(), "catalog attachment remains unavailable after projection attempt");
            }
        }
        Ok(())
    }

    /// Stops all local admission before retiring existing leases. Durable
    /// attachments remain unchanged, so a later authoritative reconcile can
    /// construct fresh generations after a freshness outage.
    pub(crate) fn unpublish_all(&self) {
        let instances = self
            .projections
            .lock()
            .map(|projections| projections.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        for instance_id in instances {
            self.retire_projection(&instance_id);
        }
    }

    fn retire_projection(&self, instance_id: &ConnectorInstanceId) {
        if let Ok(mut projections) = self.projections.lock() {
            projections.remove(instance_id);
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
        let repository = self.repository()?;
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
            return self.admit_catalog(&command.instance_id).require_ready();
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
        self.install_created(&created.attachment, binding)
    }

    fn drop_catalog(&self, command: CatalogDropCommand) -> Result<(), CatalogApplicationError> {
        let repository = self.repository()?;
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
        self.block_on(repository.drop_exact_fenced_by_materialized_views(existing, 256))?;
        self.retire_projection(&command.instance_id);
        // Durable deletion is authoritative. A local generation can be absent
        // or already retiring; either case converges through reconciliation.
        Ok(())
    }

    fn admit_catalog(&self, instance_id: &ConnectorInstanceId) -> CatalogAdmission {
        let repository = match self.repository() {
            Ok(repository) => repository,
            Err(error) => {
                return CatalogAdmission::Unavailable {
                    reason: error.to_string(),
                };
            }
        };
        match self.block_on(repository.get(instance_id)) {
            Ok(None) => CatalogAdmission::Absent,
            Ok(Some(_)) => self.observation(instance_id),
            Err(error) => CatalogAdmission::Unavailable {
                reason: error.to_string(),
            },
        }
    }
}

impl CatalogAttachmentObservationSource for FrontendCatalogApplicationPort {
    fn capture(
        &self,
        catalogs: &std::collections::BTreeSet<String>,
    ) -> Result<Vec<crate::catalog_attachment::CatalogAttachmentVersioned>, MvRepositoryError> {
        let repository = self.repository().map_err(mv_repository_error)?;
        let mut observations = Vec::with_capacity(catalogs.len());
        for catalog in catalogs {
            let instance_id = ConnectorInstanceId::parse(catalog).map_err(|error| {
                MvRepositoryError::new(MvRepositoryErrorKind::InvalidRequest, error.to_string())
            })?;
            match self.admit_catalog(&instance_id) {
                CatalogAdmission::Ready(observation) => {
                    let versioned = self
                        .block_on(repository.get(&instance_id))
                        .map_err(mv_repository_error)?
                        .ok_or_else(|| {
                            MvRepositoryError::new(
                                MvRepositoryErrorKind::Conflict,
                                "catalog attachment disappeared during MV admission",
                            )
                        })?;
                    if versioned.attachment.attachment_id != observation.attachment_id
                        || versioned.attachment.provider_id != observation.provider_id
                    {
                        return Err(MvRepositoryError::new(
                            MvRepositoryErrorKind::Conflict,
                            "catalog attachment changed during MV admission",
                        ));
                    }
                    observations.push(versioned);
                }
                CatalogAdmission::Absent => {
                    return Err(MvRepositoryError::new(
                        MvRepositoryErrorKind::Conflict,
                        "materialized view references a catalog attachment that is absent",
                    ));
                }
                CatalogAdmission::Unavailable { reason } => {
                    return Err(MvRepositoryError::new(
                        MvRepositoryErrorKind::Unavailable,
                        format!("materialized view catalog admission is unavailable: {reason}"),
                    ));
                }
            }
        }
        Ok(observations)
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
        CatalogApplicationErrorKind::InvalidRequest => MvRepositoryErrorKind::InvalidRequest,
        CatalogApplicationErrorKind::NotFound
        | CatalogApplicationErrorKind::AlreadyExists
        | CatalogApplicationErrorKind::Conflict => MvRepositoryErrorKind::Conflict,
        CatalogApplicationErrorKind::Unavailable => MvRepositoryErrorKind::Unavailable,
        CatalogApplicationErrorKind::Internal => MvRepositoryErrorKind::Corruption,
    };
    MvRepositoryError::new(kind, error.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

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
    async fn mv_observation_source_rejects_catalogs_without_a_durable_frontend_attachment() {
        let port = FrontendCatalogApplicationPort::unavailable(
            Arc::new(ConnectorControlHost::new()),
            tokio::runtime::Handle::current(),
        );
        let error = CatalogAttachmentObservationSource::capture(
            &port,
            &BTreeSet::from(["catalog.analytics".to_string()]),
        )
        .expect_err("an unavailable attachment repository cannot freeze an MV dependency");
        assert_eq!(error.kind(), MvRepositoryErrorKind::Unavailable);
    }
}
