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

//! Mint a real document management admission for tests.
//!
//! An admission can only come from a provider, so these tests stand up a
//! minimal one rather than fabricating the value. That keeps the admission's
//! own invariants — owner, catalog generation, operation and exact object —
//! under test instead of bypassed.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use novarocks_spi::connector::document_storage::{
    ConnectorDocumentManagementAdmission, ConnectorDocumentManagementAdmissionRequest,
    ConnectorDocumentManagementOperation, ConnectorDocumentStorageBinding,
    ConnectorDocumentStorageManagement, ConnectorPrepareDocumentsRequest,
};
use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorError, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorMutationOperationId, ConnectorProviderId,
    ConnectorRequestContext, ConnectorTableIdentity, ConnectorTableObjectId, LakePublicationId,
    ProviderBindingEpoch,
};

fn catalog_properties(
    instance_id: &ConnectorInstanceId,
) -> novarocks_spi::connector::CatalogProperties {
    novarocks_spi::connector::CatalogProperties::new(
        novarocks_spi::connector::CatalogHandle::new(
            instance_id.clone(),
            novarocks_spi::connector::CatalogVersion::from_bytes([3; 32]),
        ),
        ConnectorProviderId::parse("iceberg").expect("test provider"),
        1,
        Vec::new(),
        Vec::new(),
    )
    .expect("test catalog properties")
}

struct AdmittingManagement {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
}

impl ConnectorDocumentStorageManagement for AdmittingManagement {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn incarnation(&self) -> ProviderBindingEpoch {
        self.incarnation
    }

    fn admit_management(
        &self,
        _request: ConnectorDocumentManagementAdmissionRequest,
    ) -> Result<Bytes, ConnectorError> {
        Ok(Bytes::from_static(b"test-admission-token"))
    }

    fn prepare_documents(
        &self,
        _request: ConnectorPrepareDocumentsRequest,
    ) -> Result<Bytes, ConnectorError> {
        Ok(Bytes::from_static(b"test-prepared-token"))
    }
}

/// A publication admission for `target`, pinned to `publication_id` and the
/// exact object the publication will commit against.
pub(crate) fn publication_admission(
    catalog: &str,
    namespace: &str,
    table: &str,
    publication_id: LakePublicationId,
    object_id: &ConnectorTableObjectId,
) -> ConnectorDocumentManagementAdmission {
    let instance_id = ConnectorInstanceId::parse(catalog).expect("test instance ID");
    let incarnation = ProviderBindingEpoch::from_bytes([8; 16]);
    let descriptor = ConnectorInstanceDescriptor {
        provider_id: ConnectorProviderId::parse("iceberg").expect("test provider"),
        instance_id: instance_id.clone(),
    };
    let documents = ConnectorDocumentStorageBinding::try_new(
        descriptor.clone(),
        incarnation,
        None,
        Some(Arc::new(AdmittingManagement {
            descriptor,
            incarnation,
        })),
    )
    .expect("test document storage binding");
    let binding = novarocks_catalog_application::test_support::test_control_binding_for(
        instance_id.clone(),
        8,
    )
    .with_catalog_properties(catalog_properties(&instance_id))
    .and_then(|binding| binding.try_with_document_storage(Some(documents)))
    .expect("test control binding");
    let lease = ConnectorControlPlanningLease::new(Arc::new(binding), || {})
        .derive_document_storage_lease()
        .expect("test document storage lease");
    let context = ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(30),
        novarocks_spi::connector::ConnectorStopOwner::new().view(),
        4096,
        64 * 1024,
    )
    .expect("test request context");
    lease
        .admit_management(
            ConnectorDocumentManagementAdmissionRequest::try_new(
                lease.owner().clone(),
                lease.catalog_handle().clone(),
                ConnectorMutationOperationId::from_bytes(publication_id.to_bytes()),
                ConnectorTableIdentity {
                    instance_id,
                    namespace: Arc::from(namespace),
                    table: Arc::from(table),
                },
                Some(object_id.clone()),
                ConnectorDocumentManagementOperation::Publication,
                context,
            )
            .expect("test admission request"),
        )
        .expect("test publication admission")
}

/// Mutable inputs for FE tests; every build decodes an actual keyed Current view.
pub(crate) struct KeyedProjectionFixture {
    pub target: novarocks_mv_application::product::MvTarget,
    pub object_id: ConnectorTableObjectId,
    pub definition: novarocks_mv_application::persistence::codec::DefinitionDocument,
    pub interpretation: novarocks_mv_application::persistence::codec::InterpretationDocument,
    pub configuration: novarocks_mv_application::persistence::codec::ConfigurationDocument,
    pub publication:
        Option<novarocks_mv_application::persistence::codec::keyed::KeyedPublicationDocument>,
    pub metadata_version: novarocks_spi::connector::ConnectorCommittedVersion,
}
impl KeyedProjectionFixture {
    pub fn new(target: novarocks_mv_application::product::MvTarget, snapshot: Option<i64>) -> Self {
        let f = novarocks_mv_application::persistence::test_support::ProjectionFixture::new(
            target.clone(),
            snapshot,
        );
        let (catalog, view) =
            novarocks_mv_application::persistence::keyed_documents::keyed_view_fixture(
                snapshot,
                snapshot.is_some(),
            );
        let observed =
            novarocks_mv_application::persistence::keyed_documents::decode_current_keyed_documents(
                view,
                catalog,
                Default::default(),
            )
            .expect("keyed fixture");
        Self {
            target,
            object_id: f.object_id,
            definition: f.definition,
            interpretation: f.interpretation,
            configuration: f.configuration,
            publication: observed.publication,
            metadata_version: f.metadata_version,
        }
    }
    pub fn with_retraction_count(mut self) -> Self {
        self.interpretation =
            novarocks_mv_application::persistence::test_support::ProjectionFixture::new(
                self.target.clone(),
                None,
            )
            .with_retraction_count()
            .interpretation;
        self
    }
    pub fn build(
        mut self,
    ) -> Result<
        novarocks_mv_application::persistence::keyed_documents::MvKeyedDocumentProjection,
        String,
    > {
        use novarocks_mv_application::persistence::{
            codec::keyed::{BindingDocument, DataVersionFormat},
            identity::ObjectIdentity,
            keyed_documents::*,
        };
        use novarocks_spi::connector::document_storage::*;
        self.definition = novarocks_mv_application::persistence::codec::build_definition(
            self.definition.created_at_ms,
            self.definition.query.clone(),
            self.definition.relation_occurrences.clone(),
            self.definition.outputs.clone(),
        )
        .map_err(|e| e.to_string())?;
        self.interpretation.computation_identity = self.definition.computation_identity;
        self.interpretation.target.object_id =
            ObjectIdentity::try_new(self.object_id.as_bytes().to_vec())
                .map_err(|e| e.to_string())?;
        let formats=self.definition.relation_occurrences.iter().map(|o|{
            let object=novarocks_mv_application::persistence::exact_revision::restore_persisted_object(&o.object_id).map_err(|e|e.to_string())?;
            let fact=novarocks_spi::connector::ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(ConnectorProviderId::parse("iceberg").unwrap(),&object,Some(17)).map_err(|e|e.to_string())?;
            DataVersionFormat::from_issued_fact(fact.data_version()).map_err(|e|e.to_string())
        }).collect::<Result<Vec<_>,String>>()?;
        let versions = build_keyed_versions(
            &self.definition,
            &self.interpretation,
            BindingDocument {
                data_versions: formats,
            },
        )
        .map_err(|e| e.to_string())?;
        let snapshot = self.metadata_version.snapshot_id();
        let (mut catalog, mut view) = keyed_view_fixture(snapshot, false);
        view.table = ConnectorTableIdentity {
            instance_id: ConnectorInstanceId::parse(
                self.target
                    .catalog()
                    .ok_or("fixture target catalog missing")?,
            )
            .map_err(|e| e.to_string())?,
            namespace: Arc::from(self.target.namespace()),
            table: Arc::from(self.target.name()),
        };
        catalog = novarocks_spi::connector::CatalogHandle::new(
            view.table.instance_id.clone(),
            novarocks_spi::connector::CatalogVersion::from_bytes([1; 32]),
        );
        view.owner.instance_id = view.table.instance_id.clone();
        view.object_id = self.object_id;
        view.metadata_version = self.metadata_version;
        view.versions = versions
            .versions()
            .into_iter()
            .map(|v| (v.identity(), v))
            .collect();
        view.roots.get_mut("novarocks.mv").unwrap().identity = versions.binding.identity();
        view.slots
            .get_mut(&configuration_slot_name().map_err(|e| e.to_string())?)
            .unwrap()
            .value = keyed_configuration_slot_change(&self.configuration)
            .map_err(|e| e.to_string())?
            .slots
            .into_values()
            .next()
            .unwrap();
        view.slots
            .get_mut(&management_slot_name().map_err(|e| e.to_string())?)
            .unwrap()
            .value = management_slot(
            &novarocks_mv_application::management::DeploymentOwner::parse("test-deployment")
                .unwrap(),
            &novarocks_mv_application::management::ProcessIncarnation::parse("test-process")
                .unwrap(),
        )
        .map_err(|e| e.to_string())?;
        if let Some(p) = self.publication.as_mut() {
            p.definition_revision = versions.definition_revision();
            p.interpretation_revision = versions.interpretation_revision();
            view.current_records = keyed_publication_document_set(&versions, &self.definition, p)
                .map_err(|e| e.to_string())?
                .records;
            view.current_snapshot_file_totals.data_files = Some(1);
        }
        let observed = decode_current_keyed_documents(view, catalog, Default::default())
            .map_err(|e| e.to_string())?;
        MvKeyedDocumentProjection::try_from_current(observed)
    }
}
pub(crate) fn sample_keyed_projection(
    target: novarocks_mv_application::product::MvTarget,
    snapshot: Option<i64>,
) -> novarocks_mv_application::persistence::keyed_documents::MvKeyedDocumentProjection {
    KeyedProjectionFixture::new(target, snapshot)
        .build()
        .expect("keyed projection")
}

pub(crate) async fn keyed_repository()
-> Arc<novarocks_mv_application::state_store_repository::StateStoreMvRepository> {
    novarocks_mv_application::state_store_repository::StateStoreMvRepository::open(
        Arc::new(novarocks_state_store_testkit::testing::InMemoryStateStore::new("fe-keyed-tests")),
        Default::default(),
    )
    .await
    .expect("test keyed repository")
}
pub(crate) fn management_entrance() -> Arc<novarocks_mv_application::management::ManagementEntrance>
{
    Arc::new(
        novarocks_mv_application::management::ManagementEntrance::new(
            novarocks_mv_application::management::DeploymentOwner::parse("test-deployment")
                .unwrap(),
            novarocks_mv_application::management::ProcessIncarnation::parse("test-process")
                .unwrap(),
        ),
    )
}
pub(crate) async fn keyed_product() -> (
    novarocks_mv_application::service::MvKeyedProductService,
    Arc<novarocks_mv_application::state_store_repository::StateStoreMvRepository>,
) {
    let repository = keyed_repository().await;
    let product=novarocks_mv_application::service::MvKeyedProductService::new_with_keyed_management_continuation(Default::default(),repository.clone(),Default::default(),management_entrance(),Default::default());
    (product, repository)
}

/// Each test gets an independent MV state owner; only the Tokio executor is shared.
pub(crate) fn test_readiness() -> Arc<crate::mv::domain::readiness::MvReadinessPort> {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    let runtime = RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("test MV executor")
    });
    let (product, _) = std::thread::scope(|scope| {
        scope
            .spawn(|| runtime.block_on(keyed_product()))
            .join()
            .expect("construct test MV owner")
    });
    Arc::new(crate::mv::domain::readiness::MvReadinessPort::from_product(
        product.keyed_readiness_service().unwrap(),
        runtime.handle().clone(),
        product.management_entrance().unwrap(),
    ))
}
