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

//! Cross-layer Current-source regression tests using native retained leases.

use super::{ManagedTargetCurrentSource, MvConvergence, keyed_request};
use bytes::Bytes;
use novarocks_mv_application::management::{
    CreateIntent, DeploymentOwner, EffectDisposition, EffectIdentity, EffectResponsibility,
    EffectScope, ManagedMvTarget, ManagementContinuation, ManagementEntrance,
    ManagementObservationPhase, ManagementObservationRequestId, ManagementRequest,
    ManagementTimestamp, MvManagementPhase,
};
use novarocks_mv_application::persistence::keyed_documents::{
    MvKeyedObservedCurrentDocuments, decode_current_keyed_documents, keyed_view_fixture,
    management_slot, management_slot_name,
};
use novarocks_mv_application::persistence::test_support::{
    KeyedProjectionFixture, observed_current_keyed,
};
use novarocks_mv_application::product::MvTarget;
use novarocks_mv_application::readiness::{
    MvCurrentProjectionRequest, MvKeyedCurrentProjectionSource, MvProjectionError,
    MvProjectionErrorKind,
};
use novarocks_spi::connector::*;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

type CurrentResult = Result<
    (
        MvKeyedObservedCurrentDocuments,
        novarocks_mv_application::management::MvCurrentManagementAdmission,
    ),
    MvProjectionError,
>;

fn unused() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        "unused admitted Current test capability",
    )
}

struct TargetCaps {
    descriptor: ConnectorInstanceDescriptor,
    epoch: ProviderBindingEpoch,
    current: Mutex<KeyedDocumentView>,
    captured_object: Mutex<ConnectorTableObjectId>,
    conclusions: Mutex<VecDeque<CommitConclusion>>,
    events: Mutex<Vec<&'static str>>,
}

impl TargetCaps {
    fn assert_scope(&self, request: &KeyedDocumentObservationRequest) {
        let view = self.current.lock().unwrap();
        assert_eq!(request.owner, view.owner);
        assert_eq!(request.target, view.table);
        assert_eq!(request.object_id, view.object_id);
        assert_eq!(
            request.catalog_handle,
            CatalogHandle::new(
                view.table.instance_id.clone(),
                CatalogVersion::from_bytes([1; 32])
            )
        );
    }

    fn advance_to_committed_slot(&self) {
        let mut view = self.current.lock().unwrap();
        view.current_snapshot = Some(18);
        view.metadata_version =
            ConnectorCommittedVersion::try_new(Bytes::from_static(b"committed-slot-18"), Some(18))
                .unwrap();
        view.slots
            .get_mut(&configuration_slot_name())
            .unwrap()
            .writer = Some(18);
    }
}

fn configuration_slot_name() -> ConnectorSlotName {
    novarocks_mv_application::persistence::keyed_documents::configuration_slot_name().unwrap()
}

impl ConnectorMetadata for TargetCaps {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.descriptor.instance_id
    }

    fn namespace_exists(&self, _: ConnectorNamespaceRequest) -> Result<bool, ConnectorError> {
        Err(unused())
    }

    fn table_exists(&self, _: ConnectorTableRequest) -> Result<bool, ConnectorError> {
        Err(unused())
    }

    fn list_tables(
        &self,
        _: ConnectorListTablesRequest,
    ) -> Result<Vec<ConnectorTableIdentity>, ConnectorError> {
        Err(unused())
    }

    fn load_table(
        &self,
        _: ConnectorTableRequest,
        _: &mut read_stack::ConnectorReadSchemaCreation<'_>,
    ) -> Result<
        ConnectorTableMetadata,
        novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
    > {
        Err(unused().into())
    }

    fn capture_table_object_binding(
        &self,
        request: ConnectorTableObjectCaptureRequest,
    ) -> Result<ConnectorTableObjectBinding, ConnectorError> {
        self.events.lock().unwrap().push("capture");
        assert_eq!(request.table, self.current.lock().unwrap().table);
        assert!(matches!(
            request.resolution,
            ConnectorTableResolution::StrictBaseTable
        ));
        assert!(matches!(
            request.selector,
            ConnectorTableObjectSelector::Current
        ));
        let header = ConnectorTableMetadataHeader {
            identity: request.table,
            planning_facts: Default::default(),
            definition_facts: Default::default(),
            version: None,
            statistics_data_version: None,
            table: ConnectorTableHandle::try_new(
                self.descriptor.instance_id.clone(),
                Bytes::from_static(b"captured-current-target"),
            )?,
        };
        // This source consumes identity only; the fixture explicitly declares
        // a zero-column provider table through the real schema creation path.
        let metadata =
            crate::connector::schema_creation::with_schema_creation(&request.context, |creation| {
                crate::connector::schema_creation::declared_test_metadata_with_creation(
                    header,
                    Arc::new(arrow::datatypes::Schema::empty()),
                    vec![],
                    creation,
                )
            })
            .map_err(crate::connector::schema_creation::connector_schema_error)?;
        Ok(ConnectorTableObjectBinding {
            metadata,
            object_id: self.captured_object.lock().unwrap().clone(),
        })
    }
}

impl ConnectorDocumentStorageObservation for TargetCaps {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn incarnation(&self) -> ProviderBindingEpoch {
        self.epoch
    }

    fn observe_keyed_documents(
        &self,
        request: KeyedDocumentObservationRequest,
    ) -> Result<KeyedDocumentView, KeyedDocumentReadError> {
        self.assert_scope(&request);
        self.events.lock().unwrap().push("observe");
        // Only ConnectorDocumentStorageLease validates this reply and grants
        // the observation authority used by the MV state machine.
        Ok(self.current.lock().unwrap().clone())
    }

    fn observe_full_keyed_documents(
        &self,
        _: KeyedDocumentObservationRequest,
    ) -> Result<FullKeyedDocumentView, KeyedDocumentReadError> {
        Err(unused().into())
    }

    fn decode_keyed_slot_change_evidence(
        &self,
        _: KeyedDocumentObservationRequest,
        _: ExternalMutationEvidence,
    ) -> Result<UnknownCommitEvidence, ConnectorError> {
        Err(unused())
    }

    fn resolve_keyed_commit(
        &self,
        request: KeyedDocumentObservationRequest,
        evidence: UnknownCommitEvidence,
    ) -> Result<CommitConclusion, ConnectorError> {
        self.assert_scope(&request);
        assert_eq!(evidence, slot_evidence());
        self.events.lock().unwrap().push("resolve");
        let conclusion = self
            .conclusions
            .lock()
            .unwrap()
            .pop_front()
            .expect("only the predeclared slot resolution calls are allowed");
        if matches!(conclusion, CommitConclusion::Committed(_)) {
            self.advance_to_committed_slot();
        }
        Ok(conclusion)
    }

    fn discover_documents(
        &self,
        _: ConnectorDocumentDiscoveryRequest,
    ) -> Result<ConnectorDocumentDiscoveryPage, ConnectorError> {
        Err(unused())
    }
}

struct Resolver {
    binding: Arc<ConnectorControlBinding>,
}

impl Resolver {
    fn lease(&self) -> ConnectorControlPlanningLease {
        ConnectorControlPlanningLease::new(Arc::clone(&self.binding), || {})
    }
}

impl ConnectorControlResolver for Resolver {
    fn observe_current_binding(
        &self,
        instance: &ConnectorInstanceId,
    ) -> Result<ConnectorProviderBindingKey, ConnectorError> {
        assert_eq!(instance, &self.binding.descriptor().instance_id);
        Ok(ConnectorProviderBindingKey {
            instance_id: self.binding.descriptor().instance_id.clone(),
            incarnation: self.binding.incarnation(),
        })
    }

    fn observe_current_control_runtime(
        &self,
        instance: &ConnectorInstanceId,
    ) -> Result<ConnectorControlRuntimeId, ConnectorError> {
        assert_eq!(instance, &self.binding.descriptor().instance_id);
        Ok(self.binding.control_runtime_id())
    }

    fn acquire_current(
        &self,
        instance: &ConnectorInstanceId,
    ) -> Result<ConnectorControlPlanningLease, ConnectorError> {
        assert_eq!(instance, &self.binding.descriptor().instance_id);
        Ok(self.lease())
    }
}

struct Fixture {
    entrance: ManagementEntrance,
    resolver: Resolver,
    caps: Arc<TargetCaps>,
    catalog: CatalogHandle,
    target: MvTarget,
    managed: ManagedMvTarget,
}

impl Fixture {
    fn new(conclusions: Vec<CommitConclusion>) -> Self {
        let (catalog, _) = keyed_view_fixture(Some(17), true);
        let target = MvTarget::from_parts(Some("ice"), "sales", "mv");
        let facts = KeyedProjectionFixture::new(target.clone(), Some(17))
            .build()
            .unwrap();
        let observed = observed_current_keyed(&facts, catalog.clone());
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: observed.view.table.instance_id.clone(),
        };
        let epoch = observed.view.owner.incarnation;
        let caps = Arc::new(TargetCaps {
            descriptor: descriptor.clone(),
            epoch,
            captured_object: Mutex::new(observed.view.object_id.clone()),
            current: Mutex::new(observed.view),
            conclusions: Mutex::new(conclusions.into()),
            events: Mutex::new(vec![]),
        });
        let template = novarocks_catalog_application::test_support::test_control_binding_for(
            descriptor.instance_id.clone(),
            3,
        );
        let documents = ConnectorDocumentStorageBinding::try_new(
            descriptor.clone(),
            epoch,
            Some(caps.clone()),
            None,
        )
        .unwrap();
        let binding = ConnectorControlBinding::try_new(
            descriptor,
            epoch,
            caps.clone(),
            Arc::clone(template.planning()),
            Arc::clone(template.execution_distribution()),
            None,
        )
        .unwrap()
        .with_catalog_properties(
            CatalogProperties::new(
                catalog.clone(),
                ConnectorProviderId::parse("iceberg").unwrap(),
                1,
                vec![],
                vec![],
            )
            .unwrap(),
        )
        .and_then(|binding| binding.try_with_document_storage(Some(documents)))
        .unwrap();
        let resolver = Resolver {
            binding: Arc::new(binding),
        };
        let lease = resolver.lease().derive_document_storage_lease().unwrap();
        let view = caps.current.lock().unwrap().clone();
        let current =
            decode_current_keyed_documents(view, catalog.clone(), Default::default()).unwrap();
        let entrance = ManagementEntrance::new(
            current.deployment_owner.clone(),
            current.process_incarnation.clone(),
        );
        let managed = current.management_target.clone();
        // Seed only fixture CREATE responsibility facts, not a native table
        // creation receipt. Admission itself still requires the real lease's
        // exact Current observation; no Fresh value or seal is fabricated.
        let intent = CreateIntent::try_new(
            catalog.clone(),
            managed.table().clone(),
            EffectIdentity::from_bytes([40; 16]),
        )
        .unwrap();
        let mut creation = entrance
            .acquire(
                ManagementRequest::for_create_intent(intent, EffectScope::CATALOG_COMMIT),
                || false,
            )
            .unwrap();
        creation
            .mark_create_intent_dispatched(ManagementTimestamp::from_unix_millis(900))
            .unwrap();
        creation.late_bind_create_target(managed.clone()).unwrap();
        creation
            .record_terminal(EffectDisposition::KnownCommitted)
            .unwrap();
        let mut state = entrance
            .begin_committed_convergence(
                managed.table(),
                ManagementContinuation::SameOwner {
                    previous_incarnation: entrance.incarnation().clone(),
                },
            )
            .unwrap();
        let pending = state
            .begin_current_observation(ManagementObservationRequestId::from_bytes([41; 16]))
            .unwrap();
        let (phase, view) = state
            .complete_keyed_current_observation(
                pending,
                &lease,
                keyed_request(
                    &lease,
                    managed.table().clone(),
                    managed.object_id().clone(),
                    context(),
                ),
            )
            .unwrap();
        assert_eq!(phase, ManagementObservationPhase::Ready);
        let observed =
            decode_current_keyed_documents(view, catalog.clone(), Default::default()).unwrap();
        entrance
            .install_observed_target(
                &state,
                observed.management_dependencies(resolver.binding.control_runtime_id()),
            )
            .unwrap();
        caps.events.lock().unwrap().clear();
        Self {
            entrance,
            resolver,
            caps,
            catalog,
            target,
            managed,
        }
    }

    fn request(&self) -> MvCurrentProjectionRequest {
        MvCurrentProjectionRequest::try_new(
            self.catalog.clone(),
            self.target.clone(),
            context(),
            Default::default(),
        )
        .unwrap()
    }

    async fn observe(&self, convergence: MvConvergence) -> CurrentResult {
        self.observe_request(convergence, self.request()).await
    }

    async fn observe_request(
        &self,
        convergence: MvConvergence,
        request: MvCurrentProjectionRequest,
    ) -> CurrentResult {
        ManagedTargetCurrentSource {
            entrance: &self.entrance,
            connector_control: &self.resolver,
            published: None,
            convergence,
        }
        .observe_current_keyed(request)
        .await
    }

    fn management_request(&self) -> ManagementRequest {
        let current = decode_current_keyed_documents(
            self.caps.current.lock().unwrap().clone(),
            self.catalog.clone(),
            Default::default(),
        )
        .unwrap();
        ManagementRequest::try_new(
            self.catalog.clone(),
            self.managed.table().clone(),
            Some(self.managed.object_id().clone()),
            ConnectorDocumentManagementOperation::SingleTargetUpdate,
            Some(current.management_dependencies(self.resolver.binding.control_runtime_id())),
            EffectScope::CATALOG_COMMIT,
        )
        .unwrap()
    }

    fn dispatched(&self) -> novarocks_mv_application::management::ManagementEntranceLease {
        let mut lease = self
            .entrance
            .acquire(self.management_request(), || false)
            .unwrap();
        lease
            .mark_dispatched(EffectResponsibility::new(
                EffectIdentity::from_bytes([51; 16]),
                self.managed.clone(),
                self.entrance.incarnation().clone(),
                EffectScope::CATALOG_COMMIT,
                ManagementTimestamp::from_unix_millis(1000),
            ))
            .unwrap();
        lease
    }

    fn assert_manageable(&self) {
        assert_eq!(
            self.entrance.management_phase(self.managed.table()),
            MvManagementPhase::Manageable
        );
        assert!(
            self.entrance
                .unsettled_effects(self.managed.table())
                .is_empty()
        );
    }
}

fn context() -> ConnectorRequestContext {
    crate::connector::test_request_context()
}

fn slot_evidence() -> UnknownCommitEvidence {
    UnknownCommitEvidence {
        asserted_parent: Some(17),
        preselected_snapshot: 18,
        identity: UnknownCommitIdentity::SlotWriter {
            slot: configuration_slot_name(),
        },
    }
}

#[tokio::test]
async fn healthy_admitted_current_does_not_require_a_local_committed_effect() {
    let fixture = Fixture::new(vec![]);
    fixture.assert_manageable();
    let error = fixture
        .observe(MvConvergence::CommittedEffect)
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), MvProjectionErrorKind::SourceConflict);
    assert!(error.to_string().contains("ObservationNotAllowed"));
    assert_eq!(*fixture.caps.events.lock().unwrap(), vec!["capture"]);
    fixture.caps.events.lock().unwrap().clear();

    let (observed, _admission) = fixture
        .observe(MvConvergence::AdmittedCurrent)
        .await
        .unwrap();
    assert_eq!(observed.management_target, fixture.managed);
    assert_eq!(observed.view.current_snapshot, Some(17));
    assert_eq!(
        *fixture.caps.events.lock().unwrap(),
        vec!["capture", "observe"]
    );
    fixture.assert_manageable();
    drop(
        fixture
            .entrance
            .acquire(fixture.management_request(), || false)
            .unwrap(),
    );
}

async fn resolved_slot_current(still_unknown_first: bool) {
    let committed = CommitConclusion::Committed(CommitProof::SlotWriter);
    let conclusions = if still_unknown_first {
        vec![CommitConclusion::StillUnknown, committed.clone()]
    } else {
        vec![committed.clone()]
    };
    let fixture = Fixture::new(conclusions);
    fixture
        .dispatched()
        .record_slot_change_unknown(slot_evidence())
        .unwrap();
    assert_eq!(
        fixture
            .entrance
            .pending_slot_changes(fixture.managed.table()),
        1
    );
    fixture.assert_manageable();
    if still_unknown_first {
        let documents = fixture
            .resolver
            .lease()
            .derive_document_storage_lease()
            .unwrap();
        let result = fixture
            .entrance
            .resolve_pending_slot_changes(
                &documents,
                keyed_request(
                    &documents,
                    fixture.managed.table().clone(),
                    fixture.managed.object_id().clone(),
                    context(),
                ),
            )
            .unwrap();
        assert!(!result.reinstall_current_projection);
        assert!(result.conclusions.is_empty());
        assert_eq!(
            fixture
                .entrance
                .pending_slot_changes(fixture.managed.table()),
            1
        );
        assert_eq!(
            fixture
                .entrance
                .latest_slot_change_conclusion(fixture.managed.table()),
            None
        );
        fixture.assert_manageable();
        assert_eq!(*fixture.caps.events.lock().unwrap(), vec!["resolve"]);
        fixture.caps.events.lock().unwrap().clear();
    }

    let (observed, _admission) = fixture
        .observe(MvConvergence::AdmittedCurrent)
        .await
        .unwrap();
    assert_eq!(observed.management_target, fixture.managed);
    assert_eq!(observed.view.current_snapshot, Some(18));
    assert_eq!(
        observed.view.metadata_version.payload().as_ref(),
        b"committed-slot-18"
    );
    assert_eq!(
        *fixture.caps.events.lock().unwrap(),
        vec!["capture", "observe", "resolve", "observe"]
    );
    assert_eq!(
        fixture
            .entrance
            .pending_slot_changes(fixture.managed.table()),
        0
    );
    assert_eq!(
        fixture
            .entrance
            .latest_slot_change_conclusion(fixture.managed.table()),
        Some(committed)
    );
    assert!(fixture.caps.conclusions.lock().unwrap().is_empty());
    fixture.assert_manageable();
    drop(
        fixture
            .entrance
            .acquire(fixture.management_request(), || false)
            .unwrap(),
    );
    assert!(
        fixture
            .observe(MvConvergence::CommittedEffect)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn admitted_current_consumes_a_committed_slot_without_creating_a_barrier() {
    resolved_slot_current(false).await;
}

#[tokio::test]
async fn admitted_current_retries_resolution_after_a_still_unknown_observation() {
    resolved_slot_current(true).await;
}

#[tokio::test]
async fn an_actual_local_committed_effect_still_uses_committed_convergence() {
    let fixture = Fixture::new(vec![]);
    fixture
        .dispatched()
        .record_terminal(EffectDisposition::KnownCommitted)
        .unwrap();
    assert_ne!(
        fixture.entrance.management_phase(fixture.managed.table()),
        MvManagementPhase::Manageable
    );
    fixture.caps.advance_to_committed_slot();
    let (observed, _admission) = fixture
        .observe(MvConvergence::CommittedEffect)
        .await
        .unwrap();
    assert_eq!(observed.view.current_snapshot, Some(18));
    assert_eq!(
        *fixture.caps.events.lock().unwrap(),
        vec!["capture", "observe"]
    );
    fixture.assert_manageable();
    drop(
        fixture
            .entrance
            .acquire(fixture.management_request(), || false)
            .unwrap(),
    );
}

#[tokio::test]
async fn admitted_current_does_not_issue_authority_for_a_foreign_owner() {
    let fixture = Fixture::new(vec![]);
    fixture
        .caps
        .current
        .lock()
        .unwrap()
        .slots
        .get_mut(&management_slot_name().unwrap())
        .unwrap()
        .value = management_slot(
        &DeploymentOwner::parse("foreign-deployment").unwrap(),
        fixture.entrance.incarnation(),
    )
    .unwrap();
    let error = fixture
        .observe(MvConvergence::AdmittedCurrent)
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), MvProjectionErrorKind::SourceConflict);
    assert!(
        error
            .to_string()
            .contains("cannot renew this target admission")
    );
    assert_eq!(
        *fixture.caps.events.lock().unwrap(),
        vec!["capture", "observe"]
    );
    assert_eq!(
        fixture
            .entrance
            .pending_slot_changes(fixture.managed.table()),
        0
    );
}

#[tokio::test]
async fn admitted_current_does_not_issue_authority_for_a_replaced_exact_object() {
    let fixture = Fixture::new(vec![]);
    let mut replacement = KeyedProjectionFixture::new(fixture.target.clone(), Some(17));
    replacement.object_id =
        ConnectorTableObjectId::try_new(Bytes::from_static(b"replacement-object")).unwrap();
    let replacement =
        observed_current_keyed(&replacement.build().unwrap(), fixture.catalog.clone()).view;
    *fixture.caps.captured_object.lock().unwrap() = replacement.object_id.clone();
    *fixture.caps.current.lock().unwrap() = replacement;
    let error = fixture
        .observe(MvConvergence::AdmittedCurrent)
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), MvProjectionErrorKind::SourceConflict);
    assert!(error.to_string().contains("target was replaced"));
    assert_eq!(
        *fixture.caps.events.lock().unwrap(),
        vec!["capture", "observe"]
    );
    fixture.assert_manageable();
}

#[tokio::test]
async fn admitted_current_rejects_a_foreign_catalog_before_metadata_or_document_io() {
    let fixture = Fixture::new(vec![]);
    let request = MvCurrentProjectionRequest::try_new(
        CatalogHandle::new(
            fixture.catalog.catalog_name().clone(),
            CatalogVersion::from_bytes([9; 32]),
        ),
        fixture.target.clone(),
        context(),
        Default::default(),
    )
    .unwrap();
    let error = fixture
        .observe_request(MvConvergence::AdmittedCurrent, request)
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), MvProjectionErrorKind::SourceConflict);
    assert!(error.to_string().contains("catalog generation changed"));
    assert!(fixture.caps.events.lock().unwrap().is_empty());
    fixture.assert_manageable();
}
