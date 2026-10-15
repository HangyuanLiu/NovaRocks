use super::super::*;
use super::*;
use crate::connector::{
    CatalogVersion, ConnectorErrorKind, ConnectorInstanceId, ConnectorProviderId,
    ConnectorStopOwner,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

fn context() -> ConnectorRequestContext {
    ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(30),
        ConnectorStopOwner::new().view(),
        crate::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        crate::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .unwrap()
}
fn descriptor() -> ConnectorInstanceDescriptor {
    ConnectorInstanceDescriptor {
        provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
        instance_id: ConnectorInstanceId::parse("catalog").unwrap(),
    }
}
fn catalog(version: u8) -> CatalogHandle {
    CatalogHandle::new(
        descriptor().instance_id,
        CatalogVersion::from_bytes([version; 32]),
    )
}
fn request() -> KeyedDocumentObservationRequest {
    KeyedDocumentObservationRequest {
        owner: ConnectorProviderBindingKey {
            instance_id: descriptor().instance_id.clone(),
            incarnation: ProviderBindingEpoch::from_bytes([1; 16]),
        },
        catalog_handle: catalog(7),
        target: ConnectorTableIdentity {
            instance_id: descriptor().instance_id,
            namespace: "ns".into(),
            table: "table".into(),
        },
        object_id: ConnectorTableObjectId::try_new(Bytes::from_static(b"object")).unwrap(),
        context: context(),
    }
}
fn metadata() -> crate::connector::ConnectorCommittedVersion {
    crate::connector::ConnectorCommittedVersion::try_new(Bytes::from_static(b"metadata"), Some(7))
        .unwrap()
}
struct Management {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    calls: Arc<AtomicUsize>,
}
impl ConnectorDocumentStorageManagement for Management {
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
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Bytes::from_static(b"admission"))
    }
}
struct Observation {
    calls: Arc<AtomicUsize>,
    descriptor: ConnectorInstanceDescriptor,
    foreign_object: bool,
    foreign_token: bool,
}
impl ConnectorDocumentStorageObservation for Observation {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    fn incarnation(&self) -> ProviderBindingEpoch {
        request().owner.incarnation
    }
    fn observe_keyed_documents(
        &self,
        request: KeyedDocumentObservationRequest,
    ) -> Result<KeyedDocumentView, KeyedDocumentReadError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(KeyedDocumentView {
            owner: request.owner,
            table: request.target,
            object_id: if self.foreign_object {
                ConnectorTableObjectId::try_new(Bytes::from_static(b"foreign")).unwrap()
            } else {
                request.object_id
            },
            metadata_version: metadata(),
            current_snapshot: Some(7),
            current_snapshot_file_totals: SnapshotFileTotals {
                data_files: Some(0),
                delete_files: Some(0),
            },
            slots: Default::default(),
            markers: Default::default(),
            versions: Default::default(),
            roots: Default::default(),
            current_records: vec![],
            management: None,
        })
    }
    fn observe_full_keyed_documents(
        &self,
        request: KeyedDocumentObservationRequest,
    ) -> Result<FullKeyedDocumentView, KeyedDocumentReadError> {
        let current = self.observe_keyed_documents(request.clone())?;
        let version = if self.foreign_token {
            crate::connector::ConnectorCommittedVersion::try_new(
                Bytes::from_static(b"other metadata"),
                Some(8),
            )
            .unwrap()
        } else {
            current.metadata_version.clone()
        };
        Ok(FullKeyedDocumentView {
            current,
            snapshots: vec![],
            refs: Default::default(),
            diagnostics: vec![],
            token: FullObservationToken::try_new(
                request.owner,
                request.target,
                request.object_id,
                version,
                Bytes::from_static(b"provider full metadata"),
            )?,
        })
    }
    fn decode_keyed_slot_change_evidence(
        &self,
        _: KeyedDocumentObservationRequest,
        _: crate::connector::ExternalMutationEvidence,
    ) -> Result<UnknownCommitEvidence, ConnectorError> {
        unreachable!("not used by these lease tests")
    }
    fn resolve_keyed_commit(
        &self,
        _: KeyedDocumentObservationRequest,
        _: UnknownCommitEvidence,
    ) -> Result<CommitConclusion, ConnectorError> {
        unreachable!("not used by these lease tests")
    }
    fn discover_documents(
        &self,
        request: ConnectorDocumentDiscoveryRequest,
    ) -> Result<ConnectorDocumentDiscoveryPage, ConnectorError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut target = super::tests::request().target;
        target.namespace = "escaped".into();
        ConnectorDocumentDiscoveryPage::try_new(
            vec![ConnectorDocumentDiscoveryItem::try_new(
                target,
                super::tests::request().object_id,
                metadata(),
                ConnectorManagedObjectMarker::try_new("materialized-view", "owner", "incarnation")?,
            )?],
            None,
            ConnectorDocumentDiscoveryCompleteness::Complete,
            &request,
        )
    }
}
fn lease_with(
    observation: Option<Arc<dyn ConnectorDocumentStorageObservation>>,
    management: Option<Arc<dyn ConnectorDocumentStorageManagement>>,
    drops: Arc<AtomicUsize>,
) -> ConnectorDocumentStorageLease {
    let descriptor = descriptor();
    let binding = ConnectorDocumentStorageBinding::try_new(
        descriptor.clone(),
        request().owner.incarnation,
        observation,
        management,
    )
    .unwrap();
    ConnectorDocumentStorageLease::new(
        descriptor,
        ConnectorControlRuntimeId::new(),
        request().owner.incarnation,
        catalog(7),
        binding,
        move || {
            drops.fetch_add(1, Ordering::SeqCst);
        },
    )
    .unwrap()
}
fn observation(
    calls: Arc<AtomicUsize>,
    foreign_object: bool,
    foreign_token: bool,
) -> Arc<Observation> {
    Arc::new(Observation {
        calls,
        descriptor: descriptor(),
        foreign_object,
        foreign_token,
    })
}
fn admission_request() -> ConnectorDocumentManagementAdmissionRequest {
    let r = request();
    ConnectorDocumentManagementAdmissionRequest::try_new(
        r.owner,
        r.catalog_handle,
        ConnectorMutationOperationId::from_bytes([8; 16]),
        r.target,
        Some(r.object_id),
        ConnectorDocumentManagementOperation::SingleTargetUpdate,
        r.context,
    )
    .unwrap()
}

#[test]
fn full_token_becomes_expiry_authority_only_after_the_exact_lease_validates_it() {
    let obs = observation(Default::default(), false, false);
    let raw = obs.observe_full_keyed_documents(request()).unwrap();
    assert!(raw.token.validate_sealed().is_err());
    let management = Arc::new(Management {
        descriptor: descriptor(),
        incarnation: request().owner.incarnation,
        calls: Default::default(),
    });
    let lease = lease_with(Some(obs), Some(management), Default::default());
    let admitted = lease.admit_management(admission_request()).unwrap();
    let make = |token| KeyedMutationIntent {
        admission: admitted.clone(),
        expected_metadata: metadata(),
        expected_snapshot: Some(7),
        mutation: KeyedMutation::Expiry(KeyedExpiry {
            observation: token,
            remove_snapshots: Default::default(),
        }),
    };
    assert!(
        make(raw.token)
            .validate_for(lease.owner(), admitted.operation_id())
            .is_err()
    );
    let validated = lease.observe_full_keyed_documents(request()).unwrap();
    validated.token.validate_sealed().unwrap();
    make(validated.token)
        .validate_for(lease.owner(), admitted.operation_id())
        .unwrap();
}
#[test]
fn foreign_catalog_generation_is_rejected_before_observation_or_management_io() {
    let calls = Arc::new(AtomicUsize::new(0));
    let lease = lease_with(
        Some(observation(calls.clone(), false, false)),
        Some(Arc::new(Management {
            descriptor: descriptor(),
            incarnation: request().owner.incarnation,
            calls: calls.clone(),
        })),
        Default::default(),
    );
    let mut wrong = request();
    wrong.catalog_handle = catalog(9);
    assert!(lease.observe_keyed_documents(wrong).is_err());
    let r = request();
    let wrong = ConnectorDocumentManagementAdmissionRequest::try_new(
        r.owner,
        catalog(9),
        ConnectorMutationOperationId::new(),
        r.target,
        Some(r.object_id),
        ConnectorDocumentManagementOperation::SingleTargetUpdate,
        r.context,
    )
    .unwrap();
    assert_eq!(
        lease.admit_management(wrong).unwrap_err().kind(),
        ConnectorErrorKind::InvalidRequest
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
#[test]
fn foreign_object_or_full_metadata_token_cannot_be_sealed() {
    for (object, token) in [(true, false), (false, true)] {
        let lease = lease_with(
            Some(observation(Default::default(), object, token)),
            None,
            Default::default(),
        );
        assert!(lease.observe_full_keyed_documents(request()).is_err());
    }
}
#[test]
fn mismatched_capability_generation_is_rejected_at_binding() {
    let management = Arc::new(Management {
        descriptor: descriptor(),
        incarnation: ProviderBindingEpoch::from_bytes([2; 16]),
        calls: Default::default(),
    });
    assert!(
        ConnectorDocumentStorageBinding::try_new(
            descriptor(),
            request().owner.incarnation,
            None,
            Some(management)
        )
        .is_err()
    );
}
#[test]
fn management_only_binding_does_not_synthesize_an_observation() {
    let calls = Arc::new(AtomicUsize::new(0));
    let lease = lease_with(
        None,
        Some(Arc::new(Management {
            descriptor: descriptor(),
            incarnation: request().owner.incarnation,
            calls: calls.clone(),
        })),
        Default::default(),
    );
    lease.admit_management(admission_request()).unwrap();
    assert!(
        matches!(lease.observe_keyed_documents(request()), Err(KeyedDocumentReadError::Invalid(error)) if error.kind() == ConnectorErrorKind::Unsupported)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
#[test]
fn retained_lease_releases_generation_once_after_the_last_clone() {
    let drops = Arc::new(AtomicUsize::new(0));
    let lease = lease_with(
        Some(observation(Default::default(), false, false)),
        None,
        drops.clone(),
    );
    let retained = lease.clone();
    drop(lease);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(retained);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
#[test]
fn discovery_cannot_escape_the_original_catalog_namespace() {
    let lease = lease_with(
        Some(observation(Default::default(), false, false)),
        None,
        Default::default(),
    );
    let r = request();
    let request = ConnectorDocumentDiscoveryRequest::try_new(
        r.owner,
        r.catalog_handle,
        Some("requested".into()),
        1,
        ConnectorDocumentDiscoveryBudget::try_new(1).unwrap(),
        context(),
    )
    .unwrap();
    assert_eq!(
        lease.discover_documents(request).unwrap_err().kind(),
        ConnectorErrorKind::InvalidRequest
    );
}

#[test]
fn discovery_continuation_requires_a_sealed_page_and_a_shared_reduced_item_budget() {
    let origin = request();
    let request = ConnectorDocumentDiscoveryRequest::try_new(
        origin.owner,
        origin.catalog_handle,
        Some("ns".into()),
        2,
        ConnectorDocumentDiscoveryBudget::try_new(3).unwrap(),
        context(),
    )
    .unwrap();
    let item = ConnectorDocumentDiscoveryItem::try_new(
        origin.target,
        origin.object_id,
        metadata(),
        ConnectorManagedObjectMarker::try_new("materialized-view", "owner", "incarnation").unwrap(),
    )
    .unwrap();
    let mut page = ConnectorDocumentDiscoveryPage::try_new(
        vec![item],
        Some(Bytes::from_static(b"next")),
        ConnectorDocumentDiscoveryCompleteness::Incomplete(
            ConnectorDocumentDiscoveryIncompleteReason::PageBoundary,
        ),
        &request,
    )
    .unwrap();
    assert!(page.try_next_request(&request, context()).is_err());
    request.reserve_page(page.items().len()).unwrap();
    page.seal_for(&request);
    let next = page.try_next_request(&request, context()).unwrap().unwrap();
    assert_eq!(next.cursor().map(Bytes::as_ref), Some(b"next".as_slice()));
    assert_eq!(next.remaining_items(), 2);
    assert_eq!(next.page_size(), 2);
    let concurrent = next.clone();
    next.reserve_page(2).unwrap();
    assert_eq!(
        concurrent.reserve_page(1).unwrap_err().kind(),
        ConnectorErrorKind::ResourceExhausted
    );
}
