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

//! Observe the real MV guard's provider boundary before any document lookup.
//! A query's sink alone cannot consume a vended table response: its collection
//! port must also carry this exact retained catalog's execution properties.
use super::{IcebergMvUserMutation, reject_if_iceberg_mv_table_with_planning_lease_and_context};
use crate::catalog_application::resolver::TargetBackend;
use novarocks_spi::connector::*;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn missing_table() -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::NotFound, "guard test table is absent")
}

struct RecordingMetadata {
    instance: ConnectorInstanceId,
    captured: Mutex<Option<ConnectorTableObjectCaptureRequest>>,
}

impl ConnectorMetadata for RecordingMetadata {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance
    }

    fn namespace_exists(&self, _: ConnectorNamespaceRequest) -> Result<bool, ConnectorError> {
        Err(missing_table())
    }

    fn table_exists(&self, _: ConnectorTableRequest) -> Result<bool, ConnectorError> {
        Err(missing_table())
    }

    fn list_tables(
        &self,
        _: ConnectorListTablesRequest,
    ) -> Result<Vec<ConnectorTableIdentity>, ConnectorError> {
        Err(missing_table())
    }

    fn load_table(
        &self,
        _: ConnectorTableRequest,
        _: &mut read_stack::ConnectorReadSchemaCreation<'_>,
    ) -> Result<
        ConnectorTableMetadata,
        novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
    > {
        Err(missing_table().into())
    }

    fn capture_table_object_binding(
        &self,
        request: ConnectorTableObjectCaptureRequest,
    ) -> Result<ConnectorTableObjectBinding, ConnectorError> {
        *self.captured.lock().unwrap() = Some(request);
        // Stop at the provider boundary: an absent table has no keyed document
        // authority. This avoids inventing a successful MV observation.
        Err(missing_table())
    }
}

struct UnusedSink;

impl ConnectorVendedCredentialLeaseSink for UnusedSink {
    fn offer_vended_s3_credential_lease(
        &self,
        _: &CatalogProperties,
        _: VendedS3CredentialLeaseContribution,
    ) -> Result<(), ConnectorError> {
        panic!("the absent test table cannot contribute credentials")
    }
}

fn fixture(with_properties: bool) -> (ConnectorControlPlanningLease, Arc<RecordingMetadata>) {
    let instance = ConnectorInstanceId::parse("guard_catalog").unwrap();
    let original =
        novarocks_catalog_application::test_support::test_control_binding_for(instance.clone(), 8);
    let metadata = Arc::new(RecordingMetadata {
        instance: instance.clone(),
        captured: Mutex::new(None),
    });
    let mut binding = ConnectorControlBinding::try_new(
        original.descriptor().clone(),
        original.incarnation(),
        metadata.clone(),
        original.planning().clone(),
        original.execution_distribution().clone(),
        None,
    )
    .unwrap();
    if with_properties {
        binding = binding
            .with_catalog_properties(super::super::test_admission::catalog_properties(&instance))
            .unwrap();
    }
    (
        ConnectorControlPlanningLease::new(Arc::new(binding), || {}),
        metadata,
    )
}

fn context(stop: &ConnectorStopOwner) -> ConnectorRequestContext {
    ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(30),
        stop.view(),
        4096,
        64 * 1024,
    )
    .unwrap()
}

fn invoke(
    lease: &ConnectorControlPlanningLease,
    context: ConnectorRequestContext,
    catalog: &str,
) -> String {
    let target = TargetBackend {
        provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
        catalog: catalog.into(),
        namespace: "db".into(),
        table: "table".into(),
    };
    reject_if_iceberg_mv_table_with_planning_lease_and_context(
        &UnavailableMvStorageObservationPort,
        lease,
        super::super::test_admission::test_readiness().as_ref(),
        &target,
        IcebergMvUserMutation::Insert,
        context,
    )
    .unwrap_err()
}

#[test]
fn attempt_guard_collects_with_retained_catalog_and_original_sink() {
    let (lease, provider) = fixture(true);
    let stop = ConnectorStopOwner::new();
    let sink: Arc<dyn ConnectorVendedCredentialLeaseSink> = Arc::new(UnusedSink);
    let original = context(&stop).with_vended_credential_lease_sink(sink.clone());
    assert!(original.vended_credential_lease_collection().is_none());
    let error = invoke(&lease, original.clone(), "guard_catalog");
    assert!(error.contains("guard test table is absent"));
    let recorded = provider.captured.lock().unwrap();
    let request = recorded.as_ref().unwrap();
    let collection = request.context.vended_credential_lease_collection().expect(
        "MV guard must bind the attempt sink to its exact retained catalog before provider I/O",
    );
    assert_eq!(
        collection.catalog_properties(),
        lease.binding().catalog_properties().unwrap()
    );
    assert!(Arc::ptr_eq(
        request.context.vended_credential_lease_sink().unwrap(),
        &sink
    ));
    assert_eq!(request.context.deadline(), original.deadline());
    assert_eq!(
        request.table.instance_id,
        lease.binding().descriptor().instance_id
    );
    assert!(matches!(
        request.selector,
        ConnectorTableObjectSelector::Current
    ));
    assert!(matches!(
        request.resolution,
        ConnectorTableResolution::StrictBaseTable
    ));
    stop.request_stop();
    assert!(request.context.is_cancelled());
    assert!(original.vended_credential_lease_collection().is_none());
}

#[test]
fn control_guard_does_not_require_execution_properties_or_mint_an_attempt() {
    let (lease, provider) = fixture(false);
    assert!(lease.binding().catalog_properties().is_err());
    let original = context(&ConnectorStopOwner::new());
    assert!(
        invoke(&lease, original.clone(), "guard_catalog").contains("guard test table is absent")
    );
    let recorded = provider.captured.lock().unwrap();
    let request = recorded.as_ref().unwrap();
    assert!(request.context.vended_credential_lease_sink().is_none());
    assert!(
        request
            .context
            .vended_credential_lease_collection()
            .is_none()
    );
    assert_eq!(request.context.deadline(), original.deadline());
}

#[test]
fn mismatched_catalog_is_rejected_before_provider_capture() {
    let (lease, provider) = fixture(true);
    let original =
        context(&ConnectorStopOwner::new()).with_vended_credential_lease_sink(Arc::new(UnusedSink));
    assert!(
        invoke(&lease, original, "another_catalog").contains("does not match the target connector")
    );
    assert!(provider.captured.lock().unwrap().is_none());
}
