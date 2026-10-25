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
    refusal: ConnectorError,
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
        Err(self.refusal.clone())
    }

    fn capture_table_object_binding_async(
        &self,
        request: ConnectorTableObjectCaptureRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<ConnectorTableObjectBinding, ConnectorError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            *self.captured.lock().unwrap() = Some(request);
            Err(self.refusal.clone())
        })
    }

    fn capture_table_object_with_snapshot_cpu(
        &self,
        request: ConnectorTableObjectCaptureRequest,
        _: Arc<dyn ProviderMetadataCpuPort>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<(ConnectorTableObjectBinding, Option<i64>), ConnectorError>,
                > + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            *self.captured.lock().unwrap() = Some(request);
            Err(self.refusal.clone())
        })
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
    refusal_fixture(with_properties, missing_table())
}

fn refusal_fixture(
    with_properties: bool,
    refusal: ConnectorError,
) -> (ConnectorControlPlanningLease, Arc<RecordingMetadata>) {
    let instance = ConnectorInstanceId::parse("guard_catalog").unwrap();
    let original =
        novarocks_catalog_application::test_support::test_control_binding_for(instance.clone(), 8);
    let metadata = Arc::new(RecordingMetadata {
        instance: instance.clone(),
        captured: Mutex::new(None),
        refusal,
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

// The provider refuses before creating a metadata CPU input. This port must
// never be used as a fabricated positive admission or an actual-exit proof.
struct UnusedMetadataCpu;
impl ProviderMetadataCpuPort for UnusedMetadataCpu {
    fn submit_metadata_in(
        &self,
        _: &mut Option<ProviderMetadataCpuInput>,
    ) -> Result<(), ConnectorError> {
        panic!("provider already refused before CPU input")
    }
    fn poll_finished(
        &self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), ConnectorError>> {
        panic!("no CPU input was dispatched")
    }
    fn poll_closed(
        &self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), ConnectorError>> {
        panic!("the fixture claims no CPU exit")
    }
    fn local_failure(&self) -> Option<Arc<dyn std::error::Error + Send + Sync>> {
        None
    }
}

struct GuardRefusalPort {
    error: novarocks_query_application::session_error::QueryServiceError,
}
impl novarocks_table_maintenance::product::TableMaintenanceEffectPort for GuardRefusalPort {
    fn observe_cleanup_owned_ref<'a>(
        &'a self,
        _: &'a novarocks_table_maintenance::gc_observation::GcOwnedRefObservationAccelerator,
        _: novarocks_table_maintenance::gc_observation::GcOwnedRefObservation,
        _: i64,
        _: i64,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<
            novarocks_table_maintenance::gc_observation::GcOwnedRefObservationDecision,
            novarocks_table_maintenance::runtime::TerminalError,
        >,
    > {
        Box::pin(async {
            Err(
                novarocks_table_maintenance::runtime::TerminalError::pre_dispatch_failed(
                    "this effect source cannot observe owned-ref cleanup",
                ),
            )
        })
    }
    fn reject_user_action_on_mv<'a>(
        &'a self,
        _: &'a novarocks_table_maintenance::MaintenanceTarget,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<(), novarocks_table_maintenance::runtime::TerminalError>,
    > {
        Box::pin(async move {
            Err(
                crate::query_execution::maintenance::rewrite_job::guard_terminal_error(
                    self.error.clone(),
                ),
            )
        })
    }
    fn execute_metadata<'a>(
        &'a self,
        _: novarocks_table_maintenance::MaintenanceActionRequest,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<novarocks_table_maintenance::MaintenanceActionOutcome, String>,
    > {
        Box::pin(async { panic!("MV guard refusal precedes metadata effects") })
    }
    fn begin_rewrite<'a>(
        &'a self,
        _: &'a novarocks_table_maintenance::MaintenanceTarget,
        _: novarocks_table_maintenance::product::RewriteIntent,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<
            Box<dyn novarocks_table_maintenance::product::DistributedRewriteSession + 'a>,
            String,
        >,
    > {
        Box::pin(async { panic!("MV guard refusal precedes Rewrite") })
    }
    fn begin_cleanup<'a>(
        &'a self,
        _: &'a novarocks_table_maintenance::MaintenanceTarget,
        _: i64,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<
            Box<dyn novarocks_table_maintenance::product::CleanupSession + 'a>,
            novarocks_table_maintenance::runtime::TerminalError,
        >,
    > {
        Box::pin(async { panic!("MV guard refusal precedes Cleanup session creation") })
    }
}

#[tokio::test]
async fn asynchronous_guard_provider_cause_survives_cleanup_product_and_service() {
    use novarocks_failure_contract::{FailureProjection, SharedFailureCause};
    use novarocks_table_maintenance::{
        MaintenanceActionRequest, MaintenanceTarget, product::TableMaintenanceProduct,
        runtime::MaintenanceJobState,
    };
    for control in [
        novarocks_type_contract::CompileControlError::ResourceExhausted,
        novarocks_type_contract::CompileControlError::Cancelled,
    ] {
        for governed in [false, true] {
            let original = ConnectorError::from_domain_static(
                ConnectorErrorKind::Interrupted,
                control.clone(),
                "actual guard provider refusal",
            );
            let cause = std::error::Error::source(&original)
                .unwrap()
                .downcast_ref::<SharedFailureCause>()
                .unwrap()
                .clone();
            let expected = original.failure_fact();
            let (lease, provider) = refusal_fixture(true, original);
            let target = TargetBackend {
                provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                catalog: "guard_catalog".into(),
                namespace: "db".into(),
                table: "table".into(),
            };
            let stop = ConnectorStopOwner::new();
            let readiness = super::super::test_admission::test_readiness();
            let error = if governed {
                super::reject_if_iceberg_mv_table_with_governed_planning_lease_and_context_async(
                    &UnavailableMvStorageObservationPort,
                    &lease,
                    &readiness,
                    &target,
                    IcebergMvUserMutation::Maintenance,
                    context(&stop),
                    Arc::new(UnusedMetadataCpu),
                )
                .await
                .unwrap_err()
            } else {
                super::reject_if_iceberg_mv_table_with_planning_lease_and_context_async(
                    &UnavailableMvStorageObservationPort,
                    &lease,
                    &readiness,
                    &target,
                    IcebergMvUserMutation::Insert,
                    context(&stop),
                )
                .await
                .unwrap_err()
            };
            assert!(
                provider.captured.lock().unwrap().is_some(),
                "actual selected provider capture was called"
            );
            assert_eq!(error.failure_cause(), Some(&cause));
            assert_eq!(error.execution_failure().unwrap().fact(), expected);
            let terminal = TableMaintenanceProduct::new(None)
                .execute_user_action(
                    &GuardRefusalPort { error },
                    MaintenanceActionRequest::RemoveOrphanFiles {
                        target: MaintenanceTarget {
                            catalog: target.catalog,
                            namespace: target.namespace,
                            table: target.table,
                        },
                        older_than_ms: 1,
                    },
                )
                .await
                .unwrap_err();
            assert_eq!(terminal.state, MaintenanceJobState::PreDispatchFailed);
            assert_eq!(
                terminal.effect,
                Some(novarocks_coordination_contract::ExecutionEffect::None)
            );
            assert_eq!(terminal.failure_cause.as_ref(), Some(&cause));
            assert_eq!(terminal.selected_failure_fact(), Some(expected));
            let service =
                crate::query_execution::maintenance::rewrite_job::computation_service_error(
                    terminal,
                );
            assert_eq!(service.failure_cause(), Some(&cause));
            assert_eq!(service.execution_failure().unwrap().fact(), expected);
        }
    }
}

#[tokio::test]
async fn ordinary_mv_policy_denial_has_no_invented_source_or_failure_fact() {
    use novarocks_table_maintenance::{
        MaintenanceActionRequest, MaintenanceTarget, product::TableMaintenanceProduct,
    };
    let message = "table guard_catalog.db.table is a materialized view; use ALTER MATERIALIZED VIEW or DROP MATERIALIZED VIEW";
    let error = super::materialized_view_denial(
        &TargetBackend {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            catalog: "guard_catalog".into(),
            namespace: "db".into(),
            table: "table".into(),
        },
        IcebergMvUserMutation::Maintenance,
    );
    let terminal = TableMaintenanceProduct::new(None)
        .execute_user_action(
            &GuardRefusalPort { error },
            MaintenanceActionRequest::RemoveOrphanFiles {
                target: MaintenanceTarget {
                    catalog: "guard_catalog".into(),
                    namespace: "db".into(),
                    table: "table".into(),
                },
                older_than_ms: 1,
            },
        )
        .await
        .unwrap_err();
    assert!(terminal.failure_cause.is_none());
    assert!(terminal.selected_failure_fact().is_none());
    let service =
        crate::query_execution::maintenance::rewrite_job::computation_service_error(terminal);
    assert_eq!(service.message(), message);
    assert!(service.failure_cause().is_none());
    assert!(service.execution_failure().is_none());
}
