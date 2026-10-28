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

//! The pure MV reader's single named I/O suspension slot.
//!
//! A reader turn never polls an external future. The original async driver
//! services this slot only after that turn's physical worker and forwarder
//! have both exited. Cancellation of a driver borrower does not own this slot.

use novarocks_spi::connector::ConnectorOperationControl;
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll};

use crate::mv::domain::application::{MvApplicationError, MvApplicationErrorKind};
use novarocks_spi::connector::{
    ConnectorChangeWindow, ConnectorControlPlanningLease, ConnectorError,
    ConnectorOriginalResultScope, ConnectorRequestContext, ConnectorScan, ConnectorTableHandle,
    ConnectorTableIdentity, ConnectorTableMetadata, ConnectorTablePreparation,
    ConnectorTableRequest, ConnectorTableResolution, ProviderMetadataCpuPort,
};

/// Concrete inputs only. There is no callback or caller-supplied future.
use crate::mv::domain::iceberg_refresh::IcebergMvCorePorts;
use crate::mv::domain::staged_create::{
    AdmittedMvPublication, CurrentProjectionRegistration, MvPublicationAdmissionReceiving,
};
use novarocks_mv_application::persistence::keyed_documents::{
    MvKeyedObservedCurrentDocuments, StoredKeyedMvProjection,
};
use novarocks_mv_application::state_store_repository::keyed::LoadedKeyedMvProjection;
use novarocks_spi::connector::document_storage::{
    ConnectorDocumentStorageLease, KeyedDocumentObservationRequest,
};

#[expect(
    clippy::large_enum_variant,
    reason = "The complete request stays in the original Arc-owned gate slot and its single loan, then moves into the existing boxed operation; inline variants avoid another request allocation."
)]
enum Request {
    RefreshGraph {
        readiness: Arc<crate::mv::domain::readiness::MvReadinessPort>,
    },
    RebuildReadOnly {
        source: IcebergMvCorePorts,
        lease: ConnectorControlPlanningLease,
        catalog: novarocks_spi::connector::CatalogHandle,
        target: novarocks_mv_application::product::MvTarget,
        table: ConnectorTableIdentity,
        object: novarocks_spi::connector::ConnectorTableObjectId,
        context: ConnectorRequestContext,
    },
    LoadReady {
        source: IcebergMvCorePorts,
        target: novarocks_sql::planning::mv::SqlMvTarget,
    },
    ObserveCurrent {
        source: IcebergMvCorePorts,
        lease: ConnectorDocumentStorageLease,
        request: KeyedDocumentObservationRequest,
    },
    CloseMismatch {
        source: IcebergMvCorePorts,
        lease: ConnectorDocumentStorageLease,
        request: KeyedDocumentObservationRequest,
    },
    Admit {
        source: IcebergMvCorePorts,
        lease: ConnectorControlPlanningLease,
        projection: StoredKeyedMvProjection,
        id: novarocks_spi::connector::LakePublicationId,
        context: ConnectorRequestContext,
    },
    InstallCurrent {
        source: IcebergMvCorePorts,
        catalog: novarocks_spi::connector::CatalogHandle,
        expected:
            Arc<novarocks_mv_application::persistence::keyed_documents::MvKeyedDocumentProjection>,
        context: ConnectorRequestContext,
    },

    Metadata {
        lease: ConnectorControlPlanningLease,
        request: ConnectorTableRequest,
    },
    ChangeWindow {
        lease: ConnectorControlPlanningLease,
        table: ConnectorTableHandle,
        request: novarocks_spi::connector::ConnectorBeginScanRequest,
    },
}
enum Response {
    RefreshGraph(
        Result<crate::mv::domain::dependency::refresh::MvRefreshGraphFacts, MvApplicationError>,
    ),
    RebuildReadOnly(Result<(), MvApplicationError>),
    LoadReady(Result<Option<LoadedKeyedMvProjection>, MvApplicationError>),
    ObserveCurrent(Result<MvKeyedObservedCurrentDocuments, MvApplicationError>),
    CloseMismatch(Result<bool, MvApplicationError>),
    Admit(Result<(), MvApplicationError>),
    InstallCurrent(Result<(), MvApplicationError>),
    MetadataReady,
    ChangeWindow(Result<(), ConnectorError>),
}
type Operation = Pin<Box<dyn Future<Output = Response> + Send + 'static>>;
#[expect(
    clippy::large_enum_variant,
    reason = "The original Arc-owned gate has one complete state slot and loan; Running already owns its boxed operation, so inline request and response variants avoid per-phase containers."
)]
enum State {
    Idle,
    Requested(Request),
    Running(Operation),
    Returned(Response),
    Poisoned,
}

pub(crate) struct MvRefreshPreparationServices {
    pub(crate) cpu: crate::query_execution::internal_result_cpu::InternalResultCpu,
    pub(crate) scope: novarocks_workload_control::WorkScope,
    pub(crate) window: novarocks_workload_control::ResultWindowAlias,
    pub(crate) cancellation: novarocks_query_application::cancellation::QueryCancellationView,
    pub(crate) runtime: tokio::runtime::Handle,
    pub(crate) wake: Arc<dyn crate::task_execution::status_intake::StatusIntakeWake>,
    pub(crate) requests: Arc<dyn novarocks_state_store_api::StateStoreExecutionRequests>,
}
struct ScanReceiving {
    result: Option<Result<ConnectorScan, ConnectorError>>,
    owner: Option<novarocks_spi::connector::ConnectorProcessLocalPayload>,
    cpu: Arc<dyn ProviderMetadataCpuPort>,
}
// The actual Provider future only borrows the opaque-owner slot. Unwind
// restores that same owner before the original driver may retire its future.
struct ScanOwnerLoan {
    gate: std::sync::Weak<MvRefreshPreparationIo>,
    owner: Option<novarocks_spi::connector::ConnectorProcessLocalPayload>,
}
impl Drop for ScanOwnerLoan {
    fn drop(&mut self) {
        let gate = self
            .gate
            .upgrade()
            .expect("original scan gate outlives its child");
        let mut receiver = gate.scan_result.lock().unwrap_or_else(|e| e.into_inner());
        let receiver = receiver
            .as_mut()
            .expect("original scan receiver was installed");
        assert!(
            receiver.owner.is_none(),
            "original scan owner restored twice"
        );
        receiver.owner = self.owner.take();
    }
}
struct Retained {
    _owner: Option<novarocks_spi::connector::ConnectorProcessLocalPayload>,
    cpu: Arc<dyn ProviderMetadataCpuPort>,
}
pub(crate) struct MvRefreshPreparationIo {
    state: Mutex<Option<State>>,
    poisoned: AtomicBool,
    retired_originals: Mutex<Option<Vec<Retained>>>,
    services: Arc<MvRefreshPreparationServices>,
    original: ConnectorOriginalResultScope,
    admission: Arc<MvPublicationAdmissionReceiving>,
    registration: Arc<CurrentProjectionRegistration>,
    optional_failure: Mutex<Option<MvApplicationError>>,
    metadata_result: Mutex<Option<Result<ConnectorTablePreparation, ConnectorError>>>,
    scan_result: Mutex<Option<ScanReceiving>>,
    current_metadata_cpu: Arc<crate::mv::domain::staged_create::CurrentProjectionMetadataCpu>,
}

struct Loan<'a> {
    owner: &'a MvRefreshPreparationIo,
    state: Option<State>,
}
impl Drop for Loan<'_> {
    fn drop(&mut self) {
        // Keep a panicking foreign future in its original slot. Re-polling it
        // is forbidden; the outer Source retains the original health cause.
        let state = self.state.take().expect("MV I/O loan owns its state");
        let mut slot = self.owner.state.lock().unwrap_or_else(|e| e.into_inner());
        assert!(slot.is_none(), "MV I/O loan was restored twice");
        *slot = Some(state);
    }
}
impl MvRefreshPreparationIo {
    pub(crate) fn new(
        services: MvRefreshPreparationServices,
        original: ConnectorOriginalResultScope,
    ) -> Self {
        let current_metadata_cpu = Arc::new(
            crate::mv::domain::staged_create::CurrentProjectionMetadataCpu::business(
                services.cpu.clone(),
                services.scope.clone(),
                services.window.clone(),
                services.cancellation.clone(),
                services.runtime.clone(),
                Arc::clone(&services.wake),
            ),
        );
        Self {
            state: Mutex::new(Some(State::Idle)),
            poisoned: AtomicBool::new(false),
            retired_originals: Mutex::new(Some(Vec::new())),
            services: Arc::new(services),
            original,
            admission: Arc::new(MvPublicationAdmissionReceiving::new()),
            registration: Arc::new(CurrentProjectionRegistration::new()),
            optional_failure: Mutex::new(None),
            metadata_result: Mutex::new(None),
            scan_result: Mutex::new(None),
            current_metadata_cpu,
        }
    }

    pub(crate) async fn refresh_graph(
        &self,
        readiness: Arc<crate::mv::domain::readiness::MvReadinessPort>,
    ) -> Result<crate::mv::domain::dependency::refresh::MvRefreshGraphFacts, MvApplicationError>
    {
        let Response::RefreshGraph(result) =
            self.exchange(Request::RefreshGraph { readiness }).await
        else {
            panic!("original MV gate returned another response to refresh graph");
        };
        result
    }

    fn loan(&self) -> Loan<'_> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .expect("MV I/O has one original driver");
        Loan {
            owner: self,
            state: Some(state),
        }
    }

    async fn exchange(&self, request: Request) -> Response {
        let mut request = Some(request);
        std::future::poll_fn(|_| {
            let mut loan = self.loan();
            if request.is_some() {
                assert!(
                    matches!(loan.state.as_ref(), Some(State::Idle)),
                    "MV pure reader issued overlapping I/O requests"
                );
                loan.state = Some(State::Requested(request.take().unwrap()));
                return Poll::Pending;
            }
            if matches!(loan.state.as_ref(), Some(State::Returned(_))) {
                let Some(State::Returned(response)) = loan.state.replace(State::Idle) else {
                    unreachable!()
                };
                return Poll::Ready(response);
            }
            assert!(
                !matches!(loan.state.as_ref(), Some(State::Idle | State::Poisoned)),
                "MV pure reader lost its I/O response"
            );
            Poll::Pending
        })
        .await
    }

    /// Called only by the async driver, after the preceding CPU job joined.
    pub(crate) fn poll_io(self: &Arc<Self>, cx: &mut Context<'_>) -> Poll<()> {
        assert!(
            !self.poisoned.load(Ordering::Acquire),
            "MV I/O source is poisoned"
        );
        let mut loan = self.loan();
        if matches!(loan.state.as_ref(), Some(State::Requested(_))) {
            let Some(State::Requested(request)) = loan.state.replace(State::Poisoned) else {
                unreachable!()
            };
            let services = Arc::clone(&self.services);
            let original = self.original.clone();
            let admission = Arc::clone(&self.admission);
            let registration = Arc::clone(&self.registration);
            let original_gate = Arc::downgrade(self);
            let current_metadata_cpu = Arc::clone(&self.current_metadata_cpu);
            let operation: Operation = Box::pin(async move {
                match request {
                    Request::RefreshGraph { readiness } => {
                        let view = readiness.in_requests(Arc::clone(&services.requests));
                        Response::RefreshGraph(
                            crate::mv::domain::dependency::refresh::capture_refresh_graph(&view)
                                .await
                                .map_err(repository_error),
                        )
                    }
                    Request::RebuildReadOnly {
                        source,
                        lease,
                        catalog,
                        target,
                        table,
                        object,
                        context,
                    } => {
                        let view = source
                            .readiness()
                            .in_requests(Arc::clone(&services.requests));
                        Response::RebuildReadOnly(crate::mv::domain::lake_rebuild::rebuild_one_lake_package_if_missing_verified_original(
                            &view,&lease,catalog,target,table,object,context).await.map_err(projection_error))
                    }
                    Request::LoadReady { source, target } => {
                        let view = source
                            .readiness()
                            .in_requests(Arc::clone(&services.requests));
                        Response::LoadReady(
                            view.load_ready(&target).await.map_err(repository_error),
                        )
                    }
                    Request::ObserveCurrent {
                        source,
                        lease,
                        request,
                    } => {
                        let reader = source
                            .readiness()
                            .in_requests(Arc::clone(&services.requests))
                            .candidate_reader();
                        Response::ObserveCurrent(
                            reader
                                .observe_current_application_async(&lease, request)
                                .await
                                .map_err(query_error),
                        )
                    }
                    Request::CloseMismatch {
                        source,
                        lease,
                        request,
                    } => {
                        let result = match source.management_entrance() {
                            Ok(entrance) => entrance
                                .close_on_keyed_current_management_mismatch_async(&lease, request)
                                .await
                                .map_err(document_error),
                            Err(error) => Err(contract_error(error)),
                        };
                        Response::CloseMismatch(result)
                    }
                    Request::Admit {
                        source,
                        lease,
                        projection,
                        id,
                        context,
                    } => {
                        let result = match source.management_entrance() {
                            Ok(entrance) => {
                                crate::mv::domain::staged_create::admit_mv_publication_async_in(
                                    entrance,
                                    &lease,
                                    &projection,
                                    id,
                                    &context,
                                    async {
                                        let _ = services.cancellation.cancelled().await;
                                    },
                                    &admission,
                                )
                                .await
                            }
                            Err(error) => Err(contract_error(error)),
                        };
                        Response::Admit(result)
                    }
                    Request::InstallCurrent {
                        source,
                        catalog,
                        expected,
                        context,
                    } => {
                        let view = source
                            .readiness()
                            .in_requests(Arc::clone(&services.requests));
                        let result=match source.management_entrance() {
                            Ok(entrance)=>crate::mv::domain::staged_create::install_metadata_only_current_projection_async_in(
                                entrance,&view,source.connector_control(),catalog,expected,context,&current_metadata_cpu,&registration
                            ).await.map_err(projection_error),
                            Err(error)=>Err(contract_error(error)),
                        };
                        Response::InstallCurrent(result)
                    }
                    Request::Metadata { lease, request } => {
                        let mut child = lease.binding().metadata().prepare_table(request);
                        let result = std::future::poll_fn(|cx| child.as_mut().poll(cx)).await;
                        {
                            let gate = original_gate
                                .upgrade()
                                .expect("original MV gate outlives its child");
                            let mut receiver = gate
                                .metadata_result
                                .lock()
                                .unwrap_or_else(|e| e.into_inner());
                            assert!(
                                receiver.is_none(),
                                "original MV metadata receiver is occupied"
                            );
                            *receiver = Some(result);
                        }
                        // The unique result is in the original gate before the
                        // Provider future is allowed to run its destructor.
                        drop(child);
                        Response::MetadataReady
                    }
                    Request::ChangeWindow {
                        lease,
                        table,
                        request,
                    } => {
                        let cpu = match services.cpu.planning_metadata_cpu_port(
                            &services.scope,
                            services.window.clone(),
                            &lease,
                            services.cancellation.clone(),
                            services.runtime.clone(),
                            Arc::clone(&services.wake),
                        ) {
                            Ok(cpu) => cpu,
                            Err(error) => {
                                return Response::ChangeWindow(Err(ConnectorError::from_domain(
                                    work_error_kind(&error),
                                    error,
                                )));
                            }
                        };
                        {
                            let gate = original_gate
                                .upgrade()
                                .expect("original scan gate outlives its child");
                            let mut receiver =
                                gate.scan_result.lock().unwrap_or_else(|e| e.into_inner());
                            assert!(receiver.is_none(), "original MV scan receiver is occupied");
                            *receiver = Some(ScanReceiving {
                                result: None,
                                owner: None,
                                cpu: Arc::clone(&cpu),
                            });
                        }
                        let mut owner = ScanOwnerLoan {
                            gate: original_gate.clone(),
                            owner: None,
                        };
                        let mut child = lease.binding().planning().begin_scan_async_in(
                            &table,
                            request,
                            &cpu,
                            &original,
                            &mut owner.owner,
                        );
                        let result = std::future::poll_fn(|cx| child.as_mut().poll(cx)).await;
                        {
                            let gate = original_gate
                                .upgrade()
                                .expect("original scan gate outlives its child");
                            let mut receiver =
                                gate.scan_result.lock().unwrap_or_else(|e| e.into_inner());
                            let receiver = receiver
                                .as_mut()
                                .expect("original scan receiver was installed");
                            assert!(
                                receiver.result.is_none(),
                                "original scan result received twice"
                            );
                            receiver.result = Some(result);
                        }
                        drop(child);
                        drop(owner);
                        Response::ChangeWindow(Ok(()))
                    }
                }
            });
            loan.state = Some(State::Running(operation));
        }
        match loan.state.as_mut().expect("MV I/O state") {
            State::Running(operation) => {
                self.poisoned.store(true, Ordering::Release);
                let result = operation.as_mut().poll(cx);
                self.poisoned.store(false, Ordering::Release);
                match result {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(response) => {
                        // Store the result before dropping its child future.
                        // The loan restores it even if that destructor panics.
                        let retired = loan.state.replace(State::Returned(response));
                        self.poisoned.store(true, Ordering::Release);
                        drop(retired);
                        self.poisoned.store(false, Ordering::Release);
                        Poll::Ready(())
                    }
                }
            }
            State::Returned(_) => Poll::Ready(()),
            State::Idle => panic!("MV reader suspended without a named I/O request"),
            State::Requested(_) => unreachable!(),
            State::Poisoned => panic!("MV I/O factory did not complete"),
        }
    }

    pub(crate) async fn rebuild_read_only(
        &self,
        source: &IcebergMvCorePorts,
        lease: ConnectorControlPlanningLease,
        catalog: novarocks_spi::connector::CatalogHandle,
        target: novarocks_mv_application::product::MvTarget,
        table: ConnectorTableIdentity,
        object: novarocks_spi::connector::ConnectorTableObjectId,
        context: ConnectorRequestContext,
    ) -> Result<(), MvApplicationError> {
        let Response::RebuildReadOnly(result) = self
            .exchange(Request::RebuildReadOnly {
                source: source.clone(),
                lease,
                catalog,
                target,
                table,
                object,
                context,
            })
            .await
        else {
            panic!("MV read-only installation response changed kind")
        };
        result
    }
    pub(crate) fn retain_optional_failure(&self, error: MvApplicationError) {
        let mut error = Some(error);
        {
            let mut slot = self
                .optional_failure
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if slot.is_none() {
                *slot = error.take();
            }
        }
        drop(error);
    }
    pub(crate) fn registration(&self) -> &CurrentProjectionRegistration {
        &self.registration
    }
    pub(crate) async fn load_ready(
        &self,
        source: &IcebergMvCorePorts,
        target: novarocks_sql::planning::mv::SqlMvTarget,
    ) -> Result<Option<LoadedKeyedMvProjection>, MvApplicationError> {
        let Response::LoadReady(result) = self
            .exchange(Request::LoadReady {
                source: source.clone(),
                target,
            })
            .await
        else {
            panic!("MV readiness response changed kind")
        };
        result
    }
    pub(crate) async fn definition(
        &self,
        source: &IcebergMvCorePorts,
        target: &crate::mv::domain::refresh::target::IcebergMvTarget,
    ) -> Result<StoredKeyedMvProjection, MvApplicationError> {
        let ready = self
            .load_ready(
                source,
                novarocks_sql::planning::mv::SqlMvTarget {
                    catalog: Some(target.catalog.clone()),
                    database: target.namespace.clone(),
                    name: target.table.clone(),
                },
            )
            .await?;
        ready.map(|v| v.projection).ok_or_else(|| {
            contract_error(format!(
                "iceberg materialized view {}.{}.{} has no MV definition",
                target.catalog, target.namespace, target.table
            ))
        })
    }
    pub(crate) async fn observe_current(
        &self,
        source: &IcebergMvCorePorts,
        lease: &ConnectorDocumentStorageLease,
        request: KeyedDocumentObservationRequest,
    ) -> Result<MvKeyedObservedCurrentDocuments, MvApplicationError> {
        let Response::ObserveCurrent(result) = self
            .exchange(Request::ObserveCurrent {
                source: source.clone(),
                lease: lease.clone(),
                request,
            })
            .await
        else {
            panic!("MV Current response changed kind")
        };
        result
    }
    pub(crate) async fn close_mismatch(
        &self,
        source: &IcebergMvCorePorts,
        lease: &ConnectorDocumentStorageLease,
        request: KeyedDocumentObservationRequest,
    ) -> Result<bool, MvApplicationError> {
        let Response::CloseMismatch(result) = self
            .exchange(Request::CloseMismatch {
                source: source.clone(),
                lease: lease.clone(),
                request,
            })
            .await
        else {
            panic!("MV management response changed kind")
        };
        result
    }
    pub(crate) async fn admit(
        &self,
        source: &IcebergMvCorePorts,
        lease: &ConnectorControlPlanningLease,
        projection: &StoredKeyedMvProjection,
        id: novarocks_spi::connector::LakePublicationId,
        context: &ConnectorRequestContext,
    ) -> Result<AdmittedMvPublication, MvApplicationError> {
        let Response::Admit(result) = self
            .exchange(Request::Admit {
                source: source.clone(),
                lease: lease.clone(),
                projection: projection.clone(),
                id,
                context: context.clone(),
            })
            .await
        else {
            panic!("MV admission response changed kind")
        };
        result?;
        let mut result = None;
        self.admission.take_admitted_in(&mut result)?;
        Ok(result.expect("successful MV admission installs the original lease"))
    }
    pub(crate) async fn install_current(
        &self,
        source: &IcebergMvCorePorts,
        catalog: novarocks_spi::connector::CatalogHandle,
        expected: Arc<
            novarocks_mv_application::persistence::keyed_documents::MvKeyedDocumentProjection,
        >,
        context: ConnectorRequestContext,
    ) -> Result<(), MvApplicationError> {
        let Response::InstallCurrent(result) = self
            .exchange(Request::InstallCurrent {
                source: source.clone(),
                catalog,
                expected,
                context,
            })
            .await
        else {
            panic!("MV Current installation response changed kind")
        };
        result
    }

    pub(crate) fn has_request(&self) -> bool {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        matches!(
            state.as_ref(),
            Some(State::Requested(_) | State::Running(_) | State::Returned(_))
        )
    }

    /// The metadata future only prepares Provider facts. Schema creation and
    /// all application projection below execute in the original CPU turn.
    pub(crate) async fn metadata(
        &self,
        lease: &ConnectorControlPlanningLease,
        context: ConnectorRequestContext,
        namespace: &str,
        table: &str,
        resolution: ConnectorTableResolution,
    ) -> Result<ConnectorTableMetadata, ConnectorError> {
        let context = crate::connector::context_for_planning_lease_typed(lease, context)?;
        context.original_operation_control().check_active()?;
        let request = ConnectorTableRequest {
            table: ConnectorTableIdentity {
                instance_id: lease.binding().descriptor().instance_id.clone(),
                namespace: Arc::from(namespace),
                table: Arc::from(table),
            },
            resolution,
            context: context.clone(),
        };
        let Response::MetadataReady = self
            .exchange(Request::Metadata {
                lease: lease.clone(),
                request,
            })
            .await
        else {
            panic!("MV metadata response changed kind")
        };
        let preparation = self
            .metadata_result
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .expect("original metadata result was received before its child exited")?;
        crate::connector::schema_creation::with_schema_creation(&context, |creation| {
            lease
                .binding()
                .metadata()
                .create_prepared_table(preparation, creation)
        })
        .map_err(crate::connector::schema_creation::connector_schema_error)
    }

    pub(crate) async fn change_window(
        &self,
        lease: &ConnectorControlPlanningLease,
        table: &ConnectorTableHandle,
        schema: arrow::datatypes::SchemaRef,
        context: ConnectorRequestContext,
        window: ConnectorChangeWindow,
    ) -> Result<ConnectorScan, MvApplicationError> {
        crate::connector::scan_admission::validate_owner(table, lease).map_err(provider_error)?;
        let request = crate::connector::scan_admission::change_window_request(
            schema.as_ref(),
            context,
            window,
        );
        let Response::ChangeWindow(result) = self
            .exchange(Request::ChangeWindow {
                lease: lease.clone(),
                table: table.clone(),
                request,
            })
            .await
        else {
            panic!("MV change-window response changed kind")
        };
        result.map_err(provider_error)?;
        let mut received = self
            .scan_result
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .expect("original scan result was received before its child exited");
        let result = received
            .result
            .take()
            .expect("original scan raw result is present");
        self.retired_originals
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_mut()
            .expect("original MV retained custody is not loaned")
            .push(Retained {
                _owner: received.owner,
                cpu: received.cpu,
            });
        let scan = result.map_err(provider_error)?;
        crate::connector::scan_admission::validate_scan(scan, schema.as_ref(), lease, window)
            .map_err(provider_error)
    }
}

pub(crate) fn provider_error(error: ConnectorError) -> MvApplicationError {
    let kind = if error.kind() == novarocks_spi::connector::ConnectorErrorKind::Unavailable {
        MvApplicationErrorKind::Unavailable
    } else {
        MvApplicationErrorKind::Engine
    };
    MvApplicationError::from_connector(kind, error)
}
pub(crate) fn contract_error(message: impl Into<String>) -> MvApplicationError {
    MvApplicationError::new(MvApplicationErrorKind::InvalidRequest, message)
}

fn work_error_kind(
    error: &novarocks_workload_control::WorkError,
) -> novarocks_spi::connector::ConnectorErrorKind {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use novarocks_spi::connector::ConnectorErrorKind as Kind;
    match error.failure_fact().class() {
        FailureClass::Resource => Kind::ResourceExhausted,
        FailureClass::Control => Kind::Interrupted,
        FailureClass::Data | FailureClass::Representation => Kind::CorruptData,
        FailureClass::Dependency => Kind::Unavailable,
        FailureClass::Unsupported => Kind::Unsupported,
        FailureClass::Invariant | FailureClass::Unexpected => Kind::Internal,
    }
}

fn repository_error(
    error: novarocks_mv_application::repository::MvRepositoryError,
) -> MvApplicationError {
    let message = error.to_string();
    let cause = error
        .original_failure()
        .cloned()
        .unwrap_or_else(|| novarocks_failure_contract::SharedFailureCause::new(error));
    MvApplicationError::new(MvApplicationErrorKind::Engine, message).with_original_failure(cause)
}
fn query_error(
    error: novarocks_query_application::session_error::QueryServiceError,
) -> MvApplicationError {
    let mut result = MvApplicationError::new(MvApplicationErrorKind::Engine, error.to_string());
    if let Some(cause) = error.failure_cause() {
        result = result.with_original_failure(cause.clone());
    }
    result
}
fn projection_error(
    error: novarocks_mv_application::readiness::MvProjectionError,
) -> MvApplicationError {
    let mut result = MvApplicationError::new(MvApplicationErrorKind::Engine, error.to_string());
    if let Some(cause) = error.original_failure() {
        result = result.with_original_failure(cause.clone());
    }
    result
}
fn document_error(
    error: novarocks_mv_application::persistence::documents::MvDocumentError,
) -> MvApplicationError {
    query_error(crate::mv::domain::readiness::document_application_error(
        error,
    ))
}

struct RetainedLoan<'a> {
    original: &'a MvRefreshPreparationIo,
    values: Option<Vec<Retained>>,
}
impl Drop for RetainedLoan<'_> {
    fn drop(&mut self) {
        let values = self
            .values
            .take()
            .expect("MV retained loan owns its values");
        let mut slot = self
            .original
            .retired_originals
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        assert!(slot.is_none(), "MV retained loan restored twice");
        *slot = Some(values);
    }
}
impl MvRefreshPreparationIo {
    fn retained_loan(&self) -> RetainedLoan<'_> {
        let values = self
            .retired_originals
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .expect("MV retained custody has one original driver");
        RetainedLoan {
            original: self,
            values: Some(values),
        }
    }
    pub(crate) fn local_health_failure(
        &self,
    ) -> Option<novarocks_query_application::api::StatementNativeHealthFailure> {
        if let Some(health) = self.registration.local_health_failure() {
            return Some(health);
        }
        let scan_cpu = self
            .scan_result
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|received| Arc::clone(&received.cpu));
        if let Some(cpu) = scan_cpu
            && let Some(cause) = cpu.local_failure()
        {
            return Some(novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven(
                    "MV metadata CPU original exit is unproven", cause));
        }
        let loan = self.retained_loan();
        for retained in loan.values.as_ref().unwrap() {
            if let Some(cause) = retained.cpu.local_failure() {
                return Some(novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven(
                    "MV metadata CPU original exit is unproven",cause));
            }
        }
        None
    }
    /// Original Action calls this after its reader CPU job actually joined.
    /// A dropped borrower does not drop a running Provider operation or owner.
    pub(crate) fn poll_closed(
        self: &Arc<Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), novarocks_query_application::api::StatementNativeHealthFailure>> {
        if self.poisoned.load(Ordering::Acquire) {
            return Poll::Pending;
        }
        let running = {
            let slot = self.state.lock().unwrap_or_else(|e| e.into_inner());
            matches!(slot.as_ref(), Some(State::Running(_)))
        };
        if running && self.poll_io(cx).is_pending() {
            return Poll::Pending;
        }
        let mut loan = self.loan();
        // Unstarted requests have no child or physical action. Withdrawal is
        // outside the slot lock; already running requests were drained above.
        if matches!(loan.state.as_ref(), Some(State::Requested(_))) {
            let retired = loan.state.replace(State::Idle);
            drop(retired);
        }
        let scan_cpu = self
            .scan_result
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|received| Arc::clone(&received.cpu));
        if let Some(cpu) = scan_cpu {
            match cpu.poll_closed(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven(
                    "MV metadata CPU close is unproven", Arc::new(error)))),
                Poll::Ready(Ok(())) => {},
            }
            if let Some(cause) = cpu.local_failure() {
                return Poll::Ready(Err(novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven(
                    "MV metadata CPU original exit is unproven", cause)));
            }
        }
        drop(loan);
        let retained = self.retained_loan();
        for item in retained.values.as_ref().unwrap() {
            match item.cpu.poll_closed(cx) {
                Poll::Pending=>return Poll::Pending,
                Poll::Ready(Err(error))=>return Poll::Ready(Err(novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven(
                    "MV metadata CPU close is unproven",Arc::new(error)))),
                Poll::Ready(Ok(()))=>{},
            }
            if let Some(cause) = item.cpu.local_failure() {
                return Poll::Ready(Err(novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven(
                    "MV metadata CPU original exit is unproven",cause)));
            }
        }
        drop(retained);
        let _ = self.registration.poll_progress(cx);
        if self.registration.poll_registration_closed(cx).is_pending() {
            return Poll::Pending;
        }
        if let Some(health) = self.registration.local_health_failure() {
            return Poll::Ready(Err(health));
        }
        // Original source owners remain here until the containing Action has
        // received this proof and actually disposes this Source outside locks.
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod repository_original_cause_tests {
    use super::*;
    use novarocks_failure_contract::FailureProjection;
    use novarocks_state_store_api::{
        StateStoreError, StateStoreErrorKind, StateStoreExecutionContext,
        StateStoreExecutionFailure, StateStoreExecutionRequests, StateStoreReadExecution,
    };
    use std::sync::atomic::AtomicUsize;

    struct RefusedRequests {
        cause: StateStoreExecutionFailure,
        reads: AtomicUsize,
    }
    impl StateStoreReadExecution for RefusedRequests {
        fn read_transaction_context(
            &self,
        ) -> Result<StateStoreExecutionContext, StateStoreExecutionFailure> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            Err(self.cause.clone())
        }
    }
    impl StateStoreExecutionRequests for RefusedRequests {
        fn write_operation_context(
            &self,
        ) -> Result<StateStoreExecutionContext, StateStoreExecutionFailure> {
            panic!("the original repository read must not request a write context")
        }
    }

    #[tokio::test]
    async fn repository_read_keeps_the_original_request_refusal_identity() {
        // Reuse the real repository and its original request-local read entry.
        // This is a pre-begin API refusal, not a physical SQLite/QB exit test.
        let (_product, repository) = crate::mv::domain::test_admission::keyed_product().await;
        let cause = StateStoreError::new(
            StateStoreErrorKind::Cancelled,
            "original repository read owner is stopped",
        )
        .into_original_failure();
        let requests = RefusedRequests {
            cause: cause.clone(),
            reads: AtomicUsize::new(0),
        };
        let target =
            novarocks_mv_application::product::MvTarget::from_parts(Some("ice"), "sales", "orders");
        let error = match repository
            .in_requests(&requests)
            .find_keyed_by_target(
                &target,
                novarocks_mv_application::persistence::validation::PersistenceDecodeBudget::default(
                ),
            )
            .await
        {
            Err(error) => error,
            Ok(payload) => {
                drop(payload);
                panic!("the actual request refusal must reach the repository boundary");
            }
        };
        assert_eq!(requests.reads.load(Ordering::SeqCst), 1);
        assert_eq!(error.original_failure(), Some(&cause));
        let projected = repository_error(error);
        assert_eq!(projected.kind(), MvApplicationErrorKind::Engine);
        assert_eq!(projected.original_failure(), Some(&cause));
        assert_eq!(
            projected.original_failure().unwrap().failure_fact(),
            cause.failure_fact()
        );
        assert!(
            projected
                .original_failure()
                .unwrap()
                .downcast_ref::<StateStoreError>()
                .is_some()
        );
        assert_eq!(projected.clone().original_failure(), Some(&cause));
    }

    #[test]
    fn repository_local_refusal_captures_its_actual_author_once() {
        let error = novarocks_mv_application::repository::MvRepositoryError::new(
            novarocks_mv_application::repository::MvRepositoryErrorKind::Corruption,
            "actual local repository codec refusal",
        );
        let fact = error.failure_fact();
        let projected = repository_error(error.clone());
        let cause = projected.original_failure().unwrap();
        assert_eq!(
            cause.downcast_ref::<novarocks_mv_application::repository::MvRepositoryError>(),
            Some(&error)
        );
        assert_eq!(cause.failure_fact(), fact);
        assert_eq!(projected.clone().original_failure(), Some(cause));
    }
}
