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

//! Production Backend gRPC service and its domain handlers.
//!
//! The generic generated listener, authenticated transport, and its lifecycle
//! are owned by `novarocks-native-adapter`; this role owns its service facts.

use std::sync::Arc;

use crate::rpc::data_plane::BackendDataPlane;
use crate::rpc::task_execution::{TaskExecutionIngress, TaskStatusEventStream};
use novarocks_execution::runtime::fragment::io::ExchangeReceiverPort;
use novarocks_proto_codec::catalog::{PruneCatalogsRequest, PruneCatalogsResponse};
use novarocks_proto_models::{catalog, filter, novarocks as proto};
use tokio_stream::wrappers::ReceiverStream;

use crate::runtime_filter::rpc::{
    BackendRuntimeFilterEnvelopeIngress, handle_runtime_filter_envelope,
};
use novarocks_native_adapter::{
    backend_heartbeat::BackendHeartbeatResponder, generated::nova_rocks_grpc_server::NovaRocksGrpc,
};
use novarocks_worker::{CatalogPruneResult, TaskInboundCapabilities};

/// What a rejected catalog prune is allowed to say on the wire.
///
/// A catalog definition carries credential material. This detail is a fixed
/// string rather than anything derived from the handles involved, so no part
/// of a catalog's properties can reach an error, a log, or a status by being
/// interpolated into a rejection.
const CATALOG_PRUNE_STALE_SNAPSHOT_DETAIL: &str =
    "catalog reachability snapshot omits one or more live catalogs";

/// This process's owner of catalog reachability.
///
/// The frontend sends one complete reachability snapshot; the owner reconciles
/// its retained catalog runtimes against it and answers in its own vocabulary.
/// This port exists so the wire handler never holds a catalog registry of its
/// own: there is exactly one `CatalogManager` per process, and a second
/// reconciler would be a second authority over the same leases.
pub(crate) trait CatalogReachabilityAuthority: Send + Sync + 'static {
    fn prune_unreachable_catalogs(
        &self,
        reachable: std::collections::BTreeSet<novarocks_spi::connector::CatalogHandle>,
    ) -> CatalogPruneResult;
}

/// Backend-owned production Tonic service. Domain owners contribute the narrow
/// ingress ports while this service composes them with `BackendDataPlane`.
#[derive(Clone)]
pub(crate) struct BackendRpcService {
    task_execution_ingress: Arc<dyn TaskExecutionIngress>,
    catalog_reachability: Arc<dyn CatalogReachabilityAuthority>,
    heartbeat: BackendHeartbeatResponder,
    data_plane: BackendDataPlane,
    runtime_filter_ingress: Arc<dyn BackendRuntimeFilterEnvelopeIngress>,
}

impl BackendRpcService {
    pub(crate) fn new(
        task_execution_ingress: Arc<dyn TaskExecutionIngress>,
        catalog_reachability: Arc<dyn CatalogReachabilityAuthority>,
        runtime_filter_ingress: Arc<dyn BackendRuntimeFilterEnvelopeIngress>,
        exchange_receiver_port: Arc<dyn ExchangeReceiverPort>,
        task_inbound_capabilities: Arc<TaskInboundCapabilities>,
        heartbeat: BackendHeartbeatResponder,
    ) -> Self {
        Self {
            task_execution_ingress,
            catalog_reachability,
            heartbeat,
            data_plane: BackendDataPlane::with_exchange_receiver_port(
                exchange_receiver_port,
                task_inbound_capabilities,
            ),
            runtime_filter_ingress,
        }
    }
}

#[tonic::async_trait]
impl NovaRocksGrpc for BackendRpcService {
    type ExchangeStream = std::pin::Pin<
        Box<
            dyn tokio_stream::Stream<Item = Result<proto::ExchangeResponse, tonic::Status>>
                + Send
                + 'static,
        >,
    >;
    type SubscribeTaskStatusStream = TaskStatusEventStream;

    async fn announce_backend(
        &self,
        _request: tonic::Request<proto::AnnounceBackendRequest>,
    ) -> Result<tonic::Response<proto::AnnounceBackendResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "backend announce is accepted only by the frontend native ingress",
        ))
    }

    async fn exchange(
        &self,
        request: tonic::Request<tonic::Streaming<proto::ExchangeRequest>>,
    ) -> Result<tonic::Response<Self::ExchangeStream>, tonic::Status> {
        let mut inbound = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(4096);
        let kernel = self.data_plane.clone();
        tokio::spawn(async move {
            loop {
                let request = match inbound.message().await {
                    Ok(Some(request)) => request,
                    Ok(None) => break,
                    Err(error) => {
                        let _ = tx
                            .send(Err(tonic::Status::internal(format!(
                                "exchange recv failed: {error}"
                            ))))
                            .await;
                        break;
                    }
                };
                let kernel = kernel.clone();
                let response =
                    match tokio::task::spawn_blocking(move || kernel.exchange(request)).await {
                        Ok(response) => response,
                        Err(error) => {
                            let _ = tx
                                .send(Err(tonic::Status::internal(format!(
                                    "exchange handler panicked: {error}"
                                ))))
                                .await;
                            break;
                        }
                    };
                let failed = response
                    .status
                    .as_ref()
                    .is_some_and(|status| status.code != 0);
                if tx.send(Ok(response)).await.is_err() || failed {
                    break;
                }
            }
        });
        Ok(tonic::Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn exchange_unary(
        &self,
        request: tonic::Request<proto::ExchangeRequest>,
    ) -> Result<tonic::Response<proto::ExchangeResponse>, tonic::Status> {
        let kernel = self.data_plane.clone();
        let response = tokio::task::spawn_blocking(move || kernel.exchange(request.into_inner()))
            .await
            .map_err(|error| {
                tonic::Status::internal(format!("exchange_unary handler panicked: {error}"))
            })?;
        Ok(tonic::Response::new(response))
    }

    async fn transmit_runtime_filter_envelope(
        &self,
        request: tonic::Request<filter::RuntimeFilterEnvelope>,
    ) -> Result<tonic::Response<filter::RuntimeFilterEnvelopeResponse>, tonic::Status> {
        let ingress = Arc::clone(&self.runtime_filter_ingress);
        let response = tokio::task::spawn_blocking(move || {
            handle_runtime_filter_envelope(ingress, request.into_inner())
        })
        .await
        .map_err(|error| {
            tonic::Status::internal(format!(
                "transmit_runtime_filter_envelope handler panicked: {error}"
            ))
        })??;
        Ok(tonic::Response::new(response))
    }

    async fn fetch_result(
        &self,
        request: tonic::Request<proto::FetchResultRequest>,
    ) -> Result<tonic::Response<proto::FetchResultResponse>, tonic::Status> {
        let kernel = self.data_plane.clone();
        let response =
            tokio::task::spawn_blocking(move || kernel.fetch_result(request.into_inner()))
                .await
                .map_err(|error| {
                    tonic::Status::internal(format!("fetch_result handler panicked: {error}"))
                })?;
        Ok(tonic::Response::new(response))
    }

    async fn prune_catalogs(
        &self,
        request: tonic::Request<catalog::PruneCatalogsRequest>,
    ) -> Result<tonic::Response<catalog::PruneCatalogsResponse>, tonic::Status> {
        let request = PruneCatalogsRequest::parse(request.into_inner())
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        let reachable = request
            .reachable_catalogs()
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?
            .into_iter()
            .collect();
        let authority = Arc::clone(&self.catalog_reachability);
        let response = tokio::task::spawn_blocking(move || {
            match authority.prune_unreachable_catalogs(reachable) {
                CatalogPruneResult::Pruned { .. } => PruneCatalogsResponse::accepted(),
                // The rejected handles are deliberately not reported: naming
                // them would put catalog properties on the wire.
                CatalogPruneResult::Rejected { .. } => {
                    PruneCatalogsResponse::rejected(CATALOG_PRUNE_STALE_SNAPSHOT_DETAIL)
                        .expect("the fixed stale-snapshot detail is a bounded safe detail")
                }
            }
        })
        .await
        .map_err(|error| {
            tonic::Status::internal(format!("prune_catalogs handler panicked: {error}"))
        })?;
        Ok(tonic::Response::new(response.as_proto().clone()))
    }

    async fn heartbeat(
        &self,
        request: tonic::Request<proto::HeartbeatRequest>,
    ) -> Result<tonic::Response<proto::HeartbeatResponse>, tonic::Status> {
        self.heartbeat
            .respond(request.into_inner())
            .map(tonic::Response::new)
    }

    async fn apply_task_operations(
        &self,
        request: tonic::Request<proto::ApplyTaskOperationsRequest>,
    ) -> Result<tonic::Response<proto::ApplyTaskOperationsResponse>, tonic::Status> {
        let ingress = Arc::clone(&self.task_execution_ingress);
        let response = tokio::task::spawn_blocking(move || {
            ingress.apply_task_operations(request.into_inner())
        })
        .await
        .map_err(|error| {
            tonic::Status::internal(format!("apply_task_operations handler panicked: {error}"))
        })??;
        Ok(tonic::Response::new(response))
    }

    async fn subscribe_task_status(
        &self,
        request: tonic::Request<proto::SubscribeTaskStatusRequest>,
    ) -> Result<tonic::Response<Self::SubscribeTaskStatusStream>, tonic::Status> {
        // Opening a subscription only registers a cursor, so it does not need
        // the blocking pool the mutation path uses.
        let stream = self
            .task_execution_ingress
            .subscribe_task_status(request.into_inner())?;
        Ok(tonic::Response::new(stream))
    }

    async fn fetch_task_dynamic_filters(
        &self,
        request: tonic::Request<proto::FetchTaskDynamicFiltersRequest>,
    ) -> Result<tonic::Response<proto::FetchTaskDynamicFiltersResponse>, tonic::Status> {
        let ingress = Arc::clone(&self.task_execution_ingress);
        let response = tokio::task::spawn_blocking(move || {
            ingress.fetch_task_dynamic_filters(request.into_inner())
        })
        .await
        .map_err(|error| {
            tonic::Status::internal(format!(
                "fetch_task_dynamic_filters handler panicked: {error}"
            ))
        })??;
        Ok(tonic::Response::new(response))
    }

    async fn get_final_task_info(
        &self,
        request: tonic::Request<proto::GetFinalTaskInfoRequest>,
    ) -> Result<tonic::Response<proto::GetFinalTaskInfoResponse>, tonic::Status> {
        let ingress = Arc::clone(&self.task_execution_ingress);
        let response =
            tokio::task::spawn_blocking(move || ingress.get_final_task_info(request.into_inner()))
                .await
                .map_err(|error| {
                    tonic::Status::internal(format!(
                        "get_final_task_info handler panicked: {error}"
                    ))
                })??;
        Ok(tonic::Response::new(response))
    }

    async fn fetch_task_result(
        &self,
        request: tonic::Request<proto::FetchTaskResultRequest>,
    ) -> Result<tonic::Response<proto::FetchResultResponse>, tonic::Status> {
        let response = self
            .task_execution_ingress
            .fetch_task_result(request.into_inner())
            .await?;
        Ok(tonic::Response::new(response))
    }
}
