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

//! Compat-owned HTTP, internal gRPC, and Starlet listener group.
//!
//! The generated service handlers deliberately stay small at this boundary:
//! `GrpcService` is the core neutral RPC handler while this module owns every
//! compat listener, route composition, readiness, supervision, and shutdown.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr, TcpListener};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::JoinHandle;

use crate::proto::staros;
use axum::Router;
use axum::extract::Query;
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::get;
use novarocks::proto::novarocks as novarocks_proto;
use novarocks::query_execution::report::NativeReportHandler;
use novarocks::runtime::starlet_shard_registry::{self, S3StoreConfig, StarletShardInfo};
use novarocks::service::grpc_server::GrpcService;
use novarocks::service::{render_metrics, render_metrics_json};
use prost::Message;
use tokio::net::TcpListener as TokioTcpListener;
use tokio::sync::watch;
use tonic::service::Routes;

const GRPC_MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

/// Immutable bind configuration for the three compat-owned listener ports.
#[derive(Clone, Debug)]
pub(crate) struct CompatListenerConfig {
    pub(crate) host: String,
    pub(crate) http_port: u16,
    pub(crate) grpc_port: u16,
    pub(crate) starlet_port: u16,
}

/// Narrow Starlet control interface consumed by the wire adapter.
///
/// The implementation is application-owned.  No listener or provider state
/// is stored globally in this module.
pub(crate) trait StarletControl: Send + Sync {
    fn parse_file_path_s3_profile(
        &self,
        encoded_file_path: &[u8],
    ) -> Result<Option<S3StoreConfig>, String>;

    fn observe_service(&self, service_id: &str);

    fn observe_heartbeat(
        &self,
        leader_addr: &str,
        service_id: &str,
        worker_group_id: u64,
        worker_id: u64,
    );
}

#[derive(Clone)]
struct StarletGrpcService {
    control: Arc<dyn StarletControl>,
}

fn staros_ok_status() -> staros::StarStatus {
    staros::StarStatus {
        status_code: staros::StatusCode::Ok as i32,
        error_msg: String::new(),
        extra_info: Vec::new(),
    }
}

fn parse_add_shard_s3_config(
    control: &dyn StarletControl,
    path_info: &staros::FilePathInfo,
) -> Result<Option<S3StoreConfig>, String> {
    control.parse_file_path_s3_profile(&path_info.encode_to_vec())
}

fn summarize_top_counts(counts: &HashMap<String, usize>, top_n: usize) -> String {
    if counts.is_empty() {
        return "-".to_string();
    }
    let mut entries = counts
        .iter()
        .map(|(key, count)| (key.clone(), *count))
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    entries
        .into_iter()
        .take(top_n.max(1))
        .map(|(key, count)| format!("{key}:{count}"))
        .collect::<Vec<_>>()
        .join(",")
}

#[tonic::async_trait]
impl staros::starlet_server::Starlet for StarletGrpcService {
    async fn add_shard(
        &self,
        request: tonic::Request<staros::AddShardRequest>,
    ) -> Result<tonic::Response<staros::AddShardResponse>, tonic::Status> {
        let request = request.into_inner();
        self.control.observe_service(&request.service_id);
        let worker_id = request.worker_id;
        let shard_count = request.shard_info.len();
        let shard_infos = request.shard_info;
        let control = Arc::clone(&self.control);

        // AddShard may carry a large batch.  The acknowledgement intentionally
        // precedes registry mutation so Starlet heartbeats remain responsive.
        tokio::task::spawn_blocking(move || {
            let mut updates = Vec::with_capacity(shard_infos.len());
            let mut invalid_shard_id = 0usize;
            let mut missing_full_path = 0usize;
            let mut invalid_s3_config = 0usize;
            let mut s3_config_count = 0usize;
            let mut s3_endpoint_counts = HashMap::new();
            let mut s3_bucket_counts = HashMap::new();
            for shard in &shard_infos {
                let Ok(shard_id) = i64::try_from(shard.shard_id) else {
                    invalid_shard_id += 1;
                    continue;
                };
                let Some(path_info) = shard.file_path_info.as_ref() else {
                    missing_full_path += 1;
                    continue;
                };
                if path_info.full_path.trim().is_empty() {
                    missing_full_path += 1;
                    continue;
                }
                let s3 = match parse_add_shard_s3_config(control.as_ref(), path_info) {
                    Ok(value) => value,
                    Err(error) => {
                        invalid_s3_config += 1;
                        tracing::warn!(
                            target: "novarocks::grpc",
                            shard_id,
                            error = %error,
                            "skip invalid AddShard S3 fs_info; only full_path is cached"
                        );
                        None
                    }
                };
                if let Some(config) = s3.as_ref() {
                    s3_config_count = s3_config_count.saturating_add(1);
                    *s3_endpoint_counts
                        .entry(config.endpoint().to_string())
                        .or_insert(0) += 1;
                    *s3_bucket_counts
                        .entry(config.bucket().to_string())
                        .or_insert(0) += 1;
                }
                updates.push((
                    shard_id,
                    StarletShardInfo::new(path_info.full_path.clone(), s3),
                ));
            }
            let upserted = starlet_shard_registry::upsert_many_infos(updates);
            tracing::info!(
                target: "novarocks::grpc",
                worker_id,
                shard_count,
                upserted,
                invalid_shard_id,
                missing_full_path,
                invalid_s3_config,
                s3_config_count,
                s3_endpoint_summary = %summarize_top_counts(&s3_endpoint_counts, 3),
                s3_bucket_summary = %summarize_top_counts(&s3_bucket_counts, 3),
                "processed starlet AddShard"
            );
        });

        tracing::info!(
            target: "novarocks::grpc",
            worker_id,
            shard_count,
            "accepted starlet AddShard"
        );
        Ok(tonic::Response::new(staros::AddShardResponse {
            status: Some(staros_ok_status()),
        }))
    }

    async fn remove_shard(
        &self,
        request: tonic::Request<staros::RemoveShardRequest>,
    ) -> Result<tonic::Response<staros::RemoveShardResponse>, tonic::Status> {
        let request = request.into_inner();
        self.control.observe_service(&request.service_id);
        let tablet_ids = request
            .shard_ids
            .iter()
            .filter_map(|id| i64::try_from(*id).ok())
            .collect::<Vec<_>>();
        let removed = starlet_shard_registry::remove_many(tablet_ids);
        tracing::info!(
            target: "novarocks::grpc",
            worker_id = request.worker_id,
            service_id = request.service_id,
            shard_count = request.shard_ids.len(),
            removed,
            "received starlet RemoveShard"
        );
        Ok(tonic::Response::new(staros::RemoveShardResponse {
            status: Some(staros_ok_status()),
        }))
    }

    async fn starlet_heartbeat(
        &self,
        request: tonic::Request<staros::StarletHeartbeatRequest>,
    ) -> Result<tonic::Response<staros::StarletHeartbeatResponse>, tonic::Status> {
        let request = request.into_inner();
        self.control.observe_heartbeat(
            &request.star_mgr_leader,
            &request.service_id,
            request.worker_group_id,
            request.worker_id,
        );
        tracing::info!(
            target: "novarocks::grpc",
            worker_id = request.worker_id,
            worker_group_id = request.worker_group_id,
            service_id = request.service_id,
            star_mgr_leader = request.star_mgr_leader,
            "received starlet StarletHeartbeat"
        );
        Ok(tonic::Response::new(staros::StarletHeartbeatResponse {
            status: Some(staros_ok_status()),
        }))
    }

    async fn write_cache(
        &self,
        request: tonic::Request<staros::WriteCacheRequest>,
    ) -> Result<tonic::Response<staros::WriteCacheResponse>, tonic::Status> {
        let request = request.into_inner();
        tracing::info!(
            target: "novarocks::grpc",
            shard_id = request.shard_id,
            payload_bytes = request.data.len(),
            "received starlet WriteCache"
        );
        Ok(tonic::Response::new(staros::WriteCacheResponse {
            status: Some(staros_ok_status()),
        }))
    }
}

#[derive(Debug)]
struct ListenerState {
    stop_requested: Arc<AtomicBool>,
    shutdown_tx: watch::Sender<bool>,
    join_handle: Option<JoinHandle<()>>,
    failure_rx: mpsc::Receiver<String>,
}

/// Host-owned lifecycle handle for all compat HTTP/gRPC listeners.
///
/// The group reserves every port before a serving thread is started.  A bind
/// failure therefore drops every prior reservation, and shutdown joins the
/// single owner thread before the ports can be reused by a later host.
pub(crate) struct CompatListenerGroup {
    state: Mutex<Option<ListenerState>>,
    http_addr: SocketAddr,
    grpc_addr: SocketAddr,
    starlet_addr: SocketAddr,
}

impl CompatListenerGroup {
    pub(crate) fn start(
        config: CompatListenerConfig,
        compat_routes: Router,
        report_handler: Arc<dyn NativeReportHandler>,
        starlet_control: Arc<dyn StarletControl>,
    ) -> Result<Self, String> {
        validate_ports(config.http_port, config.grpc_port, config.starlet_port)?;
        let http_listener = bind_listener(&config.host, config.http_port, "novarocks http")?;
        let grpc_listener = bind_listener(&config.host, config.grpc_port, "novarocks grpc")?;
        let starlet_listener = bind_listener(&config.host, config.starlet_port, "starlet grpc")?;
        let http_addr = http_listener
            .local_addr()
            .map_err(|error| format!("read novarocks http bound address failed: {error}"))?;
        let grpc_addr = grpc_listener
            .local_addr()
            .map_err(|error| format!("read novarocks grpc bound address failed: {error}"))?;
        let starlet_addr = starlet_listener
            .local_addr()
            .map_err(|error| format!("read starlet grpc bound address failed: {error}"))?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(8)
            .thread_stack_size(novarocks::runtime::global_async_runtime::WORKER_STACK_SIZE_BYTES)
            .build()
            .map_err(|error| format!("build compat listener runtime failed: {error}"))?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (ready_tx, ready_rx) = mpsc::sync_channel::<Result<(), String>>(1);
        let (failure_tx, failure_rx) = mpsc::channel();
        let stop_requested = Arc::new(AtomicBool::new(false));
        let stop_requested_for_thread = Arc::clone(&stop_requested);
        let host = config.host;

        let join_handle = std::thread::Builder::new()
            .name("compat-listener-group".to_string())
            .spawn(move || {
                runtime.block_on(async move {
                    let http_listener = match TokioTcpListener::from_std(http_listener) {
                        Ok(listener) => listener,
                        Err(error) => {
                            let _ = ready_tx.send(Err(format!(
                                "create novarocks http tokio listener failed: {error}"
                            )));
                            return;
                        }
                    };
                    let grpc_listener = match TokioTcpListener::from_std(grpc_listener) {
                        Ok(listener) => listener,
                        Err(error) => {
                            let _ = ready_tx.send(Err(format!(
                                "create novarocks grpc tokio listener failed: {error}"
                            )));
                            return;
                        }
                    };
                    let starlet_listener = match TokioTcpListener::from_std(starlet_listener) {
                        Ok(listener) => listener,
                        Err(error) => {
                            let _ = ready_tx.send(Err(format!(
                                "create starlet grpc tokio listener failed: {error}"
                            )));
                            return;
                        }
                    };
                    tracing::info!(
                        target: "novarocks::grpc",
                        host = %host,
                        http_port = http_addr.port(),
                        grpc_port = grpc_addr.port(),
                        starlet_port = starlet_addr.port(),
                        "starting compat http and grpc servers"
                    );

                    let http_service =
                        novarocks_proto::nova_rocks_grpc_server::NovaRocksGrpcServer::new(
                            GrpcService::internal_execution_without_native_fragment_ingress(
                                Arc::clone(&report_handler),
                            ),
                        )
                        .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
                        .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES);
                    let http_app = Routes::new(http_service)
                        .into_axum_router()
                        .merge(compat_routes)
                        .route("/metrics", get(handle_metrics));
                    let grpc_service =
                        novarocks_proto::nova_rocks_grpc_server::NovaRocksGrpcServer::new(
                            GrpcService::internal_execution_without_native_fragment_ingress(
                                report_handler,
                            ),
                        )
                        .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
                        .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES);
                    let grpc_app = Routes::new(grpc_service).into_axum_router();
                    let starlet_service =
                        staros::starlet_server::StarletServer::new(StarletGrpcService {
                            control: starlet_control,
                        })
                        .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
                        .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES);
                    let starlet_app = Routes::new(starlet_service).into_axum_router();

                    if ready_tx.send(Ok(())).is_err() {
                        return;
                    }
                    supervise_listeners(
                        serve_router(http_listener, http_app, shutdown_rx.clone()),
                        serve_router(grpc_listener, grpc_app, shutdown_rx.clone()),
                        serve_router(starlet_listener, starlet_app, shutdown_rx),
                        stop_requested_for_thread,
                        failure_tx,
                    )
                    .await;
                });
            })
            .map_err(|error| format!("spawn compat listener thread failed: {error}"))?;

        match ready_rx.recv() {
            Ok(Ok(())) => Ok(Self {
                state: Mutex::new(Some(ListenerState {
                    stop_requested,
                    shutdown_tx,
                    join_handle: Some(join_handle),
                    failure_rx,
                })),
                http_addr,
                grpc_addr,
                starlet_addr,
            }),
            Ok(Err(error)) => {
                let _ = join_handle.join();
                Err(error)
            }
            Err(error) => {
                let _ = join_handle.join();
                Err(format!("compat listener readiness channel closed: {error}"))
            }
        }
    }

    pub(crate) const fn http_addr(&self) -> SocketAddr {
        self.http_addr
    }

    pub(crate) const fn grpc_addr(&self) -> SocketAddr {
        self.grpc_addr
    }

    pub(crate) const fn starlet_addr(&self) -> SocketAddr {
        self.starlet_addr
    }

    pub(crate) fn poll_failure(&self) -> Result<Option<String>, String> {
        let state = self
            .state
            .lock()
            .map_err(|_| "lock compat listener state failed".to_string())?;
        let Some(state) = state.as_ref() else {
            return Ok(None);
        };
        match state.failure_rx.try_recv() {
            Ok(failure) => Ok(Some(failure)),
            Err(mpsc::TryRecvError::Empty) => Ok(None),
            Err(mpsc::TryRecvError::Disconnected)
                if state.stop_requested.load(Ordering::Acquire) =>
            {
                Ok(None)
            }
            Err(mpsc::TryRecvError::Disconnected) => Ok(Some(
                "compat listener supervisor exited unexpectedly after readiness".to_string(),
            )),
        }
    }

    pub(crate) fn stop(&self) -> Result<(), String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "lock compat listener state failed".to_string())?;
        let Some(mut state) = state.take() else {
            return Ok(());
        };
        state.stop_requested.store(true, Ordering::Release);
        let _ = state.shutdown_tx.send(true);
        let mut failures = state.failure_rx.try_iter().collect::<Vec<_>>();
        if let Some(join_handle) = state.join_handle.take()
            && let Err(payload) = join_handle.join()
        {
            failures.push(format!(
                "compat listener thread panicked: {}",
                panic_payload_message(payload)
            ));
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
    }
}

impl Drop for CompatListenerGroup {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

async fn handle_metrics(Query(params): Query<HashMap<String, String>>) -> impl IntoResponse {
    if params
        .get("type")
        .is_some_and(|value| value.eq_ignore_ascii_case("json"))
    {
        return match render_metrics_json() {
            Ok(body) => ([(header::CONTENT_TYPE, "application/json")], body).into_response(),
            Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error).into_response(),
        };
    }
    match render_metrics() {
        Ok(body) => ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body).into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error).into_response(),
    }
}

async fn serve_router(
    listener: TokioTcpListener,
    router: Router,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), String> {
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            while !*shutdown.borrow() {
                if shutdown.changed().await.is_err() {
                    break;
                }
            }
        })
        .await
        .map_err(|error| error.to_string())
}

async fn supervise_listeners<H, G, S>(
    http_server: H,
    grpc_server: G,
    starlet_server: S,
    stop_requested: Arc<AtomicBool>,
    failure_tx: mpsc::Sender<String>,
) where
    H: std::future::Future<Output = Result<(), String>>,
    G: std::future::Future<Output = Result<(), String>>,
    S: std::future::Future<Output = Result<(), String>>,
{
    let (service, result) = tokio::select! {
        result = http_server => ("http", result),
        result = grpc_server => ("grpc", result),
        result = starlet_server => ("starlet", result),
    };
    if stop_requested.load(Ordering::Acquire) {
        return;
    }
    let detail = match result {
        Ok(()) => "serve future ended unexpectedly after readiness".to_string(),
        Err(error) => format!("serve future failed after readiness: {error}"),
    };
    let _ = failure_tx.send(format!("compat listener {service} {detail}"));
}

fn validate_ports(http_port: u16, grpc_port: u16, starlet_port: u16) -> Result<(), String> {
    if http_port == starlet_port {
        return Err(format!(
            "invalid config: server.http_port ({http_port}) and server.starlet_port ({starlet_port}) must be different"
        ));
    }
    if grpc_port == http_port || grpc_port == starlet_port {
        return Err(format!(
            "invalid config: server.grpc_port ({grpc_port}) must differ from server.http_port ({http_port}) and server.starlet_port ({starlet_port})"
        ));
    }
    Ok(())
}

fn bind_listener(host: &str, port: u16, role: &str) -> Result<TcpListener, String> {
    let address = parse_bind_addr(host, port)
        .map_err(|error| format!("parse {role} bind addr failed: {error}"))?;
    let listener = TcpListener::bind(address)
        .map_err(|error| format!("failed to bind {role} listener on {address}: {error}"))?;
    listener
        .set_nonblocking(true)
        .map_err(|error| format!("failed to configure {role} listener on {address}: {error}"))?;
    Ok(listener)
}

fn parse_bind_addr(host: &str, port: u16) -> Result<SocketAddr, String> {
    let bare = host
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(host);
    if let Ok(ip) = bare.parse::<IpAddr>() {
        return Ok(SocketAddr::new(ip, port));
    }
    let formatted = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    formatted
        .parse::<SocketAddr>()
        .map_err(|error| format!("parse gRPC bind addr '{formatted}' failed: {error}"))
}

fn panic_payload_message(payload: Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| {
            payload
                .downcast_ref::<&str>()
                .map(|value| (*value).to_string())
        })
        .unwrap_or_else(|| "unknown panic payload".to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::Router;

    use super::{CompatListenerConfig, CompatListenerGroup, StarletControl, parse_bind_addr};
    use novarocks::query_execution::report::{NativeReportHandler, NativeReportHandlerError};
    use novarocks::runtime::starlet_shard_registry::S3StoreConfig;

    struct RejectingReportHandler;

    impl NativeReportHandler for RejectingReportHandler {
        fn handle_native_report(
            &self,
            _report: novarocks::proto::novarocks::ExecStatusReport,
        ) -> Result<(), NativeReportHandlerError> {
            Err(NativeReportHandlerError::role_rejected(
                "test report handler",
            ))
        }
    }

    struct TestStarletControl;

    impl StarletControl for TestStarletControl {
        fn parse_file_path_s3_profile(
            &self,
            _encoded_file_path: &[u8],
        ) -> Result<Option<S3StoreConfig>, String> {
            Ok(None)
        }

        fn observe_service(&self, _service_id: &str) {}

        fn observe_heartbeat(
            &self,
            _leader_addr: &str,
            _service_id: &str,
            _worker_group_id: u64,
            _worker_id: u64,
        ) {
        }
    }

    #[test]
    fn listener_group_stop_releases_all_ports_for_immediate_rebind() {
        let free_port = || {
            let listener = std::net::TcpListener::bind("127.0.0.1:0")
                .expect("reserve ephemeral listener port");
            listener
                .local_addr()
                .expect("read ephemeral listener port")
                .port()
        };
        let http_port = free_port();
        let mut grpc_port = free_port();
        while grpc_port == http_port {
            grpc_port = free_port();
        }
        let mut starlet_port = free_port();
        while starlet_port == http_port || starlet_port == grpc_port {
            starlet_port = free_port();
        }
        let group = CompatListenerGroup::start(
            CompatListenerConfig {
                host: "127.0.0.1".to_string(),
                http_port,
                grpc_port,
                starlet_port,
            },
            Router::new(),
            Arc::new(RejectingReportHandler),
            Arc::new(TestStarletControl),
        )
        .expect("start listener group");
        let addresses = [group.http_addr(), group.grpc_addr(), group.starlet_addr()];
        group.stop().expect("stop listener group");
        for address in addresses {
            let listener = std::net::TcpListener::bind(address)
                .unwrap_or_else(|error| panic!("rebind {address} after stop failed: {error}"));
            drop(listener);
        }
    }

    #[test]
    fn rejects_duplicate_listener_ports_before_binding() {
        let error = match CompatListenerGroup::start(
            CompatListenerConfig {
                host: "127.0.0.1".to_string(),
                http_port: 12345,
                grpc_port: 12345,
                starlet_port: 12346,
            },
            Router::new(),
            Arc::new(RejectingReportHandler),
            Arc::new(TestStarletControl),
        ) {
            Ok(_) => panic!("duplicate ports must fail"),
            Err(error) => error,
        };
        assert!(
            error.contains("server.grpc_port (12345) must differ"),
            "{error}"
        );
    }

    #[test]
    fn parses_bare_ipv6_bind_address() {
        let address = parse_bind_addr("::1", 9070).expect("parse IPv6 address");
        assert_eq!(address.port(), 9070);
        assert!(address.ip().is_ipv6());
    }
}
