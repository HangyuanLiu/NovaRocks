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

use std::collections::HashMap;
use std::io::ErrorKind;
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime};

use novarocks::runtime::backend_id as backend_id_store;
use novarocks::thrift::{
    heartbeat_service::{
        HeartbeatServiceSyncHandler, HeartbeatServiceSyncProcessor, TBackendInfo, THeartbeatResult,
        TMasterInfo,
    },
    status::TStatus,
    status_code::TStatusCode,
};
use threadpool::ThreadPool;
use thrift::protocol::{
    TBinaryInputProtocolFactory, TBinaryOutputProtocolFactory, TInputProtocolFactory,
    TOutputProtocolFactory,
};
use thrift::server::TProcessor;
use thrift::transport::{
    TBufferedReadTransportFactory, TBufferedWriteTransportFactory, TIoChannel,
    TReadTransportFactory, TTcpChannel, TWriteTransportFactory,
};

use crate::control::FrontendControlState;
use crate::disk_report::DiskReportWorker;

/// Configuration for the StarRocks heartbeat service.
#[derive(Debug, Clone)]
pub(crate) struct HeartbeatConfig {
    pub(crate) host: String,
    pub(crate) advertise_host: String,
    pub(crate) heartbeat_port: u16,
    pub(crate) be_port: u16,
    pub(crate) brpc_port: u16,
    pub(crate) http_port: u16,
    pub(crate) starlet_port: u16,
    pub(crate) mem_limit_bytes: u64,
}

struct HeartbeatHandler {
    config: HeartbeatConfig,
    start_time: SystemTime,
    control: Arc<FrontendControlState>,
    disk_report_worker: Arc<DiskReportWorker>,
}

struct ActiveHeartbeatConnection {
    id: u64,
    connections: Arc<Mutex<HashMap<u64, TcpStream>>>,
}

impl Drop for ActiveHeartbeatConnection {
    fn drop(&mut self) {
        let mut connections = self
            .connections
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        connections.remove(&self.id);
    }
}

/// Host-owned heartbeat listener.  There is intentionally no process-global
/// listener state: a compat application can stop, join, and recreate this
/// service without inheriting an earlier host's control observations.
pub(crate) struct HeartbeatServer {
    stop: Arc<AtomicBool>,
    join_handle: Option<JoinHandle<()>>,
    wake_addr: String,
    active_connections: Arc<Mutex<HashMap<u64, TcpStream>>>,
}

fn shutdown_probe_host(bind_host: &str) -> String {
    match bind_host {
        "" | "0.0.0.0" => "127.0.0.1".to_string(),
        "::" | "[::]" => "::1".to_string(),
        other => other.to_string(),
    }
}

impl HeartbeatHandler {
    fn new(
        config: HeartbeatConfig,
        control: Arc<FrontendControlState>,
        disk_report_worker: Arc<DiskReportWorker>,
    ) -> Self {
        Self {
            config,
            start_time: SystemTime::now(),
            control,
            disk_report_worker,
        }
    }
}

impl HeartbeatServiceSyncHandler for HeartbeatHandler {
    fn handle_heartbeat(&self, master_info: TMasterInfo) -> thrift::Result<THeartbeatResult> {
        tracing::debug!(
            fe_host = %master_info.network_address.hostname,
            fe_port = master_info.network_address.port,
            epoch = master_info.epoch,
            backend_id = ?master_info.backend_id,
            backend_ip = ?master_info.backend_ip,
            http_port = ?master_info.http_port,
            run_mode = ?master_info.run_mode,
            node_type = ?master_info.node_type,
            heartbeat_flags = ?master_info.heartbeat_flags,
            min_active_txn_id = ?master_info.min_active_txn_id,
            encrypted = ?master_info.encrypted,
            "Received HeartbeatService.heartbeat"
        );
        if let Some(id) = master_info.backend_id {
            backend_id_store::set_backend_id(id);
        }
        let num_cores = thread::available_parallelism()
            .map(|n| n.get() as i32)
            .unwrap_or(1);
        let reboot_time = self
            .start_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let mem_limit_bytes = self.config.mem_limit_bytes.min(i64::MAX as u64);
        let backend_info = TBackendInfo::new(
            self.config.be_port as i32,
            self.config.http_port as i32,
            Some(self.config.brpc_port as i32),
            Some(self.config.brpc_port as i32),
            Some(novarocks::version::short_version().to_string()),
            Some(num_cores),
            Some(self.config.starlet_port as i32),
            Some(reboot_time),
            Some(true),
            Some(mem_limit_bytes as i64),
            None,
        );
        self.control.observe_heartbeat(
            &master_info.network_address,
            master_info.http_port,
            self.config.advertise_host.clone(),
        );
        self.disk_report_worker
            .request(self.config.be_port, self.config.http_port);

        let status = TStatus::new(TStatusCode::OK, None);

        tracing::debug!("Heartbeat response: reboot_time={}", reboot_time);

        Ok(THeartbeatResult::new(status, backend_info))
    }
}

impl HeartbeatServer {
    pub(crate) fn start(
        config: HeartbeatConfig,
        control: Arc<FrontendControlState>,
        disk_report_worker: Arc<DiskReportWorker>,
    ) -> Result<Self, String> {
        let host = if config.host.is_empty() {
            "0.0.0.0".to_string()
        } else {
            config.host.clone()
        };

        let addr = format!("{}:{}", host, config.heartbeat_port);
        let addr_for_log = addr.clone();

        tracing::info!(
            "Starting heartbeat service on {} (advertise_host={}, brpc_port={}, starlet_port={})",
            addr,
            config.advertise_host,
            config.brpc_port,
            config.starlet_port
        );

        let listener = TcpListener::bind(&addr)
            .map_err(|error| format!("HeartbeatService bind error on {addr}: {error}"))?;
        listener.set_nonblocking(true).map_err(|error| {
            format!("HeartbeatService set_nonblocking failed on {addr}: {error}")
        })?;

        let processor = Arc::new(HeartbeatServiceSyncProcessor::new(HeartbeatHandler::new(
            config,
            control,
            disk_report_worker,
        )));
        let stop = Arc::new(AtomicBool::new(false));
        let stop_for_thread = Arc::clone(&stop);
        let active_connections = Arc::new(Mutex::new(HashMap::new()));
        let active_connections_for_thread = Arc::clone(&active_connections);
        let next_connection_id = Arc::new(AtomicU64::new(1));
        let wake_addr = format!(
            "{}:{}",
            shutdown_probe_host(&host),
            listener
                .local_addr()
                .map_err(|error| format!("read HeartbeatService bound address failed: {error}"))?
                .port()
        );

        let join_handle = thread::Builder::new()
            .name("heartbeat-server".to_string())
            .spawn(move || {
                tracing::info!("Heartbeat service listening on {}", addr_for_log);
                let worker_pool = ThreadPool::with_name("HeartbeatService processor".to_owned(), 4);
                while !stop_for_thread.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            if stop_for_thread.load(Ordering::Acquire) {
                                break;
                            }
                            if let Err(error) = stream.set_nonblocking(false) {
                                tracing::warn!(
                                    "configure accepted heartbeat stream as blocking failed: {error}"
                                );
                                continue;
                            }
                            let tracked_stream = match stream.try_clone() {
                                Ok(stream) => stream,
                                Err(error) => {
                                    tracing::warn!(
                                        "clone accepted heartbeat stream for shutdown failed: {error}"
                                    );
                                    continue;
                                }
                            };
                            let connection_id = next_connection_id.fetch_add(1, Ordering::Relaxed);
                            let mut connections = active_connections_for_thread
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            if stop_for_thread.load(Ordering::Acquire) {
                                break;
                            }
                            connections.insert(connection_id, tracked_stream);
                            drop(connections);
                            let processor = Arc::clone(&processor);
                            let active_connections = Arc::clone(&active_connections_for_thread);
                            worker_pool.execute(move || {
                                let _active = ActiveHeartbeatConnection {
                                    id: connection_id,
                                    connections: active_connections,
                                };
                                let channel = TTcpChannel::with_stream(stream);
                                let (read_channel, write_channel) = match channel.split() {
                                    Ok(channels) => channels,
                                    Err(error) => {
                                        tracing::warn!("split heartbeat channel failed: {error}");
                                        return;
                                    }
                                };
                                let read_transport = TBufferedReadTransportFactory::new()
                                    .create(Box::new(read_channel));
                                let mut input = TBinaryInputProtocolFactory::new().create(read_transport);
                                let write_transport = TBufferedWriteTransportFactory::new()
                                    .create(Box::new(write_channel));
                                let mut output = TBinaryOutputProtocolFactory::new().create(write_transport);
                                loop {
                                    match processor.process(&mut *input, &mut *output) {
                                        Ok(()) => {}
                                        Err(thrift::Error::Transport(ref error))
                                            if error.kind == thrift::TransportErrorKind::EndOfFile =>
                                        {
                                            break;
                                        }
                                        Err(error) => {
                                            tracing::warn!("heartbeat processor error: {error:?}");
                                            break;
                                        }
                                    }
                                }
                            });
                        }
                        Err(error) if error.kind() == ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(25));
                        }
                        Err(error) => {
                            if !stop_for_thread.load(Ordering::Acquire) {
                                tracing::error!("Heartbeat server accept error: {error}");
                            }
                        }
                    }
                }
                worker_pool.join();
            })
            .map_err(|error| format!("Failed to spawn heartbeat thread: {error}"))?;

        Ok(Self {
            stop,
            join_handle: Some(join_handle),
            wake_addr,
            active_connections,
        })
    }

    pub(crate) fn poll_failure(&mut self) -> Result<Option<String>, String> {
        let Some(handle) = self.join_handle.as_ref() else {
            return Ok(None);
        };
        if !handle.is_finished() || self.stop.load(Ordering::Acquire) {
            return Ok(None);
        }
        let handle = self
            .join_handle
            .take()
            .expect("heartbeat join handle checked as present");
        join_heartbeat_server_thread(handle)
            .map(|()| Some("HeartbeatService listener stopped unexpectedly".to_string()))
    }

    pub(crate) fn stop(&mut self) -> Result<(), String> {
        self.stop.store(true, Ordering::Release);
        let mut failures = Vec::new();
        let connections = match self.active_connections.lock() {
            Ok(connections) => connections,
            Err(poisoned) => {
                failures.push("lock heartbeat active connections failed".to_string());
                poisoned.into_inner()
            }
        };
        for stream in connections.values() {
            let _ = stream.shutdown(Shutdown::Both);
        }
        drop(connections);
        let _ = TcpStream::connect(&self.wake_addr);
        if let Some(join_handle) = self.join_handle.take() {
            if let Err(error) = join_heartbeat_server_thread(join_handle) {
                failures.push(error);
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
    }
}

impl Drop for HeartbeatServer {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

fn join_heartbeat_server_thread(handle: JoinHandle<()>) -> Result<(), String> {
    handle.join().map_err(|payload| {
        let detail = payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| {
                payload
                    .downcast_ref::<&str>()
                    .map(|value| (*value).to_string())
            })
            .unwrap_or_else(|| "unknown panic payload".to_string());
        format!("HeartbeatService listener thread panicked: {detail}")
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use novarocks::thrift::{master_service, types};

    use super::*;
    use crate::disk_report::DiskReportSender;

    struct NoopDiskReportSender;

    impl DiskReportSender for NoopDiskReportSender {
        fn send_disk_report(
            &self,
            _: &types::TNetworkAddress,
            _: &master_service::TReportRequest,
        ) -> Result<(), String> {
            Ok(())
        }
    }

    fn worker() -> Arc<DiskReportWorker> {
        Arc::new(DiskReportWorker::new(
            Arc::new(FrontendControlState::new()),
            Arc::new(NoopDiskReportSender),
        ))
    }

    #[test]
    fn stop_heartbeat_server_is_idempotent() {
        let stop = Arc::new(AtomicBool::new(false));
        let mut server = HeartbeatServer {
            stop,
            join_handle: Some(std::thread::spawn(|| {})),
            wake_addr: "127.0.0.1:1".to_string(),
            active_connections: Arc::new(Mutex::new(HashMap::new())),
        };

        server.stop().expect("first stop");
        server.stop().expect("second stop");
    }

    #[test]
    fn stop_heartbeat_server_reports_stored_listener_panic() {
        let mut server = HeartbeatServer {
            stop: Arc::new(AtomicBool::new(false)),
            join_handle: Some(std::thread::spawn(|| panic!("heartbeat listener failed"))),
            wake_addr: "127.0.0.1:1".to_string(),
            active_connections: Arc::new(Mutex::new(HashMap::new())),
        };

        let error = server.stop().expect_err("panic must be reported");

        assert_eq!(
            error,
            "HeartbeatService listener thread panicked: heartbeat listener failed"
        );
    }

    #[test]
    fn stop_heartbeat_server_reports_active_connection_lock_poison() {
        let active_connections = Arc::new(Mutex::new(HashMap::new()));
        let poison_target = Arc::clone(&active_connections);
        let _ = std::thread::spawn(move || {
            let _guard = poison_target
                .lock()
                .expect("lock active connections before poison");
            panic!("poison active connections");
        })
        .join();
        let mut server = HeartbeatServer {
            stop: Arc::new(AtomicBool::new(false)),
            join_handle: Some(std::thread::spawn(|| {})),
            wake_addr: "127.0.0.1:1".to_string(),
            active_connections,
        };

        let error = server
            .stop()
            .expect_err("connection lock poison must be reported");

        assert_eq!(error, "lock heartbeat active connections failed");
    }

    #[test]
    fn heartbeat_server_stop_releases_bound_port() {
        let port = TcpListener::bind("127.0.0.1:0")
            .expect("reserve test port")
            .local_addr()
            .expect("read reserved port")
            .port();
        let config = HeartbeatConfig {
            host: "127.0.0.1".to_string(),
            advertise_host: "be".to_string(),
            heartbeat_port: port,
            be_port: 9060,
            brpc_port: 8060,
            http_port: 8040,
            starlet_port: 9070,
            mem_limit_bytes: 1,
        };
        let control = Arc::new(FrontendControlState::new());
        let mut server = HeartbeatServer::start(config, control, worker()).expect("start server");
        server.stop().expect("stop server");
        TcpListener::bind(("127.0.0.1", port)).expect("port is immediately reusable");
    }

    #[test]
    fn heartbeat_handler_updates_host_control_and_reports_once() {
        struct CountingSender(AtomicUsize);
        impl DiskReportSender for CountingSender {
            fn send_disk_report(
                &self,
                _: &types::TNetworkAddress,
                _: &master_service::TReportRequest,
            ) -> Result<(), String> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }

        let control = Arc::new(FrontendControlState::new());
        let sender = Arc::new(CountingSender(AtomicUsize::new(0)));
        let worker = Arc::new(DiskReportWorker::new(Arc::clone(&control), sender.clone()));
        let handler = HeartbeatHandler::new(
            HeartbeatConfig {
                host: "127.0.0.1".to_string(),
                advertise_host: "be".to_string(),
                heartbeat_port: 9050,
                be_port: 9060,
                brpc_port: 8060,
                http_port: 8040,
                starlet_port: 9070,
                mem_limit_bytes: 1,
            },
            Arc::clone(&control),
            Arc::clone(&worker),
        );
        let master = TMasterInfo::new(
            types::TNetworkAddress::new("fe".to_string(), 9020),
            None,
            1,
            None,
            None,
            Some(8030),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );

        handler
            .handle_heartbeat(master.clone())
            .expect("first heartbeat");
        handler.handle_heartbeat(master).expect("second heartbeat");
        worker.shutdown().expect("join report worker");

        assert_eq!(control.latest_fe_addr().expect("FE address").hostname, "fe");
        assert_eq!(control.latest_fe_http_port(), Some(8030));
        assert_eq!(sender.0.load(Ordering::SeqCst), 1);
    }
}
