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
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime};

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

use crate::runtime::backend_id as backend_id_store;
use crate::service::disk_report;
use crate::thrift::{
    heartbeat_service::{
        HeartbeatServiceSyncHandler, HeartbeatServiceSyncProcessor, TBackendInfo, THeartbeatResult,
        TMasterInfo,
    },
    status::TStatus,
    status_code::TStatusCode,
};

/// Configuration for the heartbeat service
#[derive(Debug, Clone)]
pub struct HeartbeatConfig {
    pub host: String,
    pub advertise_host: String,
    pub heartbeat_port: u16,
    pub be_port: u16,
    pub brpc_port: u16,
    pub http_port: u16,
    pub starlet_port: u16,
    pub mem_limit_bytes: u64,
}

struct HeartbeatHandler {
    config: HeartbeatConfig,
    start_time: SystemTime,
}

#[derive(Default)]
struct HeartbeatServerState {
    started: bool,
    stop: Option<Arc<AtomicBool>>,
    join_handle: Option<JoinHandle<()>>,
    wake_addr: Option<String>,
    active_connections: Option<Arc<Mutex<HashMap<u64, TcpStream>>>>,
}

#[cfg(test)]
static HEARTBEAT_ACTIVE_WORKERS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

#[cfg(test)]
fn heartbeat_active_worker_count() -> usize {
    HEARTBEAT_ACTIVE_WORKERS.load(Ordering::SeqCst)
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
        #[cfg(test)]
        HEARTBEAT_ACTIVE_WORKERS.fetch_sub(1, Ordering::SeqCst);
    }
}

fn heartbeat_server_state() -> &'static Mutex<HeartbeatServerState> {
    static STATE: OnceLock<Mutex<HeartbeatServerState>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(HeartbeatServerState::default()))
}

fn shutdown_probe_host(bind_host: &str) -> String {
    match bind_host {
        "" | "0.0.0.0" => "127.0.0.1".to_string(),
        "::" | "[::]" => "::1".to_string(),
        other => other.to_string(),
    }
}

fn backend_host_for_fe(advertise_host: &str) -> String {
    advertise_host.to_string()
}

impl HeartbeatHandler {
    fn new(config: HeartbeatConfig) -> Self {
        Self {
            config,
            start_time: SystemTime::now(),
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
            Some(crate::version::short_version().to_string()),
            Some(num_cores),
            Some(self.config.starlet_port as i32),
            Some(reboot_time),
            Some(true),
            Some(mem_limit_bytes as i64),
            None,
        );
        disk_report::maybe_report_disks(
            &master_info.network_address,
            backend_host_for_fe(&self.config.advertise_host),
            self.config.be_port,
            self.config.http_port,
            master_info.http_port,
        );

        let status = TStatus::new(TStatusCode::OK, None);

        tracing::debug!("Heartbeat response: reboot_time={}", reboot_time);

        Ok(THeartbeatResult::new(status, backend_info))
    }
}

pub fn start_heartbeat_server(config: HeartbeatConfig) -> Result<(), String> {
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

    let mut state = heartbeat_server_state()
        .lock()
        .map_err(|_| "lock heartbeat service state failed".to_string())?;
    if state.started {
        return Ok(());
    }
    let listener = TcpListener::bind(&addr)
        .map_err(|error| format!("HeartbeatService bind error on {addr}: {error}"))?;
    listener
        .set_nonblocking(true)
        .map_err(|error| format!("HeartbeatService set_nonblocking failed on {addr}: {error}"))?;

    let processor = Arc::new(HeartbeatServiceSyncProcessor::new(HeartbeatHandler::new(
        config,
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
                        #[cfg(test)]
                        HEARTBEAT_ACTIVE_WORKERS.fetch_add(1, Ordering::SeqCst);
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
                            let read_transport =
                                TBufferedReadTransportFactory::new().create(Box::new(read_channel));
                            let mut input =
                                TBinaryInputProtocolFactory::new().create(read_transport);
                            let write_transport = TBufferedWriteTransportFactory::new()
                                .create(Box::new(write_channel));
                            let mut output =
                                TBinaryOutputProtocolFactory::new().create(write_transport);
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
        .map_err(|e| format!("Failed to spawn heartbeat thread: {}", e))?;

    state.started = true;
    state.stop = Some(stop);
    state.join_handle = Some(join_handle);
    state.wake_addr = Some(wake_addr);
    state.active_connections = Some(active_connections);

    Ok(())
}

pub fn stop_heartbeat_server() {
    let (stop, wake_addr, join_handle, active_connections) = {
        let mut state = match heartbeat_server_state().lock() {
            Ok(state) => state,
            Err(_) => return,
        };
        if !state.started {
            return;
        }
        state.started = false;
        (
            state.stop.take(),
            state.wake_addr.take(),
            state.join_handle.take(),
            state.active_connections.take(),
        )
    };
    if let Some(stop) = stop {
        stop.store(true, Ordering::Release);
    }
    if let Some(active_connections) = active_connections {
        let connections = active_connections
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for stream in connections.values() {
            let _ = stream.shutdown(Shutdown::Both);
        }
    }
    if let Some(wake_addr) = wake_addr {
        let _ = TcpStream::connect(wake_addr);
    }
    if let Some(join_handle) = join_handle {
        let _ = join_handle.join();
    }
}
