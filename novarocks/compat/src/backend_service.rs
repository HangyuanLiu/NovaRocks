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

//! Compat-owned StarRocks `BackendService` Thrift listener.

use std::collections::BTreeMap;
use std::io::ErrorKind;
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

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

use crate::thrift::master_service;
use crate::thrift::{
    agent_service,
    backend_service::{
        BackendServiceSyncHandler, BackendServiceSyncProcessor, TExportTaskRequest,
        TGetTabletsInfoRequest, TGetTabletsInfoResult, TRoutineLoadTask, TStreamLoadChannel,
        TTabletStatResult,
    },
    internal_service, starrocks_external_service,
    status::TStatus,
    status_code::TStatusCode,
    types,
};
use novarocks::common::network;
use novarocks::connector::starrocks::sink::clear_auto_increment_cache_for_table;
use novarocks::novarocks_config::config as novarocks_app_config;
use novarocks::runtime::starlet_shard_registry;

use crate::control::FrontendControlState;
use crate::frontend_rpc;
use crate::lake_agent_tasks::CompatLakeAgentTaskAdapter;
use crate::load::CompatLoadService;
use crate::thrift_debug::thrift_named_json;

#[derive(Debug, Clone)]
pub(crate) struct BackendServiceConfig {
    pub(crate) host: String,
    pub(crate) be_port: u16,
}

struct BackendState {
    stop: AtomicBool,
    join_handle: Mutex<Option<JoinHandle<()>>>,
    wake_addr: String,
    connections: Mutex<Vec<TcpStream>>,
    submit_task_pool: Arc<ThreadPool>,
}

/// A host-owned listener handle.  `stop` is idempotent and closes active
/// Thrift connections before joining the accept loop, so idle sockets cannot
/// keep shutdown or an immediate rebind alive indefinitely.
pub(crate) struct BackendServiceHandle {
    state: Arc<BackendState>,
}

#[derive(Clone)]
struct BackendHandler {
    peer: Option<std::net::SocketAddr>,
    control: Arc<FrontendControlState>,
    load_service: Arc<CompatLoadService>,
    lake_agent_task_adapter: Arc<CompatLakeAgentTaskAdapter>,
    submit_task_pool: Arc<ThreadPool>,
}

fn stub_status(method: &str) -> TStatus {
    TStatus::new(
        TStatusCode::NOT_IMPLEMENTED_ERROR,
        Some(vec![format!("novarocks BackendService stub: {method}")]),
    )
}

fn ok_status() -> TStatus {
    TStatus::new(TStatusCode::OK, None)
}

fn internal_error_status(message: String) -> TStatus {
    TStatus::new(TStatusCode::INTERNAL_ERROR, Some(vec![message]))
}

fn next_report_version() -> i64 {
    static REPORT_VERSION: AtomicI64 = AtomicI64::new(1);
    REPORT_VERSION.fetch_add(1, Ordering::AcqRel)
}

const CREATE_TABLET_ADD_SHARD_WAIT_MS: u64 = 1_500;
const CREATE_TABLET_ADD_SHARD_POLL_MS: u64 = 25;
const ALTER_FAILFAST_ERROR_REPORT_TIMES: usize = 3;
const FINISH_TASK_MAX_RETRY: usize = 3;
const ALTER_FINISH_TASK_MAX_RETRY: usize = 10;
const FINISH_TASK_RETRY_INITIAL_BACKOFF: Duration = Duration::from_secs(1);

fn shutdown_probe_host(bind_host: &str) -> String {
    match bind_host {
        "" | "0.0.0.0" => "127.0.0.1".to_string(),
        "::" | "[::]" => "::1".to_string(),
        other => other.to_string(),
    }
}

fn json_summary<T: thrift::protocol::TSerializable>(value: &T) -> String {
    match thrift_named_json(value) {
        Ok(json) if json.len() > 2048 => format!("{}...<truncated>", &json[..2048]),
        Ok(json) => json,
        Err(error) => format!("<json_error:{error}>"),
    }
}

fn build_backend_for_finish_task(
    control: &FrontendControlState,
) -> Result<types::TBackend, String> {
    let cfg = novarocks_app_config().map_err(|error| error.to_string())?;
    let host = control
        .latest_backend_host()
        .filter(|host| !host.trim().is_empty())
        .unwrap_or(network::advertise_host()?);
    Ok(types::TBackend::new(
        host,
        cfg.server.be_port as i32,
        cfg.server.http_port as i32,
    ))
}

fn build_finish_task_request(
    control: &FrontendControlState,
    task: &agent_service::TAgentTaskRequest,
    task_status: TStatus,
) -> Result<master_service::TFinishTaskRequest, String> {
    Ok(master_service::TFinishTaskRequest::new(
        build_backend_for_finish_task(control)?,
        task.task_type,
        task.signature,
        task_status,
        Some(next_report_version()),
        None::<Vec<master_service::TTabletInfo>>,
        None::<i64>,
        None::<i64>,
        None::<i64>,
        None::<String>,
        None::<Vec<types::TTabletId>>,
        None::<Vec<String>>,
        None::<BTreeMap<types::TTabletId, Vec<String>>>,
        None::<Vec<types::TTabletId>>,
        None::<i64>,
        None::<i64>,
        None::<Vec<master_service::TTabletVersionPair>>,
        None::<Vec<master_service::TTabletVersionPair>>,
        None::<types::TSnapshotInfo>,
    ))
}

fn send_finish_task_with_retry(
    control: &FrontendControlState,
    task: &agent_service::TAgentTaskRequest,
    task_status: TStatus,
) -> Result<(), String> {
    let fe_addr = control.latest_fe_addr().ok_or_else(|| {
        "missing FE address for finish_task (heartbeat not received yet)".to_string()
    })?;
    let request = build_finish_task_request(control, task, task_status)?;
    let mut attempt = 0usize;
    let mut max_attempts = FINISH_TASK_MAX_RETRY;
    let mut retry_delay = FINISH_TASK_RETRY_INITIAL_BACKOFF;
    loop {
        attempt += 1;
        tracing::debug!(signature = request.signature, task_type = ?request.task_type,
            report_version = ?request.report_version, req = %json_summary(&request),
            "BackendService.finish_task sending request");
        match frontend_rpc::finish_task(&fe_addr, request.clone()) {
            Ok(result) if result.status.status_code == TStatusCode::OK => return Ok(()),
            Ok(result) => {
                let code = result.status.status_code;
                let delay = if code == TStatusCode::TOO_MANY_TASKS
                    && task.task_type == types::TTaskType::ALTER
                {
                    max_attempts = ALTER_FINISH_TASK_MAX_RETRY;
                    retry_delay = retry_delay.saturating_mul(2);
                    Some(retry_delay)
                } else if code == TStatusCode::LEADER_TRANSFERRED
                    && task.task_type == types::TTaskType::CREATE
                {
                    retry_delay = retry_delay.saturating_mul(2);
                    Some(retry_delay)
                } else {
                    None
                };
                if let Some(delay) = delay.filter(|_| attempt < max_attempts) {
                    tracing::warn!(signature = task.signature, task_type = ?task.task_type,
                        attempt, max_attempts, status_code = ?code,
                        sleep_ms = delay.as_millis() as u64,
                        "BackendService.finish_task got retryable FE status");
                    thread::sleep(delay);
                    continue;
                }
                return Err(format!(
                    "FE finish_task returned non-OK for signature={}: {:?}",
                    task.signature, result.status
                ));
            }
            Err(error) => {
                if frontend_rpc::is_transport_error(&error) && attempt < max_attempts {
                    tracing::warn!(signature = task.signature, task_type = ?task.task_type,
                        attempt, max_attempts, sleep_ms = retry_delay.as_millis() as u64,
                        error = %error, "BackendService.finish_task transport error, retrying");
                    thread::sleep(retry_delay);
                    continue;
                }
                return Err(format!(
                    "FE finish_task rpc failed for signature={}: {error}",
                    task.signature
                ));
            }
        }
    }
}

fn wait_for_starlet_add_shard(tablet_id: i64) -> Option<starlet_shard_registry::StarletShardInfo> {
    let started_at = Instant::now();
    loop {
        let mut infos = starlet_shard_registry::select_infos(&[tablet_id]);
        if let Some(info) = infos.remove(&tablet_id) {
            if started_at.elapsed() >= Duration::from_millis(CREATE_TABLET_ADD_SHARD_POLL_MS) {
                tracing::info!(
                    tablet_id,
                    waited_ms = started_at.elapsed().as_millis(),
                    "resolved AddShard path for create_tablet after waiting"
                );
            }
            return Some(info);
        }
        if started_at.elapsed() >= Duration::from_millis(CREATE_TABLET_ADD_SHARD_WAIT_MS) {
            return None;
        }
        thread::sleep(Duration::from_millis(CREATE_TABLET_ADD_SHARD_POLL_MS));
    }
}

fn execute_backend_task(
    task: &agent_service::TAgentTaskRequest,
    adapter: &CompatLakeAgentTaskAdapter,
) -> Result<(), String> {
    match task.task_type {
        types::TTaskType::CREATE => {
            let request = task.create_tablet_req.as_ref()
                .ok_or_else(|| "create_tablet task missing create_tablet_req".to_string())?;
            let tablet_type = request.tablet_type
                .unwrap_or(agent_service::TTabletType::TABLET_TYPE_DISK);
            if tablet_type != agent_service::TTabletType::TABLET_TYPE_LAKE {
                return Err(format!("unsupported create_tablet tablet_type={tablet_type:?} for tablet_id={} (only TABLET_TYPE_LAKE is supported)", request.tablet_id));
            }
            let shard = wait_for_starlet_add_shard(request.tablet_id).ok_or_else(|| format!(
                "missing shard path from Starlet AddShard cache for create_tablet tablet_id={} after waiting {}ms",
                request.tablet_id, CREATE_TABLET_ADD_SHARD_WAIT_MS))?;
            adapter.create_tablet(request, &shard).map_err(|error| format!("create_tablet failed: {error}"))
        }
        types::TTaskType::ALTER => task.alter_tablet_req_v2.as_ref()
            .ok_or_else(|| "alter task missing alter_tablet_req_v2".to_string())
            .and_then(|request| adapter.alter_tablet(request))
            .map_err(|error| format!("alter task failed: {error}")),
        types::TTaskType::UPDATE_TABLET_META_INFO => task.update_tablet_meta_info_req.as_ref()
            .ok_or_else(|| "update_tablet_meta_info task missing update_tablet_meta_info_req".to_string())
            .and_then(|request| {
                tracing::info!(signature = task.signature, txn_id = ?request.txn_id,
                    tablet_meta_info_count = request.tablet_meta_infos.as_ref().map_or(0, Vec::len),
                    req = %json_summary(request), "BackendService.update_tablet_meta_info received request");
                adapter.update_tablet_meta_info(request)
            })
            .map_err(|error| format!("update_tablet_meta_info failed: {error}")),
        types::TTaskType::DROP_AUTO_INCREMENT_MAP => task.drop_auto_increment_map_req.as_ref()
            .ok_or_else(|| "drop_auto_increment_map task missing drop_auto_increment_map_req".to_string())
            .map(|request| clear_auto_increment_cache_for_table(request.table_id))
            .map_err(|error| format!("drop_auto_increment_map failed: {error}")),
        other => Err(format!("unsupported backend task_type={other:?} in submit_tasks")),
    }
}

fn finish_task_report_times_for_error(task_type: types::TTaskType, error: &str) -> usize {
    if task_type == types::TTaskType::ALTER
        && (error.contains("unsupported") || error.contains("does not support"))
    {
        ALTER_FAILFAST_ERROR_REPORT_TIMES
    } else {
        1
    }
}

fn process_submit_task(handler: BackendHandler, task: agent_service::TAgentTaskRequest) {
    let create_tablet_id = task
        .create_tablet_req
        .as_ref()
        .map(|request| request.tablet_id);
    let drop_tablet_id = task
        .drop_tablet_req
        .as_ref()
        .map(|request| request.tablet_id);
    tracing::info!(peer = ?handler.peer, signature = task.signature, task_type = ?task.task_type,
        create_tablet_id = ?create_tablet_id, drop_tablet_id = ?drop_tablet_id,
        "BackendService.submit_tasks accepted task");
    let task_result = execute_backend_task(&task, &handler.lake_agent_task_adapter);
    if let Err(error) = &task_result {
        tracing::warn!(peer = ?handler.peer, signature = task.signature, task_type = ?task.task_type,
            error = %error, "BackendService.submit_tasks task execution failed");
    }
    let task_error = task_result.err();
    for report_attempt in 0..task_error
        .as_deref()
        .map(|error| finish_task_report_times_for_error(task.task_type, error))
        .unwrap_or(1)
    {
        let status = task_error
            .as_ref()
            .map_or_else(|| ok_status(), |error| internal_error_status(error.clone()));
        if let Err(error) = send_finish_task_with_retry(&handler.control, &task, status) {
            tracing::warn!(peer = ?handler.peer, signature = task.signature, task_type = ?task.task_type,
                report_attempt, error = %error, "BackendService.submit_tasks failed to report finish_task");
            break;
        }
    }
}

impl BackendServiceSyncHandler for BackendHandler {
    fn handle_exec_plan_fragment(
        &self,
        _: internal_service::TExecPlanFragmentParams,
    ) -> thrift::Result<internal_service::TExecPlanFragmentResult> {
        Ok(internal_service::TExecPlanFragmentResult::new(
            Some(stub_status("exec_plan_fragment")),
            None,
        ))
    }
    fn handle_cancel_plan_fragment(
        &self,
        _: internal_service::TCancelPlanFragmentParams,
    ) -> thrift::Result<internal_service::TCancelPlanFragmentResult> {
        Ok(internal_service::TCancelPlanFragmentResult::new(Some(
            stub_status("cancel_plan_fragment"),
        )))
    }
    fn handle_transmit_data(
        &self,
        _: internal_service::TTransmitDataParams,
    ) -> thrift::Result<internal_service::TTransmitDataResult> {
        Ok(internal_service::TTransmitDataResult::new(
            Some(stub_status("transmit_data")),
            None,
            None,
            None,
        ))
    }
    fn handle_fetch_data(
        &self,
        _: internal_service::TFetchDataParams,
    ) -> thrift::Result<internal_service::TFetchDataResult> {
        Ok(internal_service::TFetchDataResult::new(
            crate::thrift::data::TResultBatch::new(vec![], false, 0, None),
            true,
            0,
            Some(stub_status("fetch_data")),
        ))
    }
    fn handle_submit_tasks(
        &self,
        tasks: Vec<agent_service::TAgentTaskRequest>,
    ) -> thrift::Result<agent_service::TAgentResult> {
        let tasks_len = tasks.len();
        for task in tasks {
            let handler = self.clone();
            self.submit_task_pool
                .execute(move || process_submit_task(handler, task));
        }
        tracing::debug!(peer = ?self.peer, tasks_len, "Received BackendService.submit_tasks and queued tasks");
        Ok(agent_service::TAgentResult::new(
            ok_status(),
            None,
            None,
            None,
        ))
    }
    fn handle_make_snapshot(
        &self,
        request: agent_service::TSnapshotRequest,
    ) -> thrift::Result<agent_service::TAgentResult> {
        tracing::debug!(peer = ?self.peer, req = %json_summary(&request), "Received BackendService.make_snapshot");
        Ok(agent_service::TAgentResult::new(
            stub_status("make_snapshot"),
            None,
            None,
            None,
        ))
    }
    fn handle_release_snapshot(
        &self,
        snapshot_path: String,
    ) -> thrift::Result<agent_service::TAgentResult> {
        tracing::debug!(peer = ?self.peer, snapshot_path = %snapshot_path, "Received BackendService.release_snapshot");
        Ok(agent_service::TAgentResult::new(
            stub_status("release_snapshot"),
            None,
            None,
            None,
        ))
    }
    fn handle_publish_cluster_state(
        &self,
        request: agent_service::TAgentPublishRequest,
    ) -> thrift::Result<agent_service::TAgentResult> {
        tracing::debug!(peer = ?self.peer, req = %json_summary(&request), "Received BackendService.publish_cluster_state");
        Ok(agent_service::TAgentResult::new(
            stub_status("publish_cluster_state"),
            None,
            None,
            None,
        ))
    }
    fn handle_submit_etl_task(
        &self,
        request: agent_service::TMiniLoadEtlTaskRequest,
    ) -> thrift::Result<agent_service::TAgentResult> {
        tracing::debug!(peer = ?self.peer, req = %json_summary(&request), "Received BackendService.submit_etl_task");
        Ok(agent_service::TAgentResult::new(
            stub_status("submit_etl_task"),
            None,
            None,
            None,
        ))
    }
    fn handle_get_etl_status(
        &self,
        request: agent_service::TMiniLoadEtlStatusRequest,
    ) -> thrift::Result<agent_service::TMiniLoadEtlStatusResult> {
        tracing::debug!(peer = ?self.peer, req = %json_summary(&request), "Received BackendService.get_etl_status");
        Ok(agent_service::TMiniLoadEtlStatusResult::new(
            stub_status("get_etl_status"),
            types::TEtlState::UNKNOWN,
            None,
            None,
            None,
        ))
    }
    fn handle_delete_etl_files(
        &self,
        request: agent_service::TDeleteEtlFilesRequest,
    ) -> thrift::Result<agent_service::TAgentResult> {
        tracing::debug!(peer = ?self.peer, req = %json_summary(&request), "Received BackendService.delete_etl_files");
        Ok(agent_service::TAgentResult::new(
            stub_status("delete_etl_files"),
            None,
            None,
            None,
        ))
    }
    fn handle_submit_export_task(&self, request: TExportTaskRequest) -> thrift::Result<TStatus> {
        tracing::debug!(peer = ?self.peer, req = %json_summary(&request), "Received BackendService.submit_export_task");
        Ok(stub_status("submit_export_task"))
    }
    fn handle_get_export_status(
        &self,
        task_id: types::TUniqueId,
    ) -> thrift::Result<internal_service::TExportStatusResult> {
        tracing::debug!(peer = ?self.peer, task_id = %json_summary(&task_id), "Received BackendService.get_export_status");
        Ok(internal_service::TExportStatusResult::new(
            stub_status("get_export_status"),
            types::TExportState::UNKNOWN,
            None,
        ))
    }
    fn handle_erase_export_task(&self, task_id: types::TUniqueId) -> thrift::Result<TStatus> {
        tracing::debug!(peer = ?self.peer, task_id = %json_summary(&task_id), "Received BackendService.erase_export_task");
        Ok(stub_status("erase_export_task"))
    }
    fn handle_get_tablet_stat(&self) -> thrift::Result<TTabletStatResult> {
        tracing::debug!(peer = ?self.peer, "Received BackendService.get_tablet_stat");
        Ok(TTabletStatResult::new(BTreeMap::new()))
    }
    fn handle_submit_routine_load_task(
        &self,
        tasks: Vec<TRoutineLoadTask>,
    ) -> thrift::Result<TStatus> {
        tracing::debug!(peer = ?self.peer, tasks_len = tasks.len(), "Received BackendService.submit_routine_load_task");
        Ok(stub_status("submit_routine_load_task"))
    }
    fn handle_finish_stream_load_channel(
        &self,
        channel: TStreamLoadChannel,
    ) -> thrift::Result<TStatus> {
        tracing::debug!(peer = ?self.peer, channel = %json_summary(&channel), "Received BackendService.finish_stream_load_channel");
        Ok(
            match self.load_service.finish_stream_load_channel(
                channel.label.as_deref(),
                channel.table_name.as_deref(),
                channel.channel_id,
            ) {
                Ok(()) => ok_status(),
                Err(message) => TStatus::new(TStatusCode::TXN_NOT_EXISTS, Some(vec![message])),
            },
        )
    }
    fn handle_open_scanner(
        &self,
        params: starrocks_external_service::TScanOpenParams,
    ) -> thrift::Result<starrocks_external_service::TScanOpenResult> {
        tracing::debug!(peer = ?self.peer, params = %json_summary(&params), "Received BackendService.open_scanner");
        Ok(starrocks_external_service::TScanOpenResult::new(
            stub_status("open_scanner"),
            None,
            None,
        ))
    }
    fn handle_get_next(
        &self,
        params: starrocks_external_service::TScanNextBatchParams,
    ) -> thrift::Result<starrocks_external_service::TScanBatchResult> {
        tracing::debug!(peer = ?self.peer, params = %json_summary(&params), "Received BackendService.get_next");
        Ok(starrocks_external_service::TScanBatchResult::new(
            stub_status("get_next"),
            None,
            None,
        ))
    }
    fn handle_close_scanner(
        &self,
        params: starrocks_external_service::TScanCloseParams,
    ) -> thrift::Result<starrocks_external_service::TScanCloseResult> {
        tracing::debug!(peer = ?self.peer, params = %json_summary(&params), "Received BackendService.close_scanner");
        Ok(starrocks_external_service::TScanCloseResult::new(
            stub_status("close_scanner"),
        ))
    }
    fn handle_get_tablets_info(
        &self,
        request: TGetTabletsInfoRequest,
    ) -> thrift::Result<TGetTabletsInfoResult> {
        tracing::debug!(peer = ?self.peer, req = %json_summary(&request), "Received BackendService.get_tablets_info");
        Ok(TGetTabletsInfoResult::new(
            stub_status("get_tablets_info"),
            None,
            None,
        ))
    }
}

pub(crate) fn start_backend_service(
    config: BackendServiceConfig,
    control: Arc<FrontendControlState>,
    load_service: Arc<CompatLoadService>,
    lake_agent_task_adapter: Arc<CompatLakeAgentTaskAdapter>,
) -> Result<BackendServiceHandle, String> {
    let host = if config.host.is_empty() {
        "0.0.0.0".to_string()
    } else {
        config.host
    };
    let address = format!("{}:{}", host, config.be_port);
    let listener = TcpListener::bind(&address)
        .map_err(|error| format!("BackendService bind error: {error}"))?;
    listener
        .set_nonblocking(true)
        .map_err(|error| format!("BackendService set_nonblocking failed: {error}"))?;
    let submit_task_pool = Arc::new(ThreadPool::with_name(
        "BackendService submit_task".to_string(),
        8,
    ));
    let state = Arc::new(BackendState {
        stop: AtomicBool::new(false),
        join_handle: Mutex::new(None),
        wake_addr: format!("{}:{}", shutdown_probe_host(&host), config.be_port),
        connections: Mutex::new(Vec::new()),
        submit_task_pool: Arc::clone(&submit_task_pool),
    });
    let state_for_thread = Arc::clone(&state);
    let address_for_log = address.clone();
    let join_handle = thread::Builder::new()
        .name("compat-backend-service".to_string())
        .spawn(move || {
            tracing::info!("BackendService listening on {}", address_for_log);
            let worker_pool = ThreadPool::with_name("BackendService processor".to_owned(), 4);
            while !state_for_thread.stop.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        if state_for_thread.stop.load(Ordering::Acquire) {
                            break;
                        }
                        if stream.set_nonblocking(false).is_err() {
                            continue;
                        }
                        let peer = stream.peer_addr().ok();
                        if let Ok(copy) = stream.try_clone() {
                            state_for_thread
                                .connections
                                .lock()
                                .expect("backend connection lock")
                                .push(copy);
                        }
                        let state = Arc::clone(&state_for_thread);
                        let handler = BackendHandler {
                            peer,
                            control: Arc::clone(&control),
                            load_service: Arc::clone(&load_service),
                            lake_agent_task_adapter: Arc::clone(&lake_agent_task_adapter),
                            submit_task_pool: Arc::clone(&submit_task_pool),
                        };
                        worker_pool.execute(move || process_connection(stream, handler, state));
                    }
                    Err(error) if error.kind() == ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(50))
                    }
                    Err(error) if !state_for_thread.stop.load(Ordering::Acquire) => {
                        tracing::warn!("failed to accept remote connection: {error}")
                    }
                    Err(_) => break,
                }
            }
            worker_pool.join();
        })
        .map_err(|error| format!("Failed to spawn backend service thread: {error}"))?;
    *state
        .join_handle
        .lock()
        .map_err(|_| "lock backend service state failed".to_string())? = Some(join_handle);
    Ok(BackendServiceHandle { state })
}

fn process_connection(stream: TcpStream, handler: BackendHandler, _state: Arc<BackendState>) {
    let peer = handler.peer;
    let mut first8 = [0u8; 8];
    let count = stream
        .set_read_timeout(Some(Duration::from_millis(50)))
        .and_then(|_| stream.peek(&mut first8))
        .unwrap_or(0);
    let _ = stream.set_read_timeout(None);
    let channel = TTcpChannel::with_stream(stream);
    let Ok((read, write)) = channel.split() else {
        return;
    };
    let input = TBufferedReadTransportFactory::new().create(Box::new(read));
    let mut input = TBinaryInputProtocolFactory::new().create(input);
    let output = TBufferedWriteTransportFactory::new().create(Box::new(write));
    let mut output = TBinaryOutputProtocolFactory::new().create(output);
    let processor = BackendServiceSyncProcessor::new(handler);
    loop {
        match processor.process(&mut *input, &mut *output) {
            Ok(()) => {}
            Err(thrift::Error::Transport(error))
                if error.kind == thrift::TransportErrorKind::EndOfFile =>
            {
                break;
            }
            Err(thrift::Error::Protocol(error))
                if error.kind == thrift::ProtocolErrorKind::BadVersion =>
            {
                tracing::warn!(peer = ?peer, first_bytes = ?&first8[..count.min(first8.len())], thrift_message = %error.message, "ProtocolError(BadVersion) on be_port");
                break;
            }
            Err(error) => {
                tracing::warn!(peer = ?peer, "processor error: {error:?}");
                break;
            }
        }
    }
}

impl BackendServiceHandle {
    pub(crate) fn poll_failure(&self) -> Result<Option<String>, String> {
        if self.state.stop.load(Ordering::Acquire) {
            return Ok(None);
        }
        let handle = {
            let mut handle = self
                .state
                .join_handle
                .lock()
                .map_err(|_| "lock backend service state failed".to_string())?;
            let Some(listener) = handle.as_ref() else {
                return Ok(None);
            };
            if !listener.is_finished() {
                return Ok(None);
            }
            handle
                .take()
                .expect("backend listener was checked as present")
        };
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
            format!("BackendService listener thread panicked: {detail}")
        })?;
        Ok(Some(
            "BackendService listener stopped unexpectedly".to_string(),
        ))
    }

    pub(crate) fn stop(&self) -> Result<(), String> {
        if self.state.stop.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        for connection in self
            .state
            .connections
            .lock()
            .map_err(|_| "lock backend connections failed".to_string())?
            .iter()
        {
            let _ = connection.shutdown(Shutdown::Both);
        }
        let _ = TcpStream::connect(&self.state.wake_addr);
        let handle = self
            .state
            .join_handle
            .lock()
            .map_err(|_| "lock backend service state failed".to_string())?
            .take();
        if let Some(handle) = handle {
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
                format!("BackendService listener thread panicked: {detail}")
            })?;
        }
        self.state.submit_task_pool.join();
        Ok(())
    }
}

impl Drop for BackendServiceHandle {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::{BackendServiceHandle, BackendState, finish_task_report_times_for_error};
    use crate::thrift::types::TTaskType;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex};

    #[test]
    fn alter_unsupported_error_preserves_failfast_finish_reports() {
        assert_eq!(
            finish_task_report_times_for_error(TTaskType::ALTER, "unsupported request"),
            3
        );
        assert_eq!(
            finish_task_report_times_for_error(TTaskType::CREATE, "unsupported request"),
            1
        );
    }

    #[test]
    fn listener_handle_stop_is_idempotent() {
        let handle = BackendServiceHandle {
            state: Arc::new(BackendState {
                stop: AtomicBool::new(false),
                join_handle: Mutex::new(Some(std::thread::spawn(|| {}))),
                wake_addr: "127.0.0.1:1".to_string(),
                connections: Mutex::new(Vec::new()),
                submit_task_pool: Arc::new(threadpool::ThreadPool::new(1)),
            }),
        };

        handle.stop().expect("first stop");
        handle.stop().expect("second stop");
    }

    #[test]
    fn listener_handle_reports_unexpected_exit_to_supervisor() {
        let handle = BackendServiceHandle {
            state: Arc::new(BackendState {
                stop: AtomicBool::new(false),
                join_handle: Mutex::new(Some(std::thread::spawn(|| {}))),
                wake_addr: "127.0.0.1:1".to_string(),
                connections: Mutex::new(Vec::new()),
                submit_task_pool: Arc::new(threadpool::ThreadPool::new(1)),
            }),
        };

        for _ in 0..100 {
            if let Some(error) = handle.poll_failure().expect("poll listener") {
                assert_eq!(error, "BackendService listener stopped unexpectedly");
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        panic!("listener exit was not reported to the supervisor");
    }
}
