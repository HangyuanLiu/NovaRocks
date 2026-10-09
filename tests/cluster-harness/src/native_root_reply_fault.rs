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

//! Harness-only authenticated plaintext H2 response-message fault actor.
//! Its owned-buffer counters exclude H2 internals, kernel buffers and DTO metadata.

use anyhow::{Context, Result, bail, ensure};
use bytes::Bytes;
use h2::{RecvStream, SendStream, client, server};
use http::{HeaderMap, Request, Response};
use novarocks_execution_contract::{identity::TaskIdentity, root_result::RootResultRead};
use novarocks_native_trust::{NativeCallerSubject, NativeProcessIdentity, NativeServerAdmission};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::{novarocks as wire, result as result_wire};
use novarocks_result_contract::{
    ClientRowProfile, ClientRowStreamCursor, RootOutputKind, RootProfileId, RootProfileV1,
};
use novarocks_task_codec::root_result::{decode_read, decode_reply};
use novarocks_types::FrontendProcessId;
use prost::Message;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::future::poll_fn;
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tokio::sync::{Notify, Semaphore, watch};
use tokio::task::JoinSet;

const ROOT_PATH: &str = "/novarocks.NovaRocksGrpc/FetchRootResult";
const REQUEST_BYTES: usize = RootProfileV1::ENVELOPE_BYTES;
const RESPONSE_BYTES: usize = RootProfileV1::SEGMENT_BYTES + RootProfileV1::ENVELOPE_BYTES;
const FRAME_BYTES: usize = 16 * 1024;
const HEADER_BYTES: u32 = 16 * 1024;
const H2_STREAMS: u32 = 128;
const H2_WINDOW: u32 = 256 * 1024;
const H2_CONNECTION_WINDOW: u32 = 1024 * 1024;
const H2_SEND_BUFFER: usize = 64 * 1024;
const H2_RESETS: usize = 32;
const FAILURE_COUNT: usize = 32;
const FAILURE_BYTES: usize = 4096;
const MAX_TARGET_ATTEMPTS: u64 = 32;

#[derive(Clone, Debug, Serialize)]
pub struct RootReplyFaultBounds {
    pub maximum_connections: usize,
    pub maximum_active_streams: usize,
    pub maximum_owned_buffer_bytes: usize,
    pub handshake_timeout_millis: u64,
    pub forward_timeout_millis: u64,
    pub maximum_capture_millis: u64,
}

impl Default for RootReplyFaultBounds {
    fn default() -> Self {
        Self {
            maximum_connections: 32,
            maximum_active_streams: 256,
            maximum_owned_buffer_bytes: 16 * 1024 * 1024,
            handshake_timeout_millis: 2000,
            forward_timeout_millis: 20_000,
            maximum_capture_millis: 5000,
        }
    }
}

impl RootReplyFaultBounds {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            (1..=32).contains(&self.maximum_connections),
            "invalid actor connection bound"
        );
        ensure!(
            (1..=256).contains(&self.maximum_active_streams),
            "invalid actor stream bound"
        );
        ensure!(
            self.maximum_owned_buffer_bytes >= 2 * RESPONSE_BYTES + REQUEST_BYTES
                && self.maximum_owned_buffer_bytes <= 16 * 1024 * 1024,
            "invalid actor owned-buffer bound"
        );
        for millis in [
            self.handshake_timeout_millis,
            self.forward_timeout_millis,
            self.maximum_capture_millis,
        ] {
            ensure!(
                (1..=20_000).contains(&millis),
                "invalid actor deadline bound"
            );
        }
        ensure!(
            self.maximum_capture_millis <= 5000,
            "capture ceiling cannot exceed five seconds"
        );
        Ok(())
    }

    pub(crate) fn semantics(&self) -> serde_json::Value {
        serde_json::json!({
            "mode":"authenticated-plaintext-root-reply-message-v1", "bounds":self,
            "target_slots":1, "maximum_data_listeners":3, "one_shot_per_actor_group":true,
            "maximum_target_attempts":MAX_TARGET_ATTEMPTS, "target_attempt_evidence_positions":MAX_TARGET_ATTEMPTS,
            "not_ready_transparency":true,
            "capture_deadline":"single-absolute-never-reset",
            "request_bytes":REQUEST_BYTES, "response_bytes":RESPONSE_BYTES,
            "h2_frame_bytes":FRAME_BYTES, "h2_header_bytes":HEADER_BYTES,
            "h2_streams_per_connection":H2_STREAMS, "h2_stream_window_bytes":H2_WINDOW,
            "h2_connection_window_bytes":H2_CONNECTION_WINDOW,
            "h2_send_buffer_bytes":H2_SEND_BUFFER, "h2_reset_positions":H2_RESETS,
            "client_hpack_table_bytes":4096, "server_hpack":"locked-h2-public-default",
            "failures":FAILURE_COUNT, "failure_bytes":FAILURE_BYTES,
            "failure_digest_positions":3, "failure_digest_sha256_bytes":64,
            "overflow_policy":"fixed-first-cause-first-overflow-last-overflow-wholefailure",
            "accounting":"actor-owned-Vec-capacity-only-not-production-physical-envelope"
        })
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
pub enum RootReplyMutation {
    ProfileTwo,
    ClientRowsFalse,
    FourBytePrefixOnly,
}

/// No mutation is authorized by a candidate. The runner must independently
/// resolve this task as its installed root before calling `arm_exact`.
#[derive(Clone, Debug)]
pub struct RootReplyCandidate {
    pub read: RootResultRead,
    pub backend_index: usize,
    pub caller: FrontendProcessId,
    pub downstream_stream: u32,
    pub upstream_stream: u32,
    pub request_sha256: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct RootReplyFaultObservation {
    pub active_listeners: usize,
    pub joined_listeners: u64,
    pub active_connections: usize,
    pub active_streams: usize,
    pub connection_positions: usize,
    pub stream_positions: usize,
    pub target_slots: usize,
    pub owned_buffer_bytes: usize,
    pub peak_owned_buffer_bytes: usize,
    pub claimed: u64,
    pub emitted_messages: u64,
    pub frozen_root_task: Option<String>,
    pub frozen_request: Option<serde_json::Value>,
    pub target_requests: u64,
    pub target_consumed_positive_requests: u64,
    pub joined_connections: u64,
    pub joined_children: u64,
    pub shutdown_joined: bool,
    pub failures: Vec<String>,
    pub failure_overflow: bool,
    pub first_failure: Option<FailureDigest>,
    pub first_overflow: Option<FailureDigest>,
    pub last_overflow: Option<FailureDigest>,
    pub overflow_failures: u64,
    pub non_target_peer_cancels: u64,
    pub target_attempts: u64,
    pub not_ready_observed: u64,
    pub not_ready_forwarded: u64,
    pub not_ready_replies: Vec<serde_json::Value>,
    pub target_attempt_requests: Vec<serde_json::Value>,
    pub original: Option<serde_json::Value>,
    pub mutated: Option<serde_json::Value>,
}

#[derive(Clone, Debug, Serialize)]
pub struct FailureDigest {
    pub class: &'static str,
    pub sha256: String,
    pub bytes: usize,
}
impl FailureDigest {
    fn new(error: &str) -> Self {
        let class = if error.contains("capture absolute deadline") {
            "capture-deadline"
        } else if error.contains("target attempt cap") {
            "target-attempt-cap"
        } else if error.starts_with("root actor") {
            "actor-lifecycle"
        } else if error.starts_with("root child") {
            "child-lifecycle"
        } else if error.starts_with("root cleanup") {
            "cleanup"
        } else if error.starts_with("root connection") {
            "connection-or-stream"
        } else {
            "actor-refusal"
        };
        Self {
            class,
            sha256: hash(error.as_bytes()),
            bytes: error.len(),
        }
    }
}
struct Slot {
    candidate: RootReplyCandidate,
}
#[derive(Default)]
struct State {
    used_capture: bool,
    deadline: Option<Instant>,
    slot: Option<Slot>,
    pending_capture: bool,
    claimed: u64,
    emitted: u64,
    frozen_root: Option<(TaskIdentity, FrontendProcessId)>,
    frozen_request: Option<serde_json::Value>,
    frozen_backend_index: Option<usize>,
    armed_mutation: Option<RootReplyMutation>,
    target_attempts: u64,
    non_target_peer_cancels: u64,
    not_ready_observed: u64,
    not_ready_forwarded: u64,
    not_ready_replies: Vec<serde_json::Value>,
    target_attempt_requests: Vec<serde_json::Value>,
    target_requests: u64,
    target_consumed_positive_requests: u64,
    shutdown_joined: bool,
    failures: Vec<String>,
    failure_overflow: bool,
    first_failure: Option<FailureDigest>,
    first_overflow: Option<FailureDigest>,
    last_overflow: Option<FailureDigest>,
    overflow_failures: u64,
    original: Option<serde_json::Value>,
    mutated: Option<serde_json::Value>,
}
struct Core {
    bounds: RootReplyFaultBounds,
    verifier: NativeServerAdmission,
    normal_subject: NativeCallerSubject,
    state: Mutex<State>,
    changed: Notify,
    stop: watch::Sender<bool>,
    listeners: AtomicUsize,
    joined_listeners: AtomicU64,
    connection_positions: Arc<Semaphore>,
    stream_positions: Arc<Semaphore>,
    connections: AtomicUsize,
    streams: AtomicUsize,
    bytes: AtomicUsize,
    peak_bytes: AtomicUsize,
    joined_connections: AtomicU64,
    joined_children: AtomicU64,
}

impl Core {
    fn failure(&self, error: &str) {
        let mut state = self.state.lock().expect("root fault state lock");
        let digest = FailureDigest::new(error);
        if state.first_failure.is_none() {
            state.first_failure = Some(digest.clone());
        }
        if error.len() > FAILURE_BYTES || state.failures.len() == FAILURE_COUNT {
            state.failure_overflow = true;
            state.overflow_failures = state.overflow_failures.saturating_add(1);
            if state.first_overflow.is_none() {
                state.first_overflow = Some(digest.clone());
            }
            state.last_overflow = Some(digest);
        } else {
            state.failures.push(error.to_owned());
        }
        // A diagnostic overflow is itself a failure; observations cannot pass
        // on the strength of a truncated or partially preserved failure set.
        state.deadline = None;
        self.changed.notify_waiters();
    }
}

/// Non-owning control. No secret, JWT or arbitrary response bytes are exposed.
#[derive(Clone)]
pub struct RootReplyFaultControl {
    core: Arc<Core>,
}

impl RootReplyFaultControl {
    pub fn begin_capture(&self, deadline: Instant) -> Result<()> {
        let now = Instant::now();
        ensure!(
            deadline > now
                && deadline.duration_since(now)
                    <= Duration::from_millis(self.core.bounds.maximum_capture_millis),
            "invalid absolute capture deadline"
        );
        let mut state = self.core.state.lock().expect("root fault state lock");
        ensure!(
            !state.used_capture && state.failures.is_empty() && !state.failure_overflow,
            "root fault actor cannot capture again or after failure"
        );
        state.used_capture = true;
        state.deadline = Some(deadline);
        drop(state);
        self.core.changed.notify_waiters();
        Ok(())
    }

    pub fn candidate(&self) -> Option<RootReplyCandidate> {
        self.core
            .state
            .lock()
            .expect("root fault state lock")
            .slot
            .as_ref()
            .map(|slot| slot.candidate.clone())
    }

    pub fn arm_exact(
        &self,
        root: TaskIdentity,
        caller: FrontendProcessId,
        backend_index: usize,
        mutation: RootReplyMutation,
    ) -> Result<()> {
        let mut state = self.core.state.lock().expect("root fault state lock");
        ensure!(
            state
                .deadline
                .is_some_and(|deadline| Instant::now() < deadline)
                && state.claimed == 0
                && state.failures.is_empty()
                && !state.failure_overflow,
            "root fault cannot arm outside its original capture deadline"
        );
        ensure!(
            state.armed_mutation.is_none(),
            "root mutation already armed"
        );
        let slot = state.slot.as_ref().context("no captured root candidate")?;
        ensure!(
            slot.candidate.read.root_task() == root
                && slot.candidate.caller == caller
                && slot.candidate.backend_index == backend_index,
            "independent root or frontend identity differs from candidate"
        );
        let frozen_request = serde_json::json!({"backend_index":slot.candidate.backend_index,
            "task":root.to_string(), "frontend_process":caller.to_string(),
            "downstream_h2_stream":slot.candidate.downstream_stream,
            "upstream_h2_stream":slot.candidate.upstream_stream,
            "request_sha256":slot.candidate.request_sha256});
        state.armed_mutation = Some(mutation);
        state.frozen_backend_index = Some(backend_index);
        state.frozen_request = Some(frozen_request);
        state.frozen_root = Some((root, caller));
        // The captured request is counted only after independent exact arm.
        state.target_requests = 1;
        drop(state);
        self.core.changed.notify_waiters();
        Ok(())
    }

    pub fn disarm(&self) {
        self.core
            .state
            .lock()
            .expect("root fault state lock")
            .deadline = None;
        self.core.changed.notify_waiters();
    }

    pub fn observation(&self) -> RootReplyFaultObservation {
        let state = self.core.state.lock().expect("root fault state lock");
        RootReplyFaultObservation {
            active_listeners: self.core.listeners.load(Ordering::Acquire),
            joined_listeners: self.core.joined_listeners.load(Ordering::Acquire),
            active_connections: self.core.connections.load(Ordering::Acquire),
            active_streams: self.core.streams.load(Ordering::Acquire),
            connection_positions: self.core.bounds.maximum_connections
                - self.core.connection_positions.available_permits(),
            stream_positions: self.core.bounds.maximum_active_streams
                - self.core.stream_positions.available_permits(),
            target_slots: usize::from(state.pending_capture || state.slot.is_some()),
            owned_buffer_bytes: self.core.bytes.load(Ordering::Acquire),
            peak_owned_buffer_bytes: self.core.peak_bytes.load(Ordering::Acquire),
            claimed: state.claimed,
            emitted_messages: state.emitted,
            frozen_root_task: state.frozen_root.map(|(root, _)| root.to_string()),
            frozen_request: state.frozen_request.clone(),
            target_requests: state.target_requests,
            target_consumed_positive_requests: state.target_consumed_positive_requests,
            joined_connections: self.core.joined_connections.load(Ordering::Acquire),
            joined_children: self.core.joined_children.load(Ordering::Acquire),
            shutdown_joined: state.shutdown_joined,
            failures: state.failures.clone(),
            failure_overflow: state.failure_overflow,
            first_failure: state.first_failure.clone(),
            first_overflow: state.first_overflow.clone(),
            last_overflow: state.last_overflow.clone(),
            overflow_failures: state.overflow_failures,
            non_target_peer_cancels: state.non_target_peer_cancels,
            target_attempts: state.target_attempts,
            not_ready_observed: state.not_ready_observed,
            not_ready_forwarded: state.not_ready_forwarded,
            not_ready_replies: state.not_ready_replies.clone(),
            target_attempt_requests: state.target_attempt_requests.clone(),
            original: state.original.clone(),
            mutated: state.mutated.clone(),
        }
    }
}

pub(crate) struct RootReplyFaultProxy {
    address: SocketAddr,
    generation: watch::Sender<u64>,
    core: Arc<Core>,
    thread: Option<JoinHandle<()>>,
}

impl RootReplyFaultProxy {
    pub(crate) fn start(
        upstream: SocketAddr,
        backend_index: usize,
        shared: Option<&Self>,
        bounds: RootReplyFaultBounds,
        verifier: NativeServerAdmission,
        normal_subject: NativeCallerSubject,
    ) -> Result<Self> {
        bounds.validate()?;
        let listener = TcpListener::bind("127.0.0.1:0").context("bind root fault actor")?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?;
        let (stop, _) = watch::channel(false);
        let (generation, _) = watch::channel(0);
        let core = if let Some(shared) = shared {
            shared.core.clone()
        } else {
            Arc::new(Core {
                connection_positions: Arc::new(Semaphore::new(bounds.maximum_connections)),
                stream_positions: Arc::new(Semaphore::new(bounds.maximum_active_streams)),
                bounds,
                verifier,
                normal_subject,
                state: Mutex::new(State::default()),
                changed: Notify::new(),
                stop,
                listeners: AtomicUsize::new(0),
                joined_listeners: AtomicU64::new(0),
                connections: AtomicUsize::new(0),
                streams: AtomicUsize::new(0),
                bytes: AtomicUsize::new(0),
                peak_bytes: AtomicUsize::new(0),
                joined_connections: AtomicU64::new(0),
                joined_children: AtomicU64::new(0),
            })
        };
        core.listeners.fetch_add(1, Ordering::AcqRel);
        core.state
            .lock()
            .expect("root fault state lock")
            .shutdown_joined = false;
        let thread_core = core.clone();
        let thread_generation = generation.clone();
        let thread = std::thread::Builder::new()
            .name("native-root-reply-fault".into())
            .spawn(move || {
                let outcome = (|| -> Result<()> {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()?;
                    // serve_listener joins all connection owners before returning;
                    // each connection joins its stream tasks and upstream driver.
                    runtime.block_on(serve_listener(
                        listener,
                        upstream,
                        backend_index,
                        thread_generation,
                        thread_core.clone(),
                    ))
                })();
                if let Err(error) = outcome {
                    thread_core.failure(&format!("root actor exit: {error:#}"));
                }
                if thread_core.listeners.fetch_sub(1, Ordering::AcqRel) == 1 {
                    thread_core
                        .state
                        .lock()
                        .expect("root fault state lock")
                        .shutdown_joined = true;
                }
            })
            .map_err(|error| {
                core.listeners.fetch_sub(1, Ordering::AcqRel);
                core.failure("root actor listener thread spawn failed");
                error
            })?;
        Ok(Self {
            address,
            generation,
            core,
            thread: Some(thread),
        })
    }
    pub(crate) fn address(&self) -> SocketAddr {
        self.address
    }
    pub(crate) fn control(&self) -> RootReplyFaultControl {
        RootReplyFaultControl {
            core: self.core.clone(),
        }
    }
    pub(crate) fn disconnect_all(&self) {
        self.control().disarm();
        self.generation
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }
    pub(crate) fn stop(&mut self) {
        self.control().disarm();
        self.core.stop.send_replace(true);
        if let Some(thread) = self.thread.take() {
            if thread.join().is_err() {
                self.core
                    .failure("root actor thread panicked before its join barrier");
            }
            self.core.joined_listeners.fetch_add(1, Ordering::AcqRel);
        }
    }
}
impl Drop for RootReplyFaultProxy {
    fn drop(&mut self) {
        self.stop();
    }
}

struct CountGuard {
    core: Arc<Core>,
    connection: bool,
}
impl CountGuard {
    fn new(core: Arc<Core>, connection: bool) -> Self {
        if connection {
            core.connections.fetch_add(1, Ordering::AcqRel);
        } else {
            core.streams.fetch_add(1, Ordering::AcqRel);
        }
        Self { core, connection }
    }
}
impl Drop for CountGuard {
    fn drop(&mut self) {
        if self.connection {
            self.core.connections.fetch_sub(1, Ordering::AcqRel);
        } else {
            self.core.streams.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

/// The credit survives every H2 DATA alias through Bytes::from_owner. It
/// returns after the Vec backing has been destroyed, never at send_data.
struct OwnedBuffer {
    bytes: Vec<u8>,
    _credit: ByteCredit,
}
impl AsRef<[u8]> for OwnedBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
struct ByteCredit {
    core: Arc<Core>,
    bytes: usize,
}
impl ByteCredit {
    fn new(core: Arc<Core>, bytes: usize) -> Result<Self> {
        let previous = core
            .bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current
                    .checked_add(bytes)
                    .filter(|next| *next <= core.bounds.maximum_owned_buffer_bytes)
            })
            .map_err(|_| anyhow::anyhow!("root actor owned-buffer positions exhausted"))?;
        core.peak_bytes
            .fetch_max(previous + bytes, Ordering::AcqRel);
        Ok(Self { core, bytes })
    }
}
impl Drop for ByteCredit {
    fn drop(&mut self) {
        self.core.bytes.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
impl OwnedBuffer {
    fn new(core: Arc<Core>, capacity: usize) -> Result<Self> {
        let mut credit = ByteCredit::new(core.clone(), capacity)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(capacity)
            .context("allocate bounded actor buffer")?;
        if bytes.capacity() > capacity {
            let mut extra = ByteCredit::new(core, bytes.capacity() - capacity)?;
            credit.bytes += extra.bytes;
            extra.bytes = 0;
            drop(extra);
        }
        Ok(Self {
            bytes,
            _credit: credit,
        })
    }
    fn append(&mut self, bytes: &[u8]) -> Result<()> {
        ensure!(
            self.bytes
                .len()
                .checked_add(bytes.len())
                .is_some_and(|length| length <= self.bytes.capacity()),
            "root actor buffer exceeds fixed capacity"
        );
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
    fn into_bytes(self) -> Bytes {
        Bytes::from_owner(self)
    }
}

async fn serve_listener(
    listener: TcpListener,
    upstream: SocketAddr,
    backend_index: usize,
    generation: watch::Sender<u64>,
    core: Arc<Core>,
) -> Result<()> {
    let listener = tokio::net::TcpListener::from_std(listener)?;
    let mut stop = core.stop.subscribe();
    if *stop.borrow() {
        return Ok(());
    }
    let mut connections = JoinSet::new();
    let outcome = async {
        loop {
            tokio::select! {
                _ = stop.changed() => break Ok::<_, anyhow::Error>(()),
                _ = capture_expiry(core.clone()) => {},
                joined = connections.join_next(), if !connections.is_empty() => {
                    core.joined_connections.fetch_add(1, Ordering::AcqRel);
                    if let Some(result) = joined {
                        match result {Ok(Ok(())) => {}, Ok(Err(error)) => core.failure(&format!("root connection: {error:#}")),
                            Err(error) => core.failure(&format!("root connection join: {error}"))}
                    }
                }
                accepted = listener.accept() => {
                    let (stream, _) = accepted.context("accept root fault actor")?;
                    let Ok(position) = core.connection_positions.clone().try_acquire_owned() else {
                        core.failure("root actor connection positions exhausted"); drop(stream); continue;
                    };
                    let child_core = core.clone();
                    let child_generation = generation.subscribe();
                    connections.spawn(async move {
                        let _position = position;
                        let _count = CountGuard::new(child_core.clone(), true);
                        serve_connection(stream, upstream, backend_index, child_generation, child_core).await
                    });
                }
            }
        }
    }.await;
    // Do not abort connection owners: each must first abort AND JOIN its own
    // descendants. A shared stop signal cancels their accept/hold operations.
    core.stop.send_replace(true);
    while let Some(joined) = connections.join_next().await {
        core.joined_connections.fetch_add(1, Ordering::AcqRel);
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(error)) => core.failure(&format!("root cleanup: {error:#}")),
            Err(error) => core.failure(&format!("root cleanup join: {error}")),
        }
    }
    outcome
}

async fn capture_expiry(core: Arc<Core>) {
    loop {
        let changed = core.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let deadline = core.state.lock().expect("root fault state lock").deadline;
        if let Some(deadline) = deadline {
            tokio::select! {
                _ = &mut changed => {},
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                    let expired = {
                        let mut state = core.state.lock().expect("root fault state lock");
                        if state.deadline == Some(deadline) && state.claimed == 0 {
                            state.deadline = None;
                            true
                        } else { false }
                    };
                    if expired {
                        core.failure("capture absolute deadline expired without mutated Data1");
                        return;
                    }
                }
            }
        } else {
            changed.await;
        }
    }
}

enum ChildExit {
    Upstream,
    Stream,
}
async fn serve_connection(
    stream: tokio::net::TcpStream,
    upstream: SocketAddr,
    backend_index: usize,
    mut generation: watch::Receiver<u64>,
    core: Arc<Core>,
) -> Result<()> {
    let mut stop = core.stop.subscribe();
    if *stop.borrow() {
        return Ok(());
    }
    let handshake_deadline =
        tokio::time::Instant::now() + Duration::from_millis(core.bounds.handshake_timeout_millis);
    let connect = async {
        let mut server_builder = server::Builder::new();
        server_builder
            .initial_window_size(H2_WINDOW)
            .initial_connection_window_size(H2_CONNECTION_WINDOW)
            .max_frame_size(FRAME_BYTES as u32)
            .max_header_list_size(HEADER_BYTES)
            .max_concurrent_streams(H2_STREAMS)
            .max_pending_accept_reset_streams(H2_RESETS)
            .max_concurrent_reset_streams(H2_RESETS)
            .max_local_error_reset_streams(Some(H2_RESETS))
            .max_send_buffer_size(H2_SEND_BUFFER);
        let downstream = server_builder.handshake::<_, Bytes>(stream).await?;
        let socket = tokio::net::TcpStream::connect(upstream).await?;
        let mut client_builder = client::Builder::new();
        client_builder
            .initial_window_size(H2_WINDOW)
            .initial_connection_window_size(H2_CONNECTION_WINDOW)
            .max_frame_size(FRAME_BYTES as u32)
            .max_header_list_size(HEADER_BYTES)
            .max_concurrent_streams(H2_STREAMS)
            .initial_max_send_streams(H2_STREAMS as usize)
            .max_pending_accept_reset_streams(H2_RESETS)
            .max_concurrent_reset_streams(H2_RESETS)
            .max_local_error_reset_streams(Some(H2_RESETS))
            .max_send_buffer_size(H2_SEND_BUFFER)
            .enable_push(false)
            .header_table_size(4096);
        let (sender, driver) = client_builder.handshake::<_, Bytes>(socket).await?;
        Ok::<_, anyhow::Error>((downstream, sender, driver))
    };
    let (mut downstream, sender, driver) = tokio::select! {
        _ = stop.changed() => return Ok(()),
        _ = generation.changed() => return Ok(()),
        result = tokio::time::timeout_at(handshake_deadline, connect) => result.context("root actor handshake deadline")??,
    };
    let mut children = JoinSet::new();
    children.spawn(async move {
        driver.await.context("root upstream H2 driver")?;
        Ok::<_, anyhow::Error>(ChildExit::Upstream)
    });
    let outcome = async {
        loop {
            tokio::select! {
                _ = stop.changed() => break Ok::<_,anyhow::Error>(()),
                _ = generation.changed() => break Ok(()),
                joined = children.join_next(), if !children.is_empty() => {
                    core.joined_children.fetch_add(1, Ordering::AcqRel);
                    match joined {
                        Some(Ok(Ok(ChildExit::Stream))) => {},
                        Some(Ok(Ok(ChildExit::Upstream))) | None => break Ok(()),
                        Some(Ok(Err(error))) => break Err(error),
                        Some(Err(error)) => break Err(anyhow::anyhow!("root child join: {error}")),
                    }
                }
                accepted = downstream.accept() => {
                    let Some((request,mut response)) = accepted.transpose()? else {break Ok(())};
                    let Ok(position) = core.stream_positions.clone().try_acquire_owned() else {
                        response.send_reset(h2::Reason::REFUSED_STREAM);
                        core.failure("root actor stream positions exhausted"); continue;
                    };
                    let child_core = core.clone();
                    let child_sender = sender.clone();
                    children.spawn(async move {
                        let _position = position;
                        let _count = CountGuard::new(child_core.clone(), false);
                        forward_stream(request, response, child_sender, backend_index, child_core).await?;
                        Ok(ChildExit::Stream)
                    });
                }
            }
        }
    }
    .await;
    // Abort is only harness shutdown/failure cleanup, never a production task
    // terminal. Await every original child JoinHandle before returning.
    children.abort_all();
    while let Some(joined) = children.join_next().await {
        core.joined_children.fetch_add(1, Ordering::AcqRel);
        match joined {
            Ok(Err(error)) => core.failure(&format!("root child cleanup failure: {error:#}")),
            Err(error) if !error.is_cancelled() => {
                core.failure(&format!("root child cleanup join: {error}"))
            }
            Ok(Ok(_)) | Err(_) => {}
        }
    }
    drop(sender);
    drop(downstream);
    outcome
}

// Classification is monotonic within one RPC. A provisional capture remains
// protected even after its SlotExit drops; an unknown identity is never exempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StreamClass {
    Unknown,
    NonTarget,
    Target,
    ProvisionalCapture,
}

fn classify_frozen_read(
    state: &State,
    root: TaskIdentity,
    caller: FrontendProcessId,
) -> StreamClass {
    match (state.frozen_root, state.frozen_backend_index) {
        (Some(frozen), Some(_)) if frozen == (root, caller) => StreamClass::Target,
        (Some(_), Some(_)) => StreamClass::NonTarget,
        _ => StreamClass::Unknown,
    }
}

fn harmless_non_target_cancel(class: StreamClass, error: &anyhow::Error) -> bool {
    class == StreamClass::NonTarget
        && error
            .downcast_ref::<h2::Error>()
            .is_some_and(|error| error.is_reset() && error.reason() == Some(h2::Reason::CANCEL))
}

async fn forward_stream(
    request: Request<RecvStream>,
    mut response: server::SendResponse<Bytes>,
    sender: client::SendRequest<Bytes>,
    backend_index: usize,
    core: Arc<Core>,
) -> Result<()> {
    let mut stream_class = StreamClass::Unknown;
    let normal = if request.method() == http::Method::POST && request.uri().path() == ROOT_PATH {
        core.verifier
            .admit_headers(request.headers())
            .ok()
            .and_then(|caller| {
                if caller.subject() != &core.normal_subject {
                    return None;
                }
                match caller.process_identity() {
                    Some(NativeProcessIdentity::Frontend(id)) => Some(id),
                    _ => None,
                }
            })
    } else {
        None
    };
    let capture = normal.and_then(|caller| {
        let state = core.state.lock().expect("root fault state lock");
        state.deadline.map(|deadline| (caller, deadline))
    });
    let inspect = normal.is_some() && {
        let state = core.state.lock().expect("root fault state lock");
        state.deadline.is_some() || state.frozen_root.is_some()
    };
    let deadline = capture.map(|(_, deadline)| deadline).unwrap_or_else(|| {
        Instant::now() + Duration::from_millis(core.bounds.forward_timeout_millis)
    });
    let work = async {
        let (parts, mut inbound) = request.into_parts();
        let mut sender = sender
            .ready()
            .await
            .context("ready root upstream request")?;
        let saved_request = if inspect {
            require_uncompressed(&parts.headers)?;
            let (body, trailers) = read_bounded(&mut inbound, REQUEST_BYTES, core.clone()).await?;
            ensure!(
                trailers.as_ref().is_none_or(HeaderMap::is_empty),
                "captured root request has trailers"
            );
            let message = single_message(&body)?;
            let wire = wire::FetchRootResultRequest::decode(message)
                .context("captured root request protobuf")?;
            let read = decode_read(&wire, FieldPath::root("root_fault_request"))?;
            {
                let mut state = core.state.lock().expect("root fault state lock");
                if let Some(caller) = normal {
                    stream_class = classify_frozen_read(&state, read.root_task(), caller);
                }
                if state.frozen_root == normal.map(|caller| (read.root_task(), caller)) {
                    state.target_requests += 1;
                    state.target_consumed_positive_requests += u64::from(read.consumed() > 0);
                }
                // Observation never rejects ACKs. A healthy query with another
                // exact TaskIdentity is excluded from these counters.
            }
            Some((body, read))
        } else {
            None
        };
        // FE may start its next exact read as soon as queued NotReady trailers
        // arrive, before the previous worker drops its slot guard. That read
        // waits under the same absolute clock instead of bypassing injection.
        if let (Some((caller, _)), Some((_, read))) = (capture, saved_request.as_ref()) {
            loop {
                let changed = core.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let wait_for_exact_slot = {
                    let state = core.state.lock().expect("root fault state lock");
                    let exact = state.frozen_root == Some((read.root_task(), caller))
                        && state.frozen_backend_index == Some(backend_index);
                    if exact && state.claimed == 0 {
                        ensure!(
                            state.deadline == Some(deadline)
                                && state.first_failure.is_none()
                                && !state.failure_overflow,
                            "exact root phase failed while awaiting its prior RPC slot"
                        );
                    }
                    exact && state.claimed == 0 && (state.pending_capture || state.slot.is_some())
                };
                if !wait_for_exact_slot {
                    break;
                }
                changed.await;
            }
        }
        // A single candidate is published, but only independent controller
        // arm authorizes mutation. Other calls retain bounded normal forwarding.
        let selected_capture = capture.filter(|(caller, _)| {
            let state = core.state.lock().expect("root fault state lock");
            if state.claimed != 0 || state.pending_capture || state.slot.is_some() {
                return false;
            }
            let Some((_, read)) = saved_request.as_ref() else {
                return false;
            };
            match state.frozen_root {
                Some(exact) => {
                    exact == (read.root_task(), *caller)
                        && state.frozen_backend_index == Some(backend_index)
                }
                None => state.target_attempts == 0,
            }
        });
        if selected_capture.is_some() {
            stream_class = StreamClass::ProvisionalCapture;
            let read = &saved_request.as_ref().context("candidate body absent")?.1;
            ensure!(
                read.profile() == RootProfileId::V1
                    && read.kind() == RootOutputKind::ClientRows
                    && read.wanted().map(|n| n.get()) == Some(1)
                    && read.consumed() == 0,
                "capture requires exact ClientRows V1 wanted1 consumed0 request"
            );
        }
        let _capture_guard = if selected_capture.is_some() {
            let mut state = core.state.lock().expect("root fault state lock");
            ensure!(
                !state.pending_capture
                    && state.slot.is_none()
                    && state.claimed == 0
                    && state.deadline == Some(deadline)
                    && Instant::now() < deadline,
                "root capture lost its unique slot or original phase"
            );
            ensure!(
                state.target_attempts < MAX_TARGET_ATTEMPTS,
                "target attempt cap exhausted before forwarding another request"
            );
            state.target_attempts += 1;
            state.pending_capture = true;
            Some(SlotExit(core.clone()))
        } else {
            None
        };
        let (upstream_response, mut upstream_send) =
            sender.send_request(Request::from_parts(parts, ()), false)?;
        let upstream_id = upstream_send.stream_id().as_u32();
        let downstream_id = response.stream_id().as_u32();
        if let Some((caller, _)) = selected_capture {
            let (body, read) = saved_request.context("captured request body is absent")?;
            {
                let mut state = core.state.lock().expect("root fault state lock");
                ensure!(
                    state.pending_capture
                        && state.slot.is_none()
                        && state.claimed == 0
                        && state.deadline == Some(deadline),
                    "root capture has multiple candidates or lost its exact phase"
                );
                let evidence = serde_json::json!({"attempt":state.target_attempts,
                    "backend_index":backend_index,"task":read.root_task().to_string(),
                    "frontend_process":caller.to_string(),"downstream_h2_stream":downstream_id,
                    "upstream_h2_stream":upstream_id,"request_sha256":hash(&body),
                    "wanted":read.wanted().map(|n| n.get()),"consumed":read.consumed()});
                state.target_attempt_requests.push(evidence);
                state.pending_capture = false;
                state.slot = Some(Slot {
                    candidate: RootReplyCandidate {
                        read: read.clone(),
                        backend_index,
                        caller,
                        downstream_stream: downstream_id,
                        upstream_stream: upstream_id,
                        request_sha256: hash(&body),
                    },
                });
            }
            core.changed.notify_waiters();
            send_bytes(&mut upstream_send, body).await?;
            upstream_send.send_data(Bytes::new(), true)?;
            let upstream_response = upstream_response.await?;
            inject_reply(upstream_response, response, read, deadline, core.clone()).await
        } else {
            let request_forward = async {
                if let Some((body, _read)) = saved_request {
                    send_bytes(&mut upstream_send, body).await?;
                    upstream_send.send_data(Bytes::new(), true)?;
                    Ok::<_, anyhow::Error>(())
                } else {
                    copy_body(inbound, upstream_send, core.clone()).await
                }
            };
            tokio::try_join!(request_forward, async {
                let upstream_response = upstream_response.await?;
                let (parts, body) = upstream_response.into_parts();
                let output = response.send_response(Response::from_parts(parts, ()), false)?;
                copy_body(body, output, core.clone()).await
            })?;
            Ok(())
        }
    };
    // A timeout drops the work future before classification. It never becomes
    // a cancellation exemption, and errors from driver/accept never enter here.
    let outcome = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), work)
        .await
        .context("root stream absolute deadline")?;
    match outcome {
        Err(error) if harmless_non_target_cancel(stream_class, &error) => {
            let mut state = core.state.lock().expect("root fault state lock");
            state.non_target_peer_cancels = state.non_target_peer_cancels.saturating_add(1);
            Ok(())
        }
        outcome => outcome,
    }
}

struct SlotExit(Arc<Core>);
impl Drop for SlotExit {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().expect("root fault state lock");
        state.slot = None;
        state.pending_capture = false;
        // A legal NotReady closes only this RPC slot. Its original phase clock
        // and independently frozen target/mutation remain for the next exact read.
        if state.claimed != 0 || !state.failures.is_empty() || state.failure_overflow {
            state.deadline = None;
        }
        self.0.changed.notify_waiters();
    }
}

async fn read_bounded(
    body: &mut RecvStream,
    cap: usize,
    core: Arc<Core>,
) -> Result<(Bytes, Option<HeaderMap>)> {
    let mut buffer = OwnedBuffer::new(core, cap)?;
    while let Some(bytes) = body.data().await {
        let bytes = bytes?;
        ensure!(
            bytes.len() <= FRAME_BYTES,
            "root actor received oversized DATA frame"
        );
        buffer.append(&bytes)?;
        body.flow_control().release_capacity(bytes.len())?;
    }
    let trailers = body.trailers().await?;
    Ok((buffer.into_bytes(), trailers))
}

async fn copy_body(
    mut input: RecvStream,
    mut output: SendStream<Bytes>,
    core: Arc<Core>,
) -> Result<()> {
    while let Some(bytes) = input.data().await {
        let bytes = bytes?;
        ensure!(
            bytes.len() <= FRAME_BYTES,
            "root forwarding received oversized DATA frame"
        );
        let mut copy = OwnedBuffer::new(core.clone(), FRAME_BYTES)?;
        copy.append(&bytes)?;
        input.flow_control().release_capacity(bytes.len())?;
        drop(bytes);
        send_bytes(&mut output, copy.into_bytes()).await?;
    }
    match input.trailers().await? {
        Some(trailers) => output.send_trailers(trailers)?,
        None => output.send_data(Bytes::new(), true)?,
    }
    Ok(())
}

async fn send_bytes(output: &mut SendStream<Bytes>, mut bytes: Bytes) -> Result<()> {
    while !bytes.is_empty() {
        output.reserve_capacity(bytes.len().min(FRAME_BYTES));
        let available = if output.capacity() > 0 {
            output.capacity()
        } else {
            poll_fn(|cx| output.poll_capacity(cx))
                .await
                .context("root output capacity closed")??
        };
        ensure!(available > 0, "root output returned zero capacity");
        let take = bytes.len().min(available).min(FRAME_BYTES);
        output.send_data(bytes.split_to(take), false)?;
    }
    Ok(())
}

fn require_uncompressed(headers: &HeaderMap) -> Result<()> {
    let mut types = headers.get_all(http::header::CONTENT_TYPE).iter();
    let content_type = types
        .next()
        .context("missing gRPC content type")?
        .to_str()?;
    ensure!(
        types.next().is_none()
            && (content_type == "application/grpc" || content_type == "application/grpc+proto"),
        "unsupported or duplicate gRPC content type"
    );
    let mut encodings = headers.get_all("grpc-encoding").iter();
    if let Some(encoding) = encodings.next() {
        ensure!(
            encoding == "identity" && encodings.next().is_none(),
            "unsupported or duplicate gRPC encoding"
        );
    }
    Ok(())
}
fn single_message(bytes: &Bytes) -> Result<Bytes> {
    ensure!(
        bytes.len() >= 5 && bytes[0] == 0,
        "root message prefix is truncated or compressed"
    );
    let length = u32::from_be_bytes(bytes[1..5].try_into()?) as usize;
    ensure!(
        length == bytes.len() - 5,
        "root response is not exactly one complete gRPC message"
    );
    Ok(bytes.slice(5..))
}
fn success_status(headers: &HeaderMap, trailers: Option<&HeaderMap>) -> Result<()> {
    ensure!(
        headers.get_all("grpc-status").iter().next().is_none(),
        "Data response unexpectedly contains header grpc-status"
    );
    let trailers = trailers.context("real Data response is missing final gRPC trailers")?;
    ensure!(
        trailers.get_all("grpc-status").iter().count() == 1
            && trailers
                .get("grpc-status")
                .is_some_and(|status| status == "0"),
        "real root reply needs exactly one successful final gRPC status"
    );
    Ok(())
}

fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn require_canonical_reply(
    wire: &wire::FetchRootResultResponse,
    original_message: &Bytes,
    core: Arc<Core>,
) -> Result<()> {
    let canonical_length = wire.encoded_len();
    ensure!(
        canonical_length
            .checked_add(5)
            .is_some_and(|length| length <= RESPONSE_BYTES),
        "canonical real root protobuf exceeds existing root envelope"
    );
    ensure!(
        canonical_length == original_message.len(),
        "real root protobuf length differs from canonical prost encoding"
    );
    // Byte equality, rather than length alone, establishes this actor's
    // canonical wire precondition. The temporary Vec shares the same credit;
    // OwnedBuffer also charges any excess actual allocation capacity.
    let mut canonical = OwnedBuffer::new(core, canonical_length)?;
    wire.encode(&mut canonical.bytes)
        .context("encode bounded canonical real root protobuf")?;
    ensure!(
        canonical.bytes.len() == canonical_length
            && canonical.bytes.as_slice() == original_message.as_ref(),
        "real root protobuf bytes differ from canonical prost encoding"
    );
    drop(canonical);
    Ok(())
}

async fn inject_reply(
    upstream: Response<RecvStream>,
    mut downstream: server::SendResponse<Bytes>,
    expected: RootResultRead,
    deadline: Instant,
    core: Arc<Core>,
) -> Result<()> {
    let (mut parts, mut body) = upstream.into_parts();
    ensure!(
        parts.status == http::StatusCode::OK,
        "real root response HTTP status is not 200"
    );
    require_uncompressed(&parts.headers)?;
    let (original, trailers) = read_bounded(&mut body, RESPONSE_BYTES, core.clone()).await?;
    success_status(&parts.headers, trailers.as_ref())?;
    let original_message = single_message(&original)?;
    let mut wire = wire::FetchRootResultResponse::decode(original_message.clone())
        .context("real root reply protobuf")?;
    require_canonical_reply(&wire, &original_message, core.clone())?;
    drop(original_message);
    ensure!(
        parts
            .headers
            .get_all(http::header::CONTENT_LENGTH)
            .iter()
            .count()
            <= 1,
        "real root response has duplicate content-length"
    );
    if let Some(length) = parts.headers.get(http::header::CONTENT_LENGTH) {
        ensure!(
            length.to_str()?.parse::<usize>()? == original.len(),
            "real root content-length differs from body"
        );
    }
    let reply = decode_reply(
        wire.clone(),
        &expected,
        expected.consumed(),
        FieldPath::root("root_fault_original"),
    )?;
    let is_not_ready = matches!(
        wire.outcome,
        Some(wire::fetch_root_result_response::Outcome::NotReady(true))
    );
    ensure!(
        is_not_ready
            || matches!(
                wire.outcome,
                Some(wire::fetch_root_result_response::Outcome::Data(_))
            ),
        "one-shot root phase requires a legal NotReady or real Data1 reply"
    );
    if is_not_ready {
        let mut state = core.state.lock().expect("root fault state lock");
        ensure!(
            state.not_ready_observed < MAX_TARGET_ATTEMPTS,
            "target NotReady observation cap exhausted"
        );
        state.not_ready_observed += 1;
        let evidence = serde_json::json!({"attempt":state.target_attempts,
            "task":reply.root_task.to_string(),"profile":wire.profile_id,
            "kind":"ClientRows","accepted_consumed":wire.accepted_consumed_sequence,
            "wire_bytes":original.len(),"wire_sha256":hash(&original)});
        state.not_ready_replies.push(evidence);
    }
    let mutation = loop {
        let changed = core.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        {
            let state = core.state.lock().expect("root fault state lock");
            ensure!(
                state.deadline == Some(deadline)
                    && state.failures.is_empty()
                    && !state.failure_overflow,
                "root fault target was disarmed or failed"
            );
            if let Some(mutation) = state.armed_mutation {
                break mutation;
            }
        }
        changed.await;
    };
    if is_not_ready {
        drop(wire);
        drop(reply);
        drop(body);
        let mut send = downstream.send_response(Response::from_parts(parts, ()), false)?;
        send_bytes(&mut send, original).await?;
        send.send_trailers(trailers.context("verified NotReady trailers disappeared")?)?;
        core.state
            .lock()
            .expect("root fault state lock")
            .not_ready_forwarded += 1;
        // No claim, mutation, deadline renewal or fabricated Data on NotReady.
        return Ok(());
    }
    let original_data = match wire.outcome.as_ref() {
        Some(wire::fetch_root_result_response::Outcome::Data(data)) => data.clone(),
        _ => bail!("verified Data1 outcome unexpectedly disappeared"),
    };
    // Validate the untouched first body with the same carrier-neutral grammar
    // used by the real FE relay; a preexisting malformed reply is never a fixture.
    let profile = ClientRowProfile::try_new(
        RootProfileV1::SEGMENT_BYTES,
        RootProfileV1::ROW_PAYLOAD_BYTES,
    )?;
    let after = ClientRowStreamCursor::new()
        .validate_body(profile, &original_data.body)?
        .after();
    if let Some(end) = original_data.end_after_data.as_ref() {
        after.validate_end()?;
        ensure!(
            after.completed_rows() == end.output_rows,
            "real initial Data End row count differs from validated ClientRows body"
        );
    }
    let before = serde_json::json!({"task":reply.root_task.to_string(),"profile":wire.profile_id,
        "kind":"ClientRows","accepted_consumed":wire.accepted_consumed_sequence,
        "sequence":original_data.sequence,"body_bytes":original_data.body.len(),
        "body_sha256":hash(&original_data.body),"wire_bytes":original.len(),"wire_sha256":hash(&original),
        "end_after_data":original_data.end_after_data.as_ref().map(|end|
            serde_json::json!({"sequence":end.sequence,"output_rows":end.output_rows}))});
    match mutation {
        RootReplyMutation::ProfileTwo => wire.profile_id = 2,
        RootReplyMutation::ClientRowsFalse => {
            wire.output_kind = Some(result_wire::RootOutputKind {
                kind: Some(result_wire::root_output_kind::Kind::ClientRows(false)),
            });
        }
        RootReplyMutation::FourBytePrefixOnly => {
            if let Some(wire::fetch_root_result_response::Outcome::Data(data)) =
                wire.outcome.as_mut()
            {
                data.body = Bytes::from_static(&[1, 0, 0, 0]);
            }
        }
    }
    // The mutation list is closed. Identity, watermark, sequence and optional
    // End are preserved by the operations above, not re-created from guesses.
    let length = wire.encoded_len();
    let frame_length = length
        .checked_add(5)
        .context("mutated root message length overflow")?;
    ensure!(
        frame_length <= RESPONSE_BYTES,
        "mutated root exceeds existing root envelope"
    );
    let mut frame = OwnedBuffer::new(core.clone(), frame_length)?;
    frame.append(&[0])?;
    frame.append(&(u32::try_from(length)?).to_be_bytes())?;
    wire.encode(&mut frame.bytes)?;
    ensure!(
        frame.bytes.len() == frame_length,
        "mutated root protobuf length differs from preflight"
    );
    let frame = frame.into_bytes();
    let changed_data = match wire.outcome.as_ref() {
        Some(wire::fetch_root_result_response::Outcome::Data(data)) => data,
        _ => bail!("closed mutation unexpectedly changed real Data outcome"),
    };
    let altered = serde_json::json!({"mutation":mutation,"task":reply.root_task.to_string(),
        "profile":wire.profile_id,"output_kind":format!("{:?}",wire.output_kind),
        "accepted_consumed":wire.accepted_consumed_sequence,
        "sequence":changed_data.sequence,"body_bytes":changed_data.body.len(),
        "body_sha256":hash(&changed_data.body),
        "end_after_data":changed_data.end_after_data.as_ref().map(|end|
            serde_json::json!({"sequence":end.sequence,"output_rows":end.output_rows})),
        "wire_bytes":frame.len(),"wire_sha256":hash(&frame),
        "body_hex":if matches!(mutation,RootReplyMutation::FourBytePrefixOnly) {Some("01000000")} else {None}});
    {
        let mut state = core.state.lock().expect("root fault state lock");
        ensure!(
            state.claimed == 0 && state.deadline == Some(deadline) && Instant::now() < deadline,
            "root fault claim is not exact and within deadline"
        );
        state.claimed = 1;
        state.deadline = None;
        state.original = Some(before);
        state.mutated = Some(altered);
    }
    core.changed.notify_waiters();
    if let Some(length) = parts.headers.get(http::header::CONTENT_LENGTH) {
        let _validated_original_length = length;
        parts.headers.insert(
            http::header::CONTENT_LENGTH,
            http::HeaderValue::from_str(&frame.len().to_string())?,
        );
    }
    drop(wire);
    drop(reply);
    drop(original_data);
    drop(original);
    drop(body);
    let mut send = downstream.send_response(Response::from_parts(parts, ()), false)?;
    send_bytes(&mut send, frame).await?;
    match trailers {
        Some(trailers) => send.send_trailers(trailers)?,
        None => send.send_data(Bytes::new(), true)?,
    }
    // Queueing a complete message is observable, not proof that FE accepted it
    // or that the original H2 DATA aliases physically exited.
    core.state.lock().expect("root fault state lock").emitted += 1;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_execution_contract::root_result::{RootReadOutcome, RootResultReply};
    use novarocks_native_trust::{
        DeploymentId, NativeTransportMode, NativeTrust, ValidatedSharedSecret,
    };
    use novarocks_secret::SecretValue;
    use novarocks_task_codec::root_result::encode_reply;
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use std::num::NonZeroU64;

    fn core() -> Arc<Core> {
        let bounds = RootReplyFaultBounds::default();
        let subject = NativeCallerSubject::parse("fe@127.0.0.1:9080").unwrap();
        let trust = NativeTrust::new(
            DeploymentId::parse("root-actor-component").unwrap(),
            ValidatedSharedSecret::new(SecretValue::new("0123456789abcdef0123456789abcdef"))
                .unwrap(),
            subject.clone(),
            NativeTransportMode::Disabled,
        );
        let (stop, _) = watch::channel(false);
        Arc::new(Core {
            connection_positions: Arc::new(Semaphore::new(bounds.maximum_connections)),
            stream_positions: Arc::new(Semaphore::new(bounds.maximum_active_streams)),
            bounds,
            verifier: trust.server_admission(),
            normal_subject: subject,
            state: Mutex::new(State::default()),
            changed: Notify::new(),
            stop,
            listeners: AtomicUsize::new(0),
            joined_listeners: AtomicU64::new(0),
            connections: AtomicUsize::new(0),
            streams: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
            peak_bytes: AtomicUsize::new(0),
            joined_connections: AtomicU64::new(0),
            joined_children: AtomicU64::new(0),
        })
    }

    #[test]
    fn same_length_reordered_protobuf_decodes_but_fails_canonical_bytes() {
        let core = core();
        let canonical = Bytes::from_static(&[0x10, 1, 0x20, 1]);
        let reordered = Bytes::from_static(&[0x20, 1, 0x10, 1]);
        let expected = wire::FetchRootResultResponse::decode(canonical.clone()).unwrap();
        let actual = wire::FetchRootResultResponse::decode(reordered.clone()).unwrap();
        assert_eq!(actual, expected);
        assert_eq!(actual.encoded_len(), reordered.len());
        require_canonical_reply(&expected, &canonical, core.clone()).unwrap();
        assert!(require_canonical_reply(&actual, &reordered, core.clone()).is_err());
        assert_eq!(core.bytes.load(Ordering::Acquire), 0);
    }

    #[test]
    fn credited_vec_refuses_global_cap_and_preserves_live_alias_credit() {
        let core = core();
        assert!(
            OwnedBuffer::new(core.clone(), core.bounds.maximum_owned_buffer_bytes + 1).is_err()
        );
        assert_eq!(core.bytes.load(Ordering::Acquire), 0);
        let mut owner = OwnedBuffer::new(core.clone(), 8).unwrap();
        owner.append(b"12345678").unwrap();
        let capacity = owner.bytes.capacity();
        let body = owner.into_bytes();
        let alias = body.clone();
        let slice = body.slice(2..6);
        assert_eq!(core.bytes.load(Ordering::Acquire), capacity);
        drop(body);
        drop(alias);
        assert_eq!(core.bytes.load(Ordering::Acquire), capacity);
        assert_eq!(slice.as_ref(), b"3456");
        let remaining = core.bounds.maximum_owned_buffer_bytes - capacity;
        let credit = ByteCredit::new(core.clone(), remaining).unwrap();
        assert!(OwnedBuffer::new(core.clone(), 1).is_err());
        drop(credit);
        assert_eq!(core.bytes.load(Ordering::Acquire), capacity);
        drop(slice);
        assert_eq!(core.bytes.load(Ordering::Acquire), 0);
    }

    #[test]
    fn canonical_validation_cannot_bypass_shared_credit() {
        let core = core();
        let message = Bytes::from_static(&[0x10, 1, 0x20, 1]);
        let wire = wire::FetchRootResultResponse::decode(message.clone()).unwrap();
        let credit =
            ByteCredit::new(core.clone(), core.bounds.maximum_owned_buffer_bytes - 3).unwrap();
        assert!(require_canonical_reply(&wire, &message, core.clone()).is_err());
        assert_eq!(
            core.bytes.load(Ordering::Acquire),
            core.bounds.maximum_owned_buffer_bytes - 3
        );
        drop(credit);
        assert_eq!(core.bytes.load(Ordering::Acquire), 0);
    }

    #[test]
    fn full_identity_is_required_for_non_target_classification() {
        let request = expected();
        let caller = FrontendProcessId::new_v7();
        let different = expected();
        let mut state = State::default();
        assert_eq!(
            classify_frozen_read(&state, request.root_task(), caller),
            StreamClass::Unknown
        );
        state.frozen_root = Some((request.root_task(), caller));
        assert_eq!(
            classify_frozen_read(&state, different.root_task(), caller),
            StreamClass::Unknown
        );
        state.frozen_backend_index = Some(1);
        assert_eq!(
            classify_frozen_read(&state, request.root_task(), caller),
            StreamClass::Target
        );
        assert_eq!(
            classify_frozen_read(&state, different.root_task(), caller),
            StreamClass::NonTarget
        );
    }

    #[test]
    fn reason_cancel_without_typed_stream_reset_is_not_exempt() {
        let reason_only = anyhow::Error::from(h2::Error::from(h2::Reason::CANCEL));
        assert!(!harmless_non_target_cancel(
            StreamClass::NonTarget,
            &reason_only
        ));
        let textual = anyhow::anyhow!("stream error: received RST_STREAM CANCEL");
        assert!(!harmless_non_target_cancel(
            StreamClass::NonTarget,
            &textual
        ));
    }

    // Real H2 peer reset, rather than manufacturing a Reason-only error. The
    // bounded duplex transport never starts a service or provider process.
    #[tokio::test]
    async fn typed_cancel_is_exempt_only_for_proven_non_target_stream() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(2), async {
            let (client_io, server_io) = tokio::io::duplex(65536);
            let server = tokio::spawn(async move {
                let mut connection = server::handshake::<_>(server_io).await?;
                let (_request, mut reply) = connection
                    .accept()
                    .await
                    .context("component request absent")??;
                reply.send_reset(h2::Reason::CANCEL);
                let (_request, mut next) = connection
                    .accept()
                    .await
                    .context("next component request absent")??;
                next.send_response(Response::builder().status(200).body(())?, true)?;
                while connection.accept().await.is_some() {}
                Ok::<_, anyhow::Error>(())
            });
            let (mut sender, driver) = client::handshake::<_>(client_io).await?;
            let driver = tokio::spawn(driver);
            let (reply, _send) = sender.send_request(
                Request::builder()
                    .uri("http://localhost/component")
                    .body(())?,
                true,
            )?;
            let reset = reply
                .await
                .expect_err("peer reset unexpectedly returned a response");
            assert!(reset.is_reset());
            assert_eq!(reset.reason(), Some(h2::Reason::CANCEL));
            let error = anyhow::Error::from(reset).context("actual H2 response stream");
            assert!(harmless_non_target_cancel(StreamClass::NonTarget, &error));
            for class in [
                StreamClass::Unknown,
                StreamClass::Target,
                StreamClass::ProvisionalCapture,
            ] {
                assert!(!harmless_non_target_cancel(class, &error));
            }
            let (next_reply, next_send) = sender.send_request(
                Request::builder().uri("http://localhost/next").body(())?,
                true,
            )?;
            assert_eq!(next_reply.await?.status(), http::StatusCode::OK);
            drop(next_send);
            drop(sender);
            drop(_send);
            driver.abort();
            let _ = driver.await;
            server.abort();
            let _ = server.await;
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("component reset deadline")?
    }

    #[test]
    fn slot_exit_preserves_not_ready_phase_and_existing_failure() {
        let core = core();
        let deadline = Instant::now() + Duration::from_secs(1);
        {
            let mut state = core.state.lock().unwrap();
            state.pending_capture = true;
            state.deadline = Some(deadline);
            state.not_ready_forwarded = 1;
        }
        drop(SlotExit(core.clone()));
        let control = RootReplyFaultControl { core: core.clone() };
        assert_eq!(control.observation().claimed, 0);
        assert_eq!(control.observation().emitted_messages, 0);
        assert_eq!(core.state.lock().unwrap().deadline, Some(deadline));
        core.failure("component first failure");
        drop(SlotExit(core.clone()));
        assert_eq!(control.observation().failures, ["component first failure"]);
        assert!(core.state.lock().unwrap().deadline.is_none());
    }

    fn expected() -> RootResultRead {
        let execution =
            QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap();
        RootResultRead::try_new(
            TaskIdentity::new(
                execution,
                StageId::new(1).unwrap(),
                TaskId::new(1).unwrap(),
                BackendProcessId::new_v7(),
            ),
            RootProfileId::V1,
            RootOutputKind::ClientRows,
            NonZeroU64::new(1),
            0,
            Duration::from_millis(100),
        )
        .unwrap()
    }

    fn frame(core: Arc<Core>, wire: &wire::FetchRootResultResponse) -> Result<Bytes> {
        let length = wire.encoded_len();
        let mut frame = OwnedBuffer::new(core, length + 5)?;
        frame.append(&[0])?;
        frame.append(&(length as u32).to_be_bytes())?;
        wire.encode(&mut frame.bytes)?;
        Ok(frame.into_bytes())
    }

    #[tokio::test]
    async fn actual_not_ready_forwarding_preserves_wire_and_does_not_claim() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(2), async {
            let core = core();
            let expected = expected();
            let wire = encode_reply(&RootResultReply {
                root_task: expected.root_task(),
                profile: expected.profile(),
                kind: expected.kind(),
                accepted_consumed: 0,
                outcome: RootReadOutcome::NotReady,
            })?;
            let original = frame(core.clone(), &wire)?;
            let original_bytes = original.clone();
            let original_hash = hash(&original);
            let original_len = original.len();
            let deadline = Instant::now() + Duration::from_secs(1);
            {
                let mut state = core.state.lock().unwrap();
                state.deadline = Some(deadline);
                state.armed_mutation = Some(RootReplyMutation::ProfileTwo);
                state.target_attempts = 1;
            }
            let mut trailers = HeaderMap::new();
            trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
            trailers.insert(
                "x-component-trailer",
                http::HeaderValue::from_static("untouched"),
            );
            let expected_trailers = trailers.clone();
            let upstream_headers = Response::builder()
                .status(200)
                .header("content-type", "application/grpc")
                .header("content-length", original_len)
                .header("x-component-header", "untouched")
                .body(())?;
            let expected_headers = upstream_headers.headers().clone();

            let (up_client_io, up_server_io) = tokio::io::duplex(65536);
            let upstream = tokio::spawn(async move {
                let mut connection = server::handshake::<_>(up_server_io).await?;
                let (_request, mut response) = connection
                    .accept()
                    .await
                    .context("upstream component request absent")??;
                let mut send = response.send_response(upstream_headers, false)?;
                send.send_data(original, false)?;
                send.send_trailers(trailers)?;
                drop(send);
                while connection.accept().await.is_some() {}
                Ok::<_, anyhow::Error>(())
            });
            let (mut up_sender, up_driver) = client::handshake::<_>(up_client_io).await?;
            let up_driver = tokio::spawn(up_driver);
            let (up_reply, up_send) = up_sender.send_request(
                Request::builder().uri("http://localhost/root").body(())?,
                true,
            )?;
            let up_reply = up_reply.await?;

            let (down_client_io, down_server_io) = tokio::io::duplex(65536);
            let (response_tx, response_rx) = tokio::sync::oneshot::channel();
            let downstream = tokio::spawn(async move {
                let mut connection = server::handshake::<_>(down_server_io).await?;
                let (_request, response) = connection
                    .accept()
                    .await
                    .context("downstream component request absent")??;
                response_tx
                    .send(response)
                    .map_err(|_| anyhow::anyhow!("component response owner gone"))?;
                while connection.accept().await.is_some() {}
                Ok::<_, anyhow::Error>(())
            });
            let (mut down_sender, down_driver) = client::handshake::<_>(down_client_io).await?;
            let down_driver = tokio::spawn(down_driver);
            let (down_reply, down_send) = down_sender.send_request(
                Request::builder().uri("http://localhost/root").body(())?,
                true,
            )?;
            let response = response_rx.await?;
            inject_reply(up_reply, response, expected, deadline, core.clone()).await?;
            let response = down_reply.await?;
            assert_eq!(response.status(), http::StatusCode::OK);
            assert_eq!(response.headers(), &expected_headers);
            let mut body = response.into_body();
            let (actual, actual_trailers) =
                read_bounded(&mut body, RESPONSE_BYTES, core.clone()).await?;
            assert_eq!(actual, original_bytes);
            assert_eq!(actual.len(), original_len);
            assert_eq!(hash(&actual), original_hash);
            assert_eq!(actual_trailers.as_ref(), Some(&expected_trailers));
            let observation = RootReplyFaultControl { core: core.clone() }.observation();
            assert_eq!(observation.claimed, 0);
            assert_eq!(observation.emitted_messages, 0);
            assert_eq!(observation.not_ready_observed, 1);
            assert_eq!(observation.not_ready_forwarded, 1);
            assert!(observation.original.is_none() && observation.mutated.is_none());
            assert!(observation.failures.is_empty());
            assert_eq!(core.state.lock().unwrap().deadline, Some(deadline));
            drop(actual);
            drop(original_bytes);
            drop(body);
            drop(up_sender);
            drop(up_send);
            drop(down_sender);
            drop(down_send);
            up_driver.abort();
            down_driver.abort();
            upstream.abort();
            downstream.abort();
            let _ = up_driver.await;
            let _ = down_driver.await;
            let _ = upstream.await;
            let _ = downstream.await;
            assert_eq!(core.bytes.load(Ordering::Acquire), 0);
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("component NotReady deadline")?
    }
    #[tokio::test]
    async fn actor_stop_joins_pending_stream_owners_and_returns_credit() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(3), async {
            let upstream_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let upstream_address = upstream_listener.local_addr()?;
            let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
            let upstream = tokio::spawn(async move {
                let (socket, _) = upstream_listener.accept().await?;
                let mut connection = server::handshake(socket).await?;
                let (request, response) = connection
                    .accept()
                    .await
                    .context("pending component request absent")??;
                accepted_tx
                    .send(())
                    .map_err(|_| anyhow::anyhow!("component waiter gone"))?;
                let _held = (request, response);
                while connection.accept().await.is_some() {}
                Ok::<_, anyhow::Error>(())
            });
            let core = core();
            let mut actor = RootReplyFaultProxy::start(
                upstream_address,
                0,
                None,
                RootReplyFaultBounds::default(),
                core.verifier.clone(),
                core.normal_subject.clone(),
            )?;
            let control = actor.control();
            let socket = tokio::net::TcpStream::connect(actor.address()).await?;
            let (mut sender, driver) = client::handshake(socket).await?;
            let driver = tokio::spawn(driver);
            let (reply, mut send) = sender.send_request(
                Request::builder()
                    .uri("http://localhost/pending")
                    .body(())?,
                false,
            )?;
            send.send_data(Bytes::from_static(b"bounded-live-body"), false)?;
            accepted_rx.await?;
            // The stream cannot complete normally: both request and response
            // are held. stop must join the actual child owners before returning.
            tokio::task::spawn_blocking(move || actor.stop()).await?;
            drop(reply);
            drop(send);
            drop(sender);
            driver.abort();
            let _ = driver.await;
            upstream.abort();
            let _ = upstream.await;
            let observed = control.observation();
            assert!(observed.shutdown_joined);
            assert_eq!(observed.active_listeners, 0);
            assert_eq!(observed.active_connections, 0);
            assert_eq!(observed.active_streams, 0);
            assert_eq!(observed.connection_positions, 0);
            assert_eq!(observed.stream_positions, 0);
            assert_eq!(observed.owned_buffer_bytes, 0);
            assert!(observed.joined_children >= 2);
            assert!(observed.failures.is_empty());
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("component actor join deadline")?
    }
}
