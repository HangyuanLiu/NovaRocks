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

//! Actual public h2/Hyper/Tonic lifecycle gates with scripted frame I/O.
//! Original Worker credit covers only lifecycle Core/observer and its carrier.
//! Scripted IO, ledger, executor/tasks and all other connection storage are
//! separate. Component deadlines do not change Native's frozen two seconds.
//! Hyper's initial yield is a handoff to the outer acquisition owner, which must
//! publish its final verdict before resuming application dispatch.

use bytes::Bytes;
use h2::{ConnectionLifecycle, ConnectionLifecycleObserver};
use hyper::body::{Body, Frame, SizeHint};
use hyper::http::{Response, Uri};
use hyper::rt::Executor;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::task::JoinHandle;
use tonic::transport::{Endpoint, Http2ConnectionConfig};
use tower::Service;

const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
const COMPONENT: Duration = Duration::from_secs(1);
const WATCHDOG: Duration = Duration::from_secs(5);

#[derive(Default)]
struct Ledger {
    initial: AtomicUsize,
    acquired: AtomicUsize,
    retiring: AtomicUsize,
    observer_exit: AtomicUsize,
    owner_exit: AtomicUsize,
    io_exit: AtomicUsize,
    connector_calls: AtomicUsize,
    services: AtomicUsize,
    acquisition_exit: AtomicUsize,
}
struct IoState {
    input: Vec<u8>,
    at: usize,
    output: Vec<u8>,
    reads: usize,
    writes: usize,
    ack_flushed: bool,
    allow_ack_flush: bool,
}
impl IoState {
    fn ack_written(&self) -> bool {
        let mut at = if self.output.starts_with(PREFACE) {
            PREFACE.len()
        } else {
            0
        };
        while self.output.len().saturating_sub(at) >= 9 {
            let head = &self.output[at..at + 9];
            let length =
                usize::from(head[0]) << 16 | usize::from(head[1]) << 8 | usize::from(head[2]);
            if at + 9 + length > self.output.len() {
                break;
            }
            if head[3] == 4 && head[4] == 1 && length == 0 {
                return true;
            }
            at += 9 + length;
        }
        false
    }
}
struct ScriptIo {
    state: Arc<Mutex<IoState>>,
    ledger: Arc<Ledger>,
}
impl AsyncRead for ScriptIo {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut state = self.state.lock().unwrap();
        state.reads += 1;
        if state.at == state.input.len() {
            return Poll::Pending;
        }
        let amount = buf.remaining().min(state.input.len() - state.at);
        let at = state.at;
        buf.put_slice(&state.input[at..at + amount]);
        state.at += amount;
        Poll::Ready(Ok(()))
    }
}
impl AsyncWrite for ScriptIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut state = self.state.lock().unwrap();
        state.writes += 1;
        state.output.extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut state = self.state.lock().unwrap();
        if state.ack_written() {
            if !state.allow_ack_flush {
                return Poll::Pending;
            }
            state.ack_flushed = true;
        }
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
impl Drop for ScriptIo {
    fn drop(&mut self) {
        self.ledger.io_exit.fetch_add(1, Ordering::SeqCst);
    }
}
#[derive(Clone, Copy)]
enum FinalWork {
    Ready,
    Refuse,
    CrossDeadline,
}
struct Observer {
    ledger: Arc<Ledger>,
    io: Arc<Mutex<IoState>>,
    final_work: FinalWork,
}
impl ConnectionLifecycleObserver for Observer {
    fn on_initial_settings_complete(&self) -> io::Result<()> {
        assert!(
            self.io.lock().unwrap().ack_flushed,
            "the real ACK poll_flush must succeed before initial event"
        );
        assert_eq!(self.ledger.io_exit.load(Ordering::SeqCst), 0);
        self.ledger.initial.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn on_acquisition_complete(&self) -> io::Result<()> {
        assert_eq!(self.ledger.initial.load(Ordering::SeqCst), 1);
        assert_eq!(self.ledger.io_exit.load(Ordering::SeqCst), 0);
        self.ledger.acquired.fetch_add(1, Ordering::SeqCst);
        match self.final_work {
            FinalWork::Ready => Ok(()),
            FinalWork::Refuse => Err(io::ErrorKind::PermissionDenied.into()),
            FinalWork::CrossDeadline => {
                // Model finite callback work which returns after the SAME D.
                // D was minted before this callback and its 50ms allowance.
                let started = Instant::now();
                std::thread::sleep(Duration::from_millis(60));
                assert!(started.elapsed() >= Duration::from_millis(50));
                Ok(())
            }
        }
    }
    fn on_retiring(&self) -> io::Result<()> {
        self.ledger.retiring.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
impl Drop for Observer {
    fn drop(&mut self) {
        self.ledger.observer_exit.fetch_add(1, Ordering::SeqCst);
    }
}
struct OwnerExit {
    ledger: Arc<Ledger>,
    credit: Option<ResultWriteCredit>,
}
impl Drop for OwnerExit {
    fn drop(&mut self) {
        assert_eq!(self.ledger.observer_exit.load(Ordering::SeqCst), 1);
        self.ledger.owner_exit.fetch_add(1, Ordering::SeqCst);
        drop(self.credit.take());
    }
}
fn grant_bytes() -> usize {
    ConnectionLifecycle::allocation_capacity_bound::<Observer>().unwrap()
        + Bytes::owner_with_exit_guard_metadata_size::<Bytes, OwnerExit>()
        + Bytes::owner_with_exit_guard_metadata_size::<Bytes, AcquisitionExit>()
}

struct AcquisitionExit {
    ledger: Arc<Ledger>,
    failed: bool,
}

impl Drop for AcquisitionExit {
    fn drop(&mut self) {
        if self.failed {
            assert_eq!(
                self.ledger.io_exit.load(Ordering::SeqCst),
                1,
                "failed acquisition position must wait for actual independent IO exit"
            );
        }
        self.ledger.acquisition_exit.fetch_add(1, Ordering::SeqCst);
    }
}
struct Fixture {
    ledger: Arc<Ledger>,
    io: Arc<Mutex<IoState>>,
    budget: Arc<ResultRetainedBudget>,
    lifecycle: ConnectionLifecycle,
}
impl Fixture {
    fn new(server: bool, input: Vec<u8>, allow_ack_flush: bool) -> Self {
        Self::new_with_final_work(server, input, allow_ack_flush, FinalWork::Ready)
    }
    fn new_with_final_work(
        server: bool,
        input: Vec<u8>,
        allow_ack_flush: bool,
        final_work: FinalWork,
    ) -> Self {
        let ledger = Arc::new(Ledger::default());
        let mut wire = if server { PREFACE.to_vec() } else { Vec::new() };
        wire.extend(input);
        let io = Arc::new(Mutex::new(IoState {
            input: wire,
            at: 0,
            output: Vec::new(),
            reads: 0,
            writes: 0,
            ack_flushed: false,
            allow_ack_flush,
        }));
        let budget = ResultRetainedBudget::new(NonZeroUsize::new(grant_bytes()).unwrap());
        let ResultWriteAdmission::Granted(credit) =
            budget.try_reserve_process(grant_bytes()).unwrap()
        else {
            panic!("pregrant lifecycle backing before construction");
        };
        let owner = Bytes::from_owner_with_exit_guard(
            Bytes::new(),
            OwnerExit {
                ledger: ledger.clone(),
                credit: Some(credit),
            },
        );
        let lifecycle = ConnectionLifecycle::new(
            Observer {
                ledger: ledger.clone(),
                io: io.clone(),
                final_work,
            },
            owner,
        )
        .unwrap();
        Self {
            ledger,
            io,
            budget,
            lifecycle,
        }
    }
    fn socket(&self) -> ScriptIo {
        ScriptIo {
            state: self.io.clone(),
            ledger: self.ledger.clone(),
        }
    }
    fn held(&self) {
        assert!(matches!(
            self.budget.try_reserve_process(grant_bytes()).unwrap(),
            ResultWriteAdmission::Blocked
        ));
    }
    fn no_io(&self) {
        let io = self.io.lock().unwrap();
        assert_eq!((io.reads, io.writes), (0, 0));
    }
    fn finish(self) {
        self.held();
        drop(self.lifecycle);
        assert_eq!(self.ledger.owner_exit.load(Ordering::SeqCst), 1);
        let ResultWriteAdmission::Granted(credit) =
            self.budget.try_reserve_process(grant_bytes()).unwrap()
        else {
            panic!("original funding must return after actual owners exit");
        };
        drop(credit);
    }
}
fn frame(kind: u8, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
    let mut wire = Vec::new();
    wire.extend_from_slice(&(payload.len() as u32).to_be_bytes()[1..]);
    wire.extend_from_slice(&[kind, flags]);
    wire.extend_from_slice(&stream.to_be_bytes());
    wire.extend_from_slice(payload);
    wire
}
fn settings() -> Vec<u8> {
    frame(4, 0, 0, &[0, 3, 0, 0, 0, 7])
}
fn request() -> Vec<u8> {
    let mut block = vec![0x82, 0x86, 0x84, 0x01, 12];
    block.extend_from_slice(b"example.test");
    frame(1, 5, 1, &block)
}
fn cx() -> Context<'static> {
    Context::from_waker(Waker::noop())
}
#[derive(Default)]
struct Empty;
impl Body for Empty {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        Poll::Ready(None)
    }
    fn is_end_stream(&self) -> bool {
        true
    }
    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(0)
    }
}
#[derive(Clone, Default)]
struct JoinedExecutor(Arc<Mutex<Vec<JoinHandle<()>>>>);
impl<F> Executor<F> for JoinedExecutor
where
    F: Future + Send + 'static,
    F::Output: Send,
{
    fn execute(&self, future: F) {
        self.0.lock().unwrap().push(tokio::spawn(async move {
            let _ = future.await;
        }));
    }
}
impl JoinedExecutor {
    async fn join_naturally(&self) {
        let mut joined = 0;
        loop {
            let tasks = std::mem::take(&mut *self.0.lock().unwrap());
            if tasks.is_empty() {
                break;
            }
            for task in tasks {
                tokio::time::timeout(WATCHDOG, task)
                    .await
                    .expect("failed acquisition's independently spawned task must naturally exit")
                    .expect("natural task exit must not require abort or panic");
                joined += 1;
            }
        }
        assert!(
            joined > 0,
            "the real Hyper ConnTask must have been observed"
        );
    }
    async fn stop(&self) {
        loop {
            let tasks = std::mem::take(&mut *self.0.lock().unwrap());
            if tasks.is_empty() {
                break;
            }
            for task in &tasks {
                task.abort();
            }
            for task in tasks {
                let _ = tokio::time::timeout(WATCHDOG, task).await.unwrap();
            }
        }
    }
}

#[tokio::test]
async fn lifecycle_without_deadline_refuses_h2_and_hyper_before_first_io() {
    for server in [false, true] {
        let fixture = Fixture::new(server, settings(), true);
        if server {
            let mut builder = h2::server::Builder::new();
            builder.connection_lifecycle(fixture.lifecycle.clone());
            assert!(
                builder
                    .handshake::<_, Bytes>(fixture.socket())
                    .await
                    .is_err()
            );
        } else {
            let mut builder = h2::client::Builder::new();
            builder.connection_lifecycle(fixture.lifecycle.clone());
            assert!(
                builder
                    .handshake::<_, Bytes>(fixture.socket())
                    .await
                    .is_err()
            );
        }
        fixture.no_io();
        assert_eq!(fixture.ledger.io_exit.load(Ordering::SeqCst), 1);
        fixture.finish();
    }
    for server in [false, true] {
        let fixture = Fixture::new(server, settings(), true);
        let executor = JoinedExecutor::default();
        if server {
            let ledger = fixture.ledger.clone();
            let service = service_fn(move |_| {
                ledger.services.fetch_add(1, Ordering::SeqCst);
                async { Ok::<_, Infallible>(Response::new(Empty)) }
            });
            let mut builder = hyper::server::conn::http2::Builder::new(executor.clone());
            builder.connection_lifecycle(fixture.lifecycle.clone());
            assert!(
                builder
                    .serve_connection(TokioIo::new(fixture.socket()), service)
                    .await
                    .is_err()
            );
        } else {
            let mut builder = hyper::client::conn::http2::Builder::new(executor.clone());
            builder.connection_lifecycle(fixture.lifecycle.clone());
            assert!(
                builder
                    .handshake::<_, Empty>(TokioIo::new(fixture.socket()))
                    .await
                    .is_err()
            );
        }
        fixture.no_io();
        assert_eq!(fixture.ledger.services.load(Ordering::SeqCst), 0);
        executor.stop().await;
        fixture.finish();
    }
}

#[tokio::test]
async fn same_lifecycle_refuses_second_client_or_server_before_io() {
    let fixture = Fixture::new(false, settings(), true);
    let mut first = h2::client::Builder::new();
    first
        .connection_lifecycle(fixture.lifecycle.clone())
        .initial_settings_deadline(Instant::now() + COMPONENT);
    let (sender, connection) = first.handshake::<_, Bytes>(fixture.socket()).await.unwrap();
    for server in [false, true] {
        let state = Arc::new(Mutex::new(IoState {
            input: PREFACE.to_vec(),
            at: 0,
            output: Vec::new(),
            reads: 0,
            writes: 0,
            ack_flushed: false,
            allow_ack_flush: true,
        }));
        let io = ScriptIo {
            state: state.clone(),
            ledger: fixture.ledger.clone(),
        };
        if server {
            let mut builder = h2::server::Builder::new();
            builder
                .connection_lifecycle(fixture.lifecycle.clone())
                .initial_settings_deadline(Instant::now() + COMPONENT);
            assert!(builder.handshake::<_, Bytes>(io).await.is_err());
        } else {
            let mut builder = h2::client::Builder::new();
            builder
                .connection_lifecycle(fixture.lifecycle.clone())
                .initial_settings_deadline(Instant::now() + COMPONENT);
            assert!(builder.handshake::<_, Bytes>(io).await.is_err());
        }
        let state = state.lock().unwrap();
        assert_eq!((state.reads, state.writes), (0, 0));
    }
    drop(sender);
    drop(connection);
    drop(first);
    assert_eq!(fixture.ledger.retiring.load(Ordering::SeqCst), 1);
    fixture.finish();
}

#[tokio::test]
async fn real_h2_initial_event_waits_for_ack_flush_and_final_event_is_separate() {
    for server in [false, true] {
        let fixture = Fixture::new(server, settings(), false);
        if server {
            let mut builder = h2::server::Builder::new();
            builder
                .connection_lifecycle(fixture.lifecycle.clone())
                .initial_settings_deadline(Instant::now() + COMPONENT);
            let mut connection = builder
                .handshake::<_, Bytes>(fixture.socket())
                .await
                .unwrap();
            assert!(connection.poll_initial_settings(&mut cx()).is_pending());
            assert_eq!(fixture.ledger.initial.load(Ordering::SeqCst), 0);
            fixture.io.lock().unwrap().allow_ack_flush = true;
            assert!(matches!(
                connection.poll_initial_settings(&mut cx()),
                Poll::Ready(Ok(()))
            ));
            assert_eq!(fixture.ledger.initial.load(Ordering::SeqCst), 1);
            assert_eq!(fixture.ledger.acquired.load(Ordering::SeqCst), 0);
            fixture.lifecycle.on_acquisition_complete().unwrap();
            drop(connection);
        } else {
            let mut builder = h2::client::Builder::new();
            builder
                .connection_lifecycle(fixture.lifecycle.clone())
                .initial_settings_deadline(Instant::now() + COMPONENT);
            let (sender, mut connection) = builder
                .handshake::<_, Bytes>(fixture.socket())
                .await
                .unwrap();
            assert!(connection.poll_initial_settings(&mut cx()).is_pending());
            assert_eq!(fixture.ledger.initial.load(Ordering::SeqCst), 0);
            fixture.io.lock().unwrap().allow_ack_flush = true;
            assert!(matches!(
                connection.poll_initial_settings(&mut cx()),
                Poll::Ready(Ok(()))
            ));
            assert_eq!(
                connection.max_concurrent_send_streams(),
                7,
                "real peer SETTINGS must have applied"
            );
            assert_eq!(fixture.ledger.initial.load(Ordering::SeqCst), 1);
            assert_eq!(fixture.ledger.acquired.load(Ordering::SeqCst), 0);
            fixture.lifecycle.on_acquisition_complete().unwrap();
            drop(sender);
            drop(connection);
        }
        assert_eq!(fixture.ledger.acquired.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.ledger.retiring.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.ledger.io_exit.load(Ordering::SeqCst), 1);
        fixture.finish();
    }
}

#[tokio::test]
async fn hyper_server_yields_after_initial_before_ready_headers_dispatch() {
    let mut wire = settings();
    wire.extend(request());
    let fixture = Fixture::new(true, wire, true);
    let executor = JoinedExecutor::default();
    let ledger = fixture.ledger.clone();
    let service = service_fn(move |_| {
        assert_eq!(
            ledger.acquired.load(Ordering::SeqCst),
            1,
            "outer final verdict must precede service"
        );
        ledger.services.fetch_add(1, Ordering::SeqCst);
        async { Ok::<_, Infallible>(Response::new(Empty)) }
    });
    let mut builder = hyper::server::conn::http2::Builder::new(executor.clone());
    builder
        .connection_lifecycle(fixture.lifecycle.clone())
        .initial_settings_deadline(Instant::now() + COMPONENT);
    let mut connection =
        Box::pin(builder.serve_connection(TokioIo::new(fixture.socket()), service));
    assert!(connection.as_mut().poll(&mut cx()).is_pending());
    assert!(connection.initial_settings_complete());
    assert_eq!(fixture.ledger.initial.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.ledger.services.load(Ordering::SeqCst), 0);
    fixture.lifecycle.on_acquisition_complete().unwrap();
    assert!(connection.as_mut().poll(&mut cx()).is_pending());
    assert_eq!(fixture.ledger.services.load(Ordering::SeqCst), 1);
    drop(connection);
    drop(builder);
    executor.stop().await;
    assert_eq!(fixture.ledger.io_exit.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.ledger.retiring.load(Ordering::SeqCst), 1);
    fixture.finish();
}

#[tokio::test]
async fn hyper_client_handshake_reports_initial_but_does_not_publish_final() {
    let fixture = Fixture::new(false, settings(), true);
    let executor = JoinedExecutor::default();
    let mut builder = hyper::client::conn::http2::Builder::new(executor.clone());
    builder
        .connection_lifecycle(fixture.lifecycle.clone())
        .initial_settings_deadline(Instant::now() + COMPONENT);
    let (sender, connection) = tokio::time::timeout(
        WATCHDOG,
        builder.handshake::<_, Empty>(TokioIo::new(fixture.socket())),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(fixture.ledger.initial.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.ledger.acquired.load(Ordering::SeqCst), 0);
    fixture.lifecycle.on_acquisition_complete().unwrap();
    drop(sender);
    drop(connection);
    drop(builder);
    executor.stop().await;
    assert_eq!(fixture.ledger.io_exit.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.ledger.retiring.load(Ordering::SeqCst), 1);
    fixture.finish();
}

#[tokio::test]
async fn real_h2_goaway_retires_once_while_public_handle_still_holds_original_grant() {
    let fixture = Fixture::new(false, settings(), true);
    let mut builder = h2::client::Builder::new();
    builder
        .connection_lifecycle(fixture.lifecycle.clone())
        .initial_settings_deadline(Instant::now() + COMPONENT);
    let (sender, mut connection) = builder
        .handshake::<_, Bytes>(fixture.socket())
        .await
        .unwrap();
    assert!(matches!(
        connection.poll_initial_settings(&mut cx()),
        Poll::Ready(Ok(()))
    ));
    fixture.lifecycle.on_acquisition_complete().unwrap();
    fixture
        .io
        .lock()
        .unwrap()
        .input
        .extend(frame(7, 0, 0, &[0, 0, 0, 0, 0, 0, 0, 1]));
    assert!(matches!(
        Pin::new(&mut connection).poll(&mut cx()),
        Poll::Ready(Err(_))
    ));
    assert_eq!(fixture.ledger.retiring.load(Ordering::SeqCst), 1);
    fixture.held();
    assert_eq!(
        fixture.ledger.io_exit.load(Ordering::SeqCst),
        0,
        "a Ready verdict is not physical IO exit"
    );
    drop(sender);
    drop(connection);
    drop(builder);
    assert_eq!(fixture.ledger.io_exit.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.ledger.retiring.load(Ordering::SeqCst), 1);
    fixture.finish();
}

struct Connector {
    socket: Option<ScriptIo>,
    ledger: Arc<Ledger>,
}
impl Service<Uri> for Connector {
    type Response = TokioIo<ScriptIo>;
    type Error = io::Error;
    type Future = std::future::Ready<io::Result<Self::Response>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, _: Uri) -> Self::Future {
        self.ledger.connector_calls.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok(TokioIo::new(self.socket.take().unwrap())))
    }
}
#[tokio::test]
async fn tonic_final_verdict_follows_real_initial_phase_and_missing_timeout_never_dials() {
    for configured in [false, true] {
        let fixture = Fixture::new(false, settings(), true);
        let executor = JoinedExecutor::default();
        let lifecycle = fixture.lifecycle.clone();
        let endpoint = Endpoint::from_static("http://example.test")
            .executor(executor.clone())
            .http2_connection_factory(move || {
                Ok::<_, io::Error>(Http2ConnectionConfig {
                    connection_lifecycle: Some(lifecycle.clone()),
                    initial_settings_timeout: configured.then_some(COMPONENT),
                    ..Default::default()
                })
            });
        let connector = Connector {
            socket: Some(fixture.socket()),
            ledger: fixture.ledger.clone(),
        };
        let result = tokio::time::timeout(WATCHDOG, endpoint.connect_with_connector(connector))
            .await
            .unwrap();
        if configured {
            let channel = result.unwrap();
            assert_eq!(fixture.ledger.initial.load(Ordering::SeqCst), 1);
            assert_eq!(fixture.ledger.acquired.load(Ordering::SeqCst), 1);
            assert_eq!(fixture.ledger.connector_calls.load(Ordering::SeqCst), 1);
            assert_eq!(fixture.ledger.io_exit.load(Ordering::SeqCst), 0);
            fixture.held();
            drop(channel);
        } else {
            assert!(result.is_err());
            assert_eq!(fixture.ledger.connector_calls.load(Ordering::SeqCst), 0);
            assert_eq!(fixture.ledger.initial.load(Ordering::SeqCst), 0);
            assert_eq!(fixture.ledger.acquired.load(Ordering::SeqCst), 0);
            fixture.no_io();
            assert_eq!(fixture.ledger.retiring.load(Ordering::SeqCst), 1);
            assert!(fixture.lifecycle.bind().is_err());
        }
        drop(endpoint);
        executor.stop().await;
        assert_eq!(fixture.ledger.io_exit.load(Ordering::SeqCst), 1);
        if configured {
            assert_eq!(fixture.ledger.retiring.load(Ordering::SeqCst), 1);
        }
        fixture.finish();
    }
}

#[tokio::test]
async fn canceled_tonic_initial_phase_permanently_retires_even_with_factory_alias() {
    let fixture = Fixture::new(false, Vec::new(), true);
    let executor = JoinedExecutor::default();
    let lifecycle = fixture.lifecycle.clone();
    let endpoint = Endpoint::from_static("http://example.test")
        .executor(executor.clone())
        .http2_connection_factory(move || {
            Ok::<_, io::Error>(Http2ConnectionConfig {
                connection_lifecycle: Some(lifecycle.clone()),
                initial_settings_timeout: Some(COMPONENT),
                ..Default::default()
            })
        });
    let connector = Connector {
        socket: Some(fixture.socket()),
        ledger: fixture.ledger.clone(),
    };
    let mut attempt = Box::pin(endpoint.connect_with_connector(connector));
    assert!(attempt.as_mut().poll(&mut cx()).is_pending());
    assert_eq!(fixture.ledger.connector_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.ledger.initial.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.ledger.retiring.load(Ordering::SeqCst), 0);
    drop(attempt);
    assert_eq!(fixture.ledger.io_exit.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.ledger.retiring.load(Ordering::SeqCst), 1);
    assert!(fixture.lifecycle.bind().is_err());
    assert!(fixture.lifecycle.on_acquisition_complete().is_err());
    fixture.held();
    drop(endpoint);
    executor.stop().await;
    fixture.finish();
}

#[tokio::test]
async fn tonic_final_callback_refusal_exits_actual_io_but_public_alias_keeps_original_grant() {
    tonic_failed_final_work(
        FinalWork::Refuse,
        Duration::from_secs(1),
        io::ErrorKind::PermissionDenied,
    )
    .await;
}

#[tokio::test]
async fn tonic_final_callback_crossing_same_deadline_cannot_publish_channel() {
    tonic_failed_final_work(
        FinalWork::CrossDeadline,
        Duration::from_millis(50),
        io::ErrorKind::TimedOut,
    )
    .await;
}

async fn tonic_failed_final_work(work: FinalWork, allowance: Duration, expected: io::ErrorKind) {
    let fixture = Fixture::new_with_final_work(false, settings(), true, work);
    let executor = JoinedExecutor::default();
    let lifecycle = fixture.lifecycle.clone();
    let endpoint = Endpoint::from_static("http://example.test")
        .executor(executor.clone())
        .http2_connection_factory(move || {
            Ok::<_, io::Error>(Http2ConnectionConfig {
                connection_lifecycle: Some(lifecycle.clone()),
                initial_settings_timeout: Some(allowance),
                ..Default::default()
            })
        });
    let connector = Connector {
        socket: Some(fixture.socket()),
        ledger: fixture.ledger.clone(),
    };
    let outcome = tokio::time::timeout(WATCHDOG, endpoint.connect_with_connector(connector))
        .await
        .unwrap();
    let error = match outcome {
        Err(error) => error,
        Ok(_) => panic!("failed final callback must not publish a Channel"),
    };
    let mut cause: &(dyn std::error::Error + 'static) = &error;
    loop {
        if let Some(error) = cause.downcast_ref::<io::Error>() {
            assert_eq!(error.kind(), expected);
            break;
        }
        cause = cause
            .source()
            .expect("final acquisition error must preserve its actual IO error");
    }
    assert_eq!(fixture.ledger.initial.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.ledger.acquired.load(Ordering::SeqCst),
        1,
        "this is an executed final callback, not an earlier timeout"
    );
    assert_eq!(fixture.ledger.connector_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.ledger.retiring.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.ledger.owner_exit.load(Ordering::SeqCst), 0);
    assert!(fixture.lifecycle.bind().is_err());
    assert!(fixture.lifecycle.on_acquisition_complete().is_err());
    fixture.held();
    drop(endpoint);
    executor.join_naturally().await;
    assert_eq!(fixture.ledger.io_exit.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.ledger.owner_exit.load(Ordering::SeqCst), 0);
    fixture.held();
    fixture.finish();
}

#[tokio::test]
async fn actual_tonic_acquisition_position_waits_for_independent_failed_io_exit() {
    for final_work in [
        FinalWork::Ready,
        FinalWork::Refuse,
        FinalWork::CrossDeadline,
    ] {
        let failed = !matches!(final_work, FinalWork::Ready);
        let fixture = Fixture::new_with_final_work(false, settings(), true, final_work);
        let executor = JoinedExecutor::default();
        let lifecycle = fixture.lifecycle.clone();
        let acquisition = Mutex::new(Some(Bytes::from_owner_with_exit_guard(
            Bytes::new(),
            AcquisitionExit {
                ledger: fixture.ledger.clone(),
                failed,
            },
        )));
        let allowance = if matches!(final_work, FinalWork::CrossDeadline) {
            Duration::from_millis(50)
        } else {
            COMPONENT
        };
        let endpoint = Endpoint::from_static("http://example.test")
            .executor(executor.clone())
            .http2_connection_factory(move || {
                Ok::<_, io::Error>(Http2ConnectionConfig {
                    connection_lifecycle: Some(lifecycle.clone()),
                    acquisition_owner: Some(
                        acquisition
                            .lock()
                            .unwrap()
                            .take()
                            .expect("one actual factory attempt"),
                    ),
                    initial_settings_timeout: Some(allowance),
                    ..Default::default()
                })
            });
        let result = tokio::time::timeout(
            WATCHDOG,
            endpoint.connect_with_connector(Connector {
                socket: Some(fixture.socket()),
                ledger: fixture.ledger.clone(),
            }),
        )
        .await
        .unwrap();
        if failed {
            assert!(result.is_err());
            assert_eq!(fixture.ledger.retiring.load(Ordering::SeqCst), 1);
            assert_eq!(fixture.ledger.acquisition_exit.load(Ordering::SeqCst), 0);
        } else {
            let channel = result.unwrap();
            assert_eq!(fixture.ledger.io_exit.load(Ordering::SeqCst), 0);
            assert_eq!(
                fixture.ledger.acquisition_exit.load(Ordering::SeqCst),
                1,
                "successful final verdict releases acquisition while IO serves"
            );
            drop(channel);
        }
        fixture.held();
        drop(endpoint);
        executor.join_naturally().await;
        assert_eq!(fixture.ledger.io_exit.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.ledger.acquisition_exit.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.ledger.owner_exit.load(Ordering::SeqCst), 0);
        fixture.finish();
    }
}

#[tokio::test]
async fn none_lifecycle_keeps_legacy_preface_only_handshake() {
    let fixture = Fixture::new(false, Vec::new(), true);
    let (sender, connection) = h2::client::Builder::new()
        .handshake::<_, Bytes>(fixture.socket())
        .await
        .unwrap();
    assert_eq!(fixture.ledger.initial.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.ledger.acquired.load(Ordering::SeqCst), 0);
    drop(sender);
    drop(connection);
    let server = Fixture::new(true, Vec::new(), true);
    let connection = h2::server::Builder::new()
        .handshake::<_, Bytes>(server.socket())
        .await
        .unwrap();
    assert_eq!(server.ledger.initial.load(Ordering::SeqCst), 0);
    drop(connection);
    assert_eq!(fixture.ledger.io_exit.load(Ordering::SeqCst), 1);
    assert_eq!(server.ledger.io_exit.load(Ordering::SeqCst), 1);
    fixture.finish();
    server.finish();
}
