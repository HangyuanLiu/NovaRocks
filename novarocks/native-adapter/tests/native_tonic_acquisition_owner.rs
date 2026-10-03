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

//! Actual Endpoint acquisition owner ordering, including cancellation.
//! The real Worker pregrant covers the original Bytes exit-guard carrier only.
//! Connector/IO, task, Hyper/Tonic, timer and test-ledger allocations are separate;
//! the component 200ms allowance does not alter Native's frozen two seconds.

use bytes::Bytes;
use hyper::http::{Request, Uri};
use hyper::rt::Executor;
use hyper_util::rt::TokioIo;
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::future::Future;
use std::io;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};
use tokio::task::JoinHandle;
use tonic::transport::{Endpoint, Http2ConnectionConfig};
use tower::{Service, ServiceExt};

const ALLOWANCE: Duration = Duration::from_millis(200);
const WATCHDOG: Duration = Duration::from_secs(5);
const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

#[derive(Default)]
struct Ledger {
    sequence: AtomicUsize,
    io_exit: AtomicUsize,
    dial_exit: AtomicUsize,
    owner_exit: AtomicUsize,
    calls: AtomicUsize,
    polls: AtomicUsize,
    reads: AtomicUsize,
    writes: AtomicUsize,
}
impl Ledger {
    fn mark(&self, field: &AtomicUsize) {
        field.store(
            self.sequence.fetch_add(1, Ordering::SeqCst) + 1,
            Ordering::SeqCst,
        );
    }
    fn io_before_owner(&self) {
        let io = self.io_exit.load(Ordering::SeqCst);
        let owner = self.owner_exit.load(Ordering::SeqCst);
        assert!(
            io > 0 && owner > io,
            "the actual IO must exit before the acquisition owner: {io}/{owner}"
        );
    }
}
struct OwnerExit {
    credit: Option<ResultWriteCredit>,
    ledger: Arc<Ledger>,
}
impl Drop for OwnerExit {
    fn drop(&mut self) {
        self.ledger.mark(&self.ledger.owner_exit);
        drop(self.credit.take());
    }
}
fn carrier_bytes() -> usize {
    Bytes::owner_with_exit_guard_metadata_size::<Bytes, OwnerExit>()
}
fn budget() -> Arc<ResultRetainedBudget> {
    ResultRetainedBudget::new(NonZeroUsize::new(carrier_bytes()).unwrap())
}
fn owner(budget: &Arc<ResultRetainedBudget>, ledger: Arc<Ledger>) -> Bytes {
    let ResultWriteAdmission::Granted(credit) =
        budget.try_reserve_process(carrier_bytes()).unwrap()
    else {
        panic!("original acquisition carrier must be granted before allocation");
    };
    Bytes::from_owner_with_exit_guard(
        Bytes::new(),
        OwnerExit {
            credit: Some(credit),
            ledger,
        },
    )
}
fn held(budget: &Arc<ResultRetainedBudget>) {
    assert!(matches!(
        budget.try_reserve_process(carrier_bytes()).unwrap(),
        ResultWriteAdmission::Blocked
    ));
}
fn returned(budget: &Arc<ResultRetainedBudget>) {
    let ResultWriteAdmission::Granted(credit) =
        budget.try_reserve_process(carrier_bytes()).unwrap()
    else {
        panic!("actual acquisition exit must return the original credit");
    };
    drop(credit);
}
struct ExitIo {
    inner: Option<DuplexStream>,
    ledger: Arc<Ledger>,
}
impl AsyncRead for ExitIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.ledger.reads.fetch_add(1, Ordering::SeqCst);
        Pin::new(self.inner.as_mut().unwrap()).poll_read(cx, buf)
    }
}
impl AsyncWrite for ExitIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.ledger.writes.fetch_add(1, Ordering::SeqCst);
        Pin::new(self.inner.as_mut().unwrap()).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.inner.as_mut().unwrap()).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.inner.as_mut().unwrap()).poll_shutdown(cx)
    }
}
impl Drop for ExitIo {
    fn drop(&mut self) {
        drop(self.inner.take());
        self.ledger.mark(&self.ledger.io_exit);
    }
}
fn io_pair(ledger: &Arc<Ledger>) -> (ExitIo, DuplexStream) {
    let (inner, peer) = tokio::io::duplex(65536);
    (
        ExitIo {
            inner: Some(inner),
            ledger: ledger.clone(),
        },
        peer,
    )
}
#[derive(Clone, Copy)]
enum Dial {
    Ready,
    Error,
    Pending,
}
struct DialFuture {
    io: Option<ExitIo>,
    ledger: Arc<Ledger>,
    mode: Dial,
}
impl Future for DialFuture {
    type Output = io::Result<TokioIo<ExitIo>>;
    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        self.ledger.polls.fetch_add(1, Ordering::SeqCst);
        match self.mode {
            Dial::Pending => Poll::Pending,
            Dial::Error => {
                drop(self.io.take());
                Poll::Ready(Err(io::ErrorKind::ConnectionRefused.into()))
            }
            Dial::Ready => Poll::Ready(Ok(TokioIo::new(self.io.take().unwrap()))),
        }
    }
}
impl Drop for DialFuture {
    fn drop(&mut self) {
        drop(self.io.take());
        self.ledger.mark(&self.ledger.dial_exit);
    }
}
#[derive(Clone)]
struct Connector {
    input: Arc<Mutex<Option<ExitIo>>>,
    ledger: Arc<Ledger>,
    mode: Dial,
    call_delay: Duration,
}
impl Connector {
    fn new(io: ExitIo, ledger: &Arc<Ledger>, mode: Dial) -> Self {
        Self {
            input: Arc::new(Mutex::new(Some(io))),
            ledger: ledger.clone(),
            mode,
            call_delay: Duration::ZERO,
        }
    }
}
impl Service<Uri> for Connector {
    type Response = TokioIo<ExitIo>;
    type Error = io::Error;
    type Future = DialFuture;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, _: Uri) -> Self::Future {
        self.ledger.calls.fetch_add(1, Ordering::SeqCst);
        let io = self.input.lock().unwrap().take();
        if !self.call_delay.is_zero() {
            std::thread::sleep(self.call_delay);
        }
        DialFuture {
            io,
            ledger: self.ledger.clone(),
            mode: self.mode,
        }
    }
}
#[derive(Clone, Default)]
struct JoinedExecutor(Arc<Mutex<Vec<JoinHandle<()>>>>);
impl<F: Future + Send + 'static> Executor<F> for JoinedExecutor
where
    F::Output: Send,
{
    fn execute(&self, future: F) {
        self.0.lock().unwrap().push(tokio::spawn(async move {
            let _ = future.await;
        }));
    }
}
impl JoinedExecutor {
    async fn stop(&self) {
        loop {
            let tasks = std::mem::take(&mut *self.0.lock().unwrap());
            if tasks.is_empty() {
                return;
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
fn endpoint(
    budget: &Arc<ResultRetainedBudget>,
    ledger: &Arc<Ledger>,
    executor: &JoinedExecutor,
) -> Endpoint {
    let budget = budget.clone();
    let ledger = ledger.clone();
    Endpoint::from_static("http://example.test")
        .executor(executor.clone())
        .http2_connection_factory(move || {
            Ok::<_, io::Error>(Http2ConnectionConfig {
                acquisition_owner: Some(owner(&budget, ledger.clone())),
                initial_settings_timeout: Some(ALLOWANCE),
                ..Default::default()
            })
        })
}
fn frame(kind: u8, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
    let mut wire = Vec::with_capacity(payload.len() + 9);
    wire.extend_from_slice(&(payload.len() as u32).to_be_bytes()[1..]);
    wire.extend_from_slice(&[kind, flags]);
    wire.extend_from_slice(&stream.to_be_bytes());
    wire.extend_from_slice(payload);
    wire
}
async fn read_frame(peer: &mut DuplexStream) -> (u8, u8, u32) {
    tokio::time::timeout(WATCHDOG, async {
        let mut header = [0; 9];
        peer.read_exact(&mut header).await.unwrap();
        let len = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
        let mut payload = vec![0; len];
        peer.read_exact(&mut payload).await.unwrap();
        (
            header[3],
            header[4],
            u32::from_be_bytes(header[5..9].try_into().unwrap()) & 0x7fff_ffff,
        )
    })
    .await
    .unwrap()
}
async fn preface(peer: &mut DuplexStream) {
    let mut bytes = [0; 24];
    tokio::time::timeout(WATCHDOG, peer.read_exact(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&bytes, PREFACE);
    assert_eq!(read_frame(peer).await.0, 4);
}
async fn one_pending_poll<F: Future>(future: Pin<&mut F>) {
    let mut future = future;
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

#[tokio::test]
async fn dial_failure_drops_actual_connector_and_io_before_original_acquisition_owner() {
    let ledger = Arc::new(Ledger::default());
    let budget = budget();
    let executor = JoinedExecutor::default();
    let endpoint = endpoint(&budget, &ledger, &executor);
    let (io, peer) = io_pair(&ledger);
    let result = endpoint
        .connect_with_connector(Connector::new(io, &ledger, Dial::Error))
        .await;
    assert!(result.is_err());
    ledger.io_before_owner();
    assert!(ledger.dial_exit.load(Ordering::SeqCst) < ledger.owner_exit.load(Ordering::SeqCst));
    returned(&budget);
    drop(peer);
    drop(endpoint);
    executor.stop().await;
}

#[tokio::test]
async fn canceled_pending_dial_retires_actual_future_and_io_before_owner() {
    let ledger = Arc::new(Ledger::default());
    let budget = budget();
    let executor = JoinedExecutor::default();
    let endpoint = endpoint(&budget, &ledger, &executor);
    let (io, peer) = io_pair(&ledger);
    let mut pending =
        Box::pin(endpoint.connect_with_connector(Connector::new(io, &ledger, Dial::Pending)));
    one_pending_poll(pending.as_mut()).await;
    assert_eq!(ledger.calls.load(Ordering::SeqCst), 1);
    assert_eq!(ledger.polls.load(Ordering::SeqCst), 1);
    held(&budget);
    drop(pending);
    ledger.io_before_owner();
    assert!(ledger.dial_exit.load(Ordering::SeqCst) < ledger.owner_exit.load(Ordering::SeqCst));
    returned(&budget);
    drop(peer);
    drop(endpoint);
    executor.stop().await;
}

#[tokio::test]
async fn canceled_initial_settings_wait_exits_actual_io_before_owner() {
    let ledger = Arc::new(Ledger::default());
    let budget = budget();
    let executor = JoinedExecutor::default();
    let endpoint = endpoint(&budget, &ledger, &executor);
    let (io, mut peer) = io_pair(&ledger);
    let attempt_ledger = ledger.clone();
    let task = tokio::spawn(async move {
        endpoint
            .connect_with_connector(Connector::new(io, &attempt_ledger, Dial::Ready))
            .await
    });
    preface(&mut peer).await;
    held(&budget);
    task.abort();
    assert!(
        tokio::time::timeout(WATCHDOG, task)
            .await
            .unwrap()
            .unwrap_err()
            .is_cancelled()
    );
    ledger.io_before_owner();
    // The EOF agrees with the actual destructor trace, not just task cancellation.
    assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
    returned(&budget);
    executor.stop().await;
}

#[tokio::test]
async fn initial_settings_timeout_drops_io_before_releasing_acquisition_position() {
    let ledger = Arc::new(Ledger::default());
    let budget = budget();
    let executor = JoinedExecutor::default();
    let endpoint = endpoint(&budget, &ledger, &executor);
    let (io, mut peer) = io_pair(&ledger);
    let attempt_ledger = ledger.clone();
    let task = tokio::spawn(async move {
        endpoint
            .connect_with_connector(Connector::new(io, &attempt_ledger, Dial::Ready))
            .await
    });
    preface(&mut peer).await;
    held(&budget);
    assert!(
        tokio::time::timeout(WATCHDOG, task)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    ledger.io_before_owner();
    returned(&budget);
    assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
    executor.stop().await;
}

#[tokio::test]
async fn successful_applied_settings_and_flush_release_owner_before_live_application_io() {
    let ledger = Arc::new(Ledger::default());
    let budget = budget();
    let executor = JoinedExecutor::default();
    let endpoint = endpoint(&budget, &ledger, &executor);
    let (io, mut peer) = io_pair(&ledger);
    let (peer_ready, ready) = tokio::sync::oneshot::channel();
    let peer_task = tokio::spawn(async move {
        preface(&mut peer).await;
        peer.write_all(&frame(4, 0, 0, &[])).await.unwrap();
        loop {
            let (kind, flags, _) = read_frame(&mut peer).await;
            if kind == 4 && flags & 1 != 0 {
                break;
            }
        }
        peer_ready.send(()).unwrap();
        loop {
            let (kind, _, stream) = read_frame(&mut peer).await;
            if kind == 1 {
                peer.write_all(&frame(1, 5, stream, &[0x88])).await.unwrap();
                return peer;
            }
        }
    });
    let channel = endpoint
        .connect_with_connector(Connector::new(io, &ledger, Dial::Ready))
        .await
        .unwrap();
    tokio::time::timeout(WATCHDOG, ready)
        .await
        .unwrap()
        .unwrap();
    assert!(ledger.owner_exit.load(Ordering::SeqCst) > 0);
    assert_eq!(ledger.io_exit.load(Ordering::SeqCst), 0);
    returned(&budget);
    let request = Request::builder()
        .uri("http://example.test/after-bootstrap")
        .body(tonic::body::empty_body())
        .unwrap();
    let response = tokio::time::timeout(WATCHDOG, channel.clone().oneshot(request))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), 200);
    let peer = peer_task.await.unwrap();
    drop(response);
    drop(channel);
    drop(endpoint);
    executor.stop().await;
    drop(peer);
    assert!(ledger.io_exit.load(Ordering::SeqCst) > ledger.owner_exit.load(Ordering::SeqCst));
    returned(&budget);
}

#[tokio::test]
async fn original_owner_without_positive_phase_or_invalid_config_refuses_before_connector() {
    for case in 0..4 {
        let ledger = Arc::new(Ledger::default());
        let budget = budget();
        let factory_budget = budget.clone();
        let factory_ledger = ledger.clone();
        let executor = JoinedExecutor::default();
        let endpoint = Endpoint::from_static("http://example.test")
            .executor(executor.clone())
            .http2_connection_factory(move || {
                let acquisition_owner = Some(owner(&factory_budget, factory_ledger.clone()));
                // A slow synchronous factory tests the absolute origin. It is not
                // an arbitrary sleep used to infer task or IO exit.
                if case == 3 {
                    std::thread::sleep(ALLOWANCE + Duration::from_millis(50));
                }
                Ok::<_, io::Error>(Http2ConnectionConfig {
                    acquisition_owner,
                    initial_settings_timeout: match case {
                        0 => None,
                        1 => Some(Duration::ZERO),
                        _ => Some(ALLOWANCE),
                    },
                    max_frame_size: (case == 2).then_some(1),
                    ..Default::default()
                })
            });
        // No pre-created IO can contaminate a before-connector exit assertion.
        let connector = tower::service_fn({
            let ledger = ledger.clone();
            move |_: Uri| {
                ledger.calls.fetch_add(1, Ordering::SeqCst);
                async {
                    Err::<TokioIo<DuplexStream>, io::Error>(io::ErrorKind::ConnectionRefused.into())
                }
            }
        });
        assert!(endpoint.connect_with_connector(connector).await.is_err());
        assert_eq!(ledger.calls.load(Ordering::SeqCst), 0);
        assert!(ledger.owner_exit.load(Ordering::SeqCst) > 0);
        returned(&budget);
        drop(endpoint);
        executor.stop().await;
    }
}

#[tokio::test]
async fn expired_first_connector_future_poll_never_reads_or_writes_and_retires_before_owner() {
    let ledger = Arc::new(Ledger::default());
    let budget = budget();
    let executor = JoinedExecutor::default();
    let endpoint = endpoint(&budget, &ledger, &executor);
    let (io, peer) = io_pair(&ledger);
    let mut connector = Connector::new(io, &ledger, Dial::Ready);
    connector.call_delay = ALLOWANCE + Duration::from_millis(50);
    assert!(endpoint.connect_with_connector(connector).await.is_err());
    // call created the queued future before D expired. The first future poll
    // must refuse before any TCP/TLS/IO work; claiming call()==0 would be false.
    assert_eq!(ledger.calls.load(Ordering::SeqCst), 1);
    assert_eq!(ledger.polls.load(Ordering::SeqCst), 0);
    assert_eq!(ledger.reads.load(Ordering::SeqCst), 0);
    assert_eq!(ledger.writes.load(Ordering::SeqCst), 0);
    ledger.io_before_owner();
    assert!(ledger.dial_exit.load(Ordering::SeqCst) < ledger.owner_exit.load(Ordering::SeqCst));
    returned(&budget);
    drop(peer);
    drop(endpoint);
    executor.stop().await;
}

#[test]
fn unpolled_actual_acquisition_drop_retires_io_and_future_before_original_owner() {
    let ledger = Arc::new(Ledger::default());
    let budget = budget();
    let (io, peer) = io_pair(&ledger);
    let acquisition = tonic::transport::ConnectionAcquisition::new(
        DialFuture {
            io: Some(io),
            ledger: ledger.clone(),
            mode: Dial::Pending,
        },
        Some(owner(&budget, ledger.clone())),
    );
    held(&budget);
    drop(acquisition);
    assert_eq!(ledger.calls.load(Ordering::SeqCst), 0);
    assert_eq!(ledger.polls.load(Ordering::SeqCst), 0);
    assert_eq!(ledger.reads.load(Ordering::SeqCst), 0);
    assert_eq!(ledger.writes.load(Ordering::SeqCst), 0);
    ledger.io_before_owner();
    assert!(ledger.io_exit.load(Ordering::SeqCst) < ledger.dial_exit.load(Ordering::SeqCst));
    assert!(ledger.dial_exit.load(Ordering::SeqCst) < ledger.owner_exit.load(Ordering::SeqCst));
    returned(&budget);
    drop(peer);
}

/// Retain the actual IO after producing Ready Err, so the public wrapper's
/// explicit future retirement, rather than cleanup inside poll, is measured.
struct ReadyErrorRetainsIo(DialFuture);
impl Future for ReadyErrorRetainsIo {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.ledger.polls.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(Err(io::ErrorKind::ConnectionRefused.into()))
    }
}

#[tokio::test]
async fn actual_acquisition_ready_error_destroys_retained_io_before_original_owner() {
    let ledger = Arc::new(Ledger::default());
    let budget = budget();
    let (io, peer) = io_pair(&ledger);
    let acquisition = tonic::transport::ConnectionAcquisition::new(
        ReadyErrorRetainsIo(DialFuture {
            io: Some(io),
            ledger: ledger.clone(),
            mode: Dial::Error,
        }),
        Some(owner(&budget, ledger.clone())),
    );
    held(&budget);
    assert!(acquisition.await.is_err());
    assert_eq!(ledger.polls.load(Ordering::SeqCst), 1);
    assert_eq!(ledger.reads.load(Ordering::SeqCst), 0);
    assert_eq!(ledger.writes.load(Ordering::SeqCst), 0);
    ledger.io_before_owner();
    assert!(ledger.io_exit.load(Ordering::SeqCst) < ledger.dial_exit.load(Ordering::SeqCst));
    assert!(ledger.dial_exit.load(Ordering::SeqCst) < ledger.owner_exit.load(Ordering::SeqCst));
    returned(&budget);
    drop(peer);
}
