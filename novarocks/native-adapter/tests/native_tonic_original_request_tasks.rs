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

//! Real Tonic split request tasks, finite pair refusal and original alias exit.
//! One Worker grant covers pool/pairs, two finite attempt token constructions,
//! their actual queried task bounds, concrete Tonic IO boxes and the carrier.
//! Socket registration, channel queue/callback/body backing, executor/runtime,
//! TLS and the complete connection envelope remain independently owned inputs.

use bytes::Bytes;
use hyper::body::{Body, Frame, SizeHint};
use hyper::http::{Request, Response, Uri};
use hyper::rt::Executor;
use hyper_util::rt::TokioIo;
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::alloc::Layout;
use std::error::Error;
use std::future::Future;
use std::io;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::transport::{
    Endpoint, Http2ConnectionConfig, OriginalConnectionDriver, OriginalHttp2ProtocolTask,
    OriginalHttp2RequestTaskPool, http2_split_client_task_allocation_capacity_bounds,
};
use tower::{Service, ServiceExt};

const WATCHDOG: Duration = Duration::from_secs(5);
const PHASE: Duration = Duration::from_secs(2);
#[derive(Default)]
struct Ledger {
    exited: AtomicBool,
    early_io_exit: AtomicBool,
    created: AtomicUsize,
    io_exit: AtomicUsize,
}
struct OriginalExit {
    credit: Option<ResultWriteCredit>,
    ledger: Arc<Ledger>,
}
impl Drop for OriginalExit {
    fn drop(&mut self) {
        self.ledger.early_io_exit.store(
            self.ledger.io_exit.load(Ordering::SeqCst)
                != self.ledger.created.load(Ordering::SeqCst),
            Ordering::SeqCst,
        );
        self.ledger.exited.store(true, Ordering::SeqCst);
        drop(self.credit.take());
    }
}
#[derive(Clone, Copy)]
enum Mode {
    Valid,
    ShortPipe,
    ShortSend,
}
struct Funding {
    pool: Option<OriginalHttp2RequestTaskPool>,
    original: Option<Bytes>,
    budget: Arc<ResultRetainedBudget>,
    total: usize,
    ledger: Arc<Ledger>,
}
impl Funding {
    fn new(mode: Mode) -> Self {
        let facts = http2_split_client_task_allocation_capacity_bounds().unwrap();
        let pipe = if matches!(mode, Mode::ShortPipe) {
            1
        } else {
            facts.pipe
        };
        let send = if matches!(mode, Mode::ShortSend) {
            1
        } else {
            facts.send
        };
        let per_attempt = OriginalConnectionDriver::task_allocation_capacity_bound()
            .unwrap()
            .checked_add(OriginalConnectionDriver::metadata_allocation_capacity_bound().unwrap())
            .unwrap()
            .checked_add(facts.connection)
            .unwrap()
            .checked_add(OriginalHttp2ProtocolTask::metadata_allocation_capacity_bound().unwrap())
            .unwrap()
            .checked_add(Layout::new::<TokioIo<ObservedIo>>().size())
            .unwrap();
        let total = OriginalHttp2RequestTaskPool::allocation_capacity_bound(1, pipe, send)
            .unwrap()
            .checked_add(2 * per_attempt)
            .unwrap()
            .checked_add(Bytes::owner_with_exit_guard_metadata_size::<
                Bytes,
                OriginalExit,
            >())
            .unwrap();
        let budget = ResultRetainedBudget::new(NonZeroUsize::new(total).unwrap());
        let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(total).unwrap()
        else {
            panic!("pregrant complete fixed pool/task/token/IO carrier before construction")
        };
        let ledger = Arc::new(Ledger::default());
        let original = Bytes::from_owner_with_exit_guard(
            Bytes::new(),
            OriginalExit {
                credit: Some(credit),
                ledger: ledger.clone(),
            },
        );
        let pool =
            OriginalHttp2RequestTaskPool::with_original(1, pipe, send, original.clone()).unwrap();
        Self {
            pool: Some(pool),
            original: Some(original),
            budget,
            total,
            ledger,
        }
    }
    fn drop_public_sources(&mut self) {
        drop(self.pool.take());
        drop(self.original.take());
    }
    fn held(&self) {
        assert!(!self.ledger.exited.load(Ordering::SeqCst));
        assert!(matches!(
            self.budget.try_reserve_process(1).unwrap(),
            ResultWriteAdmission::Blocked
        ));
    }
    fn returned(&self) {
        assert!(self.ledger.exited.load(Ordering::SeqCst));
        assert!(!self.ledger.early_io_exit.load(Ordering::SeqCst));
        let ResultWriteAdmission::Granted(credit) =
            self.budget.try_reserve_process(self.total).unwrap()
        else {
            panic!("real final original aliases must return full grant")
        };
        drop(credit);
    }
}
struct ObservedIo {
    inner: Option<TcpStream>,
    ledger: Arc<Ledger>,
}
impl Drop for ObservedIo {
    fn drop(&mut self) {
        drop(self.inner.take());
        self.ledger.io_exit.fetch_add(1, Ordering::SeqCst);
    }
}
impl AsyncRead for ObservedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(self.inner.as_mut().unwrap()).poll_read(cx, buf)
    }
}
impl AsyncWrite for ObservedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(self.inner.as_mut().unwrap()).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.inner.as_mut().unwrap()).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.inner.as_mut().unwrap()).poll_shutdown(cx)
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
    async fn join(&self) {
        loop {
            let handles = std::mem::take(&mut *self.0.lock().unwrap());
            if handles.is_empty() {
                return;
            }
            for handle in handles {
                tokio::time::timeout(WATCHDOG, handle)
                    .await
                    .expect("actual channel worker did not exit")
                    .unwrap();
            }
        }
    }
}
#[derive(Default)]
struct Tokens {
    drivers: Vec<OriginalConnectionDriver>,
    protocols: Vec<OriginalHttp2ProtocolTask>,
}
type TokenLedger = Arc<Mutex<Tokens>>;
fn endpoint(
    address: std::net::SocketAddr,
    funding: &Funding,
    executor: &JoinedExecutor,
    tokens: &TokenLedger,
) -> Endpoint {
    let original = funding.original.as_ref().unwrap().clone();
    let pool = funding.pool.as_ref().unwrap().clone();
    let tokens = tokens.clone();
    Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .executor(executor.clone())
        .http2_connection_factory(move || {
            // Two finite fixture attempts are prepaid, including rejected fresh tokens.
            let mut tokens = tokens.lock().unwrap();
            assert!(
                tokens.drivers.len() < 2,
                "fixture must not silently grow reconnect inventory"
            );
            let driver = OriginalConnectionDriver::with_original(
                OriginalConnectionDriver::task_allocation_capacity_bound()?,
                original.clone(),
            )?;
            let protocol = OriginalHttp2ProtocolTask::with_original(
                http2_split_client_task_allocation_capacity_bounds()?.connection,
                original.clone(),
            )?;
            tokens.drivers.push(driver.clone());
            tokens.protocols.push(protocol.clone());
            Ok::<_, io::Error>(Http2ConnectionConfig {
                connection_driver: Some(driver),
                protocol_task: Some(protocol),
                request_task_pool: Some(pool.clone()),
                io_owner: Some(original.clone()),
                initial_settings_timeout: Some(PHASE),
                ..Default::default()
            })
        })
}
fn connector(
    address: std::net::SocketAddr,
    calls: Arc<AtomicUsize>,
    ledger: Arc<Ledger>,
) -> impl Service<
    Uri,
    Response = TokioIo<ObservedIo>,
    Error = io::Error,
    Future = impl Future<Output = io::Result<TokioIo<ObservedIo>>> + Send,
> + Clone
+ use<> {
    tower::service_fn(move |_: Uri| {
        calls.fetch_add(1, Ordering::SeqCst);
        let ledger = ledger.clone();
        async move {
            let io = TcpStream::connect(address).await?;
            ledger.created.fetch_add(1, Ordering::SeqCst);
            Ok(TokioIo::new(ObservedIo {
                inner: Some(io),
                ledger,
            }))
        }
    })
}
async fn join_tokens(tokens: &TokenLedger) {
    let tokens = std::mem::take(&mut *tokens.lock().unwrap());
    let mut handles = Vec::new();
    for driver in tokens.drivers {
        if let Some(handle) = driver.take_task_handle() {
            handles.push(handle);
        }
        drop(driver);
    }
    for protocol in tokens.protocols {
        if let Some(handle) = protocol.take_task_handle() {
            handles.push(handle);
        }
        drop(protocol);
    }
    for handle in handles {
        tokio::time::timeout(WATCHDOG, handle)
            .await
            .expect("actual original driver/protocol task did not exit")
            .unwrap();
    }
}
#[derive(Default)]
struct BodyProbe {
    size: AtomicUsize,
    end: AtomicUsize,
    poll: AtomicUsize,
    drops: AtomicUsize,
    changed_task: AtomicBool,
    first: Mutex<Option<Waker>>,
    last: Mutex<Option<Waker>>,
}
impl BodyProbe {
    fn untouched(&self) {
        assert_eq!(self.size.load(Ordering::SeqCst), 0);
        assert_eq!(self.end.load(Ordering::SeqCst), 0);
        assert_eq!(self.poll.load(Ordering::SeqCst), 0);
    }
    fn pipe_waker(&self) -> Waker {
        assert!(
            self.changed_task.load(Ordering::SeqCst),
            "captured real spawned PipeMap poll, distinct from eager outer driver poll"
        );
        drop(self.first.lock().unwrap().take());
        self.last.lock().unwrap().take().unwrap()
    }
}
struct PendingBody {
    receiver: Option<oneshot::Receiver<Bytes>>,
    probe: Arc<BodyProbe>,
}
impl Drop for PendingBody {
    fn drop(&mut self) {
        self.probe.drops.fetch_add(1, Ordering::SeqCst);
    }
}
impl Body for PendingBody {
    type Data = Bytes;
    type Error = tonic::Status;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, tonic::Status>>> {
        self.probe.poll.fetch_add(1, Ordering::SeqCst);
        {
            let mut first = self.probe.first.lock().unwrap();
            if let Some(first) = first.as_ref() {
                if !first.will_wake(cx.waker()) {
                    self.probe.changed_task.store(true, Ordering::SeqCst);
                }
            } else {
                *first = Some(cx.waker().clone());
            }
        }
        let old = self.probe.last.lock().unwrap().replace(cx.waker().clone());
        drop(old);
        let Some(receiver) = self.receiver.as_mut() else {
            return Poll::Ready(None);
        };
        match Pin::new(receiver).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(bytes) => {
                self.receiver = None;
                Poll::Ready(Some(Ok(Frame::data(bytes.unwrap()))))
            }
        }
    }
    fn is_end_stream(&self) -> bool {
        self.probe.end.fetch_add(1, Ordering::SeqCst);
        self.receiver.is_none()
    }
    fn size_hint(&self) -> SizeHint {
        self.probe.size.fetch_add(1, Ordering::SeqCst);
        SizeHint::with_exact(7)
    }
}
fn request() -> (
    Request<tonic::body::BoxBody>,
    oneshot::Sender<Bytes>,
    Arc<BodyProbe>,
) {
    let (release, receiver) = oneshot::channel();
    let probe = Arc::new(BodyProbe::default());
    let request = Request::builder()
        .method("POST")
        .uri("http://localhost/request-pair")
        .body(tonic::body::boxed(PendingBody {
            receiver: Some(receiver),
            probe: probe.clone(),
        }))
        .unwrap();
    (request, release, probe)
}
async fn peer() -> (std::net::SocketAddr, Arc<AtomicUsize>, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let heads = Arc::new(AtomicUsize::new(0));
    let observed = heads.clone();
    let task = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        let mut conn = h2::server::handshake(io).await.unwrap();
        let mut handlers = Vec::new();
        while let Some(request) = conn.accept().await {
            let (request, mut response) = request.unwrap();
            observed.fetch_add(1, Ordering::SeqCst);
            handlers.push(tokio::spawn(async move {
                let mut body = request.into_body();
                let mut bytes = Vec::new();
                while let Some(data) = body.data().await {
                    let data = data.unwrap();
                    bytes.extend_from_slice(&data);
                    body.flow_control().release_capacity(data.len()).unwrap();
                }
                assert_eq!(bytes, b"payload");
                response
                    .send_response(
                        Response::builder()
                            .header("x-original-pair", "actual")
                            .body(())
                            .unwrap(),
                        true,
                    )
                    .unwrap();
            }));
        }
        drop(conn);
        for handler in handlers {
            tokio::time::timeout(WATCHDOG, handler)
                .await
                .unwrap()
                .unwrap();
        }
    });
    (address, heads, task)
}
async fn wait(f: impl Fn() -> bool) {
    tokio::time::timeout(WATCHDOG, async {
        while !f() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual fixture event did not occur");
}
fn error_kind(mut error: &(dyn Error + 'static)) -> Option<io::ErrorKind> {
    loop {
        if let Some(error) = error.downcast_ref::<io::Error>() {
            return Some(error.kind());
        }
        error = error.source()?;
    }
}

#[tokio::test]
async fn actual_tonic_one_pair_refuses_before_body_headers_and_keeps_last_real_pipe_cell_original()
{
    let mut funding = Funding::new(Mode::Valid);
    let tokens = Arc::new(Mutex::new(Tokens::default()));
    let executor = JoinedExecutor::default();
    let (address, heads, peer) = peer().await;
    let endpoint = endpoint(address, &funding, &executor, &tokens);
    let calls = Arc::new(AtomicUsize::new(0));
    let mut channel = endpoint
        .connect_with_connector(connector(address, calls.clone(), funding.ledger.clone()))
        .await
        .unwrap();
    let (first, release, first_probe) = request();
    channel.ready().await.unwrap();
    let first = channel.call(first);
    wait(|| heads.load(Ordering::SeqCst) == 1 && first_probe.changed_task.load(Ordering::SeqCst))
        .await;
    funding.held();
    let (second, _release, refused_probe) = request();
    channel.ready().await.unwrap();
    let second = channel.call(second);
    let error = tokio::time::timeout(WATCHDOG, second)
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error_kind(&error), Some(io::ErrorKind::WouldBlock));
    refused_probe.untouched();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    release.send(Bytes::from_static(b"payload")).unwrap();
    let response = tokio::time::timeout(WATCHDOG, first)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.headers()["x-original-pair"], "actual");
    drop(response);
    wait(|| first_probe.drops.load(Ordering::SeqCst) == 1).await;
    let waker = first_probe.pipe_waker();
    drop(channel);
    drop(endpoint);
    join_tokens(&tokens).await;
    executor.join().await;
    tokio::time::timeout(WATCHDOG, peer)
        .await
        .expect("actual peer IO did not exit")
        .unwrap();
    assert_eq!(heads.load(Ordering::SeqCst), 1);
    assert_eq!(funding.ledger.io_exit.load(Ordering::SeqCst), 1);
    funding.drop_public_sources();
    // Every public configuration/pool and actual connection task has exited.
    // Only the Waker captured from the real PipeMap retains this TaskCell.
    funding.held();
    drop(waker);
    funding.returned();
}

#[tokio::test]
async fn actual_final_pipe_and_send_short_bounds_refuse_before_connector_without_fallback() {
    for mode in [Mode::ShortPipe, Mode::ShortSend] {
        let mut funding = Funding::new(mode);
        let tokens = Arc::new(Mutex::new(Tokens::default()));
        let executor = JoinedExecutor::default();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let endpoint = endpoint(address, &funding, &executor, &tokens);
        let error = endpoint
            .connect_with_connector(connector(address, calls.clone(), funding.ledger.clone()))
            .await
            .unwrap_err();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(error_kind(&error), Some(io::ErrorKind::InvalidInput));
        assert_eq!(funding.ledger.created.load(Ordering::SeqCst), 0);
        drop(endpoint);
        drop(listener);
        join_tokens(&tokens).await;
        executor.join().await;
        funding.drop_public_sources();
        funding.returned();
    }
}

#[tokio::test]
async fn actual_shared_pool_second_attempt_with_fresh_task_tokens_refuses_before_connector() {
    let mut funding = Funding::new(Mode::Valid);
    let tokens = Arc::new(Mutex::new(Tokens::default()));
    let executor = JoinedExecutor::default();
    let (address, heads, peer) = peer().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let endpoint = endpoint(address, &funding, &executor, &tokens);
    let channel = endpoint
        .connect_with_connector(connector(address, calls.clone(), funding.ledger.clone()))
        .await
        .unwrap();
    let error = endpoint
        .clone()
        .connect_with_connector(connector(address, calls.clone(), funding.ledger.clone()))
        .await
        .unwrap_err();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(error_kind(&error), Some(io::ErrorKind::WouldBlock));
    {
        let tokens = tokens.lock().unwrap();
        assert_eq!(tokens.drivers.len(), 2);
        assert_eq!(tokens.protocols.len(), 2);
    }
    drop(channel);
    drop(endpoint);
    join_tokens(&tokens).await;
    executor.join().await;
    tokio::time::timeout(WATCHDOG, peer).await.unwrap().unwrap();
    assert_eq!(heads.load(Ordering::SeqCst), 0);
    funding.drop_public_sources();
    funding.returned();
}
