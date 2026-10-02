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

//! Real Tonic Channel attempts with per-attempt owners and a custom connector.
//! These are HTTP/2 Service<BoxBody> fixtures, not a Native deployment or a
//! complete connection/allocator/deadline proof. Pool/raw/carrier original
//! and fixed writer backing is funded through one real Worker wallet; tasks, IO, decoded frames,
//! HPACK and Tonic/Hyper/error metadata are separate. Watchdogs only fail hangs.

use bytes::Bytes;
use h2::{
    ReceiveBufferPool, ReceiveFrameBuffer, ReceiveHeaderBlockBuffer, SendFrameBuffer,
    SendHeaderBlockPool,
};
use hyper::body::Body;
use hyper::http::{Request, Response, Uri};
use hyper::rt::Executor;
use hyper_util::rt::TokioIo;
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::collections::VecDeque;
use std::error::Error as StdError;
use std::future::Future;
use std::io;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};
use tokio::sync::oneshot;
use tonic::transport::{Endpoint, Http2ConnectionConfig};
use tower::{Service, ServiceExt, service_fn};

const FRAME_BYTES: usize = 16384;
type Task = Pin<Box<dyn Future<Output = ()> + Send>>;
#[derive(Clone, Default)]
struct ManualExecutor {
    tasks: Arc<Mutex<Vec<Task>>>,
    completed: Arc<AtomicUsize>,
}
impl<F: Future<Output = ()> + Send + 'static> Executor<F> for ManualExecutor {
    fn execute(&self, task: F) {
        self.tasks.lock().unwrap().push(Box::pin(task));
    }
}
impl ManualExecutor {
    fn poll_once(&self, cx: &mut Context<'_>) {
        let tasks = std::mem::take(&mut *self.tasks.lock().unwrap());
        for mut task in tasks {
            if task.as_mut().poll(cx).is_pending() {
                self.tasks.lock().unwrap().push(task);
            } else {
                // Actual future return and drop, outside the executor lock.
                drop(task);
                self.completed.fetch_add(1, Ordering::AcqRel);
            }
        }
    }
    fn active(&self) -> usize {
        self.tasks.lock().unwrap().len()
    }
    fn drop_all(&self) {
        // Explicitly owned cancellation in error-only fixtures. Future Drop
        // completes synchronously; no executor/future Arc cycle is left live.
        let tasks = std::mem::take(&mut *self.tasks.lock().unwrap());
        drop(tasks);
    }
}
async fn drive<F: Future>(executor: &ManualExecutor, future: F) -> F::Output {
    let mut future = Box::pin(future);
    tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| {
            executor.poll_once(cx);
            match future.as_mut().poll(cx) {
                Poll::Ready(value) => Poll::Ready(value),
                Poll::Pending => {
                    // Explicitly pump only owned futures; yielding to Tokio lets
                    // the actual peer progress. No sleep is an exit assertion.
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
        }),
    )
    .await
    .expect("test fixture failed to make protocol progress")
}
async fn drain(executor: &ManualExecutor) {
    drive(
        executor,
        std::future::poll_fn(|_| {
            if executor.active() == 0 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }),
    )
    .await;
}

struct ExitIo {
    inner: Option<DuplexStream>,
    exit: Option<oneshot::Sender<()>>,
    // Fixture-only trace; it is not part of the original attempt grant.
    wire: Arc<Mutex<Vec<u8>>>,
}
impl AsyncRead for ExitIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(self.inner.as_mut().unwrap()).poll_read(cx, buf)
    }
}
impl AsyncWrite for ExitIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match Pin::new(self.inner.as_mut().unwrap()).poll_write(cx, buf) {
            Poll::Ready(Ok(n)) => {
                self.wire.lock().unwrap().extend_from_slice(&buf[..n]);
                Poll::Ready(Ok(n))
            }
            other => other,
        }
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
        // This notification follows the actual original IO handle's Drop,
        // not the beginning of this wrapper's destructor or a logical close.
        drop(self.inner.take());
        if let Some(exit) = self.exit.take() {
            let _ = exit.send(());
        }
    }
}
fn io_pair() -> (ExitIo, DuplexStream, oneshot::Receiver<()>) {
    let (client, peer) = tokio::io::duplex(65536);
    let (exit, observed) = oneshot::channel();
    (
        ExitIo {
            inner: Some(client),
            exit: Some(exit),
            wire: Arc::new(Mutex::new(Vec::new())),
        },
        peer,
        observed,
    )
}
fn connector(
    inputs: Arc<Mutex<VecDeque<ExitIo>>>,
    calls: Arc<AtomicUsize>,
) -> impl Service<
    Uri,
    Response = TokioIo<ExitIo>,
    Error = io::Error,
    Future = impl Future<Output = io::Result<TokioIo<ExitIo>>> + Send,
> + Clone {
    service_fn(move |_: Uri| {
        calls.fetch_add(1, Ordering::AcqRel);
        let input = inputs.lock().unwrap().pop_front();
        async move {
            input
                .map(TokioIo::new)
                .ok_or_else(|| io::ErrorKind::ConnectionRefused.into())
        }
    })
}
fn attempt_bytes() -> usize {
    ReceiveFrameBuffer::allocation_capacity_bound(FRAME_BYTES).unwrap()
        + ReceiveHeaderBlockBuffer::allocation_capacity_bound(FRAME_BYTES).unwrap()
        + SendFrameBuffer::allocation_capacity_bound(65536, FRAME_BYTES).unwrap()
        + SendHeaderBlockPool::allocation_capacity_bound(FRAME_BYTES).unwrap()
        + 2 * ReceiveBufferPool::allocation_capacity_bound(2, FRAME_BYTES).unwrap()
        + Bytes::owner_with_exit_guard_metadata_size::<Bytes, ResultWriteCredit>()
}
fn budget(attempts: usize) -> Arc<ResultRetainedBudget> {
    ResultRetainedBudget::new(NonZeroUsize::new(attempts * attempt_bytes()).unwrap())
}
fn funded_config(budget: &Arc<ResultRetainedBudget>) -> io::Result<Http2ConnectionConfig> {
    let ResultWriteAdmission::Granted(credit) = budget
        .try_reserve_process(attempt_bytes())
        .map_err(io::Error::other)?
    else {
        return Err(io::ErrorKind::WouldBlock.into());
    };
    let owner = Bytes::from_owner_with_exit_guard(Bytes::new(), credit);
    Ok(Http2ConnectionConfig {
        initial_settings_timeout: None,
        max_frame_size: Some(FRAME_BYTES as u32),
        max_header_list_size: Some(FRAME_BYTES as u32),
        max_receive_header_block_size: Some(FRAME_BYTES),
        header_table_size: None,
        max_send_header_table_size: Some(0),
        send_header_block_pool: Some(SendHeaderBlockPool::new(FRAME_BYTES, owner.clone())?),
        max_receive_buffered_events: Some(8),
        max_send_buffer_size: Some(65536),
        retain_data_payloads: true,
        receive_buffer_pool: Some(ReceiveBufferPool::new(2, FRAME_BYTES, owner.clone())?),
        receive_frame_buffer: Some(ReceiveFrameBuffer::new(FRAME_BYTES, owner.clone())?),
        receive_header_block_buffer: Some(ReceiveHeaderBlockBuffer::new(
            FRAME_BYTES,
            owner.clone(),
        )?),
        receive_header_field_pool: None,
        receive_header_table_buffer: None,
        receive_header_map_pool: None,
        send_frame_buffer: Some(SendFrameBuffer::new(65536, FRAME_BYTES, owner.clone())?),
        receive_goaway_buffer_pool: Some(ReceiveBufferPool::new(2, FRAME_BYTES, owner)?),
    })
}
fn reserve_all(budget: &Arc<ResultRetainedBudget>, bytes: usize) {
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(bytes).unwrap() else {
        panic!("physical attempt owners must exit before the original grant is reusable");
    };
    drop(credit);
}
fn blocked(budget: &Arc<ResultRetainedBudget>, bytes: usize) {
    assert!(matches!(
        budget.try_reserve_process(bytes).unwrap(),
        ResultWriteAdmission::Blocked
    ));
}
fn boxed_request() -> Request<tonic::body::BoxBody> {
    Request::builder()
        .uri("http://localhost/factory/test")
        .body(tonic::body::empty_body())
        .unwrap()
}
async fn first_data(
    executor: &ManualExecutor,
    mut response: Response<tonic::body::BoxBody>,
) -> Bytes {
    let frame = drive(
        executor,
        std::future::poll_fn(|cx| Pin::new(response.body_mut()).poll_frame(cx)),
    )
    .await
    .expect("actual response DATA")
    .expect("successful DATA frame");
    frame.into_data().expect("first response frame is DATA")
}
fn response_peer(
    io: DuplexStream,
    payload: &'static [u8],
) -> (oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let (close, mut closed) = oneshot::channel();
    let task = tokio::spawn(async move {
        let mut connection = h2::server::handshake(io).await.unwrap();
        let (request, mut response) = connection.accept().await.unwrap().unwrap();
        drop(request);
        let mut stream = response.send_response(Response::new(()), false).unwrap();
        stream.send_data(Bytes::from_static(payload), true).unwrap();
        tokio::select! {
            _ = &mut closed => {},
            incoming = connection.accept() => {
                assert!(incoming.is_none(), "fixture serves one request per actual connection");
            }
        }
        // The JoinHandle proves this real peer and its connection exited.
        drop(stream);
        drop(connection);
    });
    (close, task)
}

#[tokio::test]
async fn cloned_endpoint_factory_funds_fresh_pools_on_actual_channel_reconnect() {
    let (first_io, first_peer, first_exit) = io_pair();
    let (second_io, second_peer, second_exit) = io_pair();
    let (first_close, first_join) = response_peer(first_peer, b"first original DATA");
    let (second_close, second_join) = response_peer(second_peer, b"second original DATA");
    let inputs = Arc::new(Mutex::new(VecDeque::from([first_io, second_io])));
    let dials = Arc::new(AtomicUsize::new(0));
    let factories = Arc::new(AtomicUsize::new(0));
    let original_budget = budget(2);
    let factory_budget = original_budget.clone();
    let factory_count = factories.clone();
    let executor = ManualExecutor::default();
    let endpoint = Endpoint::from_static("http://localhost")
        .executor(executor.clone())
        .http2_connection_factory(move || {
            factory_count.fetch_add(1, Ordering::AcqRel);
            funded_config(&factory_budget)
        });
    let channel = endpoint
        .clone()
        .connect_with_connector(connector(inputs, dials.clone()))
        .await
        .unwrap();
    let first_response = drive(&executor, channel.clone().oneshot(boxed_request()))
        .await
        .unwrap();
    let first = first_data(&executor, first_response).await;
    let first_alias = first.clone();
    assert_eq!(first.as_ref(), b"first original DATA");
    drop(first);
    first_close.send(()).unwrap();
    first_join.await.unwrap();
    drive(&executor, first_exit).await.unwrap();
    assert!(
        executor.completed.load(Ordering::Acquire) >= 1,
        "the old actual connection task must have returned and dropped"
    );
    assert_eq!(factories.load(Ordering::Acquire), 1);
    assert_eq!(dials.load(Ordering::Acquire), 1);
    let second_response = drive(&executor, channel.clone().oneshot(boxed_request()))
        .await
        .unwrap();
    let second = first_data(&executor, second_response).await;
    assert_eq!(second.as_ref(), b"second original DATA");
    assert_eq!(factories.load(Ordering::Acquire), 2);
    assert_eq!(dials.load(Ordering::Acquire), 2);
    // Both complete attempt grants remain: one escaped original DATA alias
    // from the old connection, plus the independent new live connection.
    blocked(&original_budget, 1);
    assert_eq!(first_alias.as_ref(), b"first original DATA");
    drop(first_alias);
    reserve_all(&original_budget, attempt_bytes());
    blocked(&original_budget, 2 * attempt_bytes());
    second_close.send(()).unwrap();
    second_join.await.unwrap();
    drive(&executor, second_exit).await.unwrap();
    drop(channel);
    drop(endpoint);
    drain(&executor).await;
    blocked(&original_budget, 2 * attempt_bytes());
    drop(second);
    reserve_all(&original_budget, 2 * attempt_bytes());
}

#[tokio::test]
async fn factory_rejection_happens_before_connector_call_or_owned_io() {
    let calls = Arc::new(AtomicUsize::new(0));
    let factories = Arc::new(AtomicUsize::new(0));
    let count = factories.clone();
    let endpoint = Endpoint::from_static("http://localhost").http2_connection_factory(move || {
        count.fetch_add(1, Ordering::AcqRel);
        Err::<Http2ConnectionConfig, _>(io::Error::other("injected factory refusal"))
    });
    let error = endpoint
        .connect_with_connector(connector(
            Arc::new(Mutex::new(VecDeque::new())),
            calls.clone(),
        ))
        .await
        .unwrap_err();
    assert!(format!("{error:?}").contains("injected factory refusal"));
    assert_eq!(factories.load(Ordering::Acquire), 1);
    assert_eq!(calls.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn invalid_scalar_or_owned_geometry_rejects_before_dial_and_returns_grant() {
    for case in 0..17 {
        let original_budget = budget(1);
        let factory_budget = original_budget.clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let endpoint =
            Endpoint::from_static("http://localhost").http2_connection_factory(move || {
                let mut config = funded_config(&factory_budget)?;
                match case {
                    0 => config.max_frame_size = Some(0),
                    1 => config.max_frame_size = Some(16383),
                    2 => config.max_frame_size = Some(16777216),
                    3 => {
                        // Preserve the scalar-only check independently of the
                        // new owned workspace geometry checks below.
                        config.receive_header_block_buffer = None;
                        config.max_receive_header_block_size = Some(0);
                    }
                    4 => config.max_receive_buffered_events = Some(0),
                    5 => config.max_send_buffer_size = Some(usize::MAX),
                    6 => config.max_frame_size = Some(32768),
                    7 => config.max_receive_buffered_events = None,
                    8 => {
                        // Isolate raw input geometry from both escaped pools.
                        config.receive_buffer_pool = None;
                        config.receive_goaway_buffer_pool = None;
                        config.max_frame_size = Some(32768);
                    }
                    9 => {
                        // Isolate GOAWAY geometry from DATA and raw input.
                        config.receive_buffer_pool = None;
                        config.receive_frame_buffer = None;
                        config.receive_header_block_buffer = None;
                        config.max_frame_size = Some(32768);
                    }
                    10 => {
                        config.receive_header_block_buffer = None;
                        config.max_receive_header_block_size = Some(usize::MAX);
                    }
                    11 => config.max_send_header_table_size = None,
                    12 => config.max_send_header_table_size = Some(1),
                    13 => config.receive_frame_buffer = None,
                    14 => config.max_receive_header_block_size = None,
                    15 => config.max_receive_header_block_size = Some(0),
                    16 => config.max_receive_header_block_size = Some(FRAME_BYTES + 1),
                    _ => unreachable!(),
                }
                Ok::<_, io::Error>(config)
            });
        let error = endpoint
            .connect_with_connector(connector(
                Arc::new(Mutex::new(VecDeque::new())),
                calls.clone(),
            ))
            .await
            .unwrap_err();
        assert!(
            format!("{error:?}").contains("per-connection"),
            "case {case}: {error:?}"
        );
        if case >= 13 {
            assert!(
                format!("{error:?}").contains("encoded header buffer requires"),
                "case {case} must refuse at the owned encoded-input geometry gate: {error:?}"
            );
        }
        assert_eq!(
            calls.load(Ordering::Acquire),
            0,
            "case {case} must not dial"
        );
        reserve_all(&original_budget, attempt_bytes());
    }
}

#[tokio::test]
async fn header_field_pool_validates_dependencies_and_effective_endpoint_cap_before_dial() {
    let field_bytes =
        h2::ReceiveHeaderFieldPool::allocation_capacity_bound(65536, 1024, FRAME_BYTES).unwrap()
            + Bytes::owner_with_exit_guard_metadata_size::<Bytes, ResultWriteCredit>();
    let total = attempt_bytes() + field_bytes;
    for case in 0..10 {
        let original_budget = ResultRetainedBudget::new(NonZeroUsize::new(total).unwrap());
        let factory_budget = original_budget.clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut endpoint = Endpoint::from_static("http://localhost");
        if matches!(case, 5 | 6) {
            endpoint = endpoint.http2_max_header_list_size(32768);
        } else if case == 8 {
            endpoint = endpoint.http2_max_header_list_size(31);
        } else if case == 9 {
            endpoint = endpoint.http2_max_header_list_size(FRAME_BYTES as u32);
        }
        let endpoint = endpoint.http2_connection_factory(move || {
            let mut config = funded_config(&factory_budget)?;
            let ResultWriteAdmission::Granted(credit) = factory_budget
                .try_reserve_process(field_bytes)
                .map_err(io::Error::other)?
            else {
                return Err(io::ErrorKind::WouldBlock.into());
            };
            config.receive_header_field_pool = Some(h2::ReceiveHeaderFieldPool::new(
                65536,
                1024,
                FRAME_BYTES,
                Bytes::from_owner_with_exit_guard(Bytes::new(), credit),
            )?);
            match case {
                0 => config.receive_frame_buffer = None,
                1 => config.receive_header_block_buffer = None,
                2 => config.max_receive_header_block_size = None,
                3 => config.max_header_list_size = Some(31),
                4 => config.max_header_list_size = Some(32768),
                5 | 7 | 8 | 9 => config.max_header_list_size = None,
                6 => config.max_header_list_size = Some(FRAME_BYTES as u32),
                _ => unreachable!(),
            }
            Ok::<_, io::Error>(config)
        });
        let error = endpoint
            .connect_with_connector(connector(
                Arc::new(Mutex::new(VecDeque::new())),
                calls.clone(),
            ))
            .await
            .unwrap_err();
        let valid = matches!(case, 6 | 7 | 9);
        assert_eq!(
            calls.load(Ordering::Acquire),
            usize::from(valid),
            "case {case}: {error:?}"
        );
        if !valid {
            assert!(
                format!("{error:?}").contains("per-connection"),
                "case {case}: {error:?}"
            );
            if !matches!(case, 0 | 2) {
                assert!(
                    format!("{error:?}").contains("header field pool requires"),
                    "case {case}: {error:?}"
                );
            }
        } else {
            // A deliberate empty connector queue proves valid geometry passed
            // the pre-dial gate. It is not a successful native deployment.
            let mut cause: &(dyn StdError + 'static) = &error;
            loop {
                if let Some(cause) = cause.downcast_ref::<io::Error>() {
                    assert_eq!(cause.kind(), io::ErrorKind::ConnectionRefused);
                    break;
                }
                cause = cause.source().expect("actual dial refusal source");
            }
        }
        reserve_all(&original_budget, total);
    }
}

#[tokio::test]
async fn dial_failure_returns_every_fresh_attempt_owner() {
    let original_budget = budget(1);
    let factory_budget = original_budget.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let endpoint = Endpoint::from_static("http://localhost")
        .http2_connection_factory(move || funded_config(&factory_budget));
    let error = endpoint
        .connect_with_connector(connector(
            Arc::new(Mutex::new(VecDeque::new())),
            calls.clone(),
        ))
        .await
        .unwrap_err();
    let mut cause: &(dyn StdError + 'static) = &error;
    loop {
        if let Some(cause) = cause.downcast_ref::<io::Error>() {
            assert_eq!(cause.kind(), io::ErrorKind::ConnectionRefused);
            break;
        }
        cause = cause.source().expect("actual dial IO failure source");
    }
    assert_eq!(calls.load(Ordering::Acquire), 1);
    reserve_all(&original_budget, attempt_bytes());
}

struct PendingDial(Arc<AtomicUsize>);
impl Future for PendingDial {
    type Output = io::Result<TokioIo<ExitIo>>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}
impl Drop for PendingDial {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}
#[tokio::test]
async fn cancelled_dial_drops_the_actual_pending_future_and_original_attempt_grant() {
    let original_budget = budget(1);
    let factory_budget = original_budget.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let exits = Arc::new(AtomicUsize::new(0));
    let dial_calls = calls.clone();
    let dial_exits = exits.clone();
    let dial = service_fn(move |_: Uri| {
        dial_calls.fetch_add(1, Ordering::AcqRel);
        PendingDial(dial_exits.clone())
    });
    let endpoint = Endpoint::from_static("http://localhost")
        .http2_connection_factory(move || funded_config(&factory_budget));
    let mut attempt = Box::pin(endpoint.connect_with_connector(dial));
    assert!(
        attempt
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_eq!(calls.load(Ordering::Acquire), 1);
    assert_eq!(exits.load(Ordering::Acquire), 0);
    blocked(&original_budget, 1);
    drop(attempt);
    assert_eq!(exits.load(Ordering::Acquire), 1);
    reserve_all(&original_budget, attempt_bytes());
}

#[tokio::test]
async fn reusing_a_once_bound_pool_refuses_after_dial_before_the_next_preface() {
    let pool_bytes = ReceiveBufferPool::allocation_capacity_bound(2, FRAME_BYTES).unwrap()
        + Bytes::owner_with_exit_guard_metadata_size::<Bytes, ResultWriteCredit>();
    let original_budget = ResultRetainedBudget::new(NonZeroUsize::new(pool_bytes).unwrap());
    let ResultWriteAdmission::Granted(credit) =
        original_budget.try_reserve_process(pool_bytes).unwrap()
    else {
        panic!("original once-bound pool funding");
    };
    let pool = ReceiveBufferPool::new(
        2,
        FRAME_BYTES,
        Bytes::from_owner_with_exit_guard(Bytes::new(), credit),
    )
    .unwrap();
    let shared = pool.clone();
    let executor = ManualExecutor::default();
    let endpoint = Endpoint::from_static("http://localhost")
        .executor(executor.clone())
        .http2_connection_factory(move || {
            Ok::<_, io::Error>(Http2ConnectionConfig {
                receive_goaway_buffer_pool: Some(shared.clone()),
                ..Default::default()
            })
        });
    let (first, _first_peer, first_exit) = io_pair();
    let (second, mut second_peer, second_exit) = io_pair();
    let inputs = Arc::new(Mutex::new(VecDeque::from([first, second])));
    let calls = Arc::new(AtomicUsize::new(0));
    let channel = endpoint
        .connect_with_connector(connector(inputs.clone(), calls.clone()))
        .await
        .unwrap();
    drop(channel);
    executor.drop_all();
    first_exit.await.unwrap();
    let error = endpoint
        .clone()
        .connect_with_connector(connector(inputs, calls.clone()))
        .await
        .unwrap_err();
    assert!(format!("{error:?}").contains("already bound"));
    assert_eq!(
        calls.load(Ordering::Acquire),
        2,
        "bind refusal happens after obtaining IO"
    );
    second_exit.await.unwrap();
    assert_eq!(
        second_peer.read(&mut [0]).await.unwrap(),
        0,
        "reused pool must refuse before client preface or SETTINGS I/O"
    );
    drop(error);
    drop(endpoint);
    drop(pool);
    reserve_all(&original_budget, pool_bytes);
}

#[tokio::test]
async fn reused_header_workspace_alone_proves_actual_factory_forwarding() {
    let carrier_bytes = Bytes::owner_with_exit_guard_metadata_size::<Bytes, ResultWriteCredit>();
    let header_bytes =
        ReceiveHeaderBlockBuffer::allocation_capacity_bound(FRAME_BYTES).unwrap() + carrier_bytes;
    let raw_bytes =
        ReceiveFrameBuffer::allocation_capacity_bound(FRAME_BYTES).unwrap() + carrier_bytes;
    let total = header_bytes + 2 * raw_bytes;
    let original_budget = ResultRetainedBudget::new(NonZeroUsize::new(total).unwrap());
    let ResultWriteAdmission::Granted(header_credit) =
        original_budget.try_reserve_process(header_bytes).unwrap()
    else {
        panic!("original header workspace pregrant");
    };
    let header = ReceiveHeaderBlockBuffer::new(
        FRAME_BYTES,
        Bytes::from_owner_with_exit_guard(Bytes::new(), header_credit),
    )
    .unwrap();
    let shared_header = header.clone();
    let factory_budget = original_budget.clone();
    let factories = Arc::new(AtomicUsize::new(0));
    let factory_calls = factories.clone();
    let executor = ManualExecutor::default();
    let endpoint = Endpoint::from_static("http://localhost")
        .executor(executor.clone())
        .http2_connection_factory(move || {
            factory_calls.fetch_add(1, Ordering::AcqRel);
            let ResultWriteAdmission::Granted(raw_credit) = factory_budget
                .try_reserve_process(raw_bytes)
                .map_err(io::Error::other)?
            else {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "fresh raw pregrant unavailable",
                ));
            };
            // Every raw input is fresh. The header workspace is the ONLY reused
            // once-bound object; no DATA/GOAWAY/writer/outbound pool can mask a
            // missing Tonic -> Hyper -> h2 header-workspace forwarding seam.
            Ok::<_, io::Error>(Http2ConnectionConfig {
                max_frame_size: Some(FRAME_BYTES as u32),
                max_header_list_size: Some(FRAME_BYTES as u32),
                max_receive_header_block_size: Some(FRAME_BYTES),
                receive_frame_buffer: Some(ReceiveFrameBuffer::new(
                    FRAME_BYTES,
                    Bytes::from_owner_with_exit_guard(Bytes::new(), raw_credit),
                )?),
                receive_header_block_buffer: Some(shared_header.clone()),
                ..Default::default()
            })
        });
    let (first, first_peer, first_exit) = io_pair();
    let (second, mut second_peer, second_exit) = io_pair();
    let (first_close, first_join) = response_peer(first_peer, b"decoded with original workspace");
    let calls = Arc::new(AtomicUsize::new(0));
    let inputs = Arc::new(Mutex::new(VecDeque::from([first, second])));
    let channel = endpoint
        .clone()
        .connect_with_connector(connector(inputs.clone(), calls.clone()))
        .await
        .unwrap();
    let response = drive(&executor, channel.clone().oneshot(boxed_request()))
        .await
        .unwrap();
    let data = first_data(&executor, response).await;
    assert_eq!(data.as_ref(), b"decoded with original workspace");
    drop(data);
    first_close.send(()).unwrap();
    first_join.await.unwrap();
    drive(&executor, first_exit).await.unwrap();
    drop(channel);
    drain(&executor).await;
    reserve_all(&original_budget, 2 * raw_bytes);
    blocked(&original_budget, total);

    let error = endpoint
        .clone()
        .connect_with_connector(connector(inputs, calls.clone()))
        .await
        .unwrap_err();
    let diagnostic = format!("{error:?}");
    assert!(
        diagnostic.contains("encoded header buffer already bound"),
        "{diagnostic}"
    );
    assert_eq!(factories.load(Ordering::Acquire), 2);
    assert_eq!(
        calls.load(Ordering::Acquire),
        2,
        "once-bind refusal follows an actual fresh dial"
    );
    second_exit.await.unwrap();
    assert_eq!(
        second_peer.read(&mut [0]).await.unwrap(),
        0,
        "reused header workspace must refuse before the second preface"
    );
    drop(error);
    drop(endpoint);
    drop(header);
    drain(&executor).await;
    reserve_all(&original_budget, total);
}

#[tokio::test]
async fn no_factory_preserves_the_default_channel_and_connector_path() {
    let (input, peer, exit) = io_pair();
    let (close, join) = response_peer(peer, b"default channel DATA");
    let calls = Arc::new(AtomicUsize::new(0));
    let executor = ManualExecutor::default();
    let endpoint = Endpoint::from_static("http://localhost").executor(executor.clone());
    let channel = endpoint
        .connect_with_connector(connector(
            Arc::new(Mutex::new(VecDeque::from([input]))),
            calls.clone(),
        ))
        .await
        .unwrap();
    let response = drive(&executor, channel.clone().oneshot(boxed_request()))
        .await
        .unwrap();
    let data = first_data(&executor, response).await;
    assert_eq!(data.as_ref(), b"default channel DATA");
    assert_eq!(calls.load(Ordering::Acquire), 1);
    close.send(()).unwrap();
    join.await.unwrap();
    drive(&executor, exit).await.unwrap();
    drop(data);
    drop(channel);
    drop(endpoint);
    drain(&executor).await;
}

fn wire_frame(kind: u8, flags: u8, payload: &[u8]) -> Vec<u8> {
    let mut frame = (payload.len() as u32).to_be_bytes()[1..].to_vec();
    frame.extend_from_slice(&[kind, flags, 0, 0, 0, 0]);
    frame.extend_from_slice(payload);
    frame
}
fn h2_cause<'a>(mut error: &'a (dyn StdError + 'static)) -> &'a h2::Error {
    loop {
        if let Some(error) = error.downcast_ref::<h2::Error>() {
            return error;
        }
        error = error
            .source()
            .expect("Tonic must preserve the original h2 GOAWAY source");
    }
}
#[tokio::test]
async fn tonic_response_error_can_retain_original_goaway_grant_after_connection_exit() {
    let (input, mut peer, exit) = io_pair();
    let peer_join = tokio::spawn(async move {
        let mut preface = [0; 24];
        peer.read_exact(&mut preface).await.unwrap();
        assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
        peer.write_all(&wire_frame(4, 0, &[])).await.unwrap();
        peer.write_all(&wire_frame(4, 1, &[])).await.unwrap();
        // Wait for the real outgoing request, so the escaped response error
        // originates in h2 instead of an earlier canceled dispatch attempt.
        loop {
            let mut header = [0; 9];
            peer.read_exact(&mut header).await.unwrap();
            let length =
                ((header[0] as usize) << 16) | ((header[1] as usize) << 8) | header[2] as usize;
            assert!(length <= FRAME_BYTES);
            let mut payload = vec![0; length];
            peer.read_exact(&mut payload).await.unwrap();
            if header[3] == 1 {
                break;
            }
        }
        let mut payload = vec![0; 4];
        payload.extend_from_slice(&u32::from(h2::Reason::INTERNAL_ERROR).to_be_bytes());
        payload.extend_from_slice(b"tonic-owned-diagnostic");
        peer.write_all(&wire_frame(7, 0, &payload)).await.unwrap();
    });
    let original_budget = budget(1);
    let factory_budget = original_budget.clone();
    let executor = ManualExecutor::default();
    let endpoint = Endpoint::from_static("http://localhost")
        .executor(executor.clone())
        .http2_connection_factory(move || funded_config(&factory_budget));
    let channel = endpoint
        .connect_with_connector(connector(
            Arc::new(Mutex::new(VecDeque::from([input]))),
            Arc::new(AtomicUsize::new(0)),
        ))
        .await
        .unwrap();
    let error = drive(&executor, channel.clone().oneshot(boxed_request()))
        .await
        .unwrap_err();
    let cause = h2_cause(&error);
    assert!(cause.is_remote() && cause.is_go_away());
    assert_eq!(cause.reason(), Some(h2::Reason::INTERNAL_ERROR));
    assert!(cause.to_string().contains("tonic-owned-diagnostic"));
    peer_join.await.unwrap();
    drive(&executor, exit).await.unwrap();
    drop(channel);
    drop(endpoint);
    drain(&executor).await;
    blocked(&original_budget, 1);
    assert!(
        h2_cause(&error)
            .to_string()
            .contains("tonic-owned-diagnostic")
    );
    drop(error);
    reserve_all(&original_budget, attempt_bytes());
}

#[tokio::test]
async fn actual_channel_factory_forwards_zero_outbound_table_before_first_headers() {
    let (input, peer, exit) = io_pair();
    let wire = input.wire.clone();
    let (close, join) = response_peer(peer, b"table proof");
    let executor = ManualExecutor::default();
    let original_budget = budget(1);
    let factory_budget = original_budget.clone();
    let endpoint = Endpoint::from_static("http://localhost")
        .executor(executor.clone())
        .http2_connection_factory(move || funded_config(&factory_budget));
    let channel = endpoint
        .clone()
        .connect_with_connector(connector(
            Arc::new(Mutex::new(VecDeque::from([input]))),
            Arc::new(AtomicUsize::new(0)),
        ))
        .await
        .unwrap();
    let response = drive(&executor, channel.clone().oneshot(boxed_request()))
        .await
        .unwrap();
    let data = first_data(&executor, response).await;
    assert_eq!(&data[..], b"table proof");
    {
        let trace = wire.lock().unwrap();
        let preface = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
        assert!(trace.starts_with(preface));
        let mut frames = &trace[preface.len()..];
        let mut first = None;
        while !frames.is_empty() {
            assert!(frames.len() >= 9);
            let len =
                ((frames[0] as usize) << 16) | ((frames[1] as usize) << 8) | frames[2] as usize;
            assert!(frames.len() >= len + 9);
            if frames[3] == 1 {
                first = Some(frames[9]);
                break;
            }
            frames = &frames[9 + len..];
        }
        assert_eq!(
            first,
            Some(0x20),
            "actual Tonic factory must emit the initial zero HPACK table update"
        );
    }
    drop(data);
    close.send(()).unwrap();
    join.await.unwrap();
    drive(&executor, exit).await.unwrap();
    drop(channel);
    drop(endpoint);
    drain(&executor).await;
    reserve_all(&original_budget, attempt_bytes());
}

#[tokio::test]
async fn actual_channel_factory_refuses_oversized_block_before_first_headers() {
    let (input, peer, exit) = io_pair();
    let wire = input.wire.clone();
    let join = tokio::spawn(async move {
        let mut connection = h2::server::handshake(peer).await.unwrap();
        if let Some(request) = connection.accept().await {
            assert!(
                request.is_err(),
                "oversized request must never reach the actual peer"
            );
        }
    });
    let executor = ManualExecutor::default();
    let original_budget = budget(1);
    let factory_budget = original_budget.clone();
    let endpoint = Endpoint::from_static("http://localhost")
        .executor(executor.clone())
        .http2_connection_factory(move || funded_config(&factory_budget));
    let channel = endpoint
        .clone()
        .connect_with_connector(connector(
            Arc::new(Mutex::new(VecDeque::from([input]))),
            Arc::new(AtomicUsize::new(0)),
        ))
        .await
        .unwrap();
    let mut request = boxed_request();
    request.headers_mut().insert(
        "x-full-block",
        hyper::http::HeaderValue::from_bytes(&vec![b'~'; FRAME_BYTES]).unwrap(),
    );
    assert!(
        drive(&executor, channel.clone().oneshot(request))
            .await
            .is_err()
    );
    join.await.unwrap();
    drive(&executor, exit).await.unwrap();
    drop(channel);
    drop(endpoint);
    drain(&executor).await;
    {
        let trace = wire.lock().unwrap();
        let preface = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
        assert!(trace.starts_with(preface));
        let mut frames = &trace[preface.len()..];
        while !frames.is_empty() {
            assert!(frames.len() >= 9);
            let len =
                ((frames[0] as usize) << 16) | ((frames[1] as usize) << 8) | frames[2] as usize;
            assert!(frames.len() >= len + 9);
            assert!(
                !matches!(frames[3], 1 | 9),
                "refused block wrote HEADERS/CONTINUATION"
            );
            frames = &frames[9 + len..];
        }
    }
    reserve_all(&original_budget, attempt_bytes());
}

#[tokio::test]
async fn initial_settings_allowance_includes_synchronous_factory_before_connector_call() {
    // This component-only 50ms setting distinguishes the factory-origin
    // allowance; the installed Native profile remains exactly two seconds.
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let endpoint = Endpoint::from_static("http://localhost:9070").http2_connection_factory(|| {
        std::thread::sleep(Duration::from_millis(80));
        Ok::<_, io::Error>(Http2ConnectionConfig {
            initial_settings_timeout: Some(Duration::from_millis(50)),
            ..Http2ConnectionConfig::default()
        })
    });
    let connector = service_fn(move |_: Uri| {
        counted.fetch_add(1, Ordering::AcqRel);
        async { Err::<TokioIo<DuplexStream>, _>(io::Error::from(io::ErrorKind::ConnectionRefused)) }
    });
    let result = endpoint.connect_with_connector(connector).await;
    assert!(result.is_err());
    assert_eq!(
        calls.load(Ordering::Acquire),
        0,
        "the factory cannot restart the allowance before connector.call"
    );
}

#[tokio::test]
async fn zero_initial_settings_allowance_refuses_before_connector_call() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let endpoint = Endpoint::from_static("http://localhost:9070").http2_connection_factory(|| {
        Ok::<_, io::Error>(Http2ConnectionConfig {
            initial_settings_timeout: Some(Duration::ZERO),
            ..Http2ConnectionConfig::default()
        })
    });
    let connector = service_fn(move |_: Uri| {
        counted.fetch_add(1, Ordering::AcqRel);
        async { Err::<TokioIo<DuplexStream>, _>(io::Error::from(io::ErrorKind::ConnectionRefused)) }
    });
    assert!(endpoint.connect_with_connector(connector).await.is_err());
    assert_eq!(calls.load(Ordering::Acquire), 0);
}
