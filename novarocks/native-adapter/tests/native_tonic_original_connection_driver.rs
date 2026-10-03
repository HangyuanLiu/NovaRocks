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

//! Real Endpoint TCP/H2 attempts and the original live Tonic driver TaskCell.
//! The grant covers the queried driver task, driver metadata and Bytes carrier.
//! Hyper's independent IO task, channel worker, socket/TLS, executor and scheduler
//! storage remain separate. Watchdogs detect hangs; they do not prove exit.

use bytes::Bytes;
use hyper::http::{Request, Response, Uri};
use hyper::rt::Executor;
use hyper_util::rt::TokioIo;
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::error::Error;
use std::future::Future;
use std::io;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::transport::{Endpoint, Http2ConnectionConfig, OriginalConnectionDriver};
use tower::{Service, ServiceExt};

const WATCHDOG: Duration = Duration::from_secs(5);
const PHASE: Duration = Duration::from_secs(2);
const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct Record {
    pointer: AtomicUsize,
    bytes: AtomicUsize,
    freed: AtomicBool,
}
impl Record {
    const fn new() -> Self {
        Self {
            pointer: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
            freed: AtomicBool::new(false),
        }
    }
}
static RECORDS: [Record; 4] = [const { Record::new() }; 4];
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
thread_local! { static TRACK: Cell<bool> = const { Cell::new(false) }; }
fn allocated(pointer: *mut u8, layout: Layout) {
    if TRACK.try_with(Cell::get).unwrap_or(false) {
        let index = ALLOCATIONS.fetch_add(1, Ordering::SeqCst);
        if let Some(record) = RECORDS.get(index) {
            record.bytes.store(layout.size(), Ordering::SeqCst);
            record.pointer.store(pointer as usize, Ordering::SeqCst);
        }
    }
}
struct Probe;
// SAFETY: Calls delegate unchanged to System. Fixed atomic address records do
// not dereference allocations, allocate recursively or require the freeing thread.
unsafe impl GlobalAlloc for Probe {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        allocated(pointer, layout);
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        allocated(pointer, layout);
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        for record in &RECORDS {
            if record.pointer.load(Ordering::SeqCst) == pointer as usize {
                record.freed.store(true, Ordering::SeqCst);
            }
        }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, bytes: usize) -> *mut u8 {
        let next = unsafe { System.realloc(pointer, layout, bytes) };
        for record in &RECORDS {
            if record.pointer.load(Ordering::SeqCst) == pointer as usize {
                record.freed.store(true, Ordering::SeqCst);
            }
        }
        allocated(
            next,
            Layout::from_size_align(bytes, layout.align()).unwrap(),
        );
        next
    }
}
#[global_allocator]
static ALLOCATOR: Probe = Probe;
struct Tracking;
impl Drop for Tracking {
    fn drop(&mut self) {
        TRACK.with(|v| v.set(false));
    }
}
fn measure<T>(f: impl FnOnce() -> T) -> T {
    assert!(!TRACK.with(|v| v.replace(true)));
    let tracking = Tracking;
    let output = f();
    drop(tracking);
    output
}
fn reset_records() {
    assert!(!TRACK.with(Cell::get));
    ALLOCATIONS.store(0, Ordering::SeqCst);
    for record in &RECORDS {
        record.pointer.store(0, Ordering::SeqCst);
        record.bytes.store(0, Ordering::SeqCst);
        record.freed.store(false, Ordering::SeqCst);
    }
}
fn metadata_freed() -> bool {
    RECORDS
        .iter()
        .all(|r| r.pointer.load(Ordering::SeqCst) == 0 || r.freed.load(Ordering::SeqCst))
}
#[derive(Default)]
struct Ledger {
    exited: AtomicBool,
    early_metadata_exit: AtomicBool,
}
struct Exit {
    credit: Option<ResultWriteCredit>,
    ledger: Arc<Ledger>,
}
impl Drop for Exit {
    fn drop(&mut self) {
        // A failing source mutation records one ordinary assertion failure.
        self.ledger
            .early_metadata_exit
            .store(!metadata_freed(), Ordering::SeqCst);
        self.ledger.exited.store(true, Ordering::SeqCst);
        drop(self.credit.take());
    }
}
struct Funding {
    driver: OriginalConnectionDriver,
    budget: Arc<ResultRetainedBudget>,
    total: usize,
    ledger: Arc<Ledger>,
}
impl Funding {
    fn new(short: bool) -> Self {
        reset_records();
        let bound = OriginalConnectionDriver::task_allocation_capacity_bound().unwrap();
        let metadata = OriginalConnectionDriver::metadata_allocation_capacity_bound().unwrap();
        let carrier = Bytes::owner_with_exit_guard_metadata_size::<Bytes, Exit>();
        let total = bound
            .checked_add(metadata)
            .unwrap()
            .checked_add(carrier)
            .unwrap();
        let budget = ResultRetainedBudget::new(NonZeroUsize::new(total).unwrap());
        let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(total).unwrap()
        else {
            panic!("pregrant actual queried driver and metadata before their allocations");
        };
        let ledger = Arc::new(Ledger::default());
        let owner = Bytes::from_owner_with_exit_guard(
            Bytes::new(),
            Exit {
                credit: Some(credit),
                ledger: ledger.clone(),
            },
        );
        let driver = measure(|| {
            OriginalConnectionDriver::with_original(if short { 1 } else { bound }, owner)
        })
        .unwrap();
        assert!(ALLOCATIONS.load(Ordering::SeqCst) <= RECORDS.len());
        let requested: usize = RECORDS.iter().map(|r| r.bytes.load(Ordering::SeqCst)).sum();
        assert_eq!(
            requested, metadata,
            "actual Core Arc and prewarmed mutex backing"
        );
        Self {
            driver,
            budget,
            total,
            ledger,
        }
    }
    fn held(&self) {
        assert!(!self.ledger.exited.load(Ordering::SeqCst));
        assert!(matches!(
            self.budget.try_reserve_process(1).unwrap(),
            ResultWriteAdmission::Blocked
        ));
    }
    fn finish(self) {
        drop(self.driver);
        assert!(self.ledger.exited.load(Ordering::SeqCst));
        assert!(!self.ledger.early_metadata_exit.load(Ordering::SeqCst));
        assert!(metadata_freed());
        let ResultWriteAdmission::Granted(credit) =
            self.budget.try_reserve_process(self.total).unwrap()
        else {
            panic!("original driver credit remains held after all actual owners exit");
        };
        drop(credit);
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
                break;
            }
            for handle in handles {
                tokio::time::timeout(WATCHDOG, handle)
                    .await
                    .expect("independent actual executor task did not exit")
                    .unwrap();
            }
        }
    }
    fn count(&self) -> usize {
        self.0.lock().unwrap().len()
    }
}
fn endpoint(
    address: std::net::SocketAddr,
    executor: &JoinedExecutor,
    driver: Option<OriginalConnectionDriver>,
) -> Endpoint {
    let endpoint = Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .executor(executor.clone());
    endpoint.http2_connection_factory(move || {
        Ok::<_, io::Error>(Http2ConnectionConfig {
            connection_driver: driver.clone(),
            initial_settings_timeout: Some(PHASE),
            ..Default::default()
        })
    })
}
fn connector(
    address: std::net::SocketAddr,
    calls: Arc<AtomicUsize>,
) -> impl Service<
    Uri,
    Response = TokioIo<TcpStream>,
    Error = io::Error,
    Future = impl Future<Output = io::Result<TokioIo<TcpStream>>> + Send,
> + Clone
+ use<> {
    let calls = calls.clone();
    tower::service_fn(move |_: Uri| {
        calls.fetch_add(1, Ordering::SeqCst);
        async move { TcpStream::connect(address).await.map(TokioIo::new) }
    })
}
fn error_kind(mut error: &(dyn Error + 'static)) -> Option<io::ErrorKind> {
    loop {
        if let Some(io) = error.downcast_ref::<io::Error>() {
            return Some(io.kind());
        }
        error = error.source()?;
    }
}
async fn peer() -> (std::net::SocketAddr, oneshot::Sender<()>, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, mut stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        let mut connection = h2::server::handshake(io).await.unwrap();
        loop {
            tokio::select! {
                _ = &mut stopped => break,
                request = connection.accept() => match request {
                    Some(Ok((_request, mut respond))) => {
                        respond.send_response(Response::builder().header("x-driver", "actual-tcp-h2").body(()).unwrap(), true).unwrap();
                    }
                    Some(Err(_)) | None => break,
                }
            }
        }
        // The owned connection and real socket exit here, not at a watchdog.
        drop(connection);
    });
    (address, stop, task)
}
async fn join_peer(peer: JoinHandle<()>) {
    tokio::time::timeout(WATCHDOG, peer)
        .await
        .expect("actual peer task did not exit")
        .unwrap();
}
async fn roundtrip(channel: tonic::transport::Channel) {
    let response = tokio::time::timeout(
        WATCHDOG,
        channel.oneshot(Request::new(tonic::body::empty_body())),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(response.headers()["x-driver"], "actual-tcp-h2");
    drop(response);
}
async fn completed(handle: &JoinHandle<()>) {
    tokio::time::timeout(WATCHDOG, async {
        while !handle.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual live driver did not complete");
}

#[tokio::test]
async fn actual_metadata_constructor_and_clone_have_exact_requested_backings() {
    let _serial = SERIAL.lock().await;
    let funding = Funding::new(false);
    let before = ALLOCATIONS.load(Ordering::SeqCst);
    let clone = measure(|| funding.driver.clone());
    assert!(measure(|| clone.take_task_handle()).is_none());
    assert!(!measure(|| clone.task_finished()));
    assert_eq!(ALLOCATIONS.load(Ordering::SeqCst), before);
    funding.held();
    drop(clone);
    funding.finish();
}

#[tokio::test]
async fn actual_short_task_bound_refuses_before_tcp_connector() {
    let _serial = SERIAL.lock().await;
    let funding = Funding::new(true);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let executor = JoinedExecutor::default();
    let endpoint = endpoint(address, &executor, Some(funding.driver.clone()));
    let calls = Arc::new(AtomicUsize::new(0));
    let error = endpoint
        .connect_with_connector(connector(address, calls.clone()))
        .await
        .unwrap_err();
    assert_eq!(error_kind(&error), Some(io::ErrorKind::InvalidInput));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(funding.driver.take_task_handle().is_none());
    drop(endpoint);
    drop(listener);
    executor.join().await;
    funding.finish();
}

#[tokio::test]
async fn actual_cloned_token_second_attempt_refuses_before_tcp_connector() {
    let _serial = SERIAL.lock().await;
    let funding = Funding::new(false);
    let (address, stop, peer) = peer().await;
    let executor = JoinedExecutor::default();
    let endpoint = endpoint(address, &executor, Some(funding.driver.clone()));
    let calls = Arc::new(AtomicUsize::new(0));
    let channel = endpoint
        .connect_with_connector(connector(address, calls.clone()))
        .await
        .unwrap();
    roundtrip(channel.clone()).await;
    let error = endpoint
        .clone()
        .connect_with_connector(connector(address, calls.clone()))
        .await
        .unwrap_err();
    assert_eq!(error_kind(&error), Some(io::ErrorKind::WouldBlock));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let handle = funding
        .driver
        .take_task_handle()
        .expect("real installed driver must expose its actual task");
    assert!(funding.driver.take_task_handle().is_none());
    funding.held();
    drop(channel);
    drop(endpoint);
    let _ = stop.send(());
    join_peer(peer).await;
    tokio::time::timeout(WATCHDOG, handle)
        .await
        .unwrap()
        .unwrap();
    executor.join().await;
    funding.finish();
}

#[tokio::test]
async fn actual_dial_error_abandons_token_without_reselecting_it() {
    let _serial = SERIAL.lock().await;
    let funding = Funding::new(false);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let executor = JoinedExecutor::default();
    let endpoint = endpoint(address, &executor, Some(funding.driver.clone()));
    let calls = Arc::new(AtomicUsize::new(0));
    let error = endpoint
        .connect_with_connector(connector(address, calls.clone()))
        .await
        .unwrap_err();
    assert_eq!(error_kind(&error), Some(io::ErrorKind::ConnectionRefused));
    let error = endpoint
        .connect_with_connector(connector(address, calls.clone()))
        .await
        .unwrap_err();
    assert_eq!(error_kind(&error), Some(io::ErrorKind::WouldBlock));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(funding.driver.take_task_handle().is_none());
    drop(endpoint);
    executor.join().await;
    funding.finish();
}

#[tokio::test]
async fn actual_partial_handshake_cancel_abandons_token_and_exits_tcp_io() {
    let _serial = SERIAL.lock().await;
    let funding = Funding::new(false);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (ready, received) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut preface = [0; 24];
        socket.read_exact(&mut preface).await.unwrap();
        assert_eq!(&preface, PREFACE);
        ready.send(()).unwrap();
        let mut remaining = Vec::new();
        socket.read_to_end(&mut remaining).await.unwrap();
        // Actual EOF proves the canceled phase released its TCP output.
    });
    let executor = JoinedExecutor::default();
    let endpoint = endpoint(address, &executor, Some(funding.driver.clone()));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut connecting =
        Box::pin(endpoint.connect_with_connector(connector(address, calls.clone())));
    tokio::time::timeout(WATCHDOG, async {
        tokio::select! {
            result = &mut connecting => panic!("missing peer SETTINGS cannot complete: {result:?}"),
            result = received => result.unwrap(),
        }
    })
    .await
    .unwrap();
    drop(connecting);
    join_peer(peer).await;
    let error = endpoint
        .connect_with_connector(connector(address, calls.clone()))
        .await
        .unwrap_err();
    assert_eq!(error_kind(&error), Some(io::ErrorKind::WouldBlock));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(funding.driver.take_task_handle().is_none());
    drop(endpoint);
    executor.join().await;
    funding.finish();
}

#[tokio::test]
async fn actual_completed_unpolled_join_and_abort_handles_hold_original_task_credit() {
    let _serial = SERIAL.lock().await;
    let funding = Funding::new(false);
    let (address, stop, peer) = peer().await;
    let executor = JoinedExecutor::default();
    let endpoint = endpoint(address, &executor, Some(funding.driver.clone()));
    let calls = Arc::new(AtomicUsize::new(0));
    let channel = endpoint
        .connect_with_connector(connector(address, calls.clone()))
        .await
        .unwrap();
    roundtrip(channel.clone()).await;
    let handle = funding.driver.take_task_handle().unwrap();
    let abort = handle.abort_handle();
    drop(channel);
    drop(endpoint);
    let _ = stop.send(());
    join_peer(peer).await;
    completed(&handle).await;
    executor.join().await;
    let Funding {
        driver,
        budget,
        total,
        ledger,
    } = funding;
    drop(driver);
    assert!(metadata_freed());
    assert!(
        !ledger.exited.load(Ordering::SeqCst),
        "completed but unpolled JoinHandle is a real TaskCell holder"
    );
    assert!(matches!(
        budget.try_reserve_process(1).unwrap(),
        ResultWriteAdmission::Blocked
    ));
    tokio::time::timeout(WATCHDOG, handle)
        .await
        .unwrap()
        .unwrap();
    assert!(
        !ledger.exited.load(Ordering::SeqCst),
        "AbortHandle still holds the actual completed task"
    );
    drop(abort);
    assert!(ledger.exited.load(Ordering::SeqCst));
    assert!(!ledger.early_metadata_exit.load(Ordering::SeqCst));
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(total).unwrap() else {
        panic!("last real task handle must return original credit")
    };
    drop(credit);
}

#[tokio::test]
async fn actual_none_driver_preserves_custom_executor_and_tcp_h2_roundtrip() {
    let _serial = SERIAL.lock().await;
    reset_records();
    let (address, stop, peer) = peer().await;
    let executor = JoinedExecutor::default();
    let endpoint = endpoint(address, &executor, None);
    let calls = Arc::new(AtomicUsize::new(0));
    let channel = endpoint
        .connect_with_connector(connector(address, calls.clone()))
        .await
        .unwrap();
    roundtrip(channel.clone()).await;
    assert!(
        executor.count() >= 2,
        "legacy driver and actual IO task use the selected executor"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    drop(channel);
    drop(endpoint);
    let _ = stop.send(());
    join_peer(peer).await;
    executor.join().await;
}
