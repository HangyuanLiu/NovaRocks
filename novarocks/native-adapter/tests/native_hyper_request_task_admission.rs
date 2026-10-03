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

//! Actual request admission and the concrete Hyper Pipe/Send task lifetimes.
//! The fixture prepays one pair's TaskCells, lease wrappers and grant/carrier
//! metadata from one Worker wallet. Provider/control ledgers, connection tasks,
//! sockets, runtime/scheduler and wire metadata are separately owned test inputs.

use bytes::Bytes;
use hyper::body::{Body, Frame, SizeHint};
use hyper::http::{Method, Request, Response};
use hyper::rt::{
    ClientRequestAdmission, ClientRequestAdmissionProvider, ClientRequestTaskGrant,
    ClientRequestTaskKind, ClientRequestTaskLease, Executor, SplitClientExecutor,
};
use hyper_util::rt::TokioIo;
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::convert::Infallible;
use std::error::Error;
use std::future::Future;
use std::io;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

const WATCHDOG: Duration = Duration::from_secs(5);
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
thread_local! { static CAPTURE_GRANT: Cell<bool> = const { Cell::new(false) }; }
static GRANT_POINTER: AtomicUsize = AtomicUsize::new(0);
static GRANT_BYTES: AtomicUsize = AtomicUsize::new(0);
static GRANT_FREED: AtomicBool = AtomicBool::new(false);
struct Probe;
fn record(pointer: *mut u8, bytes: usize) {
    if CAPTURE_GRANT.try_with(Cell::get).unwrap_or(false) {
        GRANT_BYTES.store(bytes, Ordering::SeqCst);
        GRANT_POINTER.store(pointer as usize, Ordering::SeqCst);
    }
}
// SAFETY: Forward unchanged to System. Only the actual grant Arc construction
// is tagged. Deallocation can occur on a different thread; recorded addresses
// are never dereferenced and the records do not allocate.
unsafe impl GlobalAlloc for Probe {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        record(p, layout.size());
        p
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc_zeroed(layout) };
        record(p, layout.size());
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        unsafe { System.dealloc(p, layout) };
        if GRANT_POINTER.load(Ordering::SeqCst) == p as usize {
            GRANT_FREED.store(true, Ordering::SeqCst);
        }
    }
    unsafe fn realloc(&self, p: *mut u8, layout: Layout, bytes: usize) -> *mut u8 {
        let next = unsafe { System.realloc(p, layout, bytes) };
        if GRANT_POINTER.load(Ordering::SeqCst) == p as usize {
            GRANT_FREED.store(true, Ordering::SeqCst);
        }
        record(next, bytes);
        next
    }
}
#[global_allocator]
static ALLOCATOR: Probe = Probe;
fn arc_bytes<T>() -> usize {
    Layout::new::<[AtomicUsize; 2]>()
        .extend(Layout::new::<T>())
        .unwrap()
        .0
        .pad_to_align()
        .size()
}
struct Grant {
    pipe: usize,
    send: usize,
    elected: AtomicU8,
}
impl ClientRequestTaskGrant for Grant {
    fn task_allocation_capacity_bound(&self, kind: ClientRequestTaskKind) -> io::Result<usize> {
        Ok(match kind {
            ClientRequestTaskKind::Pipe => self.pipe,
            ClientRequestTaskKind::Send => self.send,
        })
    }
    fn elect_dispatch(&self, kind: ClientRequestTaskKind, actual: usize) -> io::Result<()> {
        if actual > self.task_allocation_capacity_bound(kind)? {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let bit = match kind {
            ClientRequestTaskKind::Pipe => 1,
            ClientRequestTaskKind::Send => 2,
        };
        if self.elected.fetch_or(bit, Ordering::SeqCst) & bit != 0 {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        Ok(())
    }
}
// Grant deliberately has no release Drop. Only the original Bytes exit returns
// the pair, after ClientRequestTaskLease has destroyed its final grant Arc.
struct Slot {
    available: AtomicBool,
    calls: AtomicUsize,
    exits: AtomicUsize,
    early_exit: AtomicBool,
    pipe: usize,
    send: usize,
    total: usize,
    budget: Arc<ResultRetainedBudget>,
}
struct SlotExit {
    slot: Arc<Slot>,
    credit: Option<ResultWriteCredit>,
}
impl Drop for SlotExit {
    fn drop(&mut self) {
        self.slot
            .early_exit
            .store(!GRANT_FREED.load(Ordering::SeqCst), Ordering::SeqCst);
        drop(self.credit.take());
        self.slot.exits.fetch_add(1, Ordering::SeqCst);
        self.slot.available.store(true, Ordering::SeqCst);
    }
}
struct Provider(Arc<Slot>);
impl ClientRequestAdmissionProvider for Provider {
    fn try_acquire(&self, _: &Method) -> io::Result<ClientRequestTaskLease> {
        let slot = &self.0;
        slot.calls.fetch_add(1, Ordering::SeqCst);
        slot.available
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
            .map_err(|_| io::ErrorKind::WouldBlock)?;
        let credit = match slot
            .budget
            .try_reserve_process(slot.total)
            .map_err(|_| io::ErrorKind::InvalidInput)?
        {
            ResultWriteAdmission::Granted(credit) => credit,
            ResultWriteAdmission::Blocked => {
                slot.available.store(true, Ordering::SeqCst);
                return Err(io::ErrorKind::WouldBlock.into());
            }
        };
        GRANT_POINTER.store(0, Ordering::SeqCst);
        GRANT_FREED.store(false, Ordering::SeqCst);
        CAPTURE_GRANT.with(|v| v.set(true));
        let grant = Arc::new(Grant {
            pipe: slot.pipe,
            send: slot.send,
            elected: AtomicU8::new(0),
        });
        CAPTURE_GRANT.with(|v| v.set(false));
        assert_eq!(GRANT_BYTES.load(Ordering::SeqCst), arc_bytes::<Grant>());
        let original = Bytes::from_owner_with_exit_guard(
            Bytes::new(),
            SlotExit {
                slot: slot.clone(),
                credit: Some(credit),
            },
        );
        Ok(ClientRequestTaskLease::new(grant, original))
    }
}
#[derive(Default)]
struct Tasks {
    handles: Mutex<Vec<(Option<ClientRequestTaskKind>, JoinHandle<()>)>>,
}
impl Tasks {
    fn take(&self, kind: ClientRequestTaskKind) -> JoinHandle<()> {
        let mut handles = self.handles.lock().unwrap();
        let index = handles
            .iter()
            .position(|(role, _)| *role == Some(kind))
            .expect("actual typed request task handle");
        handles.swap_remove(index).1
    }
    fn derived(&self) -> usize {
        self.handles
            .lock()
            .unwrap()
            .iter()
            .filter(|(kind, _)| kind.is_some())
            .count()
    }
    async fn join(&self) {
        loop {
            let handles = std::mem::take(&mut *self.handles.lock().unwrap());
            if handles.is_empty() {
                return;
            }
            for (_, handle) in handles {
                tokio::time::timeout(WATCHDOG, handle)
                    .await
                    .expect("actual typed task did not exit")
                    .unwrap();
            }
        }
    }
}
#[derive(Clone)]
struct Typed {
    slot: Arc<Slot>,
    tasks: Arc<Tasks>,
    lease: Option<(ClientRequestTaskKind, ClientRequestTaskLease)>,
}
impl<F: Future<Output = ()> + Send + 'static> Executor<F> for Typed {
    fn task_allocation_capacity_bound() -> io::Result<usize> {
        tokio::runtime::Handle::task_allocation_capacity_bound::<F>()
    }
    fn client_request_admission(&self) -> io::Result<Option<ClientRequestAdmission>> {
        // Provider and control metadata are ordinary, independently owned
        // fixture inputs. The pair grant and actual request task graph are prepaid.
        Ok(Some(ClientRequestAdmission::new(
            Arc::new(Provider(self.slot.clone())),
            Bytes::new(),
        )))
    }
    fn supports_client_request_task_lease() -> bool {
        true
    }
    fn with_client_request_task_lease(
        &self,
        kind: ClientRequestTaskKind,
        lease: ClientRequestTaskLease,
    ) -> io::Result<Self> {
        Ok(Self {
            slot: self.slot.clone(),
            tasks: self.tasks.clone(),
            lease: Some((kind, lease)),
        })
    }
    fn try_execute(&self, future: F) -> io::Result<()> {
        let handle = match &self.lease {
            Some((kind, lease)) => {
                // Elect before creating the paid owner wrapper or TaskCell.
                let actual = tokio::runtime::Handle::task_allocation_capacity_bound::<F>()?;
                lease.elect_dispatch(*kind, actual)?;
                let owner = lease.clone().into_task_owner();
                tokio::runtime::Handle::current().spawn_with_task_owner(future, owner)?
            }
            None => tokio::spawn(future),
        };
        self.tasks
            .handles
            .lock()
            .unwrap()
            .push((self.lease.as_ref().map(|(kind, _)| *kind), handle));
        Ok(())
    }
    fn execute(&self, future: F) {
        self.try_execute(future).expect("fixture typed dispatch");
    }
}
#[derive(Clone, Default)]
struct Legacy;
impl<F: Future<Output = ()> + Send + 'static> Executor<F> for Legacy {
    fn execute(&self, future: F) {
        drop(tokio::spawn(future));
    }
}
type Split = SplitClientExecutor<Legacy, Typed>;
impl Slot {
    fn new() -> Arc<Self> {
        let bounds = Split::allocation_capacity_bounds::<TestBody, TokioIo<TcpStream>>().unwrap();
        let total = bounds
            .pipe
            .checked_add(bounds.send)
            .unwrap()
            .checked_add(
                2 * ClientRequestTaskLease::task_owner_metadata_allocation_capacity_bound(),
            )
            .unwrap()
            .checked_add(arc_bytes::<Grant>())
            .unwrap()
            .checked_add(Bytes::owner_with_exit_guard_metadata_size::<Bytes, SlotExit>())
            .unwrap();
        Arc::new(Self {
            available: AtomicBool::new(true),
            calls: AtomicUsize::new(0),
            exits: AtomicUsize::new(0),
            early_exit: AtomicBool::new(false),
            pipe: bounds.pipe,
            send: bounds.send,
            total,
            budget: ResultRetainedBudget::new(NonZeroUsize::new(total).unwrap()),
        })
    }
    fn held(&self) {
        assert!(!self.available.load(Ordering::SeqCst));
        assert!(matches!(
            self.budget.try_reserve_process(1).unwrap(),
            ResultWriteAdmission::Blocked
        ));
    }
    fn returned(&self) {
        assert!(self.available.load(Ordering::SeqCst));
        assert!(!self.early_exit.load(Ordering::SeqCst));
        assert_eq!(self.exits.load(Ordering::SeqCst), 1);
        let ResultWriteAdmission::Granted(credit) =
            self.budget.try_reserve_process(self.total).unwrap()
        else {
            panic!("original pair still held")
        };
        drop(credit);
    }
}
#[derive(Default)]
struct BodyProbe {
    size: AtomicUsize,
    end: AtomicUsize,
    poll: AtomicUsize,
    drop: AtomicUsize,
    early_drop: AtomicBool,
}
impl BodyProbe {
    fn untouched(&self) {
        assert_eq!(self.size.load(Ordering::SeqCst), 0);
        assert_eq!(self.end.load(Ordering::SeqCst), 0);
        assert_eq!(self.poll.load(Ordering::SeqCst), 0);
    }
}
struct TestBody {
    receiver: Option<oneshot::Receiver<Bytes>>,
    probe: Arc<BodyProbe>,
    slot: Arc<Slot>,
    require_held: bool,
}
impl Drop for TestBody {
    fn drop(&mut self) {
        self.probe.drop.fetch_add(1, Ordering::SeqCst);
        if self.require_held && self.slot.available.load(Ordering::SeqCst) {
            self.probe.early_drop.store(true, Ordering::SeqCst);
        }
    }
}
impl Body for TestBody {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        self.probe.poll.fetch_add(1, Ordering::SeqCst);
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
    fn size_hint(&self) -> SizeHint {
        self.probe.size.fetch_add(1, Ordering::SeqCst);
        SizeHint::with_exact(7)
    }
    fn is_end_stream(&self) -> bool {
        self.probe.end.fetch_add(1, Ordering::SeqCst);
        self.receiver.is_none()
    }
}
fn request(
    slot: &Arc<Slot>,
    method: Method,
    require_held: bool,
) -> (Request<TestBody>, oneshot::Sender<Bytes>, Arc<BodyProbe>) {
    let (send, receiver) = oneshot::channel();
    let probe = Arc::new(BodyProbe::default());
    let uri = if method == Method::CONNECT {
        "localhost:80"
    } else {
        "http://localhost/admission"
    };
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .body(TestBody {
            receiver: Some(receiver),
            probe: probe.clone(),
            slot: slot.clone(),
            require_held,
        })
        .unwrap();
    (request, send, probe)
}
async fn wait(f: impl Fn() -> bool) {
    tokio::time::timeout(WATCHDOG, async {
        while !f() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual event did not occur");
}
fn error_kind(mut error: &(dyn Error + 'static)) -> Option<io::ErrorKind> {
    loop {
        if let Some(error) = error.downcast_ref::<io::Error>() {
            return Some(error.kind());
        }
        error = error.source()?;
    }
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
                let mut input = request.into_body();
                let mut bytes = Vec::new();
                while let Some(data) = input.data().await {
                    let data = data.unwrap();
                    bytes.extend_from_slice(&data);
                    input.flow_control().release_capacity(data.len()).unwrap();
                }
                assert_eq!(bytes, b"payload");
                response.send_response(Response::new(()), true).unwrap();
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
async fn connect(
    slot: &Arc<Slot>,
    tasks: &Arc<Tasks>,
) -> (
    hyper::client::conn::http2::SendRequest<TestBody>,
    hyper::client::conn::http2::Connection<TokioIo<TcpStream>, TestBody, Split>,
    Arc<AtomicUsize>,
    JoinHandle<()>,
) {
    let (address, heads, peer) = peer().await;
    let io = TcpStream::connect(address).await.unwrap();
    let typed = Typed {
        slot: slot.clone(),
        tasks: tasks.clone(),
        lease: None,
    };
    let mut builder = hyper::client::conn::http2::Builder::new(Split::new(Legacy, typed));
    builder.initial_settings_deadline(std::time::Instant::now() + WATCHDOG);
    let (sender, connection) = builder.handshake(TokioIo::new(io)).await.unwrap();
    (sender, connection, heads, peer)
}
async fn finish(
    connection: JoinHandle<Result<(), hyper::Error>>,
    tasks: &Arc<Tasks>,
    peer: JoinHandle<()>,
) {
    tokio::time::timeout(WATCHDOG, connection)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    tasks.join().await;
    tokio::time::timeout(WATCHDOG, peer)
        .await
        .expect("actual peer IO did not exit")
        .unwrap();
}

#[tokio::test]
async fn actual_full_pair_refuses_synchronously_before_body_inspection_and_extra_headers() {
    let _serial = SERIAL.lock().await;
    let slot = Slot::new();
    let tasks = Arc::new(Tasks::default());
    let (mut sender, connection, heads, peer) = connect(&slot, &tasks).await;
    let connection = tokio::spawn(connection);
    let (first, release, first_probe) = request(&slot, Method::POST, true);
    let first = sender.send_request(first);
    slot.held();
    wait(|| heads.load(Ordering::SeqCst) == 1 && tasks.derived() == 2).await;
    let (second, _release, refused_probe) = request(&slot, Method::POST, false);
    let refused = sender.send_request(second);
    refused_probe.untouched();
    assert_eq!(slot.calls.load(Ordering::SeqCst), 2);
    let error = tokio::time::timeout(WATCHDOG, refused)
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error_kind(&error), Some(io::ErrorKind::WouldBlock));
    release.send(Bytes::from_static(b"payload")).unwrap();
    let response = tokio::time::timeout(WATCHDOG, first)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), 200);
    drop(response);
    drop(sender);
    finish(connection, &tasks, peer).await;
    assert_eq!(heads.load(Ordering::SeqCst), 1);
    assert!(!first_probe.early_drop.load(Ordering::SeqCst));
    slot.returned();
}

#[tokio::test]
async fn actual_connect_refuses_before_provider_body_and_derived_tasks() {
    let _serial = SERIAL.lock().await;
    let slot = Slot::new();
    let tasks = Arc::new(Tasks::default());
    let (mut sender, connection, heads, peer) = connect(&slot, &tasks).await;
    let connection = tokio::spawn(connection);
    let (request, _release, probe) = request(&slot, Method::CONNECT, false);
    let refused = sender.send_request(request);
    assert_eq!(slot.calls.load(Ordering::SeqCst), 0);
    probe.untouched();
    let error = tokio::time::timeout(WATCHDOG, refused)
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error_kind(&error), Some(io::ErrorKind::Unsupported));
    assert_eq!(tasks.derived(), 0);
    drop(sender);
    finish(connection, &tasks, peer).await;
    assert_eq!(heads.load(Ordering::SeqCst), 0);
    assert!(slot.available.load(Ordering::SeqCst));
    assert_eq!(slot.exits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn actual_queued_cancel_drops_body_before_its_original_pair() {
    let _serial = SERIAL.lock().await;
    let slot = Slot::new();
    let tasks = Arc::new(Tasks::default());
    let (mut sender, connection, heads, peer) = connect(&slot, &tasks).await;
    // The outer real ClientTask is not polled until cancellation has removed
    // its callback receiver. The independent protocol task is still actual IO.
    let (request, _release, probe) = request(&slot, Method::POST, true);
    let response = sender.send_request(request);
    probe.untouched();
    slot.held();
    drop(response);
    slot.held();
    assert_eq!(probe.drop.load(Ordering::SeqCst), 0);
    let connection = tokio::spawn(connection);
    wait(|| probe.drop.load(Ordering::SeqCst) == 1).await;
    assert!(!probe.early_drop.load(Ordering::SeqCst));
    probe.untouched();
    assert_eq!(tasks.derived(), 0);
    drop(sender);
    finish(connection, &tasks, peer).await;
    assert_eq!(heads.load(Ordering::SeqCst), 0);
    slot.returned();
}

#[tokio::test]
async fn actual_pipe_send_completed_join_and_abort_aliases_hold_pair_until_last_cells_exit() {
    let _serial = SERIAL.lock().await;
    let slot = Slot::new();
    let tasks = Arc::new(Tasks::default());
    let (mut sender, connection, heads, peer) = connect(&slot, &tasks).await;
    let connection = tokio::spawn(connection);
    let (request, release, probe) = request(&slot, Method::POST, true);
    let response = sender.send_request(request);
    wait(|| heads.load(Ordering::SeqCst) == 1 && tasks.derived() == 2).await;
    let pipe = tasks.take(ClientRequestTaskKind::Pipe);
    let send = tasks.take(ClientRequestTaskKind::Send);
    let pipe_abort = pipe.abort_handle();
    let send_abort = send.abort_handle();
    release.send(Bytes::from_static(b"payload")).unwrap();
    let response = tokio::time::timeout(WATCHDOG, response)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), 200);
    drop(response);
    wait(|| pipe.is_finished() && send.is_finished()).await;
    slot.held();
    assert!(!GRANT_FREED.load(Ordering::SeqCst));
    drop(sender);
    finish(connection, &tasks, peer).await;
    // Both tasks really completed; these untouched JoinHandles still retain
    // their TaskCells beyond future exit and beyond the actual TCP IO exit.
    slot.held();
    tokio::time::timeout(WATCHDOG, pipe).await.unwrap().unwrap();
    tokio::time::timeout(WATCHDOG, send).await.unwrap().unwrap();
    slot.held();
    drop(pipe_abort);
    slot.held();
    drop(send_abort);
    assert!(!probe.early_drop.load(Ordering::SeqCst));
    slot.returned();
}
