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

//! Actual EOS request teardown before typed Send task dispatch. Only the request
//! pair, grant and task-owner metadata are prepaid by the Worker wallet here.
//! Provider/control metadata, protocol tasks, sockets and scheduler are fixture
//! inputs; this is neither a whole-connection nor an allocator-coverage proof.

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
use std::alloc::Layout;
use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

const WATCHDOG: Duration = Duration::from_secs(5);

#[derive(Default)]
struct DropGate {
    entered: Notify,
    release: Mutex<bool>,
    changed: Condvar,
    finished: AtomicBool,
    timed_out: AtomicBool,
}
impl DropGate {
    fn release(&self) {
        *self.release.lock().unwrap() = true;
        self.changed.notify_all();
    }
    fn block_drop(&self) {
        self.entered.notify_one();
        let deadline = Instant::now() + WATCHDOG;
        let mut released = self.release.lock().unwrap();
        while !*released {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                self.timed_out.store(true, Ordering::SeqCst);
                break;
            };
            released = self.changed.wait_timeout(released, remaining).unwrap().0;
        }
        self.finished.store(true, Ordering::SeqCst);
    }
}
// A failed assertion must still unblock the actual destructor and runtime.
struct ReleaseOnDrop(Arc<DropGate>);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct EosBody(Arc<DropGate>);
impl Drop for EosBody {
    fn drop(&mut self) {
        self.0.block_drop();
    }
}
impl Body for EosBody {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        panic!("an EOS request must not poll its body")
    }
    fn is_end_stream(&self) -> bool {
        true
    }
    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(0)
    }
}

fn arc_bytes<T>() -> usize {
    Layout::new::<[AtomicUsize; 2]>()
        .extend(Layout::new::<T>())
        .unwrap()
        .0
        .pad_to_align()
        .size()
}
struct Grant {
    bounds: [usize; 2],
    elected: AtomicU8,
}
fn role(kind: ClientRequestTaskKind) -> usize {
    match kind {
        ClientRequestTaskKind::Pipe => 0,
        ClientRequestTaskKind::Send => 1,
    }
}
impl ClientRequestTaskGrant for Grant {
    fn task_allocation_capacity_bound(&self, kind: ClientRequestTaskKind) -> io::Result<usize> {
        Ok(self.bounds[role(kind)])
    }
    fn elect_dispatch(&self, kind: ClientRequestTaskKind, actual: usize) -> io::Result<()> {
        if actual > self.bounds[role(kind)] {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let bit = 1 << role(kind);
        if self.elected.fetch_or(bit, Ordering::SeqCst) & bit != 0 {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        Ok(())
    }
}
struct Slot {
    available: AtomicBool,
    exits: AtomicUsize,
    total: usize,
    bounds: [usize; 2],
    budget: Arc<ResultRetainedBudget>,
}
struct SlotExit {
    slot: Arc<Slot>,
    credit: Option<ResultWriteCredit>,
}
impl Drop for SlotExit {
    fn drop(&mut self) {
        drop(self.credit.take());
        self.slot.exits.fetch_add(1, Ordering::SeqCst);
        self.slot.available.store(true, Ordering::SeqCst);
    }
}
struct Provider(Arc<Slot>);
impl ClientRequestAdmissionProvider for Provider {
    fn try_acquire(&self, _: &Method) -> io::Result<ClientRequestTaskLease> {
        self.0
            .available
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
            .map_err(|_| io::ErrorKind::WouldBlock)?;
        let credit = match self
            .0
            .budget
            .try_reserve_process(self.0.total)
            .map_err(|_| io::ErrorKind::InvalidInput)?
        {
            ResultWriteAdmission::Granted(credit) => credit,
            ResultWriteAdmission::Blocked => {
                self.0.available.store(true, Ordering::SeqCst);
                return Err(io::ErrorKind::WouldBlock.into());
            }
        };
        let original = Bytes::from_owner_with_exit_guard(
            Bytes::new(),
            SlotExit {
                slot: self.0.clone(),
                credit: Some(credit),
            },
        );
        Ok(ClientRequestTaskLease::new(
            Arc::new(Grant {
                bounds: self.0.bounds,
                elected: AtomicU8::new(0),
            }),
            original,
        ))
    }
}
#[derive(Default)]
struct Tasks {
    handles: Mutex<Vec<JoinHandle<()>>>,
    sends: AtomicUsize,
}
#[derive(Clone)]
struct Typed {
    slot: Arc<Slot>,
    tasks: Arc<Tasks>,
    gate: Arc<DropGate>,
    lease: Option<(ClientRequestTaskKind, ClientRequestTaskLease)>,
}
impl<F: Future<Output = ()> + Send + 'static> Executor<F> for Typed {
    fn task_allocation_capacity_bound() -> io::Result<usize> {
        tokio::runtime::Handle::task_allocation_capacity_bound::<F>()
    }
    fn client_request_admission(&self) -> io::Result<Option<ClientRequestAdmission>> {
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
        let mut prepared = self.clone();
        prepared.lease = Some((kind, lease));
        Ok(prepared)
    }
    fn try_execute(&self, future: F) -> io::Result<()> {
        let handle = if let Some((kind, lease)) = &self.lease {
            assert_eq!(
                *kind,
                ClientRequestTaskKind::Send,
                "EOS spawned a Pipe task"
            );
            // A deterministic dispatch oracle: even a fast/canceled Send must
            // not receive the last pair aliases before EOS Body teardown ends.
            assert!(
                self.gate.finished.load(Ordering::SeqCst),
                "Send dispatched before the EOS Body destructor finished"
            );
            self.tasks.sends.fetch_add(1, Ordering::SeqCst);
            lease.elect_dispatch(
                *kind,
                tokio::runtime::Handle::task_allocation_capacity_bound::<F>()?,
            )?;
            tokio::runtime::Handle::current()
                .spawn_with_task_owner(future, lease.clone().into_task_owner())?
        } else {
            tokio::spawn(future)
        };
        self.tasks.handles.lock().unwrap().push(handle);
        Ok(())
    }
    fn execute(&self, future: F) {
        self.try_execute(future).expect("fixture typed dispatch");
    }
}
#[derive(Clone)]
struct Legacy;
impl<F: Future<Output = ()> + Send + 'static> Executor<F> for Legacy {
    fn execute(&self, future: F) {
        drop(tokio::spawn(future));
    }
}
type Split = SplitClientExecutor<Legacy, Typed>;

fn held(slot: &Slot) {
    assert!(!slot.available.load(Ordering::SeqCst));
    assert!(matches!(
        slot.budget.try_reserve_process(1).unwrap(),
        ResultWriteAdmission::Blocked
    ));
}
async fn joined<T>(handle: JoinHandle<T>) -> T {
    tokio::time::timeout(WATCHDOG, handle)
        .await
        .expect("actual task did not exit")
        .expect("actual task panicked")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn actual_eos_body_drop_finishes_before_send_dispatch_after_response_cancel() {
    let gate = Arc::new(DropGate::default());
    let _unblock = ReleaseOnDrop(gate.clone());
    let tasks = Arc::new(Tasks::default());
    let bounds = Split::allocation_capacity_bounds::<EosBody, TokioIo<TcpStream>>().unwrap();
    let total = bounds.pipe
        + bounds.send
        + 2 * ClientRequestTaskLease::task_owner_metadata_allocation_capacity_bound()
        + arc_bytes::<Grant>()
        + Bytes::owner_with_exit_guard_metadata_size::<Bytes, SlotExit>();
    let slot = Arc::new(Slot {
        available: AtomicBool::new(true),
        exits: AtomicUsize::new(0),
        total,
        bounds: [bounds.pipe, bounds.send],
        budget: ResultRetainedBudget::new(NonZeroUsize::new(total).unwrap()),
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        let mut connection = h2::server::handshake(io).await.unwrap();
        while let Some(request) = connection.accept().await {
            let (request, mut response) = request.unwrap();
            assert_eq!(request.uri().path(), "/eos");
            assert!(request.body().is_end_stream());
            // The wire response is immediate; no payload timing heuristic is
            // needed to observe the Send task's admission/dispatch boundary.
            response.send_response(Response::new(()), true).unwrap();
        }
    });
    let io = TcpStream::connect(address).await.unwrap();
    let typed = Typed {
        slot: slot.clone(),
        tasks: tasks.clone(),
        gate: gate.clone(),
        lease: None,
    };
    let mut builder = hyper::client::conn::http2::Builder::new(Split::new(Legacy, typed));
    builder.initial_settings_deadline(Instant::now() + WATCHDOG);
    let (mut sender, connection) = builder.handshake(TokioIo::new(io)).await.unwrap();
    let response = sender.send_request(
        Request::builder()
            .method(Method::POST)
            .uri("http://localhost/eos")
            .body(EosBody(gate.clone()))
            .unwrap(),
    );
    held(&slot);
    let driver = tokio::spawn(connection);
    tokio::time::timeout(WATCHDOG, gate.entered.notified())
        .await
        .expect("actual EOS Body Drop did not start");
    assert!(!gate.finished.load(Ordering::SeqCst));
    assert_eq!(tasks.sends.load(Ordering::SeqCst), 0);
    drop(response);
    // Canceling the response receiver does not retire the Body still being
    // destroyed on the actual ClientTask worker thread.
    held(&slot);
    gate.release();
    drop(sender);
    joined(driver).await.unwrap();
    let handles = std::mem::take(&mut *tasks.handles.lock().unwrap());
    for handle in handles {
        joined(handle).await;
    }
    joined(peer).await;
    assert!(gate.finished.load(Ordering::SeqCst));
    assert!(!gate.timed_out.load(Ordering::SeqCst));
    assert_eq!(tasks.sends.load(Ordering::SeqCst), 1);
    assert_eq!(slot.exits.load(Ordering::SeqCst), 1);
    assert!(slot.available.load(Ordering::SeqCst));
    let ResultWriteAdmission::Granted(credit) = slot.budget.try_reserve_process(total).unwrap()
    else {
        panic!("original request pair still held after all actual owners exited")
    };
    drop(credit);
}
