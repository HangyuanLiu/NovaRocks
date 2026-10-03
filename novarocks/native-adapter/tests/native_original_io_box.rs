// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Actual concrete IO Box and original carrier ordering only. Socket/runtime,
//! TLS internal allocations, trust material, reservation callbacks and test
//! ledgers are separate. System observes Rust Box deallocation, not TLS/RSS.

use bytes::Bytes;
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_native_trust::{
    AutomaticTlsMaterial, BoxedNativeIo, DeploymentId, NativeCallerSubject,
    NativeEndpointConnector, NativeIncomingAdapter, NativeIoDirection, NativeTransportMode,
    NativeTrust, OwnedNativeIo, ValidatedSharedSecret, native_io_box_layout,
};
use novarocks_secret::SecretValue;
use novarocks_types::NativeEndpoint;
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::io::{self, IoSlice};
use std::num::NonZeroUsize;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpListener;

thread_local! {
    static IO_PTRS: Cell<[usize; 2]> = const { Cell::new([0; 2]) };
    static IO_FREED: Cell<[usize; 2]> = const { Cell::new([0; 2]) };
    static CARRIER_PTRS: Cell<[usize; 2]> = const { Cell::new([0; 2]) };
    static CARRIER_FREED: Cell<[usize; 2]> = const { Cell::new([0; 2]) };
    static EXITS: Cell<[usize; 2]> = const { Cell::new([0; 2]) };
    static CAPTURE: Cell<Option<usize>> = const { Cell::new(None) };
    static MEASURE: Cell<bool> = const { Cell::new(false) };
    static CALLS: Cell<usize> = const { Cell::new(0) };
    static REQUESTED: Cell<usize> = const { Cell::new(0) };
}
fn put(cell: &'static std::thread::LocalKey<Cell<[usize; 2]>>, index: usize, value: usize) {
    let _ = cell.try_with(|c| {
        let mut values = c.get();
        values[index] = value;
        c.set(values);
    });
}
fn allocated(pointer: *mut u8, layout: Layout) {
    if MEASURE.try_with(Cell::get).unwrap_or(false) {
        let _ = CALLS.try_with(|c| c.set(c.get() + 1));
        let _ = REQUESTED.try_with(|c| c.set(c.get() + layout.size()));
    }
    if let Some(index) = CAPTURE.try_with(Cell::get).ok().flatten() {
        put(&CARRIER_PTRS, index, pointer as usize);
    }
}
struct Probe;
// SAFETY: All allocator operations forward unchanged to System. Fixed TLS
// cells observe addresses/layouts only and never dereference freed pointers.
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
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let next = unsafe { System.realloc(pointer, layout, size) };
        allocated(next, Layout::from_size_align(size, layout.align()).unwrap());
        next
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        let ios = IO_PTRS.try_with(Cell::get).unwrap_or([0; 2]);
        let carriers = CARRIER_PTRS.try_with(Cell::get).unwrap_or([0; 2]);
        for index in 0..2 {
            if ios[index] == pointer as usize {
                put(&IO_FREED, index, layout.size());
                put(&IO_PTRS, index, 0);
            }
            if carriers[index] == pointer as usize {
                put(&CARRIER_FREED, index, layout.size());
                put(&CARRIER_PTRS, index, 0);
            }
        }
    }
}
#[global_allocator]
static ALLOCATOR: Probe = Probe;

struct PhysicalExit {
    credit: Option<ResultWriteCredit>,
    index: usize,
    io_bytes: usize,
}
impl Drop for PhysicalExit {
    fn drop(&mut self) {
        assert_eq!(
            IO_FREED.with(Cell::get)[self.index],
            self.io_bytes,
            "actual concrete Box must be physically freed before original owner exits"
        );
        assert_eq!(
            CARRIER_FREED.with(Cell::get)[self.index],
            carrier_bytes(),
            "carrier backing must also exit before credit release"
        );
        put(&EXITS, self.index, 1);
        drop(self.credit.take());
    }
}
fn carrier_bytes() -> usize {
    Bytes::owner_with_exit_guard_metadata_size::<Bytes, PhysicalExit>()
}
struct Funded {
    owner: Bytes,
    budget: Arc<ResultRetainedBudget>,
    grant: usize,
    layout: Layout,
    index: usize,
}
impl Funded {
    fn new(layout: Layout, index: usize) -> Self {
        assert!(layout.size() > 0);
        put(&IO_PTRS, index, 0);
        put(&IO_FREED, index, 0);
        put(&CARRIER_PTRS, index, 0);
        put(&CARRIER_FREED, index, 0);
        put(&EXITS, index, 0);
        let grant = layout.size().checked_add(carrier_bytes()).unwrap();
        let budget = ResultRetainedBudget::new(NonZeroUsize::new(grant).unwrap());
        let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(grant).unwrap()
        else {
            panic!("pregrant actual concrete IO Box and original carrier before allocation");
        };
        CAPTURE.with(|c| c.set(Some(index)));
        let owner = Bytes::from_owner_with_exit_guard(
            Bytes::new(),
            PhysicalExit {
                credit: Some(credit),
                index,
                io_bytes: layout.size(),
            },
        );
        CAPTURE.with(|c| c.set(None));
        Self {
            owner,
            budget,
            grant,
            layout,
            index,
        }
    }
    fn wrap(&self, io: BoxedNativeIo) -> OwnedNativeIo {
        assert_eq!(
            Layout::for_value(io.as_ref()),
            self.layout,
            "public layout must match actual concrete dyn IO metadata"
        );
        put(
            &IO_PTRS,
            self.index,
            io.as_ref() as *const _ as *const () as usize,
        );
        no_allocation(|| OwnedNativeIo::new(io, Some(self.owner.clone())))
    }
    fn held(&self) {
        assert!(matches!(
            self.budget.try_reserve_process(1).unwrap(),
            ResultWriteAdmission::Blocked
        ));
        assert_eq!(EXITS.with(Cell::get)[self.index], 0);
    }
    fn finish(self) {
        drop(self.owner);
        assert_eq!(EXITS.with(Cell::get)[self.index], 1);
        let ResultWriteAdmission::Granted(credit) =
            self.budget.try_reserve_process(self.grant).unwrap()
        else {
            panic!("full original credit must return after actual IO/carrier exit");
        };
        drop(credit);
    }
}
fn no_allocation<T>(work: impl FnOnce() -> T) -> T {
    CALLS.with(|c| c.set(0));
    REQUESTED.with(|c| c.set(0));
    assert!(!MEASURE.with(|c| c.replace(true)));
    struct End;
    impl Drop for End {
        fn drop(&mut self) {
            MEASURE.with(|c| c.set(false));
        }
    }
    let end = End;
    let result = work();
    drop(end);
    assert_eq!(CALLS.with(Cell::get), 0);
    assert_eq!(REQUESTED.with(Cell::get), 0);
    result
}
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

#[test]
fn actual_plaintext_connector_and_acceptor_boxes_exit_before_original_credit() {
    let runtime = runtime();
    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint =
            NativeEndpoint::from_host_port("127.0.0.1", listener.local_addr().unwrap().port())
                .unwrap();
        let connector = NativeEndpointConnector::plaintext(endpoint);
        let incoming = NativeIncomingAdapter::plaintext();
        let client = Funded::new(
            native_io_box_layout(NativeTransportMode::Disabled, NativeIoDirection::Client),
            0,
        );
        let server = Funded::new(
            native_io_box_layout(NativeTransportMode::Disabled, NativeIoDirection::Server),
            1,
        );
        let (client_io, server_io) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(connector.connect(), async {
                let (stream, _) = listener.accept().await.unwrap();
                incoming.accept(stream).await
            })
        })
        .await
        .unwrap();
        let mut client_io = client.wrap(client_io.unwrap());
        let mut server_io = server.wrap(server_io.unwrap());
        assert!(client_io.is_write_vectored());
        client_io.write_all(b"native").await.unwrap();
        client_io.flush().await.unwrap();
        let mut data = [0; 6];
        server_io.read_exact(&mut data).await.unwrap();
        assert_eq!(&data, b"native");
        client_io.shutdown().await.unwrap();
        assert_eq!(server_io.read(&mut data).await.unwrap(), 0);
        assert_eq!(IO_FREED.with(Cell::get), [0, 0], "EOF is not Box exit");
        client.held();
        server.held();
        drop(client_io);
        drop(server_io);
        // Public carrier aliases independently retain the original credit.
        client.held();
        server.held();
        client.finish();
        server.finish();
    });
}

#[test]
fn actual_automatic_tls_client_and_server_boxes_match_layout_and_exit_before_owner() {
    let runtime = runtime();
    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint =
            NativeEndpoint::from_host_port("127.0.0.1", listener.local_addr().unwrap().port())
                .unwrap();
        let trust = NativeTrust::new(
            DeploymentId::parse("io-box-probe").unwrap(),
            ValidatedSharedSecret::new(SecretValue::new("0123456789abcdef0123456789abcdef"))
                .unwrap(),
            NativeCallerSubject::parse("io-box-probe").unwrap(),
            NativeTransportMode::Automatic,
        );
        let material = AutomaticTlsMaterial::for_endpoint(trust, endpoint.clone()).unwrap();
        let connector = NativeEndpointConnector::automatic(endpoint, &material).unwrap();
        let incoming = NativeIncomingAdapter::automatic(&material);
        let client = Funded::new(
            native_io_box_layout(NativeTransportMode::Automatic, NativeIoDirection::Client),
            0,
        );
        let server = Funded::new(
            native_io_box_layout(NativeTransportMode::Automatic, NativeIoDirection::Server),
            1,
        );
        assert_eq!(
            client.layout,
            native_io_box_layout(NativeTransportMode::Pem, NativeIoDirection::Client)
        );
        assert_eq!(
            server.layout,
            native_io_box_layout(NativeTransportMode::Pem, NativeIoDirection::Server)
        );
        let (client_io, server_io) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(connector.connect(), async {
                let (stream, _) = listener.accept().await.unwrap();
                incoming.accept(stream).await
            })
        })
        .await
        .unwrap();
        let mut client_io = client.wrap(client_io.unwrap());
        let mut server_io = server.wrap(server_io.unwrap());
        tokio::time::timeout(Duration::from_secs(5), async {
            client_io.write_all(b"tls").await.unwrap();
            client_io.flush().await.unwrap();
            let mut data = [0; 3];
            server_io.read_exact(&mut data).await.unwrap();
            assert_eq!(&data, b"tls");
            server_io.write_all(b"ack").await.unwrap();
            server_io.flush().await.unwrap();
            client_io.read_exact(&mut data).await.unwrap();
            assert_eq!(&data, b"ack");
        })
        .await
        .unwrap();
        drop(client_io);
        drop(server_io);
        client.held();
        server.held();
        client.finish();
        server.finish();
    });
}

#[derive(Default)]
struct Calls {
    read: AtomicUsize,
    write: AtomicUsize,
    vectored: AtomicUsize,
    flush: AtomicUsize,
    shutdown: AtomicUsize,
}
struct ScriptIo {
    calls: Arc<Calls>,
    panic_on_drop: bool,
}
impl AsyncRead for ScriptIo {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.calls.read.fetch_add(1, Ordering::SeqCst) == 0 {
            return Poll::Pending;
        }
        buf.put_slice(b"io");
        Poll::Ready(Ok(()))
    }
}
impl AsyncWrite for ScriptIo {
    fn poll_write(self: Pin<&mut Self>, _: &mut Context<'_>, _: &[u8]) -> Poll<io::Result<usize>> {
        if self.calls.write.fetch_add(1, Ordering::SeqCst) == 0 {
            Poll::Pending
        } else {
            Poll::Ready(Ok(1))
        }
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.calls.vectored.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(Ok(2))
    }
    fn is_write_vectored(&self) -> bool {
        true
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.calls.flush.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(Err(io::ErrorKind::WouldBlock.into()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.calls.shutdown.fetch_add(1, Ordering::SeqCst) == 0 {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }
}
impl Drop for ScriptIo {
    fn drop(&mut self) {
        assert!(!self.panic_on_drop, "actual boxed IO destructor panic");
    }
}
#[test]
fn forwarding_keeps_partial_pending_vectored_and_error_results_without_allocating() {
    let calls = Arc::new(Calls::default());
    let funded = Funded::new(Layout::new::<ScriptIo>(), 0);
    let mut io = funded.wrap(Box::new(ScriptIo {
        calls: calls.clone(),
        panic_on_drop: false,
    }));
    let mut context = Context::from_waker(Waker::noop());
    let mut bytes = [0; 2];
    let mut buf = ReadBuf::new(&mut bytes);
    no_allocation(|| {
        assert!(
            Pin::new(&mut io)
                .poll_read(&mut context, &mut buf)
                .is_pending()
        );
        assert!(matches!(
            Pin::new(&mut io).poll_read(&mut context, &mut buf),
            Poll::Ready(Ok(()))
        ));
        assert_eq!(buf.filled(), b"io");
        assert!(
            Pin::new(&mut io)
                .poll_write(&mut context, b"abc")
                .is_pending()
        );
        assert!(matches!(
            Pin::new(&mut io).poll_write(&mut context, b"abc"),
            Poll::Ready(Ok(1))
        ));
        assert!(io.is_write_vectored());
        assert!(matches!(
            Pin::new(&mut io)
                .poll_write_vectored(&mut context, &[IoSlice::new(b"a"), IoSlice::new(b"bc")]),
            Poll::Ready(Ok(2))
        ));
        let Poll::Ready(Err(error)) = Pin::new(&mut io).poll_flush(&mut context) else {
            panic!("forward actual flush error");
        };
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        assert!(Pin::new(&mut io).poll_shutdown(&mut context).is_pending());
        assert!(matches!(
            Pin::new(&mut io).poll_shutdown(&mut context),
            Poll::Ready(Ok(()))
        ));
    });
    assert_eq!(calls.read.load(Ordering::SeqCst), 2);
    assert_eq!(calls.write.load(Ordering::SeqCst), 2);
    assert_eq!(calls.vectored.load(Ordering::SeqCst), 1);
    assert_eq!(calls.flush.load(Ordering::SeqCst), 1);
    assert_eq!(calls.shutdown.load(Ordering::SeqCst), 2);
    drop(io);
    funded.finish();
}

#[test]
fn actual_last_original_owner_exits_after_normal_box_deallocation() {
    let funded = Funded::new(Layout::new::<ScriptIo>(), 0);
    let io = funded.wrap(Box::new(ScriptIo {
        calls: Arc::new(Calls::default()),
        panic_on_drop: false,
    }));
    let Funded {
        owner,
        budget,
        grant,
        index,
        ..
    } = funded;
    // Only the live IO wrapper now holds this original capability. A normal
    // Box destructor makes owner-first regressions a single caught assertion,
    // rather than a second destructor panic or an alias-masked false success.
    drop(owner);
    let result = catch_unwind(AssertUnwindSafe(|| drop(io)));
    assert!(
        result.is_ok(),
        "normal IO must exit before its last original owner"
    );
    assert_eq!(EXITS.with(Cell::get)[index], 1);
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(grant).unwrap() else {
        panic!("normal actual Box/carrier exit must return the complete original grant");
    };
    drop(credit);
}

#[test]
fn actual_box_deallocation_during_destructor_unwind_precedes_original_owner_exit() {
    let funded = Funded::new(Layout::new::<ScriptIo>(), 0);
    let io = funded.wrap(Box::new(ScriptIo {
        calls: Arc::new(Calls::default()),
        panic_on_drop: true,
    }));
    // Remove the public alias before drop: only OwnedNativeIo now retains the
    // original guard. Its destructor must free the real Box while unwinding.
    let Funded {
        owner,
        budget,
        grant,
        index,
        ..
    } = funded;
    drop(owner);
    assert!(catch_unwind(AssertUnwindSafe(|| drop(io))).is_err());
    assert_eq!(EXITS.with(Cell::get)[index], 1);
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(grant).unwrap() else {
        panic!("unwind must return original credit after physical exit");
    };
    drop(credit);
}

#[test]
fn optional_none_owner_preserves_default_io_forwarding_and_actual_box_drop() {
    let calls = Arc::new(Calls::default());
    let io: BoxedNativeIo = Box::new(ScriptIo {
        calls: calls.clone(),
        panic_on_drop: false,
    });
    let mut owned = no_allocation(|| OwnedNativeIo::new(io, None));
    let mut context = Context::from_waker(Waker::noop());
    assert!(owned.is_write_vectored());
    assert!(matches!(
        Pin::new(&mut owned).poll_write_vectored(&mut context, &[IoSlice::new(b"abc")]),
        Poll::Ready(Ok(2))
    ));
    drop(owned);
    assert_eq!(calls.vectored.load(Ordering::SeqCst), 1);
}
