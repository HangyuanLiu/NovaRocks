//! Public actual h2 writer constructor/deallocation oracle, using a normal h2
//! dependency. Only the original Core and actual selected 64 KiB buffer are
//! covered; HPACK, reads, queues, connection metadata, IO and carrier are separate.
use bytes::Bytes;
use h2::SendFrameBuffer;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

const CAPACITY: usize = 65536;
thread_local! { static MODE: Cell<u8> = const { Cell::new(0) }; }
static CORE: AtomicPtr<u8> = AtomicPtr::new(ptr::null_mut());
static WRITER: AtomicPtr<u8> = AtomicPtr::new(ptr::null_mut());
static CORE_CALLS: AtomicUsize = AtomicUsize::new(0);
static CORE_BYTES: AtomicUsize = AtomicUsize::new(0);
static WRITER_CALLS: AtomicUsize = AtomicUsize::new(0);
static WRITER_DEALLOCS: AtomicUsize = AtomicUsize::new(0);
struct Tracked;
// SAFETY: Forward all pointers and Layouts unchanged to System. Record the
// exact constructor Core and the actual 64 KiB requested writer allocation;
// other allocations are explicitly outside the proven ledger.
unsafe impl GlobalAlloc for Tracked {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        let mode = MODE.try_with(Cell::get).unwrap_or(0);
        if !pointer.is_null() {
            if mode == 1 {
                CORE_CALLS.fetch_add(1, Ordering::AcqRel);
                CORE_BYTES.fetch_add(layout.size(), Ordering::AcqRel);
                CORE.store(pointer, Ordering::Release);
            } else if mode == 2 && layout.size() == CAPACITY {
                WRITER_CALLS.fetch_add(1, Ordering::AcqRel);
                WRITER.store(pointer, Ordering::Release);
            }
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        let _ = CORE.compare_exchange(
            pointer,
            ptr::null_mut(),
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        if WRITER
            .compare_exchange(
                pointer,
                ptr::null_mut(),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            WRITER_DEALLOCS.fetch_add(1, Ordering::AcqRel);
        }
    }
}
#[global_allocator]
static ALLOCATOR: Tracked = Tracked;
struct Scope;
impl Drop for Scope {
    fn drop(&mut self) {
        MODE.with(|v| v.set(0));
    }
}
fn measured<T>(mode: u8, f: impl FnOnce() -> T) -> T {
    MODE.with(|v| v.set(mode));
    let scope = Scope;
    let value = f();
    drop(scope);
    value
}
struct Original(Arc<AtomicUsize>);
impl Drop for Original {
    fn drop(&mut self) {
        assert!(
            WRITER.load(Ordering::Acquire).is_null(),
            "original grant exited before the actual fixed writer Vec deallocation"
        );
        assert!(
            CORE.load(Ordering::Acquire).is_null(),
            "original grant exited before Core Arc deallocation"
        );
        assert_eq!(WRITER_DEALLOCS.load(Ordering::Acquire), 1);
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}
fn funded() -> (SendFrameBuffer, Arc<AtomicUsize>) {
    assert!(WRITER.load(Ordering::Acquire).is_null());
    assert!(CORE.load(Ordering::Acquire).is_null());
    CORE_CALLS.store(0, Ordering::Release);
    CORE_BYTES.store(0, Ordering::Release);
    WRITER_CALLS.store(0, Ordering::Release);
    WRITER_DEALLOCS.store(0, Ordering::Release);
    let exits = Arc::new(AtomicUsize::new(0));
    let carrier = Bytes::from_owner_with_exit_guard(Bytes::new(), Original(exits.clone()));
    let buffer = measured(1, || SendFrameBuffer::new(CAPACITY, 16384, carrier)).unwrap();
    assert_eq!(CORE_CALLS.load(Ordering::Acquire), 1);
    assert!(
        CORE_BYTES.load(Ordering::Acquire) + CAPACITY
            <= SendFrameBuffer::allocation_capacity_bound(CAPACITY, 16384).unwrap()
    );
    (buffer, exits)
}
struct ReadyIo(Arc<AtomicUsize>);
impl AsyncRead for ReadyIo {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}
impl AsyncWrite for ReadyIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.0.fetch_add(bytes.len(), Ordering::AcqRel);
        Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
fn handshake(
    builder: &h2::client::Builder,
    writes: Arc<AtomicUsize>,
) -> (
    h2::client::SendRequest<Bytes>,
    h2::client::Connection<ReadyIo, Bytes>,
) {
    let mut future = Box::pin(builder.handshake(ReadyIo(writes)));
    match measured(2, || {
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
    }) {
        Poll::Ready(result) => result.unwrap(),
        Poll::Pending => {
            panic!("ready IO must complete the actual client handshake without peer SETTINGS")
        }
    }
}
#[test]
fn actual_fixed_writer_vec_frees_before_original_funding() {
    let (buffer, exits) = funded();
    let writes = Arc::new(AtomicUsize::new(0));
    let mut builder = h2::client::Builder::new();
    builder.send_frame_buffer(buffer.clone());
    let (request, connection) = handshake(&builder, writes.clone());
    assert_eq!(WRITER_CALLS.load(Ordering::Acquire), 1);
    assert!(!WRITER.load(Ordering::Acquire).is_null());
    println!("Actual public h2 writer Core={}B + selected Vec={}B, bound={}B; unrelated handshake allocations excluded", CORE_BYTES.load(Ordering::Acquire), CAPACITY, SendFrameBuffer::allocation_capacity_bound(CAPACITY, 16384).unwrap());
    assert!(
        writes.load(Ordering::Acquire) >= 24,
        "actual client preface was written"
    );
    drop(builder);
    drop(buffer);
    drop(request);
    assert_eq!(exits.load(Ordering::Acquire), 0);
    drop(connection);
    assert_eq!(WRITER_DEALLOCS.load(Ordering::Acquire), 1);
    assert_eq!(exits.load(Ordering::Acquire), 1);
}
#[test]
fn public_clones_retain_funding_after_actual_writer_exit() {
    let (buffer, exits) = funded();
    let alias = buffer.clone();
    let mut builder = h2::client::Builder::new();
    builder.send_frame_buffer(buffer.clone());
    let (request, connection) = handshake(&builder, Arc::new(AtomicUsize::new(0)));
    drop(buffer);
    drop(builder);
    drop(request);
    drop(connection);
    assert_eq!(WRITER_CALLS.load(Ordering::Acquire), 1);
    assert_eq!(WRITER_DEALLOCS.load(Ordering::Acquire), 1);
    assert_eq!(exits.load(Ordering::Acquire), 0);
    drop(alias);
    assert_eq!(exits.load(Ordering::Acquire), 1);
}
#[test]
fn reused_public_buffer_rejects_before_second_io_or_writer_allocation() {
    let (buffer, exits) = funded();
    let mut builder = h2::client::Builder::new();
    builder.send_frame_buffer(buffer.clone());
    let (request, connection) = handshake(&builder, Arc::new(AtomicUsize::new(0)));
    drop(request);
    drop(connection);
    assert_eq!(WRITER_DEALLOCS.load(Ordering::Acquire), 1);
    let writes = Arc::new(AtomicUsize::new(0));
    let mut future = Box::pin(builder.handshake::<_, Bytes>(ReadyIo(writes.clone())));
    let result = measured(2, || {
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
    });
    match result {
        Poll::Ready(Err(error)) => assert!(format!("{error:?}").contains("already bound")),
        _ => panic!("second bind must fail before any preface"),
    }
    assert_eq!(writes.load(Ordering::Acquire), 0);
    assert_eq!(WRITER_CALLS.load(Ordering::Acquire), 1);
    drop(future);
    drop(builder);
    drop(buffer);
    assert_eq!(exits.load(Ordering::Acquire), 1);
}
