//! Fixed retained DATA backing for one opt-in HTTP/2 connection.

use atomic_waker::AtomicWaker;
use bytes::Bytes;
use std::cell::UnsafeCell;
use std::fmt;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

const FREE: u8 = 0;
const CHECKED_OUT: u8 = 1;
const RETIRING: u8 = 2;

struct Slot {
    state: AtomicU8,
    buffer: UnsafeCell<Option<Vec<u8>>>,
}
// SAFETY: Only the single bound parser takes a buffer after a successful
// FREE -> CHECKED_OUT CAS. Its sole PoolBuffer owner returns that buffer, then
// publishes RETIRING with Release. The post-deallocation exit guard alone
// publishes FREE; a subsequent parser CAS acquires the returned buffer.
unsafe impl Sync for Slot {}

struct Core {
    // All Rust-owned backing and the registered task reference exit before
    // the caller's original physical-exit owner carrier.
    slots: Vec<Slot>,
    available: AtomicUsize,
    waker: AtomicWaker,
    bound: AtomicBool,
    block_bytes: usize,
    _ownership: Bytes,
}

/// Preallocated, fixed DATA buffers whose positions last through the final
/// escaping Bytes alias and its owner wrapper's actual deallocation.
///
/// One pool binds once to one connection. The caller must obtain funding for
/// the complete allocation bound before constructing this pool, and supply
/// its original physical-exit ownership carrier. The carrier is retained until
/// every buffer, wrapper, pool object and escaping DATA alias physically exits.
/// This does not fund or bound the codec's original read buffer, HPACK, headers,
/// write buffers, streams, task allocations or caller carrier metadata.
pub struct ReceiveBufferPool {
    core: Option<Arc<Core>>,
}

impl ReceiveBufferPool {
    /// A conservative Rust allocation bound for the fixed pool, all DATA
    /// buffers and the maximum simultaneously live/retiring Bytes wrappers.
    /// Caller ownership-carrier metadata and task Waker targets are separate.
    /// No allocator caches or whole-process RSS are included.
    pub fn allocation_capacity_bound(slots: usize, block_bytes: usize) -> io::Result<usize> {
        validate_geometry(slots, block_bytes)?;
        let arc = std::mem::size_of::<Core>()
            .checked_add(3 * std::mem::size_of::<usize>())
            .and_then(|n| n.checked_add(std::mem::align_of::<Core>()));
        let per_slot = block_bytes
            .checked_add(std::mem::size_of::<Slot>())
            .and_then(|n| {
                n.checked_add(Bytes::owner_with_exit_guard_metadata_size::<
                    PoolBuffer,
                    BufferExit,
                >())
            });
        arc.and_then(|n| per_slot?.checked_mul(slots)?.checked_add(n))
            .ok_or_else(|| invalid("receive pool allocation size overflow"))
    }

    /// Construct under the supplied original owner, before any frame parsing.
    /// Each buffer and the fixed slot array use exactly their requested Vec
    /// capacities; no return path grows either allocation.
    pub fn new(slots: usize, block_bytes: usize, ownership: Bytes) -> io::Result<Self> {
        Self::allocation_capacity_bound(slots, block_bytes)?;
        let mut storage = Vec::with_capacity(slots);
        assert_eq!(storage.capacity(), slots);
        for _ in 0..slots {
            let buffer = Vec::with_capacity(block_bytes);
            assert_eq!(buffer.capacity(), block_bytes);
            storage.push(Slot {
                state: AtomicU8::new(FREE),
                buffer: UnsafeCell::new(Some(buffer)),
            });
        }
        Ok(Self {
            core: Some(Arc::new(Core {
                slots: storage,
                available: AtomicUsize::new(slots),
                waker: AtomicWaker::new(),
                bound: AtomicBool::new(false),
                block_bytes,
                _ownership: ownership,
            })),
        })
    }

    /// Number of actual fixed buffers, including checked-out and retiring ones.
    pub fn buffer_positions(&self) -> usize {
        self.core().slots.len()
    }

    /// The complete capacity of each retained DATA buffer, independent of its
    /// current visible payload length.
    pub fn buffer_capacity_bytes(&self) -> usize {
        self.core().block_bytes
    }

    /// Buffers currently free after their previous wrapper physically exited.
    pub fn available_buffers(&self) -> usize {
        self.core().available.load(Ordering::Acquire)
    }

    fn core(&self) -> &Core {
        self.core.as_ref().expect("live receive pool")
    }

    pub(crate) fn bind(&self, max_frame: usize) -> io::Result<Self> {
        if max_frame > self.buffer_capacity_bytes() {
            return Err(invalid(
                "receive frame maximum exceeds pool buffer capacity",
            ));
        }
        if self.core().bound.swap(true, Ordering::AcqRel) {
            return Err(invalid("receive pool is already bound to a connection"));
        }
        Ok(self.clone())
    }

    pub(crate) fn poll_ready(&self, cx: &Context<'_>) -> Poll<()> {
        if self.available_buffers() > 0 {
            return Poll::Ready(());
        }
        self.core().waker.register(cx.waker());
        // Close return-before-registration races. Only the single bound
        // parser checks out slots; aliases on other threads only return them.
        if self.available_buffers() > 0 {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }

    pub(crate) fn detach_waker(&self) {
        drop(self.core().waker.take());
    }

    pub(crate) fn copy_data(&self, data: &[u8]) -> Bytes {
        assert!(data.len() <= self.buffer_capacity_bytes());
        let (index, slot) = self
            .core()
            .slots
            .iter()
            .enumerate()
            .find(|(_, slot)| {
                slot.state
                    .compare_exchange(FREE, CHECKED_OUT, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            })
            .expect("DATA parser must have a free backing position before reading");
        let previous = self.core().available.fetch_sub(1, Ordering::AcqRel);
        assert!(previous > 0);
        // SAFETY: The successful CAS grants this single parser exclusive
        // access. No owner or exit guard from the previous use remains.
        let mut buffer = unsafe { (&mut *slot.buffer.get()).take().unwrap() };
        buffer.clear();
        buffer.extend_from_slice(data);
        Bytes::from_owner_with_exit_guard(
            PoolBuffer {
                buffer: Some(buffer),
                index,
                pool: self.clone(),
            },
            BufferExit {
                index,
                pool: Some(self.clone()),
            },
        )
    }
}

impl Clone for ReceiveBufferPool {
    fn clone(&self) -> Self {
        Self {
            core: Some(Arc::clone(self.core.as_ref().expect("live receive pool"))),
        }
    }
}
impl Drop for ReceiveBufferPool {
    fn drop(&mut self) {
        // No Weak, raw Arc or Deref escapes. into_inner frees the final Arc
        // allocation before returning Core; its fixed buffers exit before the
        // original ownership carrier, even after the connection itself exits.
        drop(Arc::into_inner(
            self.core.take().expect("live receive pool"),
        ));
    }
}
impl fmt::Debug for ReceiveBufferPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReceiveBufferPool")
            .field("buffer_positions", &self.buffer_positions())
            .field("buffer_capacity_bytes", &self.buffer_capacity_bytes())
            .finish_non_exhaustive()
    }
}

struct PoolBuffer {
    buffer: Option<Vec<u8>>,
    index: usize,
    pool: ReceiveBufferPool,
}
impl AsRef<[u8]> for PoolBuffer {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_ref().expect("live DATA buffer")
    }
}
impl Drop for PoolBuffer {
    fn drop(&mut self) {
        let slot = &self.pool.core().slots[self.index];
        assert_eq!(slot.state.load(Ordering::Acquire), CHECKED_OUT);
        // SAFETY: This is the sole Vec owner for this checked-out slot. The
        // parser cannot acquire it until BufferExit publishes FREE below.
        unsafe { *slot.buffer.get() = self.buffer.take() };
        slot.state.store(RETIRING, Ordering::Release);
    }
}

struct BufferExit {
    index: usize,
    pool: Option<ReceiveBufferPool>,
}
impl Drop for BufferExit {
    fn drop(&mut self) {
        let pool = self.pool.take().expect("live DATA exit owner");
        let slot = &pool.core().slots[self.index];
        assert_eq!(slot.state.load(Ordering::Acquire), RETIRING);
        // bytes' physical-exit contract calls this only after the PoolBuffer
        // wrapper allocation is gone. This ordering bounds both wrappers and
        // backings, including the retiring transition.
        slot.state.store(FREE, Ordering::Release);
        pool.core().available.fetch_add(1, Ordering::Release);
        let waker = pool.core().waker.take();
        let notification = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if let Some(waker) = waker {
                waker.wake();
            }
        }));
        // Release the actual pool/owner before resuming a notification panic.
        // An already unwinding destructor must not produce a second panic.
        drop(pool);
        if let Err(panic) = notification {
            if !std::thread::panicking() {
                std::panic::resume_unwind(panic);
            }
        }
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
fn validate_geometry(slots: usize, block_bytes: usize) -> io::Result<()> {
    if slots == 0 || slots > 4096 || !(16384..=16777215).contains(&block_bytes) {
        return Err(invalid("invalid fixed receive pool geometry"));
    }
    Ok(())
}


#[cfg(test)]
mod ownership_probe {
    use super::*;
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;
    use std::ptr;
    use std::sync::atomic::{AtomicPtr, AtomicUsize};
    use std::sync::{Barrier, Arc};
    use std::task::{Wake, Waker};

    thread_local! { static TRACK: Cell<u8> = const { Cell::new(0) }; }
    static POINTERS: [AtomicPtr<u8>; 32] = [const { AtomicPtr::new(ptr::null_mut()) }; 32];
    static LIVE: AtomicUsize = AtomicUsize::new(0);
    static REQUESTED_TOTAL: AtomicUsize = AtomicUsize::new(0);
    static WRAPPER: AtomicPtr<u8> = AtomicPtr::new(ptr::null_mut());
    static WRAPPER_FREED: AtomicBool = AtomicBool::new(false);
    struct Tracked;
    // SAFETY: Calls delegate to System with the unchanged pointer/Layout.
    // Fixed atomics only record allocations made in the explicit test scope.
    unsafe impl GlobalAlloc for Tracked {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            // SAFETY: Forward the allocator contract unchanged.
            let ptr = unsafe { System.alloc(layout) };
            let mode = TRACK.try_with(Cell::get).unwrap_or(0);
            if mode > 0 && !ptr.is_null() {
                REQUESTED_TOTAL.fetch_add(layout.size(), Ordering::AcqRel);
                for entry in &POINTERS {
                    if entry.compare_exchange(ptr::null_mut(), ptr, Ordering::AcqRel, Ordering::Acquire).is_ok() {
                        LIVE.fetch_add(1, Ordering::AcqRel);
                        if mode == 2 { WRAPPER.store(ptr, Ordering::Release); }
                        break;
                    }
                }
            }
            ptr
        }
        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            // SAFETY: Forward the allocator contract unchanged. Observations
            // follow actual System.dealloc, not owner Drop or logical release.
            unsafe { System.dealloc(ptr, layout) };
            if WRAPPER.load(Ordering::Acquire) == ptr {
                WRAPPER_FREED.store(true, Ordering::Release);
            }
            for entry in &POINTERS {
                if entry.compare_exchange(ptr, ptr::null_mut(), Ordering::AcqRel, Ordering::Acquire).is_ok() {
                    LIVE.fetch_sub(1, Ordering::AcqRel);
                    break;
                }
            }
        }
    }
    #[global_allocator] static ALLOCATOR: Tracked = Tracked;

    struct TrackGuard;
    impl Drop for TrackGuard { fn drop(&mut self) { TRACK.with(|v| v.set(0)); } }
    fn tracked<T>(mode: u8, f: impl FnOnce() -> T) -> T {
        TRACK.with(|v| v.set(mode)); let guard = TrackGuard;
        let result = f(); drop(guard); result
    }
    struct Original(Arc<AtomicUsize>);
    impl Drop for Original {
        fn drop(&mut self) {
            assert_eq!(LIVE.load(Ordering::Acquire), 0, "pool allocations remain when original owner exits");
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }
    fn pool(slots: usize) -> (ReceiveBufferPool, Arc<AtomicUsize>) {
        assert_eq!(LIVE.load(Ordering::Acquire), 0);
        REQUESTED_TOTAL.store(0, Ordering::Release);
        WRAPPER.store(ptr::null_mut(), Ordering::Release);
        WRAPPER_FREED.store(false, Ordering::Release);
        let exit = Arc::new(AtomicUsize::new(0));
        let carrier = Bytes::from_owner_with_exit_guard(Bytes::new(), Original(exit.clone()));
        let pool = tracked(1, || ReceiveBufferPool::new(slots, 16384, carrier).unwrap());
        assert!(LIVE.load(Ordering::Acquire) > 0, "actual pool allocations must be observed");
        let bound = pool.bind(16384).unwrap(); drop(bound);
        (pool, exit)
    }
    fn copy(pool: &ReceiveBufferPool, data: &[u8]) -> Bytes { tracked(2, || pool.copy_data(data)) }

    #[test]
    fn allocation_bound_covers_pool_and_all_checked_out_wrappers() {
        let (pool, exit) = pool(2);
        let first = copy(&pool, &[1]); let second = copy(&pool, &[]);
        assert_eq!(pool.available_buffers(), 0);
        assert!(REQUESTED_TOTAL.load(Ordering::Acquire) <= ReceiveBufferPool::allocation_capacity_bound(2, 16384).unwrap());
        drop(pool); assert_eq!(exit.load(Ordering::Acquire), 0);
        drop(first); assert_eq!(exit.load(Ordering::Acquire), 0);
        drop(second); assert_eq!(exit.load(Ordering::Acquire), 1);
    }

    #[test]
    fn last_alias_exits_all_allocations_before_original_owner() {
        let (pool, exit) = pool(2);
        let data = copy(&pool, &[1,2,3]); let alias = data.slice(1..);
        drop(data); drop(pool);
        assert_eq!(exit.load(Ordering::Acquire), 0);
        assert_eq!(alias.as_ref(), &[2,3]);
        drop(alias);
        assert_eq!(exit.load(Ordering::Acquire), 1);
    }

    struct PhysicalWake(Arc<AtomicUsize>);
    impl Wake for PhysicalWake {
        fn wake(self: Arc<Self>) {
            assert!(WRAPPER_FREED.load(Ordering::Acquire), "wake preceded physical Bytes wrapper exit");
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }
    #[test]
    fn actual_wrapper_deallocation_precedes_return_and_wake() {
        let (pool, exit) = pool(1); let data = copy(&pool, &[9]);
        let wakes = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(PhysicalWake(wakes.clone())));
        let cx = Context::from_waker(&waker);
        assert!(pool.poll_ready(&cx).is_pending()); drop(data);
        assert_eq!(wakes.load(Ordering::Acquire), 1);
        assert_eq!(pool.available_buffers(), 1);
        let data = copy(&pool, &[8]); assert_eq!(data.as_ref(), &[8]);
        drop(data); drop(pool); assert_eq!(exit.load(Ordering::Acquire), 1);
    }

    #[test]
    fn empty_owner_clone_retains_backing_but_empty_slice_detaches() {
        let (pool, exit) = pool(1); let data = copy(&pool, &[]);
        let empty_slice = data.slice(..); let strong = data.clone();
        drop(data); drop(pool); assert_eq!(exit.load(Ordering::Acquire), 0);
        drop(strong); assert_eq!(exit.load(Ordering::Acquire), 1);
        assert!(empty_slice.is_empty()); drop(empty_slice);
    }

    struct PanicWake;
    impl Wake for PanicWake { fn wake(self: Arc<Self>) { panic!("injected wake failure"); } }
    #[test]
    fn wake_panic_restores_position_and_completes_original_owner_exit() {
        let (pool, exit) = pool(1); let data = copy(&pool, &[9]);
        let waker = Waker::from(Arc::new(PanicWake));
        let cx = Context::from_waker(&waker);
        assert!(pool.poll_ready(&cx).is_pending());
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(data))).is_err());
        assert_eq!(pool.available_buffers(), 1);
        drop(pool); assert_eq!(exit.load(Ordering::Acquire), 1);
    }

    #[test]
    fn concurrent_last_aliases_keep_original_owner_until_physical_exit() {
        let (pool, exit) = pool(1); let data = copy(&pool, &[9]);
        let a = data.clone(); let b = data.clone(); drop(data); drop(pool);
        let barrier = Arc::new(Barrier::new(3)); let first = barrier.clone(); let second = barrier.clone();
        let a = std::thread::spawn(move || { first.wait(); drop(a); });
        let b = std::thread::spawn(move || { second.wait(); drop(b); });
        assert_eq!(exit.load(Ordering::Acquire), 0); barrier.wait(); a.join().unwrap(); b.join().unwrap();
        assert_eq!(exit.load(Ordering::Acquire), 1);
    }
}
