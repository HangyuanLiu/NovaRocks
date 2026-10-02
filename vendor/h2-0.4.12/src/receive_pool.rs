//! Fixed retained payload backing for one opt-in HTTP/2 connection.

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

/// Preallocated, fixed payload buffers whose positions last through the final
/// escaping Bytes alias and its owner wrapper's actual deallocation.
///
/// One pool binds once to one connection. The caller must obtain funding for
/// the complete allocation bound before constructing this pool, and supply
/// its original physical-exit ownership carrier. The carrier is retained until
/// every buffer, wrapper, pool object and escaping payload alias physically exits.
/// DATA and GOAWAY use distinct once-bound pools: DATA waits before another frame
/// read, whereas nonempty GOAWAY debug refuses before copying when its pool is full.
/// This does not fund or bound the codec's original read buffer, HPACK, headers,
/// write buffers, streams, task allocations or caller carrier metadata.
pub struct ReceiveBufferPool {
    core: Option<Arc<Core>>,
}

impl ReceiveBufferPool {
    /// A conservative Rust allocation bound for the fixed pool, all payload
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

    /// The complete capacity of each retained payload buffer, independent of its
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
        self.try_copy_payload(data)
            .expect("DATA parser must have a free backing position before reading")
    }

    /// The single bound parser may refuse another diagnostic without waiting.
    /// No payload copy or wrapper allocation occurs until a position is acquired.
    pub(crate) fn try_copy_payload(&self, data: &[u8]) -> Option<Bytes> {
        assert!(data.len() <= self.buffer_capacity_bytes());
        // FREE is published before available is incremented by the exit guard.
        // Observe the count first so that transient FREE cannot underflow it.
        if self.available_buffers() == 0 {
            return None;
        }
        let (index, slot) = self.core().slots.iter().enumerate().find(|(_, slot)| {
            slot.state
                .compare_exchange(FREE, CHECKED_OUT, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        })?;
        let previous = self.core().available.fetch_sub(1, Ordering::AcqRel);
        assert!(previous > 0);
        // SAFETY: The successful CAS grants this single parser exclusive
        // access. No owner or exit guard from the previous use remains.
        let mut buffer = unsafe { (&mut *slot.buffer.get()).take().unwrap() };
        buffer.clear();
        buffer.extend_from_slice(data);
        Some(Bytes::from_owner_with_exit_guard(
            PoolBuffer {
                buffer: Some(buffer),
                index,
                pool: self.clone(),
            },
            BufferExit {
                index,
                pool: Some(self.clone()),
            },
        ))
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
