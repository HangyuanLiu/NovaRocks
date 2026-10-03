//! Original aggregate backing for independently retained decoded fields.

use bytes::Bytes;
use std::fmt;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const QUANTUM: usize = 64;

/// Failure to obtain or fill originally funded header payload storage.
#[derive(Debug)]
pub enum HeaderFieldFillError<E> {
    /// The requested field exceeds the fixed per-field maximum.
    TooLarge,
    /// No position or contiguous extent is available.
    Exhausted,
    /// The callback failed; its original error is preserved unchanged.
    Fill(E),
}

struct Core {
    // Never resized or accessed through a reference after publication. Only
    // exclusive claimed extents are written; published fields are immutable.
    _arena: Vec<u8>,
    base: *mut u8,
    used: Vec<AtomicBool>,
    live: AtomicUsize,
    checkout: Mutex<()>,
    bound: AtomicBool,
    capacity: usize,
    positions: usize,
    max_field: usize,
    // The final Arc allocation, arena and bitmap exit before original credit.
    _ownership: Bytes,
}
// SAFETY: A short checkout lock serializes disjoint extent claims and retirement. A claimed extent stays
// immutable after publication until its final Bytes wrapper physically exits.
// The arena never moves/grows and final Core destruction requires all owners.
unsafe impl Send for Core {}
unsafe impl Sync for Core {}

/// Fixed aggregate bytes and a finite number of independently retained fields.
///
/// Obtain the complete allocation bound before construction, including every
/// possible live Bytes owner wrapper. Fields occupy rounded 64-byte extents,
/// rather than one maximum-sized buffer per field. Exhaustion or fragmentation
/// refuses decoding without a heap fallback. Extent claims serialize only the
/// finite bitmap scan; fill callbacks execute outside that lock. This does not
/// bound the operating system's mutex scheduling time. All aliases retain the
/// original owner; a position is reusable only after its wrapper physically
/// exits. This does not cover HeaderMap/table/pseudo-header metadata.
pub struct HeaderFieldAllocationPool {
    core: Option<Arc<Core>>,
}

impl HeaderFieldAllocationPool {
    /// Complete Rust-requested arena/bitmap/Core/Arc and maximum live wrapper
    /// bound. Caller carrier metadata and allocator caches/RSS are separate.
    pub fn allocation_capacity_bound(
        capacity: usize,
        positions: usize,
        max_field: usize,
    ) -> io::Result<usize> {
        validate(capacity, positions, max_field)?;
        let platform = mutex_backing_bytes()?;
        std::mem::size_of::<Core>()
            .checked_add(3 * std::mem::size_of::<usize>())
            .and_then(|n| n.checked_add(std::mem::align_of::<Core>()))
            .and_then(|n| n.checked_add(capacity))
            .and_then(|n| n.checked_add(platform))
            .and_then(|n| n.checked_add((capacity / QUANTUM) * std::mem::size_of::<AtomicBool>()))
            .and_then(|n| {
                Bytes::owner_with_exit_guard_metadata_size::<PoolField, FieldExit>()
                    .checked_mul(positions)
                    .and_then(|wrappers| n.checked_add(wrappers))
            })
            .ok_or_else(|| invalid("header field allocation size overflow"))
    }

    /// Construct after obtaining the complete original capacity bound.
    pub fn new(
        capacity: usize,
        positions: usize,
        max_field: usize,
        ownership: Bytes,
    ) -> io::Result<Self> {
        Self::allocation_capacity_bound(capacity, positions, max_field)?;
        let mut arena = vec![0; capacity];
        let base = arena.as_mut_ptr();
        let used = (0..capacity / QUANTUM)
            .map(|_| AtomicBool::new(false))
            .collect::<Vec<_>>();
        assert_eq!(arena.capacity(), capacity);
        assert_eq!(used.capacity(), capacity / QUANTUM);
        let core = Arc::new(Core {
            _arena: arena,
            base,
            used,
            live: AtomicUsize::new(0),
            checkout: Mutex::new(()),
            bound: AtomicBool::new(false),
            capacity,
            positions,
            max_field,
            _ownership: ownership,
        });
        // Initialize the pinned std's lazy Darwin PAL allocation while the
        // final Core is unpublished. Concurrent first lockers could otherwise
        // allocate multiple temporary PAL Boxes despite one retained mutex.
        drop(
            core.checkout
                .lock()
                .unwrap_or_else(|error| error.into_inner()),
        );
        Ok(Self { core: Some(core) })
    }

    /// Actual fixed arena capacity, including extent rounding and fragmentation.
    pub fn capacity_bytes(&self) -> usize {
        self.core().capacity
    }
    /// Maximum simultaneous prepared/live/retiring owner wrappers.
    pub fn field_positions(&self) -> usize {
        self.core().positions
    }
    /// Maximum decoded length of one field name or value.
    pub fn max_field_bytes(&self) -> usize {
        self.core().max_field
    }
    /// Positions free after previous wrappers physically exited.
    pub fn available_positions(&self) -> usize {
        self.core().positions - self.core().live.load(Ordering::Acquire)
    }

    /// Bind this original arena once across all aliases, before connection I/O.
    /// Cancellation or connection exit does not permit rebinding.
    pub fn try_bind_once(&self) -> io::Result<()> {
        self.core()
            .bound
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| invalid("header field pool already bound to a connection"))?;
        Ok(())
    }

    /// Whether both handles refer to the same originally funded arena.
    pub fn same_pool(&self, other: &Self) -> bool {
        Arc::ptr_eq(self.core.as_ref().unwrap(), other.core.as_ref().unwrap())
    }

    /// Fill one disjoint extent and retain it through the final Bytes wrapper exit.
    /// Empty output invokes the callback without claiming a position. Callback
    /// errors or unwinding return the extent without publishing a partial field.
    pub fn try_fill<E>(
        &self,
        len: usize,
        fill: impl FnOnce(&mut [u8]) -> Result<(), E>,
    ) -> Result<Bytes, HeaderFieldFillError<E>> {
        if len > self.max_field_bytes() {
            return Err(HeaderFieldFillError::TooLarge);
        }
        if len == 0 {
            fill(&mut []).map_err(HeaderFieldFillError::Fill)?;
            return Ok(Bytes::new());
        }
        let blocks = len.div_ceil(QUANTUM);
        let checkout = self
            .core()
            .checkout
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if self.core().live.load(Ordering::Acquire) == self.field_positions() {
            return Err(HeaderFieldFillError::Exhausted);
        }
        let mut run = 0;
        let mut found = None;
        for (i, used) in self.core().used.iter().enumerate() {
            run = if used.load(Ordering::Acquire) {
                0
            } else {
                run + 1
            };
            if run == blocks {
                found = Some(i + 1 - blocks);
                break;
            }
        }
        let start = found.ok_or(HeaderFieldFillError::Exhausted)?;
        for used in &self.core().used[start..start + blocks] {
            used.store(true, Ordering::Relaxed);
        }
        self.core().live.fetch_add(1, Ordering::AcqRel);
        // Only the fixed scan and claim execute under this lock. User fill,
        // owner construction, publication and all payload destruction are
        // outside it, permitting concurrent and reentrant disjoint fills.
        drop(checkout);
        // This guard exists before callback/owner construction, so errors and
        // unwinding release the claim. No callback runs under a blocking lock.
        let exit = FieldExit {
            pool: self.clone(),
            start,
            blocks,
        };
        // SAFETY: the exclusive checkout lock granted this exact disjoint extent. No
        // whole-arena reference is formed, including while other fields live.
        let output =
            unsafe { std::slice::from_raw_parts_mut(self.core().base.add(start * QUANTUM), len) };
        fill(output).map_err(HeaderFieldFillError::Fill)?;
        let owner = PoolField {
            pool: self.clone(),
            start,
            len,
        };
        Ok(Bytes::from_owner_with_exit_guard(owner, exit))
    }

    fn core(&self) -> &Core {
        self.core.as_ref().expect("live header field pool")
    }
}

impl Clone for HeaderFieldAllocationPool {
    fn clone(&self) -> Self {
        Self {
            core: Some(Arc::clone(self.core.as_ref().unwrap())),
        }
    }
}
impl Drop for HeaderFieldAllocationPool {
    fn drop(&mut self) {
        drop(Arc::into_inner(self.core.take().unwrap()));
    }
}
impl fmt::Debug for HeaderFieldAllocationPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HeaderFieldAllocationPool")
            .field("capacity_bytes", &self.capacity_bytes())
            .field("field_positions", &self.field_positions())
            .field("max_field_bytes", &self.max_field_bytes())
            .finish_non_exhaustive()
    }
}

struct PoolField {
    pool: HeaderFieldAllocationPool,
    start: usize,
    len: usize,
}
impl AsRef<[u8]> for PoolField {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: this owner holds the immutable claimed extent. FieldExit
        // returns it only after this owner and its Bytes wrapper are destroyed.
        unsafe {
            std::slice::from_raw_parts(self.pool.core().base.add(self.start * QUANTUM), self.len)
        }
    }
}
struct FieldExit {
    pool: HeaderFieldAllocationPool,
    start: usize,
    blocks: usize,
}
impl Drop for FieldExit {
    fn drop(&mut self) {
        let checkout = self
            .pool
            .core()
            .checkout
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        for used in &self.pool.core().used[self.start..self.start + self.blocks] {
            used.store(false, Ordering::Release);
        }
        self.pool.core().live.fetch_sub(1, Ordering::AcqRel);
        drop(checkout);
    }
}
fn mutex_backing_bytes() -> io::Result<usize> {
    // Source-audited pinned Rust 1.92 std sys/sync/mutex/pthread.rs uses one
    // Box<pal::Mutex>, whose sole field is UnsafeCell<pthread_mutex_t>.
    // On supported 64-bit Darwin, that type is a signature plus 56 bytes.
    // Linux's supported atomic-32 futex implementation has no heap backing.
    // Revalidate this receipt when changing the pinned std/platform ABI.
    #[cfg(all(target_os = "macos", target_pointer_width = "64"))]
    {
        Ok(std::alloc::Layout::new::<(isize, [u8; 56])>().size())
    }
    #[cfg(all(target_os = "linux", target_has_atomic = "32"))]
    {
        Ok(0)
    }
    #[cfg(not(any(
        all(target_os = "macos", target_pointer_width = "64"),
        all(target_os = "linux", target_has_atomic = "32")
    )))]
    {
        Err(io::ErrorKind::Unsupported.into())
    }
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
fn validate(capacity: usize, positions: usize, max_field: usize) -> io::Result<()> {
    if capacity == 0
        || capacity % QUANTUM != 0
        || positions == 0
        || positions > capacity / QUANTUM
        || max_field == 0
        || max_field > capacity
    {
        return Err(invalid("header field pool requires aligned nonzero capacity, positions and a fitting maximum field"));
    }
    std::alloc::Layout::array::<u8>(capacity)
        .and_then(|_| std::alloc::Layout::array::<AtomicBool>(capacity / QUANTUM))
        .map_err(|_| {
            invalid("header field arena or bitmap layout exceeds addressable allocation")
        })?;
    Ok(())
}
