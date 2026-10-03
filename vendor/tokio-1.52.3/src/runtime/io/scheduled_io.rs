use crate::io::interest::Interest;
use crate::io::ready::Ready;
use crate::loom::sync::atomic::AtomicUsize;
use crate::loom::sync::Mutex;
use crate::runtime::io::{Direction, ReadyEvent, Tick};
use crate::util::bit;
use crate::util::linked_list::{self, LinkedList};
use crate::util::WakeList;

use std::cell::UnsafeCell;
use std::future::Future;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::atomic::Ordering::{AcqRel, Acquire};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

/// Stored in the I/O driver resource slab.
#[derive(Debug)]
// # This struct should be cache padded to avoid false sharing. The cache padding rules are copied
// from crossbeam-utils/src/cache_padded.rs
//
// Starting from Intel's Sandy Bridge, spatial prefetcher is now pulling pairs of 64-byte cache
// lines at a time, so we have to align to 128 bytes rather than 64.
//
// Sources:
// - https://www.intel.com/content/dam/www/public/us/en/documents/manuals/64-ia-32-architectures-optimization-manual.pdf
// - https://github.com/facebook/folly/blob/1b5288e6eea6df074758f877c849b6e73bbb9fbb/folly/lang/Align.h#L107
//
// ARM's big.LITTLE architecture has asymmetric cores and "big" cores have 128-byte cache line size.
//
// Sources:
// - https://www.mono-project.com/news/2016/09/12/arm64-icache/
//
// powerpc64 has 128-byte cache line size.
//
// Sources:
// - https://github.com/golang/go/blob/3dd58676054223962cd915bb0934d1f9f489d4d2/src/internal/cpu/cpu_ppc64x.go#L9
#[cfg_attr(
    any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "powerpc64",
    ),
    repr(align(128))
)]
// arm, mips, mips64, sparc, and hexagon have 32-byte cache line size.
//
// Sources:
// - https://github.com/golang/go/blob/3dd58676054223962cd915bb0934d1f9f489d4d2/src/internal/cpu/cpu_arm.go#L7
// - https://github.com/golang/go/blob/3dd58676054223962cd915bb0934d1f9f489d4d2/src/internal/cpu/cpu_mips.go#L7
// - https://github.com/golang/go/blob/3dd58676054223962cd915bb0934d1f9f489d4d2/src/internal/cpu/cpu_mipsle.go#L7
// - https://github.com/golang/go/blob/3dd58676054223962cd915bb0934d1f9f489d4d2/src/internal/cpu/cpu_mips64x.go#L9
// - https://github.com/torvalds/linux/blob/3516bd729358a2a9b090c1905bd2a3fa926e24c6/arch/sparc/include/asm/cache.h#L17
// - https://github.com/torvalds/linux/blob/3516bd729358a2a9b090c1905bd2a3fa926e24c6/arch/hexagon/include/asm/cache.h#L12
#[cfg_attr(
    any(
        target_arch = "arm",
        target_arch = "mips",
        target_arch = "mips64",
        target_arch = "sparc",
        target_arch = "hexagon",
    ),
    repr(align(32))
)]
// m68k has 16-byte cache line size.
//
// Sources:
// - https://github.com/torvalds/linux/blob/3516bd729358a2a9b090c1905bd2a3fa926e24c6/arch/m68k/include/asm/cache.h#L9
#[cfg_attr(target_arch = "m68k", repr(align(16)))]
// s390x has 256-byte cache line size.
//
// Sources:
// - https://github.com/golang/go/blob/3dd58676054223962cd915bb0934d1f9f489d4d2/src/internal/cpu/cpu_s390x.go#L7
// - https://github.com/torvalds/linux/blob/3516bd729358a2a9b090c1905bd2a3fa926e24c6/arch/s390/include/asm/cache.h#L13
#[cfg_attr(target_arch = "s390x", repr(align(256)))]
// x86, riscv, wasm, and sparc64 have 64-byte cache line size.
//
// Sources:
// - https://github.com/golang/go/blob/dda2991c2ea0c5914714469c4defc2562a907230/src/internal/cpu/cpu_x86.go#L9
// - https://github.com/golang/go/blob/3dd58676054223962cd915bb0934d1f9f489d4d2/src/internal/cpu/cpu_wasm.go#L7
// - https://github.com/torvalds/linux/blob/3516bd729358a2a9b090c1905bd2a3fa926e24c6/arch/sparc/include/asm/cache.h#L19
// - https://github.com/torvalds/linux/blob/3516bd729358a2a9b090c1905bd2a3fa926e24c6/arch/riscv/include/asm/cache.h#L10
//
// All others are assumed to have 64-byte cache line size.
#[cfg_attr(
    not(any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "powerpc64",
        target_arch = "arm",
        target_arch = "mips",
        target_arch = "mips64",
        target_arch = "sparc",
        target_arch = "hexagon",
        target_arch = "m68k",
        target_arch = "s390x",
    )),
    repr(align(64))
)]
pub(crate) struct ScheduledIo {
    pub(super) linked_list_pointers: UnsafeCell<linked_list::Pointers<Self>>,
    // Separate links: the driver must keep the registration list membership
    // while a deregistered pointer may still occur in its current event batch.
    pub(super) retirement_pointers: UnsafeCell<linked_list::Pointers<Self>>,

    /// Packs the resource's readiness and I/O driver latest tick.
    readiness: AtomicUsize,

    waiters: Mutex<Waiters>,
    #[cfg(feature = "io-util")]
    registration_owner: Option<bytes::Bytes>,
}

/// Private strong-only registration handle. No Weak or extraction API exists.
#[derive(Debug)]
pub(crate) struct ScheduledIoHandle(Option<Arc<ScheduledIo>>);

impl ScheduledIoHandle {
    pub(super) fn new(io: ScheduledIo) -> Self {
        Self(Some(Arc::new(io)))
    }

    pub(super) fn as_ptr(&self) -> *const ScheduledIo {
        Arc::as_ptr(self.0.as_ref().expect("live IO registration"))
    }

    pub(super) unsafe fn from_raw(pointer: *const ScheduledIo) -> Self {
        // SAFETY: the caller transfers exactly one intrusive list-owned strong
        // count, using the matching pointer produced by as_ptr.
        Self(Some(unsafe { Arc::from_raw(pointer) }))
    }
}
impl Clone for ScheduledIoHandle {
    fn clone(&self) -> Self {
        Self(Some(self.0.as_ref().expect("live IO registration").clone()))
    }
}
impl std::ops::Deref for ScheduledIoHandle {
    type Target = ScheduledIo;
    fn deref(&self) -> &ScheduledIo {
        self.0.as_deref().expect("live IO registration")
    }
}
impl Drop for ScheduledIoHandle {
    fn drop(&mut self) {
        let arc = self.0.take().expect("live IO registration");
        #[cfg(feature = "io-util")]
        if arc.registration_owner.is_some() {
            if let Some(mut io) = Arc::into_inner(arc) {
                // No Weak handles exist. The Arc allocation has physically
                // exited before into_inner returns. Keep the original owner
                // outside ScheduledIo so wake/destructor unwinding drops the
                // waiters/PAL before releasing this final capability as well.
                let owner = io.registration_owner.take();
                drop(io);
                drop(owner);
            }
            return;
        }
        // Preserve the ordinary Arc destructor behavior for default callers.
        drop(arc);
    }
}

type WaitList = LinkedList<Waiter, <Waiter as linked_list::Link>::Target>;

#[derive(Debug, Default)]
struct Waiters {
    /// List of all current waiters.
    list: WaitList,

    /// Waker used for `AsyncRead`.
    reader: Option<Waker>,

    /// Waker used for `AsyncWrite`.
    writer: Option<Waker>,
}

#[derive(Debug)]
struct Waiter {
    pointers: linked_list::Pointers<Waiter>,

    /// The waker for this task.
    waker: Option<Waker>,

    /// The interest this waiter is waiting on.
    interest: Interest,

    is_ready: bool,

    /// Should never be `Unpin`.
    _p: PhantomPinned,
}

generate_addr_of_methods! {
    impl<> Waiter {
        unsafe fn addr_of_pointers(self: NonNull<Self>) -> NonNull<linked_list::Pointers<Waiter>> {
            &self.pointers
        }
    }
}

/// Future returned by `readiness()`.
struct Readiness<'a> {
    scheduled_io: &'a ScheduledIo,

    state: State,

    /// Entry in the waiter `LinkedList`.
    waiter: UnsafeCell<Waiter>,
}

enum State {
    Init,
    Waiting,
    Done,
}

// The `ScheduledIo::readiness` (`AtomicUsize`) is packed full of goodness.
//
// | shutdown | driver tick | readiness |
// |----------+-------------+-----------|
// |   1 bit  |   15 bits   |  16 bits  |

const READINESS: bit::Pack = bit::Pack::least_significant(16);

const TICK: bit::Pack = READINESS.then(15);

const SHUTDOWN: bit::Pack = TICK.then(1);

// ===== impl ScheduledIo =====

impl Default for ScheduledIo {
    fn default() -> ScheduledIo {
        ScheduledIo {
            linked_list_pointers: UnsafeCell::new(linked_list::Pointers::new()),
            retirement_pointers: UnsafeCell::new(linked_list::Pointers::new()),
            readiness: AtomicUsize::new(0),
            waiters: Mutex::new(Waiters::default()),
            #[cfg(feature = "io-util")]
            registration_owner: None,
        }
    }
}

impl ScheduledIo {
    pub(super) fn is_funded(&self) -> bool {
        #[cfg(feature = "io-util")]
        {
            self.registration_owner.is_some()
        }
        #[cfg(not(feature = "io-util"))]
        {
            false
        }
    }

    #[cfg(feature = "io-util")]
    pub(super) fn with_owner(owner: bytes::Bytes) -> Self {
        let mut io = Self::default();
        io.registration_owner = Some(owner);
        io
    }

    #[cfg(feature = "io-util")]
    pub(super) fn prewarm(&self) {
        // Called on the final unpublished Arc. This prevents concurrent first
        // locks from constructing multiple temporary Darwin PAL allocations.
        drop(self.waiters.lock());
    }

    #[cfg(feature = "io-util")]
    pub(crate) fn platform_mutex_allocation_capacity_bound() -> std::io::Result<usize> {
        // Loom's modeled mutex/allocator is not this platform allocation ABI.
        #[cfg(loom)]
        {
            Err(std::io::ErrorKind::Unsupported.into())
        }
        // Mirror src/loom/std/mod.rs exactly: Miri deliberately selects std.
        // parking_lot stores this mutex's state inline. Its shared parking
        // table/thread parker allocations are outside this node's receipt.
        #[cfg(all(not(loom), feature = "parking_lot", not(miri)))]
        {
            Ok(0)
        }
        #[cfg(all(not(loom), not(all(feature = "parking_lot", not(miri)))))]
        {
            // Pinned Rust 1.92 sys/sync/mutex/pthread.rs owns one Box<pal::Mutex>.
            // On supported 64-bit macOS its sole pthread_mutex_t field is a
            // signature plus 56 bytes. Linux atomic-32 std futex state is inline.
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
                Err(std::io::ErrorKind::Unsupported.into())
            }
        }
    }

    #[cfg(feature = "io-util")]
    pub(crate) fn allocation_capacity_bound() -> std::io::Result<usize> {
        // Reject modeled/unsupported mutexes before calculating a normal Arc.
        let mutex = Self::platform_mutex_allocation_capacity_bound()?;
        use std::alloc::Layout;
        // This is the actual std ArcInner prefix and padded ScheduledIo layout.
        // The cache-line alignment is part of Layout, not a guessed discount.
        let arc = Layout::new::<[std::sync::atomic::AtomicUsize; 2]>()
            .extend(Layout::new::<Self>())
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))?
            .0
            .pad_to_align()
            .size();
        arc.checked_add(mutex)
            .ok_or_else(|| std::io::ErrorKind::InvalidInput.into())
    }

    pub(crate) fn token(&self) -> mio::Token {
        mio::Token(super::EXPOSE_IO.expose_provenance(self))
    }

    /// Invoked when the IO driver is shut down; forces this `ScheduledIo` into a
    /// permanently shutdown state.
    pub(super) fn shutdown(&self) {
        let mask = SHUTDOWN.pack(1, 0);
        self.readiness.fetch_or(mask, AcqRel);
        self.wake(Ready::ALL);
    }

    /// Sets the readiness on this `ScheduledIo` by invoking the given closure on
    /// the current value, returning the previous readiness value.
    ///
    /// # Arguments
    /// - `tick`: whether setting the tick or trying to clear readiness for a
    ///   specific tick.
    /// - `f`: a closure returning a new readiness value given the previous
    ///   readiness.
    pub(super) fn set_readiness(&self, tick_op: Tick, f: impl Fn(Ready) -> Ready) {
        let _ = self.readiness.fetch_update(AcqRel, Acquire, |curr| {
            // If the io driver is shut down, then you are only allowed to clear readiness.
            debug_assert!(SHUTDOWN.unpack(curr) == 0 || matches!(tick_op, Tick::Clear(_)));

            const MAX_TICK: usize = TICK.max_value() + 1;
            let tick = TICK.unpack(curr);

            let new_tick = match tick_op {
                // Trying to clear readiness with an old event!
                Tick::Clear(t) if tick as u8 != t => return None,
                Tick::Clear(t) => t as usize,
                Tick::Set => tick.wrapping_add(1) % MAX_TICK,
            };
            let ready = Ready::from_usize(READINESS.unpack(curr));
            Some(TICK.pack(new_tick, f(ready).as_usize()))
        });
    }

    /// Notifies all pending waiters that have registered interest in `ready`.
    ///
    /// There may be many waiters to notify. Waking the pending task **must** be
    /// done from outside of the lock otherwise there is a potential for a
    /// deadlock.
    ///
    /// A stack array of wakers is created and filled with wakers to notify, the
    /// lock is released, and the wakers are notified. Because there may be more
    /// than 32 wakers to notify, if the stack array fills up, the lock is
    /// released, the array is cleared, and the iteration continues.
    pub(super) fn wake(&self, ready: Ready) {
        let mut wakers = WakeList::new();

        let mut waiters = self.waiters.lock();

        // check for AsyncRead slot
        if ready.is_readable() {
            if let Some(waker) = waiters.reader.take() {
                wakers.push(waker);
            }
        }

        // check for AsyncWrite slot
        if ready.is_writable() {
            if let Some(waker) = waiters.writer.take() {
                wakers.push(waker);
            }
        }

        'outer: loop {
            let mut iter = waiters.list.drain_filter(|w| ready.satisfies(w.interest));

            while wakers.can_push() {
                match iter.next() {
                    Some(waiter) => {
                        let waiter = unsafe { &mut *waiter.as_ptr() };

                        if let Some(waker) = waiter.waker.take() {
                            waiter.is_ready = true;
                            wakers.push(waker);
                        }
                    }
                    None => {
                        break 'outer;
                    }
                }
            }

            drop(waiters);

            wakers.wake_all();

            // Acquire the lock again.
            waiters = self.waiters.lock();
        }

        // Release the lock before notifying
        drop(waiters);

        wakers.wake_all();
    }

    pub(super) fn ready_event(&self, interest: Interest) -> ReadyEvent {
        let curr = self.readiness.load(Acquire);

        ReadyEvent {
            tick: TICK.unpack(curr) as u8,
            ready: interest.mask() & Ready::from_usize(READINESS.unpack(curr)),
            is_shutdown: SHUTDOWN.unpack(curr) != 0,
        }
    }

    /// Polls for readiness events in a given direction.
    ///
    /// These are to support `AsyncRead` and `AsyncWrite` polling methods,
    /// which cannot use the `async fn` version. This uses reserved reader
    /// and writer slots.
    pub(super) fn poll_readiness(
        &self,
        cx: &mut Context<'_>,
        direction: Direction,
    ) -> Poll<ReadyEvent> {
        let curr = self.readiness.load(Acquire);

        let ready = direction.mask() & Ready::from_usize(READINESS.unpack(curr));
        let is_shutdown = SHUTDOWN.unpack(curr) != 0;

        if ready.is_empty() && !is_shutdown {
            // Update the task info
            let mut waiters = self.waiters.lock();
            let waker = match direction {
                Direction::Read => &mut waiters.reader,
                Direction::Write => &mut waiters.writer,
            };

            // Avoid cloning the waker if one is already stored that matches the
            // current task.
            match waker {
                Some(waker) => waker.clone_from(cx.waker()),
                None => *waker = Some(cx.waker().clone()),
            }

            // Try again, in case the readiness was changed while we were
            // taking the waiters lock
            let curr = self.readiness.load(Acquire);
            let ready = direction.mask() & Ready::from_usize(READINESS.unpack(curr));
            let is_shutdown = SHUTDOWN.unpack(curr) != 0;
            if is_shutdown {
                Poll::Ready(ReadyEvent {
                    tick: TICK.unpack(curr) as u8,
                    ready: direction.mask(),
                    is_shutdown,
                })
            } else if ready.is_empty() {
                Poll::Pending
            } else {
                Poll::Ready(ReadyEvent {
                    tick: TICK.unpack(curr) as u8,
                    ready,
                    is_shutdown,
                })
            }
        } else {
            Poll::Ready(ReadyEvent {
                tick: TICK.unpack(curr) as u8,
                ready,
                is_shutdown,
            })
        }
    }

    pub(crate) fn clear_readiness(&self, event: ReadyEvent) {
        // This consumes the current readiness state **except** for closed
        // states. Closed states are excluded because they are final states.
        let mask_no_closed = event.ready - Ready::READ_CLOSED - Ready::WRITE_CLOSED;
        self.set_readiness(Tick::Clear(event.tick), |curr| curr - mask_no_closed);
    }

    pub(crate) fn clear_wakers(&self) {
        let mut waiters = self.waiters.lock();
        waiters.reader.take();
        waiters.writer.take();
    }
}

impl Drop for ScheduledIo {
    fn drop(&mut self) {
        self.wake(Ready::ALL);
    }
}

unsafe impl Send for ScheduledIo {}
unsafe impl Sync for ScheduledIo {}

impl ScheduledIo {
    /// An async version of `poll_readiness` which uses a linked list of wakers.
    pub(crate) async fn readiness(&self, interest: Interest) -> ReadyEvent {
        self.readiness_fut(interest).await
    }

    // This is in a separate function so that the borrow checker doesn't think
    // we are borrowing the `UnsafeCell` possibly over await boundaries.
    //
    // Go figure.
    fn readiness_fut(&self, interest: Interest) -> Readiness<'_> {
        Readiness {
            scheduled_io: self,
            state: State::Init,
            waiter: UnsafeCell::new(Waiter {
                pointers: linked_list::Pointers::new(),
                waker: None,
                is_ready: false,
                interest,
                _p: PhantomPinned,
            }),
        }
    }
}

unsafe impl linked_list::Link for Waiter {
    type Handle = NonNull<Waiter>;
    type Target = Waiter;

    fn as_raw(handle: &NonNull<Waiter>) -> NonNull<Waiter> {
        *handle
    }

    unsafe fn from_raw(ptr: NonNull<Waiter>) -> NonNull<Waiter> {
        ptr
    }

    unsafe fn pointers(target: NonNull<Waiter>) -> NonNull<linked_list::Pointers<Waiter>> {
        unsafe { Waiter::addr_of_pointers(target) }
    }
}

// ===== impl Readiness =====

impl Future for Readiness<'_> {
    type Output = ReadyEvent;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        use std::sync::atomic::Ordering::SeqCst;

        let (scheduled_io, state, waiter) = {
            // Safety: `Self` is `!Unpin`
            //
            // While we could use `pin_project!` to remove
            // this unsafe block, there are already unsafe blocks here,
            // so it wouldn't significantly ease the mental burden
            // and would actually complicate the code.
            // That's why we didn't use it.
            let me = unsafe { self.get_unchecked_mut() };
            (me.scheduled_io, &mut me.state, &me.waiter)
        };

        loop {
            match *state {
                State::Init => {
                    // Optimistically check existing readiness
                    let curr = scheduled_io.readiness.load(SeqCst);
                    let is_shutdown = SHUTDOWN.unpack(curr) != 0;

                    // Safety: `waiter.interest` never changes
                    let interest = unsafe { (*waiter.get()).interest };
                    let ready = Ready::from_usize(READINESS.unpack(curr)).intersection(interest);

                    if !ready.is_empty() || is_shutdown {
                        // Currently ready!
                        let tick = TICK.unpack(curr) as u8;
                        *state = State::Done;
                        return Poll::Ready(ReadyEvent {
                            tick,
                            ready,
                            is_shutdown,
                        });
                    }

                    // Wasn't ready, take the lock (and check again while locked).
                    let mut waiters = scheduled_io.waiters.lock();

                    let curr = scheduled_io.readiness.load(SeqCst);
                    let mut ready = Ready::from_usize(READINESS.unpack(curr));
                    let is_shutdown = SHUTDOWN.unpack(curr) != 0;

                    if is_shutdown {
                        ready = Ready::ALL;
                    }

                    let ready = ready.intersection(interest);

                    if !ready.is_empty() || is_shutdown {
                        // Currently ready!
                        let tick = TICK.unpack(curr) as u8;
                        *state = State::Done;
                        return Poll::Ready(ReadyEvent {
                            tick,
                            ready,
                            is_shutdown,
                        });
                    }

                    // Not ready even after locked, insert into list...

                    // Safety: Since the `waiter` is not in the intrusive list yet,
                    // we have exclusive access to it. The Mutex ensures
                    // that this modification is visible to other threads that
                    // acquire the same Mutex.
                    let waker = unsafe { &mut (*waiter.get()).waker };
                    let old = waker.replace(cx.waker().clone());
                    debug_assert!(old.is_none(), "waker should be None at the first poll");

                    // Insert the waiter into the linked list
                    //
                    // safety: pointers from `UnsafeCell` are never null.
                    waiters
                        .list
                        .push_front(unsafe { NonNull::new_unchecked(waiter.get()) });
                    *state = State::Waiting;
                }
                State::Waiting => {
                    // Currently in the "Waiting" state, implying the caller has
                    // a waiter stored in the waiter list (guarded by
                    // `notify.waiters`). In order to access the waker fields,
                    // we must hold the lock.

                    let waiters = scheduled_io.waiters.lock();

                    // Safety: With the lock held, we have exclusive access to
                    // the waiter. In other words, `ScheduledIo::wake()`
                    // cannot access the waiter concurrently.
                    let w = unsafe { &mut *waiter.get() };

                    if w.is_ready {
                        // Our waker has been notified.
                        *state = State::Done;
                    } else {
                        // Update the waker, if necessary.
                        w.waker.as_mut().unwrap().clone_from(cx.waker());
                        return Poll::Pending;
                    }

                    // Explicit drop of the lock to indicate the scope that the
                    // lock is held. Because holding the lock is required to
                    // ensure safe access to fields not held within the lock, it
                    // is helpful to visualize the scope of the critical
                    // section.
                    drop(waiters);
                }
                State::Done => {
                    let curr = scheduled_io.readiness.load(Acquire);
                    let is_shutdown = SHUTDOWN.unpack(curr) != 0;

                    // The returned tick might be newer than the event
                    // which notified our waker. This is ok because the future
                    // still didn't return `Poll::Ready`.
                    let tick = TICK.unpack(curr) as u8;

                    // Safety: We don't need to acquire the lock here because
                    //   1. `State::Done`` means `waiter` is no longer shared,
                    //      this means no concurrent access to `waiter` can happen
                    //      at this point.
                    //   2. `waiter.interest` is never changed, this means
                    //      no side effects need to be synchronized by the lock.
                    let interest = unsafe { (*waiter.get()).interest };
                    // The readiness state could have been cleared in the meantime,
                    // but we allow the returned ready set to be empty.
                    let ready = Ready::from_usize(READINESS.unpack(curr)).intersection(interest);

                    return Poll::Ready(ReadyEvent {
                        tick,
                        ready,
                        is_shutdown,
                    });
                }
            }
        }
    }
}

impl Drop for Readiness<'_> {
    fn drop(&mut self) {
        let mut waiters = self.scheduled_io.waiters.lock();

        // Safety: `waiter` is only ever stored in `waiters`
        unsafe {
            waiters
                .list
                .remove(NonNull::new_unchecked(self.waiter.get()))
        };
    }
}

unsafe impl Send for Readiness<'_> {}
unsafe impl Sync for Readiness<'_> {}
