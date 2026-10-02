//! Original fixed typed backing for one connection's resident streams and waiters.

use super::Stream;
use crate::frame::StreamId;
use bytes::Bytes;
use std::alloc::Layout;
use std::cell::UnsafeCell;
use std::fmt;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::Waker;

struct OriginalStorage {
    slots: Vec<FixedStreamSlot>,
    waiters: Vec<ResidentWaiter>,
}

struct Core {
    // A successful bind takes both allocations together. Later aliases can
    // observe only immutable geometry, never the extracted stream storage.
    storage: UnsafeCell<Option<OriginalStorage>>,
    bound: AtomicBool,
    max_streams: usize,
    max_waiters: usize,
    // The final Arc allocation and any still-unbound arrays exit first.
    _ownership: Bytes,
}

// SAFETY: only the once-only bind CAS winner accesses storage through the
// cell. Public aliases observe immutable metadata and never access the cell.
// The final strong Arc extraction exclusively owns Core before unbound
// storage is dropped; bound storage is owned by the noncloneable lease.
unsafe impl Sync for Core {}

/// Preallocated typed stream slots and readiness positions for one connection.
///
/// Obtain the complete Rust-requested allocation bound before construction.
/// Clones retain the same original owner and permit only one successful bind.
/// A resident slot includes closed streams that still have live references;
/// this is independent of the peer's concurrent-stream SETTINGS limit.
/// Stream-owned payloads, queues, socket/future metadata, stack storage and
/// enclosing shared connection structures are separate. Target-specific
/// internal resources outside these fixed allocations are not included.
pub struct StreamStoreBuffer {
    core: Option<Arc<Core>>,
}

impl StreamStoreBuffer {
    /// Checked Rust-requested bytes for the exact typed arrays and Core/Arc.
    ///
    /// Caller ownership-carrier metadata and allocator caches/RSS are separate.
    /// Arc's pinned standard-library allocation layout is its two atomic
    /// reference counters followed by the properly aligned Core value.
    pub fn allocation_capacity_bound(max_streams: usize, max_waiters: usize) -> io::Result<usize> {
        let (slots, waiters) = validate(max_streams, max_waiters)?;
        let (arc, _) = Layout::new::<[AtomicUsize; 2]>()
            .extend(Layout::new::<Core>())
            .map_err(|_| invalid("stream store Core/Arc layout overflow"))?;
        slots
            .size()
            .checked_add(waiters.size())
            .and_then(|bytes| bytes.checked_add(arc.pad_to_align().size()))
            .ok_or_else(|| invalid("stream store allocation size overflow"))
    }

    /// Construct fixed empty storage under the caller's original owner.
    /// No Stream is constructed and no waiter is installed during allocation.
    pub fn new(max_streams: usize, max_waiters: usize, ownership: Bytes) -> io::Result<Self> {
        Self::allocation_capacity_bound(max_streams, max_waiters)?;
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(max_streams)
            .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
        slots.resize_with(max_streams, || FixedStreamSlot {
            value: None,
            linked: false,
        });
        let mut waiters = Vec::new();
        waiters
            .try_reserve_exact(max_waiters)
            .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
        waiters.resize_with(max_waiters, ResidentWaiter::default);
        assert_eq!(slots.capacity(), max_streams);
        assert_eq!(waiters.capacity(), max_waiters);
        Ok(Self {
            core: Some(Arc::new(Core {
                storage: UnsafeCell::new(Some(OriginalStorage { slots, waiters })),
                bound: AtomicBool::new(false),
                max_streams,
                max_waiters,
                _ownership: ownership,
            })),
        })
    }

    /// Hard number of resident stream slots, including retained closed streams.
    pub fn max_resident_streams(&self) -> usize {
        self.core().max_streams
    }

    /// Fixed readiness registration count for this connection.
    pub fn max_waiters(&self) -> usize {
        self.core().max_waiters
    }

    pub(crate) fn bind(&self) -> io::Result<FixedStreamStore> {
        self.core()
            .bound
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| invalid("stream store buffer already bound to a connection"))?;
        // SAFETY: this alias retains a strong Core reference throughout bind.
        // The unique CAS winner alone accesses the cell; losing aliases never
        // read it. Neither bound nor storage is reset after this one move.
        let storage = unsafe { (&mut *self.core().storage.get()).take() }
            .expect("unbound stream store storage");
        Ok(FixedStreamStore {
            slots: storage.slots,
            waiters: storage.waiters,
            _owner: self.clone(),
        })
    }

    fn core(&self) -> &Core {
        self.core.as_ref().expect("live stream store buffer")
    }
}

impl Clone for StreamStoreBuffer {
    fn clone(&self) -> Self {
        Self {
            core: Some(Arc::clone(
                self.core.as_ref().expect("live stream store buffer"),
            )),
        }
    }
}
impl Drop for StreamStoreBuffer {
    fn drop(&mut self) {
        // No Weak or raw Arc escapes. The last strong alias first frees the
        // Arc allocation; moved Core drop then retires arrays and original owner.
        drop(Arc::into_inner(
            self.core.take().expect("live stream store buffer"),
        ));
    }
}
impl fmt::Debug for StreamStoreBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamStoreBuffer")
            .field("max_resident_streams", &self.max_resident_streams())
            .field("max_waiters", &self.max_waiters())
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub(super) struct FixedStreamSlot {
    pub(super) value: Option<Stream>,
    pub(super) linked: bool,
}

#[derive(Debug, Default)]
pub(super) struct ResidentWaiter {
    pub(super) leased: bool,
    pub(super) notify: bool,
    pub(super) waker: Option<Waker>,
}

#[derive(Debug)]
pub(crate) struct FixedStreamStore {
    // Drop order is part of the physical ownership contract, including unwind:
    // actual Stream/Waker payloads and both Vec allocations exit before owner.
    pub(super) slots: Vec<FixedStreamSlot>,
    pub(super) waiters: Vec<ResidentWaiter>,
    _owner: StreamStoreBuffer,
}

fn validate(max_streams: usize, max_waiters: usize) -> io::Result<(Layout, Layout)> {
    if max_streams == 0 || max_streams > u32::from(StreamId::MAX) as usize {
        return Err(invalid(
            "resident stream capacity must be nonzero and fit StreamId",
        ));
    }
    if max_waiters == 0 || max_waiters > u32::MAX as usize {
        return Err(invalid(
            "stream waiter capacity must be nonzero and fit u32",
        ));
    }
    let slots = Layout::array::<FixedStreamSlot>(max_streams)
        .map_err(|_| invalid("stream slot layout exceeds addressable allocation"))?;
    let waiters = Layout::array::<ResidentWaiter>(max_waiters)
        .map_err(|_| invalid("stream waiter layout exceeds addressable allocation"))?;
    Ok((slots, waiters))
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::Wake;

    struct Owner {
        exits: Arc<AtomicUsize>,
        waiter_exits: Option<Arc<AtomicUsize>>,
    }
    impl AsRef<[u8]> for Owner {
        fn as_ref(&self) -> &[u8] {
            &[]
        }
    }
    impl Drop for Owner {
        fn drop(&mut self) {
            if let Some(waiter_exits) = &self.waiter_exits {
                assert_eq!(waiter_exits.load(Ordering::SeqCst), 1);
            }
            self.exits.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct WaiterExit(Arc<AtomicUsize>);
    impl Wake for WaiterExit {
        fn wake(self: Arc<Self>) {}
    }
    impl Drop for WaiterExit {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn invalid_geometry_refuses_without_storage() {
        for (streams, waiters) in [(0, 1), (1, 0), (usize::MAX, 1), (1, usize::MAX)] {
            assert_eq!(
                StreamStoreBuffer::allocation_capacity_bound(streams, waiters)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
        }
    }

    #[test]
    fn all_slots_are_empty_and_every_alias_observes_once_only_binding() {
        let buffer = StreamStoreBuffer::new(7, 3, Bytes::new()).unwrap();
        let alias = buffer.clone();
        assert_eq!(alias.max_resident_streams(), 7);
        assert_eq!(alias.max_waiters(), 3);
        let fixed = buffer.bind().unwrap();
        assert_eq!(fixed.slots.len(), 7);
        assert_eq!(fixed.slots.capacity(), 7);
        assert!(fixed
            .slots
            .iter()
            .all(|slot| slot.value.is_none() && !slot.linked));
        assert_eq!(fixed.waiters.len(), 3);
        assert_eq!(fixed.waiters.capacity(), 3);
        assert!(fixed
            .waiters
            .iter()
            .all(|waiter| !waiter.leased && waiter.waker.is_none()));
        assert!(alias
            .bind()
            .unwrap_err()
            .to_string()
            .contains("already bound"));
        drop(fixed);
        assert!(buffer.bind().is_err());
    }

    #[test]
    fn bound_waiters_exit_before_the_last_original_owner() {
        let exits = Arc::new(AtomicUsize::new(0));
        let waiter_exits = Arc::new(AtomicUsize::new(0));
        let ownership = Bytes::from_owner(Owner {
            exits: exits.clone(),
            waiter_exits: Some(waiter_exits.clone()),
        });
        let buffer = StreamStoreBuffer::new(2, 1, ownership).unwrap();
        let mut fixed = buffer.bind().unwrap();
        fixed.waiters[0].waker = Some(Waker::from(Arc::new(WaiterExit(waiter_exits.clone()))));
        drop(buffer);
        assert_eq!(exits.load(Ordering::SeqCst), 0);
        drop(fixed);
        assert_eq!(waiter_exits.load(Ordering::SeqCst), 1);
        assert_eq!(exits.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn concurrent_binding_has_one_winner_and_aliases_keep_the_original_owner() {
        let exits = Arc::new(AtomicUsize::new(0));
        let buffer = StreamStoreBuffer::new(
            2,
            2,
            Bytes::from_owner(Owner {
                exits: exits.clone(),
                waiter_exits: None,
            }),
        )
        .unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let threads = (0..2)
            .map(|_| {
                let alias = buffer.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    let won = alias.bind().is_ok();
                    drop(alias);
                    won
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        let winners = threads
            .into_iter()
            .filter_map(|thread| {
                thread
                    .join()
                    .expect("binding thread panicked")
                    .then_some(())
            })
            .count();
        assert_eq!(winners, 1);
        assert_eq!(exits.load(Ordering::SeqCst), 0);
        assert!(buffer.bind().is_err());
        drop(buffer);
        assert_eq!(exits.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn unbound_original_owner_survives_until_the_final_alias() {
        let exits = Arc::new(AtomicUsize::new(0));
        let buffer = StreamStoreBuffer::new(
            1,
            1,
            Bytes::from_owner(Owner {
                exits: exits.clone(),
                waiter_exits: None,
            }),
        )
        .unwrap();
        let alias = buffer.clone();
        drop(buffer);
        assert_eq!(exits.load(Ordering::SeqCst), 0);
        drop(alias);
        assert_eq!(exits.load(Ordering::SeqCst), 1);
    }
}
