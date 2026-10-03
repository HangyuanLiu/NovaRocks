use crate::loom::sync::atomic::AtomicUsize;
use crate::runtime::io::{ScheduledIo, ScheduledIoHandle};
use crate::util::linked_list::{self, LinkedList};

use std::io;
use std::ptr::NonNull;
use std::sync::atomic::Ordering::{Acquire, Release};

// Kind of arbitrary, but buffering 16 `ScheduledIo`s doesn't seem like much
const NOTIFY_AFTER: usize = 16;

pub(super) struct RegistrationSet {
    num_pending_release: AtomicUsize,
}

pub(super) struct Synced {
    // True when the I/O driver shutdown. At this point, no more registrations
    // should be added to the set.
    is_shutdown: bool,

    // List of all registrations tracked by the set
    registrations: LinkedList<ScheduledIoHandle, ScheduledIo>,

    // Registrations that are pending drop. When a `Registration` is dropped, it
    // stores its `ScheduledIo` in this list. The I/O driver is responsible for
    // dropping it. This ensures the `ScheduledIo` is not freed while it can
    // still be included in an I/O event.
    pending_release: Vec<ScheduledIoHandle>,
    funded_registrations: LinkedList<ScheduledIoHandle, ScheduledIo>,
    funded_pending: LinkedList<RetirementLink, ScheduledIo>,
    funded_pending_count: usize,
}

impl RegistrationSet {
    pub(super) fn new() -> (RegistrationSet, Synced) {
        let set = RegistrationSet {
            num_pending_release: AtomicUsize::new(0),
        };

        let synced = Synced {
            is_shutdown: false,
            registrations: LinkedList::new(),
            pending_release: Vec::with_capacity(NOTIFY_AFTER),
            funded_registrations: LinkedList::new(),
            funded_pending: LinkedList::new(),
            funded_pending_count: 0,
        };

        (set, synced)
    }

    pub(super) fn is_shutdown(&self, synced: &Synced) -> bool {
        synced.is_shutdown
    }

    /// Returns `true` if there are registrations that need to be released
    pub(super) fn needs_release(&self) -> bool {
        self.num_pending_release.load(Acquire) != 0
    }

    pub(super) fn allocate(&self, synced: &mut Synced) -> io::Result<ScheduledIoHandle> {
        if synced.is_shutdown {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                crate::util::error::RUNTIME_SHUTTING_DOWN_ERROR,
            ));
        }

        let ret = ScheduledIoHandle::new(ScheduledIo::default());

        // Push a ref into the list of all resources.
        synced.registrations.push_front(ret.clone());

        Ok(ret)
    }

    #[cfg(feature = "io-util")]
    pub(super) fn prepare_with_owner(owner: bytes::Bytes) -> io::Result<ScheduledIoHandle> {
        ScheduledIo::allocation_capacity_bound()?;
        let ret = ScheduledIoHandle::new(ScheduledIo::with_owner(owner));
        // Preparation and all failure destruction are outside driver.synced.
        // Only the unpublished final Arc can first initialize this mutex.
        ret.prewarm();
        Ok(ret)
    }

    #[cfg(feature = "io-util")]
    pub(super) fn publish_funded(
        &self,
        synced: &mut Synced,
        io: &ScheduledIoHandle,
    ) -> io::Result<()> {
        if synced.is_shutdown {
            return Err(io::ErrorKind::Other.into());
        }
        synced.funded_registrations.push_front(io.clone());
        Ok(())
    }

    // Returns `true` if the caller should unblock the I/O driver to purge
    // registrations pending release.
    pub(super) fn deregister(&self, synced: &mut Synced, registration: &ScheduledIoHandle) -> bool {
        if registration.is_funded() {
            // Shutdown detached the original driver list and no event turn can
            // still dereference its token. The caller's handle remains alive.
            if synced.is_shutdown {
                return false;
            }
            synced.funded_pending.push_front(registration.clone());
            synced.funded_pending_count += 1;
        } else {
            synced.pending_release.push(registration.clone());
        }
        let len = synced
            .pending_release
            .len()
            .saturating_add(synced.funded_pending_count);
        self.num_pending_release.store(len, Release);
        // Funded registrations wake every retirement, including a single last
        // socket, so an idle driver does not postpone physical credit return.
        registration.is_funded() || len == NOTIFY_AFTER
    }

    pub(super) fn shutdown(&self, synced: &mut Synced) -> ShutdownRegistrations {
        if synced.is_shutdown {
            return ShutdownRegistrations::empty();
        }
        synced.is_shutdown = true;
        synced.pending_release.clear();
        // Default registrations keep their existing shutdown Vec behavior.
        let mut legacy = vec![];
        while let Some(io) = synced.registrations.pop_back() {
            legacy.push(io);
        }
        let funded = std::mem::replace(&mut synced.funded_registrations, LinkedList::new());
        let pending = std::mem::replace(&mut synced.funded_pending, LinkedList::new());
        synced.funded_pending_count = 0;
        self.num_pending_release.store(0, Release);
        ShutdownRegistrations {
            legacy,
            funded,
            pending,
        }
    }

    pub(super) fn release(&self, synced: &mut Synced) -> LinkedList<RetirementLink, ScheduledIo> {
        let pending = std::mem::take(&mut synced.pending_release);
        for io in pending {
            // SAFETY: this default registration is part of the original list.
            let _ = unsafe { self.remove(synced, &io) };
        }
        let funded = std::mem::replace(&mut synced.funded_pending, LinkedList::new());
        synced.funded_pending_count = 0;
        self.num_pending_release.store(0, Release);
        // The reactor removes/drops each funded node outside this locked call.
        funded
    }

    // This function is marked as unsafe, because the caller must make sure that
    // `io` is part of the registration set.
    pub(super) unsafe fn remove(
        &self,
        synced: &mut Synced,
        io: &ScheduledIoHandle,
    ) -> Option<ScheduledIoHandle> {
        // SAFETY: Pointers into an Arc are never null.
        let funded = io.is_funded();
        let io = unsafe { NonNull::new_unchecked(io.as_ptr().cast_mut()) };

        super::EXPOSE_IO.unexpose_provenance(io.as_ptr());
        // SAFETY: the caller guarantees that `io` is part of this list.
        if funded {
            unsafe { synced.funded_registrations.remove(io) }
        } else {
            unsafe { synced.registrations.remove(io) }
        }
    }
}

// Safety: `Arc` pins the inner data
unsafe impl linked_list::Link for ScheduledIoHandle {
    type Handle = ScheduledIoHandle;
    type Target = ScheduledIo;

    fn as_raw(handle: &Self::Handle) -> NonNull<ScheduledIo> {
        // safety: Arc::as_ptr never returns null
        unsafe { NonNull::new_unchecked(handle.as_ptr() as *mut _) }
    }

    unsafe fn from_raw(ptr: NonNull<Self::Target>) -> ScheduledIoHandle {
        // safety: the linked list currently owns a ref count
        unsafe { ScheduledIoHandle::from_raw(ptr.as_ptr() as *const _) }
    }

    unsafe fn pointers(
        target: NonNull<Self::Target>,
    ) -> NonNull<linked_list::Pointers<ScheduledIo>> {
        // safety: `target.as_ref().linked_list_pointers` is a `UnsafeCell` that
        // always returns a non-null pointer.
        unsafe { NonNull::new_unchecked(target.as_ref().linked_list_pointers.get()) }
    }
}

// A distinct Link implementation uses a second embedded pointer pair. Both
// intrusive memberships own independent strong counts of the same pinned Arc.
pub(super) struct RetirementLink;
unsafe impl linked_list::Link for RetirementLink {
    type Handle = ScheduledIoHandle;
    type Target = ScheduledIo;
    fn as_raw(handle: &Self::Handle) -> NonNull<ScheduledIo> {
        // SAFETY: every live strong handle has a nonnull Arc pointer.
        unsafe { NonNull::new_unchecked(handle.as_ptr().cast_mut()) }
    }
    unsafe fn from_raw(ptr: NonNull<Self::Target>) -> Self::Handle {
        // SAFETY: the pending list transfers its own strong count.
        unsafe { ScheduledIoHandle::from_raw(ptr.as_ptr()) }
    }
    unsafe fn pointers(
        target: NonNull<Self::Target>,
    ) -> NonNull<linked_list::Pointers<ScheduledIo>> {
        // SAFETY: the embedded UnsafeCell exists for this live pinned node.
        unsafe { NonNull::new_unchecked(target.as_ref().retirement_pointers.get()) }
    }
}

pub(super) struct ShutdownRegistrations {
    pub(super) legacy: Vec<ScheduledIoHandle>,
    pub(super) funded: LinkedList<ScheduledIoHandle, ScheduledIo>,
    pending: LinkedList<RetirementLink, ScheduledIo>,
}
impl ShutdownRegistrations {
    fn empty() -> Self {
        Self {
            legacy: Vec::new(),
            funded: LinkedList::new(),
            pending: LinkedList::new(),
        }
    }
}
impl Drop for ShutdownRegistrations {
    fn drop(&mut self) {
        // Intrusive lists do not implicitly drop their elements. Drain these
        // detached strong counts even when a shutdown notification unwinds.
        while let Some(io) = self.funded.pop_back() {
            drop(io);
        }
        while let Some(io) = self.pending.pop_back() {
            drop(io);
        }
    }
}
