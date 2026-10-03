//! Original-funded observation of one connection's acquisition lifecycle.

use bytes::Bytes;
use std::alloc::Layout;
use std::fmt;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;

const UNBOUND: u8 = 0;
const BOUND: u8 = 1;
const IN_INITIAL: u8 = 2;
const INITIAL_COMPLETE: u8 = 3;
const IN_ACQUISITION: u8 = 4;
const ACQUISITION_COMPLETE: u8 = 5;
const RETIRED: u8 = 6;
const INSTALLING_OWNER: u8 = 7;

/// Observes state transitions without any internal mutex held.
///
/// Methods must be thread-safe, must not panic, and must not retain a connection
/// or this handle through a strong ownership cycle. Concurrent callbacks may
/// arrive in a different order from their atomic state transitions: a successful
/// callback started before retirement may still be delivering its event when
/// retirement is observed. Completion commits only after callback success, and
/// refuses if retirement won. Transient callbacks never permit later phases:
/// concurrent phase attempts return WouldBlock without waiting or spinning.
/// These events do not prove the exit of an outer future, task, socket or TLS.
pub trait ConnectionLifecycleObserver: Send + Sync + 'static {
    /// The peer's initial SETTINGS were applied and the initial writes flushed.
    fn on_initial_settings_complete(&self) -> io::Result<()> {
        Ok(())
    }
    /// The caller's complete acquisition verdict, including late-Ready checks.
    fn on_acquisition_complete(&self) -> io::Result<()> {
        Ok(())
    }
    /// Acquisition failed, was cancelled, or its bound connection retired.
    fn on_retiring(&self) -> io::Result<()> {
        Ok(())
    }
}

struct Core {
    state: AtomicU8,
    observer: Box<dyn ConnectionLifecycleObserver>,
    acquisition_owner_installed: AtomicBool,
    acquisition_owner: AtomicPtr<Bytes>,
    owner: Bytes,
}

/// Strong-only original ownership for lifecycle Core and observer allocations.
///
/// Obtain [`Self::allocation_capacity_bound`] before constructing the observer
/// and this handle. The bound covers the concrete observer Box, Core Arc and
/// one optional retained-acquisition Bytes Box. Observer-owned allocations, ownership-carrier
/// metadata, future/task/IO backing and all connection pools are separate.
/// Public clones allocate nothing and conservatively extend original funding.
/// No Arc, Weak or raw ownership escape is exposed. The final Core Arc allocation
/// is freed before the observer Box exits, and both exit before the owner Bytes.
pub struct ConnectionLifecycle {
    core: Option<Arc<Core>>,
}

impl ConnectionLifecycle {
    /// Requested layouts for observer Box, Core Arc and optional retention Box.
    /// This follows the standard-library ArcInner layout (two atomic counters
    /// followed by aligned Core). A zero-sized observer Box requests no bytes.
    pub fn allocation_capacity_bound<O: ConnectionLifecycleObserver>() -> io::Result<usize> {
        let arc = Layout::new::<[AtomicUsize; 2]>()
            .extend(Layout::new::<Core>())
            .map_err(|_| invalid("connection lifecycle layout overflow"))?
            .0
            .pad_to_align();
        arc.size()
            .checked_add(Layout::new::<O>().size())
            .and_then(|bytes| bytes.checked_add(Layout::new::<Bytes>().size()))
            .ok_or_else(|| invalid("connection lifecycle allocation size overflow"))
    }

    /// Construct after granting all original metadata, including the observer's
    /// own separately allocated members and the Bytes carrier, if any.
    pub fn new<O: ConnectionLifecycleObserver>(observer: O, owner: Bytes) -> io::Result<Self> {
        Self::allocation_capacity_bound::<O>()?;
        Ok(Self {
            core: Some(Arc::new(Core {
                state: AtomicU8::new(UNBOUND),
                observer: Box::new(observer),
                acquisition_owner_installed: AtomicBool::new(false),
                acquisition_owner: AtomicPtr::new(std::ptr::null_mut()),
                owner,
            })),
        })
    }

    /// Bind this family to exactly one connection. Dropping the lease never
    /// makes the family bindable again, even while public aliases remain.
    pub fn bind(&self) -> io::Result<BoundConnectionLifecycle> {
        self.core()
            .state
            .compare_exchange(UNBOUND, BOUND, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| invalid("connection lifecycle already bound or retired"))?;
        Ok(BoundConnectionLifecycle {
            lifecycle: self.clone(),
        })
    }

    /// Retain the original acquisition position through actual bound IO exit
    /// on failure. Install once before binding; aliases allocate no backing.
    /// The caller's ordered attempt wrapper must also retain its own alias.
    pub fn retain_acquisition_owner(&self, owner: Bytes) -> io::Result<()> {
        self.core()
            .acquisition_owner_installed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
        self.core()
            .state
            .compare_exchange(
                UNBOUND,
                INSTALLING_OWNER,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
        // Only the once-install winner allocates. Binding cannot race this
        // publication, and a concurrent retirement never resurrects UNBOUND.
        self.core()
            .acquisition_owner
            .store(Box::into_raw(Box::new(owner)), Ordering::Release);
        if self
            .core()
            .state
            .compare_exchange(
                INSTALLING_OWNER,
                UNBOUND,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            self.release_retained_acquisition();
            return Err(io::ErrorKind::ConnectionAborted.into());
        }
        Ok(())
    }

    /// Release only after the caller's successful final deadline verdict.
    /// Failed bound acquisitions keep this alias until their real IO exits.
    pub fn release_acquisition_owner(&self) -> io::Result<()> {
        if self.core().state.load(Ordering::Acquire) != ACQUISITION_COMPLETE {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        self.release_retained_acquisition();
        Ok(())
    }

    fn release_retained_acquisition(&self) {
        release_acquisition_pointer(&self.core().acquisition_owner);
    }

    /// Idempotently retire the family, including a failed pre-bind acquisition.
    /// The atomic retirement is permanent; only its first caller notifies.
    pub fn retire(&self) -> io::Result<()> {
        let previous = self.core().state.swap(RETIRED, Ordering::AcqRel);
        if previous == UNBOUND {
            // No bound IO exists. The outer ordered attempt still owns its
            // alias until its connector/handshake future physically exits.
            self.release_retained_acquisition();
        }
        if previous != RETIRED {
            self.core().observer.on_retiring()?;
        }
        Ok(())
    }

    /// Publish final acquisition only after initial SETTINGS and the caller's
    /// absolute-deadline/late-Ready verdict. The h2 connection retains its lease.
    pub fn on_acquisition_complete(&self) -> io::Result<()> {
        self.complete(
            INITIAL_COMPLETE,
            IN_ACQUISITION,
            ACQUISITION_COMPLETE,
            |observer| observer.on_acquisition_complete(),
        )
    }

    fn complete(
        &self,
        from: u8,
        transient: u8,
        completed: u8,
        callback: impl FnOnce(&dyn ConnectionLifecycleObserver) -> io::Result<()>,
    ) -> io::Result<()> {
        match self.core().state.compare_exchange(
            from,
            transient,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {}
            Err(state)
                if state == completed
                    || (completed == INITIAL_COMPLETE && state == ACQUISITION_COMPLETE) =>
            {
                return Ok(())
            }
            Err(IN_INITIAL | IN_ACQUISITION) => {
                // A primitive rejection has no independently allocated error backing.
                return Err(io::ErrorKind::WouldBlock.into());
            }
            Err(_) => return Err(invalid("connection lifecycle phase is not ready")),
        }
        if let Err(error) = callback(self.core().observer.as_ref()) {
            let _ = self.retire();
            return Err(error);
        }
        self.core()
            .state
            .compare_exchange(transient, completed, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| invalid("connection lifecycle retired during callback"))
    }

    fn core(&self) -> &Core {
        self.core.as_ref().expect("live connection lifecycle")
    }
}

impl Clone for ConnectionLifecycle {
    fn clone(&self) -> Self {
        Self {
            core: Some(Arc::clone(
                self.core.as_ref().expect("live connection lifecycle"),
            )),
        }
    }
}

impl Drop for ConnectionLifecycle {
    fn drop(&mut self) {
        if let Some(core) = Arc::into_inner(self.core.take().expect("live connection lifecycle")) {
            // Arc::into_inner frees ArcInner before returning Core. Keep owner
            // as a local so unwinding from observer Drop still frees its Box
            // before owner exits. There are no Weak owners retaining ArcInner.
            let Core {
                observer,
                acquisition_owner,
                owner,
                ..
            } = core;
            // This guard is declared after owner, so observer unwinding also
            // clears retained acquisition backing before the common carrier.
            let acquisition_owner = AcquisitionPointer(acquisition_owner);
            drop(observer);
            drop(acquisition_owner);
            drop(owner);
        }
    }
}

impl fmt::Debug for ConnectionLifecycle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionLifecycle")
            .field("state", &self.core().state.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

/// Non-cloneable connection lease. Dropping it permanently retires the family.
/// Initial SETTINGS completion and final acquisition publication are distinct:
/// the caller must perform its deadline/late-Ready verdict before final success.
/// This primitive adds no timer and does not decide either deadline.
#[derive(Debug)]
pub struct BoundConnectionLifecycle {
    lifecycle: ConnectionLifecycle,
}

impl BoundConnectionLifecycle {
    /// Notify once after the actual initial SETTINGS and write-flush phase.
    pub fn on_initial_settings_complete(&self) -> io::Result<()> {
        self.lifecycle
            .complete(BOUND, IN_INITIAL, INITIAL_COMPLETE, |observer| {
                observer.on_initial_settings_complete()
            })
    }

    /// Equivalent to [`ConnectionLifecycle::on_acquisition_complete`].
    pub fn on_acquisition_complete(&self) -> io::Result<()> {
        self.lifecycle.on_acquisition_complete()
    }

    /// Permanently retire without releasing this lease's original funding.
    pub fn retire(&self) -> io::Result<()> {
        self.lifecycle.retire()
    }
}

impl Drop for BoundConnectionLifecycle {
    fn drop(&mut self) {
        let _ = self.retire();
        self.lifecycle.release_retained_acquisition();
    }
}

struct AcquisitionPointer(AtomicPtr<Bytes>);

impl Drop for AcquisitionPointer {
    fn drop(&mut self) {
        release_acquisition_pointer(&self.0);
    }
}

fn release_acquisition_pointer(pointer: &AtomicPtr<Bytes>) {
    let owned = pointer.swap(std::ptr::null_mut(), Ordering::AcqRel);
    if !owned.is_null() {
        // SAFETY: one once-install publishes one Box; only the winner of this
        // atomic swap can take it. No pointer or borrowed reference escapes.
        // Moving Bytes out first frees the Box before its original owner exits.
        let owner = unsafe { *Box::from_raw(owned) };
        drop(owner);
    }
}

fn invalid(_message: &'static str) -> io::Error {
    // Keep the call-site diagnostic in source without allocating a custom error.
    io::ErrorKind::InvalidInput.into()
}
