//! Scheduler-neutral observable callbacks used by execution queues and ports.

use std::alloc::Layout;
use std::any::Any;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

/// Callback invoked after an observable state transition.
pub type Observer = Arc<dyn Fn() + Send + Sync + 'static>;

type ObserverOwner = Arc<dyn Any + Send + Sync>;

// A root admits at most 64 drivers, each with distinct sink and finish
// registrations, plus one owner/observation slot. This is a local capacity
// bound, not a Native protocol or general scheduler limit.
const MAX_BOUNDED_OBSERVERS: usize = 129;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObservableCapacityError {
    InvalidCapacity,
    AllocationFailed,
    CapacityExceeded,
    BoundedRequired,
}

impl fmt::Display for ObservableCapacityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidCapacity => "bounded observable capacity exceeds 129",
            Self::AllocationFailed => "bounded observable backing allocation failed",
            Self::CapacityExceeded => "bounded observable registration capacity exceeded",
            Self::BoundedRequired => "retained observable subscriptions require a bounded registry",
        })
    }
}

impl std::error::Error for ObservableCapacityError {}

/// Thread-safe callback registry for execution readiness transitions.
pub struct Observable {
    observers: Mutex<Vec<Observer>>,
    subscriptions: Mutex<Subscriptions>,
    generation: AtomicU64,
    bounded_capacity: Option<usize>,
}

impl Observable {
    pub fn new() -> Self {
        Self {
            observers: Mutex::new(Vec::new()),
            subscriptions: Mutex::new(Subscriptions::Unbounded(Vec::new())),
            generation: AtomicU64::new(0),
            bounded_capacity: None,
        }
    }

    /// Creates a registry whose permanent and live scoped callbacks together
    /// never exceed capacity. Backing admission must precede this constructor.
    pub fn try_bounded(capacity: usize) -> Result<Self, ObservableCapacityError> {
        Self::bounded_backing_bytes(capacity)?;
        let mut observers = Vec::new();
        let mut subscriptions = Vec::new();
        observers
            .try_reserve_exact(capacity)
            .map_err(|_| ObservableCapacityError::AllocationFailed)?;
        subscriptions
            .try_reserve_exact(capacity)
            .map_err(|_| ObservableCapacityError::AllocationFailed)?;
        if observers.capacity() != capacity || subscriptions.capacity() != capacity {
            return Err(ObservableCapacityError::AllocationFailed);
        }
        for _ in 0..capacity {
            let slot = Arc::new(BoundedRegistration {
                observer: Mutex::new(None),
            });
            drop(slot.observer.lock().expect("observable bounded slot lock"));
            subscriptions.push(slot);
        }
        let observable = Self {
            observers: Mutex::new(observers),
            subscriptions: Mutex::new(Subscriptions::Bounded(subscriptions)),
            generation: AtomicU64::new(0),
            bounded_capacity: Some(capacity),
        };
        // Initialize platform mutex backings before publication, so concurrent
        // first use cannot temporarily allocate duplicate OnceBox candidates.
        drop(observable.observers.lock().expect("observable lock"));
        drop(
            observable
                .subscriptions
                .lock()
                .expect("observable subscription lock"),
        );
        Ok(observable)
    }

    /// Exact requested heap backing under pinned Rust 1.92: two Vec backings,
    /// capacity fixed Arc<BoundedRegistration> slots, and all Darwin lazy
    /// pthread mutex allocations. Excludes Self, its Arc wrapper and
    /// caller-owned callback closures/retained owners; callers must admit those
    /// separately. Retained subscriptions extend an existing backing lease.
    pub fn bounded_backing_bytes(capacity: usize) -> Result<usize, ObservableCapacityError> {
        if capacity > MAX_BOUNDED_OBSERVERS {
            return Err(ObservableCapacityError::InvalidCapacity);
        }
        let registration = Layout::new::<[usize; 2]>()
            .extend(Layout::new::<BoundedRegistration>())
            .map_err(|_| ObservableCapacityError::AllocationFailed)?
            .0
            .pad_to_align()
            .size();
        let observers = Layout::array::<Observer>(capacity)
            .map_err(|_| ObservableCapacityError::AllocationFailed)?
            .size();
        let subscriptions = Layout::array::<Arc<BoundedRegistration>>(capacity)
            .map_err(|_| ObservableCapacityError::AllocationFailed)?
            .size();
        let bytes = observers + subscriptions + registration * capacity;
        // Source-audited std 1.92 Darwin pthread_mutex_t: signature + 56 bytes.
        #[cfg(target_vendor = "apple")]
        let bytes = bytes
            + Layout::array::<(isize, [u8; 56])>(capacity + 2)
                .unwrap()
                .size();
        Ok(bytes)
    }

    /// Returns the monotonic transition generation observed by waiters.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub fn add_observer(&self, observer: Observer) {
        self.try_add_observer(observer)
            .expect("observable registration capacity");
    }

    pub fn try_add_observer(&self, observer: Observer) -> Result<(), ObservableCapacityError> {
        let mut observers = self.observers.lock().expect("observable lock");
        if let Some(capacity) = self.bounded_capacity {
            let subscriptions = self
                .subscriptions
                .lock()
                .expect("observable subscription lock");
            let Subscriptions::Bounded(slots) = &*subscriptions else {
                unreachable!("bounded observable slots");
            };
            if observers.len() + occupied_slots(slots) >= capacity {
                return Err(ObservableCapacityError::CapacityExceeded);
            }
        }
        observers.push(observer);
        Ok(())
    }

    /// A lifetime-scoped registration for a long-lived shared capacity owner.
    /// Repeated contexts must not leave one permanent callback per old root.
    pub fn subscribe(&self, observer: Observer) -> ObserverSubscription {
        self.try_subscribe(observer)
            .expect("observable subscription capacity")
    }

    pub fn try_subscribe(
        &self,
        observer: Observer,
    ) -> Result<ObserverSubscription, ObservableCapacityError> {
        self.try_subscribe_with_owner(observer, None)
    }

    /// Retains an existing backing lease through callback and slot destruction.
    /// This API requires a bounded registry and creates no new lease or backing.
    pub fn try_subscribe_retained(
        &self,
        observer: Observer,
        owner: Arc<dyn Any + Send + Sync>,
    ) -> Result<ObserverSubscription, ObservableCapacityError> {
        if self.bounded_capacity.is_none() {
            return Err(ObservableCapacityError::BoundedRequired);
        }
        self.try_subscribe_with_owner(observer, Some(owner))
    }

    fn try_subscribe_with_owner(
        &self,
        observer: Observer,
        owner: Option<ObserverOwner>,
    ) -> Result<ObserverSubscription, ObservableCapacityError> {
        // Bounded paths always acquire observers before subscriptions. Their
        // registration tails already exist; admission only clones a fixed slot.
        let observers = self
            .bounded_capacity
            .map(|_| self.observers.lock().expect("observable lock"));
        let mut subscriptions = self
            .subscriptions
            .lock()
            .expect("observable subscription lock");
        match &mut *subscriptions {
            Subscriptions::Unbounded(entries) => {
                entries.retain(|entry| entry.strong_count() != 0);
                let registration = Arc::new(ObserverRegistration { observer });
                entries.push(Arc::downgrade(&registration));
                Ok(ObserverSubscription {
                    registration: SubscriptionRegistration::Unbounded {
                        _registration: registration,
                    },
                })
            }
            Subscriptions::Bounded(slots) => {
                let capacity = self.bounded_capacity.expect("bounded observable capacity");
                if observers.as_ref().expect("bounded observer lock").len() + occupied_slots(slots)
                    >= capacity
                {
                    return Err(ObservableCapacityError::CapacityExceeded);
                }
                let slot = slots
                    .iter()
                    .find(|slot| Arc::strong_count(slot) == 1)
                    .expect("bounded observable free slot");
                let retained_owner = owner.clone();
                *slot.observer.lock().expect("observable bounded slot lock") =
                    Some((observer, owner));
                Ok(ObserverSubscription {
                    registration: SubscriptionRegistration::Bounded {
                        slot: Arc::clone(slot),
                        _owner: retained_owner,
                    },
                })
            }
        }
    }

    pub fn defer_notify(self: &Arc<Self>) -> DeferNotify {
        DeferNotify::new(Arc::clone(self))
    }

    pub fn notify_observers(&self) {
        // Publish the transition before invoking callbacks. A waiter that has
        // not registered yet can therefore detect the transition by comparing
        // the generation it froze before deciding to block.
        self.generation.fetch_add(1, Ordering::Release);
        if self.bounded_capacity.is_some() {
            self.notify_bounded();
            return;
        }
        let observers = self.observers.lock().expect("observable lock").clone();
        for observer in observers {
            observer();
        }
        let subscriptions = {
            let mut registry = self
                .subscriptions
                .lock()
                .expect("observable subscription lock");
            let Subscriptions::Unbounded(entries) = &mut *registry else {
                unreachable!("unbounded observable subscriptions");
            };
            entries.retain(|entry| entry.strong_count() != 0);
            entries.iter().filter_map(Weak::upgrade).collect::<Vec<_>>()
        };
        for registration in subscriptions {
            (registration.observer)();
        }
    }

    fn notify_bounded(&self) {
        let mut observers: [Option<Observer>; MAX_BOUNDED_OBSERVERS] =
            std::array::from_fn(|_| None);
        let mut subscriptions: [Option<BoundedSnapshot>; MAX_BOUNDED_OBSERVERS] =
            std::array::from_fn(|_| None);
        {
            let entries = self.observers.lock().expect("observable lock");
            let registry = self
                .subscriptions
                .lock()
                .expect("observable subscription lock");
            let Subscriptions::Bounded(scoped) = &*registry else {
                unreachable!("bounded observable slots");
            };
            for (slot, entry) in observers.iter_mut().zip(entries.iter()) {
                *slot = Some(Arc::clone(entry));
            }
            for (slot, entry) in subscriptions.iter_mut().zip(scoped.iter()) {
                let observer = entry.observer.lock().expect("observable bounded slot lock");
                if let Some((observer, owner)) = &*observer {
                    *slot = Some(BoundedSnapshot {
                        observer: Arc::clone(observer),
                        _slot: Arc::clone(entry),
                        _owner: owner.clone(),
                    });
                }
            }
        }
        // Callback invocation and captured-owner destruction happen after both
        // locks are released. Snapshots retain scoped slots until actual exit.
        for observer in observers.into_iter().flatten() {
            observer();
        }
        for registration in subscriptions.into_iter().flatten() {
            (registration.observer)();
        }
    }

    pub fn num_observers(&self) -> usize {
        self.observers.lock().expect("observable lock").len()
    }
}

struct ObserverRegistration {
    observer: Observer,
}

enum Subscriptions {
    Unbounded(Vec<Weak<ObserverRegistration>>),
    Bounded(Vec<Arc<BoundedRegistration>>),
}

struct BoundedRegistration {
    observer: Mutex<Option<(Observer, Option<ObserverOwner>)>>,
}

fn occupied_slots(slots: &[Arc<BoundedRegistration>]) -> usize {
    // Registry ownership is the one permanent strong handle. Scoped owners,
    // snapshots and their in-progress captured-owner destructors retain more.
    slots
        .iter()
        .filter(|slot| Arc::strong_count(slot) > 1)
        .count()
}

struct BoundedSnapshot {
    // Destroy the captured callback before making the fixed slot reusable.
    observer: Observer,
    _slot: Arc<BoundedRegistration>,
    // The metadata lease outlives the callback and the slot's actual Arc tail.
    _owner: Option<ObserverOwner>,
}

enum SubscriptionRegistration {
    Unbounded {
        _registration: Arc<ObserverRegistration>,
    },
    Bounded {
        // Field order is significant: the tail drops before its backing lease.
        slot: Arc<BoundedRegistration>,
        _owner: Option<ObserverOwner>,
    },
}

/// Retains a callback through its owner's lifetime. Unbounded registries keep
/// Weak entries. Bounded registries keep fixed reusable slots and snapshot
/// holders delay reuse until the captured callback has actually dropped.
pub struct ObserverSubscription {
    registration: SubscriptionRegistration,
}

impl Drop for ObserverSubscription {
    fn drop(&mut self) {
        if let SubscriptionRegistration::Bounded { slot, .. } = &self.registration {
            let observer = slot
                .observer
                .lock()
                .expect("observable bounded slot lock")
                .take();
            // No registry/slot lock is held while arbitrary captures drop.
            // self.registration keeps this slot occupied through actual Drop.
            drop(observer);
        }
    }
}

impl Default for Observable {
    fn default() -> Self {
        Self::new()
    }
}

/// Defers observable callbacks until the surrounding state transition ends.
#[must_use]
pub struct DeferNotify {
    observable: Arc<Observable>,
    armed: AtomicBool,
}

impl DeferNotify {
    pub fn new(observable: Arc<Observable>) -> Self {
        Self {
            observable,
            armed: AtomicBool::new(false),
        }
    }

    pub fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }
}

impl Drop for DeferNotify {
    fn drop(&mut self) {
        if self.armed.load(Ordering::Acquire) {
            self.observable.notify_observers();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scoped_len(observable: &Observable) -> usize {
        match &*observable.subscriptions.lock().unwrap() {
            Subscriptions::Unbounded(entries) => entries.len(),
            Subscriptions::Bounded(slots) => occupied_slots(slots),
        }
    }

    #[test]
    fn generation_is_published_before_callbacks() {
        let observable = Arc::new(Observable::new());
        let callback_generation = Arc::new(AtomicU64::new(u64::MAX));
        let observed = Arc::clone(&observable);
        let callback_generation_clone = Arc::clone(&callback_generation);
        observable.add_observer(Arc::new(move || {
            callback_generation_clone.store(observed.generation(), Ordering::Release);
        }));

        observable.notify_observers();

        assert_eq!(observable.generation(), 1);
        assert_eq!(callback_generation.load(Ordering::Acquire), 1);
    }

    #[test]
    fn generation_advances_without_registered_observers() {
        let observable = Observable::new();

        observable.notify_observers();
        observable.notify_observers();

        assert_eq!(observable.generation(), 2);
    }

    #[test]
    fn scoped_callbacks_do_not_accumulate_across_context_lifetimes() {
        let observable = Observable::new();
        let calls = Arc::new(AtomicU64::new(0));
        for expected in 1..=1024 {
            let counter = Arc::clone(&calls);
            let subscription = observable.subscribe(Arc::new(move || {
                counter.fetch_add(1, Ordering::Relaxed);
            }));
            observable.notify_observers();
            assert_eq!(calls.load(Ordering::Relaxed), expected);
            assert_eq!(scoped_len(&observable), 1);
            drop(subscription);
            observable.notify_observers();
            assert_eq!(calls.load(Ordering::Relaxed), expected);
            assert_eq!(scoped_len(&observable), 0);
        }
    }

    #[test]
    fn bounded_capacity_is_validated_before_construction() {
        assert!(matches!(
            Observable::try_bounded(MAX_BOUNDED_OBSERVERS + 1),
            Err(ObservableCapacityError::InvalidCapacity)
        ));
        assert_eq!(
            Observable::bounded_backing_bytes(usize::MAX),
            Err(ObservableCapacityError::InvalidCapacity)
        );
        let empty = Observable::try_bounded(0).unwrap();
        assert_eq!(
            empty.try_add_observer(Arc::new(|| {})),
            Err(ObservableCapacityError::CapacityExceeded)
        );
        empty.notify_observers();
        assert_eq!(empty.generation(), 1);
    }

    #[test]
    fn bounded_permanent_and_scoped_callbacks_share_capacity() {
        let observable = Observable::try_bounded(2).unwrap();
        observable.try_add_observer(Arc::new(|| {})).unwrap();
        let subscription = observable.try_subscribe(Arc::new(|| {})).unwrap();
        assert_eq!(
            observable.try_add_observer(Arc::new(|| {})),
            Err(ObservableCapacityError::CapacityExceeded)
        );
        assert!(matches!(
            observable.try_subscribe(Arc::new(|| {})),
            Err(ObservableCapacityError::CapacityExceeded)
        ));
        drop(subscription);
        observable.try_add_observer(Arc::new(|| {})).unwrap();
        assert_eq!(observable.num_observers(), 2);
        assert_eq!(scoped_len(&observable), 0);
    }

    #[test]
    fn bounded_callback_can_subscribe_after_snapshot_unlock() {
        let observable = Arc::new(Observable::try_bounded(2).unwrap());
        let installed = Arc::new(Mutex::new(None));
        let calls = Arc::new(AtomicU64::new(0));
        let weak = Arc::downgrade(&observable);
        let callback_installed = Arc::clone(&installed);
        let callback_calls = Arc::clone(&calls);
        observable
            .try_add_observer(Arc::new(move || {
                let mut installed = callback_installed.lock().unwrap();
                if installed.is_none() {
                    let calls = Arc::clone(&callback_calls);
                    *installed = Some(
                        weak.upgrade()
                            .unwrap()
                            .try_subscribe(Arc::new(move || {
                                calls.fetch_add(1, Ordering::Relaxed);
                            }))
                            .unwrap(),
                    );
                }
            }))
            .unwrap();

        observable.notify_observers();
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        observable.notify_observers();
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn bounded_snapshot_keeps_registration_charged_until_callback_exit() {
        let observable = Arc::new(Observable::try_bounded(1).unwrap());
        let owner = Arc::new(Mutex::new(None));
        let refused = Arc::new(AtomicBool::new(false));
        let weak = Arc::downgrade(&observable);
        let callback_owner = Arc::clone(&owner);
        let callback_refused = Arc::clone(&refused);
        let subscription = observable
            .try_subscribe(Arc::new(move || {
                let old = callback_owner.lock().unwrap().take();
                drop(old);
                callback_refused.store(
                    matches!(
                        weak.upgrade().unwrap().try_subscribe(Arc::new(|| {})),
                        Err(ObservableCapacityError::CapacityExceeded)
                    ),
                    Ordering::Relaxed,
                );
            }))
            .unwrap();
        *owner.lock().unwrap() = Some(subscription);

        observable.notify_observers();

        assert!(refused.load(Ordering::Relaxed));
        let replacement = observable.try_subscribe(Arc::new(|| {})).unwrap();
        assert_eq!(scoped_len(&observable), 1);
        drop(replacement);
    }

    #[test]
    fn bounded_notifications_reuse_fixed_vec_backings() {
        let observable = Observable::try_bounded(MAX_BOUNDED_OBSERVERS).unwrap();
        let observer_backing = observable.observers.lock().unwrap().as_ptr();
        let subscription_backing = {
            let registry = observable.subscriptions.lock().unwrap();
            let Subscriptions::Bounded(slots) = &*registry else {
                unreachable!();
            };
            slots.as_ptr()
        };
        for _ in 0..1024 {
            let subscription = observable.try_subscribe(Arc::new(|| {})).unwrap();
            observable.notify_observers();
            drop(subscription);
            observable.notify_observers();
        }
        let observers = observable.observers.lock().unwrap();
        let registry = observable.subscriptions.lock().unwrap();
        let Subscriptions::Bounded(subscriptions) = &*registry else {
            unreachable!();
        };
        assert_eq!(observers.capacity(), MAX_BOUNDED_OBSERVERS);
        assert_eq!(subscriptions.capacity(), MAX_BOUNDED_OBSERVERS);
        assert_eq!(observers.as_ptr(), observer_backing);
        assert_eq!(subscriptions.as_ptr(), subscription_backing);
        assert_eq!(occupied_slots(subscriptions), 0);
    }

    #[test]
    fn bounded_slot_is_not_reused_during_captured_owner_destruction() {
        struct ReenterOnDrop {
            observable: Weak<Observable>,
            refused: Arc<AtomicBool>,
        }
        impl Drop for ReenterOnDrop {
            fn drop(&mut self) {
                self.refused.store(
                    matches!(
                        self.observable
                            .upgrade()
                            .unwrap()
                            .try_subscribe(Arc::new(|| {})),
                        Err(ObservableCapacityError::CapacityExceeded)
                    ),
                    Ordering::Relaxed,
                );
            }
        }

        let observable = Arc::new(Observable::try_bounded(1).unwrap());
        let refused = Arc::new(AtomicBool::new(false));
        let capture = ReenterOnDrop {
            observable: Arc::downgrade(&observable),
            refused: Arc::clone(&refused),
        };
        let subscription = observable
            .try_subscribe(Arc::new(move || {
                let _ = &capture;
            }))
            .unwrap();

        drop(subscription);

        assert!(refused.load(Ordering::Relaxed));
        let replacement = observable.try_subscribe(Arc::new(|| {})).unwrap();
        assert_eq!(scoped_len(&observable), 1);
        drop(replacement);
        assert_eq!(scoped_len(&observable), 0);
    }

    #[test]
    fn bounded_maximum_snapshot_delivers_every_scoped_callback() {
        let observable = Observable::try_bounded(MAX_BOUNDED_OBSERVERS).unwrap();
        let calls = Arc::new(AtomicU64::new(0));
        let subscriptions = (0..MAX_BOUNDED_OBSERVERS)
            .map(|_| {
                let calls = Arc::clone(&calls);
                observable
                    .try_subscribe(Arc::new(move || {
                        calls.fetch_add(1, Ordering::Relaxed);
                    }))
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert!(matches!(
            observable.try_subscribe(Arc::new(|| {})),
            Err(ObservableCapacityError::CapacityExceeded)
        ));
        observable.notify_observers();
        assert_eq!(calls.load(Ordering::Relaxed), MAX_BOUNDED_OBSERVERS as u64);
        drop(subscriptions);
        observable.notify_observers();
        assert_eq!(calls.load(Ordering::Relaxed), MAX_BOUNDED_OBSERVERS as u64);
        assert_eq!(scoped_len(&observable), 0);
    }

    #[test]
    fn bounded_callback_panic_does_not_poison_registry_or_keep_snapshot() {
        let observable = Observable::try_bounded(2).unwrap();
        observable
            .try_add_observer(Arc::new(|| panic!("callback panic")))
            .unwrap();
        let subscription = observable.try_subscribe(Arc::new(|| {})).unwrap();

        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                observable.notify_observers();
            }))
            .is_err()
        );
        drop(subscription);

        assert_eq!(scoped_len(&observable), 0);
        let replacement = observable.try_subscribe(Arc::new(|| {})).unwrap();
        drop(replacement);
    }

    #[test]
    fn retained_owner_outlives_callback_and_last_slot_handle() {
        struct Owner {
            slot: Mutex<Weak<BoundedRegistration>>,
            callback_dropped: Arc<AtomicBool>,
            dropped: Arc<AtomicBool>,
        }
        impl Drop for Owner {
            fn drop(&mut self) {
                assert!(self.callback_dropped.load(Ordering::Relaxed));
                assert_eq!(self.slot.lock().unwrap().strong_count(), 0);
                self.dropped.store(true, Ordering::Relaxed);
            }
        }
        struct Capture {
            owner: Weak<Owner>,
            dropped: Arc<AtomicBool>,
        }
        impl Drop for Capture {
            fn drop(&mut self) {
                assert!(self.owner.upgrade().is_some());
                self.dropped.store(true, Ordering::Relaxed);
            }
        }

        for retain_snapshot in [false, true] {
            let observable = Observable::try_bounded(1).unwrap();
            let callback_dropped = Arc::new(AtomicBool::new(false));
            let owner_dropped = Arc::new(AtomicBool::new(false));
            let owner = Arc::new(Owner {
                slot: Mutex::new(Weak::new()),
                callback_dropped: Arc::clone(&callback_dropped),
                dropped: Arc::clone(&owner_dropped),
            });
            let weak_owner = Arc::downgrade(&owner);
            let capture = Capture {
                owner: weak_owner.clone(),
                dropped: Arc::clone(&callback_dropped),
            };
            let retained_owner: ObserverOwner = owner.clone();
            let subscription = observable
                .try_subscribe_retained(
                    Arc::new(move || {
                        let _ = &capture;
                    }),
                    retained_owner,
                )
                .unwrap();
            let SubscriptionRegistration::Bounded { slot, .. } = &subscription.registration else {
                unreachable!();
            };
            *owner.slot.lock().unwrap() = Arc::downgrade(slot);
            let snapshot = retain_snapshot.then(|| {
                let stored = slot.observer.lock().unwrap();
                let (observer, owner) = stored.as_ref().unwrap();
                BoundedSnapshot {
                    observer: Arc::clone(observer),
                    _slot: Arc::clone(slot),
                    _owner: owner.clone(),
                }
            });
            drop(owner);
            drop(observable);

            drop(subscription);

            if retain_snapshot {
                assert!(!callback_dropped.load(Ordering::Relaxed));
                assert!(!owner_dropped.load(Ordering::Relaxed));
                assert!(weak_owner.upgrade().is_some());
            }
            drop(snapshot);
            assert!(callback_dropped.load(Ordering::Relaxed));
            assert!(owner_dropped.load(Ordering::Relaxed));
            assert!(weak_owner.upgrade().is_none());
        }
    }
}
