//! Scheduler-neutral observable callbacks used by execution queues and ports.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

/// Callback invoked after an observable state transition.
pub type Observer = Arc<dyn Fn() + Send + Sync + 'static>;

/// Thread-safe callback registry for execution readiness transitions.
pub struct Observable {
    observers: Mutex<Vec<Observer>>,
    subscriptions: Mutex<Vec<Weak<ObserverRegistration>>>,
    generation: AtomicU64,
}

impl Observable {
    pub fn new() -> Self {
        Self {
            observers: Mutex::new(Vec::new()),
            subscriptions: Mutex::new(Vec::new()),
            generation: AtomicU64::new(0),
        }
    }

    /// Returns the monotonic transition generation observed by waiters.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub fn add_observer(&self, observer: Observer) {
        self.observers
            .lock()
            .expect("observable lock")
            .push(observer);
    }

    /// A lifetime-scoped registration for a long-lived shared capacity owner.
    /// Repeated contexts must not leave one permanent callback per old root.
    pub fn subscribe(&self, observer: Observer) -> ObserverSubscription {
        let registration = Arc::new(ObserverRegistration { observer });
        let mut subscriptions = self
            .subscriptions
            .lock()
            .expect("observable subscription lock");
        subscriptions.retain(|entry| entry.strong_count() != 0);
        subscriptions.push(Arc::downgrade(&registration));
        ObserverSubscription {
            _registration: registration,
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
        let observers = self.observers.lock().expect("observable lock").clone();
        for observer in observers {
            observer();
        }
        let subscriptions = {
            let mut entries = self
                .subscriptions
                .lock()
                .expect("observable subscription lock");
            entries.retain(|entry| entry.strong_count() != 0);
            entries.iter().filter_map(Weak::upgrade).collect::<Vec<_>>()
        };
        for registration in subscriptions {
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
/// Retains the registration through its actual owner's lifetime. The shared
/// observable retains only a Weak entry, reclaimed on notification/subscribe.
pub struct ObserverSubscription {
    _registration: Arc<ObserverRegistration>,
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
            assert_eq!(observable.subscriptions.lock().unwrap().len(), 1);
            drop(subscription);
            observable.notify_observers();
            assert_eq!(calls.load(Ordering::Relaxed), expected);
            assert_eq!(observable.subscriptions.lock().unwrap().len(), 0);
        }
    }
}
