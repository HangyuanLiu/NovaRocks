// Appended to the exact production module, without replacing its implementation.
#[cfg(test)]
mod primitive_probe {
    use super::*;
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;
    use std::ptr;
    use std::sync::atomic::{AtomicPtr, AtomicUsize};
    use std::sync::Barrier;

    thread_local! {
        static TRACK: Cell<bool> = const { Cell::new(false) };
        static RETAIN_MODE: Cell<bool> = const { Cell::new(false) };
    }
    static OBSERVER: AtomicPtr<u8> = AtomicPtr::new(ptr::null_mut());
    static CORE: AtomicPtr<u8> = AtomicPtr::new(ptr::null_mut());
    static ACQUISITION: AtomicPtr<u8> = AtomicPtr::new(ptr::null_mut());
    static CALLS: AtomicUsize = AtomicUsize::new(0);
    static BYTES: AtomicUsize = AtomicUsize::new(0);
    struct Tracked;
    // SAFETY: System receives unchanged allocator contracts. The ledger records
    // two constructor allocations, and clears pointers only after real dealloc.
    unsafe impl GlobalAlloc for Tracked {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let pointer = unsafe { System.alloc(layout) };
            if TRACK.try_with(Cell::get).unwrap_or(false) && !pointer.is_null() {
                BYTES.fetch_add(layout.size(), Ordering::AcqRel);
                let ordinal = CALLS.fetch_add(1, Ordering::AcqRel);
                if RETAIN_MODE.try_with(Cell::get).unwrap_or(false) {
                    ACQUISITION.store(pointer, Ordering::Release);
                    return pointer;
                }
                match ordinal {
                    0 => OBSERVER.store(pointer, Ordering::Release),
                    1 => CORE.store(pointer, Ordering::Release),
                    _ => {}
                }
            }
            pointer
        }
        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            unsafe { System.dealloc(pointer, layout) };
            let _ = ACQUISITION.compare_exchange(
                pointer,
                ptr::null_mut(),
                Ordering::AcqRel,
                Ordering::Acquire,
            );
            let _ = CORE.compare_exchange(
                pointer,
                ptr::null_mut(),
                Ordering::AcqRel,
                Ordering::Acquire,
            );
            let _ = OBSERVER.compare_exchange(
                pointer,
                ptr::null_mut(),
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
    }
    #[global_allocator]
    static ALLOCATOR: Tracked = Tracked;
    struct Scope;
    impl Drop for Scope {
        fn drop(&mut self) {
            TRACK.with(|v| v.set(false));
            RETAIN_MODE.with(|v| v.set(false));
        }
    }
    fn measured<T>(f: impl FnOnce() -> T) -> T {
        CALLS.store(0, Ordering::Release);
        BYTES.store(0, Ordering::Release);
        TRACK.with(|v| v.set(true));
        let scope = Scope;
        let value = f();
        drop(scope);
        value
    }
    fn measured_retain<T>(f: impl FnOnce() -> T) -> T {
        RETAIN_MODE.with(|v| v.set(true));
        measured(f)
    }
    #[derive(Default)]
    struct Ledger {
        initial: AtomicUsize,
        acquired: AtomicUsize,
        retiring: AtomicUsize,
        observer_exit: AtomicUsize,
        owner_exit: AtomicUsize,
        acquisition_exit: AtomicUsize,
        sequence: AtomicUsize,
        acquisition_exit_order: AtomicUsize,
        owner_exit_order: AtomicUsize,
    }
    struct Original(Arc<Ledger>);
    impl Drop for Original {
        fn drop(&mut self) {
            assert!(
                CORE.load(Ordering::Acquire).is_null(),
                "Core Arc must physically exit first"
            );
            assert!(
                OBSERVER.load(Ordering::Acquire).is_null(),
                "observer Box must physically exit first"
            );
            assert!(
                ACQUISITION.load(Ordering::Acquire).is_null(),
                "retained acquisition Box must physically exit before common original owner"
            );
            assert_eq!(self.0.observer_exit.load(Ordering::Acquire), 1);
            self.0.owner_exit_order.store(
                self.0.sequence.fetch_add(1, Ordering::AcqRel) + 1,
                Ordering::Release,
            );
            self.0.owner_exit.fetch_add(1, Ordering::AcqRel);
        }
    }
    struct AcquisitionOriginal(Arc<Ledger>);
    impl Drop for AcquisitionOriginal {
        fn drop(&mut self) {
            assert!(
                ACQUISITION.load(Ordering::Acquire).is_null(),
                "actual retained Bytes Box must deallocate before its original acquisition owner"
            );
            self.0.acquisition_exit_order.store(
                self.0.sequence.fetch_add(1, Ordering::AcqRel) + 1,
                Ordering::Release,
            );
            self.0.acquisition_exit.fetch_add(1, Ordering::AcqRel);
        }
    }
    fn acquisition_owner(ledger: &Arc<Ledger>) -> Bytes {
        Bytes::from_owner_with_exit_guard(Bytes::new(), AcquisitionOriginal(ledger.clone()))
    }
    fn retain(handle: &ConnectionLifecycle, wrapper: &Bytes) {
        assert!(ACQUISITION.load(Ordering::Acquire).is_null());
        let alias = wrapper.clone();
        let result = measured_retain(|| handle.retain_acquisition_owner(alias));
        result.unwrap();
        assert_eq!(CALLS.load(Ordering::Acquire), 1);
        assert_eq!(BYTES.load(Ordering::Acquire), Layout::new::<Bytes>().size());
        assert!(!ACQUISITION.load(Ordering::Acquire).is_null());
    }
    struct Observer {
        ledger: Arc<Ledger>,
        gate: Option<Arc<Barrier>>,
        fail_initial: bool,
        block_acquisition: bool,
        panic_drop: bool,
    }
    impl ConnectionLifecycleObserver for Observer {
        fn on_initial_settings_complete(&self) -> io::Result<()> {
            self.ledger.initial.fetch_add(1, Ordering::AcqRel);
            if !self.block_acquisition {
                if let Some(gate) = &self.gate {
                    gate.wait();
                    gate.wait();
                }
            }
            if self.fail_initial {
                Err(io::Error::other("initial observer refusal"))
            } else {
                Ok(())
            }
        }
        fn on_acquisition_complete(&self) -> io::Result<()> {
            self.ledger.acquired.fetch_add(1, Ordering::AcqRel);
            if self.block_acquisition {
                if let Some(gate) = &self.gate {
                    gate.wait();
                    gate.wait();
                }
            }
            Ok(())
        }
        fn on_retiring(&self) -> io::Result<()> {
            self.ledger.retiring.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
    }
    impl Drop for Observer {
        fn drop(&mut self) {
            assert!(
                CORE.load(Ordering::Acquire).is_null(),
                "actual Arc free precedes observer destructor"
            );
            self.ledger.observer_exit.fetch_add(1, Ordering::AcqRel);
            if self.panic_drop {
                panic!("intentional observer destructor panic");
            }
        }
    }
    fn fixture(
        gate: Option<Arc<Barrier>>,
        fail_initial: bool,
        block_acquisition: bool,
        panic_drop: bool,
    ) -> (ConnectionLifecycle, Arc<Ledger>) {
        assert!(CORE.load(Ordering::Acquire).is_null());
        assert!(OBSERVER.load(Ordering::Acquire).is_null());
        let ledger = Arc::new(Ledger::default());
        let owner = Bytes::from_owner_with_exit_guard(Bytes::new(), Original(ledger.clone()));
        let observer = Observer {
            ledger: ledger.clone(),
            gate,
            fail_initial,
            block_acquisition,
            panic_drop,
        };
        let bound = ConnectionLifecycle::allocation_capacity_bound::<Observer>().unwrap();
        let result = measured(|| ConnectionLifecycle::new(observer, owner));
        let handle = result.unwrap();
        assert_eq!(CALLS.load(Ordering::Acquire), 2);
        assert_eq!(
            BYTES.load(Ordering::Acquire) + Layout::new::<Bytes>().size(),
            bound
        );
        (handle, ledger)
    }
    fn ordinary() -> (ConnectionLifecycle, Arc<Ledger>) {
        fixture(None, false, false, false)
    }

    #[test]
    fn actual_core_and_observer_layouts_exit_before_original_owner() {
        let (handle, ledger) = ordinary();
        let arc = Layout::new::<[AtomicUsize; 2]>()
            .extend(Layout::new::<Core>())
            .unwrap()
            .0
            .pad_to_align();
        assert_eq!(
            BYTES.load(Ordering::Acquire),
            arc.size() + Layout::new::<Observer>().size()
        );
        println!(
            "Actual Arc={}B observer={}B prepaid optional BytesBox={}B; carrier/observer members excluded",
            arc.size(),
            Layout::new::<Observer>().size(),
            Layout::new::<Bytes>().size()
        );
        drop(handle);
        assert_eq!(ledger.owner_exit.load(Ordering::Acquire), 1);
    }
    #[test]
    fn clone_bind_and_success_events_allocate_nothing_and_notify_once() {
        let (handle, ledger) = ordinary();
        let (alias, lease) = measured(|| (handle.clone(), handle.bind().unwrap()));
        assert_eq!(CALLS.load(Ordering::Acquire), 0);
        measured(|| {
            lease.on_initial_settings_complete().unwrap();
            lease.on_initial_settings_complete().unwrap();
            handle.on_acquisition_complete().unwrap();
            lease.on_acquisition_complete().unwrap();
            lease.retire().unwrap();
            handle.retire().unwrap();
        });
        assert_eq!(CALLS.load(Ordering::Acquire), 0);
        assert_eq!(ledger.initial.load(Ordering::Acquire), 1);
        assert_eq!(ledger.acquired.load(Ordering::Acquire), 1);
        assert_eq!(ledger.retiring.load(Ordering::Acquire), 1);
        drop(handle);
        drop(alias);
        assert_eq!(ledger.owner_exit.load(Ordering::Acquire), 0);
        drop(lease);
        assert_eq!(ledger.owner_exit.load(Ordering::Acquire), 1);
    }
    #[test]
    fn final_success_requires_bind_initial_success_and_live_family() {
        let (handle, ledger) = ordinary();
        assert_eq!(
            handle.on_acquisition_complete().unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        let lease = handle.bind().unwrap();
        assert!(handle.on_acquisition_complete().is_err());
        lease.on_initial_settings_complete().unwrap();
        drop(lease);
        assert!(handle.on_acquisition_complete().is_err());
        assert!(handle.bind().is_err());
        assert_eq!(ledger.acquired.load(Ordering::Acquire), 0);
        assert_eq!(ledger.retiring.load(Ordering::Acquire), 1);
        drop(handle);
    }
    #[test]
    fn early_duplicate_and_retired_rejections_allocate_nothing() {
        let (handle, ledger) = ordinary();
        let early = measured(|| handle.on_acquisition_complete());
        assert_eq!(CALLS.load(Ordering::Acquire), 0);
        assert_eq!(early.unwrap_err().kind(), io::ErrorKind::InvalidInput);
        let lease = handle.bind().unwrap();
        let (duplicate, early) = measured(|| (handle.bind(), handle.on_acquisition_complete()));
        assert_eq!(CALLS.load(Ordering::Acquire), 0);
        assert_eq!(duplicate.unwrap_err().kind(), io::ErrorKind::InvalidInput);
        assert_eq!(early.unwrap_err().kind(), io::ErrorKind::InvalidInput);
        handle.retire().unwrap();
        let (initial, acquired, rebound) = measured(|| {
            (
                lease.on_initial_settings_complete(),
                handle.on_acquisition_complete(),
                handle.bind(),
            )
        });
        assert_eq!(CALLS.load(Ordering::Acquire), 0);
        assert_eq!(initial.unwrap_err().kind(), io::ErrorKind::InvalidInput);
        assert_eq!(acquired.unwrap_err().kind(), io::ErrorKind::InvalidInput);
        assert_eq!(rebound.unwrap_err().kind(), io::ErrorKind::InvalidInput);
        assert_eq!(ledger.retiring.load(Ordering::Acquire), 1);
        drop(lease);
        drop(handle);
    }
    #[test]
    fn prebind_retirement_is_permanent_and_idempotent() {
        let (handle, ledger) = ordinary();
        handle.retire().unwrap();
        handle.retire().unwrap();
        assert!(handle.bind().is_err());
        assert!(handle.on_acquisition_complete().is_err());
        assert_eq!(ledger.retiring.load(Ordering::Acquire), 1);
        drop(handle);
    }
    #[test]
    fn once_binding_race_returns_exactly_one_noncloneable_lease() {
        let (handle, ledger) = ordinary();
        let gate = Arc::new(Barrier::new(3));
        let joins: Vec<_> = (0..2)
            .map(|_| {
                let alias = handle.clone();
                let gate = gate.clone();
                std::thread::spawn(move || {
                    gate.wait();
                    alias.bind()
                })
            })
            .collect();
        gate.wait();
        let results: Vec<_> = joins.into_iter().map(|j| j.join().unwrap()).collect();
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
        drop(handle);
        assert_eq!(ledger.owner_exit.load(Ordering::Acquire), 0);
        drop(results);
        assert_eq!(ledger.retiring.load(Ordering::Acquire), 1);
        assert_eq!(ledger.owner_exit.load(Ordering::Acquire), 1);
    }
    #[test]
    fn concurrent_last_strong_drops_free_box_and_arc_once() {
        let (handle, ledger) = ordinary();
        let gate = Arc::new(Barrier::new(3));
        let joins: Vec<_> = (0..2)
            .map(|_| {
                let alias = handle.clone();
                let gate = gate.clone();
                std::thread::spawn(move || {
                    gate.wait();
                    drop(alias);
                })
            })
            .collect();
        drop(handle);
        gate.wait();
        for join in joins {
            join.join().unwrap();
        }
        assert_eq!(ledger.observer_exit.load(Ordering::Acquire), 1);
        assert_eq!(ledger.owner_exit.load(Ordering::Acquire), 1);
    }
    #[test]
    fn blocked_failing_initial_callback_cannot_publish_acquisition() {
        let gate = Arc::new(Barrier::new(2));
        let (handle, ledger) = fixture(Some(gate.clone()), true, false, false);
        let lease = handle.bind().unwrap();
        let join = std::thread::spawn(move || {
            let result = lease.on_initial_settings_complete();
            (lease, result)
        });
        gate.wait();
        let refused = measured(|| handle.on_acquisition_complete());
        assert_eq!(CALLS.load(Ordering::Acquire), 0);
        assert_eq!(refused.unwrap_err().kind(), io::ErrorKind::WouldBlock);
        gate.wait();
        let (lease, result) = join.join().unwrap();
        assert_eq!(result.unwrap_err().to_string(), "initial observer refusal");
        assert!(handle.on_acquisition_complete().is_err());
        assert!(lease.on_initial_settings_complete().is_err());
        assert_eq!(ledger.acquired.load(Ordering::Acquire), 0);
        assert_eq!(ledger.retiring.load(Ordering::Acquire), 1);
        drop(lease);
        drop(handle);
    }
    #[test]
    fn blocked_successful_acquisition_callback_cannot_commit_after_retirement() {
        let gate = Arc::new(Barrier::new(2));
        let (handle, ledger) = fixture(Some(gate.clone()), false, true, false);
        let lease = handle.bind().unwrap();
        lease.on_initial_settings_complete().unwrap();
        let alias = handle.clone();
        let join = std::thread::spawn(move || alias.on_acquisition_complete());
        gate.wait();
        let refused = measured(|| handle.on_acquisition_complete());
        assert_eq!(CALLS.load(Ordering::Acquire), 0);
        assert_eq!(refused.unwrap_err().kind(), io::ErrorKind::WouldBlock);
        handle.retire().unwrap();
        gate.wait();
        assert_eq!(
            join.join().unwrap().unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert!(handle.on_acquisition_complete().is_err());
        assert_eq!(ledger.retiring.load(Ordering::Acquire), 1);
        drop(lease);
        drop(handle);
    }
    #[test]
    fn observer_destructor_panic_still_frees_box_before_owner() {
        let (handle, ledger) = fixture(None, false, false, true);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(handle)));
        assert!(outcome.is_err());
        assert!(CORE.load(Ordering::Acquire).is_null());
        assert!(OBSERVER.load(Ordering::Acquire).is_null());
        assert_eq!(ledger.owner_exit.load(Ordering::Acquire), 1);
    }
    #[test]
    fn zero_sized_observer_requests_only_core_arc() {
        struct Empty;
        impl ConnectionLifecycleObserver for Empty {}
        let result = measured(|| ConnectionLifecycle::new(Empty, Bytes::new()));
        let handle = result.unwrap();
        // The ledger's first pointer represents Core for this special case.
        assert_eq!(CALLS.load(Ordering::Acquire), 1);
        assert_eq!(
            BYTES.load(Ordering::Acquire),
            ConnectionLifecycle::allocation_capacity_bound::<Empty>().unwrap()
                - Layout::new::<Bytes>().size()
        );
        drop(handle);
        assert!(OBSERVER.load(Ordering::Acquire).is_null());
    }
    #[test]
    fn retained_box_consumes_prepaid_layout_and_success_releases_with_bound_lease_alive() {
        let (handle, ledger) = ordinary();
        let wrapper = acquisition_owner(&ledger);
        retain(&handle, &wrapper);
        let lease = handle.bind().unwrap();
        lease.on_initial_settings_complete().unwrap();
        handle.on_acquisition_complete().unwrap();
        drop(wrapper);
        assert_eq!(ledger.acquisition_exit.load(Ordering::Acquire), 0);
        let result = measured(|| handle.release_acquisition_owner());
        assert_eq!(CALLS.load(Ordering::Acquire), 0);
        result.unwrap();
        assert!(ACQUISITION.load(Ordering::Acquire).is_null());
        assert_eq!(ledger.acquisition_exit.load(Ordering::Acquire), 1);
        assert_eq!(ledger.retiring.load(Ordering::Acquire), 0);
        assert_eq!(ledger.owner_exit.load(Ordering::Acquire), 0);
        // This proves a still-live bound lease, not an outer socket's lifetime.
        drop(lease);
        drop(handle);
    }
    #[test]
    fn bound_retirement_keeps_acquisition_until_actual_lease_drop() {
        let (handle, ledger) = ordinary();
        let wrapper = acquisition_owner(&ledger);
        retain(&handle, &wrapper);
        let lease = handle.bind().unwrap();
        drop(wrapper);
        handle.retire().unwrap();
        assert_eq!(ledger.acquisition_exit.load(Ordering::Acquire), 0);
        assert!(!ACQUISITION.load(Ordering::Acquire).is_null());
        let result = measured(|| handle.release_acquisition_owner());
        assert_eq!(CALLS.load(Ordering::Acquire), 0);
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
        drop(lease);
        assert_eq!(ledger.acquisition_exit.load(Ordering::Acquire), 1);
        assert!(ACQUISITION.load(Ordering::Acquire).is_null());
        assert_eq!(ledger.observer_exit.load(Ordering::Acquire), 0);
        drop(handle);
    }
    #[test]
    fn prebind_retirement_releases_extra_box_but_wrapper_alias_still_funds_attempt() {
        let (handle, ledger) = ordinary();
        let wrapper = acquisition_owner(&ledger);
        retain(&handle, &wrapper);
        handle.retire().unwrap();
        assert!(ACQUISITION.load(Ordering::Acquire).is_null());
        assert_eq!(ledger.acquisition_exit.load(Ordering::Acquire), 0);
        assert!(handle.bind().is_err());
        drop(wrapper);
        assert_eq!(ledger.acquisition_exit.load(Ordering::Acquire), 1);
        assert_eq!(ledger.owner_exit.load(Ordering::Acquire), 0);
        drop(handle);
    }
    #[test]
    fn once_install_refuses_reuse_before_and_after_success_release_without_allocating() {
        let (handle, ledger) = ordinary();
        let wrapper = acquisition_owner(&ledger);
        retain(&handle, &wrapper);
        let result = measured(|| handle.retain_acquisition_owner(wrapper.clone()));
        assert_eq!(CALLS.load(Ordering::Acquire), 0);
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
        let lease = handle.bind().unwrap();
        lease.on_initial_settings_complete().unwrap();
        handle.on_acquisition_complete().unwrap();
        handle.release_acquisition_owner().unwrap();
        let result = measured(|| handle.retain_acquisition_owner(wrapper.clone()));
        assert_eq!(CALLS.load(Ordering::Acquire), 0);
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
        drop(wrapper);
        drop(lease);
        drop(handle);
        assert_eq!(ledger.acquisition_exit.load(Ordering::Acquire), 1);
    }
    #[test]
    fn concurrent_success_release_and_bound_drop_take_retained_box_exactly_once() {
        let (handle, ledger) = ordinary();
        let wrapper = acquisition_owner(&ledger);
        retain(&handle, &wrapper);
        let lease = handle.bind().unwrap();
        lease.on_initial_settings_complete().unwrap();
        handle.on_acquisition_complete().unwrap();
        drop(wrapper);
        let gate = Arc::new(Barrier::new(3));
        let release = {
            let alias = handle.clone();
            let gate = gate.clone();
            std::thread::spawn(move || {
                gate.wait();
                alias.release_acquisition_owner()
            })
        };
        let exit = {
            let gate = gate.clone();
            std::thread::spawn(move || {
                gate.wait();
                drop(lease);
            })
        };
        gate.wait();
        let result = release.join().unwrap();
        if let Err(error) = result {
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        }
        exit.join().unwrap();
        assert!(ACQUISITION.load(Ordering::Acquire).is_null());
        assert_eq!(ledger.acquisition_exit.load(Ordering::Acquire), 1);
        assert_eq!(ledger.retiring.load(Ordering::Acquire), 1);
        drop(handle);
    }
    #[test]
    fn observer_destructor_panic_clears_some_acquisition_before_common_original_owner() {
        let (handle, ledger) = fixture(None, false, false, true);
        let wrapper = acquisition_owner(&ledger);
        retain(&handle, &wrapper);
        drop(wrapper);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(handle)));
        assert!(result.is_err());
        assert!(CORE.load(Ordering::Acquire).is_null());
        assert!(OBSERVER.load(Ordering::Acquire).is_null());
        assert!(ACQUISITION.load(Ordering::Acquire).is_null());
        assert_eq!(ledger.acquisition_exit.load(Ordering::Acquire), 1);
        assert_eq!(ledger.owner_exit.load(Ordering::Acquire), 1);
        assert!(
            ledger.acquisition_exit_order.load(Ordering::Acquire)
                < ledger.owner_exit_order.load(Ordering::Acquire)
        );
    }
}
