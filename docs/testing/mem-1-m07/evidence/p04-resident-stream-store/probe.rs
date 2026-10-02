//! Public scratch wrappers call the unchanged production stream store bind.
//! These probes establish the once-CAS primitive, not whole connection funding.

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use h2::StreamStoreBuffer;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::task::{Wake, Waker};

    struct OriginalOwner {
        exits: Arc<AtomicUsize>,
        waiter_exits: Option<Arc<AtomicUsize>>,
    }
    impl AsRef<[u8]> for OriginalOwner {
        fn as_ref(&self) -> &[u8] {
            &[]
        }
    }
    impl Drop for OriginalOwner {
        fn drop(&mut self) {
            if let Some(waiters) = &self.waiter_exits {
                assert_eq!(
                    waiters.load(Ordering::SeqCst),
                    1,
                    "actual retained Waker must exit before original owner"
                );
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
    fn owned(exits: &Arc<AtomicUsize>, waiter_exits: Option<Arc<AtomicUsize>>) -> Bytes {
        Bytes::from_owner(OriginalOwner {
            exits: exits.clone(),
            waiter_exits,
        })
    }

    #[test]
    fn concurrent_bind_has_one_winner_and_original_owner_waits_for_final_alias() {
        let exits = Arc::new(AtomicUsize::new(0));
        let buffer = StreamStoreBuffer::new(2, 2, owned(&exits, None)).unwrap();
        let barrier = Arc::new(Barrier::new(3));
        let threads = (0..2)
            .map(|_| {
                let alias = buffer.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    alias.m07_bind().ok()
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        let leases = threads
            .into_iter()
            .filter_map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            leases.len(),
            1,
            "once-only CAS must have one actual storage owner"
        );
        assert_eq!(leases[0].m07_empty_geometry(), (2, 2, true));
        assert!(buffer.m07_bind().is_err());
        drop(leases);
        assert_eq!(exits.load(Ordering::SeqCst), 0);
        let alias = buffer.clone();
        drop(buffer);
        assert_eq!(exits.load(Ordering::SeqCst), 0);
        drop(alias);
        assert_eq!(exits.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn unbound_arrays_and_owner_exit_only_after_last_public_alias() {
        let exits = Arc::new(AtomicUsize::new(0));
        let buffer = StreamStoreBuffer::new(3, 4, owned(&exits, None)).unwrap();
        let alias = buffer.clone();
        drop(buffer);
        assert_eq!(exits.load(Ordering::SeqCst), 0);
        drop(alias);
        assert_eq!(exits.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn bound_waker_and_arrays_precede_the_original_owner_exit() {
        let exits = Arc::new(AtomicUsize::new(0));
        let waiters = Arc::new(AtomicUsize::new(0));
        let buffer = StreamStoreBuffer::new(1, 1, owned(&exits, Some(waiters.clone()))).unwrap();
        let mut lease = buffer.m07_bind().unwrap();
        assert_eq!(lease.m07_empty_geometry(), (1, 1, true));
        lease.m07_install_waiter(0, Waker::from(Arc::new(WaiterExit(waiters.clone()))));
        drop(buffer);
        assert_eq!(exits.load(Ordering::SeqCst), 0);
        assert_eq!(waiters.load(Ordering::SeqCst), 0);
        drop(lease);
        assert_eq!(waiters.load(Ordering::SeqCst), 1);
        assert_eq!(exits.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn live_lease_keeps_original_owner_after_all_configuration_aliases_exit() {
        let exits = Arc::new(AtomicUsize::new(0));
        let buffer = StreamStoreBuffer::new(7, 3, owned(&exits, None)).unwrap();
        let alias = buffer.clone();
        let lease = buffer.m07_bind().unwrap();
        drop(buffer);
        drop(alias);
        assert_eq!(exits.load(Ordering::SeqCst), 0);
        assert_eq!(lease.m07_empty_geometry(), (7, 3, true));
        drop(lease);
        assert_eq!(exits.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn invalid_geometry_is_rejected_before_any_actual_storage_owner_is_bound() {
        for (streams, waiters) in [(0, 1), (1, 0), (usize::MAX, 1), (1, usize::MAX)] {
            assert_eq!(
                StreamStoreBuffer::allocation_capacity_bound(streams, waiters)
                    .unwrap_err()
                    .kind(),
                std::io::ErrorKind::InvalidInput
            );
        }
        let exits = Arc::new(AtomicUsize::new(0));
        assert!(StreamStoreBuffer::new(0, 1, owned(&exits, None)).is_err());
        assert_eq!(exits.load(Ordering::SeqCst), 1);
    }
}
