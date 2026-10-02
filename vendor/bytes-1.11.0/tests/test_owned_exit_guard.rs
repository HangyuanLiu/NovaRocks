use bytes::{Bytes, BytesMut};
use std::marker::PhantomPinned;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};

#[derive(Default)]
struct Events {
    address: AtomicUsize,
    as_refs: AtomicUsize,
    owner_drops: AtomicUsize,
    guard_drops: AtomicUsize,
}
struct Owner {
    bytes: [u8; 7],
    events: Arc<Events>,
    panic_as_ref: bool,
    panic_drop: bool,
    _pinned: PhantomPinned,
}
impl AsRef<[u8]> for Owner {
    fn as_ref(&self) -> &[u8] {
        assert_eq!(self.events.as_refs.fetch_add(1, Ordering::SeqCst), 0);
        self.events
            .address
            .store(self as *const Self as usize, Ordering::SeqCst);
        assert!(!self.panic_as_ref, "injected AsRef failure");
        &self.bytes
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        assert_eq!(
            self.events.address.load(Ordering::SeqCst),
            self as *const Self as usize
        );
        assert_eq!(self.events.owner_drops.fetch_add(1, Ordering::SeqCst), 0);
        assert!(!self.panic_drop, "injected owner Drop failure");
    }
}
struct Guard(Arc<Events>);
impl Drop for Guard {
    fn drop(&mut self) {
        assert_eq!(self.0.owner_drops.load(Ordering::SeqCst), 1);
        assert_eq!(self.0.guard_drops.fetch_add(1, Ordering::SeqCst), 0);
    }
}
fn values(panic_as_ref: bool, panic_drop: bool) -> (Owner, Guard, Arc<Events>) {
    let events = Arc::new(Events::default());
    (
        Owner {
            bytes: *b"payload",
            events: Arc::clone(&events),
            panic_as_ref,
            panic_drop,
            _pinned: PhantomPinned,
        },
        Guard(Arc::clone(&events)),
        events,
    )
}
fn complete(events: &Events) {
    assert_eq!(events.as_refs.load(Ordering::SeqCst), 1);
    assert_eq!(events.owner_drops.load(Ordering::SeqCst), 1);
    assert_eq!(events.guard_drops.load(Ordering::SeqCst), 1);
}

#[test]
fn pinned_owner_and_guard_follow_last_slice_alias() {
    let (owner, guard, events) = values(false, false);
    let bytes = Bytes::from_owner_with_exit_guard(owner, guard);
    let tail = bytes.slice(2..);
    drop(bytes);
    assert_eq!(&tail[..], b"yload");
    assert_eq!(events.owner_drops.load(Ordering::SeqCst), 0);
    drop(tail);
    complete(&events);
}

#[test]
fn vector_conversion_copies_and_exits_only_its_reference() {
    let (owner, guard, events) = values(false, false);
    let bytes = Bytes::from_owner_with_exit_guard(owner, guard);
    let held = bytes.clone();
    let vec: Vec<u8> = bytes.into();
    assert_eq!(vec, b"payload");
    assert_eq!(events.guard_drops.load(Ordering::SeqCst), 0);
    drop(held);
    complete(&events);
    assert_eq!(vec, b"payload");
}

#[test]
fn mutable_conversion_is_a_separate_allocation() {
    let (owner, guard, events) = values(false, false);
    let bytes = Bytes::from_owner_with_exit_guard(owner, guard);
    let original_pointer = bytes.as_ptr();
    let mut converted: BytesMut = bytes.into();
    assert_ne!(converted.as_ptr(), original_pointer);
    complete(&events);
    converted[0] = b'P';
    assert_eq!(&converted[..], b"Payload");
}

#[test]
fn empty_and_high_alignment_owners_keep_their_guard() {
    #[repr(align(128))]
    struct Aligned([u8; 1]);
    impl AsRef<[u8]> for Aligned {
        fn as_ref(&self) -> &[u8] {
            &self.0[..0]
        }
    }
    struct Count(Arc<AtomicUsize>);
    impl Drop for Count {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let count = Arc::new(AtomicUsize::new(0));
    let bytes = Bytes::from_owner_with_exit_guard(Aligned([0]), Count(Arc::clone(&count)));
    assert!(bytes.is_empty());
    let alias = bytes.clone();
    drop(bytes);
    assert_eq!(count.load(Ordering::SeqCst), 0);
    drop(alias);
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert!(Bytes::owner_with_exit_guard_metadata_size::<Aligned, Count>() >= 256);
}

#[test]
fn as_ref_panic_still_exits_owner_and_guard() {
    let (owner, guard, events) = values(true, false);
    let failure = std::panic::catch_unwind(|| Bytes::from_owner_with_exit_guard(owner, guard));
    assert!(failure.is_err());
    complete(&events);
}

#[test]
fn owner_drop_panic_still_exits_guard_once() {
    let (owner, guard, events) = values(false, true);
    let bytes = Bytes::from_owner_with_exit_guard(owner, guard);
    let failure = std::panic::catch_unwind(|| drop(bytes));
    assert!(failure.is_err());
    complete(&events);
}

#[test]
fn concurrent_final_aliases_exit_once() {
    let (owner, guard, events) = values(false, false);
    let bytes = Bytes::from_owner_with_exit_guard(owner, guard);
    let alias = bytes.clone();
    let barrier = Arc::new(Barrier::new(2));
    let other = Arc::clone(&barrier);
    let thread = std::thread::spawn(move || {
        other.wait();
        drop(alias);
    });
    barrier.wait();
    drop(bytes);
    thread.join().unwrap();
    complete(&events);
}
