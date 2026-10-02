#![cfg(not(miri))]

use bytes::{Bytes, BytesMut};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

thread_local! { static TRACK_SIZE: Cell<usize> = const { Cell::new(0) }; }
static TARGET: AtomicUsize = AtomicUsize::new(0);
static FREED: AtomicBool = AtomicBool::new(false);
static OWNER_EXITED: AtomicBool = AtomicBool::new(false);
static GUARD_EXITED: AtomicBool = AtomicBool::new(false);

struct Probe;
#[global_allocator]
static ALLOCATOR: Probe = Probe;
unsafe impl GlobalAlloc for Probe {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        let selected = TRACK_SIZE
            .try_with(|size| size.get() != 0 && size.get() == layout.size())
            .unwrap_or(false);
        if selected {
            TARGET.store(pointer as usize, Ordering::SeqCst);
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        let selected = TARGET.load(Ordering::SeqCst) == pointer as usize;
        unsafe { System.dealloc(pointer, layout) };
        if selected {
            FREED.store(true, Ordering::SeqCst);
        }
    }
}
struct Owner {
    data: [u8; 857],
    fail: bool,
    fail_as_ref: bool,
}
impl AsRef<[u8]> for Owner {
    fn as_ref(&self) -> &[u8] {
        assert!(!self.fail_as_ref, "injected AsRef failure");
        &self.data
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        assert!(!FREED.load(Ordering::SeqCst));
        OWNER_EXITED.store(true, Ordering::SeqCst);
        assert!(!self.fail, "injected owner Drop failure");
    }
}
struct Guard;
impl Drop for Guard {
    fn drop(&mut self) {
        assert!(OWNER_EXITED.load(Ordering::SeqCst));
        assert!(
            FREED.load(Ordering::SeqCst),
            "guard returned before the actual wrapper deallocation"
        );
        GUARD_EXITED.store(true, Ordering::SeqCst);
    }
}

#[test]
fn allocator_exit_precedes_guard_on_normal_and_unwinding_paths() {
    for (fail, fail_as_ref) in [(false, false), (true, false), (false, true)] {
        TARGET.store(0, Ordering::SeqCst);
        FREED.store(false, Ordering::SeqCst);
        OWNER_EXITED.store(false, Ordering::SeqCst);
        GUARD_EXITED.store(false, Ordering::SeqCst);
        TRACK_SIZE
            .with(|size| size.set(Bytes::owner_with_exit_guard_metadata_size::<Owner, Guard>()));
        let construction = std::panic::catch_unwind(|| {
            Bytes::from_owner_with_exit_guard(
                Owner {
                    data: [1; 857],
                    fail,
                    fail_as_ref,
                },
                Guard,
            )
        });
        TRACK_SIZE.with(|size| size.set(0));
        assert_ne!(TARGET.load(Ordering::SeqCst), 0);
        if fail_as_ref {
            assert!(construction.is_err());
            assert!(FREED.load(Ordering::SeqCst));
            assert!(GUARD_EXITED.load(Ordering::SeqCst));
            continue;
        }
        let bytes = construction.unwrap();
        let alias = bytes.slice(1..);
        drop(bytes);
        assert!(!FREED.load(Ordering::SeqCst));
        assert!(!GUARD_EXITED.load(Ordering::SeqCst));
        let result = std::panic::catch_unwind(|| drop(alias));
        assert_eq!(result.is_err(), fail);
        assert!(FREED.load(Ordering::SeqCst));
        assert!(GUARD_EXITED.load(Ordering::SeqCst));
    }

    // Split/freeze uses this exact Shared Box in addition to the byte buffer.
    TARGET.store(0, Ordering::SeqCst);
    FREED.store(false, Ordering::SeqCst);
    let mut mutable = BytesMut::with_capacity(4096);
    mutable.extend_from_slice(b"shared storage");
    TRACK_SIZE.with(|size| size.set(BytesMut::shared_allocation_metadata_size()));
    let data = mutable.split_to(mutable.len()).freeze();
    TRACK_SIZE.with(|size| size.set(0));
    assert_ne!(TARGET.load(Ordering::SeqCst), 0);
    drop(mutable);
    assert!(!FREED.load(Ordering::SeqCst));
    drop(data);
    assert!(FREED.load(Ordering::SeqCst));
}
