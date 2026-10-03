//! Actual public HTTP API in a normal dependency, not a substitute map.
//! Forked from the historical status-field-arena driver; the original receipt
//! is unchanged. This snapshot includes the short claim-lock allocator fix.
//! Retirement counters observe original carrier ownership, not a product wallet
//! or whole connection allocation/OS scheduling proof.
use bytes::Bytes;
use http::header::{
    Entry, HeaderFieldAllocationPool, HeaderFieldFillError, HeaderMapAllocationPool,
};
use http::{HeaderMap, HeaderValue};
use std::cell::Cell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

fn pool(count: usize, keys: usize, extra: usize) -> HeaderMapAllocationPool {
    HeaderMapAllocationPool::new(count, keys, extra, Bytes::from_static(b"original")).unwrap()
}

#[test]
fn full_keys_still_accept_duplicates_replace_and_entry() {
    let pool = pool(1, 1, 2);
    let mut map = HeaderMap::try_from_allocation_pool(&pool).unwrap();
    assert_eq!(map.capacity(), 1);
    map.try_insert("x-a", HeaderValue::from_static("a"))
        .unwrap();
    map.try_append("x-a", HeaderValue::from_static("b"))
        .unwrap();
    if let Entry::Occupied(mut entry) = map.entry("x-a") {
        entry.try_append(HeaderValue::from_static("c")).unwrap();
        assert!(entry.try_append(HeaderValue::from_static("d")).is_err());
    } else {
        panic!("occupied key");
    }
    assert_eq!(
        map.get_all("x-a")
            .iter()
            .map(|v| v.to_str().unwrap())
            .collect::<Vec<_>>(),
        ["a", "b", "c"]
    );
    assert!(map
        .try_insert("x-b", HeaderValue::from_static("b"))
        .is_err());
    assert!(map.try_reserve(1).is_err());
    map.try_insert("x-a", HeaderValue::from_static("d"))
        .unwrap();
    assert_eq!(map.len(), 1);
    map.clear();
    map.try_insert("x-b", HeaderValue::from_static("e"))
        .unwrap();
    assert_eq!(pool.available_maps(), 0);
    drop(map);
    assert_eq!(pool.available_maps(), 1);
}

#[test]
fn eager_copy_claims_and_preserves_sensitive_values() {
    let pool = pool(2, 2, 2);
    let mut map = HeaderMap::try_from_allocation_pool(&pool).unwrap();
    let mut value = HeaderValue::from_static("secret");
    value.set_sensitive(true);
    map.insert("authorization", value);
    map.append("authorization", HeaderValue::from_static("other"));
    let mut copy = map.try_clone().unwrap();
    assert!(!std::ptr::eq(
        map.get("authorization").unwrap(),
        copy.get("authorization").unwrap()
    ));
    assert!(copy.get("authorization").unwrap().is_sensitive());
    copy.get_mut("authorization").unwrap().set_sensitive(false);
    assert!(map.get("authorization").unwrap().is_sensitive());
    assert!(map.try_clone().is_err());
    drop(copy);
    assert_eq!(pool.available_maps(), 1);
    drop(map.clone());
    assert_eq!(pool.available_maps(), 1);
}

#[test]
fn owning_iterator_holds_original_position() {
    let pool = pool(1, 2, 3);
    let mut map = HeaderMap::try_from_allocation_pool(&pool).unwrap();
    map.insert("x-a", HeaderValue::from_static("a"));
    map.append("x-a", HeaderValue::from_static("b"));
    map.append("x-a", HeaderValue::from_static("c"));
    map.insert("x-b", HeaderValue::from_static("d"));
    let mut iter = map.into_iter();
    assert_eq!(iter.next().unwrap().1, "a");
    assert_eq!(iter.next().unwrap().1, "b");
    assert!(format!("{iter:?}").contains("extra_slots"));
    assert_eq!(pool.available_maps(), 0);
    drop(iter);
    assert_eq!(pool.available_maps(), 1);
}

#[test]
fn extra_value_drain_claims_before_mutation() {
    let pool = pool(2, 2, 3);
    let mut map = HeaderMap::try_from_allocation_pool(&pool).unwrap();
    map.insert("x-a", HeaderValue::from_static("a"));
    map.append("x-a", HeaderValue::from_static("b"));
    map.append("x-a", HeaderValue::from_static("c"));
    let Entry::Occupied(entry) = map.entry("x-a") else {
        panic!("entry");
    };
    let (_, mut drain) = entry.remove_entry_mult();
    assert_eq!(pool.available_maps(), 0);
    assert_eq!(drain.next().unwrap(), "a");
    assert_eq!(drain.next().unwrap(), "b");
    assert_eq!(drain.next().unwrap(), "c");
    drop(drain);
    assert_eq!(pool.available_maps(), 1);
    map.insert("x-a", HeaderValue::from_static("a"));
    map.append("x-a", HeaderValue::from_static("b"));
    map.append("x-a", HeaderValue::from_static("c"));
    let copy = map.try_clone().unwrap();
    assert!(catch_unwind(AssertUnwindSafe(|| {
        let Entry::Occupied(entry) = map.entry("x-a") else {
            panic!("entry");
        };
        drop(entry.remove_entry_mult());
    }))
    .is_err());
    assert_eq!(map.len(), 3);
    assert_eq!(copy.len(), 3);
    drop(copy);
    let Entry::Occupied(mut entry) = map.entry("x-a") else {
        panic!("entry");
    };
    let mut replaced = entry.insert_mult(HeaderValue::from_static("replacement"));
    assert_eq!(replaced.next().unwrap(), "a");
    assert_eq!(replaced.next().unwrap(), "b");
    assert_eq!(replaced.next().unwrap(), "c");
    drop(replaced);
    assert_eq!(map.len(), 1);
}

#[test]
fn ordinary_cell_clone_and_panicking_iterator_drop() {
    let mut ordinary = HeaderMap::<Cell<i32>>::default();
    ordinary.insert("x-a", Cell::new(1));
    let copy = ordinary.clone();
    copy["x-a"].set(2);
    assert_eq!(ordinary["x-a"].get(), 1);

    struct Payload {
        id: usize,
        drops: Rc<Cell<usize>>,
    }
    impl Drop for Payload {
        fn drop(&mut self) {
            let old = self.drops.get();
            assert_eq!(old & (1 << self.id), 0, "double drop");
            self.drops.set(old | (1 << self.id));
            if self.id == 1 {
                panic!("one payload destructor");
            }
        }
    }
    let drops = Rc::new(Cell::new(0));
    let mut map = HeaderMap::<Payload>::default();
    for id in 0..4 {
        map.append(
            "x-a",
            Payload {
                id,
                drops: drops.clone(),
            },
        );
    }
    let mut iter = map.into_iter();
    drop(iter.next());
    assert!(catch_unwind(AssertUnwindSafe(|| drop(iter))).is_err());
    assert_eq!(drops.get(), 15);
}

#[test]
fn invalid_geometry_and_fixed_reserve() {
    assert!(HeaderMapAllocationPool::allocation_capacity_bound(0, 1, 1).is_err());
    assert!(HeaderMapAllocationPool::allocation_capacity_bound(1, 0, 1).is_err());
    assert!(HeaderMapAllocationPool::allocation_capacity_bound(1, usize::MAX, 1).is_err());
    assert!(HeaderMapAllocationPool::allocation_capacity_bound(usize::MAX, 1, 1).is_err());
    let pool = pool(1, 2, 0);
    let mut map = HeaderMap::try_from_allocation_pool(&pool).unwrap();
    map.try_reserve(map.capacity()).unwrap();
    map.insert("x-a", HeaderValue::from_static("a"));
    assert!(map
        .try_append("x-a", HeaderValue::from_static("b"))
        .is_err());
}

#[test]
fn ordinary_generic_clone_side_effects_and_panic_cleanup() {
    struct Value {
        id: usize,
        clones: Rc<Cell<usize>>,
        drops: Rc<Cell<usize>>,
    }
    impl Clone for Value {
        fn clone(&self) -> Self {
            self.clones.set(self.clones.get() + 1);
            if self.id == 1 {
                panic!("second clone");
            }
            Self {
                id: self.id,
                clones: self.clones.clone(),
                drops: self.drops.clone(),
            }
        }
    }
    impl Drop for Value {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }
    let clones = Rc::new(Cell::new(0));
    let drops = Rc::new(Cell::new(0));
    let mut map = HeaderMap::<Value>::default();
    for (id, name) in ["x-a", "x-b", "x-c"].into_iter().enumerate() {
        map.insert(
            name,
            Value {
                id,
                clones: clones.clone(),
                drops: drops.clone(),
            },
        );
    }
    assert!(catch_unwind(AssertUnwindSafe(|| drop(map.clone()))).is_err());
    assert_eq!(clones.get(), 2);
    assert_eq!(drops.get(), 1);
    assert_eq!(map.len(), 3);
    drop(map);
    assert_eq!(drops.get(), 4);
}

#[test]
fn aliases_share_one_connection_bind_even_across_threads() {
    let pool = std::sync::Arc::new(pool(2, 2, 2));
    let gate = std::sync::Arc::new(std::sync::Barrier::new(5));
    let mut handles = Vec::new();
    for _ in 0..4 {
        let pool = pool.clone();
        let gate = gate.clone();
        handles.push(std::thread::spawn(move || {
            gate.wait();
            pool.try_bind_connection().is_ok()
        }));
    }
    gate.wait();
    let winners = handles
        .into_iter()
        .map(|h| usize::from(h.join().unwrap()))
        .sum::<usize>();
    assert_eq!(winners, 1);
    assert!(pool.try_bind_connection().is_err());
    assert_eq!(pool.available_maps(), 2);
    let map = HeaderMap::try_from_allocation_pool(&pool).unwrap();
    assert_eq!(pool.available_maps(), 1);
    drop(map);
    assert_eq!(pool.available_maps(), 2);
}

#[test]
fn owned_merge_keeps_original_position_and_source_group_precedence() {
    let pool = pool(1, 3, 3);
    let mut source = HeaderMap::try_from_allocation_pool(&pool).unwrap();
    source.insert("host", HeaderValue::from_static("new"));
    source.append("host", HeaderValue::from_static("new-two"));
    let mut target = HeaderMap::new();
    target.insert("host", HeaderValue::from_static("old"));
    target.append("accept", HeaderValue::from_static("keep"));
    target.append("accept", HeaderValue::from_static("keep-two"));
    target.try_extend_map(source).unwrap();
    assert_eq!(pool.available_maps(), 0);
    assert!(target.allocation_pool().is_some());
    assert_eq!(
        target.get_all("host").iter().collect::<Vec<_>>(),
        ["new", "new-two"]
    );
    assert_eq!(
        target.get_all("accept").iter().collect::<Vec<_>>(),
        ["keep", "keep-two"]
    );
    drop(target);
    assert_eq!(pool.available_maps(), 1);
}

#[test]
fn fallible_grouped_merge_exhaustion_and_drop_reclaim_once() {
    let pool = pool(1, 1, 1);
    let mut target = HeaderMap::try_from_allocation_pool(&pool).unwrap();
    target.insert("host", HeaderValue::from_static("old"));
    let mut source = HeaderMap::new();
    source.append("host", HeaderValue::from_static("one"));
    source.append("host", HeaderValue::from_static("two"));
    source.append("host", HeaderValue::from_static("three"));
    assert!(target.try_extend(source).is_err());
    assert_eq!(
        target.get_all("host").iter().collect::<Vec<_>>(),
        ["one", "two"]
    );
    let mut previous = HeaderMap::new();
    previous.insert("accept", HeaderValue::from_static("old"));
    assert!(previous.try_extend_map(target).is_err());
    assert!(previous.allocation_pool().is_some());
    assert_eq!(pool.available_maps(), 0);
    drop(previous);
    assert_eq!(pool.available_maps(), 1);
}

fn fields(capacity: usize, positions: usize, max_field: usize) -> HeaderFieldAllocationPool {
    HeaderFieldAllocationPool::new(
        capacity,
        positions,
        max_field,
        Bytes::from_static(b"original-field-credit"),
    )
    .unwrap()
}

fn filled(pool: &HeaderFieldAllocationPool, len: usize, byte: u8) -> Bytes {
    pool.try_fill(len, |out| {
        out.fill(byte);
        Ok::<_, ()>(())
    })
    .unwrap()
}

#[derive(Default)]
struct Retirement {
    owner: AtomicUsize,
    wrapper_exit: AtomicUsize,
}

struct OriginalOwner(Arc<Retirement>);
impl AsRef<[u8]> for OriginalOwner {
    fn as_ref(&self) -> &[u8] {
        b"original"
    }
}
impl Drop for OriginalOwner {
    fn drop(&mut self) {
        assert_eq!(self.0.owner.fetch_add(1, Ordering::SeqCst), 0);
    }
}
struct OriginalExit {
    retired: Arc<Retirement>,
    panic: bool,
}
impl Drop for OriginalExit {
    fn drop(&mut self) {
        assert_eq!(self.retired.owner.load(Ordering::SeqCst), 1);
        assert_eq!(self.retired.wrapper_exit.fetch_add(1, Ordering::SeqCst), 0);
        if self.panic {
            panic!("original carrier exit");
        }
    }
}
fn original_credit(retired: &Arc<Retirement>, panic: bool) -> Bytes {
    Bytes::from_owner_with_exit_guard(
        OriginalOwner(retired.clone()),
        OriginalExit {
            retired: retired.clone(),
            panic,
        },
    )
}
fn assert_retired(retired: &Retirement, expected: usize) {
    assert_eq!(retired.owner.load(Ordering::SeqCst), expected);
    assert_eq!(retired.wrapper_exit.load(Ordering::SeqCst), expected);
}

#[test]
fn field_geometry_and_capacity_are_checked_before_construction() {
    for (capacity, positions, max) in [
        (0, 1, 1),
        (63, 1, 1),
        (64, 0, 1),
        (64, 2, 1),
        (64, 1, 0),
        (64, 1, 65),
        (usize::MAX & !63, 1, 1),
    ] {
        assert!(
            HeaderFieldAllocationPool::allocation_capacity_bound(capacity, positions, max).is_err()
        );
        assert!(HeaderFieldAllocationPool::new(capacity, positions, max, Bytes::new()).is_err());
    }
    let base = HeaderFieldAllocationPool::allocation_capacity_bound(256, 1, 128).unwrap();
    let two = HeaderFieldAllocationPool::allocation_capacity_bound(256, 2, 128).unwrap();
    assert!(base > 256);
    assert!(two > base, "another live owner wrapper needs prepayment");
    let pool = fields(256, 2, 128);
    assert_eq!(pool.capacity_bytes(), 256);
    assert_eq!(pool.field_positions(), 2);
    assert_eq!(pool.max_field_bytes(), 128);
    assert_eq!(pool.available_positions(), 2);
    assert!(pool.same_pool(&pool.clone()));
    assert!(!pool.same_pool(&fields(256, 2, 128)));
}

#[test]
fn position_and_size_refusal_do_not_invoke_fill_and_empty_never_claims() {
    let pool = fields(256, 1, 128);
    let called = Cell::new(false);
    assert!(matches!(
        pool.try_fill(129, |_| {
            called.set(true);
            Ok::<_, ()>(())
        }),
        Err(HeaderFieldFillError::TooLarge)
    ));
    assert!(!called.get());
    let held = filled(&pool, 128, b'a');
    assert_eq!(pool.available_positions(), 0);
    assert!(matches!(
        pool.try_fill(1, |_| {
            called.set(true);
            Ok::<_, ()>(())
        }),
        Err(HeaderFieldFillError::Exhausted)
    ));
    assert!(!called.get());
    let empty = pool
        .try_fill(0, |out| {
            assert!(out.is_empty());
            called.set(true);
            Ok::<_, ()>(())
        })
        .unwrap();
    assert!(called.get());
    assert!(empty.is_empty());
    assert_eq!(pool.available_positions(), 0);
    assert!(matches!(
        pool.try_fill(0, |_| Err::<(), _>(37u8)),
        Err(HeaderFieldFillError::Fill(37))
    ));
    drop(held);
    assert_eq!(pool.available_positions(), 1);
}

#[test]
fn bytes_clone_and_slice_hold_one_position_until_the_final_wrapper_exit() {
    let pool = fields(128, 1, 64);
    let payload = filled(&pool, 64, b'a');
    let original = payload.as_ptr();
    let alias = payload.clone();
    let slice = payload.slice(1..63);
    assert_eq!(slice.as_ptr(), original.wrapping_add(1));
    drop(payload);
    drop(alias);
    assert_eq!(pool.available_positions(), 0);
    assert_eq!(slice.as_ref(), &[b'a'; 62]);
    drop(slice);
    assert_eq!(pool.available_positions(), 1);
    let replacement = filled(&pool, 64, b'b');
    assert_eq!(replacement.as_ptr(), original);
    assert_eq!(replacement.as_ref(), &[b'b'; 64]);
}

#[test]
fn callback_error_preserves_its_type_and_reclaims_unpublished_partial_extent() {
    #[derive(Debug, PartialEq)]
    struct FillFailure(u32);
    let pool = fields(128, 1, 128);
    let failure = pool.try_fill(128, |out| {
        out.fill(b'x');
        Err::<(), _>(FillFailure(91))
    });
    match failure {
        Err(HeaderFieldFillError::Fill(error)) => assert_eq!(error, FillFailure(91)),
        other => panic!("original callback error required: {other:?}"),
    }
    assert_eq!(pool.available_positions(), 1);
    assert_eq!(filled(&pool, 128, b'y').as_ref(), &[b'y'; 128]);
    assert_eq!(pool.available_positions(), 1);
}

#[test]
fn callback_unwind_releases_both_extent_and_nonwaiting_gate() {
    let pool = fields(128, 1, 128);
    let failure = catch_unwind(AssertUnwindSafe(|| {
        let _ = pool.try_fill(128, |out| -> Result<(), ()> {
            out.fill(b'x');
            panic!("fill panic payload");
        });
    }))
    .unwrap_err();
    assert_eq!(failure.downcast_ref::<&str>(), Some(&"fill panic payload"));
    assert_eq!(pool.available_positions(), 1);
    let payload = filled(&pool, 128, b'z');
    assert_eq!(payload.as_ref(), &[b'z'; 128]);
    assert_eq!(pool.available_positions(), 0);
    drop(payload);
    assert_eq!(pool.available_positions(), 1);
}

#[test]
fn fragmentation_refuses_without_copying_or_overwriting_live_neighbors() {
    let pool = fields(256, 4, 128);
    let first = filled(&pool, 64, b'a');
    let second = filled(&pool, 64, b'b');
    let third = filled(&pool, 64, b'c');
    let fourth = filled(&pool, 64, b'd');
    let base = first.as_ptr();
    assert_eq!(second.as_ptr(), base.wrapping_add(64));
    assert_eq!(third.as_ptr(), base.wrapping_add(128));
    assert_eq!(fourth.as_ptr(), base.wrapping_add(192));
    drop(first);
    drop(third);
    assert_eq!(pool.available_positions(), 2);
    let called = Cell::new(false);
    assert!(matches!(
        pool.try_fill(128, |_| {
            called.set(true);
            Ok::<_, ()>(())
        }),
        Err(HeaderFieldFillError::Exhausted)
    ));
    assert!(!called.get());
    assert_eq!(second.as_ref(), &[b'b'; 64]);
    assert_eq!(fourth.as_ref(), &[b'd'; 64]);
    drop(second);
    let joined = filled(&pool, 128, b'e');
    assert_eq!(joined.as_ptr(), base);
    assert_eq!(joined.as_ref(), &[b'e'; 128]);
    assert_eq!(fourth.as_ref(), &[b'd'; 64]);
    drop(joined);
    drop(fourth);
    assert_eq!(pool.available_positions(), 4);
}

#[test]
fn reentrant_nonempty_fill_succeeds_with_disjoint_original_extents() {
    let pool = fields(256, 4, 128);
    let mut inner = None;
    let outer = pool
        .try_fill(128, |out| {
            out.fill(b'a');
            inner = Some(filled(&pool, 64, b'b'));
            assert_eq!(pool.available_positions(), 2);
            Ok::<_, ()>(())
        })
        .unwrap();
    let inner = inner.unwrap();
    assert_eq!(outer.as_ref(), &[b'a'; 128]);
    assert_eq!(inner.as_ref(), &[b'b'; 64]);
    assert_eq!(inner.as_ptr(), outer.as_ptr().wrapping_add(128));
    let next = filled(&pool, 64, b'c');
    assert_eq!(next.as_ptr(), inner.as_ptr().wrapping_add(64));
    assert_eq!(next.as_ref(), &[b'c'; 64]);
    drop((outer, inner, next));
    assert_eq!(pool.available_positions(), 4);
}

#[test]
fn neutral_pool_bind_is_once_across_concurrent_aliases() {
    let pool = fields(128, 2, 64);
    let gate = Arc::new(std::sync::Barrier::new(5));
    let mut handles = Vec::new();
    for _ in 0..4 {
        let alias = pool.clone();
        let gate = gate.clone();
        handles.push(std::thread::spawn(move || {
            gate.wait();
            alias.try_bind_once().is_ok()
        }));
    }
    gate.wait();
    assert_eq!(
        handles
            .into_iter()
            .map(|h| usize::from(h.join().unwrap()))
            .sum::<usize>(),
        1
    );
    assert!(pool.try_bind_once().is_err());
    assert_eq!(pool.available_positions(), 2);
    assert_eq!(filled(&pool, 64, b'a').as_ref(), &[b'a'; 64]);
}

#[test]
fn paused_original_fill_allows_another_disjoint_callback_to_complete() {
    let retired = Arc::new(Retirement::default());
    let pool =
        HeaderFieldAllocationPool::new(256, 4, 128, original_credit(&retired, false)).unwrap();
    let entered = Arc::new(std::sync::Barrier::new(2));
    let release = Arc::new(std::sync::Barrier::new(2));
    let worker = {
        let pool = pool.clone();
        let entered = entered.clone();
        let release = release.clone();
        std::thread::spawn(move || {
            pool.try_fill(128, |out| {
                out.fill(b'a');
                entered.wait();
                release.wait();
                assert_eq!(out, &[b'a'; 128]);
                Ok::<_, ()>(())
            })
            .unwrap()
        })
    };
    entered.wait();
    let other = filled(&pool, 64, b'b');
    assert_eq!(other.as_ref(), &[b'b'; 64]);
    assert_eq!(pool.available_positions(), 2);
    release.wait();
    let first = worker.join().unwrap();
    assert_eq!(first.as_ref(), &[b'a'; 128]);
    assert_eq!(other.as_ptr(), first.as_ptr().wrapping_add(128));
    drop(pool);
    assert_retired(&retired, 0);
    drop(first);
    assert_retired(&retired, 0);
    drop(other);
    assert_retired(&retired, 1);
}

#[test]
fn concurrent_extents_are_disjoint_and_last_header_alias_retires_original_carrier() {
    let retired = Arc::new(Retirement::default());
    let pool =
        HeaderFieldAllocationPool::new(256, 4, 64, original_credit(&retired, false)).unwrap();
    let start = Arc::new(std::sync::Barrier::new(5));
    let callbacks = Arc::new(std::sync::Barrier::new(5));
    let mut workers = Vec::new();
    for byte in b'a'..=b'd' {
        let pool = pool.clone();
        let start = start.clone();
        let callbacks = callbacks.clone();
        workers.push(std::thread::spawn(move || {
            start.wait();
            let bytes = pool
                .try_fill(64, |out| {
                    out.fill(byte);
                    callbacks.wait();
                    assert_eq!(out, &[byte; 64]);
                    Ok::<_, ()>(())
                })
                .unwrap();
            HeaderValue::from_maybe_shared(bytes).unwrap()
        }));
    }
    start.wait();
    callbacks.wait();
    let values = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(pool.available_positions(), 0);
    for (index, value) in values.iter().enumerate() {
        assert_eq!(value.as_bytes(), &[b'a' + index as u8; 64]);
        for earlier in &values[..index] {
            assert_ne!(value.as_bytes().as_ptr(), earlier.as_bytes().as_ptr());
        }
    }
    assert!(matches!(
        pool.try_fill(1, |_| -> Result<(), ()> {
            panic!("a real exhausted position must not invoke fill");
        }),
        Err(HeaderFieldFillError::Exhausted)
    ));
    let final_alias = values[3].clone();
    drop(values);
    assert_eq!(pool.available_positions(), 3);
    assert_eq!(final_alias.as_bytes(), &[b'd'; 64]);
    drop(pool);
    assert_retired(&retired, 0);
    drop(final_alias);
    assert_retired(&retired, 1);
}

#[test]
fn map_attachment_keeps_exact_capability_and_noarg_bind_keeps_none() {
    let fields = fields(256, 4, 128);
    let maps = pool(2, 2, 2);
    maps.try_bind_connection_with_fields(&fields).unwrap();
    assert!(maps.field_allocation_pool().unwrap().same_pool(&fields));
    assert!(
        fields.try_bind_once().is_ok(),
        "map attach does not bind the field arena"
    );
    assert!(fields.try_bind_once().is_err());
    assert!(maps.try_bind_connection_with_fields(&fields).is_err());
    let other_fields = self::fields(256, 4, 128);
    assert!(maps.try_bind_connection_with_fields(&other_fields).is_err());
    assert!(maps.field_allocation_pool().unwrap().same_pool(&fields));
    let map = HeaderMap::try_from_allocation_pool(&maps).unwrap();
    let copy = map.try_clone().unwrap();
    assert!(map.field_allocation_pool().unwrap().same_pool(&fields));
    assert!(copy.field_allocation_pool().unwrap().same_pool(&fields));
    let noarg = pool(1, 1, 0);
    noarg.try_bind_connection().unwrap();
    assert!(noarg
        .try_bind_connection_with_fields(&other_fields)
        .is_err());
    assert!(noarg.field_allocation_pool().is_none());
    let ordinary = HeaderMap::new();
    assert!(ordinary.field_allocation_pool().is_none());
    assert!(HeaderMap::try_from_allocation_pool(&noarg)
        .unwrap()
        .field_allocation_pool()
        .is_none());
}

#[test]
fn empty_maps_and_eager_copies_retain_attached_original_arena_until_last_map_exit() {
    let retired = Arc::new(Retirement::default());
    let fields =
        HeaderFieldAllocationPool::new(256, 4, 128, original_credit(&retired, false)).unwrap();
    let maps = pool(2, 2, 0);
    maps.try_bind_connection_with_fields(&fields).unwrap();
    let map = HeaderMap::try_from_allocation_pool(&maps).unwrap();
    let copy = map.try_clone().unwrap();
    assert!(copy.is_empty());
    drop(fields);
    drop(maps);
    assert_retired(&retired, 0);
    let payload = filled(copy.field_allocation_pool().unwrap(), 64, b'a');
    assert_eq!(payload.as_ref(), &[b'a'; 64]);
    drop(payload);
    drop(map);
    assert_retired(&retired, 0);
    drop(copy);
    assert_retired(&retired, 1);
}

#[test]
fn name_value_aliases_outlive_map_and_keep_the_original_field_arena() {
    let retired = Arc::new(Retirement::default());
    let fields =
        HeaderFieldAllocationPool::new(256, 4, 128, original_credit(&retired, false)).unwrap();
    let maps = pool(1, 1, 0);
    maps.try_bind_connection_with_fields(&fields).unwrap();
    let name_bytes = fields
        .try_fill(7, |out| {
            out.copy_from_slice(b"x-owned");
            Ok::<_, ()>(())
        })
        .unwrap();
    let value_bytes = fields
        .try_fill(5, |out| {
            out.copy_from_slice(b"value");
            Ok::<_, ()>(())
        })
        .unwrap();
    let name = http::header::HeaderName::from_lowercase_bytes(name_bytes.clone()).unwrap();
    let value = HeaderValue::from_maybe_shared(value_bytes.clone()).unwrap();
    assert_eq!(name.as_str().as_ptr(), name_bytes.as_ptr());
    assert_eq!(value.as_bytes().as_ptr(), value_bytes.as_ptr());
    let mut map = HeaderMap::try_from_allocation_pool(&maps).unwrap();
    map.try_insert(name.clone(), value.clone()).unwrap();
    drop(fields);
    drop(maps);
    drop(name_bytes);
    drop(value_bytes);
    drop(map);
    assert_retired(&retired, 0);
    assert_eq!(name.as_str(), "x-owned");
    assert_eq!(value.as_bytes(), b"value");
    drop(name);
    assert_retired(&retired, 0);
    drop(value);
    assert_retired(&retired, 1);
}

#[test]
fn owning_map_iterator_retains_field_capability_without_any_payload() {
    let retired = Arc::new(Retirement::default());
    let fields =
        HeaderFieldAllocationPool::new(128, 2, 64, original_credit(&retired, false)).unwrap();
    let maps = pool(1, 1, 0);
    maps.try_bind_connection_with_fields(&fields).unwrap();
    let map = HeaderMap::try_from_allocation_pool(&maps).unwrap();
    let mut iterator = map.into_iter();
    drop(fields);
    drop(maps);
    assert!(iterator.next().is_none());
    assert_retired(&retired, 0);
    drop(iterator);
    assert_retired(&retired, 1);
}

#[test]
fn field_original_exit_unwind_still_retires_the_map_original_owner() {
    let field_retired = Arc::new(Retirement::default());
    let map_retired = Arc::new(Retirement::default());
    let fields =
        HeaderFieldAllocationPool::new(128, 2, 64, original_credit(&field_retired, true)).unwrap();
    let maps = HeaderMapAllocationPool::new(1, 1, 0, original_credit(&map_retired, false)).unwrap();
    maps.try_bind_connection_with_fields(&fields).unwrap();
    let map = HeaderMap::try_from_allocation_pool(&maps).unwrap();
    drop(fields);
    drop(maps);
    assert_retired(&field_retired, 0);
    assert_retired(&map_retired, 0);
    let failure = catch_unwind(AssertUnwindSafe(|| drop(map))).unwrap_err();
    assert_eq!(
        failure.downcast_ref::<&str>(),
        Some(&"original carrier exit")
    );
    assert_retired(&field_retired, 1);
    assert_retired(&map_retired, 1);
}
