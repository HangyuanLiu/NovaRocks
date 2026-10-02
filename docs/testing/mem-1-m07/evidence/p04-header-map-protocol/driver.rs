//! Actual public HTTP API in a normal dependency, not a substitute map.
use bytes::Bytes;
use http::header::{Entry, HeaderMapAllocationPool};
use http::{HeaderMap, HeaderValue};
use std::cell::Cell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::Rc;

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
