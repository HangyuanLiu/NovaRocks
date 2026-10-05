// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use std::{
    collections::HashMap,
    hash::{BuildHasherDefault, Hasher},
};

#[test]
fn fresh_string_table_has_independent_small_and_load_factor_layout_goldens() {
    assert_eq!(Layout::new::<(String, String)>().size(), 48);
    assert_eq!(Layout::new::<(String, String)>().align(), 8);
    let width = group_layout().unwrap().size();
    for (count, buckets) in [
        (1, 4),
        (3, 4),
        (7, 8),
        (14, 16),
        (15, 32),
        (28, 32),
        (29, 64),
    ] {
        let f = fresh_table_layout::<String, String>(count).unwrap();
        let aligned_pairs = (48 * buckets + width.max(8) - 1) & !(width.max(8) - 1);
        let golden =
            Layout::from_size_align(aligned_pairs + buckets + width, width.max(8)).unwrap();
        assert_eq!(f.buckets, buckets);
        assert_eq!(f.layout, Some(golden));
        assert_eq!(f.allocation_requests_upper_bound, 1);
        assert_eq!(f.request_bytes_upper_bound, golden.size());
    }
    let small = fresh_table_layout::<String, String>(3)
        .unwrap()
        .layout
        .unwrap();
    // The actual raw request can have an unpadded size.
    assert_eq!(small.size(), 192 + 4 + width);
    assert_ne!(small.size(), small.pad_to_align().size());
}

#[test]
fn fresh_generic_pairs_use_actual_size_alignment_and_small_type_thresholds() {
    let width = group_layout().unwrap().size();
    let tiny_buckets = if width == 16 { 16 } else { 8 };
    let zero = fresh_table_layout::<(), ()>(3).unwrap();
    let byte = fresh_table_layout::<u8, ()>(3).unwrap();
    assert_eq!(zero.buckets, tiny_buckets);
    assert_eq!(byte.buckets, tiny_buckets);
    assert_eq!(zero.layout.unwrap().size(), tiny_buckets + width);
    assert_eq!(byte.layout.unwrap().size(), 2 * tiny_buckets + width);
    let two = fresh_table_layout::<u8, u8>(3).unwrap();
    assert_eq!(two.buckets, if width == 16 { 8 } else { 4 });
    let aligned = fresh_table_layout::<[u128; 2], ()>(3).unwrap();
    let pair = Layout::new::<([u128; 2], ())>();
    let align = pair.align().max(width);
    let size = ((pair.size() * 4 + align - 1) & !(align - 1)) + 4 + width;
    assert_eq!(aligned.buckets, 4);
    assert_eq!(
        aligned.layout,
        Some(Layout::from_size_align(size, align).unwrap())
    );
}

#[test]
fn zero_table_has_no_request_and_target_uses_actual_public_group_carrier() {
    let f = fresh_table_layout::<String, String>(0).unwrap();
    assert_eq!(
        f,
        FreshTableFacts {
            layout: None,
            buckets: 0,
            allocation_requests_upper_bound: 0,
            request_bytes_upper_bound: 0
        }
    );
    let mut map = HashMap::<String, String>::new();
    map.try_reserve(0).unwrap();
    assert_eq!(map.capacity(), 0);
    assert_eq!(fresh_string_table_work_upper_bound(0, 0, 0).unwrap(), 0);
    assert_eq!(
        Layout::new::<usize>(),
        Layout::new::<Option<std::ptr::NonNull<()>>>()
    );
    #[cfg(all(
        not(miri),
        target_arch = "aarch64",
        target_feature = "neon",
        target_endian = "little"
    ))]
    assert_eq!(
        group_layout().unwrap(),
        Layout::new::<core::arch::aarch64::uint8x8_t>()
    );
    #[cfg(all(not(miri), target_arch = "x86_64", target_feature = "sse2"))]
    assert_eq!(
        group_layout().unwrap(),
        Layout::new::<core::arch::x86_64::__m128i>()
    );
}

#[test]
fn counts_layouts_and_opaque_work_overflows_remain_typed_arithmetic() {
    for count in [usize::MAX, usize::MAX / 8 + 1] {
        assert!(matches!(
            fresh_table_layout::<String, String>(count),
            Err(Arithmetic(_))
        ));
    }
    // Bucket arithmetic succeeds here, but the real pair allocation cannot fit.
    assert!(matches!(
        fresh_table_layout::<[u8; 1024], ()>(isize::MAX as usize / 1024),
        Err(Arithmetic(_))
    ));
    for result in [
        string_operations_work_upper_bound(usize::MAX, 1, 0, 0),
        string_operations_work_upper_bound(4, usize::MAX, 0, 0),
        string_operations_work_upper_bound(4, 1, usize::MAX, 0),
        string_operations_work_upper_bound(4, 1, 0, usize::MAX),
        source_iterator_work_upper_bound(usize::MAX, 0),
        source_iterator_work_upper_bound(0, usize::MAX),
    ] {
        assert!(matches!(result, Err(Arithmetic(_))));
    }
}

#[derive(Default)]
struct CollisionHasher;
impl Hasher for CollisionHasher {
    fn finish(&self) -> u64 {
        0
    }
    fn write(&mut self, _: &[u8]) {}
}
#[test]
fn one_reservation_unique_inserts_keep_table_capacity_even_under_total_collisions() {
    for count in [3, 14, 15, 29] {
        let mut ordinary = HashMap::<String, String>::new();
        let mut collisions =
            HashMap::<String, String, BuildHasherDefault<CollisionHasher>>::default();
        ordinary.try_reserve(count).unwrap();
        collisions.try_reserve(count).unwrap();
        let capacity = ordinary.capacity();
        let collision_capacity = collisions.capacity();
        let f = fresh_table_layout::<String, String>(count).unwrap();
        assert_eq!(
            capacity,
            if f.buckets < 8 {
                f.buckets - 1
            } else {
                f.buckets / 8 * 7
            }
        );
        assert_eq!(collision_capacity, capacity);
        for at in 0..count {
            let key = format!("same-prefix-雪-{at:03}");
            let value = format!("independent-{at}");
            assert!(ordinary.insert(key.clone(), value.clone()).is_none());
            assert!(collisions.insert(key, value).is_none());
            assert_eq!(ordinary.capacity(), capacity);
            assert_eq!(collisions.capacity(), collision_capacity);
        }
        for at in 0..count {
            let key = format!("same-prefix-雪-{at:03}");
            assert_eq!(ordinary.get(&key), Some(&format!("independent-{at}")));
            assert_eq!(collisions.get(&key), ordinary.get(&key));
        }
        // The adversarial test hasher proves table behavior only; its custom
        // code is deliberately outside the String/RandomState work contract.
        let longest = "same-prefix-雪-000".len();
        let work = fresh_string_table_work_upper_bound(count, count * longest, longest).unwrap();
        assert!(work >= count * (count - 1) / 2 * longest);
    }
}

#[test]
fn string_work_has_independent_collision_oracle_and_deleted_source_invoice_bound() {
    let group = group_layout().unwrap().size();
    let work = string_operations_work_upper_bound(4, 3, 9, 3).unwrap();
    assert_eq!(
        work,
        256 * 3 + 64 * (9 + 3) + 3 * (4 + group) * (32 + 2 * 3)
    );
    assert_eq!(
        fresh_string_table_work_upper_bound(3, 9, 3).unwrap(),
        4 + group + 3 * 48 + work
    );
    assert!(string_operations_work_upper_bound(8, 3, 9, 3).unwrap() > work);
    assert!(string_operations_work_upper_bound(4, 4, 9, 3).unwrap() > work);
    assert!(string_operations_work_upper_bound(4, 3, 10, 3).unwrap() > work);
    assert!(string_operations_work_upper_bound(4, 3, 9, 4).unwrap() > work);
    let mut map = HashMap::<String, String>::new();
    map.try_reserve(4096).unwrap();
    for i in 0..4096 {
        map.insert(format!("key-{i}"), String::new());
    }
    // Locked one-reserve allocation invoice remains retained across deletions.
    let invoice = fresh_table_layout::<String, String>(4096)
        .unwrap()
        .request_bytes_upper_bound;
    let before = source_iterator_work_upper_bound(invoice, 4096).unwrap();
    map.retain(|key, _| key == "never-inserted");
    assert!(map.is_empty());
    let after = source_iterator_work_upper_bound(invoice, 0).unwrap();
    assert_eq!(after, 32 * (invoice + group) + 16);
    assert!(after >= invoice);
    assert!(before > after);
    let mut iter = map.iter();
    assert!(iter.next().is_none());
    // Public capacity is deliberately not an input; neither iterator output
    // length nor an empty table licenses omitting retained bucket scan work.
}
