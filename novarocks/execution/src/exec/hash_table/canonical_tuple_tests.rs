// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use crate::runtime::mem_tracker::MemTracker;
use arrow::array::{ArrayRef, Int32Array, StringArray};
use std::{
    hash::{BuildHasherDefault, Hasher},
    sync::Arc,
};
fn key(arrays: &[ArrayRef], row: usize, allocator: &AggregateAllocator) -> AggregateVec<u8> {
    encode_canonical_tuple_row_tracked(arrays.iter().map(Ok), row, allocator.clone()).unwrap()
}
#[test]
fn tuple_components_are_typed_and_length_delimited_not_display_joined() {
    let tracker = MemTracker::new_root("canonical tuple");
    let allocator = AggregateAllocator::new_nominal(tracker.clone());
    let strings = [
        Arc::new(StringArray::from(vec![
            Some("a|b"),
            Some("a"),
            None,
            Some("NULL"),
        ])) as ArrayRef,
        Arc::new(StringArray::from(vec!["c", "b|c", "x", "x"])) as ArrayRef,
    ];
    let first = key(&strings, 0, &allocator);
    let second = key(&strings, 1, &allocator);
    assert_ne!(first, second);
    let null = key(&strings, 2, &allocator);
    let text = key(&strings, 3, &allocator);
    assert_ne!(null, text);
    let int = key(&[Arc::new(Int32Array::from(vec![7]))], 0, &allocator);
    let string = key(&[Arc::new(StringArray::from(vec!["7"]))], 0, &allocator);
    assert_ne!(int, string);
    drop((first, second, null, text, int, string));
    assert_eq!(tracker.current(), 0);
}
#[derive(Default)]
struct Collision;
impl Hasher for Collision {
    fn write(&mut self, _: &[u8]) {}
    fn finish(&self) -> u64 {
        0
    }
}
#[test]
fn a_hash_collision_still_compares_the_complete_canonical_key() {
    let tracker = MemTracker::new_root("canonical collision");
    let allocator = AggregateAllocator::new_nominal(tracker.clone());
    let arrays = [Arc::new(Int32Array::from(vec![7, 8, 7])) as ArrayRef];
    let mut set = hashbrown::HashSet::with_hasher_in(
        BuildHasherDefault::<Collision>::default(),
        allocator.clone(),
    );
    set.try_reserve(3).unwrap();
    assert!(set.insert(key(&arrays, 0, &allocator)));
    assert!(set.insert(key(&arrays, 1, &allocator)));
    assert!(!set.insert(key(&arrays, 2, &allocator)));
    assert_eq!(set.len(), 2);
    drop(set);
    assert_eq!(tracker.current(), 0);
}
