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

use novarocks_result_contract::{
    BorrowedScalarRecord, ScalarField, ScalarLeafError, ScalarRecord, ScalarRecordEvent,
    ScalarRecordWriter, ScalarSchema, ScalarValueType,
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! { static ALLOCATIONS: Cell<Option<usize>> = const { Cell::new(None) }; }
struct CountingAllocator;
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = ALLOCATIONS.try_with(|count| {
            if let Some(value) = count.get() {
                count.set(Some(value + 1));
            }
        });
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn null_list(count: usize) -> (ScalarSchema, Vec<u8>) {
    let item = ScalarField {
        nullable: true,
        value_type: ScalarValueType::SignedInteger(64),
    };
    let schema = ScalarSchema::try_new(ScalarField {
        nullable: false,
        value_type: ScalarValueType::List(Box::new(item.clone())),
    })
    .unwrap();
    let mut bytes = Vec::with_capacity(64 * 1024);
    let mut writer = ScalarRecordWriter::new(&schema, &mut bytes).unwrap();
    writer.count(count).unwrap();
    for _ in 0..count {
        writer.presence(&item, true).unwrap();
    }
    writer.finish(&schema).unwrap();
    (schema, bytes)
}

#[test]
fn exact_complete_record_with_many_children_is_valid_without_owned_tree_allocation() {
    let (schema, bytes) = null_list(65_508);
    assert_eq!(bytes.len(), 65_536);
    assert_eq!(
        ScalarRecord::decode_owned(&schema, &bytes),
        Err(ScalarLeafError::ValueLimit)
    );
    ALLOCATIONS.with(|count| count.set(Some(0)));
    let record = BorrowedScalarRecord::try_decode(&schema, &bytes).unwrap();
    let mut leaves = 0;
    record
        .walk(|event| {
            if matches!(event, ScalarRecordEvent::Leaf { .. }) {
                leaves += 1;
            }
            Ok::<(), std::convert::Infallible>(())
        })
        .unwrap();
    let allocations = ALLOCATIONS.with(|count| count.replace(None).unwrap());
    assert_eq!(record.rows(), 1);
    assert_eq!(leaves, 65_508);
    assert_eq!(allocations, 0);
}

#[test]
fn malformed_presence_trailing_bytes_and_oversized_declaration_refuse_without_allocation() {
    let (schema, exact) = null_list(4);
    let mut invalid_presence = exact.clone();
    invalid_presence[28] = 2;
    let mut trailing = exact.clone();
    trailing.push(1);
    let mut oversized = exact.clone();
    oversized[4..8].copy_from_slice(&65_537u32.to_le_bytes());
    oversized[8..12].copy_from_slice(&65_513u32.to_le_bytes());
    for (bytes, expected) in [
        (invalid_presence, ScalarLeafError::MalformedRecord),
        (trailing, ScalarLeafError::MalformedRecord),
        (oversized, ScalarLeafError::ValueLimit),
    ] {
        ALLOCATIONS.with(|count| count.set(Some(0)));
        let error = BorrowedScalarRecord::try_decode(&schema, &bytes)
            .err()
            .unwrap();
        let allocations = ALLOCATIONS.with(|count| count.replace(None).unwrap());
        assert_eq!(error, expected);
        assert_eq!(allocations, 0);
    }
}
