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
use crate::build::ValueBuilder;
use arrow_buffer::NullBuffer;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

fn view(
    rows: &[(usize, usize, usize)],
    buffers: Vec<Buffer>,
    nulls: Option<NullBuffer>,
) -> ArrayRef {
    let views = rows
        .iter()
        .map(|&(index, start, len)| {
            arrow_raw::external_view(
                ByteLayout::Utf8View,
                &buffers[index].as_slice()[start..start + len],
                index,
                start,
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    // SAFETY: test buffers contain ASCII, all ranges are in-bounds and the
    // helper checked every descriptor. The supplied bitmap has one bit/row.
    unsafe { arrow_raw::make_view_unchecked(ByteLayout::Utf8View, views.into(), buffers, nulls) }
        .unwrap()
}
fn values(array: &ArrayRef) -> Vec<Option<Vec<u8>>> {
    (0..array.len())
        .map(|row| {
            arrow_raw::byte_span(array.as_ref(), row)
                .unwrap()
                .map(|v| v.bytes().to_vec())
        })
        .collect()
}
#[test]
fn ten_thousand_references_copy_the_shared_hundred_bytes_once() {
    let buffer = Buffer::from_vec(vec![b'x'; 100]);
    let input = view(&vec![(0, 0, 100); 10000], vec![buffer.clone()], None);
    let plan = CompactPlan::plan(&input, None).unwrap();
    assert_eq!(plan.facts().copied_bytes, 100);
    assert_eq!(plan.facts().copied_ranges, 1);
    let out = plan.execute().unwrap();
    assert_eq!(out.facts.output_regions, 1);
    for row in [0, 5000, 9999] {
        assert_eq!(
            arrow_raw::byte_span(out.array.as_ref(), row)
                .unwrap()
                .unwrap()
                .bytes(),
            vec![b'x'; 100]
        );
    }
    assert_ne!(
        arrow_raw::view_parts(out.array.as_ref()).unwrap().1[0].data_ptr(),
        buffer.data_ptr()
    );
    out.array.to_data().validate_full().unwrap();
}
#[test]
fn overlapping_slices_merge_only_accessible_same_allocation_ranges() {
    let buffer = Buffer::from_vec((0..100).map(|i| b'a' + (i % 26)).collect::<Vec<_>>());
    let input = view(
        &[(0, 10, 30), (1, 10, 30)],
        vec![
            buffer.slice_with_length(0, 80),
            buffer.slice_with_length(20, 80),
        ],
        None,
    );
    let plan = CompactPlan::plan(&input, None).unwrap();
    assert_eq!(plan.facts().copied_bytes, 50);
    let out = plan.execute().unwrap();
    assert_eq!(values(&out.array), values(&input));
    let left = arrow_raw::byte_span(out.array.as_ref(), 0)
        .unwrap()
        .unwrap();
    let right = arrow_raw::byte_span(out.array.as_ref(), 1)
        .unwrap()
        .unwrap();
    let (
        arrow_raw::ByteSpan::Region {
            buffer: a,
            offset: x,
            ..
        },
        arrow_raw::ByteSpan::Region {
            buffer: b,
            offset: y,
            ..
        },
    ) = (left, right)
    else {
        panic!("external regions")
    };
    assert_eq!(a.data_ptr(), b.data_ptr());
    assert_eq!((b.ptr_offset() + y) - (a.ptr_offset() + x), 20);
}
#[test]
fn equal_bytes_from_distinct_allocations_are_independent_and_unselected_regions_retain_source() {
    let a = Buffer::from_vec(vec![b'a'; 40]);
    let b = Buffer::from_vec(vec![b'a'; 40]);
    let input = view(&[(0, 0, 30), (1, 0, 30)], vec![a.clone(), b.clone()], None);
    let all = CompactPlan::plan(&input, None).unwrap();
    assert_eq!(all.facts().copied_ranges, 2);
    assert_eq!(all.facts().copied_bytes, 60);
    let one = CompactPlan::plan(&input, Some(&[0]))
        .unwrap()
        .execute()
        .unwrap();
    assert_eq!(one.facts.retained_regions, 1);
    assert_eq!(one.facts.copied_bytes, 30);
    assert!(
        arrow_raw::view_parts(one.array.as_ref())
            .unwrap()
            .1
            .iter()
            .any(|v| v.data_ptr() == b.data_ptr())
    );
    assert_eq!(values(&one.array), values(&input));
}
#[test]
fn null_residual_views_are_legal_empty_and_inline_values_remain_inline() {
    let buffer = Buffer::from_vec(vec![b'a'; 80]);
    let input = view(
        &[(0, 0, 30), (0, 35, 30), (0, 0, 3)],
        vec![buffer],
        Some(NullBuffer::from(vec![true, false, true])),
    );
    let plan = CompactPlan::plan(&input, None).unwrap();
    assert_eq!(plan.facts().copied_bytes, 30);
    let out = plan.execute().unwrap();
    assert_eq!(values(&out.array), values(&input));
    let (views, _) = arrow_raw::view_parts(out.array.as_ref()).unwrap();
    assert_eq!(views[1], 0);
    out.array.to_data().validate_full().unwrap();
}
#[test]
fn rejected_output_and_cancelled_sort_copy_preserve_input_and_can_be_retried() {
    let buffer = Buffer::from_vec(vec![b'a'; 200000]);
    let input = view(&[(0, 0, 150000)], vec![buffer], None);
    let before = values(&input);
    let plan = CompactPlan::plan(&input, None).unwrap();
    assert!(matches!(
        plan.execute_observed(&mut |_| Err(17), &mut |_| Ok(())),
        Err(ValueFailure::Caller(17))
    ));
    let mut turns = 0;
    assert!(matches!(
        plan.execute_observed(&mut |_| Ok(()), &mut |units| {
            if units > 1 {
                turns += 1;
            }
            if turns == 2 { Err(18) } else { Ok(()) }
        }),
        Err(ValueFailure::Caller(18))
    ));
    assert_eq!(values(&input), before);
    assert_eq!(values(&plan.execute().unwrap().array), before);
    let many = view(
        &(0..4096).rev().map(|i| (0, i * 20, 20)).collect::<Vec<_>>(),
        vec![Buffer::from_vec(vec![b'b'; 4096 * 20])],
        None,
    );
    let mut sort_turn = false;
    assert!(matches!(
        CompactPlan::plan_observed(&many, None, &mut |_| Ok(()), &mut |units| {
            if units >= SORT_RUN {
                sort_turn = true;
                Err(19)
            } else {
                Ok(())
            }
        }),
        Err(ValueFailure::Caller(19))
    ));
    assert!(sort_turn);
    assert_eq!(values(&many).len(), 4096);
}
#[test]
fn copied_output_does_not_claim_source_released_before_last_external_alias() {
    let drops = Arc::new(AtomicUsize::new(0));
    let source = arrow_raw::tracked_string_source(&["one longer source string"], drops.clone());
    let mut builder = ValueBuilder::new(ByteLayout::Utf8View);
    builder
        .append_array_value(
            source.as_ref(),
            0,
            &mut |_| Ok::<_, Infallible>(()),
            &mut |_| Ok::<_, Infallible>(()),
        )
        .unwrap();
    let input = builder.finish().unwrap().array;
    drop(source);
    let alias = input.clone();
    let plan = CompactPlan::plan(&input, None).unwrap();
    let output = plan.execute().unwrap();
    drop(plan);
    drop(input);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(alias);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(
        values(&output.array),
        vec![Some(b"one longer source string".to_vec())]
    );
}
#[test]
fn cancelling_planning_scratch_has_no_output_and_the_same_source_remains_usable() {
    let source = view(&[(0, 0, 30)], vec![Buffer::from_vec(vec![b'a'; 30])], None);
    assert!(matches!(
        CompactPlan::plan_observed(&source, None, &mut |_| Err(41), &mut |_| Ok(())),
        Err(ValueFailure::Caller(41))
    ));
    assert_eq!(
        values(
            &CompactPlan::plan(&source, None)
                .unwrap()
                .execute()
                .unwrap()
                .array
        ),
        values(&source)
    );
}

#[test]
fn planned_address_limits_distinguish_single_output_from_minimum_value() {
    let limit = i32::MAX as usize;
    validate_region_count(limit + 1).unwrap();
    validate_address(ByteLayout::Utf8View, limit, limit).unwrap();
    for (error, boundary, code) in [
        (
            validate_region_count(limit + 2).unwrap_err(),
            RepresentationBoundary::SingleOutput,
            7,
        ),
        (
            validate_address(ByteLayout::Utf8View, 13, limit + 1).unwrap_err(),
            RepresentationBoundary::SingleOutput,
            8,
        ),
        (
            validate_address(ByteLayout::Utf8View, limit + 1, 0).unwrap_err(),
            RepresentationBoundary::MinimumUnit,
            3,
        ),
    ] {
        let ValueError::Representation(facts) = error else {
            panic!("representation fact")
        };
        assert_eq!(facts.boundary, boundary);
        assert_eq!(facts.layout.code.get(), code);
    }
}
