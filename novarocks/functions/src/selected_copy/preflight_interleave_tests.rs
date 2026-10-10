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
use arrow_array::{BooleanArray, Decimal128Array, Int64Array, TimestampNanosecondArray};
use std::sync::Arc;

fn sources() -> Vec<ArrayRef> {
    vec![
        Arc::new(Int64Array::from(vec![Some(4), None, Some(9)])),
        Arc::new(Int64Array::from(vec![Some(-7), Some(33)])),
    ]
}

#[test]
fn fixed_interleave_preflight_preserves_actual_compact_source_order_nulls_and_slices() {
    let mut arrays = sources();
    arrays[0] = arrays[0].slice(1, 2);
    let choices = [(1, 1), (0, 0), (1, 0), (0, 1)];
    preflight_fixed_interleave(&DataType::Int64, &arrays, &choices, |_| Ok(())).unwrap();
    let refs: Vec<&dyn Array> = arrays.iter().map(|array| array.as_ref()).collect();
    let output = arrow_select::interleave::interleave(&refs, &choices).unwrap();
    let actual = output.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(
        actual.iter().collect::<Vec<_>>(),
        vec![Some(33), None, Some(-7), Some(9)]
    );
}

#[test]
fn boolean_fallback_and_decimal_primitive_keep_actual_output_type_and_nulls() {
    let bools: Vec<ArrayRef> = vec![
        Arc::new(BooleanArray::from(vec![Some(true), None])),
        Arc::new(BooleanArray::from(vec![Some(false)])),
    ];
    let choices = [(0, 1), (1, 0), (0, 0)];
    preflight_fixed_interleave(&DataType::Boolean, &bools, &choices, |_| Ok(())).unwrap();
    let refs: Vec<&dyn Array> = bools.iter().map(|array| array.as_ref()).collect();
    let output = arrow_select::interleave::interleave(&refs, &choices).unwrap();
    assert_eq!(
        output
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![None, Some(false), Some(true)]
    );
    let decimals: Vec<ArrayRef> = vec![
        Arc::new(
            Decimal128Array::from(vec![Some(155), None])
                .with_precision_and_scale(3, 2)
                .unwrap(),
        ),
        Arc::new(
            Decimal128Array::from(vec![Some(-12)])
                .with_precision_and_scale(3, 2)
                .unwrap(),
        ),
    ];
    let ty = DataType::Decimal128(3, 2);
    preflight_fixed_interleave(&ty, &decimals, &choices, |_| Ok(())).unwrap();
    let refs: Vec<&dyn Array> = decimals.iter().map(|array| array.as_ref()).collect();
    let output = arrow_select::interleave::interleave(&refs, &choices).unwrap();
    assert_eq!(output.data_type(), &ty);
    assert_eq!(
        output
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![None, Some(-12), Some(155)]
    );
}

#[test]
fn exact_fixed_type_and_all_source_bounds_are_checked_even_for_empty_choices() {
    let arrays = sources();
    preflight_fixed_interleave(&DataType::Int64, &arrays, &[], |_| Ok(())).unwrap();
    assert!(matches!(
        preflight_fixed_interleave(&DataType::Int64, &[], &[], |_| Ok(())),
        Err(CopyError::Invalid(_))
    ));
    for choices in [
        vec![(2, 0)],
        vec![(0, 3)],
        vec![(usize::MAX, usize::MAX)],
        vec![(0, 0), (1, 2)],
    ] {
        assert!(matches!(
            preflight_fixed_interleave(&DataType::Int64, &arrays, &choices, |_| Ok(())),
            Err(CopyError::Invalid(_))
        ));
    }
    let mixed: Vec<ArrayRef> = vec![arrays[0].clone(), Arc::new(BooleanArray::from(vec![true]))];
    assert!(matches!(
        preflight_fixed_interleave(&DataType::Int64, &mixed, &[], |_| Ok(())),
        Err(CopyError::Invalid(_))
    ));
    let timestamp: Vec<ArrayRef> = vec![Arc::new(
        TimestampNanosecondArray::from(vec![0]).with_timezone("UTC"),
    )];
    let wrong_zone = DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some("+00:00".into()));
    assert!(matches!(
        preflight_fixed_interleave(&wrong_zone, &timestamp, &[], |_| Ok(())),
        Err(CopyError::Invalid(_))
    ));
}

#[test]
fn unsupported_variable_and_encoded_carriers_are_not_silently_copied_or_retagged() {
    for ty in [
        DataType::Null,
        DataType::Utf8,
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        DataType::List(Arc::new(arrow_schema::Field::new(
            "item",
            DataType::Int64,
            true,
        ))),
    ] {
        let arrays = vec![arrow_array::new_empty_array(&ty)];
        assert!(
            matches!(preflight_fixed_interleave(&ty, &arrays, &[], |_| Ok(())), Err(CopyError::Unsupported(actual)) if actual == ty)
        );
    }
}

#[test]
fn fixed_output_extents_include_rounded_validity_and_check_width_before_allocation() {
    let maximum = isize::MAX as usize;
    assert!(fixed_interleave_extent(&DataType::Int64, maximum / 8).is_ok());
    assert!(matches!(
        fixed_interleave_extent(&DataType::Int64, maximum / 8 + 1),
        Err(CopyError::Extent)
    ));
    assert!(fixed_interleave_extent(&DataType::Decimal256(76, 0), maximum / 32).is_ok());
    assert!(matches!(
        fixed_interleave_extent(&DataType::Decimal256(76, 0), maximum / 32 + 1),
        Err(CopyError::Extent)
    ));
    for rows in [0, 1, 7, 8, 511, 512, 513] {
        fixed_interleave_extent(&DataType::Boolean, rows).unwrap();
    }
}

#[test]
fn every_actual_preflight_callback_preserves_primary_categories_without_retry() {
    let arrays = sources();
    let choices: Vec<_> = (0..320).map(|ordinal| (ordinal % 2, 0)).collect();
    let mut trace = Vec::new();
    preflight_fixed_interleave(&DataType::Int64, &arrays, &choices, |boundary| {
        trace.push(boundary);
        Ok(())
    })
    .unwrap();
    assert_eq!(trace.first(), Some(&true));
    assert_eq!(trace.last(), Some(&true));
    assert!(trace.iter().filter(|boundary| !**boundary).count() > 256);
    for primary in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
    ] {
        for refused in 0..trace.len() {
            let mut actual = Vec::new();
            let result =
                preflight_fixed_interleave(&DataType::Int64, &arrays, &choices, |boundary| {
                    actual.push(boundary);
                    if actual.len() == refused + 1 {
                        Err(primary.clone())
                    } else {
                        Ok(())
                    }
                });
            assert!(matches!(result, Err(CopyError::Control(error)) if error == primary));
            assert_eq!(actual, trace[..=refused]);
        }
    }
}

#[test]
fn ordinary_choice_failure_still_observes_tail_and_tail_refusal_is_primary() {
    let arrays = sources();
    let mut trace = Vec::new();
    let choices = [(0, 0), (1, 2)];
    assert!(matches!(
        preflight_fixed_interleave(&DataType::Int64, &arrays, &choices, |boundary| {
            trace.push(boundary);
            Ok(())
        }),
        Err(CopyError::Invalid(_))
    ));
    assert_eq!(trace.last(), Some(&true));
    for primary in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
    ] {
        let mut callbacks = 0;
        assert!(
            matches!(preflight_fixed_interleave(&DataType::Int64, &arrays, &choices, |_| {
            callbacks += 1;
            if callbacks == trace.len() { Err(primary.clone()) } else { Ok(()) }
        }), Err(CopyError::Control(error)) if error == primary)
        );
        assert_eq!(callbacks, trace.len());
    }
}

#[test]
fn fixed_binary_interleave_preserves_selected_bytes_nulls_and_exact_width() {
    use arrow_array::FixedSizeBinaryArray;
    let first = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
        [Some([1u8; 16]), None, Some([3u8; 16])].into_iter(),
        16,
    )
    .unwrap();
    let second = FixedSizeBinaryArray::try_from_iter([[9u8; 16]].into_iter()).unwrap();
    let arrays: Vec<ArrayRef> = vec![Arc::new(first.slice(1, 2)), Arc::new(second)];
    let choices = [(1, 0), (0, 0), (0, 1), (1, 0)];
    preflight_guarded_interleave(
        &DataType::FixedSizeBinary(16),
        &arrays,
        &choices,
        |_| Ok(()),
    )
    .unwrap();
    let refs: Vec<&dyn Array> = arrays.iter().map(AsRef::as_ref).collect();
    let result = arrow_select::interleave::interleave(&refs, &choices).unwrap();
    let result = result
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(result.value(0), &[9u8; 16]);
    assert!(result.is_null(1));
    assert_eq!(result.value(2), &[3u8; 16]);
    assert_eq!(result.value(3), &[9u8; 16]);
    assert!(matches!(
        preflight_guarded_interleave(&DataType::FixedSizeBinary(8), &arrays, &choices, |_| Ok(())),
        Err(CopyError::Invalid(_))
    ));
    assert!(
        fixed_interleave_extent(&DataType::FixedSizeBinary(16), isize::MAX as usize / 16).is_ok()
    );
    assert!(matches!(
        fixed_interleave_extent(&DataType::FixedSizeBinary(16), isize::MAX as usize / 16 + 1),
        Err(CopyError::Extent)
    ));
}
