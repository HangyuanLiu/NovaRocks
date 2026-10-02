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
use arrow_array::{BinaryArray, LargeBinaryArray, LargeStringArray, StringArray};
use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use std::sync::Arc;

fn types() -> [DataType; 4] {
    [
        DataType::Utf8,
        DataType::Binary,
        DataType::LargeUtf8,
        DataType::LargeBinary,
    ]
}
fn bytes_array(ty: &DataType, values: &[&[u8]], valid: &[bool]) -> ArrayRef {
    assert_eq!(values.len(), valid.len());
    let mut bytes = vec![];
    let mut offsets = vec![0usize];
    for value in values {
        bytes.extend_from_slice(value);
        offsets.push(bytes.len());
    }
    let nulls = Some(NullBuffer::from(valid.to_vec()));
    let data = Buffer::from(bytes);
    match ty {
        DataType::Utf8 | DataType::Binary => {
            let offsets = OffsetBuffer::new(ScalarBuffer::from(
                offsets
                    .into_iter()
                    .map(|offset| i32::try_from(offset).unwrap())
                    .collect::<Vec<_>>(),
            ));
            if *ty == DataType::Utf8 {
                Arc::new(StringArray::new(offsets, data, nulls))
            } else {
                Arc::new(BinaryArray::new(offsets, data, nulls))
            }
        }
        DataType::LargeUtf8 | DataType::LargeBinary => {
            let offsets = OffsetBuffer::new(ScalarBuffer::from(
                offsets
                    .into_iter()
                    .map(|offset| i64::try_from(offset).unwrap())
                    .collect::<Vec<_>>(),
            ));
            if *ty == DataType::LargeUtf8 {
                Arc::new(LargeStringArray::new(offsets, data, nulls))
            } else {
                Arc::new(LargeBinaryArray::new(offsets, data, nulls))
            }
        }
        _ => panic!("the fixture is an offset byte carrier"),
    }
}
fn value(array: &dyn Array, row: usize) -> &[u8] {
    match array.data_type() {
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(row)
            .as_bytes(),
        DataType::Binary => array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(row),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .value(row)
            .as_bytes(),
        DataType::LargeBinary => array
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap()
            .value(row),
        _ => panic!("the fixture is an offset byte carrier"),
    }
}
fn arrays(ty: &DataType) -> Vec<ArrayRef> {
    vec![
        bytes_array(
            ty,
            &[b"ignore", "α".as_bytes(), b"hidden-null", "尾".as_bytes()],
            &[true, true, false, true],
        )
        .slice(1, 3),
        bytes_array(ty, &[b"unselected", b"", b"\0", b"ascii"], &[true; 4]).slice(1, 3),
        bytes_array(ty, &[b"one", b"two"], &[true; 2]),
    ]
}
fn copy(sources: &[ArrayRef], choices: &[(usize, usize)]) -> ArrayRef {
    let refs: Vec<&dyn Array> = sources.iter().map(|array| array.as_ref()).collect();
    arrow_select::interleave::interleave(&refs, choices).unwrap()
}

#[test]
fn four_byte_carriers_preserve_sparse_sources_slices_and_null_payload_copy() {
    let choices = [(1, 2), (0, 1), (2, 1), (0, 0), (1, 0), (0, 2), (1, 1)];
    let expected: [&[u8]; 7] = [
        b"ascii",
        b"hidden-null",
        b"two",
        "α".as_bytes(),
        b"",
        "尾".as_bytes(),
        b"\0",
    ];
    for ty in types() {
        let sources = arrays(&ty);
        preflight_guarded_interleave(&ty, &sources, &choices, |_| Ok(())).unwrap();
        let actual = copy(&sources, &choices);
        assert_eq!(actual.data_type(), &ty);
        assert_eq!(actual.len(), expected.len());
        for (row, bytes) in expected.iter().enumerate() {
            assert_eq!(actual.is_null(row), row == 1);
            // Arrow copies offset ranges even for NULL rows; validity alone does not reduce extent.
            assert_eq!(value(actual.as_ref(), row), *bytes);
        }
        assert_eq!(actual.to_data().buffers()[1].as_slice(), expected.concat());
        preflight_guarded_interleave(&ty, &sources, &[], |_| Ok(())).unwrap();
        let empty = copy(&sources, &[]);
        assert_eq!(empty.data_type(), &ty);
        assert_eq!(empty.len(), 0);
    }
}

#[test]
fn binary_carriers_keep_non_utf8_bytes_without_copying_unselected_pool_payload() {
    for ty in [DataType::Binary, DataType::LargeBinary] {
        let unselected = vec![0x80; 257 * 1024];
        let sources = vec![bytes_array(
            &ty,
            &[&unselected, &[0xff, 0, 0x80], b""],
            &[true; 3],
        )];
        let choices = [(0, 1), (0, 2), (0, 1)];
        preflight_guarded_interleave(&ty, &sources, &choices, |_| Ok(())).unwrap();
        let actual = copy(&sources, &choices);
        assert_eq!(
            actual.to_data().buffers()[1].as_slice(),
            &[0xff, 0, 0x80, 0xff, 0, 0x80]
        );
        assert_eq!(value(actual.as_ref(), 1), b"");
    }
}

#[test]
fn exact_byte_sources_and_choice_bounds_are_required_even_with_empty_choices() {
    let variants = types();
    for ty in &variants {
        let sources = arrays(ty);
        assert!(matches!(
            preflight_guarded_interleave(ty, &[], &[], |_| Ok(())),
            Err(CopyError::Invalid(_))
        ));
        for other in variants.iter().filter(|other| *other != ty) {
            let mixed = vec![sources[0].clone(), arrays(other)[0].clone()];
            assert!(matches!(
                preflight_guarded_interleave(ty, &mixed, &[], |_| Ok(())),
                Err(CopyError::Invalid(_))
            ));
        }
        for choices in [
            vec![(3, 0)],
            vec![(0, 3)],
            vec![(usize::MAX, usize::MAX)],
            vec![(1, 2), (2, 2)],
        ] {
            assert!(matches!(
                preflight_guarded_interleave(ty, &sources, &choices, |_| Ok(())),
                Err(CopyError::Invalid(_))
            ));
        }
    }
    for ty in [
        DataType::Utf8View,
        DataType::BinaryView,
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        DataType::List(Arc::new(arrow_schema::Field::new(
            "item",
            DataType::Utf8,
            true,
        ))),
    ] {
        assert!(matches!(
            guarded_interleave_extent(&ty, 0),
            Err(CopyError::Unsupported(_))
        ));
    }
}

#[test]
fn byte_offset_and_payload_extents_refuse_overflow_without_large_allocations() {
    for ty in types() {
        let width = if matches!(ty, DataType::LargeUtf8 | DataType::LargeBinary) {
            8
        } else {
            4
        };
        let maximum = isize::MAX as usize;
        assert!(guarded_interleave_extent(&ty, maximum / width - 1).is_ok());
        assert!(matches!(
            guarded_interleave_extent(&ty, maximum / width),
            Err(CopyError::Extent)
        ));
        assert!(matches!(
            guarded_interleave_extent(&ty, usize::MAX),
            Err(CopyError::Extent)
        ));
        for rows in [0, 1, 511, 512, 513] {
            guarded_interleave_extent(&ty, rows).unwrap();
        }
    }
    byte_interleave_payload_extent(i32::MAX as usize, false).unwrap();
    assert!(matches!(
        byte_interleave_payload_extent(i32::MAX as usize + 1, false),
        Err(CopyError::Extent)
    ));
    byte_interleave_payload_extent(i32::MAX as usize + 1, true).unwrap();
    byte_interleave_payload_extent(isize::MAX as usize, true).unwrap();
    assert!(matches!(
        byte_interleave_payload_extent(isize::MAX as usize + 1, true),
        Err(CopyError::Extent)
    ));
}

#[test]
fn byte_preflight_every_callback_preserves_three_primary_causes_without_a_tail_retry() {
    let choices: Vec<_> = (0..320).map(|row| (row % 3, 0)).collect();
    for ty in types() {
        let sources = arrays(&ty);
        let mut trace = vec![];
        preflight_guarded_interleave(&ty, &sources, &choices, |boundary| {
            trace.push(boundary);
            Ok(())
        })
        .unwrap();
        assert_eq!(trace.first(), Some(&true));
        assert_eq!(trace.last(), Some(&true));
        assert!(trace.iter().filter(|boundary| !**boundary).count() >= 320);
        for primary in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
        ] {
            for refused in 0..trace.len() {
                let mut actual = vec![];
                let result = preflight_guarded_interleave(&ty, &sources, &choices, |boundary| {
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
}

#[test]
fn byte_ordinary_failure_observes_completed_tail_but_tail_refusal_remains_primary() {
    for ty in types() {
        let sources = arrays(&ty);
        let choices = [(0, 1), (2, 2)];
        let mut trace = vec![];
        assert!(matches!(
            preflight_guarded_interleave(&ty, &sources, &choices, |boundary| {
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
            let mut actual = vec![];
            assert!(
                matches!(preflight_guarded_interleave(&ty, &sources, &choices, |boundary| {
                actual.push(boundary);
                if actual.len() == trace.len() { Err(primary.clone()) } else { Ok(()) }
            }), Err(CopyError::Control(error)) if error == primary)
            );
            assert_eq!(actual, trace);
        }
    }
}
