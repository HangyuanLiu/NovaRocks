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
//! Permanent exact selected Binary overload comparison; malformed values remain in-domain.
use super::*;
use arrow::array::BinaryArray;
use novarocks_types::value::bitmap;
use std::collections::BTreeSet;
fn array(v: &[Option<Vec<u8>>]) -> ArrayRef {
    Arc::new(BinaryArray::from_iter(v.iter().map(|v| v.as_deref())))
}
fn check(a: ArrayRef, nullable: bool) {
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("bitmap_to_string")
            .typed_column(FunctionValueType::new(DataType::Binary, nullable), a)
            .sparse_selections(5, 320451),
    );
}
#[test]
fn pure_differential_bitmap_to_string_full_selected_binary_formats_nullability_slice_empty() {
    let values = BTreeSet::from([0, 1, u32::MAX as u64, u32::MAX as u64 + 1, u64::MAX]);
    let roaring = (0u64..96).collect::<BTreeSet<_>>();
    let roaring64 = (0u64..96)
        .map(|v| (1u64 << 32) + v)
        .collect::<BTreeSet<_>>();
    let mut payloads = vec![
        vec![],
        vec![0],
        bitmap::encode_bitmap_single(u64::MAX),
        bitmap::encode_internal_bitmap(&values).unwrap(),
        bitmap::encode_external_bitmap(&values).unwrap(),
        bitmap::encode_bitmap_aggregate(&values).unwrap(),
        b" 7,1,7,0,18446744073709551615 ".to_vec(),
        bitmap::encode_external_bitmap(&roaring).unwrap(),
        bitmap::encode_external_bitmap(&roaring64).unwrap(),
    ];
    for at in [7, 8] {
        let mut v2 = payloads[at].clone();
        v2[0] = if v2[0] == bitmap::BITMAP_TYPE_BITMAP32 {
            bitmap::BITMAP_TYPE_BITMAP32_SERIV2
        } else {
            bitmap::BITMAP_TYPE_BITMAP64_SERIV2
        };
        payloads.push(v2);
    }
    for nullable in [true, false] {
        let mut values = payloads.iter().cloned().map(Some).collect::<Vec<_>>();
        if nullable {
            values.insert(3, None);
        }
        let a = array(&values);
        for a in [a.clone(), a.slice(1, 7), a.slice(0, 0)] {
            check(a, nullable);
        }
    }
}
#[test]
fn pure_differential_bitmap_to_string_in_domain_malformed_long_errors_and_hidden_null() {
    let long = "雪".repeat(600);
    let a = array(&[
        Some(b"7,1".to_vec()),
        Some(b"bad".to_vec()),
        None,
        Some(vec![0xff]),
        Some(long.into_bytes()),
        Some(b"-1".to_vec()),
        Some(vec![0]),
        Some(b"18446744073709551616".to_vec()),
    ]);
    check(a.clone(), true);
    check(a.slice(1, 6), true);
    let hidden = Arc::new(BinaryArray::new(
        arrow_buffer::OffsetBuffer::new(vec![0i32, 3, 4].into()),
        arrow_buffer::Buffer::from(b"bad\0".as_slice()),
        Some(arrow_buffer::NullBuffer::from(vec![false, true])),
    )) as ArrayRef;
    check(hidden, true);
    // The harness compares error rows and legacy.contains(pure.message).
    // A separate actual owner/host test must prove full >512B diagnostic text.
}
#[test]
fn pure_differential_bitmap_to_string_literal_pool_constants_and_actual_ordinal() {
    for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
        for bytes in [
            None,
            Some(vec![]),
            Some(b"7,1,7".to_vec()),
            Some(b"bad".to_vec()),
        ] {
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("bitmap_to_string")
                    .constant_array(array(&[bytes]))
                    .legacy_constants(form)
                    .constant_rows(5)
                    .sparse_selections(4, 993281),
            );
        }
    }
    let ty = FunctionValueType::new(DataType::Binary, false);
    let backing = array(&[Some(vec![0]), Some(b"9,1,9".to_vec())]);
    let pool = ConstantPool::try_new(
        Arc::new(ty.try_to_field("constant").unwrap()),
        ty,
        backing.to_data(),
        constant_policy(),
        CompilePhase::FunctionSpecialization,
        &HarnessControl,
    )
    .unwrap();
    let constant = pool.value(1).unwrap();
    assert_eq!(constant.ordinal(), 1);
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("bitmap_to_string")
            .constant(constant)
            .legacy_constants(LegacyConstantForm::Pool)
            .constant_rows(5)
            .sparse_selections(4, 993282),
    );
}
