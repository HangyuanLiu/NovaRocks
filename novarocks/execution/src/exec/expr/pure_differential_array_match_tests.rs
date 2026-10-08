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
//! Each actual generic declaration is tested across the original full castable shape domain.
use super::*;
use arrow::array::*;
use arrow::datatypes::*;
use arrow_buffer::{NullBuffer, OffsetBuffer};

// The original fixed List<T> binder authors this canonical field. Raw
// metadata witnesses stay in the independent original-v1 fixture module.
fn list(values: ArrayRef, offsets: Vec<i32>, valid: Option<Vec<bool>>) -> ArrayRef {
    Arc::new(ListArray::new(
        Arc::new(Field::new("item", values.data_type().clone(), true)),
        OffsetBuffer::new(offsets.into()),
        values,
        valid.map(NullBuffer::from),
    ))
}
#[test]
fn pure_differential_array_match_both_real_overloads_all_primitive_castable_shapes() {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(BooleanArray::from(vec![
            Some(true),
            None,
            Some(false),
            Some(true),
        ])),
        Arc::new(Int8Array::from(vec![
            Some(i8::MIN),
            None,
            Some(0),
            Some(i8::MAX),
        ])),
        Arc::new(Int16Array::from(vec![
            Some(i16::MIN),
            None,
            Some(0),
            Some(i16::MAX),
        ])),
        Arc::new(Int32Array::from(vec![
            Some(i32::MIN),
            None,
            Some(0),
            Some(i32::MAX),
        ])),
        Arc::new(Int64Array::from(vec![
            Some(i64::MIN),
            None,
            Some(0),
            Some(i64::MAX),
        ])),
        Arc::new(UInt8Array::from(vec![
            Some(0),
            None,
            Some(1),
            Some(u8::MAX),
        ])),
        Arc::new(UInt16Array::from(vec![
            Some(0),
            None,
            Some(1),
            Some(u16::MAX),
        ])),
        Arc::new(UInt32Array::from(vec![
            Some(0),
            None,
            Some(1),
            Some(u32::MAX),
        ])),
        Arc::new(UInt64Array::from(vec![
            Some(0),
            None,
            Some(1),
            Some(u64::MAX),
        ])),
        Arc::new(Float16Array::new(
            arrow::buffer::ScalarBuffer::new(
                arrow::buffer::Buffer::from_slice_ref(&[0u16, 0x8000, 0x7c00, 0x7e00]),
                0,
                4,
            ),
            None,
        )),
        Arc::new(Float32Array::from(vec![
            Some(f32::NAN),
            None,
            Some(-0.0),
            Some(f32::INFINITY),
        ])),
        Arc::new(Float64Array::from(vec![
            Some(f64::NEG_INFINITY),
            None,
            Some(-0.0),
            Some(f64::NAN),
        ])),
        Arc::new(StringArray::from(vec![
            Some("true"),
            None,
            Some("invalid"),
            Some("false"),
        ])),
        Arc::new(LargeStringArray::from(vec![
            Some(" Y "),
            None,
            Some("invalid"),
            Some(""),
        ])),
        Arc::new(StringViewArray::from(vec![
            Some("true"),
            None,
            Some("invalid"),
            Some("false"),
        ])),
        Arc::new(NullArray::new(4)),
        novarocks_types::largeint::array_from_i128(&[
            Some(i128::MIN),
            None,
            Some(0),
            Some(i128::MAX),
        ])
        .unwrap(),
    ];
    for name in ["all_match", "any_match"] {
        for values in &arrays {
            let a = list(
                values.clone(),
                vec![0, 2, 2, 3, 4],
                Some(vec![true, true, false, true]),
            );
            for a in [a.clone(), a.slice(1, 3), a.slice(0, 0)] {
                assert_scalar_matches_v1(ScalarDiffSpec::new(name).column(a));
            }
        }
    }
}
#[test]
fn pure_differential_array_match_both_real_overloads_encoded_and_fixed_singleton_items() {
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(1), None, Some(0), Some(1)]),
            Arc::new(StringArray::from(vec!["false", "true"])),
        )
        .unwrap(),
    );
    let fixed: ArrayRef = Arc::new(
        FixedSizeListArray::try_new(
            Arc::new(Field::new("inner", DataType::Int64, true)),
            1,
            Arc::new(Int64Array::from(vec![Some(1), None, Some(0), Some(1)])),
            None,
        )
        .unwrap(),
    );
    for name in ["all_match", "any_match"] {
        for values in [dictionary.clone(), fixed.clone()] {
            assert_scalar_matches_v1(ScalarDiffSpec::new(name).column(list(
                values,
                vec![0, 2, 4],
                None,
            )));
        }
    }
}

#[test]
fn pure_differential_array_match_both_real_overloads_run_and_union_projection() {
    let run_values = Int64Array::from(vec![Some(1), None, Some(0)]);
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(
            RunArray::<Int16Type>::try_new(&Int16Array::from(vec![1, 2, 4]), &run_values).unwrap(),
        ),
        Arc::new(
            RunArray::<Int32Type>::try_new(&Int32Array::from(vec![1, 2, 4]), &run_values).unwrap(),
        ),
        Arc::new(
            RunArray::<Int64Type>::try_new(&Int64Array::from(vec![1, 2, 4]), &run_values).unwrap(),
        ),
        // Arrow's original union cast resolves the exact BOOLEAN child and projects NULL for other ids.
        Arc::new(
            UnionArray::try_new(
                UnionFields::try_new(
                    [7, 3],
                    [
                        Arc::new(Field::new("boolean", DataType::Boolean, true)),
                        Arc::new(Field::new("text", DataType::Utf8, true)),
                    ],
                )
                .unwrap(),
                vec![7i8, 3, 7, 7].into(),
                Some(vec![0i32, 0, 1, 2].into()),
                vec![
                    Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
                    Arc::new(StringArray::from(vec!["true"])),
                ],
            )
            .unwrap(),
        ),
        Arc::new(
            UnionArray::try_new(
                UnionFields::try_new(
                    [7, 3],
                    [
                        Arc::new(Field::new("boolean", DataType::Boolean, true)),
                        Arc::new(Field::new("text", DataType::Utf8, true)),
                    ],
                )
                .unwrap(),
                vec![7i8, 3, 7, 7].into(),
                None,
                vec![
                    Arc::new(BooleanArray::from(vec![
                        Some(true),
                        None,
                        None,
                        Some(false),
                    ])),
                    Arc::new(StringArray::from(vec!["false", "true", "false", "false"])),
                ],
            )
            .unwrap(),
        ),
    ];
    for name in ["all_match", "any_match"] {
        for values in &arrays {
            let a = list(values.clone(), vec![0, 2, 4], None);
            for a in [a.clone(), a.slice(1, 1), a.slice(0, 0)] {
                assert_scalar_matches_v1(ScalarDiffSpec::new(name).column(a));
            }
        }
    }
}
