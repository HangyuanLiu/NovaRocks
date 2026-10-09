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
//! Original aggregate methods and state. No owner/state/decoder replacement.
use super::*;
use arrow::array::new_null_array;
use arrow::datatypes::Field;
use std::mem::MaybeUninit;
struct Original {
    raw: MaybeUninit<BitmapState>,
    spec: AggSpec,
}
impl Original {
    fn new(name: &str, input: &DataType) -> Result<Self, String> {
        let f = AggFunction {
            name: name.into(),
            ..Default::default()
        };
        let spec = BitmapUnionIntAgg.build_spec_from_type(&f, Some(input), false)?;
        let mut this = Self {
            raw: MaybeUninit::uninit(),
            spec,
        };
        BitmapUnionIntAgg.init_state_with_tracker(
            &this.spec,
            this.raw.as_mut_ptr().cast(),
            Some(MemTracker::new_root("original-bitmap-union-int")),
        )?;
        Ok(this)
    }
    fn pointer(&mut self) -> AggStatePtr {
        self.raw.as_mut_ptr() as AggStatePtr
    }
    fn update(&mut self, a: ArrayRef) -> Result<(), String> {
        let pointers = vec![self.pointer(); a.len()];
        BitmapUnionIntAgg.update_batch(&self.spec, 0, &pointers, &AggInputView::Any(&a))
    }
    fn merge(&mut self, a: ArrayRef) -> Result<(), String> {
        let pointers = vec![self.pointer(); a.len()];
        let wrapped = Some(a);
        let input = BitmapUnionIntAgg.build_merge_view(&self.spec, &wrapped)?;
        BitmapUnionIntAgg.merge_batch(&self.spec, 0, &pointers, &input)
    }
    fn output(&mut self, partial: bool) -> ArrayRef {
        let p = self.pointer();
        BitmapUnionIntAgg
            .build_array(&self.spec, 0, &[p], partial)
            .unwrap()
    }
    fn count(&mut self) -> Option<i64> {
        let a = self.output(false);
        let a = a.as_any().downcast_ref::<Int64Array>().unwrap();
        (!a.is_null(0)).then(|| a.value(0))
    }
    fn bytes(&mut self) -> Option<Vec<u8>> {
        let a = self.output(true);
        let a = a.as_any().downcast_ref::<BinaryArray>().unwrap();
        (!a.is_null(0)).then(|| a.value(0).to_vec())
    }
}
impl Drop for Original {
    fn drop(&mut self) {
        BitmapUnionIntAgg.drop_state(&self.spec, self.raw.as_mut_ptr().cast());
    }
}
fn state(a: ArrayRef) -> Original {
    Original::new("bitmap_union_int", a.data_type()).unwrap()
}
fn count(a: ArrayRef) -> Option<i64> {
    let mut s = state(a.clone());
    s.update(a).unwrap();
    s.count()
}
fn binary(v: &[Option<Vec<u8>>]) -> ArrayRef {
    Arc::new(BinaryArray::from_iter(v.iter().map(|v| v.as_deref())))
}
#[test]
fn original_bitmap_union_int_all_integer_widths_boolean_negative_and_unsigned_domain() {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int8Array::from(vec![
            Some(i8::MIN),
            None,
            Some(0),
            Some(i8::MAX),
            Some(i8::MIN),
        ])),
        Arc::new(Int16Array::from(vec![
            Some(i16::MIN),
            None,
            Some(0),
            Some(i16::MAX),
            Some(i16::MIN),
        ])),
        Arc::new(Int32Array::from(vec![
            Some(i32::MIN),
            None,
            Some(0),
            Some(i32::MAX),
            Some(i32::MIN),
        ])),
        Arc::new(Int64Array::from(vec![
            Some(i64::MIN),
            None,
            Some(0),
            Some(i64::MAX),
            Some(i64::MIN),
        ])),
        Arc::new(UInt8Array::from(vec![
            Some(u8::MAX),
            None,
            Some(0),
            Some(1),
            Some(u8::MAX),
        ])),
        Arc::new(UInt16Array::from(vec![
            Some(u16::MAX),
            None,
            Some(0),
            Some(1),
            Some(u16::MAX),
        ])),
        Arc::new(UInt32Array::from(vec![
            Some(u32::MAX),
            None,
            Some(0),
            Some(1),
            Some(u32::MAX),
        ])),
        Arc::new(UInt64Array::from(vec![
            Some(u64::MAX),
            None,
            Some(0),
            Some(1),
            Some(u64::MAX),
        ])),
    ];
    for a in arrays {
        assert_eq!(count(a.clone()), Some(3));
        assert_eq!(count(a.slice(1, 3)), Some(2));
    }
    assert_eq!(
        count(Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            Some(true)
        ]))),
        Some(2)
    );
    let a = Arc::new(Int64Array::from(vec![Some(-1), Some(i64::MIN)])) as ArrayRef;
    let mut s = state(a.clone());
    s.update(a).unwrap();
    assert_eq!(
        decode_bitmap(&s.bytes().unwrap()).unwrap(),
        BTreeSet::from([u64::MAX, i64::MIN as u64])
    );
}
#[test]
fn original_bitmap_union_int_text_skips_invalid_and_outside_exact_i128_window() {
    let v = vec![
        Some(" -1 "),
        Some("18446744073709551615"),
        Some("7"),
        Some("bad"),
        Some("é雪"),
        Some("-9223372036854775809"),
        Some("18446744073709551616"),
        None,
    ];
    for a in [
        Arc::new(StringArray::from(v.clone())) as ArrayRef,
        Arc::new(LargeStringArray::from(v)),
    ] {
        let mut s = state(a.clone());
        s.update(a).unwrap();
        assert_eq!(s.count(), Some(2));
        assert_eq!(
            decode_bitmap(&s.bytes().unwrap()).unwrap(),
            BTreeSet::from([7, u64::MAX])
        );
    }
    for a in [
        Arc::new(StringArray::from(vec![Some("bad"), None])) as ArrayRef,
        Arc::new(LargeStringArray::from(vec![Some("bad"), None])),
    ] {
        assert_eq!(count(a), None);
    }
}
#[test]
fn original_bitmap_union_int_binary_decode_before_single_numeric_text_and_has_value() {
    let v = vec![
        Some(encode_bitmap(&BTreeSet::from([1, 7, u64::MAX])).unwrap()),
        Some(b"-1".to_vec()),
        Some(b"bad".to_vec()),
        Some(vec![0xff]),
        None,
        Some(vec![0]),
    ];
    for a in [
        binary(&v),
        Arc::new(LargeBinaryArray::from_iter(v.iter().map(|v| v.as_deref()))) as ArrayRef,
    ] {
        let mut s = state(a.clone());
        s.update(a).unwrap();
        assert_eq!(s.count(), Some(3));
        assert_eq!(
            decode_bitmap(&s.bytes().unwrap()).unwrap(),
            BTreeSet::from([1, 7, u64::MAX])
        );
    }
    assert_eq!(count(binary(&[Some(vec![0])])), Some(0));
    assert_eq!(count(binary(&[Some(vec![0xff]), None])), None);
}
#[test]
fn original_bitmap_union_int_null_empty_partial_bytes_and_merge_null_vs_empty() {
    let a = Arc::new(Int32Array::from(vec![None, None])) as ArrayRef;
    for a in [a.clone(), a.slice(0, 0)] {
        let mut s = state(a.clone());
        assert_eq!(s.count(), None);
        assert_eq!(s.bytes(), None);
        s.update(a).unwrap();
        assert_eq!(s.count(), None);
        assert_eq!(s.bytes(), None);
    }
    let mut s = Original::new("bitmap_union_int", &DataType::Int32).unwrap();
    s.update(Arc::new(Int32Array::from(vec![Some(7), Some(1), Some(7)])))
        .unwrap();
    let encoded = s.bytes().unwrap();
    assert_eq!(encoded, encode_bitmap(&BTreeSet::from([1, 7])).unwrap());
    let mut final_state = Original::new("bitmap_union_int", &DataType::Int32).unwrap();
    final_state.merge(binary(&[None])).unwrap();
    assert_eq!(final_state.count(), None);
    final_state.merge(binary(&[Some(vec![0])])).unwrap();
    assert_eq!(final_state.count(), Some(0));
    final_state.merge(binary(&[Some(encoded)])).unwrap();
    assert_eq!(final_state.count(), Some(2));
}
#[test]
fn original_bitmap_union_int_merge_first_full_error_and_prior_success_are_preserved() {
    let long = "雪".repeat(600);
    for bad in [b"bad".to_vec(), vec![0xff], long.into_bytes()] {
        let expected = decode_bitmap(&bad).unwrap_err();
        let mut s = Original::new("bitmap_union_int", &DataType::Int32).unwrap();
        let error = s
            .merge(binary(&[
                Some(encode_bitmap(&BTreeSet::from([7])).unwrap()),
                Some(bad.clone()),
                Some(encode_bitmap(&BTreeSet::from([9])).unwrap()),
            ]))
            .unwrap_err();
        assert_eq!(error, expected);
        assert_eq!(s.count(), Some(1));
        assert_eq!(
            decode_bitmap(&s.bytes().unwrap()).unwrap(),
            BTreeSet::from([7])
        );
        if bad.len() > 512 {
            assert!(error.len() > 512);
        }
    }
}
#[test]
fn original_bitmap_union_int_type_admission_full_error_and_view_order() {
    let long = DataType::Struct(
        (0..80)
            .map(|i| Field::new(format!("original_full_field_{i}"), DataType::Utf8, true))
            .collect(),
    );
    for ty in [
        DataType::Null,
        DataType::Float64,
        DataType::Decimal128(38, 2),
        DataType::FixedSizeBinary(16),
        long,
    ] {
        let f = AggFunction {
            name: "bitmap_union_int".into(),
            ..Default::default()
        };
        let expected =
            format!("bitmap aggregate expects BOOLEAN/INTEGER/VARCHAR/BINARY input, got {ty:?}");
        assert_eq!(
            BitmapUnionIntAgg
                .build_spec_from_type(&f, Some(&ty), false)
                .unwrap_err(),
            expected
        );
        if matches!(ty, DataType::Struct(_)) {
            assert!(expected.len() > 512);
        }
        // The original intermediate spec bypasses type admission, then the
        // actual merge view owns its own exact BinaryArray downcast error.
        let spec = BitmapUnionIntAgg
            .build_spec_from_type(&f, Some(&ty), true)
            .unwrap();
        let a = Some(new_null_array(&ty, 1));
        assert!(matches!(
            BitmapUnionIntAgg.build_input_view(&spec, &a).unwrap(),
            AggInputView::Any(_)
        ));
        assert_eq!(
            BitmapUnionIntAgg.build_merge_view(&spec, &a).err().unwrap(),
            "failed to downcast to BinaryArray"
        );
    }
    let mut s = Original::new("bitmap_union_int", &DataType::Int32).unwrap();
    let p = s.pointer();
    let wrong = Arc::new(Int32Array::from(vec![1])) as ArrayRef;
    let wrong = AggInputView::Any(&wrong);
    assert_eq!(
        BitmapUnionIntAgg
            .merge_batch(&s.spec, 0, &[p], &wrong)
            .unwrap_err(),
        "bitmap_union_int merge input type mismatch"
    );
    assert_eq!(
        BitmapUnionIntAgg
            .build_input_view(&s.spec, &None)
            .err()
            .unwrap(),
        "bitmap_union_int input missing"
    );
    assert_eq!(
        BitmapUnionIntAgg
            .build_merge_view(&s.spec, &None)
            .err()
            .unwrap(),
        "bitmap_union_int intermediate input missing"
    );
}
#[test]
fn original_bitmap_union_int_shared_family_negative_policy_and_binary_result_contract() {
    for name in [
        "bitmap_union_int",
        "bitmap_union_count",
        "bitmap_agg",
        "bitmap_union",
    ] {
        let a = Arc::new(Int32Array::from(vec![Some(-1), Some(7), None])) as ArrayRef;
        let mut s = Original::new(name, a.data_type()).unwrap();
        s.update(a).unwrap();
        let expected = if name == "bitmap_union_int" || name == "bitmap_union_count" {
            BTreeSet::from([7, u64::MAX])
        } else {
            BTreeSet::from([7])
        };
        assert_eq!(decode_bitmap(&s.bytes().unwrap()).unwrap(), expected);
        let out = s.output(false);
        assert_eq!(
            out.data_type(),
            if name == "bitmap_union_int" || name == "bitmap_union_count" {
                &DataType::Int64
            } else {
                &DataType::Binary
            }
        );
    }
}
#[test]
fn original_bitmap_union_int_actual_catalogue_fixed_arity_and_layout_memory_policy() {
    use novarocks_functions::{FunctionArgument, FunctionBindingRequest, FunctionValueType};
    let catalogue = novarocks_functions::builtin::catalogue::builtin_engine_function_catalog();
    for n in [0, 1, 2] {
        let args = (0..n)
            .map(|_| FunctionArgument::Value {
                value_type: FunctionValueType::new(DataType::Int32, true),
                constant: None,
            })
            .collect::<Vec<_>>();
        let resolved = catalogue.resolve_bound_user(
            "bitmap_union_int",
            novarocks_functions::FunctionKind::Aggregate,
            FunctionBindingRequest {
                expected_result_type: None,
                arguments: &args,
                logical_argument_count: n,
            },
            &crate::exec::expr::pure_differential::HarnessControl,
        );
        assert_eq!(resolved.is_ok(), n == 1);
    }
    let s = Original::new("bitmap_union_int", &DataType::Int32).unwrap();
    assert_eq!(
        BitmapUnionIntAgg.state_layout_for(&s.spec.kind),
        (
            std::mem::size_of::<BitmapState>(),
            std::mem::align_of::<BitmapState>()
        )
    );
    assert!(matches!(
        BitmapUnionIntAgg.retained_memory_policy(&s.spec),
        RetainedMemoryPolicy::AllocationTracked
    ));
}
