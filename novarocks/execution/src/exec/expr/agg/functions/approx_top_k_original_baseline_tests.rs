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
//! Original ApproxTopKAgg entrypoints, typed state, codec and tracker only.
use super::*;
use crate::exec::node::aggregate::AggTypeSignature;
use arrow::array::{
    Array, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray, UInt32Array,
};
use arrow::datatypes::{Field, Fields, TimeUnit};
use arrow_buffer::{NullBuffer, i256};
use std::mem::MaybeUninit;
struct Original {
    raw: MaybeUninit<ApproxTopKState>,
    spec: AggSpec,
}
impl Original {
    fn new(item: DataType) -> Self {
        let (output, intermediate) =
            novarocks_functions::aggregate_types::infer_agg_function_types(
                "approx_top_k",
                &[item.clone()],
                false,
            )
            .unwrap();
        let f = AggFunction {
            name: "approx_top_k".into(),
            types: Some(AggTypeSignature {
                output_type: Some(output),
                intermediate_type: intermediate,
                input_arg_type: Some(item.clone()),
            }),
            ..Default::default()
        };
        let spec = ApproxTopKAgg
            .build_spec_from_type(&f, Some(&item), false)
            .unwrap();
        let mut this = Self {
            raw: MaybeUninit::uninit(),
            spec,
        };
        ApproxTopKAgg
            .init_state_with_tracker(
                &this.spec,
                this.raw.as_mut_ptr().cast(),
                Some(MemTracker::new_root("original-approx-top-k")),
            )
            .unwrap();
        this
    }
    fn ptr(&mut self) -> AggStatePtr {
        self.raw.as_mut_ptr() as AggStatePtr
    }
    fn state(&self) -> &ApproxTopKState {
        unsafe { self.raw.assume_init_ref() }
    }
    fn update(&mut self, a: ArrayRef) -> Result<(), String> {
        let ps = vec![self.ptr(); a.len()];
        ApproxTopKAgg.update_batch(&self.spec, 0, &ps, &AggInputView::Any(&a))
    }
    fn args(&mut self, a: Vec<ArrayRef>) -> Result<(), String> {
        if a.len() == 1 {
            return self.update(a[0].clone());
        }
        let fields = a
            .iter()
            .enumerate()
            .map(|(i, v)| Field::new(format!("argument-{i}"), v.data_type().clone(), true))
            .collect::<Vec<_>>();
        self.update(Arc::new(StructArray::new(Fields::from(fields), a, None)))
    }
    fn output(&mut self, intermediate: bool) -> Result<ArrayRef, String> {
        let p = self.ptr();
        ApproxTopKAgg.build_array(&self.spec, 0, &[p], intermediate)
    }
    fn bytes(&mut self) -> Vec<u8> {
        let a = self.output(true).unwrap();
        a.as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0)
            .to_vec()
    }
    fn merge(&mut self, a: ArrayRef) -> Result<(), String> {
        let ps = vec![self.ptr(); a.len()];
        let a = Some(a);
        let view = ApproxTopKAgg.build_merge_view(&self.spec, &a)?;
        ApproxTopKAgg.merge_batch(&self.spec, 0, &ps, &view)
    }
    fn pairs(&mut self) -> Vec<(Option<AggScalarValue>, i64)> {
        let a = self.output(false).unwrap();
        let list = a.as_any().downcast_ref::<ListArray>().unwrap();
        assert!(!list.is_null(0));
        let row = list.value(0);
        let row = row.as_any().downcast_ref::<StructArray>().unwrap();
        (0..row.len())
            .map(|i| {
                let value = scalar_from_array(row.column(0), i).unwrap();
                let count = row
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(i);
                (value, count)
            })
            .collect()
    }
}
impl Drop for Original {
    fn drop(&mut self) {
        ApproxTopKAgg.drop_state(&self.spec, self.raw.as_mut_ptr().cast());
    }
}
fn ints(a: Vec<Option<i32>>) -> ArrayRef {
    Arc::new(Int32Array::from(a))
}
fn binary(v: &[u8]) -> ArrayRef {
    Arc::new(BinaryArray::from(vec![Some(v)]))
}
fn payload(v: &[u8], count: i64) -> Vec<u8> {
    let mut x = Vec::new();
    for n in [5_u32, 100, 1, v.len() as u32] {
        x.extend_from_slice(&n.to_le_bytes());
    }
    x.extend_from_slice(v);
    x.extend_from_slice(&count.to_le_bytes());
    x
}
#[test]
fn original_approx_top_k_three_arities_default_limits_null_key_and_tie_order() {
    for n in 1..=3 {
        let mut s = Original::new(DataType::Int32);
        let mut args = vec![ints(vec![Some(2), Some(1), None, Some(2), Some(1), None])];
        if n >= 2 {
            args.push(Arc::new(Int64Array::from(vec![3; 6])));
        }
        if n >= 3 {
            args.push(Arc::new(Int64Array::from(vec![100; 6])));
        }
        s.args(args).unwrap();
        assert_eq!(s.state().k, if n == 1 { 5 } else { 3 });
        assert_eq!(s.state().counter_num, 100);
        let p = s.pairs();
        assert_eq!(p.len(), 3);
        assert!(p[0].0.is_none());
        assert!(matches!(p[1].0, Some(AggScalarValue::Int64(1))));
        assert!(matches!(p[2].0, Some(AggScalarValue::Int64(2))));
        assert!(p.iter().all(|p| p.1 == 2));
    }
}
#[test]
fn original_approx_top_k_parameter_reader_clamps_invalid_and_per_row_update_order() {
    let v = ints(vec![None, Some(7), Some(8), Some(9)]);
    let mut s = Original::new(DataType::Int32);
    s.args(vec![
        v,
        Arc::new(Float64Array::from(vec![f64::NAN, 2.9, -1.0, 3.0])),
        Arc::new(Float64Array::from(vec![f64::INFINITY, 1.9, 0.0, 2.0])),
    ])
    .unwrap();
    assert_eq!(s.state().k, 3);
    assert_eq!(s.state().counter_num, 3);
    let mut s = Original::new(DataType::Int32);
    s.args(vec![
        ints(vec![None]),
        Arc::new(BooleanArray::from(vec![true])),
        Arc::new(StringArray::from(vec!["ignored"])),
    ])
    .unwrap();
    assert_eq!(s.state().k, 5);
    assert_eq!(s.state().counter_num, 100);
    assert_eq!(s.pairs().len(), 1);
    let mut s = Original::new(DataType::Int32);
    assert_eq!(
        s.args(vec![ints(vec![None]), Arc::new(UInt32Array::from(vec![1]))])
            .unwrap_err(),
        "unsupported scalar type: UInt32"
    );
}
#[test]
fn original_approx_top_k_empty_and_all_null_return_nonnull_list_and_exact_empty_state_header() {
    let mut s = Original::new(DataType::Int32);
    assert!(s.pairs().is_empty());
    assert_eq!(
        s.bytes(),
        [
            5_u32.to_le_bytes(),
            100_u32.to_le_bytes(),
            0_u32.to_le_bytes()
        ]
        .concat()
    );
    s.update(ints(Vec::new())).unwrap();
    assert!(s.pairs().is_empty());
    s.update(ints(vec![None, None])).unwrap();
    let p = s.pairs();
    assert_eq!(p.len(), 1);
    assert!(p[0].0.is_none());
    assert_eq!(p[0].1, 2);
}
#[test]
fn original_approx_top_k_tracked_counter_eviction_saturation_and_drop_keep_original_author() {
    let tracker = MemTracker::new_root("original-topk-counter");
    let mut s = ApproxTopKState::new(tracker.clone());
    s.initialized = true;
    s.k = 1;
    s.counter_num = 2;
    for i in [1, 2, 3] {
        let a = ints(vec![Some(i)]);
        let value = tracked_scalar_from_array(&a, 0, &s.allocator).unwrap();
        update_one(&mut s, value, 1).unwrap();
    }
    assert_eq!(s.counts.len(), 2);
    assert!(s.counts.values().any(|v| v.count == 2));
    s.counter_num = 1;
    enforce_counter_limit(&mut s);
    assert_eq!(s.counts.len(), 1);
    let a = ints(vec![Some(7)]);
    let value = tracked_scalar_from_array(&a, 0, &s.allocator).unwrap();
    update_one(&mut s, value, i64::MAX).unwrap();
    let value = tracked_scalar_from_array(&a, 0, &s.allocator).unwrap();
    update_one(&mut s, value, 1).unwrap();
    assert_eq!(s.counts.values().next().unwrap().count, i64::MAX);
    drop(s);
    assert_eq!(tracker.current(), 0);
}
#[test]
fn original_approx_top_k_merge_full_codec_errors_and_null_mask_are_exact() {
    for (bytes, message) in [
        (vec![], "approx_top_k merge payload too short".to_string()),
        (
            [
                5_u32.to_le_bytes(),
                100_u32.to_le_bytes(),
                100001_u32.to_le_bytes(),
            ]
            .concat(),
            "approx_top_k merge entry count 100001 exceeds 100000".to_string(),
        ),
        (
            [
                5_u32.to_le_bytes(),
                100_u32.to_le_bytes(),
                1_u32.to_le_bytes(),
            ]
            .concat(),
            "approx_top_k decode entry len failed".to_string(),
        ),
        (
            payload(&[1, 255], 1),
            "approx_top_k decode: unknown tag 255".to_string(),
        ),
        (
            payload(&[0, 0], 1),
            "approx_top_k decode: trailing bytes".to_string(),
        ),
    ] {
        let mut s = Original::new(DataType::Int32);
        assert_eq!(s.merge(binary(&bytes)).unwrap_err(), message);
    }
    let mut s = Original::new(DataType::Int32);
    s.merge(Arc::new(BinaryArray::from(vec![None::<&[u8]>])))
        .unwrap();
    assert!(s.pairs().is_empty());
    assert_eq!(
        s.merge(ints(vec![Some(1)])).unwrap_err(),
        "approx_top_k merge input must be BinaryArray"
    );
}
#[test]
fn original_approx_top_k_all_tracked_scalar_classes_and_nested_payload_roundtrip() {
    let nested = DataType::Struct(vec![Field::new("authored-child", DataType::Int32, true)].into());
    let list = DataType::List(Arc::new(Field::new("authored-item", DataType::Int32, true)));
    let map = DataType::Map(
        Arc::new(Field::new(
            "authored-entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Int32, false),
                    Field::new("value", DataType::Utf8, true),
                ]
                .into(),
            ),
            false,
        )),
        false,
    );
    let mut cases = vec![
        (DataType::Boolean, AggScalarValue::Bool(true)),
        (DataType::Int8, AggScalarValue::Int64(7)),
        (DataType::Int16, AggScalarValue::Int64(7)),
        (DataType::Int32, AggScalarValue::Int64(7)),
        (DataType::Int64, AggScalarValue::Int64(7)),
        (DataType::Float32, AggScalarValue::Float64(-0.0)),
        (DataType::Float64, AggScalarValue::Float64(f64::NAN)),
        (DataType::Utf8, AggScalarValue::Utf8("é雪😀".into())),
        (DataType::Binary, AggScalarValue::Binary(vec![0, 255, 17])),
        (
            DataType::LargeBinary,
            AggScalarValue::Binary(vec![0, 255, 17]),
        ),
        (DataType::Date32, AggScalarValue::Date32(i32::MAX)),
        (
            DataType::FixedSizeBinary(16),
            AggScalarValue::Decimal128(i128::MIN),
        ),
        (
            list,
            AggScalarValue::List(vec![Some(AggScalarValue::Int64(7)), None]),
        ),
        (
            nested,
            AggScalarValue::Struct(vec![Some(AggScalarValue::Int64(7))]),
        ),
        (
            map,
            AggScalarValue::Map(vec![(
                Some(AggScalarValue::Int64(1)),
                Some(AggScalarValue::Utf8("x".into())),
            )]),
        ),
    ];
    for u in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        cases.push((
            DataType::Timestamp(u, Some("UTC".into())),
            AggScalarValue::Timestamp(1),
        ));
    }
    for (p, scale) in [(1, -1), (1, 0), (38, -38), (38, 38)] {
        cases.push((
            DataType::Decimal128(p, scale),
            AggScalarValue::Decimal128(1),
        ));
    }
    for (p, scale) in [(1, -1), (1, 0), (76, -76), (76, 76)] {
        cases.push((
            DataType::Decimal256(p, scale),
            AggScalarValue::Decimal256(i256::ONE),
        ));
    }
    for (ty, v) in cases {
        let a = build_scalar_array(&ty, vec![Some(v), None]).unwrap();
        let mut source = Original::new(ty.clone());
        source
            .args(vec![a, Arc::new(Int64Array::from(vec![5; 2]))])
            .unwrap();
        let expected = source.output(false);
        let bytes = source.bytes();
        let mut target = Original::new(ty.clone());
        target.merge(binary(&bytes)).unwrap();
        let actual = target.output(false);
        assert_eq!(actual.unwrap().to_data(), expected.unwrap().to_data());
    }
}
#[test]
fn original_approx_top_k_first_struct_single_argument_is_original_argument_pack_ambiguity() {
    let ty = DataType::Struct(vec![Field::new("authored", DataType::Int32, true)].into());
    let a = Arc::new(StructArray::new(
        vec![Field::new("authored", DataType::Int32, true)].into(),
        vec![ints(vec![Some(1)])],
        None,
    )) as ArrayRef;
    let mut s = Original::new(ty);
    s.update(a).unwrap();
    assert_eq!(
        s.output(false).unwrap_err(),
        "scalar output type mismatch for Struct"
    );
}
#[test]
fn original_approx_top_k_hidden_root_null_masks_tracked_value_but_not_parameter_setup() {
    let packed = Arc::new(StructArray::new(
        vec![
            Field::new("value", DataType::UInt32, true),
            Field::new("k", DataType::Int64, true),
        ]
        .into(),
        vec![
            Arc::new(UInt32Array::from(vec![Some(u32::MAX)])) as ArrayRef,
            Arc::new(Int64Array::from(vec![2])),
        ],
        Some(NullBuffer::from(vec![false])),
    )) as ArrayRef;
    let mut s = Original::new(DataType::Int32);
    s.update(packed).unwrap();
    assert_eq!(s.state().k, 2);
    assert!(s.pairs()[0].0.is_none());
    let packed = Arc::new(StructArray::new(
        vec![
            Field::new("value", DataType::Int32, true),
            Field::new("k", DataType::UInt32, true),
        ]
        .into(),
        vec![
            ints(vec![None]),
            Arc::new(UInt32Array::from(vec![Some(1)])) as ArrayRef,
        ],
        Some(NullBuffer::from(vec![false])),
    )) as ArrayRef;
    let mut s = Original::new(DataType::Int32);
    assert_eq!(
        s.update(packed).unwrap_err(),
        "unsupported scalar type: UInt32"
    );
}
#[test]
fn original_approx_top_k_output_contract_and_tracker_refusal_errors_are_exact() {
    let mut s = Original::new(DataType::Int32);
    s.spec.output_type = DataType::Int64;
    assert_eq!(
        s.output(false).unwrap_err(),
        "approx_top_k output type must be LIST<STRUCT>, got Int64"
    );
    let mut s = Original::new(DataType::Int32);
    s.spec.output_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    assert_eq!(
        s.output(false).unwrap_err(),
        "approx_top_k list element type must be STRUCT, got Int64"
    );
    let mut s = Original::new(DataType::Int32);
    s.spec.output_type = DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(vec![Field::new("only", DataType::Int32, true)].into()),
        true,
    )));
    assert_eq!(
        s.output(false).unwrap_err(),
        "approx_top_k output struct must have at least 2 fields"
    );
    let mut raw = MaybeUninit::<ApproxTopKState>::uninit();
    assert_eq!(
        ApproxTopKAgg
            .init_state_with_tracker(&s.spec, raw.as_mut_ptr().cast(), None)
            .unwrap_err(),
        "allocation-tracked approx_top_k requires an aggregate memory tracker"
    );
}
