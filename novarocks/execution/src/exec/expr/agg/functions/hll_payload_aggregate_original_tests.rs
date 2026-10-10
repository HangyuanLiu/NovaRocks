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

//! Original HLL payload aggregation methods: no replacement state, reader or decoder.
use super::*;
use arrow::array::{BooleanArray, Float64Array, Int32Array, NullArray, new_null_array};
use arrow::datatypes::Field;
struct Original {
    slot: Box<usize>,
    spec: AggSpec,
}
impl Original {
    fn new(name: &str, ty: &DataType) -> Self {
        let spec = HllRawAgg
            .build_spec_from_type(
                &AggFunction {
                    name: name.into(),
                    ..Default::default()
                },
                Some(ty),
                false,
            )
            .unwrap();
        let mut s = Self {
            slot: Box::new(0),
            spec,
        };
        let ptr = s.ptr();
        HllRawAgg.init_state(&s.spec, ptr);
        s
    }
    fn ptr(&mut self) -> *mut u8 {
        (&mut *self.slot as *mut usize).cast()
    }
    fn update(&mut self, a: ArrayRef) -> Result<(), String> {
        let rows = vec![self.ptr() as AggStatePtr; a.len()];
        HllRawAgg.update_batch(&self.spec, 0, &rows, &AggInputView::Any(&a))
    }
    fn merge(&mut self, a: ArrayRef) -> Result<(), String> {
        let rows = vec![self.ptr() as AggStatePtr; a.len()];
        HllRawAgg.merge_batch(&self.spec, 0, &rows, &AggInputView::Any(&a))
    }
    fn output(&mut self, partial: bool) -> ArrayRef {
        let ptr = self.ptr();
        HllRawAgg
            .build_array(&self.spec, 0, &[ptr as AggStatePtr], partial)
            .unwrap()
    }
    fn bytes(&mut self, partial: bool) -> Option<Vec<u8>> {
        let a = self.output(partial);
        let a = a.as_any().downcast_ref::<BinaryArray>().unwrap();
        (!a.is_null(0)).then(|| a.value(0).to_vec())
    }
    fn count(&mut self) -> Option<i64> {
        let a = self.output(false);
        let a = a.as_any().downcast_ref::<Int64Array>().unwrap();
        (!a.is_null(0)).then(|| a.value(0))
    }
}
impl Drop for Original {
    fn drop(&mut self) {
        let ptr = self.ptr();
        HllRawAgg.drop_state(&self.spec, ptr)
    }
}
fn names() -> [&'static str; 3] {
    ["hll_union", "hll_raw_agg", "hll_union_agg"]
}
fn arrays(values: &[Option<Vec<u8>>]) -> Vec<ArrayRef> {
    let text: Vec<Option<&str>> = values
        .iter()
        .map(|v| v.as_ref().map(|v| std::str::from_utf8(v).unwrap()))
        .collect();
    vec![
        std::sync::Arc::new(BinaryArray::from_iter(values.iter().map(|v| v.as_deref()))),
        std::sync::Arc::new(LargeBinaryArray::from_iter(
            values.iter().map(|v| v.as_deref()),
        )),
        std::sync::Arc::new(StringArray::from(text.clone())),
        std::sync::Arc::new(LargeStringArray::from(text)),
    ]
}
#[test]
fn original_hll_payload_aggregate_names_output_state_and_missing_argument() {
    for name in names() {
        for supplied in [name.to_owned(), format!("{name}|original-option")] {
            let spec = HllRawAgg
                .build_spec_from_type(
                    &AggFunction {
                        name: supplied,
                        ..Default::default()
                    },
                    Some(&DataType::Int32),
                    false,
                )
                .unwrap();
            assert_eq!(
                spec.output_type,
                if name == "hll_union_agg" {
                    DataType::Int64
                } else {
                    DataType::Binary
                }
            );
            assert_eq!(spec.intermediate_type, DataType::Binary);
        }
        assert_eq!(
            HllRawAgg
                .build_spec_from_type(
                    &AggFunction {
                        name: name.into(),
                        ..Default::default()
                    },
                    None,
                    false
                )
                .unwrap_err(),
            "hll_raw expects exactly one argument"
        );
    }
}
#[test]
fn original_hll_payload_aggregate_all_four_carriers_null_empty_and_singleton() {
    for name in names() {
        for a in arrays(&[None, Some(vec![0])]) {
            let mut s = Original::new(name, a.data_type());
            assert!(s.output(false).is_null(0));
            assert_eq!(s.bytes(true), None);
            s.update(a.slice(0, 1)).unwrap();
            assert!(s.output(false).is_null(0));
            s.update(a.slice(1, 1)).unwrap();
            assert_eq!(s.bytes(true), Some(vec![0]));
            if name == "hll_union_agg" {
                assert_eq!(s.count(), Some(0))
            } else {
                assert_eq!(s.bytes(false), Some(vec![0]))
            }
            s.update(
                arrays(&[Some(vec![1, 1, 1, 0, 0, 0, 0, 0, 0, 0])])
                    .into_iter()
                    .find(|x| x.data_type() == a.data_type())
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(s.bytes(true), Some(vec![2, 1, 0, 0, 0, 1, 0, 51]));
            if name == "hll_union_agg" {
                assert_eq!(s.count(), Some(1))
            }
        }
    }
}
#[test]
fn original_hll_payload_aggregate_malformed_first_failure_mutation_and_full_message() {
    for name in names() {
        for (payload, message) in [
            (vec![], "hll_raw merge payload is empty"),
            (vec![1], "hll_raw EXPLICIT payload is malformed"),
            (vec![2, 0], "hll_raw SPARSE payload is malformed"),
        ] {
            for a in arrays(&[
                Some(vec![0]),
                Some(payload.clone()),
                Some(vec![1, 1, 1, 0, 0, 0, 0, 0, 0, 0]),
            ]) {
                let mut s = Original::new(name, a.data_type());
                assert_eq!(s.update(a).unwrap_err(), message);
                assert_eq!(s.bytes(true), Some(vec![0]));
            }
            for a in arrays(&[Some(payload.clone())]) {
                let mut s = Original::new(name, a.data_type());
                assert_eq!(s.merge(a).unwrap_err(), message);
                // The original materializes and marks the group before decoder failure.
                assert_eq!(s.bytes(true), Some(vec![0]));
            }
        }
    }
}
#[test]
fn original_hll_payload_aggregate_opaque_fallback_and_duplicate_payload_are_original_bytes() {
    for name in names() {
        for payload in [
            vec![99, 1, 2],
            vec![1, 1],
            vec![3, 1],
            vec![2, 1, 0, 0, 0],
            "雪".as_bytes().to_vec(),
        ] {
            let mut expected = HllRawState {
                has_value: true,
                ..Default::default()
            };
            update_state_register_from_hash(&mut expected, hash_bytes_for_hll(&payload));
            let expected = serialize_hll_state(&expected);
            for a in arrays(&[Some(payload.clone()), None, Some(payload.clone())]) {
                let mut s = Original::new(name, a.data_type());
                s.update(a).unwrap();
                assert_eq!(s.bytes(true), expected);
            }
        }
    }
}
#[test]
fn original_hll_payload_aggregate_full_and_sparse_state_phase_roundtrip() {
    let mut full = vec![3];
    full.extend(std::iter::repeat_n(1, HLL_REGISTERS_COUNT));
    for name in names() {
        for payload in [full.clone(), vec![2, 1, 0, 0, 0, 1, 0, 51]] {
            let mut local = Original::new(name, &DataType::Binary);
            local
                .update(arrays(&[Some(payload.clone())]).remove(0))
                .unwrap();
            let bytes = local.bytes(true).unwrap();
            assert_eq!(bytes, payload);
            let mut final_ = Original::new(name, &DataType::Binary);
            final_
                .merge(arrays(&[None, Some(bytes.clone()), Some(bytes)]).remove(0))
                .unwrap();
            assert_eq!(final_.bytes(true), local.bytes(true));
            if name == "hll_union_agg" {
                assert_eq!(final_.count(), local.count())
            } else {
                assert_eq!(final_.bytes(false), local.bytes(false))
            }
        }
    }
}
#[test]
fn original_hll_payload_aggregate_unsupported_carrier_error_precedes_null_and_empty() {
    for name in names() {
        for ty in [
            DataType::Null,
            DataType::Int32,
            DataType::Float64,
            DataType::FixedSizeBinary(16),
            DataType::Decimal128(38, 2),
            DataType::List(std::sync::Arc::new(Field::new(
                "item",
                DataType::Int32,
                true,
            ))),
        ] {
            for rows in [0, 3] {
                let mut s = Original::new(name, &ty);
                let a = new_null_array(&ty, rows);
                let expected =
                    format!("hll aggregate expects HLL/BINARY payload input, got {ty:?}");
                assert_eq!(s.update(a.clone()).unwrap_err(), expected);
                assert_eq!(s.merge(a).unwrap_err(), expected);
                assert!(s.output(false).is_null(0));
            }
        }
    }
}
#[test]
fn original_hll_payload_aggregate_group_order_and_nullable_final_contract() {
    for name in names() {
        let mut a = Original::new(name, &DataType::Binary);
        let mut b = Original::new(name, &DataType::Binary);
        let mut c = Original::new(name, &DataType::Binary);
        let input = arrays(&[
            None,
            Some(vec![0]),
            Some(vec![1, 1, 1, 0, 0, 0, 0, 0, 0, 0]),
        ])
        .remove(0);
        let rows = [
            a.ptr() as AggStatePtr,
            b.ptr() as AggStatePtr,
            c.ptr() as AggStatePtr,
        ];
        HllRawAgg
            .update_batch(&a.spec, 0, &rows, &AggInputView::Any(&input))
            .unwrap();
        let result = HllRawAgg
            .build_array(&a.spec, 0, &[rows[2], rows[0], rows[1]], false)
            .unwrap();
        assert!(!result.is_null(0) && result.is_null(1) && !result.is_null(2));
        if name == "hll_union_agg" {
            let x = result.as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(x.value(0), 1);
            assert_eq!(x.value(2), 0)
        }
    }
}
