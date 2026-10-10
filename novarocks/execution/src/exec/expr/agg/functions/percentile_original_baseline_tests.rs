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

//! Original aggregate entry tests, mounted before shared-core extraction.
use super::*;
use crate::exec::node::aggregate::AggTypeSignature;
use arrow::array::{
    BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array, StringArray,
};
use arrow::datatypes::{Field, Fields};
use std::mem::MaybeUninit;

struct Legacy {
    raw: MaybeUninit<ExactPercentileState>,
    spec: AggSpec,
}
impl Legacy {
    fn new(name: &str, output: DataType) -> Self {
        let function = AggFunction {
            name: name.into(),
            types: Some(AggTypeSignature {
                intermediate_type: Some(DataType::Binary),
                output_type: Some(output),
                input_arg_type: None,
            }),
            ..Default::default()
        };
        let spec = PercentilePlaceholderAgg
            .build_spec_from_type(&function, None, false)
            .unwrap();
        let mut this = Self {
            raw: MaybeUninit::uninit(),
            spec,
        };
        PercentilePlaceholderAgg
            .init_state_with_tracker(
                &this.spec,
                this.raw.as_mut_ptr().cast(),
                Some(MemTracker::new_root("original-percentile")),
            )
            .unwrap();
        this
    }
    fn pointer(&mut self) -> AggStatePtr {
        self.raw.as_mut_ptr() as AggStatePtr
    }
    fn update(&mut self, values: ArrayRef, rates: ArrayRef) -> Result<(), String> {
        let input: ArrayRef = Arc::new(StructArray::new(
            Fields::from(vec![
                Field::new("v", values.data_type().clone(), true),
                Field::new("r", rates.data_type().clone(), true),
            ]),
            vec![values, rates],
            None,
        ));
        let pointers = vec![self.pointer(); input.len()];
        PercentilePlaceholderAgg.update_batch(&self.spec, 0, &pointers, &AggInputView::Any(&input))
    }
    fn merge(&mut self, input: ArrayRef) -> Result<(), String> {
        let pointers = vec![self.pointer(); input.len()];
        PercentilePlaceholderAgg.merge_batch(&self.spec, 0, &pointers, &AggInputView::Any(&input))
    }
    fn output(&mut self, partial: bool) -> Result<ArrayRef, String> {
        let pointer = self.pointer();
        PercentilePlaceholderAgg.build_array(&self.spec, 0, &[pointer], partial)
    }
    fn rate(&self) -> Option<f64> {
        unsafe { self.raw.assume_init_ref() }.rate
    }
    fn len(&self) -> usize {
        unsafe { self.raw.assume_init_ref() }.values.len()
    }
}
impl Drop for Legacy {
    fn drop(&mut self) {
        PercentilePlaceholderAgg.drop_state(&self.spec, self.raw.as_mut_ptr().cast());
    }
}
fn ints(values: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}
fn rates(values: Vec<Option<f64>>) -> ArrayRef {
    Arc::new(Float64Array::from(values))
}
fn bin(value: &[u8]) -> ArrayRef {
    Arc::new(BinaryArray::from(vec![Some(value)]))
}
fn f64_output(state: &mut Legacy) -> Option<f64> {
    state
        .output(false)
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .iter()
        .next()
        .unwrap()
}
#[test]
fn original_percentile_cont_interpolates_original_sorted_duplicates_and_endpoints() {
    for (rate, expected) in [
        (0.0, 1.0),
        (0.25, 1.75),
        (0.5, 3.0),
        (0.75, 5.25),
        (1.0, 9.0),
    ] {
        let mut state = Legacy::new("percentile_cont", DataType::Float64);
        state
            .update(
                ints(vec![Some(9), Some(1), Some(4), Some(2)]),
                rates(vec![Some(rate); 4]),
            )
            .unwrap();
        assert_eq!(f64_output(&mut state), Some(expected));
    }
}
#[test]
fn original_percentile_disc_and_lc_share_ceil_rank_and_utf8_date_decimal_carriers() {
    for name in ["percentile_disc", "percentile_disc_lc"] {
        let mut state = Legacy::new(name, DataType::Int64);
        state
            .update(
                ints(vec![Some(9), Some(1), Some(4), Some(2)]),
                rates(vec![Some(0.25); 4]),
            )
            .unwrap();
        assert_eq!(
            state
                .output(false)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            2
        );
        let mut text = Legacy::new(name, DataType::Utf8);
        text.update(
            Arc::new(StringArray::from(vec!["z", "a", "m"])),
            rates(vec![Some(0.5); 3]),
        )
        .unwrap();
        assert_eq!(
            text.output(false)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "m"
        );
        let mut date = Legacy::new(name, DataType::Date32);
        date.update(
            Arc::new(Date32Array::from(vec![3, 1, 2])),
            rates(vec![Some(0.5); 3]),
        )
        .unwrap();
        assert_eq!(
            date.output(false)
                .unwrap()
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .value(0),
            2
        );
        let ty = DataType::Decimal128(38, -2);
        let mut decimal = Legacy::new(name, ty);
        decimal
            .update(
                Arc::new(
                    Decimal128Array::from(vec![300, 100, 200])
                        .with_precision_and_scale(38, -2)
                        .unwrap(),
                ),
                rates(vec![Some(0.5); 3]),
            )
            .unwrap();
        assert_eq!(
            decimal
                .output(false)
                .unwrap()
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(0),
            200
        );
    }
}
#[test]
fn original_percentile_null_rate_defaults_zero_empty_is_null_and_rate_precedes_value_null() {
    for name in ["percentile_cont", "percentile_disc", "percentile_disc_lc"] {
        let mut state = Legacy::new(name, DataType::Float64);
        assert_eq!(f64_output(&mut state), None);
        state
            .update(
                rates(vec![Some(3.0), None, Some(1.0)]),
                rates(vec![None; 3]),
            )
            .unwrap();
        assert_eq!(f64_output(&mut state), Some(1.0));
        assert_eq!(state.rate(), None);
        let mut invalid = Legacy::new(name, DataType::Float64);
        assert_eq!(
            invalid
                .update(ints(vec![None]), rates(vec![Some(2.0)]))
                .unwrap_err(),
            "Percentile rate must be between 0 and 1"
        );
        assert_eq!(invalid.len(), 0);
    }
}
#[test]
fn original_percentile_rate_errors_nan_infinity_epsilon_and_partial_mutation_keep_full_text() {
    for rate in [-0.1, 1.1, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let mut state = Legacy::new("percentile_cont", DataType::Float64);
        assert_eq!(
            state
                .update(ints(vec![Some(1)]), rates(vec![Some(rate)]))
                .unwrap_err(),
            "Percentile rate must be between 0 and 1"
        );
    }
    let mut state = Legacy::new("percentile_cont", DataType::Float64);
    assert_eq!(
        state
            .update(
                ints(vec![Some(1), Some(2)]),
                rates(vec![Some(0.5), Some(0.75)])
            )
            .unwrap_err(),
        "percentile rate mismatch while merging states: existing=0.5 incoming=0.75"
    );
    assert_eq!(state.len(), 1);
    assert_eq!(state.rate(), Some(0.5));
    state
        .update(ints(vec![Some(2)]), rates(vec![Some(0.5 + f64::EPSILON)]))
        .unwrap();
    assert_eq!(state.len(), 2);
    assert_eq!(state.rate(), Some(0.5));
}
#[test]
fn original_percentile_exact_state_bytes_empty_unknown_fields_and_trailing_tokens() {
    let mut state = Legacy::new("percentile_cont", DataType::Float64);
    assert_eq!(
        state
            .output(true)
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        b"\xc3\x01{\"rate\":null,\"values\":[]}"
    );
    state
        .update(ints(vec![Some(3), Some(1)]), rates(vec![Some(0.5); 2]))
        .unwrap();
    let payload = state.output(true).unwrap();
    let bytes = payload
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap()
        .value(0);
    assert_eq!(
        bytes,
        b"\xc3\x01{\"rate\":0.5,\"values\":[{\"Int64\":3},{\"Int64\":1}]}"
    );
    let mut merged = Legacy::new("percentile_cont", DataType::Float64);
    merged.merge(payload).unwrap();
    assert_eq!(f64_output(&mut merged), Some(2.0));
    merged
        .merge(bin(b"\xc3\x01{\"unknown\":[1],\"values\":[]} trailing"))
        .unwrap();
    merged.merge(bin(b"")).unwrap();
    assert_eq!(merged.len(), 2);
}
#[test]
fn original_percentile_malformed_state_header_and_merge_rate_preserve_original_errors() {
    let mut state = Legacy::new("percentile_cont", DataType::Float64);
    for (bytes, expected) in [
        (b"x".as_slice(), "exact percentile payload too short"),
        (
            b"\x00\x01".as_slice(),
            "unsupported exact percentile payload magic: expected=0xc3 actual=0x00",
        ),
        (
            b"\xc3\x02".as_slice(),
            "unsupported exact percentile payload version: expected=1 actual=2",
        ),
    ] {
        assert_eq!(state.merge(bin(bytes)).unwrap_err(), expected);
    }
    state
        .update(ints(vec![Some(1)]), rates(vec![Some(0.5)]))
        .unwrap();
    assert_eq!(
        state
            .merge(bin(b"\xc3\x01{\"rate\":0.75,\"values\":[{\"Int64\":9}]}"))
            .unwrap_err(),
        "percentile rate mismatch while merging states: existing=0.5 incoming=0.75"
    );
    assert_eq!(state.len(), 1);
}
#[test]
fn original_percentile_cont_keeps_non_interpolating_success_and_original_output_errors() {
    for ty in [DataType::Int64, DataType::Decimal128(38, 2)] {
        let values: ArrayRef = if ty == DataType::Int64 {
            ints(vec![Some(1), Some(3)])
        } else {
            Arc::new(
                Decimal128Array::from(vec![100, 300])
                    .with_precision_and_scale(38, 2)
                    .unwrap(),
            )
        };
        let mut endpoints = Legacy::new("percentile_cont", ty.clone());
        endpoints
            .update(values.clone(), rates(vec![Some(0.0); 2]))
            .unwrap();
        assert!(endpoints.output(false).is_ok());
        let mut inner = Legacy::new("percentile_cont", ty.clone());
        inner.update(values, rates(vec![Some(0.5); 2])).unwrap();
        assert_eq!(
            inner.output(false).unwrap_err(),
            format!("unsupported percentile_cont output type {:?}", ty)
        );
    }
    let mut text = Legacy::new("percentile_cont", DataType::Utf8);
    text.update(
        Arc::new(StringArray::from(vec!["a", "z"])),
        rates(vec![Some(0.5); 2]),
    )
    .unwrap();
    assert_eq!(
        text.output(false).unwrap_err(),
        "percentile_cont: unsupported percentile_cont interpolation input Utf8(\"a\")"
    );
}
#[test]
fn original_percentile_unsupported_value_and_rate_type_errors_preserve_full_debug() {
    let mut state = Legacy::new("percentile_disc", DataType::Boolean);
    assert_eq!(
        state
            .update(
                Arc::new(BooleanArray::from(vec![true])),
                rates(vec![Some(0.5)])
            )
            .unwrap_err(),
        "unsupported percentile_disc/cont input scalar Bool(true)"
    );
    let mut invalidrate = Legacy::new("percentile_disc", DataType::Int64);
    assert_eq!(
        invalidrate
            .update(ints(vec![None]), Arc::new(BooleanArray::from(vec![true])))
            .unwrap_err(),
        "percentile_disc_cont_update: unsupported numeric input type Boolean"
    );
    assert_eq!(invalidrate.len(), 0);
}

#[test]
fn original_percentile_parent_struct_null_is_not_a_child_mask_and_binary_like_merge() {
    use arrow::array::{LargeBinaryArray, LargeStringArray};
    use arrow_buffer::NullBuffer;
    let mut state = Legacy::new("percentile_cont", DataType::Float64);
    let values = rates(vec![Some(3.0)]);
    let rs = rates(vec![Some(0.5)]);
    let input: ArrayRef = Arc::new(StructArray::new(
        Fields::from(vec![
            Field::new("v", DataType::Float64, true),
            Field::new("r", DataType::Float64, true),
        ]),
        vec![values, rs],
        Some(NullBuffer::from(vec![false])),
    ));
    let pointer = state.pointer();
    PercentilePlaceholderAgg
        .update_batch(&state.spec, 0, &[pointer], &AggInputView::Any(&input))
        .unwrap();
    assert_eq!(f64_output(&mut state), Some(3.0));
    let encoded = state.output(true).unwrap();
    let bytes = encoded
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap()
        .value(0);
    let mut merged = Legacy::new("percentile_cont", DataType::Float64);
    merged
        .merge(Arc::new(LargeBinaryArray::from(vec![Some(bytes)])))
        .unwrap();
    assert_eq!(f64_output(&mut merged), Some(3.0));
    for empty in [
        Arc::new(StringArray::from(vec![""])) as ArrayRef,
        Arc::new(LargeStringArray::from(vec![""])),
    ] {
        merged.merge(empty).unwrap();
    }
    assert_eq!(merged.len(), 1);
}
#[test]
fn original_percentile_original_scalar_rate_carriers_and_missing_tracker_type_signature() {
    use arrow::array::{Float32Array, Int8Array, Int16Array, Int32Array};
    for rate in [
        Arc::new(Int8Array::from(vec![0])) as ArrayRef,
        Arc::new(Int16Array::from(vec![0])),
        Arc::new(Int32Array::from(vec![0])),
        ints(vec![Some(0)]),
        Arc::new(Float32Array::from(vec![0.5])),
        rates(vec![Some(0.5)]),
        Arc::new(
            Decimal128Array::from(vec![50])
                .with_precision_and_scale(38, 2)
                .unwrap(),
        ),
        novarocks_types::largeint::array_from_i128(&[Some(0)]).unwrap(),
    ] {
        let mut state = Legacy::new("percentile_cont", DataType::Float64);
        state.update(ints(vec![Some(3)]), rate).unwrap();
        assert_eq!(f64_output(&mut state), Some(3.0));
    }
    assert_eq!(
        PercentilePlaceholderAgg
            .build_spec_from_type(
                &AggFunction {
                    name: "percentile_cont".into(),
                    ..Default::default()
                },
                None,
                false
            )
            .err()
            .unwrap(),
        "aggregate type signature is required"
    );
    let mut state = Legacy::new("percentile_disc_lc|original-suffix", DataType::Int64);
    assert!(matches!(state.spec.kind, AggKind::PercentileDiscLc));
    let mut unused = MaybeUninit::<ExactPercentileState>::uninit();
    assert_eq!(
        PercentilePlaceholderAgg
            .init_state_with_tracker(&state.spec, unused.as_mut_ptr().cast(), None)
            .unwrap_err(),
        "allocation-tracked exact percentile requires an aggregate memory tracker"
    );
    assert_eq!(
        PercentilePlaceholderAgg
            .build_input_view(&state.spec, &None)
            .err()
            .unwrap(),
        "percentile_disc/cont input missing"
    );
    assert_eq!(
        PercentilePlaceholderAgg
            .build_merge_view(&state.spec, &None)
            .err()
            .unwrap(),
        "percentile_disc/cont merge input missing"
    );
    state
        .update(ints(vec![Some(7)]), rates(vec![Some(0.5)]))
        .unwrap();
}
