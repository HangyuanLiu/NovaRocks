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

//! Immutable original aggregate-entry witnesses, before TDigest extraction.
use super::*;
use crate::exec::node::aggregate::AggTypeSignature;
use arrow::array::{BinaryArray, BooleanArray, Float64Array, Int64Array, NullArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{Field, Fields};
use std::mem::MaybeUninit;
struct Original {
    raw: MaybeUninit<TrackedPercentileState>,
    spec: AggSpec,
}
impl Original {
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
        let spec = PercentileAgg
            .build_spec_from_type(&function, None, false)
            .unwrap();
        let mut this = Self {
            raw: MaybeUninit::uninit(),
            spec,
        };
        PercentileAgg
            .init_state_with_tracker(
                &this.spec,
                this.raw.as_mut_ptr().cast(),
                Some(MemTracker::new_root("original-approx-percentile")),
            )
            .unwrap();
        this
    }
    fn pointer(&mut self) -> AggStatePtr {
        self.raw.as_mut_ptr() as AggStatePtr
    }
    fn update(&mut self, columns: Vec<ArrayRef>) -> Result<(), String> {
        let fields = columns
            .iter()
            .enumerate()
            .map(|(i, a)| Field::new(format!("argument-{i}"), a.data_type().clone(), true))
            .collect::<Vec<_>>();
        let packed = Arc::new(StructArray::new(Fields::from(fields), columns, None)) as ArrayRef;
        self.update_packed(packed)
    }
    fn update_packed(&mut self, packed: ArrayRef) -> Result<(), String> {
        let pointers = vec![self.pointer(); packed.len()];
        PercentileAgg.update_batch(&self.spec, 0, &pointers, &AggInputView::Any(&packed))
    }
    fn merge(&mut self, payload: ArrayRef) -> Result<(), String> {
        let pointers = vec![self.pointer(); payload.len()];
        PercentileAgg.merge_batch(&self.spec, 0, &pointers, &AggInputView::Any(&payload))
    }
    fn output(&mut self, partial: bool) -> Result<ArrayRef, String> {
        let pointer = self.pointer();
        PercentileAgg.build_array(&self.spec, 0, &[pointer], partial)
    }
    fn count(&self) -> f32 {
        unsafe { self.raw.assume_init_ref() }.digest.count()
    }
    fn compression(&self) -> usize {
        unsafe { self.raw.assume_init_ref() }.compression
    }
    fn scalar(&mut self) -> Option<f64> {
        let out = self.output(false).unwrap();
        let out = out.as_any().downcast_ref::<Float64Array>().unwrap();
        (!out.is_null(0)).then(|| out.value(0))
    }
    fn bytes(&mut self) -> Vec<u8> {
        let out = self.output(true).unwrap();
        out.as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0)
            .to_vec()
    }
}
impl Drop for Original {
    fn drop(&mut self) {
        PercentileAgg.drop_state(&self.spec, self.raw.as_mut_ptr().cast());
    }
}
fn floats(values: Vec<Option<f64>>) -> ArrayRef {
    Arc::new(Float64Array::from(values))
}
fn one(value: Option<f64>) -> ArrayRef {
    floats(vec![value])
}
fn bin(payload: &[u8]) -> ArrayRef {
    Arc::new(BinaryArray::from(vec![payload]))
}
fn quantiles(values: Vec<Option<f64>>) -> ArrayRef {
    let len = values.len() as i32;
    Arc::new(ListArray::new(
        Arc::new(Field::new("authored-rate", DataType::Float64, true)),
        OffsetBuffer::new(vec![0, len].into()),
        floats(values),
        None,
    ))
}
fn list_type() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Float64, true)))
}
#[test]
fn original_approx_percentile_all_four_arities_preserve_singleton_f32_value_bits() {
    for (name, weighted, compression) in [
        ("percentile_approx", false, false),
        ("percentile_approx", false, true),
        ("percentile_approx_weighted", true, false),
        ("percentile_approx_weighted", true, true),
    ] {
        let mut state = Original::new(name, DataType::Float64);
        let mut args = vec![one(Some(16777217.0))];
        if weighted {
            args.push(Arc::new(Int64Array::from(vec![3])));
        }
        args.push(one(Some(0.5)));
        if compression {
            args.push(Arc::new(Int64Array::from(vec![2048])));
        }
        state.update(args).unwrap();
        assert_eq!(state.scalar().unwrap().to_bits(), 16777216.0_f64.to_bits());
        assert_eq!(state.count(), if weighted { 3.0 } else { 1.0 });
    }
}
#[test]
fn original_approx_percentile_rate_and_compression_errors_precede_null_value() {
    for (name, weighted) in [
        ("percentile_approx", false),
        ("percentile_approx_weighted", true),
    ] {
        let mut state = Original::new(name, DataType::Float64);
        let mut args = vec![one(None)];
        if weighted {
            args.push(one(Some(-1.0)));
        }
        args.push(one(Some(1.25)));
        args.push(one(Some(f64::NAN)));
        assert_eq!(
            state.update(args).unwrap_err(),
            format!("{name}: percentile parameter must be between 0 and 1, got 1.25")
        );
        let mut args = vec![one(None)];
        if weighted {
            args.push(one(Some(-1.0)));
        }
        args.push(one(Some(0.5)));
        args.push(one(Some(f64::NAN)));
        assert_eq!(
            state.update(args).unwrap_err(),
            "percentile compression must be finite"
        );
        let mut args = vec![one(None)];
        if weighted {
            args.push(one(Some(-1.0)));
        }
        args.push(one(Some(0.5)));
        args.push(one(Some(0.0)));
        assert_eq!(
            state.update(args).unwrap_err(),
            "compression parameter must be positive in percentile_approx_weighted, but got: 0"
        );
    }
}
#[test]
fn original_approx_percentile_weight_null_truncation_and_value_null_mask() {
    for weight in [None, Some(f64::NAN), Some(0.5)] {
        let mut state = Original::new("percentile_approx_weighted", DataType::Float64);
        state
            .update(vec![one(Some(7.0)), one(weight), one(Some(0.5))])
            .unwrap();
        assert_eq!(state.count(), 0.0);
        assert_eq!(state.scalar(), None);
    }
    let mut state = Original::new("percentile_approx_weighted", DataType::Float64);
    state
        .update(vec![one(None), one(Some(-8.0)), one(Some(0.5))])
        .unwrap();
    assert_eq!(state.count(), 0.0);
    assert_eq!(
        state
            .update(vec![one(Some(f64::NAN)), one(Some(-8.0)), one(Some(0.5))])
            .unwrap_err(),
        "percentile_approx_weighted: percentile weight must be non-negative, got -8"
    );
    let mut state = Original::new("percentile_approx_weighted", DataType::Float64);
    state
        .update(vec![
            one(Some(7.0)),
            one(Some(f64::INFINITY)),
            one(Some(0.5)),
        ])
        .unwrap();
    assert_eq!(state.scalar(), Some(7.0));
}
#[test]
fn original_approx_percentile_compression_default_range_rounding_and_late_digest_identity() {
    for (input, expected) in [
        (None, 10000),
        (Some(1.0), 10000),
        (Some(2048.0), 2048),
        (Some(2048.5), 2049),
        (Some(10000.0), 10000),
        (Some(10001.0), 10000),
    ] {
        let mut state = Original::new("percentile_approx", DataType::Float64);
        state
            .update(vec![one(None), one(Some(0.5)), one(input)])
            .unwrap();
        assert_eq!(state.compression(), expected);
        let bytes = state.bytes();
        assert_eq!(
            u32::from_le_bytes(bytes[3..7].try_into().unwrap()) as usize,
            expected
        );
    }
    let mut state = Original::new("percentile_approx", DataType::Float64);
    state.update(vec![one(Some(7.0)), one(Some(0.5))]).unwrap();
    state
        .update(vec![one(None), one(Some(0.5)), one(Some(2048.0))])
        .unwrap();
    let bytes = state.bytes();
    assert_eq!(u32::from_le_bytes(bytes[3..7].try_into().unwrap()), 2048);
    assert_eq!(
        f32::from_le_bytes(bytes[19..23].try_into().unwrap()),
        10000.0
    );
}
#[test]
fn original_approx_percentile_quantile_array_null_empty_count_and_order() {
    for (name, weighted) in [
        ("percentile_approx", false),
        ("percentile_approx_weighted", true),
    ] {
        let mut state = Original::new(name, list_type());
        let mut args = vec![one(Some(9.0))];
        if weighted {
            args.push(one(Some(2.0)));
        }
        args.push(quantiles(vec![]));
        assert_eq!(
            state.update(args).unwrap_err(),
            "percentile array cannot be empty"
        );
        let mut args = vec![one(None)];
        if weighted {
            args.push(one(Some(-1.0)));
        }
        args.push(quantiles(vec![Some(0.5), None]));
        assert_eq!(
            state.update(args).unwrap_err(),
            format!("{name}: percentile array element[1] cannot be null")
        );
        let mut args = vec![one(None)];
        if weighted {
            args.push(one(Some(1.0)));
        }
        args.push(quantiles(vec![Some(0.5); 4097]));
        assert_eq!(
            state.update(args).unwrap_err(),
            format!("{name}: percentile quantile count 4097 exceeds 4096")
        );
        let mut state = Original::new(name, list_type());
        let mut args = vec![one(Some(9.0))];
        if weighted {
            args.push(one(Some(2.0)));
        }
        args.push(quantiles(vec![Some(1.0), Some(0.0), Some(0.5)]));
        state.update(args).unwrap();
        let output = state.output(false).unwrap();
        assert_eq!(output.data_type(), &list_type());
        let output = output.as_any().downcast_ref::<ListArray>().unwrap();
        let values = output.value(0);
        assert_eq!(
            values
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[9.0, 9.0, 9.0]
        );
    }
}
#[test]
fn original_approx_percentile_empty_scalar_and_array_outputs_and_exact_v4_header() {
    for name in ["percentile_approx", "percentile_approx_weighted"] {
        let mut state = Original::new(name, DataType::Float64);
        assert_eq!(state.scalar(), None);
        assert_eq!(
            state.bytes(),
            vec![0xa2, 4, 0, 0x10, 0x27, 0, 0, 0, 0, 0, 0]
        );
    }
    let mut state = Original::new("percentile_approx", list_type());
    state
        .update(vec![one(None), quantiles(vec![Some(0.0), Some(1.0)])])
        .unwrap();
    let out = state.output(false).unwrap();
    let out = out.as_any().downcast_ref::<ListArray>().unwrap();
    assert!(!out.is_null(0));
    let values = out.value(0);
    let values = values.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(values.len(), 2);
    assert!(values.value(0).is_nan() && values.value(1).is_nan());
    let mut state = Original::new("percentile_approx", DataType::Float64);
    state.update(vec![one(None), one(Some(0.5))]).unwrap();
    assert_eq!(
        state.bytes(),
        vec![
            0xa2, 4, 1, 0x10, 0x27, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xe0, 0x3f
        ]
    );
}
#[test]
fn original_approx_percentile_rate_mismatch_keeps_prior_row_mutation() {
    for name in ["percentile_approx", "percentile_approx_weighted"] {
        let mut state = Original::new(name, DataType::Float64);
        let mut args = vec![floats(vec![Some(3.0), Some(8.0)])];
        if name.ends_with("weighted") {
            args.push(floats(vec![Some(1.0), Some(1.0)]));
        }
        args.push(floats(vec![Some(0.5), Some(0.75)]));
        assert_eq!(
            state.update(args).unwrap_err(),
            "percentile quantile mismatch while merging states: existing=0.5 incoming=0.75"
        );
        assert_eq!(state.count(), 1.0);
        assert_eq!(state.scalar(), Some(3.0));
    }
}
#[test]
fn original_approx_percentile_null_struct_parent_and_extra_child_are_ignored() {
    let columns = vec![
        one(Some(4.0)),
        one(Some(0.5)),
        one(Some(2048.0)),
        Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
    ];
    let fields = columns
        .iter()
        .map(|a| Field::new("actual", a.data_type().clone(), true))
        .collect::<Vec<_>>();
    let input = Arc::new(StructArray::new(
        Fields::from(fields),
        columns,
        Some(NullBuffer::from(vec![false])),
    )) as ArrayRef;
    let mut state = Original::new("percentile_approx", DataType::Float64);
    state.update_packed(input).unwrap();
    assert_eq!(state.scalar(), Some(4.0));
}
#[test]
fn original_approx_percentile_bounded_merge_accepts_wrong_magic_but_rejects_v3() {
    for name in ["percentile_approx", "percentile_approx_weighted"] {
        let mut state = Original::new(name, DataType::Float64);
        state
            .merge(bin(&[0, 4, 0, 0x10, 0x27, 0, 0, 0, 0, 0, 0]))
            .unwrap();
        assert_eq!(
            state.bytes(),
            vec![0xa2, 4, 0, 0x10, 0x27, 0, 0, 0, 0, 0, 0]
        );
        assert_eq!(
            state.merge(bin(&[0xa2, 3])).unwrap_err(),
            "bounded percentile aggregate requires state version 4"
        );
        assert_eq!(
            state.merge(bin(&[])).unwrap_err(),
            "bounded percentile aggregate requires state version 4"
        );
        assert_eq!(
            state.merge(bin(&[0xa2, 4])).unwrap_err(),
            "percentile state payload too short"
        );
    }
}
#[test]
fn original_approx_percentile_v4_count_metadata_and_trailing_payload_errors_exact() {
    let mut state = Original::new("percentile_approx", DataType::Float64);
    assert_eq!(
        state
            .merge(bin(&[0xa2, 4, 0, 0x10, 0x27, 0, 0, 1, 0, 0, 0]))
            .unwrap_err(),
        "percentile state quantile payload truncated"
    );
    assert_eq!(
        state
            .merge(bin(&[0xa2, 4, 1, 0x10, 0x27, 0, 0, 0, 0, 0, 0]))
            .unwrap_err(),
        "invalid percentile state quantile metadata: kind=1 count=0"
    );
    let mut bytes = vec![0xa2, 4, 0, 0x10, 0x27, 0, 0, 0, 0, 0, 0];
    bytes.extend_from_slice(&[0]);
    assert_eq!(
        state.merge(bin(&bytes)).unwrap_err(),
        "tdigest compression truncated"
    );
}
#[test]
fn original_approx_percentile_raw_type_error_null_carrier_and_serialized_value_route() {
    let mut state = Original::new("percentile_approx", DataType::Float64);
    assert_eq!(
        state
            .update(vec![
                Arc::new(BooleanArray::from(vec![true])),
                one(Some(0.5))
            ])
            .unwrap_err(),
        "percentile_approx: unsupported numeric input type Boolean"
    );
    assert_eq!(
        state
            .update(vec![one(None), Arc::new(NullArray::new(1))])
            .unwrap_err(),
        "percentile_approx: unsupported numeric input type Null"
    );
    let mut source = Original::new("percentile_approx", DataType::Float64);
    source.update(vec![one(Some(8.0)), one(Some(0.5))]).unwrap();
    let bytes = source.bytes();
    let mut target = Original::new("percentile_approx", DataType::Float64);
    target.update(vec![bin(&bytes), one(Some(0.5))]).unwrap();
    assert_eq!(target.scalar(), Some(8.0));
    let mut weighted = Original::new("percentile_approx_weighted", DataType::Float64);
    assert_eq!(
        weighted
            .update(vec![bin(&bytes), one(Some(1.0)), one(Some(0.5))])
            .unwrap_err(),
        "percentile_approx_weighted: unsupported numeric input type Binary"
    );
}
#[test]
fn original_approx_percentile_nan_is_skipped_and_signed_inf_singletons_preserved() {
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let mut state = Original::new("percentile_approx", DataType::Float64);
        state
            .update(vec![one(Some(value)), one(Some(0.5))])
            .unwrap();
        if value.is_nan() {
            assert_eq!(state.scalar(), None);
        } else {
            assert_eq!(state.scalar().unwrap().to_bits(), value.to_bits());
        }
    }
}

#[cfg(test)]
#[path = "percentile_union_original_baseline_tests.rs"]
mod percentile_union_original_baseline_tests;
