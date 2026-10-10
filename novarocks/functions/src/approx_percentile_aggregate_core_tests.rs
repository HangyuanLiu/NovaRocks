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
use allocator_api2::alloc::Global;
use arrow_array::{
    BinaryArray, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, StringArray,
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{Field, Fields};
use std::sync::Arc;
fn one(value: Option<f64>) -> ArrayRef {
    Arc::new(Float64Array::from(vec![value]))
}
fn packed(columns: Vec<ArrayRef>) -> StructArray {
    let fields = Fields::from(
        columns
            .iter()
            .enumerate()
            .map(|(i, a)| Field::new(format!("arg-{i}"), a.data_type().clone(), true))
            .collect::<Vec<_>>(),
    );
    StructArray::new(fields, columns, None)
}
#[test]
fn approx_percentile_shared_original_eight_numeric_consumers_keep_singleton_bits() {
    let values: Vec<ArrayRef> = vec![
        Arc::new(Int8Array::from(vec![7])),
        Arc::new(Int16Array::from(vec![7])),
        Arc::new(Int32Array::from(vec![7])),
        Arc::new(Int64Array::from(vec![7])),
        Arc::new(Float32Array::from(vec![7.0])),
        Arc::new(Float64Array::from(vec![7.0])),
        Arc::new(
            Decimal128Array::from(vec![700])
                .with_precision_and_scale(5, 2)
                .unwrap(),
        ),
        crate::largeint::array_from_i128(&[Some(7)]).unwrap(),
    ];
    for value in values {
        for optional_compression in [false, true] {
            let mut args = vec![value.clone(), one(Some(0.5))];
            if optional_compression {
                args.push(one(Some(2048.0)));
            }
            let input = UnweightedInput::try_new(
                &packed(args),
                ApproxPercentileDiagnostic::UnweightedUpdate,
            )
            .unwrap();
            let mut state = percentile::PercentileState::new_in(10000, Global);
            input
                .update_row(&mut state, 0, ApproxPercentileDiagnostic::UnweightedUpdate)
                .unwrap();
            let Some(AggScalarValue::Float64(result)) =
                scalar_output(&state, ScalarOutput::Float64).unwrap()
            else {
                panic!("scalar output")
            };
            assert_eq!(result.to_bits(), 7.0_f64.to_bits());
        }
    }
    let input = UnweightedInput::try_new(
        &packed(vec![one(Some(16777217.0)), one(Some(0.5))]),
        ApproxPercentileDiagnostic::UnweightedUpdate,
    )
    .unwrap();
    let mut state = percentile::PercentileState::new_in(10000, Global);
    input
        .update_row(&mut state, 0, ApproxPercentileDiagnostic::UnweightedUpdate)
        .unwrap();
    let Some(AggScalarValue::Float64(result)) =
        scalar_output(&state, ScalarOutput::Float64).unwrap()
    else {
        panic!("scalar output")
    };
    assert_eq!(result.to_bits(), 16777216.0_f64.to_bits());
}
#[test]
fn approx_percentile_shared_original_weighted_demand_and_full_type_errors() {
    let mut state = percentile::PercentileState::new_in(10000, Global);
    let input = WeightedInput::try_new(
        &packed(vec![one(None), one(Some(-8.0)), one(Some(0.5))]),
        ApproxPercentileDiagnostic::WeightedUpdate,
    )
    .unwrap();
    input
        .update_row(&mut state, 0, ApproxPercentileDiagnostic::WeightedUpdate)
        .unwrap();
    assert_eq!(state.digest.count(), 0.0);
    let input = WeightedInput::try_new(
        &packed(vec![one(Some(f64::NAN)), one(Some(-8.0)), one(Some(0.5))]),
        ApproxPercentileDiagnostic::WeightedUpdate,
    )
    .unwrap();
    assert_eq!(
        input
            .update_row(&mut state, 0, ApproxPercentileDiagnostic::WeightedUpdate)
            .unwrap_err(),
        "percentile_approx_weighted: percentile weight must be non-negative, got -8"
    );
    let input = WeightedInput::try_new(
        &packed(vec![
            one(None),
            Arc::new(BooleanArray::from(vec![true])),
            one(Some(1.25)),
        ]),
        ApproxPercentileDiagnostic::WeightedUpdate,
    )
    .unwrap();
    assert_eq!(
        input
            .update_row(&mut state, 0, ApproxPercentileDiagnostic::WeightedUpdate)
            .unwrap_err(),
        "percentile_approx_weighted: percentile parameter must be between 0 and 1, got 1.25"
    );
    let input = WeightedInput::try_new(
        &packed(vec![
            one(Some(3.0)),
            Arc::new(BooleanArray::from(vec![true])),
            one(Some(0.5)),
        ]),
        ApproxPercentileDiagnostic::WeightedUpdate,
    )
    .unwrap();
    assert_eq!(
        input
            .update_row(&mut state, 0, ApproxPercentileDiagnostic::WeightedUpdate)
            .unwrap_err(),
        "percentile_approx_weighted: unsupported numeric input type Boolean"
    );
}
#[test]
fn approx_percentile_shared_original_four_payload_readers_and_magic_bug() {
    let bytes = [0, 4, 0, 0x10, 0x27, 0, 0, 0, 0, 0, 0];
    let text = std::str::from_utf8(&bytes).unwrap();
    for values in [
        Arc::new(BinaryArray::from(vec![bytes.as_slice()])) as ArrayRef,
        Arc::new(LargeBinaryArray::from(vec![bytes.as_slice()])),
        Arc::new(StringArray::from(vec![text])),
        Arc::new(LargeStringArray::from(vec![text])),
    ] {
        let mut state = percentile::PercentileState::new_in(10000, Global);
        merge_row(
            &mut state,
            &values,
            0,
            ApproxPercentileDiagnostic::UnweightedMerge,
        )
        .unwrap();
        assert_eq!(
            percentile::encode_state(&state),
            vec![0xa2, 4, 0, 0x10, 0x27, 0, 0, 0, 0, 0, 0]
        );
    }
    let dtype = DataType::Struct(Fields::from(vec![Field::new(
        "nested",
        DataType::Int64,
        true,
    )]));
    let values = arrow_array::new_null_array(&dtype, 1);
    let mut state = percentile::PercentileState::new_in(10000, Global);
    assert_eq!(
        merge_row(
            &mut state,
            &values,
            0,
            ApproxPercentileDiagnostic::WeightedMerge
        )
        .unwrap_err(),
        format!(
            "percentile_approx_weighted_merge: unsupported percentile payload type {:?}",
            dtype
        )
    );
}
#[test]
fn approx_percentile_shared_original_list_output_order_and_empty_digest_nan() {
    let field = Arc::new(Field::new("authored-quantile", DataType::Float64, true));
    let q = Arc::new(ListArray::new(
        field,
        OffsetBuffer::new(vec![0, 3].into()),
        Arc::new(Float64Array::from(vec![1.0, 0.0, 0.5])),
        None,
    )) as ArrayRef;
    let input = UnweightedInput::try_new(
        &packed(vec![one(None), q.clone()]),
        ApproxPercentileDiagnostic::UnweightedUpdate,
    )
    .unwrap();
    let mut state = percentile::PercentileState::new_in(10000, Global);
    input
        .update_row(&mut state, 0, ApproxPercentileDiagnostic::UnweightedUpdate)
        .unwrap();
    let Some(AggScalarValue::List(values)) = scalar_output(&state, ScalarOutput::List).unwrap()
    else {
        panic!("list output")
    };
    assert_eq!(values.len(), 3);
    for value in values {
        let Some(AggScalarValue::Float64(value)) = value else {
            panic!("item")
        };
        assert!(value.is_nan());
    }
    let input = UnweightedInput::try_new(
        &packed(vec![one(Some(9.0)), q]),
        ApproxPercentileDiagnostic::UnweightedUpdate,
    )
    .unwrap();
    input
        .update_row(&mut state, 0, ApproxPercentileDiagnostic::UnweightedUpdate)
        .unwrap();
    let Some(AggScalarValue::List(values)) = scalar_output(&state, ScalarOutput::List).unwrap()
    else {
        panic!("list output")
    };
    for value in values {
        let Some(AggScalarValue::Float64(value)) = value else {
            panic!("item")
        };
        assert_eq!(value.to_bits(), 9.0_f64.to_bits());
    }
}
