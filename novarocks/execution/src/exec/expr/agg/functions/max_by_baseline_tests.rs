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
//! Independent unchanged-v1 MAX_BY/MIN_BY baseline, registered as max_by child.
use super::*;
use arrow::array::{Array, Float64Array, Int64Array, StringArray, new_null_array};
use arrow::datatypes::{Field, Fields};
use arrow_buffer::NullBuffer;
use std::mem::MaybeUninit;

struct LegacyState {
    raw: MaybeUninit<MaxMinByState>,
    spec: AggSpec,
}
impl LegacyState {
    fn new(name: &str, value: DataType, key: DataType, tracker: Arc<MemTracker>) -> Self {
        let packed = DataType::Struct(Fields::from(vec![
            Field::new("v", value, true),
            Field::new("k", key, true),
        ]));
        let spec = MaxMinByAgg
            .build_spec_from_type(
                &AggFunction {
                    name: name.into(),
                    ..Default::default()
                },
                Some(&packed),
                false,
            )
            .unwrap();
        let mut state = Self {
            raw: MaybeUninit::uninit(),
            spec,
        };
        MaxMinByAgg
            .init_state_with_tracker(&state.spec, state.raw.as_mut_ptr().cast(), Some(tracker))
            .unwrap();
        state
    }
    fn pointer(&mut self) -> AggStatePtr {
        self.raw.as_mut_ptr() as AggStatePtr
    }
    fn update(&mut self, input: &ArrayRef) -> Result<(), String> {
        let pointers = vec![self.pointer(); input.len()];
        MaxMinByAgg.update_batch(&self.spec, 0, &pointers, &AggInputView::Any(input))
    }
    fn merge(&mut self, input: &ArrayRef) -> Result<(), String> {
        let pointers = vec![self.pointer(); input.len()];
        let array = Some(input.clone());
        let view = MaxMinByAgg.build_merge_view(&self.spec, &array)?;
        MaxMinByAgg.merge_batch(&self.spec, 0, &pointers, &view)
    }
    fn output(&mut self, intermediate: bool) -> ArrayRef {
        let pointer = self.pointer();
        MaxMinByAgg
            .build_array(&self.spec, 0, &[pointer], intermediate)
            .unwrap()
    }
}
impl Drop for LegacyState {
    fn drop(&mut self) {
        MaxMinByAgg.drop_state(&self.spec, self.raw.as_mut_ptr().cast());
    }
}
fn packed(values: ArrayRef, keys: ArrayRef, parent_nulls: Option<NullBuffer>) -> ArrayRef {
    Arc::new(StructArray::new(
        Fields::from(vec![
            Field::new("v", values.data_type().clone(), true),
            Field::new("k", keys.data_type().clone(), true),
        ]),
        vec![values, keys],
        parent_nulls,
    ))
}
fn ints(values: Vec<Option<i64>>, keys: Vec<Option<i64>>) -> ArrayRef {
    packed(
        Arc::new(Int64Array::from(values)),
        Arc::new(Int64Array::from(keys)),
        None,
    )
}
fn strings(values: Vec<&str>, keys: Vec<&str>) -> ArrayRef {
    packed(
        Arc::new(StringArray::from(values)),
        Arc::new(StringArray::from(keys)),
        None,
    )
}
fn int_value(state: &mut LegacyState) -> Option<i64> {
    let out = state.output(false);
    let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
    (!out.is_null(0)).then(|| out.value(0))
}
fn binary(bytes: &[u8]) -> ArrayRef {
    Arc::new(BinaryArray::from(vec![Some(bytes)]))
}
fn int_codec(key: i64, value: Option<i64>) -> Vec<u8> {
    let mut bytes = vec![1, 2];
    bytes.extend_from_slice(&key.to_le_bytes());
    if let Some(value) = value {
        bytes.extend_from_slice(&[1, 2]);
        bytes.extend_from_slice(&value.to_le_bytes());
    } else {
        bytes.push(0);
    }
    bytes
}
const NAMES: [&str; 4] = ["max_by", "min_by", "max_by_v2", "min_by_v2"];
#[test]
fn legacy_by_first_tie_null_key_and_winning_null_value_all_raw_variants() {
    for name in NAMES {
        let is_max = name.starts_with("max");
        let tracker = MemTracker::new_root(format!("legacy-by-tie-{name}"));
        let mut state = LegacyState::new(name, DataType::Int64, DataType::Int64, tracker.clone());
        state
            .update(&ints(
                vec![Some(11), Some(22), Some(33)],
                vec![Some(2), Some(2), None],
            ))
            .unwrap();
        assert_eq!(int_value(&mut state), Some(11));
        let winning = if is_max { 3 } else { 1 };
        state
            .update(&ints(
                vec![None, Some(99)],
                vec![Some(winning), Some(winning)],
            ))
            .unwrap();
        assert_eq!(
            int_value(&mut state),
            None,
            "NULL winning value still owns its key and tie"
        );
        let wire = state.output(true);
        assert_eq!(
            wire.as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0),
            int_codec(winning, None)
        );
        drop(state);
        assert_eq!(tracker.current(), 0);
    }
}
#[test]
fn legacy_by_empty_and_all_null_key_keep_nullable_binding_and_binary_null_state() {
    for name in ["max_by", "min_by"] {
        let mut state = LegacyState::new(
            name,
            DataType::Int64,
            DataType::Int64,
            MemTracker::new_root("legacy-by-empty"),
        );
        assert!(state.output(true).is_null(0));
        assert_eq!(int_value(&mut state), None);
        state
            .update(&ints(vec![Some(1), None], vec![None, None]))
            .unwrap();
        assert!(state.output(true).is_null(0));
        assert_eq!(int_value(&mut state), None);
        use novarocks_functions::{
            EngineFunctionCatalog, FunctionArgument, FunctionBindingRequest, FunctionKind,
            FunctionResultType,
        };
        let catalog = novarocks_functions::builtin::catalogue::builtin_engine_function_catalog();
        let args = [false, true].map(|nullable| FunctionArgument::Value {
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, nullable),
            constant: None,
        });
        let bound = catalog
            .resolve_bound_user(
                name,
                FunctionKind::Aggregate,
                FunctionBindingRequest {
                    arguments: &args,
                    logical_argument_count: 2,
                    expected_result_type: None,
                },
                &crate::exec::expr::pure_differential::HarnessControl,
            )
            .unwrap();
        let FunctionResultType::Scalar(out) = bound.selected.result_type else {
            panic!("scalar")
        };
        assert_eq!(out.data_type, DataType::Int64);
        assert!(out.nullable);
        let state = &bound.selected.aggregate.unwrap().intermediate_type;
        assert_eq!(state.data_type, DataType::Binary);
        assert!(state.nullable);
    }
}
#[test]
fn legacy_by_losing_candidate_allocates_value_before_comparison_and_preserves_prior_on_oom() {
    for name in ["max_by", "min_by"] {
        let tracker = MemTracker::new_root("legacy-by-loser-oom");
        tracker.install_limit_once(8).unwrap();
        let mut state = LegacyState::new(name, DataType::Utf8, DataType::Utf8, tracker.clone());
        state.update(&strings(vec!["aa"], vec!["mm"])).unwrap();
        assert_eq!(tracker.current(), 4);
        let loser = if name == "max_by" { "a" } else { "z" };
        let error = state
            .update(&strings(vec!["012345678"], vec![loser]))
            .unwrap_err();
        assert_eq!(
            error,
            "ResourceExhausted: reserve aggregate byte value: aggregate allocation was rejected by memory tracker legacy-by-loser-oom or the system allocator"
        );
        assert_eq!(tracker.current(), 4);
        let out = state.output(false);
        assert_eq!(
            out.as_any().downcast_ref::<StringArray>().unwrap().value(0),
            "aa"
        );
        drop(state);
        assert_eq!(tracker.current(), 0);
    }
}
#[test]
fn legacy_by_ignores_packed_parent_null_and_reads_non_null_children() {
    for name in ["max_by", "min_by"] {
        let mut state = LegacyState::new(
            name,
            DataType::Int64,
            DataType::Int64,
            MemTracker::new_root("legacy-by-parent-null"),
        );
        let input = packed(
            Arc::new(Int64Array::from(vec![41])),
            Arc::new(Int64Array::from(vec![7])),
            Some(NullBuffer::from(vec![false])),
        );
        assert!(input.is_null(0));
        state.update(&input).unwrap();
        assert_eq!(int_value(&mut state), Some(41));
    }
}
#[test]
fn legacy_by_nan_first_is_admitted_later_comparison_fails_and_signed_zero_ties() {
    let nan = f64::from_bits(0x7ff8_0000_0000_0041);
    for name in ["max_by", "min_by"] {
        let tracker = MemTracker::new_root("legacy-by-nan");
        let mut state = LegacyState::new(name, DataType::Int64, DataType::Float64, tracker.clone());
        state
            .update(&packed(
                Arc::new(Int64Array::from(vec![11])),
                Arc::new(Float64Array::from(vec![nan])),
                None,
            ))
            .unwrap();
        assert_eq!(int_value(&mut state), Some(11));
        let original = state.output(true);
        let error = state
            .update(&packed(
                Arc::new(Int64Array::from(vec![22])),
                Arc::new(Float64Array::from(vec![1.0])),
                None,
            ))
            .unwrap_err();
        assert_eq!(error, "float comparison is not ordered");
        assert_eq!(int_value(&mut state), Some(11));
        let out = state.output(true);
        assert_eq!(out.to_data(), original.to_data());
        let mut zero = LegacyState::new(name, DataType::Int64, DataType::Float64, tracker.clone());
        zero.update(&packed(
            Arc::new(Int64Array::from(vec![31, 32])),
            Arc::new(Float64Array::from(vec![-0.0, 0.0])),
            None,
        ))
        .unwrap();
        assert_eq!(int_value(&mut zero), Some(31));
        let bytes = zero.output(true);
        let bytes = bytes
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0);
        assert_eq!(&bytes[2..10], &(-0.0f64).to_bits().to_le_bytes());
    }
}
#[test]
fn legacy_by_codec_exact_wire_merge_tie_and_complete_malformed_diagnostics() {
    for name in ["max_by", "min_by"] {
        let tracker = MemTracker::new_root("legacy-by-codec");
        let mut state = LegacyState::new(name, DataType::Int64, DataType::Int64, tracker.clone());
        state.merge(&binary(&int_codec(9, Some(51)))).unwrap();
        let wire = state.output(true);
        assert_eq!(
            wire.as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0),
            int_codec(9, Some(51))
        );
        state.merge(&binary(&int_codec(9, Some(52)))).unwrap();
        assert_eq!(int_value(&mut state), Some(51));
        for (bytes, message) in [
            (vec![], "max_by/min_by tracked scalar decode failed"),
            (
                vec![2],
                "max_by/min_by tracked scalar decode failed: invalid null flag",
            ),
            (
                vec![1, 99],
                "max_by/min_by tracked scalar decode failed: unknown tag",
            ),
            (vec![1, 2], "max_by/min_by tracked int64 decode failed"),
            (vec![0], "max_by/min_by merge missing key"),
            (
                {
                    let mut bytes = int_codec(10, Some(53));
                    bytes.push(99);
                    bytes
                },
                "max_by/min_by merge input has trailing bytes",
            ),
        ] {
            assert_eq!(state.merge(&binary(&bytes)).unwrap_err(), message);
            assert_eq!(int_value(&mut state), Some(51));
            assert_eq!(tracker.current(), 0);
        }
        let null: ArrayRef = new_null_array(&DataType::Binary, 1);
        state.merge(&null).unwrap();
        assert_eq!(int_value(&mut state), Some(51));
    }
}
#[test]
fn legacy_by_invalid_utf8_decode_releases_candidate_and_keeps_complete_error() {
    let tracker = MemTracker::new_root("legacy-by-invalid-utf8");
    let mut state = LegacyState::new("max_by", DataType::Utf8, DataType::Utf8, tracker.clone());
    state.update(&strings(vec!["old"], vec!["m"])).unwrap();
    assert_eq!(tracker.current(), 4);
    let mut wire = vec![1, 4];
    wire.extend_from_slice(&1u32.to_le_bytes());
    wire.push(b'z');
    wire.extend_from_slice(&[1, 4]);
    wire.extend_from_slice(&1u32.to_le_bytes());
    wire.push(255);
    assert_eq!(
        state.merge(&binary(&wire)).unwrap_err(),
        "invalid utf-8 sequence of 1 bytes from index 0"
    );
    assert_eq!(tracker.current(), 4);
    let out = state.output(false);
    assert_eq!(
        out.as_any().downcast_ref::<StringArray>().unwrap().value(0),
        "old"
    );
    drop(state);
    assert_eq!(tracker.current(), 0);
}
#[test]
fn legacy_by_codec_boolean_nonzero_flag_and_recursive_null_order_are_preserved() {
    for name in ["max_by", "min_by"] {
        let mut state = LegacyState::new(
            name,
            DataType::Int64,
            DataType::Boolean,
            MemTracker::new_root("legacy-by-bool"),
        );
        let mut wire = vec![1, 1, 255, 1, 2];
        wire.extend_from_slice(&61i64.to_le_bytes());
        state.merge(&binary(&wire)).unwrap();
        assert_eq!(int_value(&mut state), Some(61));
        let out = state.output(true);
        let out = out.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(out.value(0)[2], 1);
        use super::super::common::AggScalarValue as V;
        let list = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let key = build_scalar_array(
            &list,
            vec![
                Some(V::List(vec![None])),
                Some(V::List(vec![Some(V::Int64(1))])),
            ],
        )
        .unwrap();
        let mut state = LegacyState::new(
            name,
            DataType::Int64,
            list,
            MemTracker::new_root("legacy-by-nested-key"),
        );
        state
            .update(&packed(Arc::new(Int64Array::from(vec![71, 72])), key, None))
            .unwrap();
        assert_eq!(
            int_value(&mut state),
            Some(if name == "max_by" { 72 } else { 71 })
        );
    }
}
