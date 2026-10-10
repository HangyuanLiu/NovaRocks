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
use arrow::array::{ArrayRef, BinaryArray};
use arrow::datatypes::DataType;

use crate::exec::node::aggregate::AggFunction;

use super::super::*;
use super::AggregateFunction;

pub(super) struct CovarCorrAgg;

fn kind_from_name(name: &str) -> Option<AggKind> {
    match name {
        "covar_pop" => Some(AggKind::CovarPop),
        "covar_samp" => Some(AggKind::CovarSamp),
        "corr" => Some(AggKind::Corr),
        _ => None,
    }
}

impl AggregateFunction for CovarCorrAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let name = func.name.as_str();
        let kind = kind_from_name(name)
            .ok_or_else(|| format!("unsupported covar/corr function: {name}"))?;
        let data_type = input_type.ok_or_else(|| format!("{name} input type missing"))?;

        if input_is_intermediate {
            return Ok(AggSpec {
                kind,
                output_type: DataType::Float64,
                intermediate_type: data_type.clone(),
                input_arg_type: None,
                count_all: false,
            });
        }

        match data_type {
            DataType::Struct(fields) => {
                if fields.len() != 2 {
                    return Err(format!("{name} expects 2 arguments"));
                }
                Ok(AggSpec {
                    kind,
                    output_type: DataType::Float64,
                    intermediate_type: DataType::Binary,
                    input_arg_type: None,
                    count_all: false,
                })
            }
            other => Err(format!("{name} expects struct input, got {:?}", other)),
        }
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::CovarPop | AggKind::CovarSamp => (
                std::mem::size_of::<CovarState>(),
                std::mem::align_of::<CovarState>(),
            ),
            AggKind::Corr => (
                std::mem::size_of::<CorrState>(),
                std::mem::align_of::<CorrState>(),
            ),
            other => unreachable!("unexpected kind for covar/corr: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "covar/corr input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "covar/corr merge input missing".to_string())?;
        match arr.data_type() {
            DataType::Binary => {
                let bin = arr
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| "failed to downcast to BinaryArray".to_string())?;
                Ok(AggInputView::Binary(bin))
            }
            DataType::Utf8 => Ok(AggInputView::Utf8(Utf8ArrayView::new(arr)?)),
            other => Err(format!(
                "covar/corr intermediate type mismatch: {:?}",
                other
            )),
        }
    }

    fn init_state(&self, spec: &AggSpec, ptr: *mut u8) {
        match spec.kind {
            AggKind::CovarPop | AggKind::CovarSamp => unsafe {
                std::ptr::write(ptr as *mut CovarState, CovarState::default());
            },
            AggKind::Corr => unsafe {
                std::ptr::write(ptr as *mut CorrState, CorrState::default());
            },
            _ => {}
        }
    }

    fn drop_state(&self, _spec: &AggSpec, _ptr: *mut u8) {}

    fn retained_bytes(&self, _spec: &AggSpec, _ptr: *const u8) -> usize {
        0
    }
    fn retained_memory_policy(&self, _spec: &AggSpec) -> RetainedMemoryPolicy {
        RetainedMemoryPolicy::FixedZero
    }
    fn update_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        super::aggregate_basic_adapter::update(spec, offset, state_ptrs, input, None)
    }

    fn merge_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        if !matches!(input, AggInputView::Binary(_) | AggInputView::Utf8(_)) {
            return Err("covar/corr merge input type mismatch".to_owned());
        }
        super::aggregate_basic_adapter::merge(spec, offset, state_ptrs, input)
    }

    fn build_array(
        &self,
        spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        super::aggregate_basic_adapter::build(spec, offset, group_states, output_intermediate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, StructArray};
    use arrow::datatypes::{DataType, Field, Fields};
    use std::mem::MaybeUninit;

    #[test]
    fn test_covar_spec() {
        let func = AggFunction {
            name: "covar_pop".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(DataType::Binary),
                output_type: Some(DataType::Float64),
                input_arg_type: None,
            }),
            ..Default::default()
        };
        let struct_type = DataType::Struct(
            vec![
                Field::new("x", DataType::Float64, true),
                Field::new("y", DataType::Float64, true),
            ]
            .into(),
        );
        let spec = CovarCorrAgg
            .build_spec_from_type(&func, Some(&struct_type), false)
            .unwrap();
        assert!(matches!(spec.kind, AggKind::CovarPop));
    }

    #[test]
    fn test_covar_pop_and_samp() {
        let values_x = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef;
        let values_y = Arc::new(Float64Array::from(vec![2.0, 4.0, 6.0])) as ArrayRef;
        let fields = vec![
            Field::new("x", DataType::Float64, true),
            Field::new("y", DataType::Float64, true),
        ];
        let struct_type = DataType::Struct(Fields::from(fields.clone()));
        let struct_arr = StructArray::new(fields.into(), vec![values_x, values_y], None);
        let array_ref = Arc::new(struct_arr) as ArrayRef;
        let input = AggInputView::Any(&array_ref);

        for (name, expected) in [("covar_pop", 4.0 / 3.0), ("covar_samp", 2.0)] {
            let func = AggFunction {
                name: name.to_string(),
                inputs: vec![],
                input_is_intermediate: false,
                types: Some(crate::exec::node::aggregate::AggTypeSignature {
                    intermediate_type: Some(DataType::Binary),
                    output_type: Some(DataType::Float64),
                    input_arg_type: None,
                }),
                ..Default::default()
            };
            let spec = CovarCorrAgg
                .build_spec_from_type(&func, Some(&struct_type), false)
                .unwrap();

            let mut state = MaybeUninit::<CovarState>::uninit();
            CovarCorrAgg.init_state(&spec, state.as_mut_ptr() as *mut u8);
            let state_ptr = state.as_mut_ptr() as AggStatePtr;
            let state_ptrs = vec![state_ptr; 3];
            CovarCorrAgg
                .update_batch(&spec, 0, &state_ptrs, &input)
                .unwrap();
            let out = CovarCorrAgg
                .build_array(&spec, 0, &[state_ptr], false)
                .unwrap();
            CovarCorrAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

            let out_arr = out.as_any().downcast_ref::<Float64Array>().unwrap();
            let got = out_arr.value(0);
            assert!((got - expected).abs() < 1e-9, "name={}", name);
        }
    }

    #[test]
    fn test_corr() {
        let values_x = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef;
        let values_y = Arc::new(Float64Array::from(vec![2.0, 4.0, 6.0])) as ArrayRef;
        let fields = vec![
            Field::new("x", DataType::Float64, true),
            Field::new("y", DataType::Float64, true),
        ];
        let struct_type = DataType::Struct(Fields::from(fields.clone()));
        let struct_arr = StructArray::new(fields.into(), vec![values_x, values_y], None);
        let array_ref = Arc::new(struct_arr) as ArrayRef;
        let input = AggInputView::Any(&array_ref);

        let func = AggFunction {
            name: "corr".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(DataType::Binary),
                output_type: Some(DataType::Float64),
                input_arg_type: None,
            }),
            ..Default::default()
        };
        let spec = CovarCorrAgg
            .build_spec_from_type(&func, Some(&struct_type), false)
            .unwrap();

        let mut state = MaybeUninit::<CorrState>::uninit();
        CovarCorrAgg.init_state(&spec, state.as_mut_ptr() as *mut u8);
        let state_ptr = state.as_mut_ptr() as AggStatePtr;
        let state_ptrs = vec![state_ptr; 3];
        CovarCorrAgg
            .update_batch(&spec, 0, &state_ptrs, &input)
            .unwrap();
        let out = CovarCorrAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .unwrap();
        CovarCorrAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

        let out_arr = out.as_any().downcast_ref::<Float64Array>().unwrap();
        let got = out_arr.value(0);
        assert!((got - 1.0).abs() < 1e-9);
    }
}
