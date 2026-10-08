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

pub(super) struct VarStdAgg;

fn kind_from_name(name: &str) -> Option<AggKind> {
    match name {
        "variance" | "variance_pop" | "var_pop" => Some(AggKind::VariancePop),
        "variance_samp" | "var_samp" => Some(AggKind::VarianceSamp),
        "stddev" | "std" | "stddev_pop" => Some(AggKind::StddevPop),
        "stddev_samp" => Some(AggKind::StddevSamp),
        _ => None,
    }
}

fn dev_from_ave_spec_from_input_type(name: &str, data_type: &DataType) -> Result<AggSpec, String> {
    let kind = kind_from_name(name).ok_or_else(|| format!("unsupported agg function: {name}"))?;
    match data_type {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64 => Ok(AggSpec {
            kind,
            output_type: DataType::Float64,
            intermediate_type: DataType::Binary,
            input_arg_type: None,
            count_all: false,
        }),
        other => Err(format!("{} unsupported input type: {:?}", name, other)),
    }
}

fn dev_from_ave_spec_from_intermediate_type(
    func: &AggFunction,
    name: &str,
    data_type: &DataType,
) -> Result<AggSpec, String> {
    let kind = kind_from_name(name).ok_or_else(|| format!("unsupported agg function: {name}"))?;

    match data_type {
        DataType::Binary | DataType::Utf8 => {
            // When the intermediate is opaque, rely on FE signatures.
            let sig = agg_type_signature(func)
                .ok_or_else(|| format!("{name} intermediate type signature missing"))?;
            let output_type = sig
                .output_type
                .as_ref()
                .ok_or_else(|| format!("{name} intermediate output_type signature missing"))?;
            if !matches!(output_type, DataType::Float64) {
                return Err(format!(
                    "{name} intermediate output type unsupported: {:?}",
                    output_type
                ));
            }
            Ok(AggSpec {
                kind,
                output_type: DataType::Float64,
                intermediate_type: data_type.clone(),
                input_arg_type: sig.input_arg_type.clone(),
                count_all: false,
            })
        }
        other => Err(format!(
            "{name} intermediate unsupported input type: {:?}",
            other
        )),
    }
}

impl AggregateFunction for VarStdAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let name = func.name.as_str();
        if input_is_intermediate {
            let data_type =
                input_type.ok_or_else(|| format!("{name} intermediate input type missing"))?;
            dev_from_ave_spec_from_intermediate_type(func, name, data_type)
        } else {
            let data_type = input_type.ok_or_else(|| format!("{name} input type missing"))?;
            dev_from_ave_spec_from_input_type(name, data_type)
        }
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::VariancePop
            | AggKind::VarianceSamp
            | AggKind::StddevPop
            | AggKind::StddevSamp => (
                std::mem::size_of::<DevFromAveState>(),
                std::mem::align_of::<DevFromAveState>(),
            ),
            other => unreachable!("unexpected kind for variance/stddev: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "variance/stddev input missing".to_string())?;
        match arr.data_type() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                Ok(AggInputView::Int(IntArrayView::new(arr)?))
            }
            DataType::Float32 | DataType::Float64 => {
                Ok(AggInputView::Float(FloatArrayView::new(arr)?))
            }
            other => Err(format!(
                "variance/stddev input type mismatch: {:?} for {:?}",
                other, spec.kind
            )),
        }
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "variance/stddev merge input missing".to_string())?;
        match arr.data_type() {
            DataType::Binary => arr
                .as_any()
                .downcast_ref::<BinaryArray>()
                .map(AggInputView::Binary)
                .ok_or_else(|| "failed to downcast to BinaryArray".to_string()),
            DataType::Utf8 => Ok(AggInputView::Utf8(Utf8ArrayView::new(arr)?)),
            other => Err(format!(
                "variance/stddev merge input type mismatch: {:?}",
                other
            )),
        }
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe { std::ptr::write(ptr as *mut DevFromAveState, DevFromAveState::default()) }
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
        if !matches!(input, AggInputView::Int(_) | AggInputView::Float(_)) {
            return Err("variance/stddev update input type mismatch".to_owned());
        }
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
            return Err("variance/stddev merge input type mismatch".to_owned());
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
        if output_intermediate
            && !matches!(spec.intermediate_type, DataType::Binary | DataType::Utf8)
        {
            return Err(format!(
                "variance/stddev intermediate output type unsupported: {:?}",
                spec.intermediate_type
            ));
        }
        super::aggregate_basic_adapter::build(spec, offset, group_states, output_intermediate)
    }
}
