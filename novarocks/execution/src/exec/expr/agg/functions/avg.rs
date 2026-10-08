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
use arrow::array::{ArrayRef, BinaryArray, Decimal128Array, Decimal256Array, StructArray};
use arrow::datatypes::DataType;
use arrow_buffer::i256;

use crate::exec::node::aggregate::AggFunction;

use super::super::*;
use super::AggregateFunction;
#[cfg(test)]
use crate::exec::expr::decimal::pow10_i256;

pub(super) struct AvgAgg;

fn avg_intermediate_type() -> DataType {
    // StarRocks commonly uses VARBINARY/VARCHAR; we represent it as Utf8 here.
    DataType::Utf8
}

fn avg_decimal_intermediate_type(_precision: u8, _scale: i8) -> DataType {
    // StarRocks commonly uses VARBINARY/VARCHAR; we represent it as Utf8 here.
    DataType::Utf8
}

fn is_count_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

fn avg_spec_from_input_type(data_type: &DataType) -> Result<AggSpec, String> {
    match data_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => Ok(AggSpec {
            kind: AggKind::AvgInt,
            output_type: DataType::Float64,
            intermediate_type: avg_intermediate_type(),
            input_arg_type: None,
            count_all: false,
        }),
        DataType::Float32 | DataType::Float64 => Ok(AggSpec {
            kind: AggKind::AvgFloat,
            output_type: DataType::Float64,
            intermediate_type: avg_intermediate_type(),
            input_arg_type: None,
            count_all: false,
        }),
        DataType::Decimal128(precision, scale) => Ok(AggSpec {
            kind: AggKind::AvgDecimal128,
            // Canonical avg decimal output is Decimal128(38, division-scale) (P2
            // single source of truth), matching analyzer/codegen. The
            // intermediate stays the sum/count accumulator layout.
            output_type: novarocks_type_contract::canonical_agg_decimal_type("avg", data_type)
                .expect("avg decimal canonical type"),
            intermediate_type: avg_decimal_intermediate_type(*precision, *scale),
            input_arg_type: None,
            count_all: false,
        }),
        DataType::Decimal256(precision, scale) => Ok(AggSpec {
            kind: AggKind::AvgDecimal256,
            output_type: DataType::Decimal256(*precision, *scale),
            intermediate_type: avg_decimal_intermediate_type(*precision, *scale),
            input_arg_type: None,
            count_all: false,
        }),
        other => Err(format!("avg unsupported input type: {:?}", other)),
    }
}

fn avg_spec_from_intermediate_type(
    func: &AggFunction,
    data_type: &DataType,
) -> Result<AggSpec, String> {
    // StarRocks avg intermediate is often a VARBINARY/VARCHAR blob.
    // When the intermediate is opaque (Binary/Utf8), we must rely on FE type signature.
    match data_type {
        DataType::Binary | DataType::Utf8 => {
            let sig = agg_type_signature(func)
                .ok_or_else(|| "avg intermediate type signature missing".to_string())?;
            let output_type = sig
                .output_type
                .as_ref()
                .ok_or_else(|| "avg intermediate output_type signature missing".to_string())?;

            match output_type {
                DataType::Decimal128(p, s) => {
                    let input_arg_type = sig
                        .input_arg_type
                        .clone()
                        .ok_or_else(|| "avg intermediate input_arg_type missing".to_string())?;
                    Ok(AggSpec {
                        kind: AggKind::AvgDecimal128,
                        output_type: DataType::Decimal128(*p, *s),
                        intermediate_type: data_type.clone(),
                        input_arg_type: Some(input_arg_type),
                        count_all: false,
                    })
                }
                DataType::Decimal256(p, s) => {
                    let input_arg_type = sig
                        .input_arg_type
                        .clone()
                        .ok_or_else(|| "avg intermediate input_arg_type missing".to_string())?;
                    Ok(AggSpec {
                        kind: AggKind::AvgDecimal256,
                        output_type: DataType::Decimal256(*p, *s),
                        intermediate_type: data_type.clone(),
                        input_arg_type: Some(input_arg_type),
                        count_all: false,
                    })
                }
                DataType::Float64 => {
                    let arg0 = sig.input_arg_type.as_ref().ok_or_else(|| {
                        "avg intermediate input_arg_type signature is required".to_string()
                    })?;
                    let kind = match arg0 {
                        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                            AggKind::AvgInt
                        }
                        DataType::Float32 | DataType::Float64 => AggKind::AvgFloat,
                        other => {
                            return Err(format!(
                                "avg intermediate input_arg_type unsupported: {:?}",
                                other
                            ));
                        }
                    };
                    Ok(AggSpec {
                        kind,
                        output_type: DataType::Float64,
                        intermediate_type: data_type.clone(),
                        input_arg_type: Some(arg0.clone()),
                        count_all: false,
                    })
                }
                other => Err(format!(
                    "avg intermediate output type unsupported: {:?}",
                    other
                )),
            }
        }
        DataType::Struct(fields) => {
            if fields.len() != 2 {
                return Err("avg intermediate expects 2 fields".to_string());
            }
            let sum_type = fields[0].data_type();
            let count_type = fields[1].data_type();
            if !is_count_type(count_type) {
                return Err(format!(
                    "avg intermediate count type mismatch: {:?}",
                    count_type
                ));
            }
            match sum_type {
                DataType::Float64 | DataType::Float32 => Ok(AggSpec {
                    kind: AggKind::AvgFloat,
                    output_type: DataType::Float64,
                    intermediate_type: data_type.clone(),
                    input_arg_type: None,
                    count_all: false,
                }),
                DataType::Decimal128(precision, scale) => Ok(AggSpec {
                    kind: AggKind::AvgDecimal128,
                    output_type: DataType::Decimal128(*precision, *scale),
                    intermediate_type: data_type.clone(),
                    input_arg_type: None,
                    count_all: false,
                }),
                DataType::Decimal256(precision, scale) => Ok(AggSpec {
                    kind: AggKind::AvgDecimal256,
                    output_type: DataType::Decimal256(*precision, *scale),
                    intermediate_type: data_type.clone(),
                    input_arg_type: None,
                    count_all: false,
                }),
                other => Err(format!(
                    "avg intermediate sum type unsupported: {:?}",
                    other
                )),
            }
        }
        other => Err(format!(
            "avg intermediate unsupported input type: {:?}",
            other
        )),
    }
}

impl AggregateFunction for AvgAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        if input_is_intermediate {
            if let Some(data_type) = input_type {
                avg_spec_from_intermediate_type(func, data_type)
            } else {
                Err("avg intermediate input type missing".to_string())
            }
        } else if let Some(data_type) = input_type {
            avg_spec_from_input_type(data_type)
        } else {
            Err("avg input type missing".to_string())
        }
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::AvgInt | AggKind::AvgFloat => (
                std::mem::size_of::<AvgState>(),
                std::mem::align_of::<AvgState>(),
            ),
            AggKind::AvgDecimal128 => (
                std::mem::size_of::<AvgDecimal128State>(),
                std::mem::align_of::<AvgDecimal128State>(),
            ),
            AggKind::AvgDecimal256 => (
                std::mem::size_of::<AvgDecimal256State>(),
                std::mem::align_of::<AvgDecimal256State>(),
            ),
            other => unreachable!("unexpected kind for avg: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        match spec.kind {
            AggKind::AvgInt => {
                let arr = array
                    .as_ref()
                    .ok_or_else(|| "int input missing".to_string())?;
                Ok(AggInputView::Int(IntArrayView::new(arr)?))
            }
            AggKind::AvgFloat => {
                let arr = array
                    .as_ref()
                    .ok_or_else(|| "float input missing".to_string())?;
                Ok(AggInputView::Float(FloatArrayView::new(arr)?))
            }
            AggKind::AvgDecimal128 => {
                let arr = array
                    .as_ref()
                    .ok_or_else(|| "utf8 input missing".to_string())?;
                Ok(AggInputView::Utf8(Utf8ArrayView::new(arr)?))
            }
            AggKind::AvgDecimal256 => {
                let arr = array
                    .as_ref()
                    .ok_or_else(|| "decimal256 input missing".to_string())?;
                Ok(AggInputView::Any(arr))
            }
            _ => Err("avg input type mismatch".to_string()),
        }
    }

    fn build_merge_view<'a>(
        &self,
        spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        match spec.kind {
            AggKind::AvgInt | AggKind::AvgFloat => {
                let arr = array
                    .as_ref()
                    .ok_or_else(|| "avg input missing".to_string())?;
                if matches!(arr.data_type(), DataType::Binary) {
                    Ok(AggInputView::Binary(
                        arr.as_any()
                            .downcast_ref::<BinaryArray>()
                            .ok_or_else(|| "failed to downcast to BinaryArray".to_string())?,
                    ))
                } else if matches!(arr.data_type(), DataType::Utf8) {
                    Ok(AggInputView::Utf8(Utf8ArrayView::new(arr)?))
                } else {
                    let struct_arr = arr
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .ok_or_else(|| "failed to downcast to StructArray".to_string())?;
                    if struct_arr.num_columns() != 2 {
                        return Err("avg intermediate expects 2 fields".to_string());
                    }
                    let sum = struct_arr.column(0);
                    let count = struct_arr.column(1);
                    let sum_view = FloatArrayView::new(sum)?;
                    let count_view = IntArrayView::new(count)?;
                    Ok(AggInputView::AvgState(AvgStateView {
                        sums: sum_view,
                        counts: count_view,
                    }))
                }
            }
            AggKind::AvgDecimal128 => {
                let arr = array
                    .as_ref()
                    .ok_or_else(|| "avg input missing".to_string())?;
                if matches!(arr.data_type(), DataType::Binary) {
                    Ok(AggInputView::Binary(
                        arr.as_any()
                            .downcast_ref::<BinaryArray>()
                            .ok_or_else(|| "failed to downcast to BinaryArray".to_string())?,
                    ))
                } else if matches!(arr.data_type(), DataType::Utf8) {
                    Ok(AggInputView::Utf8(Utf8ArrayView::new(arr)?))
                } else {
                    let struct_arr = arr
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .ok_or_else(|| "failed to downcast to StructArray".to_string())?;
                    if struct_arr.num_columns() != 2 {
                        return Err("avg decimal intermediate expects 2 fields".to_string());
                    }
                    let sum = struct_arr.column(0);
                    let count = struct_arr.column(1);
                    let sum_view = sum
                        .as_any()
                        .downcast_ref::<Decimal128Array>()
                        .ok_or_else(|| "failed to downcast to Decimal128Array".to_string())?;
                    let count_view = IntArrayView::new(count)?;
                    Ok(AggInputView::AvgDecimalState(AvgDecimalStateView {
                        sums: sum_view,
                        counts: count_view,
                    }))
                }
            }
            AggKind::AvgDecimal256 => {
                let arr = array
                    .as_ref()
                    .ok_or_else(|| "avg input missing".to_string())?;
                if matches!(arr.data_type(), DataType::Binary) {
                    Ok(AggInputView::Binary(
                        arr.as_any()
                            .downcast_ref::<BinaryArray>()
                            .ok_or_else(|| "failed to downcast to BinaryArray".to_string())?,
                    ))
                } else if matches!(arr.data_type(), DataType::Utf8) {
                    Ok(AggInputView::Utf8(Utf8ArrayView::new(arr)?))
                } else {
                    Ok(AggInputView::Any(arr))
                }
            }
            _ => Err("avg merge input type mismatch".to_string()),
        }
    }

    fn init_state(&self, spec: &AggSpec, ptr: *mut u8) {
        match spec.kind {
            AggKind::AvgInt | AggKind::AvgFloat => unsafe {
                std::ptr::write(ptr as *mut AvgState, AvgState { sum: 0.0, count: 0 });
            },
            AggKind::AvgDecimal128 => unsafe {
                std::ptr::write(
                    ptr as *mut AvgDecimal128State,
                    AvgDecimal128State { sum: 0, count: 0 },
                );
            },
            AggKind::AvgDecimal256 => unsafe {
                std::ptr::write(
                    ptr as *mut AvgDecimal256State,
                    AvgDecimal256State {
                        sum: i256::ZERO,
                        count: 0,
                    },
                );
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
        match spec.kind {
            AggKind::AvgInt => {
                if !matches!(input, AggInputView::Int(_)) {
                    return Err("avg int input type mismatch".to_owned());
                }
                super::aggregate_basic_adapter::update(spec, offset, state_ptrs, input, None)
            }
            AggKind::AvgFloat => {
                if !matches!(input, AggInputView::Float(_)) {
                    return Err("avg float input type mismatch".to_owned());
                }
                super::aggregate_basic_adapter::update(spec, offset, state_ptrs, input, None)
            }
            AggKind::AvgDecimal128 => {
                let sum_scale =
                    if matches!(spec.intermediate_type, DataType::Binary | DataType::Utf8) {
                        match spec.input_arg_type.as_ref() {
                            Some(DataType::Decimal128(_, scale)) => *scale,
                            other => {
                                return Err(format!(
                                    "avg decimal arg0 type missing/mismatch: {:?}",
                                    other
                                ));
                            }
                        }
                    } else {
                        avg_decimal_sum_scale(&spec.intermediate_type, &spec.output_type)?
                    };
                if !matches!(input, AggInputView::Utf8(Utf8ArrayView::Decimal128(..))) {
                    return Err("avg decimal input type mismatch".to_owned());
                }
                super::aggregate_basic_adapter::update(
                    spec,
                    offset,
                    state_ptrs,
                    input,
                    Some(sum_scale),
                )
            }
            AggKind::AvgDecimal256 => {
                let sum_scale =
                    if matches!(spec.intermediate_type, DataType::Binary | DataType::Utf8) {
                        match spec.input_arg_type.as_ref() {
                            Some(DataType::Decimal256(_, scale)) => *scale,
                            other => {
                                return Err(format!(
                                    "avg decimal256 arg0 type missing/mismatch: {:?}",
                                    other
                                ));
                            }
                        }
                    } else {
                        avg_decimal_sum_scale(&spec.intermediate_type, &spec.output_type)?
                    };
                let AggInputView::Any(a) = input else {
                    return Err("avg decimal256 input type mismatch".to_owned());
                };
                if !matches!(a.data_type(), DataType::Decimal256(..)) {
                    return Err(format!(
                        "avg decimal256 input type mismatch: {:?}",
                        a.data_type()
                    ));
                }
                super::aggregate_basic_adapter::update(
                    spec,
                    offset,
                    state_ptrs,
                    input,
                    Some(sum_scale),
                )
            }
            _ => Err("avg update kind mismatch".to_string()),
        }
    }

    fn merge_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let valid = match spec.kind {
            AggKind::AvgInt | AggKind::AvgFloat => matches!(
                input,
                AggInputView::AvgState(_) | AggInputView::Binary(_) | AggInputView::Utf8(_)
            ),
            AggKind::AvgDecimal128 => matches!(
                input,
                AggInputView::AvgDecimalState(_) | AggInputView::Binary(_) | AggInputView::Utf8(_)
            ),
            AggKind::AvgDecimal256 => matches!(
                input,
                AggInputView::Any(_) | AggInputView::Binary(_) | AggInputView::Utf8(_)
            ),
            _ => return Err("avg merge kind mismatch".to_owned()),
        };
        if !valid {
            return Err(match spec.kind {
                AggKind::AvgDecimal128 => "avg decimal merge input type mismatch",
                AggKind::AvgDecimal256 => "avg decimal256 merge input type mismatch",
                _ => "avg merge input type mismatch",
            }
            .to_owned());
        }
        if let AggInputView::Utf8(v) = input {
            if !matches!(v, Utf8ArrayView::Utf8(_)) {
                return Err(match spec.kind {
                    AggKind::AvgDecimal128 => "avg decimal intermediate type mismatch",
                    AggKind::AvgDecimal256 => "avg decimal256 intermediate type mismatch",
                    _ => "avg intermediate type mismatch",
                }
                .to_owned());
            }
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
mod decimal256_metadata_tests {
    use super::*;
    use crate::exec::expr::agg::{
        AggStateArena, build_kernel_set, test_builtin_execution_function_set,
    };
    use crate::exec::node::aggregate::AggTypeSignature;
    use novarocks_functions::AggregateInputBatch;

    #[test]
    fn ordinary_decimal256_avg_selected_binding_roundtrips_wide_partial_state() {
        let functions = test_builtin_execution_function_set();
        let input_type = DataType::Decimal256(60, 2);
        let selected = functions
            .catalog()
            .resolve_aggregate_trusted("avg", &[input_type.clone()])
            .unwrap();
        assert_eq!(selected.output_type, input_type);
        assert_eq!(selected.intermediate_type, DataType::Utf8);
        let function = |merge| AggFunction {
            name: "avg".to_string(),
            input_is_intermediate: merge,
            types: Some(AggTypeSignature {
                intermediate_type: Some(DataType::Utf8),
                output_type: Some(input_type.clone()),
                input_arg_type: Some(input_type.clone()),
            }),
            ..Default::default()
        };
        let update = build_kernel_set(
            &functions,
            &[function(false)],
            &[Some(input_type.clone())],
            &[selected.clone()],
        )
        .unwrap();
        let merge = build_kernel_set(
            &functions,
            &[function(true)],
            &[Some(DataType::Utf8)],
            &[selected],
        )
        .unwrap();
        let mut arena = AggStateArena::new(4096);
        let update = &update.entries[0];
        let merge = &merge.entries[0];
        let unit = pow10_i256(50).unwrap();
        assert!(unit.to_i128().is_none());
        let mut partials = Vec::new();
        for coefficients in [
            vec![Some(unit), Some(unit), Some(unit * i256::from_i128(3))],
            vec![
                Some(unit * i256::from_i128(3)),
                Some(unit * i256::from_i128(5)),
                None,
            ],
        ] {
            let values = Arc::new(
                Decimal256Array::from(coefficients)
                    .with_precision_and_scale(60, 2)
                    .unwrap(),
            ) as ArrayRef;
            let local = arena.alloc(update.state.size, update.state_align());
            update.init_state(local).unwrap();
            update
                .update_batch(
                    &vec![local; values.len()],
                    AggregateInputBatch::try_new(Some(&values), values.len()).unwrap(),
                )
                .unwrap();
            partials.push(update.build_array(&[local], true).unwrap());
            update.drop_state(local);
        }
        let root = arena.alloc(merge.state.size, merge.state_align());
        merge.init_state(root).unwrap();
        for partial in partials {
            merge
                .merge_batch(
                    &[root],
                    AggregateInputBatch::try_new(Some(&partial), 1).unwrap(),
                )
                .unwrap();
        }
        let result = merge.build_array(&[root], false).unwrap();
        merge.drop_state(root);
        assert_eq!(result.data_type(), &DataType::Decimal256(60, 2));
        // Ordinary AVG keeps duplicates: (1+1+3+3+5)/5 = 2.6 units.
        assert_eq!(
            result
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .unwrap()
                .value(0),
            pow10_i256(49).unwrap() * i256::from_i128(26)
        );
    }
}
