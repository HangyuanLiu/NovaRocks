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
use arrow::array::{ArrayRef, Decimal256Array, FixedSizeBinaryArray};
use arrow::datatypes::DataType;
use arrow_buffer::i256;

use crate::exec::node::aggregate::AggFunction;
use novarocks_types::largeint;

use super::super::*;
use super::AggregateFunction;

#[derive(Debug)]
enum SumFailure {
    ExactStateOverflow,
    DecimalOverflow,
    BigIntResult(std::num::TryFromIntError),
    LargeIntResult,
    Decimal38Result,
    Arrow(arrow::error::ArrowError),
    LargeIntWidth {
        actual: usize,
        source: std::array::TryFromSliceError,
    },
}
impl std::fmt::Display for SumFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ExactStateOverflow => f.write_str("sum exact state overflow"),
            Self::DecimalOverflow => f.write_str("decimal overflow"),
            Self::BigIntResult(_) => f.write_str("sum result overflows BIGINT"),
            Self::LargeIntResult => f.write_str("sum result overflows LARGEINT"),
            Self::Decimal38Result => f.write_str("sum result overflows DECIMAL(38)"),
            Self::Arrow(source) => source.fmt(f),
            Self::LargeIntWidth { actual, .. } => write!(
                f,
                "sum LARGEINT value width differs: expected 16, got {actual}"
            ),
        }
    }
}
impl std::error::Error for SumFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Arrow(source) => Some(source),
            Self::BigIntResult(source) => Some(source),
            Self::LargeIntWidth { source, .. } => Some(source),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for SumFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::fragment::ExecutionReason;
        use novarocks_failure_contract::{FailureClass, FailureFact};
        let (class, reason) = match self {
            Self::ExactStateOverflow
            | Self::DecimalOverflow
            | Self::BigIntResult(_)
            | Self::LargeIntResult
            | Self::Decimal38Result => {
                return novarocks_functions::DataFailureReason::NumericOutOfRange.failure_fact();
            }
            Self::Arrow(arrow::error::ArrowError::MemoryError(_)) => {
                (FailureClass::Resource, ExecutionReason::SumStateAllocation)
            }
            // These builders use fixed primitive state metadata, with no I/O
            // or data-dependent offset carrier.
            Self::Arrow(_) => (
                FailureClass::Invariant,
                ExecutionReason::SumStateConstruction,
            ),
            Self::LargeIntWidth { .. } => {
                (FailureClass::Invariant, ExecutionReason::SumLargeIntWidth)
            }
        };
        FailureFact::new(class, reason.reason())
    }
}
impl From<SumFailure> for crate::runtime::fragment::ExecutionFailure {
    fn from(source: SumFailure) -> Self {
        Self::from_domain(source)
    }
}

/// SUM with the exact contract the pure owner implements (R-SUM-exact).
///
/// A group's value is the exact sum of its non-NULL inputs: integers
/// accumulate in i128 and travel between phases as `Decimal128(38, 0)`;
/// LARGEINT and DECIMAL accumulate in i256 and travel as `Decimal256(76, s)`.
/// Only building the final value can overflow: BIGINT and LARGEINT report an
/// error and DECIMAL reports one past 38 digits. This executor has no frozen
/// decimal overflow policy, so it always reports. FLOAT/DOUBLE and DECIMAL256
/// keep their previous accumulation.
pub(super) struct SumAgg;

/// Largest DECIMAL(38, s) magnitude, 10^38 - 1, as an unscaled value.
const MAX_DECIMAL38: i128 = 99_999_999_999_999_999_999_999_999_999_999_999_999;

fn sum_spec_from_type(data_type: &DataType) -> crate::runtime::fragment::ExecutionResult<AggSpec> {
    match data_type {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64 => Ok(AggSpec {
            kind: AggKind::SumInt,
            output_type: DataType::Int64,
            intermediate_type: DataType::Decimal128(38, 0),
            input_arg_type: None,
            count_all: false,
        }),
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            Ok(AggSpec {
                kind: AggKind::SumLargeInt,
                output_type: DataType::FixedSizeBinary(*width),
                intermediate_type: DataType::Decimal256(76, 0),
                input_arg_type: None,
                count_all: false,
            })
        }
        DataType::Float32 | DataType::Float64 => Ok(AggSpec {
            kind: AggKind::SumFloat,
            output_type: DataType::Float64,
            intermediate_type: DataType::Float64,
            input_arg_type: None,
            count_all: false,
        }),
        DataType::Decimal128(..) => {
            // Canonical sum decimal output is Decimal128(38, scale) (P2 single
            // source of truth), matching the analyzer/codegen so the same slot
            // carries an identical descriptor fleet-wide.
            let canonical = novarocks_type_contract::canonical_agg_decimal_type("sum", data_type)
                .expect("sum decimal canonical type");
            let DataType::Decimal128(_, scale) = canonical else {
                return Err(
                    crate::exec::expr::agg::functions::aggregate_binding_contract(format_args!(
                        "sum canonical decimal is not 128-bit: {canonical:?}"
                    )),
                );
            };
            Ok(AggSpec {
                kind: AggKind::SumDecimal128,
                output_type: canonical,
                intermediate_type: DataType::Decimal256(76, scale),
                input_arg_type: None,
                count_all: false,
            })
        }
        DataType::Decimal256(precision, scale) => Ok(AggSpec {
            kind: AggKind::SumDecimal256,
            output_type: DataType::Decimal256(*precision, *scale),
            intermediate_type: DataType::Decimal256(*precision, *scale),
            input_arg_type: None,
            count_all: false,
        }),
        other => Err(
            crate::exec::expr::agg::functions::aggregate_binding_contract(format_args!(
                "sum unsupported input type: {:?}",
                other
            )),
        ),
    }
}

impl AggregateFunction for SumAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> crate::runtime::fragment::ExecutionResult<AggSpec> {
        if !input_is_intermediate {
            let data_type = input_type.ok_or_else(|| {
                crate::exec::expr::agg::functions::aggregate_binding_contract(
                    "sum input type missing",
                )
            })?;
            return sum_spec_from_type(data_type);
        }
        // A merge reads the wider intermediate; its domain is the logical
        // argument the selection names, never the state carrier. The merge
        // view checks each actual state array against the intermediate.
        let logical = func
            .types
            .as_ref()
            .and_then(|types| types.input_arg_type.as_ref())
            .ok_or_else(|| {
                crate::exec::expr::agg::functions::aggregate_binding_contract(
                    "sum merge requires its logical argument type",
                )
            })?;
        sum_spec_from_type(logical)
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::SumInt => (
                std::mem::size_of::<SumIntState>(),
                std::mem::align_of::<SumIntState>(),
            ),
            AggKind::SumFloat => (
                std::mem::size_of::<SumFloatState>(),
                std::mem::align_of::<SumFloatState>(),
            ),
            AggKind::SumLargeInt | AggKind::SumDecimal128 => (
                std::mem::size_of::<SumDecimal128State>(),
                std::mem::align_of::<SumDecimal128State>(),
            ),
            AggKind::SumDecimal256 => (
                std::mem::size_of::<SumDecimal256State>(),
                std::mem::align_of::<SumDecimal256State>(),
            ),
            other => unreachable!("unexpected kind for sum: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> crate::runtime::fragment::ExecutionResult<AggInputView<'a>> {
        match spec.kind {
            AggKind::SumInt => {
                let arr = array.as_ref().ok_or_else(|| {
                    crate::exec::expr::agg::views::aggregate_input_contract("int input missing")
                })?;
                if arr.data_type() == &DataType::Boolean {
                    let arr = arr
                        .as_any()
                        .downcast_ref::<arrow::array::BooleanArray>()
                        .ok_or_else(|| {
                            crate::exec::expr::agg::views::aggregate_input_contract(
                                "failed to downcast to BooleanArray",
                            )
                        })?;
                    Ok(AggInputView::Bool(arr))
                } else {
                    Ok(AggInputView::Int(IntArrayView::new(arr)?))
                }
            }
            AggKind::SumLargeInt => {
                let arr = array.as_ref().ok_or_else(|| {
                    crate::exec::expr::agg::views::aggregate_input_contract(
                        "largeint input missing",
                    )
                })?;
                Ok(AggInputView::Any(arr))
            }
            AggKind::SumFloat => {
                let arr = array.as_ref().ok_or_else(|| {
                    crate::exec::expr::agg::views::aggregate_input_contract("float input missing")
                })?;
                Ok(AggInputView::Float(FloatArrayView::new(arr)?))
            }
            AggKind::SumDecimal128 => {
                let arr = array.as_ref().ok_or_else(|| {
                    crate::exec::expr::agg::views::aggregate_input_contract("utf8 input missing")
                })?;
                Ok(AggInputView::Utf8(Utf8ArrayView::new(arr)?))
            }
            AggKind::SumDecimal256 => {
                let arr = array.as_ref().ok_or_else(|| {
                    crate::exec::expr::agg::views::aggregate_input_contract(
                        "decimal256 input missing",
                    )
                })?;
                Ok(AggInputView::Any(arr))
            }
            _ => Err(crate::exec::expr::agg::views::aggregate_input_contract(
                "sum input type mismatch",
            )),
        }
    }

    fn build_merge_view<'a>(
        &self,
        spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> crate::runtime::fragment::ExecutionResult<AggInputView<'a>> {
        match spec.kind {
            // The exact states travel in their wider decimal carriers.
            AggKind::SumInt | AggKind::SumLargeInt | AggKind::SumDecimal128 => {
                let arr = array.as_ref().ok_or_else(|| {
                    crate::exec::expr::agg::views::aggregate_input_contract(
                        "sum state input missing",
                    )
                })?;
                if arr.data_type() != &spec.intermediate_type {
                    return Err(crate::exec::expr::agg::views::aggregate_input_contract(
                        crate::exec::expr::agg::views::aggregate_input_contract(format_args!(
                            "sum state input {:?} differs from its intermediate {:?}",
                            arr.data_type(),
                            spec.intermediate_type
                        )),
                    ));
                }
                Ok(AggInputView::Any(arr))
            }
            _ => self.build_input_view(spec, array),
        }
    }

    fn init_state(&self, spec: &AggSpec, ptr: *mut u8) {
        match spec.kind {
            AggKind::SumInt => unsafe {
                std::ptr::write(
                    ptr as *mut SumIntState,
                    SumIntState {
                        sum: 0,
                        has_value: false,
                    },
                );
            },
            AggKind::SumFloat => unsafe {
                std::ptr::write(
                    ptr as *mut SumFloatState,
                    SumFloatState {
                        sum: 0.0,
                        has_value: false,
                    },
                );
            },
            AggKind::SumLargeInt | AggKind::SumDecimal128 => unsafe {
                std::ptr::write(
                    ptr as *mut SumDecimal128State,
                    SumDecimal128State {
                        sum: i256::ZERO,
                        has_value: false,
                    },
                );
            },
            AggKind::SumDecimal256 => unsafe {
                std::ptr::write(
                    ptr as *mut SumDecimal256State,
                    SumDecimal256State {
                        sum: i256::ZERO,
                        has_value: false,
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
    ) -> crate::runtime::fragment::ExecutionResult<()> {
        match spec.kind {
            AggKind::SumInt => update_sum_int(offset, state_ptrs, input),
            AggKind::SumLargeInt => update_sum_largeint(offset, state_ptrs, input),
            AggKind::SumFloat => update_sum_float(offset, state_ptrs, input),
            AggKind::SumDecimal128 => update_sum_decimal128(offset, state_ptrs, input),
            AggKind::SumDecimal256 => {
                add_decimal256::<SumDecimal256State>(offset, state_ptrs, input)
            }
            _ => Err(crate::exec::expr::agg::views::aggregate_input_contract(
                "sum update kind mismatch",
            )),
        }
    }

    fn merge_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> crate::runtime::fragment::ExecutionResult<()> {
        match spec.kind {
            AggKind::SumInt => merge_sum_int(offset, state_ptrs, input),
            // LARGEINT and DECIMAL states share the i256 state and carrier.
            AggKind::SumLargeInt | AggKind::SumDecimal128 => {
                add_decimal256::<SumDecimal128State>(offset, state_ptrs, input)
            }
            _ => self.update_batch(spec, offset, state_ptrs, input),
        }
    }

    fn build_array(
        &self,
        spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        output_intermediate: bool,
    ) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
        match (spec.kind.clone(), output_intermediate) {
            (AggKind::SumInt, true) => build_sum_int_state_array(offset, group_states),
            (AggKind::SumInt, false) => build_sum_int_array(offset, group_states),
            (AggKind::SumLargeInt | AggKind::SumDecimal128, true) => {
                build_decimal256_array::<SumDecimal128State>(
                    offset,
                    group_states,
                    &spec.intermediate_type,
                )
            }
            (AggKind::SumLargeInt, false) => build_sum_largeint_array(offset, group_states),
            (AggKind::SumFloat, _) => build_sum_float_array(offset, group_states),
            (AggKind::SumDecimal128, false) => {
                build_sum_decimal128_array(offset, group_states, &spec.output_type)
            }
            (AggKind::SumDecimal256, _) => build_decimal256_array::<SumDecimal256State>(
                offset,
                group_states,
                &spec.output_type,
            ),
            _ => Err(crate::exec::expr::agg::views::aggregate_input_contract(
                "sum output kind mismatch",
            )),
        }
    }
}

fn add_exact(
    state: &mut SumIntState,
    value: i128,
) -> crate::runtime::fragment::ExecutionResult<()> {
    state.sum = state.sum.checked_add(value).ok_or_else(|| {
        crate::runtime::fragment::ExecutionFailure::from_domain(SumFailure::ExactStateOverflow)
    })?;
    state.has_value = true;
    Ok(())
}

fn update_sum_int(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> crate::runtime::fragment::ExecutionResult<()> {
    match input {
        AggInputView::Int(view) => {
            for (row, &base) in state_ptrs.iter().enumerate() {
                if let Some(v) = view.value_at(row) {
                    let state =
                        unsafe { &mut *((base as *mut u8).add(offset) as *mut SumIntState) };
                    add_exact(state, i128::from(v))?;
                }
            }
            Ok(())
        }
        AggInputView::Bool(arr) => {
            for (row, &base) in state_ptrs.iter().enumerate() {
                if arr.is_null(row) {
                    continue;
                }
                let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut SumIntState) };
                add_exact(state, i128::from(arr.value(row)))?;
            }
            Ok(())
        }
        _ => Err(crate::exec::expr::agg::views::aggregate_input_contract(
            "sum int input type mismatch",
        )),
    }
}

fn merge_sum_int(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> crate::runtime::fragment::ExecutionResult<()> {
    let AggInputView::Any(array) = input else {
        return Err(crate::exec::expr::agg::views::aggregate_input_contract(
            "sum int state input type mismatch",
        ));
    };
    let arr = array
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| {
            crate::exec::expr::agg::views::aggregate_input_contract(
                "sum int state input downcast failed",
            )
        })?;
    for (row, &base) in state_ptrs.iter().enumerate() {
        if arr.is_null(row) {
            continue;
        }
        let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut SumIntState) };
        add_exact(state, arr.value(row))?;
    }
    Ok(())
}

fn update_sum_float(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> crate::runtime::fragment::ExecutionResult<()> {
    match input {
        AggInputView::Float(view) => {
            for (row, &base) in state_ptrs.iter().enumerate() {
                if let Some(v) = view.value_at(row) {
                    let state =
                        unsafe { &mut *((base as *mut u8).add(offset) as *mut SumFloatState) };
                    state.sum += v;
                    state.has_value = true;
                }
            }
            Ok(())
        }
        _ => Err(crate::exec::expr::agg::views::aggregate_input_contract(
            "sum float input type mismatch",
        )),
    }
}

fn add_wide(
    state: &mut SumDecimal128State,
    value: i256,
) -> crate::runtime::fragment::ExecutionResult<()> {
    state.sum = state.sum.checked_add(value).ok_or_else(|| {
        crate::runtime::fragment::ExecutionFailure::from_domain(SumFailure::ExactStateOverflow)
    })?;
    state.has_value = true;
    Ok(())
}

fn update_sum_largeint(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> crate::runtime::fragment::ExecutionResult<()> {
    let AggInputView::Any(array) = input else {
        return Err(crate::exec::expr::agg::views::aggregate_input_contract(
            "sum largeint input type mismatch",
        ));
    };
    let arr = array
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| {
            crate::exec::expr::agg::views::aggregate_input_contract(
                "sum largeint input downcast failed",
            )
        })?;
    if arr.value_length() != largeint::LARGEINT_BYTE_WIDTH {
        return Err(crate::exec::expr::agg::views::aggregate_input_contract(
            format_args!(
                "sum largeint input width mismatch: expected {}, got {}",
                largeint::LARGEINT_BYTE_WIDTH,
                arr.value_length()
            ),
        ));
    }
    for (row, &base) in state_ptrs.iter().enumerate() {
        if arr.is_null(row) {
            continue;
        }
        let bytes = arr.value(row);
        let fixed = bytes.try_into().map_err(|source| {
            crate::runtime::fragment::ExecutionFailure::from_domain(SumFailure::LargeIntWidth {
                actual: bytes.len(),
                source,
            })
        })?;
        let value = i128::from_be_bytes(fixed);
        let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut SumDecimal128State) };
        add_wide(state, i256::from_i128(value))?;
    }
    Ok(())
}

fn update_sum_decimal128(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> crate::runtime::fragment::ExecutionResult<()> {
    match input {
        AggInputView::Utf8(Utf8ArrayView::Decimal128(arr, _)) => {
            for (row, &base) in state_ptrs.iter().enumerate() {
                if arr.is_null(row) {
                    continue;
                }
                let state =
                    unsafe { &mut *((base as *mut u8).add(offset) as *mut SumDecimal128State) };
                add_wide(state, i256::from_i128(arr.value(row)))?;
            }
            Ok(())
        }
        AggInputView::Utf8(_) => Err(crate::exec::expr::agg::views::aggregate_input_contract(
            "sum decimal input type mismatch",
        )),
        _ => Err(crate::exec::expr::agg::views::aggregate_input_contract(
            "sum decimal input type mismatch",
        )),
    }
}

/// One i256 sum state: the exact LARGEINT and DECIMAL states, and the
/// DECIMAL256 one.
trait WideSumState {
    fn sum(&self) -> Option<i256>;
    fn add(&mut self, value: i256) -> crate::runtime::fragment::ExecutionResult<()>;
}
impl WideSumState for SumDecimal128State {
    fn sum(&self) -> Option<i256> {
        self.has_value.then_some(self.sum)
    }
    fn add(&mut self, value: i256) -> crate::runtime::fragment::ExecutionResult<()> {
        add_wide(self, value)
    }
}
impl WideSumState for SumDecimal256State {
    fn sum(&self) -> Option<i256> {
        self.has_value.then_some(self.sum)
    }
    fn add(&mut self, value: i256) -> crate::runtime::fragment::ExecutionResult<()> {
        self.sum = self.sum.checked_add(value).ok_or_else(|| {
            crate::runtime::fragment::ExecutionFailure::from_domain(SumFailure::DecimalOverflow)
        })?;
        self.has_value = true;
        Ok(())
    }
}

/// Add Decimal256 values: DECIMAL256 inputs, or the exact LARGEINT and
/// DECIMAL states.
fn add_decimal256<S: WideSumState>(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> crate::runtime::fragment::ExecutionResult<()> {
    let AggInputView::Any(array) = input else {
        return Err(crate::exec::expr::agg::views::aggregate_input_contract(
            "sum decimal256 input type mismatch",
        ));
    };
    let arr = array
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .ok_or_else(|| {
            crate::exec::expr::agg::views::aggregate_input_contract(
                "sum decimal256 input downcast failed",
            )
        })?;
    for (row, &base) in state_ptrs.iter().enumerate() {
        if arr.is_null(row) {
            continue;
        }
        let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut S) };
        state.add(arr.value(row))?;
    }
    Ok(())
}

fn build_sum_int_array(
    offset: usize,
    group_states: &[AggStatePtr],
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    let mut builder = Int64Builder::new();
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const SumIntState) };
        if state.has_value {
            let value = i64::try_from(state.sum).map_err(|source| {
                crate::runtime::fragment::ExecutionFailure::from_domain(SumFailure::BigIntResult(
                    source,
                ))
            })?;
            builder.append_value(value);
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_sum_int_state_array(
    offset: usize,
    group_states: &[AggStatePtr],
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    let mut values = Vec::with_capacity(group_states.len());
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const SumIntState) };
        values.push(state.has_value.then_some(state.sum));
    }
    let array = Decimal128Array::from(values)
        .with_precision_and_scale(38, 0)
        .map_err(|source| {
            crate::runtime::fragment::ExecutionFailure::from_domain(SumFailure::Arrow(source))
        })?;
    Ok(Arc::new(array))
}

fn build_sum_largeint_array(
    offset: usize,
    group_states: &[AggStatePtr],
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    let mut values = Vec::with_capacity(group_states.len());
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const SumDecimal128State) };
        if !state.has_value {
            values.push(None);
            continue;
        }
        let sum = state.sum.to_i128().ok_or_else(|| {
            crate::runtime::fragment::ExecutionFailure::from_domain(SumFailure::LargeIntResult)
        })?;
        values.push(Some(sum));
    }
    // The original fixed-width builder has an infallible observer here.
    // Keeping its generic result exposes no fictitious text failure.
    let output =
        largeint::array_from_i128_observed(&values, &mut |_| Ok::<_, std::convert::Infallible>(()));
    Ok(output.unwrap_or_else(|never| match never {}))
}

fn build_sum_float_array(
    offset: usize,
    group_states: &[AggStatePtr],
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    let mut builder = Float64Builder::new();
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const SumFloatState) };
        if state.has_value {
            builder.append_value(state.sum);
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_sum_decimal128_array(
    offset: usize,
    group_states: &[AggStatePtr],
    output_type: &DataType,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    let (precision, scale) = match output_type {
        DataType::Decimal128(precision, scale) => (*precision, *scale),
        other => {
            return Err(crate::exec::expr::agg::views::aggregate_input_contract(
                format_args!("decimal output type mismatch: {:?}", other),
            ));
        }
    };
    let mut values = Vec::with_capacity(group_states.len());
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const SumDecimal128State) };
        if !state.has_value {
            values.push(None);
            continue;
        }
        let sum = state
            .sum
            .to_i128()
            .filter(|sum| sum.unsigned_abs() <= MAX_DECIMAL38 as u128)
            .ok_or_else(|| {
                crate::runtime::fragment::ExecutionFailure::from_domain(SumFailure::Decimal38Result)
            })?;
        values.push(Some(sum));
    }
    let array = Decimal128Array::from(values)
        .with_precision_and_scale(precision, scale)
        .map_err(|source| {
            crate::runtime::fragment::ExecutionFailure::from_domain(SumFailure::Arrow(source))
        })?;
    Ok(Arc::new(array))
}

fn build_decimal256_array<S: WideSumState>(
    offset: usize,
    group_states: &[AggStatePtr],
    output_type: &DataType,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    let (precision, scale) = match output_type {
        DataType::Decimal256(precision, scale) => (*precision, *scale),
        other => {
            return Err(crate::exec::expr::agg::views::aggregate_input_contract(
                format_args!("decimal256 output type mismatch: {:?}", other),
            ));
        }
    };
    let mut values = Vec::with_capacity(group_states.len());
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const S) };
        values.push(state.sum());
    }
    let array = Decimal256Array::from(values)
        .with_precision_and_scale(precision, scale)
        .map_err(|source| {
            crate::runtime::fragment::ExecutionFailure::from_domain(SumFailure::Arrow(source))
        })?;
    Ok(Arc::new(array))
}

#[cfg(test)]
mod source_failure_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    #[test]
    fn exact_state_overflow_keeps_the_value_and_projects_numeric_data() {
        let mut state = SumIntState {
            sum: i128::MAX,
            has_value: false,
        };
        let failure = add_exact(&mut state, 1).unwrap_err();
        assert_eq!(state.sum, i128::MAX);
        assert!(!state.has_value);
        assert_eq!(
            failure.failure_fact(),
            novarocks_functions::DataFailureReason::NumericOutOfRange.failure_fact()
        );
    }
    #[test]
    fn final_narrowing_keeps_the_integer_source_and_not_a_shape_reason() {
        let state = SumIntState {
            sum: i128::from(i64::MAX) + 1,
            has_value: true,
        };
        let address = &state as *const SumIntState as usize;
        let failure = build_sum_int_array(0, &[address]).unwrap_err();
        assert_eq!(failure.failure_fact().class(), FailureClass::Data);
        assert_eq!(failure.failure_fact().reason().code(), 41);
        let crate::runtime::fragment::ExecutionFailureCause::Domain(cause) = failure.cause() else {
            panic!("expected native SUM source")
        };
        let source = cause.downcast_ref::<SumFailure>().unwrap();
        assert!(
            std::error::Error::source(source)
                .unwrap()
                .is::<std::num::TryFromIntError>()
        );
    }
}
