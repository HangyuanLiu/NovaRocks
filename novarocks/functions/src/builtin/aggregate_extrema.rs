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

//! Exact selected fixed-width extrema with inline state and original four phases.
//! Utf8 retained state and aggregate OVER are not implemented by this attachment.

use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::*;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, PrimitiveArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, types::*,
};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, i256};
use arrow_schema::{DataType, TimeUnit};
use std::{alloc::Layout, cmp::Ordering, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ExtremaOperation {
    Min,
    Max,
}
#[derive(Clone, Copy, Debug)]
pub(super) enum ExtremaValue {
    Bool(bool),
    I64(i64),
    F64(f64),
    I128(i128),
    I256(i256),
}
#[derive(Debug)]
pub(super) struct ExtremaKernel {
    pub(super) contract: Arc<AggregateCallContract>,
    pub(super) operation: ExtremaOperation,
}

pub(super) fn supported_type(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32
            | DataType::Timestamp(..)
            | DataType::FixedSizeBinary(16)
            | DataType::Decimal128(..)
            | DataType::Decimal256(..)
    )
}
fn observed<T>(
    control: &dyn KernelEvaluationControl,
    f: impl FnOnce(&mut EvaluationCheckpoints<'_>) -> Result<T, KernelFailure>,
) -> Result<T, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = f(&mut work);
    if matches!(
        &result,
        Err(KernelFailure::Cancelled
            | KernelFailure::DeadlineExceeded
            | KernelFailure::ResourceExhausted)
    ) {
        return result;
    }
    work.finish()?;
    result
}
fn class(array: &dyn Array) -> bool {
    match array.data_type() {
        DataType::Boolean => array.as_any().is::<BooleanArray>(),
        DataType::Int8 => array.as_any().is::<Int8Array>(),
        DataType::Int16 => array.as_any().is::<Int16Array>(),
        DataType::Int32 => array.as_any().is::<Int32Array>(),
        DataType::Int64 => array.as_any().is::<Int64Array>(),
        DataType::Float32 => array.as_any().is::<Float32Array>(),
        DataType::Float64 => array.as_any().is::<Float64Array>(),
        DataType::Date32 => array.as_any().is::<Date32Array>(),
        DataType::Timestamp(TimeUnit::Second, _) => array.as_any().is::<TimestampSecondArray>(),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            array.as_any().is::<TimestampMillisecondArray>()
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            array.as_any().is::<TimestampMicrosecondArray>()
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            array.as_any().is::<TimestampNanosecondArray>()
        }
        DataType::Decimal128(..) => array.as_any().is::<Decimal128Array>(),
        DataType::Decimal256(..) => array.as_any().is::<Decimal256Array>(),
        DataType::FixedSizeBinary(16) => array.as_any().is::<FixedSizeBinaryArray>(),
        _ => false,
    }
}
fn read(
    array: &dyn Array,
    row: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<ExtremaValue>, KernelFailure> {
    let concrete = class(array);
    work.step()?;
    if !concrete {
        return Err(internal(
            "MIN/MAX selected carrier has a foreign concrete array class",
        ));
    }
    let within = row < array.len();
    work.step()?;
    if !within {
        return Err(internal(
            "MIN/MAX selected address is outside its original carrier",
        ));
    }
    let null = array.is_null(row);
    work.step()?;
    if null {
        return Ok(None);
    }
    macro_rules! primitive {
        ($array:ty, $variant:ident, $convert:expr) => {{
            let array = array
                .as_any()
                .downcast_ref::<$array>()
                .ok_or_else(|| internal("MIN/MAX carrier class changed"))?;
            ExtremaValue::$variant(($convert)(array.value(row)))
        }};
    }
    let value = match array.data_type() {
        DataType::Boolean => primitive!(BooleanArray, Bool, |v| v),
        DataType::Int8 => primitive!(Int8Array, I64, |v| v as i64),
        DataType::Int16 => primitive!(Int16Array, I64, |v| v as i64),
        DataType::Int32 => primitive!(Int32Array, I64, |v| v as i64),
        DataType::Int64 => primitive!(Int64Array, I64, |v| v),
        DataType::Float32 => primitive!(Float32Array, F64, |v| v as f64),
        DataType::Float64 => primitive!(Float64Array, F64, |v| v),
        DataType::Date32 => primitive!(Date32Array, I64, |v| v as i64),
        DataType::Timestamp(TimeUnit::Second, _) => primitive!(TimestampSecondArray, I64, |v| v),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            primitive!(TimestampMillisecondArray, I64, |v| v)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            primitive!(TimestampMicrosecondArray, I64, |v| v)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            primitive!(TimestampNanosecondArray, I64, |v| v)
        }
        DataType::Decimal128(..) => primitive!(Decimal128Array, I128, |v| v),
        DataType::Decimal256(..) => primitive!(Decimal256Array, I256, |v| v),
        DataType::FixedSizeBinary(16) => {
            let array = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| internal("MIN/MAX FSB carrier class changed"))?;
            let bytes: [u8; 16] = array
                .value(row)
                .try_into()
                .map_err(|_| internal("MIN/MAX signed BE value width differs"))?;
            ExtremaValue::I128(i128::from_be_bytes(bytes))
        }
        _ => return Err(internal("MIN/MAX prepared carrier is unsupported")),
    };
    work.step()?;
    Ok(Some(value))
}
fn update(
    state: &mut Option<ExtremaValue>,
    value: Option<ExtremaValue>,
    operation: ExtremaOperation,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    let Some(value) = value else {
        return Ok(());
    };
    let comparison = match (*state, value) {
        (None, _) => None,
        (Some(ExtremaValue::Bool(a)), ExtremaValue::Bool(b)) => Some(b.cmp(&a)),
        (Some(ExtremaValue::I64(a)), ExtremaValue::I64(b)) => Some(b.cmp(&a)),
        (Some(ExtremaValue::F64(a)), ExtremaValue::F64(b)) => Some(b.total_cmp(&a)),
        (Some(ExtremaValue::I128(a)), ExtremaValue::I128(b)) => Some(b.cmp(&a)),
        (Some(ExtremaValue::I256(a)), ExtremaValue::I256(b)) => Some(b.cmp(&a)),
        _ => {
            work.step()?;
            return Err(internal(
                "MIN/MAX state value differs from its selected carrier",
            ));
        }
    };
    let replace = comparison.is_none_or(|order| {
        order
            == match operation {
                ExtremaOperation::Min => Ordering::Less,
                ExtremaOperation::Max => Ordering::Greater,
            }
    });
    work.step()?;
    if replace {
        *state = Some(value);
    }
    Ok(())
}
impl PreparedAggregateKernel for ExtremaKernel {
    type State = Option<ExtremaValue>;
    type PreparedUpdateBatch<'batch> = SelectedAggregateUpdateInput<'batch, 'batch>;
    type PreparedMergeBatch<'batch> = SelectedAggregateMergeInput<'batch, 'batch>;
    fn contract(&self) -> &Arc<AggregateCallContract> {
        &self.contract
    }
    fn memory_policy(&self) -> AggregateStateMemoryPolicy {
        AggregateStateMemoryPolicy::FixedZero
    }
    fn retained_bytes(&self, _: &Self::State) -> usize {
        0
    }
    fn create_state(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        observed(control, |work| {
            work.step()?;
            Ok(None)
        })
    }
    fn prepare_update<'batch>(
        &'batch self,
        input: Self::PreparedUpdateBatch<'batch>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedUpdateBatch<'batch>, KernelFailure> {
        observed(control, |work| {
            let valid = std::ptr::eq(input.contract(), self.contract.as_ref())
                && self.contract.phase().consumes_logical_arguments()
                && input.logical_arguments().len() == 1
                && input.order_arguments().is_empty();
            work.step()?;
            if !valid {
                return Err(invalid(
                    "MIN/MAX update differs from its exact phase or channels",
                ));
            }
            let concrete = class(input.logical_arguments()[0].array().as_ref());
            work.step()?;
            if !concrete {
                return Err(internal(
                    "MIN/MAX update carrier has a foreign concrete array class",
                ));
            }
            Ok(input)
        })
    }
    fn update_row<'batch>(
        &self,
        state: &mut Self::State,
        prepared: &Self::PreparedUpdateBatch<'batch>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        observed(control, |work| {
            let row = prepared.selection().row(ordinal);
            work.step()?;
            let row =
                row.ok_or_else(|| invalid("MIN/MAX selected update ordinal is out of bounds"))?;
            let argument = prepared.logical_arguments()[0];
            let address = argument.value_row(ordinal, row);
            let value = read(argument.array().as_ref(), address, work)?;
            update(state, value, self.operation, work)
        })
    }
    fn prepare_merge<'batch>(
        &'batch self,
        input: Self::PreparedMergeBatch<'batch>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedMergeBatch<'batch>, KernelFailure> {
        observed(control, |work| {
            let valid = std::ptr::eq(input.contract(), self.contract.as_ref())
                && !self.contract.phase().consumes_logical_arguments();
            work.step()?;
            if !valid {
                return Err(invalid("MIN/MAX merge differs from its exact phase"));
            }
            let concrete = class(input.state().array().as_ref());
            work.step()?;
            if !concrete {
                return Err(internal(
                    "MIN/MAX merge carrier has a foreign concrete array class",
                ));
            }
            Ok(input)
        })
    }
    fn merge_row<'batch>(
        &self,
        state: &mut Self::State,
        prepared: &Self::PreparedMergeBatch<'batch>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        observed(control, |work| {
            let row = prepared.selection().row(ordinal);
            work.step()?;
            let row =
                row.ok_or_else(|| invalid("MIN/MAX selected merge ordinal is out of bounds"))?;
            let argument = prepared.state();
            let value = read(
                argument.array().as_ref(),
                argument.value_row(ordinal, row),
                work,
            )?;
            update(state, value, self.operation, work)
        })
    }
    fn build_intermediate<'state, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'state Self::State>,
    {
        build(
            states,
            &self.contract.intermediate_type().data_type,
            control,
        )
    }
    fn build_final<'state, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'state Self::State>,
    {
        build(states, &self.contract.final_type().data_type, control)
    }
}
fn bitmap_bytes(rows: usize) -> Result<usize, KernelFailure> {
    rows.checked_add(7)
        .map(|n| n / 8)
        .ok_or(KernelFailure::ResourceExhausted)
}
fn output_capacity(rows: usize, width: usize) -> Result<usize, KernelFailure> {
    let bitmap = bitmap_bytes(rows)?;
    Layout::array::<u8>(bitmap).map_err(|_| KernelFailure::ResourceExhausted)?;
    Layout::from_size_align(
        rows.checked_mul(width)
            .ok_or(KernelFailure::ResourceExhausted)?,
        width,
    )
    .map_err(|_| KernelFailure::ResourceExhausted)?;
    Ok(bitmap)
}
fn reserve<T>(len: usize, work: &mut EvaluationCheckpoints<'_>) -> Result<Vec<T>, KernelFailure> {
    Layout::array::<T>(len).map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    let mut result = Vec::new();
    result
        .try_reserve_exact(len)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    Ok(result)
}
fn validity(bytes: usize, work: &mut EvaluationCheckpoints<'_>) -> Result<Vec<u8>, KernelFailure> {
    let mut bitmap = reserve(bytes, work)?;
    for _ in 0..bytes {
        bitmap.push(0);
        work.step()?;
    }
    Ok(bitmap)
}
fn nulls(validity: Vec<u8>, rows: usize) -> NullBuffer {
    NullBuffer::new(BooleanBuffer::new(Buffer::from(validity), 0, rows))
}
fn build_primitive<
    'a,
    T: ArrowPrimitiveType,
    I: ExactSizeIterator<Item = &'a Option<ExtremaValue>>,
>(
    states: I,
    ty: &DataType,
    work: &mut EvaluationCheckpoints<'_>,
    convert: impl Fn(ExtremaValue) -> Result<T::Native, KernelFailure>,
) -> Result<ArrayRef, KernelFailure> {
    let rows = states.len();
    let bytes = output_capacity(rows, size_of::<T::Native>())?;
    let mut values = reserve::<T::Native>(rows, work)?;
    let mut valid = validity(bytes, work)?;
    for state in states {
        let within = values.len() < rows;
        work.step()?;
        if !within {
            return Err(internal(
                "MIN/MAX emission iterator exceeds admitted extent",
            ));
        }
        let row = values.len();
        let value = if let Some(value) = state {
            valid[row / 8] |= 1 << (row % 8);
            convert(*value)?
        } else {
            T::Native::default()
        };
        values.push(value);
        work.step()?;
    }
    let exact = values.len() == rows;
    work.step()?;
    if !exact {
        return Err(internal(
            "MIN/MAX emission iterator changed its exact extent",
        ));
    }
    work.flush()?;
    let array = PrimitiveArray::<T>::new(values.into(), Some(nulls(valid, rows)))
        .with_data_type(ty.clone());
    let array = Arc::new(array) as ArrayRef;
    work.flush()?;
    Ok(array)
}
fn build<'a, I: ExactSizeIterator<Item = &'a Option<ExtremaValue>>>(
    states: I,
    ty: &DataType,
    control: &dyn KernelEvaluationControl,
) -> Result<ArrayRef, KernelFailure> {
    observed(control, |work| {
        macro_rules! integer {
            ($t:ty, $native:ty) => {
                build_primitive::<$t, _>(states, ty, work, |v| match v {
                    ExtremaValue::I64(v) => <$native>::try_from(v).map_err(|_| {
                        internal("MIN/MAX inline integer cannot fit exact output width")
                    }),
                    _ => Err(internal("MIN/MAX integer state differs from exact output")),
                })
            };
        }
        macro_rules! float {
            ($t:ty, $native:ty) => {
                build_primitive::<$t, _>(states, ty, work, |v| match v {
                    ExtremaValue::F64(v) => Ok(v as $native),
                    _ => Err(internal("MIN/MAX float state differs from exact output")),
                })
            };
        }
        match ty {
            DataType::Int8 => integer!(Int8Type, i8),
            DataType::Int16 => integer!(Int16Type, i16),
            DataType::Int32 => integer!(Int32Type, i32),
            DataType::Int64 => integer!(Int64Type, i64),
            DataType::Float32 => float!(Float32Type, f32),
            DataType::Float64 => float!(Float64Type, f64),
            DataType::Date32 => integer!(Date32Type, i32),
            DataType::Timestamp(TimeUnit::Second, _) => integer!(TimestampSecondType, i64),
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                integer!(TimestampMillisecondType, i64)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                integer!(TimestampMicrosecondType, i64)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => integer!(TimestampNanosecondType, i64),
            DataType::Decimal128(..) => {
                build_primitive::<Decimal128Type, _>(states, ty, work, |v| match v {
                    ExtremaValue::I128(v) => Ok(v),
                    _ => Err(internal("MIN/MAX decimal state differs from exact output")),
                })
            }
            DataType::Decimal256(..) => {
                build_primitive::<Decimal256Type, _>(states, ty, work, |v| match v {
                    ExtremaValue::I256(v) => Ok(v),
                    _ => Err(internal("MIN/MAX decimal state differs from exact output")),
                })
            }
            DataType::Boolean => build_bits(states, work),
            DataType::FixedSizeBinary(16) => build_largeint(states, work),
            _ => Err(internal("MIN/MAX output carrier is not implemented")),
        }
    })
}
fn build_bits<'a, I: ExactSizeIterator<Item = &'a Option<ExtremaValue>>>(
    states: I,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    let rows = states.len();
    let bytes = bitmap_bytes(rows)?;
    // Admit both independent bitmaps before the first reserve.
    Layout::array::<u8>(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
    let mut values = validity(bytes, work)?;
    let mut valid = validity(bytes, work)?;
    let mut count = 0;
    for state in states {
        let within = count < rows;
        work.step()?;
        if !within {
            return Err(internal(
                "MIN/MAX emission iterator exceeds admitted extent",
            ));
        }
        if let Some(value) = state {
            let ExtremaValue::Bool(value) = value else {
                return Err(internal("MIN/MAX boolean state differs from exact output"));
            };
            valid[count / 8] |= 1 << (count % 8);
            if *value {
                values[count / 8] |= 1 << (count % 8);
            }
        }
        count += 1;
        work.step()?;
    }
    let exact = count == rows;
    work.step()?;
    if !exact {
        return Err(internal(
            "MIN/MAX emission iterator changed its exact extent",
        ));
    }
    work.flush()?;
    let output = Arc::new(BooleanArray::new(
        BooleanBuffer::new(Buffer::from(values), 0, rows),
        Some(nulls(valid, rows)),
    )) as ArrayRef;
    work.flush()?;
    Ok(output)
}
fn build_largeint<'a, I: ExactSizeIterator<Item = &'a Option<ExtremaValue>>>(
    states: I,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    let rows = states.len();
    let bitmap = output_capacity(rows, 16)?;
    let count = rows
        .checked_mul(16)
        .ok_or(KernelFailure::ResourceExhausted)?;
    let mut values = reserve::<u8>(count, work)?;
    let mut valid = validity(bitmap, work)?;
    let mut emitted = 0;
    for state in states {
        let within = emitted < rows;
        work.step()?;
        if !within {
            return Err(internal(
                "MIN/MAX emission iterator exceeds admitted extent",
            ));
        }
        let bytes = if let Some(value) = state {
            let ExtremaValue::I128(value) = value else {
                return Err(internal(
                    "MIN/MAX signed BE state differs from exact output",
                ));
            };
            valid[emitted / 8] |= 1 << (emitted % 8);
            value.to_be_bytes()
        } else {
            [0; 16]
        };
        values.extend_from_slice(&bytes);
        emitted += 1;
        work.step()?;
    }
    let exact = emitted == rows;
    work.step()?;
    if !exact {
        return Err(internal(
            "MIN/MAX emission iterator changed its exact extent",
        ));
    }
    work.flush()?;
    let output = FixedSizeBinaryArray::try_new(16, Buffer::from(values), Some(nulls(valid, rows)))
        .map_err(|_| internal("MIN/MAX signed BE output shape differs"))?;
    let output = Arc::new(output) as ArrayRef;
    work.flush()?;
    Ok(output)
}

#[cfg(test)]
#[path = "aggregate_extrema_tests.rs"]
mod tests;
