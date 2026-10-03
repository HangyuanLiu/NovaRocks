// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Selected CARDINALITY counts root List/Map offsets without inspecting children.
//! Output Layout checks establish representability, not a formal memory grant.

use crate::{
    FunctionArgumentType, FunctionValueType, KernelEvaluationControl, KernelFailure,
    ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef, Int32Array, ListArray, MapArray};
use arrow_buffer::{BooleanBufferBuilder, NullBuffer};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;
use std::{alloc::Layout, sync::Arc};

struct CollectionOffsets<'a> {
    offsets: &'a [i32],
    children: usize,
}

fn checked_input<'a>(
    source: &FunctionValueType,
    array: &'a dyn Array,
) -> Result<CollectionOffsets<'a>, KernelFailure> {
    if source.logical_type != ValueLogicalType::Physical {
        return Err(invalid(
            "cardinality requires its selected Physical collection domain",
        ));
    }
    // The original ScalarCallInput wrapper has already compared full fields
    // through the observed type author. Here only the concrete root class is
    // checked; a second uncontrolled recursive Arrow equality is unnecessary.
    match &source.data_type {
        DataType::List(_) => {
            let list = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                internal("cardinality exact List source has a foreign carrier class")
            })?;
            Ok(CollectionOffsets {
                offsets: list.value_offsets(),
                children: list.values().len(),
            })
        }
        DataType::Map(_, _) => {
            let map = array.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
                internal("cardinality exact Map source has a foreign carrier class")
            })?;
            Ok(CollectionOffsets {
                offsets: map.value_offsets(),
                children: map.entries().len(),
            })
        }
        _ => Err(invalid(
            "cardinality requires its exact selected List or Map source",
        )),
    }
}

fn output_capacity(rows: usize) -> Result<(), KernelFailure> {
    let values = Layout::array::<i32>(rows)
        .map_err(|_| KernelFailure::ResourceExhausted)?
        .size();
    let bitmap = rows
        .checked_add(63)
        .map(|bits| bits / 64)
        .and_then(|words| words.checked_mul(8))
        .ok_or(KernelFailure::ResourceExhausted)?;
    Layout::array::<u8>(bitmap).map_err(|_| KernelFailure::ResourceExhausted)?;
    values
        .checked_add(bitmap)
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(())
}

pub(super) fn evaluate_collection_cardinality<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let ([FunctionArgumentType::Value(source)], [argument]) = (
            input.contract().selected().argument_types.as_ref(),
            input.arguments(),
        ) else {
            return Err(invalid(
                "cardinality requires one exact checked value argument",
            ));
        };
        let target = input.contract().result_type();
        let exact = target.logical_type == ValueLogicalType::Physical
            && target.data_type == DataType::Int32
            && target.nullable;
        work.step()?;
        if !exact {
            return Err(invalid(
                "cardinality differs from its exact nullable Int32 result",
            ));
        }
        let collection = checked_input(source, argument.array().as_ref());
        work.step()?;
        let collection = collection?;
        let selection = input.selection();
        output_capacity(selection.len())?;
        work.flush()?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(selection.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        let mut validity = BooleanBufferBuilder::new(selection.len());
        work.flush()?;
        let mut has_null = false;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            let row = argument.value_row(ordinal, batch_row);
            let valid_address = row < argument.array().len();
            work.step()?;
            if !valid_address {
                return Err(internal("cardinality selected row is out of bounds"));
            }
            let source_null = argument.array().is_null(row);
            work.step()?;
            if source_null && !source.nullable {
                return Err(internal(
                    "cardinality non-null source contains selected SQL NULL",
                ));
            }
            let cardinality = if source_null {
                None
            } else {
                // Use checked access/subtraction so malformed foreign backing
                // is a protocol error, never a panic or guessed successful NULL.
                let next = row.checked_add(1);
                let pair = next.and_then(|next| {
                    Some((
                        *collection.offsets.get(row)?,
                        *collection.offsets.get(next)?,
                    ))
                });
                work.step()?;
                let (start, end) =
                    pair.ok_or_else(|| internal("cardinality selected offsets are missing"))?;
                let valid = start >= 0 && end >= start && (end as usize) <= collection.children;
                work.step()?;
                if !valid {
                    return Err(internal(
                        "cardinality selected offsets violate the collection backing",
                    ));
                }
                let count = end.checked_sub(start);
                work.step()?;
                Some(count.ok_or_else(|| {
                    internal("cardinality selected offset difference is unrepresentable")
                })?)
            };
            values.push(cardinality.unwrap_or(0));
            validity.append(cardinality.is_some());
            has_null |= cardinality.is_none();
            work.step()?;
        }
        work.flush()?;
        let array = Arc::new(Int32Array::new(
            values.into(),
            has_null.then(|| NullBuffer::new(validity.finish())),
        )) as ArrayRef;
        work.flush()?;
        SelectedValues::try_new_observed::<KernelFailure>(
            selection,
            &target.data_type,
            array,
            Box::default(),
            || work.step(),
        )
    })();
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

#[cfg(test)]
#[path = "collection_cardinality_tests.rs"]
mod tests;
