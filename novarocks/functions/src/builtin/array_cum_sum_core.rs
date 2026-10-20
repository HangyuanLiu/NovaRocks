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
//! Cumulative numeric values over the original logical List rows. This is a
//! required invocation author, not a newly installed selected-kernel ABI.
use super::collection_arrow::{self, ArrowSite};
use crate::KernelFailure;
use arrow_array::{Array, ArrayRef, Float64Array, Int64Array, ListArray};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_schema::DataType;
use novarocks_failure_contract::{FailureFact, FailureProjection, OuterFailureProjection};
use std::sync::Arc;

#[derive(Debug)]
struct PrefixOverflow;
impl std::fmt::Display for PrefixOverflow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("array_cum_sum integer accumulation overflow")
    }
}
impl std::error::Error for PrefixOverflow {}
impl FailureProjection for PrefixOverflow {
    fn failure_fact(&self) -> FailureFact {
        crate::DataFailureReason::NumericOutOfRange.failure_fact()
    }
}
impl OuterFailureProjection for PrefixOverflow {}
fn reserve<T>(count: usize) -> Result<Vec<T>, KernelFailure> {
    let mut out = Vec::new();
    out.try_reserve_exact(count).map_err(|source| {
        KernelFailure::from_domain(crate::allocation_failure::AllocationRefusal::new(
            "array_cum_sum output",
            source,
        ))
    })?;
    Ok(out)
}
/// NULL parents demand no child arithmetic. Slices and scalar broadcasts build
/// offsets for their ACTUAL output, never reuse offsets into the source backing.
pub fn cumulative_sum(
    input: &ArrayRef,
    output_type: Option<&DataType>,
    rows: usize,
) -> Result<ArrayRef, KernelFailure> {
    let list = input.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        collection_arrow::contract("array_cum_sum requires its prepared List carrier")
    })?;
    if list.len() != 1 && list.len() != rows {
        return Err(collection_arrow::contract(
            "array_cum_sum input length differs from its invocation",
        ));
    }
    let field = match output_type {
        Some(DataType::List(field)) => field.clone(),
        Some(_) => {
            return Err(collection_arrow::contract(
                "array_cum_sum output must be List",
            ));
        }
        None => match list.data_type() {
            DataType::List(field) => field.clone(),
            _ => {
                return Err(collection_arrow::contract(
                    "array_cum_sum source list declaration differs",
                ));
            }
        },
    };
    let target = field.data_type();
    let values = if list.values().data_type() == target {
        list.values().clone()
    } else {
        arrow_cast::cast(list.values(), target).map_err(|source| {
            collection_arrow::arrow_failure("array_cum_sum", ArrowSite::Cast, source)
        })?
    };
    // Metadata/type refusal precedes output storage. Known List format limits
    // use the same author as every other collection output, without any grant.
    if !matches!(target, DataType::Int64 | DataType::Float64) {
        return Err(collection_arrow::contract(format_args!(
            "array_cum_sum unsupported output element type: {target:?}"
        )));
    }
    let mut offsets = reserve::<i32>(collection_arrow::list_offset_entries(rows)?)?;
    offsets.push(0);
    let mut nulls = NullBufferBuilder::new(rows);
    let source_offsets = list.value_offsets();
    let mut current = 0i32;
    for row in 0..rows {
        let index = if list.len() == 1 { 0 } else { row };
        if list.is_null(index) {
            nulls.append_null();
        } else {
            current = crate::aggregate_scalar::checked_list_output_offset(
                current,
                (source_offsets[index + 1] - source_offsets[index]) as usize,
            )?;
            nulls.append_non_null();
        }
        offsets.push(current);
    }
    let output: ArrayRef = match target {
        DataType::Int64 => {
            let values = values
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    collection_arrow::contract("array_cum_sum signed value carrier differs")
                })?;
            let mut out = reserve::<Option<i64>>(current as usize)?;
            for row in 0..rows {
                let index = if list.len() == 1 { 0 } else { row };
                if list.is_null(index) {
                    continue;
                }
                let mut sum = 0i64;
                for index in source_offsets[index] as usize..source_offsets[index + 1] as usize {
                    if values.is_null(index) {
                        out.push(None);
                    } else {
                        sum = sum
                            .checked_add(values.value(index))
                            .ok_or_else(|| KernelFailure::from_domain(PrefixOverflow))?;
                        out.push(Some(sum));
                    }
                }
            }
            Arc::new(Int64Array::from(out))
        }
        DataType::Float64 => {
            let values = values
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    collection_arrow::contract("array_cum_sum floating value carrier differs")
                })?;
            let mut out = reserve::<Option<f64>>(current as usize)?;
            for row in 0..rows {
                let index = if list.len() == 1 { 0 } else { row };
                if list.is_null(index) {
                    continue;
                }
                let mut sum = 0.0f64;
                for index in source_offsets[index] as usize..source_offsets[index + 1] as usize {
                    if values.is_null(index) {
                        out.push(None);
                    } else {
                        sum += values.value(index);
                        out.push(Some(sum));
                    }
                }
            }
            Arc::new(Float64Array::from(out))
        }
        _ => {
            return Err(collection_arrow::contract(
                "array_cum_sum output escaped its admitted numeric family",
            ));
        }
    };
    let out = ListArray::try_new(
        field,
        OffsetBuffer::new(offsets.into()),
        output,
        nulls.finish(),
    )
    .map_err(|source| {
        collection_arrow::arrow_failure("array_cum_sum", ArrowSite::Construct, source)
    })?;
    Ok(Arc::new(out))
}
#[cfg(test)]
mod properties {
    use super::*;
    use arrow_array::{
        builder::{Int64Builder, ListBuilder},
        new_null_array,
    };
    use arrow_buffer::NullBuffer;
    use arrow_schema::Field;
    use novarocks_failure_contract::FailureClass;
    fn field() -> Arc<Field> {
        Arc::new(Field::new("item", DataType::Int64, true))
    }
    #[test]
    fn sliced_and_broadcast_rows_have_real_output_offsets_and_original_null_elements() {
        let mut b = ListBuilder::new(Int64Builder::new());
        b.values().append_value(99);
        b.append(true);
        b.values().append_value(2);
        b.values().append_null();
        b.values().append_value(3);
        b.append(true);
        let source = b.finish().slice(1, 1);
        let source: ArrayRef = Arc::new(source);
        let target = DataType::List(field());
        let out = cumulative_sum(&source, Some(&target), 2).unwrap();
        let out = out.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(out.value_offsets(), &[0, 3, 6]);
        assert_eq!(
            out.values()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(2), None, Some(5), Some(2), None, Some(5)]
        );
    }
    #[test]
    fn null_parent_does_not_demand_hidden_overflowing_payload() {
        let source: ArrayRef = Arc::new(ListArray::new(
            field(),
            OffsetBuffer::new(vec![0, 2, 3].into()),
            Arc::new(Int64Array::from(vec![i64::MAX, 1, 4])),
            Some(NullBuffer::from(vec![false, true])),
        ));
        let out = cumulative_sum(&source, None, 2).unwrap();
        let out = out.as_any().downcast_ref::<ListArray>().unwrap();
        assert!(out.is_null(0));
        assert_eq!(out.value_offsets(), &[0, 0, 1]);
        assert_eq!(
            out.values()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            4
        );
    }
    #[test]
    fn actual_integer_overflow_is_typed_and_retains_no_input() {
        let input: ArrayRef = Arc::new(ListArray::new(
            field(),
            OffsetBuffer::new(vec![0, 2].into()),
            Arc::new(Int64Array::from(vec![i64::MAX, 1])),
            None,
        ));
        let weak = Arc::downgrade(&input);
        let failure = cumulative_sum(&input, None, 1).unwrap_err();
        drop(input);
        assert!(weak.upgrade().is_none());
        assert_eq!(
            failure.failure_fact(),
            crate::DataFailureReason::NumericOutOfRange.failure_fact()
        );
    }
    #[test]
    fn float_nan_and_strict_shape_refusals_keep_their_own_domains() {
        let field = Arc::new(Field::new("item", DataType::Float64, true));
        let input: ArrayRef = Arc::new(ListArray::new(
            field,
            OffsetBuffer::new(vec![0, 3].into()),
            Arc::new(Float64Array::from(vec![1.0, f64::NAN, 2.0])),
            None,
        ));
        let out = cumulative_sum(&input, None, 1).unwrap();
        let list = out.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(values.value(0), 1.0);
        assert!(values.value(1).is_nan());
        assert!(values.value(2).is_nan());
        assert_eq!(
            cumulative_sum(&input, Some(&DataType::Int64), 1)
                .unwrap_err()
                .failure_fact()
                .class(),
            FailureClass::Invariant
        );
        assert_eq!(
            cumulative_sum(&new_null_array(&DataType::Null, 1), None, 1)
                .unwrap_err()
                .failure_fact()
                .class(),
            FailureClass::Invariant
        );
        assert_eq!(
            collection_arrow::list_offset_entries(usize::MAX)
                .unwrap_err()
                .failure_fact()
                .class(),
            FailureClass::Representation
        );
    }
}
