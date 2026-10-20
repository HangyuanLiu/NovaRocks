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
use crate::runtime::fragment::{ExecutionFailure, ExecutionReason, ExecutionResult};
use arrow::array::Array;
use arrow::datatypes::DataType;

use crate::exec::expr::agg::{AggregateAllocator, AggregateVec, IntArrayView};

use super::key_builder::GroupKeyArrayView;
use super::key_strategy::is_compressible_key_type;

#[derive(Debug)]
struct CompressedKeyContract(novarocks_failure_contract::BoundedDetail);
impl std::fmt::Display for CompressedKeyContract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.as_str())
    }
}
impl std::error::Error for CompressedKeyContract {}
impl novarocks_failure_contract::FailureProjection for CompressedKeyContract {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            ExecutionReason::CompressedKeyContract.reason(),
        )
    }
}
pub(super) fn compressed_key_contract(detail: impl std::fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from_domain(CompressedKeyContract(
        novarocks_failure_contract::BoundedDetail::truncating_display(detail),
    ))
}

pub(crate) struct CompressedKeyContext {
    pub(crate) bases: AggregateVec<i128>,
    pub(crate) used_bits: AggregateVec<u8>,
    pub(crate) null_offsets: AggregateVec<u16>,
    pub(crate) value_offsets: AggregateVec<u16>,
}

pub(crate) fn build_compressed_key_context(
    views: &[GroupKeyArrayView<'_>],
    types: &[DataType],
    allocator: AggregateAllocator,
) -> ExecutionResult<Option<CompressedKeyContext>> {
    if views.len() != types.len() {
        return Err(compressed_key_contract(
            "compressed key type length mismatch",
        ));
    }
    for data_type in types {
        if !is_compressible_key_type(data_type) {
            return Ok(None);
        }
    }
    if types.len() > 128 {
        return Ok(None);
    }
    // One bounded stack plan, one pass over input. An inapplicable optional
    // representation neither allocates metadata nor hides a real refusal.
    let mut planned_bases = [0_i128; 128];
    let mut planned_bits = [0_u8; 128];
    let mut total_bits = 0_u16;
    for (idx, view) in views.iter().enumerate() {
        let (base, max_value) = min_max_i128(view)?.unwrap_or((0, 0));
        let bits = bits_for_range(max_value - base);
        if u32::from(total_bits) + 1 + u32::from(bits) > 128 {
            return Ok(None);
        }
        total_bits += 1 + u16::from(bits);
        planned_bases[idx] = base;
        planned_bits[idx] = bits;
    }
    let mut bases = AggregateVec::new_in(allocator.clone());
    let mut used_bits = AggregateVec::new_in(allocator.clone());
    let mut null_offsets = AggregateVec::new_in(allocator.clone());
    let mut value_offsets = AggregateVec::new_in(allocator.clone());
    bases
        .try_reserve_exact(types.len())
        .map_err(|source| allocator.allocation_error("reserve compressed-key bases", source))?;
    used_bits.try_reserve_exact(types.len()).map_err(|source| {
        allocator.allocation_error("reserve compressed-key bit widths", source)
    })?;
    null_offsets
        .try_reserve_exact(types.len())
        .map_err(|source| {
            allocator.allocation_error("reserve compressed-key null offsets", source)
        })?;
    value_offsets
        .try_reserve_exact(types.len())
        .map_err(|source| {
            allocator.allocation_error("reserve compressed-key value offsets", source)
        })?;
    let mut offset = 0_u16;
    for idx in 0..types.len() {
        bases.push(planned_bases[idx]);
        used_bits.push(planned_bits[idx]);
        null_offsets.push(offset);
        offset += 1;
        value_offsets.push(offset);
        offset += u16::from(planned_bits[idx]);
    }

    Ok(Some(CompressedKeyContext {
        bases,
        used_bits,
        null_offsets,
        value_offsets,
    }))
}

pub(crate) fn compressed_key_is_valid(
    ctx: &CompressedKeyContext,
    views: &[GroupKeyArrayView<'_>],
    row: usize,
) -> ExecutionResult<bool> {
    Ok(compressed_key_bits(ctx, views, row)?.is_some())
}

fn compressed_key_bits(
    ctx: &CompressedKeyContext,
    views: &[GroupKeyArrayView<'_>],
    row: usize,
) -> ExecutionResult<Option<u128>> {
    if views.len() != ctx.bases.len()
        || views.len() != ctx.used_bits.len()
        || views.len() != ctx.null_offsets.len()
        || views.len() != ctx.value_offsets.len()
    {
        return Err(compressed_key_contract(
            "compressed context key count mismatch",
        ));
    }
    let mut bits: u128 = 0;
    for (idx, view) in views.iter().enumerate() {
        let null_offset = *ctx
            .null_offsets
            .get(idx)
            .ok_or_else(|| compressed_key_contract("compressed null offset missing"))?;
        let value_offset = *ctx
            .value_offsets
            .get(idx)
            .ok_or_else(|| compressed_key_contract("compressed value offset missing"))?;
        let used = *ctx
            .used_bits
            .get(idx)
            .ok_or_else(|| compressed_key_contract("compressed bits missing"))?;
        let base = *ctx
            .bases
            .get(idx)
            .ok_or_else(|| compressed_key_contract("compressed base missing"))?;
        if null_offset >= 128
            || value_offset > 128
            || used >= 128
            || u32::from(value_offset) + u32::from(used) > 128
        {
            return Err(compressed_key_contract(
                "compressed context bit extent is invalid",
            ));
        }
        let (value, is_null) = match view {
            GroupKeyArrayView::Int(view) => (view.value_at(row).map(|v| v as i128), false),
            GroupKeyArrayView::Date32(arr) => {
                if arr.is_null(row) {
                    (None, true)
                } else {
                    (Some(arr.value(row) as i128), false)
                }
            }
            GroupKeyArrayView::TimestampSecond(arr) => {
                if arr.is_null(row) {
                    (None, true)
                } else {
                    (Some(arr.value(row) as i128), false)
                }
            }
            GroupKeyArrayView::TimestampMillisecond(arr) => {
                if arr.is_null(row) {
                    (None, true)
                } else {
                    (Some(arr.value(row) as i128), false)
                }
            }
            GroupKeyArrayView::TimestampMicrosecond(arr) => {
                if arr.is_null(row) {
                    (None, true)
                } else {
                    (Some(arr.value(row) as i128), false)
                }
            }
            GroupKeyArrayView::TimestampNanosecond(arr) => {
                if arr.is_null(row) {
                    (None, true)
                } else {
                    (Some(arr.value(row) as i128), false)
                }
            }
            GroupKeyArrayView::Boolean(arr) => {
                if arr.is_null(row) {
                    (None, true)
                } else {
                    (Some((if arr.value(row) { 1 } else { 0 }) as i128), false)
                }
            }
            _ => {
                return Err(compressed_key_contract("compressed key unsupported type"));
            }
        };

        if is_null || value.is_none() {
            bits |= 1u128 << u32::from(null_offset);
            continue;
        }
        let value = value.expect("compressed value");
        let diff = value - base;
        if diff < 0 {
            return Ok(None);
        }
        let diff = diff as u128;
        let max_diff = if used == 0 {
            0
        } else if used == 128 {
            return Err(compressed_key_contract("compressed bits overflow"));
        } else {
            (1u128 << used) - 1
        };
        if diff > max_diff {
            return Ok(None);
        }
        if used > 0 {
            bits |= diff << u32::from(value_offset);
        }
    }
    Ok(Some(bits))
}

fn min_max_i128(view: &GroupKeyArrayView<'_>) -> ExecutionResult<Option<(i128, i128)>> {
    let mut min: Option<i128> = None;
    let mut max: Option<i128> = None;
    match view {
        GroupKeyArrayView::Int(view) => match view {
            IntArrayView::Int64(arr) => {
                for row in 0..arr.len() {
                    if !arr.is_null(row) {
                        let value = arr.value(row) as i128;
                        min = Some(min.map_or(value, |m| m.min(value)));
                        max = Some(max.map_or(value, |m| m.max(value)));
                    }
                }
            }
            IntArrayView::Int32(arr) => {
                for row in 0..arr.len() {
                    if !arr.is_null(row) {
                        let value = arr.value(row) as i128;
                        min = Some(min.map_or(value, |m| m.min(value)));
                        max = Some(max.map_or(value, |m| m.max(value)));
                    }
                }
            }
            IntArrayView::Int16(arr) => {
                for row in 0..arr.len() {
                    if !arr.is_null(row) {
                        let value = arr.value(row) as i128;
                        min = Some(min.map_or(value, |m| m.min(value)));
                        max = Some(max.map_or(value, |m| m.max(value)));
                    }
                }
            }
            IntArrayView::Int8(arr) => {
                for row in 0..arr.len() {
                    if !arr.is_null(row) {
                        let value = arr.value(row) as i128;
                        min = Some(min.map_or(value, |m| m.min(value)));
                        max = Some(max.map_or(value, |m| m.max(value)));
                    }
                }
            }
        },
        GroupKeyArrayView::Date32(arr) => {
            for row in 0..arr.len() {
                if !arr.is_null(row) {
                    let value = arr.value(row) as i128;
                    min = Some(min.map_or(value, |m| m.min(value)));
                    max = Some(max.map_or(value, |m| m.max(value)));
                }
            }
        }
        GroupKeyArrayView::TimestampSecond(arr) => {
            for row in 0..arr.len() {
                if !arr.is_null(row) {
                    let value = arr.value(row) as i128;
                    min = Some(min.map_or(value, |m| m.min(value)));
                    max = Some(max.map_or(value, |m| m.max(value)));
                }
            }
        }
        GroupKeyArrayView::TimestampMillisecond(arr) => {
            for row in 0..arr.len() {
                if !arr.is_null(row) {
                    let value = arr.value(row) as i128;
                    min = Some(min.map_or(value, |m| m.min(value)));
                    max = Some(max.map_or(value, |m| m.max(value)));
                }
            }
        }
        GroupKeyArrayView::TimestampMicrosecond(arr) => {
            for row in 0..arr.len() {
                if !arr.is_null(row) {
                    let value = arr.value(row) as i128;
                    min = Some(min.map_or(value, |m| m.min(value)));
                    max = Some(max.map_or(value, |m| m.max(value)));
                }
            }
        }
        GroupKeyArrayView::TimestampNanosecond(arr) => {
            for row in 0..arr.len() {
                if !arr.is_null(row) {
                    let value = arr.value(row) as i128;
                    min = Some(min.map_or(value, |m| m.min(value)));
                    max = Some(max.map_or(value, |m| m.max(value)));
                }
            }
        }
        GroupKeyArrayView::Boolean(arr) => {
            for row in 0..arr.len() {
                if !arr.is_null(row) {
                    let value = if arr.value(row) { 1 } else { 0 };
                    let value = value as i128;
                    min = Some(min.map_or(value, |m| m.min(value)));
                    max = Some(max.map_or(value, |m| m.max(value)));
                }
            }
        }
        _ => {
            return Err(compressed_key_contract(
                "compressed key min/max unsupported type",
            ));
        }
    }
    match (min, max) {
        (Some(min), Some(max)) => Ok(Some((min, max))),
        _ => Ok(None),
    }
}

fn bits_for_range(range: i128) -> u8 {
    if range <= 0 {
        0
    } else {
        let value = range as u128;
        (128 - value.leading_zeros()) as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::mem_tracker::MemTracker;
    use arrow::array::Int64Array;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    #[test]
    fn optional_compression_distinguishes_wide_shape_from_actual_allocation_refusal() {
        let tracker = MemTracker::new_root("compressed-layout-refusal");
        tracker.install_limit_once(1).unwrap();
        let allocator = AggregateAllocator::new(tracker.clone());
        let wide = Int64Array::from(vec![i64::MIN, i64::MAX]);
        let views = [
            GroupKeyArrayView::Int(IntArrayView::Int64(&wide)),
            GroupKeyArrayView::Int(IntArrayView::Int64(&wide)),
        ];
        assert!(
            build_compressed_key_context(
                &views,
                &[DataType::Int64, DataType::Int64],
                allocator.clone()
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(tracker.current(), 0);
        let error = match build_compressed_key_context(&views[..1], &[DataType::Int64], allocator) {
            Err(error) => error,
            Ok(_) => panic!("applicable compression must propagate the actual allocation refusal"),
        };
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Resource);
        assert_eq!(fact.resource().unwrap().requested, Some(16));
        assert_eq!(fact.resource().unwrap().limit, None);
        crate::runtime_filter::assert_real_vector_refusal(&error, Some(16));
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn frozen_context_failure_is_distinct_from_row_not_fitting_the_selected_range() {
        let tracker = MemTracker::new_root("compressed-layout-values");
        let first = Int64Array::from(vec![Some(1), Some(3), None]);
        let views = [GroupKeyArrayView::Int(IntArrayView::Int64(&first))];
        let context = build_compressed_key_context(
            &views,
            &[DataType::Int64],
            AggregateAllocator::new(tracker),
        )
        .unwrap()
        .unwrap();
        assert!(compressed_key_is_valid(&context, &views, 0).unwrap());
        assert!(compressed_key_is_valid(&context, &views, 2).unwrap());
        let arity = compressed_key_is_valid(&context, &[], 0).unwrap_err();
        assert_eq!(arity.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(
            arity.failure_fact().reason(),
            ExecutionReason::CompressedKeyContract.reason()
        );
        let next = Int64Array::from(vec![100]);
        assert!(
            !compressed_key_is_valid(
                &context,
                &[GroupKeyArrayView::Int(IntArrayView::Int64(&next))],
                0
            )
            .unwrap()
        );
        let error =
            match build_compressed_key_context(&views, &[], context.bases.allocator().clone()) {
                Err(error) => error,
                Ok(_) => panic!("frozen key arity mismatch must be rejected"),
            };
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(
            error.failure_fact().reason(),
            ExecutionReason::CompressedKeyContract.reason()
        );
    }
}
