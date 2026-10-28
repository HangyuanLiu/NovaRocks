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

//! Exact observations of a caller-selected input occurrence range. These are
//! neither NDV hints nor a proof of completed task-graph coverage.

use super::{
    KeySpec, LocalHashBatch, LocalHashPolicy, NullPolicy, PreparedKeyBatch, checkpoint_at,
    invariant,
};
use crate::{
    exec::pipeline::operator::ConstructionFailure,
    runtime::fragment::{ExecutionFailure, ExecutionResult},
};
use arrow::{
    array::{Array, PrimitiveArray},
    datatypes::{ArrowPrimitiveType, DataType, Int8Type, Int16Type, Int32Type, Int64Type},
};
use novarocks_value_arrays::ValueViolation;
use std::ops::Range;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct IntegerDomain {
    pub(super) min_max: Option<(i64, i64)>,
}
impl IntegerDomain {
    fn observe(&mut self, value: i64) {
        self.min_max = Some(self.min_max.map_or((value, value), |(min, max)| {
            (min.min(value), max.max(value))
        }));
    }
    fn merge(&mut self, other: Self) {
        if let Some((min, max)) = other.min_max {
            self.observe(min);
            self.observe(max);
        }
    }
    /// A widened span is an optimization fact, never an input-size quota.
    pub(super) fn span(self) -> Option<u128> {
        self.min_max
            .map(|(min, max)| ((max as i128) - (min as i128) + 1) as u128)
    }
}

pub(super) struct BuildRangeStats {
    spec: KeySpec,
    input_rows: usize,
    indexable_rows: usize,
    null_rows: usize,
    integer: Option<IntegerDomain>,
}
impl BuildRangeStats {
    pub(super) fn spec(&self) -> &KeySpec {
        &self.spec
    }
    /// Preserve the original signed-integer vocabulary at its single author.
    pub(super) fn empty_for_spec(spec: &KeySpec) -> Self {
        Self::empty(spec, direct_integer(spec))
    }
    pub(super) fn input_rows(&self) -> usize {
        self.input_rows
    }
    pub(super) fn indexable_rows(&self) -> usize {
        self.indexable_rows
    }
    pub(super) fn null_rows(&self) -> usize {
        self.null_rows
    }
    pub(super) fn integer(&self) -> Option<IntegerDomain> {
        self.integer
    }
    fn empty(spec: &KeySpec, integer: bool) -> Self {
        Self {
            spec: spec.clone(),
            input_rows: 0,
            indexable_rows: 0,
            null_rows: 0,
            integer: integer.then_some(IntegerDomain { min_max: None }),
        }
    }
    /// Consume actual saved participation facts; this never reads key bytes to
    /// reconstruct a hash or treats a legal hash of zero as absent.
    pub(super) fn collect_hashed(
        input: &PreparedKeyBatch<'_>,
        hashes: &LocalHashBatch,
        policy: &LocalHashPolicy,
        range: Range<usize>,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<Self> {
        checkpoint()?;
        validate_range(input, &range)?;
        if !policy.accepts(&input.spec) {
            return Err(invariant(ValueViolation::Layout));
        }
        hashes.validate(input.input, policy)?;
        // The old production direct optimization covers these exact signed
        // integer domains. Other legal types keep their normal hash path.
        if direct_integer(&input.spec) {
            return Self::collect_integer(input, range, checkpoint);
        }
        let mut output = Self::empty(&input.spec, false);
        let tuples = input.tuples();
        for (i, occurrence) in range.enumerate() {
            checkpoint_at(i, checkpoint)?;
            let row = input.input.row(occurrence)?;
            output.input_rows += 1;
            output.indexable_rows += usize::from(hashes.participates(occurrence)?);
            output.null_rows += usize::from(tuples.row_has_null_observed(row, checkpoint)?);
        }
        checkpoint()?;
        Ok(output)
    }
    /// Direct indexing does not need a hash. Select a concrete integer loop
    /// once at this batch boundary; preserve duplicate caller occurrences.
    pub(super) fn collect_integer(
        input: &PreparedKeyBatch<'_>,
        range: Range<usize>,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<Self> {
        checkpoint()?;
        validate_range(input, &range)?;
        if !direct_integer(&input.spec) {
            return Err(invariant(ValueViolation::Layout));
        }
        let (array, mapping) = input.tuples().physical_component(0)?;
        match input.spec.types()[0].data_type() {
            DataType::Int8 => {
                integer_rows::<Int8Type>(input, array.as_ref(), mapping, range, checkpoint)
            }
            DataType::Int16 => {
                integer_rows::<Int16Type>(input, array.as_ref(), mapping, range, checkpoint)
            }
            DataType::Int32 => {
                integer_rows::<Int32Type>(input, array.as_ref(), mapping, range, checkpoint)
            }
            DataType::Int64 => {
                integer_rows::<Int64Type>(input, array.as_ref(), mapping, range, checkpoint)
            }
            _ => Err(invariant(ValueViolation::Layout)),
        }
    }
    /// The graph owner supplies disjoint, exactly completed task outputs.
    /// Consuming these small facts does not independently certify coverage.
    pub(super) fn merge_completed_range(
        &mut self,
        other: Self,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<()> {
        checkpoint()?;
        if !self.spec.same_instance(&other.spec)
            || self.integer.is_some() != other.integer.is_some()
        {
            return Err(invariant(ValueViolation::Layout));
        }
        let rows = add(self.input_rows, other.input_rows)?;
        let indexable = add(self.indexable_rows, other.indexable_rows)?;
        let nulls = add(self.null_rows, other.null_rows)?;
        let mut integer = self.integer;
        if let (Some(dest), Some(source)) = (&mut integer, other.integer) {
            dest.merge(source);
        }
        checkpoint()?;
        self.input_rows = rows;
        self.indexable_rows = indexable;
        self.null_rows = nulls;
        self.integer = integer;
        Ok(())
    }
}
fn direct_integer(spec: &KeySpec) -> bool {
    spec.types().len() == 1
        && matches!(
            spec.types()[0].data_type(),
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        )
}
fn validate_range(input: &PreparedKeyBatch<'_>, range: &Range<usize>) -> ExecutionResult<()> {
    if range.start > range.end || range.end > input.len() {
        return Err(invariant(ValueViolation::Bounds));
    }
    Ok(())
}
fn integer_rows<T: ArrowPrimitiveType>(
    input: &PreparedKeyBatch<'_>,
    array: &dyn Array,
    mapping: Option<&[usize]>,
    range: Range<usize>,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<BuildRangeStats>
where
    T::Native: Into<i64>,
{
    let source = array
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| invariant(ValueViolation::Carrier))?;
    let mut output = BuildRangeStats::empty(&input.spec, true);
    for (i, occurrence) in range.enumerate() {
        checkpoint_at(i, checkpoint)?;
        let row = super::pairs::physical_row(mapping, input.input.row(occurrence)?)?;
        if row >= source.len() {
            return Err(invariant(ValueViolation::Bounds));
        }
        output.input_rows += 1;
        if source.is_null(row) {
            output.null_rows += 1;
            output.indexable_rows += usize::from(input.spec.nulls()[0] == NullPolicy::Equal);
        } else {
            output.indexable_rows += 1;
            output
                .integer
                .as_mut()
                .expect("integer range selected")
                .observe(source.value(row).into());
        }
    }
    checkpoint()?;
    Ok(output)
}
fn add(left: usize, right: usize) -> ExecutionResult<usize> {
    left.checked_add(right).ok_or_else(|| {
        ExecutionFailure::from(ConstructionFailure::from(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
        ))
    })
}
