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

use super::{checkpoint_at, invariant, original, shared, vector};
use crate::{
    exec::{
        hash_table::{
            allocation::AggregateAllocator,
            partition_keys::{CanonicalTupleStorage, CanonicalTupleView, prepare_canonical_tuple},
        },
        pipeline::operator::InputConstructionOwner,
    },
    runtime::fragment::{ExecutionFailure, ExecutionResult},
};
use arrow::{array::ArrayRef, datatypes::DataType};
use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
use novarocks_value_arrays::{
    ByteLayout, ValueViolation,
    batch::PreparedBytes,
    decode::ValueConversionContext,
    reachability::{ReachableRows, SelectionSet},
};
use std::sync::Arc;

/// SQL matching policy, independent of the signed nullable input declaration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NullPolicy {
    /// A logical NULL does not participate in this equality index.
    Unmatchable,
    /// NULL is one equality class (GROUP BY or null-safe equality).
    Equal,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IndexPurpose {
    KeyIds,
    PresenceOnly,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) struct KeyId(pub(super) usize);
impl KeyId {
    pub(crate) fn index(self) -> usize {
        self.0
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum KeyStrategy {
    Fixed,
    Bytes,
    Composite,
    Canonical,
}

struct SpecData {
    types: Vec<FunctionValueType>,
    nulls: Vec<NullPolicy>,
    strategy: KeyStrategy,
    _storage: ValueTypeStorageLifetime,
}
#[derive(Clone)]
pub(crate) struct KeySpec(Arc<SpecData>);
impl KeySpec {
    pub(crate) fn new(
        types: &[FunctionValueType],
        nulls: &[NullPolicy],
        owner: &dyn InputConstructionOwner,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<Self> {
        checkpoint()?;
        if types.is_empty() || types.len() != nulls.len() {
            return Err(invariant(ValueViolation::Layout));
        }
        let mut copied_types = vector(owner, types.len())?;
        let mut copied_nulls = vector(owner, nulls.len())?;
        let mut canonical = false;
        for (i, (ty, policy)) in types.iter().zip(nulls).enumerate() {
            checkpoint_at(i, checkpoint)?;
            copied_types.push(ty.clone_observed(checkpoint)?);
            copied_nulls.push(*policy);
            canonical |=
                !is_fixed(ty.data_type()) && ByteLayout::from_data_type(ty.data_type()).is_none();
        }
        let strategy = if canonical {
            KeyStrategy::Canonical
        } else if types.len() != 1 {
            KeyStrategy::Composite
        } else if ByteLayout::from_data_type(types[0].data_type()).is_some() {
            KeyStrategy::Bytes
        } else {
            KeyStrategy::Fixed
        };
        Ok(Self(shared(
            owner,
            SpecData {
                types: copied_types,
                nulls: copied_nulls,
                strategy,
                _storage: owner.storage_lifetime(),
            },
        )?))
    }
    pub(crate) fn types(&self) -> &[FunctionValueType] {
        &self.0.types
    }
    pub(crate) fn nulls(&self) -> &[NullPolicy] {
        &self.0.nulls
    }
    pub(crate) fn strategy(&self) -> KeyStrategy {
        self.0.strategy
    }
    pub(super) fn same_instance(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
pub(super) fn is_fixed(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(..)
            | DataType::Decimal256(..)
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(..)
            | DataType::Time32(..)
            | DataType::Time64(..)
            | DataType::Duration(..)
            | DataType::FixedSizeBinary(_)
    )
}

struct InputData {
    arrays: Vec<ArrayRef>,
    bindings: Vec<FunctionValueType>,
    selected: SelectionSet,
    rows: Vec<usize>,
    ordinals: Vec<usize>,
    storage: ValueTypeStorageLifetime,
    tuples: CanonicalTupleStorage,
}
/// One immutable, evaluated page. Saved hashes bind to this actual owner, not
/// to an array address, caller-supplied hash or reconstructed dictionary code.
#[derive(Clone)]
pub(crate) struct KeyInput(Arc<InputData>);
impl KeyInput {
    pub(crate) fn new(
        arrays: &[ArrayRef],
        bindings: &[FunctionValueType],
        readable: &SelectionSet,
        occurrences: &[usize],
        conversions: &mut ValueConversionContext,
        owner: &dyn InputConstructionOwner,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<Self> {
        checkpoint()?;
        if arrays.is_empty()
            || arrays.len() != bindings.len()
            || arrays.iter().any(|a| a.len() != readable.len())
        {
            return Err(invariant(ValueViolation::Bounds));
        }
        let mut columns = vector(owner, arrays.len())?;
        let mut types = vector(owner, bindings.len())?;
        for (i, (array, binding)) in arrays.iter().zip(bindings).enumerate() {
            checkpoint_at(i, checkpoint)?;
            columns.push(Arc::clone(array));
            types.push(binding.clone_observed(checkpoint)?);
        }
        let mut rows = vector(owner, occurrences.len())?;
        let mut ordered = true;
        for (i, &row) in occurrences.iter().enumerate() {
            checkpoint_at(i, checkpoint)?;
            if !readable.contains(row) {
                return Err(invariant(ValueViolation::UndefinedRow));
            }
            ordered &= rows.last().is_none_or(|&previous| previous <= row);
            rows.push(row);
        }
        if !ordered {
            sort_rows(&mut rows, owner, checkpoint)?;
        }
        let mut unique = 0;
        for i in 0..rows.len() {
            checkpoint_at(i, checkpoint)?;
            if unique == 0 || rows[i] != rows[unique - 1] {
                rows[unique] = rows[i];
                unique += 1;
            }
        }
        rows.truncate(unique);
        let mut selected = SelectionSet::try_new_with_control_observed(
            readable.len(),
            &mut |layout| owner.request(layout).map_err(ExecutionFailure::from),
            &mut |_| checkpoint(),
        )
        .map_err(original)?;
        for (i, &row) in rows.iter().enumerate() {
            checkpoint_at(i, checkpoint)?;
            selected
                .insert(row)
                .map_err(ExecutionFailure::from_domain)?;
        }
        let mut ordinals = vector(owner, occurrences.len())?;
        for (i, row) in occurrences.iter().enumerate() {
            checkpoint_at(i, checkpoint)?;
            ordinals.push(
                rows.binary_search(row)
                    .map_err(|_| invariant(ValueViolation::Bounds))?,
            );
        }
        checkpoint()?;
        let tuples = prepare_canonical_tuple(
            &columns,
            &types,
            &ReachableRows::Selection(&selected),
            conversions,
            owner,
            checkpoint,
        )?
        .into_storage();
        Ok(Self(shared(
            owner,
            InputData {
                arrays: columns,
                bindings: types,
                selected,
                rows,
                ordinals,
                storage: owner.storage_lifetime(),
                tuples,
            },
        )?))
    }
    pub(crate) fn len(&self) -> usize {
        self.0.ordinals.len()
    }
    pub(crate) fn row(&self, occurrence: usize) -> ExecutionResult<usize> {
        let ordinal = *self
            .0
            .ordinals
            .get(occurrence)
            .ok_or_else(|| invariant(ValueViolation::Bounds))?;
        Ok(self.0.rows[ordinal])
    }
    pub(crate) fn arrays(&self) -> &[ArrayRef] {
        &self.0.arrays
    }
    pub(crate) fn bindings(&self) -> &[FunctionValueType] {
        &self.0.bindings
    }
    pub(crate) fn selected(&self) -> &SelectionSet {
        &self.0.selected
    }
    pub(crate) fn unique_rows(&self) -> &[usize] {
        &self.0.rows
    }
    pub(super) fn ordinal(&self, occurrence: usize) -> usize {
        self.0.ordinals[occurrence]
    }
    pub(super) fn same_instance(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub(crate) fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.0.storage.clone()
    }
}
/// Coordinate sorting is ordinary fast-path linear for ordered pages. The
/// uncommon reordered occurrence input uses controlled primitive merge passes;
/// no uninterruptible full-input sort or per-key Arrow array is introduced.
pub(super) fn sort_rows(
    rows: &mut [usize],
    owner: &dyn InputConstructionOwner,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<()> {
    let mut scratch = vector(owner, rows.len())?;
    for (i, &row) in rows.iter().enumerate() {
        checkpoint_at(i, checkpoint)?;
        scratch.push(row);
    }
    let mut width = 1usize;
    while width < rows.len() {
        checkpoint()?;
        for start in (0..rows.len()).step_by(width.saturating_mul(2)) {
            let middle = start.saturating_add(width).min(rows.len());
            let end = middle.saturating_add(width).min(rows.len());
            let (mut left, mut right) = (start, middle);
            for (offset, slot) in scratch[start..end].iter_mut().enumerate() {
                checkpoint_at(offset, checkpoint)?;
                if left < middle && (right == end || rows[left] <= rows[right]) {
                    *slot = rows[left];
                    left += 1;
                } else {
                    *slot = rows[right];
                    right += 1;
                }
            }
        }
        for (i, (to, &from)) in rows.iter_mut().zip(&scratch).enumerate() {
            checkpoint_at(i, checkpoint)?;
            *to = from;
        }
        width = width.saturating_mul(2);
    }
    Ok(())
}

enum DomainFailure {
    Type(novarocks_type_contract::ValueTypeError),
    Caller(ExecutionFailure),
}
impl From<novarocks_type_contract::ValueTypeError> for DomainFailure {
    fn from(e: novarocks_type_contract::ValueTypeError) -> Self {
        Self::Type(e)
    }
}
impl From<DomainFailure> for ExecutionFailure {
    fn from(e: DomainFailure) -> Self {
        match e {
            DomainFailure::Type(e) => novarocks_functions::KernelFailure::from(e).into(),
            DomainFailure::Caller(e) => e,
        }
    }
}

pub(crate) struct PreparedKeyBatch<'a> {
    pub(super) input: &'a KeyInput,
    pub(super) spec: KeySpec,
    pub(super) tuples: CanonicalTupleView<'a>,
    bytes: Vec<Option<PreparedBytes<'a>>>,
    pub(super) allocator: AggregateAllocator,
}
impl<'a> PreparedKeyBatch<'a> {
    pub(crate) fn new(
        spec: &KeySpec,
        input: &'a KeyInput,
        allocator: AggregateAllocator,
        owner: &dyn InputConstructionOwner,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<Self> {
        checkpoint()?;
        if spec.types().len() != input.bindings().len() {
            return Err(invariant(ValueViolation::Layout));
        }
        for (expected, actual) in spec.types().iter().zip(input.bindings()) {
            if !expected
                .same_value_domain_observed::<DomainFailure>(actual, || {
                    checkpoint().map_err(DomainFailure::Caller)
                })
                .map_err(ExecutionFailure::from)?
            {
                return Err(invariant(ValueViolation::Layout));
            }
        }
        let tuples = input.0.tuples.view(input.arrays());
        let mut bytes = vector(owner, spec.types().len())?;
        for column in 0..spec.types().len() {
            bytes.push(
                input
                    .0
                    .tuples
                    .decoded_component(column)
                    .map(|decoded| {
                        PreparedBytes::new_observed(decoded, input.selected(), &mut |_| {
                            checkpoint()
                        })
                        .map_err(original)
                    })
                    .transpose()?,
            );
        }
        Ok(Self {
            input,
            spec: spec.clone(),
            tuples,
            bytes,
            allocator,
        })
    }
    pub(crate) fn len(&self) -> usize {
        self.input.len()
    }
    pub(crate) fn bytes(&self, column: usize) -> ExecutionResult<Option<&PreparedBytes<'a>>> {
        self.bytes
            .get(column)
            .map(Option::as_ref)
            .ok_or_else(|| invariant(ValueViolation::Bounds))
    }

    /// Bind a materialization subset once, borrowing the same decoded source.
    /// The subset does not authorize rows outside this input's readable domain.
    pub(super) fn byte_subset<'b>(
        &'b self,
        column: usize,
        rows: &'b SelectionSet,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<PreparedBytes<'b>> {
        let decoded = self
            .input
            .0
            .tuples
            .decoded_component(column)
            .ok_or_else(|| invariant(ValueViolation::Carrier))?;
        PreparedBytes::new_observed(decoded, rows, &mut |_| checkpoint()).map_err(original)
    }
}
