// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Compact selected rows, never an original unselected column sent to IPC.
use super::cow_closed_ast::{BuildError, Result, exact_vec};
use super::cow_compile_receipt::recipe::Recipe;
use arrow::array::{Array, ArrayRef, UInt64Array};
use arrow::datatypes::Field;
use novarocks_functions::selected_copy::{CopyConstructionError, CopyIndexView};
use novarocks_functions::{ConstantCreationFailure, ConstantPolicy, ConstantPool, ConstantValue};
use novarocks_spi::connector::{
    ConnectorOriginalResultScope, ConnectorRowMutationSelection,
    ConnectorRowMutationSelectionOrdinal,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, FunctionValueType, PureCompileControl, ValueSchemaCreation,
    ValueTypeStorageLifetime,
};
use std::alloc::Layout;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
pub(super) type Rows = Arc<[Box<[ConstantValue]>]>;

/// Stack-only prospective arithmetic on the ORIGINAL window. It neither
/// grants bytes nor records physical facts. First refusal returns directly.
struct Growth<'a> {
    base: u64,
    added: AtomicU64,
    maximum_transient: AtomicU64,
    original: &'a ConnectorOriginalResultScope,
}
impl Growth<'_> {
    fn bytes(&self, bytes: usize) -> Result<()> {
        let next = self
            .added
            .load(Ordering::Relaxed)
            .checked_add(u64::try_from(bytes).map_err(|_| BuildError::ResourceExhausted)?)
            .ok_or(BuildError::ResourceExhausted)?;
        let total = self
            .base
            .checked_add(self.maximum_transient.load(Ordering::Relaxed))
            .and_then(|n| n.checked_add(next))
            .ok_or(BuildError::ResourceExhausted)?;
        self.original
            .check_before_growth(total)
            .map_err(BuildError::Original)?;
        self.added.store(next, Ordering::Relaxed);
        Ok(())
    }
    fn layout(&self, layout: Layout) -> Result<()> {
        self.bytes(layout.size())
    }
    fn array<T>(&self, count: usize) -> Result<()> {
        if count != 0 {
            self.layout(Layout::array::<T>(count).map_err(|_| BuildError::ResourceExhausted)?)?;
        }
        Ok(())
    }
    fn check(&self) -> Result<()> {
        self.original.check_active().map_err(BuildError::Original)
    }
}
/// Each temporary quote expires after its original opaque copy and pool loan
/// use. Only the maximum coexistence envelope enters the all-target receipt.
struct Transient<'a, 'b> {
    parent: &'a Growth<'b>,
    bytes: AtomicU64,
}
impl Transient<'_, '_> {
    fn bytes(&self, bytes: usize) -> Result<()> {
        let next = self
            .bytes
            .load(Ordering::Relaxed)
            .checked_add(u64::try_from(bytes).map_err(|_| BuildError::ResourceExhausted)?)
            .ok_or(BuildError::ResourceExhausted)?;
        let total = self
            .parent
            .base
            .checked_add(self.parent.added.load(Ordering::Relaxed))
            .and_then(|n| n.checked_add(next))
            .ok_or(BuildError::ResourceExhausted)?;
        self.parent
            .original
            .check_before_growth(total)
            .map_err(BuildError::Original)?;
        self.bytes.store(next, Ordering::Relaxed);
        self.parent
            .maximum_transient
            .fetch_max(next, Ordering::Relaxed);
        Ok(())
    }
    fn layout(&self, layout: Layout) -> Result<()> {
        self.bytes(layout.size())
    }
    fn array<T>(&self, count: usize) -> Result<()> {
        if count != 0 {
            self.layout(Layout::array::<T>(count).map_err(|_| BuildError::ResourceExhausted)?)?;
        }
        Ok(())
    }
}
struct ConstantHold {
    _original: ConnectorOriginalResultScope,
}
impl novarocks_type_contract::ValueTypeStorageHold for ConstantHold {}
fn copy_failure(error: CopyConstructionError<BuildError>) -> BuildError {
    match error {
        CopyConstructionError::Creator(error) => error,
        CopyConstructionError::Copy(error) => BuildError::Copy(error),
        CopyConstructionError::Resource(error) => BuildError::Layout(error),
    }
}
fn constant_failure(error: ConstantCreationFailure<BuildError>) -> BuildError {
    match error {
        ConstantCreationFailure::Creator(error) => error,
        ConstantCreationFailure::Constant(error) => BuildError::Constant(error),
        ConstantCreationFailure::Resource(error) => BuildError::Layout(error),
        ConstantCreationFailure::SourceMetadata(_) => {
            BuildError::InvalidSource("new COW constant has external source annotations")
        }
    }
}
fn pool(
    array: &ArrayRef,
    value_type: &FunctionValueType,
    storage: &ValueTypeStorageLifetime,
    policy: ConstantPolicy,
    growth: &Growth<'_>,
    control: &dyn PureCompileControl,
) -> Result<ConstantPool> {
    // Original to_data + safe validation + canonical array wrappers, on the
    // sole borrowed metadata graph. This quote excludes existing payload B.
    let metadata =
        novarocks_functions::source_metadata_bytes_observed(array.as_ref(), &mut |_| {
            growth.check()
        })?;
    growth.bytes(metadata)?;
    let field_layout =
        novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<Field>())
            .map_err(BuildError::Layout)?;
    growth.layout(field_layout)?;
    // Canonical value layout shares child Field/parameter holders. The empty
    // source name requests no String bytes and no provider facts are copied.
    let field = Arc::new(Field::new(
        "",
        value_type.data_type().clone(),
        value_type.nullable(),
    ));
    growth.check()?;
    let data = array.to_data();
    growth.check()?;
    let mut previous = 0;
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    ConstantPool::try_new_owned_observed_in(
        field,
        value_type.clone(),
        data,
        policy,
        storage.clone(),
        &mut |facts| {
            let next = facts.allocation_request_bytes_upper_bound;
            let delta = next
                .checked_sub(previous)
                .ok_or(BuildError::ResourceExhausted)?;
            growth.bytes(delta)?;
            previous = next;
            Ok(())
        },
        &mut work,
    )
    .map_err(constant_failure)
}

fn protocol_type(
    ty: arrow::datatypes::DataType,
    storage: &ValueTypeStorageLifetime,
    growth: &Growth<'_>,
) -> Result<FunctionValueType> {
    growth.layout(
        novarocks_type_contract::ValueTypeRef::storage_allocation_layout()
            .map_err(BuildError::Layout)?,
    )?;
    growth.layout(
        novarocks_type_contract::ValueLayout::storage_allocation_layout()
            .map_err(BuildError::Layout)?,
    )?;
    let value = novarocks_type_contract::ValueTypeRef::try_new_observed(
        novarocks_type_contract::ValueKind::Scalar(ty.clone()),
        false,
        storage.clone(),
        |_| growth.check(),
    )?;
    let layout = novarocks_type_contract::ValueLayout::try_from_normalized_arrow_observed(
        ty,
        storage.clone(),
        |_| growth.check(),
    )?;
    let binding = novarocks_type_contract::TypedValueBinding::try_new(value, layout)
        .map_err(BuildError::ValueType)?;
    FunctionValueType::try_from_typed_binding(binding)
        .map_err(|e| BuildError::Constant(novarocks_functions::ConstantError::Type(e)))
}

/// All compact copy/owner requests are admitted before the corresponding
/// constructor. The caller invokes this for ALL targets before any AST graph.
/// `columns` borrows the route's actual signed ordinal and complete domain.
pub(super) fn build(
    selection: &ConnectorRowMutationSelection,
    selected: &[ConnectorRowMutationSelectionOrdinal],
    columns: &[(u32, &FunctionValueType)],
    protocol_tail: bool,
    effect: Option<u32>,
    recipe: &mut Recipe,
    original: &ConnectorOriginalResultScope,
    policy: ConstantPolicy,
    control: &dyn PureCompileControl,
) -> Result<Rows> {
    if selected.is_empty() || columns.is_empty() || protocol_tail != effect.is_some() {
        return Err(BuildError::InvalidSource(
            "COW compact source shape is invalid",
        ));
    }
    let growth = Growth {
        base: recipe
            .peak()
            .map_err(super::cow_closed_route::map_footprint)?,
        added: AtomicU64::new(0),
        maximum_transient: AtomicU64::new(0),
        original,
    };
    let width = columns
        .len()
        .checked_add(if protocol_tail { 2 } else { 0 })
        .ok_or(BuildError::ResourceExhausted)?;
    growth.array::<Vec<ConstantValue>>(selected.len())?;
    let mut output = exact_vec::<Vec<ConstantValue>>(selected.len())?;
    for _ in selected {
        growth.array::<ConstantValue>(width)?;
        output.push(exact_vec(width)?);
    }
    growth.layout(
        novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<ConstantHold>())
            .map_err(BuildError::Layout)?,
    )?;
    let storage = ValueTypeStorageLifetime::held(Arc::new(ConstantHold {
        _original: original.clone(),
    }));
    // This ORIGINAL operation owns all temporary conversions. No cross-record
    // source receipt exists in its fixed budget, so reuse is explicitly off.
    let mut value_conversions = novarocks_value_arrays::decode::ValueConversionContext::new(0);
    let effect_type = if effect.is_some() {
        Some(protocol_type(
            arrow::datatypes::DataType::Int8,
            &storage,
            &growth,
        )?)
    } else {
        None
    };
    let matched_type = if protocol_tail {
        Some(protocol_type(
            arrow::datatypes::DataType::Boolean,
            &storage,
            &growth,
        )?)
    } else {
        None
    };
    let matched = if let Some(ty) = matched_type.as_ref() {
        let mut request = |layout| growth.layout(layout);
        let mut opaque = |bytes| growth.bytes(bytes);
        let mut creation = ValueSchemaCreation::new(storage.clone(), &mut request, &mut opaque);
        novarocks_functions::selected_copy::preflight_flat_array_structure_construction(
            ty.typed_binding().layout(),
            false,
            &mut creation,
            &mut |_| growth.check(),
        )
        .map_err(copy_failure)?;
        growth.array::<u8>(1)?;
        growth.check()?;
        let array: ArrayRef = Arc::new(arrow::array::BooleanArray::new(
            arrow::buffer::BooleanBuffer::new(arrow::buffer::Buffer::from(vec![1u8]), 0, 1),
            None,
        ));
        Some(pool(&array, ty, &storage, policy, &growth, control)?)
    } else {
        None
    };
    let effect_column = effect.zip(effect_type.as_ref());
    for (ordinal, value_type) in columns.iter().copied().chain(effect_column) {
        // Keep the original scalar constant-policy unit. Larger contiguous
        // pooling would apply max_rows/retained-buffer limits to a new set and
        // change availability. This compact array contains only this selected
        // occurrence; its complete checked value is addressed by local zero.
        for (position, row) in selected.iter().enumerate() {
            growth.check()?;
            let view = selection.locate(*row).ok_or(BuildError::InvalidSource(
                "COW selected row is outside its original owner",
            ))?;
            let batch = view.batch();
            let transient = Transient {
                parent: &growth,
                bytes: AtomicU64::new(0),
            };
            transient.array::<u64>(1)?;
            transient.array::<usize>(1)?;
            let mut indices = exact_vec(1)?;
            indices
                .push(u64::try_from(view.row_index()).map_err(|_| BuildError::ResourceExhausted)?);
            let mut output_positions = exact_vec(1)?;
            output_positions.push(position);
            let mut request = |layout| transient.layout(layout);
            let mut opaque = |bytes| transient.bytes(bytes);
            let mut creation = ValueSchemaCreation::new(storage.clone(), &mut request, &mut opaque);
            novarocks_functions::selected_copy::preflight_primitive_vec_adoption_construction_observed(&mut creation, &mut |_| growth.check()).map_err(copy_failure)?;
            growth.check()?;
            let indices = UInt64Array::from(indices);
            growth.check()?;
            let array = batch
                .columns()
                .get(ordinal as usize)
                .ok_or(BuildError::InvalidSource(
                    "COW signed column is outside its actual batch",
                ))?;
            let mut retained_request = |layout| growth.layout(layout);
            let mut retained_opaque = |bytes| growth.bytes(bytes);
            let mut retained = ValueSchemaCreation::new(
                storage.clone(),
                &mut retained_request,
                &mut retained_opaque,
            );
            novarocks_functions::selected_copy::preflight_take_construction_observed(
                array.as_ref(),
                CopyIndexView::UInt64(&indices),
                &mut creation,
                &mut retained,
                &mut |_| growth.check(),
            )
            .map_err(copy_failure)?;
            growth.check()?;
            let compact = arrow::compute::take(
                array.as_ref(),
                &indices,
                Some(arrow::compute::TakeOptions { check_bounds: true }),
            )
            .map_err(|error| BuildError::OriginalSemantic(error.to_string()))?;
            growth.check()?;
            // The existing shared representation author owns dictionary/plain
            // adaptation; semantic meaning remains the signed full declaration.
            let mut request = |layout| growth.layout(layout);
            let mut opaque =
                |bound: novarocks_functions::array_projection::ArrayProjectionOpaqueBound| {
                    growth.bytes(bound.bytes_upper)
                };
            let mut work = |_units| growth.check();
            let compact =
                novarocks_functions::array_projection::normalize_typed_array_plain_observed(
                    &compact,
                    value_type.typed_binding(),
                    &mut value_conversions,
                    &storage,
                    &mut |layout| growth.layout(layout),
                    &mut |_| growth.check(),
                    &mut |step| {
                        novarocks_functions::array_projection::observe_array_normalization_construction(
                        step,
                        &mut request,
                        &mut opaque,
                        &mut work,
                    )
                    },
                )?;
            let owner = pool(&compact, value_type, &storage, policy, &growth, control)?;
            for (local, &position) in output_positions.iter().enumerate() {
                growth.check()?;
                // Rewrite's matched protocol column precedes its effect.
                if protocol_tail && output[position].len() == columns.len() {
                    output[position].push(
                        matched
                            .as_ref()
                            .expect("protocol constant authored before data")
                            .value(0)
                            .map_err(BuildError::Constant)?,
                    );
                }
                output[position].push(
                    owner
                        .value(u32::try_from(local).map_err(|_| BuildError::ResourceExhausted)?)
                        .map_err(BuildError::Constant)?,
                );
            }
        }
    }
    growth.array::<Box<[ConstantValue]>>(output.len())?;
    let mut rows = exact_vec(output.len())?;
    for row in output {
        if row.len() != width {
            return Err(BuildError::InvalidSource(
                "COW compact source lost an output occurrence",
            ));
        }
        rows.push(row.into_boxed_slice());
    }
    growth.layout(
        novarocks_type_contract::owned_resources::layout::arc_layout(
            Layout::array::<Box<[ConstantValue]>>(rows.len())
                .map_err(|_| BuildError::ResourceExhausted)?,
        )
        .map_err(BuildError::Layout)?,
    )?;
    growth.check()?;
    let rows = Arc::from(rows);
    recipe
        .observe_materialized_transient(growth.maximum_transient.load(Ordering::Relaxed))
        .map_err(super::cow_closed_route::map_footprint)?;
    recipe
        .add_materialized_owned(growth.added.load(Ordering::Relaxed))
        .map_err(super::cow_closed_route::map_footprint)?;
    Ok(rows)
}
