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

//! ONE original array_append value/type/NULL and MutableArrayData author.
use super::{
    array_literal_core::CollectionObservation,
    map_lookup_core::{cast_output, row_index},
};
use arrow_array::{Array, ArrayRef, ListArray, make_array};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_data::transform::MutableArrayData;
use arrow_schema::{DataType, Field};
use std::sync::Arc;
#[derive(Debug)]
pub enum AppendFailure<E> {
    Data(String),
    Control(E),
}
fn observe<E>(
    observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
    event: CollectionObservation,
) -> Result<(), AppendFailure<E>> {
    observer(event).map_err(AppendFailure::Control)
}
/// The raw shell reports its actual legacy projection; selected always supplies
/// a frozen List field. No caller input type or default is synthesized here.
pub enum AppendOutputFacts<'a> {
    FrozenList(&'a Arc<Field>),
    OriginalInputList,
}
pub struct ArrayAppendInputs {
    array: ArrayRef,
    values: ArrayRef,
    targets: ArrayRef,
    output_field: Arc<Field>,
}
impl ArrayAppendInputs {
    pub fn new<E>(
        array: ArrayRef,
        target: ArrayRef,
        output: AppendOutputFacts<'_>,
        observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
    ) -> Result<Self, AppendFailure<E>> {
        let list = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
            AppendFailure::Data(format!(
                "array_append expects ListArray, got {:?}",
                array.data_type()
            ))
        })?;
        let output_field = match output {
            AppendOutputFacts::FrozenList(field) => field.clone(),
            AppendOutputFacts::OriginalInputList => match list.data_type() {
                DataType::List(field) => field.clone(),
                other => {
                    return Err(AppendFailure::Data(format!(
                        "array_append output type must be List, got {:?}",
                        other
                    )));
                }
            },
        };
        let target_item_type = output_field.data_type().clone();
        let mut values = list.values().clone();
        if values.data_type() != &target_item_type {
            observe(observer, CollectionObservation::OpaqueBoundary)?;
            values = cast_output(values.clone(), Some(&target_item_type)).map_err(|cause| {
                AppendFailure::Data(format!(
                    "array_append failed to cast element type {:?} -> {:?}: {}",
                    values.data_type(),
                    target_item_type,
                    cause
                ))
            })?;
            observe(observer, CollectionObservation::OpaqueBoundary)?;
        }
        let mut targets = target;
        if targets.data_type() != &target_item_type {
            observe(observer, CollectionObservation::OpaqueBoundary)?;
            targets = cast_output(targets.clone(), Some(&target_item_type)).map_err(|cause| {
                AppendFailure::Data(format!(
                    "array_append failed to cast target type {:?} -> {:?}: {}",
                    targets.data_type(),
                    target_item_type,
                    cause
                ))
            })?;
            observe(observer, CollectionObservation::OpaqueBoundary)?;
        }
        Ok(Self {
            array,
            values,
            targets,
            output_field,
        })
    }
    pub fn list(&self) -> &ListArray {
        self.array
            .as_any()
            .downcast_ref()
            .expect("original List admission")
    }
    /// Exact sources passed to the original two-source MutableArrayData constructor.
    pub fn constructor_sources(&self) -> [&dyn Array; 2] {
        [self.values.as_ref(), self.targets.as_ref()]
    }
    fn row_action(&self, rows: AppendRows) -> AppendRowAction {
        let list = self.list();
        if list.is_null(rows.list) {
            return AppendRowAction::NullParent;
        }
        let offsets = list.value_offsets();
        AppendRowAction::Copy {
            start: offsets[rows.list] as usize,
            end: offsets[rows.list + 1] as usize,
            target: rows.target,
        }
    }
    /// Produce the actual extension plan through the same original row author.
    /// This method allocates nothing and leaves plan scratch ownership to its caller.
    pub fn extensions_observed<E>(
        &self,
        selection: crate::Selection<'_>,
        mut rows: impl FnMut(usize, usize) -> Result<AppendRows, E>,
        mut emit: impl FnMut(usize, usize, usize) -> Result<(), E>,
        observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
    ) -> Result<(), AppendFailure<E>> {
        for (ordinal, row) in selection.iter().enumerate() {
            observe(observer, CollectionObservation::Step)?;
            let rows = rows(ordinal, row).map_err(AppendFailure::Control)?;
            match self.row_action(rows) {
                AppendRowAction::NullParent => {}
                AppendRowAction::Copy { start, end, target } => {
                    if end > start {
                        emit(0, start, end - start).map_err(AppendFailure::Control)?;
                    }
                    emit(1, target, 1).map_err(AppendFailure::Control)?;
                }
            }
        }
        Ok(())
    }
    pub fn legacy_rows(&self, row: usize) -> AppendRows {
        AppendRows {
            list: row_index(row, self.list().len()),
            target: row_index(row, self.targets.len()),
        }
    }
}
enum AppendRowAction {
    NullParent,
    Copy {
        start: usize,
        end: usize,
        target: usize,
    },
}
#[derive(Clone, Copy)]
pub struct AppendRows {
    pub list: usize,
    pub target: usize,
}
/// Legacy projection: no plan allocation, no preflight and original copy ordering.
/// Pure selected callers use the guarded entry point below.
pub fn append_observed<E>(
    inputs: &ArrayAppendInputs,
    selection: crate::Selection<'_>,
    rows: impl FnMut(usize, usize) -> Result<AppendRows, E>,
    before_extend: impl FnMut(&dyn Array, usize, usize, i64) -> Result<(), E>,
    observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
) -> Result<ArrayRef, AppendFailure<E>> {
    append_observed_guarded(inputs, selection, rows, |_| Ok(()), before_extend, observer)
}
pub fn append_observed_guarded<E>(
    inputs: &ArrayAppendInputs,
    selection: crate::Selection<'_>,
    mut rows: impl FnMut(usize, usize) -> Result<AppendRows, E>,
    mut before_construct: impl FnMut(&ArrayAppendInputs) -> Result<(), E>,
    mut before_extend: impl FnMut(&dyn Array, usize, usize, i64) -> Result<(), E>,
    observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
) -> Result<ArrayRef, AppendFailure<E>> {
    let values = &inputs.values;
    let targets = &inputs.targets;
    let output_field = inputs.output_field.clone();
    before_construct(inputs).map_err(AppendFailure::Control)?;
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    let values_data = values.to_data();
    let targets_data = targets.to_data();
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    let mut mutable = MutableArrayData::new(vec![&values_data, &targets_data], false, 0);
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    let mut out_offsets = Vec::with_capacity(selection.len() + 1);
    out_offsets.push(0_i32);
    let mut current: i64 = 0;
    let mut null_builder = NullBufferBuilder::new(selection.len());
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    for (ordinal, row) in selection.iter().enumerate() {
        observe(observer, CollectionObservation::Step)?;
        let rows = rows(ordinal, row).map_err(AppendFailure::Control)?;
        let (start, end, target_row) = match inputs.row_action(rows) {
            AppendRowAction::NullParent => {
                null_builder.append_null();
                out_offsets.push(current as i32);
                continue;
            }
            AppendRowAction::Copy { start, end, target } => (start, end, target),
        };
        if end > start {
            before_extend(values.as_ref(), start, end - start, current)
                .map_err(AppendFailure::Control)?;
            observe(observer, CollectionObservation::OpaqueBoundary)?;
            mutable.extend(0, start, end);
            observe(observer, CollectionObservation::OpaqueBoundary)?;
            current += (end - start) as i64;
        }
        before_extend(targets.as_ref(), target_row, 1, current).map_err(AppendFailure::Control)?;
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        mutable.extend(1, target_row, target_row + 1);
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        current += 1;
        if current > i32::MAX as i64 {
            return Err(AppendFailure::Data(
                "array_append offset overflow".to_string(),
            ));
        }
        out_offsets.push(current as i32);
        null_builder.append_non_null();
    }
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    let out_values = make_array(mutable.freeze());
    let out = ListArray::new(
        output_field,
        OffsetBuffer::new(out_offsets.into()),
        out_values,
        null_builder.finish(),
    );
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    Ok(Arc::new(out) as ArrayRef)
}
