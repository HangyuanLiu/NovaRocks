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
    pub fn legacy_rows(&self, row: usize) -> AppendRows {
        AppendRows {
            list: row_index(row, self.list().len()),
            target: row_index(row, self.targets.len()),
        }
    }
}
#[derive(Clone, Copy)]
pub struct AppendRows {
    pub list: usize,
    pub target: usize,
}
pub fn append_observed<E>(
    inputs: &ArrayAppendInputs,
    selection: crate::Selection<'_>,
    mut rows: impl FnMut(usize, usize) -> Result<AppendRows, E>,
    mut before_extend: impl FnMut(&dyn Array, usize, usize, i64) -> Result<(), E>,
    observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
) -> Result<ArrayRef, AppendFailure<E>> {
    let list = inputs.list();
    let values = &inputs.values;
    let targets = &inputs.targets;
    let output_field = inputs.output_field.clone();
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    let values_data = values.to_data();
    let targets_data = targets.to_data();
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    let mut mutable = MutableArrayData::new(vec![&values_data, &targets_data], false, 0);
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    let list_offsets = list.value_offsets();
    let mut out_offsets = Vec::with_capacity(selection.len() + 1);
    out_offsets.push(0_i32);
    let mut current: i64 = 0;
    let mut null_builder = NullBufferBuilder::new(selection.len());
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    for (ordinal, row) in selection.iter().enumerate() {
        observe(observer, CollectionObservation::Step)?;
        let rows = rows(ordinal, row).map_err(AppendFailure::Control)?;
        let list_row = rows.list;
        if list.is_null(list_row) {
            null_builder.append_null();
            out_offsets.push(current as i32);
            continue;
        }
        let start = list_offsets[list_row] as usize;
        let end = list_offsets[list_row + 1] as usize;
        if end > start {
            before_extend(values.as_ref(), start, end - start, current)
                .map_err(AppendFailure::Control)?;
            observe(observer, CollectionObservation::OpaqueBoundary)?;
            mutable.extend(0, start, end);
            observe(observer, CollectionObservation::OpaqueBoundary)?;
            current += (end - start) as i64;
        }
        let target_row = rows.target;
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
