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

//! ONE original List-to-List CAST body. Scheduling and child semantic schemas
//! remain owned by the caller; callbacks only consume an already evaluated array.
use arrow_array::{Array, ArrayRef, ListArray, new_empty_array, new_null_array};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, FieldRef};
use std::sync::Arc;

#[derive(Clone, Copy, Debug)]
pub enum ListCastObservation {
    Step,
    OpaqueBoundary,
}
#[derive(Debug)]
pub enum ListCastError<E> {
    Data(String),
    Host(E),
}

/// Original Null-source lifting; this does not infer a child logical identity.
pub fn null_source_observed<E>(
    len: usize,
    target: &DataType,
    observe: &mut dyn FnMut(ListCastObservation) -> Result<(), E>,
) -> Result<ArrayRef, ListCastError<E>> {
    observe(ListCastObservation::OpaqueBoundary).map_err(ListCastError::Host)?;
    let out = new_null_array(target, len);
    observe(ListCastObservation::OpaqueBoundary).map_err(ListCastError::Host)?;
    Ok(out)
}
pub fn null_source(len: usize, target: &DataType) -> ArrayRef {
    null_source_observed::<std::convert::Infallible>(len, target, &mut |_| Ok(())).unwrap_or_else(
        |error| match error {
            ListCastError::Host(never) => match never {},
            ListCastError::Data(_) => unreachable!("original Null lifting has no data error"),
        },
    )
}

/// Preserve the original physical null-count test, target Field, offsets,
/// full data errors and ListArray constructor panic. No decoder is introduced.
pub fn cast_observed<E>(
    array: &ArrayRef,
    target_field: &FieldRef,
    cast_child: &mut dyn FnMut(&ArrayRef, &DataType) -> Result<ArrayRef, ListCastError<E>>,
    observe: &mut dyn FnMut(ListCastObservation) -> Result<(), E>,
) -> Result<ArrayRef, ListCastError<E>> {
    observe(ListCastObservation::Step).map_err(ListCastError::Host)?;
    let list = array
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| ListCastError::Data("failed to downcast to ListArray".to_string()))?;
    // This is the original child carrier equality, not an identity of Fields.
    if list.values().data_type() == target_field.data_type() {
        observe(ListCastObservation::OpaqueBoundary).map_err(ListCastError::Host)?;
        let out: ArrayRef = Arc::new(ListArray::new(
            target_field.clone(),
            list.offsets().clone(),
            list.values().clone(),
            list.nulls().cloned(),
        ));
        observe(ListCastObservation::OpaqueBoundary).map_err(ListCastError::Host)?;
        return Ok(out);
    }
    let cast_values = if list.values().is_empty() {
        observe(ListCastObservation::OpaqueBoundary).map_err(ListCastError::Host)?;
        let out = new_empty_array(target_field.data_type());
        observe(ListCastObservation::OpaqueBoundary).map_err(ListCastError::Host)?;
        out
    } else if list.values().null_count() == list.values().len() {
        null_source_observed(list.values().len(), target_field.data_type(), observe)?
    } else {
        // NullArray has physical null_count()==0 in Arrow 58.2.0. Its nonempty
        // values therefore reach the caller's original Null-source branch.
        cast_child(list.values(), target_field.data_type())?
    };
    observe(ListCastObservation::OpaqueBoundary).map_err(ListCastError::Host)?;
    let mut offsets = Vec::with_capacity(list.value_offsets().len());
    observe(ListCastObservation::OpaqueBoundary).map_err(ListCastError::Host)?;
    for offset in list.value_offsets() {
        offsets.push(*offset);
        observe(ListCastObservation::Step).map_err(ListCastError::Host)?;
    }
    observe(ListCastObservation::OpaqueBoundary).map_err(ListCastError::Host)?;
    let out: ArrayRef = Arc::new(ListArray::new(
        target_field.clone(),
        OffsetBuffer::new(offsets.into()),
        cast_values,
        list.nulls().cloned(),
    ));
    observe(ListCastObservation::OpaqueBoundary).map_err(ListCastError::Host)?;
    Ok(out)
}

pub fn cast(
    array: &ArrayRef,
    target_field: &FieldRef,
    cast_child: &mut dyn FnMut(&ArrayRef, &DataType) -> Result<ArrayRef, String>,
) -> Result<ArrayRef, String> {
    cast_observed::<std::convert::Infallible>(
        array,
        target_field,
        &mut |child, target| cast_child(child, target).map_err(ListCastError::Data),
        &mut |_| Ok(()),
    )
    .map_err(|error| match error {
        ListCastError::Data(message) => message,
        ListCastError::Host(never) => match never {},
    })
}
