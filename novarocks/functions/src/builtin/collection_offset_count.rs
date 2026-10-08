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
//! ONE original root-NULL and i32-offset counting body shared by CARDINALITY and MAP_SIZE.
use super::array_literal_core::CollectionObservation;
use arrow_array::{Array, ArrayRef, Int32Array};
use std::sync::Arc;
#[derive(Debug)]
pub enum OffsetCountFailure<E> {
    Data(String),
    Control(E),
}
/// The offset supplier preserves each raw shell's original classification/allocation order.
/// Arrays and offsets are exact borrowed facts; this function does not decode children.
pub fn count_observed<'a, E>(
    array: &dyn Array,
    offsets: impl FnOnce() -> Result<&'a [i32], String>,
    selection: crate::Selection<'_>,
    mut row: impl FnMut(usize, usize) -> Result<usize, E>,
    observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
) -> Result<ArrayRef, OffsetCountFailure<E>> {
    observer(CollectionObservation::OpaqueBoundary).map_err(OffsetCountFailure::Control)?;
    let mut out = Vec::with_capacity(selection.len());
    observer(CollectionObservation::OpaqueBoundary).map_err(OffsetCountFailure::Control)?;
    let offsets = offsets().map_err(OffsetCountFailure::Data)?;
    for (ordinal, batch_row) in selection.iter().enumerate() {
        observer(CollectionObservation::Step).map_err(OffsetCountFailure::Control)?;
        let row = row(ordinal, batch_row).map_err(OffsetCountFailure::Control)?;
        if array.is_null(row) {
            out.push(None);
        } else {
            out.push(Some(offsets[row + 1] - offsets[row]));
        }
    }
    observer(CollectionObservation::OpaqueBoundary).map_err(OffsetCountFailure::Control)?;
    let out = Arc::new(Int32Array::from(out)) as ArrayRef;
    observer(CollectionObservation::OpaqueBoundary).map_err(OffsetCountFailure::Control)?;
    Ok(out)
}

#[cfg(test)]
#[path = "collection_offset_count_tests.rs"]
mod tests;
