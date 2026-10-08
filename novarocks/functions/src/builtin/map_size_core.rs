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
//! ONE original arena-free MAP_SIZE root NULL/offset computation.
use super::array_literal_core::CollectionObservation;
use arrow_array::{Array, ArrayRef, Int32Array, MapArray};
use std::sync::Arc;
#[derive(Debug)]
pub enum MapSizeFailure<E> {
    Data(String),
    Control(E),
}
pub fn map_input(array: &dyn Array) -> Result<&MapArray, String> {
    array
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| format!("map_size expects MapArray, got {:?}", array.data_type()))
}
/// Callers provide already evaluated row addresses; raw uses All with identity mapping.
/// Opaque observations neither grant memory nor replace the original allocator.
pub fn count_observed<E>(
    map: &MapArray,
    selection: crate::Selection<'_>,
    mut row: impl FnMut(usize, usize) -> Result<usize, E>,
    observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
) -> Result<ArrayRef, MapSizeFailure<E>> {
    let offsets = map.value_offsets();
    observer(CollectionObservation::OpaqueBoundary).map_err(MapSizeFailure::Control)?;
    let mut out = Vec::with_capacity(selection.len());
    observer(CollectionObservation::OpaqueBoundary).map_err(MapSizeFailure::Control)?;
    for (ordinal, batch_row) in selection.iter().enumerate() {
        observer(CollectionObservation::Step).map_err(MapSizeFailure::Control)?;
        let row = row(ordinal, batch_row).map_err(MapSizeFailure::Control)?;
        if map.is_null(row) {
            out.push(None);
        } else {
            out.push(Some(offsets[row + 1] - offsets[row]));
        }
    }
    observer(CollectionObservation::OpaqueBoundary).map_err(MapSizeFailure::Control)?;
    let out = Arc::new(Int32Array::from(out)) as ArrayRef;
    observer(CollectionObservation::OpaqueBoundary).map_err(MapSizeFailure::Control)?;
    Ok(out)
}
