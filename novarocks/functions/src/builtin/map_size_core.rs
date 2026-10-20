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
use super::collection_offset_count::OffsetCountFailure;
use arrow_array::{Array, ArrayRef, MapArray};
pub fn map_input(array: &dyn Array) -> Result<&MapArray, crate::KernelFailure> {
    array.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
        super::collection_arrow::contract(format_args!(
            "map_size expects MapArray, got {:?}",
            array.data_type()
        ))
    })
}
/// Callers provide already evaluated row addresses; raw uses All with identity mapping.
/// Opaque observations neither grant memory nor replace the original allocator.
pub fn count_observed<E>(
    map: &MapArray,
    selection: crate::Selection<'_>,
    row: impl FnMut(usize, usize) -> Result<usize, E>,
    observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
) -> Result<ArrayRef, OffsetCountFailure<E>> {
    let offsets = map.value_offsets();
    super::collection_offset_count::count_observed(map, || Ok(offsets), selection, row, observer)
}
