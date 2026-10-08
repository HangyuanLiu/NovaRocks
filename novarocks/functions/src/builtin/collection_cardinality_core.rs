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
//! Original CARDINALITY Map-first/List-second classification and sole shared offset body.
use super::{
    array_literal_core::CollectionObservation,
    collection_offset_count::{self, OffsetCountFailure},
};
use arrow_array::{Array, ArrayRef, ListArray, MapArray};
fn offsets(array: &dyn Array) -> Result<&[i32], String> {
    if let Some(map) = array.as_any().downcast_ref::<MapArray>() {
        Ok(map.value_offsets())
    } else if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        Ok(list.value_offsets())
    } else {
        Err(format!(
            "cardinality expects ARRAY or MAP, got {:?}",
            array.data_type()
        ))
    }
}
pub fn count_observed<E>(
    array: &dyn Array,
    selection: crate::Selection<'_>,
    row: impl FnMut(usize, usize) -> Result<usize, E>,
    observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
) -> Result<ArrayRef, OffsetCountFailure<E>> {
    collection_offset_count::count_observed(array, || offsets(array), selection, row, observer)
}
