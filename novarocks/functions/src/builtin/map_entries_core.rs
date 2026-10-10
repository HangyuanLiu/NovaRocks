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
//! ONE original MAP_ENTRIES Map-to-List projection and original output cast.
use super::array_literal_core::CollectionObservation;
use arrow_array::{Array, ArrayRef, ListArray, MapArray};
use arrow_schema::{DataType, Field};
use std::sync::Arc;
#[derive(Debug)]
pub enum MapEntriesFailure<E> {
    Data(String),
    Control(E),
}
/// Array admission is the same original raw downcast; no key/value decoder is involved.
pub fn project_observed<E>(
    map_arr: &ArrayRef,
    output_type: Option<&DataType>,
    observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
) -> Result<ArrayRef, MapEntriesFailure<E>> {
    let map = map_arr.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
        MapEntriesFailure::Data(format!(
            "map_entries expects MapArray, got {:?}",
            map_arr.data_type()
        ))
    })?;
    observer(CollectionObservation::OpaqueBoundary).map_err(MapEntriesFailure::Control)?;
    let entries = Arc::new(map.entries().clone()) as ArrayRef;
    let list = ListArray::new(
        Arc::new(Field::new("item", map.entries().data_type().clone(), true)),
        map.offsets().clone(),
        entries,
        map.nulls().cloned(),
    );
    let out = Arc::new(list) as ArrayRef;
    observer(CollectionObservation::OpaqueBoundary).map_err(MapEntriesFailure::Control)?;
    let out = super::map_lookup_core::cast_output(out, output_type).map_err(|cause| {
        MapEntriesFailure::Data(format!("map_entries: failed to cast output: {cause}"))
    })?;
    observer(CollectionObservation::OpaqueBoundary).map_err(MapEntriesFailure::Control)?;
    Ok(out)
}
/// Raw wrapper uses the same core with no runtime authority or observation.
pub fn project(map_arr: &ArrayRef, output_type: Option<&DataType>) -> Result<ArrayRef, String> {
    project_observed(map_arr, output_type, &mut |_| Ok::<_, String>(())).map_err(|failure| {
        match failure {
            MapEntriesFailure::Data(text) | MapEntriesFailure::Control(text) => text,
        }
    })
}
