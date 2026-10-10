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

//! ONE original array access normalizer, index algorithm and UTF8 projection.
use super::{
    array_literal_core::CollectionObservation,
    map_lookup_core::{cast_output, row_index},
};
use arrow_array::builder::StringBuilder;
use arrow_array::{Array, ArrayRef, BooleanArray, Int32Array, ListArray, StringArray, UInt32Array};
use arrow_schema::DataType;
use arrow_select::take::take;
#[derive(Debug)]
pub enum AccessFailure<E> {
    Data(String),
    Control(E),
}
fn observe<E>(
    observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
    event: CollectionObservation,
) -> Result<(), AccessFailure<E>> {
    observer(event).map_err(AccessFailure::Control)
}
pub struct ArrayAccessInputs {
    array: ArrayRef,
    subscript: ArrayRef,
    check: Option<ArrayRef>,
}
impl ArrayAccessInputs {
    pub fn new<E>(
        array: ArrayRef,
        mut subscript: ArrayRef,
        check: Option<ArrayRef>,
        observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
    ) -> Result<Self, AccessFailure<E>> {
        array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
            AccessFailure::Data(format!(
                "element_at expects ListArray, got {:?}",
                array.data_type()
            ))
        })?;
        if subscript.data_type() != &DataType::Int32 {
            observe(observer, CollectionObservation::OpaqueBoundary)?;
            // The ORIGINAL common::cast_with_special_rules always reaches Arrow's
            // default cast for target Int32. No date/decimal/VARCHAR branch applies.
            let normalized =
                cast_output(subscript.clone(), Some(&DataType::Int32)).map_err(|cause| {
                    AccessFailure::Data(format!(
                        "element_at: failed to cast {:?} -> Int32: {}",
                        subscript.data_type(),
                        cause
                    ))
                })?;
            subscript = normalized;
            observe(observer, CollectionObservation::OpaqueBoundary)?;
        }
        subscript
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| AccessFailure::Data("element_at expects INT subscript".to_string()))?;
        if let Some(flags) = &check {
            flags
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    AccessFailure::Data("element_at check flag must be BOOLEAN".to_string())
                })?;
        }
        Ok(Self {
            array,
            subscript,
            check,
        })
    }
    pub fn list(&self) -> &ListArray {
        self.array
            .as_any()
            .downcast_ref()
            .expect("original List admission")
    }
    fn subscript(&self) -> &Int32Array {
        self.subscript
            .as_any()
            .downcast_ref()
            .expect("original INT admission")
    }
    fn flags(&self) -> Option<&BooleanArray> {
        self.check.as_ref().map(|a| {
            a.as_any()
                .downcast_ref()
                .expect("original BOOLEAN admission")
        })
    }
    pub fn validate_legacy_lengths(&self) -> Result<(), String> {
        let list = self.list();
        let subscript_arr = self.subscript();
        let check_arr = self.flags();
        if subscript_arr.len() != 1 && subscript_arr.len() != list.len() {
            return Err(format!(
                "element_at subscript length mismatch: list rows={}, subscript rows={}",
                list.len(),
                subscript_arr.len()
            ));
        }
        if let Some(flags) = check_arr
            && flags.len() != 1
            && flags.len() != list.len()
        {
            return Err(format!(
                "element_at check flag length mismatch: list rows={}, check rows={}",
                list.len(),
                flags.len()
            ));
        }
        Ok(())
    }
    pub fn legacy_rows(&self, row: usize) -> AccessRows {
        AccessRows {
            list: row,
            subscript: row_index(row, self.subscript.len()),
            check: self.check.as_ref().map(|f| row_index(row, f.len())),
        }
    }
}
#[derive(Clone, Copy)]
pub struct AccessRows {
    pub list: usize,
    pub subscript: usize,
    pub check: Option<usize>,
}
pub fn lookup_observed<E>(
    inputs: &ArrayAccessInputs,
    selection: crate::Selection<'_>,
    mut rows: impl FnMut(usize, usize) -> Result<AccessRows, E>,
    output_type: Option<&DataType>,
    mut before_take: impl FnMut(&dyn Array, &[Option<u32>]) -> Result<(), E>,
    observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
) -> Result<ArrayRef, AccessFailure<E>> {
    let list = inputs.list();
    let subscript_arr = inputs.subscript();
    let check_arr = inputs.flags();
    let offsets = list.value_offsets();
    let values = list.values();
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    let mut indices = Vec::with_capacity(selection.len());
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    for (ordinal, row) in selection.iter().enumerate() {
        observe(observer, CollectionObservation::Step)?;
        let rows = rows(ordinal, row).map_err(AccessFailure::Control)?;
        let row = rows.list;
        let subscript_idx = rows.subscript;
        let check_idx = rows.check;
        let check_out_of_bounds = check_idx
            .map(|idx| {
                let flags = check_arr.expect("check_arr exists when check_idx exists");
                !flags.is_null(idx) && flags.value(idx)
            })
            .unwrap_or(false);
        if list.is_null(row) || subscript_arr.is_null(subscript_idx) {
            indices.push(None);
            continue;
        }
        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        let row_len = end - start;
        let subscript = subscript_arr.value(subscript_idx);
        if subscript <= 0 {
            if check_out_of_bounds {
                return Err(AccessFailure::Data(
                    "Array subscript start at 1".to_string(),
                ));
            }
            indices.push(None);
            continue;
        }
        let subscript = usize::try_from(subscript).map_err(|_| {
            AccessFailure::Data("element_at subscript conversion failed".to_string())
        })?;
        if subscript > row_len {
            if check_out_of_bounds {
                return Err(AccessFailure::Data(format!(
                    "Array subscript must be less than or equal to array length: {} > {}",
                    subscript, row_len
                )));
            }
            indices.push(None);
            continue;
        }
        let target = start + subscript - 1;
        let target = u32::try_from(target).map_err(|_| {
            AccessFailure::Data("element_at index exceeds UInt32 range".to_string())
        })?;
        indices.push(Some(target));
    }
    before_take(values.as_ref(), &indices).map_err(AccessFailure::Control)?;
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    let indices = UInt32Array::from(indices);
    let out =
        take(values.as_ref(), &indices, None).map_err(|e| AccessFailure::Data(e.to_string()))?;
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    let out = maybe_unquote_wrapped_utf8_observed(out, observer)?;
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    let out = cast_output(out, output_type).map_err(|cause| {
        AccessFailure::Data(format!("element_at: failed to cast output: {}", cause))
    })?;
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    Ok(out)
}
pub fn maybe_unquote_wrapped_utf8_observed<E>(
    array: ArrayRef,
    observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
) -> Result<ArrayRef, AccessFailure<E>> {
    if array.data_type() != &DataType::Utf8 {
        return Ok(array);
    }
    let arr = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| AccessFailure::Data("failed to downcast to StringArray".to_string()))?;
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    let mut builder = StringBuilder::new();
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    for row in 0..arr.len() {
        observe(observer, CollectionObservation::Step)?;
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        if arr.is_null(row) {
            builder.append_null();
            observe(observer, CollectionObservation::OpaqueBoundary)?;
            continue;
        }
        let value = arr.value(row);
        if value.len() >= 2 && value.starts_with('\"') && value.ends_with('\"') {
            let inner = &value[1..value.len() - 1];
            if !inner.contains('\"') && !inner.contains('\\') {
                builder.append_value(inner);
                observe(observer, CollectionObservation::OpaqueBoundary)?;
                continue;
            }
        }
        builder.append_value(value);
        observe(observer, CollectionObservation::OpaqueBoundary)?;
    }
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    let result = std::sync::Arc::new(builder.finish()) as ArrayRef;
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    Ok(result)
}
