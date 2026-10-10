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
//! ONE original ArrayExpr construction author; no arena, type inference or decoder.
use arrow_array::{ArrayRef, ListArray, make_array, new_empty_array, new_null_array};
use arrow_buffer::OffsetBuffer;
use arrow_data::transform::MutableArrayData;
use arrow_schema::{DataType, Field, Fields};
use std::sync::Arc;
#[derive(Clone, Copy, Debug)]
pub enum CollectionObservation {
    Step,
    OpaqueBoundary,
}
#[derive(Debug)]
pub enum ArrayLiteralError {
    MissingOutput,
    NotList(DataType),
    ElementLength {
        expected: usize,
        actual: usize,
    },
    ElementCast {
        source: DataType,
        target: DataType,
        cause: String,
    },
    OffsetOverflow,
}
impl ArrayLiteralError {
    pub fn legacy_message(self) -> String {
        match self {
            Self::MissingOutput => "array_expr missing output type".to_string(),
            Self::NotList(ty) => format!("array_expr output type must be List, got {ty:?}"),
            Self::ElementLength { expected, actual } => {
                format!("array_expr element length mismatch: expected {expected}, got {actual}")
            }
            Self::ElementCast {
                source,
                target,
                cause,
            } => {
                format!("array_expr failed to cast element type {source:?} -> {target:?}: {cause}")
            }
            Self::OffsetOverflow => "array_expr offset overflow".to_string(),
        }
    }
}
#[derive(Debug)]
pub enum ConstructionFailure<E> {
    Data(ArrayLiteralError),
    Control(E),
}
fn observe<E>(
    observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
    event: CollectionObservation,
) -> Result<(), ConstructionFailure<E>> {
    observer(event).map_err(ConstructionFailure::Control)
}
fn relax_map_entry_nullability<E>(
    target_type: &DataType,
    source_type: &DataType,
    observe: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
) -> Result<DataType, E> {
    let (DataType::Map(target_map_field, ordered), DataType::Map(source_map_field, _)) =
        (target_type, source_type)
    else {
        return Ok(target_type.clone());
    };
    let DataType::Struct(target_entries) = target_map_field.data_type() else {
        return Ok(target_type.clone());
    };
    let DataType::Struct(source_entries) = source_map_field.data_type() else {
        return Ok(target_type.clone());
    };
    if target_entries.len() != source_entries.len() {
        return Ok(target_type.clone());
    }

    let mut adjusted_entries = target_entries.iter().cloned().collect::<Vec<_>>();
    let mut changed = false;
    for idx in 0..adjusted_entries.len() {
        observe(CollectionObservation::Step)?;
        if !target_entries[idx].is_nullable() && source_entries[idx].is_nullable() {
            adjusted_entries[idx] = Arc::new(Field::new(
                target_entries[idx].name(),
                target_entries[idx].data_type().clone(),
                true,
            ));
            changed = true;
        }
    }
    if !changed {
        return Ok(target_type.clone());
    }

    Ok(DataType::Map(
        Arc::new(Field::new(
            target_map_field.name(),
            DataType::Struct(Fields::from(adjusted_entries)),
            target_map_field.is_nullable(),
        )),
        *ordered,
    ))
}

/// Incremental original admission keeps child-evaluation order in the shell.
/// Every child is checked immediately before a later child can be evaluated.
pub struct ArrayLiteralInputs {
    field: Arc<Field>,
    element_type: DataType,
    num_rows: usize,
    num_elements: usize,
    raw_arrays: Vec<ArrayRef>,
}
impl ArrayLiteralInputs {
    pub fn from_output<E>(
        output_type: Option<&DataType>,
        num_rows: usize,
        num_elements: usize,
        observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
    ) -> Result<Self, ConstructionFailure<E>> {
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        let output_type = output_type
            .cloned()
            .ok_or(ConstructionFailure::Data(ArrayLiteralError::MissingOutput))?;
        let (field, element_type) = match output_type {
            DataType::List(field) => {
                let element_type = field.data_type().clone();
                (field, element_type)
            }
            other => return Err(ConstructionFailure::Data(ArrayLiteralError::NotList(other))),
        };
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        let raw_arrays = Vec::with_capacity(num_elements);
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        Ok(Self {
            field,
            element_type,
            num_rows,
            num_elements,
            raw_arrays,
        })
    }
    pub fn push_evaluated<E>(
        &mut self,
        array: ArrayRef,
        observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
    ) -> Result<(), ConstructionFailure<E>> {
        if array.len() != self.num_rows {
            return Err(ConstructionFailure::Data(
                ArrayLiteralError::ElementLength {
                    expected: self.num_rows,
                    actual: array.len(),
                },
            ));
        }
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        self.element_type =
            relax_map_entry_nullability(&self.element_type, array.data_type(), observer)
                .map_err(ConstructionFailure::Control)?;
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        self.raw_arrays.push(array);
        Ok(())
    }
    /// Legacy supplies its ORIGINAL special-cast port. Exact selected inputs
    /// already have the frozen item type, so that port is never invoked there.
    pub fn finish<E>(
        mut self,
        mut special_cast: impl FnMut(&ArrayRef, &DataType) -> Result<ArrayRef, String>,
        observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
    ) -> Result<ArrayRef, ConstructionFailure<E>> {
        if self.num_elements == 0 {
            observe(observer, CollectionObservation::OpaqueBoundary)?;
            let mut offsets = Vec::with_capacity(self.num_rows + 1);
            offsets.push(0_i32);
            for _ in 0..self.num_rows {
                observe(observer, CollectionObservation::Step)?;
                offsets.push(0_i32);
            }
            observe(observer, CollectionObservation::OpaqueBoundary)?;
            let values = new_empty_array(&self.element_type);
            let list = ListArray::new(self.field, OffsetBuffer::new(offsets.into()), values, None);
            observe(observer, CollectionObservation::OpaqueBoundary)?;
            return Ok(Arc::new(list));
        }
        if self.field.data_type() != &self.element_type {
            self.field = Arc::new(Field::new(
                self.field.name(),
                self.element_type.clone(),
                self.field.is_nullable(),
            ));
        }
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        let mut element_arrays = Vec::with_capacity(self.num_elements);
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        for array in self.raw_arrays {
            observe(observer, CollectionObservation::Step)?;
            let array = if array.data_type() == &self.element_type {
                array
            } else if array.data_type() == &DataType::Null && self.element_type != DataType::Null {
                observe(observer, CollectionObservation::OpaqueBoundary)?;
                let out = new_null_array(&self.element_type, self.num_rows);
                observe(observer, CollectionObservation::OpaqueBoundary)?;
                out
            } else {
                observe(observer, CollectionObservation::OpaqueBoundary)?;
                let out = special_cast(&array, &self.element_type);
                observe(observer, CollectionObservation::OpaqueBoundary)?;
                out.map_err(|cause| {
                    ConstructionFailure::Data(ArrayLiteralError::ElementCast {
                        source: array.data_type().clone(),
                        target: self.element_type.clone(),
                        cause,
                    })
                })?
            };
            element_arrays.push(array);
        }
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        let mut offsets = Vec::with_capacity(self.num_rows + 1);
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        offsets.push(0_i32);
        let mut current: i64 = 0;
        for _ in 0..self.num_rows {
            observe(observer, CollectionObservation::Step)?;
            current += self.num_elements as i64;
            if current > i32::MAX as i64 {
                return Err(ConstructionFailure::Data(ArrayLiteralError::OffsetOverflow));
            }
            offsets.push(current as i32);
        }
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        let mut data_storage = Vec::with_capacity(element_arrays.len());
        for arr in &element_arrays {
            observe(observer, CollectionObservation::Step)?;
            data_storage.push(arr.to_data());
        }
        let mut data_refs = Vec::with_capacity(data_storage.len());
        for data in &data_storage {
            observe(observer, CollectionObservation::Step)?;
            data_refs.push(data);
        }
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        let mut mutable = MutableArrayData::new(
            data_refs,
            true,
            self.num_rows.saturating_mul(self.num_elements),
        );
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        for row in 0..self.num_rows {
            for idx in 0..element_arrays.len() {
                observe(observer, CollectionObservation::Step)?;
                observe(observer, CollectionObservation::OpaqueBoundary)?;
                mutable.extend(idx, row, row + 1);
                observe(observer, CollectionObservation::OpaqueBoundary)?;
            }
        }
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        let values = make_array(mutable.freeze());
        let list = ListArray::new(self.field, OffsetBuffer::new(offsets.into()), values, None);
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        Ok(Arc::new(list))
    }
}
