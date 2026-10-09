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
use arrow_array::{Array, ArrayRef, ListArray, StringArray, StructArray, UInt32Array};
use arrow_cast::cast;
use arrow_select::take::take;
use std::sync::Arc;

use std::fmt;

/// Exact original operation inputs. These observations do not grant memory.
pub(crate) enum ProjectionOperation<'a> {
    FieldNameCopy {
        text: &'a str,
    },
    Indices {
        values: &'a StructArray,
    },
    IndicesArray {
        rows: usize,
    },
    Take {
        values: &'a ArrayRef,
        indices: &'a UInt32Array,
    },
    TargetTypeClone {
        original: &'a arrow_schema::DataType,
    },
    Cast {
        values: &'a ArrayRef,
        target: &'a arrow_schema::DataType,
    },
    ListResult {
        list: &'a ListArray,
        values: &'a ArrayRef,
        field: &'a arrow_schema::FieldRef,
    },
}
/// The caller owns operation authorization and diagnostics. A port must keep
/// actual grants through the backing's real Drop; this trait is not a wallet.
pub(crate) trait ProjectionPort {
    type Failure;
    fn diagnostic(&mut self, original: fmt::Arguments<'_>) -> Self::Failure;
    fn arrow_error(&mut self, original: arrow_schema::ArrowError) -> Self::Failure;
    fn step(&mut self) -> Result<(), Self::Failure>;
    fn before_operation(&mut self, original: ProjectionOperation<'_>) -> Result<(), Self::Failure>;
    /// Only a successful operation observes its completion boundary.
    fn after_operation(&mut self) -> Result<(), Self::Failure>;
}
struct LegacyPort;
impl ProjectionPort for LegacyPort {
    type Failure = String;
    fn diagnostic(&mut self, original: fmt::Arguments<'_>) -> String {
        std::fmt::format(original)
    }
    fn arrow_error(&mut self, original: arrow_schema::ArrowError) -> String {
        original.to_string()
    }
    fn step(&mut self) -> Result<(), String> {
        Ok(())
    }
    fn before_operation(&mut self, _: ProjectionOperation<'_>) -> Result<(), String> {
        Ok(())
    }
    fn after_operation(&mut self) -> Result<(), String> {
        Ok(())
    }
}
/// Preserve v1 allocation/formatting policy and the original error prefix.
pub fn project(
    input: &ArrayRef,
    field_name_arr: &ArrayRef,
    output_type: Option<&arrow_schema::DataType>,
) -> Result<ArrayRef, String> {
    project_with_port(input, field_name_arr, output_type, &mut LegacyPort)
}
/// ONE original computation; the port changes ownership/control, never math.
pub(crate) fn project_with_port<P: ProjectionPort>(
    input: &ArrayRef,
    field_name_arr: &ArrayRef,
    output_type: Option<&arrow_schema::DataType>,
    port: &mut P,
) -> Result<ArrayRef, P::Failure> {
    let list = input.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        port.diagnostic(format_args!(
            "__array_struct_subfield expects ListArray, got {:?}",
            input.data_type()
        ))
    })?;
    let struct_values = list
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            port.diagnostic(format_args!(
                "__array_struct_subfield expects list values to be StructArray, got {:?}",
                list.values().data_type()
            ))
        })?;
    let field_name = parse_constant_field_name(field_name_arr.as_ref(), port)?;
    let mut field_idx = None;
    for (index, field) in struct_values.fields().iter().enumerate() {
        port.step()?;
        if field.name() == field_name.as_str() {
            field_idx = Some(index);
            break;
        }
    }
    let field_idx = field_idx.ok_or_else(|| {
        port.diagnostic(format_args!(
            "__array_struct_subfield field '{}' does not exist",
            field_name
        ))
    })?;
    let field_col = struct_values.column(field_idx);

    port.before_operation(ProjectionOperation::Indices {
        values: struct_values,
    })?;
    let mut indices = Vec::with_capacity(struct_values.len());
    port.after_operation()?;
    for row in 0..struct_values.len() {
        port.step()?;
        if struct_values.is_null(row) {
            indices.push(None);
        } else {
            let idx = u32::try_from(row).map_err(|_| {
                port.diagnostic(format_args!(
                    "__array_struct_subfield index exceeds UInt32 range"
                ))
            })?;
            indices.push(Some(idx));
        }
    }

    port.before_operation(ProjectionOperation::IndicesArray {
        rows: indices.len(),
    })?;
    let indices = UInt32Array::from(indices);
    port.after_operation()?;
    port.before_operation(ProjectionOperation::Take {
        values: field_col,
        indices: &indices,
    })?;
    let mut out_values =
        take(field_col.as_ref(), &indices, None).map_err(|e| port.arrow_error(e))?;
    port.after_operation()?;
    let output_field = match output_type {
        Some(arrow_schema::DataType::List(field)) => field.clone(),
        _ => match list.data_type() {
            arrow_schema::DataType::List(field) => field.clone(),
            other => {
                return Err(port.diagnostic(format_args!(
                    "__array_struct_subfield output type must be List, got {:?}",
                    other
                )));
            }
        },
    };
    port.before_operation(ProjectionOperation::TargetTypeClone {
        original: output_field.data_type(),
    })?;
    let target_item_type = output_field.data_type().clone();
    port.after_operation()?;
    if out_values.data_type() != &target_item_type {
        port.before_operation(ProjectionOperation::Cast {
            values: &out_values,
            target: &target_item_type,
        })?;
        out_values = cast(&out_values, &target_item_type).map_err(|e| {
            port.diagnostic(format_args!(
                "__array_struct_subfield: failed to cast output {:?} -> {:?}: {}",
                out_values.data_type(),
                target_item_type,
                e
            ))
        })?;
        port.after_operation()?;
    }

    port.before_operation(ProjectionOperation::ListResult {
        list,
        values: &out_values,
        field: &output_field,
    })?;
    let out = ListArray::new(
        output_field,
        list.offsets().clone(),
        out_values,
        list.nulls().cloned(),
    );
    let out = Arc::new(out) as ArrayRef;
    port.after_operation()?;
    Ok(out)
}
fn parse_constant_field_name<P: ProjectionPort>(
    field_name_arr: &dyn Array,
    port: &mut P,
) -> Result<String, P::Failure> {
    let arr = field_name_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            port.diagnostic(format_args!(
                "__array_struct_subfield field-name argument must be VARCHAR"
            ))
        })?;
    if arr.is_empty() {
        return Err(port.diagnostic(format_args!(
            "__array_struct_subfield field-name argument is empty"
        )));
    }
    let first = if arr.is_null(0) {
        return Err(port.diagnostic(format_args!(
            "__array_struct_subfield field-name argument must be non-null"
        )));
    } else {
        arr.value(0)
    };
    for i in 1..arr.len() {
        port.step()?;
        if arr.is_null(i) || arr.value(i) != first {
            return Err(port.diagnostic(format_args!(
                "__array_struct_subfield field-name argument must be constant"
            )));
        }
    }
    port.before_operation(ProjectionOperation::FieldNameCopy { text: first })?;
    let result = first.to_string();
    port.after_operation()?;
    Ok(result)
}

#[cfg(test)]
#[path = "array_projection_operation_before_tests.rs"]
mod array_projection_operation_before_tests;
