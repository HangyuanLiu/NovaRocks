// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Required encoded children use logical NULL on active ancestor rows.
//! Arrow's safe ArrayData validation checks physical child masks, whereas its
//! high-level container constructors also check dictionary value NULLs.

use arrow_array::types::Int32Type;
use arrow_array::{
    Array, ArrayRef, DictionaryArray, FixedSizeListArray, LargeListArray, ListArray, MapArray,
    StructArray,
};
use arrow_schema::DataType;
use novarocks_type_contract::{MAX_VALUE_TYPE_DEPTH, ValueKind, ValueTypeRef};

use super::{ArrayNormalizationStep, ArrayPathStep};

pub fn validate_required_dictionary_children<E, F>(
    source: &ArrayRef,
    value: &ValueTypeRef,
    observe: &mut F,
) -> Result<(), E>
where
    E: From<String>,
    F: for<'a> FnMut(ArrayNormalizationStep<'a>) -> Result<(), E>,
{
    // Root dictionaries were checked independently with every visible key.
    // Ordinary scalar batches require neither paths nor another row scan.
    if !matches!(
        source.data_type(),
        DataType::Struct(_)
            | DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Map(_, _)
    ) {
        return Ok(());
    }
    let mut path = [ArrayPathStep::ListItem; MAX_VALUE_TYPE_DEPTH];
    visit_required_paths(source, source, value, &mut path, 0, observe)
}

fn physical<'a, T: 'static, E: From<String>>(source: &'a ArrayRef) -> Result<&'a T, E> {
    source.as_any().downcast_ref::<T>().ok_or_else(|| {
        E::from("typed required-child validation has a different physical wrapper".to_owned())
    })
}

fn visit_required_paths<E, F>(
    root: &ArrayRef,
    source: &ArrayRef,
    value: &ValueTypeRef,
    path: &mut [ArrayPathStep; MAX_VALUE_TYPE_DEPTH],
    depth: usize,
    observe: &mut F,
) -> Result<(), E>
where
    E: From<String>,
    F: for<'a> FnMut(ArrayNormalizationStep<'a>) -> Result<(), E>,
{
    observe(ArrayNormalizationStep::InspectType(source.data_type()))?;
    if depth >= MAX_VALUE_TYPE_DEPTH {
        return Err(E::from(
            "typed required-child validation exceeds its nesting profile".to_owned(),
        ));
    }
    if matches!(source.data_type(), DataType::Dictionary(_, _)) {
        if !value.nullable() && !source.is_empty() {
            let mut pending = 0;
            for start in (0..root.len()).step_by(1024) {
                let end = root.len().min(start.saturating_add(1024));
                observe(ArrayNormalizationStep::InspectConstructionWork { units: end - start })?;
                for row in start..end {
                    validate_active_path(root, &path[..depth], row, &mut pending, observe)?;
                }
            }
            if pending != 0 {
                observe(ArrayNormalizationStep::InspectConstructionWork { units: pending })?;
            }
        }
        return Ok(());
    }
    match value.kind() {
        ValueKind::Struct(fields) => {
            let array = physical::<StructArray, E>(source)?;
            for (index, field) in fields.iter().enumerate() {
                path[depth] = ArrayPathStep::StructField(index);
                visit_required_paths(
                    root,
                    array.column(index),
                    field.value_type(),
                    path,
                    depth + 1,
                    observe,
                )?;
            }
        }
        ValueKind::List(item) => {
            path[depth] = ArrayPathStep::ListItem;
            let values = match source.data_type() {
                DataType::List(_) => physical::<ListArray, E>(source)?.values(),
                DataType::LargeList(_) => physical::<LargeListArray, E>(source)?.values(),
                _ => {
                    return Err(E::from(
                        "typed List required-child path has a different carrier".to_owned(),
                    ));
                }
            };
            visit_required_paths(root, values, item, path, depth + 1, observe)?;
        }
        ValueKind::FixedSizeList { item, .. } => {
            path[depth] = ArrayPathStep::ListItem;
            visit_required_paths(
                root,
                physical::<FixedSizeListArray, E>(source)?.values(),
                item,
                path,
                depth + 1,
                observe,
            )?;
        }
        ValueKind::Map { key, value } => {
            let array = physical::<MapArray, E>(source)?;
            path[depth] = ArrayPathStep::MapKey;
            visit_required_paths(root, array.keys(), key, path, depth + 1, observe)?;
            path[depth] = ArrayPathStep::MapValue;
            visit_required_paths(root, array.values(), value, path, depth + 1, observe)?;
        }
        _ => {}
    }
    Ok(())
}

fn validate_active_path<E, F>(
    source: &ArrayRef,
    path: &[ArrayPathStep],
    row: usize,
    pending: &mut usize,
    observe: &mut F,
) -> Result<(), E>
where
    E: From<String>,
    F: for<'a> FnMut(ArrayNormalizationStep<'a>) -> Result<(), E>,
{
    *pending += 1;
    if *pending == 1024 {
        observe(ArrayNormalizationStep::InspectConstructionWork { units: *pending })?;
        *pending = 0;
    }
    if path.is_empty() {
        let dictionary = physical::<DictionaryArray<Int32Type>, E>(source)?;
        // The independent all-keys pass has already checked these indices,
        // including keys in masked rows. This pass only checks active NULLs.
        if dictionary.keys().is_null(row)
            || dictionary
                .values()
                .is_null(dictionary.keys().value(row) as usize)
        {
            return Err(E::from(
                "typed required dictionary child contains an unmasked logical null".to_owned(),
            ));
        }
        return Ok(());
    }
    if source.is_null(row) {
        return Ok(());
    }
    let rest = &path[1..];
    match path[0] {
        ArrayPathStep::StructField(index) => {
            validate_active_path(
                physical::<StructArray, E>(source)?.column(index),
                rest,
                row,
                pending,
                observe,
            )?;
        }
        ArrayPathStep::ListItem => {
            let (values, start, end) = match source.data_type() {
                DataType::List(_) => {
                    let array = physical::<ListArray, E>(source)?;
                    (
                        array.values(),
                        array.value_offsets()[row] as usize,
                        array.value_offsets()[row + 1] as usize,
                    )
                }
                DataType::LargeList(_) => {
                    let array = physical::<LargeListArray, E>(source)?;
                    (
                        array.values(),
                        array.value_offsets()[row] as usize,
                        array.value_offsets()[row + 1] as usize,
                    )
                }
                DataType::FixedSizeList(_, _) => {
                    let array = physical::<FixedSizeListArray, E>(source)?;
                    let start =
                        row.checked_mul(array.value_length() as usize)
                            .ok_or_else(|| {
                                E::from("typed required List child range overflow".to_owned())
                            })?;
                    let end = start
                        .checked_add(array.value_length() as usize)
                        .ok_or_else(|| {
                            E::from("typed required List child range overflow".to_owned())
                        })?;
                    (array.values(), start, end)
                }
                _ => {
                    return Err(E::from(
                        "typed required List path has a different carrier".to_owned(),
                    ));
                }
            };
            for index in start..end {
                validate_active_path(values, rest, index, pending, observe)?;
            }
        }
        ArrayPathStep::MapKey | ArrayPathStep::MapValue => {
            let array = physical::<MapArray, E>(source)?;
            let values = if path[0] == ArrayPathStep::MapKey {
                array.keys()
            } else {
                array.values()
            };
            for index in
                array.value_offsets()[row] as usize..array.value_offsets()[row + 1] as usize
            {
                validate_active_path(values, rest, index, pending, observe)?;
            }
        }
        _ => {
            return Err(E::from(
                "typed required-child path has an unsupported encoding step".to_owned(),
            ));
        }
    }
    Ok(())
}
