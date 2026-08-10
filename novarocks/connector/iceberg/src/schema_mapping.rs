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

//! Provider-owned Parquet field-identity and Iceberg name-mapping helpers.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::iceberg::spec::{MappedField, NameMapping};

pub fn schema_field_id_coverage(schema: &SchemaRef) -> Result<(usize, usize), String> {
    schema
        .fields()
        .iter()
        .try_fold((0usize, 0usize), |(identified, total), field| {
            let (field_identified, field_total) = field_id_coverage(field.as_ref())?;
            Ok::<_, String>((identified + field_identified, total + field_total))
        })
}

pub fn apply_name_mapping_to_schema(
    schema: &SchemaRef,
    name_mapping: &NameMapping,
) -> Result<SchemaRef, String> {
    let mappings = name_mapping
        .fields()
        .iter()
        .cloned()
        .map(Arc::new)
        .collect::<Vec<_>>();
    let fields = schema
        .fields()
        .iter()
        .map(|field| map_field_id_recursive(field.as_ref(), &mappings))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}

fn map_field_id_recursive(field: &Field, mappings: &[Arc<MappedField>]) -> Result<Field, String> {
    let mapped = mapped_field_for_name(mappings, field.name())?;
    let field_id = mapped.field_id().ok_or_else(|| {
        format!(
            "Iceberg name mapping entry for {} does not contain a field ID",
            field.name()
        )
    })?;
    let mut metadata = field.metadata().clone();
    metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string());
    let data_type = match field.data_type() {
        data_type if is_variant_struct_data_type(data_type) => data_type.clone(),
        DataType::Struct(children) => DataType::Struct(
            children
                .iter()
                .map(|child| map_field_id_recursive(child.as_ref(), mapped.fields()))
                .collect::<Result<Vec<_>, _>>()?
                .into(),
        ),
        DataType::List(child) => DataType::List(Arc::new(map_field_id_recursive(
            child.as_ref(),
            mapped.fields(),
        )?)),
        DataType::LargeList(child) => DataType::LargeList(Arc::new(map_field_id_recursive(
            child.as_ref(),
            mapped.fields(),
        )?)),
        DataType::FixedSizeList(child, size) => DataType::FixedSizeList(
            Arc::new(map_field_id_recursive(child.as_ref(), mapped.fields())?),
            *size,
        ),
        DataType::Map(entries, sorted) => {
            let DataType::Struct(entry_fields) = entries.data_type() else {
                return Err(format!(
                    "Iceberg mapped map field {} has non-struct entries",
                    field.name()
                ));
            };
            if entry_fields.len() != 2 {
                return Err(format!(
                    "Iceberg mapped map field {} must have key and value entries",
                    field.name()
                ));
            }
            let key = map_field_id_recursive(entry_fields[0].as_ref(), mapped.fields())?;
            let value = map_field_id_recursive(entry_fields[1].as_ref(), mapped.fields())?;
            let entries = Field::new(
                entries.name(),
                DataType::Struct(vec![key, value].into()),
                entries.is_nullable(),
            )
            .with_metadata(entries.metadata().clone());
            DataType::Map(Arc::new(entries), *sorted)
        }
        data_type => data_type.clone(),
    };
    Ok(Field::new(field.name(), data_type, field.is_nullable()).with_metadata(metadata))
}

fn mapped_field_for_name<'a>(
    mappings: &'a [Arc<MappedField>],
    name: &str,
) -> Result<&'a MappedField, String> {
    let mut matches = mappings
        .iter()
        .filter(|mapped| mapped.names().iter().any(|candidate| candidate == name));
    let mapped = matches
        .next()
        .ok_or_else(|| format!("Iceberg name mapping does not contain physical field {name}"))?;
    if matches.next().is_some() {
        return Err(format!(
            "Iceberg name mapping contains duplicate aliases for physical field {name}"
        ));
    }
    Ok(mapped)
}

fn field_id_coverage(field: &Field) -> Result<(usize, usize), String> {
    let identified = usize::from(parse_field_id(field)?.is_some());
    if is_variant_struct_data_type(field.data_type()) {
        return Ok((identified, 1));
    }
    let (children_identified, children_total) = match field.data_type() {
        DataType::Struct(children) => {
            children
                .iter()
                .try_fold((0usize, 0usize), |(identified, total), child| {
                    let (child_identified, child_total) = field_id_coverage(child.as_ref())?;
                    Ok::<_, String>((identified + child_identified, total + child_total))
                })?
        }
        DataType::List(child) | DataType::LargeList(child) | DataType::FixedSizeList(child, _) => {
            field_id_coverage(child.as_ref())?
        }
        DataType::Map(entries, _) => {
            let DataType::Struct(children) = entries.data_type() else {
                return Err(format!("map field {} has non-struct entries", field.name()));
            };
            children
                .iter()
                .try_fold((0usize, 0usize), |(identified, total), child| {
                    let (child_identified, child_total) = field_id_coverage(child.as_ref())?;
                    Ok::<_, String>((identified + child_identified, total + child_total))
                })?
        }
        _ => (0, 0),
    };
    Ok((identified + children_identified, 1 + children_total))
}

fn parse_field_id(field: &Field) -> Result<Option<i32>, String> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .map(|value| {
            value.parse::<i32>().map_err(|error| {
                format!(
                    "invalid Iceberg field ID metadata for column {}: {error}",
                    field.name()
                )
            })
        })
        .transpose()
}

pub fn is_variant_struct_data_type(data_type: &DataType) -> bool {
    let DataType::Struct(fields) = data_type else {
        return false;
    };
    if fields.is_empty() {
        return false;
    }
    let mut has_metadata = false;
    let mut has_value = false;
    let mut has_typed_value = false;
    for field in fields {
        match field.name().as_str() {
            "metadata" if is_binary_like(field.data_type()) => has_metadata = true,
            "value" if is_binary_like(field.data_type()) => has_value = true,
            "typed_value" => has_typed_value = true,
            _ => return false,
        }
    }
    has_metadata && (has_value || has_typed_value)
}

fn is_binary_like(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView
    )
}
