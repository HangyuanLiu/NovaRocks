// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.

//! Iceberg/Parquet VARIANT physical wire conversion.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, LargeBinaryArray, LargeBinaryBuilder,
    StructArray,
};
use arrow::datatypes::DataType;
use novarocks_types::value::variant::{VariantValue, is_variant_struct_data_type};
use parquet::variant::{VariantArray, unshred_variant};

fn binary_value_at_any(array: &ArrayRef, row: usize) -> Result<Option<&[u8]>, String> {
    if array.is_null(row) {
        return Ok(None);
    }
    if let Some(array) = array.as_any().downcast_ref::<BinaryArray>() {
        return Ok(Some(array.value(row)));
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeBinaryArray>() {
        return Ok(Some(array.value(row)));
    }
    if let Some(array) = array.as_any().downcast_ref::<BinaryViewArray>() {
        return Ok(Some(array.value(row)));
    }
    Err(format!(
        "expected a binary array for variant metadata/value, got {:?}",
        array.data_type()
    ))
}

/// Collapse an unshredded or shredded Parquet VARIANT struct into NovaRocks'
/// canonical `[size:u32 LE | metadata | value]` LargeBinary representation.
pub fn collapse_variant_struct_to_largebinary(
    source_array: &ArrayRef,
    column_name: &str,
) -> Result<ArrayRef, String> {
    let struct_array = source_array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| format!("expected StructArray for variant column `{column_name}`"))?;
    let has_typed_value = struct_array
        .fields()
        .iter()
        .any(|field| field.name() == "typed_value");

    let unshredded_holder;
    let (metadata_column, value_column): (ArrayRef, ArrayRef) = if has_typed_value {
        let variant = VariantArray::try_new(source_array.as_ref()).map_err(|error| {
            format!("variant column `{column_name}`: invalid shredded layout: {error}")
        })?;
        unshredded_holder = unshred_variant(&variant)
            .map_err(|error| format!("variant column `{column_name}`: unshred failed: {error}"))?;
        let value = unshredded_holder.value_field().ok_or_else(|| {
            format!("variant column `{column_name}`: unshred produced no value column")
        })?;
        (unshredded_holder.metadata_field().clone(), value.clone())
    } else {
        let mut metadata_index = None;
        let mut value_index = None;
        for (index, field) in struct_array.fields().iter().enumerate() {
            match field.name().as_str() {
                "metadata" => metadata_index = Some(index),
                "value" => value_index = Some(index),
                _ => {}
            }
        }
        let metadata_index = metadata_index.ok_or_else(|| {
            format!("variant column `{column_name}`: struct missing metadata field")
        })?;
        let value_index = value_index
            .ok_or_else(|| format!("variant column `{column_name}`: struct missing value field"))?;
        (
            struct_array.column(metadata_index).clone(),
            struct_array.column(value_index).clone(),
        )
    };

    let mut builder = LargeBinaryBuilder::new();
    for row in 0..struct_array.len() {
        if struct_array.is_null(row) {
            builder.append_null();
            continue;
        }
        match (
            binary_value_at_any(&metadata_column, row)?,
            binary_value_at_any(&value_column, row)?,
        ) {
            (Some(metadata), Some(value)) => {
                let serialized = VariantValue::create(metadata, value)
                    .map_err(|error| format!("variant column `{column_name}` row {row}: {error}"))?
                    .serialize();
                builder.append_value(serialized.as_slice());
            }
            _ => {
                return Err(format!(
                    "variant column `{column_name}` row {row}: missing metadata/value bytes (corrupt file or unsupported variant encoding)"
                ));
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}
