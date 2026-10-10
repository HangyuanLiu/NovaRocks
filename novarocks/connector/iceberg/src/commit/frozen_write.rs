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

//! Provider-owned reconstruction of a DATA writer context from frozen handle facts.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef as ArrowSchemaRef};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};

use crate::access_binding::IcebergReadBinding;
use crate::commit::data_writer::{PARQUET_ROW_GROUP_SIZE_BYTES_PROPERTY, StagedWriteContext};
use crate::commit::write_io::build_staged_file_io;
use crate::iceberg::spec::{
    ListType, MapType, NestedField, PartitionSpec, PrimitiveType, SortOrder, StructType,
    TableMetadata, TableMetadataBuilder, Transform, Type, UnboundPartitionSpec,
};
use crate::scan_model::IcebergSchemaDef;
use crate::schema_mapping::annotate_schema_from_scan_model;

/// Secret-free DATA writer facts decoded from one exact-generation handle.
#[derive(Clone, Debug)]
pub struct FrozenDataWriteFacts {
    pub table_location: String,
    pub data_location: String,
    pub target_partition_spec_id: i32,
    pub partition_source_column_names: Vec<String>,
    pub partition_column_names: Vec<String>,
    pub transform_exprs: Vec<String>,
    pub data_input_schema: IcebergSchemaDef,
    pub parquet_row_group_size_bytes: Option<u64>,
}

#[derive(Debug)]
pub(crate) enum FrozenWriteSchemaError {
    Source(String),
    Control(CompileControlError),
}
impl std::fmt::Display for FrozenWriteSchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Source(message) => f.write_str(message),
            Self::Control(error) => std::fmt::Display::fmt(error, f),
        }
    }
}
impl std::error::Error for FrozenWriteSchemaError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Source(_) => None,
            Self::Control(error) => Some(error),
        }
    }
}
impl From<CompileControlError> for FrozenWriteSchemaError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}

/// Pure schema preparation only: this carries no FileIO, access binding or
/// runtime capability, and does not grant allocation or execution permission.
pub(crate) struct PreparedFrozenWriteSchema {
    pub(crate) metadata: TableMetadata,
    pub(crate) writer_schema: Arc<crate::iceberg::spec::Schema>,
    pub(crate) arrow_schema: ArrowSchemaRef,
}

struct SchemaWork<'a>(Option<CompileCheckpoints<'a>>);
impl SchemaWork<'_> {
    fn step(&mut self) -> Result<(), FrozenWriteSchemaError> {
        if let Some(work) = &mut self.0 {
            work.step()?;
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<(), FrozenWriteSchemaError> {
        if let Some(work) = &mut self.0 {
            work.flush()?;
        }
        Ok(())
    }
    fn opaque<T>(&mut self, operation: impl FnOnce() -> T) -> Result<T, FrozenWriteSchemaError> {
        self.flush()?;
        let value = operation();
        self.flush()?;
        Ok(value)
    }
    fn opaque_source<T>(
        &mut self,
        operation: impl FnOnce() -> Result<T, String>,
    ) -> Result<T, FrozenWriteSchemaError> {
        self.flush()?;
        let result = operation().map_err(FrozenWriteSchemaError::Source);
        self.flush()?;
        result
    }
}

/// The complete-input pure provider path always uses the original control.
/// Existing writer bounds make opaque annotation/vendor/JSON calls finite;
/// their internals and all temporary allocations remain host obligations.
pub(crate) fn prepare_frozen_write_schema(
    input_schema: &ArrowSchemaRef,
    facts: &FrozenDataWriteFacts,
    control: &dyn PureCompileControl,
) -> Result<PreparedFrozenWriteSchema, FrozenWriteSchemaError> {
    let mut work = SchemaWork(Some(CompileCheckpoints::try_new(
        control,
        CompilePhase::ProviderValidation,
    )?));
    let result = (|| {
        preflight_schema_inputs(input_schema, facts, &mut work)?;
        prepare_schema_core(input_schema, facts, &mut work)
    })();
    if matches!(&result, Err(FrozenWriteSchemaError::Control(_))) {
        return result;
    }
    work.flush()?;
    result
}

fn prepare_schema_core(
    input_schema: &ArrowSchemaRef,
    facts: &FrozenDataWriteFacts,
    work: &mut SchemaWork<'_>,
) -> Result<PreparedFrozenWriteSchema, FrozenWriteSchemaError> {
    let annotated_schema = work.opaque_source(|| {
        annotate_schema_from_scan_model(input_schema, &facts.data_input_schema)
    })?;
    let writer_schema = Arc::new(iceberg_schema_from_arrow_schema(
        annotated_schema.as_ref(),
        work,
    )?);
    let arrow_schema = scalar_integer_storage_schema_with_work(&annotated_schema, work)?;
    let metadata = build_target_table_metadata(facts, writer_schema.as_ref(), work)?;
    Ok(PreparedFrozenWriteSchema {
        metadata,
        writer_schema,
        arrow_schema,
    })
}

/// Build the provider writer context exclusively from the sealed handle and
/// local execution binding.  No FE plan, credentials, or Core runtime state is
/// reconstructed here.
pub fn staged_write_context_from_frozen_facts(
    binding: &IcebergReadBinding,
    input_schema: &ArrowSchemaRef,
    facts: FrozenDataWriteFacts,
) -> Result<StagedWriteContext, String> {
    let prepared =
        prepare_schema_core(input_schema, &facts, &mut SchemaWork(None)).map_err(|error| {
            match error {
                FrozenWriteSchemaError::Source(message) => message,
                FrozenWriteSchemaError::Control(_) => {
                    unreachable!("legacy schema preparation has no control port")
                }
            }
        })?;
    let file_io = build_staged_file_io(binding, &facts.data_location)?;
    StagedWriteContext::from_parts_with_partition_spec_id(
        prepared.metadata,
        file_io,
        prepared.writer_schema,
        prepared.arrow_schema,
        facts.target_partition_spec_id,
    )
}

/// Iceberg INT has a four-byte storage carrier even when the signed SQL input
/// is TINYINT or SMALLINT. The writer's existing reannotation performs the
/// lossless conversion; the Parquet schema must describe the same carrier.
pub(super) fn scalar_integer_storage_schema(schema: &ArrowSchemaRef) -> ArrowSchemaRef {
    scalar_integer_storage_schema_with_work(schema, &mut SchemaWork(None))
        .unwrap_or_else(|_| unreachable!("legacy storage-schema conversion has no control port"))
}

fn scalar_integer_storage_schema_with_work(
    schema: &ArrowSchemaRef,
    work: &mut SchemaWork<'_>,
) -> Result<ArrowSchemaRef, FrozenWriteSchemaError> {
    fn field(
        field: &Field,
        work: &mut SchemaWork<'_>,
    ) -> Result<Arc<Field>, FrozenWriteSchemaError> {
        let cloned = work.opaque(|| field.clone())?;
        let converted = data_type(field.data_type(), work)?;
        let result = Arc::new(cloned.with_data_type(converted));
        work.step()?;
        Ok(result)
    }
    fn data_type(
        value: &DataType,
        work: &mut SchemaWork<'_>,
    ) -> Result<DataType, FrozenWriteSchemaError> {
        let result = match value {
            DataType::Int8 | DataType::Int16 => DataType::Int32,
            DataType::Struct(fields) => {
                let mut converted = Vec::with_capacity(fields.len());
                for value in fields {
                    converted.push(field(value, work)?);
                }
                DataType::Struct(converted.into())
            }
            DataType::List(element) => DataType::List(field(element, work)?),
            DataType::Map(entries, sorted) => DataType::Map(field(entries, work)?, *sorted),
            other => work.opaque(|| other.clone())?,
        };
        work.step()?;
        Ok(result)
    }
    let mut fields = Vec::with_capacity(schema.fields().len());
    for value in schema.fields() {
        fields.push(field(value, work)?);
    }
    let metadata = work.opaque(|| schema.metadata().clone())?;
    let result = Arc::new(Schema::new_with_metadata(fields, metadata));
    work.step()?;
    Ok(result)
}

/// Apply the existing writer schema envelope before opaque library calls.
/// The legacy runtime's acceptance and ordinary diagnostics remain unchanged;
/// its input has already crossed the runtime handle/schema admission owners.
fn preflight_schema_inputs(
    schema: &ArrowSchemaRef,
    facts: &FrozenDataWriteFacts,
    work: &mut SchemaWork<'_>,
) -> Result<(), FrozenWriteSchemaError> {
    use novarocks_connector_contract::{
        MAX_CONNECTOR_WRITE_INPUT_FIELDS, MAX_CONNECTOR_WRITER_HANDLE_BYTES,
        MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES, validate_write_field_schema,
    };
    fn add(bytes: &mut usize, amount: usize) -> Result<(), FrozenWriteSchemaError> {
        *bytes = bytes
            .checked_add(amount)
            .filter(|n| *n <= MAX_CONNECTOR_WRITER_HANDLE_BYTES)
            .ok_or_else(|| {
                FrozenWriteSchemaError::Source(
                    "frozen Iceberg writer facts exceed the writer-handle byte envelope"
                        .to_string(),
                )
            })?;
        Ok(())
    }
    fn text(
        value: &str,
        bytes: &mut usize,
        work: &mut SchemaWork<'_>,
    ) -> Result<(), FrozenWriteSchemaError> {
        add(bytes, value.len())?;
        work.step()
    }
    fn frozen_fields(
        fields: &[crate::scan_model::IcebergSchemaFieldDef],
        bytes: &mut usize,
        work: &mut SchemaWork<'_>,
    ) -> Result<(), FrozenWriteSchemaError> {
        // These are complete private source facts, including unprojected
        // fields. Public Arrow projection limits do not constrain this tree.
        // Borrowed iterator-stack growth still needs host allocation admission.
        let mut pending = vec![fields.iter()];
        while let Some(frame) = pending.last_mut() {
            let Some(field) = frame.next() else {
                pending.pop();
                work.step()?;
                continue;
            };
            text(&field.name, bytes, work)?;
            for value in [&field.initial_default_json, &field.write_default_json]
                .into_iter()
                .flatten()
            {
                text(value, bytes, work)?;
            }
            let has_children = !field.children.is_empty();
            work.step()?;
            if has_children {
                pending.push(field.children.iter());
            }
        }
        Ok(())
    }
    if schema.fields().len() > MAX_CONNECTOR_WRITE_INPUT_FIELDS {
        return Err(FrozenWriteSchemaError::Source(
            "Iceberg writer input exceeds the writer field count limit".to_string(),
        ));
    }
    let mut schema_bytes = 0;
    for field in schema.fields() {
        work.opaque_source(|| {
            validate_write_field_schema(field, 1, &mut schema_bytes).map_err(|e| e.to_string())
        })?;
        work.step()?;
    }
    for (key, value) in schema.metadata() {
        schema_bytes = schema_bytes
            .checked_add(key.len())
            .and_then(|n| n.checked_add(value.len()))
            .filter(|n| *n <= MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES)
            .ok_or_else(|| {
                FrozenWriteSchemaError::Source(
                    "Iceberg writer schema metadata exceeds the writer byte envelope".to_string(),
                )
            })?;
        work.step()?;
    }
    let mut bytes = 0;
    text(&facts.table_location, &mut bytes, work)?;
    text(&facts.data_location, &mut bytes, work)?;
    frozen_fields(&facts.data_input_schema.fields, &mut bytes, work)?;
    for values in [
        &facts.partition_source_column_names,
        &facts.partition_column_names,
        &facts.transform_exprs,
    ] {
        for value in values {
            text(value, &mut bytes, work)?;
            work.step()?;
        }
    }
    Ok(())
}

fn build_target_table_metadata(
    facts: &FrozenDataWriteFacts,
    writer_schema: &crate::iceberg::spec::Schema,
    work: &mut SchemaWork<'_>,
) -> Result<TableMetadata, FrozenWriteSchemaError> {
    let partition_spec = build_staged_partition_spec(
        writer_schema,
        facts.target_partition_spec_id,
        &facts.partition_source_column_names,
        &facts.partition_column_names,
        &facts.transform_exprs,
        work,
    )?;
    let mut properties = std::collections::HashMap::new();
    let data_location = work.opaque(|| facts.data_location.clone())?;
    properties.insert("write.data.path".to_string(), data_location);
    work.step()?;
    if let Some(value) = facts.parquet_row_group_size_bytes {
        properties.insert(
            PARQUET_ROW_GROUP_SIZE_BYTES_PROPERTY.to_string(),
            value.to_string(),
        );
        work.step()?;
    }
    let metadata = work.opaque_source(|| {
        TableMetadataBuilder::new(
            writer_schema.clone(),
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            facts.table_location.clone(),
            crate::iceberg::spec::FormatVersion::V2,
            properties,
        )
        .map_err(|error| format!("build staged Iceberg table metadata: {error}"))?
        .add_current_schema(writer_schema.clone())
        .map_err(|error| format!("add staged Iceberg writer schema: {error}"))?
        .add_default_partition_spec(partition_spec)
        .map_err(|error| format!("add staged Iceberg partition spec: {error}"))?
        .build()
        .map_err(|error| format!("finalize staged Iceberg table metadata: {error}"))
        .map(|built| built.metadata)
    })?;
    retag_default_partition_spec_id(
        metadata,
        facts.target_partition_spec_id,
        &facts.partition_column_names,
        work,
    )
}

fn iceberg_schema_from_arrow_schema(
    schema: &Schema,
    work: &mut SchemaWork<'_>,
) -> Result<crate::iceberg::spec::Schema, FrozenWriteSchemaError> {
    let mut fields = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        fields.push(iceberg_nested_field_from_arrow_field(field, work)?);
    }
    work.opaque_source(|| {
        crate::iceberg::spec::Schema::builder()
            .with_schema_id(1)
            .with_fields(fields)
            .build()
            .map_err(|error| format!("build staged Iceberg writer schema: {error}"))
    })
}

fn iceberg_nested_field_from_arrow_field(
    field: &Field,
    work: &mut SchemaWork<'_>,
) -> Result<crate::iceberg::spec::NestedFieldRef, FrozenWriteSchemaError> {
    let field_id = work
        .opaque_source(|| crate::schema_mapping::field_id_for_arrow_field(field))?
        .ok_or_else(|| {
            FrozenWriteSchemaError::Source(format!(
                "Iceberg writer field {} is missing parquet field ID metadata",
                field.name()
            ))
        })?;
    let field_type = iceberg_type_from_arrow_type(field.data_type(), work)?;
    let result = work.opaque(|| {
        Arc::new(NestedField::new(
            field_id,
            field.name(),
            field_type,
            !field.is_nullable(),
        ))
    })?;
    work.step()?;
    Ok(result)
}

fn iceberg_type_from_arrow_type(
    data_type: &DataType,
    work: &mut SchemaWork<'_>,
) -> Result<Type, FrozenWriteSchemaError> {
    use arrow::datatypes::TimeUnit;
    let primitive = match data_type {
        DataType::Boolean => Some(PrimitiveType::Boolean),
        DataType::Int8 | DataType::Int16 | DataType::Int32 => Some(PrimitiveType::Int),
        DataType::Int64 => Some(PrimitiveType::Long),
        DataType::Float32 => Some(PrimitiveType::Float),
        DataType::Float64 => Some(PrimitiveType::Double),
        DataType::Decimal128(precision, scale) => Some(PrimitiveType::Decimal {
            precision: (*precision).into(),
            scale: u32::try_from(*scale).map_err(|_| {
                FrozenWriteSchemaError::Source(format!(
                    "Iceberg writer decimal scale {scale} cannot convert to u32"
                ))
            })?,
        }),
        DataType::Date32 => Some(PrimitiveType::Date),
        DataType::Time64(TimeUnit::Microsecond) => Some(PrimitiveType::Time),
        DataType::Timestamp(TimeUnit::Microsecond, None) => Some(PrimitiveType::Timestamp),
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => Some(PrimitiveType::Timestamptz),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => Some(PrimitiveType::TimestampNs),
        DataType::Timestamp(TimeUnit::Nanosecond, Some(_)) => Some(PrimitiveType::TimestamptzNs),
        DataType::Utf8 | DataType::LargeUtf8 => Some(PrimitiveType::String),
        DataType::Binary => Some(PrimitiveType::Binary),
        DataType::LargeBinary => Some(PrimitiveType::Variant),
        DataType::FixedSizeBinary(size) => Some(PrimitiveType::Fixed(
            u64::try_from(*size).map_err(|_| {
                FrozenWriteSchemaError::Source(format!(
                    "Iceberg writer fixed binary width {size} cannot convert to u64"
                ))
            })?,
        )),
        _ => None,
    };
    if let Some(primitive) = primitive {
        work.step()?;
        return Ok(Type::Primitive(primitive));
    }
    let result = match data_type {
        DataType::Struct(fields) => {
            let mut converted = Vec::with_capacity(fields.len());
            for field in fields {
                converted.push(iceberg_nested_field_from_arrow_field(field, work)?);
            }
            Type::Struct(StructType::new(converted))
        }
        DataType::List(element) => Type::List(ListType::new(
            iceberg_nested_field_from_arrow_field(element, work)?,
        )),
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(FrozenWriteSchemaError::Source(format!(
                    "Iceberg MAP entries field must be Struct, got {:?}",
                    entries.data_type()
                )));
            };
            if fields.len() != 2 {
                return Err(FrozenWriteSchemaError::Source(format!(
                    "Iceberg MAP entries Struct must have 2 fields, got {}",
                    fields.len()
                )));
            }
            Type::Map(MapType::new(
                iceberg_nested_field_from_arrow_field(&fields[0], work)?,
                iceberg_nested_field_from_arrow_field(&fields[1], work)?,
            ))
        }
        other => {
            return Err(FrozenWriteSchemaError::Source(format!(
                "unsupported Arrow type for staged Iceberg writer schema: {other:?}"
            )));
        }
    };
    work.step()?;
    Ok(result)
}

fn build_staged_partition_spec(
    schema: &crate::iceberg::spec::Schema,
    partition_spec_id: i32,
    source_column_names: &[String],
    partition_column_names: &[String],
    transform_exprs: &[String],
    work: &mut SchemaWork<'_>,
) -> Result<UnboundPartitionSpec, FrozenWriteSchemaError> {
    if source_column_names.len() != partition_column_names.len()
        || source_column_names.len() != transform_exprs.len()
    {
        return Err(FrozenWriteSchemaError::Source(format!(
            "Iceberg writer partition metadata mismatch: sources={} names={} transforms={}",
            source_column_names.len(),
            partition_column_names.len(),
            transform_exprs.len()
        )));
    }
    let mut builder = UnboundPartitionSpec::builder().with_spec_id(partition_spec_id);
    for ((source_name, partition_name), transform_expr) in source_column_names
        .iter()
        .zip(partition_column_names)
        .zip(transform_exprs)
    {
        let field = work.opaque_source(|| schema.field_by_name_case_insensitive(source_name)
            .ok_or_else(|| format!("Iceberg writer partition source column {source_name} is missing from schema")))?;
        let transform = work.opaque_source(|| parse_partition_transform(transform_expr))?;
        builder = work.opaque_source(|| {
            builder
                .add_partition_field(field.id, partition_name, transform)
                .map_err(|error| format!("build staged Iceberg partition field: {error}"))
        })?;
        work.step()?;
    }
    work.opaque(|| builder.build())
}

fn parse_partition_transform(raw: &str) -> Result<Transform, String> {
    let normalized = raw.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "identity" => Ok(Transform::Identity),
        "year" => Ok(Transform::Year),
        "month" => Ok(Transform::Month),
        "day" => Ok(Transform::Day),
        "hour" => Ok(Transform::Hour),
        "void" => Ok(Transform::Void),
        _ => {
            if let Some(width) = parse_transform_arg(&normalized, "bucket")? {
                return Ok(Transform::Bucket(width));
            }
            if let Some(width) = parse_transform_arg(&normalized, "truncate")? {
                return Ok(Transform::Truncate(width));
            }
            Err(format!(
                "unsupported Iceberg partition transform for writer: {raw}"
            ))
        }
    }
}
fn parse_transform_arg(raw: &str, name: &str) -> Result<Option<u32>, String> {
    let Some(rest) = raw.strip_prefix(name) else {
        return Ok(None);
    };
    let Some(rest) = rest
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
    else {
        return Err(format!(
            "Iceberg partition transform {raw} must use {name}[N] syntax"
        ));
    };
    let value = rest.parse::<u32>().map_err(|error| {
        format!("Iceberg partition transform {raw} has invalid numeric argument: {error}")
    })?;
    if value == 0 {
        return Err(format!(
            "Iceberg partition transform {raw} requires a positive numeric argument"
        ));
    }
    Ok(Some(value))
}
fn retag_default_partition_spec_id(
    metadata: TableMetadata,
    target_spec_id: i32,
    partition_column_names: &[String],
    work: &mut SchemaWork<'_>,
) -> Result<TableMetadata, FrozenWriteSchemaError> {
    let mut value = work.opaque_source(|| {
        serde_json::to_value(metadata)
            .map_err(|error| format!("serialize staged Iceberg table metadata: {error}"))
    })?;
    let object = value.as_object_mut().ok_or_else(|| {
        FrozenWriteSchemaError::Source(
            "staged Iceberg table metadata must serialize to an object".to_string(),
        )
    })?;
    object.insert(
        "default-spec-id".to_string(),
        serde_json::Value::from(target_spec_id),
    );
    work.step()?;
    let specs = object
        .get_mut("partition-specs")
        .and_then(serde_json::Value::as_array_mut)
        .ok_or_else(|| {
            FrozenWriteSchemaError::Source(
                "staged Iceberg table metadata is missing partition-specs".to_string(),
            )
        })?;
    let mut matched = None;
    for (index, spec) in specs.iter().enumerate() {
        let equal = partition_spec_names_match(spec, partition_column_names, work)?;
        work.step()?;
        if equal {
            matched = Some(index);
            break;
        }
    }
    let index = matched.ok_or_else(|| {
        FrozenWriteSchemaError::Source(format!(
            "staged Iceberg metadata is missing partition fields {partition_column_names:?}"
        ))
    })?;
    let mut spec = work.opaque(|| specs[index].clone())?;
    spec.as_object_mut()
        .ok_or_else(|| {
            FrozenWriteSchemaError::Source(
                "staged Iceberg partition spec must be an object".to_string(),
            )
        })?
        .insert(
            "spec-id".to_string(),
            serde_json::Value::from(target_spec_id),
        );
    *specs = vec![spec];
    work.step()?;
    let metadata: TableMetadata = work.opaque_source(|| {
        serde_json::from_value(value)
            .map_err(|error| format!("deserialize staged Iceberg table metadata: {error}"))
    })?;
    let spec = metadata
        .partition_spec_by_id(target_spec_id)
        .ok_or_else(|| {
            FrozenWriteSchemaError::Source(format!(
                "staged Iceberg metadata failed to retain partition spec {target_spec_id}"
            ))
        })?;
    let mut equal = spec.fields().len() == partition_column_names.len();
    if equal {
        for (field, name) in spec.fields().iter().zip(partition_column_names) {
            let matched = field.name == *name;
            work.step()?;
            if !matched {
                equal = false;
                break;
            }
        }
    }
    if metadata.default_partition_spec_id() != target_spec_id || !equal {
        return Err(FrozenWriteSchemaError::Source(
            "staged Iceberg metadata partition spec does not match frozen handle".to_string(),
        ));
    }
    Ok(metadata)
}
fn partition_spec_names_match(
    spec: &serde_json::Value,
    names: &[String],
    work: &mut SchemaWork<'_>,
) -> Result<bool, FrozenWriteSchemaError> {
    let Some(fields) = spec
        .as_object()
        .and_then(|object| object.get("fields"))
        .and_then(serde_json::Value::as_array)
    else {
        return Ok(names.is_empty());
    };
    if fields.len() != names.len() {
        return Ok(false);
    }
    for (field, expected) in fields.iter().zip(names) {
        let equal = field
            .as_object()
            .and_then(|object| object.get("name"))
            .and_then(serde_json::Value::as_str)
            == Some(expected.as_str());
        work.step()?;
        if !equal {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
#[path = "frozen_write_tests.rs"]
mod tests;

#[cfg(test)]
mod pure_schema_tests {
    use super::*;
    use crate::scan_model::IcebergSchemaFieldDef;
    use std::{collections::HashMap, sync::Mutex};

    #[derive(Default)]
    struct Control {
        trace: Mutex<Vec<u32>>,
        refusal: Option<(CompileControlError, Stop)>,
    }
    #[derive(Clone, Copy)]
    enum Stop {
        Entry,
        Quantum,
        Call(usize),
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert_eq!(phase, CompilePhase::ProviderValidation);
            assert!(units <= 256);
            let mut trace = self.trace.lock().unwrap();
            trace.push(units);
            if let Some((error, stop)) = self.refusal {
                let refuse = match stop {
                    Stop::Entry => trace.len() == 1,
                    Stop::Quantum => units == 256,
                    Stop::Call(index) => trace.len() == index,
                };
                if refuse {
                    return Err(error);
                }
            }
            Ok(())
        }
    }
    fn causes() -> [CompileControlError; 3] {
        [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ]
    }
    fn frozen_field(
        id: i32,
        name: &str,
        children: Vec<IcebergSchemaFieldDef>,
    ) -> IcebergSchemaFieldDef {
        IcebergSchemaFieldDef {
            field_id: id,
            name: name.to_string(),
            children,
            initial_default: None,
            write_default: None,
            initial_default_json: None,
            write_default_json: None,
        }
    }
    fn facts(fields: Vec<IcebergSchemaFieldDef>) -> FrozenDataWriteFacts {
        FrozenDataWriteFacts {
            table_location: "file:///tmp/frozen-schema-pure".to_string(),
            data_location: "file:///tmp/frozen-schema-pure/data".to_string(),
            target_partition_spec_id: 7,
            partition_source_column_names: vec![],
            partition_column_names: vec![],
            transform_exprs: vec![],
            data_input_schema: IcebergSchemaDef { fields },
            parquet_row_group_size_bytes: Some(1024),
        }
    }
    fn marker(name: &str, ty: DataType, nullable: bool) -> Field {
        Field::new(name, ty, nullable).with_metadata(HashMap::from([(
            "source-marker".to_string(),
            name.to_string(),
        )]))
    }
    fn fixture() -> (ArrowSchemaRef, FrozenDataWriteFacts) {
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                marker("tiny", DataType::Int8, false),
                marker(
                    "record",
                    DataType::Struct(vec![Arc::new(marker("small", DataType::Int16, true))].into()),
                    true,
                ),
                marker(
                    "items",
                    DataType::List(Arc::new(marker("element", DataType::Int8, true))),
                    true,
                ),
                marker(
                    "mapping",
                    DataType::Map(
                        Arc::new(marker(
                            "entries",
                            DataType::Struct(
                                vec![
                                    Arc::new(marker("key", DataType::Int8, false)),
                                    Arc::new(marker("value", DataType::Int16, true)),
                                ]
                                .into(),
                            ),
                            false,
                        )),
                        false,
                    ),
                    true,
                ),
                marker("fixed", DataType::FixedSizeBinary(16), true),
            ],
            HashMap::from([("schema-marker".to_string(), "unchanged".to_string())]),
        ));
        let mut data = facts(vec![
            frozen_field(11, "tiny", vec![]),
            frozen_field(12, "record", vec![frozen_field(121, "small", vec![])]),
            frozen_field(13, "items", vec![frozen_field(131, "element", vec![])]),
            frozen_field(
                14,
                "mapping",
                vec![
                    frozen_field(141, "key", vec![]),
                    frozen_field(142, "value", vec![]),
                ],
            ),
            frozen_field(15, "fixed", vec![]),
        ]);
        data.data_input_schema.fields[0].initial_default_json = Some("7".to_string());
        (schema, data)
    }
    fn source_error(result: Result<PreparedFrozenWriteSchema, FrozenWriteSchemaError>) -> String {
        match result {
            Err(FrozenWriteSchemaError::Source(message)) => message,
            Err(FrozenWriteSchemaError::Control(_)) => {
                panic!("ordinary fixture error became control")
            }
            Ok(_) => panic!("invalid frozen schema accepted"),
        }
    }

    #[test]
    fn pure_schema_matches_legacy_core_and_preserves_actual_nested_metadata() {
        let (schema, data) = fixture();
        let pure = prepare_frozen_write_schema(&schema, &data, &Control::default()).unwrap();
        let legacy = prepare_schema_core(&schema, &data, &mut SchemaWork(None)).unwrap();
        assert_eq!(pure.writer_schema, legacy.writer_schema);
        assert!(novarocks_type_contract::arrow_schemas_exact(
            &pure.arrow_schema,
            &legacy.arrow_schema
        ));
        assert_eq!(
            pure.metadata.current_schema(),
            legacy.metadata.current_schema()
        );
        assert_eq!(pure.metadata.default_partition_spec_id(), 7);
        assert_eq!(pure.metadata.properties(), legacy.metadata.properties());
        assert_eq!(
            pure.arrow_schema
                .metadata()
                .get("schema-marker")
                .map(String::as_str),
            Some("unchanged")
        );
        assert_eq!(pure.arrow_schema.field(0).data_type(), &DataType::Int32);
        assert!(!pure.arrow_schema.field(0).is_nullable());
        assert_eq!(
            pure.arrow_schema
                .field(0)
                .metadata()
                .get(crate::default_value::ICEBERG_INITIAL_DEFAULT_META_KEY)
                .map(String::as_str),
            Some("7")
        );
        let DataType::Struct(children) = pure.arrow_schema.field(1).data_type() else {
            panic!("record carrier lost")
        };
        assert_eq!(children[0].data_type(), &DataType::Int32);
        assert!(children[0].is_nullable());
        assert_eq!(
            children[0]
                .metadata()
                .get("source-marker")
                .map(String::as_str),
            Some("small")
        );
        assert_eq!(
            crate::schema_mapping::field_id_for_arrow_field(&children[0]).unwrap(),
            Some(121)
        );
        for field_id in [11, 121, 131, 141, 142] {
            assert_eq!(
                pure.writer_schema
                    .field_by_id(field_id)
                    .unwrap()
                    .field_type
                    .as_ref(),
                &Type::Primitive(PrimitiveType::Int)
            );
        }
        assert_eq!(
            pure.writer_schema
                .field_by_id(15)
                .unwrap()
                .field_type
                .as_ref(),
            &Type::Primitive(PrimitiveType::Fixed(16))
        );
        assert!(
            !pure
                .arrow_schema
                .field(4)
                .metadata()
                .contains_key(novarocks_type_contract::NR_LOGICAL_TYPE_KEY)
        );
        let annotated = annotate_schema_from_scan_model(&schema, &data.data_input_schema).unwrap();
        assert!(novarocks_type_contract::arrow_schemas_exact(
            &pure.arrow_schema,
            &scalar_integer_storage_schema(&annotated)
        ));
    }

    #[test]
    fn pure_partition_preserves_spec_id_exact_source_ids_transforms_order_and_properties() {
        let schema = Arc::new(Schema::new(vec![Field::new("tiny", DataType::Int8, false)]));
        let mut data = facts(vec![frozen_field(11, "tiny", vec![])]);
        data.target_partition_spec_id = 29;
        data.partition_source_column_names = vec!["tiny".into(), "TINY".into(), "tiny".into()];
        data.partition_column_names = vec![
            "identity_part".into(),
            "bucket_part".into(),
            "truncate_part".into(),
        ];
        data.transform_exprs = vec![
            " identity ".into(),
            "BUCKET[8]".into(),
            "truncate[2]".into(),
        ];
        let pure = prepare_frozen_write_schema(&schema, &data, &Control::default()).unwrap();
        let legacy = prepare_schema_core(&schema, &data, &mut SchemaWork(None)).unwrap();
        assert_eq!(pure.metadata.default_partition_spec_id(), 29);
        let actual = pure.metadata.partition_spec_by_id(29).unwrap();
        assert_eq!(actual, legacy.metadata.partition_spec_by_id(29).unwrap());
        assert_eq!(
            actual
                .fields()
                .iter()
                .map(|f| f.source_id)
                .collect::<Vec<_>>(),
            vec![11; 3]
        );
        assert_eq!(
            actual
                .fields()
                .iter()
                .map(|f| f.name.as_str())
                .collect::<Vec<_>>(),
            vec!["identity_part", "bucket_part", "truncate_part"]
        );
        assert_eq!(
            actual
                .fields()
                .iter()
                .map(|f| f.transform)
                .collect::<Vec<_>>(),
            vec![
                Transform::Identity,
                Transform::Bucket(8),
                Transform::Truncate(2)
            ]
        );
        assert_eq!(
            pure.metadata.properties().get("write.data.path"),
            Some(&data.data_location)
        );
        assert_eq!(
            pure.metadata
                .properties()
                .get(PARQUET_ROW_GROUP_SIZE_BYTES_PROPERTY)
                .map(String::as_str),
            Some("1024")
        );
    }

    #[test]
    fn pure_schema_uses_legacy_annotation_and_partition_error_diagnostics() {
        let schema = Arc::new(Schema::new(vec![Field::new("tiny", DataType::Int8, false)]));
        for case in 0..4 {
            let mut data = facts(vec![frozen_field(11, "tiny", vec![])]);
            match case {
                0 => data.data_input_schema.fields.clear(),
                1 => data.partition_source_column_names.push("tiny".into()),
                2 => {
                    data.partition_source_column_names.push("missing".into());
                    data.partition_column_names.push("p".into());
                    data.transform_exprs.push("identity".into());
                }
                _ => {
                    data.partition_source_column_names.push("tiny".into());
                    data.partition_column_names.push("p".into());
                    data.transform_exprs.push("bucket[0]".into());
                }
            }
            let ordinary = source_error(prepare_schema_core(&schema, &data, &mut SchemaWork(None)));
            assert_eq!(
                source_error(prepare_frozen_write_schema(
                    &schema,
                    &data,
                    &Control::default()
                )),
                ordinary
            );
        }
    }

    #[test]
    fn pure_schema_keeps_existing_unsupported_carrier_failure_without_new_conversion() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
        )]));
        let data = facts(vec![frozen_field(1, "v", vec![])]);
        let ordinary = source_error(prepare_schema_core(&schema, &data, &mut SchemaWork(None)));
        assert!(ordinary.contains("unsupported Arrow type for staged Iceberg writer schema"));
        assert_eq!(
            source_error(prepare_frozen_write_schema(
                &schema,
                &data,
                &Control::default()
            )),
            ordinary
        );
    }

    #[test]
    fn pure_schema_accepts_unprojected_private_depth_and_long_name_like_legacy() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "selected",
            DataType::Int64,
            false,
        )]));
        let mut unused = frozen_field(200, &"x".repeat(2048), vec![]);
        for depth in (0..40).rev() {
            unused = frozen_field(100 + depth, &format!("unused_{depth}"), vec![unused]);
        }
        let data = facts(vec![frozen_field(1, "selected", vec![]), unused]);
        // This is a genuine finite private SchemaDef, rather than a public
        // Arrow field fabricated to bypass its writer admission bounds.
        assert!(serde_json::to_vec(&data.data_input_schema).unwrap().len() < 8 * 1024 * 1024);
        let legacy = prepare_schema_core(&schema, &data, &mut SchemaWork(None)).unwrap();
        let pure = prepare_frozen_write_schema(&schema, &data, &Control::default()).unwrap();
        assert_eq!(pure.writer_schema, legacy.writer_schema);
        assert!(novarocks_type_contract::arrow_schemas_exact(
            &pure.arrow_schema,
            &legacy.arrow_schema
        ));
        assert_eq!(
            pure.metadata.current_schema(),
            legacy.metadata.current_schema()
        );
        assert_eq!(pure.writer_schema.as_struct().fields().len(), 1);
        assert_eq!(pure.writer_schema.field_by_id(1).unwrap().name, "selected");
        assert_eq!(
            pure.writer_schema
                .field_by_id(1)
                .unwrap()
                .field_type
                .as_ref(),
            &Type::Primitive(PrimitiveType::Long)
        );
        assert!(pure.writer_schema.field_by_id(100).is_none());
        assert_eq!(
            crate::schema_mapping::field_id_for_arrow_field(pure.arrow_schema.field(0)).unwrap(),
            Some(1)
        );
    }

    #[test]
    fn pure_schema_control_entry_quantum_and_publication_preserve_all_three_causes() {
        let (schema, data) = fixture();
        let baseline = Control::default();
        prepare_frozen_write_schema(&schema, &data, &baseline).unwrap();
        let final_call = baseline.trace.lock().unwrap().len();
        let wide = Arc::new(Schema::new(
            (0..320)
                .map(|i| Field::new(format!("v{i}"), DataType::Int64, false))
                .collect::<Vec<_>>(),
        ));
        let wide_facts = facts(
            (0..320)
                .map(|i| frozen_field(i + 1, &format!("v{i}"), vec![]))
                .collect(),
        );
        for cause in causes() {
            for stop in [Stop::Entry, Stop::Call(final_call)] {
                let control = Control {
                    trace: Mutex::default(),
                    refusal: Some((cause, stop)),
                };
                assert!(
                    matches!(prepare_frozen_write_schema(&schema,&data,&control),Err(FrozenWriteSchemaError::Control(actual)) if actual==cause)
                );
                let trace = control.trace.lock().unwrap();
                assert_eq!(
                    trace.len(),
                    if matches!(stop, Stop::Entry) {
                        1
                    } else {
                        final_call
                    }
                );
            }
            let control = Control {
                trace: Mutex::default(),
                refusal: Some((cause, Stop::Quantum)),
            };
            assert!(
                matches!(prepare_frozen_write_schema(&wide,&wide_facts,&control),Err(FrozenWriteSchemaError::Control(actual)) if actual==cause)
            );
            assert_eq!(control.trace.lock().unwrap().last(), Some(&256));
        }
    }

    #[test]
    fn pure_schema_ordinary_failure_observes_tail_and_never_relabels_control() {
        let schema = Arc::new(Schema::new(vec![Field::new("tiny", DataType::Int8, false)]));
        let data = facts(vec![]);
        let baseline = Control::default();
        let message = source_error(prepare_frozen_write_schema(&schema, &data, &baseline));
        assert!(message.contains("missing its frozen schema field"));
        let final_call = baseline.trace.lock().unwrap().len();
        assert_eq!(baseline.trace.lock().unwrap().last(), Some(&0));
        for cause in causes() {
            let control = Control {
                trace: Mutex::default(),
                refusal: Some((cause, Stop::Call(final_call))),
            };
            assert!(
                matches!(prepare_frozen_write_schema(&schema,&data,&control),Err(FrozenWriteSchemaError::Control(actual)) if actual==cause)
            );
            assert_eq!(control.trace.lock().unwrap().len(), final_call);
        }
    }
}
