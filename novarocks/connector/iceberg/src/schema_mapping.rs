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

use std::collections::HashSet;
use std::sync::Arc;

type ListFieldWrapper = Box<dyn FnOnce(Arc<Field>) -> DataType>;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
pub use novarocks_types::value::variant::is_variant_struct_data_type;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::default_value::ICEBERG_INITIAL_DEFAULT_META_KEY;
use crate::iceberg::spec::{MappedField, NameMapping};
use crate::row_lineage_synth::{
    ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
    ICEBERG_RESERVED_FIELD_ID_ROW_ID, ICEBERG_ROW_ID_COL,
};
use crate::scan_model::{IcebergSchemaDef, IcebergSchemaFieldDef};

/// Convert one frozen Iceberg schema into the SQL read carrier exposed by the
/// Provider. iceberg-rust's generic Arrow mapping intentionally chooses wider
/// physical representations for several primitives; NovaRocks binds these
/// exact logical carriers at admission and must reproduce them at begin-scan.
pub fn sql_read_schema_from_iceberg(
    iceberg_schema: &crate::iceberg::spec::Schema,
) -> Result<SchemaRef, String> {
    let arrow_schema = crate::iceberg::arrow::schema_to_arrow_schema(iceberg_schema)
        .map_err(|error| format!("convert Iceberg schema to Arrow: {error}"))?;
    if arrow_schema.fields().len() != iceberg_schema.as_struct().fields().len() {
        return Err("Iceberg schema field count does not match its Arrow carrier".to_string());
    }
    let fields = arrow_schema
        .fields()
        .iter()
        .zip(iceberg_schema.as_struct().fields())
        .map(|(field, iceberg_field)| {
            provider_engine_field(field.as_ref(), iceberg_field, true).map(Arc::new)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        arrow_schema.metadata().clone(),
    )))
}

/// Writer projection applies the same recursive primitive carrier overrides,
/// retaining exact provider required children instead of SQL read key widening.
pub(crate) fn sql_write_schema_from_iceberg(
    schema: &crate::iceberg::spec::Schema,
) -> Result<SchemaRef, String> {
    validate_exact_schema(schema)?;
    let arrow = crate::iceberg::arrow::schema_to_arrow_schema(schema).map_err(|e| e.to_string())?;
    if arrow.fields().len() != schema.as_struct().fields().len() {
        return Err("provider write schema field count mismatch".into());
    }
    let fields = arrow
        .fields()
        .iter()
        .zip(schema.as_struct().fields())
        .map(|(f, p)| provider_engine_field(f, p, false).map(Arc::new))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        arrow.metadata().clone(),
    )))
}

fn provider_engine_field(
    field: &Field,
    iceberg_field: &crate::iceberg::spec::NestedField,
    widen_map_keys: bool,
) -> Result<Field, String> {
    Ok(field.clone().with_data_type(sql_read_data_type(
        field.data_type(),
        iceberg_field.field_type.as_ref(),
        field.name(),
        widen_map_keys,
    )?))
}

fn sql_read_data_type(
    arrow_type: &DataType,
    iceberg_type: &crate::iceberg::spec::Type,
    path: &str,
    widen_map_keys: bool,
) -> Result<DataType, String> {
    use crate::iceberg::spec::{PrimitiveType, Type};
    use arrow::datatypes::TimeUnit;

    match iceberg_type {
        Type::Primitive(PrimitiveType::Binary) => Ok(DataType::Binary),
        Type::Primitive(PrimitiveType::Variant) => Ok(DataType::LargeBinary),
        Type::Primitive(PrimitiveType::Timestamptz) => {
            Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
        }
        Type::Primitive(PrimitiveType::TimestamptzNs) => {
            Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
        }
        Type::Primitive(_) => Ok(arrow_type.clone()),
        Type::Struct(iceberg_struct) => {
            let DataType::Struct(arrow_fields) = arrow_type else {
                return Err(format!(
                    "Iceberg struct {path} has incompatible Arrow carrier {arrow_type:?}"
                ));
            };
            if arrow_fields.len() != iceberg_struct.fields().len() {
                return Err(format!(
                    "Iceberg struct {path} field count does not match its Arrow carrier"
                ));
            }
            let fields = arrow_fields
                .iter()
                .zip(iceberg_struct.fields())
                .map(|(field, iceberg_field)| {
                    provider_engine_field(field.as_ref(), iceberg_field, widen_map_keys)
                        .map(Arc::new)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(DataType::Struct(fields.into()))
        }
        Type::List(iceberg_list) => {
            let (field, wrap): (&Field, ListFieldWrapper) = match arrow_type {
                DataType::List(field) => (field.as_ref(), Box::new(DataType::List)),
                DataType::LargeList(field) => (field.as_ref(), Box::new(DataType::LargeList)),
                DataType::FixedSizeList(field, size) => {
                    let size = *size;
                    (
                        field.as_ref(),
                        Box::new(move |field| DataType::FixedSizeList(field, size)),
                    )
                }
                _ => {
                    return Err(format!(
                        "Iceberg list {path} has incompatible Arrow carrier {arrow_type:?}"
                    ));
                }
            };
            Ok(wrap(Arc::new(provider_engine_field(
                field,
                &iceberg_list.element_field,
                widen_map_keys,
            )?)))
        }
        Type::Map(iceberg_map) => {
            let DataType::Map(entries, sorted) = arrow_type else {
                return Err(format!(
                    "Iceberg map {path} has incompatible Arrow carrier {arrow_type:?}"
                ));
            };
            let DataType::Struct(arrow_fields) = entries.data_type() else {
                return Err(format!("Iceberg map {path} has non-struct Arrow entries"));
            };
            if arrow_fields.len() != 2 {
                return Err(format!(
                    "Iceberg map {path} does not have key/value entries"
                ));
            }
            // Iceberg requires a map key to be present, and iceberg-rust's
            // generic mapping reproduces that as a non-nullable Arrow field.
            // NovaRocks stores and returns NULL map keys, so the SQL read
            // carrier has to say so: the plan codec decodes a map key as
            // nullable, `retag_iceberg_array` refuses to narrow a key the
            // data proves nullable, and the runtime retag widens any child
            // that carries nulls. Leaving the carrier non-nullable made the
            // declared result type disagree with its own rows the moment a
            // NULL key appeared in them.
            let fields = vec![
                Arc::new(
                    provider_engine_field(
                        arrow_fields[0].as_ref(),
                        &iceberg_map.key_field,
                        widen_map_keys,
                    )?
                    .with_nullable(widen_map_keys || !iceberg_map.key_field.required),
                ),
                Arc::new(provider_engine_field(
                    arrow_fields[1].as_ref(),
                    &iceberg_map.value_field,
                    widen_map_keys,
                )?),
            ];
            let entries = Arc::new(
                entries
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::Struct(fields.into())),
            );
            Ok(DataType::Map(entries, *sorted))
        }
    }
}

/// Validate the complete provider tree before any recursive SDK conversion or
/// private serialization. IDs and required remain provider facts, independent
/// of the read carrier's map-key widening and timestamp normalization.
pub(crate) fn validate_exact_schema(schema: &crate::iceberg::spec::Schema) -> Result<(), String> {
    validate_provider_fields(schema.as_struct().fields().iter().map(AsRef::as_ref))
}

fn validate_provider_fields<'a>(
    fields: impl ExactSizeIterator<Item = &'a crate::iceberg::spec::NestedField>,
) -> Result<(), String> {
    use crate::iceberg::spec::Type;
    let limits = novarocks_types::logical_type::LogicalTypeLimits::default();
    if fields.len() > limits.max_nodes {
        return Err("provider schema exceeds its root field budget".into());
    }
    let mut pending = fields.map(|f| (f, 1usize)).collect::<Vec<_>>();
    let mut ids = HashSet::new();
    let mut nodes = 0usize;
    let mut text = 0usize;
    while let Some((field, depth)) = pending.pop() {
        nodes += 1;
        text = text
            .checked_add(field.name.len())
            .ok_or("provider schema text overflow")?;
        if depth > limits.max_depth || nodes > limits.max_nodes || text > limits.max_text_bytes {
            return Err("provider schema exceeds its recursive budget".into());
        }
        if field.id <= 0 || !ids.insert(field.id) {
            return Err("provider schema has a duplicate or invalid nested field ID".into());
        }
        match field.field_type.as_ref() {
            Type::Primitive(_) => {}
            Type::Struct(v) => {
                if v.fields().len() > limits.max_nodes.saturating_sub(nodes + pending.len()) {
                    return Err("provider schema exceeds its recursive node budget".into());
                }
                pending.extend(v.fields().iter().map(|f| (f.as_ref(), depth + 1)))
            }
            Type::List(v) => pending.push((&v.element_field, depth + 1)),
            Type::Map(v) => {
                if !v.key_field.required {
                    return Err("provider map key must be required".into());
                }
                pending.push((&v.value_field, depth + 1));
                pending.push((&v.key_field, depth + 1));
            }
        }
        if pending.len() > limits.max_nodes.saturating_sub(nodes) {
            return Err("provider schema exceeds its recursive node budget".into());
        }
    }
    Ok(())
}

/// Engine logical facts from one retained metadata generation. The SDK's
/// generic Arrow schema is not itself an engine semantic type declaration.
pub(crate) fn exact_logical_fields(
    metadata: &crate::iceberg::spec::TableMetadata,
) -> Result<Vec<novarocks_types::logical_type::LogicalField>, String> {
    validate_exact_schema(metadata.current_schema())?;
    let carrier =
        crate::scalar_integer_domain::metadata_sql_schema(metadata, metadata.current_schema())
            .map_err(|e| e.to_string())?;
    let logical_markers = crate::metadata::logical_type_columns(metadata.properties());
    carrier
        .fields()
        .iter()
        .map(|f| {
            let mut logical = novarocks_types::logical_type::logical_field_from_engine_arrow(f)?;
            match logical_markers
                .get(&f.name().to_ascii_lowercase())
                .map(String::as_str)
            {
                Some("bitmap") => {
                    logical.data_type = novarocks_types::logical_type::LogicalType::Bitmap
                }
                Some("hll") => logical.data_type = novarocks_types::logical_type::LogicalType::Hll,
                _ => {}
            }
            Ok(logical)
        })
        .collect()
}

/// The closed historical scalar provider domain. Engine normalization cannot
/// manufacture this fact: UUID/Fixed, zoned/nanosecond timestamps and Variant
/// are explicitly outside the old leaf grammar.
pub(crate) fn legacy_scalar_type(
    field: &crate::iceberg::spec::NestedField,
) -> Option<novarocks_types::logical_type::LogicalType> {
    use crate::iceberg::spec::{PrimitiveType, Type};
    use novarocks_types::logical_type::LogicalType;
    match field.field_type.as_ref() {
        Type::Primitive(PrimitiveType::Boolean) => Some(LogicalType::Boolean),
        Type::Primitive(PrimitiveType::Int) => Some(LogicalType::Int32),
        Type::Primitive(PrimitiveType::Long) => Some(LogicalType::Int64),
        Type::Primitive(PrimitiveType::Float) => Some(LogicalType::Float32),
        Type::Primitive(PrimitiveType::Double) => Some(LogicalType::Float64),
        Type::Primitive(PrimitiveType::String) => Some(LogicalType::Utf8),
        Type::Primitive(PrimitiveType::Binary) => Some(LogicalType::Binary),
        Type::Primitive(PrimitiveType::Date) => Some(LogicalType::Date32),
        Type::Primitive(PrimitiveType::Decimal { precision, scale }) => {
            Some(LogicalType::Decimal {
                bits: 128,
                precision: u8::try_from(*precision).ok()?,
                scale: i8::try_from(*scale).ok()?,
            })
        }
        Type::Primitive(PrimitiveType::Timestamp) => Some(LogicalType::Timestamp {
            unit: arrow::datatypes::TimeUnit::Microsecond,
            timezone: None,
        }),
        _ => None,
    }
}

/// Bounded provider-private exact subtree encoding. This binds IDs, required,
/// physical families and parameters. Names belong to public logical facts;
/// docs and defaults do not replace field identity. Consumers only compare bytes.
pub(crate) fn exact_provider_type_binding(
    field: &crate::iceberg::spec::NestedField,
) -> Result<bytes::Bytes, String> {
    validate_provider_fields(std::iter::once(field))?;
    struct Sink(Vec<u8>);
    impl std::io::Write for Sink {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if self
                .0
                .len()
                .checked_add(bytes.len())
                .is_none_or(|n| n > 64 * 1024)
            {
                return Err(std::io::Error::other(
                    "provider field binding exceeds its byte budget",
                ));
            }
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut sink = Sink(b"novarocks.iceberg.exact-field.v1:".to_vec());
    serde_json::to_writer(&mut sink, &ProviderFieldIdentity(field))
        .map_err(|e| format!("encode exact provider field: {e}"))?;
    Ok(bytes::Bytes::from(sink.0))
}

// Borrowed wrappers avoid the SDK's serde `into` conversion, which clones
// the complete subtree before writing and includes mutable docs/defaults.
struct ProviderFieldIdentity<'a>(&'a crate::iceberg::spec::NestedField);
struct ProviderTypeIdentity<'a>(&'a crate::iceberg::spec::Type);
struct ProviderFieldsIdentity<'a>(&'a [Arc<crate::iceberg::spec::NestedField>]);

impl serde::Serialize for ProviderFieldIdentity<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut output = serializer.serialize_struct("ProviderFieldIdentity", 3)?;
        output.serialize_field("id", &self.0.id)?;
        output.serialize_field("required", &self.0.required)?;
        output.serialize_field("type", &ProviderTypeIdentity(&self.0.field_type))?;
        output.end()
    }
}

impl serde::Serialize for ProviderFieldsIdentity<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut output = serializer.serialize_seq(Some(self.0.len()))?;
        for field in self.0 {
            output.serialize_element(&ProviderFieldIdentity(field))?;
        }
        output.end()
    }
}

impl serde::Serialize for ProviderTypeIdentity<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use crate::iceberg::spec::Type;
        use serde::ser::SerializeStruct;
        match self.0 {
            Type::Primitive(primitive) => primitive.serialize(serializer),
            Type::Struct(fields) => {
                let mut output = serializer.serialize_struct("ProviderStructIdentity", 2)?;
                output.serialize_field("kind", "struct")?;
                output.serialize_field("fields", &ProviderFieldsIdentity(fields.fields()))?;
                output.end()
            }
            Type::List(list) => {
                let mut output = serializer.serialize_struct("ProviderListIdentity", 2)?;
                output.serialize_field("kind", "list")?;
                output.serialize_field("element", &ProviderFieldIdentity(&list.element_field))?;
                output.end()
            }
            Type::Map(map) => {
                let mut output = serializer.serialize_struct("ProviderMapIdentity", 3)?;
                output.serialize_field("kind", "map")?;
                output.serialize_field("key", &ProviderFieldIdentity(&map.key_field))?;
                output.serialize_field("value", &ProviderFieldIdentity(&map.value_field))?;
                output.end()
            }
        }
    }
}

/// Validates and canonically encodes an Iceberg name mapping.
///
/// Name mappings are physical Iceberg schema facts.  Keeping validation and
/// canonical serialization with the provider prevents catalog mutation and
/// write planning callers from each accepting a slightly different mapping.
pub fn canonical_name_mapping(raw: &str) -> Result<String, String> {
    let value: serde_json::Value = serde_json::from_str(raw)
        .map_err(|error| format!("decode schema.name-mapping.default: {error}"))?;
    validate_name_mapping_json(&value)?;
    let mapping: NameMapping = serde_json::from_value(value)
        .map_err(|error| format!("decode schema.name-mapping.default: {error}"))?;
    serde_json::to_string(&mapping)
        .map_err(|error| format!("encode canonical schema.name-mapping.default: {error}"))
}

fn validate_name_mapping_json(value: &serde_json::Value) -> Result<(), String> {
    fn visit(fields: &serde_json::Value, ids: &mut HashSet<i64>) -> Result<(), String> {
        let fields = fields
            .as_array()
            .ok_or_else(|| "Iceberg name mapping root/fields must be an array".to_string())?;
        let mut sibling_aliases = HashSet::new();
        for field in fields {
            let object = field
                .as_object()
                .ok_or_else(|| "Iceberg name mapping field must be an object".to_string())?;
            if object
                .keys()
                .any(|key| !matches!(key.as_str(), "field-id" | "names" | "fields"))
            {
                return Err("Iceberg name mapping contains an unknown field".to_string());
            }
            let id = object
                .get("field-id")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| "Iceberg name mapping field-id is required".to_string())?;
            if id <= 0 || !ids.insert(id) {
                return Err(format!(
                    "Iceberg name mapping has duplicate or invalid ID {id}"
                ));
            }
            let names = object
                .get("names")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| "Iceberg name mapping names must be an array".to_string())?;
            if names.is_empty() {
                return Err("Iceberg name mapping names must not be empty".to_string());
            }
            for name in names {
                let name = name
                    .as_str()
                    .filter(|name| !name.is_empty())
                    .ok_or_else(|| "Iceberg name mapping alias must be nonempty".to_string())?;
                if !sibling_aliases.insert(name.to_string()) {
                    return Err(format!("Iceberg name mapping has duplicate alias {name}"));
                }
            }
            if let Some(children) = object.get("fields")
                && !children.is_null()
            {
                visit(children, ids)?;
            }
        }
        Ok(())
    }

    let mut ids = HashSet::new();
    visit(value, &mut ids)
}

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

/// Re-annotate a generic native writer schema with the frozen Iceberg field-ID
/// tree carried by a provider write handle.  The generic carrier owns Arrow
/// types; Iceberg owns physical field identity and defaults.
pub fn annotate_schema_from_scan_model(
    input_schema: &SchemaRef,
    iceberg_schema: &IcebergSchemaDef,
) -> Result<SchemaRef, String> {
    let fields = input_schema
        .fields()
        .iter()
        .map(|field| {
            if is_write_virtual_column(field.name()) {
                return Ok(field.as_ref().clone());
            }
            if let Some(field_id) = reserved_row_lineage_field_id(field)? {
                let mut metadata = field.metadata().clone();
                metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string());
                return Ok(Field::new(
                    field.name(),
                    field.data_type().clone(),
                    field.is_nullable(),
                )
                .with_metadata(metadata));
            }
            let frozen = iceberg_schema
                .fields
                .iter()
                .find(|candidate| candidate.name == field.name().as_str())
                .ok_or_else(|| {
                    format!(
                        "Iceberg writer column {} is missing its frozen schema field",
                        field.name()
                    )
                })?;
            apply_scan_field_id_recursive(field.as_ref(), frozen)
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        input_schema.metadata().clone(),
    )))
}

/// Re-annotate a read output schema with the provider-owned field facts carried
/// by the frozen Iceberg schema: parquet field IDs and each column's
/// spec-encoded initial default. iceberg-rust's Arrow conversion drops initial
/// defaults, so without this a data file written before `ADD COLUMN ... DEFAULT`
/// reads back as NULL instead of the column's default.
///
/// A field with no frozen counterpart is returned unchanged: metadata
/// pseudo-columns own their own identity, and a frozen row-mutation source
/// projects an older snapshot schema whose columns may since have been renamed.
pub fn annotate_read_schema_from_scan_model(
    output_schema: &SchemaRef,
    iceberg_schema: &IcebergSchemaDef,
) -> Result<SchemaRef, String> {
    let fields = output_schema
        .fields()
        .iter()
        .map(|field| {
            let Some(frozen) = iceberg_schema
                .fields
                .iter()
                .find(|candidate| candidate.name.eq_ignore_ascii_case(field.name()))
            else {
                return Ok(field.as_ref().clone());
            };
            apply_scan_field_id_recursive(field.as_ref(), frozen)
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        output_schema.metadata().clone(),
    )))
}

fn is_write_virtual_column(name: &str) -> bool {
    matches!(name, "_file" | "_pos" | "__change_op") || name.starts_with("__nr_var_")
}

fn reserved_row_lineage_field_id(field: &Field) -> Result<Option<i32>, String> {
    let field_id = if field.name().eq_ignore_ascii_case(ICEBERG_ROW_ID_COL) {
        ICEBERG_RESERVED_FIELD_ID_ROW_ID
    } else if field
        .name()
        .eq_ignore_ascii_case(ICEBERG_LAST_UPDATED_SEQ_COL)
    {
        ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER
    } else {
        return Ok(None);
    };
    if field.data_type() != &DataType::Int64 {
        return Err(format!(
            "Iceberg reserved row-lineage column {} expects Int64, got {:?}",
            field.name(),
            field.data_type()
        ));
    }
    Ok(Some(field_id))
}

fn apply_scan_field_id_recursive(
    field: &Field,
    frozen: &IcebergSchemaFieldDef,
) -> Result<Field, String> {
    let mut metadata = field.metadata().clone();
    metadata.insert(
        PARQUET_FIELD_ID_META_KEY.to_string(),
        frozen.field_id.to_string(),
    );
    if let Some(default) = frozen.initial_default_json.as_ref() {
        metadata.insert(
            ICEBERG_INITIAL_DEFAULT_META_KEY.to_string(),
            default.clone(),
        );
    }
    let data_type = match field.data_type() {
        // Parquet VARIANT is one Iceberg primitive field whose physical Arrow
        // carrier is a Struct. Its metadata/value children are encoding
        // details, not independently identified Iceberg nested fields.
        data_type if is_variant_struct_data_type(data_type) => data_type.clone(),
        DataType::Struct(children) => DataType::Struct(
            children
                .iter()
                .map(|child| scan_child_field(child.as_ref(), &frozen.children))
                .collect::<Result<Vec<_>, _>>()?
                .into(),
        ),
        DataType::List(child) => DataType::List(Arc::new(scan_list_child(child.as_ref(), frozen)?)),
        DataType::LargeList(child) => {
            DataType::LargeList(Arc::new(scan_list_child(child.as_ref(), frozen)?))
        }
        DataType::FixedSizeList(child, size) => {
            DataType::FixedSizeList(Arc::new(scan_list_child(child.as_ref(), frozen)?), *size)
        }
        DataType::Map(entries, sorted) => {
            let DataType::Struct(children) = entries.data_type() else {
                return Err(format!(
                    "Iceberg MAP column {} has non-struct entries",
                    field.name()
                ));
            };
            if children.len() != 2 || frozen.children.len() != 2 {
                return Err(format!(
                    "Iceberg MAP column {} has incompatible frozen fields",
                    field.name()
                ));
            }
            let children = children
                .iter()
                .zip(frozen.children.iter())
                .map(|(child, frozen)| apply_scan_field_id_recursive(child.as_ref(), frozen))
                .collect::<Result<Vec<_>, _>>()?;
            let entries = Field::new(
                entries.name(),
                DataType::Struct(children.into()),
                entries.is_nullable(),
            )
            .with_metadata(entries.metadata().clone());
            DataType::Map(Arc::new(entries), *sorted)
        }
        data_type => data_type.clone(),
    };
    Ok(Field::new(field.name(), data_type, field.is_nullable()).with_metadata(metadata))
}

fn scan_child_field(
    field: &Field,
    frozen_children: &[IcebergSchemaFieldDef],
) -> Result<Field, String> {
    let frozen = frozen_children
        .iter()
        .find(|candidate| candidate.name == field.name().as_str())
        .ok_or_else(|| {
            format!(
                "Iceberg nested writer field {} is missing frozen metadata",
                field.name()
            )
        })?;
    apply_scan_field_id_recursive(field, frozen)
}

fn scan_list_child(field: &Field, frozen: &IcebergSchemaFieldDef) -> Result<Field, String> {
    let child = frozen.children.first().ok_or_else(|| {
        format!(
            "Iceberg list writer field {} is missing frozen element metadata",
            field.name()
        )
    })?;
    apply_scan_field_id_recursive(field, child)
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
    let identified = usize::from(field_id_for_arrow_field(field)?.is_some());
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

pub fn field_id_for_arrow_field(field: &Field) -> Result<Option<i32>, String> {
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

pub fn unidentified_fields_are_only_opaque_variants(schema: &SchemaRef) -> Result<bool, String> {
    let mut found_unidentified_variant = false;
    for field in schema.fields() {
        if is_variant_struct_data_type(field.data_type()) {
            if field_id_for_arrow_field(field)?.is_none() {
                found_unidentified_variant = true;
            }
            continue;
        }
        let (identified, total) = field_id_coverage(field.as_ref())?;
        if identified != total {
            return Ok(false);
        }
    }
    Ok(found_unidentified_variant)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    use super::{
        apply_scan_field_id_recursive, canonical_name_mapping, sql_read_schema_from_iceberg,
    };
    use crate::iceberg::spec::{
        ListType, MapType, NestedField, PrimitiveType, Schema as IcebergSchema, StructType, Type,
    };
    use crate::scan_model::IcebergSchemaFieldDef;

    #[test]
    fn canonical_name_mapping_is_strict_and_provider_owned() {
        assert_eq!(
            canonical_name_mapping(r#"[{"names":["legacy_id"],"field-id":1}]"#)
                .expect("canonical mapping"),
            r#"[{"field-id":1,"names":["legacy_id"]}]"#,
        );
        assert!(
            canonical_name_mapping(r#"[{"field-id":1,"names":["id"],"credential":"secret"}]"#)
                .is_err()
        );
        assert!(
            canonical_name_mapping(
                r#"[
                {"field-id":1,"names":["left"],"fields":[{"field-id":2,"names":["id"]}]},
                {"field-id":3,"names":["right"],"fields":[{"field-id":4,"names":["id"]}]}
            ]"#
            )
            .is_ok()
        );
    }

    #[test]
    fn frozen_variant_annotation_keeps_physical_children_opaque() {
        let field = Field::new(
            "v",
            DataType::Struct(
                vec![
                    Field::new("metadata", DataType::Binary, false),
                    Field::new("value", DataType::LargeBinary, false),
                ]
                .into(),
            ),
            true,
        );
        let frozen = IcebergSchemaFieldDef {
            field_id: 7,
            name: "v".to_string(),
            initial_default: None,
            write_default: None,
            initial_default_json: None,
            write_default_json: None,
            children: Vec::new(),
        };

        let annotated =
            apply_scan_field_id_recursive(&field, &frozen).expect("annotate variant field");
        assert_eq!(
            annotated.metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&"7".to_string())
        );
        assert_eq!(annotated.data_type(), field.data_type());
        let DataType::Struct(children) = annotated.data_type() else {
            panic!("variant remains a struct carrier");
        };
        assert!(children.iter().all(|child| child.metadata().is_empty()));
        assert_eq!(children.len(), 2);
    }

    #[test]
    fn frozen_iceberg_primitives_use_exact_sql_read_carriers() {
        let iceberg = IcebergSchema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "binary", Type::Primitive(PrimitiveType::Binary)).into(),
                NestedField::optional(2, "variant", Type::Primitive(PrimitiveType::Variant)).into(),
                NestedField::required(
                    3,
                    "timestamptz",
                    Type::Primitive(PrimitiveType::Timestamptz),
                )
                .into(),
            ])
            .build()
            .expect("Iceberg schema");

        let schema = sql_read_schema_from_iceberg(&iceberg).expect("SQL read schema");
        assert_eq!(schema.field(0).data_type(), &DataType::Binary);
        assert_eq!(schema.field(1).data_type(), &DataType::LargeBinary);
        assert_eq!(
            schema.field(2).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn frozen_nested_iceberg_primitives_use_exact_sql_read_carriers() {
        let iceberg = IcebergSchema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(
                    1,
                    "record",
                    Type::Struct(StructType::new(vec![Arc::new(NestedField::optional(
                        2,
                        "payload",
                        Type::Primitive(PrimitiveType::Binary),
                    ))])),
                )
                .into(),
                NestedField::optional(
                    3,
                    "items",
                    Type::List(ListType::new(Arc::new(NestedField::list_element(
                        4,
                        Type::Primitive(PrimitiveType::Binary),
                        false,
                    )))),
                )
                .into(),
                NestedField::optional(
                    5,
                    "attributes",
                    Type::Map(MapType::new(
                        Arc::new(NestedField::map_key_element(
                            6,
                            Type::Primitive(PrimitiveType::String),
                        )),
                        Arc::new(NestedField::map_value_element(
                            7,
                            Type::Primitive(PrimitiveType::Variant),
                            false,
                        )),
                    )),
                )
                .into(),
            ])
            .build()
            .expect("Iceberg schema");

        let schema = sql_read_schema_from_iceberg(&iceberg).expect("SQL read schema");
        let DataType::Struct(record) = schema.field(0).data_type() else {
            panic!("record is a struct")
        };
        assert_eq!(record[0].data_type(), &DataType::Binary);
        let DataType::List(items) = schema.field(1).data_type() else {
            panic!("items is a list")
        };
        assert_eq!(items.data_type(), &DataType::Binary);
        let DataType::Map(entries, _) = schema.field(2).data_type() else {
            panic!("attributes is a map")
        };
        let DataType::Struct(entries) = entries.data_type() else {
            panic!("map entries are a struct")
        };
        assert_eq!(entries[1].data_type(), &DataType::LargeBinary);
    }
}

#[cfg(test)]
mod exact_logical_tests {
    use super::*;
    use crate::iceberg::spec::{
        FormatVersion, ListType, MapType, NestedField, PartitionSpec, PrimitiveType, SortOrder,
        StructType, TableMetadata, TableMetadataBuilder, Type,
    };
    use novarocks_types::logical_type::LogicalType;

    fn metadata(fields: Vec<Arc<NestedField>>) -> TableMetadata {
        let schema = crate::iceberg::spec::Schema::builder()
            .with_fields(fields)
            .build()
            .unwrap();
        TableMetadataBuilder::new(
            schema.clone(),
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            "file:///exact-logical-schema".into(),
            FormatVersion::V3,
            Default::default(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata
        .into_builder(None)
        .add_schema(schema)
        .unwrap()
        .set_current_schema(-1)
        .unwrap()
        .build()
        .unwrap()
        .metadata
    }

    #[test]
    fn exact_provider_recursive_read_mutation_preserves_required_and_new_ids() {
        let source = metadata(vec![Arc::new(NestedField::optional(
            100,
            "payload",
            Type::Struct(StructType::new(vec![
                Arc::new(NestedField::required(
                    101,
                    "items",
                    Type::List(ListType::new(Arc::new(NestedField::list_element(
                        102,
                        Type::Primitive(PrimitiveType::Long),
                        true,
                    )))),
                )),
                Arc::new(NestedField::optional(
                    103,
                    "map",
                    Type::Map(MapType::new(
                        Arc::new(NestedField::map_key_element(
                            104,
                            Type::Primitive(PrimitiveType::String),
                        )),
                        Arc::new(NestedField::map_value_element(
                            105,
                            Type::Primitive(PrimitiveType::Long),
                            true,
                        )),
                    )),
                )),
            ])),
        ))]);
        let logical = exact_logical_fields(&source).unwrap();
        let LogicalType::Struct(children) = &logical[0].data_type else {
            panic!("struct")
        };
        assert!(!children[0].nullable);
        let LogicalType::Array { element, .. } = &children[0].data_type else {
            panic!("list")
        };
        assert!(!element.nullable);
        let LogicalType::Map { key, value } = &children[1].data_type else {
            panic!("map")
        };
        assert!(
            key.nullable,
            "existing SQL read representation stays widened"
        );
        assert!(!value.nullable);
        let columns = logical
            .iter()
            .map(|f| novarocks_spi::connector::ConnectorColumnDefinition {
                name: f.name.clone().into(),
                data_type: f.data_type.clone(),
                nullable: f.nullable,
                aggregation: None,
                default: None,
            })
            .collect::<Vec<_>>();
        let target_fields = crate::catalog_control::type_mapping::schema_fields(&columns).unwrap();
        let target = metadata(target_fields);
        assert_eq!(exact_logical_fields(&target).unwrap(), logical);
        assert_ne!(
            source.current_schema().as_struct().fields()[0].id,
            target.current_schema().as_struct().fields()[0].id
        );
        let Type::Struct(fields) = target.current_schema().as_struct().fields()[0]
            .field_type
            .as_ref()
        else {
            panic!("struct")
        };
        let Type::Map(map) = fields.fields()[1].field_type.as_ref() else {
            panic!("map")
        };
        assert!(map.key_field.required);
        assert!(map.value_field.required);
        assert_ne!(
            exact_provider_type_binding(&source.current_schema().as_struct().fields()[0]).unwrap(),
            exact_provider_type_binding(&target.current_schema().as_struct().fields()[0]).unwrap()
        );
    }

    #[test]
    fn exact_provider_identity_stays_distinct_from_normalized_engine_carriers() {
        let table = metadata(vec![
            Arc::new(NestedField::required(
                1,
                "uuid",
                Type::Primitive(PrimitiveType::Uuid),
            )),
            Arc::new(NestedField::required(
                2,
                "fixed",
                Type::Primitive(PrimitiveType::Fixed(16)),
            )),
            Arc::new(NestedField::required(
                3,
                "tz",
                Type::Primitive(PrimitiveType::TimestamptzNs),
            )),
        ]);
        let logical = exact_logical_fields(&table).unwrap();
        assert_eq!(logical[0].data_type, LogicalType::LargeInt);
        assert_eq!(logical[1].data_type, LogicalType::LargeInt);
        assert_eq!(
            logical[2].data_type,
            LogicalType::Timestamp {
                unit: arrow::datatypes::TimeUnit::Nanosecond,
                timezone: None
            }
        );
        assert_ne!(
            exact_provider_type_binding(&table.current_schema().as_struct().fields()[0]).unwrap(),
            exact_provider_type_binding(&table.current_schema().as_struct().fields()[1]).unwrap()
        );
    }

    #[test]
    fn legacy_scalar_domain_does_not_infer_from_normalized_carriers() {
        let field = |ty| NestedField::optional(1, "value", Type::Primitive(ty));
        assert_eq!(
            legacy_scalar_type(&field(PrimitiveType::Timestamp)),
            Some(LogicalType::Timestamp {
                unit: arrow::datatypes::TimeUnit::Microsecond,
                timezone: None
            })
        );
        for ty in [
            PrimitiveType::Timestamptz,
            PrimitiveType::TimestampNs,
            PrimitiveType::TimestamptzNs,
            PrimitiveType::Uuid,
            PrimitiveType::Fixed(16),
            PrimitiveType::Variant,
        ] {
            assert_eq!(legacy_scalar_type(&field(ty)), None);
        }
        for (physical, expected) in [
            (PrimitiveType::Boolean, LogicalType::Boolean),
            (PrimitiveType::Int, LogicalType::Int32),
            (PrimitiveType::Long, LogicalType::Int64),
            (PrimitiveType::Float, LogicalType::Float32),
            (PrimitiveType::Double, LogicalType::Float64),
            (PrimitiveType::String, LogicalType::Utf8),
            (PrimitiveType::Binary, LogicalType::Binary),
            (PrimitiveType::Date, LogicalType::Date32),
            (
                PrimitiveType::Decimal {
                    precision: 22,
                    scale: 3,
                },
                LogicalType::Decimal {
                    bits: 128,
                    precision: 22,
                    scale: 3,
                },
            ),
        ] {
            assert_eq!(legacy_scalar_type(&field(physical)), Some(expected));
        }
    }

    #[test]
    fn exact_provider_binding_ignores_metadata_and_binds_nested_replacement() {
        use crate::iceberg::spec::Literal;
        let child = NestedField::optional(2, "value", Type::Primitive(PrimitiveType::Long));
        let original = NestedField::required(
            1,
            "payload",
            Type::Struct(StructType::new(vec![Arc::new(child.clone())])),
        );
        let baseline = exact_provider_type_binding(&original).unwrap();
        let mut renamed = original.clone();
        renamed.name = "renamed".into();
        renamed.doc = Some("x".repeat(128 * 1024));
        let mut decorated_child = child.clone();
        decorated_child.doc = Some("updated documentation".into());
        decorated_child.initial_default = Some(Literal::long(11));
        decorated_child.write_default = Some(Literal::long(12));
        *renamed.field_type = Type::Struct(StructType::new(vec![Arc::new(decorated_child)]));
        assert_eq!(exact_provider_type_binding(&renamed).unwrap(), baseline);

        let mut replaced = child.clone();
        replaced.id = 3;
        let mut changed = original.clone();
        *changed.field_type = Type::Struct(StructType::new(vec![Arc::new(replaced)]));
        assert_ne!(exact_provider_type_binding(&changed).unwrap(), baseline);
        let mut required = child;
        required.required = true;
        *changed.field_type = Type::Struct(StructType::new(vec![Arc::new(required)]));
        assert_ne!(exact_provider_type_binding(&changed).unwrap(), baseline);

        let uuid = NestedField::required(1, "uuid", Type::Primitive(PrimitiveType::Uuid));
        let fixed = NestedField::required(1, "fixed", Type::Primitive(PrimitiveType::Fixed(16)));
        assert_ne!(
            exact_provider_type_binding(&uuid).unwrap(),
            exact_provider_type_binding(&fixed).unwrap()
        );
        let ts = NestedField::required(1, "ts", Type::Primitive(PrimitiveType::Timestamp));
        let tz = NestedField::required(1, "ts", Type::Primitive(PrimitiveType::Timestamptz));
        assert_ne!(
            exact_provider_type_binding(&ts).unwrap(),
            exact_provider_type_binding(&tz).unwrap()
        );
    }

    #[test]
    fn exact_logical_fields_preserve_existing_bitmap_hll_property_semantics() {
        let table = metadata(vec![
            Arc::new(NestedField::required(
                1,
                "bits",
                Type::Primitive(PrimitiveType::Binary),
            )),
            Arc::new(NestedField::optional(
                2,
                "sketch",
                Type::Primitive(PrimitiveType::Binary),
            )),
        ])
        .into_builder(None)
        .set_properties(std::collections::HashMap::from([
            ("novarocks.logical_type.BITS".into(), "BITMAP".into()),
            ("novarocks.logical_type.sketch".into(), "hll".into()),
        ]))
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let logical = exact_logical_fields(&table).unwrap();
        assert_eq!(logical[0].data_type, LogicalType::Bitmap);
        assert_eq!(logical[1].data_type, LogicalType::Hll);
        assert!(!logical[0].nullable);
        assert!(logical[1].nullable);
    }

    #[test]
    fn exact_provider_schema_rejects_nested_budget_before_recursive_conversion() {
        let mut ty = Type::Primitive(PrimitiveType::Long);
        for id in 2..70 {
            ty = Type::List(ListType::new(Arc::new(NestedField::list_element(
                id, ty, true,
            ))));
        }
        let table = metadata(vec![Arc::new(NestedField::required(1, "too_deep", ty))]);
        assert!(exact_logical_fields(&table).unwrap_err().contains("budget"));
        let field = NestedField::required(
            1,
            "x".repeat(70 * 1024),
            Type::Primitive(PrimitiveType::Long),
        );
        assert!(
            exact_provider_type_binding(&field)
                .unwrap_err()
                .contains("budget")
        );
        let wide = NestedField::required(
            1,
            "wide",
            Type::Struct(StructType::new(
                (2..3002)
                    .map(|id| {
                        Arc::new(NestedField::optional(
                            id,
                            format!("f{id}"),
                            Type::Primitive(PrimitiveType::Long),
                        ))
                    })
                    .collect(),
            )),
        );
        validate_provider_fields(std::iter::once(&wide)).unwrap();
        assert!(
            exact_provider_type_binding(&wide)
                .unwrap_err()
                .contains("byte budget")
        );
    }
}
