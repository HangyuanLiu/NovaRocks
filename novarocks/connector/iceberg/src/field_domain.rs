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

//! Exact provider-owned leaf domains over standard Iceberg INT/STRING carriers.
//! Declarations are resolved once from retained metadata; values never infer types.

use crate::iceberg::spec::{NestedField, PrimitiveType, Schema, TableMetadata, Type};
use crate::scalar_integer_domain::ScalarIntegerDomain;
use arrow::datatypes::{DataType, Field, SchemaRef};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

pub(crate) const PROPERTY: &str = "novarocks.field_domains.v1";
pub(crate) const PREFIX: &str = "novarocks.field_domains.";
const MAX_BYTES: usize = 1 << 20;
const MAX_FIELDS: usize = 16_384;

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) enum FieldDomain {
    #[serde(rename = "tinyint")]
    Int8,
    #[serde(rename = "smallint")]
    Int16,
    #[serde(rename = "json")]
    Json,
}
pub(crate) type FieldDomains = BTreeMap<i32, FieldDomain>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PersistedFieldDomains {
    None,
    LegacyTopIntegerV1(FieldDomains),
    FieldDomainsV1(FieldDomains),
}
fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}
impl FieldDomain {
    pub(crate) fn integer(self) -> Option<ScalarIntegerDomain> {
        match self {
            Self::Int8 => Some(ScalarIntegerDomain::Int8),
            Self::Int16 => Some(ScalarIntegerDomain::Int16),
            Self::Json => None,
        }
    }
    fn matches(self, ty: &Type) -> bool {
        match self {
            Self::Json => matches!(ty, Type::Primitive(PrimitiveType::String)),
            _ => matches!(ty, Type::Primitive(PrimitiveType::Int)),
        }
    }
    fn permits_history(self, ty: &Type) -> bool {
        self.matches(ty)
            || (self != Self::Json && matches!(ty, Type::Primitive(PrimitiveType::Long)))
    }
}
impl PersistedFieldDomains {
    pub(crate) fn fields(&self) -> &FieldDomains {
        static EMPTY: FieldDomains = BTreeMap::new();
        match self {
            Self::None => &EMPTY,
            Self::LegacyTopIntegerV1(v) | Self::FieldDomainsV1(v) => v,
        }
    }
}

// Deserialize entries incrementally, refusing duplicates and count before insertion.
struct DomainMap(FieldDomains);
impl<'de> serde::Deserialize<'de> for DomainMap {
    fn deserialize<D: serde::Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = DomainMap;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a bounded canonical field-ID domain map")
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                mut input: M,
            ) -> Result<Self::Value, M::Error> {
                let mut fields = BTreeMap::new();
                while let Some(key) = input.next_key::<String>()? {
                    let id = key.parse::<i32>().map_err(serde::de::Error::custom)?;
                    if id <= 0
                        || key != id.to_string()
                        || fields.len() >= MAX_FIELDS
                        || fields.contains_key(&id)
                    {
                        return Err(serde::de::Error::custom(
                            "duplicate, noncanonical, invalid, or excessive field IDs",
                        ));
                    }
                    fields.insert(id, input.next_value::<FieldDomain>()?);
                }
                Ok(DomainMap(fields))
            }
        }
        decoder.deserialize_map(Visitor)
    }
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Payload {
    version: u32,
    fields: DomainMap,
}

pub(crate) fn decode(raw: &str) -> Result<FieldDomains, ConnectorError> {
    if raw.len() > MAX_BYTES {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "Iceberg field domain payload exceeds its byte budget",
        ));
    }
    let payload: Payload = serde_json::from_str(raw)
        .map_err(|e| corrupt(format!("invalid Iceberg field domains: {e}")))?;
    if payload.version != 1 {
        return Err(corrupt("unsupported Iceberg field domain version"));
    }
    Ok(payload.fields.0)
}
pub(crate) fn encode(fields: &FieldDomains) -> Result<String, ConnectorError> {
    if fields.len() > MAX_FIELDS || fields.keys().any(|id| *id <= 0) {
        return Err(corrupt("invalid Iceberg field domain IDs or count"));
    }
    // Serialize numeric IDs in numeric order; BTreeMap<String, _> would sort lexically.
    #[derive(serde::Serialize)]
    struct Encoded<'a> {
        version: u32,
        fields: &'a FieldDomains,
    }
    let raw = serde_json::to_string(&Encoded { version: 1, fields })
        .map_err(|e| corrupt(e.to_string()))?;
    if raw.len() > MAX_BYTES {
        return Err(corrupt("Iceberg field domains exceed their byte budget"));
    }
    Ok(raw)
}
pub(crate) fn declarations(
    schema: &Schema,
    properties: &HashMap<String, String>,
) -> Result<PersistedFieldDomains, ConnectorError> {
    if properties
        .keys()
        .any(|k| k.starts_with(PREFIX) && k != PROPERTY)
    {
        return Err(corrupt("unsupported Iceberg field domain namespace"));
    }
    if let Some(raw) = properties.get(PROPERTY) {
        if properties.contains_key(crate::scalar_integer_domain::PROPERTY)
            || properties.iter().any(|(k, v)| {
                k.starts_with("novarocks.logical_type.")
                    && matches!(v.to_ascii_lowercase().as_str(), "tinyint" | "smallint")
            })
        {
            return Err(corrupt("conflicting Iceberg field domain authorities"));
        }
        let fields = decode(raw)?;
        validate_schema(schema, &fields)?;
        Ok(PersistedFieldDomains::FieldDomainsV1(fields))
    } else {
        let old = crate::scalar_integer_domain::declarations(schema, properties)?;
        if old.is_empty() {
            Ok(PersistedFieldDomains::None)
        } else {
            Ok(PersistedFieldDomains::LegacyTopIntegerV1(
                old.into_iter()
                    .map(|(id, d)| {
                        (
                            id,
                            match d {
                                ScalarIntegerDomain::Int8 => FieldDomain::Int8,
                                ScalarIntegerDomain::Int16 => FieldDomain::Int16,
                            },
                        )
                    })
                    .collect(),
            ))
        }
    }
}
pub(crate) fn metadata_declarations(
    metadata: &TableMetadata,
) -> Result<PersistedFieldDomains, ConnectorError> {
    let domains = declarations(metadata.current_schema(), metadata.properties())?;
    if matches!(domains, PersistedFieldDomains::LegacyTopIntegerV1(_)) {
        crate::scalar_integer_domain::metadata_declarations(metadata)?;
    }
    for schema in metadata.schemas_iter() {
        crate::schema_mapping::validate_exact_schema(schema).map_err(corrupt)?;
    }
    for (id, domain) in domains.fields() {
        let mut proven = false;
        for schema in metadata.schemas_iter() {
            if let Some(field) = schema.field_by_id(*id) {
                if !domain.permits_history(&field.field_type) {
                    return Err(corrupt(
                        "Iceberg field domain has incompatible storage history",
                    ));
                }
                proven |= domain.matches(&field.field_type);
                validate_default(field, *domain)?;
            }
        }
        if !proven {
            return Err(corrupt(
                "Iceberg field domain has no proven storage history",
            ));
        }
    }
    Ok(domains)
}
fn validate_default(field: &NestedField, domain: FieldDomain) -> Result<(), ConnectorError> {
    if !domain.matches(&field.field_type) {
        return Ok(());
    }
    for value in [field.initial_default.as_ref(), field.write_default.as_ref()]
        .into_iter()
        .flatten()
    {
        use crate::iceberg::spec::{Literal, PrimitiveLiteral};
        match (domain, value) {
            (FieldDomain::Json, Literal::Primitive(PrimitiveLiteral::String(_))) => {}
            (_, Literal::Primitive(PrimitiveLiteral::Int(value))) if domain.integer().is_some() => {
                domain.integer().unwrap().value(*value)?;
            }
            _ => {
                return Err(corrupt(
                    "Iceberg field domain default has incompatible storage",
                ));
            }
        }
    }
    Ok(())
}
pub(crate) fn validate_schema(
    schema: &Schema,
    domains: &FieldDomains,
) -> Result<(), ConnectorError> {
    crate::schema_mapping::validate_exact_schema(schema).map_err(corrupt)?;
    if domains.len() > MAX_FIELDS || domains.keys().any(|id| *id <= 0) {
        return Err(corrupt("invalid Iceberg field domain IDs or count"));
    }
    for (id, domain) in domains {
        if let Some(field) = schema.field_by_id(*id) {
            if !domain.permits_history(&field.field_type) {
                return Err(corrupt(
                    "Iceberg field domain requires its declared primitive storage",
                ));
            }
            validate_default(field, *domain)?;
        }
    }
    Ok(())
}
pub(crate) fn active(
    schema: &Schema,
    domains: &FieldDomains,
) -> Result<FieldDomains, ConnectorError> {
    validate_schema(schema, domains)?;
    Ok(domains
        .iter()
        .filter(|(id, d)| {
            schema
                .field_by_id(**id)
                .is_some_and(|f| d.matches(&f.field_type))
        })
        .map(|(id, d)| (*id, *d))
        .collect())
}
pub(crate) fn apply_field(
    field: &Field,
    storage: &NestedField,
    domains: &FieldDomains,
) -> Result<Field, ConnectorError> {
    let ty = match (field.data_type(), storage.field_type.as_ref()) {
        (DataType::Struct(fields), Type::Struct(s)) if fields.len() == s.fields().len() => {
            DataType::Struct(
                fields
                    .iter()
                    .zip(s.fields())
                    .map(|(f, p)| apply_field(f, p, domains).map(Arc::new))
                    .collect::<Result<Vec<_>, _>>()?
                    .into(),
            )
        }
        (DataType::List(f), Type::List(s)) => {
            DataType::List(Arc::new(apply_field(f, &s.element_field, domains)?))
        }
        (DataType::LargeList(f), Type::List(s)) => {
            DataType::LargeList(Arc::new(apply_field(f, &s.element_field, domains)?))
        }
        (DataType::Map(entries, sorted), Type::Map(s)) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(corrupt("Iceberg map entries must be struct"));
            };
            if fields.len() != 2 {
                return Err(corrupt("Iceberg map entries arity differs"));
            }
            let children = vec![
                Arc::new(apply_field(&fields[0], &s.key_field, domains)?),
                Arc::new(apply_field(&fields[1], &s.value_field, domains)?),
            ];
            DataType::Map(
                Arc::new(
                    entries
                        .as_ref()
                        .clone()
                        .with_data_type(DataType::Struct(children.into())),
                ),
                *sorted,
            )
        }
        (_, Type::Primitive(_)) => field.data_type().clone(),
        _ => return Err(corrupt("Iceberg field domain schema carrier differs")),
    };
    let mut output = field.clone().with_data_type(ty);
    if let Some(domain) = domains.get(&storage.id) {
        if domain.matches(&storage.field_type) {
            if let Some(integer) = domain.integer() {
                output = output.with_data_type(integer.data_type());
            } else {
                let mut metadata = output.metadata().clone();
                metadata.insert("nr_logical_type".into(), "json".into());
                output = output.with_metadata(metadata);
            }
        } else if !domain.permits_history(&storage.field_type) {
            return Err(corrupt("Iceberg field domain storage differs"));
        }
    }
    Ok(output)
}
pub(crate) fn apply_schema(
    arrow: SchemaRef,
    schema: &Schema,
    domains: &FieldDomains,
) -> Result<SchemaRef, ConnectorError> {
    validate_schema(schema, domains)?;
    if arrow.fields().len() != schema.as_struct().fields().len() {
        return Err(corrupt("Iceberg field domain schema arity differs"));
    }
    let fields = arrow
        .fields()
        .iter()
        .zip(schema.as_struct().fields())
        .map(|(f, p)| apply_field(f, p, domains).map(Arc::new))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(arrow::datatypes::Schema::new_with_metadata(
        fields,
        arrow.metadata().clone(),
    )))
}
pub(crate) fn metadata_sql_schema(
    metadata: &TableMetadata,
    schema: &Schema,
) -> Result<SchemaRef, ConnectorError> {
    let domains = metadata_declarations(metadata)?;
    apply_schema(
        crate::schema_mapping::sql_read_schema_from_iceberg(schema).map_err(corrupt)?,
        schema,
        domains.fields(),
    )
}

/// Preserve historical complete root-narrow encoding; new subtree facts are
/// included only where JSON or a nested narrow domain changes the old meaning.
pub(crate) fn exact_provider_type_binding(
    field: &NestedField,
    domains: &FieldDomains,
) -> Result<bytes::Bytes, String> {
    let old = crate::schema_mapping::exact_provider_type_binding(field)?;
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(field.clone())])
        .build()
        .map_err(|e| e.to_string())?;
    let relevant = active(&schema, domains).map_err(|e| e.to_string())?;
    let extra: FieldDomains = relevant
        .into_iter()
        .filter(|(id, d)| *id != field.id || *d == FieldDomain::Json)
        .collect();
    if extra.is_empty() {
        return Ok(old);
    }
    let mut output = b"novarocks.iceberg.exact-field-domains.v1:".to_vec();
    output.extend_from_slice(&old);
    output.push(b':');
    output.extend_from_slice(encode(&extra).map_err(|e| e.to_string())?.as_bytes());
    if output.len() > 64 * 1024 {
        return Err("provider field binding exceeds its byte budget".into());
    }
    Ok(output.into())
}

/// Restore logical leaf arrays using accurate schema facts. Visibility tracks
/// parent validity and collection offsets, so hidden physical children cannot
/// create a range error. No JSON parsing or reformatting occurs here.
pub(crate) fn restore_array(
    source: &arrow::array::ArrayRef,
    storage: &NestedField,
    target: &Field,
    domains: &FieldDomains,
) -> Result<arrow::array::ArrayRef, ConnectorError> {
    let visible = vec![true; source.len()];
    restore_visible(source, storage, target, domains, &visible)
}
fn restore_visible(
    source: &arrow::array::ArrayRef,
    storage: &NestedField,
    target: &Field,
    domains: &FieldDomains,
    visible: &[bool],
) -> Result<arrow::array::ArrayRef, ConnectorError> {
    use arrow::array::{
        Array, Int8Builder, Int16Builder, Int32Array, LargeListArray, ListArray, MapArray,
        StructArray,
    };
    if source.len() != visible.len() {
        return Err(corrupt("Iceberg domain visibility length differs"));
    }
    if let Some(integer) = domains.get(&storage.id).and_then(|d| d.integer())
        && matches!(
            storage.field_type.as_ref(),
            Type::Primitive(PrimitiveType::Int)
        )
    {
        let values = source
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| corrupt("Iceberg declared integer requires INT32 physical values"))?;
        macro_rules! narrow { ($builder:ty, $ty:ty) => {{
            let mut out=<$builder>::with_capacity(values.len());
            for (index, value) in values.iter().enumerate() {
                out.append_option(value.map(|v| if visible[index] { <$ty>::try_from(v).map_err(|_| corrupt("Iceberg visible INT exceeds its declared logical domain")) } else { Ok(0) }).transpose()?);
            }
            Arc::new(out.finish()) as arrow::array::ArrayRef
        }} }
        return Ok(match integer {
            ScalarIntegerDomain::Int8 => narrow!(Int8Builder, i8),
            ScalarIntegerDomain::Int16 => narrow!(Int16Builder, i16),
        });
    }
    match (storage.field_type.as_ref(), target.data_type()) {
        (Type::Struct(s), DataType::Struct(fields)) => {
            let array = source
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| corrupt("Iceberg struct has incompatible array"))?;
            if array.num_columns() != s.fields().len() || fields.len() != s.fields().len() {
                return Err(corrupt("Iceberg struct array arity differs"));
            }
            let mask = (0..array.len())
                .map(|i| visible[i] && array.is_valid(i))
                .collect::<Vec<_>>();
            let children = array
                .columns()
                .iter()
                .zip(s.fields())
                .zip(fields)
                .map(|((a, p), f)| restore_visible(a, p, f, domains, &mask))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Arc::new(
                StructArray::try_new(fields.clone(), children, array.nulls().cloned())
                    .map_err(|e| corrupt(e.to_string()))?,
            ))
        }
        (Type::List(s), DataType::List(f)) => {
            let array = source
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| corrupt("Iceberg list has incompatible array"))?;
            let mut mask = vec![false; array.values().len()];
            for i in 0..array.len() {
                if visible[i] && array.is_valid(i) {
                    let offsets = array.value_offsets();
                    mask[offsets[i] as usize..offsets[i + 1] as usize].fill(true);
                }
            }
            let values = restore_visible(array.values(), &s.element_field, f, domains, &mask)?;
            Ok(Arc::new(
                ListArray::try_new(
                    f.clone(),
                    array.offsets().clone(),
                    values,
                    array.nulls().cloned(),
                )
                .map_err(|e| corrupt(e.to_string()))?,
            ))
        }
        (Type::List(s), DataType::LargeList(f)) => {
            let array = source
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| corrupt("Iceberg large list has incompatible array"))?;
            let mut mask = vec![false; array.values().len()];
            for i in 0..array.len() {
                if visible[i] && array.is_valid(i) {
                    let offsets = array.value_offsets();
                    mask[offsets[i] as usize..offsets[i + 1] as usize].fill(true);
                }
            }
            let values = restore_visible(array.values(), &s.element_field, f, domains, &mask)?;
            Ok(Arc::new(
                LargeListArray::try_new(
                    f.clone(),
                    array.offsets().clone(),
                    values,
                    array.nulls().cloned(),
                )
                .map_err(|e| corrupt(e.to_string()))?,
            ))
        }
        (Type::Map(s), DataType::Map(entries, sorted)) => {
            let array = source
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| corrupt("Iceberg map has incompatible array"))?;
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(corrupt("Iceberg map entries have incompatible type"));
            };
            if fields.len() != 2 {
                return Err(corrupt("Iceberg map entries arity differs"));
            }
            let mut mask = vec![false; array.entries().len()];
            for i in 0..array.len() {
                if visible[i] && array.is_valid(i) {
                    let offsets = array.value_offsets();
                    mask[offsets[i] as usize..offsets[i + 1] as usize].fill(true);
                }
            }
            let key = restore_visible(array.keys(), &s.key_field, &fields[0], domains, &mask)?;
            let value =
                restore_visible(array.values(), &s.value_field, &fields[1], domains, &mask)?;
            let entries_array = StructArray::try_new(
                fields.clone(),
                vec![key, value],
                array.entries().nulls().cloned(),
            )
            .map_err(|e| corrupt(e.to_string()))?;
            Ok(Arc::new(
                MapArray::try_new(
                    entries.clone(),
                    array.offsets().clone(),
                    entries_array,
                    array.nulls().cloned(),
                    *sorted,
                )
                .map_err(|e| corrupt(e.to_string()))?,
            ))
        }
        (Type::Primitive(_), _) if source.data_type() == target.data_type() => Ok(source.clone()),
        (Type::Primitive(_), _) => arrow::compute::cast_with_options(
            source,
            target.data_type(),
            &arrow::compute::CastOptions {
                safe: false,
                ..Default::default()
            },
        )
        .map_err(|e| corrupt(e.to_string())),
        _ => Err(corrupt("Iceberg declared array carrier differs")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::spec::{ListType, StructType};
    use arrow::array::{Array, Int8Array, Int32Array, ListArray, StringArray, StructArray};
    use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};

    fn schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::optional(
                    2,
                    "root",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::optional(
                    3,
                    "items",
                    Type::List(ListType::new(Arc::new(NestedField::list_element(
                        5,
                        Type::Primitive(PrimitiveType::Int),
                        false,
                    )))),
                )),
                Arc::new(NestedField::optional(
                    9,
                    "js",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .unwrap()
    }
    #[test]
    fn field_domain_codec_is_bounded_closed_and_canonical() {
        let input = r#"{"version":1,"fields":{"2":"tinyint","5":"smallint","9":"json"}}"#;
        assert_eq!(encode(&decode(input).unwrap()).unwrap(), input);
        for raw in [
            r#"{"version":2,"fields":{}}"#,
            r#"{"version":1,"fields":{},"extra":1}"#,
            r#"{"version":1,"version":1,"fields":{}}"#,
            r#"{"version":1,"fields":{"1":"json","1":"json"}}"#,
            r#"{"version":1,"fields":{"0":"json"}}"#,
            r#"{"version":1,"fields":{"01":"json"}}"#,
            r#"{"version":1,"fields":{"1":"JSON"}}"#,
        ] {
            assert!(decode(raw).is_err(), "{raw}");
        }
        assert!(decode(&" ".repeat(MAX_BYTES + 1)).is_err());
        let excessive = FieldDomains::from_iter((1..=16_385).map(|id| (id, FieldDomain::Json)));
        assert!(encode(&excessive).is_err());
        let raw = format!(
            "{{\"version\":1,\"fields\":{{{}}}}}",
            (1..=16_385)
                .map(|id| format!("\"{id}\":\"json\""))
                .collect::<Vec<_>>()
                .join(",")
        );
        assert!(decode(&raw).is_err());
    }
    #[test]
    fn field_domain_recursive_schema_and_independent_binding_preserve_old_bytes() {
        let schema = schema();
        let domains = FieldDomains::from([
            (2, FieldDomain::Int8),
            (5, FieldDomain::Int16),
            (9, FieldDomain::Json),
        ]);
        let read = apply_schema(
            crate::schema_mapping::sql_read_schema_from_iceberg(&schema).unwrap(),
            &schema,
            &domains,
        )
        .unwrap();
        assert_eq!(read.field(0).data_type(), &DataType::Int8);
        let DataType::List(child) = read.field(1).data_type() else {
            panic!("list");
        };
        assert_eq!(child.data_type(), &DataType::Int16);
        assert_eq!(
            novarocks_types::logical_type::logical_field_from_engine_arrow(read.field(2))
                .unwrap()
                .data_type,
            novarocks_types::logical_type::LogicalType::Json
        );
        let root = &schema.as_struct().fields()[0];
        assert_eq!(
            exact_provider_type_binding(root, &domains).unwrap(),
            crate::schema_mapping::exact_provider_type_binding(root).unwrap()
        );
        let items = &schema.as_struct().fields()[1];
        assert_ne!(
            exact_provider_type_binding(items, &domains).unwrap(),
            crate::schema_mapping::exact_provider_type_binding(items).unwrap()
        );
        let plain = apply_schema(
            crate::schema_mapping::sql_read_schema_from_iceberg(&schema).unwrap(),
            &schema,
            &FieldDomains::new(),
        )
        .unwrap();
        assert_eq!(plain.field(0).data_type(), &DataType::Int32);
        assert_eq!(
            novarocks_types::logical_type::logical_field_from_engine_arrow(plain.field(2))
                .unwrap()
                .data_type,
            novarocks_types::logical_type::LogicalType::Utf8
        );
        assert!(validate_schema(&schema, &FieldDomains::from([(9, FieldDomain::Int8)])).is_err());
    }
    #[test]
    fn field_domain_authority_selection_never_falls_back() {
        let schema = schema();
        let mut properties = HashMap::from([(
            PROPERTY.into(),
            encode(&FieldDomains::from([(5, FieldDomain::Int8)])).unwrap(),
        )]);
        assert!(matches!(
            declarations(&schema, &properties).unwrap(),
            PersistedFieldDomains::FieldDomainsV1(_)
        ));
        properties.insert(crate::scalar_integer_domain::PROPERTY.into(), "{}".into());
        assert!(declarations(&schema, &properties).is_err());
        properties.remove(crate::scalar_integer_domain::PROPERTY);
        properties.insert("novarocks.field_domains.v2".into(), "{}".into());
        assert!(declarations(&schema, &properties).is_err());
    }
    #[test]
    fn field_domain_visible_parent_masks_and_sliced_offsets_control_range_checks() {
        let leaf = Arc::new(NestedField::optional(
            4,
            "n",
            Type::Primitive(PrimitiveType::Int),
        ));
        let storage = NestedField::optional(3, "s", Type::Struct(StructType::new(vec![leaf])));
        let field = Field::new(
            "s",
            DataType::Struct(vec![Arc::new(Field::new("n", DataType::Int32, true))].into()),
            true,
        );
        let domains = FieldDomains::from([(4, FieldDomain::Int8)]);
        let target = apply_field(&field, &storage, &domains).unwrap();
        let source: arrow::array::ArrayRef = Arc::new(StructArray::new(
            match field.data_type() {
                DataType::Struct(f) => f.clone(),
                _ => unreachable!(),
            },
            vec![Arc::new(Int32Array::from(vec![Some(999), Some(127), None]))],
            Some(NullBuffer::from(vec![false, true, true])),
        ));
        let restored = restore_array(&source, &storage, &target, &domains).unwrap();
        assert_eq!(restored.nulls(), source.nulls());
        let array = restored.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(
            array
                .column(0)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(1),
            127
        );
        let bad: arrow::array::ArrayRef = Arc::new(StructArray::new(
            match field.data_type() {
                DataType::Struct(f) => f.clone(),
                _ => unreachable!(),
            },
            vec![Arc::new(Int32Array::from(vec![999]))],
            None,
        ));
        assert!(restore_array(&bad, &storage, &target, &domains).is_err());
        let list_schema = schema();
        let list_storage = &list_schema.as_struct().fields()[1];
        let domains = FieldDomains::from([(5, FieldDomain::Int8)]);
        let source_field = Arc::new(Field::new("element", DataType::Int32, true));
        let list: arrow::array::ArrayRef = Arc::new(ListArray::new(
            source_field,
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 3])),
            Arc::new(Int32Array::from(vec![999, 127, -128])),
            None,
        ));
        let sliced = list.slice(1, 1);
        let field = Field::new("items", sliced.data_type().clone(), true);
        let target = apply_field(&field, list_storage, &domains).unwrap();
        let restored = restore_array(&sliced, list_storage, &target, &domains).unwrap();
        assert_eq!(restored.len(), 1);
        assert_eq!(
            restored
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value_offsets(),
            &[1, 3]
        );
    }
    #[test]
    fn field_domain_json_keeps_engine_text_unchanged() {
        let schema = schema();
        let field = &schema.as_struct().fields()[2];
        let domains = FieldDomains::from([(9, FieldDomain::Json)]);
        let source: arrow::array::ArrayRef = Arc::new(StringArray::from(vec![
            Some(" {\\\"b\\\":2, \\\"a\\\":1} "),
            None,
        ]));
        let target = apply_field(&Field::new("js", DataType::Utf8, true), field, &domains).unwrap();
        let restored = restore_array(&source, field, &target, &domains).unwrap();
        assert!(Arc::ptr_eq(&source, &restored));
    }
}

/// Pair requested logical leaves with the SDK's newly allocated IDs. The
/// allocator, never source IDs or names alone, determines target identity.
pub(crate) fn requested_field_domains(
    field: &NestedField,
    logical: &novarocks_types::logical_type::LogicalType,
) -> Result<FieldDomains, ConnectorError> {
    logical.validate(Default::default()).map_err(corrupt)?;
    crate::schema_mapping::exact_provider_type_binding(field).map_err(corrupt)?;
    fn collect(
        field: &NestedField,
        logical: &novarocks_types::logical_type::LogicalType,
        out: &mut FieldDomains,
    ) -> Result<(), ConnectorError> {
        use novarocks_types::logical_type::LogicalType as L;
        let domain = match logical {
            L::Int8 => Some(FieldDomain::Int8),
            L::Int16 => Some(FieldDomain::Int16),
            L::Json => Some(FieldDomain::Json),
            _ => None,
        };
        if let Some(domain) = domain {
            if !domain.matches(&field.field_type) {
                return Err(corrupt(
                    "requested logical domain has incompatible SDK storage",
                ));
            }
            out.insert(field.id, domain);
            validate_default(field, domain)?;
            return Ok(());
        }
        match (field.field_type.as_ref(), logical) {
            (Type::Struct(s), L::Struct(l)) if s.fields().len() == l.len() => {
                for (p, l) in s.fields().iter().zip(l) {
                    if p.name != l.name || p.required == l.nullable {
                        return Err(corrupt("requested struct shape differs from SDK storage"));
                    }
                    collect(p, &l.data_type, out)?;
                }
            }
            (
                Type::List(s),
                L::Array {
                    element,
                    fixed_length: None,
                },
            ) if s.element_field.required != element.nullable => {
                collect(&s.element_field, &element.data_type, out)?
            }
            (Type::Map(s), L::Map { key, value })
                if s.key_field.required && s.value_field.required != value.nullable =>
            {
                collect(&s.key_field, &key.data_type, out)?;
                collect(&s.value_field, &value.data_type, out)?;
            }
            (Type::Primitive(_), _)
                if !matches!(logical, L::Struct(_) | L::Array { .. } | L::Map { .. }) => {}
            _ => return Err(corrupt("requested logical shape differs from SDK storage")),
        }
        Ok(())
    }
    let mut out = FieldDomains::new();
    collect(field, logical, &mut out)?;
    Ok(out)
}

#[cfg(test)]
mod history_tests {
    use super::*;
    use crate::iceberg::{
        TableUpdate,
        spec::{FormatVersion, PartitionSpec, SortOrder, TableMetadataBuilder},
    };
    fn metadata() -> TableMetadata {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                7,
                "tiny",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .unwrap();
        TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "memory://domains".into(),
            FormatVersion::V3,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata
    }
    fn property(metadata: TableMetadata, fields: FieldDomains) -> TableMetadata {
        TableUpdate::SetProperties {
            updates: HashMap::from([(PROPERTY.into(), encode(&fields).unwrap())]),
        }
        .apply(metadata.into_builder(None))
        .unwrap()
        .build()
        .unwrap()
        .metadata
    }
    #[test]
    fn field_domain_history_requires_real_ids_and_restores_old_int_after_promotion() {
        let initial = metadata();
        let id = initial.current_schema().field_by_name("tiny").unwrap().id;
        let domains = FieldDomains::from([(id, FieldDomain::Int8)]);
        let initial = property(initial, domains.clone());
        assert!(metadata_declarations(&initial).is_ok());
        let promoted = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                id,
                "renamed",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap();
        let mut builder = initial.into_builder(None);
        for update in [
            TableUpdate::AddSchema {
                schema: promoted,
                last_column_id: Some(id),
            },
            TableUpdate::SetCurrentSchema { schema_id: -1 },
        ] {
            builder = update.apply(builder).unwrap();
        }
        let metadata = builder.build().unwrap().metadata;
        assert_eq!(
            metadata_sql_schema(&metadata, metadata.current_schema())
                .unwrap()
                .field(0)
                .data_type(),
            &DataType::Int64
        );
        let old = metadata
            .schemas_iter()
            .find(|s| {
                matches!(
                    s.field_by_id(id).map(|f| f.field_type.as_ref()),
                    Some(Type::Primitive(PrimitiveType::Int))
                )
            })
            .unwrap();
        assert_eq!(
            metadata_sql_schema(&metadata, old)
                .unwrap()
                .field(0)
                .data_type(),
            &DataType::Int8
        );
        let corrupted = property(
            metadata,
            FieldDomains::from([(id + 100, FieldDomain::Int8)]),
        );
        assert!(metadata_declarations(&corrupted).is_err());
    }
    #[test]
    fn field_domain_legacy_namespace_migration_keeps_complete_bound_root_identity() {
        let initial = metadata();
        let id = initial.current_schema().field_by_name("tiny").unwrap().id;
        let initial = TableUpdate::SetProperties {
            updates: HashMap::from([
                (
                    crate::scalar_integer_domain::PROPERTY.into(),
                    format!("{{\"{id}\":\"tinyint\"}}"),
                ),
                ("novarocks.logical_type.tiny".into(), "tinyint".into()),
            ]),
        }
        .apply(initial.into_builder(None))
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let old = metadata_declarations(&initial).unwrap();
        assert!(matches!(old, PersistedFieldDomains::LegacyTopIntegerV1(_)));
        let binding = exact_provider_type_binding(
            initial.current_schema().field_by_id(id).unwrap(),
            old.fields(),
        )
        .unwrap();
        let mut builder = initial.into_builder(None);
        for update in [
            TableUpdate::SetProperties {
                updates: HashMap::from([(PROPERTY.into(), encode(old.fields()).unwrap())]),
            },
            TableUpdate::RemoveProperties {
                removals: vec![
                    crate::scalar_integer_domain::PROPERTY.into(),
                    "novarocks.logical_type.tiny".into(),
                ],
            },
        ] {
            builder = update.apply(builder).unwrap();
        }
        let metadata = builder.build().unwrap().metadata;
        let migrated = metadata_declarations(&metadata).unwrap();
        assert_eq!(
            exact_provider_type_binding(
                metadata.current_schema().field_by_id(id).unwrap(),
                migrated.fields()
            )
            .unwrap(),
            binding
        );
        assert_eq!(
            crate::schema_mapping::legacy_scalar_type(
                metadata.current_schema().field_by_id(id).unwrap()
            ),
            Some(novarocks_types::logical_type::LogicalType::Int32)
        );
        assert_eq!(
            metadata_sql_schema(&metadata, metadata.current_schema())
                .unwrap()
                .field(0)
                .data_type(),
            &DataType::Int8
        );
    }
    #[test]
    fn field_domain_nested_defaults_and_ordered_map_values_are_checked() {
        use crate::iceberg::spec::{ListType, Literal, MapType, PrimitiveLiteral};
        use arrow::array::{
            Array, ArrayRef, Int8Array, Int32Array, MapArray, StringArray, StructArray,
        };
        use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
        let child = NestedField::list_element(3, Type::Primitive(PrimitiveType::Int), false)
            .with_initial_default(Literal::Primitive(PrimitiveLiteral::Int(128)));
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "a",
                Type::List(ListType::new(Arc::new(child))),
            ))])
            .build()
            .unwrap();
        assert!(validate_schema(&schema, &FieldDomains::from([(3, FieldDomain::Int8)])).is_err());
        let storage = NestedField::optional(
            1,
            "m",
            Type::Map(MapType::new(
                Arc::new(NestedField::map_key_element(
                    2,
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::map_value_element(
                    3,
                    Type::Primitive(PrimitiveType::Int),
                    false,
                )),
            )),
        );
        let fields: arrow::datatypes::Fields = vec![
            Arc::new(Field::new("key", DataType::Utf8, false)),
            Arc::new(Field::new("value", DataType::Int32, true)),
        ]
        .into();
        let entries = Arc::new(Field::new(
            "entries",
            DataType::Struct(fields.clone()),
            false,
        ));
        let values = StructArray::new(
            fields,
            vec![
                Arc::new(StringArray::from(vec!["a", "z", "a"])),
                Arc::new(Int32Array::from(vec![999, 127, -128])),
            ],
            None,
        );
        let source: ArrayRef = Arc::new(MapArray::new(
            entries.clone(),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 3])),
            values,
            Some(NullBuffer::from(vec![false, true])),
            true,
        ));
        let domains = FieldDomains::from([(3, FieldDomain::Int8)]);
        let target = apply_field(
            &Field::new("m", DataType::Map(entries, true), true),
            &storage,
            &domains,
        )
        .unwrap();
        let output = restore_array(&source, &storage, &target, &domains).unwrap();
        let output = output.as_any().downcast_ref::<MapArray>().unwrap();
        assert!(matches!(output.data_type(), DataType::Map(_, true)));
        assert_eq!(output.nulls(), source.nulls());
        assert_eq!(
            output
                .keys()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(1),
            "z"
        );
        assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(2),
            -128
        );
    }
}
