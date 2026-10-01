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

//! Native plan-wire `TypeDesc` decoding.
//!
//! This is the inverse of this crate's Arrow-to-native type encoding. It is a
//! pure conversion of a sealed wire DTO and never depends on a role runtime,
//! Connector, or request assembly state.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use novarocks_proto_models::common;
use novarocks_types::logical::{LogicalType, field_with_logical_type};

const TIME_UNIT_MICROS: i32 = 2;
const TIME_UNIT_NANOS: i32 = 3;

pub fn decode_type(desc: &common::TypeDesc) -> Result<DataType, String> {
    decode_type_inner(desc)
}

pub fn decode_field_type(
    name: &str,
    nullable: bool,
    desc: &common::TypeDesc,
) -> Result<Field, String> {
    let data_type = decode_type_inner(desc)?;
    let field = Field::new(name, data_type, nullable);
    Ok(match logical_type_from_desc(desc) {
        Some(logical_type) => field_with_logical_type(field, logical_type),
        None => field,
    })
}

/// Fresh Native field construction keeps exact immutable metadata owners.
/// This does not infer origins from arbitrary Arrow fields or schemas.
#[derive(Clone, Debug)]
pub struct OwnedNativeField {
    field: arrow::datatypes::FieldRef,
    metadata_origins: novarocks_types::arrow_metadata_owner::FieldMetadataOrigins,
}
impl OwnedNativeField {
    pub fn field(&self) -> &arrow::datatypes::FieldRef {
        &self.field
    }
    pub fn metadata_origins(&self) -> &novarocks_types::arrow_metadata_owner::FieldMetadataOrigins {
        &self.metadata_origins
    }
}

pub fn decode_field_type_owned(
    name: &str,
    nullable: bool,
    desc: &common::TypeDesc,
) -> Result<OwnedNativeField, String> {
    // Preflight the sealed Vec/strings before allocating any Arrow field,
    // name, metadata table or receipt collection.
    let mut preflight = NativeFieldPreflight { nodes: 0, bytes: 0 };
    preflight.field(name, desc, 0)?;
    let mut owners = Vec::with_capacity(preflight.nodes);
    let field = decode_owned_field(name, nullable, desc, &mut owners)?;
    let metadata_origins =
        novarocks_types::arrow_metadata_owner::FieldMetadataOrigins::try_new(owners, 65_536)
            .map_err(|_| "native field metadata origins exceed the bounded profile".to_string())?;
    Ok(OwnedNativeField {
        field,
        metadata_origins,
    })
}

/// Check the whole output layout before constructing any field or provenance
/// vector. Per-field checks alone cannot bound a wide combined tree.
pub fn preflight_native_output_fields(columns: &[common::OutputColumn]) -> Result<(), String> {
    if columns.len() > 65_536 {
        return Err("native output layout exceeds the source field profile".to_string());
    }
    let mut preflight = NativeFieldPreflight { nodes: 0, bytes: 0 };
    for column in columns {
        if let Some(desc) = &column.r#type {
            preflight.field(&column.name, desc, 0)?;
        } else {
            // Capacity preflight does not replace the adapter's exact missing
            // field error or allocate any Arrow owner for malformed input.
            preflight.charge_field(&column.name)?;
        }
    }
    Ok(())
}

struct NativeFieldPreflight {
    nodes: usize,
    bytes: usize,
}
impl NativeFieldPreflight {
    fn charge_field(&mut self, name: &str) -> Result<(), String> {
        use novarocks_result_contract::RootProfileV1;
        if name.len() > RootProfileV1::MAX_NAME_BYTES {
            return Err("native field name exceeds the bounded profile".to_string());
        }
        self.nodes = self
            .nodes
            .checked_add(1)
            .filter(|nodes| *nodes <= 65_536)
            .ok_or("native field tree exceeds the bounded node profile")?;
        // Includes the field/Arc, slot facts, provenance index and one small
        // logical metadata pair. Data buffers remain separate source owners.
        self.bytes = self
            .bytes
            .checked_add(name.len())
            .and_then(|v| v.checked_add(512))
            .filter(|bytes| *bytes <= 96 * 1024 * 1024)
            .ok_or("native field construction exceeds the source profile")?;
        Ok(())
    }
    fn field(&mut self, name: &str, desc: &common::TypeDesc, depth: usize) -> Result<(), String> {
        use common::type_desc::Kind;
        use novarocks_result_contract::RootProfileV1;
        if depth > RootProfileV1::MAX_DEPTH {
            return Err("native field tree exceeds the bounded depth profile".to_string());
        }
        self.charge_field(name)?;
        let Some(kind) = desc.kind.as_ref() else {
            return Ok(());
        };
        match kind {
            Kind::Scalar(scalar) => {
                if let Some(zone) = &scalar.time_zone {
                    if zone.len() > RootProfileV1::MAX_NAME_BYTES {
                        return Err(
                            "native timestamp timezone exceeds the source profile".to_string()
                        );
                    }
                    self.bytes = self
                        .bytes
                        .checked_add(zone.len())
                        .filter(|bytes| *bytes <= 96 * 1024 * 1024)
                        .ok_or("native field construction exceeds the source profile")?;
                }
            }
            Kind::List(list) => {
                if let Some(element) = &list.element {
                    self.field("item", element, depth + 1)?;
                }
            }
            Kind::Map(map) => {
                self.charge_field("entries")?;
                if let Some(key) = &map.key {
                    self.field("key", key, depth + 1)?;
                }
                if let Some(value) = &map.value {
                    self.field("value", value, depth + 1)?;
                }
            }
            Kind::Strct(strct) => {
                if strct.fields.len() > 65_536 {
                    return Err("native struct exceeds the bounded field profile".to_string());
                }
                for field in &strct.fields {
                    if let Some(desc) = &field.r#type {
                        self.field(&field.name, desc, depth + 1)?;
                    } else {
                        self.charge_field(&field.name)?;
                    }
                }
            }
        }
        Ok(())
    }
}

fn owned_metadata_field(
    name: &str,
    nullable: bool,
    data_type: DataType,
    logical_type: Option<LogicalType>,
    owners: &mut Vec<novarocks_types::arrow_metadata_owner::MetadataOwnedField>,
) -> Result<arrow::datatypes::FieldRef, String> {
    use novarocks_types::arrow_metadata_owner::{ArrowMetadataOwner, MetadataOwnerLimits};
    let entries = logical_type
        .map(|logical_type| {
            vec![(
                novarocks_types::logical::NR_LOGICAL_TYPE_KEY.to_string(),
                logical_type.metadata_value().to_string(),
            )]
        })
        .unwrap_or_default();
    let owner = ArrowMetadataOwner::try_new(
        entries,
        MetadataOwnerLimits {
            entries: 1,
            construction_bytes: 4096,
        },
    )
    .map_err(|_| "native logical metadata construction exceeds its bounded profile".to_string())?
    .into_field(name.to_string(), data_type, nullable);
    let field = Arc::clone(owner.field());
    owners.push(owner);
    Ok(field)
}

fn decode_owned_field(
    name: &str,
    nullable: bool,
    desc: &common::TypeDesc,
    owners: &mut Vec<novarocks_types::arrow_metadata_owner::MetadataOwnedField>,
) -> Result<arrow::datatypes::FieldRef, String> {
    use common::type_desc::Kind;
    let data_type = match desc.kind.as_ref().ok_or("TypeDesc.kind missing")? {
        Kind::Scalar(scalar) => decode_scalar_type(scalar)?,
        Kind::List(list) => DataType::List(decode_owned_field(
            "item",
            true,
            list.element.as_ref().ok_or("ListType.element missing")?,
            owners,
        )?),
        Kind::Map(map) => {
            let key = decode_owned_field(
                "key",
                true,
                map.key.as_ref().ok_or("MapType.key missing")?,
                owners,
            )?;
            let value = decode_owned_field(
                "value",
                true,
                map.value.as_ref().ok_or("MapType.value missing")?,
                owners,
            )?;
            let entries = owned_metadata_field(
                "entries",
                false,
                DataType::Struct(vec![key, value].into()),
                None,
                owners,
            )?;
            DataType::Map(entries, false)
        }
        Kind::Strct(strct) => {
            let mut fields = Vec::with_capacity(strct.fields.len());
            for field in &strct.fields {
                fields.push(decode_owned_field(
                    &field.name,
                    true,
                    field.r#type.as_ref().ok_or("StructField.type missing")?,
                    owners,
                )?);
            }
            DataType::Struct(fields.into())
        }
    };
    owned_metadata_field(
        name,
        nullable,
        data_type,
        logical_type_from_desc(desc),
        owners,
    )
}

fn decode_type_inner(desc: &common::TypeDesc) -> Result<DataType, String> {
    use common::type_desc::Kind;

    match desc.kind.as_ref().ok_or("TypeDesc.kind missing")? {
        Kind::Scalar(scalar) => decode_scalar_type(scalar),
        Kind::List(list) => {
            let element = list.element.as_ref().ok_or("ListType.element missing")?;
            Ok(DataType::List(Arc::new(decode_field_type(
                "item", true, element,
            )?)))
        }
        Kind::Map(map) => {
            let key = map.key.as_ref().ok_or("MapType.key missing")?;
            let value = map.value.as_ref().ok_or("MapType.value missing")?;
            let entries = Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Arc::new(decode_field_type("key", true, key)?),
                    Arc::new(decode_field_type("value", true, value)?),
                ])),
                false,
            );
            Ok(DataType::Map(Arc::new(entries), false))
        }
        Kind::Strct(strct) => {
            let fields = strct
                .fields
                .iter()
                .map(|field| {
                    let field_type = field.r#type.as_ref().ok_or("StructField.type missing")?;
                    Ok(Arc::new(decode_field_type(&field.name, true, field_type)?))
                })
                .collect::<Result<Vec<_>, String>>()?;
            Ok(DataType::Struct(Fields::from(fields)))
        }
    }
}

fn decode_scalar_type(scalar: &common::ScalarType) -> Result<DataType, String> {
    use common::PrimitiveType;

    let primitive = PrimitiveType::try_from(scalar.r#type)
        .map_err(|_| format!("unknown primitive type {}", scalar.r#type))?;
    match primitive {
        PrimitiveType::Unspecified => Err("primitive type is unspecified".to_string()),
        PrimitiveType::NullType => Ok(DataType::Null),
        PrimitiveType::Boolean => Ok(DataType::Boolean),
        PrimitiveType::Tinyint => Ok(DataType::Int8),
        PrimitiveType::Smallint => Ok(DataType::Int16),
        PrimitiveType::Int => Ok(DataType::Int32),
        PrimitiveType::Bigint => Ok(DataType::Int64),
        PrimitiveType::Largeint => Ok(DataType::FixedSizeBinary(16)),
        PrimitiveType::Float => Ok(DataType::Float32),
        PrimitiveType::Double => Ok(DataType::Float64),
        PrimitiveType::Decimal32
        | PrimitiveType::Decimal64
        | PrimitiveType::Decimal128
        | PrimitiveType::Decimal256 => decode_decimal_type(primitive, scalar),
        PrimitiveType::Date => Ok(DataType::Date32),
        PrimitiveType::Datetime => {
            let unit = match scalar.time_unit {
                None | Some(TIME_UNIT_MICROS) => TimeUnit::Microsecond,
                Some(TIME_UNIT_NANOS) => TimeUnit::Nanosecond,
                Some(value) => {
                    return Err(format!(
                        "unsupported DATETIME time_unit {value}; only unset/{TIME_UNIT_MICROS}/{TIME_UNIT_NANOS} supported"
                    ));
                }
            };
            let zone = scalar
                .time_zone
                .as_ref()
                .map(|zone| Arc::from(zone.as_str()));
            Ok(DataType::Timestamp(unit, zone))
        }
        PrimitiveType::Time => Ok(DataType::Time64(TimeUnit::Microsecond)),
        PrimitiveType::Varchar | PrimitiveType::Char | PrimitiveType::Json => Ok(DataType::Utf8),
        PrimitiveType::Varbinary
        | PrimitiveType::Binary
        | PrimitiveType::Hll
        | PrimitiveType::Bitmap
        | PrimitiveType::Object
        | PrimitiveType::Percentile => Ok(DataType::Binary),
        PrimitiveType::Variant => Ok(DataType::LargeBinary),
    }
}

fn decode_decimal_type(
    primitive: common::PrimitiveType,
    scalar: &common::ScalarType,
) -> Result<DataType, String> {
    let precision = scalar
        .precision
        .ok_or_else(|| "decimal precision missing".to_string())
        .and_then(|v| u8::try_from(v).map_err(|_| format!("invalid decimal precision {v}")))?;
    let scale = scalar
        .scale
        .ok_or_else(|| "decimal scale missing".to_string())
        .and_then(|v| i8::try_from(v).map_err(|_| format!("invalid decimal scale {v}")))?;
    let (max_precision, label) = match primitive {
        common::PrimitiveType::Decimal32 => (9, "Decimal32"),
        common::PrimitiveType::Decimal64 => (18, "Decimal64"),
        common::PrimitiveType::Decimal128 => (38, "Decimal128"),
        common::PrimitiveType::Decimal256 => (76, "Decimal256"),
        _ => unreachable!(),
    };
    validate_decimal(precision, scale, max_precision, label)?;
    if primitive == common::PrimitiveType::Decimal256 || precision > 38 {
        Ok(DataType::Decimal256(precision, scale))
    } else {
        Ok(DataType::Decimal128(precision, scale))
    }
}

fn validate_decimal(
    precision: u8,
    scale: i8,
    max_precision: u8,
    label: &str,
) -> Result<(), String> {
    if precision == 0 || precision > max_precision {
        return Err(format!(
            "{label} precision {precision} must be between 1 and {max_precision}"
        ));
    }
    if scale < 0 || i32::from(scale) > i32::from(precision) {
        return Err(format!(
            "{label} scale {scale} must be between 0 and precision {precision}"
        ));
    }
    Ok(())
}

fn logical_type_from_desc(desc: &common::TypeDesc) -> Option<LogicalType> {
    let common::type_desc::Kind::Scalar(scalar) = desc.kind.as_ref()? else {
        return None;
    };
    match common::PrimitiveType::try_from(scalar.r#type).ok()? {
        common::PrimitiveType::Json => Some(LogicalType::Json),
        common::PrimitiveType::Hll => Some(LogicalType::Hll),
        common::PrimitiveType::Bitmap => Some(LogicalType::Bitmap),
        common::PrimitiveType::Object => Some(LogicalType::Object),
        common::PrimitiveType::Percentile => Some(LogicalType::Percentile),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use super::decode_type;
    use novarocks_proto_models::common;

    fn scalar(kind: common::PrimitiveType) -> common::TypeDesc {
        common::TypeDesc {
            kind: Some(common::type_desc::Kind::Scalar(common::ScalarType {
                r#type: kind as i32,
                ..Default::default()
            })),
        }
    }

    #[test]
    fn owned_nested_native_fields_preserve_wire_semantics_and_exact_map_owners() {
        let desc = common::TypeDesc {
            kind: Some(common::type_desc::Kind::Map(Box::new(common::MapType {
                key: Some(Box::new(scalar(common::PrimitiveType::Int))),
                value: Some(Box::new(common::TypeDesc {
                    kind: Some(common::type_desc::Kind::List(Box::new(common::ListType {
                        element: Some(Box::new(scalar(common::PrimitiveType::Json))),
                    }))),
                })),
            }))),
        };
        let source = super::decode_field_type_owned("payload", false, &desc).unwrap();
        assert_eq!(
            source.field().as_ref(),
            &super::decode_field_type("payload", false, &desc).unwrap()
        );
        let origins = source.metadata_origins();
        assert_eq!(origins.owners().len(), 5);
        assert_eq!(
            origins
                .for_field_tree(source.field(), 5, 2)
                .unwrap()
                .owners()
                .len(),
            5
        );
        assert!(origins.for_field_tree(source.field(), 4, 2).is_err());
        assert!(origins.for_field_tree(source.field(), 5, 1).is_err());
        for owner in origins.owners() {
            assert!(origins.metadata_bytes_for(owner.field()).is_some());
            let independent = Arc::new(owner.field().as_ref().clone());
            assert_eq!(independent.as_ref(), owner.field().as_ref());
            assert_eq!(origins.metadata_bytes_for(&independent), None);
        }
    }

    #[test]
    fn owned_native_preflight_bounds_whole_layout_before_construction() {
        let desc = scalar(common::PrimitiveType::Int);
        let name = "n".repeat(novarocks_result_contract::RootProfileV1::MAX_NAME_BYTES);
        assert!(super::decode_field_type_owned(&name, true, &desc).is_ok());
        assert!(super::decode_field_type_owned(&(name.clone() + "n"), true, &desc).is_err());
        let column = common::OutputColumn {
            column_id: 1,
            name,
            nullable: false,
            r#type: Some(desc),
            ..Default::default()
        };
        // Each individual field fits, but the combined names exceed 96 MiB.
        assert!(super::preflight_native_output_fields(&vec![column; 1536]).is_err());
        let mut nested = scalar(common::PrimitiveType::Int);
        for _ in 0..novarocks_result_contract::RootProfileV1::MAX_DEPTH {
            nested = common::TypeDesc {
                kind: Some(common::type_desc::Kind::List(Box::new(common::ListType {
                    element: Some(Box::new(nested)),
                }))),
            };
        }
        assert!(super::decode_field_type_owned("depth", true, &nested).is_ok());
        let too_deep = common::TypeDesc {
            kind: Some(common::type_desc::Kind::List(Box::new(common::ListType {
                element: Some(Box::new(nested)),
            }))),
        };
        assert!(super::decode_field_type_owned("depth", true, &too_deep).is_err());
    }

    #[test]
    fn decodes_nested_and_decimal_types_without_a_role_codec() {
        let decimal = common::TypeDesc {
            kind: Some(common::type_desc::Kind::Scalar(common::ScalarType {
                r#type: common::PrimitiveType::Decimal128 as i32,
                precision: Some(18),
                scale: Some(2),
                ..Default::default()
            })),
        };
        let desc = common::TypeDesc {
            kind: Some(common::type_desc::Kind::List(Box::new(common::ListType {
                element: Some(Box::new(decimal)),
            }))),
        };

        assert_eq!(
            decode_type(&desc).expect("decode nested decimal type"),
            DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                DataType::Decimal128(18, 2),
                true,
            )))
        );
    }
}
