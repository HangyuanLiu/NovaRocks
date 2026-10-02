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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Explicit conversion between engine Arrow carriers and complete logical facts.

use crate::logical::NR_LOGICAL_TYPE_KEY;
use arrow_schema::{DataType, Field};
pub use novarocks_type_contract::{LogicalField, LogicalType, LogicalTypeLimits, LogicalValue};
use std::sync::Arc;

/// Engine interpretation is explicit: fixed(16) carries LargeInt and
/// LargeBinary carries Variant. Provider schemas must use their own mapping.
pub fn logical_type_from_engine_arrow(data_type: &DataType) -> Result<LogicalType, String> {
    preflight(data_type, "", None, LogicalTypeLimits::default())?;
    let mut carrier = data_type;
    while let DataType::Dictionary(_, value) = carrier {
        carrier = value;
    }
    let ty = convert_type(carrier)?;
    ty.validate(LogicalTypeLimits::default())?;
    Ok(ty)
}

pub fn logical_value_from_engine_arrow(field: &Field) -> Result<LogicalValue, String> {
    preflight(
        field.data_type(),
        field.name(),
        field
            .metadata()
            .get(NR_LOGICAL_TYPE_KEY)
            .map(String::as_str),
        LogicalTypeLimits::default(),
    )?;
    let value = convert_field(field)?;
    value.data_type.validate(LogicalTypeLimits::default())?;
    Ok(value)
}

pub fn logical_field_from_engine_arrow(field: &Field) -> Result<LogicalField, String> {
    let value = logical_value_from_engine_arrow(field)?;
    Ok(LogicalField {
        name: field.name().clone(),
        data_type: value.data_type,
        nullable: value.nullable,
    })
}

// Bound the borrowed Arrow tree before allocating a recursive logical tree.
fn preflight(
    root: &DataType,
    name: &str,
    marker: Option<&str>,
    limits: LogicalTypeLimits,
) -> Result<(), String> {
    if name.len() > limits.max_text_bytes {
        return Err("Arrow field name exceeds its text budget".into());
    }
    let mut pending = vec![(root, "", marker, 1usize)];
    let mut nodes = 0usize;
    let mut bytes = 0usize;
    while let Some((data_type, name, marker, depth)) = pending.pop() {
        if nodes >= limits.max_nodes || depth > limits.max_depth {
            return Err("Arrow logical type exceeds its node or depth budget".into());
        }
        nodes += 1;
        bytes = bytes
            .checked_add(name.len())
            .ok_or("Arrow type text budget overflow")?;
        if let Some(marker) = marker {
            if marker.len() > 32 {
                return Err("Arrow logical marker exceeds its byte budget".into());
            }
        }
        let mut ty = data_type;
        let mut dictionary_depth = depth;
        while let DataType::Dictionary(key, value) = ty {
            if !matches!(
                key.as_ref(),
                DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
            ) {
                return Err("Arrow dictionary has a non-integer key carrier".into());
            }
            dictionary_depth += 1;
            if dictionary_depth > limits.max_depth || nodes >= limits.max_nodes {
                return Err("Arrow dictionary exceeds its node or depth budget".into());
            }
            nodes += 1;
            ty = value;
        }
        match ty {
            DataType::List(child)
            | DataType::LargeList(child)
            | DataType::FixedSizeList(child, _) => {
                pending.push((
                    child.data_type(),
                    "",
                    child
                        .metadata()
                        .get(NR_LOGICAL_TYPE_KEY)
                        .map(String::as_str),
                    dictionary_depth + 1,
                ));
            }
            DataType::Map(entries, _) => {
                if entries.is_nullable() || entries.metadata().contains_key(NR_LOGICAL_TYPE_KEY) {
                    return Err(
                        "Arrow map entries have invalid nullability or a semantic marker".into(),
                    );
                }
                let DataType::Struct(fields) = entries.data_type() else {
                    return Err("Arrow map entries are not a struct".into());
                };
                if fields.len() != 2 {
                    return Err("Arrow map has invalid entries".into());
                }
                for child in fields {
                    pending.push((
                        child.data_type(),
                        "",
                        child
                            .metadata()
                            .get(NR_LOGICAL_TYPE_KEY)
                            .map(String::as_str),
                        dictionary_depth + 1,
                    ));
                }
            }
            DataType::Struct(fields) => {
                if fields.len() > limits.max_nodes.saturating_sub(nodes) {
                    return Err("Arrow struct exceeds its node budget".into());
                }
                pending.extend(fields.iter().map(|f| {
                    (
                        f.data_type(),
                        f.name().as_str(),
                        f.metadata().get(NR_LOGICAL_TYPE_KEY).map(String::as_str),
                        dictionary_depth + 1,
                    )
                }));
            }
            DataType::Timestamp(_, Some(zone)) => {
                bytes = bytes
                    .checked_add(zone.len())
                    .ok_or("Arrow type text budget overflow")?;
            }
            _ => {}
        }
        if bytes > limits.max_text_bytes || pending.len() > limits.max_nodes.saturating_sub(nodes) {
            return Err("Arrow logical type exceeds its node or text budget".into());
        }
    }
    Ok(())
}

fn convert_field(field: &Field) -> Result<LogicalValue, String> {
    let mut carrier = field.data_type();
    while let DataType::Dictionary(_, value) = carrier {
        carrier = value;
    }
    let marker = field.metadata().get(NR_LOGICAL_TYPE_KEY);
    let data_type = if let Some(marker) = marker {
        let marker = marker.trim().to_ascii_lowercase();
        let (ty, valid) = match marker.as_str() {
            "json" => (
                LogicalType::Json,
                matches!(
                    carrier,
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                ),
            ),
            "bitmap" => (
                LogicalType::Bitmap,
                matches!(
                    carrier,
                    DataType::Binary | DataType::LargeBinary | DataType::BinaryView
                ),
            ),
            "hll" => (
                LogicalType::Hll,
                matches!(
                    carrier,
                    DataType::Binary | DataType::LargeBinary | DataType::BinaryView
                ),
            ),
            "object" => (
                LogicalType::Object,
                matches!(
                    carrier,
                    DataType::Binary | DataType::LargeBinary | DataType::BinaryView
                ),
            ),
            "percentile" => (
                LogicalType::Percentile,
                matches!(
                    carrier,
                    DataType::Binary | DataType::LargeBinary | DataType::BinaryView
                ),
            ),
            "variant" => (
                LogicalType::Variant,
                matches!(carrier, DataType::LargeBinary),
            ),
            "largeint" => (
                LogicalType::LargeInt,
                matches!(carrier, DataType::FixedSizeBinary(16)),
            ),
            "uuid" => (
                LogicalType::Uuid,
                matches!(carrier, DataType::FixedSizeBinary(16)),
            ),
            "binary" => (
                LogicalType::Binary,
                matches!(
                    carrier,
                    DataType::Binary | DataType::LargeBinary | DataType::BinaryView
                ),
            ),
            "fixed_size_binary" => match carrier {
                DataType::FixedSizeBinary(size) if *size >= 0 => {
                    (LogicalType::FixedSizeBinary(*size as u32), true)
                }
                _ => return Err("fixed binary marker has an incompatible Arrow carrier".into()),
            },
            _ => return Err(format!("unsupported Arrow logical type marker: {marker}")),
        };
        if !valid {
            return Err(format!(
                "logical marker {marker} has an incompatible Arrow carrier"
            ));
        }
        ty
    } else {
        convert_type(carrier)?
    };
    Ok(LogicalValue {
        data_type,
        nullable: field.is_nullable(),
    })
}

fn convert_type(ty: &DataType) -> Result<LogicalType, String> {
    use LogicalType as L;
    Ok(match ty {
        DataType::Null => L::Null,
        DataType::Boolean => L::Boolean,
        DataType::Int8 => L::Int8,
        DataType::Int16 => L::Int16,
        DataType::Int32 => L::Int32,
        DataType::Int64 => L::Int64,
        DataType::UInt8 => L::UInt8,
        DataType::UInt16 => L::UInt16,
        DataType::UInt32 => L::UInt32,
        DataType::UInt64 => L::UInt64,
        DataType::Float32 => L::Float32,
        DataType::Float64 => L::Float64,
        DataType::Decimal32(p, s) => L::Decimal {
            bits: 32,
            precision: *p,
            scale: *s,
        },
        DataType::Decimal64(p, s) => L::Decimal {
            bits: 64,
            precision: *p,
            scale: *s,
        },
        DataType::Decimal128(p, s) => L::Decimal {
            bits: 128,
            precision: *p,
            scale: *s,
        },
        DataType::Decimal256(p, s) => L::Decimal {
            bits: 256,
            precision: *p,
            scale: *s,
        },
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => L::Utf8,
        DataType::Binary | DataType::BinaryView => L::Binary,
        DataType::LargeBinary => L::Variant,
        DataType::FixedSizeBinary(16) => L::LargeInt,
        DataType::FixedSizeBinary(n) if *n >= 0 => L::FixedSizeBinary(*n as u32),
        DataType::Date32 => L::Date32,
        DataType::Date64 => L::Date64,
        DataType::Time32(unit) => L::Time {
            bits: 32,
            unit: *unit,
        },
        DataType::Time64(unit) => L::Time {
            bits: 64,
            unit: *unit,
        },
        DataType::Timestamp(unit, timezone) => L::Timestamp {
            unit: *unit,
            timezone: timezone.as_ref().map(|s| s.to_string()),
        },
        DataType::List(element) | DataType::LargeList(element) => L::Array {
            element: Box::new(convert_field(element)?),
            fixed_length: None,
        },
        DataType::FixedSizeList(element, n) if *n >= 0 => L::Array {
            element: Box::new(convert_field(element)?),
            fixed_length: Some(*n as u32),
        },
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err("Arrow map entries are not a struct".into());
            };
            if entries.is_nullable() || fields.len() != 2 {
                return Err("Arrow map has invalid entries".into());
            }
            L::Map {
                key: Box::new(convert_field(&fields[0])?),
                value: Box::new(convert_field(&fields[1])?),
            }
        }
        DataType::Struct(fields) => L::Struct(
            fields
                .iter()
                .map(|f| {
                    let value = convert_field(f)?;
                    Ok(LogicalField {
                        name: f.name().clone(),
                        data_type: value.data_type,
                        nullable: value.nullable,
                    })
                })
                .collect::<Result<_, String>>()?,
        ),
        _ => return Err("unsupported engine Arrow logical carrier".into()),
    })
}

/// Materialize a canonical carrier, retaining markers where Arrow alone is ambiguous.
pub fn engine_arrow_field_from_logical(name: &str, value: &LogicalValue) -> Result<Field, String> {
    value.data_type.validate(LogicalTypeLimits::default())?;
    if name.len() > LogicalTypeLimits::default().max_text_bytes {
        return Err("Arrow field name exceeds its text budget".into());
    }
    build_field(name, value)
}

/// Return a type-only carrier only when no root semantic marker is required.
/// Use the Field API for marked leaves; dropping their metadata loses identity.
pub fn engine_arrow_type_from_logical(ty: &LogicalType) -> Result<DataType, String> {
    ty.validate(LogicalTypeLimits::default())?;
    if matches!(
        ty,
        LogicalType::Uuid
            | LogicalType::FixedSizeBinary(16)
            | LogicalType::Json
            | LogicalType::Bitmap
            | LogicalType::Hll
            | LogicalType::Object
            | LogicalType::Percentile
    ) {
        return Err("logical type requires a root Arrow field marker".into());
    }
    engine_arrow_field_from_logical(
        "",
        &LogicalValue {
            data_type: ty.clone(),
            nullable: false,
        },
    )
    .map(|field| field.data_type().clone())
}

fn build_field(name: &str, value: &LogicalValue) -> Result<Field, String> {
    use LogicalType as L;
    let marker = match &value.data_type {
        L::Json => Some("json"),
        L::Bitmap => Some("bitmap"),
        L::Hll => Some("hll"),
        L::Object => Some("object"),
        L::Percentile => Some("percentile"),
        L::Uuid => Some("uuid"),
        L::FixedSizeBinary(16) => Some("fixed_size_binary"),
        _ => None,
    };
    let data_type = match &value.data_type {
        L::Null => DataType::Null,
        L::Boolean => DataType::Boolean,
        L::Int8 => DataType::Int8,
        L::Int16 => DataType::Int16,
        L::Int32 => DataType::Int32,
        L::Int64 => DataType::Int64,
        L::UInt8 => DataType::UInt8,
        L::UInt16 => DataType::UInt16,
        L::UInt32 => DataType::UInt32,
        L::UInt64 => DataType::UInt64,
        L::LargeInt | L::Uuid => DataType::FixedSizeBinary(16),
        L::Float32 => DataType::Float32,
        L::Float64 => DataType::Float64,
        L::Decimal {
            bits,
            precision,
            scale,
        } => match bits {
            32 => DataType::Decimal32(*precision, *scale),
            64 => DataType::Decimal64(*precision, *scale),
            128 => DataType::Decimal128(*precision, *scale),
            256 => DataType::Decimal256(*precision, *scale),
            _ => unreachable!("validated decimal width"),
        },
        L::Utf8 | L::Json => DataType::Utf8,
        L::Binary | L::Bitmap | L::Hll | L::Object | L::Percentile => DataType::Binary,
        L::Variant => DataType::LargeBinary,
        L::FixedSizeBinary(n) => DataType::FixedSizeBinary(*n as i32),
        L::Date32 => DataType::Date32,
        L::Date64 => DataType::Date64,
        L::Time { bits: 32, unit } => DataType::Time32(*unit),
        L::Time { unit, .. } => DataType::Time64(*unit),
        L::Timestamp { unit, timezone } => {
            DataType::Timestamp(*unit, timezone.as_ref().map(|s| Arc::from(s.as_str())))
        }
        L::Array {
            element,
            fixed_length,
        } => {
            let child = Arc::new(build_field("item", element)?);
            match fixed_length {
                Some(n) => DataType::FixedSizeList(child, *n as i32),
                None => DataType::List(child),
            }
        }
        L::Map { key, value } => DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(build_field("key", key)?),
                        Arc::new(build_field("value", value)?),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        ),
        L::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|f| {
                    build_field(
                        &f.name,
                        &LogicalValue {
                            data_type: f.data_type.clone(),
                            nullable: f.nullable,
                        },
                    )
                    .map(Arc::new)
                })
                .collect::<Result<Vec<_>, _>>()?
                .into(),
        ),
    };
    let mut field = Field::new(name, data_type, value.nullable);
    if let Some(marker) = marker {
        field = field.with_metadata([(NR_LOGICAL_TYPE_KEY.into(), marker.into())].into());
    }
    Ok(field)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::TimeUnit;
    #[test]
    fn nested_nullability_and_marked_leaves_round_trip() {
        let ty = LogicalType::Struct(vec![LogicalField {
            name: "payload".into(),
            nullable: false,
            data_type: LogicalType::Map {
                key: Box::new(LogicalValue {
                    data_type: LogicalType::Utf8,
                    nullable: true,
                }),
                value: Box::new(LogicalValue {
                    nullable: false,
                    data_type: LogicalType::Array {
                        fixed_length: Some(3),
                        element: Box::new(LogicalValue {
                            data_type: LogicalType::Uuid,
                            nullable: true,
                        }),
                    },
                }),
            },
        }]);
        let value = LogicalValue {
            data_type: ty,
            nullable: true,
        };
        let field = engine_arrow_field_from_logical("out", &value).unwrap();
        assert_eq!(logical_value_from_engine_arrow(&field).unwrap(), value);
        for ty in [
            LogicalType::LargeInt,
            LogicalType::FixedSizeBinary(16),
            LogicalType::Json,
            LogicalType::Bitmap,
            LogicalType::Hll,
            LogicalType::Object,
            LogicalType::Percentile,
            LogicalType::Variant,
            LogicalType::Timestamp {
                unit: TimeUnit::Nanosecond,
                timezone: Some("UTC".into()),
            },
            LogicalType::Decimal {
                bits: 128,
                precision: 38,
                scale: 9,
            },
        ] {
            let value = LogicalValue {
                data_type: ty,
                nullable: false,
            };
            assert_eq!(
                logical_value_from_engine_arrow(
                    &engine_arrow_field_from_logical("x", &value).unwrap()
                )
                .unwrap(),
                value
            );
        }
    }
    #[test]
    fn dictionary_offsets_and_container_names_are_carrier_facts() {
        let first = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        let second = DataType::LargeList(Arc::new(Field::new(
            "element",
            DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::LargeUtf8)),
            true,
        )));
        assert_eq!(
            logical_type_from_engine_arrow(&first).unwrap(),
            logical_type_from_engine_arrow(&second).unwrap()
        );
        let fixed = DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Utf8, true)), 2);
        assert_ne!(
            logical_type_from_engine_arrow(&first).unwrap(),
            logical_type_from_engine_arrow(&fixed).unwrap()
        );
        assert_ne!(
            logical_type_from_engine_arrow(&DataType::Binary).unwrap(),
            logical_type_from_engine_arrow(&DataType::LargeBinary).unwrap()
        );
    }
    #[test]
    fn canonical_map_wrappers_do_not_consume_semantic_budgets() {
        let mut ty = LogicalType::Int64;
        for _ in 0..63 {
            ty = LogicalType::Map {
                key: Box::new(LogicalValue {
                    data_type: LogicalType::Int64,
                    nullable: false,
                }),
                value: Box::new(LogicalValue {
                    data_type: ty,
                    nullable: true,
                }),
            };
        }
        let value = LogicalValue {
            data_type: ty,
            nullable: true,
        };
        let field = engine_arrow_field_from_logical("root", &value).unwrap();
        assert_eq!(logical_value_from_engine_arrow(&field).unwrap(), value);
        let value = LogicalValue {
            data_type: LogicalType::Struct(vec![LogicalField {
                name: "x".repeat(64 * 1024),
                data_type: LogicalType::Int64,
                nullable: true,
            }]),
            nullable: true,
        };
        assert_eq!(
            logical_value_from_engine_arrow(
                &engine_arrow_field_from_logical("root", &value).unwrap()
            )
            .unwrap(),
            value
        );
        assert!(engine_arrow_field_from_logical(&"x".repeat(64 * 1024 + 1), &value).is_err());
    }

    #[test]
    fn map_wrapper_semantic_markers_are_rejected() {
        let entries = Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Int64, false),
                    Field::new("value", DataType::Int64, true),
                ]
                .into(),
            ),
            false,
        )
        .with_metadata([(NR_LOGICAL_TYPE_KEY.into(), "unknown".into())].into());
        assert!(logical_type_from_engine_arrow(&DataType::Map(Arc::new(entries), false)).is_err());
    }

    #[test]
    fn type_only_conversion_refuses_loss_of_root_markers() {
        for ty in [
            LogicalType::Uuid,
            LogicalType::FixedSizeBinary(16),
            LogicalType::Json,
            LogicalType::Bitmap,
            LogicalType::Hll,
            LogicalType::Object,
            LogicalType::Percentile,
        ] {
            assert!(engine_arrow_type_from_logical(&ty).is_err());
        }
        for ty in [
            LogicalType::Int64,
            LogicalType::Binary,
            LogicalType::LargeInt,
            LogicalType::Variant,
        ] {
            assert_eq!(
                logical_type_from_engine_arrow(&engine_arrow_type_from_logical(&ty).unwrap())
                    .unwrap(),
                ty
            );
        }
    }

    #[test]
    fn invalid_carriers_markers_and_deep_trees_fail_closed() {
        let field = Field::new("x", DataType::Int32, true)
            .with_metadata([(NR_LOGICAL_TYPE_KEY.into(), "json".into())].into());
        assert!(logical_value_from_engine_arrow(&field).is_err());
        let field = Field::new("x", DataType::Binary, true)
            .with_metadata([(NR_LOGICAL_TYPE_KEY.into(), "unknown".into())].into());
        assert!(logical_value_from_engine_arrow(&field).is_err());
        assert!(
            logical_type_from_engine_arrow(&DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Int32, true)),
                -1
            ))
            .is_err()
        );
        assert!(
            logical_type_from_engine_arrow(&DataType::Dictionary(
                Box::new(DataType::Utf8),
                Box::new(DataType::Int64)
            ))
            .is_err()
        );
        let mut ty = DataType::Int64;
        for _ in 0..65 {
            ty = DataType::List(Arc::new(Field::new("item", ty, true)));
        }
        assert!(logical_type_from_engine_arrow(&ty).is_err());
    }
}
