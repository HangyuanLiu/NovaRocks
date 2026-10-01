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

use crate::{RootContractError, RootProfileV1};
use std::mem::size_of;

/// Semantic types, independent of Arrow carrier choices. In particular a
/// dictionary or a view is never a frozen result type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NativeRenderType {
    Null,
    Boolean,
    SignedInteger(u16),
    UnsignedInteger(u16),
    LargeInt,
    Float32,
    Float64,
    Decimal {
        bits: u16,
        precision: u8,
        scale: i8,
    },
    String,
    Binary,
    Date,
    Time {
        unit: RenderTimeUnit,
    },
    Timestamp {
        unit: RenderTimeUnit,
        timezone: Option<String>,
    },
    Json,
    Variant,
    Opaque(OpaqueRenderType),
    List(Box<RenderField>),
    Map {
        key: Box<RenderField>,
        value: Box<RenderField>,
    },
    Struct(Vec<NamedRenderField>),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RenderTimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpaqueRenderType {
    Hll,
    Bitmap,
    Object,
    Percentile,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RenderPresentation {
    ScalarText,
    TimestampUtcMicros,
    /// Existing MySQL container timestamp presentation preserves the frozen
    /// native unit and timezone text, unlike top-level UTC microsecond cells.
    TimestampContainerText,
    JsonText,
    VariantSerializedBytes,
    /// The frontend freezes its presentation offset once. Pure encoders must
    /// never read a backend's local timezone or clock.
    VariantJson {
        timezone_offset_seconds: i32,
    },
    OpaqueNull,
    MysqlContainer,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RenderField {
    pub presentation: RenderPresentation,
    pub nullable: bool,
    pub native_type: NativeRenderType,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamedRenderField {
    pub name: String,
    pub field: RenderField,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RenderColumn {
    /// Exact source occurrence in the root input layout, never a deduplicated
    /// expression ID or a guessed slot assignment.
    pub source_ordinal: u32,
    /// Present for a native task and checked against its exact input layout;
    /// local producers have no native slot assignment.
    pub source_slot: Option<u32>,
    pub name: String,
    pub field: RenderField,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientRenderSchema {
    columns: Vec<RenderColumn>,
}
impl ClientRenderSchema {
    /// Accept existing protected owners; source producers must check their
    /// construction bounds before allocating these values.
    pub fn try_new(
        columns: Vec<RenderColumn>,
        input_columns: usize,
    ) -> Result<Self, RootContractError> {
        if columns.is_empty()
            || columns.len() > RootProfileV1::MAX_COLUMNS
            || input_columns > RootProfileV1::MAX_COLUMNS
        {
            return Err(RootContractError::InvalidSchema);
        }
        let mut bounds = SchemaBounds {
            backing: size_of::<Self>()
                .checked_add(
                    columns
                        .capacity()
                        .checked_mul(size_of::<RenderColumn>())
                        .ok_or(RootContractError::SchemaLimit)?,
                )
                .ok_or(RootContractError::SchemaLimit)?,
            wire: 0,
        };
        for column in &columns {
            if column.source_ordinal as usize >= input_columns {
                return Err(RootContractError::InvalidSchema);
            }
            bounds.name(&column.name)?;
            bounds.field(&column.field, 1)?;
        }
        bounds.check()?;
        Ok(Self { columns })
    }
    /// Bind a planner/local schema to the exact native occurrence layout.
    /// Existing bindings must agree; they are never silently rebound.
    pub fn bind_native_slots(mut self, slots: &[u32]) -> Result<Self, RootContractError> {
        if slots.len() > RootProfileV1::MAX_COLUMNS {
            return Err(RootContractError::InvalidSchema);
        }
        for column in &mut self.columns {
            let slot = *slots
                .get(column.source_ordinal as usize)
                .ok_or(RootContractError::InvalidSchema)?;
            if column.source_slot.is_some_and(|current| current != slot) {
                return Err(RootContractError::InvalidSchema);
            }
            column.source_slot = Some(slot);
        }
        Ok(self)
    }
    pub fn columns(&self) -> &[RenderColumn] {
        &self.columns
    }
    /// Actual neutral schema backings, including spare capacity. Validation
    /// makes this traversal infallible; the value is never a reclamation claim.
    pub fn backing_bytes(&self) -> usize {
        let mut bounds = SchemaBounds {
            backing: size_of::<Self>() + self.columns.capacity() * size_of::<RenderColumn>(),
            wire: 0,
        };
        for column in &self.columns {
            bounds
                .name(&column.name)
                .expect("validated immutable schema name");
            bounds
                .field(&column.field, 1)
                .expect("validated immutable schema field");
        }
        bounds.backing
    }
    /// Native input layout is sealed separately from the render schema.
    /// Validate the exact ordinal-to-slot mapping before binding runtime input.
    pub fn validate_native_slots(&self, slots: &[u32]) -> Result<(), RootContractError> {
        if slots.len() > RootProfileV1::MAX_COLUMNS {
            return Err(RootContractError::InvalidSchema);
        }
        for column in &self.columns {
            let slot = slots
                .get(column.source_ordinal as usize)
                .ok_or(RootContractError::InvalidSchema)?;
            if column.source_slot != Some(*slot) {
                return Err(RootContractError::InvalidSchema);
            }
        }
        Ok(())
    }
}
struct SchemaBounds {
    backing: usize,
    wire: usize,
}
impl SchemaBounds {
    fn add(&mut self, backing: usize, wire: usize) -> Result<(), RootContractError> {
        self.backing = self
            .backing
            .checked_add(backing)
            .ok_or(RootContractError::SchemaLimit)?;
        self.wire = self
            .wire
            .checked_add(wire)
            .ok_or(RootContractError::SchemaLimit)?;
        self.check()
    }
    fn check(&self) -> Result<(), RootContractError> {
        if self.backing > RootProfileV1::SCHEMA_BACKING_BYTES
            || self.wire > RootProfileV1::SCHEMA_WIRE_BYTES
        {
            Err(RootContractError::SchemaLimit)
        } else {
            Ok(())
        }
    }
    fn name(&mut self, name: &String) -> Result<(), RootContractError> {
        if name.len() > RootProfileV1::MAX_NAME_BYTES {
            return Err(RootContractError::SchemaLimit);
        }
        self.add(
            name.capacity(),
            name.len()
                .checked_add(16)
                .ok_or(RootContractError::SchemaLimit)?,
        )
    }
    fn field(&mut self, field: &RenderField, depth: usize) -> Result<(), RootContractError> {
        if depth > RootProfileV1::MAX_DEPTH {
            return Err(RootContractError::SchemaLimit);
        }
        self.add(0, 32)?;
        match (&field.native_type, &field.presentation) {
            (
                NativeRenderType::Timestamp { .. },
                RenderPresentation::TimestampUtcMicros | RenderPresentation::TimestampContainerText,
            )
            | (NativeRenderType::Json, RenderPresentation::JsonText)
            | (
                NativeRenderType::Variant,
                RenderPresentation::VariantSerializedBytes | RenderPresentation::VariantJson { .. },
            )
            | (NativeRenderType::Opaque(_), RenderPresentation::OpaqueNull)
            | (
                NativeRenderType::List(_)
                | NativeRenderType::Map { .. }
                | NativeRenderType::Struct(_),
                RenderPresentation::MysqlContainer,
            ) => {}
            (
                NativeRenderType::Null
                | NativeRenderType::Boolean
                | NativeRenderType::SignedInteger(_)
                | NativeRenderType::UnsignedInteger(_)
                | NativeRenderType::LargeInt
                | NativeRenderType::Float32
                | NativeRenderType::Float64
                | NativeRenderType::Decimal { .. }
                | NativeRenderType::String
                | NativeRenderType::Binary
                | NativeRenderType::Date
                | NativeRenderType::Time { .. },
                RenderPresentation::ScalarText,
            ) => {}
            _ => return Err(RootContractError::UnsupportedRenderType),
        }
        if let RenderPresentation::VariantJson {
            timezone_offset_seconds,
        } = field.presentation
            && !(-86_399..=86_399).contains(&timezone_offset_seconds)
        {
            return Err(RootContractError::UnsupportedRenderType);
        }
        match &field.native_type {
            NativeRenderType::SignedInteger(bits) | NativeRenderType::UnsignedInteger(bits)
                if !matches!(bits, 8 | 16 | 32 | 64) =>
            {
                Err(RootContractError::UnsupportedRenderType)
            }
            NativeRenderType::Decimal {
                bits,
                precision,
                scale,
            } => {
                let max = match bits {
                    128 => 38,
                    256 => 76,
                    _ => return Err(RootContractError::UnsupportedRenderType),
                };
                if *precision == 0 || *precision > max || *scale < 0 || *scale as u8 > *precision {
                    return Err(RootContractError::UnsupportedRenderType);
                }
                Ok(())
            }
            NativeRenderType::Timestamp {
                timezone: Some(zone),
                ..
            } => {
                if zone.is_empty() {
                    return Err(RootContractError::InvalidSchema);
                }
                self.name(zone)
            }
            NativeRenderType::List(child) => {
                self.add(size_of::<RenderField>(), 0)?;
                self.field(child, depth + 1)
            }
            NativeRenderType::Map { key, value } => {
                self.add(2 * size_of::<RenderField>(), 0)?;
                self.field(key, depth + 1)?;
                self.field(value, depth + 1)
            }
            NativeRenderType::Struct(fields) => {
                if fields.is_empty() || fields.len() > RootProfileV1::MAX_COLUMNS {
                    return Err(RootContractError::InvalidSchema);
                }
                self.add(
                    fields
                        .capacity()
                        .checked_mul(size_of::<NamedRenderField>())
                        .ok_or(RootContractError::SchemaLimit)?,
                    0,
                )?;
                for child in fields {
                    self.name(&child.name)?;
                    self.field(&child.field, depth + 1)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn column(source_ordinal: u32, native_type: NativeRenderType) -> RenderColumn {
        RenderColumn {
            source_ordinal,
            source_slot: None,
            name: "same".into(),
            field: RenderField {
                presentation: RenderPresentation::ScalarText,
                nullable: true,
                native_type,
            },
        }
    }
    #[test]
    fn occurrence_order_and_duplicate_names_are_exact() {
        let columns = vec![
            column(1, NativeRenderType::String),
            column(0, NativeRenderType::SignedInteger(64)),
            column(1, NativeRenderType::String),
        ];
        assert_eq!(
            ClientRenderSchema::try_new(columns.clone(), 2)
                .unwrap()
                .columns(),
            columns
        );
        assert_eq!(
            ClientRenderSchema::try_new(vec![column(2, NativeRenderType::String)], 2),
            Err(RootContractError::InvalidSchema)
        );
    }
    #[test]
    fn native_binding_requires_exact_slot_at_each_occurrence() {
        let mut first = column(0, NativeRenderType::String);
        first.source_slot = Some(10);
        let mut second = column(1, NativeRenderType::String);
        second.source_slot = Some(11);
        let schema = ClientRenderSchema::try_new(vec![first, second], 2).unwrap();
        assert_eq!(schema.validate_native_slots(&[10, 11]), Ok(()));
        assert_eq!(
            schema.validate_native_slots(&[11, 10]),
            Err(RootContractError::InvalidSchema)
        );
        assert_eq!(
            schema.validate_native_slots(&[10]),
            Err(RootContractError::InvalidSchema)
        );
        let local =
            ClientRenderSchema::try_new(vec![column(0, NativeRenderType::String)], 1).unwrap();
        assert_eq!(
            local.validate_native_slots(&[10]),
            Err(RootContractError::InvalidSchema)
        );
    }
    #[test]
    fn semantic_shape_bounds_and_full_string_backing_are_checked() {
        let mut col = column(0, NativeRenderType::String);
        col.name = String::with_capacity(RootProfileV1::SCHEMA_BACKING_BYTES + 1);
        col.name.push('x');
        assert_eq!(
            ClientRenderSchema::try_new(vec![col], 1),
            Err(RootContractError::SchemaLimit)
        );
        for ty in [
            NativeRenderType::SignedInteger(7),
            NativeRenderType::Decimal {
                bits: 128,
                precision: 39,
                scale: 0,
            },
            NativeRenderType::Decimal {
                bits: 128,
                precision: 10,
                scale: 11,
            },
        ] {
            assert_eq!(
                ClientRenderSchema::try_new(vec![column(0, ty)], 1),
                Err(RootContractError::UnsupportedRenderType)
            );
        }
        let mut ty = NativeRenderType::String;
        for _ in 0..RootProfileV1::MAX_DEPTH {
            ty = NativeRenderType::List(Box::new(RenderField {
                presentation: if matches!(ty, NativeRenderType::List(_)) {
                    RenderPresentation::MysqlContainer
                } else {
                    RenderPresentation::ScalarText
                },
                nullable: true,
                native_type: ty,
            }));
        }
        let mut col = column(0, ty);
        col.field.presentation = RenderPresentation::MysqlContainer;
        assert_eq!(
            ClientRenderSchema::try_new(vec![col], 1),
            Err(RootContractError::SchemaLimit)
        );
    }
}
