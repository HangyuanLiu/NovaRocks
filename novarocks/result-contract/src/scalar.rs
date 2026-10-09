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

//! Exact ScalarValueV1 semantic facts. No Arrow carrier or client presentation
//! can supply a missing logical identity. Schema constructors consume owners
//! already protected by their source; these checks do not authorize growth.

use crate::{RootContractError, RootProfileV1};

/// Existing admitted scalar/session capacities, independent of transport and
/// of one another. These are ceilings, never runtime funding authorities.
pub struct ScalarProfileV1;
impl ScalarProfileV1 {
    /// The complete typed record, including its header and all nested metadata.
    pub const RECORD_BYTES: usize = 64 * 1024;
    pub const RECORD_PAYLOAD_BYTES: usize = Self::RECORD_BYTES - crate::SCALAR_LEAF_HEADER_BYTES;
    /// A staged SQL/session value has its own unchanged source ceiling.
    pub const SINGLE_VALUE_BYTES: usize = 64 * 1024;
    pub const CHILD_BYTES: usize = 128 * 1024;
    pub const ASSIGNMENT_SCRATCH_BYTES: usize = 128 * 1024;
    pub const SESSION_LIVE_BYTES: usize = 128 * 1024;
    pub const SESSION_STAGED_BYTES: usize = 128 * 1024;
    pub const VARIABLES: usize = 64;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScalarTimestampUnit {
    Microsecond,
    Nanosecond,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScalarOpaqueType {
    Hll,
    Bitmap,
    Object,
    Percentile,
}

/// The closed exact Native scalar vocabulary, rather than a formatted literal.
/// Decimal coefficients, temporal units and opaque identities survive intact.
/// Dictionary/view choices belong to the runtime carrier, not this vocabulary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScalarValueType {
    Null,
    Boolean,
    SignedInteger(u16),
    LargeInt,
    Float32,
    Float64,
    Decimal {
        bits: u16,
        precision: u8,
        scale: u8,
    },
    String,
    Binary,
    Date,
    TimeMicros,
    Timestamp {
        unit: ScalarTimestampUnit,
        timezone: Option<String>,
    },
    Json,
    Variant,
    Opaque(ScalarOpaqueType),
    List(Box<ScalarField>),
    Map {
        key: Box<ScalarField>,
        value: Box<ScalarField>,
    },
    Struct(Vec<NamedScalarField>),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScalarField {
    pub nullable: bool,
    pub value_type: ScalarValueType,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamedScalarField {
    pub name: String,
    pub field: ScalarField,
}

/// One exact root occurrence. Local producers have no Native slot; binding a
/// Native program requires its sole layout slot and never guesses a default.
#[derive(Clone, Debug)]
pub struct ScalarSchema {
    source_slot: Option<u32>,
    field: ScalarField,
    backing_bytes: usize,
    wire_capacity_bytes: usize,
    type_nodes: usize,
}
impl PartialEq for ScalarSchema {
    fn eq(&self, other: &Self) -> bool {
        self.source_slot == other.source_slot && self.field == other.field
    }
}
impl Eq for ScalarSchema {}
impl ScalarSchema {
    pub fn try_new(field: ScalarField) -> Result<Self, RootContractError> {
        let mut bounds = Bounds {
            backing: size_of::<Self>(),
            wire: 32,
            nodes: 0,
        };
        bounds.field(&field, 1)?;
        bounds.check()?;
        Ok(Self {
            source_slot: None,
            field,
            backing_bytes: bounds.backing,
            wire_capacity_bytes: bounds.wire,
            type_nodes: bounds.nodes,
        })
    }
    pub fn field(&self) -> &ScalarField {
        &self.field
    }
    pub fn source_slot(&self) -> Option<u32> {
        self.source_slot
    }
    pub const fn source_ordinal(&self) -> u32 {
        0
    }
    pub fn bind_native_slots(mut self, slots: &[u32]) -> Result<Self, RootContractError> {
        let [slot] = slots else {
            return Err(RootContractError::InvalidSchema);
        };
        if self.source_slot.is_some_and(|current| current != *slot) {
            return Err(RootContractError::InvalidSchema);
        }
        self.source_slot = Some(*slot);
        Ok(self)
    }
    pub fn validate_native_slots(&self, slots: &[u32]) -> Result<(), RootContractError> {
        match slots {
            [slot] if self.source_slot == Some(*slot) => Ok(()),
            _ => Err(RootContractError::InvalidSchema),
        }
    }
    /// Checked capacity covering owned schema backings and spare capacity.
    /// Clones retain the conservative original bound; separate Arc/wire/collector
    /// owners are excluded. Neither this bound nor Clone proves reclamation.
    pub fn backing_bytes(&self) -> usize {
        self.backing_bytes
    }
    pub fn type_nodes(&self) -> usize {
        self.type_nodes
    }
    pub fn wire_capacity_bytes(&self) -> usize {
        self.wire_capacity_bytes
    }
}
struct Bounds {
    backing: usize,
    wire: usize,
    nodes: usize,
}
impl Bounds {
    fn check(&self) -> Result<(), RootContractError> {
        if self.backing > RootProfileV1::SCHEMA_BACKING_BYTES
            || self.wire > RootProfileV1::SCHEMA_WIRE_BYTES
            || self.nodes > RootProfileV1::SCHEMA_TYPE_NODES
        {
            return Err(RootContractError::SchemaLimit);
        }
        Ok(())
    }
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
    fn name(&mut self, name: &String) -> Result<(), RootContractError> {
        if name.len() > RootProfileV1::MAX_NAME_BYTES {
            return Err(RootContractError::SchemaLimit);
        }
        self.add(
            name.capacity(),
            name.len()
                .checked_add(8)
                .ok_or(RootContractError::SchemaLimit)?,
        )
    }
    fn field(&mut self, field: &ScalarField, depth: usize) -> Result<(), RootContractError> {
        if depth > RootProfileV1::MAX_DEPTH {
            return Err(RootContractError::SchemaLimit);
        }
        self.nodes = self
            .nodes
            .checked_add(1)
            .ok_or(RootContractError::SchemaLimit)?;
        self.add(0, 32)?;
        match &field.value_type {
            ScalarValueType::SignedInteger(bits) if !matches!(bits, 8 | 16 | 32 | 64) => {
                Err(RootContractError::InvalidSchema)
            }
            ScalarValueType::Decimal {
                bits,
                precision,
                scale,
            } => {
                let max = match bits {
                    128 => 38,
                    256 => 76,
                    _ => return Err(RootContractError::InvalidSchema),
                };
                if *precision == 0 || *precision > max || *scale > *precision {
                    return Err(RootContractError::InvalidSchema);
                }
                Ok(())
            }
            ScalarValueType::Timestamp {
                timezone: Some(zone),
                ..
            } => {
                if zone.is_empty() {
                    return Err(RootContractError::InvalidSchema);
                }
                self.name(zone)
            }
            ScalarValueType::List(child) => {
                self.add(size_of::<ScalarField>(), 0)?;
                self.field(child, depth + 1)
            }
            ScalarValueType::Map { key, value } => {
                self.add(2 * size_of::<ScalarField>(), 0)?;
                self.field(key, depth + 1)?;
                self.field(value, depth + 1)
            }
            ScalarValueType::Struct(fields) => {
                if fields.len() > RootProfileV1::MAX_COLUMNS {
                    return Err(RootContractError::SchemaLimit);
                }
                self.add(
                    fields
                        .capacity()
                        .checked_mul(size_of::<NamedScalarField>())
                        .ok_or(RootContractError::SchemaLimit)?,
                    8,
                )?;
                for field in fields {
                    self.name(&field.name)?;
                    self.field(&field.field, depth + 1)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}
