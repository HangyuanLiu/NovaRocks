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

//! Exact flat v2 type projection after resource-safe DTO admission. Explicit
//! caller limits bound this projection; they are not a MEM allocation grant
//! or a pre-Prost raw-byte resource model. Sparse identities remain separate
//! from table positions, and every decoded value retains its logical domain.

use arrow::datatypes::{DataType, Field};
use novarocks_proto_models::physical_type_v2 as wire;
use novarocks_type_contract::{
    CarrierParameterError, CompileCheckpoints, CompileControlError, CompilePhase,
    FunctionValueType, PureCompileControl, ValueLogicalType, ValueTypeError, ValueTypeVisit,
    validate_arrow_carrier_parameters_observed, validate_value_type_structure_observed,
};
use std::{collections::BTreeMap, fmt, sync::Arc};

mod decode;
mod encode;
mod scalars;

#[derive(Debug)]
pub enum TypeCodecError {
    InvalidShape(&'static str),
    Control(CompileControlError),
    ValueType(ValueTypeError),
    Carrier(CarrierParameterError),
}
impl fmt::Display for TypeCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidShape(message) => f.write_str(message),
            Self::Control(error) => error.fmt(f),
            Self::ValueType(error) => error.fmt(f),
            Self::Carrier(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for TypeCodecError {}
impl From<CompileControlError> for TypeCodecError {
    fn from(value: CompileControlError) -> Self {
        Self::Control(value)
    }
}
impl From<ValueTypeError> for TypeCodecError {
    fn from(value: ValueTypeError) -> Self {
        Self::ValueType(value)
    }
}
impl From<CarrierParameterError> for TypeCodecError {
    fn from(value: CarrierParameterError) -> Self {
        Self::Carrier(value)
    }
}

/// A caller-authored projection envelope. No guessed or unbounded default is
/// available. Definitions count all three namespaces; expanded nodes count
/// each definition's unfolded carrier subtree, including field/value copies and
/// repeated references. String
/// bytes include field names, metadata and timestamp zones before copying.
#[derive(Clone, Copy, Debug)]
pub struct TypeProjectionLimits {
    pub max_definitions: usize,
    pub max_expanded_nodes: usize,
    pub max_string_bytes: usize,
}

pub struct DecodedTypeTable {
    pub(super) carriers: BTreeMap<u32, DataType>,
    pub(super) fields: BTreeMap<u32, Arc<Field>>,
    pub(super) values: BTreeMap<u32, FunctionValueType>,
}
impl DecodedTypeTable {
    pub fn carrier(&self, id: u32) -> Option<&DataType> {
        self.carriers.get(&id)
    }
    pub fn field(&self, id: u32) -> Option<&Arc<Field>> {
        self.fields.get(&id)
    }
    pub fn value_type(&self, id: u32) -> Option<&FunctionValueType> {
        self.values.get(&id)
    }
    pub fn value_types(&self) -> impl ExactSizeIterator<Item = (u32, &FunctionValueType)> {
        self.values.iter().map(|(id, value)| (*id, value))
    }
}

fn finish<T>(
    work: CompileCheckpoints<'_>,
    result: Result<T, TypeCodecError>,
) -> Result<T, TypeCodecError> {
    if matches!(&result, Err(TypeCodecError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

pub fn encode_type_table(
    values: &[(u32, FunctionValueType)],
    limits: TypeProjectionLimits,
    control: &dyn PureCompileControl,
) -> Result<wire::TypeTable, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = encode::encode(values, limits, &mut work);
    finish(work, result)
}

/// Project explicit complete root fields as well as value types. Authored
/// field IDs are retained in their own namespace; nested occurrence fields
/// receive fresh IDs that never collide with those reserved identities.
/// This does not infer a root field from a value type or add logical metadata.
pub fn encode_type_table_with_fields(
    values: &[(u32, FunctionValueType)],
    fields: &[(u32, Arc<Field>)],
    limits: TypeProjectionLimits,
    control: &dyn PureCompileControl,
) -> Result<wire::TypeTable, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = encode::encode_with_fields(values, fields, limits, &mut work);
    finish(work, result)
}

pub fn decode_type_table(
    table: &wire::TypeTable,
    limits: TypeProjectionLimits,
    control: &dyn PureCompileControl,
) -> Result<DecodedTypeTable, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = decode::decode(table, limits, &mut work);
    finish(work, result)
}

fn encode_logical(logical: ValueLogicalType) -> i32 {
    use wire::LogicalType as W;
    (match logical {
        ValueLogicalType::Physical => W::Physical,
        ValueLogicalType::Json => W::Json,
        ValueLogicalType::Variant => W::Variant,
        ValueLogicalType::Hll => W::Hll,
        ValueLogicalType::Bitmap => W::Bitmap,
        ValueLogicalType::Object => W::Object,
        ValueLogicalType::Percentile => W::Percentile,
        ValueLogicalType::LargeInt => W::LargeInt,
        ValueLogicalType::Uuid => W::Uuid,
    }) as i32
}

fn decode_logical(logical: i32) -> Result<ValueLogicalType, TypeCodecError> {
    use wire::LogicalType as W;
    Ok(match W::try_from(logical) {
        Ok(W::Physical) => ValueLogicalType::Physical,
        Ok(W::Json) => ValueLogicalType::Json,
        Ok(W::Variant) => ValueLogicalType::Variant,
        Ok(W::Hll) => ValueLogicalType::Hll,
        Ok(W::Bitmap) => ValueLogicalType::Bitmap,
        Ok(W::Object) => ValueLogicalType::Object,
        Ok(W::Percentile) => ValueLogicalType::Percentile,
        Ok(W::LargeInt) => ValueLogicalType::LargeInt,
        Ok(W::Uuid) => ValueLogicalType::Uuid,
        _ => {
            return Err(TypeCodecError::InvalidShape(
                "unknown or unspecified logical type",
            ));
        }
    })
}

fn observe_bytes(bytes: &[u8], work: &mut CompileCheckpoints<'_>) -> Result<(), TypeCodecError> {
    for _ in bytes.chunks(1024) {
        work.step()?;
    }
    Ok(())
}

fn clone_carrier(
    ty: &DataType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DataType, TypeCodecError> {
    work.step()?;
    // Dictionary boxes are the only recursively owned carrier children.
    // Other nested carriers share Arrow FieldRef/Fields backing on clone.
    Ok(match ty {
        DataType::Dictionary(key, value) => DataType::Dictionary(
            Box::new(clone_carrier(key, work)?),
            Box::new(clone_carrier(value, work)?),
        ),
        ty => ty.clone(),
    })
}

pub(crate) fn validate_field(
    field: &Field,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    use novarocks_type_contract::{
        MAX_ARROW_FIELD_METADATA_BYTES, MAX_ARROW_FIELD_METADATA_ENTRIES,
        MAX_ARROW_FIELD_METADATA_KEY_BYTES, MAX_ARROW_FIELD_METADATA_VALUE_BYTES,
        MAX_ARROW_FIELD_NAME_BYTES,
    };
    work.step()?;
    if field.name().len() > MAX_ARROW_FIELD_NAME_BYTES
        || field.metadata().len() > MAX_ARROW_FIELD_METADATA_ENTRIES
    {
        return Err(TypeCodecError::InvalidShape(
            "Arrow field attributes exceed their owner bounds",
        ));
    }
    observe_bytes(field.name().as_bytes(), work)?;
    let mut bytes = 0usize;
    for (key, value) in field.metadata() {
        work.step()?;
        if key.len() > MAX_ARROW_FIELD_METADATA_KEY_BYTES
            || value.len() > MAX_ARROW_FIELD_METADATA_VALUE_BYTES
        {
            return Err(TypeCodecError::InvalidShape(
                "Arrow field metadata entry exceeds its owner bound",
            ));
        }
        bytes = bytes
            .checked_add(key.len())
            .and_then(|n| n.checked_add(value.len()))
            .ok_or(TypeCodecError::InvalidShape(
                "Arrow field metadata size overflow",
            ))?;
        if bytes > MAX_ARROW_FIELD_METADATA_BYTES {
            return Err(TypeCodecError::InvalidShape(
                "Arrow field metadata exceeds its owner bound",
            ));
        }
        observe_bytes(key.as_bytes(), work)?;
        observe_bytes(value.as_bytes(), work)?;
    }
    Ok(())
}

pub(crate) fn validate_type(
    ty: &DataType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    validate_value_type_structure_observed(ty, |visit| {
        work.step()?;
        match visit {
            ValueTypeVisit::TypeNode(ty) => {
                validate_arrow_carrier_parameters_observed(ty, || {
                    work.step().map_err(TypeCodecError::from)
                })?;
                match ty {
                    DataType::FixedSizeBinary(size) | DataType::FixedSizeList(_, size)
                        if *size > novarocks_physical_plan::MAX_FIXED_SIZE_LENGTH =>
                    {
                        return Err(TypeCodecError::InvalidShape(
                            "Arrow fixed size exceeds its owner bound",
                        ));
                    }
                    DataType::Timestamp(_, Some(zone)) => {
                        if zone.len() > novarocks_type_contract::MAX_ARROW_TIMESTAMP_TIMEZONE_BYTES
                        {
                            return Err(TypeCodecError::InvalidShape(
                                "Arrow timestamp zone exceeds its owner bound",
                            ));
                        }
                        observe_bytes(zone.as_bytes(), work)?;
                    }
                    _ => {}
                }
                Ok(())
            }
            ValueTypeVisit::Field(field) => validate_field(field, work),
            ValueTypeVisit::ChildEdge(_) => Ok(()),
        }
    })
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod fields_tests;
