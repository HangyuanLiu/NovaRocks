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

//! Allocation-free protobuf structural preflight.
//!
//! Prost intentionally accepts unknown fields and the last occurrence of a
//! singular field. Durable MV facts use a stricter contract, so this scanner
//! rejects both before DTO allocation and recursively applies the decode
//! depth/item budgets.

use std::collections::{BTreeMap, BTreeSet};

use crate::persistence::codec::PersistenceCodecError;
use crate::persistence::validation::PersistenceDecodeBudget;

// The encoded input remains live through prost decode, runtime conversion, the
// canonical runtime clone, regenerated DTO, and canonical re-encoding. Six
// copies of all payload bytes cover those simultaneously live representations;
// 512 bytes per observed field/value/message is larger than every generated v1
// DTO header and also covers Vec growth/allocator bookkeeping per item. A
// separate frame allowance covers the fixed recursion stack. The bound is
// based on scanner-observed structure, so many tiny nested headers cannot hide
// behind a byte-only multiplier.
const WIRE_BYTE_WORKING_SET_MULTIPLIER: usize = 6;
const STRUCTURAL_ITEM_WORKING_SET_BYTES: usize = 512;
const DEPTH_FRAME_WORKING_SET_BYTES: usize = 256;

#[derive(Clone, Copy, Debug)]
pub(super) enum Schema {
    DefinitionDocument,
    QuerySource,
    ResolutionContext,
    RelationOccurrence,
    SourceFieldBinding,
    OutputDefinition,
    ExpressionShape,
    SourceFieldReference,
    InterpretationDocument,
    OutputBinding,
    StateSlot,
    ApplyKey,
    ApplyKeyComponent,
    AggregateInterpretation,
    BranchInterpretation,
    TargetBinding,
    PhysicalFieldBinding,
    PublicationDocument,
    PublicationInput,
    PublicationOutput,
    PublicationStatistics,
    ConfigurationDocument,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WireType {
    Varint,
    LengthDelimited,
}

#[derive(Clone, Copy, Debug)]
enum ScalarKind {
    Unsigned,
    Boolean,
}

#[derive(Clone, Copy, Debug)]
enum CanonicalKey {
    Bytes(u32),
    UnsignedThenBytes(u32, u32),
}

#[derive(Clone, Copy, Debug)]
struct Field {
    number: u32,
    wire_type: WireType,
    repeated: bool,
    packed_varints: bool,
    nested: Option<Schema>,
    scalar_kind: Option<ScalarKind>,
    canonical_key: Option<CanonicalKey>,
}

const fn scalar(number: u32) -> Field {
    Field {
        number,
        wire_type: WireType::Varint,
        repeated: false,
        packed_varints: false,
        nested: None,
        scalar_kind: Some(ScalarKind::Unsigned),
        canonical_key: None,
    }
}

const fn boolean(number: u32) -> Field {
    Field {
        number,
        wire_type: WireType::Varint,
        repeated: false,
        packed_varints: false,
        nested: None,
        scalar_kind: Some(ScalarKind::Boolean),
        canonical_key: None,
    }
}

const fn bytes(number: u32) -> Field {
    Field {
        number,
        wire_type: WireType::LengthDelimited,
        repeated: false,
        packed_varints: false,
        nested: None,
        scalar_kind: None,
        canonical_key: None,
    }
}

const fn repeated_bytes(number: u32) -> Field {
    Field {
        number,
        wire_type: WireType::LengthDelimited,
        repeated: true,
        packed_varints: false,
        nested: None,
        scalar_kind: None,
        canonical_key: None,
    }
}

const fn packed_varints(number: u32) -> Field {
    Field {
        number,
        wire_type: WireType::LengthDelimited,
        repeated: true,
        packed_varints: true,
        nested: None,
        scalar_kind: None,
        canonical_key: None,
    }
}

const fn message(number: u32, nested: Schema) -> Field {
    Field {
        number,
        wire_type: WireType::LengthDelimited,
        repeated: false,
        packed_varints: false,
        nested: Some(nested),
        scalar_kind: None,
        canonical_key: None,
    }
}

const fn repeated_message(number: u32, nested: Schema) -> Field {
    Field {
        number,
        wire_type: WireType::LengthDelimited,
        repeated: true,
        packed_varints: false,
        nested: Some(nested),
        scalar_kind: None,
        canonical_key: None,
    }
}

const fn repeated_set_message(number: u32, nested: Schema, key: CanonicalKey) -> Field {
    Field {
        number,
        wire_type: WireType::LengthDelimited,
        repeated: true,
        packed_varints: false,
        nested: Some(nested),
        scalar_kind: None,
        canonical_key: Some(key),
    }
}

const DEFINITION: &[Field] = &[
    scalar(1),
    message(2, Schema::QuerySource),
    repeated_message(3, Schema::RelationOccurrence),
    repeated_message(4, Schema::OutputDefinition),
    bytes(5),
];
const QUERY: &[Field] = &[bytes(1), scalar(2), message(3, Schema::ResolutionContext)];
const RESOLUTION: &[Field] = &[bytes(1), bytes(2)];
const RELATION: &[Field] = &[
    scalar(1),
    bytes(2),
    bytes(3),
    bytes(4),
    bytes(5),
    bytes(6),
    bytes(7),
    repeated_set_message(8, Schema::SourceFieldBinding, CanonicalKey::Bytes(1)),
];
const SOURCE_FIELD: &[Field] = &[bytes(1), bytes(2), bytes(3), boolean(4)];
const OUTPUT_DEFINITION: &[Field] = &[
    bytes(1),
    bytes(2),
    bytes(3),
    boolean(4),
    message(5, Schema::ExpressionShape),
];
const EXPRESSION: &[Field] = &[
    scalar(1),
    bytes(2),
    repeated_set_message(
        3,
        Schema::SourceFieldReference,
        CanonicalKey::UnsignedThenBytes(1, 2),
    ),
];
const SOURCE_FIELD_REFERENCE: &[Field] = &[scalar(1), bytes(2)];
const INTERPRETATION: &[Field] = &[
    scalar(1),
    bytes(2),
    bytes(3),
    repeated_set_message(4, Schema::OutputBinding, CanonicalKey::Bytes(1)),
    repeated_set_message(5, Schema::StateSlot, CanonicalKey::Bytes(1)),
    message(6, Schema::ApplyKey),
    repeated_set_message(7, Schema::AggregateInterpretation, CanonicalKey::Bytes(1)),
    repeated_message(8, Schema::BranchInterpretation),
    message(9, Schema::TargetBinding),
];
const OUTPUT_BINDING: &[Field] = &[bytes(1), bytes(2), bytes(3), boolean(4)];
const STATE_SLOT: &[Field] = &[
    bytes(1),
    bytes(2),
    bytes(3),
    boolean(4),
    scalar(5),
    scalar(6),
];
const APPLY_KEY: &[Field] = &[scalar(1), repeated_message(2, Schema::ApplyKeyComponent)];
const APPLY_KEY_COMPONENT: &[Field] = &[bytes(1), bytes(2)];
const AGGREGATE: &[Field] = &[
    bytes(1),
    bytes(2),
    repeated_set_message(
        3,
        Schema::SourceFieldReference,
        CanonicalKey::UnsignedThenBytes(1, 2),
    ),
    repeated_bytes(4),
];
// Repeated uint32 is encoded packed and therefore length-delimited.
const BRANCH: &[Field] = &[bytes(1), packed_varints(2), repeated_bytes(3)];
const TARGET: &[Field] = &[
    bytes(1),
    bytes(2),
    bytes(3),
    repeated_set_message(
        4,
        Schema::PhysicalFieldBinding,
        CanonicalKey::UnsignedThenBytes(1, 2),
    ),
];
const PHYSICAL_FIELD: &[Field] = &[scalar(1), bytes(2), bytes(3), bytes(4), boolean(5)];
const PUBLICATION: &[Field] = &[
    scalar(1),
    bytes(2),
    bytes(3),
    bytes(4),
    repeated_message(5, Schema::PublicationInput),
    message(6, Schema::PublicationOutput),
    scalar(7),
    message(8, Schema::PublicationStatistics),
];
const PUBLICATION_INPUT: &[Field] = &[scalar(1), bytes(2), bytes(3)];
const PUBLICATION_OUTPUT: &[Field] = &[bytes(1), boolean(2)];
const PUBLICATION_STATISTICS: &[Field] = &[scalar(1), scalar(2)];
const CONFIGURATION: &[Field] = &[scalar(1), scalar(2), boolean(3), scalar(4), scalar(5)];

impl Schema {
    fn fields(self) -> &'static [Field] {
        match self {
            Self::DefinitionDocument => DEFINITION,
            Self::QuerySource => QUERY,
            Self::ResolutionContext => RESOLUTION,
            Self::RelationOccurrence => RELATION,
            Self::SourceFieldBinding => SOURCE_FIELD,
            Self::OutputDefinition => OUTPUT_DEFINITION,
            Self::ExpressionShape => EXPRESSION,
            Self::SourceFieldReference => SOURCE_FIELD_REFERENCE,
            Self::InterpretationDocument => INTERPRETATION,
            Self::OutputBinding => OUTPUT_BINDING,
            Self::StateSlot => STATE_SLOT,
            Self::ApplyKey => APPLY_KEY,
            Self::ApplyKeyComponent => APPLY_KEY_COMPONENT,
            Self::AggregateInterpretation => AGGREGATE,
            Self::BranchInterpretation => BRANCH,
            Self::TargetBinding => TARGET,
            Self::PhysicalFieldBinding => PHYSICAL_FIELD,
            Self::PublicationDocument => PUBLICATION,
            Self::PublicationInput => PUBLICATION_INPUT,
            Self::PublicationOutput => PUBLICATION_OUTPUT,
            Self::PublicationStatistics => PUBLICATION_STATISTICS,
            Self::ConfigurationDocument => CONFIGURATION,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PreflightUsage {
    pub encoded_bytes: usize,
    pub estimated_working_set_bytes: usize,
    pub expanded_items: usize,
}

pub(super) fn preflight(
    bytes: &[u8],
    schema: Schema,
    budget: PersistenceDecodeBudget,
) -> Result<PreflightUsage, PersistenceCodecError> {
    if bytes.len() > budget.max_document_bytes {
        return Err(PersistenceCodecError::ResourceBudget {
            resource: "encoded document",
            maximum: budget.max_document_bytes,
            actual: bytes.len(),
        });
    }
    let mut measurement = MeasureState {
        expanded_items: 0,
        structural_items: 0,
        maximum_depth: 0,
        budget,
    };
    // This first pass uses only slices, integer counters, and bounded recursion.
    // It allocates no sets or canonical-key buffers, so the complete bound is
    // established before either the canonical scanner or prost can allocate.
    measure_message(bytes, schema, 1, &mut measurement)?;
    let estimated_working_set = bytes
        .len()
        .saturating_mul(WIRE_BYTE_WORKING_SET_MULTIPLIER)
        .saturating_add(
            measurement
                .structural_items
                .saturating_mul(STRUCTURAL_ITEM_WORKING_SET_BYTES),
        )
        .saturating_add(
            measurement
                .maximum_depth
                .saturating_mul(DEPTH_FRAME_WORKING_SET_BYTES),
        );
    if estimated_working_set > budget.max_working_set_bytes {
        return Err(PersistenceCodecError::ResourceBudget {
            resource: "decode working set",
            maximum: budget.max_working_set_bytes,
            actual: estimated_working_set,
        });
    }
    let mut state = ScanState {
        expanded_items: 0,
        budget,
    };
    scan_message(bytes, schema, 1, &mut state)?;
    Ok(PreflightUsage {
        encoded_bytes: bytes.len(),
        estimated_working_set_bytes: estimated_working_set,
        expanded_items: measurement.expanded_items,
    })
}

struct MeasureState {
    expanded_items: usize,
    structural_items: usize,
    maximum_depth: usize,
    budget: PersistenceDecodeBudget,
}

fn measure_message(
    mut bytes: &[u8],
    schema: Schema,
    depth: usize,
    state: &mut MeasureState,
) -> Result<(), PersistenceCodecError> {
    state.structural_items = state.structural_items.saturating_add(1);
    state.maximum_depth = state.maximum_depth.max(depth);
    if depth > state.budget.max_depth {
        return Err(PersistenceCodecError::ResourceBudget {
            resource: "structure depth",
            maximum: state.budget.max_depth,
            actual: depth,
        });
    }
    while !bytes.is_empty() {
        state.structural_items = state.structural_items.saturating_add(1);
        let (key, key_len) = read_varint(bytes)?;
        bytes = &bytes[key_len..];
        let number =
            u32::try_from(key >> 3).map_err(|_| malformed("field number does not fit u32"))?;
        if number == 0 {
            return Err(malformed("field number zero is invalid"));
        }
        let actual_wire = match key & 0x07 {
            0 => WireType::Varint,
            2 => WireType::LengthDelimited,
            value => return Err(malformed(format!("unsupported wire type {value}"))),
        };
        let Some(field) = schema.fields().iter().find(|field| field.number == number) else {
            return Err(malformed(format!("unknown field {number} in {schema:?}")));
        };
        if actual_wire != field.wire_type {
            return Err(malformed(format!(
                "field {number} in {schema:?} uses the wrong wire type"
            )));
        }
        if field.repeated && !field.packed_varints {
            record_measured_item(state)?;
        }
        match actual_wire {
            WireType::Varint => {
                let (value, consumed) = read_varint(bytes)?;
                if matches!(field.scalar_kind, Some(ScalarKind::Boolean)) && value > 1 {
                    return Err(malformed(format!(
                        "boolean field {number} in {schema:?} has illegal value {value}"
                    )));
                }
                bytes = &bytes[consumed..];
            }
            WireType::LengthDelimited => {
                let (length, prefix_len) = read_varint(bytes)?;
                bytes = &bytes[prefix_len..];
                let length = usize::try_from(length)
                    .map_err(|_| malformed("length-delimited field is too large"))?;
                if bytes.len() < length {
                    return Err(malformed("truncated length-delimited field"));
                }
                let value = &bytes[..length];
                bytes = &bytes[length..];
                if field.packed_varints {
                    let mut packed = value;
                    while !packed.is_empty() {
                        let (_, consumed) = read_varint(packed)?;
                        packed = &packed[consumed..];
                        state.structural_items = state.structural_items.saturating_add(1);
                        record_measured_item(state)?;
                    }
                } else if let Some(nested) = field.nested {
                    measure_message(value, nested, depth + 1, state)?;
                }
            }
        }
    }
    Ok(())
}

fn record_measured_item(state: &mut MeasureState) -> Result<(), PersistenceCodecError> {
    state.expanded_items = state.expanded_items.saturating_add(1);
    if state.expanded_items > state.budget.max_items {
        return Err(PersistenceCodecError::ResourceBudget {
            resource: "expanded document items",
            maximum: state.budget.max_items,
            actual: state.expanded_items,
        });
    }
    Ok(())
}

struct ScanState {
    expanded_items: usize,
    budget: PersistenceDecodeBudget,
}

fn scan_message(
    mut bytes: &[u8],
    schema: Schema,
    depth: usize,
    state: &mut ScanState,
) -> Result<(), PersistenceCodecError> {
    if depth > state.budget.max_depth {
        return Err(PersistenceCodecError::ResourceBudget {
            resource: "structure depth",
            maximum: state.budget.max_depth,
            actual: depth,
        });
    }
    let mut seen_singular = BTreeSet::new();
    let mut previous_field_number = 0;
    let mut previous_set_keys = BTreeMap::<u32, Vec<u8>>::new();
    while !bytes.is_empty() {
        let (key, key_len) = read_varint(bytes)?;
        bytes = &bytes[key_len..];
        let number =
            u32::try_from(key >> 3).map_err(|_| malformed("field number does not fit u32"))?;
        if number == 0 {
            return Err(malformed("field number zero is invalid"));
        }
        if number < previous_field_number {
            return Err(malformed(format!(
                "field {number} in {schema:?} is not in canonical tag order"
            )));
        }
        previous_field_number = number;
        let actual_wire = match key & 0x07 {
            0 => WireType::Varint,
            2 => WireType::LengthDelimited,
            value => return Err(malformed(format!("unsupported wire type {value}"))),
        };
        let Some(field) = schema.fields().iter().find(|field| field.number == number) else {
            return Err(malformed(format!("unknown field {number} in {schema:?}")));
        };
        if actual_wire != field.wire_type {
            return Err(malformed(format!(
                "field {number} in {schema:?} uses the wrong wire type"
            )));
        }
        if field.repeated && !field.packed_varints {
            state.expanded_items = state.expanded_items.saturating_add(1);
            if state.expanded_items > state.budget.max_items {
                return Err(PersistenceCodecError::ResourceBudget {
                    resource: "expanded document items",
                    maximum: state.budget.max_items,
                    actual: state.expanded_items,
                });
            }
        } else if !seen_singular.insert(number) {
            return Err(malformed(format!(
                "singular field {number} occurs more than once in {schema:?}"
            )));
        }

        match actual_wire {
            WireType::Varint => {
                let (value, consumed) = read_varint(bytes)?;
                if matches!(field.scalar_kind, Some(ScalarKind::Boolean)) && value > 1 {
                    return Err(malformed(format!(
                        "boolean field {number} in {schema:?} has illegal value {value}"
                    )));
                }
                bytes = &bytes[consumed..];
            }
            WireType::LengthDelimited => {
                let (length, prefix_len) = read_varint(bytes)?;
                bytes = &bytes[prefix_len..];
                let length = usize::try_from(length)
                    .map_err(|_| malformed("length-delimited field is too large"))?;
                if bytes.len() < length {
                    return Err(malformed("truncated length-delimited field"));
                }
                let value = &bytes[..length];
                bytes = &bytes[length..];
                if field.packed_varints {
                    let mut packed = value;
                    while !packed.is_empty() {
                        let (_, consumed) = read_varint(packed)?;
                        packed = &packed[consumed..];
                        state.expanded_items = state.expanded_items.saturating_add(1);
                        if state.expanded_items > state.budget.max_items {
                            return Err(PersistenceCodecError::ResourceBudget {
                                resource: "expanded document items",
                                maximum: state.budget.max_items,
                                actual: state.expanded_items,
                            });
                        }
                    }
                } else if let Some(nested) = field.nested {
                    scan_message(value, nested, depth + 1, state)?;
                    if let Some(key_spec) = field.canonical_key {
                        let key = extract_canonical_key(value, key_spec)?;
                        if let Some(previous) = previous_set_keys.get(&number)
                            && previous >= &key
                        {
                            return Err(malformed(format!(
                                "repeated field {number} in {schema:?} has duplicate or noncanonical set order"
                            )));
                        }
                        previous_set_keys.insert(number, key);
                    }
                }
            }
        }
    }
    Ok(())
}

fn read_varint(bytes: &[u8]) -> Result<(u64, usize), PersistenceCodecError> {
    let mut value = 0_u64;
    for (index, byte) in bytes.iter().copied().take(10).enumerate() {
        let bits = u64::from(byte & 0x7f);
        if index == 9 && bits > 1 {
            return Err(malformed("varint overflow"));
        }
        value |= bits << (index * 7);
        if byte & 0x80 == 0 {
            if index > 0 && bits == 0 {
                return Err(malformed("non-minimal varint encoding"));
            }
            return Ok((value, index + 1));
        }
    }
    Err(malformed("truncated or overlong varint"))
}

fn extract_canonical_key(
    bytes: &[u8],
    key: CanonicalKey,
) -> Result<Vec<u8>, PersistenceCodecError> {
    match key {
        CanonicalKey::Bytes(number) => read_key_bytes(bytes, number),
        CanonicalKey::UnsignedThenBytes(unsigned_field, bytes_field) => {
            let (unsigned, remaining) = read_key_unsigned(bytes, unsigned_field)?;
            let mut key = unsigned.to_be_bytes().to_vec();
            key.extend_from_slice(&read_key_bytes(remaining, bytes_field)?);
            Ok(key)
        }
    }
}

fn read_key_unsigned(
    bytes: &[u8],
    expected_field: u32,
) -> Result<(u64, &[u8]), PersistenceCodecError> {
    let (tag, tag_len) = read_varint(bytes)?;
    if tag != u64::from(expected_field) << 3 {
        return Err(malformed("canonical set key scalar field is missing"));
    }
    let (value, value_len) = read_varint(&bytes[tag_len..])?;
    Ok((value, &bytes[tag_len + value_len..]))
}

fn read_key_bytes(bytes: &[u8], expected_field: u32) -> Result<Vec<u8>, PersistenceCodecError> {
    let (tag, tag_len) = read_varint(bytes)?;
    if tag != (u64::from(expected_field) << 3) | 2 {
        return Err(malformed("canonical set key bytes field is missing"));
    }
    let (length, length_len) = read_varint(&bytes[tag_len..])?;
    let length =
        usize::try_from(length).map_err(|_| malformed("canonical set key is too large"))?;
    let start = tag_len + length_len;
    let end = start
        .checked_add(length)
        .ok_or_else(|| malformed("canonical set key length overflow"))?;
    let value = bytes
        .get(start..end)
        .ok_or_else(|| malformed("truncated canonical set key"))?;
    Ok(value.to_vec())
}

fn malformed(message: impl Into<String>) -> PersistenceCodecError {
    PersistenceCodecError::MalformedWire(message.into())
}

#[cfg(test)]
mod tests {
    use super::STRUCTURAL_ITEM_WORKING_SET_BYTES;
    use crate::persistence::generated as proto;

    #[test]
    fn structural_allowance_covers_every_generated_dto_header() {
        let generated_sizes = [
            std::mem::size_of::<proto::DefinitionDocument>(),
            std::mem::size_of::<proto::QuerySource>(),
            std::mem::size_of::<proto::ResolutionContext>(),
            std::mem::size_of::<proto::RelationOccurrence>(),
            std::mem::size_of::<proto::SourceFieldBinding>(),
            std::mem::size_of::<proto::OutputDefinition>(),
            std::mem::size_of::<proto::ExpressionShape>(),
            std::mem::size_of::<proto::SourceFieldReference>(),
            std::mem::size_of::<proto::InterpretationDocument>(),
            std::mem::size_of::<proto::OutputBinding>(),
            std::mem::size_of::<proto::StateSlot>(),
            std::mem::size_of::<proto::ApplyKey>(),
            std::mem::size_of::<proto::ApplyKeyComponent>(),
            std::mem::size_of::<proto::AggregateInterpretation>(),
            std::mem::size_of::<proto::BranchInterpretation>(),
            std::mem::size_of::<proto::TargetBinding>(),
            std::mem::size_of::<proto::PhysicalFieldBinding>(),
            std::mem::size_of::<proto::PublicationDocument>(),
            std::mem::size_of::<proto::PublicationInput>(),
            std::mem::size_of::<proto::PublicationOutput>(),
            std::mem::size_of::<proto::PublicationStatistics>(),
            std::mem::size_of::<proto::ConfigurationDocument>(),
        ];
        let largest = generated_sizes.into_iter().max().expect("generated DTOs");
        assert!(
            largest <= STRUCTURAL_ITEM_WORKING_SET_BYTES,
            "largest generated DTO header is {largest} bytes"
        );
    }
}
