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

//! Exact one-field standard Arrow IPC schema messages for constant pools.
//!
//! This schema author preserves the actual Field; it does not fabricate it
//! from a value type. It does not encode value buffers or admit a complete pool
//! stream. The explicit envelope bounds primary FlatBuffer backing before its
//! allocation; auxiliary builder/sorting storage is structurally bounded, not
//! a formal MEM grant or a total-heap/RSS model.

use crate::physical_type_v2::{TypeCodecError, validate_field, validate_type};
use arrow::datatypes::{DataType, Field};
use flatbuffers::FlatBufferBuilder;
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, PureCompileControl, ValueTypeVisit,
    validate_value_type_structure_observed,
};

mod emit;
mod verify;

// A supported Field chain has at most 64 value-type levels. The standard
// Message -> Schema -> Field framing adds three verifier table levels;
// dictionary/type payloads fit within the same source-derived cover.
const MAX_SCHEMA_VERIFIER_DEPTH: usize = novarocks_type_contract::MAX_VALUE_TYPE_DEPTH + 3;

/// Caller-resolved admission, with no implicit application policy.
#[derive(Clone, Copy, Debug)]
pub struct IpcSchemaProjectionLimits {
    pub max_field_occurrences: usize,
    pub max_type_occurrences: usize,
    pub max_string_bytes: usize,
    pub max_flatbuffer_bytes: usize,
}

struct SchemaPreflight {
    backing: usize,
    tables: usize,
}

#[derive(Default)]
struct Counts {
    fields: usize,
    types: usize,
    metadata_entries: usize,
    string_count: usize,
    string_bytes: usize,
}

fn checked_add(a: usize, b: usize) -> Result<usize, TypeCodecError> {
    a.checked_add(b)
        .ok_or(TypeCodecError::InvalidShape("IPC schema extent overflow"))
}
fn checked_mul(a: usize, b: usize) -> Result<usize, TypeCodecError> {
    a.checked_mul(b)
        .ok_or(TypeCodecError::InvalidShape("IPC schema extent overflow"))
}

impl Counts {
    fn string(
        &mut self,
        length: usize,
        limits: IpcSchemaProjectionLimits,
    ) -> Result<(), TypeCodecError> {
        self.string_count = checked_add(self.string_count, 1)?;
        self.string_bytes = checked_add(self.string_bytes, length)?;
        if self.string_bytes > limits.max_string_bytes {
            return Err(TypeCodecError::InvalidShape(
                "IPC schema string envelope exceeded",
            ));
        }
        Ok(())
    }
    fn field(
        &mut self,
        field: &Field,
        limits: IpcSchemaProjectionLimits,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), TypeCodecError> {
        self.fields = checked_add(self.fields, 1)?;
        if self.fields > limits.max_field_occurrences {
            return Err(TypeCodecError::InvalidShape(
                "IPC schema field envelope exceeded",
            ));
        }
        self.string(field.name().len(), limits)?;
        for (key, value) in field.metadata() {
            work.step()?;
            self.metadata_entries = checked_add(self.metadata_entries, 1)?;
            self.string(key.len(), limits)?;
            self.string(value.len(), limits)?;
        }
        Ok(())
    }
    fn backing_upper(&self) -> Result<usize, TypeCodecError> {
        // Arrow's current schema/message tables have at most seven slots.
        // Each slot is at most eight bytes with at most seven alignment bytes.
        // Include every possible slot, a signed vtable offset/alignment, and
        // the entire vtable even when the builder later deduplicates it.
        const TABLE_SLOTS: usize = 7;
        const TABLE_BYTES: usize = TABLE_SLOTS * (8 + 7) + (4 + 7) + (4 + 2 * TABLE_SLOTS);
        let tables = checked_add(
            checked_add(self.fields, self.types)?,
            checked_add(self.metadata_entries, 2)?,
        )?;
        // Each type can emit a children vector and a Union tag vector; each
        // Field can emit a metadata vector. The Schema has one root vector.
        let vectors = checked_add(checked_add(checked_mul(self.types, 2)?, self.fields)?, 1)?;
        let vector_payload = checked_add(
            checked_add(
                checked_mul(self.fields, 8)?,
                checked_mul(self.metadata_entries, 4)?,
            )?,
            4,
        )?;
        let string_bytes = checked_add(self.string_bytes, checked_mul(self.string_count, 8)?)?;
        checked_add(
            checked_add(checked_mul(tables, TABLE_BYTES)?, string_bytes)?,
            checked_add(checked_add(vector_payload, checked_mul(vectors, 7)?)?, 11)?,
        )
    }
}

fn preflight_source(
    field: &Field,
    limits: IpcSchemaProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Counts, TypeCodecError> {
    validate_field(field, work)?;
    novarocks_type_contract::field_logical_type(field)?;
    validate_type(field.data_type(), work)?;
    let mut counts = Counts::default();
    counts.field(field, limits, work)?;
    validate_value_type_structure_observed(field.data_type(), |visit| {
        work.step()?;
        match visit {
            ValueTypeVisit::TypeNode(ty) => {
                counts.types = checked_add(counts.types, 1)?;
                if counts.types > limits.max_type_occurrences {
                    return Err(TypeCodecError::InvalidShape(
                        "IPC schema type envelope exceeded",
                    ));
                }
                match ty {
                    DataType::Timestamp(_, Some(zone)) => counts.string(zone.len(), limits)?,
                    DataType::Dictionary(_, value)
                        if matches!(value.as_ref(), DataType::Dictionary(_, _)) =>
                    {
                        return Err(TypeCodecError::InvalidShape(
                            "direct nested dictionary has no inner IPC Field identity",
                        ));
                    }
                    _ => {}
                }
            }
            ValueTypeVisit::Field(field) => counts.field(field, limits, work)?,
            ValueTypeVisit::ChildEdge(_) => {}
        }
        Ok(())
    })?;
    Ok(counts)
}

fn preflight_writer(
    field: &Field,
    limits: IpcSchemaProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<SchemaPreflight, TypeCodecError> {
    let counts = preflight_source(field, limits, work)?;
    let backing = counts.backing_upper()?;
    if backing > limits.max_flatbuffer_bytes || backing >= (1usize << 31) {
        return Err(TypeCodecError::InvalidShape(
            "IPC schema FlatBuffer envelope exceeded",
        ));
    }
    let tables = checked_add(
        checked_add(counts.fields, counts.types)?,
        checked_add(counts.metadata_entries, 2)?,
    )?;
    Ok(SchemaPreflight { backing, tables })
}

/// Emits a V5 schema message, without stream framing or array-buffer encoding.
/// Success and ordinary errors observe the completed tail; control refusals
/// remain the original typed cause and never replay a later callback.
pub fn encode_single_field_schema(
    field: &Field,
    limits: IpcSchemaProjectionLimits,
    control: &dyn PureCompileControl,
) -> Result<Vec<u8>, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = (|| {
        let facts = preflight_writer(field, limits, &mut work)?;
        let capacity = facts.backing;
        work.flush()?;
        let mut backing = Vec::new();
        backing.try_reserve_exact(capacity).map_err(|_| {
            TypeCodecError::Control(novarocks_type_contract::CompileControlError::ResourceExhausted)
        })?;
        work.flush()?;
        // Reserved initialization is our own bounded loop, not an opaque
        // capacity-sized zero-fill hidden inside the library constructor.
        while backing.len() < capacity {
            let end = backing.len().saturating_add(1024).min(capacity);
            backing.resize(end, 0);
            work.step()?;
        }
        work.flush()?;
        let mut builder = FlatBufferBuilder::from_vec(backing);
        work.flush()?;
        let field_offset = emit::emit_field(field, &mut builder, &mut work)?;
        work.flush()?;
        let fields = builder.create_vector(&[field_offset]);
        let schema = arrow::ipc::Schema::create(
            &mut builder,
            &arrow::ipc::SchemaArgs {
                endianness: arrow::ipc::Endianness::Little,
                fields: Some(fields),
                custom_metadata: None,
                features: None,
            },
        );
        let message = arrow::ipc::Message::create(
            &mut builder,
            &arrow::ipc::MessageArgs {
                version: arrow::ipc::MetadataVersion::V5,
                header_type: arrow::ipc::MessageHeader::Schema,
                header: Some(schema.as_union_value()),
                bodyLength: 0,
                custom_metadata: None,
            },
        );
        arrow::ipc::finish_message_buffer(&mut builder, message);
        work.flush()?;
        // The proven primary backing cover must prevent builder growth. This
        // postcondition is a source-model witness, not allocation admission.
        if builder.mut_finished_buffer().0.len() > capacity {
            return Err(TypeCodecError::InvalidShape(
                "IPC schema exceeded its proven backing cover",
            ));
        }
        // These are source-derived bounds for this exact writer: every
        // emitted table is counted, and the backing cover also bounds the
        // official verifier's apparent visits, including reused vtables.
        let verifier = novarocks_arrow_ipc_frame::VerifierOptions {
            max_depth: MAX_SCHEMA_VERIFIER_DEPTH,
            max_tables: facts.tables,
            max_apparent_size: capacity,
            ignore_missing_null_terminator: false,
        };
        verify_message(builder.finished_data(), field, &verifier, &mut work)?;
        work.flush()?;
        let output = builder.finished_data().to_vec();
        work.flush()?;
        Ok(output)
    })();
    if matches!(&result, Err(TypeCodecError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

#[cfg(test)]
mod tests;

/// Validates the borrowed schema before any Arrow schema-object conversion.
/// The official verifier's envelope is mandatory and caller-authored. This
/// entry checks one V5 single-field schema message, not an IPC value stream.
/// A depth envelope wider than the supported schema grammar is refused;
/// smaller caller limits remain unchanged. Apparent-size arithmetic must
/// also cover one attempted in-buffer visit without overflowing usize.
pub fn verify_single_field_schema_message(
    metadata: &[u8],
    expected: &Field,
    limits: IpcSchemaProjectionLimits,
    verifier: &novarocks_arrow_ipc_frame::VerifierOptions,
    control: &dyn PureCompileControl,
) -> Result<(), TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result =
        verify_single_field_schema_with_work(metadata, expected, limits, verifier, &mut work);
    if matches!(&result, Err(TypeCodecError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

pub(crate) fn verify_single_field_schema_with_work(
    metadata: &[u8],
    expected: &Field,
    limits: IpcSchemaProjectionLimits,
    verifier: &novarocks_arrow_ipc_frame::VerifierOptions,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    if metadata.len() > limits.max_flatbuffer_bytes {
        return Err(TypeCodecError::InvalidShape(
            "IPC schema metadata envelope exceeded",
        ));
    }
    // Decode owns no FlatBufferBuilder. Its byte cap applies to the actual
    // borrowed metadata, independently of the encoder's conservative backing
    // cover. Reuse only common source grammar/count/string admission here.
    preflight_source(expected, limits, work)?;
    verify_message(metadata, expected, verifier, work)
}

#[cfg(test)]
mod verify_tests;

fn verify_message(
    metadata: &[u8],
    expected: &Field,
    verifier: &novarocks_arrow_ipc_frame::VerifierOptions,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    let message = verified_message_observed(metadata, verifier, work)?;
    work.step()?;
    if message.version() != arrow::ipc::MetadataVersion::V5
        || message.header_type() != arrow::ipc::MessageHeader::Schema
        || message.bodyLength() != 0
        || message
            .custom_metadata()
            .is_some_and(|entries| !entries.is_empty())
    {
        return Err(TypeCodecError::InvalidShape(
            "unsupported Arrow IPC schema message profile",
        ));
    }
    let schema = message
        .header_as_schema()
        .ok_or(TypeCodecError::InvalidShape(
            "Arrow IPC schema payload is missing",
        ))?;
    work.step()?;
    if schema.endianness() != arrow::ipc::Endianness::Little
        || schema.features().is_some_and(|entries| !entries.is_empty())
        || schema
            .custom_metadata()
            .is_some_and(|entries| !entries.is_empty())
    {
        return Err(TypeCodecError::InvalidShape(
            "unsupported Arrow IPC schema root profile",
        ));
    }
    let fields = schema.fields().ok_or(TypeCodecError::InvalidShape(
        "Arrow IPC schema fields are missing",
    ))?;
    work.step()?;
    if fields.len() != 1 {
        return Err(TypeCodecError::InvalidShape(
            "constant IPC schema must contain exactly one field",
        ));
    }
    verify::verify_field(expected, fields.get(0), work)
}

/// Common bounded, borrowed Message verification. The outer owner retains
/// its exact message profile and completes ordinary-error tails.
pub(crate) fn verified_message_observed<'a>(
    metadata: &'a [u8],
    verifier: &novarocks_arrow_ipc_frame::VerifierOptions,
    work: &mut CompileCheckpoints<'_>,
) -> Result<arrow::ipc::Message<'a>, TypeCodecError> {
    if verifier.max_depth > MAX_SCHEMA_VERIFIER_DEPTH {
        return Err(TypeCodecError::InvalidShape(
            "IPC schema verifier depth envelope exceeds supported grammar",
        ));
    }
    // The official verifier adds each in-buffer visit before comparing its
    // apparent limit. A visit cannot exceed the metadata slice, so this sum
    // proves that even the first over-limit visit cannot overflow usize.
    checked_add(verifier.max_apparent_size, metadata.len())?;
    work.flush()?;
    let parsed = novarocks_arrow_ipc_frame::verified_message(metadata, verifier);
    // Observe the opaque verifier even on its ordinary malformed outcome.
    work.flush()?;
    parsed.map_err(|_| TypeCodecError::InvalidShape("invalid Arrow IPC message metadata"))
}
