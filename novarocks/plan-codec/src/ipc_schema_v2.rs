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

/// Caller-resolved admission, with no implicit application policy.
#[derive(Clone, Copy, Debug)]
pub struct IpcSchemaProjectionLimits {
    pub max_field_occurrences: usize,
    pub max_type_occurrences: usize,
    pub max_string_bytes: usize,
    pub max_flatbuffer_bytes: usize,
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

fn preflight(
    field: &Field,
    limits: IpcSchemaProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<usize, TypeCodecError> {
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
    let backing = counts.backing_upper()?;
    if backing > limits.max_flatbuffer_bytes || backing >= (1usize << 31) {
        return Err(TypeCodecError::InvalidShape(
            "IPC schema FlatBuffer envelope exceeded",
        ));
    }
    Ok(backing)
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
        let capacity = preflight(field, limits, &mut work)?;
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
        let field = emit::emit_field(field, &mut builder, &mut work)?;
        work.flush()?;
        let fields = builder.create_vector(&[field]);
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
