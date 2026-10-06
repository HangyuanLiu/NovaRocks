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

//! Deterministic occurrence definitions, without type interning or source
//! normalization. The entire projection envelope is checked before allocating
//! output definitions or copying strings. Scratch work is not a MEM grant.

use super::encode_resources::Model;
use super::{
    FieldRootSources, PackageTypeProjectionFacts, PackageTypeProjectionLimits, TypeCodecError,
    TypeProjectionLimits, ValueRootSources, WriterTypeSource, encode_logical, validate_field,
    validate_type,
};
use crate::allocation_exit_v2::reserve_exit;
use crate::arrow_metadata_v2::{copy_string, encode_metadata};
use arrow::datatypes::{DataType, Field, UnionMode};
use novarocks_proto_models::{physical_type_v2 as wire, plan};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, FunctionValueType, field_logical_type,
    validate_arrow_carrier_parameters_observed,
};
use std::{collections::BTreeSet, sync::Arc};
use wire::carrier_type_definition::Kind;

type Error = TypeCodecError;

/// The only non-strict selector carries an actual checked recipe source.
/// It is never constructed from a DTO, nominal carrier or caller boolean.
#[derive(Clone, Copy)]
pub(super) enum SourceLaw<'source> {
    Strict,
    Writer(&'source novarocks_connector_contract::ConnectorWriteRecipeDraft),
}
impl SourceLaw<'_> {
    pub(super) fn is_strict(self) -> bool {
        match self {
            Self::Strict => true,
            Self::Writer(recipe) => {
                // Keep the real source loan through every nested occurrence.
                let _ = recipe.input();
                false
            }
        }
    }
}

struct Admission<'a> {
    model: Model,
    limits: PackageTypeProjectionLimits,
    admit: &'a mut dyn FnMut(&PackageTypeProjectionFacts) -> Result<(), CompileControlError>,
}
impl Admission<'_> {
    fn gate(&mut self) -> Result<(), Error> {
        self.model.gate(self.limits, self.admit)
    }
}

struct Counts<'a> {
    definitions: usize,
    expanded: usize,
    strings: usize,
    carriers: usize,
    fields: usize,
    limits: TypeProjectionLimits,
    resources: Option<Admission<'a>>,
}

fn add(total: &mut usize, count: usize, bound: usize) -> Result<(), Error> {
    *total = total
        .checked_add(count)
        .filter(|total| *total <= bound)
        .ok_or(Error::InvalidShape(
            "type projection exceeds its admitted envelope",
        ))?;
    Ok(())
}

impl Counts<'_> {
    fn definition(&mut self) -> Result<(), Error> {
        add(&mut self.definitions, 1, self.limits.max_definitions)
            .map_err(|error| self.envelope_error(error))?;
        if let Some(resources) = &mut self.resources {
            resources.model.definition(1)?;
            resources.gate()?;
        }
        Ok(())
    }
    fn expansion(&mut self, count: usize) -> Result<(), Error> {
        add(&mut self.expanded, count, self.limits.max_expanded_nodes)
            .map_err(|error| self.envelope_error(error))?;
        if let Some(resources) = &mut self.resources {
            resources.model.expansion(count)?;
            resources.gate()?;
        }
        Ok(())
    }
    fn string(&mut self, length: usize) -> Result<(), Error> {
        add(&mut self.strings, length, self.limits.max_string_bytes)
            .map_err(|error| self.envelope_error(error))
    }
    fn envelope_error(&self, error: Error) -> Error {
        if self.resources.is_some() {
            CompileControlError::ResourceExhausted.into()
        } else {
            error
        }
    }
    fn carrier(&mut self) -> Result<(), Error> {
        self.definition()?;
        // Separate namespaces allocate by occurrence count, never a source ID.
        u32::try_from(self.carriers)
            .map_err(|_| Error::InvalidShape("carrier definition IDs are exhausted"))?;
        self.carriers = self
            .carriers
            .checked_add(1)
            .ok_or(Error::InvalidShape("carrier definition count overflow"))?;
        Ok(())
    }
    fn field(&mut self) -> Result<(), Error> {
        self.definition()?;
        u32::try_from(self.fields)
            .map_err(|_| Error::InvalidShape("field definition IDs are exhausted"))?;
        self.fields = self
            .fields
            .checked_add(1)
            .ok_or(Error::InvalidShape("field definition count overflow"))?;
        Ok(())
    }
}

fn add_child(nodes: &mut usize, child: usize) -> Result<(), Error> {
    *nodes = nodes
        .checked_add(child)
        .ok_or(Error::InvalidShape("expanded carrier node count overflow"))?;
    Ok(())
}

// Validation precedes this recursion: Strict roots retain 64 levels/4096
// nodes; immutable Writer recipes retain their original 32-level/schema law.
// Counts include each
// definition's full referenced subtree, including repeated FieldRef uses.
fn count_type(
    ty: &DataType,
    counts: &mut Counts<'_>,
    work: &mut CompileCheckpoints<'_>,
    law: SourceLaw<'_>,
) -> Result<usize, Error> {
    if let Some(resources) = &mut counts.resources {
        resources.model.carrier(ty)?;
        resources.gate()?;
        counts.carrier()?;
        work.step()?;
    } else {
        work.step()?;
        counts.carrier()?;
    }
    if !law.is_strict() {
        validate_arrow_carrier_parameters_observed(ty, || work.step().map_err(Error::from))?;
    }
    let mut nodes = 1usize;
    match ty {
        DataType::Timestamp(_, Some(zone)) => counts.string(zone.len())?,
        DataType::List(field)
        | DataType::ListView(field)
        | DataType::LargeList(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => add_child(&mut nodes, count_field(field, counts, work, law)?)?,
        DataType::Struct(fields) => {
            for field in fields {
                add_child(&mut nodes, count_field(field, counts, work, law)?)?;
            }
        }
        DataType::Union(fields, _) => {
            for (_, field) in fields.iter() {
                add_child(&mut nodes, count_field(field, counts, work, law)?)?;
            }
        }
        DataType::Dictionary(key, value) => {
            add_child(&mut nodes, count_type(key, counts, work, law)?)?;
            add_child(&mut nodes, count_type(value, counts, work, law)?)?;
        }
        DataType::RunEndEncoded(ends, values) => {
            add_child(&mut nodes, count_field(ends, counts, work, law)?)?;
            add_child(&mut nodes, count_field(values, counts, work, law)?)?;
        }
        _ => {}
    }
    counts.expansion(nodes)?;
    Ok(nodes)
}

fn count_field(
    field: &Field,
    counts: &mut Counts<'_>,
    work: &mut CompileCheckpoints<'_>,
    law: SourceLaw<'_>,
) -> Result<usize, Error> {
    if let Some(resources) = &mut counts.resources {
        resources.model.field(field)?;
        resources.gate()?;
        counts.field()?;
        work.step()?;
    } else {
        work.step()?;
        counts.field()?;
    }
    #[allow(deprecated)]
    let dictionary_id = field.dict_id();
    if !law.is_strict() && dictionary_id.is_some() != field.dict_is_ordered().is_some() {
        return Err(Error::InvalidShape(
            "incomplete field dictionary attributes",
        ));
    }
    counts.string(field.name().len())?;
    let observed = counts.resources.is_some();
    if observed {
        work.flush()?;
    }
    let mut metadata = field.metadata().iter();
    if observed {
        work.step()?;
        work.flush()?;
    }
    loop {
        if observed {
            work.flush()?;
        }
        let entry = metadata.next();
        if let Some((key, value)) = entry {
            // The actual iterator result makes these request bounds known.
            // Numeric refusal stays primary before its opaque-exit callback.
            if let Some(resources) = &mut counts.resources {
                resources.model.string(key.len())?;
                resources.model.string(value.len())?;
                resources.gate()?;
            }
        }
        if observed {
            work.step()?;
            work.flush()?;
        }
        let Some((key, value)) = entry else {
            break;
        };
        work.step()?;
        counts.string(key.len())?;
        counts.string(value.len())?;
    }
    let nodes = count_type(field.data_type(), counts, work, law)?;
    counts.expansion(nodes)?;
    Ok(nodes)
}

fn preflight<'a>(
    values: ValueRootSources<'_>,
    fields: FieldRootSources<'_>,
    writers: &[WriterTypeSource<'_>],
    limits: TypeProjectionLimits,
    resources: Option<Admission<'a>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(Counts<'a>, BTreeSet<u32>), Error> {
    let root_count = values
        .len()
        .checked_add(fields.len())
        .and_then(|count| {
            writers.iter().try_fold(count, |count, source| {
                count.checked_add(source.field_ids.len())
            })
        })
        .ok_or(Error::InvalidShape("root definition count overflow"))?;
    if root_count > limits.max_definitions {
        return Err(Error::InvalidShape(
            "type projection exceeds its admitted envelope",
        ));
    }
    let observed = resources.is_some();
    let mut reserved_fields = BTreeSet::new();
    for (id, _) in fields.iter() {
        if observed {
            work.flush()?;
        } else {
            work.step()?;
        }
        let unique = reserved_fields.insert(id);
        if observed {
            work.step()?;
            work.flush()?;
        }
        if !unique {
            return Err(Error::InvalidShape("duplicate field definition ID"));
        }
    }
    for source in writers {
        if source.field_ids.len() != source.recipe.input().field_count() {
            return Err(Error::InvalidShape(
                "writer field IDs do not cover the original input occurrences",
            ));
        }
        for id in source.field_ids {
            work.flush()?;
            let unique = reserved_fields.insert(*id);
            work.step()?;
            work.flush()?;
            if !unique {
                return Err(Error::InvalidShape("duplicate field definition ID"));
            }
        }
    }
    let mut counts = Counts {
        definitions: 0,
        expanded: 0,
        strings: 0,
        carriers: 0,
        fields: 0,
        limits,
        resources,
    };
    // Root definitions alone already consume this many entries; reject before
    // allocating duplicate-ID scratch or visiting a source tree.
    add(
        &mut counts.definitions,
        values.len(),
        limits.max_definitions,
    )?;
    if let Some(resources) = &mut counts.resources {
        resources.model.definition(values.len())?;
        resources.gate()?;
    }
    let mut ids = BTreeSet::new();
    for (id, value) in values.iter() {
        if observed {
            work.flush()?;
        } else {
            work.step()?;
        }
        let unique = ids.insert(id);
        if observed {
            work.step()?;
            work.flush()?;
        }
        if !unique {
            return Err(Error::InvalidShape("duplicate value type definition ID"));
        }
        value.logical_type.validate_carrier(&value.data_type)?;
        validate_type(&value.data_type, work)?;
        let nodes = count_type(&value.data_type, &mut counts, work, SourceLaw::Strict)?;
        counts.expansion(nodes)?;
    }
    for (_, field) in fields.iter() {
        work.step()?;
        validate_field(field, work)?;
        field_logical_type(field)?;
        validate_type(field.data_type(), work)?;
        #[allow(deprecated)]
        let dictionary_id = field.dict_id();
        if dictionary_id.is_some() != field.dict_is_ordered().is_some() {
            return Err(Error::InvalidShape(
                "incomplete field dictionary attributes",
            ));
        }
        count_field(field, &mut counts, work, SourceLaw::Strict)?;
    }
    // The immutable Draft already passed the original complete ordered-role
    // Writer law. Borrow every one of those original occurrences, rather than
    // manufacturing a second Field/FVT owner or restarting a per-field quota.
    for source in writers {
        for binding in source.recipe.input().fields_iter() {
            count_field(
                binding.field(),
                &mut counts,
                work,
                SourceLaw::Writer(source.recipe),
            )?;
        }
    }
    Ok((counts, reserved_fields))
}

struct FieldIds {
    reserved: BTreeSet<u32>,
    cursor: u64,
    observed: bool,
}
impl FieldIds {
    fn next(&mut self, work: &mut CompileCheckpoints<'_>) -> Result<u32, Error> {
        // The cursor never resets: across the whole emission, it tests at
        // most reserved-root-count + automatic-field-count identities. MAX
        // reservations do not allocate a sparse array or force a large scan.
        loop {
            work.step()?;
            let id = u32::try_from(self.cursor)
                .map_err(|_| Error::InvalidShape("field definition IDs are exhausted"))?;
            self.cursor += 1;
            if self.observed {
                work.flush()?;
            }
            let reserved = self.reserved.contains(&id);
            if self.observed {
                work.step()?;
                work.flush()?;
            }
            if !reserved {
                return Ok(id);
            }
        }
    }
}

fn output_vec<T>(
    n: usize,
    observed: bool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<T>, Error> {
    if !observed {
        return Ok(Vec::with_capacity(n));
    }
    let mut output = Vec::new();
    work.flush()?;
    let result = output.try_reserve_exact(n);
    if result.is_ok() {
        work.step()?;
    }
    reserve_exit::<Error>(result, work)?;
    Ok(output)
}

fn emit_field(
    field: &Field,
    explicit_id: Option<u32>,
    table: &mut wire::TypeTable,
    ids: &mut FieldIds,
    work: &mut CompileCheckpoints<'_>,
    law: SourceLaw<'_>,
) -> Result<u32, Error> {
    work.step()?;
    let carrier_type_id = emit_type(field.data_type(), table, ids, work, law)?;
    let id = match explicit_id {
        Some(id) => id,
        None => ids.next(work)?,
    };
    #[allow(deprecated)]
    let dictionary_id = field.dict_id();
    table.fields.push(wire::FieldDefinition {
        id,
        name: copy_string(field.name(), work)?,
        nullable: field.is_nullable(),
        carrier_type_id: Some(carrier_type_id),
        metadata: encode_metadata(field.metadata(), work)?,
        dictionary_id,
        dictionary_is_ordered: field.dict_is_ordered(),
    });
    Ok(id)
}

fn emit_type(
    ty: &DataType,
    table: &mut wire::TypeTable,
    ids: &mut FieldIds,
    work: &mut CompileCheckpoints<'_>,
    law: SourceLaw<'_>,
) -> Result<u32, Error> {
    work.step()?;
    let position = table.carriers.len();
    let id = u32::try_from(position)
        .map_err(|_| Error::InvalidShape("carrier definition IDs are exhausted"))?;
    // Reserve this occurrence's ID before visiting its ordered children. An
    // unfinished entry is private and discarded on any projection failure.
    table
        .carriers
        .push(wire::CarrierTypeDefinition { id, kind: None });
    let kind = match ty {
        DataType::List(field) => Kind::ListFieldId(emit_field(field, None, table, ids, work, law)?),
        DataType::ListView(field) => {
            Kind::ListViewFieldId(emit_field(field, None, table, ids, work, law)?)
        }
        DataType::LargeList(field) => {
            Kind::LargeListFieldId(emit_field(field, None, table, ids, work, law)?)
        }
        DataType::LargeListView(field) => {
            Kind::LargeListViewFieldId(emit_field(field, None, table, ids, work, law)?)
        }
        DataType::FixedSizeList(field, length) => Kind::FixedSizeList(wire::FixedSizeList {
            item_field_id: Some(emit_field(field, None, table, ids, work, law)?),
            length: *length,
        }),
        DataType::Struct(fields) => {
            let mut field_ids = output_vec(fields.len(), ids.observed, work)?;
            for field in fields {
                field_ids.push(emit_field(field, None, table, ids, work, law)?);
            }
            Kind::StructType(wire::StructFields { field_ids })
        }
        DataType::Union(fields, mode) => {
            let mut output = output_vec(fields.len(), ids.observed, work)?;
            for (type_id, field) in fields.iter() {
                output.push(wire::UnionField {
                    type_id: i32::from(type_id),
                    field_id: Some(emit_field(field, None, table, ids, work, law)?),
                });
            }
            Kind::UnionType(wire::UnionFields {
                mode: match mode {
                    UnionMode::Sparse => plan::ArrowUnionMode::Sparse as i32,
                    UnionMode::Dense => plan::ArrowUnionMode::Dense as i32,
                },
                fields: output,
            })
        }
        DataType::Dictionary(key, value) => Kind::Dictionary(wire::DictionaryTypes {
            key_type_id: Some(emit_type(key, table, ids, work, law)?),
            value_type_id: Some(emit_type(value, table, ids, work, law)?),
        }),
        DataType::Map(entries, ordered) => Kind::Map(wire::MapField {
            entries_field_id: Some(emit_field(entries, None, table, ids, work, law)?),
            ordered: *ordered,
        }),
        DataType::RunEndEncoded(ends, values) => Kind::RunEndEncoded(wire::RunEndEncodedFields {
            run_ends_field_id: Some(emit_field(ends, None, table, ids, work, law)?),
            values_field_id: Some(emit_field(values, None, table, ids, work, law)?),
        }),
        _ => if ids.observed {
            super::scalars::encode_scalar_from_source(ty, law, work)?
        } else {
            super::scalars::encode_scalar(ty)?
        }
        .ok_or(Error::InvalidShape("Arrow type has no v2 carrier variant"))?,
    };
    table.carriers[position].kind = Some(kind);
    Ok(id)
}

pub(super) fn encode_with_fields(
    values: &[(u32, FunctionValueType)],
    fields: &[(u32, Arc<Field>)],
    limits: TypeProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::TypeTable, Error> {
    encode_roots(
        ValueRootSources::Owned(values),
        FieldRootSources::Owned(fields),
        &[],
        limits,
        None,
        work,
    )
}

fn encode_roots<'a>(
    values: ValueRootSources<'_>,
    fields: FieldRootSources<'_>,
    writers: &[WriterTypeSource<'_>],
    limits: TypeProjectionLimits,
    resources: Option<Admission<'a>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::TypeTable, Error> {
    let (mut counts, reserved) = preflight(values, fields, writers, limits, resources, work)?;
    if let Some(resources) = &mut counts.resources {
        resources
            .model
            .output(counts.carriers, counts.fields, values.len())?;
        resources.gate()?;
    }
    work.flush()?;
    let mut ids = FieldIds {
        reserved,
        cursor: 0,
        observed: counts.resources.is_some(),
    };
    let mut table = wire::TypeTable {
        carriers: output_vec(counts.carriers, ids.observed, work)?,
        fields: output_vec(counts.fields, ids.observed, work)?,
        value_types: output_vec(values.len(), ids.observed, work)?,
    };
    for (id, value) in values.iter() {
        work.step()?;
        let carrier_type_id = emit_type(
            &value.data_type,
            &mut table,
            &mut ids,
            work,
            SourceLaw::Strict,
        )?;
        table.value_types.push(wire::ValueTypeDefinition {
            id,
            carrier_type_id: Some(carrier_type_id),
            nullable: value.nullable,
            logical_type: encode_logical(value.logical_type),
        });
    }
    for (id, field) in fields.iter() {
        work.step()?;
        emit_field(
            field,
            Some(id),
            &mut table,
            &mut ids,
            work,
            SourceLaw::Strict,
        )?;
    }
    for source in writers {
        for (id, binding) in source
            .field_ids
            .iter()
            .zip(source.recipe.input().fields_iter())
        {
            emit_field(
                binding.field(),
                Some(*id),
                &mut table,
                &mut ids,
                work,
                SourceLaw::Writer(source.recipe),
            )?;
        }
    }
    Ok(table)
}

pub(super) fn encode_writer_sources(
    values: ValueRootSources<'_>,
    fields: FieldRootSources<'_>,
    writers: &[WriterTypeSource<'_>],
    source_retained_bytes: usize,
    limits: PackageTypeProjectionLimits,
    admit: &mut impl FnMut(&PackageTypeProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::TypeTable, Error> {
    let writer_fields = writers
        .iter()
        .try_fold(0usize, |count, source| {
            count.checked_add(source.field_ids.len())
        })
        .ok_or(CompileControlError::ResourceExhausted)?;
    let model = Model::new_sources(
        source_retained_bytes,
        values,
        fields,
        writers.len(),
        writer_fields,
        limits,
    )?;
    let mut resources = Admission {
        model,
        limits,
        admit,
    };
    // Numerical roots/source/scratch admission precedes the first source
    // traversal, opaque lookup or allocation, even on a pending caller meter.
    resources.gate()?;
    encode_roots(
        values,
        fields,
        writers,
        TypeProjectionLimits {
            max_definitions: limits.max_definitions,
            max_expanded_nodes: limits.max_expanded_nodes,
            max_string_bytes: limits.max_string_bytes,
        },
        Some(resources),
        work,
    )
}
