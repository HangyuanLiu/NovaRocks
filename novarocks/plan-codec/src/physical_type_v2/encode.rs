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

use super::{TypeCodecError, TypeProjectionLimits, encode_logical, validate_type};
use arrow::datatypes::{DataType, Field, UnionMode};
use novarocks_proto_models::{physical_type_v2 as wire, plan};
use novarocks_type_contract::{CompileCheckpoints, FunctionValueType};
use std::{cmp::Ordering, collections::BTreeSet};
use wire::carrier_type_definition::Kind;

type Error = TypeCodecError;

struct Counts {
    definitions: usize,
    expanded: usize,
    strings: usize,
    carriers: usize,
    fields: usize,
    limits: TypeProjectionLimits,
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

impl Counts {
    fn definition(&mut self) -> Result<(), Error> {
        add(&mut self.definitions, 1, self.limits.max_definitions)
    }
    fn expansion(&mut self, count: usize) -> Result<(), Error> {
        add(&mut self.expanded, count, self.limits.max_expanded_nodes)
    }
    fn string(&mut self, length: usize) -> Result<(), Error> {
        add(&mut self.strings, length, self.limits.max_string_bytes)
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

// Validation precedes this recursion; every root is bounded by the actual
// type owner to 64 levels and 4096 unfolded carrier nodes. Counts include each
// definition's full referenced subtree, including repeated FieldRef uses.
fn count_type(
    ty: &DataType,
    counts: &mut Counts,
    work: &mut CompileCheckpoints<'_>,
) -> Result<usize, Error> {
    work.step()?;
    counts.carrier()?;
    let mut nodes = 1usize;
    match ty {
        DataType::Timestamp(_, Some(zone)) => counts.string(zone.len())?,
        DataType::List(field)
        | DataType::ListView(field)
        | DataType::LargeList(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => add_child(&mut nodes, count_field(field, counts, work)?)?,
        DataType::Struct(fields) => {
            for field in fields {
                add_child(&mut nodes, count_field(field, counts, work)?)?;
            }
        }
        DataType::Union(fields, _) => {
            for (_, field) in fields.iter() {
                add_child(&mut nodes, count_field(field, counts, work)?)?;
            }
        }
        DataType::Dictionary(key, value) => {
            add_child(&mut nodes, count_type(key, counts, work)?)?;
            add_child(&mut nodes, count_type(value, counts, work)?)?;
        }
        DataType::RunEndEncoded(ends, values) => {
            add_child(&mut nodes, count_field(ends, counts, work)?)?;
            add_child(&mut nodes, count_field(values, counts, work)?)?;
        }
        _ => {}
    }
    counts.expansion(nodes)?;
    Ok(nodes)
}

fn count_field(
    field: &Field,
    counts: &mut Counts,
    work: &mut CompileCheckpoints<'_>,
) -> Result<usize, Error> {
    work.step()?;
    counts.field()?;
    counts.string(field.name().len())?;
    for (key, value) in field.metadata() {
        work.step()?;
        counts.string(key.len())?;
        counts.string(value.len())?;
    }
    let nodes = count_type(field.data_type(), counts, work)?;
    counts.expansion(nodes)?;
    Ok(nodes)
}

fn preflight(
    values: &[(u32, FunctionValueType)],
    limits: TypeProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Counts, Error> {
    let mut counts = Counts {
        definitions: 0,
        expanded: 0,
        strings: 0,
        carriers: 0,
        fields: 0,
        limits,
    };
    // Root definitions alone already consume this many entries; reject before
    // allocating duplicate-ID scratch or visiting a source tree.
    add(
        &mut counts.definitions,
        values.len(),
        limits.max_definitions,
    )?;
    let mut ids = BTreeSet::new();
    for (id, value) in values {
        work.step()?;
        if !ids.insert(*id) {
            return Err(Error::InvalidShape("duplicate value type definition ID"));
        }
        value.logical_type.validate_carrier(&value.data_type)?;
        validate_type(&value.data_type, work)?;
        let nodes = count_type(&value.data_type, &mut counts, work)?;
        counts.expansion(nodes)?;
    }
    Ok(counts)
}

fn copy_string(input: &str, work: &mut CompileCheckpoints<'_>) -> Result<String, Error> {
    let mut output = String::with_capacity(input.len());
    let mut start = 0usize;
    while start < input.len() {
        let mut end = start.saturating_add(1024).min(input.len());
        // A validated UTF-8 spelling needs at most three boundary adjustments.
        while !input.is_char_boundary(end) {
            end -= 1;
        }
        work.step()?;
        output.push_str(&input[start..end]);
        start = end;
    }
    Ok(output)
}

fn compare_keys(
    left: &str,
    right: &str,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Ordering, Error> {
    work.step()?;
    for (left, right) in left
        .as_bytes()
        .chunks(1024)
        .zip(right.as_bytes().chunks(1024))
    {
        work.step()?;
        let order = left.cmp(right);
        if order != Ordering::Equal {
            return Ok(order);
        }
    }
    Ok(left.len().cmp(&right.len()))
}

fn metadata(
    field: &Field,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<plan::ArrowFieldMetadataEntry>, Error> {
    // Fallible sorting observes each comparison and movement. The real field
    // owner has already bounded this borrowed scratch to 256 entries.
    let mut sorted: Vec<(&str, &str)> = Vec::with_capacity(field.metadata().len());
    for (key, value) in field.metadata() {
        work.step()?;
        sorted.push((key.as_str(), value.as_str()));
        let mut index = sorted.len() - 1;
        while index > 0 {
            if compare_keys(sorted[index - 1].0, sorted[index].0, work)? != Ordering::Greater {
                break;
            }
            work.step()?;
            sorted.swap(index - 1, index);
            index -= 1;
        }
    }
    let mut output = Vec::with_capacity(sorted.len());
    for (key, value) in sorted {
        work.step()?;
        output.push(plan::ArrowFieldMetadataEntry {
            key: copy_string(key, work)?,
            value: copy_string(value, work)?,
        });
    }
    Ok(output)
}

fn emit_field(
    field: &Field,
    table: &mut wire::TypeTable,
    work: &mut CompileCheckpoints<'_>,
) -> Result<u32, Error> {
    work.step()?;
    let carrier_type_id = emit_type(field.data_type(), table, work)?;
    let id = u32::try_from(table.fields.len())
        .map_err(|_| Error::InvalidShape("field definition IDs are exhausted"))?;
    #[allow(deprecated)]
    let dictionary_id = field.dict_id();
    table.fields.push(wire::FieldDefinition {
        id,
        name: copy_string(field.name(), work)?,
        nullable: field.is_nullable(),
        carrier_type_id: Some(carrier_type_id),
        metadata: metadata(field, work)?,
        dictionary_id,
        dictionary_is_ordered: field.dict_is_ordered(),
    });
    Ok(id)
}

fn emit_type(
    ty: &DataType,
    table: &mut wire::TypeTable,
    work: &mut CompileCheckpoints<'_>,
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
        DataType::List(field) => Kind::ListFieldId(emit_field(field, table, work)?),
        DataType::ListView(field) => Kind::ListViewFieldId(emit_field(field, table, work)?),
        DataType::LargeList(field) => Kind::LargeListFieldId(emit_field(field, table, work)?),
        DataType::LargeListView(field) => {
            Kind::LargeListViewFieldId(emit_field(field, table, work)?)
        }
        DataType::FixedSizeList(field, length) => Kind::FixedSizeList(wire::FixedSizeList {
            item_field_id: Some(emit_field(field, table, work)?),
            length: *length,
        }),
        DataType::Struct(fields) => {
            let mut field_ids = Vec::with_capacity(fields.len());
            for field in fields {
                field_ids.push(emit_field(field, table, work)?);
            }
            Kind::StructType(wire::StructFields { field_ids })
        }
        DataType::Union(fields, mode) => {
            let mut output = Vec::with_capacity(fields.len());
            for (type_id, field) in fields.iter() {
                output.push(wire::UnionField {
                    type_id: i32::from(type_id),
                    field_id: Some(emit_field(field, table, work)?),
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
            key_type_id: Some(emit_type(key, table, work)?),
            value_type_id: Some(emit_type(value, table, work)?),
        }),
        DataType::Map(entries, ordered) => Kind::Map(wire::MapField {
            entries_field_id: Some(emit_field(entries, table, work)?),
            ordered: *ordered,
        }),
        DataType::RunEndEncoded(ends, values) => Kind::RunEndEncoded(wire::RunEndEncodedFields {
            run_ends_field_id: Some(emit_field(ends, table, work)?),
            values_field_id: Some(emit_field(values, table, work)?),
        }),
        _ => super::scalars::encode_scalar(ty)?
            .ok_or(Error::InvalidShape("Arrow type has no v2 carrier variant"))?,
    };
    table.carriers[position].kind = Some(kind);
    Ok(id)
}

pub(super) fn encode(
    values: &[(u32, FunctionValueType)],
    limits: TypeProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::TypeTable, Error> {
    let counts = preflight(values, limits, work)?;
    work.flush()?;
    let mut table = wire::TypeTable {
        carriers: Vec::with_capacity(counts.carriers),
        fields: Vec::with_capacity(counts.fields),
        value_types: Vec::with_capacity(values.len()),
    };
    for (id, value) in values {
        work.step()?;
        let carrier_type_id = emit_type(&value.data_type, &mut table, work)?;
        table.value_types.push(wire::ValueTypeDefinition {
            id: *id,
            carrier_type_id: Some(carrier_type_id),
            nullable: value.nullable,
            logical_type: encode_logical(value.logical_type),
        });
    }
    Ok(table)
}
