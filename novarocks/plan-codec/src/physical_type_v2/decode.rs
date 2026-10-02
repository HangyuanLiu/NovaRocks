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

//! Checked sparse DAG projection. Scratch indexes and Arrow materialization
//! are bounded separately; neither is a host allocation grant.

use super::{
    DecodedTypeTable, TypeCodecError, TypeProjectionLimits, decode_logical, observe_bytes,
    validate_type,
};
use arrow::datatypes::{DataType, Field, UnionFields, UnionMode};
use novarocks_proto_models::{physical_type_v2 as wire, plan};
use novarocks_type_contract::{
    CompileCheckpoints, FunctionValueType, MAX_ARROW_FIELD_METADATA_BYTES,
    MAX_ARROW_FIELD_METADATA_ENTRIES, MAX_ARROW_FIELD_METADATA_KEY_BYTES,
    MAX_ARROW_FIELD_METADATA_VALUE_BYTES, MAX_ARROW_FIELD_NAME_BYTES,
    MAX_ARROW_TIMESTAMP_TIMEZONE_BYTES, MAX_VALUE_TYPE_DEPTH, MAX_VALUE_TYPE_NODES, ValueTypeError,
    field_logical_type,
};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};
use wire::carrier_type_definition::Kind;

type E = TypeCodecError;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Node {
    Carrier(u32),
    Field(u32),
}

#[derive(Clone, Copy, Default)]
struct Summary {
    nodes: usize,
    depth: usize,
}

struct Index<'a> {
    carriers: BTreeMap<u32, &'a wire::CarrierTypeDefinition>,
    fields: BTreeMap<u32, &'a wire::FieldDefinition>,
}

fn required(id: Option<u32>) -> Result<u32, E> {
    id.ok_or(E::InvalidShape("missing type table reference"))
}

fn add(total: &mut usize, amount: usize, limit: usize) -> Result<(), E> {
    *total = total
        .checked_add(amount)
        .ok_or(E::InvalidShape("type projection count overflow"))?;
    if *total > limit {
        return Err(E::InvalidShape("type projection exceeds caller limit"));
    }
    Ok(())
}

impl Index<'_> {
    fn carrier(&self, id: u32) -> Result<&wire::CarrierTypeDefinition, E> {
        self.carriers
            .get(&id)
            .copied()
            .ok_or(E::InvalidShape("dangling carrier type reference"))
    }
    fn field(&self, id: u32) -> Result<&wire::FieldDefinition, E> {
        self.fields
            .get(&id)
            .copied()
            .ok_or(E::InvalidShape("dangling field reference"))
    }
    fn kind(&self, id: u32) -> Result<&Kind, E> {
        self.carrier(id)?
            .kind
            .as_ref()
            .ok_or(E::InvalidShape("missing carrier kind"))
    }
    fn child_count(&self, node: Node) -> Result<usize, E> {
        Ok(match node {
            Node::Field(_) => 1,
            Node::Carrier(id) => match self.kind(id)? {
                Kind::ListFieldId(_)
                | Kind::ListViewFieldId(_)
                | Kind::FixedSizeList(_)
                | Kind::LargeListFieldId(_)
                | Kind::LargeListViewFieldId(_)
                | Kind::Map(_) => 1,
                Kind::StructType(fields) => fields.field_ids.len(),
                Kind::UnionType(fields) => fields.fields.len(),
                Kind::Dictionary(_) | Kind::RunEndEncoded(_) => 2,
                _ => 0,
            },
        })
    }
    fn child(&self, node: Node, ordinal: usize) -> Result<Node, E> {
        let child = match node {
            Node::Field(id) => Node::Carrier(required(self.field(id)?.carrier_type_id)?),
            Node::Carrier(id) => match self.kind(id)? {
                Kind::ListFieldId(id)
                | Kind::ListViewFieldId(id)
                | Kind::LargeListFieldId(id)
                | Kind::LargeListViewFieldId(id) => Node::Field(*id),
                Kind::FixedSizeList(value) => Node::Field(required(value.item_field_id)?),
                Kind::StructType(value) => Node::Field(value.field_ids[ordinal]),
                Kind::UnionType(value) => Node::Field(required(value.fields[ordinal].field_id)?),
                Kind::Dictionary(value) => Node::Carrier(required(if ordinal == 0 {
                    value.key_type_id
                } else {
                    value.value_type_id
                })?),
                Kind::Map(value) => Node::Field(required(value.entries_field_id)?),
                Kind::RunEndEncoded(value) => Node::Field(required(if ordinal == 0 {
                    value.run_ends_field_id
                } else {
                    value.values_field_id
                })?),
                _ => return Err(E::InvalidShape("invalid carrier child ordinal")),
            },
        };
        match child {
            Node::Carrier(id) => {
                self.carrier(id)?;
            }
            Node::Field(id) => {
                self.field(id)?;
            }
        }
        Ok(child)
    }
}

fn preflight<'a>(
    table: &'a wire::TypeTable,
    limits: TypeProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Index<'a>, E> {
    let mut definitions = 0;
    add(
        &mut definitions,
        table.carriers.len(),
        limits.max_definitions,
    )?;
    add(&mut definitions, table.fields.len(), limits.max_definitions)?;
    add(
        &mut definitions,
        table.value_types.len(),
        limits.max_definitions,
    )?;
    let mut index = Index {
        carriers: BTreeMap::new(),
        fields: BTreeMap::new(),
    };
    let mut bytes = 0;
    for carrier in &table.carriers {
        work.step()?;
        if index.carriers.insert(carrier.id, carrier).is_some() {
            return Err(E::InvalidShape("duplicate carrier type identity"));
        }
        let kind = carrier
            .kind
            .as_ref()
            .ok_or(E::InvalidShape("missing carrier kind"))?;
        if let Kind::Timestamp(timestamp) = kind
            && let Some(zone) = &timestamp.timezone
        {
            if zone.len() > MAX_ARROW_TIMESTAMP_TIMEZONE_BYTES {
                return Err(E::InvalidShape(
                    "Arrow timestamp zone exceeds its owner bound",
                ));
            }
            add(&mut bytes, zone.len(), limits.max_string_bytes)?;
            observe_bytes(zone.as_bytes(), work)?;
        }
        match kind {
            Kind::StructType(fields) if fields.field_ids.len() >= MAX_VALUE_TYPE_NODES => {
                return Err(ValueTypeError::TooManyNodes.into());
            }
            Kind::UnionType(fields) => {
                if fields.fields.len() > 128 {
                    return Err(E::InvalidShape("too many union fields"));
                }
                union_mode(fields.mode)?;
                let mut seen = [false; 128];
                for field in &fields.fields {
                    work.step()?;
                    let id = usize::try_from(field.type_id)
                        .ok()
                        .filter(|id| *id < 128)
                        .ok_or(E::InvalidShape("invalid union type identity"))?;
                    if seen[id] {
                        return Err(E::InvalidShape("duplicate union type identity"));
                    }
                    seen[id] = true;
                }
            }
            Kind::FixedSizeBinary(length)
            | Kind::FixedSizeList(wire::FixedSizeList { length, .. })
                if *length < 0 || *length > novarocks_physical_plan::MAX_FIXED_SIZE_LENGTH =>
            {
                return Err(E::InvalidShape("Arrow fixed size exceeds its owner bound"));
            }
            _ => {}
        }
    }
    for field in &table.fields {
        work.step()?;
        if index.fields.insert(field.id, field).is_some() {
            return Err(E::InvalidShape("duplicate field identity"));
        }
        if field.name.len() > MAX_ARROW_FIELD_NAME_BYTES
            || field.metadata.len() > MAX_ARROW_FIELD_METADATA_ENTRIES
        {
            return Err(E::InvalidShape(
                "Arrow field attributes exceed their owner bounds",
            ));
        }
        add(&mut bytes, field.name.len(), limits.max_string_bytes)?;
        observe_bytes(field.name.as_bytes(), work)?;
        let mut metadata_bytes = 0;
        let mut previous: Option<&str> = None;
        for entry in &field.metadata {
            work.step()?;
            if entry.key.len() > MAX_ARROW_FIELD_METADATA_KEY_BYTES
                || entry.value.len() > MAX_ARROW_FIELD_METADATA_VALUE_BYTES
            {
                return Err(E::InvalidShape(
                    "Arrow field metadata entry exceeds its owner bound",
                ));
            }
            add(
                &mut metadata_bytes,
                entry.key.len(),
                MAX_ARROW_FIELD_METADATA_BYTES,
            )?;
            add(
                &mut metadata_bytes,
                entry.value.len(),
                MAX_ARROW_FIELD_METADATA_BYTES,
            )?;
            add(&mut bytes, entry.key.len(), limits.max_string_bytes)?;
            add(&mut bytes, entry.value.len(), limits.max_string_bytes)?;
            observe_bytes(entry.key.as_bytes(), work)?;
            observe_bytes(entry.value.as_bytes(), work)?;
            // Keys are individually at most 1024 bytes, so this exact ordering
            // comparison is one bounded operation after its observed bytes.
            if previous.is_some_and(|key| key >= entry.key.as_str()) {
                return Err(E::InvalidShape("field metadata must be sorted and unique"));
            }
            previous = Some(&entry.key);
        }
    }
    let mut values = BTreeMap::new();
    for value in &table.value_types {
        work.step()?;
        if values.insert(value.id, ()).is_some() {
            return Err(E::InvalidShape("duplicate value type identity"));
        }
        index.carrier(required(value.carrier_type_id)?)?;
        decode_logical(value.logical_type)?;
    }
    for field in &table.fields {
        work.step()?;
        let dictionary = matches!(
            index.kind(required(field.carrier_type_id)?)?,
            Kind::Dictionary(_)
        );
        if if dictionary {
            field.dictionary_id.is_none() || field.dictionary_is_ordered.is_none()
        } else {
            field.dictionary_id.is_some() || field.dictionary_is_ordered.is_some()
        } {
            return Err(E::InvalidShape(
                "field dictionary attributes do not match its carrier",
            ));
        }
    }
    Ok(index)
}

struct Frame {
    node: Node,
    next: usize,
    summary: Summary,
}

// Fields are graph vertices, but not additional Arrow DataType nodes or depth.
// A carrier adds one to both; each repeated reference contributes again.
fn topology(
    index: &Index<'_>,
    table: &wire::TypeTable,
    limits: TypeProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<Node>, E> {
    let mut summaries = BTreeMap::<Node, Summary>::new();
    let mut active = BTreeMap::<Node, ()>::new();
    let mut order = Vec::new();
    let mut stack = Vec::<Frame>::new();
    for root in table
        .carriers
        .iter()
        .map(|entry| Node::Carrier(entry.id))
        .chain(table.fields.iter().map(|entry| Node::Field(entry.id)))
    {
        work.step()?;
        if summaries.contains_key(&root) {
            continue;
        }
        active.insert(root, ());
        stack.push(Frame {
            node: root,
            next: 0,
            summary: Summary::default(),
        });
        while let Some(frame) = stack.last_mut() {
            work.step()?;
            let node = frame.node;
            if frame.next < index.child_count(node)? {
                let child = index.child(node, frame.next)?;
                work.step()?;
                if let Some(summary) = summaries.get(&child) {
                    frame.summary.nodes = frame
                        .summary
                        .nodes
                        .checked_add(summary.nodes)
                        .ok_or(E::InvalidShape("expanded type size overflow"))?;
                    frame.summary.depth = frame.summary.depth.max(summary.depth);
                    frame.next += 1;
                    continue;
                }
                if active.insert(child, ()).is_some() {
                    return Err(E::InvalidShape("cyclic type table references"));
                }
                // A valid carrier path has at most 64 carriers and 64 fields.
                // Bound the explicit DFS scratch before pushing another vertex.
                if stack.len() >= MAX_VALUE_TYPE_DEPTH * 2 {
                    return Err(ValueTypeError::TooDeep.into());
                }
                stack.push(Frame {
                    node: child,
                    next: 0,
                    summary: Summary::default(),
                });
            } else {
                let mut summary = frame.summary;
                if matches!(node, Node::Carrier(_)) {
                    summary.nodes = summary
                        .nodes
                        .checked_add(1)
                        .ok_or(E::InvalidShape("expanded type size overflow"))?;
                    summary.depth = summary
                        .depth
                        .checked_add(1)
                        .ok_or(E::InvalidShape("expanded type depth overflow"))?;
                }
                if summary.nodes > MAX_VALUE_TYPE_NODES {
                    return Err(ValueTypeError::TooManyNodes.into());
                }
                if summary.depth > MAX_VALUE_TYPE_DEPTH {
                    return Err(ValueTypeError::TooDeep.into());
                }
                summaries.insert(node, summary);
                active.remove(&node);
                order.push(node);
                stack.pop();
            }
        }
    }
    let mut expanded = 0;
    for summary in summaries.values() {
        work.step()?;
        add(&mut expanded, summary.nodes, limits.max_expanded_nodes)?;
    }
    for value in &table.value_types {
        work.step()?;
        let id = required(value.carrier_type_id)?;
        let summary = summaries
            .get(&Node::Carrier(id))
            .ok_or(E::InvalidShape("missing carrier summary"))?;
        add(&mut expanded, summary.nodes, limits.max_expanded_nodes)?;
    }
    Ok(order)
}

fn union_mode(mode: i32) -> Result<UnionMode, E> {
    match plan::ArrowUnionMode::try_from(mode) {
        Ok(plan::ArrowUnionMode::Sparse) => Ok(UnionMode::Sparse),
        Ok(plan::ArrowUnionMode::Dense) => Ok(UnionMode::Dense),
        _ => Err(E::InvalidShape("unknown or unspecified union mode")),
    }
}

fn field_ref(
    fields: &BTreeMap<u32, Arc<Field>>,
    id: u32,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Arc<Field>, E> {
    work.step()?;
    fields
        .get(&id)
        .cloned()
        .ok_or(E::InvalidShape("field topology was not materialized"))
}

fn carrier_ref(
    carriers: &BTreeMap<u32, DataType>,
    id: u32,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DataType, E> {
    super::clone_carrier(
        carriers
            .get(&id)
            .ok_or(E::InvalidShape("carrier topology was not materialized"))?,
        work,
    )
}

fn materialize(
    kind: &Kind,
    carriers: &BTreeMap<u32, DataType>,
    fields: &BTreeMap<u32, Arc<Field>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DataType, E> {
    if let Some(ty) = super::scalars::decode_scalar(kind)? {
        return Ok(ty);
    }
    Ok(match kind {
        Kind::ListFieldId(id) => DataType::List(field_ref(fields, *id, work)?),
        Kind::ListViewFieldId(id) => DataType::ListView(field_ref(fields, *id, work)?),
        Kind::LargeListFieldId(id) => DataType::LargeList(field_ref(fields, *id, work)?),
        Kind::LargeListViewFieldId(id) => DataType::LargeListView(field_ref(fields, *id, work)?),
        Kind::FixedSizeList(value) => DataType::FixedSizeList(
            field_ref(fields, required(value.item_field_id)?, work)?,
            value.length,
        ),
        Kind::StructType(value) => {
            let mut output = Vec::with_capacity(value.field_ids.len());
            for id in &value.field_ids {
                output.push(field_ref(fields, *id, work)?);
            }
            DataType::Struct(output.into())
        }
        Kind::UnionType(value) => {
            let mut ids = Vec::with_capacity(value.fields.len());
            let mut output = Vec::with_capacity(value.fields.len());
            for field in &value.fields {
                work.step()?;
                ids.push(
                    i8::try_from(field.type_id)
                        .map_err(|_| E::InvalidShape("invalid union type identity"))?,
                );
                output.push(field_ref(fields, required(field.field_id)?, work)?);
            }
            DataType::Union(
                UnionFields::try_new(ids, output)
                    .map_err(|_| E::InvalidShape("invalid union fields"))?,
                union_mode(value.mode)?,
            )
        }
        Kind::Dictionary(value) => DataType::Dictionary(
            Box::new(carrier_ref(carriers, required(value.key_type_id)?, work)?),
            Box::new(carrier_ref(carriers, required(value.value_type_id)?, work)?),
        ),
        Kind::Map(value) => DataType::Map(
            field_ref(fields, required(value.entries_field_id)?, work)?,
            value.ordered,
        ),
        Kind::RunEndEncoded(value) => DataType::RunEndEncoded(
            field_ref(fields, required(value.run_ends_field_id)?, work)?,
            field_ref(fields, required(value.values_field_id)?, work)?,
        ),
        _ => return Err(E::InvalidShape("scalar carrier was not decoded")),
    })
}

pub(super) fn decode(
    table: &wire::TypeTable,
    limits: TypeProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedTypeTable, E> {
    let index = preflight(table, limits, work)?;
    let order = topology(&index, table, limits, work)?;
    // No Arrow type, field, metadata or timezone was allocated above. Observe
    // the admitted graph tail before the first materialization.
    work.flush()?;
    let mut carriers = BTreeMap::new();
    let mut fields = BTreeMap::new();
    let mut values = BTreeMap::new();
    for node in order {
        work.step()?;
        match node {
            Node::Carrier(id) => {
                let ty = materialize(index.kind(id)?, &carriers, &fields, work)?;
                validate_type(&ty, work)?;
                carriers.insert(id, ty);
            }
            Node::Field(id) => {
                let source = index.field(id)?;
                let ty = carrier_ref(&carriers, required(source.carrier_type_id)?, work)?;
                observe_bytes(source.name.as_bytes(), work)?;
                let field = match (source.dictionary_id, source.dictionary_is_ordered) {
                    #[allow(deprecated)]
                    (Some(id), Some(ordered)) => {
                        Field::new_dict(source.name.clone(), ty, source.nullable, id, ordered)
                    }
                    (None, None) => Field::new(source.name.clone(), ty, source.nullable),
                    _ => return Err(E::InvalidShape("incomplete field dictionary attributes")),
                };
                let mut metadata = HashMap::with_capacity(source.metadata.len());
                for entry in &source.metadata {
                    work.step()?;
                    observe_bytes(entry.key.as_bytes(), work)?;
                    observe_bytes(entry.value.as_bytes(), work)?;
                    metadata.insert(entry.key.clone(), entry.value.clone());
                }
                let field = field.with_metadata(metadata);
                field_logical_type(&field)?;
                fields.insert(id, Arc::new(field));
            }
        }
    }
    for source in &table.value_types {
        work.step()?;
        let logical = decode_logical(source.logical_type)?;
        let ty = carrier_ref(&carriers, required(source.carrier_type_id)?, work)?;
        logical.validate_carrier(&ty)?;
        // Every carrier subtree already passed the shared observed owner.
        // Preserve that exact carrier and authored root domain without an
        // additional unobserved recursive constructor traversal.
        values.insert(
            source.id,
            FunctionValueType {
                data_type: ty,
                nullable: source.nullable,
                logical_type: logical,
            },
        );
    }
    Ok(DecodedTypeTable {
        carriers,
        fields,
        values,
    })
}
