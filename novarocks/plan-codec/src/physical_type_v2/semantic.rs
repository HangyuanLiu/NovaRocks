// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! The compiled package borrows the shared semantic namespace. Physical
//! carriers and provider source fields remain independent original facts.
use super::TypeCodecError as E;
use crate::{
    host_projection_v2::ProjectionFailure,
    value_type_table::{LogicalTypeCodecError, LogicalTypeTableLimits, resources},
};
use novarocks_proto_models::value_type as wire;
use novarocks_type_contract::{CompileCheckpoints, ValueKind, ValueTypeRef};

pub(super) fn failure<H>(
    error: ProjectionFailure<LogicalTypeCodecError, H>,
) -> ProjectionFailure<E, H> {
    match error {
        ProjectionFailure::Codec(e) => ProjectionFailure::Codec(e.into()),
        ProjectionFailure::Host(e) => ProjectionFailure::Host(e),
    }
}
pub(super) fn limits(limits: super::TypeProjectionLimits) -> LogicalTypeTableLimits {
    LogicalTypeTableLimits {
        max_definitions: limits.max_definitions,
        max_expanded_nodes: limits.max_expanded_nodes,
        max_string_bytes: limits.max_string_bytes,
    }
}
/// Builder output is already sorted by the shared observed encoder.
pub(super) fn definition<'a>(
    definitions: &'a [wire::LogicalTypeDefinition],
    id: u32,
    work: &mut CompileCheckpoints<'_>,
) -> Result<&'a wire::LogicalTypeDefinition, E> {
    let pos = resources::find(
        definitions,
        id,
        |d| d.id.expect("validated shared definition ID"),
        work,
    )?
    .ok_or(E::InvalidShape("dangling logical type reference"))?;
    Ok(&definitions[pos])
}
pub(super) fn children(definition: &wire::LogicalTypeDefinition) -> Result<Children<'_>, E> {
    use wire::logical_type_definition::Kind;
    Ok(
        match definition
            .kind
            .as_ref()
            .ok_or(E::InvalidShape("logical kind absent"))?
        {
            Kind::ListItemTypeId(id) => Children::Item(*id),
            Kind::FixedSizeList(v) => Children::Item(
                v.item_type_id
                    .ok_or(E::InvalidShape("logical item reference absent"))?,
            ),
            Kind::StructValue(v) => Children::Struct(&v.fields),
            Kind::UnionValue(v) => Children::Union(&v.fields),
            Kind::MapValue(v) => Children::Map(
                v.key_type_id
                    .ok_or(E::InvalidShape("logical key reference absent"))?,
                v.value_type_id
                    .ok_or(E::InvalidShape("logical value reference absent"))?,
            ),
            _ => Children::Scalar,
        },
    )
}
#[derive(Clone, Copy)]
pub(super) enum Children<'a> {
    Scalar,
    Item(u32),
    Struct(&'a [wire::NamedValueField]),
    Union(&'a [wire::UnionValueField]),
    Map(u32, u32),
}
#[derive(Default)]
pub(super) struct Profile {
    pub definitions: usize,
    pub expanded: usize,
    pub strings: usize,
    pub work: usize,
}
impl Profile {
    pub(super) fn value(
        &mut self,
        value: &ValueTypeRef,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), E> {
        fn add(a: &mut usize, b: usize) -> Result<(), E> {
            *a = a
                .checked_add(b)
                .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?;
            Ok(())
        }
        work.step()?;
        add(&mut self.definitions, 1)?;
        add(&mut self.expanded, value.expanded_nodes())?;
        add(&mut self.work, 256)?;
        match value.kind() {
            ValueKind::List(item) | ValueKind::FixedSizeList { item, .. } => {
                self.value(item, work)?
            }
            ValueKind::Map { key, value } => {
                self.value(key, work)?;
                self.value(value, work)?;
            }
            ValueKind::Struct(fields) => {
                for field in fields {
                    add(&mut self.strings, field.name().len())?;
                    add(
                        &mut self.work,
                        field.name().len().checked_mul(8).ok_or(
                            novarocks_type_contract::CompileControlError::ResourceExhausted,
                        )?,
                    )?;
                    self.value(field.value_type(), work)?;
                }
            }
            ValueKind::Union(fields) => {
                for field in fields {
                    add(&mut self.strings, field.field.name().len())?;
                    add(
                        &mut self.work,
                        field.field.name().len().checked_mul(8).ok_or(
                            novarocks_type_contract::CompileControlError::ResourceExhausted,
                        )?,
                    )?;
                    self.value(field.field.value_type(), work)?;
                }
            }
            ValueKind::Scalar(arrow::datatypes::DataType::Timestamp(_, Some(zone))) => {
                add(&mut self.strings, zone.len())?;
                add(
                    &mut self.work,
                    zone.len()
                        .checked_mul(8)
                        .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?,
                )?;
            }
            _ => {}
        }
        Ok(())
    }
}

pub(super) struct DefinitionIndex<'a> {
    entries: Vec<(u32, &'a wire::LogicalTypeDefinition)>,
}
impl<'a> DefinitionIndex<'a> {
    pub(super) fn new<H>(
        definitions: &'a [wire::LogicalTypeDefinition],
        context: &mut resources::Context<'_, '_, H>,
    ) -> Result<Self, ProjectionFailure<LogicalTypeCodecError, H>> {
        let mut entries = context.vector(definitions.len())?;
        for definition in definitions {
            context.step()?;
            entries.push((
                definition
                    .id
                    .ok_or_else(|| resources::invalid("logical definition ID absent"))?,
                definition,
            ));
        }
        resources::sort_by_id(&mut entries, |entry| entry.0, context)?;
        Ok(Self { entries })
    }
    pub(super) fn get(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<&'a wire::LogicalTypeDefinition, E> {
        let index = resources::find(&self.entries, id, |entry| entry.0, work)?
            .ok_or(E::InvalidShape("dangling logical type reference"))?;
        Ok(self.entries[index].1)
    }
}

/// Check explicit field references against the complete logical graph before
/// any Arrow materialization. Source wrappers may carry runtime encoding;
/// execution layout wrappers are metadata-free and canonical.
pub(super) fn validate_projection(
    index: &super::graph::Index<'_>,
    definitions: &DefinitionIndex<'_>,
    carrier_id: u32,
    logical_id: u32,
    source: bool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    use novarocks_proto_models::physical_type_v2::FieldDefinition;
    use novarocks_proto_models::physical_type_v2::carrier_type_definition::Kind as Carrier;
    fn name_equal(left: &str, right: &str, work: &mut CompileCheckpoints<'_>) -> Result<bool, E> {
        if left.len() != right.len() {
            return Ok(false);
        }
        for (left, right) in left.bytes().zip(right.bytes()) {
            work.step()?;
            if left != right {
                return Ok(false);
            }
        }
        Ok(true)
    }
    fn field(
        index: &super::graph::Index<'_>,
        defs: &DefinitionIndex<'_>,
        field: &FieldDefinition,
        logical: u32,
        role: Option<&str>,
        source: bool,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), E> {
        work.step()?;
        let expected = defs.get(logical, work)?;
        if field.logical_type_id != Some(logical) || field.nullable != expected.nullable {
            return Err(E::InvalidShape(
                "field logical reference or nullability conflicts with its owner",
            ));
        }
        if !source {
            if let Some(role) = role {
                if !name_equal(&field.name, role, work)? {
                    return Err(E::InvalidShape(
                        "execution layout role label is not canonical",
                    ));
                }
            }
        }
        validate_projection(
            index,
            defs,
            super::graph::required(field.carrier_type_id)?,
            logical,
            source,
            work,
        )
    }
    fn map_entries(
        index: &super::graph::Index<'_>,
        defs: &DefinitionIndex<'_>,
        entries: &FieldDefinition,
        key: u32,
        value: u32,
        source: bool,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), E> {
        work.step()?;
        if entries.value_binding_id.is_some()
            || entries.logical_type_id.is_some()
            || entries.nullable
        {
            return Err(E::InvalidShape(
                "Map entries must be a nonnullable physical-only wrapper",
            ));
        }
        if !source && !name_equal(&entries.name, "entries", work)? {
            return Err(E::InvalidShape("Map entries layout is not canonical"));
        }
        let Carrier::StructType(children) =
            index.kind(super::graph::required(entries.carrier_type_id)?)?
        else {
            return Err(E::InvalidShape("Map entries must be Struct"));
        };
        if children.field_ids.len() != 2 {
            return Err(E::InvalidShape("Map entries must contain key and value"));
        }
        field(
            index,
            defs,
            index.field(children.field_ids[0])?,
            key,
            Some("key"),
            source,
            work,
        )?;
        field(
            index,
            defs,
            index.field(children.field_ids[1])?,
            value,
            Some("value"),
            source,
            work,
        )
    }
    work.step()?;
    let logical = definitions.get(logical_id, work)?;
    let children = children(logical)?;
    match index.kind(carrier_id)? {
        Carrier::ListFieldId(id)
        | Carrier::LargeListFieldId(id)
        | Carrier::ListViewFieldId(id)
        | Carrier::LargeListViewFieldId(id) => {
            let Children::Item(logical) = children else {
                return Err(E::InvalidShape(
                    "list carrier conflicts with semantic shape",
                ));
            };
            field(
                index,
                definitions,
                index.field(*id)?,
                logical,
                Some("item"),
                source,
                work,
            )?;
        }
        Carrier::FixedSizeList(v) => {
            let Children::Item(logical) = children else {
                return Err(E::InvalidShape(
                    "fixed list carrier conflicts with semantic shape",
                ));
            };
            field(
                index,
                definitions,
                index.field(super::graph::required(v.item_field_id)?)?,
                logical,
                Some("item"),
                source,
                work,
            )?;
        }
        Carrier::StructType(v) => {
            let Children::Struct(fields) = children else {
                return Err(E::InvalidShape(
                    "Struct carrier conflicts with semantic shape",
                ));
            };
            if fields.len() != v.field_ids.len() {
                return Err(E::InvalidShape("Struct fields differ from semantic owner"));
            }
            for (field_id, expected) in v.field_ids.iter().zip(fields) {
                let actual = index.field(*field_id)?;
                if !name_equal(&actual.name, &expected.name, work)? {
                    return Err(E::InvalidShape(
                        "Struct name/order conflicts with semantic owner",
                    ));
                }
                field(
                    index,
                    definitions,
                    actual,
                    super::graph::required(expected.value_type_id)?,
                    None,
                    source,
                    work,
                )?;
            }
        }
        Carrier::UnionType(v) => {
            let Children::Union(fields) = children else {
                return Err(E::InvalidShape(
                    "Union carrier conflicts with semantic shape",
                ));
            };
            if fields.len() != v.fields.len() {
                return Err(E::InvalidShape("Union fields differ from semantic owner"));
            }
            for (actual, expected) in v.fields.iter().zip(fields) {
                let expected_field = expected
                    .field
                    .as_ref()
                    .ok_or(E::InvalidShape("logical Union field absent"))?;
                let actual_field = index.field(super::graph::required(actual.field_id)?)?;
                if actual.type_id != expected.type_id
                    || !name_equal(&actual_field.name, &expected_field.name, work)?
                {
                    return Err(E::InvalidShape(
                        "Union IDs/name/order conflict with semantic owner",
                    ));
                }
                field(
                    index,
                    definitions,
                    actual_field,
                    super::graph::required(expected_field.value_type_id)?,
                    None,
                    source,
                    work,
                )?;
            }
        }
        Carrier::Map(v) => {
            let Children::Map(key, value) = children else {
                return Err(E::InvalidShape("Map carrier conflicts with semantic shape"));
            };
            map_entries(
                index,
                definitions,
                index.field(super::graph::required(v.entries_field_id)?)?,
                key,
                value,
                source,
                work,
            )?;
        }
        Carrier::Dictionary(v) if source => validate_projection(
            index,
            definitions,
            super::graph::required(v.value_type_id)?,
            logical_id,
            source,
            work,
        )?,
        Carrier::RunEndEncoded(v) if source => {
            let values = index.field(super::graph::required(v.values_field_id)?)?;
            let ends = index.field(super::graph::required(v.run_ends_field_id)?)?;
            if values.logical_type_id.is_some()
                || values.value_binding_id.is_some()
                || ends.logical_type_id.is_some()
                || ends.value_binding_id.is_some()
            {
                return Err(E::InvalidShape(
                    "run-end encoding fields are physical-only wrappers",
                ));
            }
            validate_projection(
                index,
                definitions,
                super::graph::required(values.carrier_type_id)?,
                logical_id,
                source,
                work,
            )?;
        }
        Carrier::Dictionary(_) | Carrier::RunEndEncoded(_) => {
            return Err(E::InvalidShape(
                "execution layout contains runtime encoding",
            ));
        }
        _ => {
            if !matches!(children, Children::Scalar) {
                return Err(E::InvalidShape(
                    "scalar carrier conflicts with semantic shape",
                ));
            }
        }
    }
    Ok(())
}

impl Profile {
    pub(super) fn decoded(
        table: &crate::value_type_table::DecodedLogicalTypeTable,
        definitions: &[wire::LogicalTypeDefinition],
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, E> {
        use wire::logical_type_definition::Kind;
        fn add(a: &mut usize, b: usize) -> Result<(), E> {
            *a = a
                .checked_add(b)
                .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?;
            Ok(())
        }
        let mut profile = Self {
            definitions: definitions.len(),
            ..Self::default()
        };
        for (_, value) in table.iter() {
            work.step()?;
            add(&mut profile.expanded, value.expanded_nodes())?;
        }
        for definition in definitions {
            work.step()?;
            match definition
                .kind
                .as_ref()
                .expect("shared decoder validated kind")
            {
                Kind::StructValue(value) => {
                    for field in &value.fields {
                        work.step()?;
                        add(&mut profile.strings, field.name.len())?;
                    }
                }
                Kind::UnionValue(value) => {
                    for field in &value.fields {
                        work.step()?;
                        add(
                            &mut profile.strings,
                            field
                                .field
                                .as_ref()
                                .expect("validated Union field")
                                .name
                                .len(),
                        )?;
                    }
                }
                Kind::Scalar(value) => {
                    if let Some(zone) = &value.timezone {
                        add(&mut profile.strings, zone.len())?;
                    }
                }
                _ => {}
            }
        }
        profile.work = profile
            .expanded
            .checked_mul(256)
            .and_then(|work| {
                profile
                    .strings
                    .checked_mul(8)
                    .and_then(|bytes| work.checked_add(bytes))
            })
            .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?;
        Ok(profile)
    }
}
