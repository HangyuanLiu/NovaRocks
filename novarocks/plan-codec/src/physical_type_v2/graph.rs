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

//! Sole sparse type-graph identities and child grammar. Root classification
//! and materialization share these exact borrowed references.

use super::{TypeCodecError, TypeProjectionLimits};
use novarocks_proto_models::physical_type_v2 as wire;
use novarocks_type_contract::{CompileCheckpoints, CompileControlError};
use std::collections::BTreeMap;
use wire::carrier_type_definition::Kind;
type E = TypeCodecError;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum Node {
    Carrier(u32),
    Field(u32),
}

pub(super) struct Index<'a> {
    pub(super) table: &'a wire::TypeTable,
    pub(super) carriers: BTreeMap<u32, &'a wire::CarrierTypeDefinition>,
    pub(super) fields: BTreeMap<u32, &'a wire::FieldDefinition>,
}

pub(super) fn required(id: Option<u32>) -> Result<u32, E> {
    id.ok_or(E::InvalidShape("missing type table reference"))
}

pub(super) fn add(total: &mut usize, amount: usize, limit: usize) -> Result<(), E> {
    *total = total
        .checked_add(amount)
        .ok_or(E::InvalidShape("type projection count overflow"))?;
    if *total > limit {
        return Err(E::InvalidShape("type projection exceeds caller limit"));
    }
    Ok(())
}

impl Index<'_> {
    pub(super) fn carrier(&self, id: u32) -> Result<&wire::CarrierTypeDefinition, E> {
        self.carriers
            .get(&id)
            .copied()
            .ok_or(E::InvalidShape("dangling carrier type reference"))
    }
    pub(super) fn field(&self, id: u32) -> Result<&wire::FieldDefinition, E> {
        self.fields
            .get(&id)
            .copied()
            .ok_or(E::InvalidShape("dangling field reference"))
    }
    pub(super) fn kind(&self, id: u32) -> Result<&Kind, E> {
        self.carrier(id)?
            .kind
            .as_ref()
            .ok_or(E::InvalidShape("missing carrier kind"))
    }
    pub(super) fn child_count(&self, node: Node) -> Result<usize, E> {
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
    pub(super) fn child(&self, node: Node, ordinal: usize) -> Result<Node, E> {
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

/// Only the original sparse Index contribution. Domain scratch, topology,
/// Arrow materialization and source-root enumeration are separate parent
/// contributions; these facts are not an allocation or type-validity grant.
#[derive(Clone, Copy, Debug)]
pub(super) struct IndexProjectionFacts {
    pub(super) definition_count: usize,
    pub(super) allocation_requests_upper_bound: usize,
    pub(super) request_bytes_upper_bound: usize,
    pub(super) coexistence_bytes_upper_bound: usize,
    pub(super) cumulative_work_upper_bound: usize,
}

fn numeric_add(a: usize, b: usize) -> Result<usize, E> {
    a.checked_add(b)
        .ok_or_else(|| CompileControlError::ResourceExhausted.into())
}
fn numeric_mul(a: usize, b: usize) -> Result<usize, E> {
    a.checked_mul(b)
        .ok_or_else(|| CompileControlError::ResourceExhausted.into())
}
fn resource_error(error: crate::btree_resources_v2::BTreeResourceError) -> E {
    match error {
        crate::btree_resources_v2::BTreeResourceError::SourceModel(message) => {
            E::InvalidShape(message)
        }
        crate::btree_resources_v2::BTreeResourceError::Arithmetic(_) => {
            CompileControlError::ResourceExhausted.into()
        }
    }
}

impl<'source> Index<'source> {
    pub(super) fn vertex_count(&self) -> Result<usize, E> {
        numeric_add(self.carriers.len(), self.fields.len())
    }

    /// Pure facts from the original scalar headers. Parents compose all known
    /// requests/work before any source observation; no scratch is allocated.
    pub(super) fn projection_facts(
        table: &wire::TypeTable,
        limits: TypeProjectionLimits,
        source_retained_bytes: usize,
    ) -> Result<IndexProjectionFacts, E> {
        let vertices = numeric_add(table.carriers.len(), table.fields.len())?;
        let definitions = numeric_add(vertices, table.value_types.len())?;
        if definitions > limits.max_definitions {
            return Err(E::InvalidShape("type projection exceeds caller limit"));
        }
        let carrier_facts = crate::btree_resources_v2::insertion_only::<
            u32,
            &wire::CarrierTypeDefinition,
        >(table.carriers.len())
        .map_err(resource_error)?;
        let field_facts = crate::btree_resources_v2::insertion_only::<u32, &wire::FieldDefinition>(
            table.fields.len(),
        )
        .map_err(resource_error)?;
        let requested = numeric_add(
            carrier_facts.request_bytes_upper_bound,
            field_facts.request_bytes_upper_bound,
        )?;
        Ok(IndexProjectionFacts {
            definition_count: definitions,
            allocation_requests_upper_bound: numeric_add(
                carrier_facts.allocation_requests_upper_bound,
                field_facts.allocation_requests_upper_bound,
            )?,
            request_bytes_upper_bound: requested,
            coexistence_bytes_upper_bound: numeric_add(source_retained_bytes, requested)?,
            cumulative_work_upper_bound: numeric_add(
                numeric_add(
                    carrier_facts.cumulative_work_upper_bound,
                    field_facts.cumulative_work_upper_bound,
                )?,
                numeric_add(128, numeric_mul(vertices, 64)?)?,
            )?,
        })
    }

    /// Build the same sparse reference index before choosing a root domain.
    /// This does not run Field/Value/Writer attribute laws or prove topology.
    /// The caller owns entry/footer and composes this contribution with the
    /// whole original source and all subsequent graph/materialization facts.
    pub(super) fn prepare_observed(
        table: &'source wire::TypeTable,
        limits: TypeProjectionLimits,
        source_retained_bytes: usize,
        admit: &mut impl FnMut(&IndexProjectionFacts) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, E> {
        let facts = Self::projection_facts(table, limits, source_retained_bytes)?;
        // All requests and fixed-header work are known before the first
        // callback or BTree insertion. No second wallet or source is created.
        admit(&facts)?;
        let mut floor = size_of::<wire::TypeTable>();
        for backing in [
            numeric_mul(
                table.carriers.capacity(),
                size_of::<wire::CarrierTypeDefinition>(),
            )?,
            numeric_mul(table.fields.capacity(), size_of::<wire::FieldDefinition>())?,
            numeric_mul(
                table.value_types.capacity(),
                size_of::<wire::ValueTypeDefinition>(),
            )?,
        ] {
            floor = numeric_add(floor, backing)?;
        }
        if floor > source_retained_bytes {
            return Err(E::InvalidShape("type graph source invoice is understated"));
        }
        work.flush()?;
        let mut index = Self {
            table,
            carriers: BTreeMap::new(),
            fields: BTreeMap::new(),
        };
        for carrier in &table.carriers {
            work.flush()?;
            let duplicate = index.carriers.insert(carrier.id, carrier).is_some();
            work.step()?;
            work.flush()?;
            if duplicate {
                return Err(E::InvalidShape("duplicate carrier type identity"));
            }
            carrier
                .kind
                .as_ref()
                .ok_or(E::InvalidShape("missing carrier kind"))?;
        }
        for field in &table.fields {
            work.flush()?;
            let duplicate = index.fields.insert(field.id, field).is_some();
            work.step()?;
            work.flush()?;
            if duplicate {
                return Err(E::InvalidShape("duplicate field identity"));
            }
        }
        Ok(index)
    }
}
