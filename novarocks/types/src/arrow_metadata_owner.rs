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

//! Immutable metadata owners with genuine fresh-construction receipts.
//!
//! These receipts describe allocation provenance only, never type/semantic
//! authority or a memory-accounting grant. There is deliberately no constructor
//! from an arbitrary HashMap, and no mutation access after construction.
//! Source callers must cover the complete construction peak before building.

use std::collections::HashMap;
use std::mem::{align_of, size_of};
use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};

type Entry = (String, String);

#[derive(Clone, Copy, Debug)]
pub struct MetadataOwnerLimits {
    pub entries: usize,
    pub construction_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataOwnerError {
    CapacityExceeded,
    AllocationFailed,
    DuplicateKey,
}

/// Owns a newly reserved map which has never had an entry removed. Its table
/// history is known by construction, rather than inferred from an input map.
#[derive(Debug)]
pub struct ArrowMetadataOwner {
    metadata: HashMap<String, String>,
    backing_bytes: usize,
    construction_bytes: usize,
}

impl ArrowMetadataOwner {
    /// Borrowed preflight over a Vec's initialized entries and actual spare
    /// capacity. No iteration of an unknown hash table and no allocation.
    pub fn preflight(
        entries: &Vec<Entry>,
        limits: MetadataOwnerLimits,
    ) -> Result<usize, MetadataOwnerError> {
        if entries.len() > limits.entries {
            return Err(MetadataOwnerError::CapacityExceeded);
        }
        let table = requested_table_bytes(entries.len())?;
        let vector = entries
            .capacity()
            .checked_mul(size_of::<Entry>())
            .ok_or(MetadataOwnerError::CapacityExceeded)?;
        let mut total = checked_add(table, vector)?;
        for (key, value) in entries {
            total = checked_add(total, key.capacity())?;
            total = checked_add(total, value.capacity())?;
            if total > limits.construction_bytes {
                return Err(MetadataOwnerError::CapacityExceeded);
            }
        }
        if total > limits.construction_bytes {
            return Err(MetadataOwnerError::CapacityExceeded);
        }
        Ok(total)
    }

    pub fn try_new(
        entries: Vec<Entry>,
        limits: MetadataOwnerLimits,
    ) -> Result<Self, MetadataOwnerError> {
        let construction_bytes = Self::preflight(&entries, limits)?;
        let mut metadata = HashMap::new();
        metadata
            .try_reserve(entries.len())
            .map_err(|_| MetadataOwnerError::AllocationFailed)?;
        for (key, value) in entries {
            if metadata.insert(key, value).is_some() {
                return Err(MetadataOwnerError::DuplicateKey);
            }
        }
        // This map is our own fresh, never-deleted table. Only that invariant
        // permits deriving buckets from its post-construction capacity.
        let buckets = if metadata.capacity() == 0 {
            0
        } else {
            metadata
                .capacity()
                .checked_add(1)
                .and_then(usize::checked_next_power_of_two)
                .ok_or(MetadataOwnerError::CapacityExceeded)?
        };
        let mut backing_bytes = table_bytes(buckets)?;
        for (key, value) in &metadata {
            backing_bytes = checked_add(backing_bytes, key.capacity())?;
            backing_bytes = checked_add(backing_bytes, value.capacity())?;
        }
        if backing_bytes > construction_bytes {
            return Err(MetadataOwnerError::CapacityExceeded);
        }
        Ok(Self {
            metadata,
            backing_bytes,
            construction_bytes,
        })
    }

    pub fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }

    /// Heap allocation upper bound only; the containing owner's inline size
    /// must be covered separately by the source/runtime scaffold proof.
    pub fn backing_bytes(&self) -> usize {
        self.backing_bytes
    }

    /// Heap peak of source Vec backing, its moved String heaps and fresh table.
    /// Generating entries, inline owners and later Arc/Field/Schema/name/type
    /// allocations remain separate caller obligations.
    pub fn construction_bytes(&self) -> usize {
        self.construction_bytes
    }

    /// Attaches the exact map to a fresh Field and keeps it immutable through
    /// an Arc identity. This proves only this field's own metadata map;
    /// name/DataType/nested field allocations require their own proof.
    pub fn into_field(
        self,
        name: String,
        data_type: DataType,
        nullable: bool,
    ) -> MetadataOwnedField {
        MetadataOwnedField {
            field: Arc::new(Field::new(name, data_type, nullable).with_metadata(self.metadata)),
            metadata_backing_bytes: self.backing_bytes,
        }
    }

    /// Proves only the top-level schema metadata map. Incoming Fields and
    /// their nested allocations are separate owners/proofs.
    pub fn into_schema(self, fields: Fields) -> MetadataOwnedSchema {
        MetadataOwnedSchema {
            schema: Arc::new(Schema::new_with_metadata(fields, self.metadata)),
            metadata_backing_bytes: self.backing_bytes,
        }
    }
}

/// Arc clones retain the same immutable actual field owner. A structural Field
/// clone or Arc::make_mut copy is a different owner and cannot reuse this proof.
#[derive(Clone, Debug)]
/// Proves only the attached metadata table and key/value String heaps,
/// excluding the Field/Arc/name/DataType/nested/wrapper scaffolds.
pub struct MetadataOwnedField {
    field: FieldRef,
    metadata_backing_bytes: usize,
}
impl MetadataOwnedField {
    pub fn field(&self) -> &FieldRef {
        &self.field
    }
    /// Derives only a metadata-map receipt from this known immutable source.
    /// Caller separately covers name/DataType clones and the new Field/Arc.
    pub fn derive_field(
        &self,
        data_type: DataType,
        nullable: bool,
        limits: MetadataOwnerLimits,
    ) -> Result<Self, MetadataOwnerError> {
        let metadata =
            clone_known_metadata(self.field.metadata(), self.metadata_backing_bytes, limits)?;
        let field = self
            .field
            .clone_with_metadata(metadata.metadata)
            .with_data_type(data_type)
            .with_nullable(nullable);
        Ok(Self {
            field: Arc::new(field),
            metadata_backing_bytes: metadata.backing_bytes,
        })
    }

    /// Metadata table + key/value heaps for this exact immutable field only.
    pub fn backing_bytes_for(&self, field: &FieldRef) -> Option<usize> {
        Arc::ptr_eq(&self.field, field).then_some(self.metadata_backing_bytes)
    }
}

/// Proves only the attached top-level metadata table and key/value String
/// heaps, excluding Schema/Arc/Fields/nested/wrapper scaffolds.
#[derive(Clone, Debug)]
pub struct MetadataOwnedSchema {
    schema: SchemaRef,
    metadata_backing_bytes: usize,
}
impl MetadataOwnedSchema {
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    /// The source map is known immutable. Field origins are supplied separately
    /// and are not inferred from the incoming Fields.
    pub fn derive_schema(
        &self,
        fields: Fields,
        limits: MetadataOwnerLimits,
    ) -> Result<Self, MetadataOwnerError> {
        let metadata =
            clone_known_metadata(self.schema.metadata(), self.metadata_backing_bytes, limits)?;
        Ok(metadata.into_schema(fields))
    }

    /// Top-level metadata table + key/value heaps for this exact schema only.
    pub fn backing_bytes_for(&self, schema: &SchemaRef) -> Option<usize> {
        Arc::ptr_eq(&self.schema, schema).then_some(self.metadata_backing_bytes)
    }
}

// Private: only exact immutable source-owner wrappers call this function.
// An arbitrary caller-owned HashMap can never obtain a receipt through it.
fn clone_known_metadata(
    source: &HashMap<String, String>,
    source_backing_bytes: usize,
    limits: MetadataOwnerLimits,
) -> Result<ArrowMetadataOwner, MetadataOwnerError> {
    let count = source.len();
    if count > limits.entries {
        return Err(MetadataOwnerError::CapacityExceeded);
    }
    let peak = checked_add(
        checked_add(source_backing_bytes, source_backing_bytes)?,
        checked_add(
            requested_table_bytes(count)?,
            count
                .checked_mul(size_of::<Entry>())
                .ok_or(MetadataOwnerError::CapacityExceeded)?,
        )?,
    )?;
    if peak > limits.construction_bytes {
        return Err(MetadataOwnerError::CapacityExceeded);
    }
    let mut entries = Vec::new();
    entries
        .try_reserve_exact(count)
        .map_err(|_| MetadataOwnerError::AllocationFailed)?;
    for (key, value) in source {
        entries.push((key.clone(), value.clone()));
    }
    ArrowMetadataOwner::try_new(entries, limits)
}

fn checked_add(left: usize, right: usize) -> Result<usize, MetadataOwnerError> {
    left.checked_add(right)
        .ok_or(MetadataOwnerError::CapacityExceeded)
}

// NovaRocks pins Rust 1.92.0 / std hashbrown 0.15.5. A toolchain change must
// revalidate these construction layout receipts against the allocator oracle.
// Group width 16 conservatively covers aarch64 NEON (8) and x86 SSE2 (16).
fn table_bytes(buckets: usize) -> Result<usize, MetadataOwnerError> {
    if buckets == 0 {
        return Ok(0);
    }
    let align = align_of::<Entry>().max(16);
    let payload = buckets
        .checked_mul(size_of::<Entry>())
        .ok_or(MetadataOwnerError::CapacityExceeded)?;
    let aligned = checked_add(payload, align - 1)? & !(align - 1);
    let total = checked_add(checked_add(aligned, buckets)?, 16)?;
    if total > isize::MAX as usize - (align - 1) {
        return Err(MetadataOwnerError::CapacityExceeded);
    }
    Ok(total)
}

fn requested_table_bytes(entries: usize) -> Result<usize, MetadataOwnerError> {
    let buckets = match entries {
        0 => 0,
        1..=3 => 4,
        4..=7 => 8,
        8..=14 => 16,
        _ => entries
            .checked_mul(8)
            .map(|value| value / 7)
            .and_then(usize::checked_next_power_of_two)
            .ok_or(MetadataOwnerError::CapacityExceeded)?,
    };
    table_bytes(buckets)
}

/// A finite, immutable, sorted index of genuine attached-field metadata owners.
/// It proves only those metadata maps, never a DataType or whole schema. Lookup
/// checks exact Arc identity; no unknown table is iterated or normalized.
#[derive(Clone, Debug)]
pub struct FieldMetadataOrigins {
    owners: Arc<[MetadataOwnedField]>,
}
impl FieldMetadataOrigins {
    pub fn try_new(
        mut owners: Vec<MetadataOwnedField>,
        maximum_nodes: usize,
    ) -> Result<Self, MetadataOwnerError> {
        if owners.len() > maximum_nodes {
            return Err(MetadataOwnerError::CapacityExceeded);
        }
        owners.sort_unstable_by_key(|owner| Arc::as_ptr(owner.field()) as usize);
        owners.dedup_by(|a, b| Arc::ptr_eq(a.field(), b.field()));
        Ok(Self {
            owners: owners.into(),
        })
    }

    pub fn metadata_bytes_for(&self, field: &FieldRef) -> Option<usize> {
        let pointer = Arc::as_ptr(field) as usize;
        let index = self
            .owners
            .binary_search_by_key(&pointer, |owner| Arc::as_ptr(owner.field()) as usize)
            .ok()?;
        self.owners[index].backing_bytes_for(field)
    }

    pub fn owner_for(&self, field: &FieldRef) -> Option<&MetadataOwnedField> {
        let pointer = Arc::as_ptr(field) as usize;
        let index = self
            .owners
            .binary_search_by_key(&pointer, |owner| Arc::as_ptr(owner.field()) as usize)
            .ok()?;
        Arc::ptr_eq(self.owners[index].field(), field).then_some(&self.owners[index])
    }

    /// New root-field properties keep existing child metadata origins. Newly
    /// constructed child fields still require their own genuine receipts.
    pub fn replacing_root(
        &self,
        old: &FieldRef,
        replacement: MetadataOwnedField,
        maximum_nodes: usize,
    ) -> Result<Self, MetadataOwnerError> {
        let mut owners = Vec::with_capacity(self.owners.len());
        for owner in self.owners.iter() {
            if !Arc::ptr_eq(owner.field(), old) {
                owners.push(owner.clone());
            }
        }
        owners.push(replacement);
        Self::try_new(owners, maximum_nodes)
    }

    /// Retain only exact metadata owners reachable through this field's type
    /// tree. This avoids one whole-layout provenance index per projected slot.
    pub fn for_field_tree(
        &self,
        root: &FieldRef,
        maximum_nodes: usize,
        maximum_depth: usize,
    ) -> Result<Self, MetadataOwnerError> {
        let mut owners = Vec::new();
        self.collect_field(root, 0, maximum_nodes, maximum_depth, &mut owners, &mut 0)?;
        Self::try_new(owners, maximum_nodes)
    }

    fn collect_field(
        &self,
        field: &FieldRef,
        depth: usize,
        nodes: usize,
        max_depth: usize,
        owners: &mut Vec<MetadataOwnedField>,
        visited_types: &mut usize,
    ) -> Result<(), MetadataOwnerError> {
        if depth > max_depth || owners.len() >= nodes {
            return Err(MetadataOwnerError::CapacityExceeded);
        }
        let owner = self
            .owner_for(field)
            .ok_or(MetadataOwnerError::CapacityExceeded)?;
        owners.push(owner.clone());
        self.collect_type(
            field.data_type(),
            depth,
            nodes,
            max_depth,
            owners,
            visited_types,
        )
    }

    fn collect_type(
        &self,
        data_type: &DataType,
        depth: usize,
        nodes: usize,
        max_depth: usize,
        owners: &mut Vec<MetadataOwnedField>,
        visited_types: &mut usize,
    ) -> Result<(), MetadataOwnerError> {
        *visited_types = visited_types
            .checked_add(1)
            .filter(|count| *count <= nodes)
            .ok_or(MetadataOwnerError::CapacityExceeded)?;
        // Dictionary key/value scalar leaves are physical storage at their
        // parent's semantic level. Composite recursion still consumes depth.
        if depth > max_depth
            && matches!(
                data_type,
                DataType::List(_)
                    | DataType::LargeList(_)
                    | DataType::ListView(_)
                    | DataType::LargeListView(_)
                    | DataType::FixedSizeList(_, _)
                    | DataType::Map(_, _)
                    | DataType::Struct(_)
                    | DataType::Union(_, _)
                    | DataType::Dictionary(_, _)
                    | DataType::RunEndEncoded(_, _)
            )
        {
            return Err(MetadataOwnerError::CapacityExceeded);
        }
        match data_type {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::ListView(field)
            | DataType::LargeListView(field)
            | DataType::FixedSizeList(field, _) => {
                self.collect_field(field, depth + 1, nodes, max_depth, owners, visited_types)?;
            }
            DataType::Map(entries, _) => {
                // The physical entries Struct is part of this semantic level.
                self.collect_field(entries, depth, nodes, max_depth, owners, visited_types)?;
            }
            DataType::Struct(fields) => {
                for field in fields {
                    self.collect_field(field, depth + 1, nodes, max_depth, owners, visited_types)?;
                }
            }
            DataType::Union(fields, _) => {
                for (_, field) in fields.iter() {
                    self.collect_field(field, depth + 1, nodes, max_depth, owners, visited_types)?;
                }
            }
            DataType::Dictionary(key, value) => {
                if depth >= max_depth
                    && (matches!(key.as_ref(), DataType::Dictionary(_, _))
                        || matches!(value.as_ref(), DataType::Dictionary(_, _)))
                {
                    return Err(MetadataOwnerError::CapacityExceeded);
                }
                self.collect_type(key, depth + 1, nodes, max_depth, owners, visited_types)?;
                self.collect_type(value, depth + 1, nodes, max_depth, owners, visited_types)?;
            }
            DataType::RunEndEncoded(runs, values) => {
                self.collect_field(runs, depth + 1, nodes, max_depth, owners, visited_types)?;
                self.collect_field(values, depth + 1, nodes, max_depth, owners, visited_types)?;
            }
            _ => {}
        }
        Ok(())
    }

    pub fn owners(&self) -> &[MetadataOwnedField] {
        &self.owners
    }

    /// Only the index's own compact Arc allocation, excluding pointed-to
    /// fields/maps and this wrapper's inline size.
    pub fn index_backing_bytes(&self) -> usize {
        self.owners.len() * size_of::<MetadataOwnedField>() + 4 * size_of::<usize>()
    }
}
