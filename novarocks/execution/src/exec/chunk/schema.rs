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
use std::collections::HashMap;
use std::sync::Arc;

use crate::exec::chunk::type_compatibility::{check_exact, nested_path_label};
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use novarocks_types::SlotId;
use novarocks_types::arrow_metadata_owner::{
    ArrowMetadataOwner, FieldMetadataOrigins, MetadataOwnedSchema, MetadataOwnerLimits,
};
use novarocks_types::logical::{LogicalType, logical_type_of_field};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ChunkFieldSchema {
    logical_type: Option<LogicalType>,
    children: Vec<ChunkFieldSchema>,
}

impl ChunkFieldSchema {
    pub(crate) fn new(logical_type: Option<LogicalType>, children: Vec<Self>) -> Self {
        Self {
            logical_type,
            children,
        }
    }

    /// Empty metadata used by owner-local test adapters when no source field
    /// metadata is available on the wire.
    pub fn empty() -> Self {
        Self {
            logical_type: None,
            children: Vec::new(),
        }
    }

    pub fn from_field(field: &Field) -> Result<Self, String> {
        Ok(Self {
            logical_type: logical_type_of_field(field),
            children: Self::children_from_arrow_data_type(field.data_type())?,
        })
    }

    fn children_from_arrow_data_type(data_type: &DataType) -> Result<Vec<Self>, String> {
        match data_type {
            DataType::Struct(fields) => fields
                .iter()
                .map(|child| Self::from_field(child.as_ref()))
                .collect::<Result<Vec<_>, _>>(),
            DataType::List(item) | DataType::LargeList(item) => {
                Ok(vec![Self::from_field(item.as_ref())?])
            }
            DataType::Map(entries, _) => {
                let DataType::Struct(entry_fields) = entries.data_type() else {
                    return Err(format!(
                        "map entries is not struct: {:?}",
                        entries.data_type()
                    ));
                };
                if entry_fields.len() != 2 {
                    return Err(format!(
                        "map entries expected 2 struct fields, got {}",
                        entry_fields.len()
                    ));
                }
                Ok(vec![
                    Self::from_field(entry_fields[0].as_ref())?,
                    Self::from_field(entry_fields[1].as_ref())?,
                ])
            }
            _ => Ok(Vec::new()),
        }
    }

    pub fn logical_type(&self) -> Option<LogicalType> {
        self.logical_type
    }

    pub fn json_semantic(&self) -> bool {
        self.logical_type == Some(LogicalType::Json)
    }

    pub fn children(&self) -> &[ChunkFieldSchema] {
        &self.children
    }

    pub fn struct_child(&self, idx: usize) -> Option<&ChunkFieldSchema> {
        self.children.get(idx)
    }

    pub fn list_item(&self) -> Option<&ChunkFieldSchema> {
        self.children.first()
    }

    pub fn map_key(&self) -> Option<&ChunkFieldSchema> {
        self.children.first()
    }

    pub fn map_value(&self) -> Option<&ChunkFieldSchema> {
        self.children.get(1)
    }
}

#[derive(Debug, Clone)]
pub struct ChunkSlotSchema {
    slot_id: SlotId,
    field: FieldRef,
    metadata_origins: Option<FieldMetadataOrigins>,
    field_schema: ChunkFieldSchema,
    unique_id: Option<i32>,
}

impl PartialEq for ChunkSlotSchema {
    fn eq(&self, other: &Self) -> bool {
        self.slot_id == other.slot_id
            && self.field == other.field
            && self.field_schema == other.field_schema
            && self.unique_id == other.unique_id
    }
}
impl Eq for ChunkSlotSchema {}

impl ChunkSlotSchema {
    pub fn new_with_field(
        slot_id: SlotId,
        field: Field,
        field_schema: Option<ChunkFieldSchema>,
        unique_id: Option<i32>,
    ) -> Self {
        Self::try_new_with_field(slot_id, field, field_schema, unique_id)
            .unwrap_or_else(|e| panic!("{e}"))
    }

    pub fn try_new_with_field(
        slot_id: SlotId,
        field: Field,
        field_schema: Option<ChunkFieldSchema>,
        unique_id: Option<i32>,
    ) -> Result<Self, String> {
        Ok(Self {
            slot_id,
            field_schema: match field_schema {
                Some(schema) => schema,
                None => ChunkFieldSchema::from_field(&field)?,
            },
            field: Arc::new(field),
            metadata_origins: None,
            unique_id,
        })
    }

    pub fn try_new_with_metadata_origins(
        slot_id: SlotId,
        field: FieldRef,
        metadata_origins: FieldMetadataOrigins,
        field_schema: Option<ChunkFieldSchema>,
        unique_id: Option<i32>,
    ) -> Result<Self, String> {
        if metadata_origins.metadata_bytes_for(&field).is_none() {
            return Err(
                "chunk slot metadata receipt does not match the exact field owner".to_string(),
            );
        }
        Ok(Self {
            slot_id,
            field_schema: match field_schema {
                Some(schema) => schema,
                None => ChunkFieldSchema::from_field(&field)?,
            },
            field,
            metadata_origins: Some(metadata_origins),
            unique_id,
        })
    }

    pub fn metadata_origins(&self) -> Option<&FieldMetadataOrigins> {
        self.metadata_origins.as_ref()
    }

    pub fn field_ref(&self) -> &FieldRef {
        &self.field
    }

    pub fn with_type_and_nullable(
        &self,
        data_type: DataType,
        nullable: bool,
    ) -> Result<Self, String> {
        if &data_type == self.field.data_type() && nullable == self.field.is_nullable() {
            return Ok(self.clone());
        }
        if let Some(origins) = &self.metadata_origins {
            let original = origins
                .owner_for(&self.field)
                .ok_or("chunk field metadata origin is missing")?;
            let replacement = original
                .derive_field(
                    data_type,
                    nullable,
                    MetadataOwnerLimits {
                        entries: 65536,
                        construction_bytes: 96 * 1024 * 1024,
                    },
                )
                .map_err(|_| "chunk field metadata derivation exceeds its source profile")?;
            let field = Arc::clone(replacement.field());
            let origins = origins
                .replacing_root(&self.field, replacement, 65536)
                .and_then(|origins| origins.for_field_tree(&field, 65536, 64))
                .map_err(|_| "chunk field metadata origins exceed the node profile or lack a child owner")?;
            Self::try_new_with_metadata_origins(
                self.slot_id,
                field,
                origins,
                Some(self.field_schema.clone()),
                self.unique_id,
            )
        } else {
            self.with_field(
                self.field
                    .as_ref()
                    .clone()
                    .with_data_type(data_type)
                    .with_nullable(nullable),
            )
        }
    }

    fn reconcile_to_carrier(&self, actual: &DataType, nullable: bool) -> Result<Self, String> {
        let Some(origins) = &self.metadata_origins else {
            let field = reconcile_chunk_field_to_data_type(self.field(), actual, nullable)?;
            return self.with_type_and_nullable(field.data_type().clone(), field.is_nullable());
        };
        // Only known immutable source maps can be copied. The actual carrier's
        // metadata remains a separate, unproven owner; it is never normalized.
        let scoped = origins
            .for_field_tree(&self.field, 65536, 64)
            .map_err(|_| "chunk field metadata tree is incomplete or exceeds its profile")?;
        let mut derived = Vec::new();
        let field = reconcile_owned_field(&self.field, actual, nullable, &scoped, &mut derived)?;
        if Arc::ptr_eq(&self.field, &field) {
            return Ok(self.clone());
        }
        let origins = FieldMetadataOrigins::try_new(derived, 65536)
            .map_err(|_| "chunk field metadata derivation exceeds its node profile")?;
        Self::try_new_with_metadata_origins(
            self.slot_id,
            field,
            origins,
            Some(self.field_schema.clone()),
            self.unique_id,
        )
    }

    /// Return a copy of this slot schema with nullable set to the given value.
    pub fn with_nullable(&self, nullable: bool) -> Self {
        if self.field.is_nullable() == nullable {
            return self.clone();
        }
        self.with_type_and_nullable(self.field.data_type().clone(), nullable)
            .unwrap_or_else(|error| panic!("{error}"))
    }

    pub fn from_field(
        slot_id: SlotId,
        field: &Field,
        unique_id: Option<i32>,
    ) -> Result<Self, String> {
        Self::try_new_with_field(slot_id, field.clone(), None, unique_id)
    }

    pub fn with_field(&self, field: Field) -> Result<Self, String> {
        Self::try_new_with_field(
            self.slot_id,
            field,
            Some(self.field_schema.clone()),
            self.unique_id,
        )
    }

    pub fn with_slot_id(&self, slot_id: SlotId) -> Result<Self, String> {
        let mut slot = self.clone();
        slot.slot_id = slot_id;
        Ok(slot)
    }

    pub fn with_field_and_slot_id(&self, slot_id: SlotId, field: Field) -> Result<Self, String> {
        Self::try_new_with_field(
            slot_id,
            field,
            Some(self.field_schema.clone()),
            self.unique_id,
        )
    }

    pub fn slot_id(&self) -> SlotId {
        self.slot_id
    }

    pub fn field(&self) -> &Field {
        &self.field
    }

    pub fn name(&self) -> &str {
        self.field.name()
    }

    pub fn nullable(&self) -> bool {
        self.field.is_nullable()
    }

    pub fn data_type(&self) -> &DataType {
        self.field.data_type()
    }

    pub fn unique_id(&self) -> Option<i32> {
        self.unique_id
    }

    pub fn field_schema(&self) -> &ChunkFieldSchema {
        &self.field_schema
    }
}

#[derive(Debug, Clone)]
pub struct ChunkSchema {
    slots: Vec<ChunkSlotSchema>,
    arrow_schema: SchemaRef,
    slot_ids: Vec<SlotId>,
    index_by_slot: HashMap<SlotId, usize>,
    field_metadata_origins: Option<FieldMetadataOrigins>,
    schema_metadata_origin: Option<MetadataOwnedSchema>,
}

impl PartialEq for ChunkSchema {
    fn eq(&self, other: &Self) -> bool {
        self.slots == other.slots
            && self.arrow_schema == other.arrow_schema
            && self.slot_ids == other.slot_ids
            && self.index_by_slot == other.index_by_slot
    }
}
impl Eq for ChunkSchema {}

pub type ChunkSchemaRef = Arc<ChunkSchema>;

fn check_chunk_data_type(
    expected: &DataType,
    actual: &DataType,
    root_label: &str,
) -> Result<(), String> {
    check_exact(expected, actual).map_err(|m| {
        format!(
            "chunk schema type mismatch at {}: expected {:?}, got {:?} ({:?})",
            nested_path_label(root_label, &m.nested_path),
            expected,
            actual,
            m.kind
        )
    })
}

fn reconcile_chunk_field_to_data_type(
    expected: &Field,
    actual: &DataType,
    actual_nullable: bool,
) -> Result<Arc<Field>, String> {
    let data_type = reconcile_chunk_data_type(expected.data_type(), actual)?;
    let nullable = expected.is_nullable() || actual_nullable;
    if &data_type == expected.data_type() && nullable == expected.is_nullable() {
        Ok(Arc::new(expected.clone()))
    } else {
        Ok(Arc::new(rebuild_chunk_field(expected, data_type, nullable)))
    }
}

fn rebuild_chunk_field(expected: &Field, data_type: DataType, nullable: bool) -> Field {
    Field::new(expected.name(), data_type, nullable).with_metadata(expected.metadata().clone())
}

fn reconcile_chunk_data_type(expected: &DataType, actual: &DataType) -> Result<DataType, String> {
    if expected == actual {
        return Ok(expected.clone());
    }
    check_chunk_data_type(expected, actual, "column")?;
    if is_dictionary_string_carrier(expected, actual) {
        return Ok(actual.clone());
    }
    match (expected, actual) {
        (DataType::List(expected_field), DataType::List(actual_field)) => Ok(DataType::List(
            reconcile_nested_chunk_field(expected_field, actual_field)?,
        )),
        (DataType::LargeList(expected_field), DataType::LargeList(actual_field)) => Ok(
            DataType::LargeList(reconcile_nested_chunk_field(expected_field, actual_field)?),
        ),
        (DataType::Map(expected_field, expected_ordered), DataType::Map(actual_field, _)) => {
            Ok(DataType::Map(
                reconcile_nested_chunk_field(expected_field, actual_field)?,
                *expected_ordered,
            ))
        }
        (DataType::Struct(expected_fields), DataType::Struct(actual_fields)) => {
            let fields = expected_fields
                .iter()
                .zip(actual_fields.iter())
                .map(|(expected_field, actual_field)| {
                    reconcile_nested_chunk_field(expected_field, actual_field)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(DataType::Struct(fields.into()))
        }
        _ => Ok(expected.clone()),
    }
}

fn reconcile_nested_chunk_field(
    expected: &Arc<Field>,
    actual: &Arc<Field>,
) -> Result<Arc<Field>, String> {
    let data_type = reconcile_chunk_data_type(expected.data_type(), actual.data_type())?;
    Ok(Arc::new(rebuild_chunk_field(
        expected,
        data_type,
        expected.is_nullable() || actual.is_nullable(),
    )))
}

fn reconcile_owned_field(
    expected: &FieldRef,
    actual: &DataType,
    actual_nullable: bool,
    origins: &FieldMetadataOrigins,
    derived: &mut Vec<novarocks_types::arrow_metadata_owner::MetadataOwnedField>,
) -> Result<FieldRef, String> {
    let data_type = reconcile_owned_type(expected.data_type(), actual, origins, derived)?;
    let nullable = expected.is_nullable() || actual_nullable;
    let owner = origins
        .owner_for(expected)
        .ok_or("nested chunk field metadata origin is missing")?;
    let owner = if &data_type == expected.data_type() && nullable == expected.is_nullable() {
        owner.clone()
    } else {
        owner
            .derive_field(
                data_type,
                nullable,
                MetadataOwnerLimits {
                    entries: 65536,
                    construction_bytes: 96 * 1024 * 1024,
                },
            )
            .map_err(|_| "nested chunk metadata derivation exceeds its source profile")?
    };
    if derived.len() >= 65536 {
        return Err("nested chunk metadata derivation exceeds its node profile".into());
    }
    let field = Arc::clone(owner.field());
    derived.push(owner);
    Ok(field)
}

fn reconcile_owned_type(
    expected: &DataType,
    actual: &DataType,
    origins: &FieldMetadataOrigins,
    derived: &mut Vec<novarocks_types::arrow_metadata_owner::MetadataOwnedField>,
) -> Result<DataType, String> {
    // Validate the complete expected tree once per subtree before copying any
    // metadata. The index walk is bounded and does not inspect unknown tables.
    check_chunk_data_type(expected, actual, "column")?;
    if is_dictionary_string_carrier(expected, actual) {
        return Ok(actual.clone());
    }
    match (expected, actual) {
        (DataType::List(a), DataType::List(b)) => Ok(DataType::List(reconcile_owned_field(
            a,
            b.data_type(),
            b.is_nullable(),
            origins,
            derived,
        )?)),
        (DataType::LargeList(a), DataType::LargeList(b)) => Ok(DataType::LargeList(
            reconcile_owned_field(a, b.data_type(), b.is_nullable(), origins, derived)?,
        )),
        (DataType::Map(a, ordered), DataType::Map(b, _)) => Ok(DataType::Map(
            reconcile_owned_field(a, b.data_type(), b.is_nullable(), origins, derived)?,
            *ordered,
        )),
        (DataType::Struct(a), DataType::Struct(b)) => {
            let fields = a
                .iter()
                .zip(b)
                .map(|(a, b)| {
                    reconcile_owned_field(a, b.data_type(), b.is_nullable(), origins, derived)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(DataType::Struct(fields.into()))
        }
        _ => Ok(expected.clone()),
    }
}

fn is_dictionary_string_carrier(expected: &DataType, actual: &DataType) -> bool {
    matches!(
        (expected, actual),
        (
            DataType::Utf8 | DataType::LargeUtf8,
            DataType::Dictionary(key, value),
        ) if key.as_ref() == &DataType::Int32 && value.as_ref() == expected
    )
}

impl ChunkSchema {
    pub(crate) fn from_static_layout(
        layout: &novarocks_local_program::StaticLayout,
    ) -> Result<ChunkSchemaRef, String> {
        if !layout.has_exact_slot_metadata() {
            return Err("local program layout has no exact slot metadata".to_string());
        }
        let slots = layout
            .schema()
            .fields()
            .iter()
            .zip(layout.slots())
            .enumerate()
            .map(|(index, (field, slot))| {
                let (field_schema, unique_id) = layout
                    .slot_metadata_at(index)
                    .ok_or_else(|| format!("local program slot {index} lacks metadata"))?;
                let metadata = Some(crate::exec::expr::static_program::thaw_field_schema(
                    field_schema,
                ));
                if let Some(origins) = layout.field_metadata_origins() {
                    ChunkSlotSchema::try_new_with_metadata_origins(
                        *slot,
                        Arc::clone(field),
                        origins
                            .for_field_tree(field, 65536, 64)
                            .map_err(|_| "local program field metadata origins are incomplete")?,
                        metadata,
                        unique_id,
                    )
                } else {
                    ChunkSlotSchema::try_new_with_field(
                        *slot,
                        field.as_ref().clone(),
                        metadata,
                        unique_id,
                    )
                }
            })
            .collect::<Result<Vec<_>, String>>()?;
        let mut schema = Self::try_new_with_schema_metadata(slots, HashMap::new())?;
        if let Some(origin) = layout.schema_metadata_origin() {
            // Keep the exact original immutable schema and map. All source
            // slot field Arcs above already refer to these same fields.
            if origin.backing_bytes_for(layout.schema()).is_none() {
                return Err(
                    "local program schema metadata owner conflicts with its layout".to_string(),
                );
            }
            schema.arrow_schema = Arc::clone(layout.schema());
            schema.schema_metadata_origin = Some(origin.clone());
        } else {
            schema.arrow_schema = Arc::new(Schema::new_with_metadata(
                schema
                    .slots
                    .iter()
                    .map(|slot| Arc::clone(slot.field_ref()))
                    .collect::<Vec<_>>(),
                layout.schema().metadata().clone(),
            ));
        }
        Ok(Arc::new(schema))
    }

    pub fn try_new(slots: Vec<ChunkSlotSchema>) -> Result<Self, String> {
        let metadata = ArrowMetadataOwner::try_new(
            Vec::new(),
            MetadataOwnerLimits {
                entries: 0,
                construction_bytes: 0,
            },
        )
        .map_err(|_| "empty schema metadata construction failed")?;
        Self::try_new_with_owned_schema_metadata(slots, metadata)
    }

    pub fn try_new_with_schema_metadata(
        slots: Vec<ChunkSlotSchema>,
        metadata: HashMap<String, String>,
    ) -> Result<Self, String> {
        Self::build(slots, Some(metadata), None)
    }

    pub fn try_new_with_owned_schema_metadata(
        slots: Vec<ChunkSlotSchema>,
        metadata: ArrowMetadataOwner,
    ) -> Result<Self, String> {
        Self::build(slots, None, Some(metadata))
    }

    fn build(
        slots: Vec<ChunkSlotSchema>,
        unknown_metadata: Option<HashMap<String, String>>,
        owned_metadata: Option<ArrowMetadataOwner>,
    ) -> Result<Self, String> {
        let mut index_by_slot = HashMap::with_capacity(slots.len());
        let mut slot_ids = Vec::with_capacity(slots.len());
        let mut fields = Vec::with_capacity(slots.len());
        for (idx, slot) in slots.iter().enumerate() {
            if index_by_slot.insert(slot.slot_id(), idx).is_some() {
                return Err(format!(
                    "duplicate slot id {} in chunk schema contract at index {}",
                    slot.slot_id(),
                    idx
                ));
            }
            slot_ids.push(slot.slot_id());
            fields.push(Arc::clone(slot.field_ref()));
        }
        let field_metadata_origins = if slots.iter().all(|slot| slot.metadata_origins().is_some()) {
            let count = slots
                .iter()
                .try_fold(0usize, |count, slot| {
                    count
                        .checked_add(slot.metadata_origins().unwrap().owners().len())
                        .filter(|count| *count <= 65536)
                })
                .ok_or("chunk schema metadata origin collection exceeds its node profile")?;
            let mut owners = Vec::with_capacity(count);
            for slot in &slots {
                owners.extend_from_slice(slot.metadata_origins().unwrap().owners());
            }
            Some(
                FieldMetadataOrigins::try_new(owners, 65536)
                    .map_err(|_| "chunk schema metadata origins exceed their node profile")?,
            )
        } else {
            None
        };
        let (arrow_schema, schema_metadata_origin) = if let Some(metadata) = owned_metadata {
            let origin = metadata.into_schema(fields.into());
            (Arc::clone(origin.schema()), Some(origin))
        } else {
            let metadata =
                unknown_metadata.ok_or("chunk schema metadata has no construction input")?;
            (Arc::new(Schema::new_with_metadata(fields, metadata)), None)
        };
        Ok(Self {
            slots,
            arrow_schema,
            slot_ids,
            index_by_slot,
            field_metadata_origins,
            schema_metadata_origin,
        })
    }

    pub fn empty() -> Self {
        Self::try_new(Vec::new()).expect("empty chunk schema construction is infallible")
    }

    pub fn field_metadata_origins(&self) -> Option<&FieldMetadataOrigins> {
        self.field_metadata_origins.as_ref()
    }

    pub fn schema_metadata_origin(&self) -> Option<&MetadataOwnedSchema> {
        self.schema_metadata_origin.as_ref()
    }

    pub fn slots(&self) -> &[ChunkSlotSchema] {
        &self.slots
    }

    pub fn arrow_schema_ref(&self) -> SchemaRef {
        Arc::clone(&self.arrow_schema)
    }

    pub fn slot_ids(&self) -> &[SlotId] {
        &self.slot_ids
    }

    pub fn field(&self, idx: usize) -> Option<&Field> {
        self.slots.get(idx).map(ChunkSlotSchema::field)
    }

    pub fn field_by_slot(&self, slot_id: SlotId) -> Option<&Field> {
        self.slot(slot_id).map(ChunkSlotSchema::field)
    }

    pub fn slot_schema_from_arrow_field(
        slot_id: SlotId,
        field: &Field,
    ) -> Result<ChunkSlotSchema, String> {
        ChunkSlotSchema::from_field(slot_id, field, None)
    }

    pub fn try_ref_from_schema_and_slot_ids(
        schema: &Schema,
        slot_ids: &[SlotId],
    ) -> Result<ChunkSchemaRef, String> {
        if schema.fields().len() != slot_ids.len() {
            return Err(format!(
                "chunk schema slot id length mismatch: schema_fields={} slot_ids={}",
                schema.fields().len(),
                slot_ids.len()
            ));
        }
        let slots = schema
            .fields()
            .iter()
            .zip(slot_ids.iter().copied())
            .map(|(field, slot_id)| Self::slot_schema_from_arrow_field(slot_id, field.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        Self::try_new_with_schema_metadata(slots, schema.metadata().clone()).map(Arc::new)
    }

    pub fn slot(&self, slot_id: SlotId) -> Option<&ChunkSlotSchema> {
        self.index_by_slot
            .get(&slot_id)
            .and_then(|idx| self.slots.get(*idx))
    }

    pub fn index_of(&self, slot_id: SlotId) -> Option<usize> {
        self.index_by_slot.get(&slot_id).copied()
    }

    pub fn index_by_slot(&self) -> &HashMap<SlotId, usize> {
        &self.index_by_slot
    }

    pub fn project_by_slots(&self, slot_ids: &[SlotId]) -> Result<Self, String> {
        let mut slots = Vec::with_capacity(slot_ids.len());
        for slot_id in slot_ids {
            let slot = self.slot(*slot_id).cloned().ok_or_else(|| {
                format!(
                    "chunk schema projection references missing slot {} (available={:?})",
                    slot_id,
                    self.slot_ids()
                )
            })?;
            slots.push(slot);
        }
        Self::try_new(slots)
    }

    pub fn with_fields_in_order(&self, fields: Vec<Field>) -> Result<Self, String> {
        if fields.len() != self.slots.len() {
            return Err(format!(
                "chunk schema field length mismatch: fields={} slots={}",
                fields.len(),
                self.slots.len()
            ));
        }
        let slots = self
            .slots
            .iter()
            .cloned()
            .zip(fields.into_iter())
            .map(|(slot, field)| slot.with_field(field))
            .collect::<Result<Vec<_>, _>>()?;
        Self::try_new(slots)
    }

    pub fn concat(parts: &[ChunkSchemaRef]) -> Result<Self, String> {
        let mut slots = Vec::new();
        for part in parts {
            slots.extend_from_slice(part.slots());
        }
        Self::try_new(slots)
    }
}

pub(super) fn align_chunk_schema_to_batch(
    batch: &RecordBatch,
    chunk_schema: &ChunkSchema,
) -> Result<ChunkSchemaRef, String> {
    if batch.num_columns() != chunk_schema.slots().len() {
        return Err(format!(
            "chunk schema contract length mismatch: batch_columns={} contract_slots={}",
            batch.num_columns(),
            chunk_schema.slots().len()
        ));
    }
    let mut slots = Vec::with_capacity(batch.num_columns());
    for (idx, field) in batch.schema().fields().iter().enumerate() {
        let expected = chunk_schema
            .slots()
            .get(idx)
            .ok_or_else(|| format!("missing chunk schema slot at index {}", idx))?;
        // Arrow field nullability is producer metadata here. A nullable batch
        // can flow through a non-nullable contract because source-level NOT
        // NULL enforcement happens downstream, and a non-nullable batch is a
        // valid runtime instance of a nullable contract.
        if field.name() != expected.name() {
            return Err(format!(
                "chunk schema field mismatch at index {}: batch=({}, {:?}, {}) contract=({}, {:?}, {})",
                idx,
                field.name(),
                field.data_type(),
                field.is_nullable(),
                expected.name(),
                expected.data_type(),
                expected.nullable()
            ));
        }
        let root = format!("slot {} ({})", expected.slot_id(), expected.name());
        check_chunk_data_type(expected.data_type(), field.data_type(), &root)?;
        slots.push(expected.reconcile_to_carrier(field.data_type(), field.is_nullable())?);
    }
    Ok(Arc::new(ChunkSchema::try_new(slots)?))
}

pub(super) fn align_chunk_schema_to_columns(
    columns: &[ArrayRef],
    chunk_schema: &ChunkSchema,
) -> Result<ChunkSchemaRef, String> {
    if columns.len() != chunk_schema.slots().len() {
        return Err(format!(
            "chunk schema contract length mismatch: columns={} contract_slots={}",
            columns.len(),
            chunk_schema.slots().len()
        ));
    }
    let mut slots = Vec::with_capacity(columns.len());
    for (idx, column) in columns.iter().enumerate() {
        let expected = chunk_schema
            .slots()
            .get(idx)
            .ok_or_else(|| format!("missing chunk schema slot at index {}", idx))?;
        let root = format!("slot {} ({})", expected.slot_id(), expected.name());
        check_chunk_data_type(expected.data_type(), column.data_type(), &root)?;
        slots.push(expected.reconcile_to_carrier(column.data_type(), column.null_count() > 0)?);
    }
    Ok(Arc::new(ChunkSchema::try_new(slots)?))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, BinaryArray, Decimal128Array, Int8Array, Int32Array, Int64Array, MapArray,
        StringArray, StructArray, TimestampMicrosecondArray,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;

    use super::{ChunkSchema, ChunkSlotSchema};
    use crate::exec::chunk::Chunk;
    use novarocks_types::SlotId;
    use novarocks_types::logical::{LogicalType, field_with_logical_type, logical_type_of_field};

    fn owned_slot(id: u32) -> ChunkSlotSchema {
        use novarocks_types::arrow_metadata_owner::{
            ArrowMetadataOwner, FieldMetadataOrigins, MetadataOwnerLimits,
        };
        let owner = ArrowMetadataOwner::try_new(
            vec![("nr_logical_type".to_string(), "JSON".to_string())],
            MetadataOwnerLimits {
                entries: 1,
                construction_bytes: 4096,
            },
        )
        .unwrap()
        .into_field(format!("v{id}"), DataType::Utf8, false);
        ChunkSlotSchema::try_new_with_metadata_origins(
            SlotId::new(id),
            Arc::clone(owner.field()),
            FieldMetadataOrigins::try_new(vec![owner], 1).unwrap(),
            None,
            Some(id as i32),
        )
        .unwrap()
    }

    #[test]
    fn owned_slot_derivation_preserves_identity_only_for_unchanged_field() {
        let source = owned_slot(7);
        let unchanged = source
            .with_type_and_nullable(DataType::Utf8, false)
            .unwrap();
        assert!(Arc::ptr_eq(source.field_ref(), unchanged.field_ref()));
        let renamed_slot = source.with_slot_id(SlotId::new(9)).unwrap();
        assert!(Arc::ptr_eq(source.field_ref(), renamed_slot.field_ref()));
        assert!(
            renamed_slot
                .metadata_origins()
                .unwrap()
                .metadata_bytes_for(source.field_ref())
                .is_some()
        );
        let nullable = source.with_nullable(true);
        assert!(!Arc::ptr_eq(source.field_ref(), nullable.field_ref()));
        assert_eq!(source.field().metadata(), nullable.field().metadata());
        assert!(
            nullable
                .metadata_origins()
                .unwrap()
                .metadata_bytes_for(nullable.field_ref())
                .is_some()
        );
        assert!(
            nullable
                .metadata_origins()
                .unwrap()
                .metadata_bytes_for(source.field_ref())
                .is_none()
        );
        let arbitrary = source.with_field(source.field().clone()).unwrap();
        assert_eq!(source, arbitrary);
        assert!(arbitrary.metadata_origins().is_none());
    }

    #[test]
    fn nested_carrier_alignment_derives_every_changed_source_field_owner() {
        use novarocks_types::arrow_metadata_owner::{
            ArrowMetadataOwner, FieldMetadataOrigins, MetadataOwnerLimits,
        };
        let limits = MetadataOwnerLimits {
            entries: 1,
            construction_bytes: 4096,
        };
        let item =
            ArrowMetadataOwner::try_new(vec![("nr_logical_type".into(), "JSON".into())], limits)
                .unwrap()
                .into_field("item".into(), DataType::Utf8, false);
        let list = ArrowMetadataOwner::try_new(vec![], limits)
            .unwrap()
            .into_field(
                "values".into(),
                DataType::List(Arc::clone(item.field())),
                false,
            );
        let root = ArrowMetadataOwner::try_new(vec![], limits)
            .unwrap()
            .into_field(
                "root".into(),
                DataType::Struct(vec![Arc::clone(list.field())].into()),
                false,
            );
        let old_item = Arc::clone(item.field());
        let old_list = Arc::clone(list.field());
        let source = ChunkSchema::try_new(vec![
            ChunkSlotSchema::try_new_with_metadata_origins(
                SlotId::new(1),
                Arc::clone(root.field()),
                FieldMetadataOrigins::try_new(vec![item, list, root], 3).unwrap(),
                None,
                None,
            )
            .unwrap(),
        ])
        .unwrap();
        let actual_item = Arc::new(Field::new(
            "item",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ));
        let actual_list = Arc::new(Field::new(
            "values",
            DataType::List(Arc::clone(&actual_item)),
            true,
        ));
        let actual_type = DataType::Struct(vec![actual_list].into());
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "root",
                actual_type.clone(),
                true,
            )])),
            vec![arrow::array::new_empty_array(&actual_type)],
        )
        .unwrap();
        let aligned = super::align_chunk_schema_to_batch(&batch, &source).unwrap();
        let slot = &aligned.slots()[0];
        let origins = slot.metadata_origins().unwrap();
        let reachable = origins.for_field_tree(slot.field_ref(), 5, 2).unwrap();
        assert_eq!(reachable.owners().len(), 3);
        let DataType::Struct(fields) = slot.data_type() else {
            panic!("struct")
        };
        let DataType::List(item) = fields[0].data_type() else {
            panic!("list")
        };
        assert!(!Arc::ptr_eq(&old_item, item));
        assert!(!Arc::ptr_eq(&old_list, &fields[0]));
        assert_eq!(logical_type_of_field(item), Some(LogicalType::Json));
        assert!(item.is_nullable());
        assert!(origins.metadata_bytes_for(item).is_some());
        assert!(origins.metadata_bytes_for(&fields[0]).is_some());
        assert!(origins.metadata_bytes_for(&actual_item).is_none());
        assert!(
            origins
                .metadata_bytes_for(&batch.schema().fields()[0])
                .is_none()
        );
        assert_eq!(
            super::align_chunk_schema_to_batch(&batch, &aligned).unwrap(),
            aligned
        );
    }

    #[test]
    fn retagged_actual_nested_array_shares_only_the_exact_known_target_field() {
        use novarocks_types::arrow_metadata_owner::{
            ArrowMetadataOwner, FieldMetadataOrigins, MetadataOwnerLimits,
        };
        let limits = MetadataOwnerLimits {
            entries: 1,
            construction_bytes: 4096,
        };
        let child =
            ArrowMetadataOwner::try_new(vec![("nr_logical_type".into(), "JSON".into())], limits)
                .unwrap()
                .into_field("item".into(), DataType::Utf8, true);
        let exact_child = Arc::clone(child.field());
        let root = ArrowMetadataOwner::try_new(vec![], limits)
            .unwrap()
            .into_field(
                "root".into(),
                DataType::Struct(vec![Arc::clone(&exact_child)].into()),
                false,
            );
        let schema = Arc::new(
            ChunkSchema::try_new(vec![
                ChunkSlotSchema::try_new_with_metadata_origins(
                    SlotId::new(1),
                    Arc::clone(root.field()),
                    FieldMetadataOrigins::try_new(vec![child, root], 2).unwrap(),
                    None,
                    None,
                )
                .unwrap(),
            ])
            .unwrap(),
        );
        let foreign = Arc::new(Field::new("item", DataType::Utf8, true));
        let values = Arc::new(StructArray::new(
            vec![Arc::clone(&foreign)].into(),
            vec![Arc::new(StringArray::from(vec![Some("{}")]))],
            None,
        )) as ArrayRef;
        let chunk = Chunk::try_new_with_columns(schema, vec![values]).unwrap();
        let DataType::Struct(actual_fields) = chunk.columns()[0].data_type() else {
            panic!("struct")
        };
        assert!(Arc::ptr_eq(&exact_child, &actual_fields[0]));
        assert!(!Arc::ptr_eq(&foreign, &actual_fields[0]));
        let origins = chunk.chunk_schema().field_metadata_origins().unwrap();
        assert!(origins.metadata_bytes_for(&actual_fields[0]).is_some());
        assert!(origins.metadata_bytes_for(&foreign).is_none());
        assert!(Arc::ptr_eq(
            &chunk.schema(),
            &chunk.chunk_schema().arrow_schema_ref()
        ));
    }

    #[test]
    fn static_layout_thaw_narrows_each_slot_and_keeps_actual_schema_owner() {
        use novarocks_local_program::{StaticFieldSchema, StaticLayout};
        // A whole-layout index on every slot would produce 66,049 entries and
        // reject this valid 257-field layout when the schema is reconstructed.
        let source = ChunkSchema::try_new((0..257).map(owned_slot).collect()).unwrap();
        let layout = StaticLayout::try_new_exact(
            source.arrow_schema_ref(),
            Arc::from(source.slot_ids()),
            source
                .slots()
                .iter()
                .map(|slot| {
                    (
                        StaticFieldSchema::new(Some(LogicalType::Json), vec![]),
                        slot.unique_id(),
                    )
                })
                .collect(),
        )
        .unwrap()
        .with_metadata_origins(
            source.field_metadata_origins().unwrap().clone(),
            source.schema_metadata_origin().cloned(),
        )
        .unwrap();
        let restored = ChunkSchema::from_static_layout(&layout).unwrap();
        assert_eq!(source, *restored);
        assert!(Arc::ptr_eq(
            &source.arrow_schema_ref(),
            &restored.arrow_schema_ref()
        ));
        assert_eq!(
            restored.field_metadata_origins().unwrap().owners().len(),
            257
        );
        for (old, new) in source.slots().iter().zip(restored.slots()) {
            assert!(Arc::ptr_eq(old.field_ref(), new.field_ref()));
            assert_eq!(new.metadata_origins().unwrap().owners().len(), 1);
        }
        let projected = layout
            .project_by_slots(&[SlotId::new(19), SlotId::new(3)])
            .unwrap();
        let restored = ChunkSchema::from_static_layout(&projected).unwrap();
        assert_eq!(restored.slot_ids(), &[SlotId::new(19), SlotId::new(3)]);
        assert!(Arc::ptr_eq(
            source.slots()[19].field_ref(),
            restored.slots()[0].field_ref()
        ));
        assert_eq!(restored.field_metadata_origins().unwrap().owners().len(), 2);
        assert!(
            restored
                .schema_metadata_origin()
                .unwrap()
                .backing_bytes_for(&restored.arrow_schema_ref())
                .is_some()
        );
    }

    #[test]
    fn strict_rejects_duplicate_slot_id() {
        let err = ChunkSchema::try_new(vec![
            ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                Field::new("a", DataType::Int32, true),
                None,
                None,
            ),
            ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                Field::new("b", DataType::Int32, true),
                None,
                None,
            ),
        ])
        .expect_err("duplicate slot ids should fail");
        assert!(err.contains("duplicate slot id"), "err={}", err);
    }

    #[test]
    fn schema_and_field_metadata_survive_slot_binding() {
        let schema = Schema::new_with_metadata(
            vec![
                Field::new("value", DataType::Int32, false)
                    .with_metadata(HashMap::from([("field".to_string(), "exact".to_string())])),
            ],
            HashMap::from([("schema".to_string(), "exact".to_string())]),
        );
        let chunk = ChunkSchema::try_ref_from_schema_and_slot_ids(&schema, &[SlotId::new(7)])
            .expect("bind schema");
        assert_eq!(chunk.arrow_schema_ref().as_ref(), &schema);
    }

    #[test]
    fn chunk_schema_recovers_logical_metadata_and_unique_id() {
        let hll_field =
            field_with_logical_type(Field::new("a", DataType::Binary, true), LogicalType::Hll);
        let schema = Arc::new(Schema::new(vec![hll_field.clone()]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(BinaryArray::from(vec![Some(b"x".as_slice())]))],
        )
        .expect("record batch");
        let chunk = Chunk::try_new_with_chunk_schema(
            batch,
            Arc::new(
                ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                    SlotId::new(7),
                    hll_field,
                    None,
                    Some(77),
                )])
                .expect("chunk schema"),
            ),
        )
        .expect("chunk");
        let slot = chunk
            .chunk_schema()
            .slot(SlotId::new(7))
            .expect("slot schema");
        assert_eq!(slot.data_type(), &DataType::Binary);
        assert_eq!(slot.field_schema().logical_type(), Some(LogicalType::Hll));
        assert_eq!(logical_type_of_field(slot.field()), Some(LogicalType::Hll));
        assert_eq!(slot.name(), "a");
        assert_eq!(slot.unique_id(), Some(77));
    }

    #[test]
    fn align_chunk_schema_preserves_logical_metadata_when_widening_nullable() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "payload",
                DataType::Utf8,
                true,
            )])),
            vec![Arc::new(StringArray::from(vec![Some(r#"{"a":1}"#)])) as ArrayRef],
        )
        .expect("record batch");
        let contract = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(9),
                field_with_logical_type(
                    Field::new("payload", DataType::Utf8, false),
                    LogicalType::Json,
                ),
                None,
                None,
            )])
            .expect("chunk schema"),
        );

        let chunk = Chunk::try_new_with_chunk_schema(batch, contract).expect("chunk");
        let slot = &chunk.chunk_schema().slots()[0];

        assert!(slot.nullable());
        assert_eq!(logical_type_of_field(slot.field()), Some(LogicalType::Json));
    }

    #[test]
    fn reconcile_chunk_field_to_data_type_preserves_logical_metadata_when_widening_nullable() {
        let expected = field_with_logical_type(
            Field::new("payload", DataType::Binary, false),
            LogicalType::Hll,
        );

        let reconciled =
            super::reconcile_chunk_field_to_data_type(&expected, &DataType::Binary, true)
                .expect("reconcile field");

        assert!(reconciled.is_nullable());
        assert_eq!(
            logical_type_of_field(reconciled.as_ref()),
            Some(LogicalType::Hll)
        );
    }

    #[test]
    fn try_new_with_chunk_schema_preserves_zero_column_row_count() {
        let options = arrow::array::RecordBatchOptions::new().with_row_count(Some(3));
        let batch = RecordBatch::try_new_with_options(Arc::new(Schema::empty()), vec![], &options)
            .expect("zero-column record batch");
        let chunk_schema = Arc::new(ChunkSchema::try_new(vec![]).expect("chunk schema"));

        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk");

        assert_eq!(chunk.batch.num_columns(), 0);
        assert_eq!(chunk.batch.num_rows(), 3);
    }

    #[test]
    fn try_new_with_chunk_schema_reuses_exact_schema_batch_arrays() {
        let column = Arc::new(Int32Array::from(vec![1_i32, 2, 3])) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![Field::new("c1", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::clone(&column)]).expect("batch");
        let chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                Field::new("c1", DataType::Int32, false),
                None,
                None,
            )])
            .expect("chunk schema"),
        );

        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk");

        assert!(Arc::ptr_eq(&chunk.columns()[0], &column));
    }

    #[test]
    fn align_chunk_schema_accepts_non_nullable_batch_for_nullable_contract() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("c13", DataType::Int8, false)])),
            vec![Arc::new(Int8Array::from(vec![1_i8, 2, 3])) as ArrayRef],
        )
        .expect("record batch");
        let contract = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(13),
                Field::new("c13", DataType::Int8, true),
                None,
                None,
            )])
            .expect("chunk schema"),
        );

        let chunk = Chunk::try_new_with_chunk_schema(batch, contract).expect("chunk");

        assert!(
            chunk.chunk_schema().slots()[0].nullable(),
            "aligned chunk schema should keep the descriptor nullability contract"
        );
    }

    #[test]
    fn align_chunk_schema_to_columns_widens_runtime_map_key_nullability() {
        let expected_map = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("key", DataType::Int32, false)),
                        Arc::new(Field::new("value", DataType::Int64, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        let schema = ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
            SlotId::new(1),
            Field::new("m", expected_map, false),
            None,
            None,
        )])
        .expect("chunk schema");

        let key_array = Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef;
        let value_array = Arc::new(Int64Array::from(vec![Some(10), Some(20)])) as ArrayRef;
        let entries = StructArray::new(
            Fields::from(vec![
                Arc::new(Field::new("key", DataType::Int32, true)),
                Arc::new(Field::new("value", DataType::Int64, true)),
            ]),
            vec![key_array, value_array],
            None,
        );
        let map = Arc::new(MapArray::new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::new(vec![0, 2].into()),
            entries,
            None,
            false,
        )) as ArrayRef;

        let aligned = super::align_chunk_schema_to_columns(&[map], &schema).expect("align schema");
        let DataType::Map(entries_field, _) = aligned.slots()[0].data_type() else {
            panic!("expected map type");
        };
        let DataType::Struct(entry_fields) = entries_field.data_type() else {
            panic!("expected entry struct");
        };
        assert!(entry_fields[0].is_nullable(), "runtime map key must widen");
    }

    #[test]
    fn align_chunk_schema_to_columns_rejects_utf8_binary_type_drift() {
        let schema = ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
            SlotId::new(9),
            Field::new("payload", DataType::Binary, true),
            None,
            None,
        )])
        .expect("chunk schema");
        let column = Arc::new(arrow::array::StringArray::from(vec![Some("abc")])) as ArrayRef;

        let err = super::align_chunk_schema_to_columns(&[column], &schema)
            .expect_err("runtime schema must reject Utf8/Binary descriptor drift");
        assert!(err.contains("chunk schema type mismatch"), "err={err}");
        assert!(err.contains("slot 9 (payload)"), "err={err}");
    }

    #[test]
    fn try_new_with_columns_rejects_utf8_binary_type_drift() {
        let chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(9),
                Field::new("payload", DataType::Binary, true),
                None,
                None,
            )])
            .expect("chunk schema"),
        );
        let column = Arc::new(StringArray::from(vec![Some("abc"), Some("xyz")])) as ArrayRef;

        let err = Chunk::try_new_with_columns(chunk_schema, vec![column])
            .expect_err("runtime schema must reject Utf8/Binary descriptor drift");
        assert!(err.contains("chunk schema type mismatch"), "err={err}");
        assert!(err.contains("slot 9 (payload)"), "err={err}");
    }

    #[test]
    fn try_new_with_chunk_schema_rejects_utf8_binary_type_drift() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "payload",
                DataType::Utf8,
                true,
            )])),
            vec![Arc::new(StringArray::from(vec![Some("abc"), Some("xyz")]))],
        )
        .expect("record batch");
        let chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(9),
                Field::new("payload", DataType::Binary, true),
                None,
                None,
            )])
            .expect("chunk schema"),
        );

        let err = Chunk::try_new_with_chunk_schema(batch, chunk_schema)
            .expect_err("runtime schema must reject Utf8/Binary descriptor drift");

        assert!(err.contains("chunk schema type mismatch"), "err={err}");
        assert!(err.contains("slot 9 (payload)"), "err={err}");
        assert!(err.contains("Binary"), "err={err}");
        assert!(err.contains("Utf8"), "err={err}");
    }

    #[test]
    fn try_new_with_chunk_schema_rejects_same_scale_decimal_precision_drift() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "price",
                DataType::Decimal128(10, 2),
                true,
            )])),
            vec![Arc::new(
                Decimal128Array::from(vec![Some(1234_i128)])
                    .with_precision_and_scale(10, 2)
                    .expect("decimal array"),
            ) as ArrayRef],
        )
        .expect("record batch");
        let chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(11),
                Field::new("price", DataType::Decimal128(38, 2), true),
                None,
                None,
            )])
            .expect("chunk schema"),
        );

        let err = Chunk::try_new_with_chunk_schema(batch, chunk_schema)
            .expect_err("runtime schema must reject decimal precision drift");

        assert!(err.contains("chunk schema type mismatch"), "err={err}");
        assert!(err.contains("slot 11 (price)"), "err={err}");
        assert!(err.contains("Decimal128(38, 2)"), "err={err}");
        assert!(err.contains("Decimal128(10, 2)"), "err={err}");
    }

    #[test]
    fn try_new_with_chunk_schema_rejects_timestamp_metadata_retag() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            )])),
            vec![Arc::new(TimestampMicrosecondArray::from(vec![
                Some(1_000_i64),
                Some(2_000),
            ]))],
        )
        .expect("record batch");
        let chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(10),
                Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
                None,
                None,
            )])
            .expect("chunk schema"),
        );

        let err = Chunk::try_new_with_chunk_schema(batch, chunk_schema)
            .expect_err("timestamp metadata retag should fail");

        assert!(err.contains("slot 10 (ts)"), "err={err}");
        assert!(err.contains("Timestamp(Nanosecond, None)"), "err={err}");
    }
}
