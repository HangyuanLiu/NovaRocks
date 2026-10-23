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

use super::construction_failure::{
    ChunkConstructionFailure, ChunkObservationFailure, ChunkViolation,
};
use novarocks_functions::array_projection::{ArrayProjectionFailure, ArrayProjectionOperation};
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use novarocks_types::SlotId;

#[cfg(test)]
use super::typed_array_normalization::rebuild_array_mode;
use super::typed_array_normalization::{
    ArrayNormalizationStep, ArrayPathStep, rebuild_array_mode_with_decoded, rebuild_field,
};
use super::{Chunk, ChunkSchema, ChunkSlotSchema};
use arrow::datatypes::{FieldRef, UnionFields};
use novarocks_type_contract::ValueTypeStorageLifetime;
use std::borrow::Cow;

#[path = "hydrated_required_strings.rs"]
mod hydrated_required_strings;

fn contains_dictionary(source: &DataType) -> bool {
    match source {
        DataType::Dictionary(_, _) => true,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => contains_dictionary(field.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| contains_dictionary(field.data_type())),
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, field)| contains_dictionary(field.data_type())),
        DataType::RunEndEncoded(_, values) => contains_dictionary(values.data_type()),
        _ => false,
    }
}

/// Borrowed demand probe. Every actual encoded child is queried independently;
/// no target tree, input-construction scope or heap path storage is created.
pub fn dictionary_hydration_required_observed<E, K, W>(
    chunk: &Chunk,
    mut keep: K,
    mut work: W,
) -> Result<bool, E>
where
    E: From<ChunkConstructionFailure>,
    K: FnMut(SlotId, usize, &[ArrayPathStep], &DataType) -> bool,
    W: FnMut(usize, &[ArrayPathStep], &DataType) -> Result<(), E>,
{
    if !chunk.chunk_schema().is_typed() {
        return Err(E::from(ChunkConstructionFailure::source(
            ChunkViolation::HydrationBinding,
        )));
    }
    for (index, (slot, array)) in chunk
        .chunk_schema()
        .slots()
        .iter()
        .zip(chunk.columns())
        .enumerate()
    {
        if dictionary_demand(
            array.data_type(),
            slot.slot_id(),
            index,
            &[],
            &mut keep,
            &mut work,
            &mut 0,
        )? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn dictionary_demand<E, K, W>(
    source: &DataType,
    slot: SlotId,
    occurrence: usize,
    path: &[ArrayPathStep],
    keep: &mut K,
    work: &mut W,
    nodes: &mut usize,
) -> Result<bool, E>
where
    E: From<ChunkConstructionFailure>,
    K: FnMut(SlotId, usize, &[ArrayPathStep], &DataType) -> bool,
    W: FnMut(usize, &[ArrayPathStep], &DataType) -> Result<(), E>,
{
    work(occurrence, path, source)?;
    *nodes = nodes.checked_add(1).ok_or_else(|| {
        E::from(ChunkConstructionFailure::from(
            novarocks_type_contract::ValueTypeError::TooManyNodes,
        ))
    })?;
    if *nodes > novarocks_type_contract::MAX_VALUE_TYPE_NODES {
        return Err(E::from(ChunkConstructionFailure::from(
            novarocks_type_contract::ValueTypeError::TooManyNodes,
        )));
    }
    if path.len() >= novarocks_type_contract::MAX_VALUE_TYPE_DEPTH {
        return Err(E::from(ChunkConstructionFailure::from(
            novarocks_type_contract::ValueTypeError::TooDeep,
        )));
    }
    if matches!(source, DataType::Dictionary(_, _)) && !keep(slot, occurrence, path, source) {
        return Ok(true);
    }
    let mut next = [ArrayPathStep::ListItem; novarocks_type_contract::MAX_VALUE_TYPE_DEPTH];
    next[..path.len()].copy_from_slice(path);
    let mut child = |ty: &DataType, step: ArrayPathStep| {
        next[path.len()] = step;
        dictionary_demand(
            ty,
            slot,
            occurrence,
            &next[..path.len() + 1],
            keep,
            work,
            nodes,
        )
    };
    match source {
        DataType::Dictionary(_, values) => child(values, ArrayPathStep::DictionaryValue),
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => child(field.data_type(), ArrayPathStep::ListItem),
        DataType::Struct(fields) => {
            for (index, field) in fields.iter().enumerate() {
                if child(field.data_type(), ArrayPathStep::StructField(index))? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(E::from(ChunkConstructionFailure::source(
                    ChunkViolation::HydrationMapEntries,
                )));
            };
            if fields.len() != 2 {
                return Err(E::from(ChunkConstructionFailure::source(
                    ChunkViolation::HydrationMapArity,
                )));
            }
            Ok(child(fields[0].data_type(), ArrayPathStep::MapKey)?
                || child(fields[1].data_type(), ArrayPathStep::MapValue)?)
        }
        DataType::Union(fields, _) => {
            for (id, field) in fields.iter() {
                if child(field.data_type(), ArrayPathStep::UnionField(id))? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        DataType::RunEndEncoded(_, values) => child(values.data_type(), ArrayPathStep::RunEndValue),
        _ => Ok(false),
    }
}

/// Explicit flat-boundary hydration. Encoding acceptance is per actual nested
/// occurrence; accepting a parent never grants acceptance to its descendants.
/// The observer covers decoded buffers and reconstruction before allocation.
pub fn hydrate_dictionary_columns_observed<E, K, F>(
    chunk: &Chunk,
    storage: ValueTypeStorageLifetime,
    keep_encoded: K,
    observe: F,
) -> Result<Chunk, E>
where
    E: From<ChunkConstructionFailure>,
    K: FnMut(SlotId, usize, &[ArrayPathStep], &DataType) -> bool,
    F: for<'a> FnMut(usize, &[ArrayPathStep], ArrayNormalizationStep<'a>) -> Result<(), E>,
{
    // Expansion/test adapter. Runtime callers use the explicit scaffold request
    // port below rather than treating this no-op as resource admission.
    hydrate_dictionary_columns_owned_observed(chunk, storage, keep_encoded, observe, |_| Ok(()))
}

/// Runtime recursive hydration under the actual input-construction owner.
/// Requests cover new scaffolds and the holder for any reused backing before
/// allocation; the representation observer covers decode/rebuild operations.
pub fn hydrate_dictionary_columns_owned_observed<E, K, F, R>(
    chunk: &Chunk,
    storage: ValueTypeStorageLifetime,
    mut keep_encoded: K,
    mut observe: F,
    mut request: R,
) -> Result<Chunk, E>
where
    E: From<ChunkConstructionFailure>,
    K: FnMut(SlotId, usize, &[ArrayPathStep], &DataType) -> bool,
    F: for<'a> FnMut(usize, &[ArrayPathStep], ArrayNormalizationStep<'a>) -> Result<(), E>,
    R: FnMut(std::alloc::Layout) -> Result<(), E>,
{
    if !chunk.chunk_schema().is_typed() {
        return Err(E::from(ChunkConstructionFailure::source(
            ChunkViolation::HydrationBinding,
        )));
    }
    if !dictionary_hydration_required_observed(chunk, &mut keep_encoded, |index, path, source| {
        observe(index, path, ArrayNormalizationStep::InspectType(source))
    })? {
        return Ok(chunk.clone());
    }
    observe(
        0,
        &[],
        ArrayNormalizationStep::RebuildChunkScaffolds {
            channels: chunk.columns().len(),
        },
    )?;
    let storage = super::schema::retain_storage_observed(
        storage,
        &chunk.array_type_storage_lifetime(),
        &mut request,
    )?;
    let channels = chunk.columns().len();
    if channels != 0 {
        request(
            std::alloc::Layout::array::<ArrayRef>(channels).map_err(|source| {
                E::from(ChunkConstructionFailure::array_request::<ArrayRef>(
                    channels, source,
                ))
            })?,
        )?;
    }
    let mut columns = Vec::with_capacity(chunk.columns().len());
    if channels != 0 {
        request(
            std::alloc::Layout::array::<ChunkSlotSchema>(channels).map_err(|source| {
                E::from(ChunkConstructionFailure::array_request::<ChunkSlotSchema>(
                    channels, source,
                ))
            })?,
        )?;
    }
    let mut slots = Vec::with_capacity(chunk.columns().len());
    let mut changed = false;
    for (index, (column, slot)) in chunk
        .columns()
        .iter()
        .zip(chunk.chunk_schema().slots())
        .enumerate()
    {
        let target = hydration_target(
            column.data_type(),
            slot.slot_id(),
            index,
            &[],
            &mut keep_encoded,
            &mut observe,
        )?;
        if matches!(target, Cow::Borrowed(_)) {
            columns.push(column.clone());
            slots.push(slot.clone());
            continue;
        }
        changed = true;
        let field = rebuild_field(slot.field(), target.as_ref(), slot.name(), &mut |step| {
            observe(index, &[], step).map_err(ChunkObservationFailure::Caller)
        })
        .map_err(ChunkObservationFailure::into_caller)?;
        let slot = slot
            .with_field_typed(field, storage.clone())
            .map_err(E::from)?;
        let column = rebuild_array_mode_with_decoded(
            column,
            target.as_ref(),
            &mut |step| {
                let path = match step {
                    ArrayNormalizationStep::RebuildArray { path, .. }
                    | ArrayNormalizationStep::DecodeDictionary { path, .. }
                    | ArrayNormalizationStep::ValidateStringWrapper { path, .. } => path,
                    _ => &[],
                };
                observe(index, path, step).map_err(ChunkObservationFailure::Caller)
            },
            true,
            &mut |source, path, decoded, observe| {
                // Immutable typed ingress already proved every required
                // dictionary leaf on ancestor-active visible rows. No legacy
                // carrier or raw child extraction grants that proof.
                hydrated_required_strings::normalize_inactive_nulls(
                    slot.typed_binding().value_type(),
                    source,
                    path,
                    decoded,
                    observe,
                )
            },
        )
        .map_err(ChunkObservationFailure::into_caller)?;
        columns.push(column);
        slots.push(slot);
    }
    if !changed {
        return Ok(chunk.clone());
    }
    let schema =
        ChunkSchema::try_new_typed_observed(slots, storage.clone(), &mut request, |units| {
            observe(
                0,
                &[],
                ArrayNormalizationStep::InspectConstructionWork { units },
            )
        })?;
    super::schema::request_arc_payload(std::alloc::Layout::new::<ChunkSchema>(), &mut request)?;
    let schema = Arc::new(schema);
    let mut result = Chunk::try_new_typed_with_columns_owned_observed(
        schema,
        columns,
        chunk.len(),
        storage,
        |step| observe(0, &[], step),
    )?;
    result.retain_source_buffer_funding(chunk);
    Ok(result)
}

fn hydration_target<'a, E, K, F>(
    source: &'a DataType,
    slot: SlotId,
    occurrence: usize,
    path: &[ArrayPathStep],
    keep: &mut K,
    observe: &mut F,
) -> Result<Cow<'a, DataType>, E>
where
    E: From<ChunkConstructionFailure>,
    K: FnMut(SlotId, usize, &[ArrayPathStep], &DataType) -> bool,
    F: for<'b> FnMut(usize, &[ArrayPathStep], ArrayNormalizationStep<'b>) -> Result<(), E>,
{
    observe(
        occurrence,
        path,
        ArrayNormalizationStep::InspectType(source),
    )?;
    if !contains_dictionary(source) {
        return Ok(Cow::Borrowed(source));
    }
    if path.len() >= novarocks_type_contract::MAX_VALUE_TYPE_DEPTH {
        return Err(E::from(ChunkConstructionFailure::from(
            novarocks_type_contract::ValueTypeError::TooDeep,
        )));
    }
    let mut next = [ArrayPathStep::ListItem; novarocks_type_contract::MAX_VALUE_TYPE_DEPTH];
    next[..path.len()].copy_from_slice(path);
    if let DataType::Dictionary(keys, values) = source {
        let accepted = keep(slot, occurrence, path, source);
        next[path.len()] = ArrayPathStep::DictionaryValue;
        let target = hydration_target(
            values,
            slot,
            occurrence,
            &next[..path.len() + 1],
            keep,
            observe,
        )?;
        if accepted && matches!(target, Cow::Borrowed(_)) {
            return Ok(Cow::Borrowed(source));
        }
        observe(
            occurrence,
            path,
            ArrayNormalizationStep::RebuildType { source },
        )?;
        return Ok(Cow::Owned(if accepted {
            DataType::Dictionary(keys.clone(), Box::new(target.into_owned()))
        } else {
            target.into_owned()
        }));
    }
    observe(
        occurrence,
        path,
        ArrayNormalizationStep::RebuildType { source },
    )?;
    let mut field_target = |field: &FieldRef, child: ArrayPathStep| -> Result<FieldRef, E> {
        next[path.len()] = child;
        let child_path = &next[..path.len() + 1];
        let target = hydration_target(
            field.data_type(),
            slot,
            occurrence,
            child_path,
            keep,
            observe,
        )?;
        if matches!(target, Cow::Borrowed(_)) {
            return Ok(field.clone());
        }
        rebuild_field(field, target.as_ref(), field.name(), &mut |step| {
            observe(occurrence, child_path, step).map_err(ChunkObservationFailure::Caller)
        })
        .map_err(ChunkObservationFailure::into_caller)
    };
    let target = match source {
        DataType::List(field) => DataType::List(field_target(field, ArrayPathStep::ListItem)?),
        DataType::LargeList(field) => {
            DataType::LargeList(field_target(field, ArrayPathStep::ListItem)?)
        }
        DataType::ListView(field) => {
            DataType::ListView(field_target(field, ArrayPathStep::ListItem)?)
        }
        DataType::LargeListView(field) => {
            DataType::LargeListView(field_target(field, ArrayPathStep::ListItem)?)
        }
        DataType::FixedSizeList(field, length) => {
            DataType::FixedSizeList(field_target(field, ArrayPathStep::ListItem)?, *length)
        }
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .enumerate()
                .map(|(index, field)| field_target(field, ArrayPathStep::StructField(index)))
                .collect::<Result<Vec<_>, _>>()?
                .into(),
        ),
        DataType::Map(entries, ordered) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(E::from(ChunkConstructionFailure::source(
                    ChunkViolation::HydrationMapEntries,
                )));
            };
            if fields.len() != 2 {
                return Err(E::from(ChunkConstructionFailure::source(
                    ChunkViolation::HydrationMapArity,
                )));
            }
            let target = DataType::Struct(
                vec![
                    field_target(&fields[0], ArrayPathStep::MapKey)?,
                    field_target(&fields[1], ArrayPathStep::MapValue)?,
                ]
                .into(),
            );
            let entries = rebuild_field(entries, &target, entries.name(), &mut |step| {
                observe(occurrence, path, step).map_err(ChunkObservationFailure::Caller)
            })
            .map_err(ChunkObservationFailure::into_caller)?;
            DataType::Map(entries, *ordered)
        }
        DataType::Union(fields, mode) => DataType::Union(
            UnionFields::try_new(
                fields.iter().map(|(id, _)| id).collect::<Vec<_>>(),
                fields
                    .iter()
                    .map(|(id, field)| field_target(field, ArrayPathStep::UnionField(id)))
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .map_err(|error| {
                E::from(ChunkConstructionFailure::from(
                    ArrayProjectionFailure::Arrow {
                        operation: ArrayProjectionOperation::RebuildUnionFields,
                        source: error,
                    },
                ))
            })?,
            *mode,
        ),
        DataType::RunEndEncoded(ends, values) => DataType::RunEndEncoded(
            ends.clone(),
            field_target(values, ArrayPathStep::RunEndValue)?,
        ),
        _ => {
            return Err(E::from(ChunkConstructionFailure::source(
                ChunkViolation::HydrationContainer,
            )));
        }
    };
    if &target == source {
        Ok(Cow::Borrowed(source))
    } else {
        Ok(Cow::Owned(target))
    }
}

#[allow(
    dead_code,
    reason = "Retained as a focused invariant helper for dictionary hydration callers."
)]
pub(crate) fn assert_no_dictionary(batch: &RecordBatch) -> Result<(), ChunkConstructionFailure> {
    for (index, field) in batch.schema().fields().iter().enumerate() {
        if contains_dictionary(field.data_type())
            || batch
                .columns()
                .get(index)
                .is_some_and(|column| contains_dictionary(column.data_type()))
        {
            return Err(ChunkConstructionFailure::source(
                ChunkViolation::FlatDictionary {
                    column: index,
                    field: novarocks_failure_contract::BoundedDetail::truncating_display(
                        field.name(),
                    ),
                },
            ));
        }
    }
    Ok(())
}

// Design: ADR-0005 (docs/adr/ADR-0005-low-cardinality-runtime-carrier-first.md)
#[allow(
    dead_code,
    reason = "Retained for connector paths that require explicit dictionary hydration."
)]
pub(crate) fn hydrate_dictionary_columns(chunk: &Chunk) -> Result<Chunk, ChunkConstructionFailure> {
    hydrate_dictionary_columns_except(chunk, |_, _| false)
}

pub fn hydrate_dictionary_columns_except(
    chunk: &Chunk,
    keep_encoded: impl Fn(SlotId, &DataType) -> bool,
) -> Result<Chunk, ChunkConstructionFailure> {
    let mut changed = false;
    let mut columns = Vec::with_capacity(chunk.columns().len());
    let mut slots = Vec::with_capacity(chunk.chunk_schema().slots().len());

    for ((column, field), slot) in chunk
        .columns()
        .iter()
        .zip(chunk.schema().fields().iter())
        .zip(chunk.chunk_schema().slots().iter())
    {
        let column_type = column.data_type();
        match column_type {
            DataType::Dictionary(_, value_type) if !keep_encoded(slot.slot_id(), column_type) => {
                changed = true;
                let value_type = value_type.as_ref().clone();
                let flat_slot =
                    slot.with_type_and_nullable(value_type.clone(), field.is_nullable())?;
                let flat = cast(column.as_ref(), &value_type).map_err(|source| {
                    ChunkConstructionFailure::from(ArrayProjectionFailure::Arrow {
                        operation: ArrayProjectionOperation::DecodeDictionary,
                        source,
                    })
                })?;
                columns.push(flat);
                slots.push(flat_slot);
            }
            _ => {
                columns.push(Arc::clone(column));
                slots.push(slot.clone());
            }
        }
    }

    if !changed {
        return Ok(chunk.clone());
    }

    let actual_schema = chunk.schema();
    let chunk_schema = Arc::new(
        if let Some(source) = chunk
            .chunk_schema()
            .schema_metadata_origin()
            .filter(|origin| origin.backing_bytes_for(&actual_schema).is_some())
        {
            ChunkSchema::try_new_with_derived_schema_metadata(slots, source)?
        } else {
            // Preserve the existing unknown-origin path without issuing provenance
            // merely because its metadata values equal a declared schema.
            ChunkSchema::try_new_with_schema_metadata(slots, actual_schema.metadata().clone())?
        },
    );
    let batch =
        RecordBatch::try_new(chunk_schema.arrow_schema_ref(), columns).map_err(|source| {
            ChunkConstructionFailure::source(super::construction_failure::ChunkRecordBatchFailure(
                source,
            ))
        })?;
    Chunk::try_new_with_chunk_schema(batch, chunk_schema)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, DictionaryArray, LargeStringArray, LargeStringDictionaryBuilder,
        StringArray,
    };
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use arrow::record_batch::RecordBatch;

    use super::{
        assert_no_dictionary, hydrate_dictionary_columns, hydrate_dictionary_columns_except,
    };
    use crate::exec::chunk::{Chunk, ChunkFieldSchema, ChunkSchema, ChunkSlotSchema};
    use novarocks_types::SlotId;
    use novarocks_types::logical::{LogicalType, field_with_logical_type};

    fn dict_utf8_with_nulls_and_empty() -> ArrayRef {
        Arc::new(
            vec![Some("PAID"), None, Some(""), Some("NEW")]
                .into_iter()
                .collect::<DictionaryArray<Int32Type>>(),
        )
    }

    fn dict_large_utf8_with_nulls_and_empty() -> ArrayRef {
        let mut builder = LargeStringDictionaryBuilder::<Int32Type>::new();
        builder.append_value("PAID");
        builder.append_null();
        builder.append_value("");
        builder.append_value("NEW");
        Arc::new(builder.finish())
    }

    fn chunk_with_column(slot_id: SlotId, field: Field, column: ArrayRef) -> Chunk {
        chunk_with_slot(
            ChunkSlotSchema::new_with_field(slot_id, field, None, None),
            column,
        )
    }

    fn chunk_with_slot(slot: ChunkSlotSchema, column: ArrayRef) -> Chunk {
        let schema = Arc::new(ChunkSchema::try_new(vec![slot]).expect("chunk schema"));
        Chunk::try_new_with_columns(schema, vec![column]).expect("chunk")
    }

    fn json_field_schema() -> ChunkFieldSchema {
        ChunkFieldSchema::from_field(&field_with_logical_type(
            Field::new("logical_payload", DataType::Utf8, true),
            LogicalType::Json,
        ))
        .expect("logical field schema")
    }

    #[test]
    fn hydration_derives_exact_known_field_and_schema_metadata_owners() {
        use novarocks_types::arrow_metadata_owner::{
            ArrowMetadataOwner, FieldMetadataOrigins, MetadataOwnerLimits,
        };
        let limits = MetadataOwnerLimits {
            entries: 1,
            construction_bytes: 4096,
        };
        let field =
            ArrowMetadataOwner::try_new(vec![("nr_logical_type".into(), "json".into())], limits)
                .unwrap()
                .into_field("payload".into(), DataType::Utf8, true);
        let slot = ChunkSlotSchema::try_new_with_metadata_origins(
            SlotId::new(1),
            Arc::clone(field.field()),
            FieldMetadataOrigins::try_new(vec![field], 1).unwrap(),
            None,
            Some(7),
        )
        .unwrap();
        let top =
            ArrowMetadataOwner::try_new(vec![("schema".into(), "source".into())], limits).unwrap();
        let source = Chunk::try_new_with_columns(
            Arc::new(ChunkSchema::try_new_with_owned_schema_metadata(vec![slot], top).unwrap()),
            vec![dict_utf8_with_nulls_and_empty()],
        )
        .unwrap();
        let source_slot = &source.chunk_schema().slots()[0];
        let flat = hydrate_dictionary_columns(&source).unwrap();
        let flat_slot = &flat.chunk_schema().slots()[0];
        assert!(!Arc::ptr_eq(source_slot.field_ref(), flat_slot.field_ref()));
        assert_eq!(source_slot.field().metadata(), flat_slot.field().metadata());
        assert_eq!(source_slot.unique_id(), flat_slot.unique_id());
        assert_eq!(source_slot.field_schema(), flat_slot.field_schema());
        assert!(
            flat_slot
                .metadata_origins()
                .unwrap()
                .metadata_bytes_for(flat_slot.field_ref())
                .is_some()
        );
        assert!(
            flat_slot
                .metadata_origins()
                .unwrap()
                .metadata_bytes_for(source_slot.field_ref())
                .is_none()
        );
        assert_eq!(flat.schema().metadata(), source.schema().metadata());
        assert!(
            flat.chunk_schema()
                .schema_metadata_origin()
                .unwrap()
                .backing_bytes_for(&flat.schema())
                .is_some()
        );
        assert!(
            source
                .chunk_schema()
                .schema_metadata_origin()
                .unwrap()
                .backing_bytes_for(&flat.schema())
                .is_none()
        );
        // The same metadata values in an independently allocated schema are
        // not evidence for its map backing, including deleted-bucket spare.
        let mut unknown = std::collections::HashMap::with_capacity(8192);
        unknown.insert("schema".into(), "source".into());
        let foreign = Arc::new(Schema::new_with_metadata(
            source.schema().fields().clone(),
            unknown,
        ));
        let batch = RecordBatch::try_new(foreign, source.columns().to_vec()).unwrap();
        let foreign_chunk =
            Chunk::try_new_with_chunk_schema(batch, source.chunk_schema_ref()).unwrap();
        let flat = hydrate_dictionary_columns(&foreign_chunk).unwrap();
        assert!(flat.chunk_schema().schema_metadata_origin().is_none());
        assert!(flat.chunk_schema().field_metadata_origins().is_some());
    }

    #[test]
    fn hydrate_dictionary_columns_flattens_utf8_dictionary_preserving_slot_contract() {
        let slot_id = SlotId::new(7);
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("source".to_string(), "dict".to_string());
        let chunk = chunk_with_column(
            slot_id,
            Field::new("status", DataType::Utf8, false).with_metadata(metadata.clone()),
            dict_utf8_with_nulls_and_empty(),
        );

        let hydrated = hydrate_dictionary_columns(&chunk).expect("hydrate");

        assert_eq!(hydrated.columns()[0].data_type(), &DataType::Utf8);
        let values = hydrated.columns()[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string array");
        assert_eq!(values.value(0), "PAID");
        assert!(values.is_null(1));
        assert_eq!(values.value(2), "");
        assert_eq!(values.value(3), "NEW");
        let field = hydrated
            .chunk_schema()
            .field_by_slot(slot_id)
            .expect("slot field");
        assert_eq!(field.name(), "status");
        assert_eq!(field.data_type(), &DataType::Utf8);
        assert_eq!(field.metadata(), &metadata);
        assert_eq!(
            hydrated
                .chunk_schema()
                .slot(slot_id)
                .expect("slot schema")
                .slot_id(),
            slot_id
        );
    }

    #[test]
    fn hydrate_dictionary_columns_flattens_large_utf8_dictionary_with_nulls_and_empty_strings() {
        let slot_id = SlotId::new(11);
        let chunk = chunk_with_column(
            slot_id,
            Field::new("status_l", DataType::LargeUtf8, true),
            dict_large_utf8_with_nulls_and_empty(),
        );

        let hydrated = hydrate_dictionary_columns(&chunk).expect("hydrate");

        assert_eq!(hydrated.columns()[0].data_type(), &DataType::LargeUtf8);
        let values = hydrated.columns()[0]
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .expect("large string array");
        assert_eq!(values.value(0), "PAID");
        assert!(values.is_null(1));
        assert_eq!(values.value(2), "");
        assert_eq!(values.value(3), "NEW");
        assert_eq!(
            hydrated
                .chunk_schema()
                .field_by_slot(slot_id)
                .expect("slot field")
                .data_type(),
            &DataType::LargeUtf8
        );
    }

    #[test]
    fn hydrate_dictionary_columns_preserves_slot_unique_id_and_field_schema() {
        let slot_id = SlotId::new(13);
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("source".to_string(), "dict".to_string());
        let slot = ChunkSlotSchema::new_with_field(
            slot_id,
            Field::new("payload", DataType::Utf8, true).with_metadata(metadata.clone()),
            Some(json_field_schema()),
            Some(77),
        );
        let chunk = chunk_with_slot(slot, dict_utf8_with_nulls_and_empty());

        let hydrated = hydrate_dictionary_columns(&chunk).expect("hydrate");

        let hydrated_slot = hydrated
            .chunk_schema()
            .slot(slot_id)
            .expect("hydrated slot");
        assert_eq!(hydrated_slot.unique_id(), Some(77));
        assert_eq!(hydrated_slot.field().metadata(), &metadata);
        assert_eq!(hydrated_slot.data_type(), &DataType::Utf8);
        assert!(hydrated_slot.field_schema().json_semantic());
    }

    #[test]
    fn hydrate_dictionary_columns_fast_path_preserves_plain_chunk_schema() {
        let slot_id = SlotId::new(17);
        let slot = ChunkSlotSchema::new_with_field(
            slot_id,
            Field::new("payload", DataType::Utf8, true),
            Some(json_field_schema()),
            Some(77),
        );
        let chunk = chunk_with_slot(
            slot,
            Arc::new(StringArray::from(vec![Some("PAID"), None, Some("")])),
        );

        let hydrated = hydrate_dictionary_columns(&chunk).expect("hydrate");

        assert_eq!(hydrated.columns()[0].data_type(), &DataType::Utf8);
        let hydrated_slot = hydrated
            .chunk_schema()
            .slot(slot_id)
            .expect("hydrated slot");
        assert_eq!(hydrated_slot.unique_id(), Some(77));
        assert_eq!(hydrated_slot.data_type(), &DataType::Utf8);
        assert!(hydrated_slot.field_schema().json_semantic());
    }

    #[test]
    fn hydrate_dictionary_columns_except_keeps_selected_slots_and_matches_default_hydration() {
        let slot_id = SlotId::new(1);
        let chunk = chunk_with_column(
            slot_id,
            Field::new("status", DataType::Utf8, true),
            dict_utf8_with_nulls_and_empty(),
        );

        let kept = hydrate_dictionary_columns_except(&chunk, |slot, _dt| slot == SlotId::new(1))
            .expect("keep selected slot");

        assert!(matches!(
            kept.columns()[0].data_type(),
            DataType::Dictionary(_, value_type) if value_type.as_ref() == &DataType::Utf8
        ));
        assert!(matches!(
            kept.schema().fields()[0].data_type(),
            DataType::Dictionary(_, value_type) if value_type.as_ref() == &DataType::Utf8
        ));
        assert!(matches!(
            kept.chunk_schema()
                .slot(slot_id)
                .expect("kept slot")
                .data_type(),
            DataType::Dictionary(_, value_type) if value_type.as_ref() == &DataType::Utf8
        ));

        let keep_none =
            hydrate_dictionary_columns_except(&chunk, |_, _| false).expect("hydrate all");
        let default_hydrated = hydrate_dictionary_columns(&chunk).expect("default hydrate");

        assert_eq!(keep_none.columns()[0].data_type(), &DataType::Utf8);
        assert_eq!(
            keep_none.columns()[0].data_type(),
            default_hydrated.columns()[0].data_type()
        );
        let keep_none_values = keep_none.columns()[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("keep-none string array");
        let default_values = default_hydrated.columns()[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("default string array");
        assert_eq!(keep_none_values, default_values);
        assert_eq!(
            keep_none
                .chunk_schema()
                .slot(slot_id)
                .expect("keep-none slot")
                .data_type(),
            default_hydrated
                .chunk_schema()
                .slot(slot_id)
                .expect("default slot")
                .data_type()
        );
    }

    #[test]
    fn hydrate_dictionary_columns_except_keeps_one_dictionary_slot_and_hydrates_another() {
        let keep_slot_id = SlotId::new(1);
        let hydrate_slot_id = SlotId::new(2);
        let keep_column = dict_utf8_with_nulls_and_empty();
        let hydrate_column = dict_utf8_with_nulls_and_empty();
        let mut schema_metadata = std::collections::HashMap::new();
        schema_metadata.insert("source".to_string(), "mixed-dict".to_string());
        let chunk_schema = Arc::new(
            ChunkSchema::try_new_with_schema_metadata(
                vec![
                    ChunkSlotSchema::new_with_field(
                        keep_slot_id,
                        Field::new("keep_status", keep_column.data_type().clone(), true),
                        None,
                        None,
                    ),
                    ChunkSlotSchema::new_with_field(
                        hydrate_slot_id,
                        Field::new("hydrate_status", hydrate_column.data_type().clone(), true),
                        None,
                        None,
                    ),
                ],
                schema_metadata.clone(),
            )
            .expect("chunk schema"),
        );
        let batch = RecordBatch::try_new(
            chunk_schema.arrow_schema_ref(),
            vec![keep_column, hydrate_column],
        )
        .expect("record batch");
        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk");
        assert_eq!(chunk.schema().metadata(), &schema_metadata);
        assert_eq!(
            chunk.chunk_schema().arrow_schema_ref().metadata(),
            &schema_metadata
        );

        let hydrated = hydrate_dictionary_columns_except(&chunk, |slot, _dt| slot == keep_slot_id)
            .expect("hydrate except kept slot");

        assert!(matches!(
            hydrated.columns()[0].data_type(),
            DataType::Dictionary(key_type, value_type)
                if key_type.as_ref() == &DataType::Int32 && value_type.as_ref() == &DataType::Utf8
        ));
        assert!(matches!(
            hydrated.schema().fields()[0].data_type(),
            DataType::Dictionary(key_type, value_type)
                if key_type.as_ref() == &DataType::Int32 && value_type.as_ref() == &DataType::Utf8
        ));
        let kept_slot = hydrated
            .chunk_schema()
            .slot(keep_slot_id)
            .expect("kept slot schema");
        assert_eq!(kept_slot.slot_id(), keep_slot_id);
        assert!(matches!(
            kept_slot.data_type(),
            DataType::Dictionary(key_type, value_type)
                if key_type.as_ref() == &DataType::Int32 && value_type.as_ref() == &DataType::Utf8
        ));

        assert_eq!(hydrated.columns()[1].data_type(), &DataType::Utf8);
        let hydrated_values = hydrated.columns()[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("hydrated string array");
        assert_eq!(hydrated_values.value(0), "PAID");
        assert!(hydrated_values.is_null(1));
        assert_eq!(hydrated_values.value(2), "");
        assert_eq!(hydrated_values.value(3), "NEW");
        assert_eq!(hydrated.schema().fields()[1].data_type(), &DataType::Utf8);
        let hydrated_slot = hydrated
            .chunk_schema()
            .slot(hydrate_slot_id)
            .expect("hydrated slot schema");
        assert_eq!(hydrated_slot.slot_id(), hydrate_slot_id);
        assert_eq!(hydrated_slot.data_type(), &DataType::Utf8);
        assert_eq!(
            hydrated.chunk_schema().slot_ids(),
            &[keep_slot_id, hydrate_slot_id]
        );
        assert_eq!(hydrated.schema().metadata(), &schema_metadata);
        assert_eq!(
            hydrated.chunk_schema().arrow_schema_ref().metadata(),
            &schema_metadata
        );
    }

    #[test]
    fn hydrate_dictionary_columns_except_preserves_schema_metadata_when_hydrating() {
        let slot_id = SlotId::new(23);
        let column = dict_utf8_with_nulls_and_empty();
        let mut chunk = chunk_with_column(
            slot_id,
            Field::new("status", DataType::Utf8, true),
            Arc::clone(&column),
        );
        let mut schema_metadata = std::collections::HashMap::new();
        schema_metadata.insert("tenant".to_string(), "analytics".to_string());
        let batch_schema = Arc::new(Schema::new_with_metadata(
            vec![Arc::new(Field::new(
                "status",
                column.data_type().clone(),
                true,
            ))],
            schema_metadata.clone(),
        ));
        chunk.replace_record_batch_for_test(
            RecordBatch::try_new(batch_schema, vec![column]).expect("record batch"),
        );
        assert_eq!(chunk.schema().metadata(), &schema_metadata);

        let hydrated =
            hydrate_dictionary_columns_except(&chunk, |_, _| false).expect("hydrate all");

        assert_eq!(hydrated.schema().metadata(), &schema_metadata);
        assert_eq!(
            hydrated.chunk_schema().arrow_schema_ref().metadata(),
            &schema_metadata
        );
    }

    #[test]
    fn assert_no_dictionary_allows_flat_utf8_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "status",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![Some("PAID"), None]))],
        )
        .expect("record batch");

        assert_no_dictionary(&batch).expect("flat batch should pass");
    }

    #[test]
    fn assert_no_dictionary_rejects_dictionary_batch_with_field_name() {
        let column = dict_utf8_with_nulls_and_empty();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "status",
            column.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![column]).expect("record batch");

        let err = assert_no_dictionary(&batch).expect_err("dictionary batch should fail");

        assert!(
            err.to_string().contains("status"),
            "error should identify field: {err}"
        );
        assert!(
            err.to_string().contains("Dictionary"),
            "error should identify dictionary type: {err}"
        );
    }
}

#[cfg(test)]
mod typed_tests {
    use super::*;
    use crate::exec::chunk::ChunkSlotSchema;
    use arrow::array::{Array, ArrayRef, DictionaryArray, Int32Array, StringArray, StructArray};
    use arrow::datatypes::{Field, Int32Type};
    use novarocks_type_contract::{
        TypedValueBinding, ValueField, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
    };

    fn typed_chunk(field: Field, value: ValueTypeRef, array: ArrayRef) -> Chunk {
        let storage = ValueTypeStorageLifetime::pure();
        let binding = TypedValueBinding::try_new(
            value,
            ValueLayout::try_new(field.data_type(), storage.clone()).unwrap(),
        )
        .unwrap();
        let slot = ChunkSlotSchema::try_new_typed(
            SlotId::new(4),
            Arc::new(field),
            binding,
            Some(8),
            storage.clone(),
        )
        .unwrap();
        Chunk::try_new_with_columns(
            Arc::new(ChunkSchema::try_new_typed(vec![slot], storage).unwrap()),
            vec![array],
        )
        .unwrap()
    }

    #[test]
    fn typed_hydration_decodes_nested_dictionary_without_parent_slot_grant() {
        let storage = ValueTypeStorageLifetime::pure();
        let dictionary: ArrayRef = Arc::new(
            vec![Some("{}"), None, Some("[]")]
                .into_iter()
                .collect::<DictionaryArray<Int32Type>>(),
        );
        let sibling: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let fields = vec![
            Arc::new(Field::new("payload", dictionary.data_type().clone(), true)),
            Arc::new(Field::new("row", DataType::Int32, false)),
        ];
        let array: ArrayRef = Arc::new(
            StructArray::try_new(
                fields.clone().into(),
                vec![dictionary.clone(), sibling.clone()],
                None,
            )
            .unwrap(),
        );
        let json = ValueTypeRef::try_new(
            ValueKind::Nominal(ValueLogicalType::Json),
            true,
            storage.clone(),
        )
        .unwrap();
        let row = ValueTypeRef::try_new(ValueKind::Scalar(DataType::Int32), false, storage.clone())
            .unwrap();
        let value = ValueTypeRef::try_new(
            ValueKind::Struct(
                vec![
                    ValueField::new("payload", json, storage.clone()),
                    ValueField::new("row", row, storage.clone()),
                ]
                .into_boxed_slice(),
            ),
            true,
            storage.clone(),
        )
        .unwrap();
        let source = typed_chunk(
            Field::new("record", DataType::Struct(fields.into()), true),
            value,
            array,
        );
        let mut decoded = Vec::new();
        let flat = hydrate_dictionary_columns_observed::<ChunkConstructionFailure, _, _>(
            &source,
            storage.clone(),
            |slot, occurrence, path, _| {
                assert_eq!(slot, SlotId::new(4));
                assert_eq!(occurrence, 0);
                // A capability for the root occurrence never accepts this child.
                path.is_empty()
            },
            |occurrence, path, step| {
                if matches!(step, ArrayNormalizationStep::DecodeDictionary { .. }) {
                    decoded.push((occurrence, path.to_vec()));
                }
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(decoded, vec![(0, vec![ArrayPathStep::StructField(0)])]);
        assert_no_dictionary(flat.record_batch()).unwrap();
        assert!(
            flat.chunk_schema().slots()[0]
                .typed_binding()
                .same_semantics(source.chunk_schema().slots()[0].typed_binding())
        );
        let flat_array = flat.columns()[0]
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(Arc::ptr_eq(&sibling, &flat_array.columns()[1]));
        let json = flat_array.columns()[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(json.value(0), "{}");
        assert!(json.is_null(1));
        assert!(assert_no_dictionary(source.record_batch()).is_err());
        let preserved = hydrate_dictionary_columns_observed::<ChunkConstructionFailure, _, _>(
            &source,
            storage,
            |_, _, path, _| path == [ArrayPathStep::StructField(0)],
            |_, _, _| Ok(()),
        )
        .unwrap();
        assert!(Arc::ptr_eq(&source.columns()[0], &preserved.columns()[0]));
    }

    #[test]
    fn typed_hydration_retains_original_connector_funding_through_clone_and_slice() {
        use novarocks_spi::connector::{
            ConnectorError, ConnectorExecutionResources, ConnectorResourceCheckpoint,
            ConnectorResourceClass, ConnectorResourceLease, ConnectorResourceLedger,
        };
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Ledger(Arc<AtomicUsize>);
        struct Lease {
            bytes: u64,
            released: Arc<AtomicUsize>,
        }
        impl ConnectorResourceLease for Lease {
            fn bytes(&self) -> u64 {
                self.bytes
            }
            fn try_grow(&mut self, additional: u64) -> Result<(), ConnectorError> {
                self.bytes += additional;
                Ok(())
            }
            fn shrink_to(&mut self, bytes: u64) -> Result<(), ConnectorError> {
                self.bytes = bytes;
                Ok(())
            }
        }
        impl Drop for Lease {
            fn drop(&mut self) {
                self.released.fetch_add(1, Ordering::SeqCst);
            }
        }
        impl ConnectorResourceLedger for Ledger {
            fn checkpoint(&self) -> Result<ConnectorResourceCheckpoint, ConnectorError> {
                Ok(ConnectorResourceCheckpoint::new(1))
            }
            fn try_reserve(
                &self,
                _: ConnectorResourceClass,
                bytes: u64,
            ) -> Result<Box<dyn ConnectorResourceLease>, ConnectorError> {
                Ok(Box::new(Lease {
                    bytes,
                    released: self.0.clone(),
                }))
            }
        }
        let released = Arc::new(AtomicUsize::new(0));
        let resources =
            ConnectorExecutionResources::from_admitted_ledger(Arc::new(Ledger(released.clone())));
        let token = resources
            .try_reserve(ConnectorResourceClass::ReaderOutput, 64)
            .unwrap()
            .into_output(64)
            .unwrap();
        let array: ArrayRef = Arc::new(
            vec![Some("{}"), Some("[]")]
                .into_iter()
                .collect::<DictionaryArray<Int32Type>>(),
        );
        let value = ValueTypeRef::try_new(
            ValueKind::Nominal(ValueLogicalType::Json),
            true,
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap();
        let mut source = typed_chunk(
            Field::new("json", array.data_type().clone(), true),
            value,
            array,
        );
        source.attach_connector_output_memory(token).unwrap();
        let flat = hydrate_dictionary_columns_observed::<ChunkConstructionFailure, _, _>(
            &source,
            ValueTypeStorageLifetime::pure(),
            |_, _, _, _| false,
            |_, _, _| Ok(()),
        )
        .unwrap();
        let clone = flat.clone();
        let slice = flat
            .slice_typed_observed::<crate::runtime::fragment::ExecutionFailure, _>(
                1,
                1,
                ValueTypeStorageLifetime::pure(),
                |_| Ok(()),
            )
            .unwrap();
        drop(source);
        drop(flat);
        drop(clone);
        assert_eq!(released.load(Ordering::SeqCst), 0);
        assert_eq!(slice.len(), 1);
        drop(slice);
        assert_eq!(released.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn hydration_capability_keeps_outer_keys_but_descendants_are_independent() {
        // This pure reconstruction vocabulary test does not widen the engine's
        // production dictionary set (Int32 with direct string values).
        let inner: ArrayRef = Arc::new(
            vec![Some("{}"), Some("[]")]
                .into_iter()
                .collect::<DictionaryArray<Int32Type>>(),
        );
        let outer =
            DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0, 1, 0]), inner).unwrap();
        let key_pointer = outer.keys().values().as_ptr();
        let source: ArrayRef = Arc::new(outer);
        let target = hydration_target::<ChunkConstructionFailure, _, _>(
            source.data_type(),
            SlotId::new(4),
            0,
            &[],
            &mut |_, _, path, _| path.is_empty(),
            &mut |_, _, _| Ok(()),
        )
        .unwrap();
        let mut paths = Vec::new();
        let result = rebuild_array_mode::<ChunkConstructionFailure, _>(
            &source,
            target.as_ref(),
            &mut |step| {
                if let ArrayNormalizationStep::DecodeDictionary { path, .. } = step {
                    paths.push(path.to_vec());
                }
                Ok(())
            },
            true,
        )
        .unwrap();
        assert_eq!(paths, vec![vec![ArrayPathStep::DictionaryValue]]);
        let array = result
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        assert_eq!(array.keys().values().as_ptr(), key_pointer);
        assert_eq!(array.values().data_type(), &DataType::Utf8);
        let plain = DataType::Utf8;
        let refusal = ChunkConstructionFailure::source(ChunkViolation::HydrationContainer);
        let error = rebuild_array_mode::<ChunkConstructionFailure, _>(
            &source,
            &plain,
            &mut |step| {
                if matches!(step, ArrayNormalizationStep::DecodeDictionary { .. }) {
                    Err(refusal.clone())
                } else {
                    Ok(())
                }
            },
            true,
        )
        .unwrap_err();
        assert_eq!(error, refusal);
        assert!(
            matches!(source.data_type(), DataType::Dictionary(_, values) if matches!(values.as_ref(), DataType::Dictionary(_, _)))
        );
    }
}
