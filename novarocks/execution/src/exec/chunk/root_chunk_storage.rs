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

//! Borrowed root input backing inspection, before any cursor/clone/hydration.

use super::root_array_storage::borrowed_root_batch_storage_with_types;
use super::root_schema_backing::{
    RootFieldSource, RootSchemaSource, RootSchemaBackingError, RootSchemaInspection,
};

/// Explicit construction namespaces attached to this same ChunkSchema.
/// Missing positive sources remain a refusal; equal schemas cannot elect one.
fn sources(
    schema: &ChunkSchema,
) -> Result<(RootSchemaSource<'_>, RootFieldSource<'_>), RootArrayStorageError> {
    if let Some(original) = schema.metadata_materializations() {
        if !original.schema_owner().lends(&schema.arrow_schema_ref()) {
            return Err(RootArrayStorageError::UnknownMetadataOwner);
        }
        if original.fields().len() > 65_536 {
            return Err(RootArrayStorageError::WorkExceeded);
        }
        let index = original
            .checked_field_index()
            .ok_or(RootArrayStorageError::UnknownMetadataOwner)?;
        let fields = RootFieldSource::Original {
            index,
            attached: schema.field_metadata_origins(),
        };
        let root = match schema.schema_metadata_origin() {
            Some(owner) => RootSchemaSource::M07(owner),
            None => RootSchemaSource::Original(original.schema_owner()),
        };
        return Ok((root, fields));
    }
    Ok((
        RootSchemaSource::M07(
            schema
                .schema_metadata_origin()
                .ok_or(RootArrayStorageError::UnknownMetadataOwner)?,
        ),
        RootFieldSource::M07(
            schema
                .field_metadata_origins()
                .ok_or(RootArrayStorageError::UnknownMetadataOwner)?,
        ),
    ))
}

fn inspect_indices<'a>(
    schema: &'a ChunkSchema,
    fields: RootFieldSource<'a>,
    inspection: &mut RootSchemaInspection<'a>,
) -> Result<(), RootArrayStorageError> {
    for slot in schema.slots() {
        if let Some(local) = slot.metadata_origins() {
            inspection.inspect_origin_index(local).map_err(map_error)?;
        } else if schema.metadata_materializations().is_none() {
            return Err(RootArrayStorageError::UnknownMetadataOwner);
        }
    }
    match fields {
        RootFieldSource::M07(origins) => {
            inspection.inspect_origin_index(origins).map_err(map_error)
        }
        RootFieldSource::Original { index, attached } => {
            if let Some(origins) = attached {
                inspection
                    .inspect_origin_index(origins)
                    .map_err(map_error)?;
            }
            inspection
                .inspect_original_index(index, attached)
                .map_err(map_error)
        }
    }
}
use super::{Chunk, ChunkSchema, RootArrayStorageError, RootArrayStorageLimits};
use novarocks_result_contract::RootProfileV1;

/// Inspect this input's actual Arrow backing, immutable metadata owners, and
/// Chunk scaffolds without allocation, hashing unknown maps, or touching rows.
/// Shared payload aliases use the array inspector's fixed identity cache;
/// metadata owners and uncached aliases remain conservative. The upper bound
/// covers the original input allowance; it does not mint a funding grant or
/// establish a bounded hydration/source-growth capability.
///
/// MEM tracker/provider governance graphs retain their existing admitted owner
/// scope. Their fixed Chunk holder shells are counted, while referenced shared
/// authorities are not relabeled as result payload allocations. All Arrow
/// buffers are inspected regardless of their existing accounting charges.
pub fn borrowed_root_chunk_storage(
    chunk: &Chunk,
    limits: RootArrayStorageLimits,
) -> Result<usize, RootArrayStorageError> {
    let cap = limits.bytes.min(96 * 1024 * 1024);
    if chunk.batch.num_columns() > RootProfileV1::MAX_COLUMNS
        || chunk.chunk_schema().slots().len() > RootProfileV1::MAX_COLUMNS
    {
        return Err(RootArrayStorageError::WorkExceeded);
    }
    let schema = chunk.chunk_schema();
    let (top, origins) = sources(schema)?;
    // Validate the ACTUAL batch Arc first. Expected equality is not provenance;
    // an independently allocated equal map can have arbitrary table history.
    let actual = chunk.batch.schema();
    match top {
        RootSchemaSource::M07(owner) if owner.backing_bytes_for(&actual).is_some() => {}
        RootSchemaSource::Original(owner) if owner.lends(&actual) => {}
        _ => return Err(RootArrayStorageError::UnknownMetadataOwner),
    }
    let mut metadata = RootSchemaInspection::new(
        cap,
        limits.nodes.min(2 * RootProfileV1::SCHEMA_TYPE_NODES),
        limits.depth,
    );
    chunk
        .inspect_root_holder_scaffolds(&mut metadata)
        .map_err(map_error)?;
    schema
        .inspect_root_scaffolds(&mut metadata)
        .map_err(map_error)?;
    inspect_indices(schema, origins, &mut metadata)?;
    metadata
        .inspect_schema_source(&actual, top, origins)
        .map_err(map_error)?;
    let storage_cap = cap
        .checked_sub(metadata.bytes())
        .ok_or(RootArrayStorageError::CapacityExceeded)?;
    let storage = borrowed_root_batch_storage_with_types(
        &chunk.batch,
        RootArrayStorageLimits {
            bytes: storage_cap,
            ..limits
        },
        &mut |data_type| {
            metadata
                .inspect_data_type_source(data_type, origins)
                .map_err(map_error)
        },
    )?;
    storage
        .checked_add(metadata.bytes())
        .filter(|bytes| *bytes <= cap)
        .ok_or(RootArrayStorageError::CapacityExceeded)
}

/// Borrow only the immutable output schema, provenance indices, and its own
/// scaffolds before a source allocates arrays. No dummy batch, map clone, or
/// row inspection is needed. This does not cover arrays, Chunk/RecordBatch
/// holders, column Vec capacity, or construction workspace; the source must
/// include those in the same original grant's preflight.
pub fn borrowed_root_chunk_schema_storage(
    schema: &ChunkSchema,
    limits: RootArrayStorageLimits,
) -> Result<usize, RootArrayStorageError> {
    if schema.slots().len() > RootProfileV1::MAX_COLUMNS {
        return Err(RootArrayStorageError::WorkExceeded);
    }
    let (top, origins) = sources(schema)?;
    let actual = schema.arrow_schema_ref();
    let mut inspection = RootSchemaInspection::new(limits.bytes, limits.nodes, limits.depth);
    schema
        .inspect_root_scaffolds(&mut inspection)
        .map_err(map_error)?;
    inspect_indices(schema, origins, &mut inspection)?;
    inspection
        .inspect_schema_source(&actual, top, origins)
        .map_err(map_error)?;
    Ok(inspection.bytes())
}

fn map_error(error: RootSchemaBackingError) -> RootArrayStorageError {
    match error {
        RootSchemaBackingError::CapacityExceeded => RootArrayStorageError::CapacityExceeded,
        RootSchemaBackingError::WorkExceeded => RootArrayStorageError::WorkExceeded,
        RootSchemaBackingError::UnsupportedType => RootArrayStorageError::UnsupportedCarrier,
        RootSchemaBackingError::UnknownSchemaMetadataOwner
        | RootSchemaBackingError::UnknownFieldMetadataOwner
        | RootSchemaBackingError::UninspectedOriginIndex => {
            RootArrayStorageError::UnknownMetadataOwner
        }
    }
}

#[cfg(test)]
#[path = "root_positive_source_tests.rs"]
mod root_positive_source_tests;
