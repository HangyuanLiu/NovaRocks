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
use super::root_schema_backing::{RootSchemaBackingError, RootSchemaInspection};
use super::{Chunk, RootArrayStorageError, RootArrayStorageLimits};
use novarocks_result_contract::RootProfileV1;

/// Inspect this input's actual Arrow backing, immutable metadata owners, and
/// Chunk scaffolds without allocation, hashing unknown maps, or touching rows.
/// Shared aliases are conservatively counted again. The returned upper bound
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
    let origins = schema
        .field_metadata_origins()
        .ok_or(RootArrayStorageError::UnknownMetadataOwner)?;
    let top = schema
        .schema_metadata_origin()
        .ok_or(RootArrayStorageError::UnknownMetadataOwner)?;
    // Validate the ACTUAL batch Arc first. Expected equality is not provenance;
    // an independently allocated equal map can have arbitrary table history.
    let actual = chunk.batch.schema();
    if top.backing_bytes_for(&actual).is_none() {
        return Err(RootArrayStorageError::UnknownMetadataOwner);
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
    // Every compact index is itself live and retains all its fields, including
    // cached owners not reachable through the visible batch's projected fields.
    for slot in schema.slots() {
        let local = slot
            .metadata_origins()
            .ok_or(RootArrayStorageError::UnknownMetadataOwner)?;
        metadata.inspect_origin_index(local).map_err(map_error)?;
    }
    metadata.inspect_origin_index(origins).map_err(map_error)?;
    metadata
        .inspect_schema(&actual, top, origins)
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
                .inspect_data_type(data_type, origins)
                .map_err(map_error)
        },
    )?;
    storage
        .checked_add(metadata.bytes())
        .filter(|bytes| *bytes <= cap)
        .ok_or(RootArrayStorageError::CapacityExceeded)
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
