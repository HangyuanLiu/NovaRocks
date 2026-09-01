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

//! The execution-side binding of the two write row relations, plus the two
//! codec ports the write data plane needs.
//!
//! The relations themselves — their columns, their kind codes, and their
//! kind-versus-nullability invariants — are defined once in
//! `novarocks_spi::connector::write_stack::relation`, because the SQL planner,
//! the execution engine, the backend, and the frontend must all agree on them.
//! This module adds only what is execution-local: the `SlotId` each column
//! carries inside a [`Chunk`], and the two narrow ports below.
//!
//! `novarocks-execution` depends only on `novarocks-types` and `novarocks-spi`,
//! and its dependency closure is forbidden from ever reaching the generated
//! wire crates, so this layer structurally cannot encode or decode a commit
//! fragment. Canonical bytes enter and leave through the ports: execution moves
//! opaque buffers and counts them, and never interprets one.
//!
//! [`Chunk`]: crate::exec::chunk::Chunk

use std::sync::{Arc, OnceLock};

use arrow::datatypes::SchemaRef;
use novarocks_spi::connector::ConnectorError;
use novarocks_spi::connector::write_stack::{
    ConnectorCommitFragment, WRITE_RELATION_FRAGMENT_INDEX, WRITE_RELATION_KIND_INDEX,
    WRITE_RELATION_ROW_COUNT_INDEX, WRITE_RELATION_TARGET_INDEX, WriteTargetOrdinal,
    root_output_schema, writer_output_schema,
};
use novarocks_types::SlotId;

use crate::exec::chunk::{ChunkSchema, ChunkSchemaRef};

/// Slot id of the `kind` column in both write relations.
pub const WRITE_RELATION_KIND_SLOT: SlotId = SlotId::new(1);
/// Slot id of the `write_target_ordinal` column in both write relations.
pub const WRITE_RELATION_TARGET_SLOT: SlotId = SlotId::new(2);
/// Slot id of the `row_count` column in both write relations.
pub const WRITE_RELATION_ROW_COUNT_SLOT: SlotId = SlotId::new(3);
/// Slot id of the `commit_fragment` column in both write relations.
pub const WRITE_RELATION_FRAGMENT_SLOT: SlotId = SlotId::new(4);

/// The slot ids of both write relations, in the column order SPI froze.
pub const WRITE_RELATION_SLOT_IDS: [SlotId; 4] = [
    WRITE_RELATION_KIND_SLOT,
    WRITE_RELATION_TARGET_SLOT,
    WRITE_RELATION_ROW_COUNT_SLOT,
    WRITE_RELATION_FRAGMENT_SLOT,
];

const _: () = {
    assert!(WRITE_RELATION_KIND_INDEX == 0);
    assert!(WRITE_RELATION_TARGET_INDEX == 1);
    assert!(WRITE_RELATION_ROW_COUNT_INDEX == 2);
    assert!(WRITE_RELATION_FRAGMENT_INDEX == 3);
};

fn chunk_schema_for(schema: &SchemaRef) -> ChunkSchemaRef {
    ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &WRITE_RELATION_SLOT_IDS)
        .expect("the frozen write relation is a valid chunk schema")
}

/// The cached `SchemaRef` every `TableWriter` output batch is built against.
pub fn writer_relation_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    Arc::clone(SCHEMA.get_or_init(writer_output_schema))
}

/// The cached `SchemaRef` the single `TableFinish` output batch is built
/// against.
pub fn root_relation_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    Arc::clone(SCHEMA.get_or_init(root_output_schema))
}

/// The chunk schema of the `TableWriter` output relation.
pub fn writer_relation_chunk_schema() -> ChunkSchemaRef {
    static CHUNK_SCHEMA: OnceLock<ChunkSchemaRef> = OnceLock::new();
    Arc::clone(CHUNK_SCHEMA.get_or_init(|| chunk_schema_for(&writer_relation_schema())))
}

/// The chunk schema of the `TableFinish` output relation.
pub fn root_relation_chunk_schema() -> ChunkSchemaRef {
    static CHUNK_SCHEMA: OnceLock<ChunkSchemaRef> = OnceLock::new();
    Arc::clone(CHUNK_SCHEMA.get_or_init(|| chunk_schema_for(&root_relation_schema())))
}

/// Canonical commit-fragment egress port.
///
/// `TableWriter` hands one finished provider fragment to its owner and receives
/// the canonical carrier bytes back. The execution layer never performs that
/// encoding itself: it cannot reach the generated wire crates, so the backend
/// installs the implementation for the exact catalog generation this query
/// froze.
pub trait ConnectorCommitFragmentEncoder: Send + Sync {
    /// Encode one commit fragment produced for `target` into its canonical
    /// carrier bytes.
    fn encode(
        &self,
        target: WriteTargetOrdinal,
        fragment: &ConnectorCommitFragment,
    ) -> Result<Vec<u8>, ConnectorError>;
}

/// Canonical commit-fragment ingress port.
///
/// `TableFinish` receives opaque canonical bytes from many senders. It must
/// reject a foreign, truncated, or non-canonical carrier before it enters the
/// prepared write set, but it must not decode one into a provider domain
/// object: that happens only in the frontend, on the provider's own control
/// binding. The implementation therefore performs a structural check and
/// nothing more.
pub trait ConnectorCommitFragmentCarrierValidator: Send + Sync {
    /// Structurally verify that `encoded` is a canonical, in-bounds commit
    /// fragment carrier of the provider expected for `target`, without
    /// decoding it into a provider value.
    fn validate(&self, target: WriteTargetOrdinal, encoded: &[u8]) -> Result<(), ConnectorError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execution_binds_the_spi_relations_without_redefining_them() {
        assert_eq!(writer_relation_schema(), writer_output_schema());
        assert_eq!(root_relation_schema(), root_output_schema());
    }

    #[test]
    fn every_relation_column_carries_its_slot_id_at_the_spi_column_index() {
        for chunk_schema in [writer_relation_chunk_schema(), root_relation_chunk_schema()] {
            assert_eq!(
                chunk_schema.index_of(WRITE_RELATION_KIND_SLOT),
                Some(WRITE_RELATION_KIND_INDEX)
            );
            assert_eq!(
                chunk_schema.index_of(WRITE_RELATION_TARGET_SLOT),
                Some(WRITE_RELATION_TARGET_INDEX)
            );
            assert_eq!(
                chunk_schema.index_of(WRITE_RELATION_ROW_COUNT_SLOT),
                Some(WRITE_RELATION_ROW_COUNT_INDEX)
            );
            assert_eq!(
                chunk_schema.index_of(WRITE_RELATION_FRAGMENT_SLOT),
                Some(WRITE_RELATION_FRAGMENT_INDEX)
            );
        }
    }
}
