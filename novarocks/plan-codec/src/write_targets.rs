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

//! Frozen write-session handles consumed by final-plan encoding.

use std::collections::BTreeMap;

use novarocks_proto_models::connector_write as write_dto;
use novarocks_spi::connector::CatalogHandle;
use novarocks_spi::connector::write_stack::WriteTargetOrdinal;

/// The frozen per-query write targets the encoder stamps into writer nodes.
///
/// The handles are already canonical and already charged against the query's
/// unique-handle budget, so the bytes submitted are exactly the bytes the
/// frontend accounted for.
#[derive(Clone, Debug)]
pub struct SealedWriteTargets {
    catalog_handle: CatalogHandle,
    handles: BTreeMap<u32, write_dto::ConnectorWriterHandle>,
}

impl SealedWriteTargets {
    pub fn new(
        catalog_handle: CatalogHandle,
        handles: BTreeMap<u32, write_dto::ConnectorWriterHandle>,
    ) -> Self {
        Self {
            catalog_handle,
            handles,
        }
    }

    pub const fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }

    fn handle_for(&self, target: WriteTargetOrdinal) -> Option<&write_dto::ConnectorWriterHandle> {
        self.handles.get(&target.get())
    }

    /// The handle this session sealed for one target, for an encoder that
    /// stamps it into a completed plan's writer node.
    pub fn handle_for_target(
        &self,
        target: WriteTargetOrdinal,
    ) -> Option<write_dto::ConnectorWriterHandle> {
        self.handle_for(target).cloned()
    }

    pub fn ordinals(&self) -> impl Iterator<Item = u32> + '_ {
        self.handles.keys().copied()
    }

    /// The one target this session sealed, for a plan shape that has exactly one
    /// writer. A single-writer plan cannot express a session with several
    /// targets, so a session that sealed more than one is refused here rather
    /// than silently having its extra targets written by nobody.
    pub fn sole_target_ordinal(&self) -> Result<WriteTargetOrdinal, String> {
        let mut ordinals = self.handles.keys().copied();
        let (Some(ordinal), None) = (ordinals.next(), ordinals.next()) else {
            return Err(format!(
                "a single-writer plan requires a write session with exactly one target, but the session sealed {}",
                self.handles.len()
            ));
        };
        WriteTargetOrdinal::try_new(ordinal).map_err(|error| error.to_string())
    }
}
