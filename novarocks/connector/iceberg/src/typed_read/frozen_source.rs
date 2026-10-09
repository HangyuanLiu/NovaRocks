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

//! The original COW source retained before adoption by a read generation.
//!
//! This receipt is FE-local. Its complete read-file facts come from the same
//! ReadSnapshot as the legacy statistics projection, never from DTO decoding
//! or a second manifest/delete walk. Only an adopted immutable proof travels
//! on the typed read wire; this object never does.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorPinnedFileSet, ConnectorProviderBindingKey,
    ConnectorTableHandle, write_stack::ConnectorWriteRewriteSource,
};

/// An unforgeable provider-owned source; generic consumers receive only its
/// SPI-erased wrapper. Fields and constructors stay inside the provider.
pub struct IcebergCowPendingReadSource {
    pub(crate) owner: ConnectorProviderBindingKey,
    pub(crate) original_source: ConnectorTableHandle,
    pub(crate) pinned: ConnectorPinnedFileSet,
    pub(crate) signed_base: [u8; 32],
    pub(crate) signed_schema: SchemaRef,
    pub(crate) metadata: crate::iceberg::spec::TableMetadataRef,
    pub(crate) access: Arc<crate::loaded_table::IcebergAttemptTableAccess>,
    pub(crate) read_file: Arc<crate::read_model::IcebergReadFile>,
}

impl std::fmt::Debug for IcebergCowPendingReadSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IcebergCowPendingReadSource")
            .field("owner", &self.owner)
            .field("snapshot_id", &self.pinned.version_ordinal())
            .finish_non_exhaustive()
    }
}

impl IcebergCowPendingReadSource {
    /// Called after the ONE original branch/source author has completed.
    /// Metadata is an Arc loan of the original SDK table, and read_file moves
    /// original fields/remainder from that table's ReadSnapshot observation.
    pub(crate) fn from_original_branch(
        owner: ConnectorProviderBindingKey,
        source: &ConnectorWriteRewriteSource,
        metadata: crate::iceberg::spec::TableMetadataRef,
        access: Arc<crate::loaded_table::IcebergAttemptTableAccess>,
        read_file: crate::read_model::IcebergReadFile,
    ) -> Result<Self, ConnectorError> {
        if source.source().owner() != &owner.instance_id
            || !matches!(source.pinned_source().files(), [path] if path.as_ref() == read_file.path)
            || metadata
                .snapshot_by_id(source.pinned_source().version_ordinal())
                .is_none()
            || read_file.read_domain().endpoint().snapshot_id()
                != source.pinned_source().version_ordinal()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg frozen COW source differs from its original observation",
            ));
        }
        Ok(Self {
            owner,
            original_source: source.source().clone(),
            pinned: source.pinned_source().clone(),
            signed_base: source.base_version_digest(),
            signed_schema: Arc::clone(source.scan_schema()),
            metadata,
            access,
            read_file: Arc::new(read_file),
        })
    }
}
