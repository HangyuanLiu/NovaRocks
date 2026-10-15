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

//! A frozen table-load response and its closed access delegation.
//!
//! A vended table response must not select a FileIO before its companion
//! credentials have entered the query-attempt collector. The response is
//! therefore frozen first, and materialized only with the FileIO the caller
//! chooses afterwards. The delegation is converted at once into a closed type
//! that has no serialization and no value-printing `Debug`.

use std::fmt;

use crate::iceberg::io::FileIO;
use crate::iceberg::spec::TableMetadata;
use crate::iceberg::table::Table;
use crate::iceberg::{Runtime, TableIdent};
use crate::rest_protocol::minimal_view::{RawStorageCredential, StorageCredentials};
use crate::rest_protocol::wire::LoadTableResult;

/// Response-local storage delegation (`storage-credentials`).
///
/// Values leave only through [`RestDelegation::into_credentials`], which the
/// one credential adapter consumes.
pub(crate) struct RestDelegation {
    credentials: StorageCredentials,
}

impl RestDelegation {
    pub(crate) fn new(credentials: StorageCredentials) -> Self {
        Self { credentials }
    }

    /// Whether the response carried a `storage-credentials` member at all.
    pub(crate) fn is_present(&self) -> bool {
        matches!(self.credentials, StorageCredentials::Present(_))
    }

    pub(crate) fn len(&self) -> usize {
        self.credentials.len()
    }

    pub(crate) fn into_credentials(self) -> StorageCredentials {
        self.credentials
    }
}

impl fmt::Debug for RestDelegation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Key names only: config values are credentials.
        formatter
            .debug_struct("RestDelegation")
            .field("credentials", &self.credentials)
            .finish()
    }
}

/// An immutable table-load response that has not selected a FileIO yet.
pub(crate) struct FrozenLoadTable {
    ident: TableIdent,
    metadata_location: Option<String>,
    metadata: TableMetadata,
    runtime: Runtime,
}

impl FrozenLoadTable {
    /// Freeze a fully decoded response. The response's FileIO configuration
    /// is dropped: NovaRocks storage access never comes from it.
    pub(crate) fn from_response(
        ident: TableIdent,
        response: LoadTableResult,
        runtime: Runtime,
    ) -> (Self, RestDelegation) {
        let LoadTableResult {
            metadata_location,
            metadata,
            config: _,
            storage_credentials,
        } = response;
        let credentials = match storage_credentials {
            None => StorageCredentials::Absent,
            Some(credentials) => StorageCredentials::Present(
                credentials
                    .into_iter()
                    .map(|credential| {
                        RawStorageCredential::from_decoded(
                            credential.prefix,
                            credential.config.into_iter().collect(),
                        )
                    })
                    .collect(),
            ),
        };
        (
            Self {
                ident,
                metadata_location,
                metadata,
                runtime,
            },
            RestDelegation::new(credentials),
        )
    }

    pub(crate) fn metadata_location(&self) -> Option<&str> {
        self.metadata_location.as_deref()
    }

    pub(crate) fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }

    /// Materialize the frozen response with the caller's FileIO.
    pub(crate) fn materialize(self, file_io: FileIO) -> crate::iceberg::Result<Table> {
        let builder = Table::builder()
            .identifier(self.ident)
            .file_io(file_io)
            .metadata(self.metadata)
            .runtime(self.runtime);
        match self.metadata_location {
            Some(location) => builder.metadata_location(location).build(),
            None => builder.build(),
        }
    }
}

impl fmt::Debug for FrozenLoadTable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FrozenLoadTable")
            .field("table", &self.ident)
            .field("has_metadata_location", &self.metadata_location.is_some())
            .finish_non_exhaustive()
    }
}
