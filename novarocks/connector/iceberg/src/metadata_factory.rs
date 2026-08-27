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

//! Construction of one exact-generation `IcebergMetadata`.
//!
//! Design: ADR-0114 (docs/adr/ADR-0114-iceberg-provider-private-catalog-owner.md)
//!
//! A control generation freezes its descriptor, its one catalog, its filesystem
//! authority, and its caches together. Splitting construction out from the
//! connector factory keeps that assembly in one readable place, and keeps the
//! connector factory about composing neutral capability adapters.

use crate::catalog_control::IcebergCatalogControlState;
use crate::metadata_context::IcebergMetadataContext;
use crate::resources::IcebergMetadataResources;

/// Builds the metadata owner for one control generation.
pub struct IcebergMetadataFactory;

impl IcebergMetadataFactory {
    /// Freeze one generation's context.
    ///
    /// The generation gets exactly one catalog. Building a second would give
    /// two clients that can disagree about the same lake, which is the failure
    /// this factory exists to make impossible.
    pub fn build_context(
        control_state: IcebergCatalogControlState,
        resources: IcebergMetadataResources,
    ) -> Result<IcebergMetadataContext, String> {
        IcebergMetadataContext::try_new(control_state, resources)
    }
}
