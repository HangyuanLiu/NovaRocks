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

//! StarRocks backend catalog-runtime materialization.
//!
//! StarRocks has no BE read or write role binding. Its explicit role factory
//! reports that capability-free state, while this local materializer retains
//! the catalog-lifecycle projection used by the backend host.

use std::sync::Arc;

use novarocks_spi::connector::{
    CatalogHandle, CatalogProperties, CatalogProviderKind, CatalogRuntime,
    CatalogRuntimeMaterializer, ConnectorError, ConnectorErrorKind,
};

/// Startup-composed materializer for the closed StarRocks catalog family.
#[derive(Default)]
pub struct StarRocksCatalogRuntimeMaterializer;

struct StarRocksCatalogRuntime {
    handle: CatalogHandle,
}

impl CatalogRuntime for StarRocksCatalogRuntime {
    fn handle(&self) -> &CatalogHandle {
        &self.handle
    }

    fn provider_kind(&self) -> CatalogProviderKind {
        CatalogProviderKind::StarRocks
    }
}

impl CatalogRuntimeMaterializer for StarRocksCatalogRuntimeMaterializer {
    fn provider_kind(&self) -> CatalogProviderKind {
        CatalogProviderKind::StarRocks
    }

    fn materialize(
        &self,
        properties: &CatalogProperties,
    ) -> Result<Arc<dyn CatalogRuntime>, ConnectorError> {
        if properties.provider_kind() != CatalogProviderKind::StarRocks {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "StarRocks catalog materializer received another provider kind",
            ));
        }
        Ok(Arc::new(StarRocksCatalogRuntime {
            handle: properties.handle().clone(),
        }))
    }
}
