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

//! Backend-local typed catalog read execution value.
//!
//! A catalog materialization retains this exact factory and codec pair. Query
//! lifecycle lookup is owned by `CatalogManager`, so this module deliberately
//! contains no second registry or lifecycle authority.

use std::fmt;
use std::sync::Arc;

use novarocks_proto_codec::connector_read::ConnectorReadCodec;
use novarocks_spi::connector::{CatalogWriteExecution, read_stack::ConnectorReadProviderFactory};

/// One exact binding's complete worker read unit. The factory and codec must
/// travel together because every recovered handle belongs to the factory that
/// will consume it.
#[derive(Clone)]
pub struct InstalledReadExecution {
    factory: Arc<dyn ConnectorReadProviderFactory>,
    codec: Arc<dyn ConnectorReadCodec>,
}

impl InstalledReadExecution {
    pub fn new(
        factory: Arc<dyn ConnectorReadProviderFactory>,
        codec: Arc<dyn ConnectorReadCodec>,
    ) -> Self {
        Self { factory, codec }
    }

    pub fn factory(&self) -> Arc<dyn ConnectorReadProviderFactory> {
        Arc::clone(&self.factory)
    }

    pub fn codec(&self) -> Arc<dyn ConnectorReadCodec> {
        Arc::clone(&self.codec)
    }
}

impl fmt::Debug for InstalledReadExecution {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstalledReadExecution")
            .finish_non_exhaustive()
    }
}

/// One exact catalog-scoped writer capability retained by a materialized
/// catalog runtime. Query lifecycle lookup is deliberately the only way a
/// fragment can obtain it.
#[derive(Clone)]
pub struct InstalledWriteExecution {
    execution: Arc<dyn CatalogWriteExecution>,
}

impl InstalledWriteExecution {
    pub fn new(execution: Arc<dyn CatalogWriteExecution>) -> Self {
        Self { execution }
    }

    pub fn execution(&self) -> Arc<dyn CatalogWriteExecution> {
        Arc::clone(&self.execution)
    }
}

impl fmt::Debug for InstalledWriteExecution {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstalledWriteExecution")
            .finish_non_exhaustive()
    }
}
