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

pub mod binding_decode;
pub mod catalog_manager;
mod execution_host;
pub(crate) mod runtime;
pub mod typed_registry;
pub mod typed_runtime;

#[cfg(test)]
mod runtime_test;

pub use execution_host::{
    ConnectorExecutionHost, ConnectorExecutionLease, ConnectorExecutionQueryResolver,
};
pub use typed_registry::InstalledReadExecution;

/// Backend-local token passed through native plan decoding.
///
/// Connector execution bindings are resolved by the backend execution host;
/// this value only preserves the established decode assembly boundary.
#[derive(Clone, Default)]
pub(crate) struct ConnectorRegistry;

impl ConnectorRegistry {
    pub(crate) fn new() -> Self {
        Self
    }
}

impl std::fmt::Debug for ConnectorRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("ConnectorRegistry").finish()
    }
}
