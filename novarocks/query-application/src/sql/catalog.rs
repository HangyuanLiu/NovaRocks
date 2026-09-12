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

//! Narrow catalog facts consumed by session SQL admission.

use std::sync::Arc;

use crate::session_error::QueryServiceError;

/// Read-only Catalog and connector facts required to resolve `USE` and
/// `SET CATALOG`. This port carries neither a command executor nor a
/// connector binding, so session admission cannot acquire mutation authority.
pub trait SessionCatalogPort: Send + Sync + 'static {
    fn database_exists(&self, database_name: &str) -> Result<bool, QueryServiceError>;

    fn require_external_catalog_ready(&self, catalog_name: &str) -> Result<(), QueryServiceError>;

    fn external_namespace_exists(
        &self,
        catalog_name: &str,
        namespace_name: &str,
    ) -> Result<bool, QueryServiceError>;
}

pub type SessionCatalogService = Arc<dyn SessionCatalogPort>;
