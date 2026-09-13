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

use std::time::Duration;

/// The bounded number of unleased materialized catalogs retained by default.
pub const DEFAULT_MAX_RETAINED_CATALOGS: usize = 64;
pub const DEFAULT_MAX_FAILED_CATALOGS: usize = 64;
const DEFAULT_FAILED_RETENTION: Duration = Duration::from_secs(60);
const DEFAULT_TRANSIENT_RETRY_COOLDOWN: Duration = Duration::from_secs(1);
const DEFAULT_PROVIDER_MAX_CONCURRENT_BINDS: usize = 4;

/// Worker-owned bounds for process-local catalog materialization and retention.
///
/// This is provider-neutral policy. The outer role composition resolves its
/// values, while the catalog-runtime owner enforces them for one BE process.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CatalogManagerConfig {
    pub max_retained_catalogs: usize,
    pub max_failed_catalogs: usize,
    pub failed_retention: Duration,
    pub transient_retry_cooldown: Duration,
    pub provider_max_concurrent_binds: usize,
    pub provider_min_bind_interval: Duration,
}

impl Default for CatalogManagerConfig {
    fn default() -> Self {
        Self {
            max_retained_catalogs: DEFAULT_MAX_RETAINED_CATALOGS,
            max_failed_catalogs: DEFAULT_MAX_FAILED_CATALOGS,
            failed_retention: DEFAULT_FAILED_RETENTION,
            transient_retry_cooldown: DEFAULT_TRANSIENT_RETRY_COOLDOWN,
            provider_max_concurrent_binds: DEFAULT_PROVIDER_MAX_CONCURRENT_BINDS,
            provider_min_bind_interval: Duration::ZERO,
        }
    }
}
