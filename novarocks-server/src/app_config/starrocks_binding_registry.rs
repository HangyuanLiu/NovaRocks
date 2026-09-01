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

//! Server construction of FE-local immutable StarRocks metadata resources.
//!
//! This module owns only process-local configuration and clients. A catalog
//! definition names one exact local binding, while endpoints and credentials
//! remain inside the FE process and are never placed in catalog state or wire
//! payloads. The resulting container is provider-owned so the Server never
//! becomes a role-binding factory owner.

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks_connector_starrocks::{
    StarRocksMetadataSource, StarRocksRemoteControlClient, StarRocksRemoteControlConfig,
    StarRocksRemoteMetadataSource, StarRocksRoleBindingResources,
};

use super::{MAX_STARROCKS_LOCAL_BINDING_ENTRIES, StarRocksLocalBindingConfig};

/// Builds the provider-owned immutable resources from Server-local config.
///
/// Client construction is intentionally local and has no remote metadata I/O.
pub(crate) fn project_starrocks_role_binding_resources(
    entries: Vec<StarRocksLocalBindingConfig>,
) -> Result<StarRocksRoleBindingResources, String> {
    if entries.len() > MAX_STARROCKS_LOCAL_BINDING_ENTRIES {
        return Err(format!(
            "StarRocks local binding registry exceeds {MAX_STARROCKS_LOCAL_BINDING_ENTRIES} entries"
        ));
    }

    let mut sources = BTreeMap::new();
    for entry in entries {
        let local_binding = entry.local_binding().clone();
        let remote_config = StarRocksRemoteControlConfig::try_new(
            entry.endpoints(),
            entry.username(),
            entry.password().expose_secret(),
            entry.request_timeout(),
            entry.retry_count(),
        )
        .map_err(|error| format!("invalid StarRocks local binding `{local_binding:?}`: {error}"))?;
        let client = Arc::new(
            StarRocksRemoteControlClient::try_new(remote_config).map_err(|error| {
                format!("construct StarRocks local binding `{local_binding:?}`: {error}")
            })?,
        );
        let source: Arc<dyn StarRocksMetadataSource> =
            Arc::new(StarRocksRemoteMetadataSource::new(client));
        if sources.insert(local_binding.clone(), source).is_some() {
            return Err(format!(
                "duplicate StarRocks local binding `{}`",
                local_binding.as_str()
            ));
        }
    }

    Ok(StarRocksRoleBindingResources::new(sources))
}
