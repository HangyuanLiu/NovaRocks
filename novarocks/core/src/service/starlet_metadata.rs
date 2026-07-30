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

//! Lifecycle-bound bridge for the temporary Starlet metadata callback.
//!
//! Lake RPC entrypoints are still rooted in the core C ABI until RCI-5G. The
//! compat application installs exactly one typed callback while that host is
//! live; the execution kernel sees only resolved domain facts.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

use crate::connector::starrocks::ports::StarletMetadataProvider;
use crate::runtime::starlet_shard_registry::{S3StoreConfig, StarletShardInfo};

type ProviderSlot = RwLock<Option<Arc<dyn StarletMetadataProvider>>>;

static ACTIVE_PROVIDER: OnceLock<ProviderSlot> = OnceLock::new();

fn provider_slot() -> &'static ProviderSlot {
    ACTIVE_PROVIDER.get_or_init(|| RwLock::new(None))
}

pub fn install(provider: Arc<dyn StarletMetadataProvider>) -> Result<(), String> {
    let mut slot = provider_slot()
        .write()
        .map_err(|_| "Starlet metadata callback lock is poisoned".to_string())?;
    if slot.is_some() {
        return Err("Starlet metadata callback is already installed".to_string());
    }
    *slot = Some(provider);
    Ok(())
}

pub fn clear() -> Result<(), String> {
    let mut slot = provider_slot()
        .write()
        .map_err(|_| "Starlet metadata callback lock is poisoned".to_string())?;
    slot.take();
    Ok(())
}

fn provider() -> Result<Arc<dyn StarletMetadataProvider>, String> {
    provider_slot()
        .read()
        .map_err(|_| "Starlet metadata callback lock is poisoned".to_string())?
        .clone()
        .ok_or_else(|| {
            "Starlet metadata capability is unavailable because no compat application host is running"
                .to_string()
        })
}

pub fn retrieve_shard_infos(tablet_ids: &[i64]) -> Result<HashMap<i64, StarletShardInfo>, String> {
    provider()?.retrieve_shard_infos(tablet_ids)
}

pub fn retrieve_s3_config_for_path(path: &str) -> Result<Option<S3StoreConfig>, String> {
    provider()?.retrieve_s3_config_for_path(path)
}
