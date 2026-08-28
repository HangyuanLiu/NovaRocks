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

//! Role-aware preflight for the closed catalog desired-state source contract.

use std::path::{Path, PathBuf};

use anyhow::{Result, bail};
use novarocks_frontend::catalog_application::{
    CatalogDesiredStateSnapshot, CatalogDesiredStateSourceInput, CatalogDesiredStateSourceMode,
    load_static_file_snapshot,
};
use novarocks_types::ClusterRole;
use serde::{Deserialize, Deserializer};

use crate::app_config::NovaRocksConfig;

#[derive(Clone)]
pub struct CatalogSourceConfig {
    mode: CatalogDesiredStateSourceMode,
    static_file_path: Option<PathBuf>,
    static_snapshot: Option<CatalogDesiredStateSnapshot>,
}

impl CatalogSourceConfig {
    pub const fn mode(&self) -> CatalogDesiredStateSourceMode {
        self.mode
    }

    pub fn static_file_path(&self) -> Option<&Path> {
        self.static_file_path.as_deref()
    }

    /// Returns the input that composition consumes. It is available only after
    /// deployable role preflight, so StaticFile is never read after stores,
    /// providers, listeners, or workers are open.
    pub fn input(&self) -> Result<CatalogDesiredStateSourceInput> {
        match self.mode {
            CatalogDesiredStateSourceMode::StaticFile => self
                .static_snapshot
                .clone()
                .map(CatalogDesiredStateSourceInput::StaticFile)
                .ok_or_else(|| {
                    anyhow::anyhow!("CatalogSourceConfig: static-file was not preflighted")
                }),
            CatalogDesiredStateSourceMode::DynamicStateStore => {
                Ok(CatalogDesiredStateSourceInput::DynamicStateStore)
            }
            CatalogDesiredStateSourceMode::ManagedController => {
                Ok(CatalogDesiredStateSourceInput::ManagedControllerUnsupported)
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CatalogSourceConfigWire {
    #[serde(default)]
    mode: Option<CatalogSourceModeWire>,
    static_file_path: Option<PathBuf>,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum CatalogSourceModeWire {
    StaticFile,
    DynamicStateStore,
    ManagedController,
}

impl From<CatalogSourceModeWire> for CatalogDesiredStateSourceMode {
    fn from(value: CatalogSourceModeWire) -> Self {
        match value {
            CatalogSourceModeWire::StaticFile => Self::StaticFile,
            CatalogSourceModeWire::DynamicStateStore => Self::DynamicStateStore,
            CatalogSourceModeWire::ManagedController => Self::ManagedController,
        }
    }
}

impl<'de> Deserialize<'de> for CatalogSourceConfig {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let wire = CatalogSourceConfigWire::deserialize(deserializer)?;
        Ok(Self {
            mode: wire
                .mode
                .map(Into::into)
                .unwrap_or(CatalogDesiredStateSourceMode::StaticFile),
            static_file_path: wire.static_file_path,
            static_snapshot: None,
        })
    }
}

/// Validates source selection before any server composition side effect.
pub fn preflight_catalog_source(config: &mut NovaRocksConfig, config_path: &Path) -> Result<()> {
    match config.cluster.role {
        ClusterRole::Be => {
            if config.catalog_source.is_some() {
                bail!("CatalogSourceConfig: [catalog_source] is only valid for [cluster].role=fe");
            }
            validate_frontend_timeouts(config, false)?;
            Ok(())
        }
        ClusterRole::Fe => {
            validate_frontend_timeouts(config, true)?;
            let has_state_store = config.state_store.is_some();
            let source = config
                .catalog_source
                .get_or_insert_with(|| CatalogSourceConfig {
                    mode: CatalogDesiredStateSourceMode::StaticFile,
                    static_file_path: None,
                    static_snapshot: None,
                });
            match source.mode {
                CatalogDesiredStateSourceMode::StaticFile => {
                    let configured = source.static_file_path.as_ref().ok_or_else(|| {
                        anyhow::anyhow!(
                            "CatalogSourceConfig: static-file requires catalog_source.static_file_path"
                        )
                    })?;
                    let resolved = if configured.is_absolute() {
                        configured.clone()
                    } else {
                        config_path
                            .parent()
                            .unwrap_or_else(|| Path::new("."))
                            .join(configured)
                    };
                    let normalized = std::fs::canonicalize(&resolved).map_err(|error| {
                        anyhow::anyhow!(
                            "CatalogSourceConfig: canonicalize static_file_path {}: {error}",
                            resolved.display()
                        )
                    })?;
                    source.static_snapshot = Some(
                        load_static_file_snapshot(&normalized)
                            .map_err(|error| anyhow::anyhow!("CatalogSourceConfig: {error}"))?,
                    );
                    source.static_file_path = Some(normalized);
                    Ok(())
                }
                CatalogDesiredStateSourceMode::DynamicStateStore => {
                    if source.static_file_path.is_some() {
                        bail!("CatalogSourceConfig: dynamic-state-store forbids static_file_path");
                    }
                    if !has_state_store {
                        bail!("CatalogSourceConfig: dynamic-state-store requires [state_store]");
                    }
                    Ok(())
                }
                CatalogDesiredStateSourceMode::ManagedController => {
                    if source.static_file_path.is_some() {
                        bail!("CatalogSourceConfig: managed-controller forbids static_file_path");
                    }
                    bail!("UnsupportedSourceMode: managed-controller is not implemented")
                }
            }
        }
    }
}

fn validate_frontend_timeouts(config: &NovaRocksConfig, is_frontend: bool) -> Result<()> {
    let drain = config.server.frontend_drain_timeout_ms;
    let cleanup = config.server.frontend_cleanup_timeout_ms;
    if !is_frontend
        && (drain != crate::app_config::DEFAULT_FRONTEND_DRAIN_TIMEOUT_MS
            || cleanup != crate::app_config::DEFAULT_FRONTEND_CLEANUP_TIMEOUT_MS)
    {
        bail!("CatalogSourceConfig: frontend drain timeouts are only valid for [cluster].role=fe");
    }
    if is_frontend && (drain == 0 || cleanup == 0 || drain.checked_add(cleanup).is_none()) {
        bail!(
            "CatalogSourceConfig: frontend drain and cleanup timeouts must be non-zero and their sum must not overflow"
        );
    }
    Ok(())
}
