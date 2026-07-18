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

use crate::types::SuiteConfig;
use anyhow::{Context, Result, bail};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum SuiteServerMode {
    #[default]
    Native,
    #[serde(rename = "starrocks-compat")]
    StarRocksCompat,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ArtifactFlavor {
    #[default]
    Default,
    Compat,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct SuiteManifest {
    pub explicit_only: bool,
    pub server_mode: SuiteServerMode,
    pub cluster_size: usize,
    pub artifact_flavor: ArtifactFlavor,
}

impl Default for SuiteManifest {
    fn default() -> Self {
        Self {
            explicit_only: false,
            server_mode: SuiteServerMode::Native,
            cluster_size: 1,
            artifact_flavor: ArtifactFlavor::Default,
        }
    }
}

impl SuiteManifest {
    pub fn parse(content: &str) -> Result<Self> {
        let manifest: Self = toml::from_str(content).context("failed to parse suite manifest")?;
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let content = fs::read_to_string(path)
            .with_context(|| format!("failed to read suite manifest {}", path.display()))?;
        Self::parse(&content).with_context(|| format!("invalid suite manifest {}", path.display()))
    }

    fn validate(&self) -> Result<()> {
        if self.cluster_size == 0 {
            bail!("suite manifest cluster_size must be greater than zero");
        }
        if self.server_mode == SuiteServerMode::StarRocksCompat
            && (!self.explicit_only
                || self.cluster_size != 3
                || self.artifact_flavor != ArtifactFlavor::Compat)
        {
            bail!(
                "starrocks-compat suites require explicit_only=true, cluster_size=3, and artifact_flavor=compat"
            );
        }
        Ok(())
    }
}

pub fn select_suite_names(
    requested: &str,
    suites: &BTreeMap<String, SuiteConfig>,
) -> Result<Vec<String>> {
    let suite_names: Vec<String> = if requested.eq_ignore_ascii_case("all") {
        suites
            .iter()
            .filter(|(_, suite)| !suite.manifest.explicit_only)
            .map(|(name, _)| name.clone())
            .collect()
    } else {
        requested
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(ToString::to_string)
            .collect()
    };

    if suite_names.is_empty() {
        bail!("no suites selected");
    }

    let all_available: Vec<String> = suites.keys().cloned().collect();
    let mut server_mode = None;
    for name in &suite_names {
        let suite = suites.get(name).with_context(|| {
            format!(
                "unknown suite '{}'; available suites: {}",
                name,
                all_available.join(", ")
            )
        })?;
        if let Some(selected_mode) = server_mode {
            if selected_mode != suite.manifest.server_mode {
                bail!("selected suites must use the same server mode");
            }
        } else {
            server_mode = Some(suite.manifest.server_mode);
        }
    }

    Ok(suite_names)
}

#[cfg(test)]
mod tests {
    use super::{ArtifactFlavor, SuiteManifest, SuiteServerMode, select_suite_names};
    use crate::types::SuiteConfig;
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    fn fixture_suites<const N: usize>(entries: [(&str, bool); N]) -> BTreeMap<String, SuiteConfig> {
        entries
            .into_iter()
            .map(|(name, explicit_only)| {
                let name = name.to_string();
                let manifest = SuiteManifest {
                    explicit_only,
                    ..SuiteManifest::default()
                };
                (
                    name.clone(),
                    SuiteConfig {
                        name,
                        sql_dir: PathBuf::new(),
                        result_dir: None,
                        sql_glob: "*.sql".to_string(),
                        default_catalog: "default_catalog".to_string(),
                        default_db: String::new(),
                        auto_case_db: false,
                        verify_default: true,
                        init_sql: None,
                        cleanup_sql: None,
                        manifest,
                    },
                )
            })
            .collect()
    }

    #[test]
    fn starrocks_compat_manifest_is_explicit_real_fe_three_be() {
        let manifest = SuiteManifest::parse(
            r#"explicit_only = true
server_mode = "starrocks-compat"
cluster_size = 3
artifact_flavor = "compat"
"#,
        )
        .unwrap();
        assert!(manifest.explicit_only);
        assert_eq!(manifest.server_mode, SuiteServerMode::StarRocksCompat);
        assert_eq!(manifest.cluster_size, 3);
        assert_eq!(manifest.artifact_flavor, ArtifactFlavor::Compat);
    }

    #[test]
    fn starrocks_compat_rejects_default_discovery_contracts() {
        for bad in [
            "explicit_only = false\nserver_mode = \"starrocks-compat\"\ncluster_size = 3\nartifact_flavor = \"compat\"\n",
            "explicit_only = true\nserver_mode = \"starrocks-compat\"\ncluster_size = 1\nartifact_flavor = \"compat\"\n",
            "explicit_only = true\nserver_mode = \"starrocks-compat\"\ncluster_size = 3\nartifact_flavor = \"default\"\n",
        ] {
            assert!(SuiteManifest::parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn all_selection_excludes_explicit_only_suites() {
        let suites = fixture_suites([("filter", false), ("starrocks-compat", true)]);
        assert_eq!(select_suite_names("all", &suites).unwrap(), vec!["filter"]);
        assert_eq!(
            select_suite_names("starrocks-compat", &suites).unwrap(),
            vec!["starrocks-compat"]
        );
    }

    #[test]
    fn native_manifest_defaults_when_suite_toml_is_absent() {
        let temp_dir =
            std::env::temp_dir().join(format!("novarocks-suite-manifest-{}", std::process::id()));
        let manifest = SuiteManifest::load(&temp_dir.join("missing-suite.toml")).unwrap();
        assert_eq!(manifest, SuiteManifest::default());
    }

    #[test]
    fn manifest_rejects_zero_cluster_size() {
        assert!(SuiteManifest::parse("cluster_size = 0\n").is_err());
    }

    #[test]
    fn mixed_server_modes_are_rejected() {
        let mut suites = fixture_suites([("filter", false), ("starrocks-compat", true)]);
        suites
            .get_mut("starrocks-compat")
            .unwrap()
            .manifest
            .server_mode = SuiteServerMode::StarRocksCompat;
        assert!(select_suite_names("filter,starrocks-compat", &suites).is_err());
    }
}
