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

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use syn::{Item, ItemMod};

use crate::cfg::{CfgExpr, analyze_attrs};
use crate::production::validate_file;

#[derive(Clone)]
pub(crate) struct SourceUnit {
    pub(crate) path: PathBuf,
    pub(crate) scope: Vec<String>,
    pub(crate) file: syn::File,
}

pub(crate) struct ModuleGraph {
    pub(crate) units: Vec<SourceUnit>,
}

pub(crate) struct GraphOptions {
    pub(crate) forbid_production_path: bool,
}

struct Builder<'a> {
    boundary: &'a Path,
    options: GraphOptions,
    units: Vec<SourceUnit>,
    active: BTreeSet<PathBuf>,
    visited: BTreeSet<PathBuf>,
}

impl Builder<'_> {
    fn canonical_in_boundary(&self, path: &Path) -> Result<PathBuf> {
        let canonical = path
            .canonicalize()
            .with_context(|| format!("resolve module source {}", path.display()))?;
        if !canonical.starts_with(self.boundary) {
            bail!("module source escapes audit boundary: {}", path.display());
        }
        Ok(canonical)
    }

    fn visit_file(&mut self, path: &Path, scope: Vec<String>, module_dir: PathBuf) -> Result<()> {
        let canonical = self.canonical_in_boundary(path)?;
        if !self.active.insert(canonical.clone()) {
            bail!("module graph cycle at {}", path.display());
        }
        if !self.visited.insert(canonical.clone()) {
            bail!(
                "module source is reachable more than once: {}",
                path.display()
            );
        }
        let source = fs::read_to_string(&canonical)
            .with_context(|| format!("read module source {}", canonical.display()))?;
        let file = syn::parse_file(&source)
            .with_context(|| format!("parse Rust source {}", canonical.display()))?;
        validate_file(&file)
            .with_context(|| format!("validate production cfg in {}", canonical.display()))?;
        self.units.push(SourceUnit {
            path: canonical.clone(),
            scope: scope.clone(),
            file: file.clone(),
        });
        self.walk_items(&file.items, &canonical, &module_dir, &scope, CfgExpr::True)?;
        self.active.remove(&canonical);
        Ok(())
    }

    fn walk_items(
        &mut self,
        items: &[Item],
        source_path: &Path,
        module_dir: &Path,
        scope: &[String],
        parent_condition: CfgExpr,
    ) -> Result<()> {
        for item in items {
            let Item::Mod(module) = item else {
                continue;
            };
            let attrs = analyze_attrs(&module.attrs)?;
            let condition = parent_condition.clone().and(attrs.item_condition.clone());
            if !condition.production_possible()? {
                continue;
            }
            let name = module.ident.to_string();
            let mut child_scope = scope.to_vec();
            child_scope.push(name.clone());
            if let Some((_, content)) = &module.content {
                self.walk_items(
                    content,
                    source_path,
                    &module_dir.join(&name),
                    &child_scope,
                    condition,
                )?;
                continue;
            }
            self.visit_external_module(
                module,
                source_path,
                module_dir,
                &child_scope,
                condition,
                attrs,
            )?;
        }
        Ok(())
    }

    fn visit_external_module(
        &mut self,
        module: &ItemMod,
        source_path: &Path,
        module_dir: &Path,
        child_scope: &[String],
        item_condition: CfgExpr,
        attrs: crate::cfg::AttrAnalysis,
    ) -> Result<()> {
        let mut production_paths = Vec::new();
        for (path, activation) in attrs.path_values.iter().zip(&attrs.path_conditions) {
            if item_condition
                .clone()
                .and(activation.clone())
                .production_possible()?
            {
                production_paths.push(path.clone());
            }
        }
        if self.options.forbid_production_path && !production_paths.is_empty() {
            bail!(
                "production-possible #[path] indirection on module `{}` in {}",
                module.ident,
                source_path.display()
            );
        }

        let candidates = if production_paths.is_empty() {
            let name = module.ident.to_string();
            vec![
                module_dir.join(format!("{name}.rs")),
                module_dir.join(&name).join("mod.rs"),
            ]
        } else {
            production_paths
                .into_iter()
                .map(|path| source_path.parent().unwrap_or(Path::new("")).join(path))
                .collect()
        };
        let existing = candidates
            .into_iter()
            .filter(|candidate| candidate.exists())
            .collect::<Vec<_>>();
        if existing.len() != 1 {
            bail!(
                "module `{}` from {} has {} production source candidates",
                module.ident,
                source_path.display(),
                existing.len()
            );
        }
        let child = &existing[0];
        let child_dir = if child.file_name().is_some_and(|name| name == "mod.rs") {
            child.parent().unwrap_or(module_dir).to_path_buf()
        } else {
            child
                .parent()
                .unwrap_or(module_dir)
                .join(module.ident.to_string())
        };
        self.visit_file(child, child_scope.to_vec(), child_dir)
    }
}

pub(crate) fn build_module_graph(
    boundary: &Path,
    entries: &[(PathBuf, Vec<String>)],
    options: GraphOptions,
) -> Result<ModuleGraph> {
    let boundary = boundary
        .canonicalize()
        .with_context(|| format!("resolve graph boundary {}", boundary.display()))?;
    let mut builder = Builder {
        boundary: &boundary,
        options,
        units: Vec::new(),
        active: BTreeSet::new(),
        visited: BTreeSet::new(),
    };
    for (entry, scope) in entries {
        if !entry.exists() {
            continue;
        }
        let module_dir = entry.parent().unwrap_or(&boundary).to_path_buf();
        builder.visit_file(entry, scope.clone(), module_dir)?;
    }
    if builder.units.is_empty() {
        bail!("production module graph is empty");
    }
    Ok(ModuleGraph {
        units: builder.units,
    })
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn inline_module_external_child_is_reachable() {
        let root = tempdir().unwrap();
        fs::write(root.path().join("mod.rs"), "mod layer { mod hidden; }").unwrap();
        fs::create_dir(root.path().join("layer")).unwrap();
        fs::write(root.path().join("layer/hidden.rs"), "pub struct Marker;").unwrap();
        let graph = build_module_graph(
            root.path(),
            &[(root.path().join("mod.rs"), vec!["crate".to_string()])],
            GraphOptions {
                forbid_production_path: true,
            },
        )
        .unwrap();
        assert_eq!(graph.units.len(), 2);
        assert_eq!(graph.units[1].scope, ["crate", "layer", "hidden"]);
    }

    #[test]
    fn generated_cfg_test_excludes_external_module() {
        let root = tempdir().unwrap();
        fs::write(
            root.path().join("mod.rs"),
            "#[cfg_attr(not(test), cfg(test))] mod hidden;",
        )
        .unwrap();
        fs::write(
            root.path().join("hidden.rs"),
            "compile_error!(\"test only\");",
        )
        .unwrap();
        let graph = build_module_graph(
            root.path(),
            &[(root.path().join("mod.rs"), vec!["crate".to_string()])],
            GraphOptions {
                forbid_production_path: true,
            },
        )
        .unwrap();
        assert_eq!(graph.units.len(), 1);
    }
}
