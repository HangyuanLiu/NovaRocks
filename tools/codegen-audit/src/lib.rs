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

mod cfg;
mod module_graph;
mod retirement;

use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use syn::visit::Visit;

use crate::cfg::production_possible;
use crate::module_graph::{GraphOptions, build_module_graph};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Violation {
    source: String,
    message: String,
}

impl Violation {
    pub fn new(source: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for Violation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.source, self.message)
    }
}

struct NativeDependencyVisitor {
    production: bool,
    violations: Vec<Violation>,
    source: String,
}

impl<'ast> Visit<'ast> for NativeDependencyVisitor {
    fn visit_item(&mut self, item: &'ast syn::Item) {
        let attrs: &[syn::Attribute] = match item {
            syn::Item::Const(item) => &item.attrs,
            syn::Item::Enum(item) => &item.attrs,
            syn::Item::ExternCrate(item) => &item.attrs,
            syn::Item::Fn(item) => &item.attrs,
            syn::Item::ForeignMod(item) => &item.attrs,
            syn::Item::Impl(item) => &item.attrs,
            syn::Item::Macro(item) => &item.attrs,
            syn::Item::Mod(item) => &item.attrs,
            syn::Item::Static(item) => &item.attrs,
            syn::Item::Struct(item) => &item.attrs,
            syn::Item::Trait(item) => &item.attrs,
            syn::Item::TraitAlias(item) => &item.attrs,
            syn::Item::Type(item) => &item.attrs,
            syn::Item::Union(item) => &item.attrs,
            syn::Item::Use(item) => &item.attrs,
            _ => &[],
        };
        if !production_possible(attrs).unwrap_or(true) {
            return;
        }
        syn::visit::visit_item(self, item);
    }

    fn visit_path(&mut self, path: &'ast syn::Path) {
        if !self.production {
            return;
        }
        let joined = path
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect::<Vec<_>>()
            .join("::");
        for forbidden in [
            "OptimizerPhysicalNode",
            "optimizer::operator::Operator",
            "optimizer::physical_tree",
        ] {
            if joined.contains(forbidden) {
                self.violations.push(Violation::new(
                    &self.source,
                    format!("native encoder production code references {forbidden}"),
                ));
            }
        }
        syn::visit::visit_path(self, path);
    }
}

pub fn audit_native_encoder(repo: &Path) -> Result<Vec<Violation>> {
    let encoder = repo.join("src/protocol/native/encode");
    let graph = build_module_graph(
        &encoder,
        &[(
            encoder.join("mod.rs"),
            vec![
                "crate".to_string(),
                "protocol".to_string(),
                "native".to_string(),
                "encode".to_string(),
            ],
        )],
        GraphOptions {
            forbid_production_path: true,
        },
    )?;
    let mut violations = Vec::new();
    for unit in graph.units {
        let mut visitor = NativeDependencyVisitor {
            production: true,
            violations: Vec::new(),
            source: unit.path.display().to_string(),
        };
        visitor.visit_file(&unit.file);
        violations.extend(visitor.violations);
    }

    let seal = fs::read_to_string(repo.join("src/sql/planner/distributed/seal.rs"))
        .context("read distributed plan seal owner")?;
    if seal.contains("scalar_arena") {
        violations.push(Violation::new(
            "src/sql/planner/distributed/seal.rs",
            "DistributedPlan must not carry scalar_arena",
        ));
    }
    let planner_root = repo.join("src/sql/planner");
    for path in rust_files(&planner_root)? {
        let source = fs::read_to_string(&path)?;
        if source.contains("enum DistributedPlanKind") {
            violations.push(Violation::new(
                path.display().to_string(),
                "DistributedPlanKind must not be reintroduced",
            ));
        }
        if source.contains("struct PlanNodeStats") {
            violations.push(Violation::new(
                path.display().to_string(),
                "migration PlanNodeStats must not be reintroduced",
            ));
        }
    }
    if !repo
        .join("src/sql/planner/optimizer_bridge/id_binding.rs")
        .is_file()
    {
        violations.push(Violation::new(
            "src/sql/planner/optimizer_bridge/id_binding.rs",
            "id binding verification must remain under planner::optimizer_bridge",
        ));
    }
    violations.sort();
    violations.dedup();
    Ok(violations)
}

pub fn audit_sql_codegen_retirement(repo: &Path) -> Result<Vec<Violation>> {
    let mut violations = Vec::new();
    for retired in ["src/sql/codegen", "src/sql/codegen.rs"] {
        if fs::symlink_metadata(repo.join(retired)).is_ok() {
            violations.push(Violation::new(retired, "retired path still exists"));
        }
    }
    let src = repo.join("src");
    let graph = build_module_graph(
        &src,
        &[
            (src.join("lib.rs"), vec!["crate".to_string()]),
            (src.join("main.rs"), vec!["crate".to_string()]),
        ],
        GraphOptions {
            forbid_production_path: false,
        },
    )?;
    violations.extend(retirement::audit_graph(&graph)?);
    violations.sort();
    violations.dedup();
    Ok(violations)
}

pub fn audit_repo(repo: &Path) -> Result<Vec<Violation>> {
    let mut violations = audit_native_encoder(repo)?;
    violations.extend(audit_sql_codegen_retirement(repo)?);
    violations.sort();
    violations.dedup();
    Ok(violations)
}

fn rust_files(root: &Path) -> Result<Vec<PathBuf>> {
    fn visit(path: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
        for entry in fs::read_dir(path)
            .with_context(|| format!("read source directory {}", path.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                visit(&path, files)?;
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                files.push(path);
            }
        }
        Ok(())
    }
    let mut files = Vec::new();
    visit(root, &mut files)?;
    files.sort();
    Ok(files)
}

fn write_native_fixture(repo: &Path, encode_mod: &str) -> Result<()> {
    fs::create_dir_all(repo.join("src/protocol/native/encode"))?;
    fs::create_dir_all(repo.join("src/sql/planner/distributed"))?;
    fs::create_dir_all(repo.join("src/sql/planner/optimizer_bridge"))?;
    fs::write(repo.join("src/protocol/native/encode/mod.rs"), encode_mod)?;
    fs::write(repo.join("src/sql/planner/distributed/seal.rs"), "")?;
    fs::write(
        repo.join("src/sql/planner/optimizer_bridge/id_binding.rs"),
        "",
    )?;
    Ok(())
}

fn write_retirement_fixture(repo: &Path, lib: &str) -> Result<()> {
    fs::create_dir_all(repo.join("src"))?;
    fs::write(repo.join("src/lib.rs"), lib)?;
    Ok(())
}

pub fn run_self_tests() -> Result<()> {
    let fixture = tempfile::tempdir()?;
    write_native_fixture(
        fixture.path(),
        r#"
#[cfg(test)]
macro_rules! test_helper { () => {} }
fn forbidden(_: OptimizerPhysicalNode) {}
"#,
    )?;
    assert!(!audit_native_encoder(fixture.path())?.is_empty());

    let fixture = tempfile::tempdir()?;
    write_native_fixture(
        fixture.path(),
        r#"
mod layer { mod hidden; }
"#,
    )?;
    fs::create_dir_all(fixture.path().join("src/protocol/native/encode/layer"))?;
    fs::write(
        fixture
            .path()
            .join("src/protocol/native/encode/layer/hidden.rs"),
        "fn forbidden(_: OptimizerPhysicalNode) {}",
    )?;
    assert!(!audit_native_encoder(fixture.path())?.is_empty());

    let fixture = tempfile::tempdir()?;
    write_native_fixture(
        fixture.path(),
        "#[cfg_attr(not(test), cfg(test))] mod hidden;\nfn production() {}",
    )?;
    fs::write(
        fixture.path().join("src/protocol/native/encode/hidden.rs"),
        "fn forbidden(_: OptimizerPhysicalNode) {}",
    )?;
    assert!(audit_native_encoder(fixture.path())?.is_empty());

    for attribute in [
        r#"#[cfg_attr(not(test), path = "prod.rs")] mod owner;"#,
        r#"#[cfg_attr(not(test), cfg_attr(feature = "prod", path = "prod.rs"))] mod owner;"#,
        r#"#[cfg_attr(any(test, feature = "prod"), path = "prod.rs")] mod owner;"#,
        r#"#[cfg_attr(all(not(test), any(feature = "a", feature = "b")), path = "prod.rs")] mod owner;"#,
    ] {
        let fixture = tempfile::tempdir()?;
        write_native_fixture(fixture.path(), attribute)?;
        fs::write(
            fixture.path().join("src/protocol/native/encode/owner.rs"),
            "fn production() {}",
        )?;
        fs::write(
            fixture.path().join("src/protocol/native/encode/prod.rs"),
            "fn forbidden(_: OptimizerPhysicalNode) {}",
        )?;
        assert!(audit_native_encoder(fixture.path()).is_err());
    }

    for attribute in [
        r#"#[cfg_attr(test, path = "tests.rs")] mod owner;"#,
        r#"#[cfg_attr(test, cfg_attr(not(test), path = "never.rs"))] mod owner;"#,
        r#"#[cfg_attr(not(not(test)), path = "tests.rs")] mod owner;"#,
        r#"
#[doc = "cfg_attr(not(test), path = prod.rs)"]
// #[path = "prod.rs"]
mod owner;
"#,
    ] {
        let fixture = tempfile::tempdir()?;
        write_native_fixture(fixture.path(), attribute)?;
        fs::write(
            fixture.path().join("src/protocol/native/encode/owner.rs"),
            "fn production() {}",
        )?;
        assert!(audit_native_encoder(fixture.path())?.is_empty());
    }

    let invalid_retirement = [
        "pub mod sql { pub mod codegen; }",
        "pub mod sql { use crate::protocol as codegen; }",
        "pub mod sql { include!(\"codegen/mod.rs\"); }",
        r#"
macro_rules! source_loader {
    () => { include!(concat!("code", "gen/mod.rs")); };
}
macro_rules! inject_with {
    ("safe", $m:ident) => { println!("safe"); };
    ("load", $m:ident) => { $m!(); };
}
inject_with!("load", source_loader);
"#,
        r#"
use std::include as inject;
macro_rules! inner { ($m:tt) => { $m!("codegen/mod.rs"); }; }
macro_rules! outer { ($m:path) => { inner!($m); }; }
outer!(inject);
"#,
        r#"unknown_wrapper!(include);"#,
    ];
    for source in invalid_retirement {
        let fixture = tempfile::tempdir()?;
        write_retirement_fixture(fixture.path(), source)?;
        let detected = match audit_sql_codegen_retirement(fixture.path()) {
            Ok(violations) => !violations.is_empty(),
            Err(_) => true,
        };
        assert!(detected, "retirement fixture escaped: {source}");
    }

    let fixture = tempfile::tempdir()?;
    write_retirement_fixture(
        fixture.path(),
        r#"
mod macros;
mod sql { crate::inject_with!("load", include); }
"#,
    )?;
    fs::write(
        fixture.path().join("src/macros.rs"),
        r#"
#[macro_export]
macro_rules! source_loader {
    () => { include!("codegen/mod.rs"); };
}
#[macro_export]
macro_rules! inject_with {
    ("safe", $m:ident) => { println!("safe"); };
    ("load", $m:path) => { $m!(); };
}
"#,
    )?;
    fs::write(
        fixture.path().join("src/lib.rs"),
        r#"
mod macros;
mod sql { crate::inject_with!("load", crate::source_loader); }
"#,
    )?;
    assert!(!audit_sql_codegen_retirement(fixture.path())?.is_empty());

    let valid_retirement = [
        r#"
#[cfg(test)]
macro_rules! inject_with { ($m:ident) => { $m!("codegen/mod.rs"); }; }
#[cfg(test)]
inject_with!(include);
"#,
        r#"
mod runtime {
    macro_rules! inject_with { ($m:ident) => { $m!("safe.rs"); }; }
    inject_with!(include);
}
"#,
        r#"
macro_rules! ordinary { ("load", $m:ident) => { $m!("ordinary"); }; }
ordinary!("load", println);
"#,
        r#"
macro_rules! source_loader { () => { include!("codegen/mod.rs"); }; }
macro_rules! inject_with {
    ("safe", $m:ident) => { println!("safe"); };
    ("load", $m:ident) => { $m!(); };
}
inject_with!("safe", source_loader);
"#,
    ];
    for source in valid_retirement {
        let fixture = tempfile::tempdir()?;
        write_retirement_fixture(fixture.path(), source)?;
        assert!(
            audit_sql_codegen_retirement(fixture.path())?.is_empty(),
            "valid retirement fixture rejected: {source}"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn shared_self_tests_pass() {
        super::run_self_tests().unwrap();
    }
}
