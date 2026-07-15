use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use super::{
    rel, rs_files, rust_canonical_use_segments, rust_module_items, rust_production_canonical_paths,
    rust_raw_production_use_statements, rust_sanitized_production_text,
};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct EngineFileOwner {
    path: &'static str,
    target_owner: &'static str,
    migration_task: &'static str,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct EngineBoundarySnapshot {
    engine_files: BTreeSet<String>,
    engine_module_declarations: BTreeSet<String>,
    external_engine_dependencies: BTreeMap<String, BTreeSet<String>>,
    standalone_state_dependencies: BTreeMap<String, BTreeSet<String>>,
    forwarding_reexports: BTreeSet<String>,
    lower_layer_frontend_dependencies: BTreeSet<String>,
}

#[derive(Clone, Debug)]
struct EngineBoundaryBaseline {
    file_owners: &'static [EngineFileOwner],
    engine_module_declarations: &'static [&'static str],
    external_engine_dependencies: &'static [(&'static str, &'static [&'static str])],
    standalone_state_dependencies: &'static [(&'static str, &'static [&'static str])],
    forwarding_reexports: &'static [&'static str],
}

const EMPTY_BASELINE: EngineBoundaryBaseline = EngineBoundaryBaseline {
    file_owners: &[],
    engine_module_declarations: &[],
    external_engine_dependencies: &[],
    standalone_state_dependencies: &[],
    forwarding_reexports: &[],
};

#[derive(Clone, Debug)]
struct GuardSource {
    path: String,
    text: String,
}

impl GuardSource {
    fn new(path: &str, text: &str) -> Self {
        Self {
            path: path.to_string(),
            text: text.to_string(),
        }
    }
}

fn is_engine_path(path: &[String]) -> bool {
    path.len() >= 2 && path[0] == "crate" && path[1] == "engine"
}

fn is_standalone_state_path(path: &[String]) -> bool {
    is_engine_path(path) && path.last().is_some_and(|item| item == "StandaloneState")
}

fn is_top_level_frontend_path(path: &[String]) -> bool {
    path.len() >= 2 && path[0] == "crate" && path[1] == "frontend"
}

fn source_is_lower_layer(path: &str) -> bool {
    ["src/sql/", "src/exec/", "src/connector/", "src/meta/"]
        .iter()
        .any(|prefix| path.starts_with(prefix))
}

fn source_is_engine(path: &str) -> bool {
    path == "src/engine.rs" || path.starts_with("src/engine/")
}

fn insert_dependency(
    dependencies: &mut BTreeMap<String, BTreeSet<String>>,
    source: &str,
    dependency: String,
) {
    dependencies
        .entry(source.to_string())
        .or_default()
        .insert(dependency);
}

fn remove_redundant_descendant_paths(paths: BTreeSet<Vec<String>>) -> BTreeSet<Vec<String>> {
    paths
        .iter()
        .filter(|path| {
            !paths.iter().any(|candidate| {
                candidate.len() < path.len() && path.starts_with(candidate.as_slice())
            })
        })
        .cloned()
        .collect()
}

fn collect_source_dependencies(snapshot: &mut EngineBoundarySnapshot, source: &GuardSource) {
    let production = rust_sanitized_production_text(&source.text);
    let canonical_paths = rust_production_canonical_paths(&production, &source.path)
        .into_iter()
        .collect::<BTreeSet<_>>();

    if !source_is_engine(&source.path) {
        for path in remove_redundant_descendant_paths(
            canonical_paths
                .iter()
                .filter(|path| is_engine_path(path))
                .cloned()
                .collect(),
        ) {
            insert_dependency(
                &mut snapshot.external_engine_dependencies,
                &source.path,
                path.join("::"),
            );
        }
    }

    for path in canonical_paths
        .iter()
        .filter(|path| is_standalone_state_path(path))
    {
        insert_dependency(
            &mut snapshot.standalone_state_dependencies,
            &source.path,
            path.join("::"),
        );
    }

    if source_is_lower_layer(&source.path) {
        snapshot.lower_layer_frontend_dependencies.extend(
            canonical_paths
                .iter()
                .filter(|path| is_top_level_frontend_path(path))
                .map(|path| format!("{}|{}", source.path, path.join("::"))),
        );
    }

    for raw in rust_raw_production_use_statements(&production)
        .into_iter()
        .filter(|raw| raw.visibility != "private")
    {
        let import = raw.path.segments.join("::");
        let Some(target) = rust_canonical_use_segments(&import, &source.path) else {
            continue;
        };
        let source_engine = source_is_engine(&source.path);
        let target_engine = is_engine_path(&target);
        if source_engine != target_engine {
            snapshot.forwarding_reexports.insert(format!(
                "{}|{}|{}",
                source.path,
                raw.visibility,
                target.join("::")
            ));
        }
    }
}

fn collect_engine_module_declarations(
    snapshot: &mut EngineBoundarySnapshot,
    source_path: &str,
    text: &str,
) {
    let production = rust_sanitized_production_text(text);
    for item in rust_module_items(&production) {
        let inline_scope = item
            .inline_modules
            .iter()
            .map(|module| module.name.as_str())
            .collect::<Vec<_>>()
            .join("::");
        let kind = if item.is_external {
            "external"
        } else {
            "inline"
        };
        snapshot
            .engine_module_declarations
            .insert(format!("{source_path}|{inline_scope}|{kind}|{}", item.name));
    }
}

fn collect_engine_boundary_snapshot(
    repo: &Path,
    sources: &[GuardSource],
) -> EngineBoundarySnapshot {
    let mut snapshot = EngineBoundarySnapshot::default();
    for source in sources {
        collect_source_dependencies(&mut snapshot, source);
    }

    let engine_dir = repo.join("src/engine");
    if engine_dir.is_dir() {
        for path in rs_files(&engine_dir) {
            let source_path = rel(&path);
            snapshot.engine_files.insert(source_path.clone());
            let text = fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("failed to read {source_path}: {error}"));
            collect_engine_module_declarations(&mut snapshot, &source_path, &text);
        }
    } else {
        for source in sources
            .iter()
            .filter(|source| source_is_engine(&source.path))
        {
            snapshot.engine_files.insert(source.path.clone());
            collect_engine_module_declarations(&mut snapshot, &source.path, &source.text);
        }
    }
    snapshot
}

fn baseline_map(
    entries: &'static [(&'static str, &'static [&'static str])],
) -> BTreeMap<String, BTreeSet<String>> {
    entries
        .iter()
        .map(|(source, dependencies)| {
            (
                (*source).to_string(),
                dependencies
                    .iter()
                    .map(|dependency| (*dependency).to_string())
                    .collect(),
            )
        })
        .collect()
}

fn append_set_diff(
    violations: &mut Vec<String>,
    actual: &BTreeSet<String>,
    expected: &BTreeSet<String>,
    missing_prefix: &str,
    unexpected_prefix: &str,
) {
    violations.extend(
        expected
            .difference(actual)
            .map(|item| format!("{missing_prefix} {item}")),
    );
    violations.extend(
        actual
            .difference(expected)
            .map(|item| format!("{unexpected_prefix} {item}")),
    );
}

fn append_map_diff(
    violations: &mut Vec<String>,
    actual: &BTreeMap<String, BTreeSet<String>>,
    expected: &BTreeMap<String, BTreeSet<String>>,
    missing_prefix: &str,
    unexpected_prefix: &str,
) {
    let actual = actual
        .iter()
        .flat_map(|(source, dependencies)| {
            dependencies
                .iter()
                .map(move |dependency| format!("{source}|{dependency}"))
        })
        .collect::<BTreeSet<_>>();
    let expected = expected
        .iter()
        .flat_map(|(source, dependencies)| {
            dependencies
                .iter()
                .map(move |dependency| format!("{source}|{dependency}"))
        })
        .collect::<BTreeSet<_>>();
    append_set_diff(
        violations,
        &actual,
        &expected,
        missing_prefix,
        unexpected_prefix,
    );
}

fn engine_boundary_violations(
    actual: &EngineBoundarySnapshot,
    baseline: &EngineBoundaryBaseline,
) -> Vec<String> {
    let expected_files = baseline
        .file_owners
        .iter()
        .map(|owner| owner.path.to_string())
        .collect::<BTreeSet<_>>();
    let missing_files = expected_files
        .difference(&actual.engine_files)
        .collect::<BTreeSet<_>>();
    let mut violations = baseline
        .file_owners
        .iter()
        .filter(|owner| missing_files.contains(&owner.path.to_string()))
        .map(|owner| {
            format!(
                "engine-file-missing: {}|target-owner={}|migration-task={}",
                owner.path, owner.target_owner, owner.migration_task
            )
        })
        .collect::<Vec<_>>();
    violations.extend(
        actual
            .engine_files
            .difference(&expected_files)
            .map(|path| format!("engine-file-unexpected: {path}")),
    );

    let expected_modules = baseline
        .engine_module_declarations
        .iter()
        .map(|item| (*item).to_string())
        .collect::<BTreeSet<_>>();
    append_set_diff(
        &mut violations,
        &actual.engine_module_declarations,
        &expected_modules,
        "engine-module-missing:",
        "engine-module-unexpected:",
    );
    append_map_diff(
        &mut violations,
        &actual.external_engine_dependencies,
        &baseline_map(baseline.external_engine_dependencies),
        "engine-dependency-missing:",
        "engine-dependency-unexpected:",
    );
    append_map_diff(
        &mut violations,
        &actual.standalone_state_dependencies,
        &baseline_map(baseline.standalone_state_dependencies),
        "standalone-state-missing:",
        "standalone-state-unexpected:",
    );
    let expected_reexports = baseline
        .forwarding_reexports
        .iter()
        .map(|item| (*item).to_string())
        .collect::<BTreeSet<_>>();
    append_set_diff(
        &mut violations,
        &actual.forwarding_reexports,
        &expected_reexports,
        "forwarding-reexport-missing:",
        "forwarding-reexport-unexpected:",
    );
    violations.extend(
        actual
            .lower_layer_frontend_dependencies
            .iter()
            .map(|item| format!("frontend-reverse-dependency: {item}")),
    );
    violations
}

#[test]
fn ebd_1_detector_rejects_new_engine_dependencies_and_forwarding() {
    let sources = vec![
        GuardSource::new(
            "src/sql/example.rs",
            r#"
use crate::engine::{catalog::TableDef, StandaloneState as State};
use crate::engine::mv as legacy_mv;
fn inspect(_: &State) { let _ = legacy_mv::table_ref::IcebergTableRef::default; }
"#,
        ),
        GuardSource::new(
            "src/catalog/legacy.rs",
            "pub(crate) use crate::engine::catalog::*;",
        ),
    ];
    let actual =
        collect_engine_boundary_snapshot(Path::new("fixture-root-that-does-not-exist"), &sources);
    let violations = engine_boundary_violations(&actual, &EMPTY_BASELINE);
    assert!(
        violations
            .iter()
            .any(|item| item.contains("crate::engine::catalog")),
        "grouped engine import must be rejected: {violations:?}"
    );
    assert!(
        violations
            .iter()
            .any(|item| item.contains("StandaloneState")),
        "StandaloneState alias must be rejected: {violations:?}"
    );
    assert!(
        violations
            .iter()
            .any(|item| item.contains("forwarding-reexport")),
        "public forwarding must be rejected: {violations:?}"
    );
}

#[test]
fn ebd_1_detector_ignores_test_noise_and_resolves_relative_aliases() {
    let source = GuardSource::new(
        "src/sql/nested/example.rs",
        r#"
// use crate::engine::catalog::TableDef;
const TEXT: &str = "crate::engine::StandaloneState";
#[cfg(test)]
mod tests { use crate::engine::StandaloneState; }
use crate::engine::catalog as legacy_catalog;
fn production() { let _ = legacy_catalog::normalize_identifier("x"); }
"#,
    );
    let actual =
        collect_engine_boundary_snapshot(Path::new("fixture-root-that-does-not-exist"), &[source]);
    let paths = actual
        .external_engine_dependencies
        .get("src/sql/nested/example.rs")
        .expect("production alias dependency");
    assert_eq!(
        paths,
        &BTreeSet::from(["crate::engine::catalog".to_string()])
    );
    assert!(actual.standalone_state_dependencies.is_empty());
}
