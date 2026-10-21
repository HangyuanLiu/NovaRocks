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

//! One admitted invocation clock and collision-free bounded SQL resource names.
use crate::types::{RunnerConfig, SuiteConfig};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::fixture_run::{
    FixturePolicy, FixtureRunContext, RunBudget, ScratchIdentity, rejected,
};
use regex::Regex;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;

const CAPACITY: usize = 36 * 36 * 36;
#[derive(Debug, Clone)]
pub(crate) struct RunNames {
    pub run_id: String,
    pub scope: String,
    pub suites: BTreeMap<String, SuiteNames>,
    pub catalogs: crate::catalog_symbol::CatalogSymbols,
    hms_warehouses: BTreeMap<String, String>,
}
#[derive(Debug, Clone)]
pub(crate) struct SuiteNames {
    pub ordinal: usize,
    pub cases: BTreeMap<String, usize>,
}
pub(crate) fn ordinal(value: usize) -> Result<String> {
    ensure!(
        value < CAPACITY,
        "FixtureBudgetRejected: SQL resource ordinal exceeds capacity"
    );
    let digit = |n: usize| char::from(b"0123456789abcdefghijklmnopqrstuvwxyz"[n]);
    Ok(format!(
        "{}{}{}",
        digit(value / 1296),
        digit(value / 36 % 36),
        digit(value % 36)
    ))
}
pub(crate) fn resource(run: &str, kind: char, index: usize, slot: usize) -> Result<String> {
    ensure!(
        slot < 10,
        "FixtureBudgetRejected: SQL resource slot exceeds capacity"
    );
    let result = format!("{run}_{kind}{}{slot}", ordinal(index)?);
    ensure!(
        result.len() <= 63,
        "FixtureBudgetRejected: SQL resource name exceeds 63 bytes"
    );
    Ok(result)
}
fn sql_namespace_declarations(source: &str) -> Result<BTreeSet<String>> {
    // Only explicit, line-start SQL DDL declares SQL namespace write ownership.
    let re = Regex::new(
        r"(?im)^\s*CREATE\s+DATABASE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:`?[A-Za-z0-9_$\{\}]+`?\s*\.\s*)?`?([A-Za-z0-9_$\{\}]+)`?\s*;",
    )?;
    Ok(re.captures_iter(source).map(|c| c[1].to_string()).collect())
}
// Recognize only the existing versioned Variant producer command, not generic
// shell --namespace arguments, command substitutions or lookalike executables.
fn variant_producer_command() -> Result<Regex> {
    Ok(Regex::new(
        r#"^\s*"\$\{NOVAROCKS_WORKSPACE_ROOT:-\.\}/tests/sql/fixtures/iceberg-variant/run\.sh"\s+--mode\s+(unshredded|shredded|bounds|nested|uuid|write-target|empty-int|empty-bigint|empty-region|observe|unchanged)\s+--namespace\s+([A-Za-z0-9_${}]+)\s+--table\s+[A-Za-z0-9_]+\s+--output\s+"\$receipt_dir"(?:\s+>"\$receipt_dir/run\.log")?\s*$"#,
    )?)
}
// These match the complete, versioned producer commands. Read/cleanup modes
// refer to a declaration from the same source; they never mint another scope.
fn hms_producer_command() -> Result<Regex> {
    Ok(Regex::new(
        r#"^\s*python3\s+"\$\{NOVAROCKS_WORKSPACE_ROOT:-\.\}/tests/sql/fixtures/iceberg-hms/fixture\.py"\s+(prepare|before|after|cleanup)\s+([A-Za-z0-9_${}]+)\s*$"#,
    )?)
}
fn trino_producer_command() -> Result<Regex> {
    Ok(Regex::new(
        r#"^\s*"\$\{NOVAROCKS_WORKSPACE_ROOT:-\.\}/tests/datasketches-tck/interop/trino/verify_rest_catalog\.sh"\s+(create-parent|verify-current)\s+([A-Za-z0-9_${}]+)\s+[A-Za-z0-9_${}]+(?:\s+id\s+4\s+3\s+1\s+3)?\s*$"#,
    )?)
}
fn shell_line<'a>(line: &'a str, shell: &mut bool) -> Option<&'a str> {
    let trimmed = line.trim_start();
    if trimmed.to_ascii_lowercase().starts_with("-- query ") {
        *shell = false;
    }
    if let Some(command) = trimmed.strip_prefix("shell:") {
        *shell = true;
        Some(command)
    } else if *shell {
        Some(line)
    } else {
        None
    }
}
pub(crate) fn namespace_declarations(source: &str) -> Result<BTreeSet<String>> {
    let mut declared = sql_namespace_declarations(source)?;
    let producer = variant_producer_command()?;
    let hms = hms_producer_command()?;
    let trino = trino_producer_command()?;
    let mut shell = false;
    for line in source.lines() {
        if let Some(command) = shell_line(line, &mut shell) {
            if let Some(capture) = producer.captures(command) {
                if !matches!(&capture[1], "observe" | "unchanged") {
                    declared.insert(capture[2].to_string());
                }
            }
            if let Some(capture) = hms.captures(command) {
                if &capture[1] == "prepare" {
                    declared.insert(capture[2].to_string());
                    declared.insert(format!("{}_new", &capture[2]));
                }
            }
            if let Some(capture) = trino.captures(command) {
                if &capture[1] == "create-parent" && !command.trim_end().ends_with(" id 4 3 1 3") {
                    declared.insert(capture[2].to_string());
                }
            }
        }
    }
    Ok(declared)
}
impl RunNames {
    pub fn prepare(
        run: &RunBudget,
        suites: &BTreeMap<String, Vec<std::path::PathBuf>>,
    ) -> Result<Self> {
        ensure!(
            suites.len() <= CAPACITY,
            "FixtureBudgetRejected: too many SQL suites"
        );
        let literal_alias =
            Regex::new(r#"(?i)^\s*['"]([^'"]+)['"]\s+(?:AS\s+)?`?([A-Za-z_][A-Za-z0-9_]*)`?\s*$"#)?;
        let mut total = 0;
        let mut names = BTreeMap::new();
        for (suite_index, (suite, files)) in suites.iter().enumerate() {
            let mut cases = BTreeMap::new();
            for file in files {
                let case = file
                    .file_stem()
                    .and_then(|v| v.to_str())
                    .context("invalid SQL case filename")?;
                ensure!(
                    cases.insert(case.to_string(), 0).is_none(),
                    "ambiguous SQL case identity {suite}/{case}"
                );
                let source = std::fs::read_to_string(file)?;
                let declared = namespace_declarations(&source)?;
                if let Some(projection) = metadata_projection(&source) {
                    let columns = metadata_columns(&source);
                    let duplicate_data_column = projection.split(',').any(|part| {
                        literal_alias.captures(part).is_some_and(|c| {
                            declared.contains(&c[1]) && columns.contains(&c[2].to_ascii_lowercase())
                        })
                    });
                    let mixed_union = !columns.is_empty()
                        && source.to_ascii_uppercase().contains("UNION")
                        && declared.iter().any(|name| {
                            source.contains(&format!("'{name}'"))
                                || source.contains(&format!("\"{name}\""))
                        });
                    ensure!(
                        !duplicate_data_column && !mixed_union,
                        "ambiguous SQL namespace metadata/data output in {suite}/{case}"
                    );
                }
                for namespace in &declared {
                    let alias = Regex::new(&format!(
                        r"(?im)\bAS\s+{}(?:\s|[,;)])",
                        regex::escape(namespace)
                    ))?;
                    let implicit_alias = Regex::new(&format!(
                        r"(?im)(?:\b(?:FROM|JOIN)\s+[A-Za-z0-9_`.${{}}]+\s+|\)\s+)(?:AS\s+)?{}(?:\s|[,;)])",
                        regex::escape(namespace)
                    ))?;
                    ensure!(
                        !alias.is_match(&source) && !implicit_alias.is_match(&source),
                        "ambiguous namespace/SQL alias ownership in {suite}/{case}"
                    );
                }
                let extra = declared
                    .iter()
                    .filter(|name| {
                        !name.starts_with("${case_db")
                            && !(name.starts_with("${uuid") && name.ends_with('}'))
                            && !(name.starts_with("${suite_uuid") && name.ends_with('}'))
                    })
                    .count();
                let reserved = 1
                    + (1..9)
                        .filter(|slot| source.contains(&format!("${{case_db_{}}}", slot + 1)))
                        .count();
                ensure!(
                    extra + reserved <= 10,
                    "FixtureBudgetRejected: SQL case {suite}/{case} exceeds namespace slots"
                );
            }
            for index in cases.values_mut() {
                ordinal(total)?;
                *index = total;
                total += 1;
            }
            names.insert(
                suite.clone(),
                SuiteNames {
                    ordinal: suite_index,
                    cases,
                },
            );
        }
        Ok(Self {
            run_id: run.identity().run_id.clone(),
            scope: run.identity().scope_root.clone(),
            suites: names,
            catalogs: Default::default(),
            hms_warehouses: Default::default(),
        })
    }
    /// Second pass: hook roles never participate in case ordinal allocation.
    pub fn seal_catalogs(
        &mut self,
        config: &RunnerConfig,
        sources: &BTreeMap<String, Vec<std::path::PathBuf>>,
        suites: &BTreeMap<String, SuiteConfig>,
    ) -> Result<()> {
        let mut rendered = Vec::new();
        let machine = config
            .values
            .get("fixture_env_file")
            .map(|env| -> Result<bool> {
                let manifest: serde_json::Value = serde_json::from_slice(&std::fs::read(
                    Path::new(env).with_file_name("manifest.json"),
                )?)?;
                Ok(manifest["runtime"]["machine"].is_object())
            })
            .transpose()?
            .unwrap_or(false);
        let hms = hms_producer_command()?;
        for (suite, files) in sources {
            if machine {
                for file in files {
                    let source = std::fs::read_to_string(file)?;
                    let mut shell = false;
                    if source.lines().any(|line| {
                        shell_line(line, &mut shell)
                            .and_then(|command| hms.captures(command))
                            .is_some_and(|capture| &capture[1] == "prepare")
                    }) {
                        self.hms_warehouses
                            .insert(suite.clone(), format!("{}/hms", self.scope));
                    }
                }
            }

            let mut variables = config.values.clone();
            crate::config::apply_suite_placeholder_defaults(&mut variables, suite);
            self.extend_suite_variables(suite, &mut variables)?;
            variables.insert("suite".into(), suite.clone());
            for path in [&suites[suite].init_sql, &suites[suite].cleanup_sql]
                .into_iter()
                .flatten()
            {
                if path.exists() {
                    rendered.push(render_expected(&std::fs::read_to_string(path)?, &variables));
                }
            }
            for file in files {
                let case = file
                    .file_stem()
                    .and_then(|v| v.to_str())
                    .context("invalid case identity")?;
                let variables = crate::config::case_placeholder_variables(&variables, case)?;
                rendered.push(render_expected(&std::fs::read_to_string(file)?, &variables));
            }
        }
        self.catalogs = crate::catalog_symbol::CatalogSymbols::seal(&self.run_id, &rendered)?;
        Ok(())
    }
    pub fn extend_suite_variables(
        &self,
        suite: &str,
        variables: &mut HashMap<String, String>,
    ) -> Result<()> {
        let names = self
            .suites
            .get(suite)
            .context("SQL suite omitted its frozen naming map")?;
        variables.insert("run_id".into(), self.run_id.clone());
        variables.insert("suite_run_id".into(), self.run_id.clone());
        variables.insert("__scratch_scope".into(), self.scope.clone());
        if let Some(warehouse) = self.hms_warehouses.get(suite) {
            variables.insert("iceberg_hms_warehouse".into(), warehouse.clone());
        }
        variables.insert("__suite_ordinal".into(), names.ordinal.to_string());
        for (case, ordinal) in &names.cases {
            variables.insert(format!("__case_ordinal.{case}"), ordinal.to_string());
        }
        self.catalogs.extend_variables(variables)?;
        for slot in 0..10 {
            let name = resource(&self.run_id, 's', names.ordinal, slot)?;
            variables.insert(format!("suite_uuid{slot}"), name.clone());
            variables.insert(format!("uuid{slot}"), name);
        }
        Ok(())
    }
}

/// Admission precedes private Docker, prepared-dataset commands and SQL writes.
/// A READY publication contributes policy only; its benchmark warehouse stays read-only.
pub(crate) fn admit(config: &RunnerConfig, root: &Path, seconds: u64) -> Result<RunBudget> {
    (|| -> Result<_> {
        let machine = config
            .values
            .get("fixture_env_file")
            .map(|env| {
                let manifest = Path::new(env)
                    .parent()
                    .context("fixture publication directory")?
                    .join("manifest.json");
                let value: serde_json::Value = serde_json::from_slice(&std::fs::read(manifest)?)?;
                let machine = value["runtime"]["machine"].clone();
                if machine.is_object() {
                    ensure!(
                        value["ready"] == true && machine["phase"] == "ready",
                        "shared fixture publication is not ready"
                    );
                    Ok(Some(machine))
                } else {
                    Ok(None)
                }
            })
            .transpose()?
            .flatten();
        let policy = match &machine {
            Some(value) => serde_json::from_value(value["policy"].clone())?,
            None => FixturePolicy::from_environment()?,
        };
        let inherited = std::env::var(novarocks_cluster_harness::fixture_run::RUN_CONTEXT_ENV)
            .ok()
            .map(|v| serde_json::from_str::<FixtureRunContext>(&v))
            .transpose()?;
        let identity = match &inherited {
            Some(value) => value.identity.clone(),
            None => {
                let fallback = format!(
                    "s3://novarocks/sql-{:x}",
                    Sha256::digest(root.to_string_lossy().as_bytes())
                );
                let warehouse = std::env::var("CATALOG_WAREHOUSE_URI")
                    .ok()
                    .filter(|v| v.starts_with("s3://") && !v.contains("${"))
                    .or_else(|| {
                        config
                            .values
                            .get("iceberg_catalog_warehouse")
                            .filter(|v| v.starts_with("s3://") && !v.contains("${"))
                            .cloned()
                    })
                    .unwrap_or(fallback);
                ScratchIdentity::create(&warehouse)?
            }
        };
        let budget = RunBudget::admit(policy, identity, seconds, inherited)?;
        if let Some(machine) = machine {
            budget.validate_machine(&machine)?;
        }
        Ok(budget)
    })()
    .map_err(rejected)
}

/// Explicit writer-start maintenance, after the complete naming preflight.
/// Read-only prepared-dataset admission never calls this helper.
pub(crate) fn startup_sweep(config: &RunnerConfig, repo: &Path, budget: &RunBudget) -> Result<()> {
    let Some(env_file) = config.values.get("fixture_env_file") else {
        return Ok(());
    };
    let manifest: serde_json::Value = serde_json::from_slice(&std::fs::read(
        Path::new(env_file).with_file_name("manifest.json"),
    )?)?;
    if !manifest["runtime"]["machine"].is_object() {
        return Ok(());
    }
    budget.check_work()?;
    novarocks_cluster_harness::fixture_run::startup_sweep(repo, Path::new(env_file), budget)?;
    budget.check_work()
}

pub(crate) fn selected_sources(
    cli: &crate::Cli,
    root: &Path,
    selected: &[String],
    suites: &BTreeMap<String, SuiteConfig>,
) -> Result<BTreeMap<String, Vec<std::path::PathBuf>>> {
    let mut result = BTreeMap::new();
    for name in selected {
        let suite = &suites[name];
        let directory = if selected.len() == 1 {
            crate::config::resolve_path(cli.sql_dir.as_deref(), root)
                .unwrap_or_else(|| suite.sql_dir.clone())
        } else {
            suite.sql_dir.clone()
        };
        let glob = if selected.len() == 1 {
            cli.sql_glob.as_deref().unwrap_or(&suite.sql_glob)
        } else {
            &suite.sql_glob
        };
        result.insert(
            name.clone(),
            crate::config::list_sql_files(&directory, glob)?,
        );
    }
    Ok(result)
}

/// Replace owned namespace identifiers, control metadata and shell references.
/// Ordinary SQL string values, column names and catalog prefixes are preserved.
pub(crate) fn rewrite_namespaces(
    source: &str,
    variables: &HashMap<String, String>,
) -> Result<String> {
    let mapping = namespace_mapping(source, variables)?;
    if mapping.is_empty() {
        return Ok(source.to_string());
    }
    let sql_declared = sql_namespace_declarations(source)?;
    let sql_mapping = mapping
        .iter()
        .filter(|(name, _)| sql_declared.contains(*name))
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect();
    let producer = variant_producer_command()?;
    let hms = hms_producer_command()?;
    let trino = trino_producer_command()?;
    let mut result = String::new();
    let mut sql = String::new();
    let mut shell = false;
    for line in source.split_inclusive('\n') {
        let trimmed = line.trim_start();
        if trimmed.to_ascii_lowercase().starts_with("-- query ") {
            shell = false;
        }
        if trimmed.starts_with("shell:") {
            shell = true;
        }
        if shell || trimmed.starts_with("-- @") {
            result.push_str(&rewrite_sql_identifiers(&sql, &mapping));
            sql.clear();
            if trimmed.starts_with("-- @") {
                result.push_str(&replace_references(line, &mapping));
            } else {
                // Producer-only ownership changes its namespace argument, never
                // unrelated shell values or arbitrary commands in the block.
                let mut rewritten = replace_references(line, &sql_mapping);
                let original_command = line.trim_start().strip_prefix("shell:").unwrap_or(line);
                if let Some(capture) = hms.captures(original_command) {
                    if let (Some(base), Some(sibling)) = (
                        mapping.get(&capture[2]),
                        mapping.get(&format!("{}_new", &capture[2])),
                    ) {
                        let argument = capture.get(2).unwrap();
                        let offset = line.len() - original_command.len();
                        let mut exact = line.to_string();
                        exact.replace_range(
                            offset + argument.start()..offset + argument.end(),
                            base,
                        );
                        let newline = if exact.ends_with('\n') { "\n" } else { "" };
                        rewritten =
                            format!("{} --new-namespace {sibling}{newline}", exact.trim_end());
                    }
                } else if let Some(capture) = trino.captures(original_command) {
                    let read = &capture[1] == "verify-current";
                    ensure!(
                        read == original_command.trim_end().ends_with(" id 4 3 1 3"),
                        "invalid versioned Trino producer argument shape"
                    );
                    if let Some(base) = mapping.get(&capture[2]) {
                        let argument = capture.get(2).unwrap();
                        let offset = line.len() - original_command.len();
                        rewritten = line.to_string();
                        rewritten.replace_range(
                            offset + argument.start()..offset + argument.end(),
                            base,
                        );
                    }
                }
                let command = rewritten
                    .trim_start()
                    .strip_prefix("shell:")
                    .unwrap_or(&rewritten);
                if let Some(capture) = producer.captures(command) {
                    if let Some(name) = mapping.get(&capture[2]) {
                        let argument = capture.get(2).unwrap();
                        let offset = rewritten.len() - command.len();
                        let range = offset + argument.start()..offset + argument.end();
                        let name = name.clone();
                        rewritten.replace_range(range, &name);
                    }
                }
                result.push_str(&rewritten);
            }
        } else {
            sql.push_str(line);
        }
    }
    result.push_str(&rewrite_sql_identifiers(&sql, &mapping));
    result = rewrite_delta_namespace_refs(&result, &mapping)?;
    let mut shell = false;
    let families: Vec<_> = source
        .lines()
        .filter_map(|line| {
            shell_line(line, &mut shell)
                .and_then(|command| hms.captures(command))
                .filter(|capture| &capture[1] == "prepare")
                .map(|capture| capture[2].to_string())
        })
        .collect();
    if !families.is_empty() {
        if let Some(warehouse) = variables.get("iceberg_hms_warehouse") {
            use novarocks_parser::ast::{
                IcebergStatement, IcebergTableAction, LiteralKind, Statement,
            };
            let masked = crate::catalog_symbol::sql_source(&result);
            let mut edits = Vec::new();
            for range in crate::catalog_symbol::ranges(&masked)? {
                let sql = &masked[range.clone()];
                if let Ok(statements) = novarocks_parser::parse(sql) {
                    for statement in statements {
                        if let Statement::Iceberg(IcebergStatement::AlterTable(table)) = statement {
                            if let IcebergTableAction::AddFiles(files) = table.action {
                                if let LiteralKind::String(value) = &files.location.kind {
                                    for family in &families {
                                        if value
                                            == &format!(
                                                "{}/iru7/{family}/v1/data",
                                                warehouse.trim_end_matches('/')
                                            )
                                        {
                                            if let Some(mapped) = mapping.get(family) {
                                                edits.push((
                                                    range.start + files.location.span.start(),
                                                    range.start + files.location.span.end(),
                                                    format!(
                                                        "'{}/iru7/{mapped}/v1/data'",
                                                        warehouse.trim_end_matches('/')
                                                    ),
                                                ));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            for (start, end, value) in edits.into_iter().rev() {
                result.replace_range(start..end, &value);
            }
        }
    }
    Ok(result)
}
// The internal delta TVF's first argument is a table identity. It is not an
// ordinary SQL string; only its namespace component consumes this mapping.
fn rewrite_delta_namespace_refs(
    source: &str,
    mapping: &BTreeMap<String, String>,
) -> Result<String> {
    use novarocks_parser::ast::{Expr, FunctionCall, LiteralKind, Visit, walk_function_call};
    struct References<'a> {
        mapping: &'a BTreeMap<String, String>,
        edits: Vec<(usize, usize, String)>,
    }
    impl Visit for References<'_> {
        fn visit_function_call(&mut self, call: &FunctionCall) {
            if call.name.parts.len() == 1
                && call.name.parts[0]
                    .value
                    .eq_ignore_ascii_case("__nr_ivm_delta")
            {
                if let Some(Expr::Literal(literal)) = call.arguments.first() {
                    if let LiteralKind::String(value) = &literal.kind {
                        let parts: Vec<_> = value.split('.').collect();
                        if parts.len() == 3 {
                            if let Some(namespace) = self.mapping.get(parts[1]) {
                                self.edits.push((
                                    literal.span.start(),
                                    literal.span.end(),
                                    format!("'{}.{namespace}.{}'", parts[0], parts[2]),
                                ));
                            }
                        }
                    }
                }
            }
            walk_function_call(self, call);
        }
    }
    let masked = crate::catalog_symbol::sql_source(source);
    let mut edits = Vec::new();
    for range in crate::catalog_symbol::ranges(&masked)? {
        if let Ok(statements) = novarocks_parser::parse(&masked[range.clone()]) {
            let mut visitor = References {
                mapping,
                edits: Vec::new(),
            };
            for statement in &statements {
                visitor.visit_statement(statement);
            }
            edits.extend(
                visitor
                    .edits
                    .into_iter()
                    .map(|(start, end, value)| (range.start + start, range.start + end, value)),
            );
        }
    }
    let mut result = source.to_string();
    for (start, end, value) in edits.into_iter().rev() {
        result.replace_range(start..end, &value);
    }
    Ok(result)
}
pub(crate) fn namespace_mapping(
    source: &str,
    variables: &HashMap<String, String>,
) -> Result<BTreeMap<String, String>> {
    let Some(run) = variables
        .get("suite_run_id")
        .filter(|_| variables.contains_key("__scratch_scope"))
    else {
        return Ok(BTreeMap::new());
    };
    let Some(case) = variables.get("case_id") else {
        return Ok(BTreeMap::new());
    };
    let index = variables
        .get(&format!("__case_ordinal.{case}"))
        .context("missing SQL ordinal")?
        .parse::<usize>()?;
    let mut used = BTreeSet::from([0usize]);
    for slot in 0..10 {
        let key = if slot == 0 {
            "case_db".into()
        } else {
            format!("case_db_{}", slot + 1)
        };
        if variables
            .get(&key)
            .is_some_and(|name| source.contains(name))
        {
            used.insert(slot);
        }
    }
    let mut mapping = BTreeMap::new();
    for name in namespace_declarations(source)? {
        if name.starts_with(&format!("{run}_")) {
            ensure!(name.len() <= 63, "owned SQL namespace exceeds 63 bytes");
            continue;
        }
        let slot = (0..10)
            .find(|slot| !used.contains(slot))
            .context("SQL namespace slots exhausted")?;
        used.insert(slot);
        mapping.insert(name, resource(run, 'd', index, slot)?);
    }
    Ok(mapping)
}
pub(crate) fn render_expected(value: &str, variables: &HashMap<String, String>) -> String {
    let mut rendered = value.to_string();
    for (name, value) in variables {
        rendered = rendered.replace(&format!("${{{name}}}"), value);
    }
    rendered
}
fn metadata_projection(sql: &str) -> Option<String> {
    let re = Regex::new(r"(?is)\bSELECT\s+(.*?)\s+FROM\s+(?:[A-Za-z0-9_`.$\{\}]+\.)?information_schema\.(?:schemata|tables|columns|materialized_views|task_runs)\b").ok()?;
    re.captures(sql).map(|c| c[1].to_string())
}
fn metadata_columns(sql: &str) -> BTreeSet<String> {
    let Some(projection) = metadata_projection(sql) else {
        return BTreeSet::new();
    };
    let re = Regex::new(r"(?i)^\s*(?:[A-Za-z_][A-Za-z0-9_]*\.)?`?(schema_name|table_schema|database|db_name)`?(?:\s+(?:AS\s+)?`?([A-Za-z_][A-Za-z0-9_]*)`?)?\s*$").unwrap();
    projection
        .split(',')
        .filter_map(|part| {
            re.captures(part).map(|c| {
                c.get(2).map_or_else(
                    || c[1].to_ascii_lowercase(),
                    |a| a.as_str().to_ascii_lowercase(),
                )
            })
        })
        .collect()
}
pub(crate) fn rewrite_metadata_filters(sql: &str, mapping: &BTreeMap<String, String>) -> String {
    if metadata_projection(sql).is_none() {
        return sql.to_string();
    }
    let re = Regex::new(
        r#"(?i)(\b(?:table_schema|schema_name|database|db_name)`?\s*=\s*)(['"])([^'"]+)(['"])"#,
    )
    .unwrap();
    re.replace_all(sql, |c: &regex::Captures<'_>| {
        let value = mapping.get(&c[3]).map_or(&c[3], String::as_str);
        format!("{}{}{}{}", &c[1], &c[2], value, &c[4])
    })
    .into_owned()
}
pub(crate) fn render_expected_result(
    sql: &str,
    result: &mut crate::types::ResultSet,
    variables: &HashMap<String, String>,
    mapping: &BTreeMap<String, String>,
) -> Result<()> {
    for cell in result
        .header
        .iter_mut()
        .chain(result.rows.iter_mut().flatten())
    {
        *cell = render_expected(cell, variables);
    }
    let columns = metadata_columns(sql);
    let upper = sql.trim().to_ascii_uppercase();
    let show_databases = upper.starts_with("SHOW DATABASES");
    let current_database = Regex::new(r"(?i)^\s*SELECT\s+DATABASE\s*\(\s*\)\s*;?\s*$")
        .unwrap()
        .is_match(sql);
    let show_create =
        upper.starts_with("SHOW CREATE TABLE") || upper.starts_with("SHOW CREATE VIEW");
    for (index, header) in result.header.iter().enumerate() {
        let namespace = columns.contains(&header.to_ascii_lowercase())
            || (show_databases && header.eq_ignore_ascii_case("database"))
            || (current_database && result.header.len() == 1);
        let ddl = show_create
            && (header.eq_ignore_ascii_case("create table")
                || header.eq_ignore_ascii_case("create view"));
        for row in &mut result.rows {
            if let Some(cell) = row.get_mut(index) {
                if namespace {
                    if let Some(name) = mapping.get(cell) {
                        *cell = name.clone();
                    }
                } else if ddl {
                    *cell = rewrite_sql_identifiers(cell, mapping);
                }
            }
        }
    }
    crate::catalog_symbol::CatalogSymbols::from_variables(variables)?
        .render_expected(sql, result)?;
    Ok(())
}
fn replace_references(text: &str, mapping: &BTreeMap<String, String>) -> String {
    let mut text = text.to_string();
    for (old, new) in mapping {
        let mut next = String::new();
        let mut offset = 0;
        for (start, _) in text.match_indices(old) {
            let end = start + old.len();
            let boundary = |c: u8| !c.is_ascii_alphanumeric() && c != b'_';
            if (start == 0 || boundary(text.as_bytes()[start - 1]))
                && (end == text.len() || boundary(text.as_bytes()[end]))
            {
                next.push_str(&text[offset..start]);
                next.push_str(new);
                offset = end;
            }
        }
        next.push_str(&text[offset..]);
        text = next;
    }
    text
}
fn rewrite_sql_identifiers(line: &str, mapping: &BTreeMap<String, String>) -> String {
    let mut result = String::new();
    let bytes = line.as_bytes();
    let mut i = 0;
    let mut database = false;
    let mut previous = String::new();
    let mut last_word = String::new();
    while i < bytes.len() {
        if bytes[i..].starts_with(b"--") {
            let end = line[i..].find('\n').map_or(bytes.len(), |n| i + n + 1);
            result.push_str(&line[i..end]);
            i = end;
            continue;
        }
        if bytes[i] == b'\'' || bytes[i] == b'"' {
            let quote = bytes[i];
            let start = i;
            i += 1;
            while i < bytes.len() {
                if bytes[i] == quote {
                    i += 1;
                    if i < bytes.len() && bytes[i] == quote {
                        i += 1;
                        continue;
                    }
                    break;
                }
                if bytes[i] == b'\\' {
                    i += 1;
                }
                i += 1;
            }
            let literal = &line[start..i.min(bytes.len())];
            if previous.ends_with("table=>") {
                result.push_str(&replace_references(literal, mapping));
            } else {
                result.push_str(literal);
            }
            continue;
        }
        if bytes[i] == b'`' || bytes[i].is_ascii_alphabetic() || bytes[i] == b'_' {
            let quoted = bytes[i] == b'`';
            let start = i;
            if quoted {
                i += 1;
            }
            let name_start = i;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            let name = &line[name_start..i];
            if quoted && i < bytes.len() && bytes[i] == b'`' {
                i += 1;
            }
            if i == start {
                i += 1;
                result.push_str(&line[start..i]);
                continue;
            }
            let tail = line[i..].trim_start();
            let following = tail.starts_with('.');
            let name_tail = tail
                .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '`'))
                .next()
                .unwrap_or("");
            let remaining_dots = name_tail.bytes().filter(|b| *b == b'.').count();
            // Iceberg branch/tag table references add one final qualifier. It
            // is part of the table reference, never a second namespace slot.
            let branch_or_tag = remaining_dots == 2
                && name_tail.rsplit('.').next().is_some_and(|part| {
                    let part = part.trim_matches('`');
                    part.starts_with("branch_") || part.starts_with("tag_")
                });
            if let Some(new) = mapping.get(name).filter(|_| {
                (database && !following)
                    || (!database && following && (remaining_dots == 1 || branch_or_tag))
            }) {
                if quoted {
                    result.push('`');
                }
                result.push_str(new);
                if quoted {
                    result.push('`');
                }
            } else {
                result.push_str(&line[start..i]);
            }
            if (name.eq_ignore_ascii_case("DATABASE")
                && ["CREATE", "DROP"]
                    .iter()
                    .any(|v| last_word.eq_ignore_ascii_case(v)))
                || name.eq_ignore_ascii_case("USE")
            {
                database = true;
            }
            last_word = name.to_string();
            if name.eq_ignore_ascii_case("table") {
                previous = "table".into();
            } else if !["IF", "NOT", "EXISTS"]
                .iter()
                .any(|v| name.eq_ignore_ascii_case(v))
                && !following
            {
                previous.clear();
            }
        } else {
            if bytes[i] == b';' {
                database = false;
                previous.clear();
            }
            if bytes[i] == b'=' || bytes[i] == b'>' {
                previous.push(bytes[i] as char);
            }
            let ch = line[i..].chars().next().unwrap();
            result.push(ch);
            i += ch.len_utf8();
        }
    }
    result
}

#[cfg(test)]
pub(crate) fn test_names(suite: &str, cases: &[&str]) -> RunNames {
    RunNames {
        catalogs: Default::default(),
        hms_warehouses: Default::default(),
        run_id: "nr_scratch_1792540800000_0123456789abcdef0123456789abcdef".into(),
        scope:
            "s3://novarocks/test/scratch/nr_scratch_1792540800000_0123456789abcdef0123456789abcdef"
                .into(),
        suites: BTreeMap::from([(
            suite.into(),
            SuiteNames {
                ordinal: 0,
                cases: cases
                    .iter()
                    .enumerate()
                    .map(|(i, n)| (n.to_string(), i))
                    .collect(),
            },
        )]),
    }
}
#[cfg(test)]
pub(crate) fn test_variables(cases: &[&str]) -> HashMap<String, String> {
    let mut variables = HashMap::new();
    test_names("test", cases)
        .extend_suite_variables("test", &mut variables)
        .unwrap();
    variables
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_hms_pair_and_trino_writer_bind_references_without_new_observer_ownership() {
        let mut vars =
            crate::config::case_placeholder_variables(&test_variables(&["case"]), "case").unwrap();
        vars.insert(
            "iceberg_hms_warehouse".into(),
            "s3://novarocks/unit/hms".into(),
        );
        let base = format!("iru7_{}", vars["uuid0"]);
        let producer = format!(
            r#"python3 "${{NOVAROCKS_WORKSPACE_ROOT:-.}}/tests/sql/fixtures/iceberg-hms/fixture.py" prepare {base}"#
        );
        let source = format!(
            "-- query 1\nshell: {producer}\n-- query 2\nCREATE DATABASE c.{base}_new;\nALTER TABLE c.{base}.v2 ADD FILES FROM 's3://novarocks/unit/hms/iru7/{base}/v1/data';\nSELECT 's3://novarocks/unit/hms/iru7/{base}/v1/data';\n-- query 3\nshell: {}\n",
            producer.replace(" prepare ", " after ")
        );
        let mapping = namespace_mapping(&source, &vars).unwrap();
        let out = rewrite_namespaces(&source, &vars).unwrap();
        let mapped = &mapping[&base];
        let sibling = &mapping[&format!("{base}_new")];
        assert_ne!(mapped, sibling);
        assert_eq!(mapped.len(), 63);
        assert_eq!(sibling.len(), 63);
        assert!(out.contains(&format!("prepare {mapped} --new-namespace {sibling}")));
        assert!(out.contains(&format!("after {mapped} --new-namespace {sibling}")));
        assert!(out.contains(&format!(
            "ADD FILES FROM 's3://novarocks/unit/hms/iru7/{mapped}/v1/data'"
        )));
        assert!(out.contains(&format!(
            "SELECT 's3://novarocks/unit/hms/iru7/{base}/v1/data'"
        )));
        assert!(
            namespace_declarations(&source.replace(" prepare ", " before "))
                .unwrap()
                .iter()
                .all(|name| name != &base)
        );
        let trino = r#""${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/datasketches-tck/interop/trino/verify_rest_catalog.sh""#;
        let source = format!(
            "-- query 1\nshell: {trino} create-parent producer_ns table1\n-- query 2\nSELECT * FROM c.producer_ns.table1;\n-- query 3\nshell: {trino} verify-current producer_ns table1 id 4 3 1 3\n"
        );
        let mapping = namespace_mapping(&source, &vars).unwrap();
        let out = rewrite_namespaces(&source, &vars).unwrap();
        assert!(out.contains(&format!("create-parent {} table1", mapping["producer_ns"])));
        assert!(out.contains(&format!(
            "verify-current {} table1 id 4 3 1 3",
            mapping["producer_ns"]
        )));
        assert!(out.contains(&format!("c.{}.table1", mapping["producer_ns"])));
        assert!(
            namespace_declarations(&source.replace("create-parent", "verify-current"))
                .unwrap()
                .is_empty()
        );
        assert!(
            namespace_declarations("shell: arbitrary --namespace producer_ns")
                .unwrap()
                .is_empty()
        );
    }
    #[test]
    fn delta_tvf_first_argument_uses_namespace_identity_but_ordinary_literals_do_not() {
        let mapping = BTreeMap::from([("old_ns".into(), "canonical_namespace".into())]);
        let sql =
            "SELECT 'c.old_ns.t', ordinary('c.old_ns.t') FROM __nr_ivm_delta('c.old_ns.t',0,0);";
        let out = rewrite_delta_namespace_refs(sql, &mapping).unwrap();
        assert!(out.contains("SELECT 'c.old_ns.t', ordinary('c.old_ns.t')"));
        assert!(out.contains("__nr_ivm_delta('c.canonical_namespace.t',0,0)"));
    }
    #[test]
    fn explicit_startup_sweep_skips_private_publications_without_starting_a_command() {
        let root = tempfile::tempdir().unwrap();
        let env_file = root.path().join("env.sh");
        std::fs::write(&env_file, "# private publication\n").unwrap();
        std::fs::write(
            root.path().join("manifest.json"),
            r#"{"ready":true,"runtime":{"catalog":{}}}"#,
        )
        .unwrap();
        let budget = RunBudget::admit(
            FixturePolicy::default(),
            ScratchIdentity::create("s3://novarocks/sql-startup").unwrap(),
            600,
            None,
        )
        .unwrap();
        let mut config = RunnerConfig::default();
        startup_sweep(&config, &root.path().join("missing-repository"), &budget).unwrap();
        config.values.insert(
            "fixture_env_file".into(),
            env_file.to_string_lossy().into_owned(),
        );
        startup_sweep(&config, &root.path().join("missing-repository"), &budget).unwrap();
        assert!(!root.path().join("started").exists());
    }
    #[test]
    fn explicit_startup_sweep_passes_the_exact_original_context_once_for_run_clones() {
        let root = tempfile::tempdir().unwrap();
        let scripts = root.path().join("docker/iceberg-rest");
        std::fs::create_dir_all(&scripts).unwrap();
        std::fs::write(scripts.join("fixture_run.py"),r#"import json,os,pathlib,sys
assert sys.argv[1]=='sweep-start' and sys.argv[2]=='--env-file'
env=pathlib.Path(sys.argv[3])
with (env.parent/'started').open('a') as output: output.write(os.environ['NOVA_FIXTURE_RUN_CONTEXT']+'\n')
print(json.dumps({'complete':True,'pending':False,'errors':[],'removed':[]}))
"#).unwrap();
        let env_file = root.path().join("env.sh");
        std::fs::write(&env_file, "# pinned publication\n").unwrap();
        std::fs::write(
            root.path().join("manifest.json"),
            r#"{"ready":true,"runtime":{"machine":{}}}"#,
        )
        .unwrap();
        let budget = RunBudget::admit(
            FixturePolicy::default(),
            ScratchIdentity::create("s3://novarocks/sql-startup").unwrap(),
            600,
            None,
        )
        .unwrap();
        let mut config = RunnerConfig::default();
        config.values.insert(
            "fixture_env_file".into(),
            env_file.to_string_lossy().into_owned(),
        );
        assert!(!root.path().join("started").exists());
        startup_sweep(&config, root.path(), &budget).unwrap();
        startup_sweep(&config, root.path(), &budget.clone()).unwrap();
        assert_eq!(
            std::fs::read_to_string(root.path().join("started"))
                .unwrap()
                .lines()
                .collect::<Vec<_>>(),
            vec![budget.to_json().unwrap()]
        );
    }

    #[test]
    fn ordinal_names_cover_every_slot_without_truncated_hashes_or_collisions() {
        let run = &test_names("suite", &[]).run_id;
        let mut seen = std::collections::HashSet::new();
        for index in [0, 1, 35, 36, 1295, 1296, CAPACITY - 1] {
            for kind in ['d', 'u', 's'] {
                for slot in 0..10 {
                    let name = resource(run, kind, index, slot).unwrap();
                    assert_eq!(name.len(), 63);
                    assert_eq!(name.rsplit_once('_').unwrap().0, run);
                    assert!(seen.insert(name));
                }
            }
        }
        assert!(resource(run, 'd', CAPACITY, 0).is_err());
        assert!(resource(run, 'd', 0, 10).is_err());
        assert_eq!(resource(run, 'd', 36, 2).unwrap(), format!("{run}_d0102"));
    }
    #[test]
    fn explicit_namespace_mapping_preserves_sql_values_columns_and_catalogs() {
        let mut vars = test_variables(&["case"]);
        vars = crate::config::case_placeholder_variables(&vars, "case").unwrap();
        let old = format!("ns_{}", vars["uuid0"]);
        let source = format!(
            "CREATE DATABASE catalog.{old};\nCREATE TABLE catalog.{old}.t (id INT);\nSELECT '{old}' AS value, {old} FROM ordinary;\nCREATE EXTERNAL CATALOG {old} PROPERTIES ('type'='iceberg');\n-- @db={old}\nshell: trino --namespace {old}\n"
        );
        let rewritten = rewrite_namespaces(&source, &vars).unwrap();
        let db = resource(&vars["run_id"], 'd', 0, 1).unwrap();
        assert!(rewritten.contains(&format!("CREATE DATABASE catalog.{db};")));
        assert!(rewritten.contains(&format!("catalog.{db}.t")));
        assert!(rewritten.contains(&format!("SELECT '{old}' AS value, {old} FROM ordinary")));
        assert!(rewritten.contains(&format!("CREATE EXTERNAL CATALOG {old}")));
        assert!(rewritten.contains(&format!("-- @db={db}")));
        assert!(rewritten.contains(&format!("--namespace {db}")));
    }
    fn variant_command(mode: &str, namespace: &str, table: &str) -> String {
        format!(
            r#""${{NOVAROCKS_WORKSPACE_ROOT:-.}}/tests/sql/fixtures/iceberg-variant/run.sh" --mode {mode} --namespace {namespace} --table {table} --output "$receipt_dir" >"$receipt_dir/run.log""#
        )
    }
    #[test]
    fn only_exact_variant_writer_invocations_declare_shell_namespace_ownership() {
        let writer = variant_command("empty-int", "iru9_${uuid0}", "target");
        assert_eq!(
            namespace_declarations(&format!("-- query 1\nshell: set -eu\n{writer}\n")).unwrap(),
            BTreeSet::from(["iru9_${uuid0}".into()])
        );
        for unowned in [
            writer.clone(),
            format!("shell: echo {writer}\n"),
            format!("shell: {writer} && other-command\n"),
            format!(
                "shell: {}\n",
                writer.replace("iceberg-variant/", "other-producer/")
            ),
            format!(
                "shell: {}\n",
                writer.replace("--mode empty-int", "--mode unknown")
            ),
            format!(
                "shell: {}\n",
                variant_command("observe", "old_namespace", "target")
            ),
            format!(
                "shell: {}\n",
                variant_command("unchanged", "old_namespace", "target")
            ),
            format!("shell: set -eu\n-- query 2\n{writer}\n"),
        ] {
            assert!(
                namespace_declarations(&unowned).unwrap().is_empty(),
                "{unowned}"
            );
        }
    }
    #[test]
    fn variant_writer_and_readonly_arguments_share_mapping_without_changing_other_shell_values() {
        let vars =
            crate::config::case_placeholder_variables(&test_variables(&["case"]), "case").unwrap();
        let source = format!(
            "-- query 1\nshell: set -eu\n{}\n{}\n{}\necho original_namespace\nother-producer --namespace original_namespace\n-- query 2\nSELECT * FROM catalog.original_namespace.target;\nSELECT 'original_namespace' AS value;\n",
            variant_command("empty-int", "original_namespace", "target"),
            variant_command("observe", "original_namespace", "target"),
            variant_command("unchanged", "original_namespace", "target"),
        );
        let mapping = namespace_mapping(&source, &vars).unwrap();
        let expected = resource(&vars["run_id"], 'd', 0, 1).unwrap();
        assert_eq!(
            mapping,
            BTreeMap::from([("original_namespace".into(), expected.clone())])
        );
        let rewritten = rewrite_namespaces(&source, &vars).unwrap();
        for mode in ["empty-int", "observe", "unchanged"] {
            assert!(rewritten.contains(&variant_command(mode, &expected, "target")));
        }
        assert!(rewritten.contains(&format!("catalog.{expected}.target")));
        assert!(rewritten.contains("SELECT 'original_namespace' AS value"));
        assert!(rewritten.contains("echo original_namespace"));
        assert!(rewritten.contains("other-producer --namespace original_namespace"));
    }
    #[test]
    fn independent_variant_namespaces_receive_distinct_bounded_slots() {
        let vars =
            crate::config::case_placeholder_variables(&test_variables(&["case"]), "case").unwrap();
        let source = format!(
            "shell: set -eu\n{}\n{}\n",
            variant_command("empty-int", "first", "t"),
            variant_command("empty-bigint", "second", "t")
        );
        let map = namespace_mapping(&source, &vars).unwrap();
        assert_eq!(map.len(), 2);
        assert_ne!(map["first"], map["second"]);
        assert!(
            map.values()
                .all(|name| name.starts_with(&format!("{}_", vars["run_id"])) && name.len() <= 63)
        );
    }
    #[test]
    fn existing_variant_dml_cases_bind_the_original_producer_namespace_before_writes() {
        for (case, source) in [
            (
                "variant_get",
                include_str!("../../../correctness/iceberg-dml/sql/variant_get.sql"),
            ),
            (
                "variant_insert",
                include_str!("../../../correctness/iceberg-dml/sql/variant_insert.sql"),
            ),
            (
                "variant_mutations",
                include_str!("../../../correctness/iceberg-dml/sql/variant_mutations.sql"),
            ),
        ] {
            assert_eq!(
                namespace_declarations(source).unwrap(),
                BTreeSet::from(["iru9_${uuid0}".into()])
            );
            let vars =
                crate::config::case_placeholder_variables(&test_variables(&[case]), case).unwrap();
            let rendered = render_expected(source, &vars);
            let old = format!("iru9_{}", vars["uuid0"]);
            let map = namespace_mapping(&rendered, &vars).unwrap();
            let expected = &map[&old];
            assert!(expected.starts_with(&format!("{}_", vars["run_id"])) && expected.len() <= 63);
            let rewritten = rewrite_namespaces(&rendered, &vars).unwrap();
            assert!(rewritten.contains(&format!("--namespace {expected} --table")));
            assert!(!rewritten.contains(&format!("--namespace {old} --table")));
            assert!(rewritten.contains(&format!(".{expected}.")));
        }
    }
    #[test]
    fn branch_qualified_tables_share_namespace_mapping_without_changing_literals() {
        let vars =
            crate::config::case_placeholder_variables(&test_variables(&["case"]), "case").unwrap();
        let old = format!("ns_{}", vars["uuid0"]);
        let source = format!(
            "CREATE DATABASE catalog.{old};\nCREATE TABLE catalog.{old}.t (id INT);\nINSERT INTO catalog.{old}.t.branch_dev VALUES (1);\nUPDATE catalog.`{old}`.`t`.`branch_dev` SET id=2;\nDELETE FROM {old}.t.branch_dev WHERE id=2;\nSELECT * FROM catalog.{old}.t.tag_snapshot;\nSELECT '{old}', 'catalog.{old}.t.branch_dev' AS data FROM ordinary;\n"
        );
        let rewritten = rewrite_namespaces(&source, &vars).unwrap();
        let db = resource(&vars["run_id"], 'd', 0, 1).unwrap();
        assert!(rewritten.contains(&format!("catalog.{db}.t.branch_dev")));
        assert!(rewritten.contains(&format!("catalog.`{db}`.`t`.`branch_dev`")));
        assert!(rewritten.contains(&format!("DELETE FROM {db}.t.branch_dev")));
        assert!(rewritten.contains(&format!("catalog.{db}.t.tag_snapshot")));
        assert!(rewritten.contains(&format!(
            "SELECT '{old}', 'catalog.{old}.t.branch_dev' AS data"
        )));
    }
    #[test]
    fn multiline_database_names_and_golden_references_use_the_same_mapping() {
        let vars =
            crate::config::case_placeholder_variables(&test_variables(&["case"]), "case").unwrap();
        let sql = "-- comment\nCREATE DATABASE\n catalog.raw_namespace;\nSELECT DATABASE(), raw_namespace FROM t;\n";
        let mapping = namespace_mapping(sql, &vars).unwrap();
        let expected = resource(&vars["run_id"], 'd', 0, 1).unwrap();
        let rewritten = rewrite_namespaces(sql, &vars).unwrap();
        assert!(rewritten.contains(&format!("catalog.{expected};")));
        assert!(rewritten.contains("SELECT DATABASE(), raw_namespace FROM t"));
        let mut result = crate::types::ResultSet {
            header: vec!["schema_name".into(), "value".into()],
            rows: vec![vec!["raw_namespace".into(), "raw_namespace".into()]],
        };
        render_expected_result(
            "SELECT schema_name, 'raw_namespace' AS value FROM information_schema.schemata",
            &mut result,
            &vars,
            &mapping,
        )
        .unwrap();
        assert_eq!(result.rows[0], vec![expected, "raw_namespace".to_string()]);
    }
    #[test]
    fn independent_invocations_and_case_ordinals_never_share_database_or_uuid_names() {
        let left = test_variables(&["same", "other"]);
        let mut right = left.clone();
        right.insert(
            "suite_run_id".into(),
            "nr_scratch_1792540800000_fedcba9876543210fedcba9876543210".into(),
        );
        let a = crate::config::case_placeholder_variables(&left, "same").unwrap();
        let b = crate::config::case_placeholder_variables(&right, "same").unwrap();
        let c = crate::config::case_placeholder_variables(&left, "other").unwrap();
        assert_ne!(a["case_db"], b["case_db"]);
        assert_ne!(a["case_db"], c["case_db"]);
        assert_ne!(a["uuid0"], b["uuid0"]);
        assert_ne!(a["uuid0"], c["uuid0"]);
        assert_eq!(a["suite_uuid0"], c["suite_uuid0"]);
        assert_eq!(
            a["case_db"],
            crate::config::case_auto_db_name(&a["run_id"], 0).unwrap()
        );
    }
    #[test]
    fn metadata_namespace_filters_and_expected_cells_do_not_rewrite_data_literals() {
        let mapping = BTreeMap::from([("old_ns".into(), "new_ns".into())]);
        let sql = "SELECT schema_name, 'old_ns' AS data_value FROM information_schema.schemata WHERE schema_name = 'old_ns'";
        let actual = rewrite_metadata_filters(sql, &mapping);
        assert!(actual.contains("'old_ns' AS data_value"));
        assert!(actual.contains("schema_name = 'new_ns'"));
        assert_eq!(
            rewrite_metadata_filters(
                "SELECT 'old_ns' FROM ordinary WHERE schema_name = 'old_ns'",
                &mapping
            ),
            "SELECT 'old_ns' FROM ordinary WHERE schema_name = 'old_ns'"
        );
        let mut result = crate::types::ResultSet {
            header: vec!["schema_name".into(), "data_value".into()],
            rows: vec![vec!["old_ns".into(), "old_ns".into()]],
        };
        render_expected_result(&actual, &mut result, &HashMap::new(), &mapping).unwrap();
        assert_eq!(result.rows[0], vec!["new_ns", "old_ns"]);
    }
    #[test]
    fn namespace_metadata_and_data_projection_ambiguity_rejects_before_scratch() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("case.sql");
        std::fs::write(&file,"CREATE DATABASE ns;\nSELECT schema_name, 'ns' AS schema_name FROM information_schema.schemata;").unwrap();
        let run = RunBudget::admit(
            FixturePolicy::default(),
            ScratchIdentity::create("s3://novarocks/test").unwrap(),
            600,
            None,
        )
        .unwrap();
        assert!(
            RunNames::prepare(&run, &BTreeMap::from([("suite".into(), vec![file])]))
                .unwrap_err()
                .to_string()
                .contains("metadata/data")
        );
    }
    #[test]
    fn namespace_alias_ambiguity_is_rejected_before_writes() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("case.sql");
        let run = RunBudget::admit(
            FixturePolicy::default(),
            ScratchIdentity::create("s3://novarocks/test").unwrap(),
            600,
            None,
        )
        .unwrap();
        for query in [
            "SELECT ns.id FROM t AS ns;",
            "SELECT ns.id FROM t ns;",
            "SELECT ns.id FROM (SELECT 1) ns;",
        ] {
            std::fs::write(&file, format!("CREATE DATABASE ns;\n{query}\n")).unwrap();
            assert!(
                RunNames::prepare(
                    &run,
                    &BTreeMap::from([("suite".into(), vec![file.clone()])])
                )
                .unwrap_err()
                .to_string()
                .contains("ambiguous"),
                "{query}"
            );
        }
    }
    #[test]
    fn namespace_capacity_and_duplicate_case_identity_fail_before_side_effects() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("case.sql");
        std::fs::write(
            &file,
            (0..10)
                .map(|n| format!("CREATE DATABASE ns{n};\n"))
                .collect::<String>(),
        )
        .unwrap();
        let run = RunBudget::admit(
            FixturePolicy::default(),
            ScratchIdentity::create("s3://novarocks/test").unwrap(),
            600,
            None,
        )
        .unwrap();
        assert!(
            RunNames::prepare(
                &run,
                &BTreeMap::from([("suite".into(), vec![file.clone()])])
            )
            .is_err()
        );
        std::fs::write(&file, "SELECT 1;").unwrap();
        assert!(
            RunNames::prepare(
                &run,
                &BTreeMap::from([("suite".into(), vec![file.clone(), file])])
            )
            .is_err()
        );
    }
}
