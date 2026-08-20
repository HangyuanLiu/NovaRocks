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

//! Test-only differential between the parser-owned Query AST and the current
//! production raw SQL AST. This module deliberately has no cluster, fixture,
//! session, or MySQL dependency: it consumes the runner's authoritative SQL
//! case loader and exits before the execution runner starts those owners.

#[cfg(test)]
use crate::suite_manifest::select_suite_names;
use crate::{
    config::{list_sql_files, placeholder_variables, resolve_path},
    parser::load_sql_case_from_file,
    runner::parse_selector_list,
    shell::is_shell_step,
    types::{RunnerConfig, SqlCase, SqlStep, SuiteConfig},
};
use anyhow::{Context, Result, bail};
use novarocks_parser::{
    Keyword, Symbol, TokenKind,
    ast::{Statement as TypedStatement, SyntaxEq},
    lex, parse as parse_typed,
    printer::print_statements,
};
use sqlparser::ast::Statement as LegacyStatement;
use std::{
    collections::{BTreeMap, HashSet},
    panic::{AssertUnwindSafe, catch_unwind},
    path::Path,
};

/// CLI selections that are meaningful for a read-only corpus inventory.
#[derive(Clone, Copy, Debug, Default)]
pub struct Options<'a> {
    pub sql_dir: Option<&'a str>,
    pub sql_glob: Option<&'a str>,
    pub only: Option<&'a str>,
    pub skip: Option<&'a str>,
    pub limit: Option<usize>,
}

/// Aggregate result of one read-only differential inventory.
#[derive(Debug, Default)]
pub struct Summary {
    pub scanned: usize,
    pub statement_payloads: usize,
    pub accept_query: usize,
    pub typed_only_explain: usize,
    ddl_dml_typed: BTreeMap<DdlDmlClass, usize>,
    ddl_dml_printer: BTreeMap<DdlDmlClass, usize>,
    row_dml_semantic: BTreeMap<DdlDmlClass, usize>,
    row_dml_legacy_unavailable: BTreeMap<DdlDmlClass, usize>,
    pub reject_excluded: usize,
    pub non_query: usize,
    pub mismatches: Vec<Mismatch>,
}

impl Summary {
    pub fn exit_code(&self) -> i32 {
        (!self.mismatches.is_empty()) as i32
    }

    pub fn print(&self) {
        for mismatch in &self.mismatches {
            eprintln!("{mismatch}");
        }
        println!(
            "SQLP-5 DDL/DML parser differential: scanned={} statement-payloads={} accept-query={} typed-only-explain={} ddl-dml-typed={} ddl-dml-printer={} row-dml-semantic={} row-dml-legacy-unavailable={} semantic-not-applicable={{table-ddl={},ctas={},add-equality-delete={}}} reject-excluded={} non-query={} mismatches={}",
            self.scanned,
            self.statement_payloads,
            self.accept_query,
            self.typed_only_explain,
            display_class_counts(&self.ddl_dml_typed),
            display_class_counts(&self.ddl_dml_printer),
            display_class_counts(&self.row_dml_semantic),
            display_class_counts(&self.row_dml_legacy_unavailable),
            self.ddl_dml_typed
                .get(&DdlDmlClass::TableDdl)
                .copied()
                .unwrap_or_default(),
            self.ddl_dml_typed
                .get(&DdlDmlClass::Ctas)
                .copied()
                .unwrap_or_default(),
            self.ddl_dml_typed
                .get(&DdlDmlClass::AddEqualityDelete)
                .copied()
                .unwrap_or_default(),
            self.reject_excluded,
            self.non_query,
            self.mismatches.len(),
        );
    }
}

/// One complete actionable mismatch diagnostic. It includes the source case
/// identity and both raw-AST renderings so convergence work has no opaque
/// corpus failure to rediscover.
#[derive(Debug)]
pub struct Mismatch {
    suite: String,
    source_file: String,
    case_id: String,
    step: usize,
    payload: usize,
    reason: String,
    original_sql: String,
    canonical_sql: Option<String>,
    legacy_original: Option<String>,
    legacy_canonical: Option<String>,
    first_legacy_difference: Option<String>,
    typed_original: Option<String>,
    typed_canonical: Option<String>,
    first_typed_difference: Option<String>,
}

struct MismatchLocation<'a> {
    suite: &'a str,
    case: &'a SqlCase,
    step: &'a SqlStep,
    payload: usize,
}

macro_rules! mismatch {
    ($suite:expr, $case:expr, $step:expr, $payload:expr, $($argument:expr),+ $(,)?) => {
        build_mismatch(
            MismatchLocation {
                suite: $suite,
                case: $case,
                step: $step,
                payload: $payload,
            },
            $($argument),+
        )
    };
}

impl std::fmt::Display for Mismatch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(formatter, "SQLP-4 query parser differential mismatch")?;
        writeln!(formatter, "  suite: {}", self.suite)?;
        writeln!(formatter, "  file: {}", self.source_file)?;
        writeln!(formatter, "  case: {}", self.case_id)?;
        writeln!(formatter, "  step: {}", self.step)?;
        writeln!(formatter, "  payload: {}", self.payload)?;
        writeln!(formatter, "  reason: {}", self.reason)?;
        writeln!(formatter, "  original SQL:\n{}", self.original_sql)?;
        writeln!(
            formatter,
            "  canonical SQL:\n{}",
            self.canonical_sql.as_deref().unwrap_or("<not available>")
        )?;
        writeln!(
            formatter,
            "  legacy original AST:\n{}",
            self.legacy_original.as_deref().unwrap_or("<not available>")
        )?;
        writeln!(
            formatter,
            "  legacy canonical AST:\n{}",
            self.legacy_canonical
                .as_deref()
                .unwrap_or("<not available>")
        )?;
        if let Some(difference) = &self.first_legacy_difference {
            writeln!(formatter, "  first legacy AST difference: {difference}")?;
        }
        writeln!(
            formatter,
            "  typed original AST:\n{}",
            self.typed_original.as_deref().unwrap_or("<not available>")
        )?;
        writeln!(
            formatter,
            "  typed canonical AST:\n{}",
            self.typed_canonical.as_deref().unwrap_or("<not available>")
        )?;
        if let Some(difference) = &self.first_typed_difference {
            writeln!(formatter, "  first typed AST difference: {difference}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueryClass {
    Query,
    ExplainQuery,
}

/// SQLP-5's typed statement kinds.  The differential deliberately reports
/// these separately: all of them require typed parse and printer checks, but
/// only the row-DML subset can be compared through the retiring sqlparser
/// production path.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum DdlDmlClass {
    TableDdl,
    Ctas,
    Insert,
    Delete,
    Update,
    Merge,
    AddEqualityDelete,
}

impl DdlDmlClass {
    const fn is_row_dml(self) -> bool {
        matches!(self, Self::Insert | Self::Delete | Self::Update | Self::Merge)
    }

    const fn label(self) -> &'static str {
        match self {
            Self::TableDdl => "table-ddl",
            Self::Ctas => "ctas",
            Self::Insert => "insert",
            Self::Delete => "delete",
            Self::Update => "update",
            Self::Merge => "merge",
            Self::AddEqualityDelete => "add-equality-delete",
        }
    }
}

/// Runs the selected corpus with no server, object-store, or connection work.
///
/// The caller has already resolved the canonical runner config and suite
/// selection. SQL files still flow through `load_sql_case_from_file`, which is
/// the single authoritative owner for runner metadata and placeholders.
pub fn run(
    base_dir: &Path,
    runner_config: &RunnerConfig,
    suite_names: &[String],
    suite_configs: &BTreeMap<String, SuiteConfig>,
    options: Options<'_>,
) -> Result<Summary> {
    if suite_names.is_empty() {
        bail!("no suites selected for query parser differential");
    }
    if suite_names.len() > 1 && (options.sql_dir.is_some() || options.sql_glob.is_some()) {
        bail!("--sql-dir and --sql-glob cannot be used with multiple suites");
    }

    let meta_re = regex::Regex::new(r"^--\s*@([a-zA-Z0-9_]+)\s*=\s*(.+?)\s*$")?;
    let marker_re = regex::Regex::new(r"(?i)^--\s*query\s+(\d+)(?:\s+.*)?$")?;
    let mut summary = Summary::default();

    for suite_name in suite_names {
        let suite = suite_configs
            .get(suite_name)
            .with_context(|| format!("selected suite {suite_name} is missing"))?;
        let sql_dir =
            resolve_path(options.sql_dir, base_dir).unwrap_or_else(|| suite.sql_dir.clone());
        let sql_glob = options.sql_glob.unwrap_or(&suite.sql_glob);
        let placeholder_vars = placeholder_variables(runner_config, &suite.name);
        let mut cases = load_selected_cases(
            suite,
            &sql_dir,
            sql_glob,
            &meta_re,
            &marker_re,
            &placeholder_vars,
            options,
        )?;
        for case in cases.drain(..) {
            inspect_case(&suite.name, &case, &mut summary);
        }
    }
    Ok(summary)
}

fn load_selected_cases(
    suite: &SuiteConfig,
    sql_dir: &Path,
    sql_glob: &str,
    meta_re: &regex::Regex,
    marker_re: &regex::Regex,
    placeholder_vars: &std::collections::HashMap<String, String>,
    options: Options<'_>,
) -> Result<Vec<SqlCase>> {
    if !sql_dir.exists() {
        bail!(
            "SQL directory not found for suite {}: {}",
            suite.name,
            sql_dir.display()
        );
    }
    let sql_files = list_sql_files(sql_dir, sql_glob)?;
    if sql_files.is_empty() {
        bail!(
            "no SQL files found in {} with pattern {} (suite {})",
            sql_dir.display(),
            sql_glob,
            suite.name
        );
    }

    let mut cases = Vec::new();
    for sql_file in sql_files {
        if let Some(case) = load_sql_case_from_file(&sql_file, meta_re, marker_re, placeholder_vars)
            .with_context(|| format!("failed to load SQL case {}", sql_file.display()))?
        {
            cases.push(case);
        }
    }
    let available_case_ids: HashSet<String> =
        cases.iter().map(|case| case.case_id.clone()).collect();
    let only_set = parse_selector_list(options.only, &available_case_ids, "--only")?;
    let skip_set = parse_selector_list(options.skip, &available_case_ids, "--skip")?;
    cases.retain(|case| {
        (only_set.is_empty() || only_set.contains(&case.case_id))
            && !skip_set.contains(&case.case_id)
    });
    if let Some(limit) = options.limit {
        cases.truncate(limit);
    }
    Ok(cases)
}

fn inspect_case(suite: &str, case: &SqlCase, summary: &mut Summary) {
    for step in &case.steps {
        summary.scanned += 1;
        if step.meta.has_error_expectation() {
            summary.reject_excluded += 1;
            continue;
        }
        if is_shell_step(&step.sql) {
            summary.non_query += 1;
            continue;
        }

        let payloads = match split_statement_payloads(step) {
            Ok(payloads) => payloads,
            Err(error) => {
                summary.mismatches.push(mismatch!(
                    suite,
                    case,
                    step,
                    0,
                    format!("could not lexically split an accept runner step: {error}"),
                    None,
                    None,
                    None,
                    None,
                ));
                continue;
            }
        };
        for (payload_index, payload) in payloads.iter().enumerate() {
            summary.statement_payloads += 1;
            inspect_payload(suite, case, payload, payload_index + 1, summary);
        }
    }
}

fn inspect_payload(
    suite: &str,
    case: &SqlCase,
    step: &SqlStep,
    payload_index: usize,
    summary: &mut Summary,
) {
    let typed = match catch_unwind(AssertUnwindSafe(|| parse_typed(&step.sql))) {
        Ok(result) => result,
        Err(_) => {
            summary.mismatches.push(mismatch(
                suite,
                case,
                step,
                payload_index,
                "typed parser panicked while handling an accept payload".to_string(),
                None,
                None,
                None,
                None,
            ));
            return;
        }
    };
    match &typed {
        Ok(statements) if classify_typed_ddl_dml(statements).is_some() => {
            inspect_ddl_dml(suite, case, step, payload_index, statements, summary);
            return;
        }
        Err(error) if ddl_dml_candidate(&step.sql) => {
            summary.mismatches.push(mismatch(
                suite,
                case,
                step,
                payload_index,
                format!("typed parser rejected a DDL/DML candidate: {error}"),
                None,
                None,
                None,
                None,
            ));
            return;
        }
        _ => {}
    }

    let legacy_original = match novarocks_sql::syntax::parse_sql_raw(&step.sql) {
        Ok(statement) => statement,
        Err(error) if is_typed_only_explain(&step.sql) => {
            inspect_typed_only_explain(suite, case, step, payload_index, summary);
            return;
        }
        Err(error) => {
            if query_candidate_after_legacy_rejection(&step.sql) {
                summary.mismatches.push(mismatch!(
                    suite,
                    case,
                    step,
                    payload_index,
                    format!("legacy parser rejected a query-shaped accept payload: {error}"),
                    None,
                    None,
                    None,
                    None,
                ));
            } else {
                summary.non_query += 1;
            }
            return;
        }
    };
    let Some(expected_class) = classify_legacy_query(&legacy_original) else {
        summary.non_query += 1;
        return;
    };
    summary.accept_query += 1;

    let typed_original = match typed {
        Ok(statements) => statements,
        Err(error) => {
            summary.mismatches.push(mismatch!(
                suite,
                case,
                step,
                payload_index,
                format!("typed parser rejected legacy-accepted query: {error}"),
                None,
                Some(&legacy_original),
                None,
                None,
            ));
            return;
        }
    };
    if !matches_typed_class(&typed_original, expected_class) {
        summary.mismatches.push(mismatch!(
            suite,
            case,
            step,
            payload_index,
            format!(
                "typed parser did not produce exactly one matching {:?} statement",
                expected_class
            ),
            None,
            Some(&legacy_original),
            None,
            Some(&typed_original),
        ));
        return;
    }

    let canonical_sql = print_statements(&typed_original);
    let typed_canonical = match parse_typed(&canonical_sql) {
        Ok(statements) => statements,
        Err(error) => {
            summary.mismatches.push(mismatch!(
                suite,
                case,
                step,
                payload_index,
                format!("canonical typed SQL did not reparse: {error}"),
                Some(canonical_sql),
                Some(&legacy_original),
                None,
                Some(&typed_original),
            ));
            return;
        }
    };
    if !typed_statements_syntax_eq(&typed_original, &typed_canonical) {
        let mut diagnostic = mismatch!(
            suite,
            case,
            step,
            payload_index,
            "typed parse-print-parse is not span-insensitively equivalent".to_owned(),
            Some(canonical_sql),
            Some(&legacy_original),
            None,
            Some(&typed_original),
        );
        diagnostic.typed_canonical = Some(debug_typed_ast(&typed_canonical));
        diagnostic.first_typed_difference = first_debug_difference(
            diagnostic.typed_original.as_deref(),
            diagnostic.typed_canonical.as_deref(),
        );
        summary.mismatches.push(diagnostic);
        return;
    }

    let legacy_canonical = match novarocks_sql::syntax::parse_sql_raw(&canonical_sql) {
        Ok(statement) => statement,
        Err(error) => {
            summary.mismatches.push(mismatch!(
                suite,
                case,
                step,
                payload_index,
                format!("legacy parser rejected canonical typed SQL: {error}"),
                Some(canonical_sql),
                Some(&legacy_original),
                None,
                Some(&typed_original),
            ));
            return;
        }
    };
    if classify_legacy_query(&legacy_canonical) != Some(expected_class) {
        summary.mismatches.push(mismatch!(
            suite,
            case,
            step,
            payload_index,
            "canonical typed SQL changed the legacy query classification".to_owned(),
            Some(canonical_sql),
            Some(&legacy_original),
            Some(&legacy_canonical),
            Some(&typed_original),
        ));
        return;
    }
    if !legacy_semantically_eq(&legacy_original, &legacy_canonical) {
        summary.mismatches.push(mismatch!(
            suite,
            case,
            step,
            payload_index,
            "legacy AST semantic equality differs after canonical typed SQL".to_owned(),
            Some(canonical_sql),
            Some(&legacy_original),
            Some(&legacy_canonical),
            Some(&typed_original),
        ));
    }
}

/// Runs SQLP-5's three DDL/DML differential layers.  The first two layers are
/// mandatory for every typed DDL/DML payload.  The third layer is intentionally
/// limited to the legacy parser's representable row-DML subset; CREATE TABLE,
/// CTAS, and ADD EQUALITY DELETE are reported as not applicable rather than as
/// a misleading pass.
fn inspect_ddl_dml(
    suite: &str,
    case: &SqlCase,
    step: &SqlStep,
    payload_index: usize,
    typed_original: &[TypedStatement],
    summary: &mut Summary,
) {
    let class = classify_typed_ddl_dml(typed_original)
        .expect("DDL/DML inspection is entered only for one typed SQLP-5 statement");
    *summary.ddl_dml_typed.entry(class).or_default() += 1;

    let canonical_sql = print_statements(typed_original);
    let typed_canonical = match parse_typed(&canonical_sql) {
        Ok(statements) => statements,
        Err(error) => {
            summary.mismatches.push(mismatch(
                suite,
                case,
                step,
                payload_index,
                format!("canonical typed DDL/DML SQL did not reparse: {error}"),
                Some(canonical_sql),
                None,
                None,
                Some(typed_original),
            ));
            return;
        }
    };
    if classify_typed_ddl_dml(&typed_canonical) != Some(class) {
        summary.mismatches.push(mismatch(
            suite,
            case,
            step,
            payload_index,
            "canonical typed SQL changed the DDL/DML statement classification".to_owned(),
            Some(canonical_sql),
            None,
            None,
            Some(typed_original),
        ));
        return;
    }
    if !typed_ddl_dml_syntax_eq(typed_original, &typed_canonical) {
        let mut diagnostic = mismatch(
            suite,
            case,
            step,
            payload_index,
            "typed DDL/DML parse-print-parse is not span-insensitively equivalent".to_owned(),
            Some(canonical_sql),
            None,
            None,
            Some(typed_original),
        );
        diagnostic.typed_canonical = Some(debug_typed_ast(&typed_canonical));
        diagnostic.first_typed_difference = first_debug_difference(
            diagnostic.typed_original.as_deref(),
            diagnostic.typed_canonical.as_deref(),
        );
        summary.mismatches.push(diagnostic);
        return;
    }
    *summary.ddl_dml_printer.entry(class).or_default() += 1;

    if !class.is_row_dml() {
        return;
    }

    let legacy_original = match novarocks_sql::planning::dml::parse_raw_statement(&step.sql) {
        Ok(statement) if legacy_matches_row_dml(&statement, class) => statement,
        Ok(_) | Err(_) => {
            *summary.row_dml_legacy_unavailable.entry(class).or_default() += 1;
            return;
        }
    };
    let legacy_canonical = match novarocks_sql::planning::dml::parse_raw_statement(&canonical_sql)
    {
        Ok(statement) if legacy_matches_row_dml(&statement, class) => statement,
        Ok(statement) => {
            summary.mismatches.push(mismatch(
                suite,
                case,
                step,
                payload_index,
                "canonical typed SQL changed the legacy row-DML classification".to_owned(),
                Some(canonical_sql),
                Some(&legacy_original),
                Some(&statement),
                Some(typed_original),
            ));
            return;
        }
        Err(error) => {
            summary.mismatches.push(mismatch(
                suite,
                case,
                step,
                payload_index,
                format!("legacy parser rejected canonical typed row DML: {error}"),
                Some(canonical_sql),
                Some(&legacy_original),
                None,
                Some(typed_original),
            ));
            return;
        }
    };
    if !legacy_semantically_eq(&legacy_original, &legacy_canonical) {
        summary.mismatches.push(mismatch(
            suite,
            case,
            step,
            payload_index,
            "legacy row-DML AST semantic equality differs after canonical typed SQL".to_owned(),
            Some(canonical_sql),
            Some(&legacy_original),
            Some(&legacy_canonical),
            Some(typed_original),
        ));
        return;
    }
    *summary.row_dml_semantic.entry(class).or_default() += 1;
}

fn inspect_typed_only_explain(
    suite: &str,
    case: &SqlCase,
    step: &SqlStep,
    payload_index: usize,
    summary: &mut Summary,
) {
    let typed_original = match parse_typed(&step.sql) {
        Ok(statements) => statements,
        Err(error) => {
            summary.mismatches.push(mismatch!(
                suite,
                case,
                step,
                payload_index,
                format!("typed-only EXPLAIN payload did not parse: {error}"),
                None,
                None,
                None,
                None,
            ));
            return;
        }
    };
    if !matches_typed_only_explain(&typed_original) {
        summary.mismatches.push(mismatch!(
            suite,
            case,
            step,
            payload_index,
            "raw-rejected EXPLAIN payload did not produce typed COSTS or LOGICAL Query EXPLAIN"
                .to_owned(),
            None,
            None,
            None,
            Some(&typed_original),
        ));
        return;
    }
    summary.typed_only_explain += 1;

    let canonical_sql = print_statements(&typed_original);
    let typed_canonical = match parse_typed(&canonical_sql) {
        Ok(statements) => statements,
        Err(error) => {
            summary.mismatches.push(mismatch!(
                suite,
                case,
                step,
                payload_index,
                format!("canonical typed-only EXPLAIN SQL did not reparse: {error}"),
                Some(canonical_sql),
                None,
                None,
                Some(&typed_original),
            ));
            return;
        }
    };
    if !typed_statements_syntax_eq(&typed_original, &typed_canonical) {
        let mut diagnostic = mismatch!(
            suite,
            case,
            step,
            payload_index,
            "typed-only EXPLAIN parse-print-parse is not span-insensitively equivalent".to_owned(),
            Some(canonical_sql),
            None,
            None,
            Some(&typed_original),
        );
        diagnostic.typed_canonical = Some(debug_typed_ast(&typed_canonical));
        diagnostic.first_typed_difference = first_debug_difference(
            diagnostic.typed_original.as_deref(),
            diagnostic.typed_canonical.as_deref(),
        );
        summary.mismatches.push(diagnostic);
    }
}

fn split_statement_payloads(step: &SqlStep) -> Result<Vec<SqlStep>, String> {
    let tokens = lex(&step.sql).map_err(|error| format!("{error:?}"))?;
    let mut payloads = Vec::new();
    let mut start = 0;
    for token in tokens {
        if !matches!(token.kind, TokenKind::Symbol(Symbol::Semicolon)) {
            continue;
        }
        push_payload(step, start, token.span.start(), &mut payloads);
        start = token.span.end();
    }
    push_payload(step, start, step.sql.len(), &mut payloads);
    Ok(payloads)
}

fn push_payload(step: &SqlStep, start: usize, end: usize, payloads: &mut Vec<SqlStep>) {
    let sql = &step.sql[start..end];
    if !sql.trim().is_empty() {
        payloads.push(SqlStep {
            query_number: step.query_number,
            sql: sql.to_owned(),
            meta: step.meta.clone(),
        });
    }
}

fn classify_legacy_query(statement: &LegacyStatement) -> Option<QueryClass> {
    match statement {
        LegacyStatement::Query(_) => Some(QueryClass::Query),
        LegacyStatement::Explain { statement, .. }
            if matches!(statement.as_ref(), LegacyStatement::Query(_)) =>
        {
            Some(QueryClass::ExplainQuery)
        }
        _ => None,
    }
}

fn classify_typed_ddl_dml(statements: &[TypedStatement]) -> Option<DdlDmlClass> {
    match statements {
        [TypedStatement::Table(_)] => Some(DdlDmlClass::TableDdl),
        [TypedStatement::Dml(novarocks_parser::ast::DmlStatement::CreateTableAsSelect(_))] => {
            Some(DdlDmlClass::Ctas)
        }
        [TypedStatement::Dml(novarocks_parser::ast::DmlStatement::Insert(_))] => {
            Some(DdlDmlClass::Insert)
        }
        [TypedStatement::Dml(novarocks_parser::ast::DmlStatement::Delete(_))] => {
            Some(DdlDmlClass::Delete)
        }
        [TypedStatement::Dml(novarocks_parser::ast::DmlStatement::Update(_))] => {
            Some(DdlDmlClass::Update)
        }
        [TypedStatement::Dml(novarocks_parser::ast::DmlStatement::Merge(_))] => {
            Some(DdlDmlClass::Merge)
        }
        [TypedStatement::Dml(novarocks_parser::ast::DmlStatement::AddEqualityDelete(_))] => {
            Some(DdlDmlClass::AddEqualityDelete)
        }
        _ => None,
    }
}

fn legacy_matches_row_dml(statement: &LegacyStatement, expected: DdlDmlClass) -> bool {
    matches!(
        (expected, statement),
        (DdlDmlClass::Insert, LegacyStatement::Insert(_))
            | (DdlDmlClass::Delete, LegacyStatement::Delete(_))
            | (DdlDmlClass::Update, LegacyStatement::Update { .. })
            | (DdlDmlClass::Merge, LegacyStatement::Merge(_))
    )
}

/// Detects only SQLP-5's top-level DDL/DML family when typed parsing fails.
/// Successful parsing remains authoritative, so this guard exists solely to
/// turn a newly missing grammar production into an actionable mismatch instead
/// of silently counting it as a non-query command.
fn ddl_dml_candidate(sql: &str) -> bool {
    // INSERT/DELETE/UPDATE/MERGE are intentionally contextual words in the
    // lexer, so failure classification must preserve source spelling rather
    // than looking only for `TokenKind::Keyword` variants.
    let upper = sql.trim_start().to_ascii_uppercase();
    let mut words = upper.split_whitespace();
    match words.next() {
        Some("INSERT" | "DELETE" | "UPDATE" | "MERGE") => true,
        Some("CREATE") => words.any(|word| word.trim_matches(|c: char| !c.is_ascii_alphabetic()) == "TABLE"),
        Some("ALTER") => upper.contains("ADD EQUALITY DELETE"),
        _ => false,
    }
}

fn display_class_counts(counts: &BTreeMap<DdlDmlClass, usize>) -> String {
    if counts.is_empty() {
        return "none".to_owned();
    }
    counts
        .iter()
        .map(|(class, count)| format!("{}={count}", class.label()))
        .collect::<Vec<_>>()
        .join(",")
}

/// The raw production parser cannot parse every NovaRocks command family. For
/// those parse failures we still need an explicit corpus class, not a hidden
/// case allowlist: only a grammar-shaped top-level Query is a mismatch. This
/// classifier is deliberately token based and narrow; it never accepts an
/// unknown statement as a Query.
fn query_candidate_after_legacy_rejection(sql: &str) -> bool {
    let Ok(tokens) = lex(sql) else {
        return false;
    };
    let significant: Vec<&TokenKind> = tokens
        .iter()
        .map(|token| &token.kind)
        .filter(|kind| !matches!(kind, TokenKind::Trivia(_) | TokenKind::End))
        .collect();
    let Some(first) = significant.first() else {
        return false;
    };
    if matches!(
        first,
        TokenKind::Keyword(Keyword::Select | Keyword::With | Keyword::Values)
    ) {
        return true;
    }
    if matches!(first, TokenKind::Symbol(Symbol::LParen)) {
        return significant.get(1).is_some_and(is_query_start);
    }
    if !matches!(first, TokenKind::Keyword(Keyword::Explain)) {
        return false;
    }
    let mut index = 1;
    while matches!(
        significant.get(index),
        Some(TokenKind::Keyword(
            Keyword::Analyze | Keyword::Verbose | Keyword::Costs | Keyword::Logical
        ))
    ) {
        index += 1;
    }
    significant.get(index).is_some_and(is_query_start)
}

fn is_query_start(kind: &&TokenKind) -> bool {
    matches!(
        kind,
        TokenKind::Keyword(Keyword::Select | Keyword::With | Keyword::Values)
            | TokenKind::Symbol(Symbol::LParen)
    )
}

fn is_typed_only_explain(sql: &str) -> bool {
    let Ok(tokens) = lex(sql) else {
        return false;
    };
    let significant: Vec<&TokenKind> = tokens
        .iter()
        .map(|token| &token.kind)
        .filter(|kind| !matches!(kind, TokenKind::Trivia(_) | TokenKind::End))
        .collect();
    matches!(
        significant.as_slice(),
        [
            TokenKind::Keyword(Keyword::Explain),
            TokenKind::Keyword(Keyword::Costs | Keyword::Logical),
            query_start,
            ..
        ] if is_query_start(query_start)
    )
}

fn matches_typed_class(statements: &[TypedStatement], expected: QueryClass) -> bool {
    matches!(
        (expected, statements),
        (QueryClass::Query, [TypedStatement::Query(_)])
            | (QueryClass::ExplainQuery, [TypedStatement::ExplainQuery(_)])
    )
}

fn matches_typed_only_explain(statements: &[TypedStatement]) -> bool {
    matches!(
        statements,
        [TypedStatement::ExplainQuery(explain)]
            if matches!(
                explain.format,
                novarocks_parser::ast::ExplainFormat::Costs
                    | novarocks_parser::ast::ExplainFormat::Logical
            )
    )
}

fn typed_statements_syntax_eq(left: &[TypedStatement], right: &[TypedStatement]) -> bool {
    match (left, right) {
        ([TypedStatement::Query(left)], [TypedStatement::Query(right)]) => left.syntax_eq(right),
        ([TypedStatement::ExplainQuery(left)], [TypedStatement::ExplainQuery(right)]) => {
            left.syntax_eq(right)
        }
        _ => false,
    }
}

/// SQLP-5 AST nodes currently derive `Eq`, which includes every `Span`.  The
/// runner is intentionally the only file owned by T3, so it compares the
/// deterministic debug tree after removing only span carriers rather than
/// adding a second production equality API here.  This retains all parser
/// syntax fields while making a printer round trip independent of offsets.
fn typed_ddl_dml_syntax_eq(left: &[TypedStatement], right: &[TypedStatement]) -> bool {
    normalized_typed_ddl_dml_ast(left) == normalized_typed_ddl_dml_ast(right)
}

fn mismatch(
    suite: &str,
    case: &SqlCase,
    step: &SqlStep,
    payload: usize,
    reason: String,
    canonical_sql: Option<String>,
    legacy_original: Option<&LegacyStatement>,
    legacy_canonical: Option<&LegacyStatement>,
    typed_original: Option<&[TypedStatement]>,
) -> Mismatch {
    build_mismatch(
        MismatchLocation {
            suite,
            case,
            step,
            payload,
        },
        reason,
        canonical_sql,
        legacy_original,
        legacy_canonical,
        typed_original,
    )
}

fn build_mismatch(
    location: MismatchLocation<'_>,
    reason: String,
    canonical_sql: Option<String>,
    legacy_original: Option<&LegacyStatement>,
    legacy_canonical: Option<&LegacyStatement>,
    typed_original: Option<&[TypedStatement]>,
) -> Mismatch {
    let normalized_legacy_original = legacy_original.map(normalized_legacy_ast);
    let normalized_legacy_canonical = legacy_canonical.map(normalized_legacy_ast);
    let legacy_original = legacy_original.map(debug_ast);
    let legacy_canonical = legacy_canonical.map(debug_ast);
    let typed_original = typed_original.map(debug_typed_ast);
    let first_legacy_difference = first_debug_difference(
        normalized_legacy_original.as_deref(),
        normalized_legacy_canonical.as_deref(),
    );
    Mismatch {
        suite: location.suite.to_owned(),
        source_file: location.case.source_file.display().to_string(),
        case_id: location.case.case_id.clone(),
        step: location.step.query_number,
        payload: location.payload,
        reason,
        original_sql: location.step.sql.clone(),
        canonical_sql,
        legacy_original,
        legacy_canonical,
        first_legacy_difference,
        typed_original,
        typed_canonical: None,
        first_typed_difference: None,
    }
}

fn debug_ast(statement: &LegacyStatement) -> String {
    format!("{statement:#?}")
}

/// `sqlparser` derives `PartialEq` for source spans and concrete keyword
/// tokens together with SQL structure. A canonical printer necessarily
/// relocates spans, case-normalizes keywords, and canonicalizes StarRocks'
/// equivalent single- and double-quoted string spellings. The crate's visitor
/// API has no hook that can rewrite every such carrier; normalize its
/// deterministic structural debug tree instead while retaining every semantic
/// AST field and value.
fn legacy_semantically_eq(left: &LegacyStatement, right: &LegacyStatement) -> bool {
    normalized_legacy_ast(left) == normalized_legacy_ast(right)
}

fn normalized_legacy_ast(statement: &LegacyStatement) -> String {
    let without_spans = strip_debug_spans(&debug_ast(statement));
    let without_tokens = strip_debug_blocks(&without_spans, "TokenWithSpan {");
    without_tokens.replace("DoubleQuotedString(", "SingleQuotedString(")
}

/// Removes only a `span: Span(...)` field from the debug tree. `Span`'s
/// compact `Debug` implementation nests `Location(...)` values, hence simple
/// line filtering would leave every span in a one-line AST rendering.
fn strip_debug_spans(debug: &str) -> String {
    const SPAN_FIELD: &str = "span: Span(";

    let mut normalized = String::with_capacity(debug.len());
    let mut cursor = 0;
    while let Some(relative_start) = debug[cursor..].find(SPAN_FIELD) {
        let start = cursor + relative_start;
        normalized.push_str(&debug[cursor..start]);

        let mut index = start + SPAN_FIELD.len();
        let mut depth = 1usize;
        while depth > 0 {
            match debug.as_bytes().get(index) {
                Some(b'(') => depth += 1,
                Some(b')') => depth -= 1,
                Some(_) => {}
                None => unreachable!("sqlparser Span debug rendering must close its parentheses"),
            }
            index += 1;
        }
        if matches!(debug.as_bytes().get(index), Some(b',')) {
            index += 1;
            if matches!(debug.as_bytes().get(index), Some(b' ')) {
                index += 1;
            }
        }
        cursor = index;
    }
    normalized.push_str(&debug[cursor..]);
    normalized
}

/// Removes `TokenWithSpan` source carriers such as `select_token` and
/// `with_token`. Their keyword spelling and coordinates are lexical metadata;
/// the corresponding typed AST fields retain the statement's meaning.
fn strip_debug_blocks(debug: &str, marker: &str) -> String {
    let mut normalized = String::with_capacity(debug.len());
    let mut cursor = 0;
    while let Some(relative_start) = debug[cursor..].find(marker) {
        let start = cursor + relative_start;
        normalized.push_str(&debug[cursor..start]);

        let mut index = start + marker.len();
        let mut depth = 1usize;
        while depth > 0 {
            match debug.as_bytes().get(index) {
                Some(b'{') => depth += 1,
                Some(b'}') => depth -= 1,
                Some(_) => {}
                None => {
                    unreachable!("sqlparser TokenWithSpan debug rendering must close its braces")
                }
            }
            index += 1;
        }
        normalized.push_str("<token>");
        cursor = index;
    }
    normalized.push_str(&debug[cursor..]);
    normalized
}

fn debug_typed_ast(statements: &[TypedStatement]) -> String {
    format!("{statements:#?}")
}

fn normalized_typed_ddl_dml_ast(statements: &[TypedStatement]) -> String {
    strip_debug_blocks(&debug_typed_ast(statements), "span: Span {")
}

fn first_debug_difference(left: Option<&str>, right: Option<&str>) -> Option<String> {
    let (left, right) = (left?, right?);
    let mut left_lines = left.lines();
    let mut right_lines = right.lines();
    for index in 1usize.. {
        match (left_lines.next(), right_lines.next()) {
            (Some(left), Some(right)) if left == right => continue,
            (left, right) => {
                return Some(format!(
                    "line {index}: original={:?}, canonical={:?}",
                    left.unwrap_or("<end>"),
                    right.unwrap_or("<end>")
                ));
            }
        }
    }
    unreachable!("different debug strings must have a first differing line")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{suite_manifest::SuiteManifest, types::RunnerConfig};
    use std::{fs, path::PathBuf};

    fn fixture_suite(sql_dir: PathBuf) -> SuiteConfig {
        SuiteConfig {
            name: "fixture".to_owned(),
            sql_dir,
            result_dir: None,
            sql_glob: "*.sql".to_owned(),
            default_catalog: "default_catalog".to_owned(),
            default_db: String::new(),
            auto_case_db: false,
            verify_default: true,
            init_sql: None,
            cleanup_sql: None,
            manifest: SuiteManifest::default(),
        }
    }

    fn run_fixture(content: &str) -> Summary {
        let temporary = tempfile::tempdir().expect("temporary fixture directory");
        let sql_dir = temporary.path().join("sql");
        fs::create_dir_all(&sql_dir).expect("create SQL fixture directory");
        fs::write(sql_dir.join("fixture.sql"), content).expect("write SQL fixture");
        let suite = fixture_suite(sql_dir);
        let suites = BTreeMap::from([(suite.name.clone(), suite)]);
        run(
            temporary.path(),
            &RunnerConfig::default(),
            &["fixture".to_owned()],
            &suites,
            Options::default(),
        )
        .expect("differential fixture runs without runtime dependencies")
    }

    #[test]
    fn inventories_multistep_placeholders_rejects_and_non_queries_without_server() {
        let summary =
            run_fixture("-- query 1\nSELECT '${case_db}';\n-- query 2\nEXPLAIN SELECT 1;\n");
        assert_eq!(summary.scanned, 2);
        assert_eq!(summary.accept_query, 2);
        assert_eq!(summary.reject_excluded, 0);
        assert_eq!(summary.non_query, 0);
        assert!(summary.mismatches.is_empty(), "{summary:#?}");

        let rejected = run_fixture("-- @expect_error = syntax\nSELECT FROM;");
        assert_eq!(rejected.scanned, 1);
        assert_eq!(rejected.reject_excluded, 1);
        assert!(rejected.mismatches.is_empty());

        let non_query = run_fixture("CREATE TABLE t (a INT);");
        assert_eq!(non_query.scanned, 1);
        assert_eq!(non_query.non_query, 0);
        assert_eq!(non_query.ddl_dml_typed.get(&DdlDmlClass::TableDdl), Some(&1));
        assert!(non_query.mismatches.is_empty());

        let legacy_rejected_command =
            run_fixture("CREATE CATALOG fixture PROPERTIES(\"type\"=\"x\");");
        assert_eq!(legacy_rejected_command.scanned, 1);
        assert_eq!(legacy_rejected_command.non_query, 1);
        assert!(legacy_rejected_command.mismatches.is_empty());
    }

    #[test]
    fn inventories_ddl_dml_at_each_applicable_oracle_layer() {
        let summary = run_fixture(
            "CREATE TABLE t (a INT);\n\
             CREATE TABLE ctas AS SELECT 1;\n\
             INSERT INTO t VALUES (1);\n\
             DELETE FROM t WHERE a = 1;\n\
             UPDATE t SET a = 2 WHERE a = 1;\n\
             MERGE INTO t USING s ON t.a = s.a WHEN MATCHED THEN DELETE;\n\
             ALTER TABLE t ADD EQUALITY DELETE (a) VALUES (1);",
        );

        assert_eq!(summary.statement_payloads, 7);
        assert_eq!(summary.ddl_dml_typed.get(&DdlDmlClass::TableDdl), Some(&1));
        assert_eq!(summary.ddl_dml_typed.get(&DdlDmlClass::Ctas), Some(&1));
        assert_eq!(summary.ddl_dml_typed.get(&DdlDmlClass::Insert), Some(&1));
        assert_eq!(summary.ddl_dml_typed.get(&DdlDmlClass::Delete), Some(&1));
        assert_eq!(summary.ddl_dml_typed.get(&DdlDmlClass::Update), Some(&1));
        assert_eq!(summary.ddl_dml_typed.get(&DdlDmlClass::Merge), Some(&1));
        assert_eq!(
            summary.ddl_dml_typed.get(&DdlDmlClass::AddEqualityDelete),
            Some(&1)
        );
        assert_eq!(summary.ddl_dml_typed, summary.ddl_dml_printer);
        assert_eq!(
            summary.row_dml_semantic.get(&DdlDmlClass::Insert),
            Some(&1)
        );
        assert_eq!(
            summary.row_dml_semantic.get(&DdlDmlClass::Delete),
            Some(&1)
        );
        assert_eq!(
            summary.row_dml_semantic.get(&DdlDmlClass::Update),
            Some(&1)
        );
        assert_eq!(
            summary.row_dml_semantic.get(&DdlDmlClass::Merge),
            Some(&1)
        );
        assert!(summary.row_dml_legacy_unavailable.is_empty());
        assert!(summary.mismatches.is_empty(), "{summary:#?}");
    }

    #[test]
    fn splits_accept_steps_lexically_and_counts_typed_only_explain() {
        let summary = run_fixture(
            "-- query 1\nSELECT ';' AS value; SET query_timeout = 1; EXPLAIN COSTS SELECT 2;",
        );
        assert_eq!(summary.scanned, 1);
        assert_eq!(summary.statement_payloads, 3);
        assert_eq!(summary.accept_query, 1);
        assert_eq!(summary.typed_only_explain, 1);
        assert_eq!(summary.non_query, 1);
        assert!(summary.mismatches.is_empty(), "{summary:#?}");
    }

    #[test]
    fn classifies_shell_steps_without_lexing_them_as_sql() {
        let summary = run_fixture("shell: printf 'a;b'\n");
        assert_eq!(summary.scanned, 1);
        assert_eq!(summary.statement_payloads, 0);
        assert_eq!(summary.non_query, 1);
        assert!(summary.mismatches.is_empty(), "{summary:#?}");
    }

    #[test]
    fn legacy_query_classifier_accepts_only_query_and_query_explain() {
        let query = novarocks_sql::syntax::parse_sql_raw("SELECT 1").expect("legacy query");
        let explain =
            novarocks_sql::syntax::parse_sql_raw("EXPLAIN SELECT 1").expect("legacy explain query");
        let command =
            novarocks_sql::syntax::parse_sql_raw("CREATE TABLE t (a INT)").expect("legacy command");
        assert_eq!(classify_legacy_query(&query), Some(QueryClass::Query));
        assert_eq!(
            classify_legacy_query(&explain),
            Some(QueryClass::ExplainQuery)
        );
        assert_eq!(classify_legacy_query(&command), None);
    }

    #[test]
    fn mismatch_diagnostics_include_case_identity_and_ast_difference() {
        let original = novarocks_sql::syntax::parse_sql_raw("SELECT 1").expect("original AST");
        let canonical = novarocks_sql::syntax::parse_sql_raw("SELECT 2").expect("canonical AST");
        let case = SqlCase {
            source_file: PathBuf::from("fixture.sql"),
            case_id: "fixture".to_owned(),
            steps: Vec::new(),
            case_dbs: Vec::new(),
            sequential: false,
        };
        let step = SqlStep {
            query_number: 7,
            sql: "SELECT 1".to_owned(),
            meta: Default::default(),
        };
        let diagnostic = mismatch!(
            "fixture",
            &case,
            &step,
            1,
            "semantic mismatch".to_owned(),
            Some("SELECT 2".to_owned()),
            Some(&original),
            Some(&canonical),
            None,
        )
        .to_string();
        assert!(diagnostic.contains("suite: fixture"));
        assert!(diagnostic.contains("case: fixture"));
        assert!(diagnostic.contains("step: 7"));
        assert!(diagnostic.contains("payload: 1"));
        assert!(diagnostic.contains("first legacy AST difference"));
    }

    #[test]
    fn legacy_semantic_equality_ignores_source_carriers_but_not_sql_values() {
        let compact = novarocks_sql::syntax::parse_sql_raw("SELECT value FROM source")
            .expect("compact legacy AST");
        let spaced = novarocks_sql::syntax::parse_sql_raw("SELECT\n  value\nFROM source")
            .expect("spaced legacy AST");
        let different = novarocks_sql::syntax::parse_sql_raw("SELECT other FROM source")
            .expect("different legacy AST");

        assert_ne!(debug_ast(&compact), debug_ast(&spaced));
        assert_eq!(
            normalized_legacy_ast(&compact),
            normalized_legacy_ast(&spaced),
            "only source positions should differ"
        );
        assert!(legacy_semantically_eq(&compact, &spaced));
        assert!(!legacy_semantically_eq(&compact, &different));

        let lowercase =
            novarocks_sql::syntax::parse_sql_raw("with cte as (SELECT 1) SELECT * FROM cte")
                .expect("lowercase keyword legacy AST");
        let uppercase =
            novarocks_sql::syntax::parse_sql_raw("WITH cte AS (SELECT 1) SELECT * FROM cte")
                .expect("uppercase keyword legacy AST");
        assert!(legacy_semantically_eq(&lowercase, &uppercase));

        let double_quoted = novarocks_sql::syntax::parse_sql_raw("SELECT \"value\"")
            .expect("double-quoted string legacy AST");
        let single_quoted = novarocks_sql::syntax::parse_sql_raw("SELECT 'value'")
            .expect("single-quoted string legacy AST");
        let different_quoted = novarocks_sql::syntax::parse_sql_raw("SELECT 'other'")
            .expect("different string legacy AST");
        assert!(legacy_semantically_eq(&double_quoted, &single_quoted));
        assert!(!legacy_semantically_eq(&double_quoted, &different_quoted));

        let ignore_first = novarocks_sql::syntax::parse_sql_raw(
            "SELECT LEAD(value IGNORE NULLS, 1) OVER (ORDER BY key) FROM source",
        )
        .expect("inner null treatment legacy AST");
        let ignore_last = novarocks_sql::syntax::parse_sql_raw(
            "SELECT LEAD(value, 1 IGNORE NULLS) OVER (ORDER BY key) FROM source",
        )
        .expect("canonical null treatment legacy AST");
        let respect = novarocks_sql::syntax::parse_sql_raw(
            "SELECT LEAD(value, 1 RESPECT NULLS) OVER (ORDER BY key) FROM source",
        )
        .expect("different null treatment legacy AST");
        assert!(legacy_semantically_eq(&ignore_first, &ignore_last));
        assert!(!legacy_semantically_eq(&ignore_first, &respect));
    }

    #[test]
    fn only_and_skip_reuse_runner_case_selection() {
        let temporary = tempfile::tempdir().expect("temporary fixture directory");
        let sql_dir = temporary.path().join("sql");
        fs::create_dir_all(&sql_dir).expect("create SQL fixture directory");
        fs::write(sql_dir.join("first.sql"), "SELECT 1;").expect("first SQL fixture");
        fs::write(sql_dir.join("second.sql"), "SELECT 2;").expect("second SQL fixture");
        let suite = fixture_suite(sql_dir);
        let suites = BTreeMap::from([(suite.name.clone(), suite)]);
        let summary = run(
            temporary.path(),
            &RunnerConfig::default(),
            &["fixture".to_owned()],
            &suites,
            Options {
                only: Some("second"),
                ..Options::default()
            },
        )
        .expect("selected fixture runs");
        assert_eq!(summary.scanned, 1);
        assert_eq!(summary.accept_query, 1);
    }

    #[test]
    fn all_selection_remains_owned_by_suite_manifest() {
        let suite = fixture_suite(PathBuf::from("fixture/sql"));
        let suites = BTreeMap::from([(suite.name.clone(), suite)]);
        assert_eq!(select_suite_names("all", &suites).unwrap(), vec!["fixture"]);
    }
}
