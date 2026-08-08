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

#[derive(Clone, Debug, Eq, PartialEq)]
enum Token {
    Identifier(String),
    PathSeparator,
    OpenBrace,
    CloseBrace,
    OpenBracket,
    CloseBracket,
    OpenParen,
    CloseParen,
    Hash,
    Comma,
    Semicolon,
}

impl Token {
    fn is_identifier(&self, expected: &str) -> bool {
        matches!(self, Self::Identifier(identifier) if identifier == expected)
    }
}

fn tokenize_rust(source: &str) -> Vec<Token> {
    let bytes = source.as_bytes();
    let mut tokens = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'/' if bytes.get(index + 1) == Some(&b'/') => {
                index += 2;
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index += 2;
                while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/')
                {
                    index += 1;
                }
                index = (index + 2).min(bytes.len());
            }
            b'"' => skip_quoted(bytes, &mut index, b'"'),
            b'\'' if is_char_literal(bytes, index) => skip_quoted(bytes, &mut index, b'\''),
            b'r' if skip_raw_string(bytes, &mut index) => {}
            b'r' if bytes.get(index + 1) == Some(&b'#')
                && bytes
                    .get(index + 2)
                    .is_some_and(|byte| byte.is_ascii_alphabetic() || *byte == b'_') =>
            {
                index += 2;
                let start = index;
                index += 1;
                while index < bytes.len()
                    && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_')
                {
                    index += 1;
                }
                tokens.push(Token::Identifier(
                    String::from_utf8_lossy(&bytes[start..index]).into_owned(),
                ));
            }
            byte if byte.is_ascii_alphabetic() || byte == b'_' => {
                let start = index;
                index += 1;
                while index < bytes.len()
                    && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_')
                {
                    index += 1;
                }
                tokens.push(Token::Identifier(
                    String::from_utf8_lossy(&bytes[start..index]).into_owned(),
                ));
            }
            b':' if bytes.get(index + 1) == Some(&b':') => {
                tokens.push(Token::PathSeparator);
                index += 2;
            }
            b'{' => push_token(&mut tokens, &mut index, Token::OpenBrace),
            b'}' => push_token(&mut tokens, &mut index, Token::CloseBrace),
            b'[' => push_token(&mut tokens, &mut index, Token::OpenBracket),
            b']' => push_token(&mut tokens, &mut index, Token::CloseBracket),
            b'(' => push_token(&mut tokens, &mut index, Token::OpenParen),
            b')' => push_token(&mut tokens, &mut index, Token::CloseParen),
            b'#' => push_token(&mut tokens, &mut index, Token::Hash),
            b',' => push_token(&mut tokens, &mut index, Token::Comma),
            b';' => push_token(&mut tokens, &mut index, Token::Semicolon),
            _ => index += 1,
        }
    }
    tokens
}

fn push_token(tokens: &mut Vec<Token>, index: &mut usize, token: Token) {
    tokens.push(token);
    *index += 1;
}

fn skip_quoted(bytes: &[u8], index: &mut usize, quote: u8) {
    *index += 1;
    while *index < bytes.len() {
        if bytes[*index] == b'\\' {
            *index += 2;
        } else if bytes[*index] == quote {
            *index += 1;
            break;
        } else {
            *index += 1;
        }
    }
}

fn is_char_literal(bytes: &[u8], index: usize) -> bool {
    let mut cursor = index + 1;
    if bytes.get(cursor) == Some(&b'\\') {
        cursor += 2;
    } else {
        cursor += 1;
    }
    bytes.get(cursor) == Some(&b'\'')
}

fn skip_raw_string(bytes: &[u8], index: &mut usize) -> bool {
    let start = *index;
    let mut cursor = start + 1;
    while bytes.get(cursor) == Some(&b'#') {
        cursor += 1;
    }
    if bytes.get(cursor) != Some(&b'"') {
        return false;
    }
    let hashes = cursor - start - 1;
    cursor += 1;
    while cursor < bytes.len() {
        if bytes[cursor] == b'"'
            && bytes
                .get(cursor + 1..cursor + 1 + hashes)
                .is_some_and(|suffix| suffix.iter().all(|byte| *byte == b'#'))
        {
            *index = cursor + 1 + hashes;
            return true;
        }
        cursor += 1;
    }
    *index = bytes.len();
    true
}

fn is_cfg_test_attribute(tokens: &[Token], start: usize) -> Option<usize> {
    matches!(
        tokens.get(start..start + 8),
        Some([
            Token::Hash,
            Token::OpenBracket,
            Token::Identifier(cfg),
            Token::OpenParen,
            Token::Identifier(test),
            Token::CloseParen,
            Token::CloseBracket,
            ..
        ]) if cfg == "cfg" && test == "test"
    )
    .then_some(start + 7)
}

fn cfg_test_item_end(tokens: &[Token], start: usize) -> usize {
    let mut cursor = start;
    while cursor < tokens.len() {
        match tokens[cursor] {
            Token::Semicolon => return cursor + 1,
            Token::OpenBrace => {
                let mut depth = 1;
                cursor += 1;
                while cursor < tokens.len() && depth > 0 {
                    match tokens[cursor] {
                        Token::OpenBrace => depth += 1,
                        Token::CloseBrace => depth -= 1,
                        _ => {}
                    }
                    cursor += 1;
                }
                return cursor;
            }
            _ => cursor += 1,
        }
    }
    tokens.len()
}

fn production_tokens(source: &str) -> Vec<Token> {
    let tokens = tokenize_rust(source);
    let mut production = Vec::new();
    let mut index = 0;
    while index < tokens.len() {
        if let Some(item_start) = is_cfg_test_attribute(&tokens, index) {
            index = cfg_test_item_end(&tokens, item_start);
        } else {
            production.push(tokens[index].clone());
            index += 1;
        }
    }
    production
}

fn group_only_starts_with(tokens: &[Token], open_brace: usize, allowed: &str) -> bool {
    let mut depth = 0;
    let mut entry_start = true;
    for token in &tokens[open_brace..] {
        match token {
            Token::OpenBrace => depth += 1,
            Token::CloseBrace => {
                depth -= 1;
                if depth == 0 {
                    return true;
                }
            }
            Token::Comma if depth == 1 => entry_start = true,
            Token::Identifier(identifier) if depth == 1 && entry_start => {
                if identifier != allowed {
                    return false;
                }
                entry_start = false;
            }
            _ => {}
        }
    }
    false
}

fn group_contains_start(tokens: &[Token], open_brace: usize, expected: &str) -> bool {
    let mut depth = 0;
    let mut entry_start = true;
    for token in &tokens[open_brace..] {
        match token {
            Token::OpenBrace => depth += 1,
            Token::CloseBrace => {
                depth -= 1;
                if depth == 0 {
                    return false;
                }
            }
            Token::Comma if depth == 1 => entry_start = true,
            Token::Identifier(identifier) if depth == 1 && entry_start => {
                if identifier == expected {
                    return true;
                }
                entry_start = false;
            }
            _ => {}
        }
    }
    false
}

fn path_child_is(tokens: &[Token], parent: &str, child: &str) -> bool {
    for (index, token) in tokens.iter().enumerate() {
        if !token.is_identifier(parent) || tokens.get(index + 1) != Some(&Token::PathSeparator) {
            continue;
        }
        match tokens.get(index + 2) {
            Some(token) if token.is_identifier(child) => return true,
            Some(Token::OpenBrace) if group_contains_start(tokens, index + 2, child) => {
                return true;
            }
            _ => {}
        }
    }
    false
}

fn contains_sql_planner_use_path(source: &str) -> bool {
    path_child_is(&production_tokens(source), "sql", "planner")
}

fn contains_non_facade_sql_access(source: &str) -> bool {
    let tokens = production_tokens(source);
    for (index, token) in tokens.iter().enumerate() {
        if !token.is_identifier("sql") || tokens.get(index + 1) != Some(&Token::PathSeparator) {
            continue;
        }
        let rooted_at_crate = index >= 2
            && tokens[index - 1] == Token::PathSeparator
            && tokens[index - 2].is_identifier("crate");
        let facade_access = rooted_at_crate
            && match tokens.get(index + 2) {
                Some(token) if token.is_identifier("plan_read") => true,
                Some(Token::OpenBrace) => group_only_starts_with(&tokens, index + 2, "plan_read"),
                _ => false,
            };
        if !facade_access {
            return true;
        }
    }
    false
}

fn contains_backend_sql_dependency(source: &str) -> bool {
    path_child_is(&production_tokens(source), "novarocks", "sql")
}

fn contains_any_sql_access(source: &str) -> bool {
    let tokens = production_tokens(source);
    tokens.iter().enumerate().any(|(index, token)| {
        token.is_identifier("sql") && tokens.get(index + 1) == Some(&Token::PathSeparator)
    })
}

fn rust_sources(root: &Path) -> Vec<PathBuf> {
    let mut sources = Vec::new();
    for entry in fs::read_dir(root).expect("source directory should be readable") {
        let path = entry
            .expect("source directory entry should be readable")
            .path();
        if path.is_dir() {
            sources.extend(rust_sources(&path));
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            sources.push(path);
        }
    }
    sources
}

fn module_directory(module_path: &Path) -> PathBuf {
    let parent = module_path
        .parent()
        .expect("module path should have a parent directory");
    if module_path.file_name().is_some_and(|name| name == "mod.rs") {
        parent.to_owned()
    } else {
        parent.join(
            module_path
                .file_stem()
                .expect("module path should have a file stem"),
        )
    }
}

fn declared_file_modules(tokens: &[Token]) -> Vec<String> {
    let mut modules = Vec::new();
    for (index, token) in tokens.iter().enumerate() {
        if !token.is_identifier("mod") {
            continue;
        }
        let Some(Token::Identifier(name)) = tokens.get(index + 1) else {
            continue;
        };
        if tokens.get(index + 2) == Some(&Token::Semicolon) {
            modules.push(name.clone());
        }
    }
    modules
}

fn resolve_module_path(parent_module: &Path, child: &str) -> Option<PathBuf> {
    let directory = module_directory(parent_module);
    let flat = directory.join(format!("{child}.rs"));
    let nested = directory.join(child).join("mod.rs");
    flat.exists()
        .then_some(flat)
        .or_else(|| nested.exists().then_some(nested))
}

fn production_module_graph(root_module: &Path) -> Vec<PathBuf> {
    let mut modules = Vec::new();
    let mut seen = BTreeSet::new();
    let mut pending = vec![root_module.to_owned()];
    while let Some(module_path) = pending.pop() {
        if !seen.insert(module_path.clone()) {
            continue;
        }
        let source = fs::read_to_string(&module_path).expect("module source should be readable");
        let tokens = production_tokens(&source);
        for child in declared_file_modules(&tokens) {
            if let Some(path) = resolve_module_path(&module_path, &child) {
                pending.push(path);
            }
        }
        modules.push(module_path);
    }
    modules
}

fn source_contains<F>(sources: impl IntoIterator<Item = PathBuf>, predicate: F) -> Option<PathBuf>
where
    F: Fn(&str) -> bool,
{
    sources.into_iter().find(|source_path| {
        let source = fs::read_to_string(source_path).expect("source should be readable");
        predicate(&source)
    })
}

#[test]
fn sql_access_classifier_rejects_non_facade_forms() {
    for source in [
        "use crate::sql::planner::distributed::PlanFragment;",
        "use crate::sql::{plan_read::DistributedPlan, r#planner::PlanFragment};",
        "let _ = crate::sql::planner::distributed::PlanFragment;",
        "use crate::r#sql::r#planner::distributed::PlanFragment;",
    ] {
        assert!(
            contains_non_facade_sql_access(source),
            "{source} should be rejected"
        );
    }
}

#[test]
fn sql_access_classifier_accepts_only_facade_and_ignores_test_content() {
    for source in [
        "use crate::sql::plan_read::DistributedPlan;",
        "use crate::sql::{plan_read::DistributedPlan, plan_read::FragmentId};",
        "let _ = crate::sql::plan_read::DistributedPlan;",
        "// crate::sql::planner::distributed::PlanFragment\nlet text = \"crate::sql::planner\";",
        "#[cfg(test)] mod tests { use crate::sql::planner::distributed::PlanFragment; }",
    ] {
        assert!(
            !contains_non_facade_sql_access(source),
            "{source} should be accepted"
        );
    }
}

#[test]
fn backend_and_exec_classifiers_reject_grouped_raw_and_inline_sql_paths() {
    for source in [
        "use novarocks::sql::plan_read::DistributedPlan;",
        "use novarocks::{r#sql::plan_read::DistributedPlan};",
        "let _ = novarocks::sql::plan_read::DistributedPlan;",
    ] {
        assert!(
            contains_backend_sql_dependency(source),
            "{source} should be rejected"
        );
    }
    assert!(contains_any_sql_access(
        "let _ = crate::r#sql::plan_read::DistributedPlan;"
    ));
}

#[test]
fn cfg_test_modules_are_excluded_from_production_tokens() {
    let tokens = production_tokens(
        "#[cfg(test)] mod tests { use crate::sql::planner::distributed::PlanFragment; }\n\
         use crate::sql::plan_read::DistributedPlan;",
    );
    assert!(!path_child_is(&tokens, "sql", "planner"));
    assert!(path_child_is(&tokens, "sql", "plan_read"));
}

#[test]
fn sql_planner_use_path_forms_are_rejected() {
    for source in [
        "use crate::sql::planner::distributed::PlanFragment;",
        "use crate::sql::{planner::distributed::PlanFragment, analysis::TypedExpr};",
        "use super::super::sql::planner::distributed::PlanFragment;",
    ] {
        assert!(
            contains_sql_planner_use_path(source),
            "{source} should be rejected"
        );
    }
}

#[test]
fn raw_identifier_planner_use_paths_are_rejected() {
    for source in [
        "use crate::sql::r#planner::distributed::PlanFragment;",
        "use crate::sql::{r#planner::distributed::PlanFragment};",
    ] {
        assert!(
            contains_sql_planner_use_path(source),
            "{source} should be rejected"
        );
    }
}

#[test]
fn non_planner_use_path_is_accepted() {
    assert!(!contains_sql_planner_use_path(
        "use crate::runtime_filter::model::contract::{BindingId, ChannelId};"
    ));
}

#[test]
fn runtime_filter_model_does_not_depend_on_sql_planner() {
    let model_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/runtime_filter/model");
    assert!(
        source_contains(rust_sources(&model_root), contains_sql_planner_use_path).is_none(),
        "runtime-filter model must not depend on the SQL planner"
    );
}

#[test]
fn native_encoder_production_graph_reads_sql_only_through_plan_read() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/protocol/native/encode/mod.rs");
    let offender = source_contains(
        production_module_graph(&root),
        contains_non_facade_sql_access,
    );
    assert!(
        offender.is_none(),
        "native encoder must read SQL only through crate::sql::plan_read: {}",
        offender.unwrap().display()
    );
}

#[test]
fn backend_production_source_does_not_depend_on_sql() {
    let backend_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../backend/src");
    let offender = source_contains(rust_sources(&backend_root), contains_backend_sql_dependency);
    assert!(
        offender.is_none(),
        "backend production source must not depend on novarocks::sql: {}",
        offender.unwrap().display()
    );
}

#[test]
fn execution_production_source_does_not_depend_on_sql() {
    let exec_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/exec");
    let offender = source_contains(rust_sources(&exec_root), contains_any_sql_access);
    assert!(
        offender.is_none(),
        "execution production source must not depend on crate::sql: {}",
        offender.unwrap().display()
    );
}
