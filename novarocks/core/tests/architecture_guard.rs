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

use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Eq, PartialEq)]
enum UseToken {
    Identifier(String),
    PathSeparator,
    OpenBrace,
    CloseBrace,
    Comma,
    Semicolon,
}

impl UseToken {
    fn is_identifier(&self, expected: &str) -> bool {
        matches!(self, Self::Identifier(identifier) if identifier == expected)
    }
}

fn tokenize_use_paths(source: &str) -> Vec<UseToken> {
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
            b'"' | b'\'' => {
                let quote = bytes[index];
                index += 1;
                while index < bytes.len() {
                    if bytes[index] == b'\\' {
                        index += 2;
                    } else if bytes[index] == quote {
                        index += 1;
                        break;
                    } else {
                        index += 1;
                    }
                }
            }
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
                tokens.push(UseToken::Identifier(
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
                tokens.push(UseToken::Identifier(
                    String::from_utf8_lossy(&bytes[start..index]).into_owned(),
                ));
            }
            b':' if bytes.get(index + 1) == Some(&b':') => {
                tokens.push(UseToken::PathSeparator);
                index += 2;
            }
            b'{' => {
                tokens.push(UseToken::OpenBrace);
                index += 1;
            }
            b'}' => {
                tokens.push(UseToken::CloseBrace);
                index += 1;
            }
            b',' => {
                tokens.push(UseToken::Comma);
                index += 1;
            }
            b';' => {
                tokens.push(UseToken::Semicolon);
                index += 1;
            }
            _ => index += 1,
        }
    }
    tokens
}

fn grouped_use_contains_planner(tokens: &[UseToken], open_brace: usize) -> bool {
    let mut depth = 0;
    for token in &tokens[open_brace..] {
        match token {
            UseToken::OpenBrace => depth += 1,
            UseToken::CloseBrace => {
                depth -= 1;
                if depth == 0 {
                    return false;
                }
            }
            _ if token.is_identifier("planner") => return true,
            _ => {}
        }
    }
    false
}

fn use_path_contains_sql_planner(tokens: &[UseToken]) -> bool {
    for (index, token) in tokens.iter().enumerate() {
        if !token.is_identifier("sql") || tokens.get(index + 1) != Some(&UseToken::PathSeparator) {
            continue;
        }
        match tokens.get(index + 2) {
            Some(token) if token.is_identifier("planner") => return true,
            Some(UseToken::OpenBrace) if grouped_use_contains_planner(tokens, index + 2) => {
                return true;
            }
            _ => {}
        }
    }
    false
}

fn contains_sql_planner_use_path(source: &str) -> bool {
    let tokens = tokenize_use_paths(source);
    let mut use_start = None;
    for (index, token) in tokens.iter().enumerate() {
        if token.is_identifier("use") {
            use_start = Some(index + 1);
        } else if *token == UseToken::Semicolon {
            if let Some(start) = use_start.take()
                && use_path_contains_sql_planner(&tokens[start..index])
            {
                return true;
            }
        }
    }
    false
}

fn rust_sources(root: &Path) -> Vec<PathBuf> {
    let mut sources = Vec::new();
    for entry in fs::read_dir(root).expect("model directory should be readable") {
        let path = entry
            .expect("model directory entry should be readable")
            .path();
        if path.is_dir() {
            sources.extend(rust_sources(&path));
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            sources.push(path);
        }
    }
    sources
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
    for source_path in rust_sources(&model_root) {
        let source = fs::read_to_string(&source_path).expect("model source should be readable");
        assert!(
            !contains_sql_planner_use_path(&source),
            "{} must not depend on the SQL planner",
            source_path.display()
        );
    }
}
