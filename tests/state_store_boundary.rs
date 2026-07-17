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

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::process::Command;

use sha2::{Digest, Sha256};

fn manifest_dir() -> &'static str {
    env!("CARGO_MANIFEST_DIR")
}

fn src_dir() -> PathBuf {
    Path::new(manifest_dir()).join("src")
}

fn rs_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(rs_files(&path));
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

#[derive(Debug)]
struct RustModuleItem {
    attributes: Vec<String>,
    inline_modules: Vec<RustInlineModuleContext>,
    is_external: bool,
    name: String,
}

#[derive(Clone, Debug)]
struct RustInlineModuleContext {
    attributes: Vec<String>,
    name: String,
}

#[derive(Clone, Debug)]
struct RustSourceToken {
    text: String,
    start: usize,
    end: usize,
}

fn rust_source_tokens(text: &str) -> Vec<RustSourceToken> {
    let sanitized = rust_lexically_sanitized(text);
    let bytes = sanitized.as_bytes();
    let mut tokens = Vec::new();
    let mut index = 0usize;
    while index < bytes.len() {
        if bytes[index].is_ascii_whitespace() {
            index += 1;
            continue;
        }
        let start = index;
        let raw_identifier = bytes[index] == b'r'
            && bytes.get(index + 1) == Some(&b'#')
            && bytes
                .get(index + 2)
                .is_some_and(|byte| byte.is_ascii_alphabetic() || *byte == b'_');
        if raw_identifier {
            index += 3;
            while bytes
                .get(index)
                .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
            {
                index += 1;
            }
        } else if bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_' {
            index += 1;
            while bytes
                .get(index)
                .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
            {
                index += 1;
            }
        } else if bytes[index] == b':' && bytes.get(index + 1) == Some(&b':') {
            index += 2;
        } else {
            index += 1;
        }
        tokens.push(RustSourceToken {
            text: if raw_identifier {
                sanitized[start + 2..index].to_string()
            } else {
                sanitized[start..index].to_string()
            },
            start,
            end: index,
        });
    }
    tokens
}

fn rust_matching_token(
    tokens: &[RustSourceToken],
    open: usize,
    left: &str,
    right: &str,
) -> Option<usize> {
    (tokens.get(open)?.text == left).then_some(())?;
    let mut depth = 0usize;
    for (index, token) in tokens.iter().enumerate().skip(open) {
        if token.text == left {
            depth += 1;
        } else if token.text == right {
            depth = depth.checked_sub(1)?;
            if depth == 0 {
                return Some(index);
            }
        }
    }
    None
}

fn rust_module_items_in_range(
    text: &str,
    tokens: &[RustSourceToken],
    range: std::ops::Range<usize>,
    inline_modules: &[RustInlineModuleContext],
    items: &mut Vec<RustModuleItem>,
) {
    let mut start = range.start;
    while start < range.end {
        let item_start = start;
        let mut cursor = item_start;
        let mut attributes = Vec::new();
        while tokens.get(cursor).is_some_and(|token| token.text == "#")
            && tokens
                .get(cursor + 1)
                .is_some_and(|token| token.text == "[")
        {
            let Some(close) = rust_matching_token(&tokens, cursor + 1, "[", "]") else {
                break;
            };
            attributes.push(text[tokens[cursor].start..tokens[close].end].to_string());
            cursor = close + 1;
        }

        if tokens.get(cursor).is_some_and(|token| token.text == "pub") {
            cursor += 1;
            if tokens.get(cursor).is_some_and(|token| token.text == "(") {
                let Some(close) = rust_matching_token(&tokens, cursor, "(", ")") else {
                    start += 1;
                    continue;
                };
                cursor = close + 1;
            }
        }
        let is_external = tokens.get(cursor).is_some_and(|token| token.text == "mod")
            && tokens
                .get(cursor + 1)
                .is_some_and(|token| token.text.chars().all(is_ident_char))
            && tokens
                .get(cursor + 2)
                .is_some_and(|token| token.text == ";");
        if is_external {
            items.push(RustModuleItem {
                attributes,
                inline_modules: inline_modules.to_vec(),
                is_external: true,
                name: tokens[cursor + 1].text.clone(),
            });
            start = cursor + 3;
        } else if tokens.get(cursor).is_some_and(|token| token.text == "mod")
            && tokens
                .get(cursor + 1)
                .is_some_and(|token| token.text.chars().all(is_ident_char))
            && tokens
                .get(cursor + 2)
                .is_some_and(|token| token.text == "{")
        {
            let open = cursor + 2;
            let Some(close) = rust_matching_token(tokens, open, "{", "}") else {
                start += 1;
                continue;
            };
            items.push(RustModuleItem {
                attributes: attributes.clone(),
                inline_modules: inline_modules.to_vec(),
                is_external: false,
                name: tokens[cursor + 1].text.clone(),
            });
            if cfg_attributes_test_requirement(attributes.iter().map(String::as_str))
                == CfgTestRequirement::RequiresTest
            {
                start = close + 1;
                continue;
            }
            let mut nested = inline_modules.to_vec();
            nested.push(RustInlineModuleContext {
                attributes,
                name: tokens[cursor + 1].text.clone(),
            });
            rust_module_items_in_range(text, tokens, open + 1..close, &nested, items);
            start = close + 1;
        } else if tokens.get(cursor).is_some_and(|token| token.text == "{") {
            start =
                rust_matching_token(tokens, cursor, "{", "}").map_or(cursor + 1, |close| close + 1);
        } else {
            start += 1;
        }
    }
}

fn rust_module_items(text: &str) -> Vec<RustModuleItem> {
    let tokens = rust_source_tokens(text);
    let mut items = Vec::new();
    rust_module_items_in_range(text, &tokens, 0..tokens.len(), &[], &mut items);
    items
}

#[derive(Clone, Debug)]
enum CfgPredicate {
    Test,
    Atom(String),
    All(Vec<CfgPredicate>),
    Any(Vec<CfgPredicate>),
    Not(Box<CfgPredicate>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CfgSatisfiability {
    Satisfiable,
    Unsatisfiable,
    Unproven,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CfgTestRequirement {
    RequiresTest,
    ProductionPossible,
    Unproven,
}

const CFG_SAT_WORK_BUDGET: usize = 256;

impl CfgPredicate {
    fn collect_atoms(&self, atoms: &mut BTreeSet<String>) {
        match self {
            Self::Test => {}
            Self::Atom(atom) => {
                atoms.insert(atom.clone());
            }
            Self::All(children) | Self::Any(children) => {
                for child in children {
                    child.collect_atoms(atoms);
                }
            }
            Self::Not(child) => child.collect_atoms(atoms),
        }
    }

    fn evaluate_partial(&self, assignments: &BTreeMap<String, bool>) -> Option<bool> {
        match self {
            Self::Test => Some(false),
            Self::Atom(atom) => assignments.get(atom).copied(),
            Self::All(children) => {
                if children
                    .iter()
                    .any(|child| child.evaluate_partial(assignments) == Some(false))
                {
                    Some(false)
                } else if children
                    .iter()
                    .all(|child| child.evaluate_partial(assignments) == Some(true))
                {
                    Some(true)
                } else {
                    None
                }
            }
            Self::Any(children) => {
                if children
                    .iter()
                    .any(|child| child.evaluate_partial(assignments) == Some(true))
                {
                    Some(true)
                } else if children
                    .iter()
                    .all(|child| child.evaluate_partial(assignments) == Some(false))
                {
                    Some(false)
                } else {
                    None
                }
            }
            Self::Not(child) => child.evaluate_partial(assignments).map(|value| !value),
        }
    }

    fn can_be_true_bounded(
        &self,
        atoms: &[String],
        assignments: &mut BTreeMap<String, bool>,
        memo: &mut BTreeMap<Vec<Option<bool>>, CfgSatisfiability>,
        work_budget: &mut usize,
    ) -> CfgSatisfiability {
        let key = atoms
            .iter()
            .map(|atom| assignments.get(atom).copied())
            .collect::<Vec<_>>();
        if let Some(result) = memo.get(&key) {
            return *result;
        }
        if *work_budget == 0 {
            return CfgSatisfiability::Unproven;
        }
        *work_budget -= 1;
        if let Some(value) = self.evaluate_partial(assignments) {
            let result = if value {
                CfgSatisfiability::Satisfiable
            } else {
                CfgSatisfiability::Unsatisfiable
            };
            memo.insert(key, result);
            return result;
        }
        let Some(atom) = atoms
            .iter()
            .find(|atom| !assignments.contains_key(atom.as_str()))
        else {
            memo.insert(key, CfgSatisfiability::Unsatisfiable);
            return CfgSatisfiability::Unsatisfiable;
        };
        let atom = atom.clone();
        let mut unproven = false;
        for value in [false, true] {
            assignments.insert(atom.clone(), value);
            match self.can_be_true_bounded(atoms, assignments, memo, work_budget) {
                CfgSatisfiability::Satisfiable => {
                    assignments.remove(&atom);
                    memo.insert(key, CfgSatisfiability::Satisfiable);
                    return CfgSatisfiability::Satisfiable;
                }
                CfgSatisfiability::Unproven => unproven = true,
                CfgSatisfiability::Unsatisfiable => {}
            }
        }
        assignments.remove(&atom);
        let result = if unproven {
            CfgSatisfiability::Unproven
        } else {
            CfgSatisfiability::Unsatisfiable
        };
        if result != CfgSatisfiability::Unproven {
            memo.insert(key, result);
        }
        result
    }
}

fn cfg_parse_predicate(tokens: &[String], start: usize) -> Option<(CfgPredicate, usize)> {
    let owner = tokens.get(start)?;
    if owner == "test"
        && tokens
            .get(start + 1)
            .is_none_or(|token| matches!(token.as_str(), "," | ")"))
    {
        return Some((CfgPredicate::Test, start + 1));
    }
    if !matches!(owner.as_str(), "all" | "any" | "not")
        || tokens.get(start + 1).is_none_or(|token| token != "(")
    {
        let mut cursor = start + 1;
        while tokens
            .get(cursor)
            .is_some_and(|token| !matches!(token.as_str(), "," | ")"))
        {
            cursor += 1;
        }
        return Some((
            CfgPredicate::Atom(tokens[start..cursor].join("\u{1f}")),
            cursor,
        ));
    }

    let mut children = Vec::new();
    let mut cursor = start + 2;
    while tokens.get(cursor).is_some_and(|token| token != ")") {
        let (predicate, next) = cfg_parse_predicate(tokens, cursor)?;
        children.push(predicate);
        cursor = next;
        if tokens.get(cursor).is_some_and(|token| token == ",") {
            cursor += 1;
        }
    }
    let end = (tokens.get(cursor)? == ")").then_some(cursor + 1)?;
    let predicate = match owner.as_str() {
        "all" => CfgPredicate::All(children),
        "any" => CfgPredicate::Any(children),
        "not" if children.len() == 1 => CfgPredicate::Not(Box::new(children.remove(0))),
        "not" => return None,
        _ => unreachable!(),
    };
    Some((predicate, end))
}

fn cfg_predicate_test_requirement_with_budget(
    tokens: &[String],
    start: usize,
    work_budget: usize,
) -> Option<(CfgTestRequirement, usize)> {
    let (predicate, end) = cfg_parse_predicate(tokens, start)?;
    Some((
        cfg_test_requirement_for_predicate(&predicate, work_budget),
        end,
    ))
}

fn cfg_test_requirement_for_predicate(
    predicate: &CfgPredicate,
    mut work_budget: usize,
) -> CfgTestRequirement {
    let mut atoms = BTreeSet::new();
    predicate.collect_atoms(&mut atoms);
    let atoms = atoms.into_iter().collect::<Vec<_>>();
    let satisfiability = predicate.can_be_true_bounded(
        &atoms,
        &mut BTreeMap::new(),
        &mut BTreeMap::new(),
        &mut work_budget,
    );
    match satisfiability {
        CfgSatisfiability::Satisfiable => CfgTestRequirement::ProductionPossible,
        CfgSatisfiability::Unsatisfiable => CfgTestRequirement::RequiresTest,
        CfgSatisfiability::Unproven => CfgTestRequirement::Unproven,
    }
}

fn cfg_predicate_test_requirement(
    tokens: &[String],
    start: usize,
) -> Option<(CfgTestRequirement, usize)> {
    cfg_predicate_test_requirement_with_budget(tokens, start, CFG_SAT_WORK_BUDGET)
}

fn cfg_predicate_requires_test(tokens: &[String], start: usize) -> Option<(bool, usize)> {
    cfg_predicate_test_requirement(tokens, start)
        .map(|(requirement, end)| (requirement == CfgTestRequirement::RequiresTest, end))
}

fn cfg_meta_end(tokens: &[String], start: usize, limit: usize) -> Option<usize> {
    let mut paren_depth = 0usize;
    let mut bracket_depth = 0usize;
    let mut brace_depth = 0usize;
    for index in start..limit {
        match tokens[index].as_str() {
            "(" => paren_depth += 1,
            ")" => paren_depth = paren_depth.checked_sub(1)?,
            "[" => bracket_depth += 1,
            "]" => bracket_depth = bracket_depth.checked_sub(1)?,
            "{" => brace_depth += 1,
            "}" => brace_depth = brace_depth.checked_sub(1)?,
            "," if paren_depth == 0 && bracket_depth == 0 && brace_depth == 0 => {
                return Some(index);
            }
            _ => {}
        }
    }
    Some(limit)
}

fn runtime_filter_matching_delimiter(
    tokens: &[String],
    open: usize,
    left: &str,
    right: &str,
) -> Option<usize> {
    (tokens.get(open)? == left).then_some(())?;
    let mut depth = 0isize;
    for (index, token) in tokens.iter().enumerate().skip(open) {
        if token == left {
            depth += 1;
        } else if token == right {
            depth -= 1;
            if depth == 0 {
                return Some(index);
            }
        }
    }
    None
}

fn cfg_generated_meta_presence_predicate(
    tokens: &[String],
    start: usize,
    limit: usize,
) -> Option<(Option<CfgPredicate>, usize)> {
    match tokens.get(start)?.as_str() {
        "cfg" => {
            if tokens.get(start + 1).is_none_or(|token| token != "(") {
                return None;
            }
            let close = runtime_filter_matching_delimiter(tokens, start + 1, "(", ")")?;
            if close >= limit {
                return None;
            }
            let (predicate, end) = cfg_parse_predicate(tokens, start + 2)?;
            (end == close).then_some((Some(predicate), close + 1))
        }
        "cfg_attr" => {
            if tokens.get(start + 1).is_none_or(|token| token != "(") {
                return None;
            }
            let close = runtime_filter_matching_delimiter(tokens, start + 1, "(", ")")?;
            if close >= limit {
                return None;
            }
            let (condition, mut cursor) = cfg_parse_predicate(tokens, start + 2)?;
            if tokens.get(cursor).is_none_or(|token| token != ",") {
                return None;
            }
            cursor += 1;
            let mut generated = Vec::new();
            while cursor < close {
                let (predicate, end) = cfg_generated_meta_presence_predicate(tokens, cursor, close)
                    .or_else(|| cfg_meta_end(tokens, cursor, close).map(|end| (None, end)))?;
                if let Some(predicate) = predicate {
                    generated.push(predicate);
                }
                cursor = end;
                if tokens.get(cursor).is_some_and(|token| token == ",") {
                    cursor += 1;
                }
            }
            let presence = CfgPredicate::Any(vec![
                CfgPredicate::Not(Box::new(condition)),
                CfgPredicate::All(generated),
            ]);
            Some((Some(presence), close + 1))
        }
        _ => cfg_meta_end(tokens, start, limit).map(|end| (None, end)),
    }
}

fn cfg_attribute_presence_predicate(attribute: &str) -> Result<Option<CfgPredicate>, ()> {
    let tokens = rust_use_tokens(attribute);
    let open = tokens.iter().position(|token| token == "[").ok_or(())?;
    let head = open + 1;
    match tokens.get(head).map(String::as_str) {
        Some("cfg") => {
            if tokens.get(head + 1).is_none_or(|token| token != "(") {
                return Err(());
            }
            let close = runtime_filter_matching_delimiter(&tokens, head + 1, "(", ")").ok_or(())?;
            let (predicate, end) = cfg_parse_predicate(&tokens, head + 2).ok_or(())?;
            (end == close).then_some(Some(predicate)).ok_or(())
        }
        Some("cfg_attr") => cfg_generated_meta_presence_predicate(&tokens, head, tokens.len())
            .and_then(|(predicate, _)| predicate)
            .map(Some)
            .ok_or(()),
        _ => Ok(None),
    }
}

fn cfg_attributes_test_requirement<'a>(
    attributes: impl IntoIterator<Item = &'a str>,
) -> CfgTestRequirement {
    let mut predicates = Vec::new();
    for attribute in attributes {
        match cfg_attribute_presence_predicate(attribute) {
            Ok(Some(predicate)) => predicates.push(predicate),
            Ok(None) => {}
            Err(()) => return CfgTestRequirement::Unproven,
        }
    }
    cfg_test_requirement_for_predicate(&CfgPredicate::All(predicates), CFG_SAT_WORK_BUDGET)
}

fn decode_rust_string_literal(literal: &str) -> Option<String> {
    let literal = literal.trim();
    if let Some(raw) = literal.strip_prefix('r') {
        let hashes = raw.chars().take_while(|ch| *ch == '#').count();
        let raw = raw.get(hashes..)?;
        let content = raw.strip_prefix('"')?;
        let closing = format!("\"{}", "#".repeat(hashes));
        return content.strip_suffix(&closing).map(str::to_string);
    }

    let content = literal.strip_prefix('"')?.strip_suffix('"')?;
    let chars = content.chars().collect::<Vec<_>>();
    let mut decoded = String::new();
    let mut index = 0usize;
    while index < chars.len() {
        if chars[index] != '\\' {
            decoded.push(chars[index]);
            index += 1;
            continue;
        }
        index += 1;
        match *chars.get(index)? {
            '\\' => decoded.push('\\'),
            '"' => decoded.push('"'),
            '\'' => decoded.push('\''),
            'n' => decoded.push('\n'),
            'r' => decoded.push('\r'),
            't' => decoded.push('\t'),
            '0' => decoded.push('\0'),
            'x' => {
                let hi = chars.get(index + 1)?.to_digit(16)?;
                let lo = chars.get(index + 2)?.to_digit(16)?;
                decoded.push(char::from_u32((hi << 4) | lo)?);
                index += 2;
            }
            'u' if chars.get(index + 1) == Some(&'{') => {
                let close = chars[index + 2..].iter().position(|ch| *ch == '}')? + index + 2;
                let value = chars[index + 2..close]
                    .iter()
                    .filter(|ch| **ch != '_')
                    .collect::<String>();
                decoded.push(char::from_u32(u32::from_str_radix(&value, 16).ok()?)?);
                index = close;
            }
            '\n' => {
                while chars.get(index + 1).is_some_and(|ch| ch.is_whitespace()) {
                    index += 1;
                }
            }
            _ => return None,
        }
        index += 1;
    }
    Some(decoded)
}

fn path_attribute_value(attribute: &str) -> Option<String> {
    let tokens = rust_use_tokens(&rust_lexically_sanitized(attribute));
    let body = tokens
        .iter()
        .position(|token| token == "[")
        .and_then(|open| tokens.get(open + 1..))?;
    if body.first().is_none_or(|token| token != "path")
        || body.get(1).is_none_or(|token| token != "=")
    {
        return None;
    }
    let equals = attribute.find('=')?;
    let value = attribute[equals + 1..].trim();
    let value = value.strip_suffix(']')?.trim();
    decode_rust_string_literal(value)
}

fn cfg_attr_generated_path_values(attribute: &str) -> Vec<String> {
    fn argument_ranges(
        tokens: &[RustSourceToken],
        open: usize,
        close: usize,
    ) -> Vec<std::ops::Range<usize>> {
        let mut arguments = Vec::new();
        let mut start = open + 1;
        let mut paren_depth = 0usize;
        let mut bracket_depth = 0usize;
        let mut brace_depth = 0usize;
        for index in open + 1..close {
            match tokens[index].text.as_str() {
                "(" => paren_depth += 1,
                ")" => paren_depth = paren_depth.saturating_sub(1),
                "[" => bracket_depth += 1,
                "]" => bracket_depth = bracket_depth.saturating_sub(1),
                "{" => brace_depth += 1,
                "}" => brace_depth = brace_depth.saturating_sub(1),
                "," if paren_depth == 0 && bracket_depth == 0 && brace_depth == 0 => {
                    arguments.push(start..index);
                    start = index + 1;
                }
                _ => {}
            }
        }
        arguments.push(start..close);
        arguments
    }

    fn collect(
        attribute: &str,
        tokens: &[RustSourceToken],
        token_texts: &[String],
        cfg_attr: usize,
        enclosing_requires_test: bool,
        paths: &mut BTreeSet<String>,
    ) {
        if tokens
            .get(cfg_attr)
            .is_none_or(|token| token.text != "cfg_attr")
            || tokens
                .get(cfg_attr + 1)
                .is_none_or(|token| token.text != "(")
        {
            return;
        }
        let Some(close) = rust_matching_token(tokens, cfg_attr + 1, "(", ")") else {
            return;
        };
        let predicate_requires_test = cfg_predicate_requires_test(token_texts, cfg_attr + 2)
            .is_some_and(|(requires_test, _)| requires_test);
        let branch_requires_test = enclosing_requires_test || predicate_requires_test;
        if branch_requires_test {
            return;
        }
        for range in argument_ranges(tokens, cfg_attr + 1, close)
            .into_iter()
            .skip(1)
        {
            let Some(head) = tokens.get(range.start) else {
                continue;
            };
            if head.text == "path"
                && tokens
                    .get(range.start + 1)
                    .is_some_and(|token| token.text == "=")
            {
                let value_start = tokens[range.start + 1].end;
                let value_end = tokens
                    .get(range.end)
                    .map_or(tokens[close].start, |token| token.start);
                if let Some(path) =
                    decode_rust_string_literal(attribute[value_start..value_end].trim())
                {
                    paths.insert(path);
                }
            } else if head.text == "cfg_attr" {
                collect(
                    attribute,
                    tokens,
                    token_texts,
                    range.start,
                    branch_requires_test,
                    paths,
                );
            }
        }
    }

    let tokens = rust_source_tokens(attribute);
    let token_texts = tokens
        .iter()
        .map(|token| token.text.clone())
        .collect::<Vec<_>>();
    let Some(open) = tokens.iter().position(|token| token.text == "[") else {
        return Vec::new();
    };
    let mut paths = BTreeSet::new();
    collect(
        attribute,
        &tokens,
        &token_texts,
        open + 1,
        false,
        &mut paths,
    );
    paths.into_iter().collect()
}

fn production_rs_files_from_entries(root: &Path, entries: &[PathBuf]) -> Vec<PathBuf> {
    fn default_module_dir(source: &Path, root: &Path) -> PathBuf {
        let parent_dir = source.parent().unwrap_or(root);
        if source
            .file_name()
            .is_some_and(|name| matches!(name.to_str(), Some("mod.rs" | "lib.rs" | "main.rs")))
        {
            parent_dir.to_path_buf()
        } else {
            source
                .file_stem()
                .map_or_else(|| parent_dir.to_path_buf(), |stem| parent_dir.join(stem))
        }
    }

    fn direct_paths(attributes: &[String]) -> Vec<String> {
        attributes
            .iter()
            .filter_map(|attribute| path_attribute_value(attribute))
            .collect()
    }

    fn conditional_paths(attributes: &[String]) -> Vec<String> {
        attributes
            .iter()
            .flat_map(|attribute| cfg_attr_generated_path_values(attribute))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    fn inline_module_dirs(
        source: &Path,
        root: &Path,
        inline_modules: &[RustInlineModuleContext],
    ) -> Vec<PathBuf> {
        let source_parent = source.parent().unwrap_or(root);
        let mut directories = BTreeSet::new();
        for (index, inline) in inline_modules.iter().enumerate() {
            let direct = direct_paths(&inline.attributes);
            let conditional = conditional_paths(&inline.attributes);
            let containing = if index == 0 {
                BTreeSet::from([source_parent.to_path_buf()])
            } else {
                directories
            };
            let mut next = BTreeSet::new();
            for directory in containing {
                if direct.is_empty() {
                    let default_parent = if index == 0 {
                        default_module_dir(source, root)
                    } else {
                        directory.clone()
                    };
                    next.insert(default_parent.join(&inline.name));
                } else {
                    next.extend(direct.iter().map(|path| directory.join(path)));
                }
                next.extend(conditional.iter().map(|path| directory.join(path)));
            }
            directories = next;
        }
        directories.into_iter().collect()
    }

    let files = rs_files(root);
    let candidates = files
        .iter()
        .filter_map(|path| {
            fs::canonicalize(path)
                .ok()
                .map(|canonical| (canonical, path.clone()))
        })
        .collect::<BTreeMap<_, _>>();
    let mut reachable = BTreeSet::new();
    let mut pending = Vec::new();
    for entry in entries {
        let Some(entry) = fs::canonicalize(entry).ok() else {
            continue;
        };
        if candidates.contains_key(&entry) && reachable.insert(entry.clone()) {
            pending.push(entry);
        }
    }
    while let Some(parent) = pending.pop() {
        let text = fs::read_to_string(&parent).unwrap_or_default();
        for item in rust_module_items(&text)
            .into_iter()
            .filter(|item| item.is_external)
        {
            if cfg_attributes_test_requirement(item.attributes.iter().map(String::as_str))
                == CfgTestRequirement::RequiresTest
            {
                continue;
            }
            let parent_dir = parent.parent().unwrap_or(root);
            let module_dirs = if item.inline_modules.is_empty() {
                vec![default_module_dir(&parent, root)]
            } else {
                inline_module_dirs(&parent, root, &item.inline_modules)
            };
            let explicit_bases = if item.inline_modules.is_empty() {
                vec![parent_dir.to_path_buf()]
            } else {
                module_dirs.clone()
            };
            let direct = direct_paths(&item.attributes);
            let conditional = conditional_paths(&item.attributes);
            let mut targets = BTreeSet::new();
            if direct.is_empty() {
                for module_dir in &module_dirs {
                    targets.insert(module_dir.join(format!("{}.rs", item.name)));
                    targets.insert(module_dir.join(&item.name).join("mod.rs"));
                }
            } else {
                for base in &explicit_bases {
                    targets.extend(direct.iter().map(|path| base.join(path)));
                }
            }
            for base in &explicit_bases {
                targets.extend(conditional.iter().map(|path| base.join(path)));
            }
            for target in targets {
                let Some(target) = fs::canonicalize(target).ok() else {
                    continue;
                };
                if candidates.contains_key(&target) && reachable.insert(target.clone()) {
                    pending.push(target);
                }
            }
        }
    }

    reachable
        .into_iter()
        .filter_map(|canonical| candidates.get(&canonical).cloned())
        .collect()
}

fn rel(path: &Path) -> String {
    path.strip_prefix(manifest_dir())
        .unwrap_or(path)
        .display()
        .to_string()
}

fn is_ident_char(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}

fn rust_char_literal_end(text: &str, start: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let first = *bytes.get(start + 1)?;
    let mut cursor = start + 1;

    if first == b'\\' {
        cursor += 1;
        match *bytes.get(cursor)? {
            b'u' if bytes.get(cursor + 1) == Some(&b'{') => {
                cursor += 2;
                while bytes.get(cursor) != Some(&b'}') {
                    cursor += 1;
                }
                cursor += 1;
            }
            b'x' => cursor += 3,
            _ => cursor += 1,
        }
    } else {
        let ch = text.get(cursor..)?.chars().next()?;
        cursor += ch.len_utf8();
    }

    (bytes.get(cursor) == Some(&b'\'')).then_some(cursor)
}

fn rust_raw_string_open(bytes: &[u8], start: usize) -> Option<(usize, usize)> {
    if bytes.get(start) != Some(&b'r') {
        return None;
    }
    let mut cursor = start + 1;
    while bytes.get(cursor) == Some(&b'#') {
        cursor += 1;
    }
    (bytes.get(cursor) == Some(&b'"')).then_some((cursor - start - 1, cursor - start + 1))
}

#[derive(Clone, Copy)]
enum RustLexicalState {
    Code,
    LineComment,
    BlockComment(usize),
    String { escaped: bool },
    RawString { hashes: usize },
}

fn rust_lexically_sanitized(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut state = RustLexicalState::Code;
    let mut index = 0usize;

    while index < bytes.len() {
        match state {
            RustLexicalState::Code => {
                if bytes[index] == b'/' && bytes.get(index + 1) == Some(&b'/') {
                    output.extend_from_slice(b"  ");
                    index += 2;
                    state = RustLexicalState::LineComment;
                } else if bytes[index] == b'/' && bytes.get(index + 1) == Some(&b'*') {
                    output.extend_from_slice(b"  ");
                    index += 2;
                    state = RustLexicalState::BlockComment(1);
                } else if let Some((hashes, opening_len)) = rust_raw_string_open(bytes, index) {
                    output.extend(std::iter::repeat_n(b' ', opening_len));
                    index += opening_len;
                    state = RustLexicalState::RawString { hashes };
                } else if bytes[index] == b'"' {
                    output.push(b' ');
                    index += 1;
                    state = RustLexicalState::String { escaped: false };
                } else if bytes[index] == b'\''
                    && let Some(end) = rust_char_literal_end(text, index)
                {
                    output.extend(std::iter::repeat_n(b' ', end - index + 1));
                    index = end + 1;
                } else {
                    output.push(bytes[index]);
                    index += 1;
                }
            }
            RustLexicalState::LineComment => {
                if bytes[index] == b'\n' {
                    output.push(b'\n');
                    state = RustLexicalState::Code;
                } else {
                    output.push(b' ');
                }
                index += 1;
            }
            RustLexicalState::BlockComment(depth) => {
                if bytes[index] == b'/' && bytes.get(index + 1) == Some(&b'*') {
                    output.extend_from_slice(b"  ");
                    index += 2;
                    state = RustLexicalState::BlockComment(depth + 1);
                } else if bytes[index] == b'*' && bytes.get(index + 1) == Some(&b'/') {
                    output.extend_from_slice(b"  ");
                    index += 2;
                    state = if depth == 1 {
                        RustLexicalState::Code
                    } else {
                        RustLexicalState::BlockComment(depth - 1)
                    };
                } else {
                    output.push(if bytes[index] == b'\n' { b'\n' } else { b' ' });
                    index += 1;
                }
            }
            RustLexicalState::String { escaped } => {
                let byte = bytes[index];
                output.push(if byte == b'\n' { b'\n' } else { b' ' });
                index += 1;
                state = if escaped {
                    RustLexicalState::String { escaped: false }
                } else if byte == b'\\' {
                    RustLexicalState::String { escaped: true }
                } else if byte == b'"' {
                    RustLexicalState::Code
                } else {
                    RustLexicalState::String { escaped: false }
                };
            }
            RustLexicalState::RawString { hashes } => {
                let closes = bytes[index] == b'"'
                    && bytes
                        .get(index + 1..index + 1 + hashes)
                        .is_some_and(|suffix| suffix.iter().all(|byte| *byte == b'#'));
                if closes {
                    output.extend(std::iter::repeat_n(b' ', hashes + 1));
                    index += hashes + 1;
                    state = RustLexicalState::Code;
                } else {
                    output.push(if bytes[index] == b'\n' { b'\n' } else { b' ' });
                    index += 1;
                }
            }
        }
    }

    String::from_utf8(output).expect("lexical sanitizer must preserve valid UTF-8 outside noise")
}

fn rust_item_end_token(tokens: &[RustSourceToken], start: usize) -> Option<usize> {
    let mut head = start;
    if tokens.get(head).is_some_and(|token| token.text == "pub") {
        head += 1;
        if tokens.get(head).is_some_and(|token| token.text == "(") {
            head = rust_matching_token(tokens, head, "(", ")")? + 1;
        }
    }
    let semicolon_terminated = tokens.get(head).is_some_and(|token| {
        matches!(token.text.as_str(), "use" | "type" | "static")
            || (token.text == "const" && tokens.get(head + 1).is_none_or(|next| next.text != "fn"))
    });
    let mut paren_depth = 0usize;
    let mut bracket_depth = 0usize;
    let mut brace_depth = 0usize;
    let mut angle_depth = 0usize;
    let mut cursor = start;
    while cursor < tokens.len() {
        match tokens[cursor].text.as_str() {
            "(" => paren_depth += 1,
            ")" => paren_depth = paren_depth.checked_sub(1)?,
            "[" => bracket_depth += 1,
            "]" => bracket_depth = bracket_depth.checked_sub(1)?,
            "<" if paren_depth == 0 && bracket_depth == 0 && brace_depth == 0 => angle_depth += 1,
            ">" if paren_depth == 0 && bracket_depth == 0 && brace_depth == 0 => {
                angle_depth = angle_depth.saturating_sub(1)
            }
            "{" if paren_depth == 0
                && bracket_depth == 0
                && angle_depth == 0
                && !semicolon_terminated =>
            {
                let close = rust_matching_token(tokens, cursor, "{", "}")?;
                return Some(
                    if tokens.get(close + 1).is_some_and(|token| token.text == ";") {
                        close + 1
                    } else {
                        close
                    },
                );
            }
            "{" => brace_depth += 1,
            "}" => brace_depth = brace_depth.checked_sub(1)?,
            ";" if paren_depth == 0 && bracket_depth == 0 && brace_depth == 0 => {
                return Some(cursor);
            }
            "," if paren_depth == 0
                && bracket_depth == 0
                && brace_depth == 0
                && angle_depth == 0 =>
            {
                return Some(cursor);
            }
            _ => {}
        }
        cursor += 1;
    }
    None
}

fn rust_test_only_item_spans(text: &str) -> Vec<std::ops::Range<usize>> {
    let tokens = rust_source_tokens(text);
    let mut spans = Vec::new();
    let mut start = 0usize;
    while start < tokens.len() {
        if tokens.get(start).is_none_or(|token| token.text != "#")
            || tokens.get(start + 1).is_none_or(|token| token.text != "[")
        {
            start += 1;
            continue;
        }

        let item_start = start;
        let mut cursor = start;
        let mut attributes = Vec::new();
        while tokens.get(cursor).is_some_and(|token| token.text == "#")
            && tokens
                .get(cursor + 1)
                .is_some_and(|token| token.text == "[")
        {
            let Some(close) = rust_matching_token(&tokens, cursor + 1, "[", "]") else {
                break;
            };
            attributes.push(text[tokens[cursor].start..tokens[close].end].to_string());
            cursor = close + 1;
        }
        if cfg_attributes_test_requirement(attributes.iter().map(String::as_str))
            != CfgTestRequirement::RequiresTest
        {
            start += 1;
            continue;
        }

        let Some(item_end) = rust_item_end_token(&tokens, cursor) else {
            start += 1;
            continue;
        };
        spans.push(tokens[item_start].start..tokens[item_end].end);
        start = item_end + 1;
    }
    spans
}

fn rust_sanitized_production_text(text: &str) -> String {
    let mut sanitized = rust_lexically_sanitized(text).into_bytes();
    for span in rust_test_only_item_spans(text) {
        for byte in &mut sanitized[span] {
            if !matches!(*byte, b'\n' | b'\r') {
                *byte = b' ';
            }
        }
    }
    String::from_utf8(sanitized).expect("production sanitizer must preserve UTF-8")
}

fn rust_use_tokens(text: &str) -> Vec<String> {
    let chars = text.chars().collect::<Vec<_>>();
    let mut tokens = Vec::new();
    let mut index = 0usize;

    while index < chars.len() {
        let ch = chars[index];
        if ch.is_whitespace() {
            index += 1;
            continue;
        }
        if ch == 'r'
            && chars.get(index + 1) == Some(&'#')
            && chars
                .get(index + 2)
                .is_some_and(|ch| ch.is_ascii_alphabetic() || *ch == '_')
        {
            let start = index + 2;
            index += 3;
            while index < chars.len() && is_ident_char(chars[index]) {
                index += 1;
            }
            tokens.push(chars[start..index].iter().collect());
            continue;
        }
        if is_ident_char(ch) {
            let start = index;
            index += 1;
            while index < chars.len() && is_ident_char(chars[index]) {
                index += 1;
            }
            tokens.push(chars[start..index].iter().collect());
            continue;
        }
        if ch == ':' && chars.get(index + 1) == Some(&':') {
            tokens.push("::".to_string());
            index += 2;
            continue;
        }
        tokens.push(ch.to_string());
        index += 1;
    }

    tokens
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RustUsePath {
    segments: Vec<String>,
    alias: Option<String>,
}

fn rust_expand_use_tree(tokens: &[String], prefix: &[String], paths: &mut Vec<RustUsePath>) {
    let mut path = prefix.to_vec();
    let mut index = 0usize;
    let mut imports_group_prefix = false;

    while index < tokens.len() {
        match tokens[index].as_str() {
            "::" | "," => index += 1,
            "{" => {
                let mut depth = 1usize;
                let mut close = index + 1;
                while close < tokens.len() && depth > 0 {
                    match tokens[close].as_str() {
                        "{" => depth += 1,
                        "}" => depth -= 1,
                        _ => {}
                    }
                    close += 1;
                }
                let inner_end = close.saturating_sub(1);
                rust_expand_use_list(&tokens[index + 1..inner_end], &path, paths);
                return;
            }
            "*" => {
                path.push("*".to_string());
                paths.push(RustUsePath {
                    segments: path,
                    alias: None,
                });
                return;
            }
            "as" => {
                if path.len() > prefix.len() || imports_group_prefix {
                    let alias = tokens
                        .get(index + 1)
                        .filter(|token| token.chars().all(is_ident_char))
                        .cloned();
                    paths.push(RustUsePath {
                        segments: path,
                        alias,
                    });
                }
                return;
            }
            "}" => return,
            token if token.chars().all(is_ident_char) => {
                if token == "self"
                    && !prefix.is_empty()
                    && path == prefix
                    && tokens.get(index + 1).is_none_or(|next| next == "as")
                {
                    imports_group_prefix = true;
                    index += 1;
                    continue;
                }
                path.push(token.to_string());
                index += 1;
            }
            _ => index += 1,
        }
    }

    if path.len() > prefix.len() || imports_group_prefix {
        paths.push(RustUsePath {
            segments: path,
            alias: None,
        });
    }
}

fn rust_expand_use_list(tokens: &[String], prefix: &[String], paths: &mut Vec<RustUsePath>) {
    let mut depth = 0usize;
    let mut start = 0usize;

    for index in 0..=tokens.len() {
        let at_separator = index == tokens.len() || (tokens[index] == "," && depth == 0);
        if at_separator {
            if start < index {
                rust_expand_use_tree(&tokens[start..index], prefix, paths);
            }
            start = index + 1;
            continue;
        }
        match tokens[index].as_str() {
            "{" => depth += 1,
            "}" => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RustRawUseStatement {
    path: RustUsePath,
    inline_modules: Vec<String>,
}

fn rust_raw_use_statements(sanitized: &str) -> Vec<RustRawUseStatement> {
    let tokens = rust_use_tokens(sanitized);
    let inline_module_openings = (0..tokens.len().saturating_sub(2))
        .filter_map(|index| {
            (tokens[index] == "mod"
                && tokens[index + 1].chars().all(is_ident_char)
                && tokens[index + 2] == "{")
                .then(|| (index + 2, tokens[index + 1].clone()))
        })
        .collect::<BTreeMap<_, _>>();
    let mut raw_imports = Vec::new();
    let mut inline_modules = Vec::<(usize, String)>::new();
    let mut brace_depth = 0usize;
    let mut index = 0usize;

    while index < tokens.len() {
        if tokens[index] == "{" {
            brace_depth += 1;
            if let Some(module) = inline_module_openings.get(&index) {
                inline_modules.push((brace_depth, module.clone()));
            }
            index += 1;
            continue;
        }
        if tokens[index] == "}" {
            if inline_modules
                .last()
                .is_some_and(|(depth, _)| *depth == brace_depth)
            {
                inline_modules.pop();
            }
            brace_depth = brace_depth.saturating_sub(1);
            index += 1;
            continue;
        }

        let mut cursor = index;

        if tokens[cursor] == "pub" {
            cursor += 1;
            if tokens.get(cursor).is_some_and(|token| token == "(") {
                let mut depth = 1usize;
                cursor += 1;
                while cursor < tokens.len() && depth > 0 {
                    match tokens[cursor].as_str() {
                        "(" => depth += 1,
                        ")" => depth -= 1,
                        _ => {}
                    }
                    cursor += 1;
                }
            }
            if tokens.get(cursor).is_none_or(|token| token != "use") {
                index += 1;
                continue;
            }
        } else if tokens[cursor] != "use" {
            index += 1;
            continue;
        }

        cursor += 1;
        let tree_start = cursor;
        while cursor < tokens.len() && tokens[cursor] != ";" {
            cursor += 1;
        }
        if cursor == tokens.len() {
            break;
        }

        let mut paths = Vec::new();
        rust_expand_use_list(&tokens[tree_start..cursor], &[], &mut paths);
        let scope = inline_modules
            .iter()
            .map(|(_, module)| module.clone())
            .collect::<Vec<_>>();
        raw_imports.extend(paths.into_iter().map(|path| RustRawUseStatement {
            path,
            inline_modules: scope.clone(),
        }));
        index = cursor + 1;
    }

    raw_imports
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RustScopedUsePath {
    segments: Vec<String>,
    inline_modules: Vec<String>,
}

type RustScopedAliases = BTreeMap<(Vec<String>, String), Vec<RustScopedUsePath>>;

fn rust_scoped_aliases(sanitized: &str) -> RustScopedAliases {
    let mut aliases = RustScopedAliases::new();
    for raw in rust_raw_use_statements(sanitized) {
        let local_name = match raw.path.alias.as_deref() {
            Some("_") => None,
            Some(alias) => Some(alias.to_string()),
            None => raw
                .path
                .segments
                .last()
                .filter(|leaf| !matches!(leaf.as_str(), "*" | "crate" | "self" | "super"))
                .cloned(),
        };
        let Some(local_name) = local_name else {
            continue;
        };
        let target = RustScopedUsePath {
            segments: raw.path.segments,
            inline_modules: raw.inline_modules.clone(),
        };
        let targets = aliases.entry((raw.inline_modules, local_name)).or_default();
        if !targets.contains(&target) {
            targets.push(target);
        }
    }
    aliases
}

fn rust_scoped_alias_key(
    path: &[String],
    inline_modules: &[String],
) -> Option<(usize, (Vec<String>, String))> {
    if path.first().is_some_and(|segment| segment == "crate") {
        return None;
    }

    let mut scope = inline_modules.to_vec();
    let mut owner_index = 0usize;
    while path
        .get(owner_index)
        .is_some_and(|segment| segment == "self")
    {
        owner_index += 1;
    }
    while path
        .get(owner_index)
        .is_some_and(|segment| segment == "super")
    {
        scope.pop()?;
        owner_index += 1;
    }
    let owner = path.get(owner_index)?;
    Some((owner_index, (scope, owner.clone())))
}

fn rust_resolve_scoped_paths(
    path: &[String],
    inline_modules: &[String],
    aliases: &RustScopedAliases,
    resolving: &mut BTreeSet<(Vec<String>, String)>,
    depth: usize,
) -> Option<Vec<RustScopedUsePath>> {
    if depth > aliases.len() {
        return None;
    }

    let Some((owner_index, alias_key)) = rust_scoped_alias_key(path, inline_modules) else {
        return Some(vec![RustScopedUsePath {
            segments: path.to_vec(),
            inline_modules: inline_modules.to_vec(),
        }]);
    };
    let Some(targets) = aliases.get(&alias_key) else {
        return Some(vec![RustScopedUsePath {
            segments: path.to_vec(),
            inline_modules: inline_modules.to_vec(),
        }]);
    };
    if !resolving.insert(alias_key.clone()) {
        return None;
    }

    let mut resolved = BTreeSet::new();
    for target in targets {
        let Some(target_paths) = rust_resolve_scoped_paths(
            &target.segments,
            &target.inline_modules,
            aliases,
            resolving,
            depth + 1,
        ) else {
            continue;
        };
        for mut target_path in target_paths {
            target_path
                .segments
                .extend_from_slice(&path[owner_index + 1..]);
            resolved.insert(target_path);
        }
    }
    resolving.remove(&alias_key);
    (!resolved.is_empty()).then(|| resolved.into_iter().collect())
}

fn rust_raw_non_use_paths(sanitized: &str) -> Vec<RustScopedUsePath> {
    let tokens = rust_use_tokens(sanitized);
    let inline_module_openings = (0..tokens.len().saturating_sub(2))
        .filter_map(|index| {
            (tokens[index] == "mod"
                && tokens[index + 1].chars().all(is_ident_char)
                && tokens[index + 2] == "{")
                .then(|| (index + 2, tokens[index + 1].clone()))
        })
        .collect::<BTreeMap<_, _>>();
    let mut paths = Vec::new();
    let mut inline_modules = Vec::<(usize, String)>::new();
    let mut brace_depth = 0usize;
    let mut index = 0usize;

    while index < tokens.len() {
        if tokens[index] == "{" {
            brace_depth += 1;
            if let Some(module) = inline_module_openings.get(&index) {
                inline_modules.push((brace_depth, module.clone()));
            }
            index += 1;
            continue;
        }
        if tokens[index] == "}" {
            if inline_modules
                .last()
                .is_some_and(|(depth, _)| *depth == brace_depth)
            {
                inline_modules.pop();
            }
            brace_depth = brace_depth.saturating_sub(1);
            index += 1;
            continue;
        }
        if tokens[index] == "use" {
            while index < tokens.len() && tokens[index] != ";" {
                index += 1;
            }
            index += usize::from(index < tokens.len());
            continue;
        }
        if !tokens[index].chars().all(is_ident_char)
            || tokens.get(index + 1).is_none_or(|token| token != "::")
        {
            index += 1;
            continue;
        }

        let mut segments = vec![tokens[index].clone()];
        let mut cursor = index + 1;
        while tokens.get(cursor).is_some_and(|token| token == "::")
            && tokens
                .get(cursor + 1)
                .is_some_and(|token| token.chars().all(is_ident_char))
        {
            segments.push(tokens[cursor + 1].clone());
            cursor += 2;
        }
        if segments.len() > 1 {
            paths.push(RustScopedUsePath {
                segments,
                inline_modules: inline_modules
                    .iter()
                    .map(|(_, module)| module.clone())
                    .collect(),
            });
        }
        index = cursor;
    }

    paths
}

fn rust_source_module_segments(source_rel: &str) -> Option<Vec<String>> {
    let path = Path::new(source_rel);
    if path.extension().is_none_or(|extension| extension != "rs") {
        return None;
    }
    let components = path
        .components()
        .map(|component| component.as_os_str().to_str().map(str::to_string))
        .collect::<Option<Vec<_>>>()?;
    if components
        .first()
        .is_none_or(|component| component != "src")
    {
        return None;
    }

    let file = components.last()?;
    let stem = file.strip_suffix(".rs")?;
    let mut module = vec!["crate".to_string()];
    module.extend(components[1..components.len() - 1].iter().cloned());
    let crate_root = components.len() == 2 && matches!(stem, "lib" | "main");
    if stem != "mod" && !crate_root {
        module.push(stem.to_string());
    }
    Some(module)
}

fn rust_canonical_path_segments_in_scope(
    path: &[String],
    source_rel: &str,
    inline_modules: &[String],
) -> Option<Vec<String>> {
    let mut index = 0usize;
    let mut canonical = if path.first().is_some_and(|segment| segment == "crate") {
        index = 1;
        vec!["crate".to_string()]
    } else {
        let mut module = rust_source_module_segments(source_rel)?;
        module.extend(inline_modules.iter().cloned());
        module
    };

    while path.get(index).is_some_and(|segment| segment == "self") {
        index += 1;
    }
    while path.get(index).is_some_and(|segment| segment == "super") {
        if canonical.len() == 1 {
            return None;
        }
        canonical.pop();
        index += 1;
    }
    canonical.extend_from_slice(&path[index..]);
    Some(canonical)
}

fn rust_canonical_paths(sanitized: &str, source_rel: &str) -> Vec<Vec<String>> {
    let aliases = rust_scoped_aliases(sanitized);
    let mut canonical = BTreeSet::new();
    for raw in rust_raw_use_statements(sanitized) {
        let resolved = rust_resolve_scoped_paths(
            &raw.path.segments,
            &raw.inline_modules,
            &aliases,
            &mut BTreeSet::new(),
            0,
        )
        .unwrap_or_else(|| {
            vec![RustScopedUsePath {
                segments: raw.path.segments,
                inline_modules: raw.inline_modules,
            }]
        });
        canonical.extend(resolved.into_iter().filter_map(|path| {
            rust_canonical_path_segments_in_scope(&path.segments, source_rel, &path.inline_modules)
        }));
    }
    for path in rust_raw_non_use_paths(sanitized) {
        let resolved = rust_resolve_scoped_paths(
            &path.segments,
            &path.inline_modules,
            &aliases,
            &mut BTreeSet::new(),
            0,
        )
        .unwrap_or_else(|| vec![path]);
        canonical.extend(resolved.into_iter().filter_map(|path| {
            rust_canonical_path_segments_in_scope(&path.segments, source_rel, &path.inline_modules)
        }));
    }
    canonical.into_iter().collect()
}

fn rust_production_canonical_paths(text: &str, source_rel: &str) -> Vec<Vec<String>> {
    rust_canonical_paths(&rust_sanitized_production_text(text), source_rel)
}

const FORBIDDEN_STATE_STORE_OWNERS: &[&str] = &[
    "catalog",
    "connector",
    "coordinator",
    "dictionary",
    "dml",
    "engine",
    "frontend",
    "meta",
    "mv",
    "sql",
    "table_maintenance",
];

const FORBIDDEN_STATE_STORE_TOKENS: &[&str] = &[
    "DictionaryDefinition",
    "IcebergTable",
    "MaterializedView",
    "MetaStoreProvider",
    "TPlanNode",
    "apache_avro",
];

const SQLITE_ONLY_EXTERNAL_OWNERS: &[&str] = &["fs2", "rusqlite"];
const SQLITE_ONLY_FFI_TOKENS: &[&str] = &["SQLITE_BUSY", "SQLITE_BUSY_SNAPSHOT"];
const FOUNDATIONDB_EXTERNAL_OWNERS: &[&str] = &["foundationdb", "foundationdb_sys"];
const FOUNDATIONDB_RAW_TOKENS: &[&str] = &[
    "FdbError",
    "FdbResult",
    "DatabaseOption",
    "NetworkOption",
    "TransactionOption",
    "MutationType",
    "Versionstamp",
];
const FOUNDATIONDB_FORBIDDEN_OWNER_TOKENS: &[&str] = &[
    "run",
    "transact",
    "on_error",
    "watch",
    "tuple",
    "directory",
    "fallback",
];

#[derive(Clone)]
struct GuardSource {
    path: String,
    text: String,
}

impl GuardSource {
    fn new(path: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            text: text.into(),
        }
    }
}

fn is_state_store_source(path: &str) -> bool {
    path.starts_with("src/state_store/")
}

fn is_connector_source(path: &str) -> bool {
    path.starts_with("src/connector/")
}

fn is_state_store_sqlite_source(path: &str) -> bool {
    path == "src/state_store/sqlite.rs" || path.starts_with("src/state_store/sqlite/")
}

fn is_state_store_foundationdb_source(path: &str) -> bool {
    path.starts_with("src/state_store/foundationdb/")
}

fn is_foundationdb_native_owner(path: &str) -> bool {
    is_state_store_foundationdb_source(path) || path == "src/state_store/runtime.rs"
}

fn path_starts_with(path: &[String], prefix: &[&str]) -> bool {
    path.len() >= prefix.len()
        && path
            .iter()
            .zip(prefix)
            .all(|(actual, expected)| actual == expected)
}

fn has_unqualified_path(tokens: &[String], owner: &str) -> bool {
    tokens
        .windows(2)
        .any(|tokens| tokens[0] == owner && tokens[1] == "::")
}

fn declares_module(tokens: &[String], owner: &str) -> bool {
    tokens.windows(3).any(|tokens| {
        tokens[0] == "mod" && tokens[1] == owner && matches!(tokens[2].as_str(), ";" | "{")
    })
}

fn references_absolute_or_extern_owner(tokens: &[String], owner: &str) -> bool {
    tokens
        .windows(2)
        .any(|tokens| tokens[0] == "::" && tokens[1] == owner)
        || tokens
            .windows(3)
            .any(|tokens| tokens[0] == "extern" && tokens[1] == "crate" && tokens[2] == owner)
}

fn state_store_boundary_violations(sources: &[GuardSource]) -> Vec<String> {
    let mut violations = Vec::new();
    for source in sources {
        let production = rust_sanitized_production_text(&source.text);
        let paths = rust_production_canonical_paths(&production, &source.path);
        let production_tokens = rust_use_tokens(&production);

        if is_state_store_source(&source.path) {
            for path in &paths {
                if path_starts_with(path, &["crate"])
                    && path
                        .get(1)
                        .is_some_and(|owner| FORBIDDEN_STATE_STORE_OWNERS.contains(&owner.as_str()))
                {
                    violations.push(format!(
                        "state-store-forbidden-owner: {} -> {}",
                        source.path,
                        path.join("::")
                    ));
                }
            }

            let tokens = rust_use_tokens(&production);
            for forbidden in FORBIDDEN_STATE_STORE_TOKENS {
                if tokens.iter().any(|token| token == forbidden) {
                    violations.push(format!(
                        "state-store-forbidden-token: {} -> {forbidden}",
                        source.path
                    ));
                }
            }
        }

        for path in &paths {
            if is_connector_source(&source.path)
                && path_starts_with(path, &["crate", "state_store"])
            {
                violations.push(format!(
                    "connector-state-store-dependency: {} -> {}",
                    source.path,
                    path.join("::")
                ));
            }

            if source.path != "src/state_store/mod.rs"
                && !is_state_store_sqlite_source(&source.path)
                && path_starts_with(path, &["crate", "state_store", "sqlite"])
            {
                violations.push(format!(
                    "state-store-sqlite-import-outside-owner: {} -> {}",
                    source.path,
                    path.join("::")
                ));
            }

            if is_state_store_source(&source.path) && !is_state_store_sqlite_source(&source.path) {
                for owner in SQLITE_ONLY_EXTERNAL_OWNERS {
                    if let Some(owner_index) = path.iter().position(|segment| segment == owner)
                        && (!declares_module(&production_tokens, owner)
                            || references_absolute_or_extern_owner(&production_tokens, owner))
                    {
                        violations.push(format!(
                            "state-store-sqlite-external-outside-owner: {} -> {}",
                            source.path,
                            path[owner_index..].join("::")
                        ));
                    }
                }
            }
        }

        if is_state_store_source(&source.path) && !is_state_store_sqlite_source(&source.path) {
            for owner in SQLITE_ONLY_EXTERNAL_OWNERS {
                if production_tokens.iter().any(|token| token == owner)
                    && (!declares_module(&production_tokens, owner)
                        || references_absolute_or_extern_owner(&production_tokens, owner))
                {
                    violations.push(format!(
                        "state-store-sqlite-external-outside-owner: {} -> {owner}",
                        source.path
                    ));
                }
            }
            for token in SQLITE_ONLY_FFI_TOKENS {
                if production_tokens.iter().any(|actual| actual == token) {
                    violations.push(format!(
                        "state-store-sqlite-ffi-outside-owner: {} -> {token}",
                        source.path
                    ));
                }
            }
        }

        if !is_foundationdb_native_owner(&source.path) {
            for path in &paths {
                for owner in FOUNDATIONDB_EXTERNAL_OWNERS {
                    if let Some(owner_index) = path.iter().position(|segment| segment == owner)
                        && (!declares_module(&production_tokens, owner)
                            || references_absolute_or_extern_owner(&production_tokens, owner))
                    {
                        violations.push(format!(
                            "state-store-foundationdb-native-outside-owner: {} -> {}",
                            source.path,
                            path[owner_index..].join("::")
                        ));
                    }
                }
            }
            for owner in FOUNDATIONDB_EXTERNAL_OWNERS {
                if production_tokens.iter().any(|token| token == owner)
                    && (!declares_module(&production_tokens, owner)
                        || references_absolute_or_extern_owner(&production_tokens, owner))
                {
                    violations.push(format!(
                        "state-store-foundationdb-native-outside-owner: {} -> {owner}",
                        source.path
                    ));
                }
            }
            for token in FOUNDATIONDB_RAW_TOKENS {
                if production_tokens.iter().any(|actual| actual == token) {
                    violations.push(format!(
                        "state-store-foundationdb-token-outside-owner: {} -> {token}",
                        source.path
                    ));
                }
            }
        }

        if is_foundationdb_native_owner(&source.path) {
            for token in FOUNDATIONDB_FORBIDDEN_OWNER_TOKENS {
                let is_member_api = matches!(*token, "run" | "transact" | "on_error");
                let present = if is_member_api {
                    production_tokens
                        .windows(2)
                        .enumerate()
                        .any(|(index, tokens)| {
                            if !matches!(tokens[0].as_str(), "." | "::") || tokens[1] != *token {
                                return false;
                            }
                            let explicit_network_runner = *token == "run"
                                && source.path == "src/state_store/runtime.rs"
                                && tokens[0] == "::"
                                && index > 0
                                && production_tokens[index - 1] == "NetworkRunner";
                            !explicit_network_runner
                        })
                } else {
                    production_tokens.iter().any(|actual| actual == token)
                };
                if present {
                    violations.push(format!(
                        "state-store-foundationdb-forbidden-api: {} -> {token}",
                        source.path
                    ));
                }
            }
        }
        if is_connector_source(&source.path)
            && paths.iter().any(|path| path.as_slice() == ["crate", "*"])
            && has_unqualified_path(&production_tokens, "state_store")
            && !declares_module(&production_tokens, "state_store")
        {
            violations.push(format!(
                "connector-state-store-dependency: {} -> crate::* + state_store::",
                source.path
            ));
        }
    }
    violations.sort();
    violations.dedup();
    violations
}

#[test]
fn state_store_owner_is_non_vacuous_and_obeys_boundary() {
    let src = src_dir();
    let owner = src.join("state_store");
    assert!(
        owner.is_dir(),
        "state store owner must exist at {}",
        rel(&owner)
    );

    let files = production_rs_files_from_entries(&src, &[src.join("lib.rs"), src.join("main.rs")]);
    let owner_files = files
        .iter()
        .filter(|path| path.starts_with(&owner))
        .collect::<Vec<_>>();
    assert!(
        !owner_files.is_empty(),
        "state store owner must contain reachable production Rust sources"
    );

    let sources = files
        .iter()
        .map(|path| {
            GuardSource::new(
                rel(path),
                fs::read_to_string(path).expect("read state store source"),
            )
        })
        .collect::<Vec<_>>();
    let violations = state_store_boundary_violations(&sources);
    assert!(
        violations.is_empty(),
        "state store architecture boundary failed:\n{}",
        violations.join("\n")
    );
}

#[test]
fn state_store_boundary_detector_rejects_forbidden_imports() {
    let sources = [
        GuardSource::new("src/state_store/contract.rs", "use crate::meta::*;"),
        GuardSource::new("src/state_store/contract.rs", "use crate::connector::*;"),
        GuardSource::new(
            "src/connector/state_store.rs",
            "use crate::state_store::StateStoreConfig;",
        ),
        GuardSource::new(
            "src/catalog/reexport.rs",
            "pub use crate::state_store::sqlite::*;",
        ),
    ];

    let violations = state_store_boundary_violations(&sources);

    assert!(
        violations
            .iter()
            .any(|item| item.contains("crate::meta::*")),
        "meta dependency must be rejected: {violations:?}"
    );
    assert!(
        violations
            .iter()
            .any(|item| item.contains("crate::connector::*")),
        "connector dependency must be rejected: {violations:?}"
    );
    assert!(
        violations
            .iter()
            .any(|item| item.contains("crate::state_store::sqlite::*")),
        "sqlite adapter re-export must be rejected: {violations:?}"
    );
    assert!(
        violations
            .iter()
            .any(|item| item.contains("connector-state-store-dependency")),
        "connector dependency on state store must be rejected: {violations:?}"
    );
}

#[test]
fn state_store_boundary_detector_rejects_canonical_alias_group_and_glob_bypasses() {
    let sources = [
        GuardSource::new(
            "src/state_store/contract.rs",
            "use crate::{meta as metadata}; fn leak() { metadata::MetaStoreProvider::open(); }",
        ),
        GuardSource::new(
            "src/state_store/contract.rs",
            "use crate::*; fn leak() { meta::MetaStoreProvider::open(); }",
        ),
        GuardSource::new(
            "src/catalog/reexport.rs",
            "use crate::state_store as durable; pub use durable::sqlite::*;",
        ),
        GuardSource::new(
            "src/connector/state_store.rs",
            "use crate::*; fn leak(_: state_store::StateStoreConfig) {}",
        ),
    ];

    let violations = state_store_boundary_violations(&sources);

    for expected in [
        "state-store-forbidden-owner: src/state_store/contract.rs -> crate::meta",
        "state-store-sqlite-import-outside-owner: src/catalog/reexport.rs -> crate::state_store::sqlite::*",
        "connector-state-store-dependency: src/connector/state_store.rs -> crate::* + state_store::",
    ] {
        assert!(
            violations.iter().any(|violation| violation == expected),
            "canonical alias/group/glob dependency escaped detection: expected={expected}, violations={violations:?}"
        );
    }
}

#[test]
fn state_store_boundary_detector_rejects_each_forbidden_token() {
    let fixtures = [
        ("MetaStoreProvider", "use crate::safe::MetaStoreProvider;"),
        ("apache_avro", "use apache_avro::Schema;"),
        ("TPlanNode", "use crate::safe::TPlanNode;"),
        ("IcebergTable", "use crate::safe::IcebergTable;"),
        ("MaterializedView", "use crate::safe::MaterializedView;"),
        (
            "DictionaryDefinition",
            "use crate::safe::DictionaryDefinition;",
        ),
    ];

    for (token, text) in fixtures {
        let violations = state_store_boundary_violations(&[GuardSource::new(
            "src/state_store/contract.rs",
            text,
        )]);
        assert!(
            violations.iter().any(|item| item
                == &format!("state-store-forbidden-token: src/state_store/contract.rs -> {token}")),
            "forbidden token {token} must be rejected: {violations:?}"
        );
    }
}

#[test]
fn state_store_sqlite_dependency_boundary_matches_provider_contract() {
    assert_eq!(
        FORBIDDEN_STATE_STORE_OWNERS,
        [
            "catalog",
            "connector",
            "coordinator",
            "dictionary",
            "dml",
            "engine",
            "frontend",
            "meta",
            "mv",
            "sql",
            "table_maintenance",
        ]
    );
    assert_eq!(
        FORBIDDEN_STATE_STORE_TOKENS,
        [
            "DictionaryDefinition",
            "IcebergTable",
            "MaterializedView",
            "MetaStoreProvider",
            "TPlanNode",
            "apache_avro",
        ]
    );
}

#[test]
fn state_store_boundary_detector_allows_sqlite_adapter_internal_imports() {
    let sources = [
        GuardSource::new(
            "src/state_store/sqlite/mod.rs",
            "use self::schema::SqliteSchema;",
        ),
        GuardSource::new(
            "src/state_store/sqlite/transaction.rs",
            "use super::schema::SqliteSchema;",
        ),
        GuardSource::new(
            "src/state_store/mod.rs",
            "pub use crate::state_store::sqlite::SqliteStateStore;",
        ),
    ];

    assert!(state_store_boundary_violations(&sources).is_empty());
}

#[test]
fn state_store_boundary_detector_rejects_sqlite_external_crates_and_ffi_tokens_outside_owner() {
    let sources = [
        GuardSource::new("src/state_store/contract.rs", "use rusqlite::Connection;"),
        GuardSource::new(
            "src/state_store/config.rs",
            "use rusqlite::{Connection as Db, ffi::*};",
        ),
        GuardSource::new("src/state_store/runner.rs", "use fs2::FileExt as LockExt;"),
        GuardSource::new(
            "src/state_store/remote/mod.rs",
            "use fs2::*; const CODE: i32 = SQLITE_BUSY_SNAPSHOT;",
        ),
        GuardSource::new(
            "src/state_store/future_provider.rs",
            "extern crate rusqlite as db; fn leak(_: db::Connection) {}",
        ),
        GuardSource::new(
            "src/state_store/runner.rs",
            "extern crate fs2 as locks; fn leak<T: locks::FileExt>() {}",
        ),
        GuardSource::new(
            "src/state_store/shadowed_rusqlite.rs",
            "mod rusqlite {} use ::rusqlite::{Connection as Db}; fn leak(_: Db) {}",
        ),
        GuardSource::new(
            "src/state_store/shadowed_fs2.rs",
            "mod fs2 {} extern crate fs2 as locks; fn leak<T: locks::FileExt>() {}",
        ),
    ];

    let violations = state_store_boundary_violations(&sources);
    for (path, dependency) in [
        ("src/state_store/contract.rs", "rusqlite::Connection"),
        ("src/state_store/config.rs", "rusqlite::ffi::*"),
        ("src/state_store/runner.rs", "fs2::FileExt"),
        ("src/state_store/remote/mod.rs", "fs2::*"),
        ("src/state_store/remote/mod.rs", "SQLITE_BUSY_SNAPSHOT"),
        ("src/state_store/future_provider.rs", "rusqlite"),
        ("src/state_store/runner.rs", "fs2"),
        ("src/state_store/shadowed_rusqlite.rs", "rusqlite"),
        ("src/state_store/shadowed_fs2.rs", "fs2"),
    ] {
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains(path) && violation.contains(dependency)),
            "SQLite-only dependency escaped at {path}: {dependency}; violations={violations:?}"
        );
    }
}

#[test]
fn state_store_boundary_detector_allows_truly_local_shadow_modules() {
    let sources = [
        GuardSource::new(
            "src/state_store/local_rusqlite.rs",
            "mod rusqlite { pub struct Connection; } \
             use rusqlite::Connection; fn local(_: Connection) {}",
        ),
        GuardSource::new(
            "src/state_store/local_fs2.rs",
            "mod fs2 { pub trait FileExt {} } \
             use fs2::FileExt; fn local<T: FileExt>() {}",
        ),
    ];

    assert!(state_store_boundary_violations(&sources).is_empty());
}

#[test]
fn state_store_boundary_detector_allows_sqlite_external_crates_and_ffi_tokens_in_owner() {
    let sources = [GuardSource::new(
        "src/state_store/sqlite/txn.rs",
        "use rusqlite::{Connection, ffi::*}; use fs2::FileExt; \
         const BUSY: i32 = SQLITE_BUSY; const SNAPSHOT: i32 = SQLITE_BUSY_SNAPSHOT;",
    )];

    assert!(state_store_boundary_violations(&sources).is_empty());
}

#[test]
fn state_store_boundary_detector_rejects_foundationdb_native_leaks_outside_owner() {
    let sources = [
        GuardSource::new(
            "src/state_store/config.rs",
            "use foundationdb::options::NetworkOption;",
        ),
        GuardSource::new(
            "src/state_store/runner.rs",
            "extern crate foundationdb_sys as fdb; fn leak(_: FdbError) {}",
        ),
        GuardSource::new(
            "src/connector/state_store.rs",
            "use crate::safe::Versionstamp;",
        ),
    ];

    let violations = state_store_boundary_violations(&sources);
    for (path, token) in [
        ("src/state_store/config.rs", "foundationdb"),
        ("src/state_store/config.rs", "NetworkOption"),
        ("src/state_store/runner.rs", "foundationdb_sys"),
        ("src/state_store/runner.rs", "FdbError"),
        ("src/connector/state_store.rs", "Versionstamp"),
    ] {
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains(path) && violation.contains(token)),
            "FoundationDB native detail escaped at {path}: {token}; violations={violations:?}"
        );
    }
}

#[test]
fn state_store_boundary_detector_allows_foundationdb_native_details_only_in_owner() {
    let sources = [
        GuardSource::new(
            "src/state_store/foundationdb/mod.rs",
            "use foundationdb::{Database, FdbError, options::TransactionOption};",
        ),
        GuardSource::new(
            "src/state_store/runtime.rs",
            "use foundationdb::{api::NetworkRunner, options::NetworkOption}; \
             fn start(runner: NetworkRunner) { unsafe { NetworkRunner::run(runner); } }",
        ),
    ];

    assert!(state_store_boundary_violations(&sources).is_empty());
}

#[test]
fn state_store_boundary_detector_rejects_foundationdb_owner_domain_and_forbidden_apis() {
    let sources = [
        GuardSource::new(
            "src/state_store/foundationdb/mod.rs",
            "use crate::engine::Engine; fn bad(db: Database) { db.run(); db.transact(); Database::run(); }",
        ),
        GuardSource::new(
            "src/state_store/foundationdb/txn.rs",
            "fn bad(tx: Transaction) { tx.on_error(); tx.watch(); tuple(); directory(); fallback(); }",
        ),
    ];

    let violations = state_store_boundary_violations(&sources);
    for token in [
        "crate::engine",
        "run",
        "transact",
        "on_error",
        "watch",
        "tuple",
        "directory",
        "fallback",
    ] {
        assert!(
            violations.iter().any(|violation| violation.contains(token)),
            "forbidden FoundationDB owner token escaped: {token}; violations={violations:?}"
        );
    }
}

#[test]
fn state_store_boundary_detector_rejects_foundationdb_runtime_forbidden_apis() {
    let violations = state_store_boundary_violations(&[GuardSource::new(
        "src/state_store/runtime.rs",
        "fn bad(db: Database, tx: Transaction) { \
         db.run(); Database::run(); db.transact(); tx.on_error(); tx.watch(); \
         tuple(); directory(); fallback(); }",
    )]);

    for token in [
        "run",
        "transact",
        "on_error",
        "watch",
        "tuple",
        "directory",
        "fallback",
    ] {
        let expected_suffix = format!(" -> {token}");
        assert!(
            violations
                .iter()
                .any(|violation| violation.ends_with(&expected_suffix)),
            "forbidden FoundationDB runtime token escaped: {token}; violations={violations:?}"
        );
    }
}

#[test]
fn state_store_foundationdb_dependency_boundary_matches_provider_contract() {
    assert_eq!(
        FOUNDATIONDB_EXTERNAL_OWNERS,
        ["foundationdb", "foundationdb_sys"]
    );
    assert_eq!(
        FOUNDATIONDB_RAW_TOKENS,
        [
            "FdbError",
            "FdbResult",
            "DatabaseOption",
            "NetworkOption",
            "TransactionOption",
            "MutationType",
            "Versionstamp",
        ]
    );
    assert_eq!(
        FOUNDATIONDB_FORBIDDEN_OWNER_TOKENS,
        [
            "run",
            "transact",
            "on_error",
            "watch",
            "tuple",
            "directory",
            "fallback",
        ]
    );
}

#[test]
fn state_store_foundationdb_provider_variant_is_feature_independent() {
    let config = src_dir().join("state_store/config.rs");
    let source = fs::read_to_string(&config).expect("read state store config");
    let tokens = rust_source_tokens(&source);
    let provider_body_open = tokens
        .iter()
        .enumerate()
        .find_map(|(index, token)| {
            (token.text == "enum"
                && tokens
                    .get(index + 1)
                    .is_some_and(|name| name.text == "StateStoreProviderConfig"))
            .then_some(index + 2)
        })
        .and_then(|after_name| {
            tokens[after_name..]
                .iter()
                .position(|token| token.text == "{")
                .map(|offset| after_name + offset)
        })
        .expect("StateStoreProviderConfig must exist in production config");
    let provider_body_close = rust_matching_token(&tokens, provider_body_open, "{", "}")
        .expect("StateStoreProviderConfig body must be balanced");

    let mut cursor = provider_body_open + 1;
    while cursor < provider_body_close {
        while tokens.get(cursor).is_some_and(|token| token.text == ",") {
            cursor += 1;
        }

        let mut has_cfg_attribute = false;
        while tokens.get(cursor).is_some_and(|token| token.text == "#")
            && tokens
                .get(cursor + 1)
                .is_some_and(|token| token.text == "[")
        {
            let attribute_end = rust_matching_token(&tokens, cursor + 1, "[", "]")
                .expect("StateStoreProviderConfig attribute must be balanced");
            has_cfg_attribute |= tokens
                .get(cursor + 2)
                .is_some_and(|attribute| attribute.text == "cfg");
            cursor = attribute_end + 1;
        }

        let variant = tokens
            .get(cursor)
            .expect("StateStoreProviderConfig variant must be present");
        if variant.text == "Foundationdb" {
            assert!(
                !has_cfg_attribute,
                "Foundationdb config variant must not be feature-gated"
            );
            return;
        }

        cursor += 1;
        while cursor < provider_body_close && tokens[cursor].text != "," {
            if matches!(tokens[cursor].text.as_str(), "{" | "(" | "[") {
                let closing = match tokens[cursor].text.as_str() {
                    "{" => "}",
                    "(" => ")",
                    "[" => "]",
                    _ => unreachable!(),
                };
                cursor = rust_matching_token(&tokens, cursor, &tokens[cursor].text, closing)
                    .expect("StateStoreProviderConfig variant must be balanced")
                    + 1;
            } else {
                cursor += 1;
            }
        }
    }

    panic!("Foundationdb provider variant must exist when the feature is off");
}

#[test]
fn state_store_boundary_detector_ignores_non_production_noise() {
    let sources = [
        GuardSource::new(
            "src/state_store/contract.rs",
            r#"
// use crate::meta::*;
// use crate::safe::{MetaStoreProvider, TPlanNode, MaterializedView};
// use apache_avro::Schema;
const EXAMPLE: &str = "crate::connector::{IcebergTable, DictionaryDefinition}";
const CONNECTOR_EXAMPLE: &str = "crate::state_store::StateStoreConfig";
"#,
        ),
        GuardSource::new(
            "src/state_store/contract.rs",
            r#"
#[cfg(test)]
mod tests {
    use apache_avro::Schema;
    use crate::safe::{
        DictionaryDefinition, IcebergTable, MaterializedView, MetaStoreProvider, TPlanNode,
    };
    use crate::state_store::sqlite::*;
}
"#,
        ),
        GuardSource::new(
            "src/connector/state_store.rs",
            r#"
// use crate::state_store::StateStoreConfig;
const EXAMPLE: &str = "crate::state_store::StateStoreConfig";
#[cfg(test)]
use crate::state_store::StateStoreConfig;
"#,
        ),
        GuardSource::new(
            "src/connector/local.rs",
            r#"
use crate::*;
mod state_store {
    pub fn local_helper() {}
}
fn use_local_module() { state_store::local_helper(); }
"#,
        ),
    ];

    assert!(state_store_boundary_violations(&sources).is_empty());
}

const MYSQL_ASYNC_EXTERNAL_OWNERS: &[&str] = &["mysql_async"];
const MYSQL_JDBC_EXTERNAL_OWNERS: &[&str] = &["mysql", "mysql_common"];
const MYSQL_JDBC_OWNER_TOKENS: &[&str] = &["JdbcScanConfig"];
const MYSQL_RAW_SERVER_CODES: &[&str] = &["1062", "1205", "1213", "2006", "2013"];
const MYSQL_TRANSACTION_SQL: &[&str] = &[
    "start transaction",
    "set transaction isolation level",
    "lock in share mode",
    "for update",
];
const MYSQL_SCHEMA_TOKENS: &[&str] = &[
    "state_store_meta",
    "state_store_kv",
    "state_store_changes",
    "state_store_commits",
];

fn is_state_store_mysql_source(path: &str) -> bool {
    path.starts_with("src/state_store/mysql/")
}

fn is_mysql_native_owner(path: &str) -> bool {
    is_state_store_mysql_source(path)
}

fn rust_string_literals(text: &str) -> Vec<String> {
    let mut production = text.as_bytes().to_vec();
    for span in rust_test_only_item_spans(text) {
        for byte in &mut production[span] {
            if !matches!(*byte, b'\n' | b'\r') {
                *byte = b' ';
            }
        }
    }
    let production =
        String::from_utf8(production).expect("production literal sanitizer must preserve UTF-8");
    let bytes = production.as_bytes();
    let mut values = Vec::new();
    let mut index = 0usize;
    while index < bytes.len() {
        if bytes[index] == b'/' && bytes.get(index + 1) == Some(&b'/') {
            index += 2;
            while bytes.get(index).is_some_and(|byte| *byte != b'\n') {
                index += 1;
            }
        } else if bytes[index] == b'/' && bytes.get(index + 1) == Some(&b'*') {
            index += 2;
            let mut depth = 1usize;
            while index < bytes.len() && depth > 0 {
                if bytes[index] == b'/' && bytes.get(index + 1) == Some(&b'*') {
                    depth += 1;
                    index += 2;
                } else if bytes[index] == b'*' && bytes.get(index + 1) == Some(&b'/') {
                    depth -= 1;
                    index += 2;
                } else {
                    index += 1;
                }
            }
        } else if let Some((hashes, opening_len)) = rust_raw_string_open(bytes, index) {
            let start = index;
            index += opening_len;
            while index < bytes.len() {
                let closes = bytes[index] == b'"'
                    && bytes
                        .get(index + 1..index + 1 + hashes)
                        .is_some_and(|suffix| suffix.iter().all(|byte| *byte == b'#'));
                if closes {
                    index += hashes + 1;
                    if let Some(value) = decode_rust_string_literal(&production[start..index]) {
                        values.push(value);
                    }
                    break;
                }
                index += 1;
            }
        } else if bytes[index] == b'"' {
            let start = index;
            index += 1;
            let mut escaped = false;
            while index < bytes.len() {
                let byte = bytes[index];
                index += 1;
                if escaped {
                    escaped = false;
                } else if byte == b'\\' {
                    escaped = true;
                } else if byte == b'"' {
                    if let Some(value) = decode_rust_string_literal(&production[start..index]) {
                        values.push(value);
                    }
                    break;
                }
            }
        } else if bytes[index] == b'\''
            && let Some(end) = rust_char_literal_end(&production, index)
        {
            index = end + 1;
        } else {
            index += 1;
        }
    }
    values
}

fn external_owner_present(paths: &[Vec<String>], tokens: &[String], owner: &str) -> Option<String> {
    if !tokens.iter().any(|token| token == owner) {
        return None;
    }
    paths.iter().find_map(|path| {
        path.iter()
            .position(|segment| segment == owner)
            .filter(|index| {
                owner != "mysql"
                    || (!(*index >= 2
                        && path[*index - 2] == "crate"
                        && path[*index - 1] == "state_store")
                        && !(*index > 0
                            && *index + 1 == path.len()
                            && path[*index - 1] == "StateStoreRuntime"))
            })
            .filter(|_| {
                !declares_module(tokens, owner)
                    || references_absolute_or_extern_owner(tokens, owner)
            })
            .map(|index| path[index..].join("::"))
    })
}

fn unqualified_external_namespace_present(tokens: &[String], owner: &str) -> bool {
    tokens.iter().enumerate().any(|(index, token)| {
        token == owner
            && tokens.get(index + 1).is_some_and(|token| token == "::")
            && (index == 0 || tokens[index - 1] != "::")
    })
}

fn state_store_mysql_boundary_violations(sources: &[GuardSource]) -> Vec<String> {
    let mut violations = Vec::new();
    for source in sources {
        let production = rust_sanitized_production_text(&source.text);
        let paths = rust_production_canonical_paths(&production, &source.path);
        let tokens = rust_use_tokens(&production);

        if !is_mysql_native_owner(&source.path) {
            for owner in MYSQL_ASYNC_EXTERNAL_OWNERS {
                if let Some(reference) =
                    external_owner_present(&paths, &tokens, owner).or_else(|| {
                        unqualified_external_namespace_present(&tokens, owner)
                            .then(|| (*owner).to_owned())
                    })
                {
                    violations.push(format!(
                        "state-store-mysql-native-outside-owner: {} -> {reference}",
                        source.path
                    ));
                }
            }
        }

        if is_state_store_source(&source.path) && !is_state_store_mysql_source(&source.path) {
            for owner in MYSQL_JDBC_EXTERNAL_OWNERS {
                if let Some(reference) = external_owner_present(&paths, &tokens, owner) {
                    violations.push(format!(
                        "state-store-mysql-jdbc-outside-jdbc-owner: {} -> {reference}",
                        source.path
                    ));
                }
            }
        }

        if is_mysql_native_owner(&source.path) {
            for owner in MYSQL_JDBC_EXTERNAL_OWNERS {
                if let Some(reference) =
                    external_owner_present(&paths, &tokens, owner).or_else(|| {
                        unqualified_external_namespace_present(&tokens, owner)
                            .then(|| (*owner).to_owned())
                    })
                {
                    violations.push(format!(
                        "state-store-mysql-jdbc-leak: {} -> {reference}",
                        source.path
                    ));
                }
            }
            for token in MYSQL_JDBC_OWNER_TOKENS {
                if tokens.iter().any(|actual| actual == token) {
                    violations.push(format!(
                        "state-store-mysql-jdbc-leak: {} -> {token}",
                        source.path
                    ));
                }
            }
        }

        if is_state_store_source(&source.path)
            && !is_mysql_native_owner(&source.path)
            && !is_state_store_foundationdb_source(&source.path)
        {
            for code in MYSQL_RAW_SERVER_CODES {
                if tokens.iter().any(|token| token == code) {
                    violations.push(format!(
                        "state-store-mysql-server-code-outside-owner: {} -> {code}",
                        source.path
                    ));
                }
            }
        }

        let string_literals = rust_string_literals(&source.text);
        if is_state_store_source(&source.path)
            && !is_state_store_mysql_source(&source.path)
            && !is_state_store_sqlite_source(&source.path)
        {
            for literal in &string_literals {
                let lower = literal.to_ascii_lowercase();
                for sql in MYSQL_TRANSACTION_SQL {
                    if lower.contains(sql) {
                        violations.push(format!(
                            "state-store-mysql-transaction-sql-outside-owner: {} -> {}",
                            source.path,
                            sql.to_ascii_uppercase()
                        ));
                    }
                }
            }
        }
        if !is_state_store_mysql_source(&source.path) && !is_state_store_sqlite_source(&source.path)
        {
            for literal in &string_literals {
                let lower = literal.to_ascii_lowercase();
                for table in MYSQL_SCHEMA_TOKENS {
                    if lower.contains(table) {
                        violations.push(format!(
                            "state-store-mysql-schema-outside-owner: {} -> {table}",
                            source.path
                        ));
                    }
                }
            }
        }
    }
    violations.sort();
    violations.dedup();
    violations
}

fn mysql_provider_variant_cfg_violations(path: &str, text: &str) -> Vec<String> {
    let tokens = rust_source_tokens(text);
    let mut violations = Vec::new();
    let Some(body_open) = tokens.iter().enumerate().find_map(|(index, token)| {
        (token.text == "enum"
            && tokens
                .get(index + 1)
                .is_some_and(|name| name.text == "StateStoreProviderConfig"))
        .then(|| {
            tokens[index + 2..]
                .iter()
                .position(|token| token.text == "{")
                .map(|offset| index + 2 + offset)
        })
        .flatten()
    }) else {
        return violations;
    };
    let Some(body_close) = rust_matching_token(&tokens, body_open, "{", "}") else {
        return vec![format!("state-store-mysql-provider-config-parse: {path}")];
    };
    let mut cursor = body_open + 1;
    while cursor < body_close {
        while tokens.get(cursor).is_some_and(|token| token.text == ",") {
            cursor += 1;
        }
        let mut cfg_gated = false;
        while tokens.get(cursor).is_some_and(|token| token.text == "#")
            && tokens
                .get(cursor + 1)
                .is_some_and(|token| token.text == "[")
        {
            let Some(close) = rust_matching_token(&tokens, cursor + 1, "[", "]") else {
                return vec![format!("state-store-mysql-provider-config-parse: {path}")];
            };
            cfg_gated |= tokens[cursor + 2..close]
                .iter()
                .any(|token| matches!(token.text.as_str(), "cfg" | "cfg_attr"));
            cursor = close + 1;
        }
        if tokens
            .get(cursor)
            .is_some_and(|token| token.text == "Mysql")
            && cfg_gated
        {
            violations.push(format!(
                "state-store-mysql-provider-variant-feature-gated: {path} -> Mysql"
            ));
        }
        while cursor < body_close && tokens[cursor].text != "," {
            if matches!(tokens[cursor].text.as_str(), "{" | "(" | "[") {
                let closing = match tokens[cursor].text.as_str() {
                    "{" => "}",
                    "(" => ")",
                    "[" => "]",
                    _ => unreachable!(),
                };
                let Some(close) =
                    rust_matching_token(&tokens, cursor, &tokens[cursor].text, closing)
                else {
                    return vec![format!("state-store-mysql-provider-config-parse: {path}")];
                };
                cursor = close + 1;
            } else {
                cursor += 1;
            }
        }
    }
    violations
}

#[test]
fn mysql_state_store_open_cancellation_gate_is_feature_gated_and_provider_owned() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mysql_mod =
        fs::read_to_string(root.join("src/state_store/mysql/mod.rs")).expect("read MySQL module");
    let hooks = fs::read_to_string(root.join("src/state_store/mysql/open_test_hooks.rs"))
        .expect("read MySQL open test hooks");
    let schema = fs::read_to_string(root.join("src/state_store/mysql/schema.rs"))
        .expect("read MySQL schema owner");
    let runtime =
        fs::read_to_string(root.join("src/state_store/runtime.rs")).expect("read runtime owner");

    assert!(
        mysql_mod.contains(
            "#[cfg(feature = \"state-store-test-hooks\")]\npub(crate) mod open_test_hooks;"
        ),
        "MySQL open cancellation hooks must be entirely feature-gated"
    );
    for required in [
        "AfterAdvisoryLock",
        "AfterReadOnlyStart",
        "HashMap<String, Arc<OpenGateState>>",
        "connection_id",
        "wait_completed",
    ] {
        assert!(
            hooks.contains(required),
            "MySQL open cancellation gate must freeze `{required}`"
        );
    }
    for forbidden in [
        "NOVA_MYSQL_PROVISIONER",
        "provisioner.cnf",
        "provision-test-database",
    ] {
        assert!(
            !hooks.contains(forbidden),
            "MySQL open cancellation gate must not access provisioner authority: {forbidden}"
        );
    }
    for required in [
        "take_mysql_open_gate(database, MysqlOpenGatePhase::AfterAdvisoryLock)",
        "take_mysql_open_gate(database, MysqlOpenGatePhase::AfterReadOnlyStart)",
        "session.destroy_connection().await",
    ] {
        assert!(
            schema.contains(required),
            "MySQL schema cancellation disposition must retain `{required}`"
        );
    }
    for required in [
        "tokio::spawn(async move",
        "oneshot::channel()",
        "MysqlOpenWaiterGuard",
        "MysqlOpenCancellation",
        "open_store_owned",
    ] {
        assert!(
            runtime.contains(required),
            "MySQL runtime must retain provider-owned open supervision: {required}"
        );
    }
    let operation_acquire = runtime
        .find("let opening = self.acquire_operation()?;")
        .expect("open must acquire its operation guard");
    let pool_acquire = runtime
        .find("let pool = self.get_or_create_pool(&database)?;")
        .expect("open must acquire its database pool");
    let owner_spawn = runtime
        .find("tokio::spawn(async move")
        .expect("open must spawn its provider owner");
    assert!(
        operation_acquire < pool_acquire && pool_acquire < owner_spawn,
        "MySQL open must register its operation before creating its pool and spawning the provider owner"
    );
    let owner_start = runtime
        .find("async fn open_store_owned(")
        .expect("find provider-owned open");
    let owner_end = runtime[owner_start..]
        .find("async fn prepare_pool(")
        .map(|offset| owner_start + offset)
        .expect("find end of provider-owned open");
    let owner = &runtime[owner_start..owner_end];
    assert!(
        owner.contains("_opening: MysqlRuntimeGuard")
            && !owner.contains("MysqlRuntimeGuard::acquire"),
        "the pre-registered operation guard must be moved into the provider owner"
    );
}

fn collect_mysql_fixture_sources(dir: &Path, root: &Path, sources: &mut Vec<GuardSource>) {
    let mut entries = fs::read_dir(dir)
        .unwrap_or_else(|error| panic!("read MySQL fixture directory {}: {error}", dir.display()))
        .map(|entry| entry.expect("read MySQL fixture entry").path())
        .collect::<Vec<_>>();
    entries.sort();
    for path in entries {
        if path.is_dir() {
            if path.file_name().and_then(|name| name.to_str()) != Some("runtime") {
                collect_mysql_fixture_sources(&path, root, sources);
            }
            continue;
        }
        let relative = path
            .strip_prefix(root)
            .expect("fixture path below repository root")
            .to_string_lossy()
            .replace('\\', "/");
        let text = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read MySQL fixture owner {relative}: {error}"));
        sources.push(GuardSource::new(relative, text));
    }
}

fn mysql_fixture_contract_violations(sources: &[GuardSource]) -> Vec<String> {
    let pinned_image = format!(
        "mysql:8.4.10@sha256:{}{}",
        "c831a0f11348d402b43d77453e17d770", "be2eef356615a2823fe0f5a0d6c8b9af"
    );
    let create_database = ["CREATE", "DATABASE"].join(" ");
    let drop_database = ["DROP", "DATABASE"].join(" ");
    let provisioner = "docker/mysql-state-store/provision-test-database.sh";
    let compose = "docker/mysql-state-store/compose.yml";
    let up = "docker/mysql-state-store/up.sh";
    let down = "docker/mysql-state-store/down.sh";
    let contract = "docker/mysql-state-store/probes/contract.sh";
    let mut violations = Vec::new();

    for required in [
        compose,
        up,
        "docker/mysql-state-store/status.sh",
        down,
        provisioner,
        "docker/mysql-state-store/README.md",
        "docker/mysql-state-store/probes/schema.sql",
        contract,
    ] {
        if !sources.iter().any(|source| source.path == required) {
            violations.push(format!("mysql-fixture-required-owner-missing: {required}"));
        }
    }

    let image_owners = sources
        .iter()
        .filter(|source| source.text.contains(&pinned_image))
        .map(|source| source.path.as_str())
        .collect::<Vec<_>>();
    if image_owners != [compose] {
        violations.push(format!(
            "mysql-fixture-pinned-image-owner: {image_owners:?}"
        ));
    }

    for source in sources {
        if ![compose, up, provisioner].contains(&source.path.as_str())
            && [
                "NOVA_MYSQL_PROVISIONER_",
                "novarocks-mysql-provisioner.cnf",
                "provisioner.cnf",
                "mysql_admin",
            ]
            .iter()
            .any(|token| source.text.contains(token))
        {
            violations.push(format!(
                "mysql-fixture-provisioner-boundary: {}",
                source.path
            ));
        }
        if source.path != provisioner
            && (source.text.contains(&create_database) || source.text.contains(&drop_database))
        {
            violations.push(format!("mysql-fixture-database-ddl-owner: {}", source.path));
        }
        for forbidden in [
            "docker-entrypoint-initdb.d",
            "/opt/homebrew",
            "iceberg-rest",
            "iceberg-hive",
        ] {
            if source.text.contains(forbidden) {
                violations.push(format!(
                    "mysql-fixture-isolation-leak: {} -> {forbidden}",
                    source.path
                ));
            }
        }
    }

    let source_text = |path: &str| {
        sources
            .iter()
            .find(|source| source.path == path)
            .map(|source| source.text.as_str())
            .unwrap_or("")
    };
    let compose_text = source_text(compose);
    let up_text = source_text(up);
    let down_text = source_text(down);
    let status_text = source_text("docker/mysql-state-store/status.sh");
    let provision_text = source_text(provisioner);
    let contract_text = source_text(contract);

    if compose_text.contains("MYSQL_DATABASE:") {
        violations.push("mysql-fixture-compose-shared-database".to_owned());
    }
    let cleaner_service = format!(
        r#"  runtime-cleaner:
    image: {pinned_image}
    profiles:
      - cleanup
    user: "0:0"
    network_mode: none
    volumes:
      - "${{NOVA_MYSQL_RUNTIME_DIR}}/data:/var/lib/mysql"
    entrypoint:
      - /bin/sh
      - -eu
      - -c
    command:
      - rm -rf -- /var/lib/mysql/* /var/lib/mysql/.[!.]* /var/lib/mysql/..?*"#
    );
    if compose_text.matches(&cleaner_service).count() != 1
        || compose_text.matches(&pinned_image).count() != 2
    {
        violations.push("mysql-fixture-runtime-cleaner-boundary".to_owned());
    }
    if provision_text.contains("ON \\`${database}\\`.*")
        || provision_text.contains("ON `${database}`.*")
    {
        violations.push("mysql-fixture-provider-database-wide-grant".to_owned());
    }
    if provision_text.contains("mysql_admin --execute")
        && provision_text.contains("NOVAROCKS_MYSQL_PASSWORD")
    {
        violations.push("mysql-fixture-provider-secret-in-argv".to_owned());
    }
    if provision_text
        .lines()
        .any(|line| line.contains("REVOKE") && line.contains("|| true"))
    {
        violations.push("mysql-fixture-revoke-failure-swallowed".to_owned());
    }
    let disarm = provision_text.rfind("trap - EXIT");
    let publish = provision_text.find("printf '%s\\n' \"$database\"");
    if !matches!((publish, disarm), (Some(publish), Some(disarm)) if publish < disarm) {
        violations.push("mysql-fixture-create-trap-disarmed-before-publish".to_owned());
    }
    for table in [
        "state_store_meta",
        "state_store_kv",
        "state_store_changes",
        "state_store_commits",
        "fixture_readiness",
        "ss3_probe_keys",
        "ss3_probe_snapshot",
        "ss3_probe_locks",
        "ss3_probe_key_3073",
    ] {
        if !provision_text.contains(table) {
            violations.push(format!("mysql-fixture-table-grant-missing: {table}"));
        }
    }
    for required in [
        "WORKSPACE_ROOT",
        "compose_project=",
        "project_running",
        "runtime is retained",
        "docker is required to stop",
        "--profile cleanup",
        "run --rm --no-deps runtime-cleaner",
        "failed to clean MySQL container-owned runtime data",
        "failed to remove MySQL cleanup project resources",
        "MySQL runtime data requires --docker cleanup",
        "failed to remove MySQL host runtime; current link and runtime are retained",
    ] {
        if !down_text.contains(required) {
            violations.push(format!("mysql-fixture-down-fail-open: {required}"));
        }
    }
    if !down_text.contains("run_with_timeout 30 rm -rf \"$runtime_dir\"") {
        violations.push("mysql-fixture-unbounded-runtime-cleanup".to_owned());
    }
    let first_down = down_text.find("down --remove-orphans");
    let cleaner = down_text.find("run --rm --no-deps runtime-cleaner");
    let final_down = down_text.rfind("down --remove-orphans");
    let unlink = down_text.find("rm -f \"$current_link\"");
    let host_cleanup = down_text.find("run_with_timeout 30 rm -rf \"$runtime_dir\"");
    if down_text.matches("down --remove-orphans").count() != 2
        || !matches!(
            (first_down, cleaner, final_down, unlink, host_cleanup),
            (Some(first_down), Some(cleaner), Some(final_down), Some(unlink), Some(host_cleanup))
                if first_down < cleaner
                    && cleaner < final_down
                    && final_down < host_cleanup
                    && host_cleanup < unlink
        )
    {
        violations.push("mysql-fixture-runtime-cleanup-order".to_owned());
    }
    if down_text.contains("sudo") {
        violations.push("mysql-fixture-runtime-cleanup-sudo".to_owned());
    }
    for required in [
        "file_mode",
        "stat --version",
        "stat -c '%a'",
        "stat -f '%Lp'",
        "MySQL runtime environment must have mode 600",
    ] {
        if !status_text.contains(required) {
            violations.push(format!("mysql-fixture-status-file-mode: {required}"));
        }
    }
    if status_text.contains("stat -f '%Lp' \"$exports_file\" 2>/dev/null || stat -c") {
        violations.push("mysql-fixture-status-mixed-stat-output".to_owned());
    }
    if up_text.contains("if docker image inspect")
        || !up_text.contains("run_with_timeout 15 docker image inspect")
    {
        violations.push("mysql-fixture-unbounded-image-inspect".to_owned());
    }
    for required in ["previous_database", "drop \"$previous_database\""] {
        if !up_text.contains(required) {
            violations.push(format!("mysql-fixture-readiness-leak: {required}"));
        }
    }
    if contract_text.contains("DO SLEEP(")
        || !contract_text.contains("wait_for_marker")
        || !contract_text.contains("reader_one_ready")
        || !contract_text.contains("reader_two_ready")
        || !contract_text.contains("deadlock_a_ready")
        || !contract_text.contains("deadlock_b_ready")
        || !contract_text.contains("lock_holder_ready")
        || !contract_text.contains("--unbuffered")
        || !contract_text.contains("cleanup_deadline")
    {
        violations.push("mysql-fixture-probe-missing-explicit-barrier".to_owned());
    }
    if contract_text.contains("deadlock_gate=")
        || contract_text.contains("GET_LOCK('${deadlock_gate}'")
        || !contract_text.contains("deadlock_a_gate")
        || !contract_text.contains("deadlock_b_gate")
    {
        violations.push("mysql-fixture-deadlock-shared-named-gate".to_owned());
    }
    violations.sort();
    violations.dedup();
    violations
}

#[test]
fn mysql_state_store_fixture_detector_rejects_security_and_lifecycle_bypasses() {
    let pinned_image = format!(
        "mysql:8.4.10@sha256:{}{}",
        "c831a0f11348d402b43d77453e17d770", "be2eef356615a2823fe0f5a0d6c8b9af"
    );
    let database_ddl = [
        ["CREATE", "DATABASE"].join(" "),
        ["DROP", "DATABASE"].join(" "),
    ];
    let sources = [
        GuardSource::new(
            "docker/mysql-state-store/compose.yml",
            format!("image: {pinned_image}"),
        ),
        GuardSource::new(
            "docker/mysql-state-store/probes/extra.sh",
            format!(
                "image={pinned_image}; {} x; {} x",
                database_ddl[0], database_ddl[1]
            ),
        ),
        GuardSource::new(
            "docker/mysql-state-store/provision-test-database.sh",
            "mysql_admin --execute=\"$NOVAROCKS_MYSQL_PASSWORD\"; \
             GRANT CREATE ON `${database}`.*; REVOKE x || true; \
             trap - EXIT; printf '%s\\n' \"$database\"",
        ),
        GuardSource::new(
            "docker/mysql-state-store/up.sh",
            "if docker image inspect x; then :; fi",
        ),
        GuardSource::new(
            "docker/mysql-state-store/down.sh",
            "rm -rf \"$runtime_dir\"",
        ),
        GuardSource::new(
            "docker/mysql-state-store/probes/contract.sh",
            "DO SLEEP(2); sleep 1",
        ),
    ];
    let violations = mysql_fixture_contract_violations(&sources);
    for expected in [
        "pinned-image-owner",
        "database-ddl-owner",
        "database-wide-grant",
        "secret-in-argv",
        "revoke-failure-swallowed",
        "trap-disarmed-before-publish",
        "down-fail-open",
        "unbounded-runtime-cleanup",
        "unbounded-image-inspect",
        "readiness-leak",
        "missing-explicit-barrier",
    ] {
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains(expected)),
            "synthetic MySQL fixture detector missed {expected}: {violations:?}"
        );
    }
}

#[test]
fn mysql_state_store_fixture_detector_rejects_shared_deadlock_named_gate() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let fixture = root.join("docker/mysql-state-store");
    let mut sources = Vec::new();
    collect_mysql_fixture_sources(&fixture, root, &mut sources);
    let mut shared_gate_sources = sources.clone();
    let contract = shared_gate_sources
        .iter_mut()
        .find(|source| source.path == "docker/mysql-state-store/probes/contract.sh")
        .expect("physical probe contract source");
    contract.text = "deadlock_gate=x; GET_LOCK('${deadlock_gate}'); \
        deadlock_a_ready; deadlock_b_ready; wait_for_marker; reader_one_ready; \
        reader_two_ready; lock_holder_ready; --unbuffered; cleanup_deadline"
        .to_owned();
    let violations = mysql_fixture_contract_violations(&shared_gate_sources);
    assert!(
        violations
            .iter()
            .any(|violation| violation.contains("deadlock-shared-named-gate")),
        "detector must reject a shared named gate in the current deadlock probe: {violations:?}"
    );
}

#[test]
fn mysql_state_store_fixture_detector_rejects_provisioner_boundary_bypasses() {
    let sources = [
        GuardSource::new(
            "docker/mysql-state-store/compose.yml",
            "NOVA_MYSQL_PROVISIONER_PASSWORD; /run/secrets/novarocks-mysql-provisioner.cnf",
        ),
        GuardSource::new(
            "docker/mysql-state-store/up.sh",
            "NOVA_MYSQL_PROVISIONER_PASSWORD; provisioner.cnf",
        ),
        GuardSource::new(
            "docker/mysql-state-store/provision-test-database.sh",
            "mysql_admin; NOVA_MYSQL_PROVISIONER_PASSWORD; \
             /run/secrets/novarocks-mysql-provisioner.cnf",
        ),
        GuardSource::new(
            "docker/mysql-state-store/status.sh",
            "mysql_admin; /run/secrets/novarocks-mysql-provisioner.cnf",
        ),
        GuardSource::new(
            "docker/mysql-state-store/down.sh",
            "NOVA_MYSQL_PROVISIONER_PASSWORD=cleanup-placeholder",
        ),
        GuardSource::new(
            "docker/mysql-state-store/probes/contract.sh",
            "mysql_admin; /run/secrets/novarocks-mysql-provisioner.cnf",
        ),
        GuardSource::new(
            "docker/mysql-state-store/probes/helper.sh",
            "NOVA_MYSQL_PROVISIONER_USERNAME",
        ),
    ];
    let violations = mysql_fixture_contract_violations(&sources);
    for forbidden_owner in [
        "docker/mysql-state-store/status.sh",
        "docker/mysql-state-store/down.sh",
        "docker/mysql-state-store/probes/contract.sh",
        "docker/mysql-state-store/probes/helper.sh",
    ] {
        assert!(
            violations.iter().any(|violation| {
                violation.contains("provisioner-boundary") && violation.contains(forbidden_owner)
            }),
            "detector must reject provisioner access in {forbidden_owner}: {violations:?}"
        );
    }
}

#[test]
fn mysql_state_store_fixture_detector_rejects_each_missing_required_owner() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let fixture = root.join("docker/mysql-state-store");
    let mut sources = Vec::new();
    collect_mysql_fixture_sources(&fixture, root, &mut sources);
    for required in [
        "docker/mysql-state-store/compose.yml",
        "docker/mysql-state-store/up.sh",
        "docker/mysql-state-store/status.sh",
        "docker/mysql-state-store/down.sh",
        "docker/mysql-state-store/provision-test-database.sh",
        "docker/mysql-state-store/README.md",
        "docker/mysql-state-store/probes/schema.sql",
        "docker/mysql-state-store/probes/contract.sh",
    ] {
        let without_required = sources
            .iter()
            .filter(|source| source.path != required)
            .cloned()
            .collect::<Vec<_>>();
        let violations = mysql_fixture_contract_violations(&without_required);
        assert!(
            violations.iter().any(|violation| {
                violation.contains("required-owner-missing") && violation.contains(required)
            }),
            "detector must reject missing required owner {required}: {violations:?}"
        );
    }
}

#[test]
fn mysql_state_store_down_preserves_runtime_until_project_cleanup_is_confirmed() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let fixture_source = root.join("docker/mysql-state-store");
    for (
        mode,
        prepare_runtime,
        stop_docker,
        expect_success,
        expect_runtime,
        expected_down_calls,
        expect_cleaner,
    ) in [
        ("pre-prepare", false, true, true, false, 1, false),
        ("running", true, false, true, true, 0, false),
        ("stopped-with-data", true, false, true, true, 0, false),
        ("inspect-fail", true, false, false, true, 0, false),
        ("first-down-fail", true, true, false, true, 1, false),
        ("cleaner-fail", true, true, false, true, 1, true),
        ("final-down-fail", true, true, false, true, 2, true),
        ("host-rm-fail", true, true, false, true, 2, true),
        ("down-success", true, true, true, false, 2, true),
    ] {
        let temp = tempfile::tempdir().expect("create fake MySQL fixture workspace");
        let workspace = fs::canonicalize(temp.path()).expect("canonical fake workspace");
        let fixture = workspace.join("docker/mysql-state-store");
        let fake_bin = workspace.join("fake-bin");
        fs::create_dir_all(&fixture).expect("create fake fixture");
        fs::create_dir_all(&fake_bin).expect("create fake bin");
        fs::copy(fixture_source.join("down.sh"), fixture.join("down.sh"))
            .expect("copy down script");
        fs::copy(
            fixture_source.join("compose.yml"),
            fixture.join("compose.yml"),
        )
        .expect("copy compose file");

        let fake_docker = fake_bin.join("docker");
        fs::write(
            &fake_docker,
            r#"#!/bin/sh
printf '%s\n' "$*" >> "$FAKE_DOCKER_LOG"
case "$FAKE_DOCKER_MODE:$*" in
  running:*" ps --all --quiet mysql")
    printf 'fake-container-id\n'
    ;;
  inspect-fail:*" ps --all --quiet mysql")
    exit 23
    ;;
  first-down-fail:*" ps --all --quiet mysql"|cleaner-fail:*" ps --all --quiet mysql"|final-down-fail:*" ps --all --quiet mysql"|host-rm-fail:*" ps --all --quiet mysql"|down-success:*" ps --all --quiet mysql")
    printf 'fake-container-id\n'
    ;;
  first-down-fail:*" down --remove-orphans")
    exit 24
    ;;
  cleaner-fail:*" run --rm --no-deps runtime-cleaner")
    exit 25
    ;;
  final-down-fail:*" down --remove-orphans")
    if [ "$(grep -c ' down --remove-orphans$' "$FAKE_DOCKER_LOG")" -ge 2 ]; then
      exit 26
    fi
    ;;
esac
"#,
        )
        .expect("write fake docker");
        fs::set_permissions(&fake_docker, fs::Permissions::from_mode(0o755))
            .expect("make fake docker executable");

        let fake_rm = fake_bin.join("rm");
        fs::write(
            &fake_rm,
            r#"#!/bin/sh
if [ "$FAKE_DOCKER_MODE" = host-rm-fail ] && [ "$1" = -rf ] && [ "$2" = "$FAKE_RUNTIME_DIR" ]; then
  exit 27
fi
exec /bin/rm "$@"
"#,
        )
        .expect("write fake rm");
        fs::set_permissions(&fake_rm, fs::Permissions::from_mode(0o755))
            .expect("make fake rm executable");

        let workspace_hash = hex::encode(Sha256::digest(workspace.to_string_lossy().as_bytes()));
        let env_id = format!("nr-mysql-{}", &workspace_hash[..12]);
        let runtime_base = fixture.join("runtime");
        let runtime_dir = runtime_base.join(&env_id);
        let current_link = runtime_base.join("current");
        if prepare_runtime {
            fs::create_dir_all(runtime_dir.join("data")).expect("create fake runtime data");
            fs::write(runtime_dir.join("sentinel"), mode).expect("write runtime sentinel");
            symlink(&env_id, &current_link).expect("link fake current runtime");
        }

        let docker_log = workspace.join("docker.log");
        let mut command = Command::new("/bin/bash");
        command.arg(fixture.join("down.sh"));
        if stop_docker {
            command.arg("--docker");
        }
        let path = format!(
            "{}:{}",
            fake_bin.display(),
            std::env::var("PATH").unwrap_or_default()
        );
        let output = command
            .env("PATH", path)
            .env("FAKE_DOCKER_MODE", mode)
            .env("FAKE_DOCKER_LOG", &docker_log)
            .env("FAKE_RUNTIME_DIR", &runtime_dir)
            .env("NOVAROCKS_WORKSPACE_ROOT", &workspace)
            .output()
            .expect("run copied down script");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(
            output.status.success(),
            expect_success,
            "unexpected down result for {mode}: {stderr}"
        );
        assert_eq!(
            runtime_dir.exists(),
            expect_runtime,
            "unexpected runtime state for {mode}: {stderr}"
        );
        assert_eq!(
            fs::symlink_metadata(&current_link).is_ok(),
            expect_runtime,
            "unexpected current-link state for {mode}: {stderr}"
        );
        let docker_calls = fs::read_to_string(&docker_log).expect("read fake docker calls");
        assert!(
            docker_calls.contains(" ps --all --quiet mysql"),
            "down must inspect the derived project for {mode}: {docker_calls}"
        );
        let calls = docker_calls.lines().collect::<Vec<_>>();
        let down_positions = calls
            .iter()
            .enumerate()
            .filter_map(|(index, call)| call.ends_with(" down --remove-orphans").then_some(index))
            .collect::<Vec<_>>();
        let cleaner_positions = calls
            .iter()
            .enumerate()
            .filter_map(|(index, call)| {
                call.ends_with(" run --rm --no-deps runtime-cleaner")
                    .then_some(index)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            down_positions.len(),
            expected_down_calls,
            "unexpected Compose down calls for {mode}: {docker_calls}"
        );
        assert_eq!(
            cleaner_positions.len(),
            usize::from(expect_cleaner),
            "unexpected runtime-cleaner calls for {mode}: {docker_calls}"
        );
        if expect_cleaner {
            assert!(
                down_positions[0] < cleaner_positions[0],
                "runtime cleaner must run after the first Compose down for {mode}: {docker_calls}"
            );
        }
        if expected_down_calls == 2 {
            assert!(
                cleaner_positions[0] < down_positions[1],
                "final Compose down must run after the runtime cleaner for {mode}: {docker_calls}"
            );
        }
    }
}

#[test]
fn mysql_state_store_status_accepts_mode_0600_with_gnu_stat() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let fixture_source = root.join("docker/mysql-state-store");
    let temp = tempfile::tempdir().expect("create GNU stat status workspace");
    let workspace = fs::canonicalize(temp.path()).expect("canonical GNU stat status workspace");
    let fixture = workspace.join("docker/mysql-state-store");
    let current = fixture.join("runtime/current");
    let fake_bin = workspace.join("fake-bin");
    fs::create_dir_all(&current).expect("create fake current runtime");
    fs::create_dir_all(&fake_bin).expect("create fake command directory");
    fs::copy(fixture_source.join("status.sh"), fixture.join("status.sh"))
        .expect("copy real status script");
    fs::set_permissions(fixture.join("status.sh"), fs::Permissions::from_mode(0o755))
        .expect("make copied status script executable");

    let exports_file = current.join("env.sh");
    fs::write(
        &exports_file,
        format!(
            "export NOVAROCKS_MYSQL_VERSION=8.4.10\n\
             export NOVA_MYSQL_ENV_ID=nr-mysql-gnu-stat\n\
             export NOVA_MYSQL_COMPOSE_PROJECT=nrss3gnustat\n\
             export NOVA_MYSQL_COMPOSE_ENV={}\n\
             export NOVA_MYSQL_COMPOSE_FILE={}\n\
             export NOVAROCKS_MYSQL_DATABASE=novarocks_ss3_gnu_stat\n\
             export NOVAROCKS_MYSQL_USERNAME=provider\n\
             export NOVAROCKS_MYSQL_PASSWORD_ENV=NOVAROCKS_MYSQL_PASSWORD\n",
            current.join("compose.env").display(),
            fixture.join("compose.yml").display(),
        ),
    )
    .expect("write fake status environment");
    fs::set_permissions(&exports_file, fs::Permissions::from_mode(0o600))
        .expect("set fake status environment mode");

    let fake_stat = fake_bin.join("stat");
    fs::write(
        &fake_stat,
        r#"#!/bin/sh
case "$1" in
  --version)
    printf 'stat (GNU coreutils) 9.4\n'
    ;;
  -c)
    printf '600\n'
    ;;
  -f)
    printf '  File: "%s"\n    ID: deadbeef Namelen: 255 Type: ext2/ext3\n' "$2"
    exit 1
    ;;
  *)
    exit 2
    ;;
esac
"#,
    )
    .expect("write fake GNU stat");
    fs::set_permissions(&fake_stat, fs::Permissions::from_mode(0o755))
        .expect("make fake GNU stat executable");

    let fake_docker = fake_bin.join("docker");
    fs::write(
        &fake_docker,
        "#!/bin/sh\nprintf '8.4.10\\t16384\\tInnoDB\\t+00:00\\tSTRICT_TRANS_TABLES\\n'\n",
    )
    .expect("write fake Docker readiness command");
    fs::set_permissions(&fake_docker, fs::Permissions::from_mode(0o755))
        .expect("make fake Docker executable");

    let path = format!(
        "{}:{}",
        fake_bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let output = Command::new(fixture.join("status.sh"))
        .arg("--self-check")
        .env("PATH", path)
        .output()
        .expect("run real status script with GNU stat behavior");
    assert!(
        output.status.success(),
        "status --self-check must accept a mode-0600 file with GNU stat: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn mysql_state_store_down_removes_container_owned_runtime_data() {
    if !cfg!(target_os = "linux") {
        eprintln!(
            "SKIP: Docker Desktop does not preserve Linux container ownership on host bind mounts"
        );
        return;
    }
    if std::env::var_os("NOVAROCKS_RUN_MYSQL_DOCKER_OWNERSHIP_TEST").is_none() {
        eprintln!("SKIP: set NOVAROCKS_RUN_MYSQL_DOCKER_OWNERSHIP_TEST=1 on Linux with Docker");
        return;
    }

    let docker_ready = Command::new("docker")
        .args(["compose", "version"])
        .output()
        .expect("Docker is required for the opted-in MySQL ownership integration test");
    assert!(
        docker_ready.status.success(),
        "Docker Compose is required for the opted-in MySQL ownership integration test: {}",
        String::from_utf8_lossy(&docker_ready.stderr)
    );

    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let fixture_source = root.join("docker/mysql-state-store");
    let temp = tempfile::tempdir().expect("create real MySQL fixture workspace");
    let workspace = fs::canonicalize(temp.path()).expect("canonical real fixture workspace");
    let fixture = workspace.join("docker/mysql-state-store");
    fs::create_dir_all(&fixture).expect("create real fixture directory");
    for owner in ["up.sh", "down.sh", "compose.yml"] {
        fs::copy(fixture_source.join(owner), fixture.join(owner))
            .unwrap_or_else(|error| panic!("copy real fixture owner {owner}: {error}"));
    }
    for script in ["up.sh", "down.sh"] {
        fs::set_permissions(fixture.join(script), fs::Permissions::from_mode(0o755))
            .unwrap_or_else(|error| {
                panic!("make real fixture script {script} executable: {error}")
            });
    }

    let workspace_hash = hex::encode(Sha256::digest(workspace.to_string_lossy().as_bytes()));
    let env_id = format!("nr-mysql-{}", &workspace_hash[..12]);
    let compose_project = format!("nrss3{}", &workspace_hash[..12]);
    let runtime_base = fixture.join("runtime");
    let runtime_dir = runtime_base.join(&env_id);
    let current_link = runtime_base.join("current");
    let pre_prepare_down = Command::new("/bin/bash")
        .arg(fixture.join("down.sh"))
        .arg("--docker")
        .env("NOVAROCKS_WORKSPACE_ROOT", &workspace)
        .output()
        .expect("run real MySQL fixture down before prepare");
    assert!(
        pre_prepare_down.status.success(),
        "down --docker before prepare failed: {}",
        String::from_utf8_lossy(&pre_prepare_down.stderr)
    );
    assert!(
        fs::symlink_metadata(&runtime_base).is_err(),
        "down --docker before prepare must not create a bind-mount source"
    );
    for (resource, output) in [
        (
            "container",
            Command::new("docker")
                .args([
                    "ps",
                    "--all",
                    "--quiet",
                    "--filter",
                    &format!("label=com.docker.compose.project={compose_project}"),
                ])
                .output()
                .expect("inspect pre-prepare container residue"),
        ),
        (
            "network",
            Command::new("docker")
                .args([
                    "network",
                    "ls",
                    "--quiet",
                    "--filter",
                    &format!("label=com.docker.compose.project={compose_project}"),
                ])
                .output()
                .expect("inspect pre-prepare network residue"),
        ),
    ] {
        assert!(
            output.status.success() && output.stdout.is_empty(),
            "down --docker before prepare retained {resource} residue: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let prepare = Command::new("/bin/bash")
        .arg(fixture.join("up.sh"))
        .arg("--prepare-only")
        .env("NOVAROCKS_WORKSPACE_ROOT", &workspace)
        .output()
        .expect("prepare real MySQL fixture runtime");
    assert!(
        prepare.status.success(),
        "prepare-only failed: {}",
        String::from_utf8_lossy(&prepare.stderr)
    );

    let data_dir = runtime_dir.join("data");
    let compose_file = fixture.join("compose.yml");
    let compose_env = runtime_dir.join("compose.env");
    let cleanup_compose_env = workspace.join("cleanup-compose.env");
    fs::copy(&compose_env, &cleanup_compose_env)
        .expect("preserve Compose environment outside the runtime under test");
    let image = fs::read_to_string(&compose_file)
        .expect("read copied MySQL compose owner")
        .lines()
        .find_map(|line| line.trim().strip_prefix("image: "))
        .expect("copied MySQL compose owner must declare its pinned image")
        .to_owned();

    let inspected = Command::new("docker")
        .args(["image", "inspect", &image])
        .output()
        .expect("inspect pinned MySQL image");
    if !inspected.status.success() {
        let pulled = Command::new("docker")
            .args(["pull", &image])
            .output()
            .expect("pull pinned MySQL image");
        assert!(
            pulled.status.success(),
            "pull pinned MySQL image failed: {}",
            String::from_utf8_lossy(&pulled.stderr)
        );
    }

    let mount = format!("{}:/fixture", data_dir.display());
    let create_owned = Command::new("docker")
        .args([
            "run",
            "--rm",
            "--network",
            "none",
            "--entrypoint",
            "/bin/sh",
            "--volume",
            &mount,
            &image,
            "-c",
            "mkdir -p /fixture/owned && printf owned > /fixture/owned/sentinel && chown -R 0:0 /fixture && chmod 0700 /fixture /fixture/owned /fixture/owned/sentinel",
        ])
        .output()
        .expect("create container-owned MySQL runtime data");
    assert!(
        create_owned.status.success(),
        "create container-owned MySQL runtime data failed: {}",
        String::from_utf8_lossy(&create_owned.stderr)
    );
    let owned_metadata = fs::symlink_metadata(&data_dir)
        .expect("container-owned MySQL data directory must remain visible to the host");
    assert_eq!(
        owned_metadata.uid(),
        0,
        "test precondition requires root ownership"
    );
    assert_eq!(
        owned_metadata.mode() & 0o777,
        0o700,
        "test precondition requires a host-inaccessible data directory"
    );

    let down = Command::new("/bin/bash")
        .arg(fixture.join("down.sh"))
        .arg("--docker")
        .env("NOVAROCKS_WORKSPACE_ROOT", &workspace)
        .output()
        .expect("run real MySQL fixture down script");
    let runtime_present = fs::symlink_metadata(&runtime_dir).is_ok();
    let current_present = fs::symlink_metadata(&current_link).is_ok();
    let data_present = fs::symlink_metadata(&data_dir).is_ok();
    let container_residue = Command::new("docker")
        .args([
            "ps",
            "--all",
            "--quiet",
            "--filter",
            &format!("label=com.docker.compose.project={compose_project}"),
        ])
        .output()
        .expect("inspect MySQL Compose container residue");
    let network_residue = Command::new("docker")
        .args([
            "network",
            "ls",
            "--quiet",
            "--filter",
            &format!("label=com.docker.compose.project={compose_project}"),
        ])
        .output()
        .expect("inspect MySQL Compose network residue");
    let project_residue =
        !container_residue.stdout.is_empty() || !network_residue.stdout.is_empty();

    if data_present {
        let owner = fs::metadata(&workspace).expect("read host workspace owner");
        let cleanup_owned = Command::new("docker")
            .args([
                "run",
                "--rm",
                "--network",
                "none",
                "--entrypoint",
                "/bin/sh",
                "--volume",
                &mount,
                "--env",
                &format!("HOST_UID={}", owner.uid()),
                "--env",
                &format!("HOST_GID={}", owner.gid()),
                &image,
                "-c",
                "rm -rf /fixture/owned && chown \"$HOST_UID:$HOST_GID\" /fixture && chmod 0700 /fixture",
            ])
            .output()
            .expect("clean container-owned MySQL runtime data");
        assert!(
            cleanup_owned.status.success(),
            "clean container-owned MySQL runtime data failed: {}",
            String::from_utf8_lossy(&cleanup_owned.stderr)
        );
    }
    if project_residue {
        let cleanup_project = Command::new("docker")
            .args([
                "compose",
                "--env-file",
                cleanup_compose_env
                    .to_str()
                    .expect("UTF-8 external cleanup Compose env path"),
                "-p",
                &compose_project,
                "-f",
                compose_file.to_str().expect("UTF-8 compose file path"),
                "down",
                "--remove-orphans",
            ])
            .output()
            .expect("clean real MySQL fixture Compose project");
        assert!(
            cleanup_project.status.success(),
            "clean real MySQL fixture Compose project failed: {}",
            String::from_utf8_lossy(&cleanup_project.stderr)
        );
    }
    if fs::symlink_metadata(&current_link).is_ok() {
        fs::remove_file(&current_link).expect("remove retained current runtime link after capture");
    }
    if fs::symlink_metadata(&runtime_dir).is_ok() {
        fs::remove_dir_all(&runtime_dir).expect("remove retained runtime after ownership cleanup");
    }

    let stderr = String::from_utf8_lossy(&down.stderr);
    assert!(
        container_residue.status.success(),
        "inspect container residue: {}",
        String::from_utf8_lossy(&container_residue.stderr)
    );
    assert!(
        network_residue.status.success(),
        "inspect network residue: {}",
        String::from_utf8_lossy(&network_residue.stderr)
    );
    assert!(
        down.status.success(),
        "down --docker must remove container-owned runtime data: {stderr}"
    );
    assert!(
        !runtime_present,
        "down --docker retained the MySQL runtime directory: {stderr}"
    );
    assert!(
        !current_present,
        "down --docker retained the MySQL current runtime link: {stderr}"
    );
    assert!(
        !project_residue,
        "down --docker retained MySQL Compose project containers or networks"
    );
}

#[test]
fn mysql_state_store_fixture_is_pinned_isolated_and_fail_closed() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let fixture = root.join("docker/mysql-state-store");
    assert!(fixture.is_dir(), "missing MySQL state-store fixture root");
    let mut sources = Vec::new();
    collect_mysql_fixture_sources(&fixture, root, &mut sources);
    let violations = mysql_fixture_contract_violations(&sources);
    assert!(
        violations.is_empty(),
        "MySQL fixture architecture failed:\n{}",
        violations.join("\n")
    );

    let combined = sources
        .iter()
        .map(|source| source.text.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    for expected in [
        "NOVA_MYSQL_ENV_ID",
        "NOVA_MYSQL_COMPOSE_PROJECT",
        "NOVA_MYSQL_COMPOSE_FILE",
        "NOVA_MYSQL_RUNTIME_DIR",
        "NOVAROCKS_MYSQL_HOST",
        "NOVAROCKS_MYSQL_PORT",
        "NOVAROCKS_MYSQL_DATABASE",
        "NOVAROCKS_MYSQL_USERNAME",
        "NOVAROCKS_MYSQL_PASSWORD_ENV",
        "NOVA_MYSQL_PROVISIONER_USERNAME",
        "NOVAROCKS_MYSQL_VERSION",
        "NOVAROCKS_MYSQL_IMAGE",
        "chmod 600",
        "--prepare-only",
        "--docker",
        "runtime-cleaner",
        "network_mode: none",
        "run --rm --no-deps runtime-cleaner",
        "SELECT VERSION()",
        "@@innodb_page_size",
        "@@default_storage_engine",
        "@@session.time_zone",
        "@@session.sql_mode",
        "STRICT_TRANS_TABLES",
        "16384",
        "8.4.10",
        "create <case-id>",
        "drop <database-name>",
        "novarocks_ss3_",
    ] {
        assert!(
            combined.contains(expected),
            "MySQL fixture contract is missing {expected}"
        );
    }
}

#[test]
fn mysql_state_store_physical_probe_inventory_is_exact() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let fixture = root.join("docker/mysql-state-store");
    let schema_path = fixture.join("probes/schema.sql");
    let contract_path = fixture.join("probes/contract.sh");
    assert!(
        schema_path.is_file(),
        "missing MySQL physical probe schema owner: {}",
        schema_path.display()
    );
    assert!(
        contract_path.is_file(),
        "missing MySQL physical probe contract owner: {}",
        contract_path.display()
    );
    let schema = fs::read_to_string(schema_path).expect("read MySQL physical probe schema");
    let contract = fs::read_to_string(contract_path).expect("read MySQL physical probe contract");
    let markers = [
        "SS3_MYSQL_PROBE_PRIVILEGE_SEPARATION_PASS",
        "SS3_MYSQL_PROBE_KEY_3072_PASS",
        "SS3_MYSQL_PROBE_KEY_3073_ERROR_1071_PASS",
        "SS3_MYSQL_PROBE_BINARY_ORDER_PASS",
        "SS3_MYSQL_PROBE_RANGE_FORWARD_REVERSE_PASS",
        "SS3_MYSQL_PROBE_PRIMARY_RANGE_EXPLAIN_PASS",
        "SS3_MYSQL_PROBE_RR_SNAPSHOT_PASS",
        "SS3_MYSQL_PROBE_RR_DUAL_NONLOCKING_READERS_PASS",
        "SS3_MYSQL_PROBE_DEADLOCK_1213_PASS",
        "SS3_MYSQL_PROBE_LOCK_TIMEOUT_1205_ROLLBACK_PASS",
        "SS3_MYSQL_PROBE_SESSION_RESET_PASS",
    ];
    for marker in markers {
        assert_eq!(
            contract.matches(marker).count(),
            1,
            "physical probe marker must have exactly one contract owner: {marker}"
        );
    }
    for expected in [
        "VARBINARY(3072)",
        "ROW_FORMAT=DYNAMIC",
        "ss3_probe_keys",
        "ss3_probe_snapshot",
        "ss3_probe_locks",
        "prior_write_visible_after_timeout=7",
        "value_after_explicit_rollback=0",
        "GET_LOCK(",
        "KILL CONNECTION",
        "--unbuffered",
    ] {
        assert!(
            schema.contains(expected) || contract.contains(expected),
            "physical probe owners are missing {expected}"
        );
    }

    let provision = fs::read_to_string(fixture.join("provision-test-database.sh"))
        .expect("read MySQL database provisioner");
    assert!(
        provision.contains("create <case-id>") && provision.contains("drop <database-name>"),
        "physical probes must use the frozen unique-database provisioner"
    );
    assert!(
        contract.contains("NOVAROCKS_MYSQL_DATABASE")
            && contract.contains("novarocks_ss3_")
            && !contract.contains("runtime-readiness"),
        "physical probes must require a unique provisioned database, never the shared readiness database"
    );
    for expected in [
        "requested_database=\"${NOVAROCKS_MYSQL_DATABASE:-}\"",
        "readiness_database=\"$NOVAROCKS_MYSQL_DATABASE\"",
        "NOVAROCKS_MYSQL_DATABASE=\"$requested_database\"",
        "must not use the shared readiness database",
    ] {
        assert!(
            contract.contains(expected),
            "physical probes must preserve and validate the caller database override: {expected}"
        );
    }
    assert!(
        !contract.contains("NOVA_MYSQL_PROVISIONER_PASSWORD"),
        "physical probes must not receive the provisioner credential"
    );
    assert!(
        contract.contains("--commands") && contract.contains("'\\x'"),
        "session reset must use the MySQL client resetconnection protocol command"
    );
    assert!(
        contract.contains("grep -F 'reset=+00:00:'"),
        "session reset must assert the literal UTC reset prefix"
    );
    assert!(
        !contract.contains("RESET CONNECTION;"),
        "RESET CONNECTION is not valid MySQL 8.4 SQL"
    );
}

#[test]
fn state_store_mysql_dependency_contract() {
    let manifest_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let manifest_text = fs::read_to_string(&manifest_path).expect("read Cargo.toml");
    let manifest: toml::Value = toml::from_str(&manifest_text).expect("parse Cargo.toml");
    let dependencies = manifest
        .get("dependencies")
        .and_then(toml::Value::as_table)
        .expect("[dependencies]");
    let mysql_async = dependencies
        .get("mysql_async")
        .and_then(toml::Value::as_table)
        .expect("mysql_async must be a structured optional dependency");
    let rustls = dependencies
        .get("rustls")
        .and_then(toml::Value::as_table)
        .expect("rustls must be a structured optional type dependency");

    assert_eq!(
        mysql_async.get("version").and_then(toml::Value::as_str),
        Some("=0.37.0")
    );
    assert_eq!(
        mysql_async.get("optional").and_then(toml::Value::as_bool),
        Some(true)
    );
    assert_eq!(
        mysql_async
            .get("default-features")
            .and_then(toml::Value::as_bool),
        Some(false)
    );
    assert_eq!(
        mysql_async
            .get("features")
            .and_then(toml::Value::as_array)
            .expect("mysql_async features")
            .iter()
            .map(|feature| feature.as_str().expect("string feature"))
            .collect::<Vec<_>>(),
        ["minimal-rust", "rustls-tls", "ring", "tls12"]
    );
    assert_eq!(
        rustls.get("version").and_then(toml::Value::as_str),
        Some("0.23")
    );
    assert_eq!(
        rustls.get("optional").and_then(toml::Value::as_bool),
        Some(true)
    );
    assert_eq!(
        rustls
            .get("default-features")
            .and_then(toml::Value::as_bool),
        Some(false)
    );

    let features = manifest
        .get("features")
        .and_then(toml::Value::as_table)
        .expect("[features]");
    assert_eq!(
        features
            .get("mysql-state-store-provider")
            .and_then(toml::Value::as_array)
            .expect("mysql-state-store-provider feature")
            .iter()
            .map(|feature| feature.as_str().expect("string feature"))
            .collect::<Vec<_>>(),
        ["dep:mysql_async", "dep:rustls"]
    );
    assert!(
        !dependencies.contains_key("mysql_common_037"),
        "state store must not add a direct mysql_common 0.37 dependency"
    );
    assert_eq!(
        dependencies.get("mysql").and_then(toml::Value::as_str),
        Some("25"),
        "the synchronous mysql dependency remains owned by JDBC scan"
    );
    assert_eq!(
        dependencies
            .get("mysql_common")
            .and_then(toml::Value::as_str),
        Some("0.32"),
        "the synchronous mysql_common dependency remains owned by JDBC scan"
    );
    let direct_mysql_common_aliases = dependencies
        .iter()
        .filter(|(name, dependency)| {
            name.as_str() != "mysql_common"
                && dependency
                    .as_table()
                    .and_then(|dependency| dependency.get("package"))
                    .and_then(toml::Value::as_str)
                    == Some("mysql_common")
        })
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>();
    assert!(
        direct_mysql_common_aliases.is_empty(),
        "state store must not add an aliased direct mysql_common dependency: {direct_mysql_common_aliases:?}"
    );
    for forbidden in ["tracing", "binlog"] {
        assert!(
            !mysql_async
                .get("features")
                .and_then(toml::Value::as_array)
                .is_some_and(|features| {
                    features
                        .iter()
                        .any(|feature| feature.as_str() == Some(forbidden))
                }),
            "mysql_async feature {forbidden} must stay disabled"
        );
    }

    let jdbc =
        fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/connector/jdbc.rs"))
            .expect("read JDBC owner");
    assert!(
        jdbc.contains("mysql::"),
        "the synchronous mysql driver must remain in the JDBC owner"
    );
    assert!(
        !jdbc.contains("mysql_async"),
        "the async state-store driver must not leak into JDBC"
    );
}

#[test]
fn state_store_mysql_boundary_rejects_native_and_jdbc_leaks() {
    let fixtures = [
        GuardSource::new(
            "src/state_store/config.rs",
            "use mysql_async::Pool; fn leak(_: mysql_async::Result<()>) {}",
        ),
        GuardSource::new(
            "src/state_store/mysql/mod.rs",
            "use mysql::Pool; use mysql_common::value::Value;",
        ),
        GuardSource::new(
            "src/state_store/mysql/txn.rs",
            "use crate::connector::jdbc::JdbcScanConfig;",
        ),
        GuardSource::new(
            "src/state_store/runtime.rs",
            "use mysql_async::Conn; const DEADLOCK: u16 = 1213; \
             const SQL: &str = \"START TRANSACTION\";",
        ),
        GuardSource::new(
            "src/state_store/config.rs",
            "const DEADLOCK: u16 = 1213; const SQL: &str = \"START TRANSACTION\";",
        ),
        GuardSource::new(
            "src/meta/state_store.rs",
            "const SQL: &str = \"CREATE TABLE state_store_kv (key_bytes VARBINARY(3072))\";",
        ),
    ];
    let violations = state_store_mysql_boundary_violations(&fixtures);

    for expected in [
        "mysql_async",
        "mysql_common",
        "JdbcScanConfig",
        "1213",
        "START TRANSACTION",
        "state_store_kv",
    ] {
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains(expected)),
            "MySQL boundary detector missed {expected}: {violations:?}"
        );
    }
    assert!(
        violations.iter().any(|violation| {
            violation == "state-store-mysql-jdbc-leak: src/state_store/mysql/mod.rs -> mysql"
        }),
        "MySQL owner must reject the synchronous mysql crate: {violations:?}"
    );
    for expected in ["mysql_async", "1213", "START TRANSACTION"] {
        assert!(
            violations.iter().any(|violation| {
                violation.contains("src/state_store/runtime.rs") && violation.contains(expected)
            }),
            "runtime must not own MySQL driver/error/transaction behavior for {expected}: {violations:?}"
        );
    }

    let allowed = [
        GuardSource::new(
            "src/state_store/mysql/mod.rs",
            "use mysql_async::{Pool, Row}; const CODE: u16 = 1213; \
             const SQL: &str = \"START TRANSACTION; CREATE TABLE state_store_kv (key_bytes VARBINARY(3072))\";",
        ),
        GuardSource::new(
            "src/state_store/runtime.rs",
            "use super::mysql::{MySqlProviderHandle, MySqlRuntime}; \
             fn compose(_: MySqlRuntime, _: MySqlProviderHandle) {}",
        ),
        GuardSource::new(
            "src/state_store/mysql/helper_protocol.rs",
            "use crate::state_store::StateStoreRuntime; \
             fn boot(config: MySqlClientConfig) { let _ = StateStoreRuntime::mysql(config); }",
        ),
        GuardSource::new(
            "src/state_store/config.rs",
            "enum StateStoreProviderConfig { Mysql { database: String } } \
             struct MySqlClientConfig { host: String }",
        ),
        GuardSource::new(
            "src/connector/unrelated.rs",
            "const YEAR: u16 = 2006; const SQL: &str = \"SELECT id FROM work FOR UPDATE\";",
        ),
        GuardSource::new(
            "src/engine/unrelated.rs",
            "const YEAR: u16 = 2013; const SQL: &str = \"START TRANSACTION\";",
        ),
    ];
    let allowed_violations = state_store_mysql_boundary_violations(&allowed);
    assert!(
        allowed_violations.is_empty(),
        "narrow MySQL owners and provider-neutral config must remain allowed: {allowed_violations:?}"
    );

    let src = src_dir();
    let files = production_rs_files_from_entries(&src, &[src.join("lib.rs"), src.join("main.rs")]);
    assert!(
        !files.is_empty(),
        "MySQL boundary source scan must be non-vacuous"
    );
    let sources = files
        .iter()
        .map(|path| {
            GuardSource::new(
                rel(path),
                fs::read_to_string(path).expect("read production source"),
            )
        })
        .collect::<Vec<_>>();
    let violations = state_store_mysql_boundary_violations(&sources);
    assert!(
        violations.is_empty(),
        "MySQL architecture boundary failed:\n{}",
        violations.join("\n")
    );
}

#[test]
fn state_store_foundationdb_runtime_rejects_mysql_provider_variant() {
    let runtime = src_dir().join("state_store/runtime.rs");
    let text = fs::read_to_string(&runtime).expect("read state store runtime");
    let tokens = rust_use_tokens(&rust_sanitized_production_text(&text));
    assert!(
        tokens.windows(3).any(|tokens| {
            tokens[0] == "StateStoreProviderConfig" && tokens[1] == "::" && tokens[2] == "Mysql"
        }),
        "FoundationDB runtime composition must explicitly reject the feature-independent Mysql provider variant"
    );
    assert!(
        rust_string_literals(&text)
            .iter()
            .any(|literal| { literal == "FoundationDB runtime cannot open a MySQL state store" }),
        "FoundationDB runtime must return a typed provider-mismatch error for Mysql config"
    );
}

#[test]
fn state_store_mysql_provider_variant_is_feature_independent() {
    let cfg_gated = r#"
enum StateStoreProviderConfig {
    #[cfg(feature = "mysql-state-store-provider")]
    Mysql { database: String },
}
"#;
    let detector = mysql_provider_variant_cfg_violations("fixture.rs", cfg_gated);
    assert!(
        detector.iter().any(|violation| violation.contains("Mysql")),
        "feature-gated MySQL provider variant must be rejected: {detector:?}"
    );

    let config = src_dir().join("state_store/config.rs");
    let text = fs::read_to_string(&config).expect("read state store config");
    let violations = mysql_provider_variant_cfg_violations(&rel(&config), &text);
    assert!(
        violations.is_empty(),
        "production MySQL provider variant must be feature-independent: {violations:?}"
    );
    assert!(
        text.contains("Mysql {"),
        "feature-independent MySQL provider vocabulary must be non-vacuous"
    );
}

#[test]
fn mysql_state_store_workflow_covers_gate_owners_and_exact_command_order() {
    let source_root = src_dir();
    let root = source_root.parent().expect("workspace root");
    let workflow_path = root.join(".github/workflows/mysql-state-store.yml");
    let gate_path = root.join("tools/ci/mysql-state-store-provider.sh");
    assert!(
        workflow_path.is_file(),
        "missing dedicated MySQL state-store workflow owner: {}",
        workflow_path.display()
    );
    assert!(
        gate_path.is_file(),
        "missing dedicated MySQL state-store production gate owner: {}",
        gate_path.display()
    );

    let workflow = fs::read_to_string(&workflow_path).expect("read MySQL state-store workflow");
    let gate = fs::read_to_string(&gate_path).expect("read MySQL state-store production gate");
    let jobs = workflow
        .split_once("\njobs:\n")
        .map(|(_, jobs)| jobs)
        .expect("MySQL workflow must have a jobs mapping");
    let job_owners = jobs
        .lines()
        .filter(|line| {
            line.starts_with("  ") && !line.starts_with("    ") && line.trim_end().ends_with(':')
        })
        .collect::<Vec<_>>();
    assert_eq!(
        job_owners,
        ["  mysql-state-store:"],
        "the MySQL production workflow must have exactly one job"
    );
    assert_eq!(
        workflow.matches("runs-on: ubuntu-24.04").count(),
        1,
        "the unique MySQL production gate must run on Linux x86_64"
    );

    let production_step = r#"      - name: Run MySQL state-store production gate
        env:
          NOVAROCKS_RUN_MYSQL_DOCKER_OWNERSHIP_TEST: "1"
        run: |
          set -euo pipefail
          trap 'docker/mysql-state-store/down.sh --docker' EXIT
          docker/mysql-state-store/up.sh
          source docker/mysql-state-store/runtime/current/env.sh
          tools/ci/mysql-state-store-provider.sh"#;
    assert_eq!(
        workflow.matches(production_step).count(),
        1,
        "the workflow must own one strict trap -> up -> source -> gate production step"
    );
    assert_eq!(
        workflow
            .matches("NOVAROCKS_RUN_MYSQL_DOCKER_OWNERSHIP_TEST: \"1\"")
            .count(),
        1,
        "the unique production step must opt into the Linux ownership regression exactly once"
    );
    assert!(
        !workflow.contains("TDD RED"),
        "the temporary isolated RED workflow step must be removed after GREEN"
    );
    assert_eq!(
        workflow.matches("if: always()").count(),
        1,
        "the workflow must always run exactly one runtime-only residue check"
    );
    assert_eq!(
        workflow
            .matches("test ! -e docker/mysql-state-store/runtime/current")
            .count(),
        1,
        "the always step must fail on retained runtime residue"
    );
    assert_eq!(
        workflow
            .matches("docker/mysql-state-store/down.sh --docker")
            .count(),
        1,
        "the workflow must not run a second stop that can mask a gate failure"
    );
    assert!(
        !workflow.contains("foundationdb") && !workflow.contains("FoundationDB"),
        "the MySQL workflow must not install or start FoundationDB"
    );

    for owner in [
        ".github/workflows/mysql-state-store.yml",
        "Cargo.lock",
        "Cargo.toml",
        "docker/mysql-state-store/**",
        "novarocks.toml.example",
        "src/common/app_config.rs",
        "src/state_store/**",
        "tests/state_store_boundary.rs",
        "tests/cluster_mvp.rs",
        "tests/common/state_store_conformance.rs",
        "tests/state_store_contract.rs",
        "tests/state_store_mysql.rs",
        "tests/state_store_mysql_cross_process.rs",
        "tests/state_store_mysql_runtime.rs",
        "tests/state_store_sqlite.rs",
        "tests/support/state_store_mysql_helper.rs",
        "tools/ci/mysql-state-store-provider.sh",
    ] {
        let trigger = format!("      - \"{owner}\"");
        assert_eq!(
            workflow.lines().filter(|line| *line == trigger).count(),
            1,
            "MySQL gate owner `{owner}` must trigger the unique production job exactly once"
        );
    }

    let mut fixture_sources = Vec::new();
    collect_mysql_fixture_sources(
        &root.join("docker/mysql-state-store"),
        root,
        &mut fixture_sources,
    );
    let pinned_image = format!(
        "mysql:8.4.10@sha256:{}{}",
        "c831a0f11348d402b43d77453e17d770", "be2eef356615a2823fe0f5a0d6c8b9af"
    );
    let image_owners = fixture_sources
        .iter()
        .filter(|source| source.text.contains(&pinned_image))
        .map(|source| source.path.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        image_owners,
        ["docker/mysql-state-store/compose.yml"],
        "the fixture compose file must remain the only pinned image owner"
    );
    assert!(
        !workflow.contains("mysql:8.4.10")
            && !workflow.contains("c831a0f11348d402b43d77453e17d770")
            && !gate.contains("mysql:8.4.10")
            && !gate.contains("c831a0f11348d402b43d77453e17d770"),
        "the workflow and gate must consume fixture version/digest instead of copying them"
    );
    for owner in [(&workflow, "workflow"), (&gate, "production gate")] {
        assert!(
            !owner.0.contains("NOVA_MYSQL_PROVISIONER_PASSWORD")
                && !owner.0.contains("NOVA_MYSQL_PROVISIONER_USERNAME"),
            "the MySQL {} must not own provisioner credentials",
            owner.1
        );
    }

    let logical_gate = gate
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect::<Vec<_>>()
        .join("\n")
        .replace("\\\n", "");
    let expected_commands = [
        "cargo fmt --all -- --check",
        "cargo test --lib state_store",
        "cargo test --test state_store_contract",
        "cargo test --test state_store_sqlite",
        "cargo test --test state_store_contract -- --list | awk '$1 == \"foundationdb_config_feature_off_open_fails_without_fallback:\" { n++ } END { exit(n != 1) }'",
        "cargo test --test state_store_contract foundationdb_config_feature_off_open_fails_without_fallback -- --exact",
        "cargo test --test state_store_boundary -- --list | awk '$1 == \"state_store_boundary_detector_rejects_foundationdb_owner_domain_and_forbidden_apis:\" { n++ } END { exit(n != 1) }'",
        "cargo test --test state_store_boundary state_store_boundary_detector_rejects_foundationdb_owner_domain_and_forbidden_apis -- --exact",
        "cargo check --no-default-features",
        "if cargo tree -e features --no-default-features | rg -q 'mysql_async|mysql_common v0\\.37'; then",
        "cargo test --test state_store_boundary state_store",
        "cargo build --profile dev-opt",
        "PROBE_DB=\"$(docker/mysql-state-store/provision-test-database.sh create production-gate-probes)\"",
        "docker/mysql-state-store/probes/contract.sh",
        "cargo test --features mysql-state-store-provider --test state_store_mysql_runtime -- --nocapture --test-threads=1",
        "cargo test --features mysql-state-store-provider,state-store-test-hooks --test state_store_mysql -- --list | awk '$1 == \"mysql_provider_state_store_accepts_3072_and_rejects_3073_before_io:\" { n++ } END { exit(n != 1) }'",
        "cargo test --features mysql-state-store-provider,state-store-test-hooks --test state_store_mysql mysql_provider_state_store_accepts_3072_and_rejects_3073_before_io -- --exact --nocapture --test-threads=1",
        "cargo test --features mysql-state-store-provider,state-store-test-hooks --test state_store_mysql -- --list | awk '$1 == \"mysql_suite:\" { n++ } END { exit(n != 1) }'",
        "cargo test --features mysql-state-store-provider,state-store-test-hooks --test state_store_mysql mysql_suite -- --exact --nocapture --test-threads=1",
        "cargo test --features mysql-state-store-provider,state-store-test-hooks --test state_store_mysql_cross_process -- --list | awk '$1 == \"mysql_cross_process_suite:\" { n++ } END { exit(n != 1) }'",
        "cargo test --features mysql-state-store-provider,state-store-test-hooks --test state_store_mysql_cross_process mysql_cross_process_suite -- --exact --nocapture --test-threads=1",
        "cargo build --profile dev-opt --features mysql-state-store-provider",
        "cargo test --test cluster_mvp -- --list | awk '$1 == \"cross_process_three_be_state_store_baseline:\" { n++ } END { exit(n != 1) }'",
        "cargo test --test cluster_mvp cross_process_three_be_state_store_baseline -- --exact --nocapture",
        "git diff --check",
    ];
    let logical_lines = logical_gate.lines().collect::<Vec<_>>();
    let mut previous = None;
    for command in expected_commands {
        assert_eq!(
            logical_lines
                .iter()
                .filter(|line| **line == command)
                .count(),
            1,
            "the production gate must contain exactly one command: {command}"
        );
        let position = logical_lines
            .iter()
            .position(|line| *line == command)
            .expect("command count already proved nonzero");
        assert!(
            previous.is_none_or(|previous| previous < position),
            "production gate command is out of order: {command}"
        );
        previous = Some(position);
    }
    assert_eq!(
        logical_gate
            .matches("docker/mysql-state-store/probes/contract.sh")
            .count(),
        1,
        "the raw InnoDB contract must run exactly once"
    );
    assert!(
        logical_gate.contains("mysql_provider_state_store_accepts_3072_and_rejects_3073_before_io")
            && logical_gate.contains("mysql_suite")
            && logical_gate.contains("mysql_cross_process_suite"),
        "the raw contract cannot replace the public 3072/3073, conformance, or cross-process suites"
    );
    assert!(
        !logical_gate.contains("--features foundationdb-provider")
            && !logical_gate.contains("docker/foundationdb/up.sh"),
        "the MySQL gate must keep FoundationDB feature-off and non-live"
    );
}

#[test]
fn mysql_state_store_gate_restores_fixture_database_after_raw_probe() {
    let source_root = src_dir();
    let gate_path = source_root
        .parent()
        .expect("workspace root")
        .join("tools/ci/mysql-state-store-provider.sh");
    let gate = fs::read_to_string(gate_path).expect("read MySQL state-store production gate");
    let ordered = [
        "READINESS_DB=\"$NOVAROCKS_MYSQL_DATABASE\"",
        "PROBE_DB=\"$(docker/mysql-state-store/provision-test-database.sh create production-gate-probes)\"",
        "cleanup_probe_db_on_exit()",
        "local gate_status=\"$?\"",
        "cleanup_probe_db || true",
        "exit \"$gate_status\"",
        "trap cleanup_probe_db_on_exit EXIT",
        "export NOVAROCKS_MYSQL_DATABASE=\"$PROBE_DB\"",
        "docker/mysql-state-store/probes/contract.sh",
        "cleanup_probe_db\n",
        "export NOVAROCKS_MYSQL_DATABASE=\"$READINESS_DB\"",
        "trap - EXIT",
        "cargo test --features mysql-state-store-provider --test state_store_mysql_runtime",
    ];
    let mut previous = 0usize;
    for owner in ordered {
        let offset = gate[previous..]
            .find(owner)
            .unwrap_or_else(|| panic!("gate is missing ordered database owner: {owner}"));
        previous += offset + owner.len();
    }
}
