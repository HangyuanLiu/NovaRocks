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
use std::path::{Path, PathBuf};

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
