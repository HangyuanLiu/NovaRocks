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

//! Architecture guards for the plan-IR layering arc (PIR-8).
//!
//! These tests mechanically enforce the PIR import and stage boundaries. Test
//! modules may still build optimizer trees as inputs; production code may not
//! leak optimizer physical types into planner/codegen main paths.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const NIDL_D3B_BASELINE_PATH: &str = "tests/proto_schema_baseline/novarocks_schema.json";
const NIDL_D3B_WRITE_BASELINE_ENV: &str = "NOVA_WRITE_PROTO_SCHEMA_BASELINE";
const NIDL_D3B_WRITE_BASELINE_COMMAND: &str = "NOVA_WRITE_PROTO_SCHEMA_BASELINE=1 cargo test --test architecture_guard nidl_d3b_current_schema_matches_baseline -- --nocapture";

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
            if attributes
                .iter()
                .any(|attribute| cfg_attribute_requires_test(attribute))
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

fn cfg_predicate_requires_test(tokens: &[String], start: usize) -> Option<(bool, usize)> {
    let owner = tokens.get(start)?;
    if owner == "test"
        && tokens
            .get(start + 1)
            .is_none_or(|token| matches!(token.as_str(), "," | ")"))
    {
        return Some((true, start + 1));
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
        return Some((false, cursor));
    }

    let mut children = Vec::new();
    let mut cursor = start + 2;
    while tokens.get(cursor).is_some_and(|token| token != ")") {
        let (requires_test, next) = cfg_predicate_requires_test(tokens, cursor)?;
        children.push(requires_test);
        cursor = next;
        if tokens.get(cursor).is_some_and(|token| token == ",") {
            cursor += 1;
        }
    }
    let end = (tokens.get(cursor)? == ")").then_some(cursor + 1)?;
    let requires_test = match owner.as_str() {
        "all" => children.into_iter().any(|child| child),
        "any" => !children.is_empty() && children.into_iter().all(|child| child),
        "not" => false,
        _ => unreachable!(),
    };
    Some((requires_test, end))
}

fn cfg_attribute_requires_test(attribute: &str) -> bool {
    let tokens = rust_use_tokens(&rust_lexically_sanitized(attribute));
    let Some(open) = tokens.iter().position(|token| token == "[") else {
        return false;
    };
    let cfg = open + 1;
    if tokens.get(cfg).is_none_or(|token| token != "cfg") {
        return false;
    }
    if tokens.get(cfg + 1).is_none_or(|token| token != "(") {
        return false;
    }
    cfg_predicate_requires_test(&tokens, cfg + 2).is_some_and(|(required, _)| required)
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

fn production_rs_files(root: &Path) -> Vec<PathBuf> {
    production_rs_files_from_entries(root, &[root.join("mod.rs")])
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
            if item
                .attributes
                .iter()
                .any(|attribute| cfg_attribute_requires_test(attribute))
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

fn brace_delta(line: &str) -> isize {
    line.chars().fold(0, |delta, ch| match ch {
        '{' => delta + 1,
        '}' => delta - 1,
        _ => delta,
    })
}

fn paren_delta(line: &str) -> isize {
    line.chars().fold(0, |delta, ch| match ch {
        '(' => delta + 1,
        ')' => delta - 1,
        _ => delta,
    })
}

fn is_comment_or_blank(line: &str) -> bool {
    let trimmed = line.trim_start();
    trimmed.is_empty()
        || trimmed.starts_with("//")
        || trimmed.starts_with("/*")
        || trimmed.starts_with('*')
}

fn non_comment_trimmed_lines(text: &str) -> Vec<&str> {
    let mut lines = Vec::new();
    let mut in_block_comment = false;

    for line in text.lines() {
        let trimmed = line.trim();
        if in_block_comment {
            if trimmed.contains("*/") {
                in_block_comment = false;
            }
            continue;
        }

        if trimmed.is_empty() || trimmed.starts_with("//") || trimmed.starts_with('*') {
            continue;
        }
        if trimmed.starts_with("/*") {
            if !trimmed.contains("*/") {
                in_block_comment = true;
            }
            continue;
        }

        lines.push(trimmed);
    }

    lines
}

fn has_non_comment_line(text: &str, needle: &str) -> bool {
    non_comment_trimmed_lines(text)
        .into_iter()
        .any(|line| line == needle)
}

fn has_cfg_test_mod_tests(text: &str) -> bool {
    non_comment_trimmed_lines(text)
        .windows(2)
        .any(|lines| lines == ["#[cfg(test)]", "mod tests;"])
}

fn is_cfg_test_attr(trimmed: &str) -> bool {
    if trimmed.starts_with("#[cfg(test") {
        return true;
    }
    compact_line(trimmed).starts_with("#[cfg(all(test,")
}

fn module_declarations(text: &str) -> BTreeSet<String> {
    non_comment_trimmed_lines(text)
        .into_iter()
        .filter_map(|line| {
            let declaration = line.strip_suffix(';')?;
            let module = declaration
                .strip_prefix("mod ")
                .or_else(|| declaration.strip_prefix("pub mod "))
                .or_else(|| declaration.strip_prefix("pub(crate) mod "))
                .or_else(|| declaration.strip_prefix("pub(super) mod "))?;
            if module
                .chars()
                .all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
            {
                Some(module.to_string())
            } else {
                None
            }
        })
        .collect()
}

fn planner_namespace_module_declarations(text: &str) -> BTreeSet<String> {
    rust_module_item_declarations(text)
}

fn rust_module_item_declarations(text: &str) -> BTreeSet<String> {
    let production = rust_sanitized_production_text(text);
    let tokens = rust_use_tokens(&production);
    tokens
        .windows(3)
        .filter_map(|tokens| {
            (tokens[0] == "mod"
                && tokens[1].chars().all(is_ident_char)
                && matches!(tokens[2].as_str(), ";" | "{"))
            .then(|| tokens[1].clone())
        })
        .collect()
}

fn has_module_declaration(text: &str, module: &str) -> bool {
    module_declarations(text).contains(module)
}

fn runtime_filter_lifecycle_source_files(src: &Path) -> Vec<PathBuf> {
    rs_files(src)
}

fn planner_root_declares_runtime_filter(text: &str) -> bool {
    rust_module_item_declarations(text).contains("runtime_filter")
}

fn non_test_line_hits<F>(path: &Path, mut predicate: F) -> Vec<(usize, String)>
where
    F: FnMut(&str) -> bool,
{
    let text = fs::read_to_string(path).unwrap_or_default();
    let mut hits = Vec::new();
    let mut pending_cfg_test = false;
    let mut test_depth = 0isize;

    for (idx, line) in text.lines().enumerate() {
        let trimmed = line.trim_start();

        if test_depth > 0 {
            test_depth += brace_delta(line);
            if test_depth < 0 {
                test_depth = 0;
            }
            continue;
        }

        if is_cfg_test_attr(trimmed) {
            pending_cfg_test = true;
            let delta = brace_delta(line);
            if delta > 0 {
                test_depth = delta;
                pending_cfg_test = false;
            }
            continue;
        }

        if pending_cfg_test {
            let delta = brace_delta(line);
            if delta > 0 {
                test_depth = delta;
            }
            pending_cfg_test = false;
            continue;
        }

        if !is_comment_or_blank(line) && predicate(line) {
            hits.push((idx + 1, line.trim().to_string()));
        }
    }

    hits
}

fn source_line_hits<F>(path: &Path, mut predicate: F) -> Vec<(usize, String)>
where
    F: FnMut(&str) -> bool,
{
    let text = fs::read_to_string(path).unwrap_or_default();
    text.lines()
        .enumerate()
        .filter_map(|(idx, line)| {
            if !is_comment_or_blank(line) && predicate(line) {
                Some((idx + 1, line.trim().to_string()))
            } else {
                None
            }
        })
        .collect()
}

fn rust_production_text_without_cfg_test(text: &str) -> String {
    let mut production = String::with_capacity(text.len());
    let mut pending_cfg_test = false;
    let mut skipping_cfg_item = false;
    let mut skip_depth = 0isize;

    for line in text.lines() {
        let trimmed = line.trim_start();

        if skip_depth > 0 {
            skip_depth += brace_delta(line);
            if skip_depth <= 0 {
                skip_depth = 0;
                skipping_cfg_item = false;
            }
            continue;
        }

        if skipping_cfg_item {
            let delta = brace_delta(line);
            if line.contains('{') {
                skip_depth = delta.max(0);
                skipping_cfg_item = skip_depth > 0;
            } else if trimmed.ends_with(';') {
                skipping_cfg_item = false;
            }
            continue;
        }

        if pending_cfg_test {
            if trimmed.is_empty() || trimmed.starts_with("#[") {
                continue;
            }

            let delta = brace_delta(line);
            if line.contains('{') {
                skip_depth = delta.max(0);
                skipping_cfg_item = skip_depth > 0;
            } else if !trimmed.ends_with(';') {
                skipping_cfg_item = true;
            }
            pending_cfg_test = false;
            continue;
        }

        if is_cfg_test_attr(trimmed) {
            pending_cfg_test = true;
            continue;
        }

        production.push_str(line);
        production.push('\n');
    }

    production
}

fn is_cfg_test_or_compat_attr(trimmed: &str) -> bool {
    if is_cfg_test_attr(trimmed) {
        return true;
    }
    let compact = compact_line(trimmed);
    compact == "#[cfg(feature=\"compat\")]"
}

fn rust_production_text_without_cfg_test_or_compat(text: &str) -> String {
    let mut production = String::with_capacity(text.len());
    let mut pending_skip_attr = false;
    let mut skipping_cfg_item = false;
    let mut skip_depth = 0isize;
    let mut skip_paren_depth = 0isize;

    for line in text.lines() {
        let trimmed = line.trim_start();

        if skip_depth > 0 {
            skip_depth += brace_delta(line);
            if skip_depth <= 0 {
                skip_depth = 0;
                skipping_cfg_item = false;
                skip_paren_depth = 0;
            }
            production.push('\n');
            continue;
        }

        if skipping_cfg_item {
            skip_paren_depth += paren_delta(line);
            let delta = brace_delta(line);
            if line.contains('{') {
                skip_depth = delta.max(0);
                skipping_cfg_item = skip_depth > 0;
                skip_paren_depth = 0;
            } else if skip_paren_depth <= 0 && (trimmed.ends_with(';') || trimmed.ends_with(',')) {
                skipping_cfg_item = false;
                skip_paren_depth = 0;
            }
            production.push('\n');
            continue;
        }

        if pending_skip_attr {
            if trimmed.is_empty() || trimmed.starts_with("#[") {
                production.push('\n');
                continue;
            }

            let delta = brace_delta(line);
            if line.contains('{') {
                skip_depth = delta.max(0);
                skipping_cfg_item = skip_depth > 0;
                skip_paren_depth = 0;
            } else if !trimmed.ends_with(';') && !trimmed.ends_with(',') {
                skipping_cfg_item = true;
                skip_paren_depth = paren_delta(line).max(0);
            }
            pending_skip_attr = false;
            production.push('\n');
            continue;
        }

        if is_cfg_test_or_compat_attr(trimmed) {
            pending_skip_attr = true;
            production.push('\n');
            continue;
        }

        production.push_str(line);
        production.push('\n');
    }

    production
}

fn nidl_e9_rust_production_text_without_cfg_test(text: &str) -> String {
    let mut production = String::with_capacity(text.len());
    let mut pending_skip_attr = false;
    let mut skipping_cfg_item = false;
    let mut skip_depth = 0isize;
    let mut skip_paren_depth = 0isize;

    for line in text.lines() {
        let trimmed = line.trim_start();

        if skip_depth > 0 {
            skip_depth += brace_delta(line);
            if skip_depth <= 0 {
                skip_depth = 0;
                skipping_cfg_item = false;
                skip_paren_depth = 0;
            }
            production.push('\n');
            continue;
        }

        if skipping_cfg_item {
            skip_paren_depth += paren_delta(line);
            let delta = brace_delta(line);
            if line.contains('{') {
                skip_depth = delta.max(0);
                skipping_cfg_item = skip_depth > 0;
                skip_paren_depth = 0;
            } else if skip_paren_depth <= 0 && (trimmed.ends_with(';') || trimmed.ends_with(',')) {
                skipping_cfg_item = false;
                skip_paren_depth = 0;
            }
            production.push('\n');
            continue;
        }

        if pending_skip_attr {
            if trimmed.is_empty() || trimmed.starts_with("#[") {
                production.push('\n');
                continue;
            }

            let delta = brace_delta(line);
            if line.contains('{') {
                skip_depth = delta.max(0);
                skipping_cfg_item = skip_depth > 0;
                skip_paren_depth = 0;
            } else if !trimmed.ends_with(';') && !trimmed.ends_with(',') {
                skipping_cfg_item = true;
                skip_paren_depth = paren_delta(line).max(0);
            }
            pending_skip_attr = false;
            production.push('\n');
            continue;
        }

        if is_cfg_test_attr(trimmed) {
            pending_skip_attr = true;
            production.push('\n');
            continue;
        }

        production.push_str(line);
        production.push('\n');
    }

    production
}

fn nidl_e2_rust_text_without_cfg_test_or_compat(text: &str) -> String {
    rust_production_text_without_cfg_test_or_compat(text)
}

fn push_forbidden_terms(
    violations: &mut Vec<String>,
    source: &str,
    text: &str,
    terms: &[&str],
    reason: &str,
) {
    for term in terms {
        if let Some((line, text)) = text
            .lines()
            .enumerate()
            .find(|(_, line)| line.contains(term))
        {
            violations.push(format!(
                "{source}:{}: {reason}: `{term}` in `{}`",
                line + 1,
                text.trim()
            ));
        }
    }
}

#[test]
fn d3l_rust_production_text_without_cfg_test_removes_cfg_test_items() {
    let input = r#"
pub(crate) fn production() {
    let keep = "TDataSink";
}

#[cfg(test)]
mod tests {
    fn fixture() {
        let forbidden = "test-only TPlan)";
    }
}

pub(crate) fn production_after_tests() {
    let keep = "TPlan)";
}

#[cfg(test)]
fn test_helper() {
    let forbidden = "test-only TDataSink";
}

#[cfg(test)]
const TEST_ONLY: &str = "test-only find_scan_plan_nodes(";

pub(crate) fn production_tail() {
    let keep = "fragment_sink_is_terminal_write_sink";
}
"#;

    let production = rust_production_text_without_cfg_test(input);

    assert!(production.contains("pub(crate) fn production()"));
    assert!(production.contains("pub(crate) fn production_after_tests()"));
    assert!(production.contains("pub(crate) fn production_tail()"));
    assert!(!production.contains("test-only TPlan)"));
    assert!(!production.contains("test-only TDataSink"));
    assert!(!production.contains("test-only find_scan_plan_nodes("));
}

fn non_test_optimizer_refs(path: &Path) -> Vec<(usize, String)> {
    let original = fs::read_to_string(path).unwrap_or_default();
    let production = rust_sanitized_production_text(&original);
    original
        .lines()
        .zip(production.lines())
        .enumerate()
        .filter_map(|(index, (original, production))| {
            production
                .contains("crate::sql::optimizer::")
                .then(|| (index + 1, original.trim().to_string()))
        })
        .collect()
}

fn test_dir() -> PathBuf {
    Path::new(manifest_dir()).join("tests")
}

fn source_and_test_rs_files() -> Vec<PathBuf> {
    let mut files = rs_files(&src_dir());
    files.extend(rs_files(&test_dir()));
    files
        .into_iter()
        .filter(|path| rel(path) != "tests/architecture_guard.rs")
        .collect()
}

#[test]
fn nidl_d3g_native_runtime_query_options_do_not_use_thrift_model() {
    let forbidden = [
        "src/runtime/runtime_state.rs",
        "src/cache/mod.rs",
        "src/runtime/coordinator.rs",
        "src/runtime/native_fragment_wire.rs",
        "src/sql/codegen/proto_encode/instance.rs",
    ];
    let repo = Path::new(manifest_dir());
    for path in forbidden {
        let text = fs::read_to_string(repo.join(path)).expect(path);
        assert!(
            !text.contains("TQueryOptions") && !text.contains("internal_service::TQueryOptions"),
            "{path} must use runtime::query_options::QueryOptions, not thrift TQueryOptions"
        );
    }
}

#[test]
fn nidl_d3h_native_runtime_filter_params_do_not_use_thrift_model() {
    let repo = Path::new(manifest_dir());
    let guarded_files = [
        "src/runtime/runtime_state.rs",
        "src/runtime/query_context.rs",
        "src/runtime/native_fragment_wire.rs",
        "src/runtime/coordinator.rs",
        "src/sql/codegen/proto_encode/instance.rs",
        "src/lower/common/fragment_runtime.rs",
        "src/exec/operators/hashjoin/hash_join_build_sink.rs",
        "src/runtime/runtime_filter_worker.rs",
    ];
    let forbidden = [
        "TRuntimeFilterParams",
        "TRuntimeFilterProberParams",
        "runtime_filter::TRuntimeFilterParams",
        "runtime_filter::TRuntimeFilterProberParams",
    ];
    let mut violations = Vec::new();

    for rel_path in guarded_files {
        let path = repo.join(rel_path);
        for (line, text) in source_line_hits(&path, |line| {
            forbidden.iter().any(|symbol| line.contains(symbol))
        }) {
            violations.push(format!("{rel_path}:{line}: {text}"));
        }
    }

    assert!(
        violations.is_empty(),
        "native runtime filter params must use runtime::runtime_filter_params::RuntimeFilterParams, not thrift runtime filter models:\n{}",
        violations.join("\n")
    );
}

fn rs_files_under(relative_roots: &[&str]) -> Vec<PathBuf> {
    let repo = Path::new(manifest_dir());
    let mut files = Vec::new();
    for root in relative_roots {
        files.extend(rs_files(&repo.join(root)));
    }
    files
}

fn is_ident_char(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}

#[derive(Clone, Copy)]
enum RustWirePolicy {
    StrictNoWire,
    StarRocksProtoOnly,
    StrictNoStarRocksWire,
    AllowNativeProto,
    PlannerPartitionBridge,
}

#[derive(Clone, Copy, Default)]
struct RustWireContext {
    in_crate_use_group: bool,
    in_proto_use_group: bool,
    in_thrift_use_group: bool,
}

fn compact_line(line: &str) -> String {
    line.chars().filter(|ch| !ch.is_whitespace()).collect()
}

fn first_ident(text: &str) -> Option<String> {
    let start = text.find(|ch| is_ident_char(ch))?;
    let tail = &text[start..];
    let end = tail.find(|ch| !is_ident_char(ch)).unwrap_or(tail.len());
    Some(tail[..end].to_string())
}

fn group_entry_modules(text: &str) -> Vec<String> {
    text.split(',')
        .filter_map(first_ident)
        .filter(|entry| !matches!(entry.as_str(), "use" | "crate" | "self" | "super"))
        .collect()
}

fn modules_after_needle(compact: &str, needle: &str) -> Vec<String> {
    let mut modules = Vec::new();
    let mut rest = compact;
    while let Some(pos) = rest.find(needle) {
        let after = &rest[pos + needle.len()..];
        if let Some(group) = after.strip_prefix('{') {
            let end = group.find('}').unwrap_or(group.len());
            modules.extend(group_entry_modules(&group[..end]));
        } else if let Some(module) = first_ident(after) {
            modules.push(module);
        }
        rest = &after[after.len().min(1)..];
    }
    modules
}

fn line_has_ident(line: &str, ident: &str) -> bool {
    line.match_indices(ident).any(|(idx, _)| {
        let before = line[..idx].chars().next_back();
        let after = line[idx + ident.len()..].chars().next();
        before.is_none_or(|ch| !is_ident_char(ch)) && after.is_none_or(|ch| !is_ident_char(ch))
    })
}

fn proto_reference_modules(line: &str, context: RustWireContext) -> Vec<String> {
    let compact = compact_line(line);
    let in_crate_group = context.in_crate_use_group || compact.contains("crate::{");
    let mut modules = modules_after_needle(&compact, "crate::proto::");
    modules.extend(modules_after_needle(&compact, "grpc_client::proto::"));
    modules.extend(modules_after_needle(
        &compact,
        "service::grpc_client::proto::",
    ));
    if in_crate_group {
        modules.extend(modules_after_needle(&compact, "proto::"));
        if line_has_ident(line, "proto") && !compact.contains("proto::") {
            modules.push("proto".to_string());
        }
    }
    if context.in_proto_use_group {
        modules.extend(group_entry_modules(&compact));
    }
    modules.sort();
    modules.dedup();
    modules
}

fn thrift_reference_modules(line: &str, context: RustWireContext) -> Vec<String> {
    let compact = compact_line(line);
    let in_crate_group = context.in_crate_use_group || compact.contains("crate::{");
    let mut modules = modules_after_needle(&compact, "crate::thrift::");
    if in_crate_group {
        modules.extend(modules_after_needle(&compact, "thrift::"));
        if line_has_ident(line, "thrift") && !compact.contains("thrift::") {
            modules.push("thrift".to_string());
        }
    }
    if context.in_thrift_use_group {
        modules.extend(group_entry_modules(&compact));
    }
    if compact.contains("crate::types::arrow_thrift")
        || (in_crate_group && compact.contains("types::arrow_thrift"))
    {
        modules.push("arrow_thrift".to_string());
    }
    modules.sort();
    modules.dedup();
    modules
}

fn contains_starrocks_proto_ref(line: &str) -> bool {
    proto_reference_modules(line, RustWireContext::default())
        .iter()
        .any(|module| module == "starrocks")
}

fn contains_staros_proto_ref(line: &str) -> bool {
    proto_reference_modules(line, RustWireContext::default())
        .iter()
        .any(|module| module == "staros")
}

fn contains_thrift_ref(line: &str) -> bool {
    !thrift_reference_modules(line, RustWireContext::default()).is_empty()
}

fn rust_wire_policy_violates_line(
    line: &str,
    context: RustWireContext,
    policy: RustWirePolicy,
) -> bool {
    let proto_modules = proto_reference_modules(line, context);
    let thrift_modules = thrift_reference_modules(line, context);
    let starrocks_proto = proto_modules.iter().any(|module| module == "starrocks");
    let staros_proto = proto_modules.iter().any(|module| module == "staros");
    let thrift = !thrift_modules.is_empty();

    match policy {
        RustWirePolicy::StrictNoWire => !proto_modules.is_empty() || thrift,
        RustWirePolicy::StarRocksProtoOnly => starrocks_proto || staros_proto,
        RustWirePolicy::StrictNoStarRocksWire | RustWirePolicy::AllowNativeProto => {
            starrocks_proto || staros_proto || thrift
        }
        RustWirePolicy::PlannerPartitionBridge => {
            starrocks_proto
                || staros_proto
                || (thrift && thrift_modules.iter().any(|module| module != "partitions"))
        }
    }
}

fn update_wire_group_depth(depth: &mut isize, line: &str) {
    if *depth > 0 {
        *depth += brace_delta(line);
        if *depth < 0 {
            *depth = 0;
        }
    }
}

fn start_wire_group_depth(depth: &mut isize, line: &str, needle: &str) {
    if *depth == 0 && compact_line(line).contains(needle) {
        *depth = brace_delta(line).max(0);
    }
}

fn rust_wire_reference_hits(path: &Path, policy: RustWirePolicy) -> Vec<(usize, String)> {
    let text = fs::read_to_string(path).unwrap_or_default();
    let mut hits = Vec::new();
    let mut pending_cfg_test = false;
    let mut test_depth = 0isize;
    let mut crate_use_group_depth = 0isize;
    let mut proto_use_group_depth = 0isize;
    let mut thrift_use_group_depth = 0isize;

    for (idx, line) in text.lines().enumerate() {
        let trimmed = line.trim_start();

        if test_depth > 0 {
            test_depth += brace_delta(line);
            if test_depth < 0 {
                test_depth = 0;
            }
            continue;
        }

        if is_cfg_test_attr(trimmed) {
            pending_cfg_test = true;
            let delta = brace_delta(line);
            if delta > 0 {
                test_depth = delta;
                pending_cfg_test = false;
            }
            continue;
        }

        if pending_cfg_test {
            let delta = brace_delta(line);
            if delta > 0 {
                test_depth = delta;
            }
            pending_cfg_test = false;
            continue;
        }

        let context = RustWireContext {
            in_crate_use_group: crate_use_group_depth > 0,
            in_proto_use_group: proto_use_group_depth > 0,
            in_thrift_use_group: thrift_use_group_depth > 0,
        };
        if !is_comment_or_blank(line) && rust_wire_policy_violates_line(line, context, policy) {
            hits.push((idx + 1, line.trim().to_string()));
        }

        update_wire_group_depth(&mut crate_use_group_depth, line);
        update_wire_group_depth(&mut proto_use_group_depth, line);
        update_wire_group_depth(&mut thrift_use_group_depth, line);
        start_wire_group_depth(&mut crate_use_group_depth, line, "usecrate::{");
        start_wire_group_depth(&mut proto_use_group_depth, line, "usecrate::proto::{");
        start_wire_group_depth(&mut thrift_use_group_depth, line, "usecrate::thrift::{");
    }

    hits
}

fn proto_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(proto_files(&path));
            } else if path.extension().is_some_and(|ext| ext == "proto") {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

fn proto_imports(path: &Path) -> Vec<(usize, String)> {
    let text = fs::read_to_string(path).unwrap_or_default();
    let mut imports = Vec::new();
    for (idx, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("import ") {
            let rest = rest
                .strip_prefix("public ")
                .or_else(|| rest.strip_prefix("weak "))
                .unwrap_or(rest);
            if let Some(rest) = rest.strip_prefix('"')
                && let Some((import, _)) = rest.split_once('"')
            {
                imports.push((idx + 1, import.to_string()));
            }
        }
    }
    imports
}

fn disallowed_novarocks_proto_imports(files: &[PathBuf]) -> Vec<String> {
    let allowed = [
        "common.proto",
        "expr.proto",
        "filter.proto",
        "plan.proto",
        "service.proto",
    ];
    let mut hits = Vec::new();
    for file in files {
        for (line, import) in proto_imports(file) {
            if !allowed.contains(&import.as_str()) {
                hits.push(format!("{}:{}: import \"{}\"", rel(file), line, import));
            }
        }
    }
    hits
}

fn named_let_array_lines<'a>(text: &'a str, name: &str) -> Option<Vec<(usize, &'a str)>> {
    let lines = text.lines().collect::<Vec<_>>();
    let start = lines
        .iter()
        .position(|line| line.contains(&format!("let {name} = [")))?;
    let mut block = Vec::new();
    for (idx, line) in lines.iter().enumerate().skip(start) {
        block.push((idx + 1, *line));
        if line.contains("];") {
            return Some(block);
        }
    }
    Some(block)
}

fn compile_protos_call_lines<'a>(
    text: &'a str,
    protos_name: &str,
) -> Option<Vec<(usize, &'a str)>> {
    let lines = text.lines().collect::<Vec<_>>();
    let start = lines
        .iter()
        .position(|line| compact_line(line).contains(&format!("compile_protos(&{protos_name}")))?;
    let mut call = Vec::new();
    for (idx, line) in lines.iter().enumerate().skip(start).take(12) {
        call.push((idx + 1, *line));
        if line.contains(';') || line.contains(".unwrap()") || line.contains(".context(") {
            break;
        }
    }
    Some(call)
}

fn contains_compat_proto_root(line: &str) -> bool {
    line.contains("COMPAT_PROTO_DIR")
        || line.contains("COMPAT_STAROS_DIR")
        || line.contains("compat/proto")
        || line.contains("compat/staros")
}

fn block_contains(lines: &[(usize, &str)], needle: &str) -> bool {
    lines.iter().any(|(_, line)| line.contains(needle))
}

fn native_proto_codegen_boundary_violations(build_rs: &Path) -> Vec<String> {
    let text = fs::read_to_string(build_rs).unwrap_or_default();
    let mut hits = Vec::new();
    let build_rel = rel(build_rs);

    if let Some(native_block) = named_let_array_lines(&text, "novarocks_protos") {
        for (line, text) in &native_block {
            if contains_compat_proto_root(text) {
                hits.push(format!(
                    "{build_rel}:{line}: novarocks_protos must not include compat proto dirs: {}",
                    text.trim()
                ));
            }
        }
    } else {
        hits.push(format!("{build_rel}:1: novarocks_protos block must exist"));
    }

    if let Some(native_call) = compile_protos_call_lines(&text, "novarocks_protos") {
        let mut call_has_compat_root = false;
        for (line, text) in &native_call {
            if contains_compat_proto_root(text) {
                call_has_compat_root = true;
                hits.push(format!(
                    "{build_rel}:{line}: native compile_protos include roots must stay NOVAROCKS_IDL_DIR only: {}",
                    text.trim()
                ));
            }
        }
        let compact_call = native_call
            .iter()
            .map(|(_, line)| compact_line(line))
            .collect::<String>();
        if !call_has_compat_root
            && !compact_call.contains("compile_protos(&novarocks_protos,&[NOVAROCKS_IDL_DIR])")
        {
            let line = native_call.first().map(|(line, _)| *line).unwrap_or(1);
            hits.push(format!(
                "{build_rel}:{line}: native compile_protos include roots must be &[NOVAROCKS_IDL_DIR]"
            ));
        }
    } else {
        hits.push(format!(
            "{build_rel}:1: native compile_protos call for novarocks_protos must exist"
        ));
    }

    if let Some(starrocks_block) = named_let_array_lines(&text, "starrocks_protos") {
        if !block_contains(&starrocks_block, "COMPAT_PROTO_DIR") {
            let line = starrocks_block.first().map(|(line, _)| *line).unwrap_or(1);
            hits.push(format!(
                "{build_rel}:{line}: starrocks_protos must explicitly use COMPAT_PROTO_DIR"
            ));
        }
    } else {
        hits.push(format!("{build_rel}:1: starrocks_protos block must exist"));
    }

    if let Some(staros_block) = named_let_array_lines(&text, "staros_protos") {
        if !block_contains(&staros_block, "COMPAT_STAROS_DIR") {
            let line = staros_block.first().map(|(line, _)| *line).unwrap_or(1);
            hits.push(format!(
                "{build_rel}:{line}: staros_protos must explicitly use COMPAT_STAROS_DIR"
            ));
        }
    } else {
        hits.push(format!("{build_rel}:1: staros_protos block must exist"));
    }
    hits
}

#[test]
fn nidl_d2c_detector_flags_proto_build_and_rust_wire_violations() {
    let tmp_dir = std::env::temp_dir().join(format!(
        "nidl_d2c_guard_probe_{}_{}",
        std::process::id(),
        "wire_refs"
    ));
    fs::create_dir_all(&tmp_dir).unwrap();

    let proto = tmp_dir.join("service.proto");
    fs::write(
        &proto,
        concat!(
            "syntax = \"proto3\";\n",
            "import \"common.proto\";\n",
            "import \"../compat/proto/internal_service.proto\";\n",
            "import \"staros/starlet.proto\";\n",
            "import public \"../compat/proto/public.proto\";\n",
            "import weak \"staros/weak.proto\";\n",
        ),
    )
    .unwrap();
    let proto_hits = disallowed_novarocks_proto_imports(&[proto.clone()]);
    assert_eq!(proto_hits.len(), 4, "{proto_hits:?}");

    let build_rs = tmp_dir.join("build.rs");
    fs::write(
        &build_rs,
        concat!(
            "let novarocks_protos = [idl_path(NOVAROCKS_IDL_DIR, \"service.proto\"), idl_path(COMPAT_PROTO_DIR, \"internal_service.proto\")];\n",
            "tonic_build::configure().compile_protos(&novarocks_protos, &[NOVAROCKS_IDL_DIR, COMPAT_PROTO_DIR]).unwrap();\n",
            "let starrocks_protos = [idl_path(COMPAT_PROTO_DIR, \"internal_service.proto\")];\n",
            "tonic_build::configure().compile_protos(&starrocks_protos, &[COMPAT_PROTO_DIR]).unwrap();\n",
            "let staros_protos = [idl_path(COMPAT_STAROS_DIR, \"starlet.proto\")];\n",
            "tonic_build::configure().compile_protos(&staros_protos, &[COMPAT_STAROS_DIR]).unwrap();\n",
        ),
    )
    .unwrap();
    let build_hits = native_proto_codegen_boundary_violations(&build_rs);
    assert_eq!(build_hits.len(), 2, "{build_hits:?}");
    assert!(
        build_hits.iter().all(|hit| hit.contains("build.rs:")),
        "{build_hits:?}"
    );

    let rust = tmp_dir.join("planner.rs");
    fs::write(
        &rust,
        concat!(
            "use crate::proto::starrocks::PPlanFragment;\n",
            "use crate::proto::staros::StarStatus;\n",
            "use crate::thrift::types;\n",
            "use crate::thrift::partitions;\n",
            "use crate::{runtime, thrift::types};\n",
            "use crate::thrift::partitions; use crate::thrift::exprs;\n",
            "use crate::service::grpc_client::proto::starrocks::PPlanFragment;\n",
        ),
    )
    .unwrap();
    let strict_hits = rust_wire_reference_hits(&rust, RustWirePolicy::StrictNoStarRocksWire);
    assert_eq!(strict_hits.len(), 7, "{strict_hits:?}");
    let planner_hits = rust_wire_reference_hits(&rust, RustWirePolicy::PlannerPartitionBridge);
    assert_eq!(planner_hits.len(), 6, "{planner_hits:?}");
    assert!(contains_starrocks_proto_ref(
        "use crate::proto::{starrocks};"
    ));
    assert!(contains_staros_proto_ref("use crate::proto::{staros};"));
    assert!(contains_thrift_ref("use crate::{runtime, thrift::types};"));

    let common = tmp_dir.join("common.rs");
    fs::write(
        &common,
        concat!(
            "use crate::{runtime, proto::plan};\n",
            "use crate::proto::{common, plan};\n",
            "use crate::service::grpc_client::proto::starrocks::PPlanFragment;\n",
            "use crate::{\n",
            "    runtime,\n",
            "    thrift::types,\n",
            "};\n",
        ),
    )
    .unwrap();
    let common_hits = rust_wire_reference_hits(&common, RustWirePolicy::StrictNoWire);
    assert_eq!(common_hits.len(), 4, "{common_hits:?}");
    let proto_only_hits = rust_wire_reference_hits(&common, RustWirePolicy::StarRocksProtoOnly);
    assert_eq!(proto_only_hits.len(), 1, "{proto_only_hits:?}");

    fs::remove_dir_all(&tmp_dir).ok();
}

#[test]
fn nidl_d2c_novarocks_proto_imports_stay_native_only() {
    let files = proto_files(&Path::new(manifest_dir()).join("idl/novarocks"));
    let violations = disallowed_novarocks_proto_imports(&files);
    assert!(
        violations.is_empty(),
        "idl/novarocks proto files must import only native proto files:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nidl_d2c_native_proto_codegen_root_excludes_compat_idl() {
    let build_rs = Path::new(manifest_dir()).join("src/build.rs");
    let violations = native_proto_codegen_boundary_violations(&build_rs);
    assert!(
        violations.is_empty(),
        "native proto codegen must stay rooted at idl/novarocks, with StarRocks protos generated explicitly:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nidl_d2c_rust_wire_imports_stay_inside_owned_boundaries() {
    let mut violations = Vec::new();

    for file in rs_files_under(&["src/sql/analyzer", "src/sql/optimizer"]) {
        for (line, text) in rust_wire_reference_hits(&file, RustWirePolicy::StrictNoStarRocksWire) {
            violations.push(format!("{}:{}: {}", rel(&file), line, text));
        }
    }

    for file in rs_files_under(&["src/sql/planner"]) {
        for (line, text) in rust_wire_reference_hits(&file, RustWirePolicy::PlannerPartitionBridge)
        {
            violations.push(format!("{}:{}: {}", rel(&file), line, text));
        }
    }

    for file in rs_files_under(&["src/sql/codegen/proto_encode"]) {
        for (line, text) in rust_wire_reference_hits(&file, RustWirePolicy::StarRocksProtoOnly) {
            violations.push(format!("{}:{}: {}", rel(&file), line, text));
        }
    }

    for file in rs_files_under(&["src/lower/novarocks"]) {
        for (line, text) in rust_wire_reference_hits(&file, RustWirePolicy::AllowNativeProto) {
            violations.push(format!("{}:{}: {}", rel(&file), line, text));
        }
    }

    for file in rs_files_under(&["src/lower/common"]) {
        for (line, text) in rust_wire_reference_hits(&file, RustWirePolicy::StrictNoWire) {
            violations.push(format!("{}:{}: {}", rel(&file), line, text));
        }
    }

    assert!(
        violations.is_empty(),
        "D2C Rust wire imports crossed native/planner/lowering ownership boundaries:\n{}",
        violations.join("\n")
    );
}

#[test]
fn detector_flags_non_test_and_skips_cfg_test_blocks() {
    let tmp = std::env::temp_dir().join(format!(
        "pir8_guard_probe_{}_{}.rs",
        std::process::id(),
        "optimizer_refs"
    ));
    fs::write(
        &tmp,
        "\
use crate::sql::optimizer::operator::AggMode;
#[allow(unused_imports)]
#[cfg(all(feature = \"compat\", test))]
use crate::sql::optimizer::operator::TestOnlyReordered;
#[allow(unused_imports)] #[cfg(all(test, feature = \"compat\"))] use crate::sql::optimizer::operator::TestOnlySameLine;
#[cfg(test_feature)] use crate::sql::optimizer::operator::ProductionFeature;
#[cfg(test = \"production\")] use crate::sql::optimizer::operator::ProductionKeyValue;
#[cfg(test)]
mod tests {
    use crate::sql::optimizer::operator::TopNPhase;
    fn fixture() { let _ = crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(); }
}
fn prod() { let _ = crate::sql::optimizer::property::DistributionSpec::Any; }
",
    )
    .unwrap();
    let hits = non_test_optimizer_refs(&tmp);
    fs::remove_file(&tmp).ok();

    assert_eq!(
        hits,
        vec![
            (
                1,
                "use crate::sql::optimizer::operator::AggMode;".to_string()
            ),
            (
                6,
                "#[cfg(test_feature)] use crate::sql::optimizer::operator::ProductionFeature;"
                    .to_string()
            ),
            (
                7,
                "#[cfg(test = \"production\")] use crate::sql::optimizer::operator::ProductionKeyValue;"
                    .to_string()
            ),
            (
                13,
                "fn prod() { let _ = crate::sql::optimizer::property::DistributionSpec::Any; }"
                    .to_string()
            ),
        ]
    );
}

#[test]
fn nidl_d3a_detector_ignores_commented_module_declarations() {
    let commented = "\
// mod proto_contract;
/*
mod proto_contract;
*/
/*
#[cfg(test)]
mod tests;
*/
";
    assert!(!has_non_comment_line(commented, "mod proto_contract;"));
    assert!(!has_cfg_test_mod_tests(commented));
    assert!(module_declarations(commented).is_empty());

    let active = "\
#[cfg(test)]
// comment between attribute and module
mod tests;
mod proto_contract;
pub(crate) mod chunk;
";
    assert!(has_cfg_test_mod_tests(active));
    assert!(has_non_comment_line(active, "mod proto_contract;"));
    assert_eq!(
        module_declarations(active),
        BTreeSet::from([
            "chunk".to_string(),
            "proto_contract".to_string(),
            "tests".to_string()
        ])
    );
}

#[test]
fn planner_namespace_inline_module_breaks_external_exact_set() {
    let source = r#"
mod fragment;
mod node;
mod compatibility {}
"#;
    let expected = BTreeSet::from(["fragment".to_string(), "node".to_string()]);
    let actual = planner_namespace_module_declarations(source);

    assert_ne!(
        actual, expected,
        "inline compatibility module must break the distributed exact module set"
    );
}

#[test]
fn planner_namespace_inline_module_parser_covers_forms_and_nested_scope() {
    let source = r#"
mod plain {}
pub(crate) mod crate_visible {}
#[allow(dead_code)]
mod attributed {}
mod outer {
    pub(super) mod nested {}
}
"#;

    assert_eq!(
        planner_namespace_module_declarations(source),
        BTreeSet::from([
            "attributed".to_string(),
            "crate_visible".to_string(),
            "nested".to_string(),
            "outer".to_string(),
            "plain".to_string(),
        ])
    );
}

#[test]
fn planner_namespace_inline_module_parser_ignores_noise_and_cfg_test() {
    let source = r###"
mod fragment;
mod node;
// mod comment_fake {}
/* pub(crate) mod block_fake {} */
const TEXT: &str = "mod string_fake {}";
const RAW: &str = r#"mod raw_fake {}"#;
#[cfg(test)]
mod tests {
    mod test_nested_fake {}
}
"###;

    assert_eq!(
        planner_namespace_module_declarations(source),
        BTreeSet::from(["fragment".to_string(), "node".to_string()])
    );
}

#[test]
fn planner_namespace_module_item_parser_covers_visibility_attributes_and_layout() {
    let source = r#"
mod private_external;
pub mod public_external;
pub(crate) mod crate_external;
pub(super) mod super_external;
pub(self) mod self_external;
pub(in crate::sql::planner) mod path_external;
pub(crate)
mod split_external;
#[allow(dead_code)]
pub(self) mod attributed_external;
#[path = "compatibility.rs"]
pub(in crate::sql)
mod path_attributed_external;

mod inline_private {}
pub(self) mod inline_self {}
pub(in crate::sql)
mod inline_path {}
"#;

    assert_eq!(
        planner_namespace_module_declarations(source),
        BTreeSet::from([
            "attributed_external".to_string(),
            "crate_external".to_string(),
            "inline_path".to_string(),
            "inline_private".to_string(),
            "inline_self".to_string(),
            "path_attributed_external".to_string(),
            "path_external".to_string(),
            "private_external".to_string(),
            "public_external".to_string(),
            "self_external".to_string(),
            "split_external".to_string(),
            "super_external".to_string(),
        ])
    );
}

#[test]
fn planner_runtime_filter_source_inventory_covers_entire_src_tree() {
    let root = std::env::temp_dir().join(format!(
        "planner_runtime_filter_source_inventory_{}",
        std::process::id()
    ));
    fs::remove_dir_all(&root).ok();
    for relative in [
        "src/sql/planner/physical/runtime_filter.rs",
        "src/sql/codegen/runtime_filter.rs",
        "src/runtime/runtime_filter_duplicate.rs",
    ] {
        let path = root.join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, "pub(crate) struct PlannedRuntimeFilter;").unwrap();
    }

    let actual = runtime_filter_lifecycle_source_files(&root.join("src"))
        .into_iter()
        .map(|path| path.strip_prefix(&root).unwrap().display().to_string())
        .collect::<BTreeSet<_>>();
    fs::remove_dir_all(&root).ok();

    assert_eq!(
        actual,
        BTreeSet::from([
            "src/runtime/runtime_filter_duplicate.rs".to_string(),
            "src/sql/codegen/runtime_filter.rs".to_string(),
            "src/sql/planner/physical/runtime_filter.rs".to_string(),
        ]),
        "runtime-filter type uniqueness must inspect every Rust source file"
    );
}

#[test]
fn planner_runtime_filter_root_module_detector_covers_rust_item_forms_and_noise() {
    for source in [
        "#[allow(dead_code)]\npub(crate) mod\nruntime_filter;",
        "pub(crate)\nmod\nruntime_filter;",
        "#[allow(dead_code)]\nmod runtime_filter {}",
    ] {
        assert!(
            planner_root_declares_runtime_filter(source),
            "active root runtime_filter module item must be detected: {source}"
        );
    }

    let noise = r###"
// mod runtime_filter;
/* mod runtime_filter {} */
const TEXT: &str = "mod runtime_filter;";
const RAW: &str = r#"mod runtime_filter {}"#;
#[cfg(test)]
mod runtime_filter;
"###;
    assert!(
        !planner_root_declares_runtime_filter(noise),
        "comments, strings, and cfg(test) module items must be ignored"
    );
}

#[test]
fn planner_distributed_and_codegen_do_not_import_optimizer() {
    let mut checked = production_rs_files(&src_dir().join("sql/planner/distributed"));
    checked.extend(production_rs_files(&src_dir().join("sql/codegen")));

    let mut violations = Vec::new();
    for file in &checked {
        for (line, text) in non_test_optimizer_refs(file) {
            violations.push(format!("{}:{}: {}", rel(file), line, text));
        }
        let text = fs::read_to_string(file).unwrap();
        for path in rust_production_canonical_paths(&text, &rel(file)) {
            if path.starts_with(&[
                "crate".to_string(),
                "sql".to_string(),
                "optimizer".to_string(),
            ]) {
                violations.push(format!("{}: {}", rel(file), path.join("::")));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "planner distributed/codegen production paths must not reference optimizer types; \
         optimizer_bridge/** is the conversion boundary. Violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn planner_stage_first_boundaries_are_closed() {
    let planner = src_dir().join("sql/planner");
    assert!(planner.join("pipeline/mod.rs").exists());
    assert!(!planner.join("optimizer_bridge/distributed.rs").exists());
    let root_files = fs::read_dir(&planner)
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| {
            path.is_file() && path.extension().is_some_and(|extension| extension == "rs")
        })
        .filter_map(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .map(str::to_string)
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        root_files,
        BTreeSet::from([
            "mod.rs".to_string(),
            "ordering.rs".to_string(),
            "payload.rs".to_string(),
        ]),
        "planner root Rust files must remain the exact stage-first facade and neutral owners"
    );

    let facade = fs::read_to_string(planner.join("mod.rs")).unwrap();
    let root_violations = planner_root_surface_violations_in(&facade);
    assert!(
        root_violations.is_empty(),
        "planner root production surface must stay exact:\n{}",
        root_violations.join("\n")
    );

    let mut path_attribute_violations = Vec::new();
    let mut dependency_violations = Vec::new();
    for file in production_rs_files(&planner) {
        let source_rel = rel(&file);
        let text = fs::read_to_string(&file).unwrap();
        path_attribute_violations.extend(
            planner_path_module_attribute_violations_in(&text)
                .into_iter()
                .map(|violation| format!("{source_rel}: {violation}")),
        );
        dependency_violations.extend(
            planner_stage_first_dependency_violations_in(&source_rel, &text)
                .into_iter()
                .map(|violation| format!("{source_rel}: {violation}")),
        );
    }
    assert!(
        path_attribute_violations.is_empty(),
        "planner production modules must not bypass physical source ownership with path attributes:\n{}",
        path_attribute_violations.join("\n")
    );
    assert!(
        dependency_violations.is_empty(),
        "planner stage-first dependency policy violations:\n{}",
        dependency_violations.join("\n")
    );
}

fn logical_build_surface_violations(text: &str) -> Vec<String> {
    let expected_modules = [
        "aggregate",
        "output",
        "query",
        "relation",
        "select",
        "subquery",
        "tests",
        "window",
    ]
    .into_iter()
    .map(str::to_string)
    .collect::<BTreeSet<_>>();
    let actual_modules = module_declarations(text);
    let mut violations = Vec::new();

    for missing in expected_modules.difference(&actual_modules) {
        violations.push(format!(
            "missing logical build module declaration: {missing}"
        ));
    }
    for extra in actual_modules.difference(&expected_modules) {
        violations.push(format!(
            "unexpected logical build module declaration: {extra}"
        ));
    }
    if !has_cfg_test_mod_tests(text) {
        violations.push("logical build tests module must stay behind #[cfg(test)]".to_string());
    }

    let expected_public_surface = [
        "pub(crate) use output::plan_output_columns;".to_string(),
        "pub(crate) use query::plan_query;".to_string(),
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    let actual_public_surface = non_comment_trimmed_lines(text)
        .into_iter()
        .filter(|line| line.starts_with("pub ") || line.starts_with("pub("))
        .map(str::to_string)
        .collect::<BTreeSet<_>>();

    for missing in expected_public_surface.difference(&actual_public_surface) {
        violations.push(format!("missing logical build public surface: {missing}"));
    }
    for extra in actual_public_surface.difference(&expected_public_surface) {
        violations.push(format!("unexpected logical build public surface: {extra}"));
    }

    violations
}

fn rust_attribute_open_len(bytes: &[u8], start: usize) -> Option<usize> {
    if bytes.get(start) != Some(&b'#') {
        return None;
    }
    if bytes.get(start + 1) == Some(&b'[') {
        return Some(2);
    }
    (bytes.get(start + 1) == Some(&b'!') && bytes.get(start + 2) == Some(&b'[')).then_some(3)
}

fn rust_attribute_group_end(text: &str) -> Option<usize> {
    rust_attribute_open_len(text.as_bytes(), 0)?;

    let mut depth = 0usize;
    for (index, ch) in text.char_indices() {
        match ch {
            '[' => depth += 1,
            ']' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(index + ch.len_utf8());
                }
            }
            _ => {}
        }
    }
    None
}

fn rust_header_has_cfg_test_attribute(header: &str) -> bool {
    let mut rest = header.trim();
    while rust_attribute_open_len(rest.as_bytes(), 0).is_some() {
        let is_outer = rest.starts_with("#[");
        let Some(end) = rust_attribute_group_end(rest) else {
            return false;
        };
        let attribute = compact_line(&rest[..end]);
        if is_outer && is_cfg_test_attr(&attribute) {
            return true;
        }
        rest = rest[end..].trim_start();
    }
    false
}

fn is_rust_function_item_header(header: &str) -> bool {
    let mut rest = header.trim();
    while rust_attribute_open_len(rest.as_bytes(), 0).is_some() {
        let Some(end) = rust_attribute_group_end(rest) else {
            return false;
        };
        rest = rest[end..].trim_start();
    }

    if let Some(after_pub) = rest.strip_prefix("pub ") {
        rest = after_pub.trim_start();
    } else if let Some(after_open) = rest.strip_prefix("pub(") {
        let Some(close) = after_open.find(')') else {
            return false;
        };
        rest = after_open[close + 1..].trim_start();
    }

    loop {
        if let Some(after) = rest.strip_prefix("const ") {
            rest = after.trim_start();
            continue;
        }
        if let Some(after) = rest.strip_prefix("async ") {
            rest = after.trim_start();
            continue;
        }
        if let Some(after) = rest.strip_prefix("unsafe ") {
            rest = after.trim_start();
            continue;
        }
        if let Some(after) = rest.strip_prefix("extern ") {
            rest = after.trim_start();
            if let Some(after_quote) = rest.strip_prefix('"') {
                let Some(close) = after_quote.find('"') else {
                    return false;
                };
                rest = after_quote[close + 1..].trim_start();
            }
            continue;
        }
        break;
    }

    rest.starts_with("fn ")
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
        if !attributes
            .iter()
            .any(|attribute| cfg_attribute_requires_test(attribute))
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

fn planner_root_surface_violations_in(text: &str) -> Vec<String> {
    const EXPECTED: &str = r#"
pub(crate) mod distributed;
pub(crate) mod imv_rewrite;
pub(crate) mod logical;
pub(crate) mod optimizer_bridge;
pub(crate) mod ordering;
pub(crate) mod payload;
pub(crate) mod physical;
pub(crate) mod pipeline;
pub(crate) use logical::build::{plan_output_columns, plan_query};
"#;

    let actual = rust_use_tokens(&rust_sanitized_production_text(text));
    let expected = rust_use_tokens(EXPECTED);
    if actual == expected {
        Vec::new()
    } else {
        vec![format!(
            "planner root production tokens differ; expected {expected:?}, actual {actual:?}"
        )]
    }
}

fn planner_path_module_attribute_violations_in(text: &str) -> Vec<String> {
    fn matching_paren(tokens: &[String], open: usize) -> Option<usize> {
        (tokens.get(open)? == "(").then_some(())?;
        let mut depth = 0usize;
        for (index, token) in tokens.iter().enumerate().skip(open) {
            if token == "(" {
                depth += 1;
            } else if token == ")" {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(index);
                }
            }
        }
        None
    }

    fn generated_meta_has_path(meta: &[String]) -> bool {
        let Some(head) = meta.first().map(String::as_str) else {
            return false;
        };
        if head == "path" {
            return true;
        }
        if head != "cfg_attr" || meta.get(1).is_none_or(|token| token != "(") {
            return false;
        }
        let Some(close) = matching_paren(meta, 1) else {
            return false;
        };
        let mut arguments = Vec::<std::ops::Range<usize>>::new();
        let mut argument_start = 2usize;
        let mut depth = 0usize;
        for index in 2..close {
            match meta[index].as_str() {
                "(" | "[" | "{" => depth += 1,
                ")" | "]" | "}" => depth = depth.saturating_sub(1),
                "," if depth == 0 => {
                    arguments.push(argument_start..index);
                    argument_start = index + 1;
                }
                _ => {}
            }
        }
        arguments.push(argument_start..close);
        arguments
            .into_iter()
            .skip(1)
            .any(|range| generated_meta_has_path(&meta[range]))
    }

    fn affects_module_path(attribute: &str) -> bool {
        let tokens = rust_use_tokens(&rust_lexically_sanitized(attribute));
        let Some(open) = tokens.iter().position(|token| token == "[") else {
            return false;
        };
        generated_meta_has_path(&tokens[open + 1..])
    }

    let production = rust_sanitized_production_text(text);
    rust_module_items(&production)
        .into_iter()
        .flat_map(|item| {
            item.attributes
                .into_iter()
                .filter(|attribute| affects_module_path(attribute))
                .map(move |attribute| format!("module {} has {attribute}", item.name))
        })
        .collect()
}

fn rust_named_type_declaration_count(text: &str, name: &str) -> usize {
    let production = rust_sanitized_production_text(text);
    let identifiers = production
        .split(|ch: char| !is_ident_char(ch))
        .filter(|token| !token.is_empty())
        .collect::<Vec<_>>();

    identifiers
        .windows(2)
        .filter(|tokens| matches!(tokens[0], "struct" | "enum" | "type") && tokens[1] == name)
        .count()
}

fn rust_named_function_declaration_count(text: &str, name: &str) -> usize {
    let production = rust_sanitized_production_text(text);
    let identifiers = production
        .split(|ch: char| !is_ident_char(ch))
        .filter(|token| !token.is_empty())
        .collect::<Vec<_>>();

    identifiers
        .windows(2)
        .filter(|tokens| tokens[0] == "fn" && tokens[1] == name)
        .count()
}

fn rust_named_declaration_owners(
    sources: &[(String, String)],
    name: &str,
    declaration_count: fn(&str, &str) -> usize,
) -> Vec<String> {
    sources
        .iter()
        .filter_map(|(path, text)| {
            let count = declaration_count(text, name);
            (count > 0).then(|| format!("{path} ({count})"))
        })
        .collect()
}

fn rust_named_const_declaration_count(text: &str, name: &str) -> usize {
    let production = rust_sanitized_production_text(text);
    let identifiers = production
        .split(|ch: char| !is_ident_char(ch))
        .filter(|token| !token.is_empty())
        .collect::<Vec<_>>();

    identifiers
        .windows(2)
        .filter(|tokens| tokens[0] == "const" && tokens[1] == name)
        .count()
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

fn rust_resolve_use_paths(
    path: &[String],
    aliases: &BTreeMap<String, Vec<Vec<String>>>,
    resolving: &mut BTreeSet<String>,
    depth: usize,
) -> Option<Vec<Vec<String>>> {
    if depth > aliases.len() {
        return None;
    }

    let Some(owner_index) = path
        .iter()
        .position(|segment| segment != "self" && segment != "super")
    else {
        return Some(vec![path.to_vec()]);
    };
    let owner = &path[owner_index];
    let Some(targets) = aliases.get(owner) else {
        return Some(vec![path.to_vec()]);
    };
    if !resolving.insert(owner.clone()) {
        return None;
    }

    let mut resolved = BTreeSet::new();
    for target in targets {
        let Some(target_paths) = rust_resolve_use_paths(target, aliases, resolving, depth + 1)
        else {
            continue;
        };
        for mut target_path in target_paths {
            target_path.extend_from_slice(&path[owner_index + 1..]);
            resolved.insert(target_path);
        }
    }
    resolving.remove(owner);
    (!resolved.is_empty()).then(|| resolved.into_iter().collect())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RustRawUseStatement {
    visibility: String,
    path: RustUsePath,
    inline_modules: Vec<String>,
}

fn rust_raw_production_use_statements(text: &str) -> Vec<RustRawUseStatement> {
    let production = rust_sanitized_production_text(text);
    let tokens = rust_use_tokens(&production);
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

        let mut visibility = "private".to_string();
        let mut cursor = index;

        if tokens[cursor] == "pub" {
            visibility = "pub".to_string();
            cursor += 1;
            if tokens.get(cursor).is_some_and(|token| token == "(") {
                let mut depth = 1usize;
                let inner_start = cursor + 1;
                cursor += 1;
                while cursor < tokens.len() && depth > 0 {
                    match tokens[cursor].as_str() {
                        "(" => depth += 1,
                        ")" => depth -= 1,
                        _ => {}
                    }
                    cursor += 1;
                }
                let inner_end = cursor.saturating_sub(1);
                visibility = format!("pub({})", tokens[inner_start..inner_end].join(""));
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
            visibility: visibility.clone(),
            path,
            inline_modules: scope.clone(),
        }));
        index = cursor + 1;
    }

    raw_imports
}

fn rust_production_use_statements(text: &str) -> Vec<String> {
    let raw_imports = rust_raw_production_use_statements(text);

    let mut aliases = BTreeMap::<String, Vec<Vec<String>>>::new();
    for raw in &raw_imports {
        let path = &raw.path;
        let Some(alias) = path.alias.as_ref().filter(|alias| alias.as_str() != "_") else {
            continue;
        };
        let targets = aliases.entry(alias.clone()).or_default();
        if !targets.contains(&path.segments) {
            targets.push(path.segments.clone());
        }
    }

    raw_imports
        .into_iter()
        .flat_map(|raw| {
            let visibility = raw.visibility;
            let path = raw.path;
            let resolved =
                rust_resolve_use_paths(&path.segments, &aliases, &mut BTreeSet::new(), 0)
                    .unwrap_or_else(|| vec![path.segments]);
            let alias = path
                .alias
                .map(|alias| format!(" as {alias}"))
                .unwrap_or_default();
            resolved
                .into_iter()
                .map(|resolved| format!("{visibility}|{}{alias}", resolved.join("::")))
                .collect::<Vec<_>>()
        })
        .collect()
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RustScopedUsePath {
    segments: Vec<String>,
    inline_modules: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RustScopedUseStatement {
    import: String,
    inline_modules: Vec<String>,
}

fn rust_resolve_scoped_use_paths(
    path: &[String],
    inline_modules: &[String],
    aliases: &BTreeMap<String, Vec<RustScopedUsePath>>,
    resolving: &mut BTreeSet<String>,
    depth: usize,
) -> Option<Vec<RustScopedUsePath>> {
    if depth > aliases.len() {
        return None;
    }

    let Some(owner_index) = path
        .iter()
        .position(|segment| segment != "self" && segment != "super")
    else {
        return Some(vec![RustScopedUsePath {
            segments: path.to_vec(),
            inline_modules: inline_modules.to_vec(),
        }]);
    };
    let owner = &path[owner_index];
    let Some(targets) = aliases.get(owner) else {
        return Some(vec![RustScopedUsePath {
            segments: path.to_vec(),
            inline_modules: inline_modules.to_vec(),
        }]);
    };
    if !resolving.insert(owner.clone()) {
        return None;
    }

    let mut resolved = BTreeSet::new();
    for target in targets {
        let Some(target_paths) = rust_resolve_scoped_use_paths(
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
    resolving.remove(owner);
    (!resolved.is_empty()).then(|| resolved.into_iter().collect())
}

fn rust_production_scoped_use_statements(text: &str) -> Vec<RustScopedUseStatement> {
    let raw_imports = rust_raw_production_use_statements(text);
    let mut aliases = BTreeMap::<String, Vec<RustScopedUsePath>>::new();
    for raw in &raw_imports {
        let Some(alias) = raw
            .path
            .alias
            .as_ref()
            .filter(|alias| alias.as_str() != "_")
        else {
            continue;
        };
        let target = RustScopedUsePath {
            segments: raw.path.segments.clone(),
            inline_modules: raw.inline_modules.clone(),
        };
        let targets = aliases.entry(alias.clone()).or_default();
        if !targets.contains(&target) {
            targets.push(target);
        }
    }

    raw_imports
        .into_iter()
        .flat_map(|raw| {
            let resolved = rust_resolve_scoped_use_paths(
                &raw.path.segments,
                &raw.inline_modules,
                &aliases,
                &mut BTreeSet::new(),
                0,
            )
            .unwrap_or_else(|| {
                vec![RustScopedUsePath {
                    segments: raw.path.segments.clone(),
                    inline_modules: raw.inline_modules.clone(),
                }]
            });
            let alias = raw
                .path
                .alias
                .map(|alias| format!(" as {alias}"))
                .unwrap_or_default();
            resolved
                .into_iter()
                .map(|resolved| RustScopedUseStatement {
                    import: format!("{}|{}{alias}", raw.visibility, resolved.segments.join("::")),
                    inline_modules: resolved.inline_modules,
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

type RustScopedAliases = BTreeMap<(Vec<String>, String), Vec<RustScopedUsePath>>;

fn rust_production_scoped_aliases(text: &str) -> RustScopedAliases {
    let mut aliases = RustScopedAliases::new();
    for raw in rust_raw_production_use_statements(text) {
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

fn rust_raw_production_non_use_paths(text: &str) -> Vec<RustScopedUsePath> {
    let production = rust_sanitized_production_text(text);
    let tokens = rust_use_tokens(&production);
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

fn rust_use_visibility(import: &str) -> &str {
    import
        .split_once('|')
        .map(|(visibility, _)| visibility)
        .unwrap_or("private")
}

fn rust_use_path(import: &str) -> &str {
    let path = import
        .split_once('|')
        .map(|(_, path)| path)
        .unwrap_or(import);
    path.split_once(" as ")
        .map(|(path, _)| path)
        .unwrap_or(path)
}

fn rust_use_is_public(import: &str) -> bool {
    rust_use_visibility(import) != "private"
}

fn rust_use_imports_stage(import: &str, stage: &str) -> bool {
    let segments = rust_use_path(import).split("::").collect::<Vec<_>>();
    if segments.windows(2).any(|pair| pair == ["planner", stage]) {
        return true;
    }

    let relative = segments
        .iter()
        .skip_while(|segment| **segment == "self" || **segment == "super")
        .copied()
        .collect::<Vec<_>>();
    relative.first() == Some(&stage)
}

fn rust_use_imports_sql_common(import: &str) -> bool {
    let segments = rust_use_path(import).split("::").collect::<Vec<_>>();
    if segments.windows(2).any(|pair| pair == ["sql", "common"]) {
        return true;
    }
    segments
        .iter()
        .skip_while(|segment| **segment == "self" || **segment == "super")
        .copied()
        .next()
        == Some("common")
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

fn rust_canonical_use_segments_in_scope(
    import: &str,
    source_rel: &str,
    inline_modules: &[String],
) -> Option<Vec<String>> {
    let path = rust_use_path(import)
        .split("::")
        .map(str::to_string)
        .collect::<Vec<_>>();
    rust_canonical_path_segments_in_scope(&path, source_rel, inline_modules)
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

fn rust_canonical_use_segments(import: &str, source_rel: &str) -> Option<Vec<String>> {
    rust_canonical_use_segments_in_scope(import, source_rel, &[])
}

fn rust_production_canonical_paths(text: &str, source_rel: &str) -> Vec<Vec<String>> {
    let mut canonical = rust_production_scoped_use_statements(text)
        .into_iter()
        .filter_map(|import| {
            rust_canonical_use_segments_in_scope(&import.import, source_rel, &import.inline_modules)
        })
        .collect::<BTreeSet<_>>();
    let aliases = rust_production_scoped_aliases(text);
    for path in rust_raw_production_non_use_paths(text) {
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

fn planner_stage_first_dependency_violations_in(source_rel: &str, text: &str) -> Vec<String> {
    fn starts_with(path: &[String], prefix: &[&str]) -> bool {
        path.len() >= prefix.len()
            && path
                .iter()
                .zip(prefix)
                .all(|(segment, expected)| segment == expected)
    }

    let source_rel = source_rel.replace('\\', "/");
    let logical = source_rel.starts_with("src/sql/planner/logical/");
    let physical = source_rel.starts_with("src/sql/planner/physical/");
    let distributed = source_rel.starts_with("src/sql/planner/distributed/");
    let optimizer_bridge = source_rel.starts_with("src/sql/planner/optimizer_bridge/");
    let pipeline = source_rel.starts_with("src/sql/planner/pipeline/");
    let neutral = matches!(
        source_rel.as_str(),
        "src/sql/planner/payload.rs" | "src/sql/planner/ordering.rs"
    );
    if !logical && !physical && !distributed && !optimizer_bridge && !pipeline && !neutral {
        return Vec::new();
    }

    rust_production_canonical_paths(text, &source_rel)
        .into_iter()
        .filter(|path| {
            let planner_logical = starts_with(path, &["crate", "sql", "planner", "logical"]);
            let planner_logical_build =
                starts_with(path, &["crate", "sql", "planner", "logical", "build"]);
            let planner_bridge =
                starts_with(path, &["crate", "sql", "planner", "optimizer_bridge"]);
            let planner_bridge_property = starts_with(
                path,
                &["crate", "sql", "planner", "optimizer_bridge", "property"],
            );
            let planner_physical = starts_with(path, &["crate", "sql", "planner", "physical"]);
            let planner_distributed =
                starts_with(path, &["crate", "sql", "planner", "distributed"]);
            let planner_pipeline = starts_with(path, &["crate", "sql", "planner", "pipeline"]);
            let optimizer = starts_with(path, &["crate", "sql", "optimizer"]);
            let optimizer_options = starts_with(path, &["crate", "sql", "optimizer", "options"]);
            let codegen = starts_with(path, &["crate", "sql", "codegen"]);
            let codegen_helpers = starts_with(path, &["crate", "sql", "codegen", "helpers"]);

            if logical {
                planner_physical
                    || planner_distributed
                    || planner_pipeline
                    || optimizer
                    || (planner_bridge && !planner_bridge_property)
                    || (codegen && !codegen_helpers)
            } else if physical {
                planner_logical
                    || planner_distributed
                    || planner_pipeline
                    || (optimizer && !optimizer_options)
                    || codegen
                    || planner_bridge
            } else if distributed {
                planner_logical_build
                    || planner_pipeline
                    || optimizer
                    || codegen
                    || (planner_bridge && !planner_bridge_property)
            } else if optimizer_bridge {
                planner_distributed || planner_pipeline || codegen
            } else if pipeline {
                planner_logical || planner_bridge || planner_pipeline || optimizer || codegen
            } else {
                planner_logical
                    || planner_bridge
                    || planner_physical
                    || planner_distributed
                    || planner_pipeline
                    || optimizer
                    || codegen
            }
        })
        .map(|path| path.join("::"))
        .collect()
}

fn rust_canonical_path_is_legacy_planner_owner(canonical: &[String]) -> bool {
    let legacy_owners = [
        "plan",
        "physical_vocab",
        "stats",
        "runtime_filter_placement",
    ];
    canonical.len() >= 4
        && canonical[..3] == ["crate", "sql", "planner"]
        && legacy_owners.contains(&canonical[3].as_str())
}

fn rust_canonical_path_is_legacy_distributed_owner(canonical: &[String]) -> bool {
    canonical.len() >= 4
        && canonical[..3] == ["crate", "sql", "planner"]
        && matches!(
            canonical[3].as_str(),
            "distributed_node" | "distributed_fragment"
        )
}

fn rust_canonical_path_is_legacy_distributed_build_owner(canonical: &[String]) -> bool {
    canonical.len() >= 4
        && canonical[..3] == ["crate", "sql", "planner"]
        && (canonical[3] == "distributed_plan_build"
            || (canonical.len() == 4
                && matches!(
                    canonical[3].as_str(),
                    "build_distributed_plan" | "union_distinct_must_be_rewritten_error"
                )))
}

fn distributed_build_surface_violations_in(source_rel: &str, text: &str) -> Vec<String> {
    rust_production_canonical_paths(text, source_rel)
        .into_iter()
        .filter(|path| rust_canonical_path_is_legacy_distributed_build_owner(path))
        .map(|path| path.join("::"))
        .collect()
}

fn top_level_production_function_name(item: &str) -> Option<String> {
    let tokens = rust_use_tokens(item);
    tokens
        .windows(2)
        .find(|tokens| tokens[0] == "fn")
        .map(|tokens| tokens[1].clone())
}

fn top_level_production_function_is_pub_crate(item: &str) -> bool {
    let tokens = rust_use_tokens(item);
    let Some(function_index) = tokens.iter().position(|token| token == "fn") else {
        return false;
    };
    tokens[..function_index].windows(4).any(|window| {
        window[0] == "pub" && window[1] == "(" && window[2] == "crate" && window[3] == ")"
    })
}

fn distributed_build_mod_surface_violations(text: &str) -> Vec<String> {
    let functions = top_level_production_functions(text);
    let mut names = functions
        .iter()
        .filter_map(|function| top_level_production_function_name(function))
        .collect::<Vec<_>>();
    names.sort();
    let mut violations = Vec::new();
    let expected = vec![
        "build_distributed_plan".to_string(),
        "union_distinct_must_be_rewritten_error".to_string(),
    ];
    if names != expected || names.len() != functions.len() {
        violations.push(format!(
            "top-level production functions must be exactly {expected:?}, got {names:?}: {functions:?}"
        ));
    }
    for function in &functions {
        if !top_level_production_function_is_pub_crate(function) {
            violations.push(format!(
                "top-level build function must use pub(crate) visibility: {function}"
            ));
        }
    }
    violations
}

fn rust_canonical_path_is_distributed_build_namespace(canonical: &[String]) -> bool {
    canonical.len() >= 5 && canonical[..5] == ["crate", "sql", "planner", "distributed", "build"]
}

fn planner_root_distributed_build_surface_violations(text: &str) -> Vec<String> {
    rust_production_canonical_paths(text, "src/sql/planner/mod.rs")
        .into_iter()
        .filter(|path| rust_canonical_path_is_distributed_build_namespace(path))
        .map(|path| path.join("::"))
        .collect()
}

#[test]
fn distributed_build_surface_detector_covers_paths_aliases_scopes_and_noise() {
    for source in [
        "use crate::sql::planner::{distributed_plan_build::build_distributed_plan};",
        "use crate::sql::planner::distributed_plan_build as legacy;\nfn f() { legacy::build_distributed_plan(); }",
        "use crate::sql::planner as planner;\nfn f() { planner::build_distributed_plan(); }",
        "fn f() { crate::sql::planner::union_distinct_must_be_rewritten_error(); }",
        "fn f() { super::super::super::planner::build_distributed_plan(); }",
        "mod nested { fn f() { crate::sql::planner::distributed_plan_build::build_distributed_plan(); } }",
    ] {
        assert!(
            !distributed_build_surface_violations_in("src/sql/codegen/ir/explain.rs", source)
                .is_empty(),
            "legacy/grouped/alias/relative/FQ/inline build path must be detected: {source}"
        );
    }

    let noise = r###"
// crate::sql::planner::build_distributed_plan();
const TEXT: &str = "crate::sql::planner::distributed_plan_build";
#[cfg(test)]
mod tests {
    use crate::sql::planner::build_distributed_plan;
}
"###;
    assert!(
        distributed_build_surface_violations_in("src/sql/codegen/ir/explain.rs", noise).is_empty(),
        "comments, strings, and cfg(test) items must be ignored"
    );
}

#[test]
fn distributed_build_mod_surface_detector_rejects_extra_and_non_crate_functions() {
    let valid = r#"
mod fragment_cut;
mod lowering;
mod runtime_filter_binding;

pub(crate) fn build_distributed_plan() {}
pub(crate) fn union_distinct_must_be_rewritten_error() {}

#[cfg(test)]
fn test_only_helper() {}
"#;
    assert!(
        distributed_build_mod_surface_violations(valid).is_empty(),
        "exact build surface must be accepted"
    );

    for invalid in [
        format!("{valid}\nfn extra_private_helper() {{}}"),
        format!("{valid}\npub(crate) fn extra_public_helper() {{}}"),
        valid.replacen(
            "pub(crate) fn build_distributed_plan",
            "fn build_distributed_plan",
            1,
        ),
        valid.replacen(
            "pub(crate) fn union_distinct_must_be_rewritten_error",
            "pub(super) fn union_distinct_must_be_rewritten_error",
            1,
        ),
    ] {
        assert!(
            !distributed_build_mod_surface_violations(&invalid).is_empty(),
            "extra function or non-pub(crate) entry must be rejected: {invalid}"
        );
    }
}

#[test]
fn distributed_build_owner_detector_includes_non_owner_planner_duplicates() {
    let sources = vec![
        (
            "src/sql/planner/distributed/build/lowering.rs".to_string(),
            "struct NodeIdAllocator; fn lower_fragment_local_node() {}".to_string(),
        ),
        (
            "src/sql/planner/physical/node.rs".to_string(),
            "struct NodeIdAllocator; fn lower_fragment_local_node() {}".to_string(),
        ),
        (
            "src/sql/planner/logical/node.rs".to_string(),
            "#[cfg(test)] struct NodeIdAllocator; #[cfg(test)] fn lower_fragment_local_node() {}"
                .to_string(),
        ),
    ];

    assert_eq!(
        rust_named_declaration_owners(
            &sources,
            "NodeIdAllocator",
            rust_named_type_declaration_count,
        ),
        vec![
            "src/sql/planner/distributed/build/lowering.rs (1)",
            "src/sql/planner/physical/node.rs (1)",
        ],
        "type duplicates outside the concern owner must be visible"
    );
    assert_eq!(
        rust_named_declaration_owners(
            &sources,
            "lower_fragment_local_node",
            rust_named_function_declaration_count,
        ),
        vec![
            "src/sql/planner/distributed/build/lowering.rs (1)",
            "src/sql/planner/physical/node.rs (1)",
        ],
        "function duplicates outside the concern owner must be visible"
    );
}

#[test]
fn planner_root_distributed_build_surface_detector_rejects_forwarding_paths() {
    for source in [
        "use crate::sql::planner::distributed::{build};",
        "use crate::sql::planner::distributed::build as builder;\npub(crate) fn forward() { builder::build_distributed_plan(); }",
        "pub(crate) fn forward() { crate::sql::planner::distributed::build::build_distributed_plan(); }",
        "pub(crate) fn forward() { distributed::build::build_distributed_plan(); }",
        "mod facade { pub(crate) fn forward() { super::distributed::build::build_distributed_plan(); } }",
        "use self::distributed::build::build_distributed_plan as inner;\npub(crate) fn differently_named_wrapper() { inner(); }",
    ] {
        assert!(
            !planner_root_distributed_build_surface_violations(source).is_empty(),
            "planner-root import/call/forwarder must be rejected: {source}"
        );
    }

    let noise = r###"
// distributed::build::build_distributed_plan();
const TEXT: &str = "crate::sql::planner::distributed::build";
#[cfg(test)]
mod tests {
    use super::distributed::build::build_distributed_plan;
}
pub(crate) use logical::build::{plan_output_columns, plan_query};
"###;
    assert!(
        planner_root_distributed_build_surface_violations(noise).is_empty(),
        "comments, strings, cfg(test), and unrelated paths must be ignored"
    );
}

fn rust_canonical_path_is_planner_root_distributed_core(canonical: &[String]) -> bool {
    let distributed_core = [
        "DataPartition",
        "DataSink",
        "DistributedNode",
        "DistributedNodeKind",
        "DistributedPlan",
        "ExchangeFlavor",
        "ExchangeReceiver",
        "FragmentEdge",
        "FragmentEdgeKind",
        "FragmentId",
        "FragmentStreamKind",
        "PartitionKind",
        "PlanFragment",
        "distributed_kind_from_physical",
        "distributed_kind_to_physical",
    ];
    canonical.len() == 4
        && canonical[..3] == ["crate", "sql", "planner"]
        && distributed_core.contains(&canonical[3].as_str())
}

fn rust_canonical_path_is_codegen_edge_topology(canonical: &[String]) -> bool {
    let edge_topology = [
        "FragmentEdge",
        "FragmentEdgeKind",
        "FragmentId",
        "FragmentStreamKind",
    ];
    canonical.len() == 4
        && canonical[..3] == ["crate", "sql", "codegen"]
        && edge_topology.contains(&canonical[3].as_str())
}

fn rust_canonical_path_is_legacy_distributed_write_owner(canonical: &[String]) -> bool {
    canonical.len() >= 4
        && canonical[..3] == ["crate", "sql", "planner"]
        && matches!(
            canonical[3].as_str(),
            "write_plan" | "write_sink" | "change_stream_write"
        )
}

fn rust_canonical_path_is_planner_root_distributed_write_item(canonical: &[String]) -> bool {
    let write_items = [
        "IcebergWriteSinkSpec",
        "IcebergWriteSinkMode",
        "IcebergWriteFileCompression",
        "IcebergWriteFragmentSink",
        "IcebergWriteInputBinding",
        "ChangeStreamWriteBranchSpec",
        "ChangeStreamWriteDagSpec",
        "IcebergChangeStreamRouterSink",
        "IcebergChangeStreamBranchRoute",
        "IcebergChangeStreamWriteTopology",
        "IcebergChangeStreamWriterBranch",
        "PlannedIcebergChangeStreamDistributedPlan",
        "with_iceberg_write_sink",
        "with_iceberg_change_stream_write",
        "synthetic_iceberg_write_table_id",
        "transform_to_sink_string",
    ];
    canonical.len() == 4
        && canonical[..3] == ["crate", "sql", "planner"]
        && write_items.contains(&canonical[3].as_str())
}

fn rust_canonical_path_is_forbidden_distributed_write_dependency(canonical: &[String]) -> bool {
    canonical.starts_with(&["crate".to_string(), "engine".to_string()])
        || canonical.starts_with(&[
            "crate".to_string(),
            "sql".to_string(),
            "optimizer".to_string(),
        ])
        || canonical.starts_with(&[
            "crate".to_string(),
            "sql".to_string(),
            "codegen".to_string(),
        ])
        || canonical.starts_with(&[
            "crate".to_string(),
            "sql".to_string(),
            "planner".to_string(),
            "logical".to_string(),
            "build".to_string(),
        ])
}

fn rust_scoped_use_violates_distributed_write_owner(
    import: &RustScopedUseStatement,
    source_rel: &str,
) -> bool {
    let Some(canonical) =
        rust_canonical_use_segments_in_scope(&import.import, source_rel, &import.inline_modules)
    else {
        return false;
    };
    rust_canonical_path_is_legacy_distributed_write_owner(&canonical)
        || rust_canonical_path_is_planner_root_distributed_write_item(&canonical)
}

fn rust_scoped_use_violates_distributed_owner(
    import: &RustScopedUseStatement,
    source_rel: &str,
) -> bool {
    let Some(canonical) =
        rust_canonical_use_segments_in_scope(&import.import, source_rel, &import.inline_modules)
    else {
        return false;
    };
    rust_canonical_path_is_legacy_distributed_owner(&canonical)
        || rust_canonical_path_is_planner_root_distributed_core(&canonical)
        || rust_canonical_path_is_codegen_edge_topology(&canonical)
}

fn rust_use_imports_legacy_planner_owner(import: &str, source_rel: &str) -> bool {
    let Some(canonical) = rust_canonical_use_segments(import, source_rel) else {
        return false;
    };
    rust_canonical_path_is_legacy_planner_owner(&canonical)
}

fn rust_scoped_use_imports_legacy_planner_owner(
    import: &RustScopedUseStatement,
    source_rel: &str,
) -> bool {
    let Some(canonical) =
        rust_canonical_use_segments_in_scope(&import.import, source_rel, &import.inline_modules)
    else {
        return false;
    };
    rust_canonical_path_is_legacy_planner_owner(&canonical)
}

fn rust_use_leaf(import: &str) -> &str {
    rust_use_path(import).rsplit("::").next().unwrap_or("")
}

#[derive(Default)]
struct RustStructuralLine {
    brace_delta: isize,
    has_open_brace: bool,
    ends_with_semicolon: bool,
}

fn rust_structural_line(line: &str, attribute_depth: &mut usize) -> RustStructuralLine {
    let bytes = line.as_bytes();
    let mut structure = RustStructuralLine::default();
    let mut last_code_byte = None;
    let mut index = 0usize;

    while index < bytes.len() {
        if *attribute_depth > 0 {
            match bytes[index] {
                b'[' => *attribute_depth += 1,
                b']' => *attribute_depth -= 1,
                _ => {}
            }
            index += 1;
            continue;
        }
        if let Some(open_len) = rust_attribute_open_len(bytes, index) {
            *attribute_depth = 1;
            index += open_len;
            continue;
        }

        match bytes[index] {
            b'{' => {
                structure.brace_delta += 1;
                structure.has_open_brace = true;
            }
            b'}' => structure.brace_delta -= 1,
            _ => {}
        }
        if !bytes[index].is_ascii_whitespace() {
            last_code_byte = Some(bytes[index]);
        }
        index += 1;
    }

    structure.ends_with_semicolon = last_code_byte == Some(b';');
    structure
}

fn top_level_production_functions(text: &str) -> Vec<String> {
    let production = rust_lexically_sanitized(text);
    let mut functions = Vec::new();
    let mut header = String::new();
    let mut header_start = 0usize;
    let mut header_is_function = false;
    let mut brace_depth = 0isize;
    let mut attribute_depth = 0usize;

    for (index, line) in production.lines().enumerate() {
        let trimmed = line.trim();
        let structure = rust_structural_line(line, &mut attribute_depth);
        if brace_depth == 0 && !trimmed.is_empty() {
            if header.is_empty() {
                header_start = index + 1;
            } else {
                header.push(' ');
            }
            header.push_str(trimmed);

            if !header_is_function && is_rust_function_item_header(&header) {
                if !rust_header_has_cfg_test_attribute(&header) {
                    functions.push(format!("{}: {}", header_start, header));
                }
                header_is_function = true;
            }

            if attribute_depth == 0 && (structure.ends_with_semicolon || structure.has_open_brace) {
                header.clear();
                header_is_function = false;
            }
        }

        brace_depth += structure.brace_delta;
        if brace_depth < 0 {
            brace_depth = 0;
        }
    }

    functions
}

#[test]
fn planner_logical_builder_surface_detector_rejects_extra_reexport() {
    let with_extra_reexport = r#"
mod aggregate;
mod output;
mod query;
mod relation;
mod select;
mod subquery;
mod window;

pub(crate) use output::plan_output_columns;
pub(crate) use query::plan_query;
pub(crate) use relation::plan_values;

#[cfg(test)]
mod tests;
"#;

    let violations = logical_build_surface_violations(with_extra_reexport);
    assert!(
        violations
            .iter()
            .any(|violation| violation.contains("pub(crate) use relation::plan_values;")),
        "extra logical build re-export must be rejected: {violations:?}"
    );
}

#[test]
fn planner_stage_first_root_surface_detector_rejects_drift() {
    let valid = r#"
//! Planner facade.
pub(crate) mod distributed;
pub(crate) mod imv_rewrite;
pub(crate) mod logical;
pub(crate) mod optimizer_bridge;
pub(crate) mod ordering;
pub(crate) mod payload;
pub(crate) mod physical;
pub(crate) mod pipeline;
pub(crate) use logical::build::{plan_output_columns, plan_query};

#[cfg(test)]
fn test_helper() {}
#[allow(dead_code)]
#[cfg(all(feature = "compat", test))]
fn reordered_test_helper() {}
"#;
    assert!(
        planner_root_surface_violations_in(valid).is_empty(),
        "the exact planner root surface must be accepted"
    );

    let invalid = [
        format!("{valid}\nmod extra;"),
        valid.replace("pub(crate) mod pipeline;", "mod pipeline;"),
        valid.replace("pub(crate) mod physical;", "mod physical;"),
        valid.replace("pub(crate) mod physical;", "pub(super) mod physical;"),
        format!("{valid}\npub(crate) use physical::PhysicalPlanNode;"),
        format!("{valid}\npub(crate) use distributed::DistributedPlan;"),
        format!("{valid}\npub(crate) fn plan() {{}}"),
        format!("{valid}\npub(crate) struct Wrapper;"),
        format!("{valid}\npub(crate) enum Kind {{ One }}"),
        format!("{valid}\npub(crate) type Alias = ();"),
        format!("{valid}\npub(crate) const VALUE: usize = 1;"),
        format!("{valid}\npub(crate) trait Marker {{}}"),
        format!("{valid}\nimpl Marker for Wrapper {{}}"),
        format!("{valid}\n#[cfg(test_feature)] fn production_wrapper() {{}}"),
        valid.replace(
            "pub(crate) mod physical;",
            "#[path = \"physical.rs\"] pub(crate) mod physical;",
        ),
    ];
    for source in invalid {
        assert!(
            !planner_root_surface_violations_in(&source).is_empty(),
            "planner root drift must be rejected: {source}"
        );
    }
}

#[test]
fn planner_path_module_attribute_detector_rejects_bypasses() {
    let invalid = [
        "#[path = \"legacy.rs\"] mod legacy;",
        "#[path = \"legacy.rs\"] pub(crate)\nmod legacy;",
        "mod stage { #[cfg_attr(feature = \"compat\", path = \"legacy.rs\")] mod legacy; }",
        "#[cfg_attr(any(feature = \"a\", feature = \"b\"), allow(dead_code), path = \"legacy.rs\")] pub(super) mod legacy;",
        "#[cfg_attr(feature = \"compat\", cfg_attr(feature = \"nested\", path = \"legacy.rs\"))] mod nested_legacy;",
        "#[cfg(test_feature)] #[path = \"legacy.rs\"] mod production_legacy;",
        "#[path = \"redirected\"] mod redirected_inline { mod hidden; }",
    ];
    for source in invalid {
        let violations = planner_path_module_attribute_violations_in(source);
        assert!(
            !violations.is_empty(),
            "path-affecting production module attribute must be rejected: {source}"
        );
    }

    let valid = r###"
mod ordinary;
#[cfg(test)]
#[path = "test_fixture.rs"]
mod test_fixture;
#[allow(dead_code)]
#[cfg(all(feature = "compat", test))]
#[path = "test_fixture.rs"]
mod reordered_test_fixture;
#[cfg_attr(feature = "compat", doc = "path = fake.rs")]
mod documented;
#[cfg_attr(path, allow(dead_code))]
mod predicate_only;
#[cfg_attr(feature = "compat", derive(path::Trait))]
mod derived;
// #[path = "comment.rs"] mod comment;
const TEXT: &str = "#[path = fake.rs] mod string_fake;";
const RAW: &str = r#"#[cfg_attr(x, path = fake.rs)] mod raw_fake;"#;
"###;
    let violations = planner_path_module_attribute_violations_in(valid);
    assert!(
        violations.is_empty(),
        "ordinary modules, cfg-test items, and lexical noise must be accepted: {violations:?}"
    );
}

#[test]
fn planner_root_function_detector_covers_visibility_and_qualifiers() {
    let function_items = [
        ("private", "fn private() {}"),
        ("pub", "pub fn public() {}"),
        ("pub(crate) async", "pub(crate) async fn crate_async() {}"),
        (
            "pub(super) unsafe",
            "pub(super) unsafe fn parent_unsafe() {}",
        ),
        ("pub(self) const", "pub(self) const fn self_const() {}"),
        (
            "pub(in) const unsafe extern",
            "pub(in crate) const unsafe extern \"C\" fn restricted() {}",
        ),
        ("private extern", "extern \"C\" fn private_extern() {}"),
        (
            "multiline qualifiers",
            "pub(crate)\nconst fn multiline() {}",
        ),
        ("attribute", "#[inline]\npub fn attributed() {}"),
        ("lifetime", "fn with_lifetime<'a>() {}"),
    ];

    for (label, source) in function_items {
        let hits = top_level_production_functions(source);
        assert_eq!(hits.len(), 1, "{label} must be detected: {hits:?}");
    }

    for source in [
        "type Handler = fn();",
        "const HANDLER: fn() = private;",
        "mod child { fn nested() {} }",
        "extern \"C\" { fn declared(); }",
    ] {
        let hits = top_level_production_functions(source);
        assert!(
            hits.is_empty(),
            "non-function item must not be flagged: {hits:?}"
        );
    }

    let cfg_test = "#[cfg(test)]\nfn test_only() {}";
    assert!(top_level_production_functions(cfg_test).is_empty());
}

#[test]
fn planner_root_function_detector_ignores_braces_in_lexical_noise() {
    let cases = [
        ("const string", "const TEXT: &str = \"{\";\nfn real() {}"),
        (
            "ordinary string",
            "static TEXT: &str = \"{\";\nfn real() {}",
        ),
        ("byte string", "const BYTES: &[u8] = b\"{\";\nfn real() {}"),
        (
            "raw string",
            "const RAW: &str = r###\"{\"###;\nfn real() {}",
        ),
        ("char", "const OPEN: char = '{';\nfn real() {}"),
        ("line comment", "// {\nfn real() {}"),
        ("block comment", "/* { */\nfn real() {}"),
        ("nested block comment", "/* { /* } */ { */\nfn real() {}"),
        ("byte char", "const OPEN: u8 = b'{';\nfn real() {}"),
        (
            "raw byte string",
            "const RAW: &[u8] = br###\"{\"###;\nfn real() {}",
        ),
        (
            "escaped quote and backslash",
            r#"const TEXT: &str = "\"{\\}";
fn real() {}"#,
        ),
    ];

    for (label, source) in cases {
        let hits = top_level_production_functions(source);
        assert_eq!(hits.len(), 1, "{label} must not hide real fn: {hits:?}");
        assert!(hits[0].contains("fn real"), "{label}: {hits:?}");
    }
}

#[test]
fn planner_root_function_detector_ignores_fake_functions_in_comments() {
    let source = r#"
/*
fn fake_block() {}
*/
// fn fake_line() {}
"#;

    let hits = top_level_production_functions(source);
    assert!(
        hits.is_empty(),
        "functions in comments must not be flagged: {hits:?}"
    );
}

#[test]
fn planner_root_function_detector_handles_multiline_attributes() {
    let source = r#"
#[allow(
    dead_code,
    clippy::missing_safety_doc,
)]
pub unsafe fn attributed() {}
"#;

    let hits = top_level_production_functions(source);
    assert_eq!(
        hits.len(),
        1,
        "multiline attribute must preserve fn: {hits:?}"
    );
    assert!(hits[0].contains("fn attributed"), "{hits:?}");
}

#[test]
fn planner_root_function_detector_excludes_cfg_test_with_multiline_attribute() {
    let source = r#"
#[cfg(
    test
)]
#[allow(
    dead_code,
)]
fn test_only() {}

fn production() {}
"#;

    let hits = top_level_production_functions(source);
    assert_eq!(hits.len(), 1, "only production fn must remain: {hits:?}");
    assert!(hits[0].contains("fn production"), "{hits:?}");
}

#[test]
fn planner_root_function_detector_handles_inner_attributes() {
    let source = "#![allow(dead_code)]\nfn leaked() {}";

    let hits = top_level_production_functions(source);
    assert_eq!(hits.len(), 1, "inner attribute must preserve fn: {hits:?}");
    assert!(hits[0].contains("fn leaked"), "{hits:?}");
}

#[test]
fn planner_root_function_detector_treats_inner_cfg_test_conservatively() {
    let source = "#![cfg(test)]\nfn conservatively_production() {}";

    let hits = top_level_production_functions(source);
    assert_eq!(
        hits.len(),
        1,
        "module cfg(test) must not suppress production guard: {hits:?}"
    );
    assert!(hits[0].contains("fn conservatively_production"), "{hits:?}");
}

#[test]
fn planner_root_function_detector_ignores_inner_attribute_structure() {
    let source = r#"
#![guard(
    { dead_code }
)]
fn after_inner_attribute() {}
"#;

    let hits = top_level_production_functions(source);
    assert_eq!(
        hits.len(),
        1,
        "inner attribute tokens must not change brace depth: {hits:?}"
    );
    assert!(hits[0].contains("fn after_inner_attribute"), "{hits:?}");
}

#[test]
fn planner_ownership_guard_sanitizes_before_stripping_cfg_test() {
    let source = r#"
#[cfg(test)]
mod tests {
    const BRACE: &str = "}";
    struct Hidden;
}

struct Visible;
"#;

    assert_eq!(rust_named_type_declaration_count(source, "Hidden"), 0);
    assert_eq!(rust_named_type_declaration_count(source, "Visible"), 1);
}

#[test]
fn planner_ownership_use_parser_preserves_visibility() {
    let source = r#"
use private_owner::PrivateItem;
pub use public_owner::PublicItem;
pub(crate) use crate_owner::CrateItem;
pub(crate)
use split_crate_owner::SplitCrateItem;
pub(in crate::sql)
use split_in_owner::SplitInItem;
"#;

    assert_eq!(
        rust_production_use_statements(source),
        vec![
            "private|private_owner::PrivateItem",
            "pub|public_owner::PublicItem",
            "pub(crate)|crate_owner::CrateItem",
            "pub(crate)|split_crate_owner::SplitCrateItem",
            "pub(incrate::sql)|split_in_owner::SplitInItem",
        ]
    );
}

#[test]
fn planner_ownership_use_parser_expands_grouped_and_relative_trees() {
    let source = r#"
use crate::sql::planner::plan::PlanScanNode;
use crate::sql::planner::plan::{PlanFilterNode, PlanProjectNode as Project};
use crate::sql::planner::{
    plan::{PlanSortNode, PlanLimitNode},
    payload::PlanValuesNode,
};
use super::{logical::LogicalPlanNode, distributed::*};
use self::plan::*;
"#;

    assert_eq!(
        rust_production_use_statements(source),
        vec![
            "private|crate::sql::planner::plan::PlanScanNode",
            "private|crate::sql::planner::plan::PlanFilterNode",
            "private|crate::sql::planner::plan::PlanProjectNode as Project",
            "private|crate::sql::planner::plan::PlanSortNode",
            "private|crate::sql::planner::plan::PlanLimitNode",
            "private|crate::sql::planner::payload::PlanValuesNode",
            "private|super::logical::LogicalPlanNode",
            "private|super::distributed::*",
            "private|self::plan::*",
        ]
    );
}

#[test]
fn planner_ownership_use_parser_resolves_module_alias_chains() {
    let source = r#"
use crate::sql::planner::payload as shared;
pub(crate) use self::shared::PlanScanNode;
use crate::sql::planner as p;
use p::plan as legacy;
use self::legacy::*;
use crate::sql::planner::{self as grouped, plan::PhysicalPlanKind};
use grouped::plan::*;
"#;

    assert_eq!(
        rust_production_use_statements(source),
        vec![
            "private|crate::sql::planner::payload as shared",
            "pub(crate)|crate::sql::planner::payload::PlanScanNode",
            "private|crate::sql::planner as p",
            "private|crate::sql::planner::plan as legacy",
            "private|crate::sql::planner::plan::*",
            "private|crate::sql::planner as grouped",
            "private|crate::sql::planner::plan::PhysicalPlanKind",
            "private|crate::sql::planner::plan::*",
        ]
    );
}

#[test]
fn planner_ownership_policy_rejects_module_alias_bypasses() {
    let plan_imports = rust_production_use_statements(
        r#"
use crate::sql::planner::payload as shared;
pub(crate) use self::shared::PlanScanNode;
"#,
    );
    assert!(plan_imports.iter().any(|import| {
        rust_use_imports_stage(import, "logical")
            || (rust_use_is_public(import)
                && (rust_use_imports_stage(import, "payload")
                    || rust_use_imports_sql_common(import)))
    }));

    for source in [
        r#"
use crate::sql::planner::plan as legacy;
use self::legacy::*;
"#,
        r#"
use crate::sql::planner as p;
use p::plan::*;
"#,
    ] {
        let imports = rust_production_use_statements(source);
        assert!(imports.iter().any(|import| {
            rust_use_imports_stage(import, "plan") && rust_use_leaf(import) == "*"
        }));
    }
}

#[test]
fn planner_ownership_use_parser_stops_on_alias_cycles() {
    let source = r#"
use self::b as a;
use self::a as b;
use self::a::PlanScanNode;
"#;

    assert_eq!(
        rust_production_use_statements(source),
        vec![
            "private|self::b as a",
            "private|self::a as b",
            "private|self::a::PlanScanNode",
        ]
    );
}

#[test]
fn planner_ownership_use_parser_resolves_relative_module_alias_target() {
    let source = r#"
use super as parent;
use parent::plan::*;
"#;

    assert_eq!(
        rust_production_use_statements(source),
        vec!["private|super as parent", "private|super::plan::*",]
    );
}

#[test]
fn planner_ownership_use_parser_conservatively_expands_cross_scope_alias_reuse() {
    let sources = [
        r#"
mod legacy_scope {
    use crate::sql::planner::plan as owner;
    use owner::*;
    use owner as stage;
    use stage::*;
}
mod shared_scope {
    use crate::sql::planner::payload as owner;
}
"#,
        r#"
mod shared_scope {
    use crate::sql::planner::payload as owner;
}
mod legacy_scope {
    use crate::sql::planner::plan as owner;
    use owner::*;
    use owner as stage;
    use stage::*;
}
"#,
    ];

    let actual = sources
        .into_iter()
        .map(|source| {
            let imports = rust_production_use_statements(source);
            let plan_wildcards = imports
                .iter()
                .filter(|import| rust_use_path(import) == "crate::sql::planner::plan::*")
                .count();
            let payload_wildcards = imports
                .iter()
                .filter(|import| rust_use_path(import) == "crate::sql::planner::payload::*")
                .count();
            let stage_targets = imports
                .iter()
                .filter(|import| import.ends_with(" as stage"))
                .map(|import| rust_use_path(import).to_string())
                .collect::<BTreeSet<_>>();
            (plan_wildcards, payload_wildcards, stage_targets)
        })
        .collect::<Vec<_>>();
    let expected_targets = BTreeSet::from([
        "crate::sql::planner::payload".to_string(),
        "crate::sql::planner::plan".to_string(),
    ]);

    assert_eq!(
        actual,
        vec![(2, 2, expected_targets.clone()), (2, 2, expected_targets),],
        "the boundary guard conservatively expands every cross-scope alias target; this may restrict legal same-name alias reuse, but a forbidden owner must never be missed"
    );
}

#[test]
fn planner_logical_module_use_surface_is_closed() {
    let expected = vec!["pub(crate)|node::*"];
    assert_eq!(
        rust_production_use_statements("pub(crate) use node::*;"),
        expected
    );

    for source in [
        "pub(crate) use node::*;\npub(crate) use crate::sql::planner::payload::*;",
        "pub(crate) use node::*;\nuse super::plan::*;",
        "pub(crate) use node::*;\nuse crate::sql::planner::{logical::*, payload::*};",
    ] {
        assert_ne!(rust_production_use_statements(source), expected);
    }
}

#[test]
fn planner_physical_legacy_owner_detector_distinguishes_sibling_and_unrelated_paths() {
    assert!(!rust_use_imports_legacy_planner_owner(
        "private|crate::proto::plan::plan_node::Kind",
        "src/sql/codegen/proto_encode/plan.rs",
    ));
    assert!(rust_use_imports_legacy_planner_owner(
        "private|crate::sql::planner::plan::PhysicalPlanNode",
        "src/sql/codegen/ir/explain.rs",
    ));
    assert!(!rust_use_imports_legacy_planner_owner(
        "pub(crate)|stats::*",
        "src/sql/planner/physical/mod.rs",
    ));
    assert!(!rust_use_imports_legacy_planner_owner(
        "private|super::stats::PhysicalPlanStats",
        "src/sql/planner/physical/node.rs",
    ));
    assert!(rust_use_imports_legacy_planner_owner(
        "private|crate::sql::planner::stats::PhysicalPlanStats",
        "src/sql/planner/physical/node.rs",
    ));
    assert!(rust_use_imports_legacy_planner_owner(
        "private|super::stats::PhysicalPlanStats",
        "src/sql/planner/distributed_node.rs",
    ));
    assert!(!rust_use_imports_legacy_planner_owner(
        "private|super::stats::PhysicalPlanStats",
        "src/sql/planner/optimizer_bridge/foo.rs",
    ));
    assert!(rust_use_imports_legacy_planner_owner(
        "private|super::super::stats::PhysicalPlanStats",
        "src/sql/planner/physical/node.rs",
    ));
    assert!(!rust_use_imports_legacy_planner_owner(
        "private|super::super::stats::PhysicalPlanStats",
        "src/sql/planner/physical/nested/foo.rs",
    ));
    assert!(rust_use_imports_legacy_planner_owner(
        "private|super::super::super::stats::PhysicalPlanStats",
        "src/sql/planner/physical/nested/foo.rs",
    ));
    assert!(rust_use_imports_legacy_planner_owner(
        "private|stats::*",
        "src/sql/planner/mod.rs",
    ));
    assert!(!rust_use_imports_legacy_planner_owner(
        "private|stats::*",
        "src/sql/planner/physical/mod.rs",
    ));

    let allowed_alias_chain = rust_production_use_statements(
        "use super::{super::{stats as stage_stats}};\n\
         use stage_stats as stats_owner;\n\
         use stats_owner::*;",
    );
    assert!(
        !allowed_alias_chain.iter().any(|import| {
            rust_use_imports_legacy_planner_owner(import, "src/sql/planner/physical/nested/foo.rs")
        }),
        "physical sibling stats alias chain must remain allowed: {allowed_alias_chain:?}"
    );

    let forbidden_alias_chain = rust_production_use_statements(
        "use super::{super::{super::{stats as root_stats}}};\n\
         use root_stats as stats_owner;\n\
         use stats_owner::*;",
    );
    assert!(
        forbidden_alias_chain.iter().any(|import| {
            rust_use_imports_legacy_planner_owner(import, "src/sql/planner/physical/nested/foo.rs")
        }),
        "planner-root stats alias chain must be rejected: {forbidden_alias_chain:?}"
    );
}

#[test]
fn planner_physical_legacy_owner_detector_tracks_inline_module_scope() {
    let source_rel = "src/sql/planner/physical/node.rs";
    let allowed =
        rust_production_scoped_use_statements("mod nested { use super::super::stats::X; }");
    assert_eq!(allowed.len(), 1, "{allowed:?}");
    assert_eq!(allowed[0].inline_modules, vec!["nested"]);
    assert!(
        !allowed
            .iter()
            .any(|import| { rust_scoped_use_imports_legacy_planner_owner(import, source_rel) }),
        "inline nested module must resolve two super segments to physical::stats: {allowed:?}"
    );

    let forbidden =
        rust_production_scoped_use_statements("mod nested { use super::super::super::stats::X; }");
    assert!(
        forbidden
            .iter()
            .any(|import| { rust_scoped_use_imports_legacy_planner_owner(import, source_rel) }),
        "inline nested module must resolve three super segments to planner::stats: {forbidden:?}"
    );

    let allowed_alias_chain = rust_production_scoped_use_statements(
        r#"
mod nested {
    use super::{super::{stats as stage_stats}};
    use stage_stats as stats_owner;
    use stats_owner::X;
}
"#,
    );
    assert!(
        !allowed_alias_chain
            .iter()
            .any(|import| { rust_scoped_use_imports_legacy_planner_owner(import, source_rel) }),
        "grouped alias targets must resolve relative to their inline declaration scope: {allowed_alias_chain:?}"
    );

    let forbidden_alias_chain = rust_production_scoped_use_statements(
        r#"
mod nested {
    use super::{super::{super::{stats as root_stats}}};
    use root_stats as stats_owner;
    use stats_owner::X;
}
"#,
    );
    assert!(
        forbidden_alias_chain
            .iter()
            .any(|import| { rust_scoped_use_imports_legacy_planner_owner(import, source_rel) }),
        "grouped alias targets that reach planner::stats must be rejected: {forbidden_alias_chain:?}"
    );
}

#[test]
fn planner_distributed_owner_detector_covers_paths_aliases_scopes_and_tests() {
    let source_rel = "src/sql/planner/distributed/node.rs";
    for source in [
        "use crate::sql::planner::DistributedPlan;",
        "use crate::sql::planner::{DataPartition, DistributedPlan};",
        "use crate::sql::planner as planner_root; use planner_root::DistributedPlan;",
        "use super::super::distributed_node::DistributedNode;",
        "mod nested { use super::super::super::distributed_fragment::DataPartition; }",
        "use crate::sql::codegen::{FragmentEdge, FragmentId};",
    ] {
        let imports = rust_production_scoped_use_statements(source);
        assert!(
            imports
                .iter()
                .any(|import| rust_scoped_use_violates_distributed_owner(import, source_rel)),
            "distributed owner bypass must be rejected: {source}\n{imports:?}"
        );
    }

    for source in [
        "use super::fragment::DataPartition;",
        "use crate::sql::planner::distributed::{DataPartition, DistributedPlan};",
        "use crate::proto::plan::distributed_node::Node;",
        "mod nested { use super::super::fragment::DataPartition; }",
        "#[cfg(test)] mod tests { use crate::sql::planner::DistributedPlan; }",
    ] {
        let imports = rust_production_scoped_use_statements(source);
        assert!(
            !imports
                .iter()
                .any(|import| rust_scoped_use_violates_distributed_owner(import, source_rel)),
            "stage-owned, unrelated, or test-only path must remain allowed: {source}\n{imports:?}"
        );
    }
}

#[test]
fn planner_distributed_write_owner_detector_covers_paths_aliases_scopes_and_tests() {
    let source_rel = "src/sql/planner/distributed/write/plan.rs";
    for source in [
        "use crate::sql::planner::{write_sink::IcebergWriteSinkSpec, IcebergWriteFragmentSink};",
        concat!(
            "use crate::sql::planner::",
            "write_plan as plan_owner; use plan_owner::with_iceberg_write_sink;"
        ),
        "use super::super::super::write_sink::IcebergWriteInputBinding;",
        concat!(
            "type Leak = crate::sql::planner::",
            "write_sink::IcebergWriteSinkMode;"
        ),
        concat!(
            "mod nested { use crate::sql::planner::",
            "change_stream_write::ChangeStreamWriteDagSpec; }"
        ),
    ] {
        let imports = rust_production_scoped_use_statements(source);
        let paths = rust_production_canonical_paths(source, source_rel);
        assert!(
            imports.iter().any(|import| {
                rust_scoped_use_violates_distributed_write_owner(import, source_rel)
            }) || paths.iter().any(|path| {
                rust_canonical_path_is_legacy_distributed_write_owner(path)
                    || rust_canonical_path_is_planner_root_distributed_write_item(path)
            }),
            "distributed write owner bypass must be rejected: {source}\n{imports:?}\n{paths:?}"
        );
    }

    for source in [
        "use super::sink::IcebergWriteSinkSpec;",
        "use crate::sql::planner::distributed::write::change_stream::ChangeStreamWriteDagSpec;",
        concat!(
            "#[cfg(test)] mod tests { use crate::sql::planner::",
            "write_sink::IcebergWriteSinkSpec; }"
        ),
        concat!(
            "#[cfg(test)] type TestOnly = crate::sql::planner::",
            "IcebergWriteFragmentSink;"
        ),
    ] {
        let imports = rust_production_scoped_use_statements(source);
        let paths = rust_production_canonical_paths(source, source_rel);
        assert!(
            !imports.iter().any(|import| {
                rust_scoped_use_violates_distributed_write_owner(import, source_rel)
            }) && !paths.iter().any(|path| {
                rust_canonical_path_is_legacy_distributed_write_owner(path)
                    || rust_canonical_path_is_planner_root_distributed_write_item(path)
            }),
            "stage-owned or test-only write path must remain allowed: {source}\n{imports:?}\n{paths:?}"
        );
    }
}

#[test]
fn planner_distributed_write_dependency_detector_covers_aliases_and_fully_qualified_paths() {
    let source_rel = "src/sql/planner/distributed/write/plan.rs";
    for source in [
        "use crate::engine::StandaloneNovaRocks;",
        "use crate::sql::{optimizer::Optimizer, codegen::FragmentBuildResult};",
        "use crate::sql as sql_root; type Leak = sql_root::planner::logical::build::LogicalPlanBuilder;",
        "type Leak = crate::engine::StandaloneSession;",
    ] {
        let paths = rust_production_canonical_paths(source, source_rel);
        assert!(
            paths
                .iter()
                .any(|path| rust_canonical_path_is_forbidden_distributed_write_dependency(path)),
            "forbidden write dependency must be rejected: {source}\n{paths:?}"
        );
    }

    let cfg_test = r#"
#[cfg(test)]
mod tests {
    use crate::engine::StandaloneNovaRocks;
    type Leak = crate::sql::codegen::FragmentBuildResult;
}
"#;
    assert!(
        !rust_production_canonical_paths(cfg_test, source_rel)
            .iter()
            .any(|path| rust_canonical_path_is_forbidden_distributed_write_dependency(path)),
        "cfg(test) dependency noise must be ignored"
    );
}

#[test]
fn planner_guard_tracks_alias_qualified_non_use_paths() {
    let source_rel = "src/sql/planner/distributed/node.rs";
    for (source, forbidden) in [
        (
            r#"
use crate::sql as sql_root;
type TypeLeak = sql_root::planner::DistributedPlan;
fn expression_leak(node: &sql_root::planner::DistributedNodeKind) {
    let _ = sql_root::planner::distributed_kind_to_physical(node);
}
use sql_root::planner as planner_root;
type ChainedLeak = planner_root::DistributedPlan;
"#,
            rust_canonical_path_is_planner_root_distributed_core as fn(&[String]) -> bool,
        ),
        (
            r#"
use crate::sql::codegen as old_edge_owner;
type TypeLeak = old_edge_owner::FragmentEdge;
fn expression_leak() {
    let _ = old_edge_owner::FragmentId(7);
}
"#,
            rust_canonical_path_is_codegen_edge_topology as fn(&[String]) -> bool,
        ),
    ] {
        let paths = rust_production_canonical_paths(source, source_rel);
        assert!(
            paths.iter().any(|path| forbidden(path)),
            "alias-qualified type/expression path must be rejected: {source}\n{paths:?}"
        );
    }

    for source in [
        r#"
use crate::sql as sql_root;
type Leak = sql_root::optimizer::OptimizerPhysicalNode;
"#,
        r#"
use crate::sql::codegen as codegen_owner;
type Leak = codegen_owner::FragmentBuildResult;
"#,
        r#"
use crate::sql::planner as planner_root;
type Leak = planner_root::logical::build::LogicalPlanBuilder;
"#,
    ] {
        let paths = rust_production_canonical_paths(source, source_rel);
        assert!(
            paths.iter().any(|path| {
                path.starts_with(&[
                    "crate".to_string(),
                    "sql".to_string(),
                    "optimizer".to_string(),
                ]) || path.starts_with(&[
                    "crate".to_string(),
                    "sql".to_string(),
                    "codegen".to_string(),
                ]) || path.starts_with(&[
                    "crate".to_string(),
                    "sql".to_string(),
                    "planner".to_string(),
                    "logical".to_string(),
                    "build".to_string(),
                ])
            }),
            "distributed alias-qualified stage dependency must be rejected: {source}\n{paths:?}"
        );
    }
}

#[test]
fn planner_guard_tracks_relative_and_inline_module_non_use_paths() {
    let source_rel = "src/sql/planner/distributed/node.rs";
    for source in [
        "type Leak = super::super::super::optimizer::OptimizerPhysicalNode;",
        r#"
mod nested {
    type Leak = super::super::super::super::optimizer::OptimizerPhysicalNode;
}
"#,
        r#"
use crate::proto as owner;
type Allowed = owner::plan::DistributedNode;
mod nested {
    use crate::sql as owner;
    type Leak = owner::optimizer::OptimizerPhysicalNode;
}
"#,
    ] {
        let paths = rust_production_canonical_paths(source, source_rel);
        assert!(
            paths.iter().any(|path| path.starts_with(&[
                "crate".to_string(),
                "sql".to_string(),
                "optimizer".to_string(),
            ])),
            "relative or inline-scope optimizer path must be rejected: {source}\n{paths:?}"
        );
    }
}

#[test]
fn planner_guard_non_use_path_detector_ignores_cfg_test_and_lexical_noise() {
    let source_rel = "src/sql/planner/distributed/node.rs";
    let source = r###"
#[cfg(test)]
mod tests {
    use crate::sql as sql_root;
    type AliasLeak = sql_root::optimizer::OptimizerPhysicalNode;
    type RelativeLeak = super::super::super::super::optimizer::OptimizerPhysicalNode;
}

// type CommentLeak = crate::sql::planner::DistributedPlan;
const TEXT: &str = "crate::sql::codegen::FragmentEdge";
const RAW: &str = r#"crate::sql::optimizer::OptimizerPhysicalNode"#;
use crate::proto as owner;
type Allowed = owner::plan::DistributedNode;
use crate::sql as _;
mod sql {
    pub mod optimizer {
        pub struct Allowed;
    }
}
type UnderscoreImportMustNotBindSql = sql::optimizer::Allowed;
mod sibling {
    use crate::proto as owner;
    type ScopedAllowed = owner::plan::DistributedNode;
}
"###;
    let paths = rust_production_canonical_paths(source, source_rel);
    assert!(
        !paths.iter().any(|path| {
            rust_canonical_path_is_planner_root_distributed_core(path)
                || rust_canonical_path_is_codegen_edge_topology(path)
                || path.starts_with(&[
                    "crate".to_string(),
                    "sql".to_string(),
                    "optimizer".to_string(),
                ])
        }),
        "test-only and lexical noise paths must remain ignored: {paths:?}"
    );
}

#[test]
fn planner_stage_first_dependency_detector_covers_bypasses() {
    let invalid = [
        (
            "src/sql/planner/logical/node.rs",
            "use crate::sql::planner::{physical::Node, distributed::Plan};",
        ),
        (
            "src/sql/planner/logical/node.rs",
            "use crate::sql::planner as p; use p::physical as stage; type Leak = stage::Node;",
        ),
        (
            "src/sql/planner/logical/node.rs",
            "type Leak = crate::sql::optimizer::Optimizer;",
        ),
        (
            "src/sql/planner/logical/node.rs",
            "#[cfg(test_feature)] use crate::sql::optimizer::Optimizer;",
        ),
        (
            "src/sql/planner/logical/node.rs",
            "#[cfg(test = \"production\")] use crate::sql::optimizer::Optimizer;",
        ),
        (
            "src/sql/planner/logical/node.rs",
            "#[cfg(any(test, test = \"production\"))] use crate::sql::optimizer::Optimizer;",
        ),
        (
            "src/sql/planner/logical/node.rs",
            "type Leak = crate::sql::planner::r#physical::Node;",
        ),
        (
            r"src\sql\planner\logical\node.rs",
            "use crate::sql::optimizer::Optimizer;",
        ),
        (
            "src/sql/planner/logical/node.rs",
            "mod nested { type Leak = super::super::super::distributed::Plan; }",
        ),
        (
            "src/sql/planner/logical/node.rs",
            "use crate::sql::planner::optimizer_bridge::physical::Bridge;",
        ),
        (
            "src/sql/planner/logical/node.rs",
            "use crate::sql::codegen::proto_encode::Encoder;",
        ),
        (
            "src/sql/planner/physical/node.rs",
            "use crate::sql::planner::{logical::Node, distributed::Plan};",
        ),
        (
            "src/sql/planner/physical/node.rs",
            "type Leak = crate::sql::codegen::FragmentBuildResult;",
        ),
        (
            "src/sql/planner/physical/node.rs",
            "use crate::sql::planner::optimizer_bridge::property::Ordering;",
        ),
        (
            "src/sql/planner/physical/node.rs",
            "use crate::sql::optimizer::operator::Operator;",
        ),
        (
            "src/sql/planner/distributed/build/lowering.rs",
            "use crate::sql::planner::logical::build::plan_query;",
        ),
        (
            "src/sql/planner/distributed/build/lowering.rs",
            "type Leak = crate::sql::optimizer::Optimizer;",
        ),
        (
            "src/sql/planner/distributed/build/lowering.rs",
            "use crate::sql::codegen::FragmentBuildResult;",
        ),
        (
            "src/sql/planner/distributed/build/lowering.rs",
            "use crate::sql::planner::optimizer_bridge::to_physical_plan;",
        ),
        (
            "src/sql/planner/optimizer_bridge/distributed.rs",
            "use crate::sql::{optimizer::Optimizer, planner::{logical::Node, physical::Physical, distributed::Plan}};",
        ),
        (
            "src/sql/planner/pipeline/mod.rs",
            "use crate::sql::optimizer::OptimizerPhysicalNode;",
        ),
        (
            "src/sql/planner/pipeline/mod.rs",
            "use crate::sql::planner::optimizer_bridge::to_physical_plan;",
        ),
        (
            "src/sql/planner/physical/node.rs",
            "use crate::sql::planner::pipeline::build_distributed_plan;",
        ),
        (
            "src/sql/planner/payload.rs",
            "use crate::sql::planner::pipeline::build_distributed_plan;",
        ),
        (
            "src/sql/planner/payload.rs",
            "use crate::sql::planner::{logical::Node, optimizer_bridge::Bridge, physical::Physical, distributed::Plan};",
        ),
        (
            "src/sql/planner/ordering.rs",
            "use crate::sql::{optimizer::Optimizer, codegen::Codegen};",
        ),
        (
            r"src\sql\planner\payload.rs",
            "use crate::sql::planner::physical::Physical;",
        ),
        (
            "src/sql/planner/logical/node.rs",
            r#"
enum Guarded {
    #[cfg(all(feature = "compat", test))]
    Hidden(crate::sql::optimizer::Optimizer),
    Visible(crate::sql::optimizer::Optimizer),
}
"#,
        ),
        (
            "src/sql/planner/logical/node.rs",
            r#"
#[cfg(all(feature = "compat", test))]
const HIDDEN: bool = crate::sql::optimizer::FLAG < 1;
enum Demo {
    #[cfg(all(feature = "compat", test))]
    Hidden(crate::sql::optimizer::Optimizer, [u8; (1 < 2) as usize]),
    Visible(crate::sql::optimizer::Optimizer),
}
"#,
        ),
    ];
    for (source_rel, source) in invalid {
        let violations = planner_stage_first_dependency_violations_in(source_rel, source);
        assert!(
            !violations.is_empty(),
            "forbidden stage dependency must be rejected: {source_rel}\n{source}"
        );
    }

    let valid = [
        (
            "src/sql/planner/logical/node.rs",
            "use crate::sql::planner::{payload::Payload, ordering::Ordering, optimizer_bridge::property::Property}; use crate::sql::codegen::helpers::display_name;",
        ),
        (
            "src/sql/planner/physical/node.rs",
            "use crate::sql::planner::payload::Payload; use crate::sql::optimizer::options::OptimizerOptions;",
        ),
        (
            "src/sql/planner/distributed/build/lowering.rs",
            "use crate::sql::planner::{physical::Physical, payload::Payload, optimizer_bridge::property::Property, logical::node::LogicalNode};",
        ),
        (
            "src/sql/codegen/ir/mod.rs",
            "use crate::sql::planner::distributed::DistributedPlan;",
        ),
        (
            "src/sql/planner/pipeline/mod.rs",
            "use crate::sql::planner::{physical::PhysicalPlanNode, distributed::DistributedPlan};",
        ),
        (
            "src/sql/planner/imv_rewrite/entrypoint.rs",
            "use crate::sql::{optimizer::Optimizer, codegen::Codegen}; use crate::sql::planner::{logical::Node, physical::Physical, distributed::Plan};",
        ),
        (
            "src/sql/planner/logical/node.rs",
            r###"
#[cfg(test)]
mod tests { use crate::sql::optimizer::Optimizer; }
// crate::sql::planner::physical::Physical
const TEXT: &str = "crate::sql::planner::distributed::Plan";
const RAW: &str = r#"crate::sql::codegen::Codegen"#;
"###,
        ),
        (
            "src/sql/planner/logical/node.rs",
            "#[allow(unused_imports)] #[cfg(all(feature = \"compat\", test))] use crate::sql::optimizer::Optimizer;",
        ),
        (
            "src/sql/planner/logical/node.rs",
            r#"
enum Guarded {
    #[cfg(all(feature = "compat", test))]
    HiddenTuple(crate::sql::optimizer::Optimizer),
    #[cfg(all(feature = "compat", test))]
    HiddenUnit,
    Safe,
}
struct GuardedFields {
    #[cfg(all(feature = "compat", test))]
    hidden: crate::sql::optimizer::Optimizer,
    safe: usize,
}
"#,
        ),
        (
            "src/sql/planner/logical/node.rs",
            r#"
#[cfg(all(feature = "compat", test))]
const HIDDEN: bool = crate::sql::optimizer::FLAG < 1;
enum Demo {
    #[cfg(all(feature = "compat", test))]
    Hidden(crate::sql::optimizer::Optimizer, [u8; (1 < 2) as usize]),
    Visible,
}
"#,
        ),
    ];
    for (source_rel, source) in valid {
        let violations = planner_stage_first_dependency_violations_in(source_rel, source);
        assert!(
            violations.is_empty(),
            "approved dependency must remain valid: {source_rel}\n{source}\n{violations:?}"
        );
    }
}

#[test]
fn planner_production_source_collection_excludes_external_test_modules() {
    let root = std::env::temp_dir().join(format!(
        "planner_production_source_collection_{}",
        std::process::id()
    ));
    fs::remove_dir_all(&root).ok();
    fs::create_dir_all(root.join("tests")).unwrap();
    fs::create_dir_all(root.join("fixtures")).unwrap();
    fs::create_dir_all(root.join("owner/inline_name")).unwrap();
    fs::create_dir_all(root.join("foo")).unwrap();
    fs::create_dir_all(root.join("unreachable_inline")).unwrap();
    fs::create_dir_all(root.join("redirected")).unwrap();
    fs::write(
        root.join("mod.rs"),
        r###"
mod production;
#[cfg(test)]
#[path = "production.rs"]
mod production_alias;
mod owner;
#[cfg(test)]
mod tests;
#[cfg(all(test, feature = "compat"))]
pub(crate) mod equiv;
#[cfg(test)]
#[path = "fixtures/custom.rs"]
mod custom_test;
#[cfg(any(test, feature = "production"))]
mod any_cfg;
#[cfg(not(test))]
mod non_test;
#[cfg_attr(feature = "compat", cfg(test))]
mod cfg_attr_production;
#[cfg_attr(feature = "alt", path = "conditional_alt.rs")]
mod conditional_owner;
#[cfg_attr(feature = "outer", cfg_attr(feature = "inner", path = "nested_alt.rs"))]
mod nested_conditional_owner;
#[cfg_attr(test, path = "test_only_alt.rs")]
mod test_only_conditional_owner;
#[cfg_attr(all(test, feature = "alt"), path = "all_test_only_alt.rs")]
mod all_test_only_conditional_owner;
#[cfg_attr(test, cfg_attr(feature = "inner", path = "nested_test_only_alt.rs"))]
mod nested_test_only_conditional_owner;
#[cfg_attr(any(test, feature = "alt"), path = "any_production_alt.rs")]
mod any_production_conditional_owner;
#[cfg(test = "production")]
mod keyed_test;
#[cfg(any(test, test = "production"))]
mod any_keyed_test;
#[path = r#"raw_hidden.rs"#]
mod raw_path;
#[path = "escaped\x2d\u{0068}idden.rs"]
mod escaped_path;
#[path = "leak\'s.rs"]
mod apostrophe_path;
mod r#type;
#[cfg(test)]
mod foo;
#[cfg(test)]
mod unreachable_inline {
    mod child;
}
"###,
    )
    .unwrap();
    let forbidden = "use crate::sql::planner::physical::PhysicalPlanNode;\n";
    fs::write(root.join("production.rs"), forbidden).unwrap();
    fs::write(
        root.join("owner.rs"),
        r#"
#[cfg(test)]
mod fixture;
mod inline_name {
    #[cfg(test)]
    mod test_child;
    #[path = "other.rs"]
    mod child;
}
#[path = "redirected"]
mod redirected_inline {
    mod hidden;
}
mod outer {
    #[path = "redirected"]
    mod inner {
        mod hidden;
    }
}
mod conditional_outer {
    #[cfg_attr(feature = "alt", path = "branch")]
    mod conditional_inner {
        mod hidden;
    }
}
"#,
    )
    .unwrap();
    fs::write(root.join("owner/fixture.rs"), forbidden).unwrap();
    fs::write(root.join("owner/inline_name/test_child.rs"), forbidden).unwrap();
    fs::write(root.join("owner/inline_name/other.rs"), forbidden).unwrap();
    fs::write(root.join("redirected/hidden.rs"), forbidden).unwrap();
    fs::create_dir_all(root.join("owner/outer/redirected")).unwrap();
    fs::write(root.join("owner/outer/redirected/hidden.rs"), forbidden).unwrap();
    fs::create_dir_all(root.join("owner/conditional_outer/conditional_inner")).unwrap();
    fs::create_dir_all(root.join("owner/conditional_outer/branch")).unwrap();
    fs::write(
        root.join("owner/conditional_outer/conditional_inner/hidden.rs"),
        forbidden,
    )
    .unwrap();
    fs::write(
        root.join("owner/conditional_outer/branch/hidden.rs"),
        forbidden,
    )
    .unwrap();
    fs::write(
        root.join("tests/mod.rs"),
        format!(
            "{forbidden}#[cfg(test)]\n#[path = \"../production.rs\"]\nmod cannot_hide_production;\n"
        ),
    )
    .unwrap();
    fs::write(root.join("tests/nested.rs"), forbidden).unwrap();
    fs::write(root.join("equiv.rs"), forbidden).unwrap();
    fs::write(root.join("fixtures/custom.rs"), forbidden).unwrap();
    fs::write(root.join("any_cfg.rs"), forbidden).unwrap();
    fs::write(root.join("non_test.rs"), forbidden).unwrap();
    fs::write(root.join("cfg_attr_production.rs"), forbidden).unwrap();
    fs::write(
        root.join("conditional_owner.rs"),
        "pub(crate) struct Safe;\n",
    )
    .unwrap();
    fs::write(
        root.join("conditional_alt.rs"),
        "use crate::sql::optimizer::Optimizer;\n",
    )
    .unwrap();
    fs::write(
        root.join("nested_conditional_owner.rs"),
        "pub(crate) struct Safe;\n",
    )
    .unwrap();
    fs::write(
        root.join("nested_alt.rs"),
        "use crate::sql::optimizer::Optimizer;\n",
    )
    .unwrap();
    for default in [
        "test_only_conditional_owner.rs",
        "all_test_only_conditional_owner.rs",
        "nested_test_only_conditional_owner.rs",
        "any_production_conditional_owner.rs",
    ] {
        fs::write(root.join(default), "pub(crate) struct Safe;\n").unwrap();
    }
    for test_only in [
        "test_only_alt.rs",
        "all_test_only_alt.rs",
        "nested_test_only_alt.rs",
    ] {
        fs::write(
            root.join(test_only),
            "use crate::sql::optimizer::Optimizer;\n",
        )
        .unwrap();
    }
    fs::write(
        root.join("any_production_alt.rs"),
        "use crate::sql::optimizer::Optimizer;\n",
    )
    .unwrap();
    fs::write(root.join("keyed_test.rs"), forbidden).unwrap();
    fs::write(root.join("any_keyed_test.rs"), forbidden).unwrap();
    fs::write(
        root.join("raw_hidden.rs"),
        "use crate::sql::optimizer::Optimizer;\n",
    )
    .unwrap();
    fs::write(root.join("escaped-hidden.rs"), forbidden).unwrap();
    fs::write(root.join("leak's.rs"), forbidden).unwrap();
    fs::write(root.join("type.rs"), forbidden).unwrap();
    fs::write(root.join("foo/mod.rs"), forbidden).unwrap();
    fs::write(root.join("foo/nested.rs"), forbidden).unwrap();
    fs::write(root.join("unreachable_inline/child.rs"), forbidden).unwrap();

    let collected = production_rs_files(&root);
    let relative = collected
        .iter()
        .map(|path| path.strip_prefix(&root).unwrap().display().to_string())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        relative,
        BTreeSet::from([
            "any_cfg.rs".to_string(),
            "any_keyed_test.rs".to_string(),
            "any_production_alt.rs".to_string(),
            "any_production_conditional_owner.rs".to_string(),
            "all_test_only_conditional_owner.rs".to_string(),
            "cfg_attr_production.rs".to_string(),
            "conditional_alt.rs".to_string(),
            "conditional_owner.rs".to_string(),
            "escaped-hidden.rs".to_string(),
            "keyed_test.rs".to_string(),
            "leak's.rs".to_string(),
            "mod.rs".to_string(),
            "non_test.rs".to_string(),
            "owner.rs".to_string(),
            "owner/conditional_outer/branch/hidden.rs".to_string(),
            "owner/conditional_outer/conditional_inner/hidden.rs".to_string(),
            "owner/inline_name/other.rs".to_string(),
            "owner/outer/redirected/hidden.rs".to_string(),
            "production.rs".to_string(),
            "raw_hidden.rs".to_string(),
            "redirected/hidden.rs".to_string(),
            "nested_alt.rs".to_string(),
            "nested_conditional_owner.rs".to_string(),
            "nested_test_only_conditional_owner.rs".to_string(),
            "test_only_conditional_owner.rs".to_string(),
            "type.rs".to_string(),
        ]),
        "only cfg predicates that require test may exclude external modules"
    );
    let production = fs::read_to_string(root.join("production.rs")).unwrap();
    assert!(
        !planner_stage_first_dependency_violations_in(
            "src/sql/planner/logical/production.rs",
            &production,
        )
        .is_empty(),
        "the same forbidden dependency in a production sibling must still fail"
    );
    assert_eq!(
        path_attribute_value(r##"#[path = "dir\\quoted\".rs"]"##),
        Some("dir\\quoted\".rs".to_string()),
        "ordinary path strings must decode escaped backslashes and quotes"
    );
    fs::remove_dir_all(&root).ok();
}

#[test]
fn planner_logical_builder_is_owned_by_logical_stage() {
    let repo = Path::new(manifest_dir());
    let expected_files = [
        "src/sql/planner/logical/mod.rs",
        "src/sql/planner/logical/build/mod.rs",
        "src/sql/planner/logical/build/query.rs",
        "src/sql/planner/logical/build/select.rs",
        "src/sql/planner/logical/build/subquery.rs",
        "src/sql/planner/logical/build/aggregate.rs",
        "src/sql/planner/logical/build/window.rs",
        "src/sql/planner/logical/build/relation.rs",
        "src/sql/planner/logical/build/output.rs",
        "src/sql/planner/logical/build/tests.rs",
    ];
    for path in expected_files {
        assert!(
            repo.join(path).is_file(),
            "missing logical builder owner: {path}"
        );
    }

    let facade = fs::read_to_string(repo.join("src/sql/planner/mod.rs")).unwrap();
    let root_functions = top_level_production_functions(&facade);
    assert!(
        root_functions.is_empty(),
        "planner facade must not own top-level production functions:\n{}",
        root_functions.join("\n")
    );
    assert!(has_non_comment_line(&facade, "pub(crate) mod logical;"));
    assert!(has_non_comment_line(
        &facade,
        "pub(crate) use logical::build::{plan_output_columns, plan_query};"
    ));

    let build_mod = fs::read_to_string(repo.join("src/sql/planner/logical/build/mod.rs")).unwrap();
    let surface_violations = logical_build_surface_violations(&build_mod);
    assert!(
        surface_violations.is_empty(),
        "logical build module must expose exactly its seven owners and two stable entries:\n{}",
        surface_violations.join("\n")
    );
}

#[test]
fn planner_physical_stage_has_single_namespace() {
    let repo = Path::new(manifest_dir());
    let planner = repo.join("src/sql/planner");
    let expected_files = [
        "physical/mod.rs",
        "physical/node.rs",
        "physical/vocab.rs",
        "physical/stats.rs",
        "physical/runtime_filter.rs",
        "physical/runtime_filter_placement.rs",
    ];
    for relative in expected_files {
        let path = planner.join(relative);
        assert!(
            path.is_file(),
            "missing physical stage owner: {}",
            rel(&path)
        );
    }
    for relative in [
        "plan.rs",
        "physical_vocab.rs",
        "stats.rs",
        "runtime_filter_placement.rs",
    ] {
        let path = planner.join(relative);
        assert!(
            !path.exists(),
            "legacy physical stage owner must be deleted: {}",
            rel(&path)
        );
    }

    let physical_node = planner.join("physical/node.rs");
    let physical_vocab = planner.join("physical/vocab.rs");
    let physical_stats = planner.join("physical/stats.rs");
    let physical_runtime_filter = planner.join("physical/runtime_filter.rs");
    let ordering = planner.join("ordering.rs");
    assert!(
        !planner.join("runtime_filter.rs").exists(),
        "root RF lifecycle owner src/sql/planner/runtime_filter.rs must be deleted"
    );
    let planner_sources = rs_files(&planner)
        .into_iter()
        .map(|path| {
            let text = fs::read_to_string(&path).unwrap();
            (path, text)
        })
        .collect::<Vec<_>>();

    for (owner, items) in [
        (
            &physical_node,
            &[
                "PhysicalTopNNode",
                "PhysicalHashAggregateNode",
                "PhysicalHashJoinNode",
                "PhysicalHashJoinEqCondition",
                "PhysicalNestLoopJoinNode",
                "PlanSetOpKind",
                "PhysicalSetOpNode",
                "DistributedChangeEventExpandNode",
                "DistributedChangeEventSpec",
                "DistributedChangeEventOutputExpr",
                "PhysicalPlanNode",
                "PhysicalPlanKind",
                "RedistributeNode",
                "RedistributeMode",
            ][..],
        ),
        (
            &physical_vocab,
            &[
                "AggMode",
                "TopNPhase",
                "JoinDistribution",
                "HashSource",
                "AggregateOutputLayout",
            ][..],
        ),
        (
            &physical_stats,
            &[
                "PlannerConfidence",
                "PlannerColumnStatistic",
                "PlannerCostEstimate",
                "PlannerBroadcastDecision",
                "PhysicalPlanStats",
            ][..],
        ),
        (
            &physical_runtime_filter,
            &[
                "JoinExecutionMode",
                "RuntimeFilterBuildIntent",
                "RuntimeFilterProbeIntent",
            ][..],
        ),
        (&ordering, &["SortKey", "OrderingSpec"][..]),
    ] {
        for item in items {
            let declarations = planner_sources
                .iter()
                .filter_map(|(path, text)| {
                    let count = rust_named_type_declaration_count(text, item);
                    (count > 0).then(|| format!("{} ({count})", rel(path)))
                })
                .collect::<Vec<_>>();
            assert_eq!(
                declarations,
                vec![format!("{} (1)", rel(owner))],
                "{item} must have exactly one declaration in its physical/neutral owner"
            );
        }
    }

    for constant in [
        "DEFAULT_CPU_COST_WEIGHT",
        "DEFAULT_MEMORY_COST_WEIGHT",
        "DEFAULT_NETWORK_COST_WEIGHT",
        "MAX_ROW_COUNT",
    ] {
        let declarations = planner_sources
            .iter()
            .filter_map(|(path, text)| {
                let count = rust_named_const_declaration_count(text, constant);
                (count > 0).then(|| format!("{} ({count})", rel(path)))
            })
            .collect::<Vec<_>>();
        assert_eq!(
            declarations,
            vec![format!("{} (1)", rel(&physical_stats))],
            "{constant} must have exactly one declaration in its physical stats owner"
        );
    }

    let physical_mod_path = planner.join("physical/mod.rs");
    let physical_mod = fs::read_to_string(&physical_mod_path).unwrap();
    assert_eq!(
        module_declarations(&physical_mod),
        BTreeSet::from([
            "node".to_string(),
            "runtime_filter".to_string(),
            "runtime_filter_placement".to_string(),
            "stats".to_string(),
            "vocab".to_string(),
        ]),
        "physical/mod.rs must declare exactly the five physical owners"
    );
    for declaration in [
        "mod node;",
        "pub(crate) mod runtime_filter;",
        "pub(crate) mod runtime_filter_placement;",
        "mod stats;",
        "mod vocab;",
    ] {
        assert!(
            has_non_comment_line(&physical_mod, declaration),
            "physical/mod.rs must contain `{declaration}`"
        );
    }
    assert_eq!(
        rust_production_use_statements(&physical_mod)
            .into_iter()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            "pub(crate)|node::*".to_string(),
            "pub(crate)|runtime_filter::JoinExecutionMode".to_string(),
            "pub(crate)|stats::DEFAULT_CPU_COST_WEIGHT".to_string(),
            "pub(crate)|stats::DEFAULT_MEMORY_COST_WEIGHT".to_string(),
            "pub(crate)|stats::DEFAULT_NETWORK_COST_WEIGHT".to_string(),
            "pub(crate)|stats::MAX_ROW_COUNT".to_string(),
            "pub(crate)|stats::PhysicalPlanStats".to_string(),
            "pub(crate)|stats::PlannerBroadcastDecision".to_string(),
            "pub(crate)|stats::PlannerColumnStatistic".to_string(),
            "pub(crate)|stats::PlannerConfidence".to_string(),
            "pub(crate)|stats::PlannerCostEstimate".to_string(),
            "pub(crate)|vocab::AggMode".to_string(),
            "pub(crate)|vocab::AggregateOutputLayout".to_string(),
            "pub(crate)|vocab::HashSource".to_string(),
            "pub(crate)|vocab::JoinDistribution".to_string(),
            "pub(crate)|vocab::TopNPhase".to_string(),
        ]),
        "physical/mod.rs must expose only the physical stage surface"
    );

    let facade_path = planner.join("mod.rs");
    let facade = fs::read_to_string(&facade_path).unwrap();
    for declaration in ["pub(crate) mod physical;", "pub(crate) mod ordering;"] {
        assert!(
            has_non_comment_line(&facade, declaration),
            "planner facade must contain `{declaration}`"
        );
    }
    for legacy_module in [
        "plan",
        "physical_vocab",
        "stats",
        "runtime_filter",
        "runtime_filter_placement",
    ] {
        assert!(
            !has_module_declaration(&facade, legacy_module),
            "planner facade must not declare legacy physical module `{legacy_module}`"
        );
    }
    let facade_uses = rust_production_use_statements(&facade);
    assert!(
        !facade_uses.iter().any(|import| {
            rust_use_imports_stage(import, "physical") || rust_use_imports_stage(import, "ordering")
        }),
        "planner facade must not import or re-export physical/ordering items: {facade_uses:?}"
    );

    let physical_and_ordering_items = BTreeSet::from([
        "PhysicalTopNNode",
        "PhysicalHashAggregateNode",
        "PhysicalHashJoinNode",
        "PhysicalHashJoinEqCondition",
        "PhysicalNestLoopJoinNode",
        "PlanSetOpKind",
        "PhysicalSetOpNode",
        "DistributedChangeEventExpandNode",
        "DistributedChangeEventSpec",
        "DistributedChangeEventOutputExpr",
        "PhysicalPlanNode",
        "PhysicalPlanKind",
        "RedistributeNode",
        "RedistributeMode",
        "AggMode",
        "TopNPhase",
        "JoinDistribution",
        "HashSource",
        "AggregateOutputLayout",
        "PlannerConfidence",
        "PlannerColumnStatistic",
        "PlannerCostEstimate",
        "PlannerBroadcastDecision",
        "PhysicalPlanStats",
        "JoinExecutionMode",
        "RuntimeFilterBuildIntent",
        "RuntimeFilterProbeIntent",
        "SortKey",
        "OrderingSpec",
    ]);
    for path in rs_files(&src_dir()) {
        let text = fs::read_to_string(&path).unwrap();
        let production = rust_sanitized_production_text(&text);
        let compact = compact_line(&production);
        let imports = rust_production_use_statements(&text);
        let scoped_imports = rust_production_scoped_use_statements(&text);
        for import in &scoped_imports {
            assert!(
                !rust_scoped_use_imports_legacy_planner_owner(import, &rel(&path)),
                "legacy physical owner import remains in {}: {import:?}",
                rel(&path)
            );
        }
        for import in &imports {
            let import_segments = rust_use_path(import).split("::").collect::<Vec<_>>();
            let imports_planner_root = import_segments.last().is_some_and(|leaf| {
                physical_and_ordering_items.contains(leaf)
                    && import_segments
                        .iter()
                        .rposition(|segment| *segment == "planner")
                        == Some(import_segments.len() - 2)
            });
            assert!(
                !imports_planner_root,
                "planner root must not flatten physical/ordering owner in {}: {import}",
                rel(&path)
            );
        }
        for item in &physical_and_ordering_items {
            assert!(
                !compact.contains(&format!("crate::sql::planner::{item}")),
                "fully-qualified planner root physical/ordering path remains in {}: {item}",
                rel(&path)
            );
        }
    }

    for path in rs_files(&planner.join("physical")) {
        let text = fs::read_to_string(&path).unwrap();
        let production = rust_sanitized_production_text(&text);
        let imports = rust_production_use_statements(&text);
        assert!(
            !imports.iter().any(|import| {
                rust_use_imports_stage(import, "logical")
                    || rust_use_imports_stage(import, "distributed")
            }) && !compact_line(&production).contains("planner::logical")
                && !compact_line(&production).contains("planner::distributed"),
            "physical production must not import logical/distributed owners in {}: {imports:?}",
            rel(&path)
        );
    }
    for path in rs_files(&planner.join("logical")) {
        let text = fs::read_to_string(&path).unwrap();
        let production = rust_sanitized_production_text(&text);
        let imports = rust_production_use_statements(&text);
        assert!(
            !imports
                .iter()
                .any(|import| rust_use_imports_stage(import, "physical"))
                && !compact_line(&production).contains("planner::physical"),
            "logical production must not import physical owner in {}: {imports:?}",
            rel(&path)
        );
    }
}

#[test]
fn planner_distributed_core_has_stage_namespace() {
    let repo = Path::new(manifest_dir());
    let planner = repo.join("src/sql/planner");
    for relative in [
        "distributed/mod.rs",
        "distributed/node.rs",
        "distributed/fragment.rs",
        "distributed/runtime_filter.rs",
        "distributed/build/mod.rs",
    ] {
        let path = planner.join(relative);
        assert!(
            path.is_file(),
            "missing distributed stage owner: {}",
            rel(&path)
        );
    }
    for relative in ["distributed_node.rs", "distributed_fragment.rs"] {
        let path = planner.join(relative);
        assert!(
            !path.exists(),
            "legacy distributed stage owner must be deleted: {}",
            rel(&path)
        );
    }
    assert!(
        !planner.join("distributed_plan_build.rs").exists(),
        "root distributed_plan_build.rs must be deleted by PDB-2 M2c"
    );

    let fragment_path = planner.join("distributed/fragment.rs");
    let node_path = planner.join("distributed/node.rs");
    let mod_path = planner.join("distributed/mod.rs");
    let fragment = fs::read_to_string(&fragment_path).unwrap();
    let node = fs::read_to_string(&node_path).unwrap();
    let distributed_mod = fs::read_to_string(&mod_path).unwrap();
    let planner_sources = rs_files(&planner)
        .into_iter()
        .map(|path| {
            let text = fs::read_to_string(&path).unwrap();
            (path, text)
        })
        .collect::<Vec<_>>();

    for (owner, items) in [
        (
            &fragment_path,
            &[
                "PartitionKind",
                "DataPartition",
                "DataSink",
                "PlanFragment",
                "DistributedPlan",
                "FragmentId",
                "FragmentEdgeKind",
                "FragmentStreamKind",
                "FragmentEdge",
            ][..],
        ),
        (
            &node_path,
            &[
                "ExchangeReceiver",
                "ExchangeFlavor",
                "DistributedNodeKind",
                "DistributedNode",
            ][..],
        ),
    ] {
        for item in items {
            let declarations = planner_sources
                .iter()
                .filter_map(|(path, text)| {
                    let count = rust_named_type_declaration_count(text, item);
                    (count > 0).then(|| format!("{} ({count})", rel(path)))
                })
                .collect::<Vec<_>>();
            assert_eq!(
                declarations,
                vec![format!("{} (1)", rel(owner))],
                "{item} must have exactly one declaration in its distributed owner"
            );
        }
    }
    for function in [
        "distributed_kind_from_physical",
        "distributed_kind_to_physical",
    ] {
        let declarations = planner_sources
            .iter()
            .filter_map(|(path, text)| {
                let count = rust_named_function_declaration_count(text, function);
                (count > 0).then(|| format!("{} ({count})", rel(path)))
            })
            .collect::<Vec<_>>();
        assert_eq!(
            declarations,
            vec![format!("{} (1)", rel(&node_path))],
            "{function} must be uniquely declared in distributed/node.rs"
        );
    }

    for item in [
        "FragmentId",
        "FragmentEdgeKind",
        "FragmentStreamKind",
        "FragmentEdge",
    ] {
        let declarations = rs_files(&repo.join("src/sql/codegen"))
            .into_iter()
            .filter_map(|path| {
                let text = fs::read_to_string(&path).unwrap();
                let count = rust_named_type_declaration_count(&text, item);
                (count > 0).then(|| format!("{} ({count})", rel(&path)))
            })
            .collect::<Vec<_>>();
        assert!(
            declarations.is_empty(),
            "codegen must not own distributed edge topology type {item}: {declarations:?}"
        );
    }

    assert_eq!(
        planner_namespace_module_declarations(&distributed_mod),
        BTreeSet::from([
            "build".to_string(),
            "fragment".to_string(),
            "node".to_string(),
            "runtime_filter".to_string(),
            "write".to_string(),
        ]),
        "distributed/mod.rs must declare exactly build, fragment, node, runtime_filter, and write"
    );
    for declaration in [
        "pub(crate) mod build;",
        "mod fragment;",
        "mod node;",
        "pub(crate) mod runtime_filter;",
        "pub(crate) mod write;",
    ] {
        assert!(
            has_non_comment_line(&distributed_mod, declaration),
            "distributed/mod.rs must contain `{declaration}`"
        );
    }
    assert_eq!(
        rust_production_use_statements(&distributed_mod)
            .into_iter()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            "pub(crate)|fragment::DataPartition".to_string(),
            "pub(crate)|fragment::DataSink".to_string(),
            "pub(crate)|fragment::DistributedPlan".to_string(),
            "pub(crate)|fragment::FragmentEdge".to_string(),
            "pub(crate)|fragment::FragmentEdgeKind".to_string(),
            "pub(crate)|fragment::FragmentId".to_string(),
            "pub(crate)|fragment::FragmentStreamKind".to_string(),
            "pub(crate)|fragment::PartitionKind".to_string(),
            "pub(crate)|fragment::PlanFragment".to_string(),
            "pub(crate)|node::DistributedNode".to_string(),
            "pub(crate)|node::DistributedNodeKind".to_string(),
            "pub(crate)|node::ExchangeFlavor".to_string(),
            "pub(crate)|node::ExchangeReceiver".to_string(),
            "pub(crate)|node::distributed_kind_from_physical".to_string(),
            "pub(crate)|node::distributed_kind_to_physical".to_string(),
        ]),
        "distributed/mod.rs must expose exactly the distributed core surface"
    );

    let facade_path = planner.join("mod.rs");
    let facade = fs::read_to_string(&facade_path).unwrap();
    assert!(has_non_comment_line(&facade, "pub(crate) mod distributed;"));
    assert!(!has_module_declaration(&facade, "distributed_node"));
    assert!(!has_module_declaration(&facade, "distributed_fragment"));
    assert!(!has_module_declaration(&facade, "distributed_plan_build"));
    let facade_uses = rust_production_use_statements(&facade);
    assert!(
        !facade_uses.iter().any(|import| {
            matches!(
                rust_use_path(import).split("::").last(),
                Some("build_distributed_plan" | "union_distinct_must_be_rewritten_error")
            )
        }),
        "planner facade must not flatten distributed build entries: {facade_uses:?}"
    );
    assert!(
        !facade_uses
            .iter()
            .any(|import| rust_use_imports_stage(import, "distributed")),
        "planner facade must not flatten distributed core items: {facade_uses:?}"
    );

    let distributed_core = [
        "DataPartition",
        "DataSink",
        "DistributedNode",
        "DistributedNodeKind",
        "DistributedPlan",
        "ExchangeFlavor",
        "ExchangeReceiver",
        "FragmentEdge",
        "FragmentEdgeKind",
        "FragmentId",
        "FragmentStreamKind",
        "PartitionKind",
        "PlanFragment",
        "distributed_kind_from_physical",
        "distributed_kind_to_physical",
    ];
    let codegen_edge_topology = [
        "FragmentEdge",
        "FragmentEdgeKind",
        "FragmentId",
        "FragmentStreamKind",
    ];
    for path in rs_files(&src_dir()) {
        let text = fs::read_to_string(&path).unwrap();
        let production = rust_sanitized_production_text(&text);
        let compact = compact_line(&production);
        let canonical_paths = rust_production_canonical_paths(&text, &rel(&path));
        let scoped_imports = rust_production_scoped_use_statements(&text);
        for import in &scoped_imports {
            assert!(
                !rust_scoped_use_violates_distributed_owner(import, &rel(&path)),
                "distributed owner bypass remains in {}: {import:?}",
                rel(&path)
            );
            if rel(&path).starts_with("src/sql/codegen/") && rust_use_is_public(&import.import) {
                let canonical = rust_canonical_use_segments_in_scope(
                    &import.import,
                    &rel(&path),
                    &import.inline_modules,
                )
                .unwrap_or_default();
                assert!(
                    !canonical.starts_with(&[
                        "crate".to_string(),
                        "sql".to_string(),
                        "planner".to_string(),
                        "distributed".to_string(),
                    ]),
                    "codegen must consume distributed core directly, not re-export it in {}: {import:?}",
                    rel(&path)
                );
            }
        }
        for canonical in &canonical_paths {
            assert!(
                !(rust_canonical_path_is_legacy_distributed_owner(canonical)
                    || rust_canonical_path_is_planner_root_distributed_core(canonical)
                    || rust_canonical_path_is_codegen_edge_topology(canonical)),
                "distributed owner bypass remains in {}: {}",
                rel(&path),
                canonical.join("::")
            );
        }
        assert!(
            !compact.contains("crate::sql::planner::distributed_node")
                && !compact.contains("crate::sql::planner::distributed_fragment"),
            "legacy distributed owner path remains in {}",
            rel(&path)
        );
        for item in distributed_core {
            assert!(
                !compact.contains(&format!("crate::sql::planner::{item}")),
                "fully-qualified planner root distributed path remains in {}: {item}",
                rel(&path)
            );
        }
        for item in codegen_edge_topology {
            assert!(
                !compact.contains(&format!("crate::sql::codegen::{item}")),
                "fully-qualified codegen edge topology path remains in {}: {item}",
                rel(&path)
            );
        }
    }

    for path in rs_files(&planner.join("distributed")) {
        let text = fs::read_to_string(&path).unwrap();
        let production = rust_sanitized_production_text(&text);
        let compact = compact_line(&production);
        let canonical_paths = rust_production_canonical_paths(&text, &rel(&path));
        let scoped_imports = rust_production_scoped_use_statements(&text);
        for import in &scoped_imports {
            let canonical = rust_canonical_use_segments_in_scope(
                &import.import,
                &rel(&path),
                &import.inline_modules,
            )
            .unwrap_or_default();
            assert!(
                !(canonical.starts_with(&[
                    "crate".to_string(),
                    "sql".to_string(),
                    "optimizer".to_string(),
                ]) || canonical.starts_with(&[
                    "crate".to_string(),
                    "sql".to_string(),
                    "codegen".to_string(),
                ]) || canonical.starts_with(&[
                    "crate".to_string(),
                    "sql".to_string(),
                    "planner".to_string(),
                    "logical".to_string(),
                    "build".to_string(),
                ])),
                "distributed production has forbidden stage dependency in {}: {import:?}",
                rel(&path)
            );
        }
        assert!(
            !compact.contains("crate::sql::optimizer")
                && !compact.contains("crate::sql::codegen")
                && !compact.contains("crate::sql::planner::logical::build"),
            "distributed production has forbidden fully-qualified dependency in {}",
            rel(&path)
        );
        for canonical in &canonical_paths {
            assert!(
                !(canonical.starts_with(&[
                    "crate".to_string(),
                    "sql".to_string(),
                    "optimizer".to_string(),
                ]) || canonical.starts_with(&[
                    "crate".to_string(),
                    "sql".to_string(),
                    "codegen".to_string(),
                ]) || canonical.starts_with(&[
                    "crate".to_string(),
                    "sql".to_string(),
                    "planner".to_string(),
                    "logical".to_string(),
                    "build".to_string(),
                ])),
                "distributed production has forbidden stage path in {}: {}",
                rel(&path),
                canonical.join("::")
            );
        }
    }
    for stage in ["physical", "logical"] {
        for path in rs_files(&planner.join(stage)) {
            let text = fs::read_to_string(&path).unwrap();
            let production = rust_sanitized_production_text(&text);
            let imports = rust_production_use_statements(&text);
            let canonical_paths = rust_production_canonical_paths(&text, &rel(&path));
            assert!(
                !imports
                    .iter()
                    .any(|import| rust_use_imports_stage(import, "distributed"))
                    && !compact_line(&production).contains("planner::distributed")
                    && !canonical_paths.iter().any(|path| {
                        path.starts_with(&[
                            "crate".to_string(),
                            "sql".to_string(),
                            "planner".to_string(),
                            "distributed".to_string(),
                        ])
                    }),
                "{stage} production must not import distributed owner in {}: {imports:?}",
                rel(&path)
            );
        }
    }

    assert!(
        nidl_e4_struct_has_code_line(&fragment, "pub(crate) struct FragmentEdge {", |line| {
            compact_line(line) == "puboutput_partition:DataPartition,"
        }),
        "distributed FragmentEdge must carry native DataPartition output_partition"
    );
    assert!(
        !rust_production_use_statements(&fragment)
            .iter()
            .any(|import| rust_use_imports_stage(import, "codegen")),
        "distributed fragment owner must not import codegen: {:?}",
        rust_production_use_statements(&fragment)
    );
    assert!(
        !rust_production_use_statements(&node)
            .iter()
            .any(|import| rust_use_imports_stage(import, "codegen")),
        "distributed node owner must not import codegen: {:?}",
        rust_production_use_statements(&node)
    );
}

#[test]
fn planner_distributed_build_has_pass_owners() {
    let planner = src_dir().join("sql/planner");
    let build = planner.join("distributed/build");
    let mod_path = build.join("mod.rs");
    let lowering_path = build.join("lowering.rs");
    let fragment_cut_path = build.join("fragment_cut.rs");
    let runtime_filter_binding_path = build.join("runtime_filter_binding.rs");
    let tests_path = build.join("tests.rs");

    for path in [
        &mod_path,
        &lowering_path,
        &fragment_cut_path,
        &runtime_filter_binding_path,
        &tests_path,
    ] {
        assert!(
            path.is_file(),
            "missing distributed build owner: {}",
            rel(path)
        );
    }
    assert!(
        !planner.join("distributed_plan_build.rs").exists(),
        "legacy root distributed_plan_build.rs must be deleted"
    );

    let build_mod = fs::read_to_string(&mod_path).unwrap();
    let build_surface_violations = distributed_build_mod_surface_violations(&build_mod);
    assert!(
        build_surface_violations.is_empty(),
        "distributed/build/mod.rs function surface must stay closed:\n{}",
        build_surface_violations.join("\n")
    );
    assert_eq!(
        planner_namespace_module_declarations(&build_mod),
        BTreeSet::from([
            "fragment_cut".to_string(),
            "lowering".to_string(),
            "runtime_filter_binding".to_string(),
        ]),
        "distributed/build/mod.rs must declare exactly three production concerns"
    );
    for declaration in [
        "mod fragment_cut;",
        "mod lowering;",
        "mod runtime_filter_binding;",
    ] {
        assert!(
            has_non_comment_line(&build_mod, declaration),
            "build/mod.rs concern owner must stay private: {declaration}"
        );
    }
    assert!(
        has_cfg_test_mod_tests(&build_mod),
        "build/mod.rs must declare sibling #[cfg(test)] mod tests"
    );
    for function in [
        "build_distributed_plan",
        "union_distinct_must_be_rewritten_error",
    ] {
        let declarations = rs_files(&planner)
            .into_iter()
            .filter_map(|path| {
                let text = fs::read_to_string(&path).unwrap();
                let count = rust_named_function_declaration_count(&text, function);
                (count > 0).then(|| format!("{} ({count})", rel(&path)))
            })
            .collect::<Vec<_>>();
        let (expected, owner_contract) = match function {
            "build_distributed_plan" => (
                vec![
                    format!("{} (1)", rel(&mod_path)),
                    format!("{} (1)", rel(&planner.join("pipeline/mod.rs"))),
                ],
                "be declared once in distributed/build and once in pipeline for the borrowed and owned stage entries",
            ),
            "union_distinct_must_be_rewritten_error" => (
                vec![format!("{} (1)", rel(&mod_path))],
                "be declared once in distributed/build",
            ),
            _ => unreachable!(),
        };
        assert_eq!(declarations, expected, "{function} must {owner_contract}");
    }
    let planner_mod_path = planner.join("mod.rs");
    let planner_mod = fs::read_to_string(&planner_mod_path).unwrap();
    let planner_root_build_violations =
        planner_root_distributed_build_surface_violations(&planner_mod);
    assert!(
        planner_root_build_violations.is_empty(),
        "planner/mod.rs must not import, call, or forward distributed::build:\n{}",
        planner_root_build_violations.join("\n")
    );
    assert!(
        !rust_use_tokens(&rust_sanitized_production_text(&build_mod))
            .windows(2)
            .any(|tokens| tokens == ["pub", "use"]),
        "build/mod.rs must not re-export concern internals"
    );
    for forbidden in ["struct", "enum", "LoweredPlanNode"] {
        assert!(
            !rust_sanitized_production_text(&build_mod).contains(forbidden),
            "build/mod.rs must remain a thin coordinator without {forbidden}"
        );
    }

    let owners = [
        (
            &fragment_cut_path,
            fs::read_to_string(&fragment_cut_path).unwrap(),
        ),
        (&lowering_path, fs::read_to_string(&lowering_path).unwrap()),
        (
            &runtime_filter_binding_path,
            fs::read_to_string(&runtime_filter_binding_path).unwrap(),
        ),
    ];
    let planner_sources = rs_files(&planner)
        .into_iter()
        .map(|path| (rel(&path), fs::read_to_string(path).unwrap()))
        .collect::<Vec<_>>();
    for (name, owner) in [
        ("FragmentCutBuilder", &fragment_cut_path),
        ("NodeIdAllocator", &lowering_path),
        ("RuntimeFilterBindings", &runtime_filter_binding_path),
        ("RuntimeFilterBuildBinding", &runtime_filter_binding_path),
        ("RuntimeFilterProbeBinding", &runtime_filter_binding_path),
    ] {
        let declarations = rust_named_declaration_owners(
            &planner_sources,
            name,
            rust_named_type_declaration_count,
        );
        assert_eq!(
            declarations,
            vec![format!("{} (1)", rel(owner))],
            "{name} must have exactly one build concern owner"
        );
    }
    for (name, owner) in [
        ("expect_child_count", &fragment_cut_path),
        ("physical_kind_name", &fragment_cut_path),
        ("lower_fragment_local_node", &lowering_path),
        ("distributed_node_ordering", &lowering_path),
        ("record", &runtime_filter_binding_path),
        ("bind_runtime_filters", &runtime_filter_binding_path),
        ("attach_runtime_filters", &runtime_filter_binding_path),
    ] {
        let declarations = rust_named_declaration_owners(
            &planner_sources,
            name,
            rust_named_function_declaration_count,
        );
        assert_eq!(
            declarations,
            vec![format!("{} (1)", rel(owner))],
            "{name} must have exactly one build concern owner"
        );
    }

    let lowering = rust_sanitized_production_text(&owners[1].1);
    for forbidden in [
        "PlanFragment",
        "FragmentEdge",
        "DataSink",
        "RuntimeFilterBuildIntent",
        "RuntimeFilterProbeIntent",
        "BoundRuntimeFilterBuild",
        "BoundRuntimeFilterProbe",
        "physical.children",
    ] {
        assert!(
            !lowering.contains(forbidden),
            "lowering.rs must not own topology/RF/physical recursion token {forbidden}"
        );
    }
    assert_eq!(
        rust_named_function_declaration_count(&lowering, "visit"),
        0,
        "lowering.rs must not recursively visit the physical tree"
    );

    let runtime_filter_binding = rust_sanitized_production_text(&owners[2].1);
    for forbidden in [
        "crate::sql::optimizer",
        "crate::sql::codegen",
        "crate::proto",
        "crate::thrift",
        "FragmentEdge",
        "PartitionKind",
        "DataPartition",
        "Redistribute",
        "CTEAnchor",
        "TopN",
    ] {
        assert!(
            !runtime_filter_binding.contains(forbidden),
            "runtime_filter_binding.rs must not decide placement/topology or wire protocol: {forbidden}"
        );
    }

    let fragment_cut = rust_sanitized_production_text(&owners[0].1);
    assert_eq!(
        rust_named_function_declaration_count(&fragment_cut, "visit"),
        1,
        "fragment_cut.rs must own the only physical-tree visitor"
    );
    for forbidden in ["LoweredPlanNode", "struct Lowered", "enum Lowered"] {
        assert!(
            !owners
                .iter()
                .any(|(_, text)| { rust_sanitized_production_text(text).contains(forbidden) }),
            "distributed build must not introduce a second full-tree IR: {forbidden}"
        );
    }

    let mut legacy = Vec::new();
    for path in rs_files(&src_dir()) {
        let text = fs::read_to_string(&path).unwrap();
        for violation in distributed_build_surface_violations_in(&rel(&path), &text) {
            legacy.push(format!("{}: {violation}", rel(&path)));
        }
    }
    assert!(
        legacy.is_empty(),
        "legacy/planner-root distributed build paths must be absent:\n{}",
        legacy.join("\n")
    );
}

#[test]
fn planner_distributed_write_surface_has_stage_namespace() {
    let repo = Path::new(manifest_dir());
    let planner = repo.join("src/sql/planner");
    let distributed = planner.join("distributed");
    let write = distributed.join("write");
    let distributed_mod_path = distributed.join("mod.rs");
    let write_mod_path = write.join("mod.rs");
    let plan_path = write.join("plan.rs");
    let sink_path = write.join("sink.rs");
    let change_stream_path = write.join("change_stream.rs");

    for path in [&write_mod_path, &plan_path, &sink_path, &change_stream_path] {
        assert!(
            path.is_file(),
            "missing distributed write owner: {}",
            rel(path)
        );
    }
    for legacy in ["write_plan.rs", "write_sink.rs", "change_stream_write.rs"] {
        let path = planner.join(legacy);
        assert!(
            !path.exists(),
            "legacy distributed write owner must be deleted: {}",
            rel(&path)
        );
    }

    let distributed_mod = fs::read_to_string(&distributed_mod_path).unwrap();
    assert_eq!(
        planner_namespace_module_declarations(&distributed_mod),
        BTreeSet::from([
            "build".to_string(),
            "fragment".to_string(),
            "node".to_string(),
            "runtime_filter".to_string(),
            "write".to_string(),
        ]),
        "distributed/mod.rs must declare exactly build, fragment, node, runtime_filter, and write"
    );
    assert!(
        has_non_comment_line(&distributed_mod, "pub(crate) mod write;"),
        "distributed write namespace must use pub(crate) visibility"
    );

    let write_mod = fs::read_to_string(&write_mod_path).unwrap();
    assert_eq!(
        planner_namespace_module_declarations(&write_mod),
        BTreeSet::from([
            "change_stream".to_string(),
            "plan".to_string(),
            "sink".to_string(),
        ]),
        "distributed/write/mod.rs must declare exactly change_stream, plan, and sink"
    );
    for declaration in [
        "pub(crate) mod change_stream;",
        "pub(crate) mod plan;",
        "pub(crate) mod sink;",
    ] {
        assert!(
            has_non_comment_line(&write_mod, declaration),
            "distributed/write/mod.rs must contain `{declaration}`"
        );
    }
    assert_eq!(
        rust_use_tokens(&rust_sanitized_production_text(&write_mod)),
        [
            "pub",
            "(",
            "crate",
            ")",
            "mod",
            "change_stream",
            ";",
            "pub",
            "(",
            "crate",
            ")",
            "mod",
            "plan",
            ";",
            "pub",
            "(",
            "crate",
            ")",
            "mod",
            "sink",
            ";",
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>(),
        "distributed/write/mod.rs production surface must contain only three pub(crate) module declarations"
    );

    let all_sources = rs_files(&src_dir())
        .into_iter()
        .map(|path| {
            let text = fs::read_to_string(&path).unwrap();
            (path, text)
        })
        .collect::<Vec<_>>();
    let planner_sources = rs_files(&planner)
        .into_iter()
        .map(|path| {
            let text = fs::read_to_string(&path).unwrap();
            (path, text)
        })
        .collect::<Vec<_>>();
    for (owner, items) in [
        (
            &sink_path,
            &[
                "IcebergWriteSinkSpec",
                "IcebergWriteSinkMode",
                "IcebergWriteFileCompression",
                "IcebergWriteFragmentSink",
                "IcebergWriteInputBinding",
            ][..],
        ),
        (
            &change_stream_path,
            &[
                "ChangeStreamWriteBranchSpec",
                "ChangeStreamWriteDagSpec",
                "IcebergChangeStreamRouterSink",
                "IcebergChangeStreamBranchRoute",
                "IcebergChangeStreamWriteTopology",
                "IcebergChangeStreamWriterBranch",
            ][..],
        ),
        (
            &plan_path,
            &["PlannedIcebergChangeStreamDistributedPlan"][..],
        ),
    ] {
        for item in items {
            let declarations = planner_sources
                .iter()
                .filter_map(|(path, text)| {
                    let count = rust_named_type_declaration_count(text, item);
                    (count > 0).then(|| format!("{} ({count})", rel(path)))
                })
                .collect::<Vec<_>>();
            assert_eq!(
                declarations,
                vec![format!("{} (1)", rel(owner))],
                "{item} must have exactly one declaration in its distributed write owner"
            );
        }
    }
    for (owner, functions) in [
        (
            &plan_path,
            &[
                "with_iceberg_write_sink",
                "with_iceberg_change_stream_write",
            ][..],
        ),
        (
            &sink_path,
            &[
                "synthetic_iceberg_write_table_id",
                "transform_to_sink_string",
            ][..],
        ),
        (&change_stream_path, &["validate_branch_set"][..]),
    ] {
        for function in functions {
            let declarations = planner_sources
                .iter()
                .filter_map(|(path, text)| {
                    let count = rust_named_function_declaration_count(text, function);
                    (count > 0).then(|| format!("{} ({count})", rel(path)))
                })
                .collect::<Vec<_>>();
            assert_eq!(
                declarations,
                vec![format!("{} (1)", rel(owner))],
                "{function} must have exactly one declaration in its distributed write owner"
            );
        }
    }

    let planner_mod_path = planner.join("mod.rs");
    let planner_mod = fs::read_to_string(&planner_mod_path).unwrap();
    for legacy in ["write_plan", "write_sink", "change_stream_write"] {
        assert!(
            !has_module_declaration(&planner_mod, legacy),
            "planner facade must not declare legacy write module {legacy}"
        );
    }
    assert!(
        !planner_mod.contains("write_export_tests"),
        "planner facade write_export_tests must be deleted"
    );

    for (path, text) in &all_sources {
        let source_rel = rel(path);
        let imports = rust_production_scoped_use_statements(text);
        let canonical_paths = rust_production_canonical_paths(text, &source_rel);
        for import in &imports {
            assert!(
                !rust_scoped_use_violates_distributed_write_owner(import, &source_rel),
                "distributed write owner bypass remains in {source_rel}: {import:?}"
            );
        }
        for canonical in &canonical_paths {
            assert!(
                !(rust_canonical_path_is_legacy_distributed_write_owner(canonical)
                    || rust_canonical_path_is_planner_root_distributed_write_item(canonical)),
                "distributed write owner bypass remains in {source_rel}: {}",
                canonical.join("::")
            );
        }
    }

    for path in rs_files(&write) {
        let text = fs::read_to_string(&path).unwrap();
        let source_rel = rel(&path);
        for canonical in rust_production_canonical_paths(&text, &source_rel) {
            assert!(
                !rust_canonical_path_is_forbidden_distributed_write_dependency(&canonical),
                "distributed write production has forbidden dependency in {source_rel}: {}",
                canonical.join("::")
            );
        }
    }
}

#[test]
fn planner_runtime_filter_lifecycle_has_stage_owners() {
    let repo = Path::new(manifest_dir());
    let planner = repo.join("src/sql/planner");
    let physical_owner = planner.join("physical/runtime_filter.rs");
    let distributed_owner = planner.join("distributed/runtime_filter.rs");
    let codegen_owner = repo.join("src/sql/codegen/runtime_filter.rs");

    for owner in [&codegen_owner, &distributed_owner, &physical_owner] {
        assert!(
            owner.is_file(),
            "missing runtime-filter stage owner: {}",
            rel(owner)
        );
    }
    assert!(
        !planner.join("runtime_filter.rs").exists(),
        "root RF lifecycle owner src/sql/planner/runtime_filter.rs must be deleted"
    );

    let lifecycle_sources = runtime_filter_lifecycle_source_files(&src_dir())
        .into_iter()
        .map(|path| {
            let text = fs::read_to_string(&path).unwrap();
            (path, text)
        })
        .collect::<Vec<_>>();
    for (owner, items) in [
        (
            &physical_owner,
            &["RuntimeFilterBuildIntent", "RuntimeFilterProbeIntent"][..],
        ),
        (
            &distributed_owner,
            &["BoundRuntimeFilterBuild", "BoundRuntimeFilterProbe"][..],
        ),
        (&codegen_owner, &["PlannedRuntimeFilter"][..]),
    ] {
        for item in items {
            let declarations = lifecycle_sources
                .iter()
                .filter_map(|(path, text)| {
                    let count = rust_named_type_declaration_count(text, item);
                    (count > 0).then(|| format!("{} ({count})", rel(path)))
                })
                .collect::<Vec<_>>();
            assert_eq!(
                declarations,
                vec![format!("{} (1)", rel(owner))],
                "{item} must have exactly one declaration in its runtime-filter stage owner"
            );
        }
    }

    let lifecycle_items = BTreeSet::from([
        "RuntimeFilterBuildIntent",
        "RuntimeFilterProbeIntent",
        "BoundRuntimeFilterBuild",
        "BoundRuntimeFilterProbe",
        "PlannedRuntimeFilter",
    ]);
    for path in rs_files(&src_dir()) {
        let relative = rel(&path);
        let text = fs::read_to_string(&path).unwrap();
        for canonical in rust_production_canonical_paths(&text, &relative) {
            let legacy_root_item = canonical.len() >= 4
                && canonical[..3] == ["crate", "sql", "planner"]
                && lifecycle_items.contains(canonical[3].as_str());
            let legacy_root_owner = canonical.len() >= 4
                && canonical[..4] == ["crate", "sql", "planner", "runtime_filter"];
            assert!(
                !legacy_root_item && !legacy_root_owner,
                "legacy planner runtime-filter owner path remains in {}: {}",
                relative,
                canonical.join("::")
            );
        }
    }

    let planner_mod = fs::read_to_string(planner.join("mod.rs")).unwrap();
    assert!(
        !planner_root_declares_runtime_filter(&planner_mod),
        "planner/mod.rs must not declare the deleted root runtime_filter owner"
    );
    let planner_uses = rust_production_use_statements(&planner_mod);
    assert!(
        !planner_uses.iter().any(|import| {
            rust_use_path(import)
                .split("::")
                .last()
                .is_some_and(|item| lifecycle_items.contains(item))
        }),
        "planner/mod.rs must not flat re-export runtime-filter lifecycle types: {planner_uses:?}"
    );

    let distributed_mod = fs::read_to_string(planner.join("distributed/mod.rs")).unwrap();
    assert!(
        has_non_comment_line(&distributed_mod, "pub(crate) mod runtime_filter;"),
        "distributed/mod.rs must expose the runtime_filter owner module"
    );
    let distributed_uses = rust_production_use_statements(&distributed_mod);
    assert!(
        !distributed_uses.iter().any(|import| {
            matches!(
                rust_use_path(import).split("::").last(),
                Some("BoundRuntimeFilterBuild" | "BoundRuntimeFilterProbe")
            )
        }),
        "distributed/mod.rs must not flat re-export bound runtime-filter types: {distributed_uses:?}"
    );

    let codegen_mod = fs::read_to_string(repo.join("src/sql/codegen/mod.rs")).unwrap();
    assert!(
        has_non_comment_line(&codegen_mod, "pub(crate) mod runtime_filter;"),
        "codegen/mod.rs must declare its Planned runtime-filter owner"
    );

    let distributed_text = fs::read_to_string(&distributed_owner).unwrap();
    for (bound, intent_type) in [
        ("BoundRuntimeFilterBuild", "RuntimeFilterBuildIntent"),
        ("BoundRuntimeFilterProbe", "RuntimeFilterProbeIntent"),
    ] {
        let header = format!("pub(crate) struct {bound} {{");
        let fields = nidl_e4_struct_code_span(&distributed_text, &header)
            .unwrap_or_else(|| panic!("missing distributed runtime-filter struct {bound}"))
            .into_iter()
            .map(|(_, line)| line)
            .collect::<Vec<_>>();
        assert!(
            fields
                .iter()
                .any(|line| line == &format!("pub intent: {intent_type},")),
            "{bound} must compose its physical intent: {fields:?}"
        );
        for duplicated in [
            "filter_id",
            "build_expr",
            "probe_expr",
            "expr_order",
            "execution_mode",
        ] {
            assert!(
                !fields
                    .iter()
                    .any(|line| line.starts_with(&format!("pub {duplicated}:"))),
                "{bound} must not redeclare physical intent field {duplicated}: {fields:?}"
            );
        }
    }

    let builder_path = planner.join("distributed/build/runtime_filter_binding.rs");
    let builder_text = fs::read_to_string(&builder_path).unwrap();
    assert!(
        nidl_e4_function_signature_contains(
            &builder_text,
            "bind_runtime_filters",
            "bindings: RuntimeFilterBindings"
        ),
        "runtime_filter_binding::bind_runtime_filters must consume owned RuntimeFilterBindings"
    );
    let builder_production = rust_sanitized_production_text(&builder_text);
    assert!(
        builder_production.contains("for build in bindings.builds {")
            && builder_production.contains("for probe in bindings.probes {"),
        "runtime_filter_binding::bind_runtime_filters must consume both owned binding Vecs"
    );
}

#[test]
fn planner_logical_ir_and_payload_have_stage_owners() {
    let repo = Path::new(manifest_dir());
    let planner = repo.join("src/sql/planner");
    let logical_node_path = planner.join("logical/node.rs");
    let payload_path = planner.join("payload.rs");
    let physical_node_path = planner.join("physical/node.rs");
    let facade_path = planner.join("mod.rs");
    let logical_mod_path = planner.join("logical/mod.rs");
    let marker_path = planner.join("imv_rewrite/marker.rs");

    for path in [&logical_node_path, &payload_path, &physical_node_path] {
        assert!(path.is_file(), "missing planner IR owner: {}", rel(path));
    }
    assert!(
        !planner.join("plan.rs").exists(),
        "legacy physical IR owner src/sql/planner/plan.rs must be deleted"
    );

    let logical_only = [
        "LogicalPlanNode",
        "LogicalPlanKind",
        "LogicalApplyNode",
        "LogicalAggregateNode",
        "LogicalJoinNode",
        "LogicalUnionNode",
        "LogicalIntersectNode",
        "LogicalExceptNode",
        "LogicalImvDeltaNode",
        "LogicalImvVersionNode",
    ];
    let shared_payload = [
        "PlanScanNode",
        "PlanFilterNode",
        "PlanProjectNode",
        "PlanSortNode",
        "PlanLimitNode",
        "PlanValuesNode",
        "PlanRepeatNode",
        "PlanWindowNode",
        "PlanGenerateSeriesNode",
        "PlanTableFunctionNode",
        "PlanRowCountAssertion",
        "PlanAssertOneRowNode",
        "PlanCTEAnchorNode",
        "PlanCTEProduceNode",
        "PlanCTEConsumeNode",
        "WindowExpr",
        "AggregateCall",
    ];
    let physical_only = [
        "PhysicalTopNNode",
        "PhysicalHashAggregateNode",
        "PhysicalHashJoinNode",
        "PhysicalHashJoinEqCondition",
        "PhysicalNestLoopJoinNode",
        "PlanSetOpKind",
        "PhysicalSetOpNode",
        "DistributedChangeEventExpandNode",
        "DistributedChangeEventSpec",
        "DistributedChangeEventOutputExpr",
        "PhysicalPlanNode",
        "PhysicalPlanKind",
        "RedistributeNode",
        "RedistributeMode",
    ];
    let retired_logical_payload = [
        "LogicalAssertOneRowNode",
        "LogicalRepeatNode",
        "LogicalWindowNode",
        "LogicalGenerateSeriesNode",
        "LogicalTableFunctionNode",
        "LogicalScanNode",
        "LogicalValuesNode",
        "LogicalFilterNode",
        "LogicalProjectNode",
        "LogicalSortNode",
        "LogicalLimitNode",
        "LogicalCTEAnchorNode",
        "LogicalCTEProduceNode",
        "LogicalCTEConsumeNode",
    ];

    let planner_sources = rs_files(&planner)
        .into_iter()
        .map(|path| {
            let text = fs::read_to_string(&path).unwrap();
            (path, text)
        })
        .collect::<Vec<_>>();

    for (expected_owner, items) in [
        (&logical_node_path, logical_only.as_slice()),
        (&payload_path, shared_payload.as_slice()),
        (&physical_node_path, physical_only.as_slice()),
    ] {
        for item in items {
            let declarations = planner_sources
                .iter()
                .filter_map(|(path, text)| {
                    let count = rust_named_type_declaration_count(text, item);
                    (count > 0).then(|| format!("{} ({count})", rel(path)))
                })
                .collect::<Vec<_>>();
            assert_eq!(
                declarations,
                vec![format!("{} (1)", rel(expected_owner))],
                "{item} must have exactly one declaration in its stage owner"
            );
        }
    }

    for item in retired_logical_payload {
        let declarations = planner_sources
            .iter()
            .filter_map(|(path, text)| {
                let count = rust_named_type_declaration_count(text, item);
                (count > 0).then(|| format!("{} ({count})", rel(path)))
            })
            .collect::<Vec<_>>();
        assert!(
            declarations.is_empty(),
            "retired logical payload {item} must not be declared: {declarations:?}"
        );
    }

    let plan = fs::read_to_string(&physical_node_path).unwrap();
    let plan_production = rust_sanitized_production_text(&plan);
    let plan_identifiers = plan_production
        .split(|ch: char| !is_ident_char(ch))
        .filter(|token| !token.is_empty())
        .collect::<Vec<_>>();
    assert!(
        !plan_identifiers
            .iter()
            .any(|token| token.starts_with("Logical")),
        "physical staging owner must not reference Logical* production types"
    );
    let plan_uses = rust_production_use_statements(&plan);
    assert!(
        !plan_uses.iter().any(|import| {
            rust_use_imports_stage(import, "logical")
                || (rust_use_is_public(import)
                    && (rust_use_imports_stage(import, "payload")
                        || rust_use_imports_sql_common(import)))
        }),
        "physical/node.rs must not import logical IR or re-export shared/common payload; the boundary guard conservatively expands aliases across lexical scopes and may restrict legal same-name reuse to avoid false negatives: {plan_uses:?}"
    );

    let payload = fs::read_to_string(&payload_path).unwrap();
    let payload_uses = rust_production_use_statements(&payload);
    assert!(
        !payload_uses.iter().any(|import| {
            ["logical", "plan", "distributed"]
                .into_iter()
                .any(|stage| rust_use_imports_stage(import, stage))
        }),
        "payload.rs must not import stage owners: {payload_uses:?}"
    );
    let payload_production = compact_line(&rust_sanitized_production_text(&payload));
    for forbidden in ["planner::logical", "planner::plan", "planner::distributed"] {
        assert!(
            !payload_production.contains(forbidden),
            "payload.rs must not depend on stage owner {forbidden}"
        );
    }

    let logical_node = fs::read_to_string(&logical_node_path).unwrap();
    let logical_production = rust_sanitized_production_text(&logical_node);
    let logical_identifiers = logical_production
        .split(|ch: char| !is_ident_char(ch))
        .filter(|token| !token.is_empty())
        .collect::<Vec<_>>();
    assert!(
        !logical_identifiers.iter().any(|token| {
            token.starts_with("Physical")
                || token.starts_with("Redistribute")
                || *token == "distributed"
                || *token == "PlanSetOpKind"
        }),
        "logical/node.rs production must not depend on physical/distributed owners"
    );
    let logical_uses = rust_production_use_statements(&logical_node);
    assert!(
        !logical_uses.iter().any(|import| {
            ["plan", "physical", "distributed"]
                .into_iter()
                .any(|stage| rust_use_imports_stage(import, stage))
        }),
        "logical/node.rs production must not import physical/distributed owners: {logical_uses:?}"
    );

    let facade = fs::read_to_string(&facade_path).unwrap();
    assert!(has_non_comment_line(&facade, "pub(crate) mod payload;"));
    let facade_uses = rust_production_use_statements(&facade);
    let actual_ir_surface = facade_uses
        .iter()
        .filter(|import| {
            ["logical", "payload", "physical"]
                .into_iter()
                .any(|stage| rust_use_imports_stage(import, stage))
        })
        .cloned()
        .collect::<BTreeSet<_>>();
    let expected_ir_surface = BTreeSet::from([
        "pub(crate)|logical::build::plan_output_columns".to_string(),
        "pub(crate)|logical::build::plan_query".to_string(),
    ]);
    assert_eq!(
        actual_ir_surface, expected_ir_surface,
        "planner facade must not hide logical/shared IR owners: {facade_uses:?}"
    );

    let logical_mod = fs::read_to_string(&logical_mod_path).unwrap();
    assert!(has_non_comment_line(&logical_mod, "mod node;"));
    let logical_mod_uses = rust_production_use_statements(&logical_mod);
    assert_eq!(
        logical_mod_uses,
        vec!["pub(crate)|node::*"],
        "logical module use surface must only re-export node::*"
    );

    let marker = fs::read_to_string(&marker_path).unwrap();
    let marker_uses = rust_production_use_statements(&marker);
    assert!(
        !marker_uses.iter().any(|import| {
            rust_use_is_public(import) && rust_use_leaf(import) == "ImvVersionRef"
        }),
        "imv_rewrite::marker must not re-export ImvVersionRef: {marker_uses:?}"
    );

    let mut checked = rs_files(&src_dir());
    checked.extend(rs_files(&repo.join("tests")));
    let legacy_logical_path = ["planner::plan::", "Logical"].concat();
    for path in checked {
        let text = fs::read_to_string(&path).unwrap();
        let sanitized = rust_lexically_sanitized(&text);
        let imports = rust_production_use_statements(&text);
        assert!(
            !sanitized.contains(&legacy_logical_path),
            "legacy logical path remains in {}",
            rel(&path)
        );
        for import in &imports {
            if !rust_use_imports_stage(import, "plan") {
                continue;
            }
            let leaf = rust_use_leaf(import);
            assert_ne!(
                leaf,
                "*",
                "legacy planner::plan wildcard import remains after conservative cross-scope alias expansion in {}: {import}",
                rel(&path)
            );
            assert!(
                !leaf.starts_with("Logical"),
                "legacy logical import remains in {}: {import}",
                rel(&path)
            );
        }
        for item in shared_payload {
            assert!(
                !sanitized.contains(&format!("planner::plan::{item}")),
                "legacy shared payload path planner::plan::{item} remains in {}",
                rel(&path)
            );
            assert!(
                !imports.iter().any(|import| {
                    rust_use_imports_stage(import, "plan") && rust_use_leaf(import) == item
                }),
                "legacy shared payload import planner::plan::{item} remains in {}",
                rel(&path)
            );
        }
    }
}

#[test]
fn optimizer_bridge_is_the_only_allowlisted_converter() {
    let bridge = src_dir().join("sql/planner/optimizer_bridge/physical.rs");
    let root = src_dir().join("sql/planner/optimizer_bridge/mod.rs");
    assert!(bridge.exists(), "Bridge 2a must exist at {}", rel(&bridge));
    let text = fs::read_to_string(&bridge).unwrap();
    let root = fs::read_to_string(&root).unwrap();
    assert!(
        text.contains("crate::sql::optimizer"),
        "Bridge 2a should be the explicit optimizer-to-planner conversion boundary"
    );
    assert!(root.contains("pub(crate) fn to_physical_plan"));
    assert!(!root.contains("mod distributed"));
}

#[test]
fn engine_has_no_direct_exec_resurrection() {
    let forbidden = [
        "collapse_distribution_enforcers_for_single_fragment",
        "DirectExecutionReason",
        "execute_query_direct_for_explicit_exception",
        "single_fragment_plan",
    ];
    let mut violations = Vec::new();

    for file in rs_files(&src_dir().join("engine")) {
        for symbol in forbidden {
            for (line, text) in non_test_line_hits(&file, |line| line.contains(symbol)) {
                violations.push(format!(
                    "{}:{}: forbidden direct-exec symbol `{}` in `{}`",
                    rel(&file),
                    line,
                    symbol,
                    text
                ));
            }
        }

        let rel_path = rel(&file);
        let optimizer_physical_allowlist = [
            "src/engine/query_stats.rs",
            "src/engine/dml_change_stream.rs",
            "src/engine/iceberg_change_stream_write.rs",
            "src/engine/mod.rs",
            "src/engine/mutation_flow.rs",
            "src/engine/mv/iceberg_refresh.rs",
        ];
        if !optimizer_physical_allowlist.contains(&rel_path.as_str()) {
            for (line, text) in non_test_line_hits(&file, |line| {
                line.contains("crate::sql::optimizer::physical_tree")
                    || line.contains("OptimizerPhysicalNode")
            }) {
                violations.push(format!(
                    "{}:{}: engine must not consume optimizer physical tree: {}",
                    rel(&file),
                    line,
                    text
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "engine direct-exec / optimizer-physical guard failed:\n{}",
        violations.join("\n")
    );
}

#[test]
fn stage_validation_guard_stays_deleted() {
    let mut violations = Vec::new();
    for file in rs_files(&src_dir().join("sql/planner")) {
        for (line, text) in non_test_line_hits(&file, |line| {
            line.contains("validate_logical_plan_stage")
                || line.contains("validate_physical_plan_stage")
        }) {
            violations.push(format!("{}:{}: {}", rel(&file), line, text));
        }
    }

    assert!(
        violations.is_empty(),
        "stage validation helpers must stay deleted; use type-level stage separation:\n{}",
        violations.join("\n")
    );
}

#[test]
fn build_distributed_plan_signature_is_planner_typed() {
    let path = src_dir().join("sql/planner/distributed/build/mod.rs");
    let text = fs::read_to_string(&path).unwrap();
    let sig = text
        .lines()
        .find(|line| line.contains("fn build_distributed_plan("))
        .expect("build_distributed_plan must exist");

    assert!(
        sig.contains("&PhysicalPlanNode") && !sig.contains("optimizer"),
        "build_distributed_plan must accept planner &PhysicalPlanNode, not optimizer types: {sig}"
    );
}

#[test]
fn distributed_plan_node_has_no_optimizer_payloads() {
    let file = src_dir().join("sql/planner/distributed/node.rs");
    let mut violations = non_test_optimizer_refs(&file)
        .into_iter()
        .map(|(line, text)| format!("{}:{}: {}", rel(&file), line, text))
        .collect::<Vec<_>>();
    let text = fs::read_to_string(&file).unwrap();
    violations.extend(
        rust_production_canonical_paths(&text, &rel(&file))
            .into_iter()
            .filter(|path| {
                path.starts_with(&[
                    "crate".to_string(),
                    "sql".to_string(),
                    "optimizer".to_string(),
                ])
            })
            .map(|path| format!("{}: {}", rel(&file), path.join("::"))),
    );

    assert!(
        violations.is_empty(),
        "DistributedPlanNode must not contain optimizer paths:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nidl_d1_pure_mode_gates_starrocks_compat_behavior() {
    let repo = Path::new(manifest_dir());
    let service_mod = fs::read_to_string(repo.join("src/service/mod.rs")).unwrap();
    for module in [
        "backend_service",
        "heartbeat_service",
        "internal_service",
        "stream_load",
        "stream_load_http",
    ] {
        let expected = format!("#[cfg(feature = \"compat\")]\npub mod {module};");
        assert!(
            service_mod.contains(&expected),
            "service module `{module}` must be compat-gated"
        );
    }

    let grpc = fs::read_to_string(repo.join("src/service/grpc_server.rs")).unwrap();
    assert!(
        grpc.contains("#[cfg(feature = \"compat\")]\nfn build_novarocks_http_app"),
        "stream-load HTTP routes must only exist in compat grpc app"
    );
    assert!(
        grpc.contains(
            "#[cfg(feature = \"compat\")]\n#[derive(Default)]\npub struct StarletGrpcService"
        ),
        "Starlet gRPC service must be compat-gated"
    );
    assert!(
        grpc.contains("SubmitFragmentRequest requires native plan and instance_params"),
        "NovaRocksGrpc SubmitFragment must require native plan and instance_params"
    );
    assert!(
        !grpc.contains("exec_plan_fragment_params_thrift"),
        "NovaRocksGrpc SubmitFragment must not retain thrift fallback payloads"
    );
}

#[test]
fn nidl_d2d_lowering_root_exposes_named_ownership_modules() {
    let repo = Path::new(manifest_dir());
    assert!(
        !repo.join(concat!("src/lower", "_native")).exists(),
        concat!(
            "src/lower",
            "_native must be deleted; native lowering lives under src/lower/novarocks"
        )
    );
    for dir in [
        "src/lower/common",
        "src/lower/compat",
        "src/lower/novarocks",
    ] {
        assert!(repo.join(dir).is_dir(), "{dir} must exist");
    }

    let lower_mod = fs::read_to_string(repo.join("src/lower/mod.rs")).unwrap();
    for expected in [
        "pub(crate) mod common;",
        "pub(crate) mod compat;",
        "pub(crate) mod novarocks;",
    ] {
        assert!(
            lower_mod.contains(expected),
            "src/lower/mod.rs must contain `{expected}`"
        );
    }
    for forbidden in [
        "pub(crate) mod expr;",
        "pub(crate) mod fragment;",
        "pub(crate) mod layout;",
        "pub(crate) mod node;",
        "pub(crate) mod sink;",
        "pub(crate) mod type_lowering;",
        "mod thrift",
        "pub(crate) mod thrift",
    ] {
        assert!(
            !lower_mod.contains(forbidden),
            "src/lower/mod.rs must not keep legacy direct module `{forbidden}`"
        );
    }
}

#[test]
fn nidl_d2d_legacy_lowering_paths_do_not_remain() {
    let forbidden = [
        concat!("crate::", "lower", "_native"),
        concat!("lower", "::thrift"),
        concat!("crate::lower", "::fragment"),
        concat!("crate::lower", "::expr"),
        concat!("crate::lower", "::layout"),
        concat!("crate::lower", "::node"),
        concat!("crate::lower", "::sink"),
        concat!("crate::lower", "::type_lowering"),
    ];

    let mut violations = Vec::new();
    for file in source_and_test_rs_files() {
        for needle in forbidden {
            for (line, text) in source_line_hits(&file, |line| line.contains(needle)) {
                violations.push(format!("{}:{}: {}", rel(&file), line, text));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "D2D lowering paths must use crate::lower::compat, crate::lower::novarocks, or crate::lower::common:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nidl_compat_boundary_names_use_compat_spelling() {
    let repo = Path::new(manifest_dir());
    let mut violations = Vec::new();

    let old_lower_dir = concat!("src/lower/", "compact");
    if repo.join(old_lower_dir).exists() {
        violations.push(format!(
            "{old_lower_dir}: compat lowering directory must use compat spelling"
        ));
    }
    let lower_mod = fs::read_to_string(repo.join("src/lower/mod.rs")).unwrap();
    if lower_mod.contains(concat!("pub(crate) mod ", "compact")) {
        violations
            .push("src/lower/mod.rs: compat lowering module must use compat spelling".to_string());
    }

    let forbidden_terms = [
        concat!("lower::", "compact"),
        concat!("src/lower/", "compact"),
        concat!("compact", "_output_partition"),
        concat!("compact", "_exec_params_from_parts"),
        concat!("compact", "_destination_from_runtime"),
        concat!("to_", "compact", "_exec_params"),
        concat!("compact", "_scan_ranges"),
        concat!("compact", "_scan_range_for_test"),
        concat!("compact", "_scan_ranges_for_placement"),
        concat!("Compact", "CteConsumer"),
        concat!("compact", "_cte"),
        concat!("compact", "_consumers"),
        concat!("compact", "_query_options"),
        concat!("compact", "_ranges"),
        concat!("compact", "_boundary"),
        concat!("compact", "_projection"),
        concat!("compact", "_only"),
        concat!("compact", " projection"),
        concat!("compact", " marker"),
    ];

    for file in source_and_test_rs_files() {
        if rel(&file) == "tests/architecture_guard.rs" {
            continue;
        }
        let text = fs::read_to_string(&file).unwrap();
        for term in forbidden_terms {
            for (line_no, line) in text.lines().enumerate() {
                if line.contains(term) {
                    violations.push(format!(
                        "{}:{}: compat boundary typo `{term}`",
                        rel(&file),
                        line_no + 1
                    ));
                }
            }
        }
    }

    for doc in ["AGENTS.md"] {
        let path = repo.join(doc);
        let text = fs::read_to_string(&path).unwrap();
        for term in forbidden_terms {
            for (line_no, line) in text.lines().enumerate() {
                if line.contains(term) {
                    violations.push(format!(
                        "{doc}:{}: compat boundary typo `{term}`",
                        line_no + 1
                    ));
                }
            }
        }
    }

    assert!(
        violations.is_empty(),
        "compat boundary names must use compat spelling, not compact:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nidl_d2d_common_lowering_has_no_wire_dependencies() {
    let common_dir = src_dir().join("lower/common");
    let forbidden = [
        "native_fragment_wire",
        "crate::thrift",
        "crate::proto",
        "thrift::",
        "proto::",
    ];

    let mut violations = Vec::new();
    for file in rs_files(&common_dir) {
        for needle in forbidden {
            for (line, text) in source_line_hits(&file, |line| line.contains(needle)) {
                violations.push(format!("{}:{}: {}", rel(&file), line, text));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "src/lower/common must stay protocol-neutral and must not depend on thrift/proto/native wire adapters:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nidl_d3a_crate_internal_tests_live_under_src_tests() {
    let repo = Path::new(manifest_dir());
    let proto_contract_dir = repo.join("src/tests/proto_contract");
    let testutil_dir = repo.join("src/tests/testutil");
    let mut violations = Vec::new();
    if repo.join("src/proto_contract").exists() {
        violations.push(
            "src/proto_contract must not be a top-level src module; move it to src/tests/proto_contract"
                .to_string(),
        );
    }
    if repo.join("src/testutil").exists() {
        violations.push(
            "src/testutil must not be a top-level src module; move it to src/tests/testutil"
                .to_string(),
        );
    }
    if !repo.join("src/tests/mod.rs").is_file() {
        violations
            .push("src/tests/mod.rs must own crate-internal white-box test suites".to_string());
    }
    if !repo.join("src/tests/proto_contract/mod.rs").is_file() {
        violations.push(
            "src/tests/proto_contract/mod.rs must own native proto contract tests".to_string(),
        );
    }
    if !testutil_dir.join("mod.rs").is_file() {
        violations.push("src/tests/testutil/mod.rs must own test utility modules".to_string());
    }
    if !testutil_dir.join("chunk.rs").is_file() {
        violations
            .push("chunk test utilities must live at src/tests/testutil/chunk.rs".to_string());
    }

    for file in [
        "common.rs",
        "expr.rs",
        "filter.rs",
        "instance_params.rs",
        "plan.rs",
        "report.rs",
        "service.rs",
    ] {
        let path = proto_contract_dir.join(file);
        if !path.is_file() {
            violations.push(format!(
                "native proto contract test file must live at {}",
                rel(&path)
            ));
        }
    }

    let lib = fs::read_to_string(repo.join("src/lib.rs")).unwrap();
    if !has_cfg_test_mod_tests(&lib) {
        violations.push(
            "src/lib.rs must mount crate-internal white-box tests through #[cfg(test)] mod tests"
                .to_string(),
        );
    }
    if has_module_declaration(&lib, "proto_contract") {
        violations.push("src/lib.rs must not keep the legacy proto_contract module".to_string());
    }
    if has_module_declaration(&lib, "testutil") {
        violations.push("src/lib.rs must not keep the legacy testutil module".to_string());
    }

    if let Ok(root_mod) = fs::read_to_string(repo.join("src/tests/mod.rs")) {
        if !has_module_declaration(&root_mod, "proto_contract") {
            violations.push("src/tests/mod.rs must mount the proto contract suite".to_string());
        }
        if !has_module_declaration(&root_mod, "testutil") {
            violations.push("src/tests/mod.rs must mount test utility modules".to_string());
        }
    }

    if let Ok(testutil_mod) = fs::read_to_string(testutil_dir.join("mod.rs")) {
        if !has_module_declaration(&testutil_mod, "chunk") {
            violations
                .push("src/tests/testutil/mod.rs must mount chunk test utilities".to_string());
        }
    }

    if let Ok(proto_mod) = fs::read_to_string(proto_contract_dir.join("mod.rs")) {
        let declared_modules = module_declarations(&proto_mod);
        let mut file_modules = BTreeSet::new();
        if let Ok(entries) = fs::read_dir(&proto_contract_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().is_some_and(|ext| ext == "rs")
                    && path.file_name().and_then(|name| name.to_str()) != Some("mod.rs")
                {
                    if let Some(module) = path.file_stem().and_then(|stem| stem.to_str()) {
                        file_modules.insert(module.to_string());
                    }
                }
            }
        }

        for module in &file_modules {
            if !declared_modules.contains(module) {
                violations.push(format!(
                    "src/tests/proto_contract/mod.rs must declare `mod {module};`"
                ));
            }
        }
        for module in &declared_modules {
            if !file_modules.contains(module) {
                violations.push(format!(
                    "src/tests/proto_contract/mod.rs declares `{module}`, but src/tests/proto_contract/{module}.rs is missing"
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "proto contract test layout guard failed:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nidl_d3a_test_contract_modules_do_not_leak_into_production_code() {
    let mut violations = Vec::new();
    for file in rs_files(&src_dir()) {
        let rel_path = rel(&file);
        if rel_path == "src/lib.rs" || rel_path.starts_with("src/tests/") {
            continue;
        }

        for (line, text) in non_test_line_hits(&file, |line| {
            line.contains("crate::tests") || line.contains("proto_contract")
        }) {
            violations.push(format!("{}:{}: {}", rel_path, line, text));
        }
    }

    assert!(
        violations.is_empty(),
        "test-only contract modules must not be referenced by production code:\n{}",
        violations.join("\n")
    );
}

#[test]
fn distributed_build_does_not_call_optimizer_cost_model() {
    let mut violations = Vec::new();
    for file in rs_files(&src_dir().join("sql/planner/distributed/build")) {
        for needle in ["compute_cost_estimate", "broadcast_decision("] {
            for (line, text) in non_test_line_hits(&file, |line| line.contains(needle)) {
                violations.push(format!("{}:{}: {}", rel(&file), line, text));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "distributed build must not call optimizer cost model:\n{}",
        violations.join("\n")
    );
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct ProtoSchema {
    version: u32,
    files: BTreeMap<String, ProtoFileSchema>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct ProtoFileSchema {
    package: String,
    messages: BTreeMap<String, ProtoMessageSchema>,
    enums: BTreeMap<String, ProtoEnumSchema>,
    services: BTreeMap<String, ProtoServiceSchema>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct ProtoMessageSchema {
    fields: BTreeMap<u32, ProtoFieldSchema>,
    reserved_numbers: BTreeSet<u32>,
    reserved_names: BTreeSet<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct ProtoFieldSchema {
    number: u32,
    name: String,
    type_name: String,
    label: String,
    oneof: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct ProtoEnumSchema {
    values: Vec<ProtoEnumValueSchema>,
    reserved_numbers: BTreeSet<u32>,
    reserved_names: BTreeSet<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct ProtoEnumValueSchema {
    number: i32,
    name: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct ProtoServiceSchema {
    rpcs: BTreeMap<String, ProtoRpcSchema>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct ProtoRpcSchema {
    request: String,
    response: String,
    client_streaming: bool,
    server_streaming: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ProtoParseContext {
    Message(String),
    Enum(String),
    Service(String),
    Oneof(String),
}

fn proto_context_label(context: &ProtoParseContext) -> String {
    match context {
        ProtoParseContext::Message(name) => format!("message {name}"),
        ProtoParseContext::Enum(name) => format!("enum {name}"),
        ProtoParseContext::Service(name) => format!("service {name}"),
        ProtoParseContext::Oneof(name) => format!("oneof {name}"),
    }
}

fn proto_parse_error(path: &str, statement: &str, detail: impl Into<String>) -> String {
    format!(
        "{}: failed to parse `{}`: {}",
        path,
        statement.trim(),
        detail.into()
    )
}

fn remove_proto_comments(path: &str, input: &str) -> Result<String, String> {
    let chars = input.chars().collect::<Vec<_>>();
    let mut out = String::with_capacity(input.len());
    let mut idx = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    let mut in_block_comment = false;

    while idx < chars.len() {
        let ch = chars[idx];
        let next = chars.get(idx + 1).copied();

        if in_block_comment {
            if ch == '*' && next == Some('/') {
                out.push(' ');
                out.push(' ');
                idx += 2;
                in_block_comment = false;
            } else {
                out.push(if ch == '\n' { '\n' } else { ' ' });
                idx += 1;
            }
            continue;
        }

        if in_string {
            out.push(ch);
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            idx += 1;
            continue;
        }

        if ch == '"' {
            in_string = true;
            out.push(ch);
            idx += 1;
        } else if ch == '/' && next == Some('/') {
            while idx < chars.len() && chars[idx] != '\n' {
                out.push(' ');
                idx += 1;
            }
        } else if ch == '/' && next == Some('*') {
            out.push(' ');
            out.push(' ');
            idx += 2;
            in_block_comment = true;
        } else {
            out.push(ch);
            idx += 1;
        }
    }

    if in_block_comment {
        Err(format!(
            "{path}: failed to parse comment: unterminated block comment"
        ))
    } else {
        Ok(out)
    }
}

fn normalize_proto_statement(statement: &str) -> String {
    let mut out = String::new();
    let mut in_string = false;
    let mut escaped = false;
    let mut pending_space = false;

    for ch in statement.chars() {
        if in_string {
            if pending_space && !out.is_empty() {
                out.push(' ');
                pending_space = false;
            }
            out.push(ch);
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }

        if ch == '"' {
            if pending_space && !out.is_empty() {
                out.push(' ');
            }
            pending_space = false;
            in_string = true;
            out.push(ch);
        } else if ch.is_whitespace() {
            pending_space = true;
        } else {
            if pending_space && !out.is_empty() {
                out.push(' ');
            }
            pending_space = false;
            out.push(ch);
        }
    }

    out.trim().to_string()
}

fn proto_logical_statements(input: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut in_string = false;
    let mut escaped = false;

    for ch in input.chars() {
        current.push(ch);

        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }

        if ch == '"' {
            in_string = true;
        } else if matches!(ch, ';' | '{' | '}') {
            let statement = normalize_proto_statement(&current);
            if !statement.is_empty() {
                statements.push(statement);
            }
            current.clear();
        }
    }

    let trailing = normalize_proto_statement(&current);
    if !trailing.is_empty() {
        statements.push(trailing);
    }

    statements
}

fn proto_statement_body<'a>(statement: &'a str, suffix: &str) -> Option<&'a str> {
    statement.trim().strip_suffix(suffix).map(str::trim)
}

fn proto_keyword_tail<'a>(statement: &'a str, keyword: &str) -> Option<&'a str> {
    let tail = statement.trim().strip_prefix(keyword)?;
    if tail
        .chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        None
    } else {
        Some(tail.trim_start())
    }
}

fn is_proto_ident_start(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphabetic()
}

fn is_proto_ident_continue(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}

fn is_proto_ident(value: &str) -> bool {
    let mut chars = value.chars();
    chars.next().is_some_and(is_proto_ident_start) && chars.all(is_proto_ident_continue)
}

fn parse_proto_named_block(path: &str, statement: &str, keyword: &str) -> Result<String, String> {
    let body = proto_statement_body(statement, "{")
        .ok_or_else(|| proto_parse_error(path, statement, "expected block opener"))?;
    let name = proto_keyword_tail(body, keyword)
        .ok_or_else(|| proto_parse_error(path, statement, format!("expected `{keyword}`")))?;
    if !is_proto_ident(name) {
        Err(proto_parse_error(
            path,
            statement,
            format!("invalid {keyword} name `{name}`"),
        ))
    } else {
        Ok(name.to_string())
    }
}

fn parse_proto_package(path: &str, statement: &str) -> Result<String, String> {
    let body = proto_statement_body(statement, ";")
        .ok_or_else(|| proto_parse_error(path, statement, "expected package terminator"))?;
    let package = proto_keyword_tail(body, "package")
        .ok_or_else(|| proto_parse_error(path, statement, "expected package name"))?;
    if package.is_empty()
        || !package
            .chars()
            .all(|ch| ch == '.' || ch == '_' || ch.is_ascii_alphanumeric())
    {
        Err(proto_parse_error(
            path,
            statement,
            format!("invalid package name `{package}`"),
        ))
    } else {
        Ok(package.to_string())
    }
}

fn parse_proto_syntax(path: &str, statement: &str) -> Result<(), String> {
    let body = proto_statement_body(statement, ";")
        .ok_or_else(|| proto_parse_error(path, statement, "expected syntax terminator"))?;
    let body = proto_keyword_tail(body, "syntax")
        .ok_or_else(|| proto_parse_error(path, statement, "expected syntax declaration"))?;
    let (left, right) = proto_split_once_top_level(body, '=')
        .ok_or_else(|| proto_parse_error(path, statement, "expected syntax assignment"))?;
    if !left.trim().is_empty() {
        return Err(proto_parse_error(
            path,
            statement,
            format!("unexpected syntax assignment prefix `{}`", left.trim()),
        ));
    }

    let value = right.trim();
    if !(value.starts_with('"') && value.ends_with('"') && value.len() >= 2) {
        return Err(proto_parse_error(
            path,
            statement,
            format!("invalid syntax literal `{value}`"),
        ));
    }
    let syntax = &value[1..value.len() - 1];
    if syntax != "proto3" {
        return Err(proto_parse_error(
            path,
            statement,
            format!("unsupported syntax `{syntax}`; expected `proto3`"),
        ));
    }

    Ok(())
}

fn current_proto_path(stack: &[String], name: &str) -> String {
    if stack.is_empty() {
        name.to_string()
    } else {
        format!("{}.{}", stack.join("."), name)
    }
}

fn truncate_proto_field_options(statement: &str) -> &str {
    let mut angle_depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;

    for (idx, ch) in statement.char_indices() {
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }

        match ch {
            '"' => in_string = true,
            '<' => angle_depth += 1,
            '>' => angle_depth = angle_depth.saturating_sub(1),
            '[' if angle_depth == 0 => return &statement[..idx],
            _ => {}
        }
    }

    statement
}

fn proto_split_once_top_level<'a>(input: &'a str, delimiter: char) -> Option<(&'a str, &'a str)> {
    let mut angle_depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;

    for (idx, ch) in input.char_indices() {
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }

        match ch {
            '"' => in_string = true,
            '<' => angle_depth += 1,
            '>' => angle_depth = angle_depth.saturating_sub(1),
            ch if ch == delimiter && angle_depth == 0 => {
                return Some((&input[..idx], &input[idx + ch.len_utf8()..]));
            }
            _ => {}
        }
    }

    None
}

fn parse_proto_u32(path: &str, statement: &str, value: &str) -> Result<u32, String> {
    value
        .trim()
        .parse::<u32>()
        .map_err(|err| proto_parse_error(path, statement, format!("invalid field number: {err}")))
}

fn parse_proto_i32(path: &str, statement: &str, value: &str) -> Result<i32, String> {
    value
        .trim()
        .parse::<i32>()
        .map_err(|err| proto_parse_error(path, statement, format!("invalid enum value: {err}")))
}

fn proto_take_label(input: &str) -> (&'static str, &str) {
    for label in ["optional", "repeated"] {
        if let Some(tail) = proto_keyword_tail(input, label) {
            return (label, tail);
        }
    }
    ("singular", input.trim())
}

fn proto_split_type_and_name<'a>(
    path: &str,
    statement: &str,
    input: &'a str,
) -> Result<(&'a str, &'a str), String> {
    let input = input.trim();
    let Some(name_start) = input
        .char_indices()
        .rev()
        .find_map(|(idx, ch)| ch.is_whitespace().then_some(idx + ch.len_utf8()))
    else {
        return Err(proto_parse_error(
            path,
            statement,
            "expected `<type> <name>` before `=`",
        ));
    };

    let type_name = input[..name_start].trim();
    let name = input[name_start..].trim();
    if type_name.is_empty() || !is_proto_ident(name) {
        Err(proto_parse_error(
            path,
            statement,
            "expected valid field type and name",
        ))
    } else {
        Ok((type_name, name))
    }
}

fn parse_proto_field(
    path: &str,
    statement: &str,
    oneof: Option<&str>,
) -> Result<ProtoFieldSchema, String> {
    let body = proto_statement_body(statement, ";")
        .ok_or_else(|| proto_parse_error(path, statement, "expected field terminator"))?;
    let body = truncate_proto_field_options(body).trim();
    let (left, right) = proto_split_once_top_level(body, '=')
        .ok_or_else(|| proto_parse_error(path, statement, "expected field number"))?;
    let (number_text, tail) = proto_take_first_token(right)
        .ok_or_else(|| proto_parse_error(path, statement, "missing field number"))?;
    if !tail.is_empty() {
        return Err(proto_parse_error(
            path,
            statement,
            format!("unexpected field number suffix `{tail}`"),
        ));
    }
    let number = parse_proto_u32(path, statement, number_text)?;
    let (label, left) = proto_take_label(left);
    let (type_name, name) = proto_split_type_and_name(path, statement, left)?;

    Ok(ProtoFieldSchema {
        number,
        name: name.to_string(),
        type_name: type_name.to_string(),
        label: label.to_string(),
        oneof: oneof.map(str::to_string),
    })
}

fn proto_split_comma_list(input: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut in_string = false;
    let mut escaped = false;

    for (idx, ch) in input.char_indices() {
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }

        if ch == '"' {
            in_string = true;
        } else if ch == ',' {
            parts.push(input[start..idx].trim());
            start = idx + ch.len_utf8();
        }
    }
    parts.push(input[start..].trim());
    parts
}

fn parse_proto_string_literal(path: &str, statement: &str, value: &str) -> Result<String, String> {
    let value = value.trim();
    if !(value.starts_with('"') && value.ends_with('"') && value.len() >= 2) {
        return Err(proto_parse_error(
            path,
            statement,
            format!("invalid reserved name literal `{value}`"),
        ));
    }

    let inner = &value[1..value.len() - 1];
    let mut out = String::new();
    let mut escaped = false;
    for ch in inner.chars() {
        if escaped {
            out.push(ch);
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else {
            out.push(ch);
        }
    }
    if escaped {
        Err(proto_parse_error(
            path,
            statement,
            "unterminated escape in string literal",
        ))
    } else {
        Ok(out)
    }
}

fn parse_proto_reserved(
    path: &str,
    statement: &str,
) -> Result<(BTreeSet<u32>, BTreeSet<String>), String> {
    let body = proto_statement_body(statement, ";")
        .ok_or_else(|| proto_parse_error(path, statement, "expected reserved terminator"))?;
    let body = proto_keyword_tail(body, "reserved")
        .ok_or_else(|| proto_parse_error(path, statement, "expected reserved clause"))?;
    let mut numbers = BTreeSet::new();
    let mut names = BTreeSet::new();

    for part in proto_split_comma_list(body) {
        if part.is_empty() {
            return Err(proto_parse_error(path, statement, "empty reserved item"));
        }
        if part.starts_with('"') {
            names.insert(parse_proto_string_literal(path, statement, part)?);
        } else if let Some((start, end)) = part.split_once(" to ") {
            let start = parse_proto_u32(path, statement, start)?;
            let end = parse_proto_u32(path, statement, end)?;
            if start > end {
                return Err(proto_parse_error(
                    path,
                    statement,
                    "reserved range start is greater than end",
                ));
            }
            numbers.extend(start..=end);
        } else {
            numbers.insert(parse_proto_u32(path, statement, part)?);
        }
    }

    Ok((numbers, names))
}

fn proto_take_first_token(input: &str) -> Option<(&str, &str)> {
    let input = input.trim_start();
    if input.is_empty() {
        return None;
    }

    for (idx, ch) in input.char_indices() {
        if ch.is_whitespace() {
            return Some((&input[..idx], input[idx..].trim_start()));
        }
    }

    Some((input, ""))
}

fn parse_proto_enum_value(path: &str, statement: &str) -> Result<ProtoEnumValueSchema, String> {
    let body = proto_statement_body(statement, ";")
        .ok_or_else(|| proto_parse_error(path, statement, "expected enum value terminator"))?;
    let body = truncate_proto_field_options(body).trim();
    let (name, right) = body
        .split_once('=')
        .ok_or_else(|| proto_parse_error(path, statement, "expected enum value number"))?;
    let name = name.trim();
    if !is_proto_ident(name) {
        return Err(proto_parse_error(
            path,
            statement,
            format!("invalid enum value name `{name}`"),
        ));
    }
    let (number_text, tail) = proto_take_first_token(right)
        .ok_or_else(|| proto_parse_error(path, statement, "missing enum value number"))?;
    if !tail.is_empty() {
        return Err(proto_parse_error(
            path,
            statement,
            format!("unexpected enum value number suffix `{tail}`"),
        ));
    }
    Ok(ProtoEnumValueSchema {
        number: parse_proto_i32(path, statement, number_text)?,
        name: name.to_string(),
    })
}

fn proto_take_ident(input: &str) -> Option<(&str, &str)> {
    let input = input.trim_start();
    let mut chars = input.char_indices();
    let (_, first) = chars.next()?;
    if !is_proto_ident_start(first) {
        return None;
    }

    let mut end = first.len_utf8();
    for (idx, ch) in chars {
        if is_proto_ident_continue(ch) {
            end = idx + ch.len_utf8();
        } else {
            break;
        }
    }
    Some((&input[..end], &input[end..]))
}

fn proto_take_parenthesized(
    path: &str,
    statement: &str,
    input: &str,
) -> Result<(String, String), String> {
    let input = input.trim_start();
    if !input.starts_with('(') {
        return Err(proto_parse_error(path, statement, "expected `(`"));
    }

    let mut depth = 0isize;
    for (idx, ch) in input.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    let inside = input[1..idx].trim().to_string();
                    let tail = input[idx + ch.len_utf8()..].trim_start().to_string();
                    return Ok((inside, tail));
                }
            }
            _ => {}
        }
    }

    Err(proto_parse_error(
        path,
        statement,
        "unterminated parenthesized type",
    ))
}

fn parse_proto_stream_type(input: &str) -> (bool, String) {
    if let Some(tail) = proto_keyword_tail(input, "stream") {
        (true, tail.trim().to_string())
    } else {
        (false, input.trim().to_string())
    }
}

fn parse_proto_rpc(path: &str, statement: &str) -> Result<(String, ProtoRpcSchema), String> {
    let body = proto_statement_body(statement, ";")
        .ok_or_else(|| proto_parse_error(path, statement, "expected rpc terminator"))?;
    let body = proto_keyword_tail(body, "rpc")
        .ok_or_else(|| proto_parse_error(path, statement, "expected rpc declaration"))?;
    let (name, tail) = proto_take_ident(body)
        .ok_or_else(|| proto_parse_error(path, statement, "expected rpc name"))?;
    let (request, tail) = proto_take_parenthesized(path, statement, tail)?;
    let tail = proto_keyword_tail(&tail, "returns")
        .ok_or_else(|| proto_parse_error(path, statement, "expected returns clause"))?;
    let (response, tail) = proto_take_parenthesized(path, statement, tail)?;
    if !tail.trim().is_empty() {
        return Err(proto_parse_error(
            path,
            statement,
            format!("unexpected rpc suffix `{tail}`"),
        ));
    }

    let (client_streaming, request) = parse_proto_stream_type(&request);
    let (server_streaming, response) = parse_proto_stream_type(&response);
    if request.is_empty() || response.is_empty() {
        return Err(proto_parse_error(
            path,
            statement,
            "rpc request and response types must be non-empty",
        ));
    }

    Ok((
        name.to_string(),
        ProtoRpcSchema {
            request,
            response,
            client_streaming,
            server_streaming,
        },
    ))
}

fn parse_proto_schema(path: &str, input: &str) -> Result<ProtoFileSchema, String> {
    let input = remove_proto_comments(path, input)?;
    let statements = proto_logical_statements(&input);
    let mut schema = ProtoFileSchema {
        package: String::new(),
        messages: BTreeMap::new(),
        enums: BTreeMap::new(),
        services: BTreeMap::new(),
    };
    let mut contexts = Vec::new();
    let mut message_stack = Vec::new();
    let mut enum_stack = Vec::new();
    let mut service_stack = Vec::new();
    let mut oneof_stack = Vec::new();
    let mut last_statement = None;

    for statement in statements {
        last_statement = Some(statement.clone());
        if statement == "}" {
            match contexts.pop() {
                Some(ProtoParseContext::Message(name)) => {
                    let popped = message_stack.pop();
                    if popped.as_deref() != Some(name.as_str()) {
                        return Err(proto_parse_error(
                            path,
                            &statement,
                            "message context stack became inconsistent",
                        ));
                    }
                }
                Some(ProtoParseContext::Enum(name)) => {
                    let popped = enum_stack.pop();
                    if popped.as_deref() != Some(name.as_str()) {
                        return Err(proto_parse_error(
                            path,
                            &statement,
                            "enum context stack became inconsistent",
                        ));
                    }
                }
                Some(ProtoParseContext::Service(name)) => {
                    let popped = service_stack.pop();
                    if popped.as_deref() != Some(name.as_str()) {
                        return Err(proto_parse_error(
                            path,
                            &statement,
                            "service context stack became inconsistent",
                        ));
                    }
                }
                Some(ProtoParseContext::Oneof(name)) => {
                    let popped = oneof_stack.pop();
                    if popped.as_deref() != Some(name.as_str()) {
                        return Err(proto_parse_error(
                            path,
                            &statement,
                            "oneof context stack became inconsistent",
                        ));
                    }
                }
                None => {
                    return Err(proto_parse_error(
                        path,
                        &statement,
                        "unexpected closing brace",
                    ));
                }
            }
            continue;
        }

        if proto_keyword_tail(&statement, "message").is_some() && statement.ends_with('{') {
            let name = parse_proto_named_block(path, &statement, "message")?;
            let key = current_proto_path(&message_stack, &name);
            if schema
                .messages
                .insert(
                    key.clone(),
                    ProtoMessageSchema {
                        fields: BTreeMap::new(),
                        reserved_numbers: BTreeSet::new(),
                        reserved_names: BTreeSet::new(),
                    },
                )
                .is_some()
            {
                return Err(proto_parse_error(
                    path,
                    &statement,
                    format!("duplicate message `{key}`"),
                ));
            }
            message_stack.push(name.clone());
            contexts.push(ProtoParseContext::Message(name));
            continue;
        }

        if proto_keyword_tail(&statement, "enum").is_some() && statement.ends_with('{') {
            let name = parse_proto_named_block(path, &statement, "enum")?;
            let key = current_proto_path(&message_stack, &name);
            if schema
                .enums
                .insert(
                    key.clone(),
                    ProtoEnumSchema {
                        values: Vec::new(),
                        reserved_numbers: BTreeSet::new(),
                        reserved_names: BTreeSet::new(),
                    },
                )
                .is_some()
            {
                return Err(proto_parse_error(
                    path,
                    &statement,
                    format!("duplicate enum `{key}`"),
                ));
            }
            enum_stack.push(key.clone());
            contexts.push(ProtoParseContext::Enum(key));
            continue;
        }

        if proto_keyword_tail(&statement, "service").is_some() && statement.ends_with('{') {
            let name = parse_proto_named_block(path, &statement, "service")?;
            if !message_stack.is_empty() || !enum_stack.is_empty() || !service_stack.is_empty() {
                return Err(proto_parse_error(
                    path,
                    &statement,
                    "service declarations must be top-level",
                ));
            }
            if schema
                .services
                .insert(
                    name.clone(),
                    ProtoServiceSchema {
                        rpcs: BTreeMap::new(),
                    },
                )
                .is_some()
            {
                return Err(proto_parse_error(
                    path,
                    &statement,
                    format!("duplicate service `{name}`"),
                ));
            }
            service_stack.push(name.clone());
            contexts.push(ProtoParseContext::Service(name));
            continue;
        }

        if proto_keyword_tail(&statement, "oneof").is_some() && statement.ends_with('{') {
            let name = parse_proto_named_block(path, &statement, "oneof")?;
            if message_stack.is_empty() || !enum_stack.is_empty() || !service_stack.is_empty() {
                return Err(proto_parse_error(
                    path,
                    &statement,
                    "oneof declarations must be inside a message",
                ));
            }
            oneof_stack.push(name.clone());
            contexts.push(ProtoParseContext::Oneof(name));
            continue;
        }

        if statement.starts_with("syntax ") {
            parse_proto_syntax(path, &statement)?;
            continue;
        }

        if statement.starts_with("import ") {
            if statement.ends_with(';') {
                continue;
            }
            return Err(proto_parse_error(
                path,
                &statement,
                "expected statement terminator",
            ));
        }

        if proto_keyword_tail(&statement, "package").is_some() {
            schema.package = parse_proto_package(path, &statement)?;
            continue;
        }

        if proto_keyword_tail(&statement, "reserved").is_some() {
            let (numbers, names) = parse_proto_reserved(path, &statement)?;
            if let Some(enum_key) = enum_stack.last() {
                let enum_schema = schema.enums.get_mut(enum_key).ok_or_else(|| {
                    proto_parse_error(path, &statement, "enum context is missing")
                })?;
                enum_schema.reserved_numbers.extend(numbers);
                enum_schema.reserved_names.extend(names);
            } else {
                let message_key = current_proto_path(&message_stack, "");
                let message_key = message_key.trim_end_matches('.');
                let message = schema.messages.get_mut(message_key).ok_or_else(|| {
                    proto_parse_error(path, &statement, "reserved clause must be inside a message")
                })?;
                message.reserved_numbers.extend(numbers);
                message.reserved_names.extend(names);
            }
            continue;
        }

        if let Some(service_key) = service_stack.last() {
            if proto_keyword_tail(&statement, "rpc").is_some() {
                let (name, rpc) = parse_proto_rpc(path, &statement)?;
                let service = schema.services.get_mut(service_key).ok_or_else(|| {
                    proto_parse_error(path, &statement, "service context is missing")
                })?;
                if service.rpcs.insert(name.clone(), rpc).is_some() {
                    return Err(proto_parse_error(
                        path,
                        &statement,
                        format!("duplicate rpc `{name}`"),
                    ));
                }
                continue;
            }
            return Err(proto_parse_error(
                path,
                &statement,
                "unsupported service statement",
            ));
        }

        if let Some(enum_key) = enum_stack.last() {
            let value = parse_proto_enum_value(path, &statement)?;
            let enum_schema = schema
                .enums
                .get_mut(enum_key)
                .ok_or_else(|| proto_parse_error(path, &statement, "enum context is missing"))?;
            enum_schema.values.push(value);
            continue;
        }

        if !message_stack.is_empty() {
            let message_key = current_proto_path(&message_stack, "");
            let message_key = message_key.trim_end_matches('.');
            let field =
                parse_proto_field(path, &statement, oneof_stack.last().map(String::as_str))?;
            let message = schema
                .messages
                .get_mut(message_key)
                .ok_or_else(|| proto_parse_error(path, &statement, "message context is missing"))?;
            if message.fields.insert(field.number, field).is_some() {
                return Err(proto_parse_error(
                    path,
                    &statement,
                    "duplicate field number",
                ));
            }
            continue;
        }

        return Err(proto_parse_error(
            path,
            &statement,
            "unsupported top-level statement",
        ));
    }

    if !contexts.is_empty() {
        let context = contexts
            .iter()
            .map(proto_context_label)
            .collect::<Vec<_>>()
            .join(" > ");
        let statement = last_statement.unwrap_or_else(|| "<end of file>".to_string());
        return Err(proto_parse_error(
            path,
            &statement,
            format!("unclosed block context: {context}"),
        ));
    }

    Ok(schema)
}

fn parse_current_novarocks_proto_schema() -> Result<ProtoSchema, String> {
    let mut files = BTreeMap::new();
    for file in proto_files(&Path::new(manifest_dir()).join("idl/novarocks")) {
        let relative = rel(&file);
        let input = fs::read_to_string(&file)
            .map_err(|err| format!("{}: failed to read proto file: {err}", relative))?;
        files.insert(relative.clone(), parse_proto_schema(&relative, &input)?);
    }

    Ok(ProtoSchema { version: 1, files })
}

fn nidl_d3b_baseline_path() -> PathBuf {
    Path::new(manifest_dir()).join(NIDL_D3B_BASELINE_PATH)
}

fn read_proto_schema_baseline(path: &Path) -> Result<ProtoSchema, String> {
    let input = fs::read_to_string(path)
        .map_err(|err| format!("{}: failed to read proto schema baseline: {err}", rel(path)))?;
    serde_json::from_str(&input).map_err(|err| {
        format!(
            "{}: failed to parse proto schema baseline JSON: {err}",
            rel(path)
        )
    })
}

fn write_proto_schema_baseline(path: &Path, schema: &ProtoSchema) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| {
            format!(
                "{}: failed to create proto schema baseline directory: {err}",
                rel(parent)
            )
        })?;
    }

    let mut json = serde_json::to_string_pretty(schema)
        .map_err(|err| format!("failed to serialize proto schema baseline JSON: {err}"))?;
    json.push('\n');
    fs::write(path, json).map_err(|err| {
        format!(
            "{}: failed to write proto schema baseline: {err}",
            rel(path)
        )
    })
}

fn next_proto_schema_baseline_for_write(
    current: &ProtoSchema,
    baseline_path: &Path,
) -> Result<ProtoSchema, String> {
    if baseline_path.exists() {
        let existing = read_proto_schema_baseline(baseline_path)?;
        merge_proto_schema_baseline(current, &existing)
    } else {
        Err(format!(
            "{}: proto schema baseline is missing; {NIDL_D3B_WRITE_BASELINE_ENV}=1 can only update an existing baseline after D3B is established",
            rel(baseline_path)
        ))
    }
}

fn compare_proto_schema_to_baseline(current: &ProtoSchema, baseline: &ProtoSchema) -> Vec<String> {
    let mut violations = Vec::new();

    if current.version != 1 {
        violations.push(format!(
            "current proto schema version must be 1, got {}",
            current.version
        ));
    }
    if baseline.version != 1 {
        violations.push(format!(
            "baseline proto schema version must be 1, got {}",
            baseline.version
        ));
    }

    for path in baseline.files.keys() {
        if !current.files.contains_key(path) {
            violations.push(format!("{path} file removed from current proto schema"));
        }
    }

    for (path, current_file) in &current.files {
        if !baseline.files.contains_key(path) {
            violations.push(format!(
                "{path} baseline stale: new file is missing from baseline; run the proto schema baseline write command"
            ));
            for service_name in current_file.services.keys() {
                violations.push(format!(
                    "{path} service {service_name} new service is not allowed; D3B only allows extending existing NovaRocksGrpc"
                ));
            }
        }
    }

    for (path, baseline_file) in &baseline.files {
        let Some(current_file) = current.files.get(path) else {
            continue;
        };

        if current_file.package != baseline_file.package {
            violations.push(format!(
                "{path} package changed from {} to {}",
                baseline_file.package, current_file.package
            ));
        }

        compare_proto_messages_to_baseline(path, current_file, baseline_file, &mut violations);
        compare_proto_enums_to_baseline(path, current_file, baseline_file, &mut violations);
        compare_proto_services_to_baseline(path, current_file, baseline_file, &mut violations);
    }

    violations.sort();
    violations.dedup();
    violations
}

fn merge_proto_schema_baseline(
    current: &ProtoSchema,
    existing: &ProtoSchema,
) -> Result<ProtoSchema, String> {
    let unsafe_violations = compare_proto_schema_to_baseline(current, existing)
        .into_iter()
        .filter(|violation| !is_proto_schema_baseline_stale_violation(violation))
        .collect::<Vec<_>>();
    if !unsafe_violations.is_empty() {
        return Err(format!(
            "cannot merge proto schema baseline because current schema contains incompatible changes:\n{}",
            format_proto_schema_violations(&unsafe_violations)
        ));
    }

    let mut merged = current.clone();
    for (path, existing_file) in &existing.files {
        let Some(current_file) = current.files.get(path) else {
            continue;
        };
        let Some(merged_file) = merged.files.get_mut(path) else {
            continue;
        };
        merge_proto_file_schema_baseline(path, current_file, existing_file, merged_file)?;
    }

    Ok(merged)
}

fn is_proto_schema_baseline_stale_violation(violation: &str) -> bool {
    violation.contains("baseline stale: new ")
}

fn merge_proto_file_schema_baseline(
    path: &str,
    current_file: &ProtoFileSchema,
    existing_file: &ProtoFileSchema,
    merged_file: &mut ProtoFileSchema,
) -> Result<(), String> {
    for (message_name, existing_message) in &existing_file.messages {
        let Some(current_message) = current_file.messages.get(message_name) else {
            continue;
        };
        let Some(merged_message) = merged_file.messages.get_mut(message_name) else {
            continue;
        };
        merge_proto_message_schema_baseline(
            path,
            message_name,
            current_message,
            existing_message,
            merged_message,
        )?;
    }

    Ok(())
}

fn merge_proto_message_schema_baseline(
    path: &str,
    message_name: &str,
    current_message: &ProtoMessageSchema,
    existing_message: &ProtoMessageSchema,
    merged_message: &mut ProtoMessageSchema,
) -> Result<(), String> {
    for (number, existing_field) in &existing_message.fields {
        if current_message.fields.contains_key(number) {
            continue;
        }
        if current_message.reserved_numbers.contains(number)
            && current_message
                .reserved_names
                .contains(&existing_field.name)
        {
            merged_message
                .fields
                .insert(*number, existing_field.clone());
        } else {
            return Err(format!(
                "{path} {message_name} removed field #{number} {} without reserved number {number} and reserved name {}; refusing to write proto schema baseline",
                existing_field.name, existing_field.name
            ));
        }
    }

    Ok(())
}

fn format_proto_schema_violations(violations: &[String]) -> String {
    violations
        .iter()
        .map(|violation| format!("  - {violation}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn compare_proto_messages_to_baseline(
    path: &str,
    current_file: &ProtoFileSchema,
    baseline_file: &ProtoFileSchema,
    violations: &mut Vec<String>,
) {
    for message_name in baseline_file.messages.keys() {
        if !current_file.messages.contains_key(message_name) {
            violations.push(format!("{path} message {message_name} removed"));
        }
    }

    for message_name in current_file.messages.keys() {
        if !baseline_file.messages.contains_key(message_name) {
            violations.push(format!(
                "{path} message {message_name} baseline stale: new message is missing from baseline; run the proto schema baseline write command"
            ));
        }
    }

    for (message_name, baseline_message) in &baseline_file.messages {
        let Some(current_message) = current_file.messages.get(message_name) else {
            continue;
        };
        compare_proto_message_to_baseline(
            path,
            message_name,
            current_message,
            baseline_message,
            violations,
        );
    }
}

fn compare_proto_message_to_baseline(
    path: &str,
    message_name: &str,
    current_message: &ProtoMessageSchema,
    baseline_message: &ProtoMessageSchema,
    violations: &mut Vec<String>,
) {
    for number in &baseline_message.reserved_numbers {
        if !current_message.reserved_numbers.contains(number) {
            violations.push(format!(
                "{path} {message_name} reserved number {number} removed from current schema"
            ));
        }
    }
    for name in &baseline_message.reserved_names {
        if !current_message.reserved_names.contains(name) {
            violations.push(format!(
                "{path} {message_name} reserved name {name} removed from current schema"
            ));
        }
    }

    for current_field in current_message.fields.values() {
        if baseline_message
            .reserved_numbers
            .contains(&current_field.number)
        {
            violations.push(format!(
                "{path} {message_name} field #{} {} uses baseline reserved number {}",
                current_field.number, current_field.name, current_field.number
            ));
        }
        if baseline_message
            .reserved_names
            .contains(&current_field.name)
        {
            violations.push(format!(
                "{path} {message_name} field #{} {} uses baseline reserved name {}",
                current_field.number, current_field.name, current_field.name
            ));
        }
    }

    let baseline_fields_by_name: BTreeMap<&str, u32> = baseline_message
        .fields
        .values()
        .map(|field| (field.name.as_str(), field.number))
        .collect();

    for (number, baseline_field) in &baseline_message.fields {
        let Some(current_field) = current_message.fields.get(number) else {
            if !current_message.reserved_numbers.contains(number) {
                violations.push(format!(
                    "{path} {message_name} removed field #{number} {} without reserved number {number}",
                    baseline_field.name
                ));
            }
            if !current_message
                .reserved_names
                .contains(&baseline_field.name)
            {
                violations.push(format!(
                    "{path} {message_name} removed field #{number} {} without reserved name {}",
                    baseline_field.name, baseline_field.name
                ));
            }
            continue;
        };

        let baseline_signature = proto_field_signature(baseline_field);
        let current_signature = proto_field_signature(current_field);
        if baseline_field.name != current_field.name
            && baseline_field.type_name != current_field.type_name
        {
            violations.push(format!(
                "{path} {message_name} field #{number} field number reuse: changed from {baseline_signature} to {current_signature}"
            ));
        } else if baseline_field.name != current_field.name {
            violations.push(format!(
                "{path} {message_name} field #{number} field rename: changed from {baseline_signature} to {current_signature}"
            ));
        } else if baseline_field.type_name != current_field.type_name {
            violations.push(format!(
                "{path} {message_name} field #{number} field type change: changed from {baseline_signature} to {current_signature}"
            ));
        }

        if baseline_field.label != current_field.label {
            violations.push(format!(
                "{path} {message_name} field #{number} field label change: changed from {baseline_signature} to {current_signature}"
            ));
        }
        if baseline_field.oneof != current_field.oneof {
            violations.push(format!(
                "{path} {message_name} field #{number} field oneof change: changed from {baseline_signature} to {current_signature}"
            ));
        }
    }

    for current_field in current_message.fields.values() {
        if baseline_message.fields.contains_key(&current_field.number) {
            continue;
        }
        if baseline_message
            .reserved_numbers
            .contains(&current_field.number)
            || baseline_message
                .reserved_names
                .contains(&current_field.name)
        {
            continue;
        }
        if let Some(baseline_number) = baseline_fields_by_name.get(current_field.name.as_str()) {
            violations.push(format!(
                "{path} {message_name} field {} field renumbered from #{baseline_number} to #{}",
                current_field.name, current_field.number
            ));
        } else {
            violations.push(format!(
                "{path} {message_name} field #{} {} baseline stale: new field is missing from baseline; run the proto schema baseline write command",
                current_field.number, current_field.name
            ));
        }
    }
}

fn compare_proto_enums_to_baseline(
    path: &str,
    current_file: &ProtoFileSchema,
    baseline_file: &ProtoFileSchema,
    violations: &mut Vec<String>,
) {
    for enum_name in baseline_file.enums.keys() {
        if !current_file.enums.contains_key(enum_name) {
            violations.push(format!("{path} enum {enum_name} removed"));
        }
    }

    for (enum_name, current_enum) in &current_file.enums {
        validate_proto_enum_zero_value(path, enum_name, current_enum, violations);
        if !baseline_file.enums.contains_key(enum_name) {
            violations.push(format!(
                "{path} enum {enum_name} baseline stale: new enum is missing from baseline; run the proto schema baseline write command"
            ));
        }
    }

    for (enum_name, baseline_enum) in &baseline_file.enums {
        let Some(current_enum) = current_file.enums.get(enum_name) else {
            continue;
        };
        compare_proto_enum_to_baseline(path, enum_name, current_enum, baseline_enum, violations);
    }
}

fn compare_proto_enum_to_baseline(
    path: &str,
    enum_name: &str,
    current_enum: &ProtoEnumSchema,
    baseline_enum: &ProtoEnumSchema,
    violations: &mut Vec<String>,
) {
    for number in &baseline_enum.reserved_numbers {
        if !current_enum.reserved_numbers.contains(number) {
            violations.push(format!(
                "{path} enum {enum_name} reserved number {number} removed from current schema"
            ));
        }
    }
    for name in &baseline_enum.reserved_names {
        if !current_enum.reserved_names.contains(name) {
            violations.push(format!(
                "{path} enum {enum_name} reserved name {name} removed from current schema"
            ));
        }
    }

    let baseline_values_by_number: BTreeMap<i32, &ProtoEnumValueSchema> = baseline_enum
        .values
        .iter()
        .map(|value| (value.number, value))
        .collect();
    let baseline_values_by_name: BTreeMap<&str, i32> = baseline_enum
        .values
        .iter()
        .map(|value| (value.name.as_str(), value.number))
        .collect();
    let current_values_by_number: BTreeMap<i32, &ProtoEnumValueSchema> = current_enum
        .values
        .iter()
        .map(|value| (value.number, value))
        .collect();
    let current_values_by_name: BTreeMap<&str, i32> = current_enum
        .values
        .iter()
        .map(|value| (value.name.as_str(), value.number))
        .collect();

    for current_value in &current_enum.values {
        if u32::try_from(current_value.number)
            .ok()
            .is_some_and(|number| baseline_enum.reserved_numbers.contains(&number))
        {
            violations.push(format!(
                "{path} enum {enum_name} value {}={} uses baseline reserved number {}",
                current_value.name, current_value.number, current_value.number
            ));
        }
        if baseline_enum.reserved_names.contains(&current_value.name) {
            violations.push(format!(
                "{path} enum {enum_name} value {}={} uses baseline reserved name {}",
                current_value.name, current_value.number, current_value.name
            ));
        }
    }

    for baseline_value in &baseline_enum.values {
        if let Some(current_value) = current_values_by_number.get(&baseline_value.number) {
            if current_value.name != baseline_value.name {
                violations.push(format!(
                    "{path} enum {enum_name} value #{} renamed from {} to {}",
                    baseline_value.number, baseline_value.name, current_value.name
                ));
            }
        } else if let Some(current_number) =
            current_values_by_name.get(baseline_value.name.as_str())
        {
            violations.push(format!(
                "{path} enum {enum_name} value {} renumbered from #{} to #{}",
                baseline_value.name, baseline_value.number, current_number
            ));
        } else {
            violations.push(format!(
                "{path} enum {enum_name} value {}={} removed",
                baseline_value.name, baseline_value.number
            ));
        }
    }

    for current_value in &current_enum.values {
        if baseline_values_by_number.contains_key(&current_value.number)
            || baseline_values_by_name.contains_key(current_value.name.as_str())
            || u32::try_from(current_value.number)
                .ok()
                .is_some_and(|number| baseline_enum.reserved_numbers.contains(&number))
            || baseline_enum.reserved_names.contains(&current_value.name)
        {
            continue;
        }
        violations.push(format!(
            "{path} enum {enum_name} value {}={} baseline stale: new enum value is missing from baseline; run the proto schema baseline write command",
            current_value.name, current_value.number
        ));
    }
}

fn validate_proto_enum_zero_value(
    path: &str,
    enum_name: &str,
    current_enum: &ProtoEnumSchema,
    violations: &mut Vec<String>,
) {
    if !current_enum
        .values
        .first()
        .is_some_and(|value| value.number == 0 && value.name.ends_with("_UNSPECIFIED"))
    {
        violations.push(format!(
            "{path} enum {enum_name} enum zero value: first value must be *_UNSPECIFIED = 0"
        ));
    }
}

fn compare_proto_services_to_baseline(
    path: &str,
    current_file: &ProtoFileSchema,
    baseline_file: &ProtoFileSchema,
    violations: &mut Vec<String>,
) {
    for service_name in baseline_file.services.keys() {
        if !current_file.services.contains_key(service_name) {
            violations.push(format!("{path} service {service_name} removed"));
        }
    }

    for service_name in current_file.services.keys() {
        if !baseline_file.services.contains_key(service_name) {
            violations.push(format!(
                "{path} service {service_name} new service is not allowed; D3B only allows extending existing NovaRocksGrpc"
            ));
        }
    }

    for (service_name, baseline_service) in &baseline_file.services {
        let Some(current_service) = current_file.services.get(service_name) else {
            continue;
        };
        compare_proto_service_to_baseline(
            path,
            service_name,
            current_service,
            baseline_service,
            violations,
        );
    }
}

fn compare_proto_service_to_baseline(
    path: &str,
    service_name: &str,
    current_service: &ProtoServiceSchema,
    baseline_service: &ProtoServiceSchema,
    violations: &mut Vec<String>,
) {
    for rpc_name in baseline_service.rpcs.keys() {
        if !current_service.rpcs.contains_key(rpc_name) {
            violations.push(format!(
                "{path} service {service_name} rpc {rpc_name} removed"
            ));
        }
    }

    for (rpc_name, current_rpc) in &current_service.rpcs {
        let Some(baseline_rpc) = baseline_service.rpcs.get(rpc_name) else {
            violations.push(format!(
                "{path} service {service_name} rpc {rpc_name} baseline stale: new rpc is missing from baseline; run the proto schema baseline write command"
            ));
            continue;
        };

        if current_rpc != baseline_rpc {
            violations.push(format!(
                "{path} service {service_name} rpc {rpc_name} signature changed: rpc signature changed from {} to {}",
                proto_rpc_signature(baseline_rpc),
                proto_rpc_signature(current_rpc)
            ));
        }
    }
}

fn proto_field_signature(field: &ProtoFieldSchema) -> String {
    let mut signature = format!("{}:{}/{}", field.name, field.type_name, field.label);
    if let Some(oneof) = &field.oneof {
        signature.push_str("/oneof=");
        signature.push_str(oneof);
    }
    signature
}

fn proto_rpc_signature(rpc: &ProtoRpcSchema) -> String {
    let request = if rpc.client_streaming {
        format!("stream {}", rpc.request)
    } else {
        rpc.request.clone()
    };
    let response = if rpc.server_streaming {
        format!("stream {}", rpc.response)
    } else {
        rpc.response.clone()
    };
    format!("{request} -> {response}")
}

fn test_proto_field(number: u32, name: &str, type_name: &str) -> ProtoFieldSchema {
    ProtoFieldSchema {
        number,
        name: name.to_string(),
        type_name: type_name.to_string(),
        label: "singular".to_string(),
        oneof: None,
    }
}

fn test_proto_field_with_label(
    number: u32,
    name: &str,
    type_name: &str,
    label: &str,
) -> ProtoFieldSchema {
    let mut field = test_proto_field(number, name, type_name);
    field.label = label.to_string();
    field
}

fn test_proto_field_with_oneof(
    number: u32,
    name: &str,
    type_name: &str,
    oneof: &str,
) -> ProtoFieldSchema {
    let mut field = test_proto_field(number, name, type_name);
    field.oneof = Some(oneof.to_string());
    field
}

fn test_proto_message_with_reserved(
    fields: Vec<ProtoFieldSchema>,
    reserved_numbers: &[u32],
    reserved_names: &[&str],
) -> ProtoMessageSchema {
    ProtoMessageSchema {
        fields: fields
            .into_iter()
            .map(|field| (field.number, field))
            .collect(),
        reserved_numbers: reserved_numbers.iter().copied().collect(),
        reserved_names: reserved_names
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
    }
}

fn test_proto_message(fields: Vec<ProtoFieldSchema>) -> ProtoMessageSchema {
    test_proto_message_with_reserved(fields, &[], &[])
}

fn test_proto_enum(values: Vec<(i32, &str)>) -> ProtoEnumSchema {
    test_proto_enum_with_reserved(values, &[], &[])
}

fn test_proto_enum_with_reserved(
    values: Vec<(i32, &str)>,
    reserved_numbers: &[u32],
    reserved_names: &[&str],
) -> ProtoEnumSchema {
    ProtoEnumSchema {
        values: values
            .into_iter()
            .map(|(number, name)| ProtoEnumValueSchema {
                number,
                name: name.to_string(),
            })
            .collect(),
        reserved_numbers: reserved_numbers.iter().copied().collect(),
        reserved_names: reserved_names
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
    }
}

fn test_proto_rpc(request: &str, response: &str) -> ProtoRpcSchema {
    ProtoRpcSchema {
        request: request.to_string(),
        response: response.to_string(),
        client_streaming: false,
        server_streaming: false,
    }
}

fn test_proto_service(rpcs: Vec<(&str, ProtoRpcSchema)>) -> ProtoServiceSchema {
    ProtoServiceSchema {
        rpcs: rpcs
            .into_iter()
            .map(|(name, rpc)| (name.to_string(), rpc))
            .collect(),
    }
}

fn test_proto_schema(
    messages: Vec<(&str, ProtoMessageSchema)>,
    enums: Vec<(&str, ProtoEnumSchema)>,
    services: Vec<(&str, ProtoServiceSchema)>,
) -> ProtoSchema {
    test_proto_schema_with_files(vec![(
        "idl/novarocks/test.proto",
        test_proto_file("novarocks.test", messages, enums, services),
    )])
}

fn test_proto_file(
    package: &str,
    messages: Vec<(&str, ProtoMessageSchema)>,
    enums: Vec<(&str, ProtoEnumSchema)>,
    services: Vec<(&str, ProtoServiceSchema)>,
) -> ProtoFileSchema {
    ProtoFileSchema {
        package: package.to_string(),
        messages: messages
            .into_iter()
            .map(|(name, message)| (name.to_string(), message))
            .collect(),
        enums: enums
            .into_iter()
            .map(|(name, enum_schema)| (name.to_string(), enum_schema))
            .collect(),
        services: services
            .into_iter()
            .map(|(name, service)| (name.to_string(), service))
            .collect(),
    }
}

fn test_proto_schema_with_files(files: Vec<(&str, ProtoFileSchema)>) -> ProtoSchema {
    ProtoSchema {
        version: 1,
        files: files
            .into_iter()
            .map(|(path, file)| (path.to_string(), file))
            .collect(),
    }
}

fn assert_proto_schema_comparator_rejects(
    current: ProtoSchema,
    baseline: ProtoSchema,
    expected_violation: &str,
) {
    let violations = compare_proto_schema_to_baseline(&current, &baseline);
    assert!(
        violations
            .iter()
            .any(|violation| violation.contains(expected_violation)),
        "expected proto schema comparator violation containing `{expected_violation}`, got: {violations:?}"
    );
}

fn assert_proto_schema_comparator_accepts(current: ProtoSchema, baseline: ProtoSchema) {
    let violations = compare_proto_schema_to_baseline(&current, &baseline);
    assert!(
        violations.is_empty(),
        "expected proto schema comparator to accept compatible schema, got: {violations:?}"
    );
}

fn assert_proto_schema_comparator_rejects_all(
    current: ProtoSchema,
    baseline: ProtoSchema,
    expected_violations: &[&str],
) {
    let violations = compare_proto_schema_to_baseline(&current, &baseline);
    for expected_violation in expected_violations {
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains(expected_violation)),
            "expected proto schema comparator violation containing `{expected_violation}`, got: {violations:?}"
        );
    }
}

fn assert_proto_schema_baseline_merge_rejects(
    current: ProtoSchema,
    existing: ProtoSchema,
    expected_error: &str,
) {
    let err = merge_proto_schema_baseline(&current, &existing)
        .expect_err("expected proto schema baseline merge to reject unsafe change");
    assert!(
        err.contains(expected_error),
        "expected proto schema baseline merge error containing `{expected_error}`, got: {err}"
    );
}

#[test]
fn nidl_d3f_native_scan_range_proto_is_file_only() {
    let repo = Path::new(manifest_dir());
    let service_proto =
        fs::read_to_string(repo.join("idl/novarocks/service.proto")).expect("read service.proto");

    for forbidden in ["HdfsScanRange", "InternalScanRange", "TScanRangeParams"] {
        assert!(
            !service_proto.contains(forbidden),
            "idl/novarocks/service.proto must not expose thrift-shaped native scan range symbol `{forbidden}`"
        );
    }
    assert!(
        service_proto.contains("message ScanRangeParams")
            && service_proto.contains("FileScanRange file = 1"),
        "idl/novarocks/service.proto must expose native ScanRangeParams -> FileScanRange"
    );
}

#[test]
fn nidl_d3f_native_runtime_layers_do_not_import_thrift_scan_ranges() {
    let repo = Path::new(manifest_dir());
    let guarded_files = [
        "src/runtime/scheduler.rs",
        "src/sql/codegen/proto_encode/instance.rs",
    ];
    let forbidden = ["TScanRangeParams", "THdfsScanRange", "TInternalScanRange"];
    let mut violations = Vec::new();

    for rel_path in guarded_files {
        let path = repo.join(rel_path);
        for (line, text) in source_line_hits(&path, |line| {
            forbidden.iter().any(|symbol| line.contains(symbol))
        }) {
            violations.push(format!("{rel_path}:{line}: {text}"));
        }
    }

    assert!(
        violations.is_empty(),
        "native scheduling/proto encoding must use runtime::scan_range, not thrift scan range types:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nidl_d3b_proto_schema_parser_handles_current_syntax() {
    let input = r#"
        syntax = "proto3";
        package novarocks.plan;

        message Outer {
          reserved 4, 6 to 8;
          reserved "old_name", "old_flag";
          message Inner {
            string value = 1;
          }
          optional string name = 1;
          repeated int64 ids = 2;
          map<int32, novarocks.plan.ScanRangeList> ranges = 3;
          oneof kind {
            bool enabled = 5;
          }
          enum InnerState {
            INNER_STATE_UNSPECIFIED = 0;
            reserved 2, 4 to 5;
            reserved "old_state";
            INNER_STATE_READY = 1;
          }
        }

        service NovaRocksGrpc {
          rpc TransmitRuntimeFilter(novarocks.filter.TransmitRuntimeFilterRequest)
              returns (novarocks.filter.TransmitRuntimeFilterResponse);
          rpc Exchange(stream ExchangeRequest) returns (stream ExchangeResponse);
        }
    "#;

    let schema =
        parse_proto_schema("idl/novarocks/sample.proto", input).expect("sample proto should parse");
    assert_eq!(schema.package, "novarocks.plan");
    assert_eq!(schema.messages["Outer"].fields[&1].label, "optional");
    assert_eq!(schema.messages["Outer"].fields[&2].label, "repeated");
    assert_eq!(
        schema.messages["Outer"].fields[&3].type_name,
        "map<int32, novarocks.plan.ScanRangeList>"
    );
    assert_eq!(schema.messages["Outer.Inner"].fields[&1].name, "value");
    assert_eq!(
        schema.messages["Outer"].fields[&5].oneof.as_deref(),
        Some("kind")
    );
    assert!(schema.messages["Outer"].reserved_numbers.contains(&4));
    assert!(schema.messages["Outer"].reserved_numbers.contains(&7));
    assert!(schema.messages["Outer"].reserved_names.contains("old_name"));
    assert_eq!(
        schema.enums["Outer.InnerState"].values[0].name,
        "INNER_STATE_UNSPECIFIED"
    );
    assert!(
        schema.enums["Outer.InnerState"]
            .reserved_numbers
            .contains(&4)
    );
    assert!(
        schema.enums["Outer.InnerState"]
            .reserved_names
            .contains("old_state")
    );
    assert_eq!(
        schema.services["NovaRocksGrpc"].rpcs["TransmitRuntimeFilter"].request,
        "novarocks.filter.TransmitRuntimeFilterRequest"
    );
    assert_eq!(
        schema.services["NovaRocksGrpc"].rpcs["TransmitRuntimeFilter"].response,
        "novarocks.filter.TransmitRuntimeFilterResponse"
    );
    assert!(schema.services["NovaRocksGrpc"].rpcs["Exchange"].client_streaming);
    assert!(schema.services["NovaRocksGrpc"].rpcs["Exchange"].server_streaming);
}

#[test]
fn nidl_d3b_proto_schema_parser_rejects_proto2_syntax() {
    let err = parse_proto_schema(
        "idl/novarocks/proto2.proto",
        r#"
        syntax = "proto2";
        package novarocks.bad;
        message Bad {
          optional string value = 1;
        }
        "#,
    )
    .expect_err("proto2 syntax should fail");

    assert!(err.contains("syntax = \"proto2\";"), "{err}");
    assert!(err.contains("expected `proto3`"), "{err}");
}

#[test]
fn nidl_d3b_proto_schema_parser_parses_all_native_proto_files() {
    let schema =
        parse_current_novarocks_proto_schema().expect("current native proto schema should parse");
    assert!(schema.files.contains_key("idl/novarocks/service.proto"));
    assert!(
        schema.files["idl/novarocks/service.proto"]
            .services
            .contains_key("NovaRocksGrpc")
    );
    assert!(
        schema.files["idl/novarocks/service.proto"]
            .messages
            .contains_key("SubmitFragmentRequest")
    );
    let fetch_status =
        &schema.files["idl/novarocks/service.proto"].enums["FetchResultResponse.Status"];
    assert_eq!(
        fetch_status
            .values
            .iter()
            .map(|value| (value.number, value.name.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (0, "RESULT_STATUS_UNSPECIFIED"),
            (1, "READY"),
            (2, "NOT_READY"),
            (3, "EOF"),
            (4, "ERROR"),
        ]
    );
    assert!(fetch_status.reserved_numbers.is_empty());
    assert!(fetch_status.reserved_names.is_empty());
}

#[test]
fn nidl_d3b_current_schema_matches_baseline() {
    let current =
        parse_current_novarocks_proto_schema().expect("current native proto schema should parse");
    let baseline_path = nidl_d3b_baseline_path();

    match env::var(NIDL_D3B_WRITE_BASELINE_ENV) {
        Ok(value) if value == "1" => {
            let next_baseline = next_proto_schema_baseline_for_write(&current, &baseline_path)
                .unwrap_or_else(|err| panic!("{err}"));

            write_proto_schema_baseline(&baseline_path, &next_baseline)
                .unwrap_or_else(|err| panic!("{err}"));
            let written =
                read_proto_schema_baseline(&baseline_path).unwrap_or_else(|err| panic!("{err}"));
            let violations = compare_proto_schema_to_baseline(&current, &written);
            assert!(
                violations.is_empty(),
                "written proto schema baseline still violates current schema:\n{}",
                format_proto_schema_violations(&violations)
            );
        }
        Ok(value) => panic!(
            "{NIDL_D3B_WRITE_BASELINE_ENV} must be exactly `1` to write the proto schema baseline, got `{value}`"
        ),
        Err(env::VarError::NotUnicode(_)) => panic!(
            "{NIDL_D3B_WRITE_BASELINE_ENV} must be valid UTF-8 and exactly `1` to write the proto schema baseline"
        ),
        Err(env::VarError::NotPresent) => {
            let baseline = read_proto_schema_baseline(&baseline_path)
                .unwrap_or_else(|err| panic!("{err}\n\n{}", nidl_d3b_baseline_update_hint()));
            let violations = compare_proto_schema_to_baseline(&current, &baseline);
            assert!(
                violations.is_empty(),
                "current native proto schema does not match baseline:\n{}\n\n{}",
                format_proto_schema_violations(&violations),
                nidl_d3b_baseline_update_hint()
            );
        }
    }
}

fn source_region<'a>(source: &'a str, start: &str, end: &str) -> &'a str {
    let start_idx = source
        .find(start)
        .unwrap_or_else(|| panic!("missing start marker `{start}`"));
    let after_start = &source[start_idx..];
    let end_idx = after_start
        .find(end)
        .unwrap_or_else(|| panic!("missing end marker `{end}` after `{start}`"));
    &after_start[..end_idx]
}

#[test]
fn nidl_e5_native_exchange_response_has_common_status() {
    let schema =
        parse_current_novarocks_proto_schema().expect("current native proto schema should parse");
    let service_proto = &schema.files["idl/novarocks/service.proto"];
    let response = &service_proto.messages["ExchangeResponse"];
    let status = response
        .fields
        .get(&2)
        .expect("ExchangeResponse field 2 must be native status");

    assert_eq!(status.name, "status");
    assert_eq!(status.type_name, "novarocks.common.Status");
    assert_eq!(status.label, "singular");
}

#[test]
fn nidl_e5_native_exchange_rpc_paths_do_not_reference_starrocks_proto() {
    let repo = Path::new(manifest_dir());
    let grpc_server =
        fs::read_to_string(repo.join("src/service/grpc_server.rs")).expect("read grpc_server.rs");
    let exchange_region = source_region(
        &grpc_server,
        "async fn exchange(",
        "async fn transmit_runtime_filter(",
    );
    assert!(
        !exchange_region.contains("proto::starrocks"),
        "native grpc_server exchange path must not reference proto::starrocks:\n{exchange_region}"
    );

    let grpc_client =
        fs::read_to_string(repo.join("src/service/grpc_client.rs")).expect("read grpc_client.rs");
    let send_chunks_region = source_region(
        &grpc_client,
        "pub fn send_chunks(",
        "pub fn transmit_runtime_filter(",
    );
    assert!(
        !send_chunks_region.contains("proto::starrocks"),
        "native grpc_client send_chunks path must not reference proto::starrocks:\n{send_chunks_region}"
    );

    let internal_rpc =
        fs::read_to_string(repo.join("src/service/internal_rpc.rs")).expect("read internal_rpc.rs");
    let native_handler = source_region(
        &internal_rpc,
        "pub(crate) fn handle_transmit_chunk(",
        "#[cfg(feature = \"compat\")]\npub(crate) fn handle_transmit_chunk_compat(",
    );
    assert!(
        native_handler.contains("proto::novarocks::ExchangeRequest"),
        "native transmit_chunk handler must accept ExchangeRequest:\n{native_handler}"
    );
    assert!(
        native_handler.contains("proto::novarocks::ExchangeResponse"),
        "native transmit_chunk handler must return ExchangeResponse:\n{native_handler}"
    );
    assert!(
        !native_handler.contains("proto::starrocks"),
        "native transmit_chunk handler must not reference proto::starrocks:\n{native_handler}"
    );
}

#[test]
fn nidl_d3e_native_runtime_routing_has_no_thrift_shaped_endpoint_model() {
    let repo = Path::new(manifest_dir());
    let mut violations = Vec::new();

    for proto in ["idl/novarocks/service.proto", "idl/novarocks/plan.proto"] {
        let text = fs::read_to_string(repo.join(proto)).unwrap();
        for forbidden in [
            "brpc_addr",
            "fragment_instance_address",
            "grpc_endpoint",
            "report_addr",
        ] {
            if text.contains(forbidden) {
                violations.push(format!(
                    "{proto}: native proto must not contain `{forbidden}`"
                ));
            }
        }
    }

    let checked_sources = [
        "src/runtime/scheduler.rs",
        "src/sql/codegen/proto_encode/instance.rs",
    ];
    for source in checked_sources {
        let path = repo.join(source);
        let text = fs::read_to_string(&path).unwrap();
        for forbidden in [
            "TPlanFragmentDestination",
            "TRuntimeFilterProberParams",
            "brpc_server",
            "fragment_instance_address",
            "grpc_endpoint",
            "brpc_addr",
        ] {
            if text.contains(forbidden) {
                violations.push(format!(
                    "{source}: native runtime routing must not contain `{forbidden}`"
                ));
            }
        }
    }

    let coordinator = fs::read_to_string(repo.join("src/runtime/coordinator.rs")).unwrap();
    assert!(
        !coordinator.contains("fn exec_destination_from_runtime"),
        "native-only coordinator must not retain the Thrift execution destination adapter"
    );
    assert!(
        coordinator.contains("fn native_stream_destination"),
        "coordinator must encode native stream destinations without thrift roundtrip"
    );

    assert!(
        violations.is_empty(),
        "D3E native runtime endpoint guard failed:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nidl_d3i_native_fragment_exec_params_are_not_thrift_shaped() {
    let repo = Path::new(manifest_dir());
    let mut violations = Vec::new();

    let native_wire = fs::read_to_string(repo.join("src/runtime/native_fragment_wire.rs")).unwrap();
    for forbidden in ["TPlanFragmentExecParams", "TPlanFragmentDestination"] {
        if native_wire.contains(forbidden) {
            violations.push(format!(
                "src/runtime/native_fragment_wire.rs: native fragment wire must not expose `{forbidden}`"
            ));
        }
    }

    let fragment_exec_params =
        fs::read_to_string(repo.join("src/runtime/fragment_exec_params.rs")).unwrap();
    let struct_start = fragment_exec_params
        .find("pub(crate) struct FragmentExecParams")
        .expect("FragmentExecParams struct must exist");
    let impl_start = fragment_exec_params[struct_start..]
        .find("impl FragmentExecParams")
        .expect("FragmentExecParams impl must exist");
    let struct_body = &fragment_exec_params[struct_start..struct_start + impl_start];
    for forbidden in ["TUniqueId", "types::TUniqueId"] {
        if struct_body.contains(forbidden) {
            violations.push(format!(
                "src/runtime/fragment_exec_params.rs: FragmentExecParams fields must use native UniqueId, not `{forbidden}`"
            ));
        }
    }
    if !struct_body.contains("query_id: UniqueId")
        || !struct_body.contains("fragment_instance_id: UniqueId")
    {
        violations.push(
            "src/runtime/fragment_exec_params.rs: FragmentExecParams must keep query ids in crate::common::types::UniqueId".to_string(),
        );
    }

    let new_signature_start = fragment_exec_params
        .find("pub(crate) fn new(")
        .expect("FragmentExecParams::new must exist");
    let new_signature_end = fragment_exec_params[new_signature_start..]
        .find(") -> Result<Self, String>")
        .expect("FragmentExecParams::new signature must return Result");
    let new_signature =
        &fragment_exec_params[new_signature_start..new_signature_start + new_signature_end];
    if new_signature.contains("TUniqueId") || new_signature.contains("types::TUniqueId") {
        violations.push(
            "src/runtime/fragment_exec_params.rs: FragmentExecParams::new must accept native UniqueId inputs".to_string(),
        );
    }

    assert!(
        violations.is_empty(),
        "D3I native fragment exec params guard failed:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nidl_d3j_native_delta_scan_sidecar_is_not_patched_from_thrift_plan() {
    let repo = Path::new(manifest_dir());
    let coordinator = fs::read_to_string(repo.join("src/runtime/coordinator.rs")).unwrap();
    let mut violations = Vec::new();

    for forbidden in [
        "patch_native_iceberg_delta_scan_payloads",
        "TIcebergDeltaScanNode",
        "TIcebergDeltaScanPlan",
        "encode_native_delta_scan_plan",
    ] {
        if coordinator.contains(forbidden) {
            violations.push(format!(
                "src/runtime/coordinator.rs: native Iceberg delta sidecar must not use `{forbidden}`"
            ));
        }
    }

    let proto_plan = fs::read_to_string(repo.join("src/sql/codegen/proto_encode/plan.rs")).unwrap();
    if proto_plan.contains("delta_plan: None") {
        violations.push(
            "src/sql/codegen/proto_encode/plan.rs: IcebergDeltaTable native encoder must not leave delta_plan as None".to_string(),
        );
    }

    assert!(
        violations.is_empty(),
        "D3J native Iceberg delta sidecar guard failed:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nidl_d3k_native_dynamic_sink_partition_does_not_roundtrip_thrift_partition() {
    let repo = Path::new(manifest_dir());
    let coordinator = fs::read_to_string(repo.join("src/runtime/coordinator.rs")).unwrap();
    let mut violations = Vec::new();

    for forbidden in [
        "native_data_partition_from_thrift",
        "native_data_partition_from_thrift_with_exprs",
    ] {
        if coordinator.contains(forbidden) {
            violations.push(format!(
                "src/runtime/coordinator.rs: native dynamic sink patch must not use `{forbidden}`"
            ));
        }
    }

    if coordinator.contains("Vec<(FragmentId, i32, partitions::TDataPartition, Vec<i32>)>") {
        violations.push(
            "src/runtime/coordinator.rs: CTE native consumer index must not store thrift TDataPartition"
                .to_string(),
        );
    }

    let scheduler = fs::read_to_string(repo.join("src/runtime/scheduler.rs")).unwrap();
    for forbidden in [
        "use crate::thrift::partitions::TPartitionType;",
        "Vec<(FragmentId, TPartitionType, FragmentStreamKind)>",
        "e.compat_output_partition.type_",
    ] {
        if scheduler.contains(forbidden) {
            violations.push(format!(
                "src/runtime/scheduler.rs: scheduling topology must use native edge.output_partition, not compat thrift partition via `{forbidden}`"
            ));
        }
    }

    let fragment =
        fs::read_to_string(repo.join("src/sql/planner/distributed/fragment.rs")).unwrap();
    if !fragment.contains("pub output_partition: DataPartition") {
        violations.push(
            "src/sql/planner/distributed/fragment.rs: FragmentEdge must carry native output_partition"
                .to_string(),
        );
    }
    if fragment.contains("pub compat_output_partition: partitions::TDataPartition") {
        violations.push(
            "src/sql/planner/distributed/fragment.rs: FragmentEdge must no longer carry compat TDataPartition in planner IR"
                .to_string(),
        );
    }

    assert!(
        violations.is_empty(),
        "D3K native dynamic sink partition guard failed:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nfe_0_novarocks_generated_execution_has_no_plan_wire_selector() {
    let repo = Path::new(manifest_dir());
    let mut violations = Vec::new();

    let app_config = fs::read_to_string(repo.join("src/common/app_config.rs")).unwrap();
    push_forbidden_terms(
        &mut violations,
        "src/common/app_config.rs",
        &app_config,
        &[
            "PlanWireFormat",
            "pub plan_wire_format:",
            "fn default_plan_wire_format(",
        ],
        "NovaRocks config must not expose the retired plan-wire selector",
    );

    let coordinator = fs::read_to_string(repo.join("src/runtime/coordinator.rs")).unwrap();
    push_forbidden_terms(
        &mut violations,
        "src/runtime/coordinator.rs",
        &coordinator,
        &[
            "PlanWireFormat",
            "current_plan_wire_format",
            "FragmentSubmission::thrift_only",
        ],
        "NovaRocks-generated execution must always use the native plan wire",
    );

    assert!(
        violations.is_empty(),
        "NFE-0 plan-wire selector guard failed:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nfe_1_task_2_native_fragment_build_owns_submission_payload() {
    let repo = Path::new(manifest_dir());
    let mut violations = Vec::new();

    let codegen_mod = fs::read_to_string(repo.join("src/sql/codegen/mod.rs")).unwrap();
    if !codegen_mod.contains("pub native_fragments:")
        || !codegen_mod.contains("BTreeMap<FragmentId, crate::proto::plan::PlanFragment>")
    {
        violations.push(
            "src/sql/codegen/mod.rs: MultiFragmentBuildResult must own required native fragments"
                .to_string(),
        );
    }

    for rel in ["src/engine/mod.rs", "src/runtime/coordinator.rs"] {
        let text = fs::read_to_string(repo.join(rel)).unwrap();
        push_forbidden_terms(
            &mut violations,
            rel,
            &text,
            &[
                "NativePlanSidecars",
                "prepare_native_plan_sidecars",
                "new_with_native_plan_sidecars",
                "new_with_optional_native_plan_sidecars",
                "refresh_native_sidecar_plan_with_lowered_edges",
            ],
            "Task 2 requires the build result to be the only native fragment owner",
        );
    }

    assert!(
        violations.is_empty(),
        "NFE-1 Task 2 native fragment ownership guard failed:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nfe_1_task_3_runtime_submission_is_native_only() {
    let repo = Path::new(manifest_dir());
    let mut violations = Vec::new();

    for rel in ["src/runtime/dispatcher.rs", "src/runtime/coordinator.rs"] {
        let text = fs::read_to_string(repo.join(rel)).unwrap();
        let production = rust_production_text_without_cfg_test(&text);
        push_forbidden_terms(
            &mut violations,
            rel,
            &production,
            &[
                "crate::thrift",
                "TExecPlanFragmentParams",
                "TDataSink",
                "TDataPartition",
                "CompatFragmentPlanPayload",
                "build_exec_plan_fragment_params",
                "submit_fragment_submission",
                "thrift_only",
                "with_native",
                "thrift_params",
                "into_thrift_params",
                "Option<crate::proto::plan::PlanFragment>",
                "Option<crate::proto::novarocks::InstanceParams>",
                "CompatEdgeSidecar",
                "inject_runtime_filter_merge_nodes",
                "compat_data_sink_requires_write_report",
            ],
            "Task 3 requires native-only coordinator and dispatcher production code",
        );
    }
    for rel in [
        "src/runtime/exec_params.rs",
        "src/runtime/exec_params_compat.rs",
    ] {
        if repo.join(rel).exists() {
            violations.push(format!(
                "{rel}: retired zero-call NovaRocks FE exec-param adapter must be deleted"
            ));
        }
    }
    let runtime_mod = fs::read_to_string(repo.join("src/runtime/mod.rs")).unwrap();
    for forbidden in ["mod exec_params;", "mod exec_params_compat;"] {
        if runtime_mod.contains(forbidden) {
            violations.push(format!(
                "src/runtime/mod.rs: retired adapter declaration `{forbidden}` must be deleted"
            ));
        }
    }

    assert!(
        violations.is_empty(),
        "NFE-1 Task 3 native-only submission guard failed:\n{}",
        violations.join("\n")
    );
}

const NFE_2_LEGACY_THRIFT_EMITTER_PATHS: &[&str] = &[
    "src/sql/codegen/descriptors.rs",
    "src/sql/codegen/expr_compiler.rs",
    "src/sql/codegen/fallback_audit.rs",
    "src/sql/codegen/iceberg_change_stream_router_wire.rs",
    "src/sql/codegen/iceberg_write_sink_wire.rs",
    "src/sql/codegen/nodes.rs",
    "src/sql/codegen/resolve.rs",
    "src/sql/codegen/runtime_filter_lowering.rs",
    "src/sql/codegen/type_infer.rs",
    "src/sql/codegen/ir/lowering.rs",
    "src/sql/codegen/ir/equiv.rs",
];

fn nfe_2_legacy_thrift_emitter_path_violations(root: &Path) -> Vec<String> {
    let mut violations = Vec::new();
    for relative in NFE_2_LEGACY_THRIFT_EMITTER_PATHS {
        match fs::symlink_metadata(root.join(relative)) {
            Ok(_) => violations.push((*relative).to_string()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => violations.push(format!(
                "{relative}: failed to inspect path metadata: {error}"
            )),
        }
    }
    violations
}

fn nfe_3_retired_control_plane_term_violations(
    source: &str,
    text: &str,
    forbidden: &[&str],
) -> Vec<String> {
    let production = rust_sanitized_production_text(text);
    forbidden
        .iter()
        .filter(|term| production.contains(**term))
        .map(|term| format!("{source}: retired generated-Thrift control-plane term `{term}`"))
        .collect()
}

fn nfe_3_raw_starrocks_idl_violations(source: &str, text: &str) -> Vec<String> {
    let production = rust_sanitized_production_text(text);
    let tokens = rust_use_tokens(&production);
    let imports = rust_production_scoped_use_statements(text);
    let canonical_paths = rust_production_canonical_paths(text, source);
    [
        "crate::thrift",
        "crate::proto::starrocks",
        "crate::proto::staros",
    ]
    .into_iter()
    .filter(|term| {
        let term_tokens = rust_use_tokens(term);
        tokens
            .windows(term_tokens.len())
            .any(|window| window == term_tokens)
            || imports.iter().any(|import| {
                let path = rust_use_path(&import.import);
                path == *term
                    || path
                        .strip_prefix(*term)
                        .is_some_and(|suffix| suffix.starts_with("::"))
            })
            || {
                let term_segments = term.split("::").collect::<Vec<_>>();
                canonical_paths.iter().any(|path| {
                    path.len() >= term_segments.len()
                        && path
                            .iter()
                            .zip(&term_segments)
                            .all(|(actual, expected)| actual == expected)
                })
            }
    })
    .map(|term| format!("{source}: FE-owned helper references raw StarRocks IDL `{term}`"))
    .collect()
}

type Nfe4PublicReexportGraph = BTreeMap<Vec<String>, Vec<Vec<String>>>;

fn nfe_4_public_reexport_graph(sources: &[(String, String)]) -> Nfe4PublicReexportGraph {
    let mut graph = Nfe4PublicReexportGraph::new();
    for (source, text) in sources {
        let aliases = rust_production_scoped_aliases(text);
        for raw in rust_raw_production_use_statements(text)
            .into_iter()
            .filter(|raw| matches!(raw.visibility.as_str(), "pub" | "pub(crate)"))
        {
            let Some(mut exported) = rust_source_module_segments(source) else {
                continue;
            };
            exported.extend(raw.inline_modules.clone());

            let glob = raw
                .path
                .segments
                .last()
                .is_some_and(|segment| segment == "*");
            if !glob {
                let Some(local_name) = raw
                    .path
                    .alias
                    .clone()
                    .filter(|alias| alias != "_")
                    .or_else(|| raw.path.segments.last().cloned())
                else {
                    continue;
                };
                exported.push(local_name);
            }

            let resolved = rust_resolve_scoped_paths(
                &raw.path.segments,
                &raw.inline_modules,
                &aliases,
                &mut BTreeSet::new(),
                0,
            )
            .unwrap_or_else(|| {
                vec![RustScopedUsePath {
                    segments: raw.path.segments.clone(),
                    inline_modules: raw.inline_modules.clone(),
                }]
            });
            for resolved in resolved {
                let Some(mut target) = rust_canonical_path_segments_in_scope(
                    &resolved.segments,
                    source,
                    &resolved.inline_modules,
                ) else {
                    continue;
                };
                if glob {
                    target.pop();
                }
                if exported == target {
                    continue;
                }
                let targets = graph.entry(exported.clone()).or_default();
                if !targets.contains(&target) {
                    targets.push(target);
                    targets.sort();
                }
            }
        }
    }
    graph
}

fn nfe_4_raw_starrocks_idl_violations(
    source: &str,
    text: &str,
    reexports: &Nfe4PublicReexportGraph,
) -> Vec<String> {
    nfe_4_raw_starrocks_idl_violations_with_cache(source, text, reexports, &mut BTreeMap::new())
}

fn nfe_4_forbidden_seed_mask(path: &[String]) -> u8 {
    [
        (&["crate", "thrift"][..], 1u8),
        (&["crate", "proto", "starrocks"][..], 2u8),
        (&["crate", "proto", "staros"][..], 4u8),
    ]
    .into_iter()
    .filter(|(prefix, _)| {
        path.len() >= prefix.len()
            && path
                .iter()
                .zip(prefix.iter())
                .all(|(actual, expected)| actual == expected)
    })
    .fold(0, |mask, (_, bit)| mask | bit)
}

fn nfe_4_reexport_forbidden_mask(
    path: &[String],
    graph: &Nfe4PublicReexportGraph,
    cache: &mut BTreeMap<Vec<String>, u8>,
    visiting: &mut BTreeSet<Vec<String>>,
) -> (u8, bool) {
    let direct = nfe_4_forbidden_seed_mask(path);
    if direct != 0 {
        return (direct, true);
    }
    if let Some(mask) = cache.get(path) {
        return (*mask, true);
    }
    let matched = (1..=path.len()).rev().find_map(|prefix_len| {
        graph
            .get(&path[..prefix_len])
            .map(|targets| (prefix_len, targets))
    });
    let Some((prefix_len, targets)) = matched else {
        cache.insert(path.to_vec(), 0);
        return (0, true);
    };
    let prefix = path[..prefix_len].to_vec();
    if !visiting.insert(prefix.clone()) {
        return (0, false);
    }
    let mut mask = 0;
    let mut complete = true;
    for target in targets {
        let mut rewritten = target.clone();
        rewritten.extend_from_slice(&path[prefix_len..]);
        let (target_mask, target_complete) =
            nfe_4_reexport_forbidden_mask(&rewritten, graph, cache, visiting);
        mask |= target_mask;
        complete &= target_complete;
    }
    visiting.remove(&prefix);
    if complete {
        cache.insert(path.to_vec(), mask);
    }
    (mask, complete)
}

fn nfe_4_raw_starrocks_idl_violations_with_cache(
    source: &str,
    text: &str,
    reexports: &Nfe4PublicReexportGraph,
    cache: &mut BTreeMap<Vec<String>, u8>,
) -> Vec<String> {
    let production = rust_sanitized_production_text(text);
    let tokens = rust_use_tokens(&production);
    let canonical_source = if rust_source_module_segments(source).is_some() {
        source.to_string()
    } else if let Some((_, crate_relative)) = source.rsplit_once("/src/") {
        format!("src/{crate_relative}")
    } else {
        source.to_string()
    };
    let canonical_paths = rust_production_canonical_paths(text, &canonical_source);
    let mut mask = canonical_paths.iter().fold(0, |mask, path| {
        mask | nfe_4_reexport_forbidden_mask(path, reexports, cache, &mut BTreeSet::new()).0
    });
    for (term, bit) in [
        ("crate::thrift", 1u8),
        ("crate::proto::starrocks", 2u8),
        ("crate::proto::staros", 4u8),
    ] {
        let term_tokens = rust_use_tokens(term);
        let direct = tokens
            .windows(term_tokens.len())
            .any(|window| window == term_tokens);
        if direct {
            mask |= bit;
        }
    }

    [
        ("crate::thrift", 1u8),
        ("crate::proto::starrocks", 2u8),
        ("crate::proto::staros", 4u8),
    ]
    .into_iter()
    .filter(|(_, bit)| mask & bit != 0)
    .map(|(term, _)| term)
    .map(|term| format!("{source}: FE-owned helper references raw StarRocks IDL `{term}`"))
    .collect()
}

fn nfe_4_collect_production_owner_files(
    root: &Path,
    entries: &[PathBuf],
) -> Result<Vec<PathBuf>, String> {
    if entries.is_empty() {
        return Err(format!("{}: owner entry set is empty", root.display()));
    }
    for entry in entries {
        match fs::symlink_metadata(entry) {
            Ok(metadata) if metadata.file_type().is_file() => {}
            Ok(_) => return Err(format!("{}: owner entry is not a file", entry.display())),
            Err(error) => {
                return Err(format!(
                    "{}: failed to inspect owner entry: {error}",
                    entry.display()
                ));
            }
        }
    }

    let files = production_rs_files_from_entries(root, entries);
    if files.is_empty() {
        return Err(format!("{}: production owner set is empty", root.display()));
    }
    for entry in entries {
        let canonical = fs::canonicalize(entry)
            .map_err(|error| format!("{}: canonicalize failed: {error}", entry.display()))?;
        if !files.iter().any(|file| {
            fs::canonicalize(file)
                .ok()
                .is_some_and(|file| file == canonical)
        }) {
            return Err(format!(
                "{}: production owner entry was not collected",
                entry.display()
            ));
        }
    }
    Ok(files)
}

const NFE_4_RETIRED_GENERATED_THRIFT_PATHS: &[&str] = &[
    "src/sql/codegen/descriptors.rs",
    "src/sql/codegen/expr_compiler.rs",
    "src/sql/codegen/fallback_audit.rs",
    "src/sql/codegen/iceberg_change_stream_router_wire.rs",
    "src/sql/codegen/iceberg_write_sink_wire.rs",
    "src/sql/codegen/nodes.rs",
    "src/sql/codegen/resolve.rs",
    "src/sql/codegen/runtime_filter_lowering.rs",
    "src/sql/codegen/type_infer.rs",
    "src/sql/codegen/ir/lowering.rs",
    "src/sql/codegen/ir/lowering_native.rs",
    "src/sql/codegen/ir/equiv.rs",
    "src/runtime/exec_params.rs",
    "src/runtime/exec_params_compat.rs",
    "src/engine/query_options_wire.rs",
    "src/exec/spill/query_options_wire.rs",
    "src/sql/codegen/connector_scan_wire.rs",
    "src/sql/codegen/iceberg_delta_scan_wire.rs",
    "src/sql/planner/distributed/build/runtime_filter_wire.rs",
];

fn nfe_4_retired_generated_thrift_path_violations(root: &Path) -> Vec<String> {
    NFE_4_RETIRED_GENERATED_THRIFT_PATHS
        .iter()
        .filter_map(|relative| match fs::symlink_metadata(root.join(relative)) {
            Ok(_) => Some((*relative).to_string()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => Some(format!(
                "{relative}: failed to inspect path metadata: {error}"
            )),
        })
        .collect()
}

const NFE_4_RETIRED_FE_SYMBOLS: &[&str] = &[
    "PlanWireFormat",
    "current_plan_wire_format",
    "default_plan_wire_format",
    "plan_wire_format",
    "thrift_only",
    "FragmentBuildResult",
    "LoweredFragmentEdge",
    "fragment_results",
    "lowered_edges",
    "NativePlanSidecars",
    "CompatFragmentPlanPayload",
    "CompatEdgeSidecar",
    "TExecPlanFragmentParams",
    "TPlanFragment",
    "TPlanNode",
    "TDescriptorTable",
    "TDataSink",
    "TDataPartition",
    "TExpr",
    "thrift_params",
    "into_thrift_params",
    "build_exec_plan_fragment_params",
    "novarocks_generated_plan",
    "PlanOrigin",
    "NovaRocksGenerated",
    "plan_origin_from_request",
    "to_compat_exec_params",
    "compat_exec_params_from_parts",
    "compat_destination_from_runtime",
    "thrift_scan_range_params_from_native",
    "thrift_hdfs_scan_range_from_native",
    "thrift_extended_columns_from_native",
    "thrift_scan_range_map_from_native",
    "compat_change_op_slot_id",
    "to_network_address",
    "connector_scan_wire",
    "iceberg_delta_scan_wire",
    "runtime_filter_wire",
    "WiredRuntimeFilterBuild",
    "WiredRuntimeFilterProbe",
    "branch_kind_from_thrift",
];

fn nfe_4_retired_fe_symbol_violations(source: &str, text: &str) -> Vec<String> {
    let forbidden = NFE_4_RETIRED_FE_SYMBOLS
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let mut seen = BTreeSet::new();
    rust_use_tokens(&rust_sanitized_production_text(text))
        .into_iter()
        .filter(|token| forbidden.contains(token.as_str()) && seen.insert(token.clone()))
        .map(|symbol| format!("{source}: retired FE generated-Thrift symbol `{symbol}`"))
        .collect()
}

fn nfe_4_ledger_and_audit_contract_violations(
    ledger: &str,
    compat_scope: &[&str],
    audit: &str,
) -> Vec<String> {
    fn baseline_max_hits(line: &str) -> Option<&str> {
        let (_, arguments) = line.split_once("BaselineEntry(")?;
        arguments.split(',').nth(2)
    }

    let mut violations = Vec::new();
    let ledger_entries = ledger
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect::<Vec<_>>();
    if !ledger_entries.is_empty() {
        violations.push(format!(
            "NIDL ledger must stay empty: {}",
            ledger_entries.join(", ")
        ));
    }

    let fe_owners = [
        "src/sql",
        "src/engine",
        "src/runtime/coordinator.rs",
        "src/runtime/dispatcher.rs",
        "tests/sql-test-runner/src",
    ];
    for scope in compat_scope {
        if fe_owners.iter().any(|owner| {
            owner == scope
                || owner.starts_with(&format!("{scope}/"))
                || scope.starts_with(&format!("{owner}/"))
        }) {
            violations.push(format!(
                "{scope}: FE owner must not enter NIDL compat scope"
            ));
        }
    }

    let compact_lines = audit.lines().map(compact_line).collect::<Vec<_>>();
    for line in &compact_lines {
        let Some((quoted_path, _)) = line.split_once("\":BaselineEntry(") else {
            continue;
        };
        let Some(path) = quoted_path.strip_prefix('"') else {
            continue;
        };
        let fe_native = path.starts_with("src/engine/")
            || matches!(
                path,
                "src/runtime/coordinator.rs" | "src/runtime/dispatcher.rs"
            );
        if fe_native && baseline_max_hits(line).is_none_or(|max_hits| max_hits != "0") {
            violations.push(format!(
                "{path}: FE/native audit baseline must stay at max_hits=0"
            ));
        }
    }

    let compact = compact_lines.join("");
    for retired in NFE_4_RETIRED_GENERATED_THRIFT_PATHS {
        if compact.contains(&format!("\"{retired}\":BaselineEntry(")) {
            violations.push(format!(
                "{retired}: retired path must not retain an audit baseline"
            ));
        }
    }
    for (path, prefix) in [
        (
            "src/runtime/query_options.rs",
            "\"src/runtime/query_options.rs\":BaselineEntry(\"legal-boundary\",\"B7-wire\",1,",
        ),
        (
            "src/runtime/runtime_filter_params.rs",
            "\"src/runtime/runtime_filter_params.rs\":BaselineEntry(\"legal-boundary\",\"B3-wire\",1,",
        ),
        (
            "src/runtime/endpoint.rs",
            "\"src/runtime/endpoint.rs\":BaselineEntry(\"legal-boundary\",\"control-plane-wire\",2,",
        ),
    ] {
        if !compact.contains(prefix) {
            violations.push(format!(
                "{path}: required external ingress audit baseline is missing"
            ));
        }
    }
    violations
}

#[test]
fn nfe_4_raw_starrocks_idl_detector_resolves_compat_and_reexports() {
    let sources = vec![
        ("src/lib.rs".to_string(), "mod bridge;".to_string()),
        (
            "src/bridge.rs".to_string(),
            r#"
                pub use crate::proto;
                pub use crate::proto as wire;
                pub use crate::proto::*;
                pub use crate::{proto as grouped_wire};
            "#
            .to_string(),
        ),
        (
            "src/second.rs".to_string(),
            "pub(crate) use crate::bridge::wire as second_wire;".to_string(),
        ),
        (
            "src/cycle_a.rs".to_string(),
            "pub use crate::cycle_b::looped;".to_string(),
        ),
        (
            "src/cycle_b.rs".to_string(),
            "pub use crate::cycle_a::looped;".to_string(),
        ),
        (
            "src/growth.rs".to_string(),
            "pub use crate::growth::nested::*;".to_string(),
        ),
    ];
    let reexports = nfe_4_public_reexport_graph(&sources);
    let direct = r#"
        fn direct(_: crate::thrift::types::TUniqueId) {
            let _ = crate::proto::starrocks::StatusPb::default();
            let _ = crate::proto::staros::WorkerInfo::default();
        }
        use crate::{thrift::types::TUniqueId, proto::{starrocks, staros}};
    "#;
    assert_eq!(
        nfe_4_raw_starrocks_idl_violations("src/direct.rs", direct, &BTreeMap::new()),
        vec![
            "src/direct.rs: FE-owned helper references raw StarRocks IDL `crate::thrift`",
            "src/direct.rs: FE-owned helper references raw StarRocks IDL `crate::proto::starrocks`",
            "src/direct.rs: FE-owned helper references raw StarRocks IDL `crate::proto::staros`",
        ]
    );

    let file_local_aliases = r#"
        use crate as root;
        use root::proto as local_wire;
        use root::thrift as local_thrift;
        fn aliases(_: local_thrift::types::TUniqueId) {
            let _ = local_wire::starrocks::StatusPb::default();
            let _ = local_wire::staros::WorkerInfo::default();
        }
    "#;
    assert_eq!(
        nfe_4_raw_starrocks_idl_violations(
            "src/file_aliases.rs",
            file_local_aliases,
            &BTreeMap::new(),
        ),
        vec![
            "src/file_aliases.rs: FE-owned helper references raw StarRocks IDL `crate::thrift`",
            "src/file_aliases.rs: FE-owned helper references raw StarRocks IDL `crate::proto::starrocks`",
            "src/file_aliases.rs: FE-owned helper references raw StarRocks IDL `crate::proto::staros`",
        ]
    );

    let noise_and_native = r##"
        // crate::bridge::proto::starrocks::StatusPb
        const NOTE: &str = "crate::bridge::wire::staros::WorkerInfo";
        const RAW_NOTE: &str = r#"crate::bridge::starrocks::StatusPb"#;
        const MARKER: char = 'x';
        #[cfg(test)]
        fn test_only() {
            let _ = crate::bridge::proto::starrocks::StatusPb::default();
        }
        fn native() {
            let _ = crate::proto::common::Status::default();
            let _ = crate::proto::expr::Expr::default();
            let _ = crate::proto::filter::RuntimeFilter::default();
            let _ = crate::proto::novarocks::InstanceParams::default();
            let _ = crate::proto::plan::PlanFragment::default();
        }
    "##;
    assert!(
        nfe_4_raw_starrocks_idl_violations("src/noise.rs", noise_and_native, &BTreeMap::new(),)
            .is_empty()
    );

    for (name, consumer, expected) in [
        (
            "ordinary facade",
            "fn f() { let _ = crate::bridge::proto::starrocks::StatusPb::default(); }",
            "crate::proto::starrocks",
        ),
        (
            "compat alias facade",
            "#[cfg(feature = \"compat\")] fn f() { let _ = crate::bridge::wire::staros::WorkerInfo::default(); }",
            "crate::proto::staros",
        ),
        (
            "glob facade",
            "fn f() { let _ = crate::bridge::starrocks::StatusPb::default(); }",
            "crate::proto::starrocks",
        ),
        (
            "grouped facade",
            "fn f() { let _ = crate::bridge::grouped_wire::staros::WorkerInfo::default(); }",
            "crate::proto::staros",
        ),
        (
            "multi-hop conservative facade",
            "#[cfg(any(test, feature = \"compat\"))] fn f() { let _ = crate::second::second_wire::starrocks::StatusPb::default(); }",
            "crate::proto::starrocks",
        ),
    ] {
        let source = format!("src/{name}.rs");
        assert!(
            nfe_4_raw_starrocks_idl_violations(&source, consumer, &BTreeMap::new()).is_empty(),
            "{name} must contain no direct/file-local raw-IDL seed"
        );
        assert_eq!(
            nfe_4_raw_starrocks_idl_violations(&source, consumer, &reexports),
            vec![format!(
                "{source}: FE-owned helper references raw StarRocks IDL `{expected}`"
            )],
            "{name} must depend exclusively on cross-file public re-export resolution"
        );
    }

    let cycle = vec![
        "crate".to_string(),
        "cycle_a".to_string(),
        "looped".to_string(),
        "StatusPb".to_string(),
    ];
    let mut cycle_cache = BTreeMap::new();
    assert_eq!(
        nfe_4_reexport_forbidden_mask(&cycle, &reexports, &mut cycle_cache, &mut BTreeSet::new(),),
        (0, false),
        "cyclic re-exports must terminate as incomplete rather than resolved-safe"
    );
    assert!(
        !cycle_cache.contains_key(&cycle),
        "an incomplete cyclic resolution must not poison the safe-result cache"
    );
    let growth = vec![
        "crate".to_string(),
        "growth".to_string(),
        "SafeType".to_string(),
    ];
    let mut growth_cache = BTreeMap::new();
    assert_eq!(
        nfe_4_reexport_forbidden_mask(&growth, &reexports, &mut growth_cache, &mut BTreeSet::new(),),
        (0, false),
        "a re-export cycle that grows the rewritten suffix must terminate without fabricating a forbidden seed"
    );
    assert!(
        !growth_cache.contains_key(&growth),
        "a suffix-growing cycle must not be cached as resolved-safe"
    );
}

#[test]
fn nfe_4_public_reexport_graph_resolves_private_source_aliases() {
    let sources = vec![
        (
            "src/bridge.rs".to_string(),
            r#"
                use crate::proto as private_wire;
                pub use private_wire::starrocks as exposed;
            "#
            .to_string(),
        ),
        (
            "src/grouped_bridge.rs".to_string(),
            r#"
                use crate::{proto as private_wire};
                pub use private_wire::{staros as exposed_staros};
            "#
            .to_string(),
        ),
    ];
    let reexports = nfe_4_public_reexport_graph(&sources);
    for (consumer, expected) in [
        (
            "fn use_facade() { let _ = crate::bridge::exposed::StatusPb::default(); }",
            "crate::proto::starrocks",
        ),
        (
            "fn use_facade() { let _ = crate::grouped_bridge::exposed_staros::WorkerInfo::default(); }",
            "crate::proto::staros",
        ),
    ] {
        assert!(
            nfe_4_raw_starrocks_idl_violations("src/fe.rs", consumer, &BTreeMap::new()).is_empty(),
            "the consumer has no direct raw-IDL seed without the defining file's re-export graph"
        );
        assert_eq!(
            nfe_4_raw_starrocks_idl_violations("src/fe.rs", consumer, &reexports),
            vec![format!(
                "src/fe.rs: FE-owned helper references raw StarRocks IDL `{expected}`"
            )],
            "public grouped re-exports must resolve private grouped aliases from their defining source file"
        );
    }
}

#[test]
fn nfe_4_public_reexport_graph_resolves_long_valid_chains() {
    let mut sources = (0..66)
        .map(|index| {
            (
                format!("src/hop_{index}.rs"),
                format!("pub use crate::hop_{}::wire;", index + 1),
            )
        })
        .collect::<Vec<_>>();
    sources.push((
        "src/hop_66.rs".to_string(),
        "pub use crate::proto::starrocks as wire;".to_string(),
    ));
    let consumer = "fn use_facade() { let _ = crate::hop_0::wire::StatusPb::default(); }";
    assert!(
        nfe_4_raw_starrocks_idl_violations("src/fe.rs", consumer, &BTreeMap::new()).is_empty(),
        "the consumer has no direct raw-IDL seed without the public re-export graph"
    );
    assert_eq!(
        nfe_4_raw_starrocks_idl_violations(
            "src/fe.rs",
            consumer,
            &nfe_4_public_reexport_graph(&sources),
        ),
        vec!["src/fe.rs: FE-owned helper references raw StarRocks IDL `crate::proto::starrocks`"],
        "valid public re-export chains must not become safe at an arbitrary fixed depth"
    );
}

#[test]
fn nfe_4_fe_owner_collection_follows_production_module_graph() {
    let root =
        std::env::temp_dir().join(format!("nfe_4_fe_owner_collection_{}", std::process::id()));
    fs::remove_dir_all(&root).ok();
    fs::create_dir_all(root.join("chosen")).unwrap();
    fs::write(
        root.join("mod.rs"),
        r#"
            mod production;
            #[cfg(feature = "compat")]
            mod compat_owner;
            #[cfg(any(test, feature = "compat"))]
            mod conservative;
            #[cfg(test)]
            mod external_tests;
            #[path = "chosen/direct.rs"]
            mod direct_path;
            #[cfg_attr(feature = "compat", path = "chosen/compat_path.rs")]
            mod conditional_path;
        "#,
    )
    .unwrap();
    for relative in [
        "production.rs",
        "compat_owner.rs",
        "conservative.rs",
        "external_tests.rs",
        "chosen/direct.rs",
        "chosen/compat_path.rs",
        "conditional_path.rs",
    ] {
        fs::write(root.join(relative), "pub fn marker() {}\n").unwrap();
    }

    let collected = nfe_4_collect_production_owner_files(&root, &[root.join("mod.rs")])
        .expect("fixture owner collection must be non-vacuous");
    let relative = collected
        .iter()
        .map(|path| path.strip_prefix(&root).unwrap().display().to_string())
        .collect::<BTreeSet<_>>();
    assert!(relative.contains("mod.rs"));
    assert!(relative.contains("production.rs"));
    assert!(relative.contains("compat_owner.rs"));
    assert!(relative.contains("conservative.rs"));
    assert!(relative.contains("chosen/direct.rs"));
    assert!(relative.contains("chosen/compat_path.rs"));
    assert!(relative.contains("conditional_path.rs"));
    assert!(!relative.contains("external_tests.rs"));
    assert!(
        nfe_4_collect_production_owner_files(&root, &[root.join("missing.rs")]).is_err(),
        "a missing owner entry must fail closed"
    );

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn nfe_4_fe_owned_raw_sources_are_starrocks_idl_free() {
    let repo = Path::new(manifest_dir());
    let src = repo.join("src");
    let runner = repo.join("tests/sql-test-runner/src");
    let mut violations = Vec::new();

    let crate_sources = nfe_4_collect_production_owner_files(&src, &[src.join("lib.rs")])
        .expect("main crate production graph must be non-vacuous")
        .into_iter()
        .map(|path| {
            let source = rel(&path);
            let text = fs::read_to_string(&path).expect(&source);
            (source, text)
        })
        .collect::<Vec<_>>();
    let reexports = nfe_4_public_reexport_graph(&crate_sources);

    let mut owners = Vec::new();
    for (root, entries) in [
        (repo.join("src/sql"), vec![repo.join("src/sql/mod.rs")]),
        (
            repo.join("src/engine"),
            vec![repo.join("src/engine/mod.rs")],
        ),
        (
            repo.join("src/runtime"),
            vec![
                repo.join("src/runtime/coordinator.rs"),
                repo.join("src/runtime/dispatcher.rs"),
            ],
        ),
        (runner.clone(), vec![runner.join("main.rs")]),
    ] {
        match nfe_4_collect_production_owner_files(&root, &entries) {
            Ok(files) => owners.extend(files),
            Err(error) => violations.push(error),
        }
    }
    owners.sort();
    owners.dedup();

    let owner_rel = owners.iter().map(|path| rel(path)).collect::<BTreeSet<_>>();
    for required in [
        "src/sql/mod.rs",
        "src/sql/codegen/ir/fragment_build.rs",
        "src/engine/mod.rs",
        "src/engine/statement.rs",
        "src/runtime/coordinator.rs",
        "src/runtime/dispatcher.rs",
        "tests/sql-test-runner/src/main.rs",
        "tests/sql-test-runner/src/cluster.rs",
    ] {
        if !owner_rel.contains(required) {
            violations.push(format!(
                "{required}: required FE production owner was not collected"
            ));
        }
    }

    let mut reexport_cache = BTreeMap::new();
    for path in owners {
        let source = rel(&path);
        let text = fs::read_to_string(&path).expect(&source);
        violations.extend(nfe_4_raw_starrocks_idl_violations_with_cache(
            &source,
            &text,
            &reexports,
            &mut reexport_cache,
        ));
        violations.extend(nfe_4_retired_fe_symbol_violations(&source, &text));
    }

    assert!(
        violations.is_empty(),
        "NFE-4 FE production owners must have zero raw StarRocks IDL references:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nfe_4_retired_generated_thrift_paths_are_physically_absent() {
    assert!(
        nfe_4_retired_generated_thrift_path_violations(Path::new(manifest_dir())).is_empty(),
        "all arc-level retired generated-Thrift paths must remain physically absent"
    );

    let root = std::env::temp_dir().join(format!(
        "nfe_4_retired_generated_thrift_paths_{}",
        std::process::id()
    ));
    fs::remove_dir_all(&root).ok();
    let relative = "src/sql/codegen/descriptors.rs";
    let fixture = root.join(relative);
    fs::create_dir_all(fixture.parent().unwrap()).unwrap();

    fs::write(&fixture, "fixture\n").unwrap();
    assert_eq!(
        nfe_4_retired_generated_thrift_path_violations(&root),
        vec![relative.to_string()]
    );
    fs::remove_file(&fixture).unwrap();
    fs::create_dir(&fixture).unwrap();
    assert_eq!(
        nfe_4_retired_generated_thrift_path_violations(&root),
        vec![relative.to_string()]
    );
    fs::remove_dir(&fixture).unwrap();

    #[cfg(unix)]
    {
        let target = root.join("target.rs");
        fs::write(&target, "target\n").unwrap();
        std::os::unix::fs::symlink(&target, &fixture).unwrap();
        assert_eq!(
            nfe_4_retired_generated_thrift_path_violations(&root),
            vec![relative.to_string()]
        );
        fs::remove_file(&fixture).unwrap();
        std::os::unix::fs::symlink(root.join("missing.rs"), &fixture).unwrap();
        assert_eq!(
            nfe_4_retired_generated_thrift_path_violations(&root),
            vec![relative.to_string()],
            "dangling symlinks must count as present retired paths"
        );
        fs::remove_file(&fixture).unwrap();
    }

    assert!(nfe_4_retired_generated_thrift_path_violations(&root).is_empty());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn nfe_4_retired_symbol_detector_keeps_compat_and_ignores_noise() {
    let fixture = r#"
        // TPlanNode and PlanWireFormat in comments are ignored.
        const NOTE: &str = "to_compat_exec_params runtime_filter_wire";
        fn ordinary(_: TPlanNode) { current_plan_wire_format(); }
        #[cfg(feature = "compat")]
        fn compat_only() { thrift_scan_range_map_from_native(); }
        #[cfg(test)]
        fn test_only(_: TExpr) { to_network_address(); }
    "#;
    assert_eq!(
        nfe_4_retired_fe_symbol_violations("fixture.rs", fixture),
        vec![
            "fixture.rs: retired FE generated-Thrift symbol `TPlanNode`",
            "fixture.rs: retired FE generated-Thrift symbol `current_plan_wire_format`",
            "fixture.rs: retired FE generated-Thrift symbol `thrift_scan_range_map_from_native`",
        ]
    );
}

#[test]
fn nfe_4_ledger_and_audit_contract_detector_is_non_vacuous() {
    let violations = nfe_4_ledger_and_audit_contract_violations(
        "src/sql/codegen/bad.rs\n",
        &["src/sql"],
        r#"
            BASELINE = {
                "src/engine/mod.rs": BaselineEntry("domain-leak", "B7", 1, "bad"),
                "src/runtime/coordinator.rs": BaselineEntry("domain-leak", "control-plane", 1, "bad"),
                "src/sql/codegen/descriptors.rs": BaselineEntry("domain-leak", "legacy", 0, "bad"),
            }
        "#,
    );
    for expected in [
        "NIDL ledger must stay empty",
        "FE owner must not enter NIDL compat scope",
        "src/engine/mod.rs: FE/native audit baseline must stay at max_hits=0",
        "src/runtime/coordinator.rs: FE/native audit baseline must stay at max_hits=0",
        "src/sql/codegen/descriptors.rs: retired path must not retain an audit baseline",
        "src/runtime/query_options.rs: required external ingress audit baseline is missing",
        "src/runtime/runtime_filter_params.rs: required external ingress audit baseline is missing",
        "src/runtime/endpoint.rs: required external ingress audit baseline is missing",
    ] {
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains(expected)),
            "missing `{expected}` in {violations:?}"
        );
    }
}

#[test]
fn nfe_4_ledger_audit_native_structure_and_external_ingress_are_fixed() {
    let repo = Path::new(manifest_dir());
    let ledger = fs::read_to_string(repo.join(NIDL_E0_LEDGER_PATH)).unwrap();
    let audit = fs::read_to_string(repo.join("tools/dev/audit_thrift_boundaries.py")).unwrap();
    let mut violations =
        nfe_4_ledger_and_audit_contract_violations(&ledger, NIDL_E0_COMPAT_SCOPE, &audit);

    let app_config = fs::read_to_string(repo.join("src/common/app_config.rs")).unwrap();
    let app_tokens = rust_use_tokens(&rust_sanitized_production_text(&app_config));
    let tombstone = ["fn", "reject_removed_plan_wire_format"];
    if !app_tokens.windows(tombstone.len()).any(|tokens| {
        tokens
            .iter()
            .map(String::as_str)
            .eq(tombstone.iter().copied())
    }) || !app_config.contains("runtime.plan_wire_format has been removed")
    {
        violations.push(
            "src/common/app_config.rs: removed plan_wire_format rejection tombstone is missing"
                .to_string(),
        );
    }

    for (source, required) in [
        ("src/runtime/query_options.rs", "from_thrift"),
        ("src/runtime/runtime_filter_params.rs", "from_thrift"),
        ("src/runtime/endpoint.rs", "from_network_address"),
    ] {
        let text = fs::read_to_string(repo.join(source)).unwrap();
        if !rust_use_tokens(&rust_sanitized_production_text(&text))
            .iter()
            .any(|token| token == required)
        {
            violations.push(format!(
                "{source}: required external ingress adapter `{required}` is missing"
            ));
        }
    }

    for source in [
        "src/sql/codegen/ir/fragment_build.rs",
        "src/sql/codegen/connector_scan_planning.rs",
        "src/sql/codegen/iceberg_delta_scan_planning.rs",
        "src/sql/codegen/proto_encode/iceberg_delta_scan.rs",
        "src/sql/planner/distributed/build/runtime_filter_binding.rs",
    ] {
        match fs::symlink_metadata(repo.join(source)) {
            Ok(metadata) if metadata.file_type().is_file() => {}
            Ok(_) => violations.push(format!("{source}: native owner is not a regular file")),
            Err(error) => violations.push(format!("{source}: native owner is missing: {error}")),
        }
    }

    let fragment_build =
        fs::read_to_string(repo.join("src/sql/codegen/ir/fragment_build.rs")).unwrap();
    let fragment_production = rust_sanitized_production_text(&fragment_build);
    if fragment_production.contains("cfg") && fragment_production.contains("compat") {
        violations.push(
            "src/sql/codegen/ir/fragment_build.rs: unique native builder must be compat-neutral"
                .to_string(),
        );
    }

    let codegen = fs::read_to_string(repo.join("src/sql/codegen/mod.rs")).unwrap();
    let codegen_compact = compact_line(&rust_sanitized_production_text(&codegen));
    if !codegen_compact.contains(
        "pubnative_fragments:std::collections::BTreeMap<FragmentId,crate::proto::plan::PlanFragment>",
    ) {
        violations.push(
            "src/sql/codegen/mod.rs: MultiFragmentBuildResult must own required native PlanFragment map"
                .to_string(),
        );
    }

    let dispatcher = fs::read_to_string(repo.join("src/runtime/dispatcher.rs")).unwrap();
    let dispatcher_compact = compact_line(&rust_sanitized_production_text(&dispatcher));
    for required in [
        "plan:crate::proto::plan::PlanFragment",
        "instance_params:crate::proto::novarocks::InstanceParams",
    ] {
        if !dispatcher_compact.contains(required) {
            violations.push(format!(
                "src/runtime/dispatcher.rs: FragmentSubmission missing required native field `{required}`"
            ));
        }
    }
    for forbidden in [
        "plan:Option<crate::proto::plan::PlanFragment>",
        "instance_params:Option<crate::proto::novarocks::InstanceParams>",
    ] {
        if dispatcher_compact.contains(forbidden) {
            violations.push(format!(
                "src/runtime/dispatcher.rs: FragmentSubmission retains optional dual carrier `{forbidden}`"
            ));
        }
    }

    assert!(
        violations.is_empty(),
        "NFE-4 ledger/audit/native structure contract failed:\n{}",
        violations.join("\n")
    );
}

const NFE_4_EXTERNAL_FIXTURE_BASENAME: &str = "select_1_exec_batch_plan_fragments_v1.bin";
const NFE_4_EXTERNAL_FIXTURE_INCLUDE_PATH: &str =
    "fixtures/external_starrocks_fe/select_1_exec_batch_plan_fragments_v1.bin";
const NFE_4_EXTERNAL_FIXTURE_SIZE: usize = 3_456;
const NFE_4_EXTERNAL_FIXTURE_SHA256: &str =
    "1c7bda906c9828d7999f93c36197f5f896e8611c27953510e07a18966e624095";
const NFE_4_EXTERNAL_PRODUCER_REVISION: &str = "fe0bed0bdcb520a758a34f572f445f398ca7d5a3";
const NFE_4_EXTERNAL_PRODUCER_ARTIFACT_SHA256: &str =
    "f2950e2f8e6d2db9091a2eb0c1ad25318f4385c1d3c586926aa414f9a97d9346";
const NFE_4_EXTERNAL_QUERY_ID: &str = "019f51afb5c37c92-9c5da9f1a67bbebf";
const NFE_4_EXTERNAL_FINST_ID: &str = "019f51afb5c37c92-9c5da9f1a67bbec0";

fn nfe_4_no_symlink_component_violations(repo: &Path, path: &Path, label: &str) -> Vec<String> {
    let mut violations = Vec::new();
    let relative = match path.strip_prefix(repo) {
        Ok(relative) => relative,
        Err(_) => {
            return vec![format!(
                "{label} must remain below repository root: {}",
                path.display()
            )];
        }
    };
    let mut current = repo.to_path_buf();
    for component in std::iter::once(None).chain(relative.components().map(Some)) {
        if let Some(component) = component {
            current.push(component.as_os_str());
        }
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => violations.push(format!(
                "{label} path component must not be a symlink: {}",
                current.display()
            )),
            Ok(_) => {}
            Err(error) => {
                violations.push(format!(
                    "{label} path component is missing or unreadable at {}: {error}",
                    current.display()
                ));
                break;
            }
        }
    }
    violations
}

fn nfe_4_syn_cfg_meta_requires_test(meta: &syn::Meta) -> bool {
    use syn::parse::Parser;

    match meta {
        syn::Meta::Path(path) => path.is_ident("test"),
        syn::Meta::List(list)
            if list.path.is_ident("all")
                || list.path.is_ident("any")
                || list.path.is_ident("not") =>
        {
            let parser = syn::punctuated::Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated;
            let Ok(children) = parser.parse2(list.tokens.clone()) else {
                return false;
            };
            if list.path.is_ident("all") {
                children.iter().any(nfe_4_syn_cfg_meta_requires_test)
            } else if list.path.is_ident("any") {
                !children.is_empty() && children.iter().all(nfe_4_syn_cfg_meta_requires_test)
            } else {
                false
            }
        }
        _ => false,
    }
}

fn nfe_4_syn_attrs_require_test(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|attr| {
        let syn::Meta::List(cfg) = &attr.meta else {
            return false;
        };
        cfg.path.is_ident("cfg")
            && syn::parse2::<syn::Meta>(cfg.tokens.clone())
                .is_ok_and(|meta| nfe_4_syn_cfg_meta_requires_test(&meta))
    })
}

fn nfe_4_syn_item_attrs(item: &syn::Item) -> &[syn::Attribute] {
    match item {
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
        syn::Item::Verbatim(_) => &[],
        _ => &[],
    }
}

fn nfe_4_syn_impl_item_attrs(item: &syn::ImplItem) -> &[syn::Attribute] {
    match item {
        syn::ImplItem::Const(item) => &item.attrs,
        syn::ImplItem::Fn(item) => &item.attrs,
        syn::ImplItem::Type(item) => &item.attrs,
        syn::ImplItem::Macro(item) => &item.attrs,
        syn::ImplItem::Verbatim(_) => &[],
        _ => &[],
    }
}

fn nfe_4_syn_trait_item_attrs(item: &syn::TraitItem) -> &[syn::Attribute] {
    match item {
        syn::TraitItem::Const(item) => &item.attrs,
        syn::TraitItem::Fn(item) => &item.attrs,
        syn::TraitItem::Type(item) => &item.attrs,
        syn::TraitItem::Macro(item) => &item.attrs,
        syn::TraitItem::Verbatim(_) => &[],
        _ => &[],
    }
}

fn nfe_4_syn_foreign_item_attrs(item: &syn::ForeignItem) -> &[syn::Attribute] {
    match item {
        syn::ForeignItem::Fn(item) => &item.attrs,
        syn::ForeignItem::Static(item) => &item.attrs,
        syn::ForeignItem::Type(item) => &item.attrs,
        syn::ForeignItem::Macro(item) => &item.attrs,
        syn::ForeignItem::Verbatim(_) => &[],
        _ => &[],
    }
}

fn nfe_4_syn_expr_attrs(expr: &syn::Expr) -> &[syn::Attribute] {
    match expr {
        syn::Expr::Array(expr) => &expr.attrs,
        syn::Expr::Assign(expr) => &expr.attrs,
        syn::Expr::Async(expr) => &expr.attrs,
        syn::Expr::Await(expr) => &expr.attrs,
        syn::Expr::Binary(expr) => &expr.attrs,
        syn::Expr::Block(expr) => &expr.attrs,
        syn::Expr::Break(expr) => &expr.attrs,
        syn::Expr::Call(expr) => &expr.attrs,
        syn::Expr::Cast(expr) => &expr.attrs,
        syn::Expr::Closure(expr) => &expr.attrs,
        syn::Expr::Const(expr) => &expr.attrs,
        syn::Expr::Continue(expr) => &expr.attrs,
        syn::Expr::Field(expr) => &expr.attrs,
        syn::Expr::ForLoop(expr) => &expr.attrs,
        syn::Expr::Group(expr) => &expr.attrs,
        syn::Expr::If(expr) => &expr.attrs,
        syn::Expr::Index(expr) => &expr.attrs,
        syn::Expr::Infer(expr) => &expr.attrs,
        syn::Expr::Let(expr) => &expr.attrs,
        syn::Expr::Lit(expr) => &expr.attrs,
        syn::Expr::Loop(expr) => &expr.attrs,
        syn::Expr::Macro(expr) => &expr.attrs,
        syn::Expr::Match(expr) => &expr.attrs,
        syn::Expr::MethodCall(expr) => &expr.attrs,
        syn::Expr::Paren(expr) => &expr.attrs,
        syn::Expr::Path(expr) => &expr.attrs,
        syn::Expr::Range(expr) => &expr.attrs,
        syn::Expr::RawAddr(expr) => &expr.attrs,
        syn::Expr::Reference(expr) => &expr.attrs,
        syn::Expr::Repeat(expr) => &expr.attrs,
        syn::Expr::Return(expr) => &expr.attrs,
        syn::Expr::Struct(expr) => &expr.attrs,
        syn::Expr::Try(expr) => &expr.attrs,
        syn::Expr::TryBlock(expr) => &expr.attrs,
        syn::Expr::Tuple(expr) => &expr.attrs,
        syn::Expr::Unary(expr) => &expr.attrs,
        syn::Expr::Unsafe(expr) => &expr.attrs,
        syn::Expr::While(expr) => &expr.attrs,
        syn::Expr::Yield(expr) => &expr.attrs,
        syn::Expr::Verbatim(_) => &[],
        _ => &[],
    }
}

fn nfe_4_syn_stmt_attrs(stmt: &syn::Stmt) -> &[syn::Attribute] {
    match stmt {
        syn::Stmt::Local(local) => &local.attrs,
        syn::Stmt::Item(item) => nfe_4_syn_item_attrs(item),
        syn::Stmt::Expr(expr, _) => nfe_4_syn_expr_attrs(expr),
        syn::Stmt::Macro(item) => &item.attrs,
    }
}

fn nfe_4_is_absolute_std_include_bytes(path: &syn::Path) -> bool {
    path.leading_colon.is_some()
        && path.segments.len() == 2
        && path.segments[0].ident == "std"
        && matches!(path.segments[0].arguments, syn::PathArguments::None)
        && path.segments[1].ident == "include_bytes"
        && matches!(path.segments[1].arguments, syn::PathArguments::None)
}

fn nfe_4_is_submit_exec_batch_path(path: &syn::Path) -> bool {
    path.leading_colon.is_some()
        && path.segments.len() == 2
        && path.segments[0].ident == "novarocks"
        && matches!(path.segments[0].arguments, syn::PathArguments::None)
        && path.segments[1].ident == "submit_exec_batch_plan_fragments"
        && matches!(path.segments[1].arguments, syn::PathArguments::None)
}

fn nfe_4_use_tree_binds_novarocks(tree: &syn::UseTree) -> bool {
    match tree {
        syn::UseTree::Path(path) => nfe_4_use_tree_binds_novarocks(&path.tree),
        syn::UseTree::Name(name) => name.ident == "novarocks",
        syn::UseTree::Rename(rename) => rename.rename == "novarocks",
        syn::UseTree::Group(group) => group.items.iter().any(nfe_4_use_tree_binds_novarocks),
        syn::UseTree::Glob(_) => false,
    }
}

fn nfe_4_is_crate_fixture_expr(expr: &syn::Expr) -> bool {
    let syn::Expr::Path(expr) = expr else {
        return false;
    };
    expr.qself.is_none()
        && expr.path.leading_colon.is_none()
        && expr.path.segments.len() == 2
        && expr.path.segments[0].ident == "crate"
        && matches!(expr.path.segments[0].arguments, syn::PathArguments::None)
        && expr.path.segments[1].ident == "FIXTURE"
        && matches!(expr.path.segments[1].arguments, syn::PathArguments::None)
}

fn nfe_4_is_exact_inner_compat_cfg(attr: &syn::Attribute) -> bool {
    if !matches!(attr.style, syn::AttrStyle::Inner(_)) {
        return false;
    }
    let syn::Meta::List(cfg) = &attr.meta else {
        return false;
    };
    if !cfg.path.is_ident("cfg") {
        return false;
    }
    let Ok(syn::Meta::NameValue(feature)) = syn::parse2::<syn::Meta>(cfg.tokens.clone()) else {
        return false;
    };
    feature.path.is_ident("feature")
        && matches!(
            feature.value,
            syn::Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Str(ref value),
                ..
            }) if value.value() == "compat"
        )
}

#[derive(Default)]
struct Nfe4IncludeBytesVisitor {
    paths: Vec<Option<String>>,
    submit_args_bind_crate_fixture: Vec<bool>,
}

impl<'ast> syn::visit::Visit<'ast> for Nfe4IncludeBytesVisitor {
    fn visit_item(&mut self, item: &'ast syn::Item) {
        if nfe_4_syn_attrs_require_test(nfe_4_syn_item_attrs(item)) {
            return;
        }
        syn::visit::visit_item(self, item);
    }

    fn visit_impl_item(&mut self, item: &'ast syn::ImplItem) {
        if nfe_4_syn_attrs_require_test(nfe_4_syn_impl_item_attrs(item)) {
            return;
        }
        syn::visit::visit_impl_item(self, item);
    }

    fn visit_trait_item(&mut self, item: &'ast syn::TraitItem) {
        if nfe_4_syn_attrs_require_test(nfe_4_syn_trait_item_attrs(item)) {
            return;
        }
        syn::visit::visit_trait_item(self, item);
    }

    fn visit_foreign_item(&mut self, item: &'ast syn::ForeignItem) {
        if nfe_4_syn_attrs_require_test(nfe_4_syn_foreign_item_attrs(item)) {
            return;
        }
        syn::visit::visit_foreign_item(self, item);
    }

    fn visit_stmt(&mut self, stmt: &'ast syn::Stmt) {
        if nfe_4_syn_attrs_require_test(nfe_4_syn_stmt_attrs(stmt)) {
            return;
        }
        syn::visit::visit_stmt(self, stmt);
    }

    fn visit_expr(&mut self, expr: &'ast syn::Expr) {
        if nfe_4_syn_attrs_require_test(nfe_4_syn_expr_attrs(expr)) {
            return;
        }
        if let syn::Expr::Call(call) = expr
            && let syn::Expr::Path(function) = call.func.as_ref()
            && function.qself.is_none()
            && nfe_4_is_submit_exec_batch_path(&function.path)
        {
            self.submit_args_bind_crate_fixture.push(
                call.args.len() == 1 && call.args.first().is_some_and(nfe_4_is_crate_fixture_expr),
            );
        }
        syn::visit::visit_expr(self, expr);
    }

    fn visit_arm(&mut self, arm: &'ast syn::Arm) {
        if nfe_4_syn_attrs_require_test(&arm.attrs) {
            return;
        }
        syn::visit::visit_arm(self, arm);
    }

    fn visit_field(&mut self, field: &'ast syn::Field) {
        if nfe_4_syn_attrs_require_test(&field.attrs) {
            return;
        }
        syn::visit::visit_field(self, field);
    }

    fn visit_variant(&mut self, variant: &'ast syn::Variant) {
        if nfe_4_syn_attrs_require_test(&variant.attrs) {
            return;
        }
        syn::visit::visit_variant(self, variant);
    }

    fn visit_field_value(&mut self, field: &'ast syn::FieldValue) {
        if nfe_4_syn_attrs_require_test(&field.attrs) {
            return;
        }
        syn::visit::visit_field_value(self, field);
    }

    fn visit_macro(&mut self, item: &'ast syn::Macro) {
        if item
            .path
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "include_bytes")
        {
            self.paths.push(
                nfe_4_is_absolute_std_include_bytes(&item.path)
                    .then(|| syn::parse2::<syn::LitStr>(item.tokens.clone()).ok())
                    .flatten()
                    .map(|lit| lit.value()),
            );
        }
        syn::visit::visit_macro(self, item);
    }
}

fn nfe_4_consumer_ast_violations(text: &str) -> Vec<String> {
    use syn::visit::Visit;

    let file = match syn::parse_file(text) {
        Ok(file) => file,
        Err(error) => return vec![format!("fixture consumer must parse as Rust: {error}")],
    };
    let mut violations = Vec::new();

    if file.attrs.len() != 1 || !nfe_4_is_exact_inner_compat_cfg(&file.attrs[0]) {
        violations.push(format!(
            "fixture consumer must have exactly one File attribute and it must be inner #![cfg(feature = \"compat\")], got {} attributes",
            file.attrs.len()
        ));
    }

    for item in &file.items {
        match item {
            syn::Item::ExternCrate(item)
                if item.ident == "std"
                    || item
                        .rename
                        .as_ref()
                        .is_some_and(|(_, rename)| rename == "std") =>
            {
                violations.push(
                    "fixture consumer must not redefine or alias crate name `std`".to_string(),
                );
            }
            syn::Item::Mod(item) if item.ident == "novarocks" => {
                violations.push(
                    "fixture consumer must not define a local module named `novarocks`".to_string(),
                );
            }
            syn::Item::ExternCrate(item)
                if item.ident == "novarocks"
                    || item
                        .rename
                        .as_ref()
                        .is_some_and(|(_, rename)| rename == "novarocks") =>
            {
                violations.push(
                    "fixture consumer must not redefine or alias crate name `novarocks`"
                        .to_string(),
                );
            }
            syn::Item::Use(item) if nfe_4_use_tree_binds_novarocks(&item.tree) => {
                violations.push(
                    "fixture consumer must not import a binding named `novarocks`".to_string(),
                );
            }
            syn::Item::Macro(item)
                if item
                    .ident
                    .as_ref()
                    .is_some_and(|ident| ident == "include_bytes") =>
            {
                violations.push(
                    "fixture consumer must not define a local `include_bytes` macro".to_string(),
                );
            }
            _ => {}
        }
    }

    let fixture_consts = file
        .items
        .iter()
        .filter_map(|item| match item {
            syn::Item::Const(item)
                if item.ident == "FIXTURE" && !nfe_4_syn_attrs_require_test(&item.attrs) =>
            {
                Some(item)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    if fixture_consts.len() != 1 {
        violations.push(format!(
            "fixture consumer must have exactly one top-level production const FIXTURE, got {}",
            fixture_consts.len()
        ));
    } else {
        let initializer = fixture_consts[0].expr.as_ref();
        let fixture_path = match initializer {
            syn::Expr::Macro(expr) if nfe_4_is_absolute_std_include_bytes(&expr.mac.path) => {
                syn::parse2::<syn::LitStr>(expr.mac.tokens.clone())
                    .ok()
                    .map(|lit| lit.value())
            }
            _ => None,
        };
        if fixture_path.as_deref() != Some(NFE_4_EXTERNAL_FIXTURE_INCLUDE_PATH) {
            violations.push(format!(
                "const FIXTURE initializer must directly include `{NFE_4_EXTERNAL_FIXTURE_INCLUDE_PATH}`, got {fixture_path:?}"
            ));
        }
    }

    let mut visitor = Nfe4IncludeBytesVisitor::default();
    visitor.visit_file(&file);
    if visitor.paths != vec![Some(NFE_4_EXTERNAL_FIXTURE_INCLUDE_PATH.to_string())] {
        violations.push(format!(
            "fixture consumer must contain exactly one production include_bytes! of `{NFE_4_EXTERNAL_FIXTURE_INCLUDE_PATH}`, got {:?}",
            visitor.paths
        ));
    }
    if visitor.submit_args_bind_crate_fixture != vec![true] {
        violations.push(format!(
            "fixture consumer must contain exactly one production ::novarocks::submit_exec_batch_plan_fragments call whose sole argument is crate::FIXTURE, got {:?}",
            visitor.submit_args_bind_crate_fixture
        ));
    }

    violations
}

fn nfe_4_fixture_reference_files(repo: &Path) -> BTreeSet<String> {
    fn ignored_dir(relative: &Path) -> bool {
        if relative.components().any(|component| {
            matches!(
                component.as_os_str().to_str(),
                Some(".git" | "target" | "logs" | ".superpowers")
            )
        }) {
            return true;
        }
        relative.starts_with("docker/iceberg-rest/runtime")
    }

    fn contains(bytes: &[u8], needle: &[u8]) -> bool {
        !needle.is_empty() && bytes.windows(needle.len()).any(|window| window == needle)
    }

    fn collect(repo: &Path, dir: &Path, out: &mut BTreeSet<String>) {
        let Ok(entries) = fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(metadata) = fs::symlink_metadata(&path) else {
                continue;
            };
            if metadata.file_type().is_symlink() {
                continue;
            }
            let relative = path.strip_prefix(repo).unwrap_or(&path);
            if ignored_dir(relative) {
                continue;
            }
            if metadata.is_dir() {
                collect(repo, &path, out);
                continue;
            }
            if !metadata.file_type().is_file() {
                continue;
            }
            let Ok(bytes) = fs::read(&path) else {
                continue;
            };
            if contains(&bytes, NFE_4_EXTERNAL_FIXTURE_BASENAME.as_bytes())
                || contains(&bytes, NFE_4_EXTERNAL_FIXTURE_INCLUDE_PATH.as_bytes())
            {
                out.insert(relative.display().to_string());
            }
        }
    }

    let mut references = BTreeSet::new();
    collect(repo, repo, &mut references);
    references
}

fn nfe_4_external_compat_fixture_contract_violations(repo: &Path) -> Vec<String> {
    use sha2::{Digest, Sha256};

    let fixture_dir = repo.join("tests/fixtures/external_starrocks_fe");
    let fixture = fixture_dir.join(NFE_4_EXTERNAL_FIXTURE_BASENAME);
    let readme = fixture_dir.join("README.md");
    let consumer = repo.join("tests/external_starrocks_fe_compat.rs");
    let mut violations = Vec::new();

    for (label, path) in [
        ("external fixture", fixture.as_path()),
        ("fixture provenance", readme.as_path()),
        ("fixture consumer", consumer.as_path()),
    ] {
        violations.extend(nfe_4_no_symlink_component_violations(repo, path, label));
    }

    for (label, path) in [
        ("external fixture", fixture.as_path()),
        ("fixture provenance", readme.as_path()),
        ("fixture consumer", consumer.as_path()),
    ] {
        match fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_file() => {}
            Ok(_) => violations.push(format!(
                "{label} must be a regular file, not a directory or symlink: {}",
                path.display()
            )),
            Err(error) => violations.push(format!(
                "{label} is missing or unreadable at {}: {error}",
                path.display()
            )),
        }
    }

    if violations.is_empty() {
        let bytes = fs::read(&fixture).expect("fixture was checked as readable");
        if bytes.len() != NFE_4_EXTERNAL_FIXTURE_SIZE {
            violations.push(format!(
                "external fixture size changed: expected {}, got {}",
                NFE_4_EXTERNAL_FIXTURE_SIZE,
                bytes.len()
            ));
        }
        let sha = format!("{:x}", Sha256::digest(&bytes));
        if sha != NFE_4_EXTERNAL_FIXTURE_SHA256 {
            violations.push(format!(
                "external fixture SHA-256 changed: expected {}, got {sha}",
                NFE_4_EXTERNAL_FIXTURE_SHA256
            ));
        }
        let readme_text = fs::read_to_string(&readme).expect("README was checked as readable");
        for required in [
            &format!("Producer revision: StarRocks upstream `{NFE_4_EXTERNAL_PRODUCER_REVISION}`"),
            &format!(
                "Producer artifact: deployed `starrocks-fe.jar`, SHA-256 `{NFE_4_EXTERNAL_PRODUCER_ARTIFACT_SHA256}`"
            ),
            &format!("Fixture: `{NFE_4_EXTERNAL_FIXTURE_BASENAME}`"),
            "SQL: `SELECT /*+SET_VAR(enable_constant_execute_in_fe=false)*/ 1`",
            "Session setup: `SET enable_single_node_schedule=true`",
            "RPC: `exec_batch_plan_fragments`",
            "Protocol: Thrift Binary",
            &format!("Query ID: `{NFE_4_EXTERNAL_QUERY_ID}`"),
            &format!("Fragment instance ID: `{NFE_4_EXTERNAL_FINST_ID}`"),
            "Normalization: none.",
            &format!("SHA-256: `{NFE_4_EXTERNAL_FIXTURE_SHA256}`"),
        ] {
            if !readme_text.contains(required) {
                violations.push(format!(
                    "fixture README is missing provenance field `{required}`"
                ));
            }
        }
        let consumer_text =
            fs::read_to_string(&consumer).expect("consumer was checked as readable");
        violations.extend(nfe_4_consumer_ast_violations(&consumer_text));
        let consumer_production = compact_line(&rust_sanitized_production_text(&consumer_text));
        for required in [
            "novarocks_rs_try_fetch_result_batch(",
            "novarocks_rs_free_buf(",
            "novarocks::thrift::data::TResultBatch",
        ] {
            if !consumer_production.contains(required) {
                violations.push(format!(
                    "fixture consumer production code is missing required operation `{required}`"
                ));
            }
        }
        for forbidden in [
            "TExecBatchPlanFragmentsParams::new",
            "TExecPlanFragmentParams::new",
            "TPlanNode::new",
            "TBinaryOutputProtocol",
            "thrift_binary_serialize",
            "lower_distributed_plan",
            "sql::codegen",
        ] {
            if consumer_production.contains(forbidden) {
                violations.push(format!(
                    "fixture consumer must not build or serialize a plan: `{forbidden}`"
                ));
            }
        }

        let reference_files = nfe_4_fixture_reference_files(repo);
        let expected_reference_files = BTreeSet::from([
            "tests/architecture_guard.rs".to_string(),
            "tests/external_starrocks_fe_compat.rs".to_string(),
            "tests/fixtures/external_starrocks_fe/README.md".to_string(),
        ]);
        if reference_files != expected_reference_files {
            violations.push(format!(
                "external fixture references changed: expected {expected_reference_files:?}, got {reference_files:?}"
            ));
        }

        match fs::read_dir(&fixture_dir) {
            Ok(entries) => {
                let mut actual_entries = BTreeSet::new();
                for entry in entries.flatten() {
                    actual_entries.insert(entry.file_name().to_string_lossy().into_owned());
                }
                let expected_entries = BTreeSet::from([
                    "README.md".to_string(),
                    NFE_4_EXTERNAL_FIXTURE_BASENAME.to_string(),
                ]);
                if actual_entries != expected_entries {
                    violations.push(format!(
                        "fixture directory must contain exactly README.md and the external binary: expected {expected_entries:?}, got {actual_entries:?}"
                    ));
                }
            }
            Err(error) => violations.push(format!(
                "fixture directory is missing or unreadable at {}: {error}",
                fixture_dir.display()
            )),
        }
    }

    violations
}

#[test]
fn nfe_4_external_compat_fixture_is_external_input() {
    let violations = nfe_4_external_compat_fixture_contract_violations(Path::new(manifest_dir()));
    assert!(
        violations.is_empty(),
        "NFE-4 external compatibility fixture must remain external read-only input:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nfe_4_external_compat_fixture_detector_rejects_self_certification_and_comment_stubs() {
    use sha2::{Digest, Sha256};

    let root = std::env::temp_dir().join(format!(
        "nfe_4_external_fixture_self_certification_{}",
        std::process::id()
    ));
    fs::remove_dir_all(&root).ok();
    let fixture_dir = root.join("tests/fixtures/external_starrocks_fe");
    fs::create_dir_all(&fixture_dir).unwrap();
    let replacement = b"replacement fixture bytes";
    fs::write(
        fixture_dir.join("select_1_exec_batch_plan_fragments_v1.bin"),
        replacement,
    )
    .unwrap();
    let replacement_sha = format!("{:x}", Sha256::digest(replacement));
    fs::write(
        fixture_dir.join("README.md"),
        format!(
            "Producer revision: fake\nSQL: SELECT /*+SET_VAR(enable_constant_execute_in_fe=false)*/ 1\nRPC: `exec_batch_plan_fragments`\nProtocol: Thrift Binary\nFragment instance ID: fake\nNormalization: none\nSHA-256: `{replacement_sha}`\n"
        ),
    )
    .unwrap();
    fs::write(
        root.join("tests/external_starrocks_fe_compat.rs"),
        r#"
            // #![cfg(feature = "compat")]
            // include_bytes!("fixtures/external_starrocks_fe/select_1_exec_batch_plan_fragments_v1.bin")
            // submit_exec_batch_plan_fragments
            // novarocks_rs_try_fetch_result_batch
            // novarocks_rs_free_buf
            // TResultBatch
        "#,
    )
    .unwrap();

    let violations = nfe_4_external_compat_fixture_contract_violations(&root);
    assert!(
        !violations.is_empty(),
        "replacement bytes plus a synchronized README and comment-only consumer must not self-certify"
    );
    fs::remove_dir_all(&root).unwrap();
}

fn nfe_4_external_fixture_attack_repo(case: &str, consumer: String) -> PathBuf {
    let source_repo = Path::new(manifest_dir());
    let root = std::env::temp_dir().join(format!(
        "nfe_4_external_fixture_{case}_{}",
        std::process::id()
    ));
    fs::remove_dir_all(&root).ok();
    let fixture_dir = root.join("tests/fixtures/external_starrocks_fe");
    fs::create_dir_all(&fixture_dir).unwrap();
    for relative in ["README.md", NFE_4_EXTERNAL_FIXTURE_BASENAME] {
        fs::copy(
            source_repo
                .join("tests/fixtures/external_starrocks_fe")
                .join(relative),
            fixture_dir.join(relative),
        )
        .unwrap();
    }
    fs::write(
        root.join("tests/architecture_guard.rs"),
        format!("const EXTERNAL_FIXTURE: &str = \"{NFE_4_EXTERNAL_FIXTURE_BASENAME}\";\n"),
    )
    .unwrap();
    fs::write(root.join("tests/external_starrocks_fe_compat.rs"), consumer).unwrap();
    root
}

#[test]
fn nfe_4_external_compat_fixture_detector_rejects_comment_metadata_and_alternative_include() {
    let root = nfe_4_external_fixture_attack_repo(
        "comment_metadata",
        format!(
            r#"
                // #![cfg(feature = "compat")]
                // include_bytes!("fixtures/external_starrocks_fe/{NFE_4_EXTERNAL_FIXTURE_BASENAME}")
                const FIXTURE: &[u8] = include_bytes!("fixtures/locally_generated.bin");
                fn consume() {{
                    ::novarocks::submit_exec_batch_plan_fragments(crate::FIXTURE);
                    novarocks_rs_try_fetch_result_batch();
                    novarocks_rs_free_buf();
                    let _: novarocks::thrift::data::TResultBatch;
                }}
            "#
        ),
    );

    let violations = nfe_4_external_compat_fixture_contract_violations(&root);
    assert!(
        !violations.is_empty(),
        "comment-only compat metadata plus an alternative production include must fail: {violations:?}"
    );
    fs::remove_dir_all(&root).unwrap();
}

#[test]
fn nfe_4_external_compat_fixture_detector_rejects_raw_string_include_decoy() {
    let root = nfe_4_external_fixture_attack_repo(
        "raw_string_decoy",
        format!(
            r####"
                #![cfg(feature = "compat")]
                const DECOY: &str = r###"include_bytes!("fixtures/external_starrocks_fe/{NFE_4_EXTERNAL_FIXTURE_BASENAME}")"###;
                const FIXTURE: &[u8] = include_bytes!("fixtures/locally_generated.bin");
                fn consume() {{
                    ::novarocks::submit_exec_batch_plan_fragments(crate::FIXTURE);
                    novarocks_rs_try_fetch_result_batch();
                    novarocks_rs_free_buf();
                    let _: novarocks::thrift::data::TResultBatch;
                }}
            "####
        ),
    );

    let violations = nfe_4_external_compat_fixture_contract_violations(&root);
    assert!(
        !violations.is_empty(),
        "a raw-string exact-path decoy plus an alternative production include must fail: {violations:?}"
    );
    fs::remove_dir_all(&root).unwrap();
}

#[test]
fn nfe_4_external_compat_fixture_detector_rejects_multiple_production_includes() {
    let root = nfe_4_external_fixture_attack_repo(
        "multiple_includes",
        format!(
            r#"
                #![cfg(feature = "compat")]
                const FIXTURE: &[u8] = ::std::include_bytes!("fixtures/external_starrocks_fe/{NFE_4_EXTERNAL_FIXTURE_BASENAME}");
                const EXTRA: &[u8] = ::std::include_bytes!("fixtures/locally_generated.bin");
                fn consume() {{
                    ::novarocks::submit_exec_batch_plan_fragments(crate::FIXTURE);
                    novarocks_rs_try_fetch_result_batch();
                    novarocks_rs_free_buf();
                    let _: novarocks::thrift::data::TResultBatch;
                }}
            "#
        ),
    );

    let violations = nfe_4_external_compat_fixture_contract_violations(&root);
    assert!(
        !violations.is_empty(),
        "the external consumer must contain exactly one production include_bytes!: {violations:?}"
    );
    fs::remove_dir_all(&root).unwrap();
}

#[test]
fn nfe_4_external_compat_fixture_detector_rejects_unused_exact_decoy_for_nonparen_fixture_include()
{
    let mut accepted = Vec::new();
    for (case, fixture_include) in [
        (
            "brace_fixture_include",
            "include_bytes!{\"fixtures/locally_generated.bin\"}",
        ),
        (
            "bracket_fixture_include",
            "include_bytes![\"fixtures/locally_generated.bin\"]",
        ),
    ] {
        let root = nfe_4_external_fixture_attack_repo(
            case,
            format!(
                r#"
                    #![cfg(feature = "compat")]
                    const UNUSED_EXACT_DECOY: &[u8] = ::std::include_bytes!("{NFE_4_EXTERNAL_FIXTURE_INCLUDE_PATH}");
                    const FIXTURE: &[u8] = {fixture_include};
                    fn consume() {{
                        ::novarocks::submit_exec_batch_plan_fragments(crate::FIXTURE);
                        novarocks_rs_try_fetch_result_batch();
                        novarocks_rs_free_buf();
                        let _: novarocks::thrift::data::TResultBatch;
                    }}
                "#
            ),
        );
        let violations = nfe_4_external_compat_fixture_contract_violations(&root);
        if violations.is_empty() {
            accepted.push(case);
        }
        fs::remove_dir_all(&root).unwrap();
    }
    assert!(
        accepted.is_empty(),
        "only const FIXTURE may include the exact external bytes; decoy-bypassed delimiters were accepted: {accepted:?}"
    );
}

#[test]
fn nfe_4_external_compat_fixture_detector_rejects_macro_token_cfg_decoy() {
    let root = nfe_4_external_fixture_attack_repo(
        "macro_token_cfg_decoy",
        format!(
            r#"
                swallow!(#![cfg(feature = "compat")]);
                const FIXTURE: &[u8] = ::std::include_bytes!("{NFE_4_EXTERNAL_FIXTURE_INCLUDE_PATH}");
                fn consume() {{
                    ::novarocks::submit_exec_batch_plan_fragments(crate::FIXTURE);
                    novarocks_rs_try_fetch_result_batch();
                    novarocks_rs_free_buf();
                    let _: novarocks::thrift::data::TResultBatch;
                }}
            "#
        ),
    );
    let violations = nfe_4_external_compat_fixture_contract_violations(&root);
    assert!(
        !violations.is_empty(),
        "cfg tokens inside another macro must not count as a crate attribute: {violations:?}"
    );
    fs::remove_dir_all(&root).unwrap();
}

#[test]
fn nfe_4_external_compat_fixture_detector_rejects_committed_generator_references() {
    let consumer =
        fs::read_to_string(Path::new(manifest_dir()).join("tests/external_starrocks_fe_compat.rs"))
            .unwrap();
    let root = nfe_4_external_fixture_attack_repo("committed_generator_refs", consumer);
    fs::create_dir_all(root.join("tools")).unwrap();
    fs::write(
        root.join("build.rs"),
        format!("// regenerate {NFE_4_EXTERNAL_FIXTURE_INCLUDE_PATH}\n"),
    )
    .unwrap();
    fs::write(
        root.join("tools/ExternalFixtureGenerator.java"),
        format!("// reads {NFE_4_EXTERNAL_FIXTURE_BASENAME}\n"),
    )
    .unwrap();

    let violations = nfe_4_external_compat_fixture_contract_violations(&root);
    assert!(
        !violations.is_empty(),
        "raw committed generator references outside the fixed three-file set must fail: {violations:?}"
    );
    fs::remove_dir_all(&root).unwrap();
}

#[test]
fn nfe_4_external_compat_fixture_ast_accepts_exact_delimiters_and_ignores_cfg_test() {
    for (case, fixture_include) in [
        (
            "exact_brace_include",
            format!("::std::include_bytes!{{\"{NFE_4_EXTERNAL_FIXTURE_INCLUDE_PATH}\"}}"),
        ),
        (
            "exact_bracket_include",
            format!("::std::include_bytes![\"{NFE_4_EXTERNAL_FIXTURE_INCLUDE_PATH}\"]"),
        ),
    ] {
        let root = nfe_4_external_fixture_attack_repo(
            case,
            format!(
                r#"
                    #![cfg(feature = "compat")]
                    const FIXTURE: &[u8] = {fixture_include};
                    #[cfg(test)]
                    const TEST_ONLY: &[u8] = include_bytes!("fixtures/locally_generated.bin");
                    fn consume() {{
                        ::novarocks::submit_exec_batch_plan_fragments(crate::FIXTURE);
                        novarocks_rs_try_fetch_result_batch();
                        novarocks_rs_free_buf();
                        let _: novarocks::thrift::data::TResultBatch;
                    }}
                "#
            ),
        );
        let violations = nfe_4_external_compat_fixture_contract_violations(&root);
        assert!(
            violations.is_empty(),
            "exact brace/bracket fixture includes must be accepted and cfg(test) includes ignored: {fixture_include}: {violations:?}"
        );
        fs::remove_dir_all(&root).unwrap();
    }
}

#[test]
fn nfe_4_external_compat_fixture_detector_rejects_local_include_bytes_shadow() {
    let root = nfe_4_external_fixture_attack_repo(
        "local_include_bytes_shadow",
        format!(
            r#"
                #![cfg(feature = "compat")]
                macro_rules! include_bytes {{ ($path:literal) => {{ b"" }}; }}
                const FIXTURE: &[u8] = include_bytes!("{NFE_4_EXTERNAL_FIXTURE_INCLUDE_PATH}");
                fn consume() {{
                    ::novarocks::submit_exec_batch_plan_fragments(crate::FIXTURE);
                    novarocks_rs_try_fetch_result_batch();
                    novarocks_rs_free_buf();
                    let _: novarocks::thrift::data::TResultBatch;
                }}
            "#
        ),
    );
    let violations = nfe_4_external_compat_fixture_contract_violations(&root);
    assert!(
        !violations.is_empty(),
        "a locally shadowed include_bytes macro must not establish external fixture provenance: {violations:?}"
    );
    fs::remove_dir_all(&root).unwrap();
}

#[test]
fn nfe_4_external_compat_fixture_detector_rejects_self_as_std_prelude_hijack() {
    let root = nfe_4_external_fixture_attack_repo(
        "self_as_std_prelude_hijack",
        format!(
            r#"
                #![cfg(feature = "compat")]
                #![no_implicit_prelude]
                #[macro_export]
                macro_rules! include_bytes {{ ($path:literal) => {{ b"" }}; }}
                extern crate self as std;
                const FIXTURE: &[u8] = ::std::include_bytes!("{NFE_4_EXTERNAL_FIXTURE_INCLUDE_PATH}");
                fn consume() {{
                    ::novarocks::submit_exec_batch_plan_fragments(crate::FIXTURE);
                    novarocks_rs_try_fetch_result_batch();
                    novarocks_rs_free_buf();
                    let _: novarocks::thrift::data::TResultBatch;
                }}
            "#
        ),
    );
    let violations = nfe_4_external_compat_fixture_contract_violations(&root);
    assert!(
        !violations.is_empty(),
        "no_implicit_prelude plus self-as-std must not hijack the absolute include macro: {violations:?}"
    );
    fs::remove_dir_all(&root).unwrap();
}

#[test]
fn nfe_4_external_compat_fixture_detector_rejects_inner_static_fixture_shadow() {
    let root = nfe_4_external_fixture_attack_repo(
        "inner_static_fixture_shadow",
        format!(
            r#"
                #![cfg(feature = "compat")]
                const FIXTURE: &[u8] = ::std::include_bytes!("{NFE_4_EXTERNAL_FIXTURE_INCLUDE_PATH}");
                fn consume() {{
                    static FIXTURE: &[u8] = b"shadow";
                    ::novarocks::submit_exec_batch_plan_fragments(FIXTURE);
                    novarocks_rs_try_fetch_result_batch();
                    novarocks_rs_free_buf();
                    let _: novarocks::thrift::data::TResultBatch;
                }}
            "#
        ),
    );
    let violations = nfe_4_external_compat_fixture_contract_violations(&root);
    assert!(
        !violations.is_empty(),
        "submit must bind explicitly to crate::FIXTURE rather than an inner static shadow: {violations:?}"
    );
    fs::remove_dir_all(&root).unwrap();
}

#[test]
fn nfe_4_external_compat_fixture_detector_rejects_novarocks_crate_rebinding() {
    for (case, rebinding, expected_violation) in [
        (
            "local_module",
            r#"
                mod novarocks {
                    pub fn submit_exec_batch_plan_fragments(_: &[u8]) {}
                }
            "#,
            "local module named `novarocks`",
        ),
        (
            "extern_crate_rename",
            r#"
                extern crate self as novarocks;
            "#,
            "redefine or alias crate name `novarocks`",
        ),
        (
            "extern_crate_name",
            r#"
                extern crate novarocks as real_novarocks;
            "#,
            "redefine or alias crate name `novarocks`",
        ),
        (
            "grouped_use_rename",
            r#"
                mod fake {
                    pub fn submit_exec_batch_plan_fragments(_: &[u8]) {}
                }
                use crate::{fake as novarocks};
            "#,
            "import a binding named `novarocks`",
        ),
    ] {
        let root = nfe_4_external_fixture_attack_repo(
            &format!("novarocks_crate_rebinding_{case}"),
            format!(
                r#"
                    #![cfg(feature = "compat")]
                    {rebinding}
                    const FIXTURE: &[u8] = ::std::include_bytes!("{NFE_4_EXTERNAL_FIXTURE_INCLUDE_PATH}");
                    fn consume() {{
                        novarocks::submit_exec_batch_plan_fragments(crate::FIXTURE);
                        novarocks_rs_try_fetch_result_batch();
                        novarocks_rs_free_buf();
                        let _: novarocks::thrift::data::TResultBatch;
                    }}
                "#
            ),
        );
        let violations = nfe_4_external_compat_fixture_contract_violations(&root);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains(expected_violation)),
            "case {case} must report its novarocks crate rebinding explicitly: {violations:?}"
        );
        fs::remove_dir_all(&root).unwrap();
    }
}

#[test]
fn nfe_4_external_compat_fixture_detector_rejects_additional_inner_gating_attributes() {
    let mut accepted = Vec::new();
    for (case, extra_gate) in [
        ("extra_cfg", "#![cfg(unix)]"),
        ("extra_cfg_attr", "#![cfg_attr(unix, allow(dead_code))]"),
    ] {
        let consumer = fs::read_to_string(
            Path::new(manifest_dir()).join("tests/external_starrocks_fe_compat.rs"),
        )
        .unwrap()
        .replacen(
            "#![cfg(feature = \"compat\")]",
            &format!("#![cfg(feature = \"compat\")]\n{extra_gate}"),
            1,
        );
        let root = nfe_4_external_fixture_attack_repo(case, consumer);
        let violations = nfe_4_external_compat_fixture_contract_violations(&root);
        if violations.is_empty() {
            accepted.push(case);
        }
        fs::remove_dir_all(&root).unwrap();
    }
    assert!(
        accepted.is_empty(),
        "the exact compat cfg must be the sole crate-level gating attribute: {accepted:?}"
    );
}

#[test]
fn nfe_4_external_compat_fixture_detector_rejects_extensionless_makefile_and_kotlin_references() {
    let consumer =
        fs::read_to_string(Path::new(manifest_dir()).join("tests/external_starrocks_fe_compat.rs"))
            .unwrap();
    let root = nfe_4_external_fixture_attack_repo("all_file_reference_scan", consumer);
    fs::create_dir_all(root.join("tools")).unwrap();
    for (relative, contents) in [
        (
            "tools/regenerate_external_fixture",
            format!("# {NFE_4_EXTERNAL_FIXTURE_BASENAME}\n"),
        ),
        (
            "Makefile",
            format!("# {NFE_4_EXTERNAL_FIXTURE_INCLUDE_PATH}\n"),
        ),
        (
            "tools/ExternalFixture.kt",
            format!("// {NFE_4_EXTERNAL_FIXTURE_BASENAME}\n"),
        ),
    ] {
        fs::write(root.join(relative), contents).unwrap();
    }
    let violations = nfe_4_external_compat_fixture_contract_violations(&root);
    assert!(
        !violations.is_empty(),
        "raw references in regular files must be detected without an extension allowlist: {violations:?}"
    );
    fs::remove_dir_all(&root).unwrap();
}

#[cfg(unix)]
#[test]
fn nfe_4_external_compat_fixture_detector_rejects_every_extra_fixture_entry_kind() {
    let consumer =
        fs::read_to_string(Path::new(manifest_dir()).join("tests/external_starrocks_fe_compat.rs"))
            .unwrap();
    let mut accepted = Vec::new();
    for case in ["extensionless_file", "directory", "symlink"] {
        let root = nfe_4_external_fixture_attack_repo(case, consumer.clone());
        let fixture_dir = root.join("tests/fixtures/external_starrocks_fe");
        match case {
            "extensionless_file" => fs::write(fixture_dir.join("generator"), []).unwrap(),
            "directory" => fs::create_dir(fixture_dir.join("nested")).unwrap(),
            "symlink" => {
                std::os::unix::fs::symlink(fixture_dir.join("README.md"), fixture_dir.join("alias"))
                    .unwrap()
            }
            _ => unreachable!(),
        }
        let violations = nfe_4_external_compat_fixture_contract_violations(&root);
        if violations.is_empty() {
            accepted.push(case);
        }
        fs::remove_dir_all(&root).unwrap();
    }
    assert!(
        accepted.is_empty(),
        "fixture directory extras must fail regardless of entry kind or extension: {accepted:?}"
    );
}

#[test]
fn nfe_4_external_compat_fixture_ast_ignores_deep_definite_cfg_test_subtrees() {
    let root = nfe_4_external_fixture_attack_repo(
        "deep_cfg_test_subtrees",
        format!(
            r#"
                #![cfg(feature = "compat")]
                const FIXTURE: &[u8] = ::std::include_bytes!("{NFE_4_EXTERNAL_FIXTURE_INCLUDE_PATH}");
                struct Holder;
                impl Holder {{
                    #[cfg(test)]
                    const TEST_ONLY: &'static [u8] = include_bytes!("fixtures/impl-test-only.bin");
                }}
                fn nested() {{
                    #[cfg(test)]
                    let _ = include_bytes!("fixtures/stmt-test-only.bin");
                }}
                fn consume() {{
                    ::novarocks::submit_exec_batch_plan_fragments(crate::FIXTURE);
                    novarocks_rs_try_fetch_result_batch();
                    novarocks_rs_free_buf();
                    let _: novarocks::thrift::data::TResultBatch;
                }}
            "#
        ),
    );
    let violations = nfe_4_external_compat_fixture_contract_violations(&root);
    assert!(
        violations.is_empty(),
        "definite cfg(test) impl items and nested statements must be excluded: {violations:?}"
    );
    fs::remove_dir_all(&root).unwrap();
}

#[test]
fn nfe_4_external_compat_fixture_ast_keeps_cfg_any_test_or_compat_subtrees() {
    let root = nfe_4_external_fixture_attack_repo(
        "cfg_any_test_or_compat",
        format!(
            r#"
                #![cfg(feature = "compat")]
                const FIXTURE: &[u8] = ::std::include_bytes!("{NFE_4_EXTERNAL_FIXTURE_INCLUDE_PATH}");
                #[cfg(any(test, feature = "compat"))]
                const COMPAT_VISIBLE: &[u8] = include_bytes!("fixtures/compat-visible.bin");
                fn consume() {{
                    ::novarocks::submit_exec_batch_plan_fragments(crate::FIXTURE);
                    novarocks_rs_try_fetch_result_batch();
                    novarocks_rs_free_buf();
                    let _: novarocks::thrift::data::TResultBatch;
                }}
            "#
        ),
    );
    let violations = nfe_4_external_compat_fixture_contract_violations(&root);
    assert!(
        !violations.is_empty(),
        "cfg(any(test, feature = \"compat\")) is production-visible under compat and must not be skipped"
    );
    fs::remove_dir_all(&root).unwrap();
}

#[cfg(unix)]
#[test]
fn nfe_4_external_compat_fixture_detector_rejects_symlinked_parent() {
    let source_repo = Path::new(manifest_dir());
    let root = std::env::temp_dir().join(format!(
        "nfe_4_external_fixture_symlink_parent_{}",
        std::process::id()
    ));
    let external = std::env::temp_dir().join(format!(
        "nfe_4_external_fixture_symlink_target_{}",
        std::process::id()
    ));
    fs::remove_dir_all(&root).ok();
    fs::remove_dir_all(&external).ok();
    fs::create_dir_all(root.join("tests")).unwrap();
    fs::create_dir_all(external.join("external_starrocks_fe")).unwrap();
    for relative in ["README.md", "select_1_exec_batch_plan_fragments_v1.bin"] {
        fs::copy(
            source_repo
                .join("tests/fixtures/external_starrocks_fe")
                .join(relative),
            external.join("external_starrocks_fe").join(relative),
        )
        .unwrap();
    }
    fs::copy(
        source_repo.join("tests/external_starrocks_fe_compat.rs"),
        root.join("tests/external_starrocks_fe_compat.rs"),
    )
    .unwrap();
    std::os::unix::fs::symlink(&external, root.join("tests/fixtures")).unwrap();

    let violations = nfe_4_external_compat_fixture_contract_violations(&root);
    assert!(
        violations
            .iter()
            .any(|violation| violation.contains("must not be a symlink")),
        "a symlinked fixture parent must fail explicitly even when each leaf resolves to a regular file: {violations:?}"
    );
    fs::remove_dir_all(&root).unwrap();
    fs::remove_dir_all(&external).unwrap();
}

#[test]
fn nfe_3_fe_owned_helpers_are_raw_starrocks_idl_free() {
    let fixture = r#"
        // crate::thrift in a comment must be ignored.
        const NOTE: &str = "crate::proto::staros in a string must be ignored";
        fn ordinary() { let _ = crate :: proto :: starrocks :: StatusPb::default(); }
        #[cfg(feature = "compat")]
        fn compat_only() { let _ = crate::thrift::types::TUniqueId::new(1, 2); }
        #[cfg(test)]
        fn test_only() { let _ = crate::proto::staros::WorkerInfo::default(); }
    "#;
    assert_eq!(
        nfe_3_raw_starrocks_idl_violations("fixture.rs", fixture),
        vec![
            "fixture.rs: FE-owned helper references raw StarRocks IDL `crate::thrift`",
            "fixture.rs: FE-owned helper references raw StarRocks IDL `crate::proto::starrocks`",
        ],
        "the NFE-3 helper guard must retain compat-cfg production items while ignoring comments, strings, and test-only items"
    );

    let grouped_use_fixture = r#"
        use crate::{thrift::types::TUniqueId};
        #[cfg(feature = "compat")]
        use crate::proto::{starrocks::StatusPb, staros::WorkerInfo};
        // use crate::{thrift::types::TUniqueId};
        const NOTE: &str = "use crate::proto::{starrocks::StatusPb};";
        #[cfg(test)]
        use crate::{thrift::data::TResultBatch};
    "#;
    assert_eq!(
        nfe_3_raw_starrocks_idl_violations("grouped.rs", grouped_use_fixture),
        vec![
            "grouped.rs: FE-owned helper references raw StarRocks IDL `crate::thrift`",
            "grouped.rs: FE-owned helper references raw StarRocks IDL `crate::proto::starrocks`",
            "grouped.rs: FE-owned helper references raw StarRocks IDL `crate::proto::staros`",
        ],
        "the NFE-3 helper guard must expand ordinary and compat-cfg grouped imports while ignoring grouped imports in comments, strings, and test-only items"
    );

    let alias_chain_fixture = r#"
        use crate as root;
        use root::thrift::types::TUniqueId;
        #[cfg(feature = "compat")]
        use crate::proto as wire;
        #[cfg(feature = "compat")]
        use wire::{starrocks::StatusPb, staros::WorkerInfo};
        // use crate as hidden_root; use hidden_root::thrift::types::TUniqueId;
        const NOTE: &str = "use crate::proto as hidden_wire; use hidden_wire::starrocks::StatusPb;";
        #[cfg(test)]
        use crate as test_root;
        #[cfg(test)]
        use test_root::thrift::data::TResultBatch;
    "#;
    assert_eq!(
        nfe_3_raw_starrocks_idl_violations("aliases.rs", alias_chain_fixture),
        vec![
            "aliases.rs: FE-owned helper references raw StarRocks IDL `crate::thrift`",
            "aliases.rs: FE-owned helper references raw StarRocks IDL `crate::proto::starrocks`",
            "aliases.rs: FE-owned helper references raw StarRocks IDL `crate::proto::staros`",
        ],
        "the NFE-3 helper guard must resolve ordinary and compat-cfg file-local use aliases while ignoring alias chains in comments, strings, and test-only items"
    );

    let alias_usage_fixture = r#"
        use crate as root;
        fn ordinary(_: root::thrift::types::TUniqueId) {}
        #[cfg(feature = "compat")]
        use crate::proto as wire;
        #[cfg(feature = "compat")]
        fn compat_only() {
            let _ = wire::starrocks::StatusPb::default();
            let _ = wire::staros::WorkerInfo::default();
        }
        // fn hidden(_: root::thrift::types::TUniqueId) {}
        const NOTE: &str = "wire::starrocks::StatusPb";
        #[cfg(test)]
        fn test_only(_: root::thrift::types::TUniqueId) {
            let _ = wire::staros::WorkerInfo::default();
        }
    "#;
    assert_eq!(
        nfe_3_raw_starrocks_idl_violations("alias_usage.rs", alias_usage_fixture),
        vec![
            "alias_usage.rs: FE-owned helper references raw StarRocks IDL `crate::thrift`",
            "alias_usage.rs: FE-owned helper references raw StarRocks IDL `crate::proto::starrocks`",
            "alias_usage.rs: FE-owned helper references raw StarRocks IDL `crate::proto::staros`",
        ],
        "the NFE-3 helper guard must apply file-local use aliases to production type and expression paths while ignoring alias-qualified paths in comments, strings, and test-only items"
    );

    let repo = Path::new(manifest_dir());
    let mut violations = Vec::new();
    for source in [
        "src/engine/query_options.rs",
        "src/engine/dml_change_stream.rs",
        "src/sql/common/change_stream.rs",
        "src/runtime/coordinator.rs",
        "src/runtime/dispatcher.rs",
        "src/runtime/fragment_exec_params.rs",
        "src/runtime/scan_range.rs",
        "src/runtime/native_fragment_wire.rs",
        "src/sql/codegen/connector_scan_planning.rs",
        "src/sql/codegen/iceberg_delta_scan_planning.rs",
        "src/sql/codegen/proto_encode/iceberg_delta_scan.rs",
        "src/sql/planner/distributed/build/runtime_filter_binding.rs",
    ] {
        let text = fs::read_to_string(repo.join(source)).expect(source);
        violations.extend(nfe_3_raw_starrocks_idl_violations(source, &text));
    }

    for retired_path in [
        "src/engine/query_options_wire.rs",
        "src/exec/spill/query_options_wire.rs",
    ] {
        match fs::symlink_metadata(repo.join(retired_path)) {
            Ok(_) => violations.push(format!("{retired_path}: retired helper file still exists")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => violations.push(format!(
                "{retired_path}: failed to inspect path metadata: {error}"
            )),
        }
    }

    let common_change_stream = fs::read_to_string(repo.join("src/sql/common/change_stream.rs"))
        .expect("src/sql/common/change_stream.rs");
    let common_mod =
        fs::read_to_string(repo.join("src/sql/common/mod.rs")).expect("src/sql/common/mod.rs");
    for (source, text) in [
        ("src/sql/common/change_stream.rs", common_change_stream),
        ("src/sql/common/mod.rs", common_mod),
    ] {
        if rust_sanitized_production_text(&text).contains("branch_kind_from_thrift") {
            violations.push(format!(
                "{source}: external Thrift enum adapter must be owned by lower/compat"
            ));
        }
    }

    assert!(
        violations.is_empty(),
        "NFE-3 FE helper ownership guard failed:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nfe_3_native_planning_owners_are_named_explicitly() {
    let repo = Path::new(manifest_dir());
    let expected_owners = [
        "src/sql/codegen/connector_scan_planning.rs",
        "src/sql/codegen/iceberg_delta_scan_planning.rs",
        "src/sql/codegen/proto_encode/iceberg_delta_scan.rs",
        "src/sql/planner/distributed/build/runtime_filter_binding.rs",
    ];
    let retired_owners = [
        "src/sql/codegen/connector_scan_wire.rs",
        "src/sql/codegen/iceberg_delta_scan_wire.rs",
        "src/sql/planner/distributed/build/runtime_filter_wire.rs",
    ];
    let mut violations = Vec::new();

    for source in expected_owners {
        match fs::symlink_metadata(repo.join(source)) {
            Ok(metadata) if metadata.file_type().is_file() => {}
            Ok(_) => violations.push(format!("{source}: expected regular owner file")),
            Err(error) => violations.push(format!("{source}: missing owner file: {error}")),
        }
    }
    for source in retired_owners {
        match fs::symlink_metadata(repo.join(source)) {
            Ok(_) => violations.push(format!("{source}: retired `_wire` owner still exists")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => violations.push(format!("{source}: failed to inspect path: {error}")),
        }
    }

    let codegen_mod = fs::read_to_string(repo.join("src/sql/codegen/mod.rs")).unwrap();
    let build_mod =
        fs::read_to_string(repo.join("src/sql/planner/distributed/build/mod.rs")).unwrap();
    let proto_mod = fs::read_to_string(repo.join("src/sql/codegen/proto_encode/mod.rs")).unwrap();
    for (source, text, required) in [
        (
            "src/sql/codegen/mod.rs",
            codegen_mod.as_str(),
            &[
                "mod connector_scan_planning;",
                "mod iceberg_delta_scan_planning;",
            ][..],
        ),
        (
            "src/sql/planner/distributed/build/mod.rs",
            build_mod.as_str(),
            &["mod runtime_filter_binding;"][..],
        ),
        (
            "src/sql/codegen/proto_encode/mod.rs",
            proto_mod.as_str(),
            &["mod iceberg_delta_scan;"][..],
        ),
    ] {
        let production = rust_sanitized_production_text(text);
        for declaration in required {
            if !compact_line(&production).contains(&compact_line(declaration)) {
                violations.push(format!("{source}: missing `{declaration}`"));
            }
        }
    }
    for (source, text) in [
        ("src/sql/codegen/mod.rs", codegen_mod.as_str()),
        (
            "src/sql/planner/distributed/build/mod.rs",
            build_mod.as_str(),
        ),
    ] {
        let production = rust_sanitized_production_text(text);
        for retired in [
            "connector_scan_wire",
            "iceberg_delta_scan_wire",
            "runtime_filter_wire",
        ] {
            if production.contains(retired) {
                violations.push(format!("{source}: retired module name `{retired}`"));
            }
        }
    }

    let planning_path = repo.join("src/sql/codegen/connector_scan_planning.rs");
    let encoder_path = repo.join("src/sql/codegen/proto_encode/plan.rs");
    if planning_path.is_file() {
        let planning = fs::read_to_string(&planning_path).unwrap();
        let encoder = fs::read_to_string(&encoder_path).unwrap();
        for descriptor in [
            "StarRocksStorageColumnDescriptor",
            "StarRocksKeysTypeDescriptor",
            "StarRocksColumnSchemaDescriptor",
            "StarRocksTabletSchemaDescriptor",
            "StarRocksScanSourceDescriptor",
        ] {
            if rust_named_type_declaration_count(&planning, descriptor) != 1 {
                violations.push(format!(
                    "{}: must own exactly one `{descriptor}` declaration",
                    rel(&planning_path)
                ));
            }
            if rust_named_type_declaration_count(&encoder, descriptor) != 0 {
                violations.push(format!(
                    "{}: encoder must import rather than declare `{descriptor}`",
                    rel(&encoder_path)
                ));
            }
        }
    }

    let delta_planning_path = repo.join("src/sql/codegen/iceberg_delta_scan_planning.rs");
    let delta_encoder_path = repo.join("src/sql/codegen/proto_encode/iceberg_delta_scan.rs");
    if delta_planning_path.is_file() && delta_encoder_path.is_file() {
        let planning = fs::read_to_string(&delta_planning_path).unwrap();
        let encoder = fs::read_to_string(&delta_encoder_path).unwrap();
        if rust_named_function_declaration_count(&planning, "build_iceberg_delta_scan_runtime_plan")
            != 1
        {
            violations.push(format!(
                "{}: must own runtime-plan construction",
                rel(&delta_planning_path)
            ));
        }
        if rust_named_function_declaration_count(&planning, "encode_iceberg_delta_scan_plan_native")
            != 0
        {
            violations.push(format!(
                "{}: planning owner must not encode Proto",
                rel(&delta_planning_path)
            ));
        }
        if rust_named_function_declaration_count(&encoder, "encode_iceberg_delta_scan_plan_native")
            != 1
        {
            violations.push(format!(
                "{}: must own native Proto encoding",
                rel(&delta_encoder_path)
            ));
        }
    }

    let sql_sources = rs_files(&repo.join("src/sql"))
        .into_iter()
        .map(|path| (rel(&path), fs::read_to_string(path).unwrap()))
        .collect::<Vec<_>>();
    for retired in [
        "WiredRuntimeFilterBuild",
        "WiredRuntimeFilterProbe",
        "runtime_filter_wire",
    ] {
        for (source, text) in &sql_sources {
            if rust_sanitized_production_text(text).contains(retired) {
                violations.push(format!("{source}: retired RF owner/symbol `{retired}`"));
            }
        }
    }
    for (symbol, expected_owner) in [
        (
            "BoundRuntimeFilterBuild",
            "src/sql/planner/distributed/runtime_filter.rs",
        ),
        (
            "BoundRuntimeFilterProbe",
            "src/sql/planner/distributed/runtime_filter.rs",
        ),
        (
            "bind_runtime_filters",
            "src/sql/planner/distributed/build/runtime_filter_binding.rs",
        ),
    ] {
        let declarations = sql_sources
            .iter()
            .filter_map(|(source, text)| {
                let count = if symbol == "bind_runtime_filters" {
                    rust_named_function_declaration_count(text, symbol)
                } else {
                    rust_named_type_declaration_count(text, symbol)
                };
                (count > 0).then(|| format!("{source} ({count})"))
            })
            .collect::<Vec<_>>();
        assert_eq!(
            declarations,
            vec![format!("{expected_owner} (1)")],
            "{symbol} must have exactly one explicit owner"
        );
    }

    for source in [
        "src/sql/codegen/connector_scan_planning.rs",
        "src/sql/codegen/iceberg_delta_scan_planning.rs",
        "src/sql/planner/distributed/build/runtime_filter_binding.rs",
    ] {
        if let Ok(text) = fs::read_to_string(repo.join(source)) {
            violations.extend(nfe_3_raw_starrocks_idl_violations(source, &text));
        }
    }

    assert!(
        violations.is_empty(),
        "NFE-3 native planning owner guard failed:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nfe_3_audit_baselines_do_not_reauthorize_retired_paths() {
    let audit_path = Path::new(manifest_dir()).join("tools/dev/audit_thrift_boundaries.py");
    let audit = fs::read_to_string(&audit_path).unwrap();
    let compact = audit.lines().map(compact_line).collect::<String>();
    let mut violations = Vec::new();

    for retired in [
        "src/runtime/exec_params.rs",
        "src/runtime/exec_params_compat.rs",
        "src/engine/query_options_wire.rs",
        "src/exec/spill/query_options_wire.rs",
    ] {
        let key = format!("\"{retired}\":BaselineEntry(");
        if compact.contains(&key) {
            violations.push(format!(
                "{retired}: retired path must not retain an audit baseline"
            ));
        }
    }

    for (native, owner) in [
        ("src/engine/dml_change_stream.rs", "B7"),
        ("src/runtime/coordinator.rs", "control-plane"),
        ("src/runtime/dispatcher.rs", "control-plane"),
        ("src/runtime/fragment_exec_params.rs", "control-plane"),
        ("src/runtime/scan_range.rs", "B5"),
        ("src/runtime/native_fragment_wire.rs", "control-plane"),
    ] {
        let prefix = format!("\"{native}\":BaselineEntry(\"domain-leak\",\"{owner}\",0,");
        if !compact.contains(&prefix) {
            violations.push(format!(
                "{native}: NFE-native owner must have domain-leak/{owner} max_hits=0"
            ));
        }
    }

    assert!(
        violations.is_empty(),
        "NFE-3 audit baseline guard failed:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nfe_3_dead_generated_thrift_control_plane_is_absent() {
    let fixture = r#"
        // to_compat_exec_params in a comment must be ignored.
        const NOTE: &str = "compat_exec_params_from_parts in a string must be ignored";
        fn ordinary() { compat_destination_from_runtime(); }
        #[cfg(feature = "compat")]
        fn compat_only() { thrift_scan_range_params_from_native(); }
        #[cfg(test)]
        fn test_only() { compat_change_op_slot_id(); }
    "#;
    let fixture_violations = nfe_3_retired_control_plane_term_violations(
        "fixture.rs",
        fixture,
        &[
            "to_compat_exec_params",
            "compat_exec_params_from_parts",
            "compat_destination_from_runtime",
            "thrift_scan_range_params_from_native",
            "compat_change_op_slot_id",
        ],
    );
    assert_eq!(
        fixture_violations,
        vec![
            "fixture.rs: retired generated-Thrift control-plane term `compat_destination_from_runtime`",
            "fixture.rs: retired generated-Thrift control-plane term `thrift_scan_range_params_from_native`",
        ],
        "the NFE-3 guard must retain compat-cfg production items while ignoring comments, strings, and test-only items"
    );

    let repo = Path::new(manifest_dir());
    let mut violations = Vec::new();
    let thrift_idl = fs::read_to_string(repo.join("idl/compat/thrift/InternalService.thrift"))
        .expect("read InternalService.thrift");
    if thrift_idl.lines().any(|line| {
        line.split("//")
            .next()
            .is_some_and(|code| code.contains("novarocks_generated_plan"))
    }) {
        violations.push(
            "idl/compat/thrift/InternalService.thrift: retired field `novarocks_generated_plan`"
                .to_string(),
        );
    }

    for (source, forbidden) in [
        (
            "src/service/internal_service.rs",
            &[
                "plan_origin_from_request",
                "PlanOrigin",
                "novarocks_generated_plan",
            ][..],
        ),
        (
            "src/lower/compat/fragment.rs",
            &["PlanOrigin", "NovaRocksGenerated"][..],
        ),
        (
            "src/lower/compat/node/mod.rs",
            &["PlanOrigin", "NovaRocksGenerated"][..],
        ),
        (
            "src/lower/compat/node/decode.rs",
            &["PlanOrigin", "NovaRocksGenerated"][..],
        ),
        (
            "src/lower/compat/node/starrocks_scan.rs",
            &["PlanOrigin", "NovaRocksGenerated"][..],
        ),
        (
            "src/runtime/fragment_exec_params.rs",
            &[
                "to_compat_exec_params",
                "compat_exec_params_from_parts",
                "compat_destination_from_runtime",
                "crate::thrift",
                "crate::proto::starrocks",
                "crate::proto::staros",
            ][..],
        ),
        (
            "src/runtime/scan_range.rs",
            &[
                "thrift_scan_range_params_from_native",
                "thrift_hdfs_scan_range_from_native",
                "thrift_extended_columns_from_native",
                "compat_change_op_slot_id",
                "crate::thrift",
                "crate::proto::starrocks",
                "crate::proto::staros",
            ][..],
        ),
        (
            "src/runtime/native_fragment_wire.rs",
            &[
                "crate::thrift",
                "crate::proto::starrocks",
                "crate::proto::staros",
            ][..],
        ),
        ("src/runtime/endpoint.rs", &["to_network_address"][..]),
    ] {
        let text = fs::read_to_string(repo.join(source)).expect(source);
        violations.extend(nfe_3_retired_control_plane_term_violations(
            source, &text, forbidden,
        ));
    }

    assert!(
        violations.is_empty(),
        "NFE-3 retired generated-Thrift control-plane guard failed:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nfe_2_legacy_thrift_emitters_are_physically_absent() {
    let violations = nfe_2_legacy_thrift_emitter_path_violations(Path::new(manifest_dir()));
    assert!(
        violations.is_empty(),
        "legacy NovaRocks FE Thrift emitters must be physically deleted:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nfe_2_legacy_thrift_emitter_absence_guard_is_non_vacuous() {
    let root = std::env::temp_dir().join(format!(
        "nfe_2_legacy_thrift_emitter_absence_guard_{}",
        std::process::id()
    ));
    fs::remove_dir_all(&root).ok();
    let relative = NFE_2_LEGACY_THRIFT_EMITTER_PATHS[0];
    let fixture = root.join(relative);
    fs::create_dir_all(fixture.parent().unwrap()).unwrap();
    fs::write(&fixture, "// legacy emitter fixture\n").unwrap();

    assert_eq!(
        nfe_2_legacy_thrift_emitter_path_violations(&root),
        vec![relative.to_string()]
    );

    #[cfg(unix)]
    {
        fs::remove_file(&fixture).unwrap();
        std::os::unix::fs::symlink("missing-target", &fixture).unwrap();
        assert_eq!(
            nfe_2_legacy_thrift_emitter_path_violations(&root),
            vec![relative.to_string()],
            "dangling symlinks must count as present legacy emitter paths"
        );
    }

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn nfe_1_task_4_fragment_build_is_unique_and_native_only() {
    let repo = Path::new(manifest_dir());
    let mut violations = Vec::new();

    let ir_mod = fs::read_to_string(repo.join("src/sql/codegen/ir/mod.rs")).unwrap();
    let ir_tokens = rust_use_tokens(&rust_sanitized_production_text(&ir_mod));
    let expected_export = [
        "pub",
        "(",
        "crate",
        ")",
        "use",
        "fragment_build",
        "::",
        "lower_distributed_plan",
        ";",
    ];
    if !ir_tokens.windows(expected_export.len()).any(|tokens| {
        tokens
            .iter()
            .map(String::as_str)
            .eq(expected_export.iter().copied())
    }) {
        violations.push(
            "src/sql/codegen/ir/mod.rs: fragment_build must be exported unconditionally"
                .to_string(),
        );
    }

    for rel in [
        "src/sql/codegen/mod.rs",
        "src/sql/codegen/fragment_builder.rs",
        "src/engine/mod.rs",
        "src/engine/dml_change_stream.rs",
        "src/runtime/coordinator.rs",
        "src/runtime/dispatcher.rs",
    ] {
        let text = fs::read_to_string(repo.join(rel)).unwrap();
        let production = rust_production_text_without_cfg_test(&text);
        push_forbidden_terms(
            &mut violations,
            rel,
            &production,
            &[
                "struct FragmentBuildResult",
                "struct LoweredFragmentEdge",
                "pub fragment_results:",
                "pub lowered_edges:",
                "refresh_fragment_schedules",
            ],
            "Task 4 removes legacy build carriers from production",
        );
    }

    let fragment_build =
        fs::read_to_string(repo.join("src/sql/codegen/ir/fragment_build.rs")).unwrap();
    let production = rust_production_text_without_cfg_test(&fragment_build);
    push_forbidden_terms(
        &mut violations,
        "src/sql/codegen/ir/fragment_build.rs",
        &production,
        &[
            "crate::thrift",
            "struct FragmentBuildResult",
            "cfg(feature = \"compat\")",
        ],
        "the unique fragment builder must be feature-neutral and Thrift-free",
    );

    assert!(
        violations.is_empty(),
        "NFE-1 Task 4 unique native builder guard failed:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nidl_d3l_native_mainline_thrift_usage_is_explicitly_allowlisted() {
    let repo = Path::new(manifest_dir());
    let mut violations = Vec::new();

    let scheduler = fs::read_to_string(repo.join("src/runtime/scheduler.rs")).unwrap();
    let scheduler = rust_production_text_without_cfg_test(&scheduler);
    push_forbidden_terms(
        &mut violations,
        "src/runtime/scheduler.rs",
        &scheduler,
        &[
            "fragment_sink_is_terminal_write_sink",
            "find_scan_plan_nodes(",
            "TDataSink",
            "TPlan)",
        ],
        "native scheduler must use FragmentBuildResult metadata, not compat thrift structs",
    );

    let coordinator = fs::read_to_string(repo.join("src/runtime/coordinator.rs")).unwrap();
    let coordinator = rust_production_text_without_cfg_test(&coordinator);
    push_forbidden_terms(
        &mut violations,
        "src/runtime/coordinator.rs",
        &coordinator,
        &[
            "patch_native_iceberg_delta_scan_payloads",
            "native_data_partition_from_thrift",
            "native_data_partition_from_thrift_with_exprs",
            "TIcebergDeltaScanNode",
            "TIcebergDeltaScanPlan",
            "Vec<(FragmentId, i32, partitions::TDataPartition, Vec<i32>)>",
        ],
        "native coordinator must not patch native sidecars from thrift-shaped payloads",
    );

    let mut native_lowering_sources = ["src/lower/novarocks/layout.rs"]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
    for dir in [
        "src/lower/novarocks/fragment",
        "src/lower/novarocks/node",
        "src/lower/novarocks/scan",
        "src/lower/novarocks/sink",
    ] {
        native_lowering_sources
            .extend(rs_files(&repo.join(dir)).into_iter().map(|path| rel(&path)));
    }
    for source in native_lowering_sources {
        let text = fs::read_to_string(repo.join(&source)).unwrap();
        let text = rust_production_text_without_cfg_test(&text);
        push_forbidden_terms(
            &mut violations,
            &source,
            &text,
            &[
                "crate::thrift",
                "thrift::",
                "TPlanFragment",
                "TPlanNode",
                "TDataSink",
            ],
            "native lowering must not take thrift as input contract",
        );
    }

    for path in rs_files(&repo.join("src/sql/codegen/proto_encode")) {
        let source = rel(&path);
        let text = fs::read_to_string(&path).unwrap();
        let text = rust_production_text_without_cfg_test(&text);
        push_forbidden_terms(
            &mut violations,
            &source,
            &text,
            &[
                "crate::thrift::partitions::TDataPartition::new",
                "crate::thrift::data_sinks::TDataSink",
                "crate::thrift::plan_nodes::TPlan",
            ],
            "native proto encoder must not construct compat thrift artifacts",
        );
    }

    let compat_allowlist = [
        ("src/runtime/query_options.rs", &["from_thrift"][..]),
        ("src/runtime/runtime_filter_params.rs", &["from_thrift"][..]),
    ];
    for (source, markers) in compat_allowlist {
        let text = fs::read_to_string(repo.join(source)).unwrap();
        let production_text = rust_production_text_without_cfg_test(&text);
        for marker in markers {
            if !production_text.contains(marker) {
                violations.push(format!(
                    "{source}: compat allowlist must contain `{marker}`"
                ));
            }
        }
    }
    let scan_range = fs::read_to_string(repo.join("src/runtime/scan_range.rs")).unwrap();
    let scan_range = rust_production_text_without_cfg_test(&scan_range);
    if scan_range.contains("thrift_scan_range_map_from_native") {
        violations.push(
            "src/runtime/scan_range.rs: retired bulk native-to-Thrift scan-range projection must remain absent"
                .to_string(),
        );
    }
    for source in [
        "src/runtime/query_options.rs",
        "src/runtime/runtime_filter_params.rs",
    ] {
        let text = fs::read_to_string(repo.join(source)).unwrap();
        let production_text = rust_production_text_without_cfg_test(&text);
        if production_text.contains("fn to_thrift") {
            violations.push(format!(
                "{source}: native runtime contract must not project back into Thrift"
            ));
        }
    }

    assert!(
        violations.is_empty(),
        "D3L native mainline thrift usage guard failed:\n{}",
        violations.join("\n")
    );
}

fn nidl_d3b_baseline_update_hint() -> String {
    format!(
        "To intentionally update the proto schema ledger, run:\n{}",
        NIDL_D3B_WRITE_BASELINE_COMMAND
    )
}

#[test]
fn nidl_d3b_proto_schema_write_mode_rejects_missing_baseline_without_bootstrap() {
    let missing_path = std::env::temp_dir().join(format!(
        "novarocks-missing-proto-schema-baseline-{}.json",
        std::process::id()
    ));
    fs::remove_file(&missing_path).ok();
    let current = test_proto_schema(vec![], vec![], vec![]);

    let err = next_proto_schema_baseline_for_write(&current, &missing_path)
        .expect_err("write mode should reject a missing baseline");

    assert!(err.contains("proto schema baseline is missing"), "{err}");
    assert!(err.contains(NIDL_D3B_WRITE_BASELINE_ENV), "{err}");
    assert!(
        !missing_path.exists(),
        "missing-baseline decision test must not write a real baseline"
    );
}

#[test]
fn nidl_d3b_proto_schema_parser_reports_unclosed_context_statement() {
    let err = parse_proto_schema(
        "idl/novarocks/broken.proto",
        r#"
        syntax = "proto3";
        package novarocks.broken;
        message Broken {
          string value = 1;
        "#,
    )
    .expect_err("unclosed message should fail");

    assert!(err.contains("idl/novarocks/broken.proto"), "{err}");
    assert!(err.contains("string value = 1;"), "{err}");
    assert!(err.contains("message Broken"), "{err}");
}

#[test]
fn nidl_d3b_proto_schema_parser_rejects_unsupported_tails_and_bad_identifiers() {
    for (name, input, expected_statement) in [
        (
            "field-tail",
            r#"
            syntax = "proto3";
            package novarocks.bad;
            message Bad {
              string x = 1 unexpected;
            }
            "#,
            "string x = 1 unexpected;",
        ),
        (
            "enum-tail",
            r#"
            syntax = "proto3";
            package novarocks.bad;
            enum Bad {
              FOO = 1 alias;
            }
            "#,
            "FOO = 1 alias;",
        ),
        (
            "message-digit-start",
            r#"
            syntax = "proto3";
            package novarocks.bad;
            message 1Bad {
            }
            "#,
            "message 1Bad {",
        ),
        (
            "field-digit-start",
            r#"
            syntax = "proto3";
            package novarocks.bad;
            message Bad {
              string 1x = 1;
            }
            "#,
            "string 1x = 1;",
        ),
        (
            "field-bad-continue",
            r#"
            syntax = "proto3";
            package novarocks.bad;
            message Bad {
              string x-y = 1;
            }
            "#,
            "string x-y = 1;",
        ),
        (
            "enum-value-digit-start",
            r#"
            syntax = "proto3";
            package novarocks.bad;
            enum Bad {
              1FOO = 1;
            }
            "#,
            "1FOO = 1;",
        ),
        (
            "service-digit-start",
            r#"
            syntax = "proto3";
            package novarocks.bad;
            service 1Bad {
            }
            "#,
            "service 1Bad {",
        ),
        (
            "oneof-digit-start",
            r#"
            syntax = "proto3";
            package novarocks.bad;
            message Bad {
              oneof 1kind {
              }
            }
            "#,
            "oneof 1kind {",
        ),
    ] {
        let err = match parse_proto_schema("idl/novarocks/bad.proto", input) {
            Ok(_) => panic!("{name} should fail"),
            Err(err) => err,
        };
        assert!(err.contains("idl/novarocks/bad.proto"), "{name}: {err}");
        assert!(err.contains(expected_statement), "{name}: {err}");
    }
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_version_drift() {
    let mut baseline = test_proto_schema(vec![], vec![], vec![]);
    let mut current = baseline.clone();
    baseline.version = 0;
    current.version = 2;

    assert_proto_schema_comparator_rejects_all(
        current,
        baseline,
        &[
            "current proto schema version must be 1",
            "baseline proto schema version must be 1",
        ],
    );
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_package_drift() {
    let baseline = test_proto_schema_with_files(vec![(
        "idl/novarocks/service.proto",
        test_proto_file("novarocks.baseline", vec![], vec![], vec![]),
    )]);
    let current = test_proto_schema_with_files(vec![(
        "idl/novarocks/service.proto",
        test_proto_file("novarocks.current", vec![], vec![], vec![]),
    )]);

    assert_proto_schema_comparator_rejects(
        current,
        baseline,
        "package changed from novarocks.baseline to novarocks.current",
    );
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_baseline_file_missing_in_current() {
    let baseline = test_proto_schema_with_files(vec![(
        "idl/novarocks/service.proto",
        test_proto_file("novarocks.test", vec![], vec![], vec![]),
    )]);
    let current = test_proto_schema_with_files(vec![]);

    assert_proto_schema_comparator_rejects(current, baseline, "file removed");
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_current_new_file_as_baseline_stale() {
    let baseline = test_proto_schema_with_files(vec![]);
    let current = test_proto_schema_with_files(vec![(
        "idl/novarocks/new.proto",
        test_proto_file("novarocks.test", vec![], vec![], vec![]),
    )]);

    assert_proto_schema_comparator_rejects(current, baseline, "new file is missing from baseline");
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_new_file_with_service_as_unsafe() {
    let baseline = test_proto_schema_with_files(vec![]);
    let current = test_proto_schema_with_files(vec![(
        "idl/novarocks/admin.proto",
        test_proto_file(
            "novarocks.admin",
            vec![],
            vec![],
            vec![(
                "AdminGrpc",
                test_proto_service(vec![(
                    "Reload",
                    test_proto_rpc("ReloadRequest", "ReloadResponse"),
                )]),
            )],
        ),
    )]);

    assert_proto_schema_comparator_rejects_all(
        current,
        baseline,
        &[
            "new file is missing from baseline",
            "service AdminGrpc new service is not allowed",
        ],
    );
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_baseline_message_missing_in_current() {
    let baseline = test_proto_schema(
        vec![("SubmitFragmentRequest", test_proto_message(vec![]))],
        vec![],
        vec![],
    );
    let current = test_proto_schema(vec![], vec![], vec![]);

    assert_proto_schema_comparator_rejects(
        current,
        baseline,
        "message SubmitFragmentRequest removed",
    );
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_current_new_message_as_baseline_stale() {
    let baseline = test_proto_schema(vec![], vec![], vec![]);
    let current = test_proto_schema(
        vec![("SubmitFragmentRequest", test_proto_message(vec![]))],
        vec![],
        vec![],
    );

    assert_proto_schema_comparator_rejects(
        current,
        baseline,
        "new message is missing from baseline",
    );
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_current_new_field_as_baseline_stale() {
    let baseline = test_proto_schema(
        vec![("SubmitFragmentRequest", test_proto_message(vec![]))],
        vec![],
        vec![],
    );
    let current = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message(vec![test_proto_field(3, "fragment_plan", "bytes")]),
        )],
        vec![],
        vec![],
    );

    assert_proto_schema_comparator_rejects(current, baseline, "new field is missing from baseline");
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_field_label_drift() {
    let baseline = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message(vec![test_proto_field(2, "plan", "PlanNode")]),
        )],
        vec![],
        vec![],
    );
    let current = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message(vec![test_proto_field_with_label(
                2, "plan", "PlanNode", "repeated",
            )]),
        )],
        vec![],
        vec![],
    );

    assert_proto_schema_comparator_rejects(current, baseline, "field label change");
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_field_oneof_drift() {
    let baseline = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message(vec![test_proto_field(2, "plan", "PlanNode")]),
        )],
        vec![],
        vec![],
    );
    let current = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message(vec![test_proto_field_with_oneof(
                2, "plan", "PlanNode", "payload",
            )]),
        )],
        vec![],
        vec![],
    );

    assert_proto_schema_comparator_rejects(current, baseline, "field oneof change");
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_missing_message_reserved_retention() {
    let baseline = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message_with_reserved(vec![], &[7], &["old_plan"]),
        )],
        vec![],
        vec![],
    );
    let current = test_proto_schema(
        vec![("SubmitFragmentRequest", test_proto_message(vec![]))],
        vec![],
        vec![],
    );

    assert_proto_schema_comparator_rejects_all(
        current,
        baseline,
        &[
            "reserved number 7 removed from current schema",
            "reserved name old_plan removed from current schema",
        ],
    );
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_field_reusing_baseline_reserved_number_or_name() {
    let baseline = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message_with_reserved(vec![], &[7], &["old_plan"]),
        )],
        vec![],
        vec![],
    );
    let current = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message_with_reserved(
                vec![
                    test_proto_field(7, "fragment_plan", "bytes"),
                    test_proto_field(8, "old_plan", "bytes"),
                ],
                &[7],
                &["old_plan"],
            ),
        )],
        vec![],
        vec![],
    );

    assert_proto_schema_comparator_rejects_all(
        current,
        baseline,
        &[
            "uses baseline reserved number 7",
            "uses baseline reserved name old_plan",
        ],
    );
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_field_type_change() {
    let baseline = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message(vec![test_proto_field(2, "plan", "PlanNode")]),
        )],
        vec![],
        vec![],
    );
    let current = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message(vec![test_proto_field(2, "plan", "PlanFragment")]),
        )],
        vec![],
        vec![],
    );

    assert_proto_schema_comparator_rejects(current, baseline, "field type change");
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_field_rename() {
    let baseline = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message(vec![test_proto_field(2, "plan", "PlanNode")]),
        )],
        vec![],
        vec![],
    );
    let current = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message(vec![test_proto_field(2, "query_plan", "PlanNode")]),
        )],
        vec![],
        vec![],
    );

    assert_proto_schema_comparator_rejects(current, baseline, "field rename");
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_field_number_reuse() {
    let baseline = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message(vec![test_proto_field(2, "plan", "PlanNode")]),
        )],
        vec![],
        vec![],
    );
    let current = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message(vec![test_proto_field(2, "scan_node", "ScanNode")]),
        )],
        vec![],
        vec![],
    );

    assert_proto_schema_comparator_rejects(current, baseline, "field number reuse");
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_deleted_field_without_reserved_number() {
    let baseline = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message(vec![test_proto_field(2, "plan", "PlanNode")]),
        )],
        vec![],
        vec![],
    );
    let current = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message_with_reserved(vec![], &[], &["plan"]),
        )],
        vec![],
        vec![],
    );

    assert_proto_schema_comparator_rejects(current, baseline, "reserved number");
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_deleted_field_without_reserved_name() {
    let baseline = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message(vec![test_proto_field(2, "plan", "PlanNode")]),
        )],
        vec![],
        vec![],
    );
    let current = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message_with_reserved(vec![], &[2], &[]),
        )],
        vec![],
        vec![],
    );

    assert_proto_schema_comparator_rejects(current, baseline, "reserved name");
}

#[test]
fn nidl_d3b_proto_schema_comparator_accepts_deleted_field_with_reserved_number_and_name() {
    let baseline = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message(vec![test_proto_field(2, "plan", "PlanNode")]),
        )],
        vec![],
        vec![],
    );
    let current = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message_with_reserved(vec![], &[2], &["plan"]),
        )],
        vec![],
        vec![],
    );

    assert_proto_schema_comparator_accepts(current, baseline);
}

#[test]
fn nidl_d3b_proto_schema_baseline_merge_preserves_reserved_deleted_field_history() {
    let existing = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message(vec![test_proto_field(2, "plan", "PlanNode")]),
        )],
        vec![],
        vec![],
    );
    let current = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message_with_reserved(
                vec![test_proto_field(3, "fragment_plan", "bytes")],
                &[2],
                &["plan"],
            ),
        )],
        vec![],
        vec![],
    );

    let merged =
        merge_proto_schema_baseline(&current, &existing).expect("baseline merge should succeed");
    let merged_message =
        &merged.files["idl/novarocks/test.proto"].messages["SubmitFragmentRequest"];

    assert_eq!(merged_message.fields[&2].name, "plan");
    assert_eq!(merged_message.fields[&3].name, "fragment_plan");
    assert_proto_schema_comparator_accepts(current, merged);
}

#[test]
fn nidl_d3b_proto_schema_baseline_merge_rejects_deleted_field_without_reserved_name() {
    let existing = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message(vec![test_proto_field(2, "plan", "PlanNode")]),
        )],
        vec![],
        vec![],
    );
    let current = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message_with_reserved(vec![], &[2], &[]),
        )],
        vec![],
        vec![],
    );

    assert_proto_schema_baseline_merge_rejects(current, existing, "without reserved name plan");
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_enum_zero_value_drift() {
    let baseline = test_proto_schema(
        vec![],
        vec![(
            "FetchResultResponse.Status",
            test_proto_enum(vec![(0, "STATUS_UNSPECIFIED"), (1, "OK")]),
        )],
        vec![],
    );
    let current = test_proto_schema(
        vec![],
        vec![(
            "FetchResultResponse.Status",
            test_proto_enum(vec![(0, "STATUS_UNKNOWN"), (1, "OK")]),
        )],
        vec![],
    );

    assert_proto_schema_comparator_rejects(current, baseline, "enum zero value");
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_rpc_signature_change() {
    let baseline = test_proto_schema(
        vec![],
        vec![],
        vec![(
            "NovaRocksGrpc",
            test_proto_service(vec![(
                "FetchResult",
                test_proto_rpc("FetchResultRequest", "FetchResultResponse"),
            )]),
        )],
    );
    let current = test_proto_schema(
        vec![],
        vec![],
        vec![(
            "NovaRocksGrpc",
            test_proto_service(vec![(
                "FetchResult",
                test_proto_rpc("FetchResultRequestV2", "FetchResultResponse"),
            )]),
        )],
    );

    assert_proto_schema_comparator_rejects(current, baseline, "rpc signature");
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_new_service() {
    let baseline = test_proto_schema(
        vec![],
        vec![],
        vec![(
            "NovaRocksGrpc",
            test_proto_service(vec![(
                "FetchResult",
                test_proto_rpc("FetchResultRequest", "FetchResultResponse"),
            )]),
        )],
    );
    let current = test_proto_schema(
        vec![],
        vec![],
        vec![
            (
                "NovaRocksGrpc",
                test_proto_service(vec![(
                    "FetchResult",
                    test_proto_rpc("FetchResultRequest", "FetchResultResponse"),
                )]),
            ),
            (
                "AdminGrpc",
                test_proto_service(vec![(
                    "Reload",
                    test_proto_rpc("ReloadRequest", "ReloadResponse"),
                )]),
            ),
        ],
    );

    assert_proto_schema_comparator_rejects(current, baseline, "new service");
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_enum_deletion() {
    let baseline = test_proto_schema(
        vec![],
        vec![(
            "FetchResultResponse.Status",
            test_proto_enum(vec![(0, "STATUS_UNSPECIFIED"), (1, "STATUS_OK")]),
        )],
        vec![],
    );
    let current = test_proto_schema(vec![], vec![], vec![]);

    assert_proto_schema_comparator_rejects(
        current,
        baseline,
        "enum FetchResultResponse.Status removed",
    );
}

#[test]
fn nidl_d3b_proto_schema_baseline_merge_rejects_enum_value_deletion() {
    let existing = test_proto_schema(
        vec![],
        vec![(
            "FetchResultResponse.Status",
            test_proto_enum(vec![(0, "STATUS_UNSPECIFIED"), (1, "STATUS_OK")]),
        )],
        vec![],
    );
    let current = test_proto_schema(
        vec![],
        vec![(
            "FetchResultResponse.Status",
            test_proto_enum(vec![(0, "STATUS_UNSPECIFIED")]),
        )],
        vec![],
    );

    assert_proto_schema_baseline_merge_rejects(current, existing, "value STATUS_OK=1 removed");
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_enum_renumber() {
    let baseline = test_proto_schema(
        vec![],
        vec![(
            "FetchResultResponse.Status",
            test_proto_enum(vec![(0, "STATUS_UNSPECIFIED"), (1, "STATUS_OK")]),
        )],
        vec![],
    );
    let current = test_proto_schema(
        vec![],
        vec![(
            "FetchResultResponse.Status",
            test_proto_enum(vec![(0, "STATUS_UNSPECIFIED"), (2, "STATUS_OK")]),
        )],
        vec![],
    );

    assert_proto_schema_comparator_rejects(current, baseline, "renumbered from #1 to #2");
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_enum_rename() {
    let baseline = test_proto_schema(
        vec![],
        vec![(
            "FetchResultResponse.Status",
            test_proto_enum(vec![(0, "STATUS_UNSPECIFIED"), (1, "STATUS_OK")]),
        )],
        vec![],
    );
    let current = test_proto_schema(
        vec![],
        vec![(
            "FetchResultResponse.Status",
            test_proto_enum(vec![(0, "STATUS_UNSPECIFIED"), (1, "STATUS_DONE")]),
        )],
        vec![],
    );

    assert_proto_schema_comparator_rejects(
        current,
        baseline,
        "renamed from STATUS_OK to STATUS_DONE",
    );
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_current_new_enum_value_as_baseline_stale() {
    let baseline = test_proto_schema(
        vec![],
        vec![(
            "FetchResultResponse.Status",
            test_proto_enum(vec![(0, "STATUS_UNSPECIFIED")]),
        )],
        vec![],
    );
    let current = test_proto_schema(
        vec![],
        vec![(
            "FetchResultResponse.Status",
            test_proto_enum(vec![(0, "STATUS_UNSPECIFIED"), (1, "STATUS_OK")]),
        )],
        vec![],
    );

    assert_proto_schema_comparator_rejects(
        current,
        baseline,
        "new enum value is missing from baseline",
    );
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_enum_reserved_retention_or_reuse() {
    let baseline = test_proto_schema(
        vec![],
        vec![(
            "FetchResultResponse.Status",
            test_proto_enum_with_reserved(vec![(0, "STATUS_UNSPECIFIED")], &[2], &["STATUS_OLD"]),
        )],
        vec![],
    );
    let current = test_proto_schema(
        vec![],
        vec![(
            "FetchResultResponse.Status",
            test_proto_enum(vec![
                (0, "STATUS_UNSPECIFIED"),
                (2, "STATUS_REUSED_NUMBER"),
                (3, "STATUS_OLD"),
            ]),
        )],
        vec![],
    );

    assert_proto_schema_comparator_rejects_all(
        current,
        baseline,
        &[
            "reserved number 2 removed from current schema",
            "reserved name STATUS_OLD removed from current schema",
            "uses baseline reserved number 2",
            "uses baseline reserved name STATUS_OLD",
        ],
    );
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_rpc_deletion() {
    let baseline = test_proto_schema(
        vec![],
        vec![],
        vec![(
            "NovaRocksGrpc",
            test_proto_service(vec![(
                "FetchResult",
                test_proto_rpc("FetchResultRequest", "FetchResultResponse"),
            )]),
        )],
    );
    let current = test_proto_schema(
        vec![],
        vec![],
        vec![("NovaRocksGrpc", test_proto_service(vec![]))],
    );

    assert_proto_schema_comparator_rejects(current, baseline, "rpc FetchResult removed");
}

#[test]
fn nidl_d3b_proto_schema_baseline_merge_rejects_service_deletion() {
    let existing = test_proto_schema(
        vec![],
        vec![],
        vec![(
            "NovaRocksGrpc",
            test_proto_service(vec![(
                "FetchResult",
                test_proto_rpc("FetchResultRequest", "FetchResultResponse"),
            )]),
        )],
    );
    let current = test_proto_schema(vec![], vec![], vec![]);

    assert_proto_schema_baseline_merge_rejects(current, existing, "service NovaRocksGrpc removed");
}

#[test]
fn nidl_d3b_proto_schema_baseline_merge_rejects_rpc_deletion() {
    let existing = test_proto_schema(
        vec![],
        vec![],
        vec![(
            "NovaRocksGrpc",
            test_proto_service(vec![(
                "FetchResult",
                test_proto_rpc("FetchResultRequest", "FetchResultResponse"),
            )]),
        )],
    );
    let current = test_proto_schema(
        vec![],
        vec![],
        vec![("NovaRocksGrpc", test_proto_service(vec![]))],
    );

    assert_proto_schema_baseline_merge_rejects(current, existing, "rpc FetchResult removed");
}

#[test]
fn nidl_d3b_proto_schema_baseline_merge_rejects_new_file_with_service() {
    let existing = test_proto_schema_with_files(vec![]);
    let current = test_proto_schema_with_files(vec![(
        "idl/novarocks/admin.proto",
        test_proto_file(
            "novarocks.admin",
            vec![],
            vec![],
            vec![(
                "AdminGrpc",
                test_proto_service(vec![(
                    "Reload",
                    test_proto_rpc("ReloadRequest", "ReloadResponse"),
                )]),
            )],
        ),
    )]);

    assert_proto_schema_baseline_merge_rejects(
        current,
        existing,
        "service AdminGrpc new service is not allowed",
    );
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_current_new_rpc_as_baseline_stale() {
    let baseline = test_proto_schema(
        vec![],
        vec![],
        vec![("NovaRocksGrpc", test_proto_service(vec![]))],
    );
    let current = test_proto_schema(
        vec![],
        vec![],
        vec![(
            "NovaRocksGrpc",
            test_proto_service(vec![(
                "FetchResult",
                test_proto_rpc("FetchResultRequest", "FetchResultResponse"),
            )]),
        )],
    );

    assert_proto_schema_comparator_rejects(current, baseline, "new rpc is missing from baseline");
}

#[test]
fn nidl_d3b_proto_schema_comparator_returns_stable_sorted_deduped_violations() {
    let mut baseline = test_proto_schema(
        vec![],
        vec![(
            "FetchResultResponse.Status",
            test_proto_enum(vec![(0, "STATUS_UNSPECIFIED")]),
        )],
        vec![],
    );
    let mut current = test_proto_schema(
        vec![],
        vec![(
            "FetchResultResponse.Status",
            test_proto_enum(vec![
                (0, "STATUS_UNSPECIFIED"),
                (1, "STATUS_DUP"),
                (1, "STATUS_DUP"),
            ]),
        )],
        vec![],
    );
    baseline.version = 0;
    current.version = 2;

    let violations = compare_proto_schema_to_baseline(&current, &baseline);
    let mut sorted_deduped = violations.clone();
    sorted_deduped.sort();
    sorted_deduped.dedup();

    assert_eq!(violations, sorted_deduped);
    assert_eq!(
        violations
            .iter()
            .filter(|violation| violation.contains("STATUS_DUP=1 baseline stale"))
            .count(),
        1,
        "expected duplicate enum-value violations to be deduped, got: {violations:?}"
    );
    assert!(
        violations[0].starts_with("baseline proto schema version must be 1"),
        "expected sorted output, got: {violations:?}"
    );
}

// ---------------------------------------------------------------------------
// NIDL-E0: non-compat StarRocks-IDL ledger guard
// ---------------------------------------------------------------------------
//
// Goal (see specs/2026-07-07-nidl-e0-noncompat-idl-ledger-guard-design):
// the non-compat compile graph must eventually contain zero references to
// StarRocks IDL (`crate::thrift`, `crate::proto::starrocks`,
// `crate::proto::staros`). This guard is a conservative lexical scan: it strips
// test-only items and directly compat-only items, but keeps ambiguous cfg
// expressions scanned. It accounts for the current production-code references
// via a shrink-only ledger; milestones E1..E9 remove ledger entries as clusters
// are cleaned, and E10 empties the ledger and adds the build.rs / lib.rs gate
// assertions.

#[test]
fn nidl_e7_result_path_uses_native_result_batch_and_primitive_types() {
    let repo = Path::new(manifest_dir());
    let guarded = [
        "src/common/types.rs",
        "src/common/util.rs",
        "src/runtime/result_buffer.rs",
        "src/service/result_batch_wire.rs",
        "src/exec/operators/result_buffer_sink.rs",
    ];
    let mut violations = Vec::new();

    for source in guarded {
        let text = fs::read_to_string(repo.join(source)).unwrap();
        let production = rust_production_text_without_cfg_test(&text);
        push_forbidden_terms(
            &mut violations,
            source,
            &production,
            &[
                "crate::thrift",
                "TResultBatch",
                "TPrimitiveType",
                "TResultSinkType",
                "TResultSinkFormatType",
                "exprs::TExpr",
                "data_sinks::",
                "types::T",
                "crate::types::arrow_thrift",
            ],
            "E7 result execution path must use native result batch, primitive tags, and sink config",
        );
    }

    let native_fragment_wire =
        fs::read_to_string(repo.join("src/runtime/native_fragment_wire.rs")).unwrap();
    let native_fragment_wire = rust_production_text_without_cfg_test(&native_fragment_wire);
    push_forbidden_terms(
        &mut violations,
        "src/runtime/native_fragment_wire.rs",
        &native_fragment_wire,
        &[
            "pub(crate) type ResultSinkType =",
            "TResultSinkType",
            "TResultSinkFormatType",
        ],
        "E7 native fragment wire must not expose thrift result-sink aliases",
    );

    let arrow_thrift = fs::read_to_string(repo.join("src/types/arrow_thrift.rs")).unwrap();
    let arrow_thrift = rust_production_text_without_cfg_test(&arrow_thrift);
    push_forbidden_terms(
        &mut violations,
        "src/types/arrow_thrift.rs",
        &arrow_thrift,
        &[
            "fn logical_type_to_primitive",
            "fn field_logical_primitive",
            "fn arrow_field_to_primitive",
            "fn arrow_type_to_primitive",
            "fn thrift_node_to_primitive",
            "fn thrift_desc_to_primitive",
        ],
        "Arrow/native primitive helpers must live outside thrift type descriptors",
    );

    let common_thrift = fs::read_to_string(repo.join("src/common/thrift.rs")).unwrap();
    let common_thrift = rust_production_text_without_cfg_test(&common_thrift);
    push_forbidden_terms(
        &mut violations,
        "src/common/thrift.rs",
        &common_thrift,
        &[
            "crate::thrift::data",
            "TResultBatch",
            "thrift_serialize_result_batch",
        ],
        "generic thrift helpers must not know the result-batch runtime model",
    );

    assert!(
        violations.is_empty(),
        "NIDL-E7 native result-batch/primitive guard failed:\n{}",
        violations.join("\n")
    );
}

const NIDL_E0_LEDGER_PATH: &str = "tests/nidl_noncompat_idl_ledger.txt";

/// Files/prefixes already gated to `#[cfg(feature = "compat")]` (or expected to
/// be, and verified elsewhere). The lexical scan skips these so the ledger only
/// tracks the non-compat mainline. Milestones E2/E9 append entries here as
/// modules are gated (e.g. "src/lower/compat").
const NIDL_E0_COMPAT_SCOPE: &[&str] = &[
    "src/connector/iceberg/file_pruning_wire.rs",
    "src/connector/starrocks",
    "src/connector/schema/fe_tables.rs",
    "src/connector/schema/frontend.rs",
    "src/connector/schema/load_tracking_logs.rs",
    "src/connector/schema/loads.rs",
    "src/formats/starrocks",
    "src/exec/chunk/schema_thrift.rs",
    "src/exec/node/fetch.rs",
    "src/exec/operators/fetch_processor.rs",
    "src/lower/compat",
    "src/runtime/descriptor_snapshot_thrift.rs",
    "src/runtime/sink_commit_wire.rs",
    "src/runtime/write_coordinator_compat.rs",
    "src/service/backend_service.rs",
    "src/service/heartbeat_service.rs",
    "src/service/internal_service.rs",
    "src/service/internal_rpc_client.rs",
    "src/service/stream_load.rs",
    "src/service/stream_load_http.rs",
    "src/service/engine_ffi.rs",
    "src/service/compat.rs",
    "src/service/disk_report.rs",
    "src/service/exec_state_reporter.rs",
    "src/service/exec_status_report.rs",
    "src/service/fe_report_compat.rs",
    "src/service/frontend_rpc.rs",
    "src/service/stream_load_registry.rs",
    "src/types/arrow_thrift.rs",
];

fn nidl_e0_starrocks_idl_terms() -> &'static [&'static str] {
    &[
        "crate::thrift",
        "crate::proto::starrocks",
        "crate::proto::staros",
    ]
}

fn nidl_e0_is_in_compat_scope(rel_path: &str) -> bool {
    NIDL_E0_COMPAT_SCOPE
        .iter()
        .any(|prefix| rel_path == *prefix || rel_path.starts_with(&format!("{prefix}/")))
}

/// Collect `.rs` files under `dir` whose production code (test modules and
/// direct compat-only items stripped) references any StarRocks IDL term. Returned
/// sorted; no compat-scope filtering (that is applied by
/// `nidl_e0_current_offenders`).
fn nidl_e0_offenders_in(dir: &Path) -> Vec<PathBuf> {
    let mut offenders = Vec::new();
    for path in rs_files(dir) {
        let text = fs::read_to_string(&path).unwrap_or_default();
        let production = rust_production_text_without_cfg_test_or_compat(&text);
        let has_hit = production.lines().any(|line| {
            !is_comment_or_blank(line)
                && nidl_e0_starrocks_idl_terms()
                    .iter()
                    .any(|term| line.contains(*term))
        });
        if has_hit {
            offenders.push(path);
        }
    }
    offenders.sort();
    offenders
}

/// Repo-relative paths of non-compat production files still referencing
/// StarRocks IDL (compat-scope excluded).
fn nidl_e0_current_offenders() -> Vec<String> {
    let mut out = Vec::new();
    for path in nidl_e0_offenders_in(&src_dir()) {
        let rel_path = rel(&path);
        if nidl_e0_is_in_compat_scope(&rel_path) {
            continue;
        }
        out.push(rel_path);
    }
    out.sort();
    out.dedup();
    out
}

fn nidl_e0_read_ledger() -> Vec<String> {
    let path = Path::new(manifest_dir()).join(NIDL_E0_LEDGER_PATH);
    let text = fs::read_to_string(&path).unwrap_or_default();
    let mut entries: Vec<String> = text
        .lines()
        .map(|line| line.trim().to_string())
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect();
    entries.sort();
    entries.dedup();
    entries
}

#[test]
fn nidl_e0_detector_flags_starrocks_idl_and_ignores_native_and_tests() {
    let dir = std::env::temp_dir().join("nidl_e0_detector");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join("offender_thrift.rs"),
        "use crate::thrift::types::TUniqueId;\n",
    )
    .unwrap();
    fs::write(
        dir.join("offender_proto.rs"),
        "let _ = crate::proto::starrocks::StatusPb::default();\n",
    )
    .unwrap();
    fs::write(
        dir.join("native.rs"),
        "use crate::proto::plan::PlanFragment;\n",
    )
    .unwrap();
    fs::write(
        dir.join("test_only.rs"),
        "#[cfg(test)]\nmod tests {\n    use crate::thrift::types::TUniqueId;\n}\n",
    )
    .unwrap();

    let offenders = nidl_e0_offenders_in(&dir);
    let names: Vec<String> = offenders
        .iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
        .collect();

    assert!(
        names.iter().any(|n| n == "offender_thrift.rs"),
        "must flag crate::thrift; got {names:?}"
    );
    assert!(
        names.iter().any(|n| n == "offender_proto.rs"),
        "must flag crate::proto::starrocks; got {names:?}"
    );
    assert!(
        !names.iter().any(|n| n == "native.rs"),
        "must ignore native crate::proto::plan; got {names:?}"
    );
    assert!(
        !names.iter().any(|n| n == "test_only.rs"),
        "must ignore #[cfg(test)] module references; got {names:?}"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn nidl_e6_ledger_detector_ignores_compat_cfg_items() {
    let dir = std::env::temp_dir().join("nidl_e6_detector");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join("compat_fn.rs"),
        "#[cfg(feature = \"compat\")]\nfn compat_only() {\n    let _ = crate::thrift::types::TUniqueId::new(1, 2);\n}\n",
    )
    .unwrap();
    fs::write(
        dir.join("compat_multiline_fn.rs"),
        "#[cfg(feature = \"compat\")]\nfn compat_multiline(\n    _id: i32,\n) -> crate::thrift::types::TUniqueId {\n    crate::thrift::types::TUniqueId::new(1, 2)\n}\n",
    )
    .unwrap();
    fs::write(
        dir.join("compat_mod.rs"),
        "#[cfg(feature = \"compat\")]\nmod compat_only {\n    use crate::proto::starrocks;\n}\n",
    )
    .unwrap();
    fs::write(
        dir.join("offender.rs"),
        "fn default_build_offender() {\n    let _ = crate::thrift::types::TUniqueId::new(3, 4);\n}\n",
    )
    .unwrap();
    fs::write(
        dir.join("compat_not.rs"),
        "#[cfg(not(feature = \"compat\"))]\nfn non_compat_offender() {\n    let _ = crate::thrift::types::TUniqueId::new(7, 8);\n}\n",
    )
    .unwrap();
    fs::write(
        dir.join("compat_any.rs"),
        "#[cfg(any(feature = \"compat\", unix))]\nfn maybe_default_offender() {\n    let _ = crate::thrift::types::TUniqueId::new(9, 10);\n}\n",
    )
    .unwrap();
    fs::write(
        dir.join("comma_then_offender.rs"),
        "enum Demo {\n    #[cfg(feature = \"compat\")]\n    CompatVariant(crate::thrift::types::TUniqueId),\n}\nfn offender_after_comma() {\n    let _ = crate::thrift::types::TUniqueId::new(5, 6);\n}\n",
    )
    .unwrap();

    let offenders = nidl_e0_offenders_in(&dir);
    let names: Vec<String> = offenders
        .iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
        .collect();

    assert!(
        names.iter().any(|n| n == "offender.rs"),
        "must keep default-build offenders; got {names:?}"
    );
    assert!(
        names.iter().any(|n| n == "compat_not.rs"),
        "must keep not(feature = \"compat\") offenders; got {names:?}"
    );
    assert!(
        names.iter().any(|n| n == "compat_any.rs"),
        "must keep ambiguous any(feature = \"compat\", ...) offenders; got {names:?}"
    );
    assert!(
        names.iter().any(|n| n == "comma_then_offender.rs"),
        "must keep default-build offenders after compat comma items; got {names:?}"
    );
    assert!(
        !names.iter().any(|n| n == "compat_fn.rs"),
        "must ignore compat-only functions; got {names:?}"
    );
    assert!(
        !names.iter().any(|n| n == "compat_multiline_fn.rs"),
        "must ignore multiline compat-only functions; got {names:?}"
    );
    assert!(
        !names.iter().any(|n| n == "compat_mod.rs"),
        "must ignore compat-only modules; got {names:?}"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn nidl_e0_noncompat_starrocks_idl_stays_within_ledger() {
    let offenders = nidl_e0_current_offenders();
    let ledger = nidl_e0_read_ledger();

    assert!(
        ledger.is_empty(),
        "NIDL-E10 final ledger must stay empty; remove these stale entries:\n{}",
        ledger.join("\n")
    );
    assert!(
        offenders.is_empty(),
        "NIDL-E10 non-compat production code must not reference StarRocks IDL:\n{}",
        offenders.join("\n")
    );
}

fn nidl_e9_module_has_compat_cfg(module_file: &Path, module_name: &str) -> bool {
    let Ok(text) = fs::read_to_string(module_file) else {
        return false;
    };
    let mut previous_non_blank = "";
    let mut in_block_comment = false;
    let target = format!("mod {module_name};");
    for line in text.lines() {
        let trimmed = line.trim();
        if nidl_e9_is_comment_or_blank_line(trimmed, &mut in_block_comment) {
            continue;
        }
        if trimmed == target
            || trimmed == format!("pub(crate) mod {module_name};")
            || trimmed == format!("pub mod {module_name};")
        {
            return previous_non_blank == "#[cfg(feature = \"compat\")]";
        }
        previous_non_blank = trimmed;
    }
    false
}

fn nidl_e9_file_is_cfg_compat_module(path: &Path) -> bool {
    let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    let Some(parent) = path.parent() else {
        return false;
    };

    if file_name == "mod.rs" {
        let Some(module_name) = parent.file_name().and_then(|name| name.to_str()) else {
            return false;
        };
        let Some(parent_parent) = parent.parent() else {
            return false;
        };
        return nidl_e9_module_has_compat_cfg(&parent_parent.join("mod.rs"), module_name);
    }

    let Some(module_name) = path.file_stem().and_then(|name| name.to_str()) else {
        return false;
    };
    nidl_e9_module_has_compat_cfg(&parent.join("mod.rs"), module_name)
}

fn nidl_e9_is_comment_or_blank_line(trimmed: &str, in_block_comment: &mut bool) -> bool {
    if *in_block_comment {
        if trimmed.contains("*/") {
            *in_block_comment = false;
        }
        return true;
    }

    if trimmed.is_empty() || trimmed.starts_with("//") || trimmed.starts_with('*') {
        return true;
    }

    if trimmed.starts_with("/*") {
        if !trimmed.contains("*/") {
            *in_block_comment = true;
        }
        return true;
    }

    false
}

fn nidl_e9_noncompat_lower_compat_import_hits_in(root: &Path) -> Vec<String> {
    let mut hits = Vec::new();
    for path in rs_files(root) {
        let rel_path = rel(&path);
        if nidl_e9_is_lower_compat_scope(&rel_path) {
            continue;
        }
        if nidl_e9_file_is_cfg_compat_module(&path) {
            continue;
        }
        let text = fs::read_to_string(&path).unwrap_or_default();
        let production = nidl_e9_rust_production_text_without_cfg_test(&text);
        let mut in_block_comment = false;
        for (idx, line) in production.lines().enumerate() {
            let trimmed = line.trim();
            if nidl_e9_is_comment_or_blank_line(trimmed, &mut in_block_comment) {
                continue;
            }
            if line.contains("crate::lower::compat") || line.contains("lower::compat") {
                hits.push(format!("{rel_path}:{}:{line}", idx + 1));
            }
        }
    }
    hits.sort();
    hits
}

fn nidl_e9_is_lower_compat_scope(rel_path: &str) -> bool {
    rel_path == "src/lower/compat" || rel_path.starts_with("src/lower/compat/")
}

fn nidl_e9_noncompat_lower_compat_import_hits() -> Vec<String> {
    nidl_e9_noncompat_lower_compat_import_hits_in(&src_dir())
}

fn nidl_e9_is_lower_compat_type_lowering_hit(hit: &str) -> bool {
    hit.contains("crate::lower::compat::type_lowering")
        || hit.contains("lower::compat::type_lowering")
}

fn nidl_e9_read(rel_path: &str) -> String {
    fs::read_to_string(Path::new(manifest_dir()).join(rel_path))
        .unwrap_or_else(|err| panic!("read {rel_path}: {err}"))
}

fn nidl_e9_text_region_between<'a>(text: &'a str, start: &str, end: &str) -> &'a str {
    let start_idx = text
        .find(start)
        .unwrap_or_else(|| panic!("missing start marker `{start}`"));
    let after_start = &text[start_idx..];
    let end_idx = after_start
        .find(end)
        .unwrap_or_else(|| panic!("missing end marker `{end}` after `{start}`"));
    &after_start[..end_idx]
}

fn nidl_e10_without_explicit_compat_regions(text: &str) -> String {
    let mut out = String::new();
    let mut in_region = false;
    for line in text.lines() {
        if line.contains("// NIDL-E10 compat-only generated IDL/codegen start")
            || line.contains("// NIDL-E10 compat-only Rust proto codegen start")
        {
            in_region = true;
            continue;
        }
        if line.contains("// NIDL-E10 compat-only generated IDL/codegen end")
            || line.contains("// NIDL-E10 compat-only Rust proto codegen end")
        {
            in_region = false;
            continue;
        }
        if !in_region {
            out.push_str(line);
            out.push('\n');
        }
    }
    out
}

fn nidl_e10_previous_nonblank_line<'a>(lines: &'a [&'a str], idx: usize) -> Option<&'a str> {
    lines[..idx]
        .iter()
        .rev()
        .map(|line| line.trim())
        .find(|line| !line.is_empty())
}

#[test]
fn nidl_e10_build_rs_gates_compat_generated_idl() {
    let build_rs = nidl_e9_read("src/build.rs");
    let default_region = nidl_e10_without_explicit_compat_regions(&build_rs);
    for forbidden in [
        "validate_thrift_rs_namespaces();",
        "resolve_thirdparty_root(&manifest_dir)",
        "let thrift_rs_cmd = find_tool(\"thrift\", &tp_bin);",
        "patch_plan_nodes_rs(&thrift_rs_out);",
        "thrift_root_mod.rs",
        "let starrocks_protos = [",
        "compile_protos(&starrocks_protos",
        "let staros_protos = [",
        "compile_protos(&staros_protos",
    ] {
        assert!(
            !default_region.contains(forbidden),
            "default build.rs path must not contain compat generated IDL/codegen `{forbidden}`"
        );
    }
}

#[test]
fn nidl_e10_proto_root_only_exposes_starrocks_and_staros_for_compat() {
    let build_rs = nidl_e9_read("src/build.rs");
    let emit_region = nidl_e9_text_region_between(&build_rs, "fn emit_proto_root_mod", "fn main()");
    assert!(
        emit_region.contains("fn emit_proto_root_mod(out_dir: &Path, compat: bool)"),
        "emit_proto_root_mod must accept compat so generated proto root can hide compat modules in default builds"
    );
    let default_region = nidl_e10_without_explicit_compat_regions(emit_region);
    for forbidden in ["pub mod starrocks", "pub mod staros"] {
        assert!(
            !default_region.contains(forbidden),
            "proto_root_mod default wrapper must not expose `{forbidden}`"
        );
    }
}

#[test]
fn nidl_e10_lib_only_includes_thrift_root_for_compat() {
    let lib_rs = nidl_e9_read("src/lib.rs");
    let lines = lib_rs.lines().collect::<Vec<_>>();
    let include_idx = lines
        .iter()
        .position(|line| line.contains("thrift_root_mod.rs"))
        .expect("src/lib.rs must contain thrift_root_mod include for compat builds");
    assert_eq!(
        nidl_e10_previous_nonblank_line(&lines, include_idx),
        Some("#[cfg(feature = \"compat\")]"),
        "src/lib.rs must cfg-gate thrift_root_mod.rs include to compat builds"
    );
}

#[test]
fn nidl_e9_native_fragment_wire_has_no_starrocks_thrift_aliases() {
    let text = nidl_e9_read("src/runtime/native_fragment_wire.rs");
    let production = rust_production_text_without_cfg_test_or_compat(&text);
    for forbidden in [
        "type DataStreamSink = data_sinks::TDataStreamSink",
        "type MultiCastDataStreamSink = data_sinks::TMultiCastDataStreamSink",
        "type DataPartition = partitions::TDataPartition",
        "crate::thrift::{data_sinks, partitions, types}",
    ] {
        assert!(
            !production.contains(forbidden),
            "native_fragment_wire must not keep StarRocks thrift alias `{forbidden}`"
        );
    }
}

#[test]
fn nidl_e9_write_coordinator_uses_native_report_types() {
    let coordinator = nidl_e9_read("src/runtime/write_coordinator.rs");
    let coordinator_region = nidl_e9_text_region_between(
        &coordinator,
        "pub(crate) use crate::runtime::write_report",
        "impl WriteCoordinator",
    );
    let write_report = nidl_e9_read("src/runtime/write_report.rs");
    let write_report_region = nidl_e9_text_region_between(
        &write_report,
        "pub(crate) struct WriterKey",
        "pub(crate) fn unique_id_from_native",
    );
    let region = format!("{coordinator_region}\n{write_report_region}");
    for forbidden in [
        "types::TUniqueId",
        "status::TStatus",
        "types::TSinkCommitInfo",
        "types::TTabletCommitInfo",
        "types::TTabletFailInfo",
    ] {
        assert!(
            !region.contains(forbidden),
            "write coordinator public report structs must not contain `{forbidden}`:\n{region}"
        );
    }
}

#[test]
fn nidl_e9_noncompat_startup_does_not_init_frontend_rpc() {
    let text = nidl_e9_read("src/main.rs");
    let production = rust_production_text_without_cfg_test_or_compat(&text);
    assert!(
        !production.contains("frontend_rpc::init_frontend_rpc_manager"),
        "non-compat startup must not initialize Frontend RPC manager"
    );
}

#[test]
fn nidl_e9_lower_compat_import_detector_ignores_cfg_compat_files() {
    let dir = std::env::temp_dir().join("nidl_e9_lower_compat_detector");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join("native_hit.rs"),
        "use crate::lower::compat::type_lowering::scalar_type_desc;\n",
    )
    .unwrap();
    fs::write(
        dir.join("native_generic_hit.rs"),
        "use crate::lower::compat::expr::parse_min_max_conjuncts;\n",
    )
    .unwrap();
    fs::write(
        dir.join("compat_only.rs"),
        "#[cfg(feature = \"compat\")]\nfn compat_only() { let _ = crate::lower::compat::type_lowering::scalar_type_desc; }\n",
    )
    .unwrap();
    fs::write(
        dir.join("mod.rs"),
        "#[cfg(feature = \"compat\")]\npub(crate) mod compat_module;\n",
    )
    .unwrap();
    fs::write(
        dir.join("compat_module.rs"),
        "use crate::lower::compat::fragment::execute_fragment;\n",
    )
    .unwrap();
    fs::write(
        dir.join("comment_note.rs"),
        "// This note mentions crate::lower::compat::type_lowering but is not production code.\n",
    )
    .unwrap();
    fs::write(
        dir.join("block_comment_note.rs"),
        "/* This note mentions crate::lower::compat::type_lowering but is not production code. */\n",
    )
    .unwrap();
    fs::write(
        dir.join("multiline_block_comment_note.rs"),
        "/*\n * This note mentions lower::compat::type_lowering but is not production code.\n */\n",
    )
    .unwrap();

    let hits = nidl_e9_noncompat_lower_compat_import_hits_in(&dir);
    assert!(
        hits.iter().any(|hit| hit.contains("native_hit.rs")),
        "must report default-build lower::compat::type_lowering imports: {hits:?}"
    );
    assert!(
        hits.iter().any(|hit| hit.contains("native_generic_hit.rs")),
        "must report default-build lower::compat imports: {hits:?}"
    );
    assert!(
        hits.iter().any(|hit| hit.contains("compat_only.rs")),
        "must report item-level cfg(feature=\"compat\") lower::compat imports in non-compat files: {hits:?}"
    );
    assert!(
        !hits.iter().any(|hit| hit.contains("compat_module.rs")),
        "must ignore lower::compat imports from modules declared cfg(feature=\"compat\"): {hits:?}"
    );
    assert!(
        !hits.iter().any(|hit| hit.contains("comment_note.rs")),
        "must ignore commented lower::compat::type_lowering mentions: {hits:?}"
    );
    assert!(
        !hits.iter().any(|hit| hit.contains("block_comment_note.rs")),
        "must ignore block-commented lower::compat::type_lowering mentions: {hits:?}"
    );
    assert!(
        !hits
            .iter()
            .any(|hit| hit.contains("multiline_block_comment_note.rs")),
        "must ignore multiline block-commented lower::compat::type_lowering mentions: {hits:?}"
    );
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn nidl_e9_lower_compat_scope_only_skips_lower_compat_implementation() {
    assert!(nidl_e9_is_lower_compat_scope("src/lower/compat"));
    assert!(nidl_e9_is_lower_compat_scope(
        "src/lower/compat/node/hdfs_scan.rs"
    ));
    assert!(
        !nidl_e9_is_lower_compat_scope("src/service/compat.rs"),
        "E9 must not inherit E0 service compat scope"
    );
    assert!(
        nidl_e9_file_is_cfg_compat_module(
            &Path::new(manifest_dir()).join("src/service/internal_service.rs")
        ),
        "E9 may ignore service/internal_service.rs only because its module declaration is cfg(feature=\"compat\")"
    );
    assert!(
        !nidl_e9_is_lower_compat_scope("src/connector/starrocks/lake/schema_change.rs"),
        "E9 must report connector lower::compat imports"
    );
    assert!(
        !nidl_e9_is_lower_compat_scope("src/exec/chunk/schema_thrift.rs"),
        "E9 must report exec/chunk lower::compat imports"
    );
}

#[test]
fn nidl_e9_native_codegen_does_not_import_lower_compat_type_lowering() {
    let hits: Vec<String> = nidl_e9_noncompat_lower_compat_import_hits()
        .into_iter()
        .filter(|hit| {
            hit.contains("src/sql/codegen/")
                || hit.contains("src/runtime/")
                || hit.contains("src/formats/parquet/")
        })
        .filter(|hit| nidl_e9_is_lower_compat_type_lowering_hit(hit))
        .collect();
    assert!(
        hits.is_empty(),
        "native codegen/runtime must not import lower::compat type lowering helpers:\n{}",
        hits.join("\n")
    );
}

#[test]
fn nidl_e9_noncompat_paths_do_not_import_lower_compat() {
    let hits = nidl_e9_noncompat_lower_compat_import_hits();
    assert!(
        hits.is_empty(),
        "non-compat paths must not import lower::compat:\n{}",
        hits.join("\n")
    );
}

#[test]
fn nidl_e9_lower_compat_module_is_cfg_gated() {
    let module_file = Path::new(manifest_dir()).join("src/lower/mod.rs");
    assert!(
        nidl_e9_module_has_compat_cfg(&module_file, "compat"),
        "src/lower/mod.rs must gate lower::compat with #[cfg(feature = \"compat\")]"
    );
}

#[test]
fn nidl_e9_guard_helpers_find_text_regions() {
    let text = "alpha\nstart\nbody\nend\nomega\n";
    assert_eq!(
        nidl_e9_text_region_between(text, "start", "end"),
        "start\nbody\n"
    );
}

#[test]
#[should_panic(expected = "missing start marker `start`")]
fn nidl_e9_guard_helpers_panic_when_text_region_start_is_missing() {
    let text = "alpha\nbody\nend\nomega\n";
    let _ = nidl_e9_text_region_between(text, "start", "end");
}

#[test]
#[should_panic(expected = "missing end marker `end` after `start`")]
fn nidl_e9_guard_helpers_panic_when_text_region_end_is_missing() {
    let text = "alpha\nstart\nbody\nomega\n";
    let _ = nidl_e9_text_region_between(text, "start", "end");
}

#[test]
fn nidl_e9_guard_helpers_ignore_block_comments_between_cfg_and_module() {
    let dir = std::env::temp_dir().join("nidl_e9_module_cfg_detector");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let file = dir.join("mod_fixture.rs");
    fs::write(
        &file,
        "#[cfg(feature = \"compat\")]\n/* compatibility module note */\npub(crate) mod compat_only;\n",
    )
    .unwrap();

    assert!(
        nidl_e9_module_has_compat_cfg(&file, "compat_only"),
        "must ignore block comments between cfg(feature=\"compat\") and module declarations"
    );
    let _ = fs::remove_dir_all(&dir);
}

fn nidl_e1_native_mv_starrocks_table_import_hits() -> Vec<String> {
    nidl_e1_native_mv_starrocks_table_import_hits_in(&[
        Path::new(manifest_dir()).join("src/exec"),
        Path::new(manifest_dir()).join("src/engine"),
        Path::new(manifest_dir()).join("src/sql"),
    ])
}

fn nidl_e1_native_mv_starrocks_table_import_hits_in(roots: &[PathBuf]) -> Vec<String> {
    let forbidden = [
        "crate::connector::starrocks::table::state_codec",
        "crate::connector::starrocks::table::aggregate_sql_calls",
        "crate::connector::starrocks::table::mv_agg_state",
        "crate::connector::starrocks::table::mv_shape",
        "crate::connector::starrocks::table::model::IcebergTableRef",
    ];
    let grouped_root = "crate::connector::starrocks::table::{";
    let grouped_terms = [
        "state_codec",
        "aggregate_sql_calls",
        "mv_agg_state",
        "mv_shape",
        "model::IcebergTableRef",
    ];

    let mut hits = Vec::new();
    for root in roots {
        for path in rs_files(root) {
            for (line, text) in non_test_line_hits(&path, |source| {
                forbidden.iter().any(|needle| source.contains(needle))
            }) {
                hits.push(format!("{}:{line}: {text}", rel(&path)));
            }
            let text = fs::read_to_string(&path).unwrap_or_default();
            let production = rust_production_text_without_cfg_test(&text);
            let compact: String = non_comment_trimmed_lines(&production).join("");
            let mut search_start = 0usize;
            while let Some(offset) = compact[search_start..].find(grouped_root) {
                let start = search_start + offset;
                let span = &compact[start..];
                let end = span.find(';').unwrap_or(span.len());
                let import_span = &span[..end];
                if let Some(term) = grouped_terms
                    .iter()
                    .find(|term| import_span.contains(**term))
                {
                    hits.push(format!(
                        "{}:1: grouped import references connector::starrocks::table::{term}",
                        rel(&path)
                    ));
                }
                search_start = start + grouped_root.len();
            }
        }
    }
    hits.sort();
    hits
}

#[test]
fn nidl_e1_detector_flags_grouped_imports_and_ignores_tests() {
    let dir = std::env::temp_dir().join("nidl_e1_detector");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join("grouped.rs"),
        "use crate::connector::starrocks::table::{\n    mv_shape::AggregateMvShape,\n    state_codec::decode_count_state,\n};\n",
    )
    .unwrap();
    fs::write(
        dir.join("test_only.rs"),
        "#[cfg(test)]\nmod tests {\n    use crate::connector::starrocks::table::state_codec;\n}\n",
    )
    .unwrap();

    let hits = nidl_e1_native_mv_starrocks_table_import_hits_in(&[dir.clone()]);
    assert!(
        hits.iter().any(|hit| hit.contains("grouped.rs")),
        "must flag grouped StarRocks table helper imports; got {hits:?}"
    );
    assert!(
        !hits.iter().any(|hit| hit.contains("test_only.rs")),
        "must ignore #[cfg(test)] imports; got {hits:?}"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn nidl_e1_native_mv_codecs_do_not_import_starrocks_table_modules() {
    let hits = nidl_e1_native_mv_starrocks_table_import_hits();
    assert!(
        hits.is_empty(),
        "native MV/aggregate code must import native agg_state/table_ref modules, not connector::starrocks::table helpers:\n{}",
        hits.join("\n")
    );
}

#[test]
fn nidl_e3_planner_ir_uses_native_partition_and_runtime_filter_types() {
    let repo = Path::new(manifest_dir());
    let mut violations = Vec::new();

    for source in [
        "src/sql/planner/distributed/fragment.rs",
        "src/sql/planner/distributed/node.rs",
        "src/sql/planner/distributed/runtime_filter.rs",
        "src/sql/planner/distributed/write/change_stream.rs",
        "src/sql/planner/distributed/write/plan.rs",
        "src/sql/planner/distributed/write/sink.rs",
        "src/sql/planner/distributed/build/mod.rs",
        "src/sql/planner/distributed/build/lowering.rs",
        "src/sql/planner/distributed/build/fragment_cut.rs",
        "src/sql/planner/distributed/build/runtime_filter_binding.rs",
        "src/sql/planner/physical/mod.rs",
        "src/sql/planner/physical/node.rs",
        "src/sql/planner/physical/runtime_filter.rs",
        "src/sql/planner/physical/runtime_filter_placement.rs",
        "src/sql/planner/physical/stats.rs",
        "src/sql/planner/physical/vocab.rs",
        "src/sql/codegen/runtime_filter.rs",
    ] {
        let text = fs::read_to_string(repo.join(source)).unwrap();
        let text = rust_production_text_without_cfg_test(&text);
        push_forbidden_terms(
            &mut violations,
            source,
            &text,
            &[
                "crate::thrift",
                "thrift::",
                "TPartitionType",
                "TDataPartition",
                "TRuntimeFilterDescription",
            ],
            "planner/codegen stage IR must use native partition and runtime-filter types",
        );
    }

    let codegen_mod = fs::read_to_string(repo.join("src/sql/codegen/mod.rs")).unwrap();
    let codegen_mod = rust_production_text_without_cfg_test(&codegen_mod);
    push_forbidden_terms(
        &mut violations,
        "src/sql/codegen/mod.rs",
        &codegen_mod,
        &[
            "pub compat_output_partition:",
            "crate::thrift::runtime_filter::TRuntimeFilterDescription",
        ],
        "codegen public IR must not expose thrift partition/RF descriptor fields",
    );

    let proto_plan = fs::read_to_string(repo.join("src/sql/codegen/proto_encode/plan.rs")).unwrap();
    let proto_plan = rust_production_text_without_cfg_test(&proto_plan);
    push_forbidden_terms(
        &mut violations,
        "src/sql/codegen/proto_encode/plan.rs",
        &proto_plan,
        &["crate::thrift::partitions", "TPartitionType"],
        "native proto encoder must encode ExchangeReceiver from native DataPartition",
    );

    assert!(
        violations.is_empty(),
        "NIDL-E3 planner IR native-type guard failed:\n{}",
        violations.join("\n")
    );
}

#[derive(Clone, Copy)]
enum NidlE4CodeScanState {
    Code,
    BlockComment { depth: usize },
    String { escaped: bool },
    RawString { hashes: usize },
}

fn nidl_e4_has_code_line<F>(text: &str, mut predicate: F) -> bool
where
    F: FnMut(&str) -> bool,
{
    nidl_e4_code_line_entries(text)
        .into_iter()
        .map(|(_, line)| line)
        .any(|line| !line.is_empty() && predicate(&line))
}

fn nidl_e4_is_ident_char(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}

fn nidl_e4_raw_string_start(chars: &[char], index: usize) -> Option<(usize, usize)> {
    if index > 0 && nidl_e4_is_ident_char(chars[index - 1]) {
        return None;
    }

    let mut cursor = match chars.get(index).copied()? {
        'r' => index + 1,
        'b' if chars.get(index + 1) == Some(&'r') => index + 2,
        _ => return None,
    };

    let mut hashes = 0usize;
    while chars.get(cursor) == Some(&'#') {
        hashes += 1;
        cursor += 1;
    }

    if chars.get(cursor) == Some(&'"') {
        Some((cursor - index + 1, hashes))
    } else {
        None
    }
}

fn nidl_e4_raw_string_end(chars: &[char], index: usize, hashes: usize) -> Option<usize> {
    if chars.get(index) != Some(&'"') {
        return None;
    }

    for offset in 0..hashes {
        if chars.get(index + 1 + offset) != Some(&'#') {
            return None;
        }
    }

    Some(1 + hashes)
}

fn nidl_e4_char_literal_len(chars: &[char], index: usize) -> Option<usize> {
    if chars.get(index) != Some(&'\'') {
        return None;
    }

    let mut cursor = index + 1;
    let first = chars.get(cursor).copied()?;
    if first == '\'' {
        return None;
    }

    if first == '\\' {
        cursor += 1;
        let escaped = chars.get(cursor).copied()?;
        if escaped == 'u' && chars.get(cursor + 1) == Some(&'{') {
            cursor += 2;
            while chars.get(cursor).is_some() && chars[cursor] != '}' {
                cursor += 1;
            }
            if chars.get(cursor) != Some(&'}') {
                return None;
            }
            cursor += 1;
        } else {
            cursor += 1;
        }
    } else {
        cursor += 1;
    }

    if chars.get(cursor) == Some(&'\'') {
        Some(cursor - index + 1)
    } else {
        None
    }
}

fn nidl_e4_code_line_entries(text: &str) -> Vec<(usize, String)> {
    let mut lines = Vec::new();
    let mut state = NidlE4CodeScanState::Code;

    for (idx, line) in text.lines().enumerate() {
        let chars: Vec<char> = line.chars().collect();
        let mut code = String::with_capacity(line.len());
        let mut cursor = 0usize;

        while cursor < chars.len() {
            match state {
                NidlE4CodeScanState::Code => {
                    if chars.get(cursor) == Some(&'/') && chars.get(cursor + 1) == Some(&'/') {
                        break;
                    }

                    if chars.get(cursor) == Some(&'/') && chars.get(cursor + 1) == Some(&'*') {
                        state = NidlE4CodeScanState::BlockComment { depth: 1 };
                        cursor += 2;
                        continue;
                    }

                    if let Some((len, hashes)) = nidl_e4_raw_string_start(&chars, cursor) {
                        state = NidlE4CodeScanState::RawString { hashes };
                        cursor += len;
                        continue;
                    }

                    if chars.get(cursor) == Some(&'"') {
                        state = NidlE4CodeScanState::String { escaped: false };
                        cursor += 1;
                        continue;
                    }

                    if let Some(len) = nidl_e4_char_literal_len(&chars, cursor) {
                        cursor += len;
                        continue;
                    }

                    code.push(chars[cursor]);
                    cursor += 1;
                }
                NidlE4CodeScanState::BlockComment { mut depth } => {
                    if chars.get(cursor) == Some(&'/') && chars.get(cursor + 1) == Some(&'*') {
                        depth += 1;
                        state = NidlE4CodeScanState::BlockComment { depth };
                        cursor += 2;
                    } else if chars.get(cursor) == Some(&'*') && chars.get(cursor + 1) == Some(&'/')
                    {
                        depth -= 1;
                        cursor += 2;
                        if depth == 0 {
                            state = NidlE4CodeScanState::Code;
                        } else {
                            state = NidlE4CodeScanState::BlockComment { depth };
                        }
                    } else {
                        cursor += 1;
                    }
                }
                NidlE4CodeScanState::String { mut escaped } => {
                    if escaped {
                        escaped = false;
                        state = NidlE4CodeScanState::String { escaped };
                    } else if chars[cursor] == '\\' {
                        state = NidlE4CodeScanState::String { escaped: true };
                    } else if chars[cursor] == '"' {
                        state = NidlE4CodeScanState::Code;
                    } else {
                        state = NidlE4CodeScanState::String { escaped };
                    }
                    cursor += 1;
                }
                NidlE4CodeScanState::RawString { hashes } => {
                    if let Some(len) = nidl_e4_raw_string_end(&chars, cursor, hashes) {
                        state = NidlE4CodeScanState::Code;
                        cursor += len;
                    } else {
                        cursor += 1;
                    }
                }
            }
        }

        let code = code.trim().to_string();
        if !code.is_empty() {
            lines.push((idx + 1, code));
        }
    }

    lines
}

fn nidl_e4_has_exact_code_line(text: &str, expected: &str) -> bool {
    nidl_e4_has_code_line(text, |line| line == expected)
}

fn nidl_e4_struct_code_span(text: &str, header: &str) -> Option<Vec<(usize, String)>> {
    let lines = nidl_e4_code_line_entries(text);
    let start = lines.iter().position(|(_, line)| line == header)?;
    let mut depth = 0isize;
    let mut seen_open = false;

    for idx in start..lines.len() {
        let line = &lines[idx].1;
        if line.contains('{') {
            seen_open = true;
        }
        depth += brace_delta(line);
        if seen_open && depth <= 0 {
            return Some(lines[start..=idx].to_vec());
        }
    }

    None
}

fn nidl_e4_struct_has_code_line<F>(text: &str, header: &str, mut predicate: F) -> bool
where
    F: FnMut(&str) -> bool,
{
    nidl_e4_struct_code_span(text, header)
        .map(|span| span.into_iter().any(|(_, line)| predicate(&line)))
        .unwrap_or(false)
}

fn nidl_e4_function_signature_contains(text: &str, fn_name: &str, needle: &str) -> bool {
    let lines = nidl_e4_code_line_entries(text);
    let fn_pattern = format!("fn {fn_name}(");
    let Some(start) = lines
        .iter()
        .position(|(_, line)| line.contains(&fn_pattern))
    else {
        return false;
    };

    let mut signature = String::new();
    for (_, line) in lines.iter().skip(start) {
        if !signature.is_empty() {
            signature.push(' ');
        }
        signature.push_str(line);
        if line.contains('{') {
            break;
        }
    }

    signature.contains(needle)
}

fn nidl_e4_push_forbidden_code_terms(
    violations: &mut Vec<String>,
    source: &str,
    text: &str,
    terms: &[&str],
    reason: &str,
) {
    let lines = nidl_e4_code_line_entries(text);
    for term in terms {
        if let Some((line, text)) = lines.iter().find(|(_, line)| line.contains(term)) {
            violations.push(format!("{source}:{line}: {reason}: `{term}` in `{text}`"));
        }
    }
}

#[test]
fn nidl_e4_scheduler_and_coordinator_use_native_scheduling_metadata() {
    let repo = Path::new(manifest_dir());
    let mut violations = Vec::new();

    let codegen_mod = fs::read_to_string(repo.join("src/sql/codegen/mod.rs")).unwrap();
    let codegen_mod_prod = rust_production_text_without_cfg_test(&codegen_mod);
    if !nidl_e4_has_exact_code_line(
        &codegen_mod_prod,
        "pub(crate) struct FragmentSchedulingMetadata {",
    ) {
        violations.push(
            "src/sql/codegen/mod.rs: E4 must expose a native FragmentSchedulingMetadata result"
                .to_string(),
        );
    }
    if !nidl_e4_struct_has_code_line(
        &codegen_mod_prod,
        "pub(crate) struct MultiFragmentBuildResult {",
        |line| {
            line.trim_end_matches(',') == "pub fragment_schedules: Vec<FragmentSchedulingMetadata>"
        },
    ) {
        violations.push(
            "src/sql/codegen/mod.rs: MultiFragmentBuildResult must carry native fragment_schedules"
                .to_string(),
        );
    }

    let scheduler = fs::read_to_string(repo.join("src/runtime/scheduler.rs")).unwrap();
    let scheduler_prod = rust_production_text_without_cfg_test(&scheduler);
    nidl_e4_push_forbidden_code_terms(
        &mut violations,
        "src/runtime/scheduler.rs",
        &scheduler_prod,
        &[
            "FragmentBuildResult",
            "plan_nodes::TPlan",
            "TPlanNodeType",
            ".plan.nodes",
            ".exec_params",
            ".output_sink",
        ],
        "scheduler must consume native FragmentSchedulingMetadata, not thrift fragment build payloads",
    );
    for fn_name in ["assign", "assign_with_live"] {
        if !nidl_e4_function_signature_contains(
            &scheduler_prod,
            fn_name,
            "fragments: &[FragmentSchedulingMetadata]",
        ) {
            violations.push(format!(
                "src/runtime/scheduler.rs: {fn_name} signature must accept FragmentSchedulingMetadata"
            ));
        }
    }

    for retired in [
        "src/runtime/exec_params.rs",
        "src/runtime/exec_params_compat.rs",
    ] {
        if repo.join(retired).exists() {
            violations.push(format!(
                "{retired}: retired NovaRocks FE exec-param adapter must remain deleted"
            ));
        }
    }

    let coordinator = fs::read_to_string(repo.join("src/runtime/coordinator.rs")).unwrap();
    let coordinator_prod = rust_production_text_without_cfg_test(&coordinator);
    nidl_e4_push_forbidden_code_terms(
        &mut violations,
        "src/runtime/coordinator.rs",
        &coordinator_prod,
        &[
            "scheduler.assign_with_live(&fragment_results",
            "topological_sort_bottom_up(&fragment_results",
            "TPlanFragment::new",
            "crate::thrift::planner::TPlanFragment",
            "planner::TPlanFragment",
            "TPlanFragment as",
            "use crate::thrift::planner::TPlanFragment",
            "use crate::thrift::planner::{TPlanFragment",
        ],
        "coordinator must schedule from native metadata and must not directly construct thrift TPlanFragment",
    );
    if !nidl_e4_has_code_line(&coordinator_prod, |line| {
        line.contains("fragment_schedules")
    }) {
        violations.push(
            "src/runtime/coordinator.rs: coordinator must destructure and use fragment_schedules"
                .to_string(),
        );
    }

    assert!(
        violations.is_empty(),
        "NIDL-E4 native scheduling metadata guard failed:\n{}",
        violations.join("\n")
    );
}
// ---------------------------------------------------------------------------
// NIDL-E2: StarRocks connector/format compat gate
// ---------------------------------------------------------------------------

fn nidl_e2_is_allowed_compat_scope(rel_path: &str) -> bool {
    rel_path == "src/connector/starrocks"
        || rel_path.starts_with("src/connector/starrocks/")
        || rel_path == "src/formats/starrocks"
        || rel_path.starts_with("src/formats/starrocks/")
        || rel_path == "src/lower/compat"
        || rel_path.starts_with("src/lower/compat/")
        || nidl_e0_is_in_compat_scope(rel_path)
}

fn nidl_e2_forbidden_terms() -> &'static [&'static str] {
    &[
        "crate::connector::starrocks",
        "crate::formats::starrocks",
        "crate::novarocks_connector_starrocks",
    ]
}

fn nidl_e2_rel_path_under_scan_root(root: &Path, path: &Path) -> String {
    root.parent()
        .and_then(|base| path.strip_prefix(base).ok())
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| rel(path))
}

fn nidl_e2_is_ident_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn nidl_e2_has_token(text: &str, token: &str) -> bool {
    let mut search_start = 0usize;
    while let Some(offset) = text[search_start..].find(token) {
        let start = search_start + offset;
        let end = start + token.len();
        let before_is_ident = start
            .checked_sub(1)
            .and_then(|idx| text.as_bytes().get(idx))
            .is_some_and(|byte| nidl_e2_is_ident_byte(*byte));
        let after_is_ident = text
            .as_bytes()
            .get(end)
            .is_some_and(|byte| nidl_e2_is_ident_byte(*byte));
        if !before_is_ident && !after_is_ident {
            return true;
        }
        search_start = end;
    }
    false
}

fn nidl_e2_import_span_has_grouped_module(import_span: &str, parent: &str, module: &str) -> bool {
    let grouped_parent = format!("{parent}::{{");
    import_span.find(&grouped_parent).is_some_and(|start| {
        nidl_e2_has_token(&import_span[start + grouped_parent.len()..], module)
    })
}

fn nidl_e2_grouped_import_hits(path: &Path) -> Vec<String> {
    let text = fs::read_to_string(path).unwrap_or_default();
    let production = nidl_e2_rust_text_without_cfg_test_or_compat(&text);
    let compact: String = non_comment_trimmed_lines(&production).join("");
    let mut hits = Vec::new();

    for grouped_root in ["crate::connector::{", "crate::formats::{", "crate::{"] {
        let mut search_start = 0usize;
        while let Some(offset) = compact[search_start..].find(grouped_root) {
            let start = search_start + offset;
            let span = &compact[start..];
            let end = span.find(';').unwrap_or(span.len());
            let import_span = &span[..end];

            if grouped_root == "crate::connector::{" && nidl_e2_has_token(import_span, "starrocks")
            {
                hits.push("grouped import references connector::starrocks".to_string());
            }
            if grouped_root == "crate::formats::{" && nidl_e2_has_token(import_span, "starrocks") {
                hits.push("grouped import references formats::starrocks".to_string());
            }
            if grouped_root == "crate::{" {
                if nidl_e2_has_token(import_span, "connector::starrocks")
                    || nidl_e2_import_span_has_grouped_module(import_span, "connector", "starrocks")
                {
                    hits.push(
                        "grouped import references StarRocks connector/format module: connector::starrocks"
                            .to_string(),
                    );
                }
                if nidl_e2_has_token(import_span, "formats::starrocks")
                    || nidl_e2_import_span_has_grouped_module(import_span, "formats", "starrocks")
                {
                    hits.push(
                        "grouped import references StarRocks connector/format module: formats::starrocks"
                            .to_string(),
                    );
                }
                if nidl_e2_has_token(import_span, "novarocks_connector_starrocks") {
                    hits.push(
                        "grouped import references StarRocks connector/format module: novarocks_connector_starrocks"
                            .to_string(),
                    );
                }
            }
            search_start = start + grouped_root.len();
        }
    }

    hits.sort();
    hits.dedup();
    hits
}

fn nidl_e2_format_hits_by_file(hits: &[String], max_per_file: usize) -> String {
    let mut by_file = BTreeMap::<String, Vec<String>>::new();
    for hit in hits {
        let file = hit.split_once(':').map(|(file, _)| file).unwrap_or(hit);
        by_file
            .entry(file.to_string())
            .or_default()
            .push(hit.to_string());
    }

    let mut out = Vec::new();
    for (_file, file_hits) in by_file {
        for hit in file_hits.iter().take(max_per_file) {
            out.push(hit.clone());
        }
        if file_hits.len() > max_per_file {
            out.push(format!(
                "{}: ... {} more hit(s)",
                file_hits[0].split_once(':').map(|(file, _)| file).unwrap(),
                file_hits.len() - max_per_file
            ));
        }
    }
    out.join("\n")
}

fn nidl_e2_noncompat_starrocks_gateway_hits_in(root: &Path) -> Vec<String> {
    let mut hits = Vec::new();
    for path in rs_files(root) {
        let rel_path = nidl_e2_rel_path_under_scan_root(root, &path);
        if nidl_e2_is_allowed_compat_scope(&rel_path) {
            continue;
        }
        let text = fs::read_to_string(&path).unwrap_or_default();
        let production = nidl_e2_rust_text_without_cfg_test_or_compat(&text);
        for (idx, line) in production.lines().enumerate() {
            if !is_comment_or_blank(line)
                && nidl_e2_forbidden_terms()
                    .iter()
                    .any(|term| line.contains(*term))
            {
                hits.push(format!("{rel_path}:{}: {}", idx + 1, line.trim()));
            }
        }
        for text in nidl_e2_grouped_import_hits(&path) {
            hits.push(format!("{rel_path}:1: {text}"));
        }
    }
    hits.sort();
    hits
}

fn has_cfg_feature_compat_before_item(text: &str, item: &str) -> bool {
    let lines: Vec<&str> = text.lines().collect();
    for (idx, line) in lines.iter().enumerate() {
        if line.trim() == item {
            let mut cursor = idx;
            while cursor > 0 {
                cursor -= 1;
                let previous = lines[cursor].trim();
                if previous.is_empty() || previous.starts_with("//") {
                    continue;
                }
                return previous == "#[cfg(feature = \"compat\")]";
            }
        }
    }
    false
}

#[test]
fn nidl_e2_detector_flags_noncompat_gateway_imports_and_ignores_compat_scopes() {
    let dir = std::env::temp_dir().join("nidl_e2_detector");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(dir.join("src/engine")).unwrap();
    fs::create_dir_all(dir.join("src/connector/starrocks")).unwrap();
    fs::create_dir_all(dir.join("src/lower/compat")).unwrap();
    fs::write(
        dir.join("src/engine/offender.rs"),
        "use crate::connector::starrocks::scan::StarRocksScanRange;\n",
    )
    .unwrap();
    fs::write(
        dir.join("src/engine/grouped_parent.rs"),
        "use crate::connector::{starrocks::scan::StarRocksScanRange};\n",
    )
    .unwrap();
    fs::write(
        dir.join("src/engine/grouped_crate.rs"),
        "use crate::{connector::starrocks, formats::starrocks};\n",
    )
    .unwrap();
    fs::write(
        dir.join("src/engine/nested_grouped_crate.rs"),
        "use crate::{connector::{starrocks::scan::StarRocksScanRange}, formats::{starrocks::metadata::load_tablet_snapshot}};\n",
    )
    .unwrap();
    fs::write(
        dir.join("src/engine/similar_name.rs"),
        "use crate::connector::{iceberg::starrocks_profile};\n",
    )
    .unwrap();
    fs::write(
        dir.join("src/engine/test_only.rs"),
        "#[cfg(test)]\nmod tests {\n    use crate::{connector::starrocks, formats::starrocks};\n}\n",
    )
    .unwrap();
    fs::write(
        dir.join("src/engine/compat_direct.rs"),
        "#[cfg(feature = \"compat\")]\nuse crate::connector::starrocks::scan::StarRocksScanRange;\n",
    )
    .unwrap();
    fs::write(
        dir.join("src/engine/compat_grouped.rs"),
        "#[cfg(feature = \"compat\")]\nuse crate::{connector::{starrocks::scan::StarRocksScanRange}, formats::{starrocks::metadata::load_tablet_snapshot}};\n",
    )
    .unwrap();
    fs::write(
        dir.join("src/connector/starrocks/allowed.rs"),
        "use crate::connector::starrocks::scan::StarRocksScanRange;\n",
    )
    .unwrap();
    fs::write(
        dir.join("src/lower/compat/allowed.rs"),
        "use crate::formats::starrocks::metadata::load_tablet_snapshot;\n",
    )
    .unwrap();

    let hits = nidl_e2_noncompat_starrocks_gateway_hits_in(&dir.join("src"));
    assert!(
        hits.iter()
            .any(|hit| hit.contains("src/engine/offender.rs")),
        "must flag non-compat StarRocks connector imports; got {hits:?}"
    );
    assert!(
        hits.iter()
            .any(|hit| hit.contains("src/engine/grouped_parent.rs")),
        "must flag grouped connector parent imports; got {hits:?}"
    );
    assert!(
        hits.iter()
            .any(|hit| hit.contains("src/engine/grouped_crate.rs")),
        "must flag grouped crate imports; got {hits:?}"
    );
    assert!(
        hits.iter()
            .any(|hit| hit.contains("src/engine/nested_grouped_crate.rs")),
        "must flag nested grouped crate imports; got {hits:?}"
    );
    assert!(
        !hits
            .iter()
            .any(|hit| hit.contains("src/engine/similar_name.rs")),
        "must not flag similar names that are not the starrocks module; got {hits:?}"
    );
    assert!(
        !hits
            .iter()
            .any(|hit| hit.contains("src/engine/test_only.rs")),
        "must ignore #[cfg(test)] grouped imports; got {hits:?}"
    );
    assert!(
        !hits
            .iter()
            .any(|hit| hit.contains("src/engine/compat_direct.rs")),
        "must ignore #[cfg(feature = \"compat\")] direct imports; got {hits:?}"
    );
    assert!(
        !hits
            .iter()
            .any(|hit| hit.contains("src/engine/compat_grouped.rs")),
        "must ignore #[cfg(feature = \"compat\")] grouped imports; got {hits:?}"
    );
    assert!(
        !hits
            .iter()
            .any(|hit| hit.contains("src/connector/starrocks/allowed.rs")),
        "must ignore the gated connector module itself; got {hits:?}"
    );
    assert!(
        !hits
            .iter()
            .any(|hit| hit.contains("src/lower/compat/allowed.rs")),
        "must ignore lower compat scope; got {hits:?}"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn nidl_e2_cfg_feature_helper_checks_nearest_non_comment_attribute() {
    assert!(
        has_cfg_feature_compat_before_item(
            "#[cfg(feature = \"compat\")]\n// module comment\npub mod starrocks;\n",
            "pub mod starrocks;"
        ),
        "must accept compat cfg immediately before module item"
    );
    assert!(
        !has_cfg_feature_compat_before_item(
            "#[cfg(feature = \"other\")]\npub mod starrocks;\n",
            "pub mod starrocks;"
        ),
        "must reject non-compat cfg before module item"
    );
    assert!(
        !has_cfg_feature_compat_before_item(
            "#[cfg(feature = \"compat\")]\npub mod iceberg;\npub mod starrocks;\n",
            "pub mod starrocks;"
        ),
        "must not treat cfg on a previous item as gating starrocks"
    );
}

#[test]
fn nidl_e2_starrocks_connector_and_format_modules_are_compat_gated() {
    let connector_mod = fs::read_to_string(Path::new(manifest_dir()).join("src/connector/mod.rs"))
        .expect("connector mod");
    let formats_mod = fs::read_to_string(Path::new(manifest_dir()).join("src/formats/mod.rs"))
        .expect("formats mod");
    let mut violations = Vec::new();
    if !has_cfg_feature_compat_before_item(&connector_mod, "pub mod starrocks;") {
        violations.push(
            "src/connector/mod.rs must gate pub mod starrocks with #[cfg(feature = \"compat\")]",
        );
    }
    if !has_cfg_feature_compat_before_item(&formats_mod, "pub mod starrocks;") {
        violations.push(
            "src/formats/mod.rs must gate pub mod starrocks with #[cfg(feature = \"compat\")]",
        );
    }
    assert!(
        violations.is_empty(),
        "StarRocks connector/format modules must be compat-gated:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nidl_e2_noncompat_code_does_not_import_starrocks_connector_or_format_modules() {
    let hits = nidl_e2_noncompat_starrocks_gateway_hits_in(&src_dir());
    assert!(
        hits.is_empty(),
        "non-compat production code must not import StarRocks connector/format modules outside compat scopes:\n{}",
        nidl_e2_format_hits_by_file(&hits, 5)
    );
}

#[test]
fn nidl_e6_runtime_adapters_are_compat_only() {
    let repo = Path::new(manifest_dir());
    let guarded = [
        (
            "src/runtime/query_options.rs",
            &[
                "crate::thrift",
                "TQueryOptions",
                "TSpillMode",
                "TSpillOptions",
            ][..],
        ),
        (
            "src/runtime/runtime_filter_params.rs",
            &[
                "crate::thrift",
                "runtime_filter::TRuntimeFilterParams",
                "runtime_filter::TRuntimeFilterProberParams",
            ][..],
        ),
        (
            "src/runtime/scan_range.rs",
            &[
                "crate::thrift",
                "descriptors::",
                "exprs::",
                "internal_service::",
                "plan_nodes::",
                "types::",
            ][..],
        ),
    ];

    let mut violations = Vec::new();
    for (source, terms) in guarded {
        let text = fs::read_to_string(repo.join(source)).expect(source);
        let default_build_text = rust_production_text_without_cfg_test_or_compat(&text);
        for term in terms {
            if let Some((idx, line)) = default_build_text
                .lines()
                .enumerate()
                .find(|(_, line)| !is_comment_or_blank(line) && line.contains(term))
            {
                violations.push(format!(
                    "{source}:{}: `{term}` in `{}`",
                    idx + 1,
                    line.trim()
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "E6 query/rf thrift adapters must be compat-only in the default build:\n{}",
        violations.join("\n")
    );
}
