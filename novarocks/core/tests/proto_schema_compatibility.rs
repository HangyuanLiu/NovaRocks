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

const BASELINE_PATH: &str = "tests/proto_schema_baseline/novarocks_schema.json";
const WRITE_BASELINE_ENV: &str = "NOVA_WRITE_PROTO_SCHEMA_BASELINE";
const WRITE_BASELINE_COMMAND: &str = "NOVA_WRITE_PROTO_SCHEMA_BASELINE=1 cargo test --test proto_schema_compatibility current_schema_matches_baseline -- --exact --nocapture";

fn manifest_dir() -> &'static str {
    env!("CARGO_MANIFEST_DIR")
}

/// The workspace root, where `idl/` lives. `CARGO_MANIFEST_DIR` is
/// `<repo>/novarocks/core`, so the root is two levels up. Recording schema
/// paths relative to this keeps them stable (`idl/novarocks/...`) regardless
/// of where the crate sits under the workspace.
fn workspace_root() -> &'static Path {
    Path::new(manifest_dir())
        .ancestors()
        .nth(2)
        .expect("CARGO_MANIFEST_DIR should have at least two ancestors")
}

fn rel(path: &Path) -> String {
    path.strip_prefix(workspace_root())
        .unwrap_or(path)
        .display()
        .to_string()
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
    for file in proto_files(&workspace_root().join("idl/novarocks")) {
        let relative = rel(&file);
        let input = fs::read_to_string(&file)
            .map_err(|err| format!("{}: failed to read proto file: {err}", relative))?;
        files.insert(relative.clone(), parse_proto_schema(&relative, &input)?);
    }

    Ok(ProtoSchema { version: 1, files })
}

fn baseline_path() -> PathBuf {
    Path::new(manifest_dir()).join(BASELINE_PATH)
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
            "{}: proto schema baseline is missing; {WRITE_BASELINE_ENV}=1 can only update an existing baseline after the compatibility baseline is established",
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
                    "{path} service {service_name} new service is not allowed; the compatibility contract only allows extending existing NovaRocksGrpc"
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
                "{path} service {service_name} new service is not allowed; the compatibility contract only allows extending existing NovaRocksGrpc"
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
fn proto_schema_parser_handles_current_syntax() {
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
fn proto_schema_parser_rejects_proto2_syntax() {
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
fn current_schema_matches_baseline() {
    let current =
        parse_current_novarocks_proto_schema().expect("current native proto schema should parse");
    let baseline_path = baseline_path();

    match env::var(WRITE_BASELINE_ENV) {
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
            "{WRITE_BASELINE_ENV} must be exactly `1` to write the proto schema baseline, got `{value}`"
        ),
        Err(env::VarError::NotUnicode(_)) => panic!(
            "{WRITE_BASELINE_ENV} must be valid UTF-8 and exactly `1` to write the proto schema baseline"
        ),
        Err(env::VarError::NotPresent) => {
            let baseline = read_proto_schema_baseline(&baseline_path)
                .unwrap_or_else(|err| panic!("{err}\n\n{}", baseline_update_hint()));
            let violations = compare_proto_schema_to_baseline(&current, &baseline);
            assert!(
                violations.is_empty(),
                "current native proto schema does not match baseline:\n{}\n\n{}",
                format_proto_schema_violations(&violations),
                baseline_update_hint()
            );
        }
    }
}
fn baseline_update_hint() -> String {
    format!(
        "To intentionally update the proto schema ledger, run:\n{}",
        WRITE_BASELINE_COMMAND
    )
}

#[test]
fn proto_schema_write_mode_rejects_missing_baseline_without_bootstrap() {
    let missing_path = std::env::temp_dir().join(format!(
        "novarocks-missing-proto-schema-baseline-{}.json",
        std::process::id()
    ));
    fs::remove_file(&missing_path).ok();
    let current = test_proto_schema(vec![], vec![], vec![]);

    let err = next_proto_schema_baseline_for_write(&current, &missing_path)
        .expect_err("write mode should reject a missing baseline");

    assert!(err.contains("proto schema baseline is missing"), "{err}");
    assert!(err.contains(WRITE_BASELINE_ENV), "{err}");
    assert!(
        !missing_path.exists(),
        "missing-baseline decision test must not write a real baseline"
    );
}

#[test]
fn proto_schema_parser_reports_unclosed_context_statement() {
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
fn proto_schema_parser_rejects_unsupported_tails_and_bad_identifiers() {
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
fn proto_schema_comparator_rejects_version_drift() {
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
fn proto_schema_comparator_rejects_package_drift() {
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
fn proto_schema_comparator_rejects_baseline_file_missing_in_current() {
    let baseline = test_proto_schema_with_files(vec![(
        "idl/novarocks/service.proto",
        test_proto_file("novarocks.test", vec![], vec![], vec![]),
    )]);
    let current = test_proto_schema_with_files(vec![]);

    assert_proto_schema_comparator_rejects(current, baseline, "file removed");
}

#[test]
fn proto_schema_comparator_rejects_current_new_file_as_baseline_stale() {
    let baseline = test_proto_schema_with_files(vec![]);
    let current = test_proto_schema_with_files(vec![(
        "idl/novarocks/new.proto",
        test_proto_file("novarocks.test", vec![], vec![], vec![]),
    )]);

    assert_proto_schema_comparator_rejects(current, baseline, "new file is missing from baseline");
}

#[test]
fn proto_schema_comparator_rejects_new_file_with_service_as_unsafe() {
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
fn proto_schema_comparator_rejects_baseline_message_missing_in_current() {
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
fn proto_schema_comparator_rejects_current_new_message_as_baseline_stale() {
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
fn proto_schema_comparator_rejects_current_new_field_as_baseline_stale() {
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
fn proto_schema_comparator_rejects_field_label_drift() {
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
fn proto_schema_comparator_rejects_field_oneof_drift() {
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
fn proto_schema_comparator_rejects_missing_message_reserved_retention() {
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
fn proto_schema_comparator_rejects_field_reusing_baseline_reserved_number_or_name() {
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
fn proto_schema_comparator_rejects_field_type_change() {
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
fn proto_schema_comparator_rejects_field_rename() {
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
fn proto_schema_comparator_rejects_field_number_reuse() {
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
fn proto_schema_comparator_rejects_deleted_field_without_reserved_number() {
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
fn proto_schema_comparator_rejects_deleted_field_without_reserved_name() {
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
fn proto_schema_comparator_accepts_deleted_field_with_reserved_number_and_name() {
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
fn proto_schema_baseline_merge_preserves_reserved_deleted_field_history() {
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
fn proto_schema_baseline_merge_rejects_deleted_field_without_reserved_name() {
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
fn proto_schema_comparator_rejects_enum_zero_value_drift() {
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
fn proto_schema_comparator_rejects_rpc_signature_change() {
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
fn proto_schema_comparator_rejects_new_service() {
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
fn proto_schema_comparator_rejects_enum_deletion() {
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
fn proto_schema_baseline_merge_rejects_enum_value_deletion() {
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
fn proto_schema_comparator_rejects_enum_renumber() {
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
fn proto_schema_comparator_rejects_enum_rename() {
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
fn proto_schema_comparator_rejects_current_new_enum_value_as_baseline_stale() {
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
fn proto_schema_comparator_rejects_enum_reserved_retention_or_reuse() {
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
fn proto_schema_comparator_rejects_rpc_deletion() {
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
fn proto_schema_baseline_merge_rejects_service_deletion() {
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
fn proto_schema_baseline_merge_rejects_rpc_deletion() {
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
fn proto_schema_baseline_merge_rejects_new_file_with_service() {
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
fn proto_schema_comparator_rejects_current_new_rpc_as_baseline_stale() {
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
fn proto_schema_comparator_returns_stable_sorted_deduped_violations() {
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

#[test]
fn rfd5b_proto_additive_binding_fields_are_exact() {
    let schema = parse_current_novarocks_proto_schema().expect("parse current native proto schema");
    let plan = &schema.files["idl/novarocks/plan.proto"];

    let fragment_field = &plan.messages["PlanFragment"].fields[&10];
    assert_eq!(fragment_field.name, "runtime_filter_bindings");
    assert_eq!(fragment_field.type_name, "RuntimeFilterBindingTable");
    assert_eq!(fragment_field.label, "singular");

    let node_field = &plan.messages["DistributedNode"].fields[&9];
    assert_eq!(node_field.name, "runtime_filter_binding_ids");
    assert_eq!(node_field.type_name, "uint32");
    assert_eq!(node_field.label, "repeated");

    for (number, name, type_name) in [
        (6, "build_runtime_filters", "RuntimeFilterBuild"),
        (7, "probe_runtime_filters", "RuntimeFilterProbe"),
    ] {
        let transitional = &plan.messages["DistributedNode"].fields[&number];
        assert_eq!(transitional.name, name);
        assert_eq!(transitional.type_name, type_name);
    }
    let transitional_join = &plan.messages["HashJoinNode"].fields[&6];
    assert_eq!(transitional_join.name, "build_runtime_filters");
    assert_eq!(transitional_join.type_name, "RuntimeFilterBuildIntent");

    let table = &plan.messages["RuntimeFilterBindingTable"];
    assert_eq!(table.fields[&1].name, "fragment_id");
    assert_eq!(table.fields[&2].name, "bindings");
    assert_eq!(table.fields[&2].label, "repeated");
    assert_eq!(
        plan.messages["RuntimeFilterOrderedContract"].fields[&1].type_name,
        "RuntimeFilterOrderKey"
    );
    assert_eq!(
        plan.messages["RuntimeFilterOrderedContract"].fields[&1].label,
        "repeated"
    );
    assert_eq!(
        plan.messages["RuntimeFilterConsumerActivation"].fields[&1]
            .oneof
            .as_deref(),
        Some("kind")
    );
    assert_eq!(
        plan.messages["RuntimeFilterConsumerActivation"].fields[&2]
            .oneof
            .as_deref(),
        Some("kind")
    );
}
