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

use anyhow::Result;
use proc_macro2::{Group, TokenStream, TokenTree};
use quote::quote;
use syn::visit::Visit;
use syn::{Item, ItemMacro, ItemMod, ItemUse, Macro, UseTree};

use crate::Violation;
use crate::cfg::analyze_attrs;
use crate::module_graph::{ModuleGraph, SourceUnit};
use crate::production::{item_attrs, node_is_production};

type Scope = Vec<String>;
type AliasMap = BTreeMap<(Scope, String), Vec<String>>;

fn path_segments(path: &syn::Path) -> Vec<String> {
    path.segments
        .iter()
        .map(|segment| segment.ident.to_string())
        .collect()
}

fn canonicalize(path: &[String], scope: &[String], aliases: &AliasMap) -> Vec<String> {
    if path.is_empty() {
        return Vec::new();
    }
    let mut index = 0usize;
    let mut base = if path[0] == "crate" {
        index = 1;
        vec!["crate".to_string()]
    } else {
        let mut owner_scope = scope.to_vec();
        while path.get(index).is_some_and(|segment| segment == "self") {
            index += 1;
        }
        while path.get(index).is_some_and(|segment| segment == "super") {
            owner_scope.pop();
            index += 1;
        }
        if let Some(owner) = path.get(index) {
            if let Some(target) = aliases.get(&(owner_scope.clone(), owner.clone())) {
                let mut resolved = target.clone();
                resolved.extend_from_slice(&path[index + 1..]);
                return canonicalize(&resolved, &owner_scope, aliases);
            }
        }
        owner_scope
    };
    base.extend_from_slice(&path[index..]);
    base
}

fn expand_use_tree(
    tree: &UseTree,
    prefix: &mut Vec<String>,
    output: &mut Vec<(Vec<String>, String)>,
) {
    match tree {
        UseTree::Path(path) => {
            prefix.push(path.ident.to_string());
            expand_use_tree(&path.tree, prefix, output);
            prefix.pop();
        }
        UseTree::Name(name) => {
            let mut path = prefix.clone();
            if name.ident != "self" {
                path.push(name.ident.to_string());
            }
            output.push((path, name.ident.to_string()));
        }
        UseTree::Rename(rename) => {
            let mut path = prefix.clone();
            if rename.ident != "self" {
                path.push(rename.ident.to_string());
            }
            output.push((path, rename.rename.to_string()));
        }
        UseTree::Group(group) => {
            for item in &group.items {
                expand_use_tree(item, prefix, output);
            }
        }
        UseTree::Glob(_) => {}
    }
}

struct AliasCollector<'a> {
    scope: Scope,
    aliases: &'a mut AliasMap,
}

impl AliasCollector<'_> {
    fn visit_items(&mut self, items: &[Item]) {
        for item in items {
            if !node_is_production(item_attrs(item)) {
                continue;
            }
            match item {
                Item::Use(item) => self.record_use(item),
                Item::ExternCrate(item) => {
                    let local = item
                        .rename
                        .as_ref()
                        .map(|(_, ident)| ident.to_string())
                        .unwrap_or_else(|| item.ident.to_string());
                    let target = if item.ident == "self" {
                        vec!["crate".to_string()]
                    } else {
                        vec![item.ident.to_string()]
                    };
                    self.aliases.insert((self.scope.clone(), local), target);
                }
                Item::Mod(module) => {
                    if let Some((_, items)) = &module.content {
                        self.scope.push(module.ident.to_string());
                        self.visit_items(items);
                        self.scope.pop();
                    }
                }
                _ => {}
            }
        }
    }

    fn record_use(&mut self, item: &ItemUse) {
        let mut expanded = Vec::new();
        expand_use_tree(&item.tree, &mut Vec::new(), &mut expanded);
        for (path, local) in expanded {
            let target = canonicalize(&path, &self.scope, self.aliases);
            self.aliases.insert((self.scope.clone(), local), target);
        }
    }
}

#[derive(Clone)]
struct MacroArm {
    matcher: Group,
    transcriber: Group,
    forwards_macro: bool,
}

#[derive(Clone)]
struct MacroDefinition {
    scope: Scope,
    arms: Vec<MacroArm>,
}

#[derive(Clone)]
struct MacroInvocation {
    scope: Scope,
    path: Vec<String>,
    arguments: TokenStream,
}

#[derive(Default)]
struct MacroInventory {
    definitions: BTreeMap<Vec<String>, MacroDefinition>,
    invocations: Vec<MacroInvocation>,
}

fn macro_arms(tokens: TokenStream) -> Vec<MacroArm> {
    let tokens = tokens.into_iter().collect::<Vec<_>>();
    let mut arms = Vec::new();
    let mut index = 0usize;
    while index + 3 < tokens.len() {
        let TokenTree::Group(matcher) = &tokens[index] else {
            index += 1;
            continue;
        };
        let arrow = matches!(
            (&tokens[index + 1], &tokens[index + 2]),
            (TokenTree::Punct(left), TokenTree::Punct(right))
                if left.as_char() == '=' && right.as_char() == '>'
        );
        let TokenTree::Group(transcriber) = &tokens[index + 3] else {
            index += 1;
            continue;
        };
        if arrow {
            arms.push(MacroArm {
                matcher: matcher.clone(),
                transcriber: transcriber.clone(),
                forwards_macro: token_stream_has_metavariable_invocation(transcriber.stream()),
            });
            index += 4;
        } else {
            index += 1;
        }
    }
    arms
}

fn normalized(tokens: &TokenStream) -> String {
    tokens.to_string().split_whitespace().collect()
}

fn token_stream_has_metavariable_invocation(tokens: TokenStream) -> bool {
    let tokens = tokens.into_iter().collect::<Vec<_>>();
    for (index, token) in tokens.iter().enumerate() {
        if let TokenTree::Group(group) = token
            && token_stream_has_metavariable_invocation(group.stream())
        {
            return true;
        }
        if index + 2 < tokens.len()
            && matches!(&tokens[index], TokenTree::Punct(punct) if punct.as_char() == '$')
            && matches!(&tokens[index + 1], TokenTree::Ident(_))
            && matches!(&tokens[index + 2], TokenTree::Punct(punct) if punct.as_char() == '!')
        {
            return true;
        }
    }
    false
}

fn token_stream_contains_dollar(tokens: TokenStream) -> bool {
    tokens.into_iter().any(|token| match token {
        TokenTree::Group(group) => token_stream_contains_dollar(group.stream()),
        TokenTree::Punct(punct) => punct.as_char() == '$',
        _ => false,
    })
}

fn matcher_is_structurally_supported(tokens: TokenStream) -> bool {
    let tokens = tokens.into_iter().collect::<Vec<_>>();
    let mut index = 0usize;
    while index < tokens.len() {
        if let TokenTree::Group(group) = &tokens[index] {
            if token_stream_contains_dollar(group.stream()) {
                return false;
            }
            index += 1;
            continue;
        }
        if !matches!(&tokens[index], TokenTree::Punct(punct) if punct.as_char() == '$') {
            index += 1;
            continue;
        }
        if index + 3 >= tokens.len()
            || !matches!(&tokens[index + 1], TokenTree::Ident(_))
            || !matches!(&tokens[index + 2], TokenTree::Punct(punct) if punct.as_char() == ':')
            || !matches!(&tokens[index + 3], TokenTree::Ident(fragment)
                if matches!(fragment.to_string().as_str(), "ident" | "path" | "tt"))
        {
            return false;
        }
        index += 4;
    }
    true
}

enum ArmMatch {
    Matched(BTreeMap<String, TokenStream>),
    NoMatch,
    Unsupported,
}

fn token_equal(left: &TokenTree, right: &TokenTree) -> bool {
    normalized(&TokenStream::from(left.clone())) == normalized(&TokenStream::from(right.clone()))
}

fn match_tokens(
    matcher: &[TokenTree],
    matcher_index: usize,
    arguments: &[TokenTree],
    argument_index: usize,
    bindings: &mut BTreeMap<String, TokenStream>,
) -> ArmMatch {
    if matcher_index == matcher.len() {
        return if argument_index == arguments.len() {
            ArmMatch::Matched(bindings.clone())
        } else {
            ArmMatch::NoMatch
        };
    }
    if matcher_index + 3 < matcher.len()
        && matches!(&matcher[matcher_index], TokenTree::Punct(punct) if punct.as_char() == '$')
        && matches!(&matcher[matcher_index + 1], TokenTree::Ident(_))
        && matches!(&matcher[matcher_index + 2], TokenTree::Punct(punct) if punct.as_char() == ':')
        && matches!(&matcher[matcher_index + 3], TokenTree::Ident(_))
    {
        let TokenTree::Ident(name) = &matcher[matcher_index + 1] else {
            unreachable!()
        };
        let TokenTree::Ident(fragment) = &matcher[matcher_index + 3] else {
            unreachable!()
        };
        let name = name.to_string();
        let fragment = fragment.to_string();
        let lengths = match fragment.as_str() {
            "ident" if matches!(arguments.get(argument_index), Some(TokenTree::Ident(_))) => {
                vec![1]
            }
            "tt" if argument_index < arguments.len() => vec![1],
            "path" => (1..=arguments.len().saturating_sub(argument_index))
                .rev()
                .filter(|length| {
                    syn::parse2::<syn::Path>(
                        arguments[argument_index..argument_index + length]
                            .iter()
                            .cloned()
                            .collect(),
                    )
                    .is_ok()
                })
                .collect(),
            "ident" | "tt" => Vec::new(),
            _ => return ArmMatch::Unsupported,
        };
        if fragment == "path" && lengths.is_empty() && argument_index < arguments.len() {
            return ArmMatch::Unsupported;
        }
        for length in lengths {
            let replacement = arguments[argument_index..argument_index + length]
                .iter()
                .cloned()
                .collect();
            let previous = bindings.insert(name.clone(), replacement);
            match match_tokens(
                matcher,
                matcher_index + 4,
                arguments,
                argument_index + length,
                bindings,
            ) {
                matched @ ArmMatch::Matched(_) => return matched,
                ArmMatch::Unsupported => return ArmMatch::Unsupported,
                ArmMatch::NoMatch => {}
            }
            if let Some(previous) = previous {
                bindings.insert(name.clone(), previous);
            } else {
                bindings.remove(&name);
            }
        }
        return ArmMatch::NoMatch;
    }
    if matches!(&matcher[matcher_index], TokenTree::Punct(punct) if punct.as_char() == '$') {
        return ArmMatch::Unsupported;
    }
    let Some(argument) = arguments.get(argument_index) else {
        return ArmMatch::NoMatch;
    };
    if !token_equal(&matcher[matcher_index], argument) {
        return ArmMatch::NoMatch;
    }
    match_tokens(
        matcher,
        matcher_index + 1,
        arguments,
        argument_index + 1,
        bindings,
    )
}

fn match_arm(arm: &MacroArm, arguments: TokenStream) -> ArmMatch {
    if !matcher_is_structurally_supported(arm.matcher.stream()) {
        return ArmMatch::Unsupported;
    }
    match_tokens(
        &arm.matcher.stream().into_iter().collect::<Vec<_>>(),
        0,
        &arguments.into_iter().collect::<Vec<_>>(),
        0,
        &mut BTreeMap::new(),
    )
}

fn substitute(tokens: TokenStream, bindings: &BTreeMap<String, TokenStream>) -> TokenStream {
    let tokens = tokens.into_iter().collect::<Vec<_>>();
    let mut output = TokenStream::new();
    let mut index = 0usize;
    while index < tokens.len() {
        if index + 1 < tokens.len()
            && matches!(&tokens[index], TokenTree::Punct(punct) if punct.as_char() == '$')
            && matches!(&tokens[index + 1], TokenTree::Ident(_))
        {
            let TokenTree::Ident(ident) = &tokens[index + 1] else {
                unreachable!()
            };
            if let Some(replacement) = bindings.get(&ident.to_string()) {
                output.extend(replacement.clone());
                index += 2;
                continue;
            }
        }
        match &tokens[index] {
            TokenTree::Group(group) => {
                let mut replacement =
                    Group::new(group.delimiter(), substitute(group.stream(), bindings));
                replacement.set_span(group.span());
                output.extend([TokenTree::Group(replacement)]);
            }
            token => output.extend([token.clone()]),
        }
        index += 1;
    }
    output
}

fn token_path(tokens: &[TokenTree]) -> Option<Vec<String>> {
    let mut path = Vec::new();
    let mut index = 0usize;
    while index < tokens.len() {
        let TokenTree::Ident(ident) = &tokens[index] else {
            return None;
        };
        path.push(ident.to_string());
        index += 1;
        if index == tokens.len() {
            break;
        }
        if index + 1 >= tokens.len()
            || !matches!(&tokens[index], TokenTree::Punct(punct) if punct.as_char() == ':')
            || !matches!(&tokens[index + 1], TokenTree::Punct(punct) if punct.as_char() == ':')
        {
            return None;
        }
        index += 2;
    }
    (!path.is_empty()).then_some(path)
}

fn macro_definition_key(
    inventory: &MacroInventory,
    scope: &[String],
    path: &[String],
    aliases: &AliasMap,
) -> Option<Vec<String>> {
    let canonical = canonicalize(path, scope, aliases);
    if inventory.definitions.contains_key(&canonical) {
        return Some(canonical);
    }
    if path.len() == 1 {
        let name = path.first()?;
        for depth in (1..=scope.len()).rev() {
            let mut key = scope[..depth].to_vec();
            key.push(name.clone());
            if inventory.definitions.contains_key(&key) {
                return Some(key);
            }
        }
    }
    None
}

fn path_is_include(path: &[String], scope: &[String], aliases: &AliasMap) -> bool {
    canonicalize(path, scope, aliases)
        .last()
        .is_some_and(|segment| segment == "include")
}

fn token_stream_mentions_include_argument(
    tokens: TokenStream,
    scope: &[String],
    aliases: &AliasMap,
) -> bool {
    let tokens = tokens.into_iter().collect::<Vec<_>>();
    for (index, token) in tokens.iter().enumerate() {
        if let TokenTree::Group(group) = token
            && token_stream_mentions_include_argument(group.stream(), scope, aliases)
        {
            return true;
        }
        if !matches!(token, TokenTree::Ident(_)) {
            continue;
        }
        for end in (index + 1..=tokens.len()).rev() {
            let Some(path) = token_path(&tokens[index..end]) else {
                continue;
            };
            if path_is_include(&path, scope, aliases) {
                return true;
            }
        }
    }
    false
}

fn expanded_tokens_reach_include(
    tokens: TokenStream,
    scope: &[String],
    aliases: &AliasMap,
    inventory: &MacroInventory,
    active: &mut BTreeSet<Vec<String>>,
) -> bool {
    let tokens = tokens.into_iter().collect::<Vec<_>>();
    for index in 0..tokens.len() {
        if let TokenTree::Group(group) = &tokens[index]
            && expanded_tokens_reach_include(group.stream(), scope, aliases, inventory, active)
        {
            return true;
        }
        let TokenTree::Punct(bang) = &tokens[index] else {
            continue;
        };
        if bang.as_char() != '!' || index + 1 >= tokens.len() {
            continue;
        }
        let TokenTree::Group(arguments) = &tokens[index + 1] else {
            continue;
        };
        let mut start = index;
        while start > 0 {
            match &tokens[start - 1] {
                TokenTree::Ident(_) | TokenTree::Punct(_) => start -= 1,
                _ => break,
            }
        }
        let Some(path) = token_path(&tokens[start..index]) else {
            continue;
        };
        if path_is_include(&path, scope, aliases) {
            return true;
        }
        if token_stream_mentions_include_argument(arguments.stream(), scope, aliases) {
            return true;
        }
        let Some(key) = macro_definition_key(inventory, scope, &path, aliases) else {
            continue;
        };
        if !active.insert(key.clone()) {
            continue;
        }
        let definition = &inventory.definitions[&key];
        for arm in &definition.arms {
            let bindings = match match_arm(arm, arguments.stream()) {
                ArmMatch::Matched(bindings) => bindings,
                ArmMatch::NoMatch => continue,
                ArmMatch::Unsupported if arm.forwards_macro => return true,
                ArmMatch::Unsupported => continue,
            };
            let expanded = substitute(arm.transcriber.stream(), &bindings);
            if expanded_tokens_reach_include(
                expanded,
                &definition.scope,
                aliases,
                inventory,
                active,
            ) {
                return true;
            }
        }
        active.remove(&key);
    }
    false
}

fn invocation_reaches_include(
    invocation: &MacroInvocation,
    aliases: &AliasMap,
    inventory: &MacroInventory,
) -> bool {
    if token_stream_mentions_include_argument(
        invocation.arguments.clone(),
        &invocation.scope,
        aliases,
    ) {
        return true;
    }
    let Some(key) = macro_definition_key(inventory, &invocation.scope, &invocation.path, aliases)
    else {
        return false;
    };
    let definition = &inventory.definitions[&key];
    for arm in &definition.arms {
        let bindings = match match_arm(arm, invocation.arguments.clone()) {
            ArmMatch::Matched(bindings) => bindings,
            ArmMatch::NoMatch => continue,
            ArmMatch::Unsupported if arm.forwards_macro => return true,
            ArmMatch::Unsupported => continue,
        };
        if expanded_tokens_reach_include(
            substitute(arm.transcriber.stream(), &bindings),
            &definition.scope,
            aliases,
            inventory,
            &mut BTreeSet::from([key.clone()]),
        ) {
            return true;
        }
    }
    false
}

struct InventoryVisitor<'a> {
    scope: Scope,
    inventory: &'a mut MacroInventory,
}

impl InventoryVisitor<'_> {
    fn record_macro(&mut self, item: &Macro) {
        if item.path.is_ident("macro_rules") {
            return;
        }
        self.inventory.invocations.push(MacroInvocation {
            scope: self.scope.clone(),
            path: path_segments(&item.path),
            arguments: item.tokens.clone(),
        });
    }
}

impl<'ast> Visit<'ast> for InventoryVisitor<'_> {
    crate::production::production_pruning_methods!();

    fn visit_item_mod(&mut self, item: &'ast ItemMod) {
        self.scope.push(item.ident.to_string());
        syn::visit::visit_item_mod(self, item);
        self.scope.pop();
    }

    fn visit_item_macro(&mut self, item: &'ast ItemMacro) {
        if item.mac.path.is_ident("macro_rules") {
            let Some(name) = item.ident.as_ref() else {
                return;
            };
            let definition = MacroDefinition {
                scope: self.scope.clone(),
                arms: macro_arms(item.mac.tokens.clone()),
            };
            let mut key = self.scope.clone();
            key.push(name.to_string());
            self.inventory.definitions.insert(key, definition.clone());
            if item
                .attrs
                .iter()
                .any(|attribute| attribute.path().is_ident("macro_export"))
            {
                self.inventory
                    .definitions
                    .insert(vec!["crate".to_string(), name.to_string()], definition);
            }
        } else {
            self.record_macro(&item.mac);
        }
    }

    fn visit_macro(&mut self, item: &'ast Macro) {
        self.record_macro(item);
        syn::visit::visit_macro(self, item);
    }
}

struct RetirementVisitor<'a> {
    source: String,
    scope: Scope,
    aliases: &'a AliasMap,
    violations: &'a mut Vec<Violation>,
}

impl RetirementVisitor<'_> {
    fn restricted_scope(&self) -> bool {
        self.scope == ["crate"]
            || self
                .scope
                .starts_with(&["crate".to_string(), "sql".to_string()])
    }

    fn check_path(&mut self, path: &syn::Path) {
        let canonical = canonicalize(&path_segments(path), &self.scope, self.aliases);
        if canonical.starts_with(&[
            "crate".to_string(),
            "sql".to_string(),
            "codegen".to_string(),
        ]) {
            self.violations.push(Violation::new(
                &self.source,
                "reaches the retired SQL encoder namespace",
            ));
        }
    }
}

impl<'ast> Visit<'ast> for RetirementVisitor<'_> {
    crate::production::production_pruning_methods!();

    fn visit_item_mod(&mut self, item: &'ast ItemMod) {
        let analysis =
            analyze_attrs(&item.attrs).expect("module attributes were validated before traversal");
        if analysis
            .path_values
            .iter()
            .zip(&analysis.path_conditions)
            .any(|(path, activation)| {
                analysis
                    .item_condition
                    .clone()
                    .and(activation.clone())
                    .production_possible()
                    .expect("module attributes were validated before traversal")
                    && path.replace('\\', "/").contains("codegen")
            })
        {
            self.violations.push(Violation::new(
                &self.source,
                format!(
                    "redirects module `{}` to retired codegen source",
                    item.ident
                ),
            ));
        }
        if self.scope == ["crate", "sql"] && item.ident == "codegen" {
            self.violations.push(Violation::new(
                &self.source,
                "declares retired codegen module",
            ));
        }
        self.scope.push(item.ident.to_string());
        syn::visit::visit_item_mod(self, item);
        self.scope.pop();
    }

    fn visit_item_use(&mut self, item: &'ast ItemUse) {
        let mut expanded = Vec::new();
        expand_use_tree(&item.tree, &mut Vec::new(), &mut expanded);
        for (path, local) in expanded {
            if self.scope == ["crate", "sql"] && local == "codegen" {
                self.violations.push(Violation::new(
                    &self.source,
                    "restores retired codegen module binding",
                ));
            }
            let canonical = canonicalize(&path, &self.scope, self.aliases);
            if canonical.starts_with(&[
                "crate".to_string(),
                "sql".to_string(),
                "codegen".to_string(),
            ]) {
                self.violations.push(Violation::new(
                    &self.source,
                    "imports the retired SQL encoder namespace",
                ));
            }
        }
        syn::visit::visit_item_use(self, item);
    }

    fn visit_item_extern_crate(&mut self, item: &'ast syn::ItemExternCrate) {
        let local = item
            .rename
            .as_ref()
            .map(|(_, ident)| ident.to_string())
            .unwrap_or_else(|| item.ident.to_string());
        if self.scope == ["crate", "sql"] && local == "codegen" {
            self.violations.push(Violation::new(
                &self.source,
                "restores retired codegen extern-crate binding",
            ));
        }
    }

    fn visit_path(&mut self, path: &'ast syn::Path) {
        self.check_path(path);
        syn::visit::visit_path(self, path);
    }

    fn visit_macro(&mut self, item: &'ast Macro) {
        if crate::tokens::contains_path(item.tokens.clone(), &["crate", "sql", "codegen"]) {
            self.violations.push(Violation::new(
                &self.source,
                "production macro tokens reach the retired SQL encoder namespace",
            ));
        }
        if self.restricted_scope() {
            let path = path_segments(&item.path);
            let known_safe = self.scope == ["crate"]
                && item.path.is_ident("include")
                && matches!(
                    normalized(&item.tokens),
                    value if value == normalized(&quote!(concat!(env!("OUT_DIR"), "/thrift_root_mod.rs")))
                        || value == normalized(&quote!(concat!(env!("OUT_DIR"), "/proto_root_mod.rs")))
                );
            if path_is_include(&path, &self.scope, self.aliases) && !known_safe {
                self.violations.push(Violation::new(
                    &self.source,
                    "uses production include! at crate root or inside SQL namespace",
                ));
            }
        }
        syn::visit::visit_macro(self, item);
    }
}

pub(crate) fn audit_graph(graph: &ModuleGraph) -> Result<Vec<Violation>> {
    let mut aliases = AliasMap::new();
    for unit in &graph.units {
        AliasCollector {
            scope: unit.scope.clone(),
            aliases: &mut aliases,
        }
        .visit_items(&unit.file.items);
    }

    let mut violations = Vec::new();
    for unit in &graph.units {
        let source = unit.path.display().to_string();
        RetirementVisitor {
            source,
            scope: unit.scope.clone(),
            aliases: &aliases,
            violations: &mut violations,
        }
        .visit_file(&unit.file);
    }

    let mut inventory = MacroInventory::default();
    for SourceUnit { scope, file, .. } in &graph.units {
        InventoryVisitor {
            scope: scope.clone(),
            inventory: &mut inventory,
        }
        .visit_file(file);
    }
    for invocation in &inventory.invocations {
        let restricted = invocation.scope == ["crate"]
            || invocation
                .scope
                .starts_with(&["crate".to_string(), "sql".to_string()]);
        if !restricted {
            continue;
        }
        if invocation_reaches_include(invocation, &aliases, &inventory) {
            violations.push(Violation::new(
                "<macro inventory>",
                "passes production include! through a macro wrapper",
            ));
        }
    }

    violations.sort();
    violations.dedup();
    Ok(violations)
}
