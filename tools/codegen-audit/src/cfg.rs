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

use anyhow::{Context, Result, bail};
use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::{Attribute, Meta, Token};

#[derive(Clone, Debug)]
pub(crate) enum CfgExpr {
    True,
    Atom(String),
    Not(Box<CfgExpr>),
    All(Vec<CfgExpr>),
    Any(Vec<CfgExpr>),
}

impl CfgExpr {
    pub(crate) fn and(self, other: CfgExpr) -> CfgExpr {
        CfgExpr::All(vec![self, other])
    }

    fn or(self, other: CfgExpr) -> CfgExpr {
        CfgExpr::Any(vec![self, other])
    }

    fn not(self) -> CfgExpr {
        CfgExpr::Not(Box::new(self))
    }

    fn atoms(&self, atoms: &mut BTreeSet<String>) {
        match self {
            CfgExpr::Atom(atom) if atom != "test" => {
                atoms.insert(atom.clone());
            }
            CfgExpr::Not(inner) => inner.atoms(atoms),
            CfgExpr::All(children) | CfgExpr::Any(children) => {
                for child in children {
                    child.atoms(atoms);
                }
            }
            _ => {}
        }
    }

    fn evaluate(&self, values: &BTreeMap<String, bool>) -> bool {
        match self {
            CfgExpr::True => true,
            CfgExpr::Atom(atom) if atom == "test" => false,
            CfgExpr::Atom(atom) => values.get(atom).copied().unwrap_or(false),
            CfgExpr::Not(inner) => !inner.evaluate(values),
            CfgExpr::All(children) => children.iter().all(|child| child.evaluate(values)),
            CfgExpr::Any(children) => children.iter().any(|child| child.evaluate(values)),
        }
    }

    pub(crate) fn production_possible(&self) -> Result<bool> {
        let mut atoms = BTreeSet::new();
        self.atoms(&mut atoms);
        let atoms = atoms.into_iter().collect::<Vec<_>>();
        if atoms.len() > 20 {
            bail!("cfg expression has too many independent atoms");
        }
        Ok((0..(1usize << atoms.len())).any(|mask| {
            let values = atoms
                .iter()
                .enumerate()
                .map(|(index, atom)| (atom.clone(), mask & (1 << index) != 0))
                .collect();
            self.evaluate(&values)
        }))
    }
}

fn meta_list(meta: &Meta) -> Result<Vec<Meta>> {
    let Meta::List(list) = meta else {
        bail!("expected cfg list, got {meta:?}");
    };
    Punctuated::<Meta, Token![,]>::parse_terminated
        .parse2(list.tokens.clone())
        .map(|items| items.into_iter().collect())
        .context("parse cfg meta list")
}

pub(crate) fn cfg_expr(meta: &Meta) -> Result<CfgExpr> {
    match meta {
        Meta::Path(path) => Ok(CfgExpr::Atom(
            path.segments
                .iter()
                .map(|segment| crate::tokens::ident_text(&segment.ident))
                .collect::<Vec<_>>()
                .join("::"),
        )),
        Meta::NameValue(value) => Ok(CfgExpr::Atom(quote::quote!(#value).to_string())),
        Meta::List(list) if list.path.is_ident("all") => Ok(CfgExpr::All(
            meta_list(meta)?
                .iter()
                .map(cfg_expr)
                .collect::<Result<Vec<_>>>()?,
        )),
        Meta::List(list) if list.path.is_ident("any") => Ok(CfgExpr::Any(
            meta_list(meta)?
                .iter()
                .map(cfg_expr)
                .collect::<Result<Vec<_>>>()?,
        )),
        Meta::List(list) if list.path.is_ident("not") => {
            let children = meta_list(meta)?;
            if children.len() != 1 {
                bail!("cfg(not(...)) requires one predicate");
            }
            Ok(cfg_expr(&children[0])?.not())
        }
        _ => Ok(CfgExpr::Atom(quote::quote!(#meta).to_string())),
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AttrAnalysis {
    pub(crate) item_condition: CfgExpr,
    pub(crate) path_conditions: Vec<CfgExpr>,
    pub(crate) path_values: Vec<String>,
}

impl Default for AttrAnalysis {
    fn default() -> Self {
        Self {
            item_condition: CfgExpr::True,
            path_conditions: Vec::new(),
            path_values: Vec::new(),
        }
    }
}

fn path_value(meta: &Meta) -> Option<String> {
    let Meta::NameValue(value) = meta else {
        return None;
    };
    if !value.path.is_ident("path") {
        return None;
    }
    let syn::Expr::Lit(expression) = &value.value else {
        return Some("<non-literal>".to_string());
    };
    let syn::Lit::Str(value) = &expression.lit else {
        return Some("<non-string>".to_string());
    };
    Some(value.value())
}

fn apply_meta(meta: &Meta, activation: CfgExpr, analysis: &mut AttrAnalysis) -> Result<()> {
    if let Some(path) = path_value(meta) {
        analysis.path_conditions.push(activation);
        analysis.path_values.push(path);
        return Ok(());
    }
    let Meta::List(list) = meta else {
        return Ok(());
    };
    if list.path.is_ident("cfg") {
        let predicates = meta_list(meta)?;
        if predicates.len() != 1 {
            bail!("cfg attribute requires one predicate");
        }
        let predicate = cfg_expr(&predicates[0])?;
        analysis.item_condition = analysis
            .item_condition
            .clone()
            .and(activation.clone().not().or(predicate));
        return Ok(());
    }
    if list.path.is_ident("cfg_attr") {
        let mut items = meta_list(meta)?;
        if items.len() < 2 {
            bail!("cfg_attr requires a predicate and generated attributes");
        }
        let predicate = cfg_expr(&items.remove(0))?;
        let generated_activation = activation.and(predicate);
        for generated in items {
            apply_meta(&generated, generated_activation.clone(), analysis)?;
        }
    }
    Ok(())
}

pub(crate) fn analyze_attrs(attrs: &[Attribute]) -> Result<AttrAnalysis> {
    let mut analysis = AttrAnalysis::default();
    for attribute in attrs {
        apply_meta(&attribute.meta, CfgExpr::True, &mut analysis)?;
    }
    Ok(analysis)
}

pub(crate) fn production_possible(attrs: &[Attribute]) -> Result<bool> {
    analyze_attrs(attrs)?.item_condition.production_possible()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attrs(source: &str) -> Vec<Attribute> {
        syn::Attribute::parse_outer
            .parse_str(source)
            .expect("parse attributes")
    }

    #[test]
    fn generated_cfg_test_excludes_production() {
        assert!(!production_possible(&attrs("#[cfg_attr(not(test), cfg(test))]")).unwrap());
    }

    #[test]
    fn nested_path_activation_respects_predicate_conjunction() {
        let analysis = analyze_attrs(&attrs(
            "#[cfg_attr(test, cfg_attr(not(test), path = \"never.rs\"))]",
        ))
        .unwrap();
        assert!(
            !analysis.path_conditions[0]
                .clone()
                .and(analysis.item_condition)
                .production_possible()
                .unwrap()
        );
    }
}
