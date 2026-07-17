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

use anyhow::{Result, bail};
use syn::visit::Visit;
use syn::{
    Arm, Attribute, BareFnArg, BareVariadic, Expr, Field, FieldPat, FieldValue, ForeignItem,
    GenericParam, ImplItem, Item, Pat, Receiver, Stmt, TraitItem, Variadic, Variant,
};

use crate::cfg::{CfgExpr, analyze_attrs, production_possible};

pub(crate) fn item_attrs(item: &Item) -> Option<&[Attribute]> {
    match item {
        Item::Const(item) => Some(&item.attrs),
        Item::Enum(item) => Some(&item.attrs),
        Item::ExternCrate(item) => Some(&item.attrs),
        Item::Fn(item) => Some(&item.attrs),
        Item::ForeignMod(item) => Some(&item.attrs),
        Item::Impl(item) => Some(&item.attrs),
        Item::Macro(item) => Some(&item.attrs),
        Item::Mod(item) => Some(&item.attrs),
        Item::Static(item) => Some(&item.attrs),
        Item::Struct(item) => Some(&item.attrs),
        Item::Trait(item) => Some(&item.attrs),
        Item::TraitAlias(item) => Some(&item.attrs),
        Item::Type(item) => Some(&item.attrs),
        Item::Union(item) => Some(&item.attrs),
        Item::Use(item) => Some(&item.attrs),
        Item::Verbatim(_) => None,
        _ => None,
    }
}

pub(crate) fn impl_item_attrs(item: &ImplItem) -> Option<&[Attribute]> {
    match item {
        ImplItem::Const(item) => Some(&item.attrs),
        ImplItem::Fn(item) => Some(&item.attrs),
        ImplItem::Macro(item) => Some(&item.attrs),
        ImplItem::Type(item) => Some(&item.attrs),
        ImplItem::Verbatim(_) => None,
        _ => None,
    }
}

pub(crate) fn trait_item_attrs(item: &TraitItem) -> Option<&[Attribute]> {
    match item {
        TraitItem::Const(item) => Some(&item.attrs),
        TraitItem::Fn(item) => Some(&item.attrs),
        TraitItem::Macro(item) => Some(&item.attrs),
        TraitItem::Type(item) => Some(&item.attrs),
        TraitItem::Verbatim(_) => None,
        _ => None,
    }
}

pub(crate) fn foreign_item_attrs(item: &ForeignItem) -> Option<&[Attribute]> {
    match item {
        ForeignItem::Fn(item) => Some(&item.attrs),
        ForeignItem::Macro(item) => Some(&item.attrs),
        ForeignItem::Static(item) => Some(&item.attrs),
        ForeignItem::Type(item) => Some(&item.attrs),
        ForeignItem::Verbatim(_) => None,
        _ => None,
    }
}

pub(crate) fn stmt_attrs(stmt: &Stmt) -> Option<&[Attribute]> {
    match stmt {
        Stmt::Local(local) => Some(&local.attrs),
        Stmt::Item(item) => item_attrs(item),
        Stmt::Expr(expr, _) => expr_attrs(expr),
        Stmt::Macro(item) => Some(&item.attrs),
    }
}

pub(crate) fn expr_attrs(expr: &Expr) -> Option<&[Attribute]> {
    match expr {
        Expr::Array(expr) => Some(&expr.attrs),
        Expr::Assign(expr) => Some(&expr.attrs),
        Expr::Async(expr) => Some(&expr.attrs),
        Expr::Await(expr) => Some(&expr.attrs),
        Expr::Binary(expr) => Some(&expr.attrs),
        Expr::Block(expr) => Some(&expr.attrs),
        Expr::Break(expr) => Some(&expr.attrs),
        Expr::Call(expr) => Some(&expr.attrs),
        Expr::Cast(expr) => Some(&expr.attrs),
        Expr::Closure(expr) => Some(&expr.attrs),
        Expr::Const(expr) => Some(&expr.attrs),
        Expr::Continue(expr) => Some(&expr.attrs),
        Expr::Field(expr) => Some(&expr.attrs),
        Expr::ForLoop(expr) => Some(&expr.attrs),
        Expr::Group(expr) => Some(&expr.attrs),
        Expr::If(expr) => Some(&expr.attrs),
        Expr::Index(expr) => Some(&expr.attrs),
        Expr::Infer(expr) => Some(&expr.attrs),
        Expr::Let(expr) => Some(&expr.attrs),
        Expr::Lit(expr) => Some(&expr.attrs),
        Expr::Loop(expr) => Some(&expr.attrs),
        Expr::Macro(expr) => Some(&expr.attrs),
        Expr::Match(expr) => Some(&expr.attrs),
        Expr::MethodCall(expr) => Some(&expr.attrs),
        Expr::Paren(expr) => Some(&expr.attrs),
        Expr::Path(expr) => Some(&expr.attrs),
        Expr::Range(expr) => Some(&expr.attrs),
        Expr::RawAddr(expr) => Some(&expr.attrs),
        Expr::Reference(expr) => Some(&expr.attrs),
        Expr::Repeat(expr) => Some(&expr.attrs),
        Expr::Return(expr) => Some(&expr.attrs),
        Expr::Struct(expr) => Some(&expr.attrs),
        Expr::Try(expr) => Some(&expr.attrs),
        Expr::TryBlock(expr) => Some(&expr.attrs),
        Expr::Tuple(expr) => Some(&expr.attrs),
        Expr::Unary(expr) => Some(&expr.attrs),
        Expr::Unsafe(expr) => Some(&expr.attrs),
        Expr::Verbatim(_) => None,
        Expr::While(expr) => Some(&expr.attrs),
        Expr::Yield(expr) => Some(&expr.attrs),
        _ => None,
    }
}

pub(crate) fn pat_attrs(pat: &Pat) -> Option<&[Attribute]> {
    match pat {
        Pat::Const(pat) => Some(&pat.attrs),
        Pat::Ident(pat) => Some(&pat.attrs),
        Pat::Lit(pat) => Some(&pat.attrs),
        Pat::Macro(pat) => Some(&pat.attrs),
        Pat::Or(pat) => Some(&pat.attrs),
        Pat::Paren(pat) => Some(&pat.attrs),
        Pat::Path(pat) => Some(&pat.attrs),
        Pat::Range(pat) => Some(&pat.attrs),
        Pat::Reference(pat) => Some(&pat.attrs),
        Pat::Rest(pat) => Some(&pat.attrs),
        Pat::Slice(pat) => Some(&pat.attrs),
        Pat::Struct(pat) => Some(&pat.attrs),
        Pat::Tuple(pat) => Some(&pat.attrs),
        Pat::TupleStruct(pat) => Some(&pat.attrs),
        Pat::Type(pat) => Some(&pat.attrs),
        Pat::Verbatim(_) => None,
        Pat::Wild(pat) => Some(&pat.attrs),
        _ => None,
    }
}

pub(crate) fn generic_param_attrs(param: &GenericParam) -> &[Attribute] {
    match param {
        GenericParam::Lifetime(param) => &param.attrs,
        GenericParam::Type(param) => &param.attrs,
        GenericParam::Const(param) => &param.attrs,
    }
}

pub(crate) fn combined_condition(parent: &CfgExpr, attrs: Option<&[Attribute]>) -> CfgExpr {
    parent.clone().and(
        analyze_attrs(attrs.unwrap_or_default())
            .expect("production attributes were validated before traversal")
            .item_condition,
    )
}

#[derive(Default)]
struct Validator {
    errors: Vec<String>,
}

impl Validator {
    fn validate(&mut self, kind: &str, attrs: Option<&[Attribute]>) {
        let Some(attrs) = attrs else {
            self.errors
                .push(format!("{kind} contains syntax not modeled by syn"));
            return;
        };
        if let Err(error) = production_possible(attrs) {
            self.errors
                .push(format!("{kind} has an unsupported cfg condition: {error}"));
        }
    }
}

macro_rules! validate_node {
    ($self:ident, $kind:literal, $attrs:expr, $visit:path, $node:ident) => {{
        $self.validate($kind, $attrs);
        $visit($self, $node);
    }};
}

impl<'ast> Visit<'ast> for Validator {
    fn visit_item(&mut self, node: &'ast Item) {
        validate_node!(self, "item", item_attrs(node), syn::visit::visit_item, node);
    }

    fn visit_impl_item(&mut self, node: &'ast ImplItem) {
        validate_node!(
            self,
            "impl item",
            impl_item_attrs(node),
            syn::visit::visit_impl_item,
            node
        );
    }

    fn visit_trait_item(&mut self, node: &'ast TraitItem) {
        validate_node!(
            self,
            "trait item",
            trait_item_attrs(node),
            syn::visit::visit_trait_item,
            node
        );
    }

    fn visit_foreign_item(&mut self, node: &'ast ForeignItem) {
        validate_node!(
            self,
            "foreign item",
            foreign_item_attrs(node),
            syn::visit::visit_foreign_item,
            node
        );
    }

    fn visit_stmt(&mut self, node: &'ast Stmt) {
        validate_node!(
            self,
            "statement",
            stmt_attrs(node),
            syn::visit::visit_stmt,
            node
        );
    }

    fn visit_expr(&mut self, node: &'ast Expr) {
        validate_node!(
            self,
            "expression",
            expr_attrs(node),
            syn::visit::visit_expr,
            node
        );
    }

    fn visit_pat(&mut self, node: &'ast Pat) {
        validate_node!(
            self,
            "pattern",
            pat_attrs(node),
            syn::visit::visit_pat,
            node
        );
    }

    fn visit_arm(&mut self, node: &'ast Arm) {
        validate_node!(
            self,
            "match arm",
            Some(&node.attrs),
            syn::visit::visit_arm,
            node
        );
    }

    fn visit_field(&mut self, node: &'ast Field) {
        validate_node!(
            self,
            "field",
            Some(&node.attrs),
            syn::visit::visit_field,
            node
        );
    }

    fn visit_variant(&mut self, node: &'ast Variant) {
        validate_node!(
            self,
            "variant",
            Some(&node.attrs),
            syn::visit::visit_variant,
            node
        );
    }

    fn visit_generic_param(&mut self, node: &'ast GenericParam) {
        validate_node!(
            self,
            "generic parameter",
            Some(generic_param_attrs(node)),
            syn::visit::visit_generic_param,
            node
        );
    }

    fn visit_field_value(&mut self, node: &'ast FieldValue) {
        validate_node!(
            self,
            "field value",
            Some(&node.attrs),
            syn::visit::visit_field_value,
            node
        );
    }

    fn visit_field_pat(&mut self, node: &'ast FieldPat) {
        validate_node!(
            self,
            "field pattern",
            Some(&node.attrs),
            syn::visit::visit_field_pat,
            node
        );
    }

    fn visit_bare_fn_arg(&mut self, node: &'ast BareFnArg) {
        validate_node!(
            self,
            "bare fn argument",
            Some(&node.attrs),
            syn::visit::visit_bare_fn_arg,
            node
        );
    }

    fn visit_bare_variadic(&mut self, node: &'ast BareVariadic) {
        validate_node!(
            self,
            "bare variadic",
            Some(&node.attrs),
            syn::visit::visit_bare_variadic,
            node
        );
    }

    fn visit_receiver(&mut self, node: &'ast Receiver) {
        validate_node!(
            self,
            "receiver",
            Some(&node.attrs),
            syn::visit::visit_receiver,
            node
        );
    }

    fn visit_variadic(&mut self, node: &'ast Variadic) {
        validate_node!(
            self,
            "variadic",
            Some(&node.attrs),
            syn::visit::visit_variadic,
            node
        );
    }
}

pub(crate) fn validate_file(file: &syn::File) -> Result<()> {
    let mut validator = Validator::default();
    validator.visit_file(file);
    if validator.errors.is_empty() {
        Ok(())
    } else {
        bail!("{}", validator.errors.join("; "))
    }
}

macro_rules! production_pruning_methods {
    () => {
        fn visit_item(&mut self, node: &'ast syn::Item) {
            let condition = crate::production::combined_condition(
                &self.condition,
                crate::production::item_attrs(node),
            );
            if condition
                .production_possible()
                .expect("production attributes were validated before traversal")
            {
                let parent = std::mem::replace(&mut self.condition, condition);
                syn::visit::visit_item(self, node);
                self.condition = parent;
            }
        }

        fn visit_impl_item(&mut self, node: &'ast syn::ImplItem) {
            let condition = crate::production::combined_condition(
                &self.condition,
                crate::production::impl_item_attrs(node),
            );
            if condition
                .production_possible()
                .expect("production attributes were validated before traversal")
            {
                let parent = std::mem::replace(&mut self.condition, condition);
                syn::visit::visit_impl_item(self, node);
                self.condition = parent;
            }
        }

        fn visit_trait_item(&mut self, node: &'ast syn::TraitItem) {
            let condition = crate::production::combined_condition(
                &self.condition,
                crate::production::trait_item_attrs(node),
            );
            if condition
                .production_possible()
                .expect("production attributes were validated before traversal")
            {
                let parent = std::mem::replace(&mut self.condition, condition);
                syn::visit::visit_trait_item(self, node);
                self.condition = parent;
            }
        }

        fn visit_foreign_item(&mut self, node: &'ast syn::ForeignItem) {
            let condition = crate::production::combined_condition(
                &self.condition,
                crate::production::foreign_item_attrs(node),
            );
            if condition
                .production_possible()
                .expect("production attributes were validated before traversal")
            {
                let parent = std::mem::replace(&mut self.condition, condition);
                syn::visit::visit_foreign_item(self, node);
                self.condition = parent;
            }
        }

        fn visit_stmt(&mut self, node: &'ast syn::Stmt) {
            let condition = crate::production::combined_condition(
                &self.condition,
                crate::production::stmt_attrs(node),
            );
            if condition
                .production_possible()
                .expect("production attributes were validated before traversal")
            {
                let parent = std::mem::replace(&mut self.condition, condition);
                syn::visit::visit_stmt(self, node);
                self.condition = parent;
            }
        }

        fn visit_expr(&mut self, node: &'ast syn::Expr) {
            let condition = crate::production::combined_condition(
                &self.condition,
                crate::production::expr_attrs(node),
            );
            if condition
                .production_possible()
                .expect("production attributes were validated before traversal")
            {
                let parent = std::mem::replace(&mut self.condition, condition);
                syn::visit::visit_expr(self, node);
                self.condition = parent;
            }
        }

        fn visit_pat(&mut self, node: &'ast syn::Pat) {
            let condition = crate::production::combined_condition(
                &self.condition,
                crate::production::pat_attrs(node),
            );
            if condition
                .production_possible()
                .expect("production attributes were validated before traversal")
            {
                let parent = std::mem::replace(&mut self.condition, condition);
                syn::visit::visit_pat(self, node);
                self.condition = parent;
            }
        }

        fn visit_arm(&mut self, node: &'ast syn::Arm) {
            let condition =
                crate::production::combined_condition(&self.condition, Some(&node.attrs));
            if condition
                .production_possible()
                .expect("production attributes were validated before traversal")
            {
                let parent = std::mem::replace(&mut self.condition, condition);
                syn::visit::visit_arm(self, node);
                self.condition = parent;
            }
        }

        fn visit_field(&mut self, node: &'ast syn::Field) {
            let condition =
                crate::production::combined_condition(&self.condition, Some(&node.attrs));
            if condition
                .production_possible()
                .expect("production attributes were validated before traversal")
            {
                let parent = std::mem::replace(&mut self.condition, condition);
                syn::visit::visit_field(self, node);
                self.condition = parent;
            }
        }

        fn visit_variant(&mut self, node: &'ast syn::Variant) {
            let condition =
                crate::production::combined_condition(&self.condition, Some(&node.attrs));
            if condition
                .production_possible()
                .expect("production attributes were validated before traversal")
            {
                let parent = std::mem::replace(&mut self.condition, condition);
                syn::visit::visit_variant(self, node);
                self.condition = parent;
            }
        }

        fn visit_generic_param(&mut self, node: &'ast syn::GenericParam) {
            let condition = crate::production::combined_condition(
                &self.condition,
                Some(crate::production::generic_param_attrs(node)),
            );
            if condition
                .production_possible()
                .expect("production attributes were validated before traversal")
            {
                let parent = std::mem::replace(&mut self.condition, condition);
                syn::visit::visit_generic_param(self, node);
                self.condition = parent;
            }
        }

        fn visit_field_value(&mut self, node: &'ast syn::FieldValue) {
            let condition =
                crate::production::combined_condition(&self.condition, Some(&node.attrs));
            if condition
                .production_possible()
                .expect("production attributes were validated before traversal")
            {
                let parent = std::mem::replace(&mut self.condition, condition);
                syn::visit::visit_field_value(self, node);
                self.condition = parent;
            }
        }

        fn visit_field_pat(&mut self, node: &'ast syn::FieldPat) {
            let condition =
                crate::production::combined_condition(&self.condition, Some(&node.attrs));
            if condition
                .production_possible()
                .expect("production attributes were validated before traversal")
            {
                let parent = std::mem::replace(&mut self.condition, condition);
                syn::visit::visit_field_pat(self, node);
                self.condition = parent;
            }
        }

        fn visit_bare_fn_arg(&mut self, node: &'ast syn::BareFnArg) {
            let condition =
                crate::production::combined_condition(&self.condition, Some(&node.attrs));
            if condition
                .production_possible()
                .expect("production attributes were validated before traversal")
            {
                let parent = std::mem::replace(&mut self.condition, condition);
                syn::visit::visit_bare_fn_arg(self, node);
                self.condition = parent;
            }
        }

        fn visit_bare_variadic(&mut self, node: &'ast syn::BareVariadic) {
            let condition =
                crate::production::combined_condition(&self.condition, Some(&node.attrs));
            if condition
                .production_possible()
                .expect("production attributes were validated before traversal")
            {
                let parent = std::mem::replace(&mut self.condition, condition);
                syn::visit::visit_bare_variadic(self, node);
                self.condition = parent;
            }
        }

        fn visit_receiver(&mut self, node: &'ast syn::Receiver) {
            let condition =
                crate::production::combined_condition(&self.condition, Some(&node.attrs));
            if condition
                .production_possible()
                .expect("production attributes were validated before traversal")
            {
                let parent = std::mem::replace(&mut self.condition, condition);
                syn::visit::visit_receiver(self, node);
                self.condition = parent;
            }
        }

        fn visit_variadic(&mut self, node: &'ast syn::Variadic) {
            let condition =
                crate::production::combined_condition(&self.condition, Some(&node.attrs));
            if condition
                .production_possible()
                .expect("production attributes were validated before traversal")
            {
                let parent = std::mem::replace(&mut self.condition, condition);
                syn::visit::visit_variadic(self, node);
                self.condition = parent;
            }
        }
    };
}

pub(crate) use production_pruning_methods;
