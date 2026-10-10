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

// This walk is NOT the initial allocation authorization.
use super::borrowed_value_footprint::Failure;
use novarocks_parser::ast::*;
use std::mem::size_of;
type Result<T> = std::result::Result<T, Failure>;
fn add(a: u64, b: u64) -> Result<u64> {
    a.checked_add(b).ok_or(Failure::ResourceExhausted)
}
fn exact_vec<T>(v: &Vec<T>) -> Result<u64> {
    if v.capacity() != v.len() {
        return Err(Failure::InvalidSource("COW AST nonexact Vec capacity"));
    }
    (v.capacity() as u64)
        .checked_mul(size_of::<T>() as u64)
        .ok_or(Failure::ResourceExhausted)
}
fn string(s: &String) -> Result<u64> {
    if s.capacity() != s.len() {
        return Err(Failure::InvalidSource("COW AST nonexact String capacity"));
    }
    Ok(s.capacity() as u64)
}
fn ident(i: &Ident) -> Result<u64> {
    string(&i.value)
}
fn idents(v: &Vec<Ident>) -> Result<u64> {
    v.iter().try_fold(exact_vec(v)?, |n, i| add(n, ident(i)?))
}
fn object(n: &ObjectName) -> Result<u64> {
    idents(&n.parts)
}
fn literal(l: &Literal) -> Result<u64> {
    match &l.kind {
        LiteralKind::Null | LiteralKind::Boolean(_) => Ok(0),
        LiteralKind::Number(s) | LiteralKind::String(s) | LiteralKind::HexString(s) => string(s),
    }
}
fn ty(t: &TypeName) -> Result<u64> {
    let mut n = add(
        add(object(&t.name)?, exact_vec(&t.arguments)?)?,
        exact_vec(&t.argument_separator_spaces)?,
    )?;
    for a in &t.arguments {
        n = add(
            n,
            match a {
                TypeNameArgument::Literal(l) => literal(l)?,
                TypeNameArgument::Type(t) => ty(t)?,
                TypeNameArgument::Field(f) => add(ident(&f.name)?, ty(&f.data_type)?)?,
            },
        )?;
    }
    Ok(n)
}
fn boxed_expr(e: &Expr) -> Result<u64> {
    add(size_of::<Expr>() as u64, expr(e)?)
}
fn exprs(v: &Vec<Expr>) -> Result<u64> {
    v.iter().try_fold(exact_vec(v)?, |n, e| add(n, expr(e)?))
}
fn expr(e: &Expr) -> Result<u64> {
    match e {
        Expr::Identifier(i) => ident(i),
        Expr::CompoundIdentifier(i) => idents(&i.parts),
        Expr::Literal(l) => literal(l),
        Expr::Unary(u) => boxed_expr(&u.expression),
        Expr::Binary(b) => add(boxed_expr(&b.left)?, boxed_expr(&b.right)?),
        Expr::Nested(n) => boxed_expr(&n.expression),
        Expr::IsPredicate(p) => boxed_expr(&p.expr),
        Expr::Cast(c) if c.format.is_none() => add(boxed_expr(&c.expr)?, ty(&c.data_type)?),
        Expr::Array(a) if a.element_type.is_none() => exprs(&a.elements),
        Expr::FunctionCall(f)
            if matches!(f.quantifier, FunctionQuantifier::None)
                && f.order_by.is_empty()
                && f.order_by.capacity() == 0
                && f.separator.is_none()
                && f.filter.is_none()
                && f.null_treatment.is_none()
                && f.over.is_none()
                && !f.substring_from_syntax =>
        {
            add(object(&f.name)?, exprs(&f.arguments)?)
        }
        Expr::Case(c) => {
            let mut n = add(exprs(&c.conditions)?, exprs(&c.results)?)?;
            if let Some(e) = &c.operand {
                n = add(n, boxed_expr(e)?)?;
            }
            if let Some(e) = &c.else_result {
                n = add(n, boxed_expr(e)?)?;
            }
            Ok(n)
        }
        _ => Err(Failure::InvalidSource(
            "AST outside closed COW generated grammar",
        )),
    }
}
fn alias(a: &Option<TableAlias>) -> Result<u64> {
    match a {
        None => Ok(0),
        Some(a) => add(ident(&a.name)?, idents(&a.columns)?),
    }
}
fn factor(f: &TableFactor) -> Result<u64> {
    match f {
        TableFactor::Table {
            name,
            metadata: None,
            alias: a,
            version: None,
            hints,
            ..
        } if hints.is_empty() && hints.capacity() == 0 => add(object(name)?, alias(a)?),
        TableFactor::Derived {
            lateral: false,
            subquery,
            hints,
            alias: a,
            ..
        } if hints.is_empty() && hints.capacity() == 0 => add(query_owned(subquery)?, alias(a)?),
        _ => Err(Failure::InvalidSource(
            "COW generated table/version grammar",
        )),
    }
}
fn select(s: &Select) -> Result<u64> {
    if !s.hints.is_empty()
        || s.hints.capacity() != 0
        || !matches!(s.quantifier, SelectQuantifier::None)
        || !matches!(s.group_by, GroupBy::None)
        || s.having.is_some()
        || s.qualify.is_some()
        || !s.windows.is_empty()
        || s.windows.capacity() != 0
    {
        return Err(Failure::InvalidSource("COW generated Select grammar"));
    }
    let mut n = add(exact_vec(&s.projection)?, exact_vec(&s.from)?)?;
    for item in &s.projection {
        n = add(
            n,
            match item {
                SelectItem::ExprWithAlias {
                    expr: e, alias: i, ..
                } => add(expr(e)?, ident(i)?)?,
                SelectItem::UnnamedExpr(e) => expr(e)?,
                _ => return Err(Failure::InvalidSource("COW generated projection grammar")),
            },
        )?;
    }
    for from in &s.from {
        n = add(add(n, factor(&from.relation)?)?, exact_vec(&from.joins)?)?;
        for join in &from.joins {
            if !matches!(join.operator, JoinOperator::LeftOuter) {
                return Err(Failure::InvalidSource("COW generated LEFT JOIN operator"));
            }
            let JoinConstraint::On(on) = &join.constraint else {
                return Err(Failure::InvalidSource("COW generated join grammar"));
            };
            n = add(add(n, factor(&join.relation)?)?, expr(on)?)?;
        }
    }
    if let Some(e) = &s.selection {
        n = add(n, expr(e)?)?;
    }
    Ok(n)
}

/// Root Query inline + every actual owned block. The same topology/capacity
/// walk bounds the existing compiler Clone graphs when their exact construction
/// contract is retained; no extra arbitrary per-cell coefficient is used.
pub fn query_owned(q: &Query) -> Result<u64> {
    if q.with.is_some()
        || !q.order_by.is_empty()
        || q.order_by.capacity() != 0
        || q.limit.is_some()
        || q.offset.is_some()
        || q.fetch.is_some()
        || q.limit_comma_offset
    {
        return Err(Failure::InvalidSource("COW generated Query grammar"));
    }
    let body = match q.body.as_ref() {
        SetExpr::Select(s) => add(size_of::<Select>() as u64, select(s)?)?,
        SetExpr::Values(v) => v
            .rows
            .iter()
            .try_fold(exact_vec(&v.rows)?, |n, row| add(n, exprs(row)?))?,
        _ => return Err(Failure::InvalidSource("COW generated SetExpr grammar")),
    };
    add(
        add(size_of::<Query>() as u64, size_of::<SetExpr>() as u64)?,
        body,
    )
}
