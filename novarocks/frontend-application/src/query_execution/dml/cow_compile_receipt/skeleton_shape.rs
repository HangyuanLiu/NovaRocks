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

// No strings/vectors/AST are built to count their prospective allocations.
use super::borrowed_value_footprint::Failure;
use novarocks_parser::ast::*;
use std::mem::size_of;
type Result<T> = std::result::Result<T, Failure>;
fn add(a: u64, b: u64) -> Result<u64> {
    a.checked_add(b).ok_or(Failure::ResourceExhausted)
}
fn mul(a: u64, b: u64) -> Result<u64> {
    a.checked_mul(b).ok_or(Failure::ResourceExhausted)
}
fn slots<T>(n: u64) -> Result<u64> {
    mul(n, size_of::<T>() as u64)
}

#[derive(Clone, Copy)]
pub struct Expression {
    pub owned: u64,
}
impl Expression {
    fn root(heap: u64) -> Result<Self> {
        Ok(Self {
            owned: add(size_of::<Expr>() as u64, heap)?,
        })
    }
    pub fn heap(self) -> Result<u64> {
        self.owned
            .checked_sub(size_of::<Expr>() as u64)
            .ok_or(Failure::InvalidSource("COW Expr shape"))
    }
    pub fn literal(payload_bytes: u64) -> Result<Self> {
        Self::root(payload_bytes)
    }
    pub fn column(alias_bytes: u64, field_bytes: u64) -> Result<Self> {
        Self::root(add(slots::<Ident>(2)?, add(alias_bytes, field_bytes)?)?)
    }
    pub fn cast(inner: Self, type_heap: u64) -> Result<Self> {
        Self::root(add(inner.owned, type_heap)?)
    }
    pub fn unary(inner: Self) -> Result<Self> {
        Self::root(inner.owned)
    }
    pub fn binary(left: Self, right: Self) -> Result<Self> {
        Self::root(add(left.owned, right.owned)?)
    }
    pub fn is_null(inner: Self) -> Result<Self> {
        Self::root(inner.owned)
    }
    pub fn case(condition: Self, result: Self, otherwise: Self) -> Result<Self> {
        // Conditions/results exact Vec<Expr>(1) slots are their root Exprs;
        // else Box<Expr> likewise charges the same root pointee once.
        Self::root(add(add(condition.owned, result.owned)?, otherwise.owned)?)
    }
}

/// The actual SelectItem root is charged by the projection vector below.
pub fn item_heap(expr: Expression, alias_bytes: u64) -> Result<u64> {
    add(expr.heap()?, alias_bytes)
}
#[derive(Clone, Copy)]
pub struct Factor {
    pub heap: u64,
}
impl Factor {
    pub fn derived_values(
        rows: u64,
        all_cells_roots_and_heaps: u64,
        alias_bytes: u64,
        width: u64,
        all_column_name_bytes: u64,
    ) -> Result<Self> {
        // TableFactor root is inside TableWithJoins/Join, not a new Box.
        // Its child Query and SetExpr ARE two separate heap allocations.
        let values_query = add(
            add(size_of::<Query>() as u64, size_of::<SetExpr>() as u64)?,
            add(slots::<Vec<Expr>>(rows)?, all_cells_roots_and_heaps)?,
        )?;
        Ok(Self {
            heap: add(
                add(values_query, alias_bytes)?,
                add(slots::<Ident>(width)?, all_column_name_bytes)?,
            )?,
        })
    }
    pub fn table(
        catalog_bytes: u64,
        namespace_bytes: u64,
        table_bytes: u64,
        alias_bytes: u64,
    ) -> Result<Self> {
        Ok(Self {
            heap: add(
                slots::<Ident>(3)?,
                add(
                    add(catalog_bytes, namespace_bytes)?,
                    add(table_bytes, alias_bytes)?,
                )?,
            )?,
        })
    }
}

/// Exact closed SELECT allocations. Empty hints/groups/windows/modifiers
/// allocate zero; the paired constructor and deep walk enforce that fact.
pub fn query_owned(
    projection_count: u64,
    projection_heap_sum: u64,
    from: Factor,
    left_join: Option<(Factor, Expression)>,
    selection: Option<Expression>,
) -> Result<u64> {
    let mut total = add(
        add(
            add(size_of::<Query>() as u64, size_of::<SetExpr>() as u64)?,
            size_of::<Select>() as u64,
        )?,
        add(slots::<SelectItem>(projection_count)?, projection_heap_sum)?,
    )?;
    total = add(total, add(slots::<TableWithJoins>(1)?, from.heap)?)?;
    if let Some((factor, on)) = left_join {
        // On(Expr) is inline inside JoinConstraint, itself inline in Join.
        total = add(
            total,
            add(add(slots::<Join>(1)?, factor.heap)?, on.heap()?)?,
        )?;
    }
    if let Some(where_expr) = selection {
        total = add(total, where_expr.heap()?)?;
    }
    Ok(total)
}

pub fn generated_value_alias_bytes(ordinal: u64) -> u64 {
    let mut n = ordinal;
    let mut digits = 1;
    while n >= 10 {
        n /= 10;
        digits += 1;
    }
    "__nr_v_".len() as u64 + digits
}
