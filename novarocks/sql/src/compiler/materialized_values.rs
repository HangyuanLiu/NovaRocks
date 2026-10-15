// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Explicit source ownership for one already-materialized VALUES relation.
//! No parser token, literal content, alias or provider attribute is an identity.
use super::SqlCompileError;
use novarocks_parser::ast::{self, Visit};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};

/// An exact structural edge in the owned query. These variants add no SQL
/// syntax; they name the existing derived relations used by closed producers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlMaterializedValuesSite {
    SelectFromDerived { from: usize },
    SelectJoinDerived { from: usize, join: usize },
}
impl SqlMaterializedValuesSite {
    fn resolve(self, query: &ast::Query) -> Result<(&ast::Values, usize), SqlCompileError> {
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            return Err(invalid(
                "materialized VALUES site requires its actual SELECT owner",
            ));
        };
        let factor = match self {
            Self::SelectFromDerived { from } => select.from.get(from).map(|item| &item.relation),
            Self::SelectJoinDerived { from, join } => select
                .from
                .get(from)
                .and_then(|item| item.joins.get(join))
                .map(|item| &item.relation),
        }
        .ok_or_else(|| invalid("materialized VALUES site is outside its owned query"))?;
        let ast::TableFactor::Derived {
            subquery, alias, ..
        } = factor
        else {
            return Err(invalid(
                "materialized VALUES site is not a derived relation",
            ));
        };
        let ast::SetExpr::Values(values) = subquery.body.as_ref() else {
            return Err(invalid(
                "materialized VALUES site does not own a VALUES relation",
            ));
        };
        let columns = alias
            .as_ref()
            .ok_or_else(|| invalid("materialized VALUES lacks its declared relation columns"))?
            .columns
            .len();
        Ok((values, columns))
    }
}
fn invalid(detail: &'static str) -> SqlCompileError {
    SqlCompileError::InvalidRequest(detail.to_owned())
}
/// Structural edges of the producer's existing eligible query body. These
/// names describe owned AST positions, never SQL text or wire identities.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlConstantQueryStep {
    NestedQuery,
    UnionLeft,
    UnionRight,
    SelectFromDerived { from: usize },
}
/// Exact expression edges below a producer-authored projection or VALUES cell.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlConstantExpressionStep {
    Nested,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlConstantExpressionSite {
    ValuesCell { row: usize, column: usize },
    SelectProjection { column: usize },
}
#[derive(Clone, Debug)]
pub struct SqlExpressionConstant {
    body_path: Box<[SqlConstantQueryStep]>,
    site: SqlConstantExpressionSite,
    expression_path: Box<[SqlConstantExpressionStep]>,
    value: novarocks_functions::ConstantValue,
}
impl SqlExpressionConstant {
    /// The source producer supplies the path after its final AST wrapper.
    pub fn new(
        body_path: Vec<SqlConstantQueryStep>,
        site: SqlConstantExpressionSite,
        value: novarocks_functions::ConstantValue,
    ) -> Self {
        Self {
            body_path: body_path.into_boxed_slice(),
            site,
            expression_path: Box::default(),
            value,
        }
    }
    /// Preserve the actual nested syntax callbacks outside this exact source.
    pub fn with_expression_path(mut self, path: Vec<SqlConstantExpressionStep>) -> Self {
        self.expression_path = path.into_boxed_slice();
        self
    }
    pub fn value(&self) -> &novarocks_functions::ConstantValue {
        &self.value
    }
    fn resolve<'a>(
        &self,
        query: &'a ast::Query,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<&'a ast::Expr, SqlCompileError> {
        let mut body = query.body.as_ref();
        for step in &self.body_path {
            work.step()?;
            body = match (step, body) {
                (SqlConstantQueryStep::NestedQuery, ast::SetExpr::Query(query)) => {
                    query.body.as_ref()
                }
                (SqlConstantQueryStep::UnionLeft, ast::SetExpr::SetOperation(op))
                    if matches!(op.operator, ast::SetOperator::Union)
                        && matches!(op.quantifier, ast::SetQuantifier::All) =>
                {
                    op.left.as_ref()
                }
                (SqlConstantQueryStep::UnionRight, ast::SetExpr::SetOperation(op))
                    if matches!(op.operator, ast::SetOperator::Union)
                        && matches!(op.quantifier, ast::SetQuantifier::All) =>
                {
                    op.right.as_ref()
                }
                (
                    SqlConstantQueryStep::SelectFromDerived { from },
                    ast::SetExpr::Select(select),
                ) => {
                    let Some(ast::TableFactor::Derived { subquery, .. }) =
                        select.from.get(*from).map(|from| &from.relation)
                    else {
                        return Err(invalid(
                            "constant source path differs from its owned derived relation",
                        ));
                    };
                    subquery.body.as_ref()
                }
                _ => {
                    return Err(invalid(
                        "constant source path differs from its owned query body",
                    ));
                }
            };
        }
        work.step()?;
        let mut expression = match (self.site, body) {
            (
                SqlConstantExpressionSite::ValuesCell { row, column },
                ast::SetExpr::Values(values),
            ) => values.rows.get(row).and_then(|row| row.get(column)),
            (
                SqlConstantExpressionSite::SelectProjection { column },
                ast::SetExpr::Select(select),
            ) => select.projection.get(column).and_then(|item| match item {
                ast::SelectItem::UnnamedExpr(expr)
                | ast::SelectItem::ExprWithAlias { expr, .. } => Some(expr),
                _ => None,
            }),
            _ => None,
        }
        .ok_or_else(|| invalid("constant source expression is outside its owned query"))?;
        for step in &self.expression_path {
            work.step()?;
            expression = match (step, expression) {
                (SqlConstantExpressionStep::Nested, ast::Expr::Nested(nested)) => {
                    &nested.expression
                }
                _ => {
                    return Err(invalid(
                        "constant source expression path differs from its owned syntax",
                    ));
                }
            };
        }
        Ok(expression)
    }
}
pub type SqlExpressionConstants = std::sync::Arc<[SqlExpressionConstant]>;

/// Each constant retains its actual immutable pool and complete value type.
/// Complete COW rows and producer-authored expressions are distinct roles.
#[derive(Clone, Debug)]
pub(crate) enum MaterializedValues {
    Complete {
        site: SqlMaterializedValuesSite,
        rows: std::sync::Arc<[Box<[novarocks_functions::ConstantValue]>]>,
    },
    Expressions {
        constants: SqlExpressionConstants,
    },
}
/// The lookup is local to one actual analysis/query borrow. It stores no
/// process-global pointer identity, query owner, provider or capability.
pub(crate) struct MaterializedSourceBorrow<'a> {
    complete: Option<(
        &'a ast::Values,
        &'a [Box<[novarocks_functions::ConstantValue]>],
    )>,
    expressions: std::collections::BTreeMap<usize, novarocks_functions::ConstantValue>,
}
impl MaterializedSourceBorrow<'_> {
    pub(crate) fn view(&self) -> MaterializedValuesLoan<'_> {
        MaterializedValuesLoan {
            complete: self.complete,
            expressions: &self.expressions,
        }
    }
}
#[derive(Clone, Copy)]
pub(crate) struct MaterializedValuesLoan<'a> {
    complete: Option<(
        &'a ast::Values,
        &'a [Box<[novarocks_functions::ConstantValue]>],
    )>,
    expressions: &'a std::collections::BTreeMap<usize, novarocks_functions::ConstantValue>,
}
impl<'a> MaterializedValuesLoan<'a> {
    pub(crate) fn rows_at(
        self,
        values: &ast::Values,
    ) -> Option<&'a [Box<[novarocks_functions::ConstantValue]>]> {
        self.complete
            .and_then(|(original, rows)| std::ptr::eq(original, values).then_some(rows))
    }
    pub(crate) fn constant_at(
        self,
        expr: &ast::Expr,
    ) -> Option<&'a novarocks_functions::ConstantValue> {
        self.expressions.get(&(std::ptr::from_ref(expr) as usize))
    }
}
impl MaterializedValues {
    pub(crate) fn try_new(
        query: &ast::Query,
        site: SqlMaterializedValuesSite,
        rows: std::sync::Arc<[Box<[novarocks_functions::ConstantValue]>]>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, SqlCompileError> {
        let source = Self::Complete { site, rows };
        source.loan(query, control)?;
        Ok(source)
    }
    pub(crate) fn try_new_expressions(
        query: &ast::Query,
        constants: SqlExpressionConstants,
        control: &dyn PureCompileControl,
    ) -> Result<Self, SqlCompileError> {
        let source = Self::Expressions { constants };
        source.loan(query, control)?;
        Ok(source)
    }
    pub(crate) fn loan<'a>(
        &'a self,
        query: &'a ast::Query,
        control: &dyn PureCompileControl,
    ) -> Result<MaterializedSourceBorrow<'a>, SqlCompileError> {
        match self {
            Self::Complete { site, rows } => Self::complete_loan(query, *site, rows, control),
            Self::Expressions { constants } => {
                let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
                work.step()?;
                if constants.is_empty() {
                    return Err(invalid("constant source has no authored expressions"));
                }
                let mut expressions = std::collections::BTreeMap::new();
                for constant in constants.iter() {
                    let expr = constant.resolve(query, &mut work)?;
                    novarocks_type_contract::visit_value_type_observed(
                        constant.value().value_type().value_type(),
                        |_| work.step().map_err(SqlCompileError::from),
                    )?;
                    if expressions
                        .insert(std::ptr::from_ref(expr) as usize, constant.value().clone())
                        .is_some()
                    {
                        return Err(invalid(
                            "constant source repeats its owned expression occurrence",
                        ));
                    }
                }
                work.finish()?;
                Ok(MaterializedSourceBorrow {
                    complete: None,
                    expressions,
                })
            }
        }
    }
    fn complete_loan<'a>(
        query: &'a ast::Query,
        site: SqlMaterializedValuesSite,
        rows: &'a [Box<[novarocks_functions::ConstantValue]>],
        control: &dyn PureCompileControl,
    ) -> Result<MaterializedSourceBorrow<'a>, SqlCompileError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        work.step()?;
        let (values, declared_width) = site.resolve(query)?;
        work.flush()?;
        // The original AST visitor is one opaque, read-only operation. Control
        // is observed before and after; no cooperative visitor claim is made.
        let mut count = ValuesPresence::default();
        count.visit_query(query);
        work.flush()?;
        if !count.found || count.repeated {
            return Err(invalid(
                "materialized query must own exactly one VALUES source",
            ));
        }
        if !values.rows.is_empty() || rows.is_empty() {
            return Err(invalid(
                "materialized VALUES requires an empty owned syntax relation and checked rows",
            ));
        }
        let width = rows[0].len();
        if width == 0 || declared_width != width {
            return Err(invalid("materialized VALUES relation has no columns"));
        }
        for row in rows.iter() {
            work.step()?;
            if row.len() != width {
                return Err(invalid("materialized VALUES rows have inconsistent width"));
            }
            for value in row.iter() {
                novarocks_type_contract::visit_value_type_observed(
                    value.value_type().value_type(),
                    |_| work.step().map_err(SqlCompileError::from),
                )?;
            }
        }
        work.finish()?;
        Ok(MaterializedSourceBorrow {
            complete: Some((values, rows)),
            expressions: std::collections::BTreeMap::new(),
        })
    }
}
#[derive(Default)]
struct ValuesPresence {
    found: bool,
    repeated: bool,
}
impl ValuesPresence {
    fn body(&mut self, body: &ast::SetExpr) {
        match body {
            ast::SetExpr::Values(_) => {
                self.repeated |= self.found;
                self.found = true;
            }
            ast::SetExpr::SetOperation(op) => {
                self.body(&op.left);
                self.body(&op.right);
            }
            // Nested Query is visited by the same original visitor exactly once.
            _ => {}
        }
    }
}
impl Visit for ValuesPresence {
    fn visit_query(&mut self, query: &ast::Query) {
        self.body(&query.body);
        ast::walk_query(self, query);
    }
}

#[cfg(test)]
#[path = "materialized_values_tests.rs"]
mod tests;
