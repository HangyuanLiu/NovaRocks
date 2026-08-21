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

//! SQL-owned typed query preparation performed before catalog lookup.
//!
//! This module deliberately consumes and returns `novarocks_parser` nodes.
//! It must never rebuild SQL text or invoke a parser.  T4 wires
//! [`preanalyze`] into the production typed-query admission path.

use std::collections::HashMap;

use novarocks_parser::{
    Span,
    ast::{
        BinaryOperator, Expr, Fold, GroupBy, Ident, JoinConstraint, JoinOperator, LiteralKind,
        Query, Select, SelectHintValue, SelectItem, SetExpr, SetOperation, SetOperator,
        SetQuantifier, TableAlias, TableFactor, TableWithJoins, Visit,
    },
};

const DEFAULT_RECURSIVE_CTE_MAX_DEPTH: usize = 5;

/// Applies syntax-complete, catalog-independent query semantics.
///
/// This is intentionally not connected to an analyzer entry point yet.  The
/// contract is owned here first so the SQLP-6 cutover can flip every producer
/// and consumer together without a raw-text bridge.
pub(crate) fn preanalyze(mut query: Query) -> Result<Query, String> {
    let max_depth = recursive_cte_max_depth(&query).unwrap_or(DEFAULT_RECURSIVE_CTE_MAX_DEPTH);
    rewrite_nested_queries(&mut query, max_depth.max(1))?;
    normalize_cross_joins(&mut query);

    let assignments = collect_user_variable_assignments(&query)?;
    if !assignments.is_empty() {
        query = UserVariableSubstituter { assignments }.fold_query(query);
    }

    remove_bare_top_level_dual(&mut query);
    Ok(query)
}

fn recursive_cte_max_depth(query: &Query) -> Option<usize> {
    let select = root_select(query.body.as_ref())?;
    select.hints.iter().find_map(|hint| {
        let value = if hint
            .name
            .value
            .eq_ignore_ascii_case("recursive_cte_max_depth")
        {
            let SelectHintValue::Assignment { value } = &hint.value else {
                return None;
            };
            value
        } else if hint.name.value.eq_ignore_ascii_case("set_var") {
            let SelectHintValue::Call { arguments } = &hint.value else {
                return None;
            };
            arguments.iter().find_map(|argument| {
                let Expr::Binary(binary) = argument else {
                    return None;
                };
                let Expr::Identifier(name) = binary.left.as_ref() else {
                    return None;
                };
                (binary.operator == BinaryOperator::Equal
                    && name.value.eq_ignore_ascii_case("recursive_cte_max_depth"))
                .then_some(binary.right.as_ref())
            })?
        } else {
            return None;
        };
        let Expr::Literal(value) = value else {
            return None;
        };
        let LiteralKind::Number(value) = &value.kind else {
            return None;
        };
        value.parse::<usize>().ok().map(|depth| depth.max(1))
    })
}

fn root_select(body: &SetExpr) -> Option<&Select> {
    match body {
        SetExpr::Select(select) => Some(select),
        SetExpr::Query(query) => root_select(query.body.as_ref()),
        SetExpr::Values(_) | SetExpr::SetOperation(_) => None,
    }
}

fn rewrite_nested_queries(query: &mut Query, max_depth: usize) -> Result<(), String> {
    struct RecursiveUnroller {
        max_depth: usize,
        error: Option<String>,
    }

    impl Fold for RecursiveUnroller {
        fn fold_query(&mut self, mut query: Query) -> Query {
            // The default fold reaches every nested `Query`, including those in
            // expressions and table-function arguments, before this query is
            // unrolled. This preserves the legacy deepest-first traversal.
            query = novarocks_parser::ast::fold_query(self, query);
            if self.error.is_none()
                && let Some(with) = query.with.as_mut()
                && with.recursive
                && let Err(error) = unroll_with_clause(with, self.max_depth)
            {
                self.error = Some(error);
            }
            query
        }
    }

    let mut unroller = RecursiveUnroller {
        max_depth,
        error: None,
    };
    let rewritten = unroller.fold_query(query.clone());
    if let Some(error) = unroller.error {
        return Err(error);
    }
    *query = rewritten;
    Ok(())
}

fn unroll_with_clause(
    with: &mut novarocks_parser::ast::With,
    max_depth: usize,
) -> Result<(), String> {
    let originals = std::mem::take(&mut with.ctes);
    let mut rewritten = Vec::with_capacity(originals.len());
    for cte in originals {
        if let Some(cte) = try_unroll_cte(&cte, max_depth)? {
            rewritten.push(cte);
        } else {
            rewritten.push(cte);
        }
    }
    with.ctes = rewritten;
    Ok(())
}

fn try_unroll_cte(
    cte: &novarocks_parser::ast::Cte,
    max_depth: usize,
) -> Result<Option<novarocks_parser::ast::Cte>, String> {
    let name = cte.name.value.to_ascii_lowercase();
    let union_chain = extract_union_chain(cte.query.body.as_ref());
    if matches!(union_chain, Some(Err(())))
        && set_expr_references_table(cte.query.body.as_ref(), &name)
    {
        // ADR-0088: a mixed chain that is actually recursive is known before
        // catalog materialization and therefore cannot fall through to it.
        return Err("unsupported recursive CTE shape: mixed UNION quantifiers".to_owned());
    }
    let Some(Ok((quantifier, operands))) = union_chain else {
        return Ok(None);
    };
    let anchor = &operands[0];
    let recursive_parts = &operands[1..];
    if set_expr_references_table(anchor, &name) {
        // ADR-0088: an anchor cannot self-reference. Reject the shape before
        // catalog resolution even when no later UNION operand references it.
        return Err(format!("unknown table: {}", cte.name.value));
    }
    let self_references = recursive_parts
        .iter()
        .any(|part| set_expr_references_table(part, &name));
    if !self_references {
        return Ok(None);
    }

    let aliases = derive_column_aliases(cte, anchor)?;
    let mut iterations = Vec::with_capacity(max_depth);
    let mut anchor = anchor.clone();
    pin_projection_aliases(&mut anchor, &aliases);
    iterations.push(query_with_body(cte.query.as_ref(), anchor));

    for index in 1..max_depth {
        let previous = iterations[index - 1].clone();
        let mut parts = recursive_parts.to_vec();
        for part in &mut parts {
            substitute_table_with_derived(part, &name, &previous, &aliases);
            pin_projection_aliases(part, &aliases);
        }
        iterations.push(query_with_body(
            cte.query.as_ref(),
            build_set_operation_chain(quantifier, &parts),
        ));
    }

    let branches = iterations
        .into_iter()
        .enumerate()
        .map(|(index, query)| inline_iteration(query, &name, index, &aliases, cte.span))
        .collect::<Vec<_>>();
    let mut rewritten = cte.clone();
    rewritten.query = Box::new(query_with_body(
        cte.query.as_ref(),
        build_set_operation_chain(SetQuantifier::All, &branches),
    ));
    Ok(Some(rewritten))
}

fn extract_union_chain(expr: &SetExpr) -> Option<Result<(SetQuantifier, Vec<SetExpr>), ()>> {
    fn visit(
        expr: &SetExpr,
        quantifier: &mut Option<SetQuantifier>,
        parts: &mut Vec<SetExpr>,
        mixed: &mut bool,
    ) {
        if let SetExpr::SetOperation(operation) = expr {
            if operation.operator != SetOperator::Union {
                parts.push(expr.clone());
                return;
            }
            match quantifier {
                Some(existing) if *existing != operation.quantifier => *mixed = true,
                Some(_) => {}
                None => *quantifier = Some(operation.quantifier),
            }
            visit(operation.left.as_ref(), quantifier, parts, mixed);
            visit(operation.right.as_ref(), quantifier, parts, mixed);
        } else {
            parts.push(expr.clone());
        }
    }

    let mut quantifier = None;
    let mut parts = Vec::new();
    let mut mixed = false;
    visit(expr, &mut quantifier, &mut parts, &mut mixed);
    if parts.len() < 2 {
        None
    } else if mixed {
        Some(Err(()))
    } else {
        quantifier.map(|quantifier| Ok((quantifier, parts)))
    }
}

fn derive_column_aliases(
    cte: &novarocks_parser::ast::Cte,
    anchor: &SetExpr,
) -> Result<Vec<Ident>, String> {
    if !cte.columns.is_empty() {
        return Ok(cte.columns.clone());
    }
    let Some(select) = root_select(anchor) else {
        return Err("recursive CTE anchor must be a SELECT statement".to_owned());
    };
    Ok(select
        .projection
        .iter()
        .enumerate()
        .map(|(index, item)| {
            projection_alias(item)
                .unwrap_or_else(|| synthetic_ident(format!("__nr_rec_col_{index}"), cte.span))
        })
        .collect())
}

fn projection_alias(item: &SelectItem) -> Option<Ident> {
    match item {
        SelectItem::ExprWithAlias { alias, .. } => Some(alias.clone()),
        SelectItem::UnnamedExpr(Expr::Identifier(ident)) => Some(ident.clone()),
        SelectItem::UnnamedExpr(Expr::CompoundIdentifier(ident)) => ident.parts.last().cloned(),
        SelectItem::UnnamedExpr(_)
        | SelectItem::Wildcard { .. }
        | SelectItem::QualifiedWildcard { .. } => None,
    }
}

fn pin_projection_aliases(expr: &mut SetExpr, aliases: &[Ident]) {
    match expr {
        SetExpr::Select(select) => {
            for (item, alias) in select.projection.iter_mut().zip(aliases) {
                let replacement = match std::mem::replace(
                    item,
                    SelectItem::Wildcard {
                        options: Default::default(),
                        span: alias.span,
                    },
                ) {
                    SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                        SelectItem::ExprWithAlias {
                            expr,
                            alias: alias.clone(),
                            explicit_as: true,
                            span: alias.span,
                        }
                    }
                    other => other,
                };
                *item = replacement;
            }
        }
        SetExpr::Query(query) => pin_projection_aliases(query.body.as_mut(), aliases),
        SetExpr::SetOperation(operation) => {
            pin_projection_aliases(operation.left.as_mut(), aliases);
            pin_projection_aliases(operation.right.as_mut(), aliases);
        }
        SetExpr::Values(_) => {}
    }
}

fn query_with_body(template: &Query, body: SetExpr) -> Query {
    Query {
        with: None,
        body: Box::new(body),
        order_by: Vec::new(),
        limit: None,
        offset: None,
        limit_comma_offset: false,
        fetch: None,
        span: template.span,
    }
}

fn build_set_operation_chain(quantifier: SetQuantifier, parts: &[SetExpr]) -> SetExpr {
    let mut parts = parts.iter();
    let mut combined = parts.next().expect("recursive CTE has an anchor").clone();
    for part in parts {
        let span = Span::new(combined.span().start(), part.span().end());
        combined = SetExpr::SetOperation(SetOperation {
            left: Box::new(combined),
            operator: SetOperator::Union,
            quantifier,
            right: Box::new(part.clone()),
            span,
        });
    }
    combined
}

fn inline_iteration(
    query: Query,
    cte_name: &str,
    index: usize,
    aliases: &[Ident],
    span: Span,
) -> SetExpr {
    let relation = TableFactor::Derived {
        lateral: false,
        subquery: Box::new(query),
        hints: Vec::new(),
        alias: Some(TableAlias {
            name: synthetic_ident(format!("__nr_rec_{cte_name}_{index}"), span),
            columns: aliases.to_vec(),
            explicit_as: false,
            span,
        }),
        span,
    };
    SetExpr::Select(Box::new(Select {
        hints: Vec::new(),
        quantifier: novarocks_parser::ast::SelectQuantifier::None,
        projection: vec![SelectItem::Wildcard {
            options: Default::default(),
            span,
        }],
        from: vec![TableWithJoins {
            relation,
            joins: Vec::new(),
            span,
        }],
        selection: None,
        group_by: novarocks_parser::ast::GroupBy::None,
        having: None,
        qualify: None,
        windows: Vec::new(),
        span,
    }))
}

fn synthetic_ident(value: String, span: Span) -> Ident {
    Ident {
        value,
        quoted: false,
        quote_style: None,
        span,
    }
}

struct TableReferenceVisitor<'a> {
    target: &'a str,
    found: bool,
}

impl Visit for TableReferenceVisitor<'_> {
    fn visit_query(&mut self, query: &Query) {
        scan_query_for_table_reference(self, query);
    }

    fn visit_table_factor(&mut self, factor: &TableFactor) {
        if let TableFactor::Table { name, .. } = factor
            && name.parts.len() == 1
            && name.parts[0].value.eq_ignore_ascii_case(self.target)
        {
            self.found = true;
        }
        novarocks_parser::ast::walk_table_factor(self, factor);
    }
}

fn set_expr_references_table(expr: &SetExpr, target: &str) -> bool {
    let mut visitor = TableReferenceVisitor {
        target,
        found: false,
    };
    scan_set_expr_for_table_reference(&mut visitor, expr);
    visitor.found
}

fn scan_query_for_table_reference(visitor: &mut TableReferenceVisitor<'_>, query: &Query) {
    let mut outer_target_visible = true;
    if let Some(with) = &query.with {
        for cte in &with.ctes {
            if outer_target_visible {
                if cte.name.value.eq_ignore_ascii_case(visitor.target) {
                    // A same-name nested CTE owns its definition and every
                    // following reference in this WITH scope. It must not be
                    // attributed to the enclosing recursive CTE anchor.
                    outer_target_visible = false;
                } else {
                    scan_query_for_table_reference(visitor, cte.query.as_ref());
                }
            }
        }
    }
    if !outer_target_visible {
        return;
    }
    scan_set_expr_for_table_reference(visitor, query.body.as_ref());
    for order in &query.order_by {
        visitor.visit_expr(&order.expr);
    }
    if let Some(limit) = &query.limit {
        visitor.visit_expr(limit);
    }
    if let Some(offset) = &query.offset {
        visitor.visit_expr(&offset.value);
    }
    if let Some(fetch) = &query.fetch
        && let Some(quantity) = &fetch.quantity
    {
        visitor.visit_expr(quantity);
    }
}

fn scan_set_expr_for_table_reference(visitor: &mut TableReferenceVisitor<'_>, expr: &SetExpr) {
    match expr {
        SetExpr::Select(select) => {
            for hint in &select.hints {
                match &hint.value {
                    SelectHintValue::Bare => {}
                    SelectHintValue::Call { arguments } => {
                        for argument in arguments {
                            visitor.visit_expr(argument);
                        }
                    }
                    SelectHintValue::Assignment { value } => visitor.visit_expr(value),
                }
            }
            if let novarocks_parser::ast::SelectQuantifier::Distinct { on, .. } = &select.quantifier
            {
                for expression in on {
                    visitor.visit_expr(expression);
                }
            }
            for item in &select.projection {
                match item {
                    SelectItem::UnnamedExpr(expression)
                    | SelectItem::ExprWithAlias {
                        expr: expression, ..
                    } => visitor.visit_expr(expression),
                    SelectItem::Wildcard { options, .. }
                    | SelectItem::QualifiedWildcard { options, .. } => {
                        for replacement in &options.replace {
                            visitor.visit_expr(&replacement.expr);
                        }
                    }
                }
            }
            for table in &select.from {
                visitor.visit_table_factor(&table.relation);
                for join in &table.joins {
                    visitor.visit_table_factor(&join.relation);
                    if let JoinConstraint::On(expression) = &join.constraint {
                        visitor.visit_expr(expression);
                    }
                }
            }
            if let Some(selection) = &select.selection {
                visitor.visit_expr(selection);
            }
            match &select.group_by {
                GroupBy::None => {}
                GroupBy::Expressions { expressions, .. }
                | GroupBy::Rollup { expressions, .. }
                | GroupBy::Cube { expressions, .. } => {
                    for expression in expressions {
                        visitor.visit_expr(expression);
                    }
                }
                GroupBy::GroupingSets { sets, .. } => {
                    for set in sets {
                        for expression in set {
                            visitor.visit_expr(expression);
                        }
                    }
                }
            }
            for expression in [&select.having, &select.qualify].into_iter().flatten() {
                visitor.visit_expr(expression);
            }
            for window in &select.windows {
                for expression in &window.specification.partition_by {
                    visitor.visit_expr(expression);
                }
                for order in &window.specification.order_by {
                    visitor.visit_expr(&order.expr);
                }
                if let Some(frame) = &window.specification.window_frame {
                    scan_window_frame_bound_for_table_reference(visitor, &frame.start_bound);
                    if let Some(end_bound) = &frame.end_bound {
                        scan_window_frame_bound_for_table_reference(visitor, end_bound);
                    }
                }
            }
        }
        SetExpr::Values(values) => {
            for row in &values.rows {
                for expression in row {
                    visitor.visit_expr(expression);
                }
            }
        }
        SetExpr::Query(query) => scan_query_for_table_reference(visitor, query),
        SetExpr::SetOperation(operation) => {
            scan_set_expr_for_table_reference(visitor, operation.left.as_ref());
            scan_set_expr_for_table_reference(visitor, operation.right.as_ref());
        }
    }
}

fn scan_window_frame_bound_for_table_reference(
    visitor: &mut TableReferenceVisitor<'_>,
    bound: &novarocks_parser::ast::WindowFrameBound,
) {
    match bound {
        novarocks_parser::ast::WindowFrameBound::Preceding(Some(expression), _)
        | novarocks_parser::ast::WindowFrameBound::Following(Some(expression), _) => {
            visitor.visit_expr(expression)
        }
        _ => {}
    }
}

fn substitute_table_with_derived(
    expr: &mut SetExpr,
    target: &str,
    replacement: &Query,
    aliases: &[Ident],
) {
    struct RecursiveTableSubstituter<'a> {
        target: &'a str,
        replacement: &'a Query,
        aliases: &'a [Ident],
    }

    impl Fold for RecursiveTableSubstituter<'_> {
        fn fold_query(&mut self, mut query: Query) -> Query {
            if let Some(with) = query.with.as_mut() {
                for cte in &mut with.ctes {
                    if cte.name.value.eq_ignore_ascii_case(self.target) {
                        // CTE scope begins at its own declaration. Earlier
                        // sibling CTEs can still reference the outer recursive
                        // source, while this declaration, later siblings, and
                        // the query body resolve the same name locally.
                        return query;
                    }
                    cte.query = Box::new(self.fold_query(*cte.query.clone()));
                }
            }
            novarocks_parser::ast::fold_query(self, query)
        }

        fn fold_table_factor(&mut self, factor: TableFactor) -> TableFactor {
            let factor = novarocks_parser::ast::fold_table_factor(self, factor);
            let is_target = matches!(
                &factor,
                TableFactor::Table { name, .. }
                    if name.parts.len() == 1
                        && name.parts[0].value.eq_ignore_ascii_case(self.target)
            );
            if !is_target {
                return factor;
            }
            let TableFactor::Table { alias, span, .. } = factor else {
                unreachable!("target factor must be a table");
            };
            let alias = alias
                .as_ref()
                .map(|alias| alias.name.clone())
                .unwrap_or_else(|| synthetic_ident(self.target.to_owned(), span));
            TableFactor::Derived {
                lateral: false,
                subquery: Box::new(self.replacement.clone()),
                hints: Vec::new(),
                alias: Some(TableAlias {
                    name: alias,
                    columns: self.aliases.to_vec(),
                    explicit_as: false,
                    span,
                }),
                span,
            }
        }
    }

    let template = Query {
        with: None,
        body: Box::new(expr.clone()),
        order_by: Vec::new(),
        limit: None,
        offset: None,
        limit_comma_offset: false,
        fetch: None,
        span: expr.span(),
    };
    let mut substituter = RecursiveTableSubstituter {
        target,
        replacement,
        aliases,
    };
    *expr = *substituter.fold_query(template).body;
}

fn normalize_cross_joins(query: &mut Query) {
    normalize_cross_set_expr(query.body.as_mut());
    if let Some(with) = query.with.as_mut() {
        for cte in &mut with.ctes {
            normalize_cross_joins(cte.query.as_mut());
        }
    }
}

fn normalize_cross_set_expr(expr: &mut SetExpr) {
    match expr {
        SetExpr::Select(select) => {
            for table in &mut select.from {
                for join in &mut table.joins {
                    if join.operator == JoinOperator::Cross
                        && !matches!(join.constraint, JoinConstraint::None)
                    {
                        join.operator = JoinOperator::InnerExplicit;
                    }
                    normalize_cross_factor(&mut join.relation);
                }
                normalize_cross_factor(&mut table.relation);
            }
        }
        SetExpr::Query(query) => normalize_cross_joins(query),
        SetExpr::SetOperation(operation) => {
            normalize_cross_set_expr(operation.left.as_mut());
            normalize_cross_set_expr(operation.right.as_mut());
        }
        SetExpr::Values(_) => {}
    }
}

fn normalize_cross_factor(factor: &mut TableFactor) {
    match factor {
        TableFactor::Derived { subquery, .. } => normalize_cross_joins(subquery),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            for join in &mut table_with_joins.joins {
                if join.operator == JoinOperator::Cross
                    && !matches!(join.constraint, JoinConstraint::None)
                {
                    join.operator = JoinOperator::InnerExplicit;
                }
                normalize_cross_factor(&mut join.relation);
            }
            normalize_cross_factor(&mut table_with_joins.relation);
        }
        TableFactor::Table { .. }
        | TableFactor::TableFunction { .. }
        | TableFactor::Unnest { .. } => {}
    }
}

fn collect_user_variable_assignments(query: &Query) -> Result<HashMap<String, Expr>, String> {
    fn collect(query: &Query, assignments: &mut HashMap<String, Expr>) -> Result<(), String> {
        fn collect_set_expr(
            expr: &SetExpr,
            assignments: &mut HashMap<String, Expr>,
        ) -> Result<(), String> {
            match expr {
                SetExpr::Select(select) => {
                    for hint in &select.hints {
                        if !hint.name.value.eq_ignore_ascii_case("set_user_variable") {
                            continue;
                        }
                        let SelectHintValue::Call { arguments } = &hint.value else {
                            return Err("invalid set_user_variable hint assignment".to_owned());
                        };
                        for assignment in arguments {
                            let Expr::Binary(binary) = assignment else {
                                return Err("invalid set_user_variable hint assignment".to_owned());
                            };
                            let Expr::UserVariable(variable) = binary.left.as_ref() else {
                                return Err("invalid set_user_variable hint assignment".to_owned());
                            };
                            if binary.operator != BinaryOperator::Equal {
                                return Err("invalid set_user_variable hint assignment".to_owned());
                            }
                            assignments.insert(
                                variable.value.to_ascii_lowercase(),
                                (*binary.right).clone(),
                            );
                        }
                    }
                    for table in &select.from {
                        collect_factor(&table.relation, assignments)?;
                        for join in &table.joins {
                            collect_factor(&join.relation, assignments)?;
                        }
                    }
                }
                SetExpr::Query(query) => collect(query, assignments)?,
                SetExpr::SetOperation(operation) => {
                    collect_set_expr(operation.left.as_ref(), assignments)?;
                    collect_set_expr(operation.right.as_ref(), assignments)?;
                }
                SetExpr::Values(_) => {}
            }
            Ok(())
        }
        fn collect_factor(
            factor: &TableFactor,
            assignments: &mut HashMap<String, Expr>,
        ) -> Result<(), String> {
            match factor {
                TableFactor::Derived { subquery, .. } => collect(subquery, assignments),
                TableFactor::NestedJoin {
                    table_with_joins, ..
                } => {
                    collect_factor(&table_with_joins.relation, assignments)?;
                    for join in &table_with_joins.joins {
                        collect_factor(&join.relation, assignments)?;
                    }
                    Ok(())
                }
                TableFactor::Table { .. }
                | TableFactor::TableFunction { .. }
                | TableFactor::Unnest { .. } => Ok(()),
            }
        }
        if let Some(with) = &query.with {
            for cte in &with.ctes {
                collect(&cte.query, assignments)?;
            }
        }
        collect_set_expr(query.body.as_ref(), assignments)
    }

    let mut assignments = HashMap::new();
    collect(query, &mut assignments)?;
    Ok(assignments)
}

struct UserVariableSubstituter {
    assignments: HashMap<String, Expr>,
}

impl Fold for UserVariableSubstituter {
    fn fold_expr(&mut self, expression: Expr) -> Expr {
        if let Expr::UserVariable(variable) = &expression
            && let Some(value) = self.assignments.get(&variable.value.to_ascii_lowercase())
        {
            return value.clone();
        }
        novarocks_parser::ast::fold_expr(self, expression)
    }
}

fn remove_bare_top_level_dual(query: &mut Query) {
    let SetExpr::Select(select) = query.body.as_mut() else {
        return;
    };
    if select.from.len() != 1 {
        return;
    }
    if select.selection.is_some()
        || !matches!(select.group_by, GroupBy::None)
        || select.having.is_some()
        || select.qualify.is_some()
        || !select.windows.is_empty()
        || !query.order_by.is_empty()
        || query.limit.is_some()
        || query.offset.is_some()
        || query.fetch.is_some()
    {
        return;
    }
    let table = &select.from[0];
    if !table.joins.is_empty() {
        return;
    }
    let TableFactor::Table {
        name,
        alias,
        version,
        hints,
        ..
    } = &table.relation
    else {
        return;
    };
    if name.parts.len() == 1
        && name.parts[0].value.eq_ignore_ascii_case("dual")
        && !name.parts[0].quoted
        && alias.is_none()
        && version.is_none()
        && hints.is_empty()
    {
        select.from.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_parser::ast::Statement;

    fn parse_query(sql: &str) -> Query {
        let mut statements = novarocks_parser::parse(sql).expect("typed query should parse");
        match statements.remove(0) {
            Statement::Query(query) => query,
            other => panic!("expected query, got {other:?}"),
        }
    }

    #[test]
    fn unrolls_recursive_cte_before_catalog_lookup() {
        let query = parse_query(
            "WITH RECURSIVE n AS (SELECT 1 AS x UNION ALL SELECT x + 1 FROM n) SELECT x FROM n",
        );
        let query = preanalyze(query).expect("recursive CTE should unroll");
        let with = query.with.expect("WITH must remain");
        assert_eq!(with.ctes.len(), 1);
        assert!(!set_expr_references_table(
            with.ctes[0].query.body.as_ref(),
            "n"
        ));
    }

    #[test]
    fn recursive_anchor_self_reference_fails_before_catalog_lookup() {
        let query =
            parse_query("WITH RECURSIVE n AS (SELECT * FROM n UNION ALL SELECT 1) SELECT * FROM n");
        assert_eq!(preanalyze(query), Err("unknown table: n".to_owned()));
    }

    #[test]
    fn recursive_anchor_nested_predicate_self_reference_fails_before_catalog_lookup() {
        let query = parse_query(
            "WITH RECURSIVE n AS (SELECT 1 WHERE 1 IN (SELECT 1 FROM n) UNION ALL SELECT 2) SELECT * FROM n",
        );
        assert_eq!(preanalyze(query), Err("unknown table: n".to_owned()));
    }

    #[test]
    fn recursive_anchor_nested_with_cte_body_self_reference_fails_before_catalog_lookup() {
        let query = parse_query(
            "WITH RECURSIVE n AS (SELECT 1 WHERE EXISTS (WITH x AS (SELECT * FROM n) SELECT 1 FROM x) UNION ALL SELECT 2) SELECT * FROM n",
        );
        assert_eq!(preanalyze(query), Err("unknown table: n".to_owned()));
    }

    #[test]
    fn recursive_anchor_nested_same_name_with_shadows_outer_cte() {
        let query = parse_query(
            "WITH RECURSIVE n AS (SELECT 1 WHERE EXISTS (WITH n AS (SELECT 1) SELECT 1 FROM n) UNION ALL SELECT 2) SELECT * FROM n",
        );
        assert!(preanalyze(query).is_ok());
    }

    #[test]
    fn unrolls_outer_recursive_reference_before_nested_same_name_cte() {
        let query = parse_query(
            "WITH RECURSIVE n AS (SELECT 1 AS x UNION ALL SELECT x + 1 FROM n WHERE EXISTS (WITH x AS (SELECT * FROM n), n AS (SELECT 1) SELECT 1 FROM x)) SELECT x FROM n",
        );
        let query = preanalyze(query).expect("recursive CTE should unroll");
        let with = query.with.expect("WITH must remain");
        assert!(!set_expr_references_table(
            with.ctes[0].query.body.as_ref(),
            "n"
        ));
    }

    #[test]
    fn unrolls_recursive_cte_reference_inside_predicate_subquery() {
        let query = parse_query(
            "WITH RECURSIVE n AS (SELECT 1 AS x UNION ALL SELECT x + 1 FROM n WHERE EXISTS (SELECT 1 FROM n)) SELECT x FROM n",
        );
        let query = preanalyze(query).expect("recursive CTE should unroll");
        let with = query.with.expect("WITH must remain");
        assert!(!set_expr_references_table(
            with.ctes[0].query.body.as_ref(),
            "n"
        ));
    }

    #[test]
    fn unrolls_recursive_cte_inside_expression_subquery() {
        let query = parse_query(
            "SELECT EXISTS (WITH RECURSIVE n AS (SELECT 1 AS x UNION ALL SELECT x + 1 FROM n) SELECT x FROM n)",
        );
        let query = preanalyze(query).expect("recursive expression subquery should unroll");
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select");
        };
        let SelectItem::UnnamedExpr(Expr::Exists(exists)) = &select.projection[0] else {
            panic!("expected EXISTS predicate");
        };
        let with = exists.query.with.as_ref().expect("nested WITH must remain");
        assert!(!set_expr_references_table(
            with.ctes[0].query.body.as_ref(),
            "n"
        ));
    }

    #[test]
    fn substitutes_typed_set_user_variable_assignments() {
        let query = parse_query("SELECT /*+ set_user_variable(@v = 7) */ @v + 1");
        let query = preanalyze(query).expect("hint should substitute");
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select");
        };
        assert!(matches!(
            select.projection[0],
            SelectItem::UnnamedExpr(Expr::Binary(_))
        ));
    }

    #[test]
    fn removes_only_bare_top_level_dual() {
        let query = preanalyze(parse_query("SELECT 1 FROM dual")).expect("prepass");
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select");
        };
        assert!(select.from.is_empty());

        let query = preanalyze(parse_query("SELECT 1 FROM dual WHERE 1 = 1")).expect("prepass");
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select");
        };
        assert_eq!(select.from.len(), 1);
    }

    #[test]
    fn cross_join_with_constraint_becomes_inner_join() {
        let query = preanalyze(parse_query("SELECT * FROM a CROSS JOIN b ON a.id = b.id"))
            .expect("prepass");
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select");
        };
        assert_eq!(
            select.from[0].joins[0].operator,
            JoinOperator::InnerExplicit
        );
    }
}
