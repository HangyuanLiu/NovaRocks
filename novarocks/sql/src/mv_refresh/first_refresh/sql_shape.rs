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

//! Pure SQL shaping for a materialized-view first refresh.
//!
//! The application captures connector generations and turns them into this
//! immutable value before it enters the compiler.  The helpers below only
//! transform SQL ASTs; they have no catalog, execution, or connector
//! dependency.

use std::collections::{BTreeMap, HashSet};

use novarocks_parser::Span;
use novarocks_parser::ast;
use novarocks_parser::printer;
use novarocks_spi::connector::ConnectorTableObjectId;
use novarocks_types::naming::TableIdentity;

use crate::planner::vocabulary::{BRANCH_ID_COLUMN_NAME, HIDDEN_APPLY_KEY_COLUMN_NAME};

/// Opaque, copied snapshot identity facts consumed by first-refresh SQL
/// shaping. It owns values only; it carries no catalog, table, or planning
/// graph handle.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SqlMvSnapshotPin {
    snapshots: BTreeMap<String, i64>,
    table_object_ids: BTreeMap<String, ConnectorTableObjectId>,
}

impl SqlMvSnapshotPin {
    pub fn try_from_maps(
        snapshots: BTreeMap<String, i64>,
        table_object_ids: BTreeMap<String, ConnectorTableObjectId>,
    ) -> Result<Self, String> {
        if snapshots.is_empty() || snapshots.len() != table_object_ids.len() {
            return Err("MV first-refresh snapshot pin has incomplete identity facts".to_string());
        }
        for (fqn, snapshot_id) in &snapshots {
            if fqn.trim().is_empty() || *snapshot_id < 0 {
                return Err("MV first-refresh snapshot pin has invalid snapshot facts".to_string());
            }
            if table_object_ids
                .get(fqn)
                .is_none_or(|object_id| object_id.as_bytes().is_empty())
            {
                return Err(
                    "MV first-refresh snapshot pin is missing a table incarnation".to_string(),
                );
            }
        }
        Ok(Self {
            snapshots,
            table_object_ids,
        })
    }

    #[cfg(test)]
    pub(super) fn from_entries_for_tests(entries: &[(&str, i64, &str)]) -> Self {
        Self::try_from_maps(
            entries
                .iter()
                .map(|(fqn, snapshot, _)| ((*fqn).to_string(), *snapshot))
                .collect(),
            entries
                .iter()
                .map(|(fqn, _, object_id)| {
                    (
                        (*fqn).to_string(),
                        ConnectorTableObjectId::try_new(bytes::Bytes::copy_from_slice(
                            object_id.as_bytes(),
                        ))
                        .expect("test object ID"),
                    )
                })
                .collect(),
        )
        .expect("test MV snapshot pin must be valid")
    }

    /// Return the captured snapshot for a frozen identity.
    pub fn get(&self, table: &TableIdentity) -> Option<i64> {
        self.snapshots.get(&table.fqn()).copied()
    }

    /// Return the captured table incarnation for a frozen identity.
    pub fn object_id(&self, table: &TableIdentity) -> Option<&ConnectorTableObjectId> {
        self.table_object_ids.get(&table.fqn())
    }

    pub fn len(&self) -> usize {
        self.snapshots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.snapshots.is_empty()
    }

    #[allow(
        dead_code,
        reason = "Retained for staged SQL planner migration consumers and test helpers."
    )]
    pub(super) fn snapshot_map(&self) -> &BTreeMap<String, i64> {
        &self.snapshots
    }

    #[allow(
        dead_code,
        reason = "Retained for staged SQL planner migration consumers and test helpers."
    )]
    pub(super) fn table_object_id_map(&self) -> &BTreeMap<String, ConnectorTableObjectId> {
        &self.table_object_ids
    }
}

pub(super) fn prepare_projection_full_read_sql(
    select_query: &ast::Query,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<String, String> {
    let mut query = select_query.clone();
    inject_pin_as_version_as_of(
        &mut query,
        pin,
        &HashSet::new(),
        current_catalog,
        current_database,
    )?;
    append_physical_apply_key(query)
}

pub(super) fn prepare_union_projection_full_read_sql(
    select_query: &ast::Query,
    branch_count: usize,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<String, String> {
    if branch_count < 2 {
        return Err("iceberg UNION ALL MV full refresh requires at least 2 branches".to_string());
    }
    let branch_count_i32 = i32::try_from(branch_count).map_err(|_| {
        format!("iceberg UNION ALL MV full refresh branch count {branch_count} does not fit in i32")
    })?;
    let mut query = select_query.clone();
    inject_pin_as_version_as_of(
        &mut query,
        pin,
        &HashSet::new(),
        current_catalog,
        current_database,
    )?;

    let mut validated_branch_count = 0;
    let mut saw_union_all = false;
    validate_union_projection_set_expr(
        query.body.as_ref(),
        branch_count,
        &mut validated_branch_count,
        &mut saw_union_all,
    )?;
    if !saw_union_all {
        return Err("iceberg UNION ALL MV full refresh requires an actual UNION ALL".to_string());
    }
    if validated_branch_count != branch_count {
        return Err(format!(
            "iceberg UNION ALL MV full refresh expected {branch_count} branches, rewrote {validated_branch_count}"
        ));
    }

    let mut next_branch_id = 0;
    append_union_projection_hidden_columns(query.body.as_mut(), &mut next_branch_id)?;
    debug_assert_eq!(next_branch_id, branch_count_i32);
    Ok(printer::print_query(&query))
}

pub(super) fn pin_state_sql(
    state_query: &ast::Query,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<String, String> {
    let mut query = state_query.clone();
    inject_pin_as_version_as_of(
        &mut query,
        pin,
        &HashSet::new(),
        current_catalog,
        current_database,
    )?;
    Ok(printer::print_query(&query))
}

pub(super) fn branch_union_queries(
    select_query: &ast::Query,
    branch_count: usize,
) -> Result<Vec<(ast::Query, String)>, String> {
    let query = select_query.clone();
    let mut branch_bodies = Vec::new();
    flatten_branch_union_all_set_expr(query.body.as_ref(), &mut branch_bodies)?;
    if branch_bodies.len() != branch_count {
        return Err(format!(
            "iceberg branch UNION ALL aggregate first refresh expected {branch_count} branches, found {}",
            branch_bodies.len()
        ));
    }
    branch_bodies
        .into_iter()
        .map(|body| {
            let mut branch_query = query.clone();
            branch_query.body = Box::new(body);
            let branch_sql = printer::print_query(&branch_query);
            Ok((branch_query, branch_sql))
        })
        .collect()
}

fn synthetic_ident(value: &str) -> ast::Ident {
    ast::Ident {
        value: value.to_string(),
        quoted: false,
        quote_style: None,
        span: Span::new(0, 0),
    }
}

fn number_literal(value: String) -> ast::Expr {
    ast::Expr::Literal(ast::Literal {
        kind: ast::LiteralKind::Number(value),
        span: Span::new(0, 0),
    })
}

fn int_type_name() -> ast::TypeName {
    ast::TypeName {
        name: ast::ObjectName {
            parts: vec![synthetic_ident("INT")],
            span: Span::new(0, 0),
        },
        arguments: vec![],
        argument_separator_spaces: vec![],
        span: Span::new(0, 0),
    }
}

fn append_physical_apply_key(mut query: ast::Query) -> Result<String, String> {
    let ast::SetExpr::Select(select) = query.body.as_mut() else {
        return Err("iceberg MV physical SELECT expects a SELECT body".to_string());
    };
    validate_reserved_projection_output_names(
        select,
        &[(HIDDEN_APPLY_KEY_COLUMN_NAME, "apply key")],
    )?;
    for item in &select.projection {
        if matches!(
            item,
            ast::SelectItem::Wildcard { .. } | ast::SelectItem::QualifiedWildcard { .. }
        ) {
            return Err(
                "iceberg MV physical SELECT requires explicit projection columns".to_string(),
            );
        }
    }
    select.projection.push(ast::SelectItem::ExprWithAlias {
        expr: ast::Expr::Identifier(synthetic_ident("_row_id")),
        alias: synthetic_ident(HIDDEN_APPLY_KEY_COLUMN_NAME),
        explicit_as: true,
        span: Span::new(0, 0),
    });
    Ok(printer::print_query(&query))
}

fn validate_reserved_projection_output_names(
    select: &ast::Select,
    reserved: &[(&str, &str)],
) -> Result<(), String> {
    for item in &select.projection {
        let output_name = match item {
            ast::SelectItem::UnnamedExpr(expr) => Some(printer::print_expr(expr)),
            ast::SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
            ast::SelectItem::Wildcard { .. } | ast::SelectItem::QualifiedWildcard { .. } => None,
        };
        let Some(output_name) = output_name else {
            continue;
        };
        for (reserved_name, purpose) in reserved {
            if output_name.eq_ignore_ascii_case(reserved_name) {
                return Err(format!(
                    "Iceberg MV output column name {reserved_name} is reserved for internal {purpose}"
                ));
            }
        }
    }
    Ok(())
}

fn validate_union_projection_set_expr(
    set_expr: &ast::SetExpr,
    branch_count: usize,
    validated_branch_count: &mut usize,
    saw_union_all: &mut bool,
) -> Result<(), String> {
    match set_expr {
        ast::SetExpr::SetOperation(ast::SetOperation {
            operator,
            quantifier,
            left,
            right,
            ..
        }) => {
            if *operator != ast::SetOperator::Union || *quantifier != ast::SetQuantifier::All {
                return Err("iceberg UNION ALL MV full refresh supports UNION ALL only".to_string());
            }
            *saw_union_all = true;
            validate_union_projection_set_expr(
                left,
                branch_count,
                validated_branch_count,
                saw_union_all,
            )?;
            validate_union_projection_set_expr(
                right,
                branch_count,
                validated_branch_count,
                saw_union_all,
            )
        }
        ast::SetExpr::Query(query) => validate_union_projection_set_expr(
            query.body.as_ref(),
            branch_count,
            validated_branch_count,
            saw_union_all,
        ),
        ast::SetExpr::Select(select) => {
            if *validated_branch_count >= branch_count {
                return Err(format!(
                    "iceberg UNION ALL MV full refresh found more than {branch_count} branches"
                ));
            }
            validate_reserved_projection_output_names(
                select,
                &[
                    (HIDDEN_APPLY_KEY_COLUMN_NAME, "apply key"),
                    (BRANCH_ID_COLUMN_NAME, "branch id"),
                ],
            )?;
            *validated_branch_count += 1;
            Ok(())
        }
        _ => Err("iceberg UNION ALL MV full refresh expects SELECT branches".to_string()),
    }
}

fn append_union_projection_hidden_columns(
    set_expr: &mut ast::SetExpr,
    next_branch_id: &mut i32,
) -> Result<(), String> {
    match set_expr {
        ast::SetExpr::SetOperation(ast::SetOperation { left, right, .. }) => {
            append_union_projection_hidden_columns(left.as_mut(), next_branch_id)?;
            append_union_projection_hidden_columns(right.as_mut(), next_branch_id)
        }
        ast::SetExpr::Query(query) => {
            append_union_projection_hidden_columns(query.body.as_mut(), next_branch_id)
        }
        ast::SetExpr::Select(select) => {
            let branch_id = *next_branch_id;
            *next_branch_id = next_branch_id
                .checked_add(1)
                .ok_or_else(|| "iceberg UNION ALL MV branch id overflow".to_string())?;
            select.projection.push(ast::SelectItem::ExprWithAlias {
                expr: ast::Expr::Identifier(synthetic_ident("_row_id")),
                alias: synthetic_ident(HIDDEN_APPLY_KEY_COLUMN_NAME),
                explicit_as: true,
                span: Span::new(0, 0),
            });
            select.projection.push(ast::SelectItem::ExprWithAlias {
                expr: ast::Expr::Cast(ast::CastExpr {
                    kind: ast::CastKind::Cast,
                    expr: Box::new(number_literal(branch_id.to_string())),
                    data_type: int_type_name(),
                    format: None,
                    span: Span::new(0, 0),
                }),
                alias: synthetic_ident(BRANCH_ID_COLUMN_NAME),
                explicit_as: true,
                span: Span::new(0, 0),
            });
            Ok(())
        }
        _ => Err("iceberg UNION ALL MV full refresh expects SELECT branches".to_string()),
    }
}

fn inject_pin_as_version_as_of(
    query: &mut ast::Query,
    pin: &SqlMvSnapshotPin,
    delta_bearing: &HashSet<TableIdentity>,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<usize, String> {
    let mut state = InjectState {
        pin,
        delta_bearing,
        current_catalog,
        current_database,
        count: 0,
        first_error: None,
    };
    if let Some(with) = &mut query.with {
        for cte in &mut with.ctes {
            walk_set_expr(cte.query.body.as_mut(), &mut state);
        }
    }
    walk_set_expr(query.body.as_mut(), &mut state);
    state.first_error.map_or(Ok(state.count), Err)
}

struct InjectState<'a> {
    pin: &'a SqlMvSnapshotPin,
    delta_bearing: &'a HashSet<TableIdentity>,
    current_catalog: Option<&'a str>,
    current_database: &'a str,
    count: usize,
    first_error: Option<String>,
}

fn walk_set_expr(expr: &mut ast::SetExpr, state: &mut InjectState<'_>) {
    if state.first_error.is_some() {
        return;
    }
    match expr {
        ast::SetExpr::Select(select) => {
            for table_with_joins in &mut select.from {
                walk_table_with_joins(table_with_joins, state);
            }
        }
        ast::SetExpr::SetOperation(ast::SetOperation { left, right, .. }) => {
            walk_set_expr(left.as_mut(), state);
            walk_set_expr(right.as_mut(), state);
        }
        ast::SetExpr::Query(query) => walk_set_expr(query.body.as_mut(), state),
        _ => {}
    }
}

fn walk_table_with_joins(table_with_joins: &mut ast::TableWithJoins, state: &mut InjectState<'_>) {
    walk_factor(&mut table_with_joins.relation, state);
    for join in &mut table_with_joins.joins {
        walk_factor(&mut join.relation, state);
    }
}

fn walk_factor(factor: &mut ast::TableFactor, state: &mut InjectState<'_>) {
    if state.first_error.is_some() {
        return;
    }
    match factor {
        ast::TableFactor::Table { name, version, .. } => {
            let parts = name
                .parts
                .iter()
                .map(|part| part.value.to_ascii_lowercase())
                .collect::<Vec<_>>();
            let Some(base_ref) =
                resolve_table_factor(&parts, state.current_catalog, state.current_database)
            else {
                return;
            };
            let Some(pinned) = state.pin.get(&base_ref) else {
                return;
            };
            if version.is_some() {
                state.first_error = Some(format!(
                    "refresh SELECT must not write explicit FOR VERSION AS OF for base table {}; refresh pin would conflict",
                    base_ref.fqn()
                ));
                return;
            }
            if state.delta_bearing.contains(&base_ref) {
                return;
            }
            *version = Some(ast::TableVersion {
                kind: ast::TableVersionKind::ForVersionAsOf,
                value: number_literal(pinned.to_string()),
                span: Span::new(0, 0),
            });
            state.count += 1;
        }
        ast::TableFactor::Derived { subquery, .. } => walk_set_expr(subquery.body.as_mut(), state),
        ast::TableFactor::NestedJoin {
            table_with_joins, ..
        } => walk_table_with_joins(table_with_joins.as_mut(), state),
        _ => {}
    }
}

fn resolve_table_factor(
    parts: &[String],
    current_catalog: Option<&str>,
    current_database: &str,
) -> Option<TableIdentity> {
    let current_database = current_database.to_ascii_lowercase();
    let current_catalog = current_catalog.map(str::to_ascii_lowercase);
    match parts {
        [table] => current_catalog.map(|catalog| TableIdentity {
            catalog,
            namespace: current_database,
            table: table.clone(),
        }),
        [database, table] => current_catalog.map(|catalog| TableIdentity {
            catalog,
            namespace: database.clone(),
            table: table.clone(),
        }),
        [catalog, database, table] => Some(TableIdentity {
            catalog: catalog.clone(),
            namespace: database.clone(),
            table: table.clone(),
        }),
        _ => None,
    }
}

fn flatten_branch_union_all_set_expr(
    body: &ast::SetExpr,
    out: &mut Vec<ast::SetExpr>,
) -> Result<(), String> {
    match body {
        ast::SetExpr::SetOperation(ast::SetOperation {
            operator,
            quantifier,
            left,
            right,
            ..
        }) => {
            if !matches!(operator, ast::SetOperator::Union)
                || !matches!(quantifier, ast::SetQuantifier::All)
            {
                return Err(
                    "iceberg branch UNION ALL aggregate first refresh supports UNION ALL only"
                        .to_string(),
                );
            }
            flatten_branch_union_all_set_expr(left, out)?;
            flatten_branch_union_all_set_expr(right, out)
        }
        ast::SetExpr::Query(query) => flatten_branch_union_all_set_expr(query.body.as_ref(), out),
        ast::SetExpr::Select(_) => {
            out.push(body.clone());
            Ok(())
        }
        _ => Err(
            "iceberg branch UNION ALL aggregate first refresh expects SELECT branches".to_string(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_query(sql: &str) -> ast::Query {
        let statements = novarocks_parser::parse(sql).expect("parse query");
        let [ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("fixture must be a query");
        };
        query.clone()
    }

    #[test]
    fn sqlx2_mv_sql_shape_pins_projection_without_application_refresh_state() {
        let pin =
            SqlMvSnapshotPin::from_entries_for_tests(&[("ice.db.fact", 42, "fact-incarnation")]);

        let query = parse_query("SELECT id FROM ice.db.fact");
        let sql = prepare_projection_full_read_sql(&query, &pin, Some("ice"), "db")
            .expect("SQL-only projection shape");

        assert!(sql.contains("VERSION AS OF 42"), "{sql}");
        assert!(sql.contains("__nova_base_row_id"), "{sql}");
        assert_eq!(pin.snapshot_map().get("ice.db.fact"), Some(&42));
        assert_eq!(
            pin.table_object_id_map()
                .get("ice.db.fact")
                .map(|object_id| object_id.as_bytes().as_ref()),
            Some(b"fact-incarnation".as_ref())
        );
    }

    #[test]
    fn sqlx2_mv_sql_shape_rejects_incomplete_snapshot_identity() {
        let error = SqlMvSnapshotPin::try_from_maps(
            BTreeMap::from([("ice.db.fact".to_string(), 42)]),
            BTreeMap::new(),
        )
        .expect_err("missing incarnation must fail before SQL shaping");

        assert!(error.contains("incomplete identity facts"), "{error}");
    }
}
