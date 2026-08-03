// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with the License.

//! Pure SQL shaping for a materialized-view first refresh.
//!
//! The application captures connector generations and turns them into this
//! immutable value before it enters the compiler.  The helpers below only
//! transform SQL ASTs; they have no catalog, execution, or connector
//! dependency.

use std::collections::{BTreeMap, HashSet};

use novarocks_catalog::identifier::TableIdentity;

use crate::sql::planner::vocabulary::{BRANCH_ID_COLUMN_NAME, HIDDEN_APPLY_KEY_COLUMN_NAME};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct SqlMvSnapshotPin {
    snapshots: BTreeMap<String, i64>,
    table_uuids: BTreeMap<String, String>,
}

impl SqlMvSnapshotPin {
    pub(crate) fn try_from_maps(
        snapshots: BTreeMap<String, i64>,
        table_uuids: BTreeMap<String, String>,
    ) -> Result<Self, String> {
        if snapshots.is_empty() || snapshots.len() != table_uuids.len() {
            return Err("MV first-refresh snapshot pin has incomplete identity facts".to_string());
        }
        for (fqn, snapshot_id) in &snapshots {
            if fqn.trim().is_empty() || *snapshot_id < 0 {
                return Err("MV first-refresh snapshot pin has invalid snapshot facts".to_string());
            }
            if table_uuids
                .get(fqn)
                .is_none_or(|table_uuid| table_uuid.trim().is_empty())
            {
                return Err(
                    "MV first-refresh snapshot pin is missing a table incarnation".to_string(),
                );
            }
        }
        Ok(Self {
            snapshots,
            table_uuids,
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
                .map(|(fqn, _, uuid)| ((*fqn).to_string(), (*uuid).to_string()))
                .collect(),
        )
        .expect("test MV snapshot pin must be valid")
    }

    pub(crate) fn get(&self, table: &TableIdentity) -> Option<i64> {
        self.snapshots.get(&table.fqn()).copied()
    }

    pub(crate) fn uuid(&self, table: &TableIdentity) -> Option<&str> {
        self.table_uuids.get(&table.fqn()).map(String::as_str)
    }

    pub(crate) fn len(&self) -> usize {
        self.snapshots.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.snapshots.is_empty()
    }

    pub(super) fn snapshot_map(&self) -> &BTreeMap<String, i64> {
        &self.snapshots
    }

    pub(super) fn table_uuid_map(&self) -> &BTreeMap<String, String> {
        &self.table_uuids
    }
}

pub(super) fn prepare_projection_full_read_sql(
    select_sql: &str,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<String, String> {
    let mut query = parse_stored_select_query(select_sql, "iceberg projection full-read")?;
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
    select_sql: &str,
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
    let mut query = parse_stored_select_query(select_sql, "iceberg UNION ALL MV full-read")?;
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
    Ok(query.to_string())
}

pub(super) fn pin_state_sql(
    state_sql: &str,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<String, String> {
    let mut query = parse_stored_select_query(state_sql, "stored MV")?;
    inject_pin_as_version_as_of(
        &mut query,
        pin,
        &HashSet::new(),
        current_catalog,
        current_database,
    )?;
    Ok(query.to_string())
}

pub(super) fn branch_union_queries(
    select_sql: &str,
    branch_count: usize,
) -> Result<Vec<(sqlparser::ast::Query, String)>, String> {
    let query = parse_stored_select_query(
        select_sql,
        "iceberg branch UNION ALL aggregate first refresh",
    )?;
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
            let branch_sql = branch_query.to_string();
            Ok((branch_query, branch_sql))
        })
        .collect()
}

fn parse_stored_select_query(sql: &str, operation: &str) -> Result<sqlparser::ast::Query, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)
        .map_err(|error| format!("{operation} SELECT normalize error: {error}"))?;
    let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|error| format!("{operation} SELECT parse error: {error}"))?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err(format!("{operation} expects a SELECT query"));
    };
    Ok(*query)
}

fn append_physical_apply_key(mut query: sqlparser::ast::Query) -> Result<String, String> {
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_mut() else {
        return Err("iceberg MV physical SELECT expects a SELECT body".to_string());
    };
    validate_reserved_projection_output_names(
        select,
        &[(HIDDEN_APPLY_KEY_COLUMN_NAME, "apply key")],
    )?;
    for item in &select.projection {
        if matches!(
            item,
            sqlparser::ast::SelectItem::Wildcard(_)
                | sqlparser::ast::SelectItem::QualifiedWildcard(_, _)
        ) {
            return Err(
                "iceberg MV physical SELECT requires explicit projection columns".to_string(),
            );
        }
    }
    select
        .projection
        .push(sqlparser::ast::SelectItem::ExprWithAlias {
            expr: sqlparser::ast::Expr::Identifier(sqlparser::ast::Ident::new("_row_id")),
            alias: sqlparser::ast::Ident::new(HIDDEN_APPLY_KEY_COLUMN_NAME),
        });
    Ok(query.to_string())
}

fn validate_reserved_projection_output_names(
    select: &sqlparser::ast::Select,
    reserved: &[(&str, &str)],
) -> Result<(), String> {
    for item in &select.projection {
        let output_name = match item {
            sqlparser::ast::SelectItem::UnnamedExpr(expr) => Some(expr.to_string()),
            sqlparser::ast::SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
            sqlparser::ast::SelectItem::Wildcard(_)
            | sqlparser::ast::SelectItem::QualifiedWildcard(_, _) => None,
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
    set_expr: &sqlparser::ast::SetExpr,
    branch_count: usize,
    validated_branch_count: &mut usize,
    saw_union_all: &mut bool,
) -> Result<(), String> {
    match set_expr {
        sqlparser::ast::SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            if *op != sqlparser::ast::SetOperator::Union
                || *set_quantifier != sqlparser::ast::SetQuantifier::All
            {
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
        sqlparser::ast::SetExpr::Query(query) => validate_union_projection_set_expr(
            query.body.as_ref(),
            branch_count,
            validated_branch_count,
            saw_union_all,
        ),
        sqlparser::ast::SetExpr::Select(select) => {
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
    set_expr: &mut sqlparser::ast::SetExpr,
    next_branch_id: &mut i32,
) -> Result<(), String> {
    match set_expr {
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            append_union_projection_hidden_columns(left.as_mut(), next_branch_id)?;
            append_union_projection_hidden_columns(right.as_mut(), next_branch_id)
        }
        sqlparser::ast::SetExpr::Query(query) => {
            append_union_projection_hidden_columns(query.body.as_mut(), next_branch_id)
        }
        sqlparser::ast::SetExpr::Select(select) => {
            let branch_id = *next_branch_id;
            *next_branch_id = next_branch_id
                .checked_add(1)
                .ok_or_else(|| "iceberg UNION ALL MV branch id overflow".to_string())?;
            select
                .projection
                .push(sqlparser::ast::SelectItem::ExprWithAlias {
                    expr: sqlparser::ast::Expr::Identifier(sqlparser::ast::Ident::new("_row_id")),
                    alias: sqlparser::ast::Ident::new(HIDDEN_APPLY_KEY_COLUMN_NAME),
                });
            select
                .projection
                .push(sqlparser::ast::SelectItem::ExprWithAlias {
                    expr: sqlparser::ast::Expr::Cast {
                        kind: sqlparser::ast::CastKind::Cast,
                        expr: Box::new(sqlparser::ast::Expr::Value(
                            sqlparser::ast::Value::Number(branch_id.to_string(), false).into(),
                        )),
                        data_type: sqlparser::ast::DataType::Int(None),
                        array: false,
                        format: None,
                    },
                    alias: sqlparser::ast::Ident::new(BRANCH_ID_COLUMN_NAME),
                });
            Ok(())
        }
        _ => Err("iceberg UNION ALL MV full refresh expects SELECT branches".to_string()),
    }
}

fn inject_pin_as_version_as_of(
    query: &mut sqlparser::ast::Query,
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
        for cte in &mut with.cte_tables {
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

fn walk_set_expr(expr: &mut sqlparser::ast::SetExpr, state: &mut InjectState<'_>) {
    if state.first_error.is_some() {
        return;
    }
    match expr {
        sqlparser::ast::SetExpr::Select(select) => {
            for table_with_joins in &mut select.from {
                walk_table_with_joins(table_with_joins, state);
            }
        }
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            walk_set_expr(left.as_mut(), state);
            walk_set_expr(right.as_mut(), state);
        }
        sqlparser::ast::SetExpr::Query(query) => walk_set_expr(query.body.as_mut(), state),
        _ => {}
    }
}

fn walk_table_with_joins(
    table_with_joins: &mut sqlparser::ast::TableWithJoins,
    state: &mut InjectState<'_>,
) {
    walk_factor(&mut table_with_joins.relation, state);
    for join in &mut table_with_joins.joins {
        walk_factor(&mut join.relation, state);
    }
}

fn walk_factor(factor: &mut sqlparser::ast::TableFactor, state: &mut InjectState<'_>) {
    use sqlparser::ast::{Expr, ObjectNamePart, TableFactor, TableVersion, Value};

    if state.first_error.is_some() {
        return;
    }
    match factor {
        TableFactor::Table {
            name,
            version,
            args,
            ..
        } => {
            if args.is_some() {
                return;
            }
            let parts = name
                .0
                .iter()
                .filter_map(|part| match part {
                    ObjectNamePart::Identifier(identifier) => {
                        Some(identifier.value.to_ascii_lowercase())
                    }
                    _ => None,
                })
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
            *version = Some(TableVersion::VersionAsOf(Expr::Value(
                Value::Number(pinned.to_string(), false).into(),
            )));
            state.count += 1;
        }
        TableFactor::Derived { subquery, .. } => walk_set_expr(subquery.body.as_mut(), state),
        TableFactor::NestedJoin {
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
    body: &sqlparser::ast::SetExpr,
    out: &mut Vec<sqlparser::ast::SetExpr>,
) -> Result<(), String> {
    match body {
        sqlparser::ast::SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            if !matches!(op, sqlparser::ast::SetOperator::Union)
                || !matches!(set_quantifier, sqlparser::ast::SetQuantifier::All)
            {
                return Err(
                    "iceberg branch UNION ALL aggregate first refresh supports UNION ALL only"
                        .to_string(),
                );
            }
            flatten_branch_union_all_set_expr(left, out)?;
            flatten_branch_union_all_set_expr(right, out)
        }
        sqlparser::ast::SetExpr::Query(query) => {
            flatten_branch_union_all_set_expr(query.body.as_ref(), out)
        }
        sqlparser::ast::SetExpr::Select(_) => {
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

    #[test]
    fn sqlx2_mv_sql_shape_pins_projection_without_application_refresh_state() {
        let pin =
            SqlMvSnapshotPin::from_entries_for_tests(&[("ice.db.fact", 42, "fact-incarnation")]);

        let sql =
            prepare_projection_full_read_sql("SELECT id FROM ice.db.fact", &pin, Some("ice"), "db")
                .expect("SQL-only projection shape");

        assert!(sql.contains("VERSION AS OF 42"), "{sql}");
        assert!(sql.contains("__nova_base_row_id"), "{sql}");
        assert_eq!(pin.snapshot_map().get("ice.db.fact"), Some(&42));
        assert_eq!(
            pin.table_uuid_map().get("ice.db.fact"),
            Some(&"fact-incarnation".to_string())
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
