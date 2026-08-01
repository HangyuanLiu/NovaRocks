// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! Result-free SQL physicalization for MV first refresh.
//!
//! A first refresh writes a fresh, empty staging target. This module makes the
//! physical rows needed by that append cohort explicit, so the caller can put a
//! connector writer at the native distributed root without materializing data
//! in the frontend.

use crate::mv::aggregate_state::aggregate_sql_calls::AggregateSqlCalls;
use crate::mv::aggregate_state::mv_agg_state::{
    AGG_RETRACTION_COUNT_STATE_COLUMN, AGG_STATE_PREFIX, ROW_ID_COLUMN, sanitize_state_column_name,
};
use crate::mv::model::{AggregateFunctionKind, VisibleAggregateOutput};
use crate::mv::persistence::schema::BRANCH_ID_COLUMN_NAME;
use crate::mv::refresh::aggregate_first_refresh::{
    prepare_aggregate_first_refresh_state_sql,
    prepare_branch_union_aggregate_first_refresh_state_sqls,
};
use crate::mv::refresh::pin::RefreshSnapshotPin;
use crate::mv::refresh::projection_first_refresh::{
    prepare_projection_full_read_sql, prepare_union_projection_full_read_sql,
};

/// Immutable SQL artifact for a distributed first-refresh write.
///
/// `root_hash_column` is the target contract's hidden apply key. The native
/// planner must derive its actual writer fanout from the admitted topology.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MvFirstRefreshPhysicalSql {
    sql: String,
    root_hash_column: String,
}

impl MvFirstRefreshPhysicalSql {
    pub(crate) fn sql(&self) -> &str {
        &self.sql
    }

    pub(crate) fn root_hash_column(&self) -> &str {
        &self.root_hash_column
    }
}

pub(crate) fn prepare_projection_first_refresh_write_sql(
    select_sql: &str,
    pin: &RefreshSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<MvFirstRefreshPhysicalSql, String> {
    let sql = prepare_projection_full_read_sql(select_sql, pin, current_catalog, current_database)?;
    Ok(MvFirstRefreshPhysicalSql {
        sql,
        root_hash_column: crate::mv::persistence::schema::HIDDEN_APPLY_KEY_COLUMN_NAME.to_string(),
    })
}

pub(crate) fn prepare_union_projection_first_refresh_write_sql(
    select_sql: &str,
    branch_count: usize,
    pin: &RefreshSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<MvFirstRefreshPhysicalSql, String> {
    let sql = prepare_union_projection_full_read_sql(
        select_sql,
        branch_count,
        pin,
        current_catalog,
        current_database,
    )?;
    Ok(MvFirstRefreshPhysicalSql {
        sql,
        root_hash_column: crate::mv::persistence::schema::HIDDEN_APPLY_KEY_COLUMN_NAME.to_string(),
    })
}

pub(crate) fn prepare_aggregate_first_refresh_write_sql(
    select_sql: &str,
    calls: &AggregateSqlCalls,
    pin: &RefreshSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<MvFirstRefreshPhysicalSql, String> {
    let state_sql = prepare_aggregate_first_refresh_state_sql(
        select_sql,
        calls,
        pin,
        current_catalog,
        current_database,
    )?;
    Ok(MvFirstRefreshPhysicalSql {
        sql: aggregate_physical_sql(&state_sql, calls, None)?,
        root_hash_column: ROW_ID_COLUMN.to_string(),
    })
}

pub(crate) fn prepare_branch_union_aggregate_first_refresh_write_sql(
    select_sql: &str,
    branch_count: usize,
    first_branch_calls: &AggregateSqlCalls,
    pin: &RefreshSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<MvFirstRefreshPhysicalSql, String> {
    let branches = prepare_branch_union_aggregate_first_refresh_state_sqls(
        select_sql,
        branch_count,
        first_branch_calls,
        pin,
        current_catalog,
        current_database,
    )?;
    let sql = branches
        .into_iter()
        .enumerate()
        .map(|(branch_index, (calls, state_sql))| {
            validate_branch_aggregate_contract(branch_index, &calls, first_branch_calls)?;
            let branch_id = i32::try_from(branch_index).map_err(|_| {
                format!("MV first-refresh branch index {branch_index} exceeds Int32")
            })?;
            aggregate_physical_sql(&state_sql, &calls, Some(branch_id))
        })
        .collect::<Result<Vec<_>, _>>()?
        .join(" UNION ALL ");
    Ok(MvFirstRefreshPhysicalSql {
        sql,
        root_hash_column: ROW_ID_COLUMN.to_string(),
    })
}

fn aggregate_physical_sql(
    state_sql: &str,
    calls: &AggregateSqlCalls,
    branch_id: Option<i32>,
) -> Result<String, String> {
    let mut projection = Vec::with_capacity(
        1 + calls.visible_outputs.len() + calls.aggregates.len() + usize::from(branch_id.is_some()),
    );
    let group_key_refs = calls
        .group_keys
        .iter()
        .map(|key| qualified_column("state", &key.output_name))
        .collect::<Vec<_>>();
    projection.push(format!(
        "mv_group_row_id({}) AS {}",
        group_key_refs.join(", "),
        quote_sql_identifier(ROW_ID_COLUMN),
    ));

    for output in &calls.visible_outputs {
        match output {
            VisibleAggregateOutput::GroupKey(group_key_index) => {
                let key = calls.group_keys.get(*group_key_index).ok_or_else(|| {
                    format!("MV first-refresh group key index {group_key_index} out of range")
                })?;
                projection.push(format!(
                    "{} AS {}",
                    qualified_column("state", &key.output_name),
                    quote_sql_identifier(&key.output_name),
                ));
            }
            VisibleAggregateOutput::Aggregate(aggregate_index) => {
                let aggregate = calls.aggregates.get(*aggregate_index).ok_or_else(|| {
                    format!("MV first-refresh aggregate index {aggregate_index} out of range")
                })?;
                let state_name = state_column_name(&aggregate.output_name);
                projection.push(format!(
                    "{}({}) AS {}",
                    aggregate_visible_function(aggregate.function),
                    qualified_column("state", &state_name),
                    quote_sql_identifier(&aggregate.output_name),
                ));
            }
        }
    }

    for aggregate in &calls.aggregates {
        let state_name = state_column_name(&aggregate.output_name);
        projection.push(format!(
            "{} AS {}",
            qualified_column("state", &state_name),
            quote_sql_identifier(&state_name),
        ));
    }
    if crate::mv::aggregate_state::mv_agg_state::aggregate_shape_needs_retraction_count_state(calls)
    {
        projection.push(format!(
            "{} AS {}",
            qualified_column("state", AGG_RETRACTION_COUNT_STATE_COLUMN),
            quote_sql_identifier(AGG_RETRACTION_COUNT_STATE_COLUMN),
        ));
    }
    if let Some(branch_id) = branch_id {
        projection.push(format!(
            "CAST({branch_id} AS INT) AS {}",
            quote_sql_identifier(BRANCH_ID_COLUMN_NAME),
        ));
    }

    Ok(format!(
        "SELECT {} FROM ({state_sql}) AS state",
        projection.join(", "),
    ))
}

fn validate_branch_aggregate_contract(
    branch_index: usize,
    calls: &AggregateSqlCalls,
    expected: &AggregateSqlCalls,
) -> Result<(), String> {
    if calls.visible_outputs != expected.visible_outputs {
        return Err(format!(
            "MV first-refresh aggregate branch {branch_index} visible output order differs from branch 0"
        ));
    }
    if calls.group_keys.len() != expected.group_keys.len() {
        return Err(format!(
            "MV first-refresh aggregate branch {branch_index} group-key count differs from branch 0"
        ));
    }
    if calls.aggregates.len() != expected.aggregates.len() {
        return Err(format!(
            "MV first-refresh aggregate branch {branch_index} aggregate count differs from branch 0"
        ));
    }
    for (aggregate_index, (actual, expected)) in calls
        .aggregates
        .iter()
        .zip(expected.aggregates.iter())
        .enumerate()
    {
        if actual.function != expected.function {
            return Err(format!(
                "MV first-refresh aggregate branch {branch_index} aggregate {aggregate_index} function differs from branch 0"
            ));
        }
    }
    Ok(())
}

fn aggregate_visible_function(kind: AggregateFunctionKind) -> &'static str {
    match kind {
        AggregateFunctionKind::Count => "count_state_visible",
        AggregateFunctionKind::Sum => "sum_state_visible",
        AggregateFunctionKind::Avg => "avg_state_visible",
        AggregateFunctionKind::Min => "min_state_visible",
        AggregateFunctionKind::Max => "max_state_visible",
        AggregateFunctionKind::BoolOr => "bool_or_state_visible",
        AggregateFunctionKind::BoolAnd => "bool_and_state_visible",
        AggregateFunctionKind::CountDistinct => "count_distinct_state_visible",
        AggregateFunctionKind::ApproxCountDistinct => "approx_count_distinct_state_visible",
    }
}

fn state_column_name(output_name: &str) -> String {
    format!(
        "{AGG_STATE_PREFIX}{}",
        sanitize_state_column_name(output_name)
    )
}

fn qualified_column(qualifier: &str, column: &str) -> String {
    format!(
        "{}.{}",
        quote_sql_identifier(qualifier),
        quote_sql_identifier(column)
    )
}

fn quote_sql_identifier(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pin() -> RefreshSnapshotPin {
        RefreshSnapshotPin::from_entries_for_tests(&[("ice.db.fact", 42, "fact-uuid")])
    }

    #[test]
    fn projection_keeps_pinned_hidden_apply_key_for_writer_distribution() {
        let prepared = prepare_projection_first_refresh_write_sql(
            "SELECT v FROM ice.db.fact",
            &pin(),
            Some("ice"),
            "db",
        )
        .unwrap();
        assert_eq!(
            prepared.root_hash_column(),
            crate::mv::persistence::schema::HIDDEN_APPLY_KEY_COLUMN_NAME
        );
        assert!(prepared.sql().contains("__nova_base_row_id"));
        assert!(
            prepared.sql().contains("VERSION AS OF 42"),
            "expected pinned physical SQL, got: {}",
            prepared.sql()
        );
    }

    #[test]
    fn aggregate_uses_be_visible_and_state_projection() {
        let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(
            "SELECT k, sum(v) AS total FROM ice.db.fact GROUP BY k",
        )
        .unwrap();
        let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized).unwrap();
        let sqlparser::ast::Statement::Query(query) = statement else {
            panic!("expected SELECT")
        };
        let calls =
            crate::mv::aggregate_state::aggregate_sql_calls::extract_aggregate_sql_calls(&query)
                .unwrap();
        let prepared = prepare_aggregate_first_refresh_write_sql(
            "SELECT k, sum(v) AS total FROM ice.db.fact GROUP BY k",
            &calls,
            &pin(),
            Some("ice"),
            "db",
        )
        .unwrap();
        assert_eq!(prepared.root_hash_column(), ROW_ID_COLUMN);
        assert!(prepared.sql().contains("mv_group_row_id"));
        assert!(prepared.sql().contains("sum_state_visible"));
        assert!(prepared.sql().contains("__agg_state_total"));
        assert!(!prepared.sql().contains("RecordBatch"));
    }
}
