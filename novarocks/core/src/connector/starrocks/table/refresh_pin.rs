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

//! Refresh-scoped snapshot pin for iceberg-backed materialized views.
//!
//! `RefreshSnapshotPin` captures, at the start of a refresh, the
//! `current_snapshot_id` and `uuid` of every base table. The pin is the
//! single source of truth for snapshot ids during the refresh:
//!
//! * `plan_changes` uses pin[base] as its `to_snapshot_id`
//! * `begin_mv_refresh_intent` records pin as the refresh target
//! * `update_starrocks_mv_refresh_summary` writes `last_refresh_snapshots = pin`
//!
//! For single-base MVs (the only shape currently supported by the DDL gate),
//! this guarantees delta computation and bookkeeping agree on the same
//! snapshot, even if the base table commits concurrently during the refresh.
//!
//! For multi-base MVs (future), the pin additionally guarantees cross-table
//! consistency: every base table is read at the snapshot it had at refresh
//! start, regardless of intervening external commits.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use crate::catalog::identifier::TableIdentity;
use crate::engine::StandaloneState;

/// Per-refresh snapshot pin: each base table is pinned to the
/// `current_snapshot_id` it had at refresh entry time.
#[allow(dead_code)]
#[derive(Clone, Debug, Default)]
pub(crate) struct RefreshSnapshotPin {
    snapshots: BTreeMap<String, i64>,
    table_uuids: BTreeMap<String, String>,
}

#[allow(dead_code)]
impl RefreshSnapshotPin {
    /// Capture the current snapshot id and uuid for each base table.
    ///
    /// Fails fast if any base table has no current snapshot - refresh
    /// against an empty iceberg table is not a supported flow at this
    /// layer; the caller is expected to handle that earlier.
    pub(crate) fn capture(
        state: &Arc<StandaloneState>,
        base_refs: &[TableIdentity],
    ) -> Result<Self, String> {
        let mut pin = RefreshSnapshotPin::default();
        for base_ref in base_refs {
            let loaded =
                crate::connector::starrocks::table::mv_refresh::load_current_iceberg_base_table(
                    state, base_ref,
                )?;
            let snapshot_id = loaded
                .table
                .metadata()
                .current_snapshot()
                .map(|s| s.snapshot_id())
                .ok_or_else(|| {
                    format!(
                        "iceberg base table {} has no current snapshot; cannot freeze refresh pin",
                        base_ref.fqn()
                    )
                })?;
            pin.snapshots.insert(base_ref.fqn(), snapshot_id);
            pin.table_uuids
                .insert(base_ref.fqn(), loaded.table.metadata().uuid().to_string());
        }
        Ok(pin)
    }

    pub(crate) fn get(&self, base: &TableIdentity) -> Option<i64> {
        self.snapshots.get(&base.fqn()).copied()
    }

    pub(crate) fn uuid(&self, base: &TableIdentity) -> Option<&str> {
        self.table_uuids.get(&base.fqn()).map(String::as_str)
    }

    pub(crate) fn len(&self) -> usize {
        self.snapshots.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.snapshots.is_empty()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&str, i64)> {
        self.snapshots.iter().map(|(k, v)| (k.as_str(), *v))
    }

    pub(crate) fn to_snapshot_map(&self) -> BTreeMap<String, i64> {
        self.snapshots.clone()
    }

    pub(crate) fn to_table_uuid_map(&self) -> BTreeMap<String, String> {
        self.table_uuids.clone()
    }
}

/// Walk `query` in place. For each `TableFactor::Table` whose 3-part name
/// resolves into the pin and is not in `delta_bearing`, set
/// `version = Some(VersionAsOf(Number(pin[base])))`. Returns the number
/// of mutations performed.
///
/// Rules:
/// - `TableFactor::Table` with `version = Some(_)` already -> Err. The
///   refresh SELECT is not allowed to combine user-written FOR VERSION AS OF
///   with refresh pinning.
/// - Table not in pin -> unchanged (likely a CTE, a different catalog, or
///   an alias not addressed by base_refs).
/// - Table in pin and in delta_bearing -> unchanged (handled by
///   the rewrite-path incremental refresh in iceberg_refresh.rs).
/// - Table in pin and not in delta_bearing -> inject version.
///
/// In scope-B single-base MVs, the unique base is delta-bearing, so this
/// function is a no-op in production. It exists for the multi-base future.
#[allow(dead_code)]
pub(crate) fn inject_pin_as_for_version_as_of(
    query: &mut sqlparser::ast::Query,
    pin: &RefreshSnapshotPin,
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
    if let Some(err) = state.first_error {
        return Err(err);
    }
    Ok(state.count)
}

struct InjectState<'a> {
    pin: &'a RefreshSnapshotPin,
    delta_bearing: &'a HashSet<TableIdentity>,
    current_catalog: Option<&'a str>,
    current_database: &'a str,
    count: usize,
    first_error: Option<String>,
}

fn walk_set_expr(expr: &mut sqlparser::ast::SetExpr, state: &mut InjectState<'_>) {
    use sqlparser::ast::SetExpr;
    if state.first_error.is_some() {
        return;
    }
    match expr {
        SetExpr::Select(select) => {
            for tw in &mut select.from {
                walk_table_with_joins(tw, state);
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            walk_set_expr(left.as_mut(), state);
            walk_set_expr(right.as_mut(), state);
        }
        SetExpr::Query(q) => walk_set_expr(q.body.as_mut(), state),
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
            // Skip table-valued functions (e.g. __nr_ivm_delta).
            if args.is_some() {
                return;
            }
            let parts: Vec<String> = name
                .0
                .iter()
                .filter_map(|p| match p {
                    ObjectNamePart::Identifier(i) => Some(i.value.to_ascii_lowercase()),
                    _ => None,
                })
                .collect();
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
        TableFactor::Derived { subquery, .. } => {
            walk_set_expr(subquery.body.as_mut(), state);
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            walk_table_with_joins(table_with_joins.as_mut(), state);
        }
        _ => {}
    }
}

fn resolve_table_factor(
    parts: &[String],
    current_catalog: Option<&str>,
    current_database: &str,
) -> Option<TableIdentity> {
    let current_database = current_database.to_ascii_lowercase();
    let current_catalog = current_catalog.map(|s| s.to_ascii_lowercase());
    match parts {
        [tbl] => current_catalog.map(|cat| TableIdentity {
            catalog: cat,
            namespace: current_database,
            table: tbl.clone(),
        }),
        [db, tbl] => current_catalog.map(|cat| TableIdentity {
            catalog: cat,
            namespace: db.clone(),
            table: tbl.clone(),
        }),
        [cat, db, tbl] => Some(TableIdentity {
            catalog: cat.clone(),
            namespace: db.clone(),
            table: tbl.clone(),
        }),
        _ => None,
    }
}
