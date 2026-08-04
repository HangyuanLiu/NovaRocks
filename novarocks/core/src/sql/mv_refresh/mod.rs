// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! SQL-owned artifacts for materialized-view refresh.
//!
//! These artifacts describe immutable SQL and refresh facts. They never carry
//! result batches, catalog handles, or a connector implementation.

use std::collections::BTreeMap;

use crate::sql::parser::ast::RefreshMaterializedViewStmt;

pub(crate) mod aggregate_shape;
pub mod first_refresh;

/// SQL classification of an aggregate state expression in an IMV plan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AggregateFunctionKind {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    BoolOr,
    BoolAnd,
    CountDistinct,
    ApproxCountDistinct,
}

/// Stable visible-output ordering for an aggregate IMV plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum VisibleAggregateOutput {
    GroupKey(usize),
    Aggregate(usize),
}

/// SQL identity of a materialized-view target.
///
/// This value is shared with the application lifecycle, but it is canonical
/// planning vocabulary: it has no repository, connector, or execution
/// authority. Application code may re-export it for compatibility while its
/// persistence and lifecycle adapters are migrated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlMvTarget {
    pub catalog: Option<String>,
    pub database: String,
    pub name: String,
}

impl SqlMvTarget {
    pub fn display_name(&self) -> String {
        match self.catalog.as_deref() {
            Some(catalog) => format!("{catalog}.{}.{}", self.database, self.name),
            None => format!("{}.{}", self.database, self.name),
        }
    }
}

pub(crate) const FULL_REFRESH_DISABLED_MESSAGE: &str = "REFRESH MATERIALIZED VIEW ... FULL is currently disabled pending redesign; \
     its previous behavior (drop target + delete definition + recreate empty target) \
     was misleading and non-atomic. To recover from a broken contract or corrupted \
     target, run DROP MATERIALIZED VIEW <name>; CREATE MATERIALIZED VIEW <name> ...; \
     REFRESH MATERIALIZED VIEW <name>; manually.";

/// Typed SQL projection of `REFRESH MATERIALIZED VIEW`.
///
/// It intentionally preserves `FULL`: the preparation service rejects that
/// unsupported request instead of allowing an application route to silently
/// downgrade it to incremental refresh.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvRefreshStatement {
    pub name_parts: Vec<String>,
    pub full: bool,
}

impl From<&RefreshMaterializedViewStmt> for MvRefreshStatement {
    fn from(statement: &RefreshMaterializedViewStmt) -> Self {
        Self {
            name_parts: statement.name.parts.clone(),
            full: statement.full,
        }
    }
}

impl MvRefreshStatement {
    pub fn validate_supported(&self) -> Result<(), String> {
        if self.full {
            return Err(FULL_REFRESH_DISABLED_MESSAGE.to_string());
        }
        Ok(())
    }
}

#[cfg(test)]
mod aggregate_vocabulary_tests {
    use super::*;

    #[test]
    fn sqlx2_mv_aggregate_vocabulary_is_sql_owned() {
        assert_eq!(AggregateFunctionKind::Count, AggregateFunctionKind::Count);
        assert_eq!(
            VisibleAggregateOutput::GroupKey(0),
            VisibleAggregateOutput::GroupKey(0)
        );
    }
}

/// SQL facts that the frontend needs to atomically finalize the MV definition
/// after provider publication. The values are observed during preparation;
/// the frontend never resolves catalog metadata itself.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvRefreshFinalizeFacts {
    pub mv_id: i64,
    pub target: SqlMvTarget,
    pub base_snapshots: BTreeMap<String, Option<i64>>,
    pub base_table_uuids: BTreeMap<String, String>,
    pub expected_target_snapshot_id: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::{FULL_REFRESH_DISABLED_MESSAGE, MvRefreshStatement, SqlMvTarget};

    #[test]
    fn sqlx2_mv_target_identity_is_sql_owned() {
        let target = SqlMvTarget {
            catalog: Some("iceberg".to_string()),
            database: "analytics".to_string(),
            name: "daily_orders".to_string(),
        };

        assert_eq!(target.display_name(), "iceberg.analytics.daily_orders");
    }

    #[test]
    fn full_refresh_remains_an_explicitly_unsupported_request() {
        let error = MvRefreshStatement {
            name_parts: vec!["mv".to_string()],
            full: true,
        }
        .validate_supported()
        .expect_err("FULL must not silently downgrade");

        assert_eq!(error, FULL_REFRESH_DISABLED_MESSAGE);
    }
}
