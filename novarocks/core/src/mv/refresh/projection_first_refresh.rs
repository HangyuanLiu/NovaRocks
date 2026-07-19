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

//! Projection/filter MV full-read preparation.
//!
//! This module owns exact snapshot pinning and physical projection shaping.
//! The standalone engine supplies query execution through an invocation-local
//! callback; lifecycle and Iceberg writes stay in the engine.

use std::collections::HashSet;

use crate::exec::chunk::Chunk;
use crate::mv::refresh::pin::{RefreshSnapshotPin, inject_pin_as_for_version_as_of};
use crate::mv::refresh::target_apply::iceberg_mv_physical_select_sql;

pub(crate) fn prepare_projection_full_read_sql(
    select_sql: &str,
    pin: &RefreshSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<String, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(select_sql)
        .map_err(|error| format!("iceberg projection full-read SELECT normalize error: {error}"))?;
    let mut statement = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|error| format!("iceberg projection full-read SELECT parse error: {error}"))?;
    let sqlparser::ast::Statement::Query(query) = &mut statement else {
        return Err("iceberg projection full read expects a SELECT query".to_string());
    };
    inject_pin_as_for_version_as_of(
        query,
        pin,
        &HashSet::new(),
        current_catalog,
        current_database,
    )?;
    iceberg_mv_physical_select_sql(&statement.to_string())
}

pub(crate) fn prepare_projection_first_refresh_chunks<F>(
    select_sql: &str,
    pin: &RefreshSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
    read: &mut F,
) -> Result<Vec<Chunk>, String>
where
    F: FnMut(&str) -> Result<Vec<Chunk>, String>,
{
    let physical_sql =
        prepare_projection_full_read_sql(select_sql, pin, current_catalog, current_database)?;
    read(&physical_sql)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pin() -> RefreshSnapshotPin {
        RefreshSnapshotPin::from_entries_for_tests(&[("ice.db.fact", 42, "fact-uuid")])
    }

    #[test]
    fn single_preparation_injects_exact_pin_and_physical_apply_key() {
        let mut reads = 0;
        let chunks = prepare_projection_first_refresh_chunks(
            "SELECT id, name FROM ice.db.fact",
            &pin(),
            Some("ice"),
            "db",
            &mut |physical_sql| {
                reads += 1;
                assert!(physical_sql.contains("VERSION AS OF 42"), "{physical_sql}");
                assert!(physical_sql.contains("_row_id"), "{physical_sql}");
                assert!(
                    physical_sql.contains("__nova_base_row_id"),
                    "{physical_sql}"
                );
                Ok(Vec::new())
            },
        )
        .expect("prepare single projection first refresh");

        assert_eq!(reads, 1);
        assert!(chunks.is_empty());
    }

    #[test]
    fn single_preparation_rejects_conflicting_explicit_time_travel_before_read() {
        let mut reads = 0;
        let error = prepare_projection_first_refresh_chunks(
            "SELECT id FROM ice.db.fact VERSION AS OF 7",
            &pin(),
            Some("ice"),
            "db",
            &mut |_| {
                reads += 1;
                Ok(Vec::new())
            },
        )
        .expect_err("conflicting time travel must fail");

        assert_eq!(reads, 0);
        assert!(error.contains("must not write explicit"), "{error}");
    }

    #[test]
    fn single_preparation_rejects_wildcard_and_reserved_alias_before_read() {
        for (select_sql, expected) in [
            (
                "SELECT * FROM ice.db.fact",
                "requires explicit projection columns",
            ),
            (
                "SELECT id AS __nova_base_row_id FROM ice.db.fact",
                "reserved for internal apply key",
            ),
        ] {
            let mut reads = 0;
            let error = prepare_projection_first_refresh_chunks(
                select_sql,
                &pin(),
                Some("ice"),
                "db",
                &mut |_| {
                    reads += 1;
                    Ok(Vec::new())
                },
            )
            .expect_err("invalid physical projection must fail");

            assert_eq!(reads, 0, "sql={select_sql}");
            assert!(error.contains(expected), "sql={select_sql} error={error}");
        }
    }

    #[test]
    fn single_callback_failure_propagates_without_chunks() {
        let error = prepare_projection_first_refresh_chunks(
            "SELECT id FROM ice.db.fact",
            &pin(),
            Some("ice"),
            "db",
            &mut |_| Err("projection read failed".to_string()),
        )
        .expect_err("callback failure must propagate");

        assert_eq!(error, "projection read failed");
    }
}
