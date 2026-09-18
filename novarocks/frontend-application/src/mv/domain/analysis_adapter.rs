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

//! Stateful materialized-view analysis and display adapter.

use arrow::datatypes::DataType;

use crate::mv::domain::analysis::{MvAnalysis, prepare_mv_select_for_catalog_provider};
use crate::mv::domain::application::MvShowStatement;
use crate::mv::domain::lifecycle::MvListRow;
use crate::mv::domain::model::MvStorageEngine;
use crate::mv::domain::readiness::MvReadinessPort;
use novarocks_mv_application::persistence::codec::{ConfigurationDocument, RefreshPolicy};
use novarocks_mv_application::persistence::projection::{MvPublicationState, StoredMvProjection};
use novarocks_mv_application::readiness::MvListedManageability;
use novarocks_query_application::api::{QueryResult, build_utf8_table_query_result};

/// Lightweight projection of the iceberg base table that
/// `validate_ivm_primary_key` needs. Built once at the top of `create_mv`
/// from the loaded iceberg table; passing this struct keeps validation
/// pure and easy to unit-test.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BaseColumnDescriptor {
    pub name: String,
    pub data_type: DataType,
    /// Uppercased SQL type as the analyzer/iceberg-schema mapper produced
    /// it (e.g. `BIGINT`, `STRING`, `DECIMAL(18,2)`, `ARRAY<STRING>`).
    pub sql_type: String,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BaseTableDescriptor {
    pub format_version: i32,
    pub columns: Vec<BaseColumnDescriptor>,
}

/// Validate that a parsed `PRIMARY KEY (col, ...)` clause on a CREATE
/// MATERIALIZED VIEW statement satisfies the IVM Phase-2 contract:
///
/// 1. The base table is iceberg format-version 2.
/// 2. Every PK column exists on the base table.
/// 3. Every PK column is NOT NULL on the base table.
/// 4. Every PK column has a hashable scalar type.
///
/// Errors fail fast in declared column order — the first mismatch wins.
/// Returns `Ok(())` on success and discards the PK list (PR-1 does not
/// persist it; PR-3 will).
pub(crate) fn validate_ivm_primary_key(
    pk_columns: &[String],
    base: &BaseTableDescriptor,
) -> Result<(), String> {
    // Messages are byte-identical to the provider's ChangeError Display, which
    // this used to borrow purely to render them; the only caller already
    // discarded the typed error via to_string().
    if base.format_version != 2 && base.format_version != 3 {
        return Err(format!(
            "iceberg base table format-version {} is not supported; IVM requires v2 or v3",
            base.format_version
        ));
    }
    for pk in pk_columns {
        let col = base
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(pk))
            .ok_or_else(|| {
                format!("PRIMARY KEY column `{pk}` does not exist on the iceberg base table")
            })?;
        if col.nullable {
            return Err(format!(
                "PRIMARY KEY column `{}` must be NOT NULL on the iceberg base table",
                col.name
            ));
        }
        if !is_hashable_pk_type(&col.sql_type) {
            return Err(format!(
                "PRIMARY KEY column `{}` has unsupported type `{}`; only hashable scalar types are allowed",
                col.name, col.sql_type
            ));
        }
    }
    Ok(())
}

/// Hashable scalar-type predicate for IVM Phase-2 PRIMARY KEY columns.
/// Accepts: BIGINT, INT, SMALLINT, TINYINT, STRING, VARCHAR, DATE,
/// DATETIME, DECIMAL (with or without precision/scale).
/// Rejects: BOOLEAN, FLOAT, DOUBLE, ARRAY, MAP, STRUCT, JSON.
fn is_hashable_pk_type(sql_type: &str) -> bool {
    let upper = sql_type.to_ascii_uppercase();
    let head = upper.split(['(', '<']).next().unwrap_or("").trim();
    matches!(
        head,
        "BIGINT"
            | "INT"
            | "INTEGER"
            | "SMALLINT"
            | "TINYINT"
            | "STRING"
            | "VARCHAR"
            | "CHAR"
            | "DATE"
            | "DATETIME"
            | "TIMESTAMP"
            | "DECIMAL"
    )
}

/// List materialized views from the readiness-filtered Accelerator projection.
pub(crate) fn list_mv_rows_with_ports(
    readiness: &MvReadinessPort,
    current_catalog: Option<&str>,
    stmt: &MvShowStatement,
    storage_filter: Option<MvStorageEngine>,
) -> Result<Vec<MvListRow>, String> {
    let projections = readiness
        .list_listable_projections()
        .map_err(|e| format!("load materialized view Accelerator projections failed: {e}"))?;

    let mut rows = Vec::new();
    for listed in &projections {
        let loaded = &listed.loaded;
        let projection = &loaded.projection;
        if !matches_show_filter(projection, current_catalog, stmt, storage_filter) {
            continue;
        }
        // A read-only target's dependency index is not a live management fact
        // and reading it would refuse. Its row reports the dependencies the
        // projection itself records, which is what SHOW is displaying anyway.
        let dependencies = match &listed.manageability {
            MvListedManageability::Manageable => {
                dependency_display_for_mv_with_readiness(readiness, loaded)?
            }
            MvListedManageability::ReadOnly(_) => String::new(),
        };
        rows.push(list_row_from_projection(
            projection,
            dependencies,
            manageability_display(&listed.manageability),
        ));
    }
    Ok(rows)
}

/// What SHOW prints for one target's manageability.
///
/// The reason is carried through rather than summarised: an operator seeing
/// READ_ONLY has to know whether it is a restart barrier they can retire or
/// another deployment's target they cannot.
fn manageability_display(manageability: &MvListedManageability) -> String {
    match manageability {
        MvListedManageability::Manageable => "MANAGEABLE".to_string(),
        MvListedManageability::ReadOnly(reason) => format!("READ_ONLY: {reason}"),
    }
}

fn matches_show_filter(
    projection: &StoredMvProjection,
    current_catalog: Option<&str>,
    stmt: &MvShowStatement,
    storage_filter: Option<MvStorageEngine>,
) -> bool {
    // Iceberg is the sole admitted MV storage provider. Catalog aliases are
    // target names, not provider identities.
    if storage_filter.is_some_and(|filter| filter != MvStorageEngine::Iceberg) {
        return false;
    }
    let target = projection.facts.target();
    if current_catalog.is_some_and(|catalog| {
        target
            .catalog()
            .is_none_or(|target_catalog| !target_catalog.eq_ignore_ascii_case(catalog))
    }) {
        return false;
    }
    !stmt
        .database
        .as_deref()
        .is_some_and(|database| !target.namespace().eq_ignore_ascii_case(database))
}

fn list_row_from_projection(
    projection: &StoredMvProjection,
    dependencies: String,
    manageability: String,
) -> MvListRow {
    let facts = &projection.facts;
    let target = facts.target();
    let definition = facts.definition();
    let configuration = facts.configuration();
    let (last_refresh_time, last_refresh_rows) = match facts.publication() {
        MvPublicationState::NeverPublished => (None, None),
        MvPublicationState::Published(published) => {
            let publication = published.document();
            (
                // This is P's frozen publication-fact time, not provider
                // commit completion or Accelerator insertion time.
                Some(publication.publication_prepared_at_ms.to_string()),
                // SHOW reports logical result rows only. Unknown is NULL,
                // never filled from physical storage or processed input rows.
                publication
                    .statistics
                    .logical_result_rows
                    .map(|rows| rows.to_string()),
            )
        }
    };
    MvListRow {
        manageability,
        name: target.name().to_string(),
        database: target.namespace().to_string(),
        storage_engine: MvStorageEngine::Iceberg.as_sql_str().to_string(),
        refresh_mode: match configuration.refresh_policy {
            RefreshPolicy::Manual => "DEFERRED_MANUAL",
            RefreshPolicy::AsyncOnChange => "ASYNC_ON_CHANGE",
            RefreshPolicy::AsyncInterval => "ASYNC_INTERVAL",
        }
        .to_string(),
        last_refresh_time,
        last_refresh_rows,
        base_tables: definition
            .relation_occurrences
            .iter()
            .map(|occurrence| {
                format!(
                    "{}.{}.{}",
                    occurrence.catalog_at_binding,
                    occurrence.namespace_at_binding,
                    occurrence.relation_at_binding,
                )
            })
            .collect::<Vec<_>>()
            .join(", "),
        select_text: definition.query.effective_sql.clone(),
        dependencies,
        refresh_paused: configuration.paused.to_string(),
        next_refresh_time: None,
        last_scheduler_error: None,
        max_staleness_ms: configuration
            .max_staleness_ms
            .map(|value| value.to_string()),
        refresh_state: refresh_status_for_configuration(configuration),
        retry_after_time: None,
    }
}

fn refresh_status_for_configuration(configuration: &ConfigurationDocument) -> String {
    if configuration.paused {
        return "PAUSED".to_string();
    }
    if matches!(configuration.refresh_policy, RefreshPolicy::Manual) {
        "MANUAL".to_string()
    } else {
        "PENDING".to_string()
    }
}

/// Render the dependency-column text for a single MV row through the typed
/// repository boundary.
fn dependency_display_for_mv_with_readiness(
    readiness: &MvReadinessPort,
    projection: &novarocks_mv_application::repository::LoadedMvProjection,
) -> Result<String, String> {
    let dependencies = readiness
        .list_ready_dependencies_by_downstream(projection)
        .map_err(|e| format!("load MV dependencies for display failed: {e}"))?;
    Ok(dependencies
        .iter()
        .map(|dep| dep.upstream.display_name())
        .collect::<Vec<_>>()
        .join(", "))
}

/// Analyze an MV SELECT against an already-admitted query-local table provider.
///
/// The provider is built by the query-assembly owner that admitted the
/// request: the request-local catalog snapshot, the exact connector control
/// lease, and the catalog-application admission gate are all frozen into it
/// before analysis starts. This adapter contributes MV SELECT preparation and
/// analysis only; it never acquires catalog or connector authority itself.
pub fn analyze_mv_select_with_provider(
    current_catalog: Option<&str>,
    provider: &dyn novarocks_sql::planning::catalog::PlannerTableProvider,
    current_database: &str,
    query: &novarocks_parser::ast::Query,
    functions: &dyn novarocks_sql::compiler::SqlFunctionCatalog,
) -> Result<MvAnalysis, String> {
    let prepared =
        prepare_mv_select_for_catalog_provider(query, current_catalog, current_database)?;
    let catalog = novarocks_sql::compiler::SqlPlannerTableSnapshot::new(provider);
    let refresh_input = novarocks_sql::compiler::analyze_mv_refresh_input(
        novarocks_sql::compiler::SqlMvRefreshAnalysisContext {
            query: Box::new(prepared.query_for_analysis().clone()),
            current_database: current_database.to_string(),
            catalog: &catalog,
            functions,
        },
    )?;
    let output_columns = refresh_input.analysis_facts().output_columns;
    Ok(MvAnalysis {
        resolved_refs: prepared.resolved_refs().to_vec(),
        output_columns,
        refresh_input,
    })
}

pub(crate) fn build_mv_rows_result(rows: &[MvListRow]) -> Result<QueryResult, String> {
    const COLUMNS: &[(&str, bool)] = &[
        ("Name", false),
        ("Database", false),
        ("StorageEngine", false),
        ("RefreshMode", false),
        ("LastRefreshTime", true),
        ("LastRefreshRows", true),
        ("BaseTables", false),
        ("SelectText", false),
        ("Dependencies", false),
        ("RefreshPaused", false),
        ("NextRefreshTime", true),
        ("LastSchedulerError", true),
        ("MaxStalenessMs", true),
        ("RefreshState", false),
        ("RetryAfterTime", true),
        ("Manageability", false),
    ];
    let rows = rows
        .iter()
        .map(|row| {
            vec![
                Some(row.name.clone()),
                Some(row.database.clone()),
                Some(row.storage_engine.clone()),
                Some(row.refresh_mode.clone()),
                row.last_refresh_time.clone(),
                row.last_refresh_rows.clone(),
                Some(row.base_tables.clone()),
                Some(row.select_text.clone()),
                Some(row.dependencies.clone()),
                Some(row.refresh_paused.clone()),
                row.next_refresh_time.clone(),
                row.last_scheduler_error.clone(),
                row.max_staleness_ms.clone(),
                Some(row.refresh_state.clone()),
                row.retry_after_time.clone(),
                Some(row.manageability.clone()),
            ]
        })
        .collect();
    build_utf8_table_query_result(COLUMNS, rows)
        .map_err(|error| format!("build SHOW MATERIALIZED VIEWS batch failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_mv_application::persistence::test_support::ProjectionFixture;
    use novarocks_mv_application::product::MvTarget;

    fn fixture(snapshot_id: Option<i64>) -> ProjectionFixture {
        ProjectionFixture::new(
            MvTarget::from_parts(Some("lake_alias"), "analytics", "orders_mv"),
            snapshot_id,
        )
    }

    fn stored(fixture: ProjectionFixture) -> StoredMvProjection {
        StoredMvProjection {
            mv_id: 42,
            facts: fixture.build().expect("valid document projection"),
        }
    }

    #[test]
    fn show_preserves_definition_occurrences_and_exact_target_names() {
        let projection = stored(fixture(Some(201)));
        let row = list_row_from_projection(
            &projection,
            "ice.sales.orders".to_string(),
            "MANAGEABLE".to_string(),
        );

        assert_eq!(row.name, "orders_mv");
        assert_eq!(row.database, "analytics");
        assert_eq!(row.storage_engine, "iceberg");
        assert_eq!(
            projection
                .facts
                .definition()
                .relation_occurrences
                .iter()
                .map(|occurrence| occurrence.occurrence_id)
                .collect::<Vec<_>>(),
            vec![7, 8],
        );
        assert_eq!(row.base_tables, "ice.sales.orders, ice.sales.orders");
        assert_eq!(
            row.select_text,
            projection.facts.definition().query.effective_sql,
        );
        assert_eq!(row.dependencies, "ice.sales.orders");
    }

    #[test]
    fn show_reports_publication_prepared_time_and_logical_rows_only() {
        let mut fixture = fixture(Some(201));
        let publication = fixture.publication.as_mut().expect("published fixture");
        publication.publication_prepared_at_ms = 1_700_000_001_234;
        publication.statistics.logical_result_rows = Some(7);
        publication.statistics.processed_input_rows = Some(101);
        fixture.storage_rows = Some(19);
        let projection = stored(fixture);
        let row = list_row_from_projection(&projection, String::new(), "MANAGEABLE".to_string());

        // P freezes this time before commit; it is neither provider commit
        // completion nor D's creation time or an Accelerator insertion time.
        assert_ne!(
            projection.facts.definition().created_at_ms,
            1_700_000_001_234,
        );
        assert_eq!(row.last_refresh_time.as_deref(), Some("1700000001234"),);
        assert_eq!(row.last_refresh_rows.as_deref(), Some("7"));
        let MvPublicationState::Published(published) = projection.facts.publication() else {
            panic!("expected a published projection");
        };
        assert_eq!(published.storage_rows(), Some(19));
        assert_eq!(
            published.document().statistics.processed_input_rows,
            Some(101)
        );
    }

    #[test]
    fn show_keeps_unknown_logical_rows_null_despite_other_row_statistics() {
        let mut fixture = fixture(Some(201));
        let publication = fixture.publication.as_mut().expect("published fixture");
        publication.statistics.logical_result_rows = None;
        publication.statistics.processed_input_rows = Some(101);
        fixture.storage_rows = Some(19);
        let row =
            list_row_from_projection(&stored(fixture), String::new(), "MANAGEABLE".to_string());

        assert!(row.last_refresh_time.is_some());
        assert_eq!(row.last_refresh_rows, None);
    }

    #[test]
    fn show_preserves_published_zero_logical_rows() {
        let mut fixture = fixture(Some(201));
        let publication = fixture.publication.as_mut().expect("published fixture");
        publication.output.empty_result = true;
        publication.statistics.logical_result_rows = Some(0);
        fixture.storage_rows = Some(0);
        let row =
            list_row_from_projection(&stored(fixture), String::new(), "MANAGEABLE".to_string());

        assert_eq!(row.last_refresh_rows.as_deref(), Some("0"));
    }

    #[test]
    fn show_never_published_has_no_refresh_time_or_row_count() {
        let projection = stored(fixture(None));
        let row = list_row_from_projection(&projection, String::new(), "MANAGEABLE".to_string());

        assert!(projection.facts.definition().created_at_ms > 0);
        assert_eq!(row.last_refresh_time, None);
        assert_eq!(row.last_refresh_rows, None);
        assert_eq!(row.next_refresh_time, None);
        assert_eq!(row.last_scheduler_error, None);
        assert_eq!(row.retry_after_time, None);
    }

    #[test]
    fn show_reads_refresh_policy_and_pause_from_configuration() {
        for (policy, interval, mode, state) in [
            (RefreshPolicy::Manual, None, "DEFERRED_MANUAL", "MANUAL"),
            (
                RefreshPolicy::AsyncOnChange,
                None,
                "ASYNC_ON_CHANGE",
                "PENDING",
            ),
            (
                RefreshPolicy::AsyncInterval,
                Some(60_000),
                "ASYNC_INTERVAL",
                "PENDING",
            ),
        ] {
            for paused in [false, true] {
                let mut fixture = fixture(None);
                fixture.configuration.refresh_policy = policy;
                fixture.configuration.refresh_interval_ms = interval;
                fixture.configuration.paused = paused;
                fixture.configuration.max_staleness_ms = Some(123);
                let row = list_row_from_projection(
                    &stored(fixture),
                    String::new(),
                    "MANAGEABLE".to_string(),
                );

                assert_eq!(row.refresh_mode, mode);
                assert_eq!(row.refresh_paused, paused.to_string());
                assert_eq!(row.refresh_state, if paused { "PAUSED" } else { state });
                assert_eq!(row.max_staleness_ms.as_deref(), Some("123"));
            }
        }
    }

    #[test]
    fn show_filters_use_projection_target_not_definition_resolution() {
        let projection = stored(fixture(None));
        let matching = MvShowStatement {
            database: Some("ANALYTICS".to_string()),
        };
        assert!(matches_show_filter(
            &projection,
            Some("LAKE_ALIAS"),
            &matching,
            Some(MvStorageEngine::Iceberg),
        ));
        assert!(!matches_show_filter(
            &projection,
            Some("ice"),
            &matching,
            None,
        ));
        assert!(!matches_show_filter(
            &projection,
            None,
            &MvShowStatement {
                database: Some("sales".to_string()),
            },
            None,
        ));
        assert!(!matches_show_filter(
            &projection,
            None,
            &matching,
            Some(MvStorageEngine::StarRocks),
        ));
    }
}

#[cfg(test)]
mod manageability_tests {
    use super::*;

    #[test]
    fn a_manageable_target_says_so_plainly() {
        assert_eq!(
            manageability_display(&MvListedManageability::Manageable),
            "MANAGEABLE"
        );
    }

    #[test]
    fn a_read_only_target_carries_the_reason_it_cannot_be_refreshed() {
        let shown = manageability_display(&MvListedManageability::ReadOnly(
            "a previous incarnation may still have an effect in flight".to_string(),
        ));

        assert!(shown.starts_with("READ_ONLY: "), "{shown}");
        assert!(
            shown.contains("previous incarnation"),
            "an operator has to tell a restart barrier from another deployment's target: {shown}"
        );
    }

    #[test]
    fn every_listed_row_has_a_manageability_column() {
        let row = MvListRow {
            name: "mv".to_string(),
            database: "db".to_string(),
            storage_engine: "iceberg".to_string(),
            refresh_mode: "DEFERRED_MANUAL".to_string(),
            last_refresh_time: None,
            last_refresh_rows: None,
            base_tables: String::new(),
            select_text: String::new(),
            dependencies: String::new(),
            refresh_paused: "false".to_string(),
            next_refresh_time: None,
            last_scheduler_error: None,
            max_staleness_ms: None,
            refresh_state: "IDLE".to_string(),
            retry_after_time: None,
            manageability: "READ_ONLY: closed after restart".to_string(),
        };

        let result = build_mv_rows_result(std::slice::from_ref(&row)).expect("one row");
        assert!(
            result
                .columns
                .iter()
                .any(|column| column.name() == "Manageability"),
            "SHOW has to report why a listed MV cannot be refreshed"
        );
    }
}
