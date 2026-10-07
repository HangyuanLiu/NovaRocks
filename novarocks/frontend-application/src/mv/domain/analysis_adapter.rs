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
#[allow(clippy::too_many_arguments)]
pub(crate) fn list_mv_rows_with_ports(
    readiness: &MvReadinessPort,
    entrance: Option<&novarocks_mv_application::management::ManagementEntrance>,
    current_catalog: Option<&str>,
    stmt: &MvShowStatement,
    storage_filter: Option<MvStorageEngine>,
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: &novarocks_spi::connector::ConnectorRequestContext,
    product_service: &novarocks_mv_application::service::MvProductService,
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
            MvListedManageability::ReadOnly(_) | MvListedManageability::Unavailable(_) => {
                String::new()
            }
        };
        let mut row = list_row_from_projection(
            projection,
            dependencies,
            manageability_display(&listed.manageability, entrance, projection),
        );
        let current = projection
            .facts
            .target()
            .catalog()
            .ok_or_else(|| "MV listing target has no catalog".to_string())
            .and_then(|catalog| {
                crate::connector::acquire_metadata_planning_lease(controls, catalog)
            })
            .and_then(|planning| {
                super::eligibility_document::observe_current(
                    &planning,
                    projection.facts.source_revision(),
                    context,
                )
                .map(|(_, documents)| documents.eligibility().cloned())
            });
        fill_eligibility_diagnostics(&mut row, &projection.facts, current);
        if let Some(stop) = product_service
            .automatic_refresh_stop_diagnostic(projection.mv_id, MAX_MV_DIAGNOSTIC_BYTES)
        {
            fill_automatic_stop_diagnostics(&mut row, &stop);
        }
        rows.push(row);
    }
    Ok(rows)
}

/// What SHOW prints for one target's manageability.
///
/// The reason is carried through rather than summarised: an operator seeing
/// READ_ONLY has to know whether it is a restart barrier they can retire or
/// another deployment's target they cannot.
fn manageability_display(
    manageability: &MvListedManageability,
    entrance: Option<&novarocks_mv_application::management::ManagementEntrance>,
    projection: &StoredMvProjection,
) -> String {
    use novarocks_mv_application::management::MvManagementPhase;

    match manageability {
        MvListedManageability::ReadOnly(reason) => {
            return bounded_diagnostic(&format!("READ_ONLY: {}", bounded_diagnostic(reason)));
        }
        MvListedManageability::Unavailable(reason) => {
            return bounded_diagnostic(&format!("UNAVAILABLE: {}", bounded_diagnostic(reason)));
        }
        MvListedManageability::Manageable => {}
    }
    // Readiness says this process may read the target. Whether it may write
    // it is the entrance's answer, and the two diverge exactly where it
    // matters: a target whose owner was just handed away is still a sound
    // query candidate while it is no longer this process's to refresh.
    let Some(entrance) = entrance else {
        return "MANAGEABLE".to_string();
    };
    match entrance.management_phase(&projection.facts.source_revision().target) {
        MvManagementPhase::Manageable => "MANAGEABLE".to_string(),
        MvManagementPhase::Managing => "MANAGING".to_string(),
        // A target this entrance has never observed is not one it has closed:
        // the projection is installed and nothing here holds it.
        MvManagementPhase::NotObserved => "MANAGEABLE".to_string(),
        other => format!("READ_ONLY: {}", other.as_str()),
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
        eligibility_state: "UNKNOWN".into(),
        eligibility_baseline: None,
        eligibility_generation: None,
        eligibility_attempt: None,
        eligibility_requested: None,
        eligibility_matched: None,
        eligibility_conclusion: "UNAVAILABLE".into(),
        eligibility_block_reason: None,
        automatic_refresh_stop_reason: None,
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

const MAX_MV_DIAGNOSTIC_BYTES: usize = 1024;

fn bounded_diagnostic(message: &str) -> String {
    let mut end = message.len().min(MAX_MV_DIAGNOSTIC_BYTES);
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    message[..end].to_owned()
}

fn hex_identity(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut result, "{byte:02x}").expect("String formatting");
    }
    result
}

fn fill_eligibility_diagnostics(
    row: &mut MvListRow,
    facts: &novarocks_mv_application::persistence::projection::MvDocumentProjection,
    observation: Result<
        Option<novarocks_mv_application::persistence::eligibility::EligibilityDocument>,
        String,
    >,
) {
    use novarocks_mv_application::persistence::eligibility::EligibilityState;
    let eligibility = match observation {
        Ok(eligibility) => eligibility,
        Err(error) => {
            row.eligibility_block_reason = Some(bounded_diagnostic(&error));
            return;
        }
    };
    if !facts.interpretation().aggregates.is_empty() {
        row.eligibility_state = "NOT_APPLICABLE".into();
        row.eligibility_conclusion = "NOT_REQUIRED".into();
        return;
    }
    let Some(eligibility) = eligibility else {
        let never_published = matches!(facts.publication(), MvPublicationState::NeverPublished);
        row.eligibility_state = if never_published {
            "NEVER_PUBLISHED"
        } else {
            "MISSING"
        }
        .into();
        row.eligibility_conclusion = "UNAVAILABLE".into();
        row.eligibility_block_reason =
            Some("REFRESH FULL is required to establish an eligible baseline".into());
        return;
    };
    let binding = &eligibility.binding;
    // Object identities can be wide. The fingerprint is explicitly labelled;
    // publication identity (at most 256 bytes) and revision remain exact.
    let fingerprint =
        novarocks_mv_application::persistence::identity::DocumentRevision::from_canonical_bytes(
            binding.object_id.as_bytes(),
        );
    row.eligibility_baseline = Some(format!(
        "object_sha256={}; publication={}; revision={}",
        hex_identity(fingerprint.as_bytes()),
        hex_identity(binding.publication_id.as_bytes()),
        hex_identity(binding.publication_revision.as_bytes())
    ));
    row.eligibility_generation = Some(binding.generation.to_string());
    match &eligibility.state {
        EligibilityState::Eligible => {
            row.eligibility_state = "ELIGIBLE".into();
            row.eligibility_conclusion = "NO_PENDING_VALIDATION".into();
        }
        EligibilityState::ValidationPending { attempt, .. } => {
            row.eligibility_state = "VALIDATION_PENDING".into();
            row.eligibility_attempt = Some(format!(
                "{:016x}{:016x}/{}",
                attempt.query_id().high(),
                attempt.query_id().low(),
                attempt.attempt_id().get()
            ));
            row.eligibility_conclusion = "VERIFICATION_RESULT_NOT_RECOVERED".into();
            row.eligibility_block_reason =
                Some("Verification result has not been recovered; REFRESH FULL is required".into());
        }
        EligibilityState::Invalid { evidence } => {
            row.eligibility_state = "INVALID".into();
            row.eligibility_requested = Some(evidence.requested.to_string());
            row.eligibility_matched = Some(evidence.matched.to_string());
            row.eligibility_conclusion = "COMPLETE_RETRACTION_SHORTAGE".into();
            row.eligibility_block_reason = Some(
                "Retraction demand exceeds visible target multiplicity; REFRESH FULL is required"
                    .into(),
            );
        }
    }
}

fn fill_automatic_stop_diagnostics(
    row: &mut MvListRow,
    stop: &novarocks_mv_application::scheduler_runtime::MvAutomaticRefreshStop,
) {
    use novarocks_mv_application::scheduler_runtime::MvAutomaticRefreshStopReason;
    row.automatic_refresh_stop_reason = Some(
        match stop.reason {
            MvAutomaticRefreshStopReason::CapacityRefused => "CAPACITY_REFUSED",
            MvAutomaticRefreshStopReason::TargetRefused => "TARGET_REFUSED",
        }
        .into(),
    );
    row.last_scheduler_error = Some(bounded_diagnostic(&stop.error));
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
        ("EligibilityState", false),
        ("EligibilityBaseline", true),
        ("EligibilityGeneration", true),
        ("EligibilityAttempt", true),
        ("EligibilityRequested", true),
        ("EligibilityMatched", true),
        ("EligibilityConclusion", false),
        ("EligibilityBlockReason", true),
        ("AutomaticRefreshStopReason", true),
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
                Some(row.eligibility_state.clone()),
                row.eligibility_baseline.clone(),
                row.eligibility_generation.clone(),
                row.eligibility_attempt.clone(),
                row.eligibility_requested.clone(),
                row.eligibility_matched.clone(),
                Some(row.eligibility_conclusion.clone()),
                row.eligibility_block_reason.clone(),
                row.automatic_refresh_stop_reason.clone(),
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

    use novarocks_mv_application::persistence::test_support::ProjectionFixture;

    fn projection() -> StoredMvProjection {
        StoredMvProjection {
            mv_id: 42,
            facts: ProjectionFixture::new(
                novarocks_mv_application::product::MvTarget::from_parts(Some("ice"), "db", "mv"),
                Some(1),
            )
            .build()
            .expect("valid document projection"),
        }
    }

    #[test]
    fn a_manageable_target_says_so_plainly() {
        assert_eq!(
            manageability_display(&MvListedManageability::Manageable, None, &projection()),
            "MANAGEABLE"
        );
    }

    #[test]
    fn a_read_only_target_carries_the_reason_it_cannot_be_refreshed() {
        let shown = manageability_display(
            &MvListedManageability::ReadOnly(
                "a previous incarnation may still have an effect in flight".to_string(),
            ),
            None,
            &projection(),
        );

        assert!(shown.starts_with("READ_ONLY: "), "{shown}");
        assert!(
            shown.contains("previous incarnation"),
            "an operator has to tell a restart barrier from another deployment's target: {shown}"
        );
    }

    #[test]
    fn a_quarantined_target_is_listed_with_the_reason_it_is_not_trusted() {
        let shown = manageability_display(
            &MvListedManageability::Unavailable("catalog discovery read failed".to_string()),
            None,
            &projection(),
        );

        assert_eq!(shown, "UNAVAILABLE: catalog discovery read failed");
    }

    #[test]
    fn a_target_the_entrance_has_closed_is_not_reported_manageable() {
        use novarocks_mv_application::management::{
            DeploymentOwner, ManagementEntrance, ProcessIncarnation,
        };

        let entrance = ManagementEntrance::new(
            DeploymentOwner::parse("deployment-a").expect("owner"),
            ProcessIncarnation::parse("inc-a").expect("incarnation"),
        );
        entrance.begin_stopping();

        let shown = manageability_display(
            &MvListedManageability::Manageable,
            Some(&entrance),
            &projection(),
        );

        assert_eq!(
            shown, "READ_ONLY: STOPPING",
            "readiness says the projection is readable; only the entrance knows it is not writable"
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
            eligibility_state: "UNKNOWN".into(),
            eligibility_baseline: None,
            eligibility_generation: None,
            eligibility_attempt: None,
            eligibility_requested: None,
            eligibility_matched: None,
            eligibility_conclusion: "UNAVAILABLE".into(),
            eligibility_block_reason: None,
            automatic_refresh_stop_reason: None,
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

    fn visible_projection() -> StoredMvProjection {
        use novarocks_mv_application::persistence::codec::PhysicalFieldLogicalIdentity;
        let mut fixture =
            novarocks_mv_application::persistence::test_support::ProjectionFixture::new(
                novarocks_mv_application::product::MvTarget::from_parts(
                    Some("lake_alias"),
                    "analytics",
                    "orders_mv",
                ),
                Some(201),
            );
        fixture.interpretation.aggregates.clear();
        fixture.interpretation.state_slots.clear();
        fixture.interpretation.apply_key = None;
        fixture.interpretation.branches.clear();
        fixture.interpretation.target.fields.retain(|field| {
            matches!(
                field.logical_identity,
                PhysicalFieldLogicalIdentity::Output(_)
            )
        });
        StoredMvProjection {
            mv_id: 42,
            facts: fixture.build().expect("visible tuple projection"),
        }
    }

    #[test]
    fn show_current_eligibility_has_exact_baseline_attempt_and_complete_shortage() {
        use novarocks_mv_application::persistence::eligibility::{
            EligibilityDocument, EligibilityEvidence, EligibilityState,
        };
        use novarocks_types::{AttemptId, QueryExecutionId, QueryId};
        let projection = visible_projection();
        let MvPublicationState::Published(publication) = projection.facts.publication() else {
            panic!("published fixture")
        };
        let eligible = EligibilityDocument {
            binding: super::super::eligibility_document::published_binding(
                projection.facts.definition(),
                publication.document(),
                1,
            )
            .unwrap(),
            state: EligibilityState::Eligible,
        };
        let attempt =
            QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(3).unwrap()).unwrap();
        let pending = super::super::eligibility_document::pending(&eligible, attempt, 201).unwrap();
        let mut invalid = pending.clone();
        invalid.binding.generation += 1;
        invalid.state = EligibilityState::Invalid {
            evidence: EligibilityEvidence {
                requested: 9,
                matched: 8,
                samples: vec![],
            },
        };
        for (document, state, conclusion) in [
            (eligible, "ELIGIBLE", "NO_PENDING_VALIDATION"),
            (
                pending,
                "VALIDATION_PENDING",
                "VERIFICATION_RESULT_NOT_RECOVERED",
            ),
            (invalid, "INVALID", "COMPLETE_RETRACTION_SHORTAGE"),
        ] {
            let mut row = list_row_from_projection(
                &projection,
                String::new(),
                "READ_ONLY: UNKNOWN_EFFECT".into(),
            );
            fill_eligibility_diagnostics(&mut row, &projection.facts, Ok(Some(document.clone())));
            assert_eq!(row.eligibility_state, state);
            assert_eq!(row.eligibility_conclusion, conclusion);
            assert_eq!(
                row.eligibility_generation,
                Some(document.binding.generation.to_string())
            );
            let baseline = row.eligibility_baseline.as_ref().unwrap();
            assert!(baseline.contains(&hex_identity(
                document.binding.publication_revision.as_bytes()
            )));
            assert!(baseline.contains(&hex_identity(document.binding.publication_id.as_bytes())));
            assert!(baseline.len() <= MAX_MV_DIAGNOSTIC_BYTES);
            assert_eq!(row.manageability, "READ_ONLY: UNKNOWN_EFFECT");
            if state == "VALIDATION_PENDING" {
                assert_eq!(
                    row.eligibility_attempt.as_deref(),
                    Some("00000000000000010000000000000002/3")
                );
                assert!(row.eligibility_requested.is_none());
                assert!(row.eligibility_matched.is_none());
            } else if state == "INVALID" {
                assert_eq!(row.eligibility_requested.as_deref(), Some("9"));
                assert_eq!(row.eligibility_matched.as_deref(), Some("8"));
            }
            let result = build_mv_rows_result(&[row]).unwrap();
            for name in [
                "EligibilityState",
                "EligibilityBaseline",
                "EligibilityGeneration",
                "EligibilityAttempt",
                "EligibilityRequested",
                "EligibilityMatched",
                "EligibilityConclusion",
                "EligibilityBlockReason",
                "AutomaticRefreshStopReason",
                "Manageability",
            ] {
                assert!(result.columns.iter().any(|column| column.name() == name));
            }
        }
    }

    #[test]
    fn show_observation_failure_never_infers_eligible_and_keeps_bounded_stop_distinct() {
        use novarocks_mv_application::scheduler_runtime::{
            MvAutomaticRefreshStop, MvAutomaticRefreshStopReason,
        };
        let projection = visible_projection();
        let mut row = list_row_from_projection(&projection, String::new(), "MANAGEABLE".into());
        fill_eligibility_diagnostics(
            &mut row,
            &projection.facts,
            Err("unavailable中".repeat(1000)),
        );
        fill_automatic_stop_diagnostics(
            &mut row,
            &MvAutomaticRefreshStop {
                reason: MvAutomaticRefreshStopReason::CapacityRefused,
                error: "capacity中".repeat(1000),
            },
        );
        assert_eq!(row.eligibility_state, "UNKNOWN");
        assert_eq!(row.eligibility_conclusion, "UNAVAILABLE");
        assert!(row.eligibility_baseline.is_none());
        assert!(row.eligibility_requested.is_none());
        assert!(row.eligibility_block_reason.as_ref().unwrap().len() <= MAX_MV_DIAGNOSTIC_BYTES);
        assert!(row.last_scheduler_error.as_ref().unwrap().len() <= MAX_MV_DIAGNOSTIC_BYTES);
        assert_eq!(
            row.automatic_refresh_stop_reason.as_deref(),
            Some("CAPACITY_REFUSED")
        );
        assert_eq!(row.manageability, "MANAGEABLE");
        assert_eq!(row.refresh_paused, "false");
        let mut missing = list_row_from_projection(&projection, String::new(), "MANAGEABLE".into());
        fill_eligibility_diagnostics(&mut missing, &projection.facts, Ok(None));
        assert_eq!(missing.eligibility_state, "MISSING");
    }
}
