//! Test-only server half of the W0 IMV statelessness harness.
//!
//! `novarocks_imv_stateless_rebuild` is a probe that rediscovers an MV
//! package's descriptor **purely from the lake** (MV table descriptor
//! properties, never SQLite) and
//! returns a one-row report describing the fidelity level the server can
//! currently reconstruct plus the descriptor content hash.
//!
//! Because this "bypass the runtime caches and rebuild from the lake" surface
//! must never exist on a production path, the procedure is guarded behind the
//! `NOVAROCKS_ENABLE_TEST_IMV_STATELESS_REBUILD` environment flag. It is
//! wired only through the standalone CALL dispatch and is exercised by the
//! sql-test runner's `@imv_stateless_rebuild` directive.
//!
//! W1 (MV package descriptors) already carries the definition, the visible
//! schema, and the base dependencies, all covered by the descriptor content
//! hash, so the server can reconstruct the `package` level today. The
//! `provenance` and `full` levels arrive with later umbrella tasks (W3a/W4).

use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::engine::mv::iceberg_discovery::discover_iceberg_mvs;
use crate::engine::procedure::CallProcedureStmt;
use crate::engine::{
    QueryResult, QueryResultColumn, StandaloneState, StatementResult, record_batch_to_chunk,
};

pub(crate) const PROCEDURE_NAME: &str = "novarocks_imv_stateless_rebuild";
const TEST_ENABLE_ENV: &str = "NOVAROCKS_ENABLE_TEST_IMV_STATELESS_REBUILD";

/// Fidelity level a stateless rebuild is expected to reconstruct. Mirrors the
/// sql-test runner's `ImvStatelessLevel`, but is a separate type because the
/// runner lives in a different crate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum StatelessLevel {
    Baseline,
    Package,
    Provenance,
    Full,
}

impl StatelessLevel {
    fn from_sql(s: &str) -> Result<Self, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "baseline" => Ok(Self::Baseline),
            "package" => Ok(Self::Package),
            "provenance" => Ok(Self::Provenance),
            "full" => Ok(Self::Full),
            other => Err(format!(
                "unknown stateless rebuild level `{other}`; expected one of baseline, package, provenance, full"
            )),
        }
    }

    fn as_sql(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::Package => "package",
            Self::Provenance => "provenance",
            Self::Full => "full",
        }
    }
}

/// Pure, race-free guard so tests never touch process env or construct state.
fn ensure_stateless_rebuild_enabled(flag: Option<&str>) -> Result<(), String> {
    if flag == Some("1") {
        Ok(())
    } else {
        Err(format!(
            "{PROCEDURE_NAME} is test-only; set {TEST_ENABLE_ENV}=1 to enable"
        ))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ImvStatelessRebuildRequest {
    pub catalog: String,
    pub namespace: String,
    pub mv: String,
    pub required_level: StatelessLevel,
}

impl ImvStatelessRebuildRequest {
    fn from_call(stmt: &CallProcedureStmt, current_database: &str) -> Result<Self, String> {
        let catalog = stmt.catalog.clone();
        let table = stmt
            .arg("table")
            .and_then(|value| value.as_string())
            .ok_or_else(|| format!("{PROCEDURE_NAME} requires a `table` argument"))?;
        let (namespace, mv) = split_table_reference(table, current_database)?;
        let required_level = match stmt.arg("level").and_then(|value| value.as_string()) {
            Some(level) => StatelessLevel::from_sql(level)?,
            None => StatelessLevel::Package,
        };
        Ok(Self {
            catalog,
            namespace,
            mv,
            required_level,
        })
    }
}

/// Split a `table` argument into `(namespace, mv)`. A bare name inherits the
/// current database as its namespace; a two-part `namespace.mv` is used as-is;
/// anything with more parts is rejected.
fn split_table_reference(table: &str, current_database: &str) -> Result<(String, String), String> {
    let parts: Vec<&str> = table.split('.').collect();
    match parts.as_slice() {
        [mv] => Ok((current_database.to_string(), (*mv).to_string())),
        [namespace, mv] => Ok(((*namespace).to_string(), (*mv).to_string())),
        _ => Err(format!(
            "{PROCEDURE_NAME} `table` must be `<mv>` or `<namespace>.<mv>`, got `{table}`"
        )),
    }
}

pub(crate) fn execute_novarocks_imv_stateless_rebuild(
    state: &Arc<StandaloneState>,
    stmt: &CallProcedureStmt,
    current_database: &str,
) -> Result<StatementResult, String> {
    ensure_stateless_rebuild_enabled(std::env::var(TEST_ENABLE_ENV).ok().as_deref())?;
    let req = ImvStatelessRebuildRequest::from_call(stmt, current_database)?;
    execute_request(state, &req)
}

fn execute_request(
    state: &Arc<StandaloneState>,
    req: &ImvStatelessRebuildRequest,
) -> Result<StatementResult, String> {
    // Discovering the MV package and reading its descriptor IS the lake
    // rebuild: it walks the Iceberg MV table descriptor and never consults
    // SQLite. If it fails, fail loud.
    let discovered = discover_iceberg_mvs(state, &req.catalog, &req.namespace)?;
    let mv = discovered
        .into_iter()
        .find(|entry| entry.public_name.eq_ignore_ascii_case(&req.mv))
        .ok_or_else(|| {
            format!(
                "MV '{}.{}' not found among lake-native Iceberg MV packages in catalog '{}'",
                req.namespace, req.mv, req.catalog
            )
        })?;

    let descriptor_hash = mv.descriptor.content_hash()?;
    // W1 landed the package descriptor, so the package level is reconstructable
    // today. W3a/W4 will raise this to provenance/full.
    let available = StatelessLevel::Package;
    let rebuild_source = "lake-mv-table";
    // The procedure reports the level it CAN reconstruct; the sql-test runner
    // asserts `available >= required`, so `required_level` is not gated here.
    let _ = req.required_level;

    Ok(StatementResult::Query(build_rebuild_result(
        available,
        &descriptor_hash,
        rebuild_source,
    )?))
}

/// Build the fixed one-row rebuild report. Columns are all `Utf8`; the three
/// hash columns are nullable because only the descriptor hash is populated at
/// the package level.
fn build_rebuild_result(
    available: StatelessLevel,
    descriptor_hash: &str,
    rebuild_source: &str,
) -> Result<QueryResult, String> {
    let columns = vec![
        column("AvailableLevel", false),
        column("DescriptorHash", true),
        column("ProvenanceHash", true),
        column("WaterlineHash", true),
        column("RebuildSource", false),
    ];
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![available.as_sql().to_string()])),
        Arc::new(StringArray::from(vec![Some(descriptor_hash.to_string())])),
        Arc::new(StringArray::from(vec![None::<String>])),
        Arc::new(StringArray::from(vec![None::<String>])),
        Arc::new(StringArray::from(vec![rebuild_source.to_string()])),
    ];
    build_query_result(columns, arrays)
}

fn build_query_result(
    columns: Vec<QueryResultColumn>,
    arrays: Vec<ArrayRef>,
) -> Result<QueryResult, String> {
    let fields = columns
        .iter()
        .map(|column| Field::new(&column.name, column.data_type.clone(), column.nullable))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|e| format!("build stateless rebuild result failed: {e}"))?;
    Ok(QueryResult {
        columns,
        chunks: vec![record_batch_to_chunk(batch)?],
    })
}

fn column(name: &str, nullable: bool) -> QueryResultColumn {
    QueryResultColumn {
        name: name.to_string(),
        data_type: DataType::Utf8,
        nullable,
        logical_type: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::procedure::parse_call_procedure_sql;

    #[test]
    fn guard_rejects_when_flag_absent() {
        let err = ensure_stateless_rebuild_enabled(None).unwrap_err();
        assert!(err.contains("test-only"), "unexpected error: {err}");
        let err = ensure_stateless_rebuild_enabled(Some("0")).unwrap_err();
        assert!(err.contains("test-only"), "unexpected error: {err}");
    }

    #[test]
    fn guard_accepts_when_flag_enabled() {
        assert!(ensure_stateless_rebuild_enabled(Some("1")).is_ok());
    }

    #[test]
    fn level_round_trips_case_insensitive() {
        for (input, expected) in [
            ("baseline", StatelessLevel::Baseline),
            ("Package", StatelessLevel::Package),
            ("PROVENANCE", StatelessLevel::Provenance),
            ("Full", StatelessLevel::Full),
        ] {
            let parsed = StatelessLevel::from_sql(input).unwrap();
            assert_eq!(parsed, expected);
            assert_eq!(StatelessLevel::from_sql(parsed.as_sql()).unwrap(), expected);
        }
    }

    #[test]
    fn level_rejects_unknown() {
        let err = StatelessLevel::from_sql("partial").unwrap_err();
        assert!(err.contains("unknown stateless rebuild level"), "{err}");
    }

    fn parse_request(
        sql: &str,
        current_database: &str,
    ) -> Result<ImvStatelessRebuildRequest, String> {
        let stmt = parse_call_procedure_sql(sql).unwrap();
        ImvStatelessRebuildRequest::from_call(&stmt, current_database)
    }

    #[test]
    fn from_call_parses_two_part_table_and_level() {
        let req = parse_request(
            "CALL ice.system.novarocks_imv_stateless_rebuild(table => 'analytics.mv_orders', level => 'baseline')",
            "default_db",
        )
        .unwrap();
        assert_eq!(req.catalog, "ice");
        assert_eq!(req.namespace, "analytics");
        assert_eq!(req.mv, "mv_orders");
        assert_eq!(req.required_level, StatelessLevel::Baseline);
    }

    #[test]
    fn from_call_bare_table_defaults_namespace_to_current_database() {
        let req = parse_request(
            "CALL ice.system.novarocks_imv_stateless_rebuild(table => 'mv_orders')",
            "analytics",
        )
        .unwrap();
        assert_eq!(req.namespace, "analytics");
        assert_eq!(req.mv, "mv_orders");
    }

    #[test]
    fn from_call_defaults_level_to_package() {
        let req = parse_request(
            "CALL ice.system.novarocks_imv_stateless_rebuild(table => 'analytics.mv_orders')",
            "default_db",
        )
        .unwrap();
        assert_eq!(req.required_level, StatelessLevel::Package);
    }

    #[test]
    fn from_call_requires_table_argument() {
        let err = parse_request(
            "CALL ice.system.novarocks_imv_stateless_rebuild(level => 'package')",
            "default_db",
        )
        .unwrap_err();
        assert!(err.contains("requires a `table` argument"), "{err}");
    }

    #[test]
    fn from_call_rejects_three_part_table() {
        let err = parse_request(
            "CALL ice.system.novarocks_imv_stateless_rebuild(table => 'ice.analytics.mv_orders')",
            "default_db",
        )
        .unwrap_err();
        assert!(
            err.contains("`table` must be `<mv>` or `<namespace>.<mv>`"),
            "{err}"
        );
    }
}
