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

//! SQL-owned DML syntax and planning entrypoints.
//!
//! Application code may retain provider leases, write fences, and execution
//! lifecycle state, but it must not reach into parser or optimizer internals.
//! This submodule is the deliberately narrow handoff for those DML-specific
//! facts.  Its module hook is installed by the SQL facade integration wave.

pub use crate::analyzer::iceberg_ref::{IcebergRefSuffix, split_ref_suffix};
pub use crate::parser::dialect::add_files::{AddFilesCommand, classify_add_files};

const ICEBERG_FILE_PATH_COLUMN: &str = "_file";
const ICEBERG_ROW_POSITION_COLUMN: &str = "_pos";
const ICEBERG_ROW_ID_COLUMN: &str = "_row_id";
const ICEBERG_LAST_UPDATED_SEQUENCE_COLUMN: &str = "_last_updated_sequence_number";

/// One application-admitted, immutable statistics observation. It contains no
/// resolver, table handle, lease, or callback, so SQL cannot retry against a
/// newer connector generation while compiling the paired request.
#[derive(Clone, Debug)]
pub enum DmlStatisticsEvidence {
    Available {
        binding: crate::binding::SqlTableBindingId,
        label: String,
        columns: Vec<novarocks_catalog::schema::ColumnDef>,
        evidence: novarocks_spi::connector::StatisticsEvidence,
        optimizer_usable: bool,
    },
    Missing {
        binding: crate::binding::SqlTableBindingId,
        label: String,
        reason: String,
    },
    Fatal {
        binding: crate::binding::SqlTableBindingId,
        label: String,
        failure: DmlStatisticsFailure,
    },
}

/// An admission-time contradiction between connector evidence and the frozen
/// table binding.  These failures are carried into SQL unchanged; SQL never
/// retries a catalog or statistics lookup to replace them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DmlStatisticsFailure {
    BindingMissing,
    OwnerMismatch,
    IncarnationMismatch,
    DataVersionMismatch,
    CorruptEvidence(String),
}

/// SQL-owned opaque statistics carrier for one compile request.
#[derive(Clone, Debug)]
pub struct DmlStatisticsSnapshot(pub(crate) crate::optimizer::stats_input::SqlStatisticsSnapshot);

impl Default for DmlStatisticsSnapshot {
    fn default() -> Self {
        Self::empty()
    }
}

impl DmlStatisticsSnapshot {
    /// Construct an intentionally empty snapshot for an already SQL-owned
    /// logical input. Empty means evidence is unavailable, never that a table
    /// has zero rows.
    pub fn empty() -> Self {
        Self(crate::optimizer::stats_input::SqlStatisticsSnapshot::empty())
    }

    /// Seal admission-frozen connector evidence into the compiler's private
    /// statistics representation. The public input is immutable data only;
    /// provider handles, leases, and resolver callbacks cannot cross this
    /// boundary.
    pub fn from_evidence(entries: impl IntoIterator<Item = DmlStatisticsEvidence>) -> Self {
        use crate::optimizer::stats_input::{
            BaseTableStatistics, SqlTableStatisticsEvidence, StatsMissingReason,
        };

        let mut snapshot = crate::optimizer::stats_input::SqlStatisticsSnapshot::empty();
        for entry in entries {
            match entry {
                DmlStatisticsEvidence::Available {
                    binding,
                    label,
                    columns,
                    evidence,
                    optimizer_usable,
                } => snapshot.insert(
                    binding,
                    SqlTableStatisticsEvidence {
                        label,
                        statistics: evidence_to_base_statistics(
                            &evidence,
                            &columns,
                            optimizer_usable,
                        ),
                    },
                ),
                DmlStatisticsEvidence::Missing {
                    binding,
                    label,
                    reason,
                } => snapshot.insert(
                    binding,
                    SqlTableStatisticsEvidence {
                        label,
                        statistics: BaseTableStatistics::missing(
                            StatsMissingReason::CatalogLoadError(reason),
                        ),
                    },
                ),
                DmlStatisticsEvidence::Fatal {
                    binding, failure, ..
                } => snapshot.insert_fatal(binding, match_failure(failure)),
            }
        }
        Self(snapshot)
    }
}

fn match_failure(
    failure: DmlStatisticsFailure,
) -> crate::optimizer::stats_input::SqlStatisticsFatalError {
    use crate::optimizer::stats_input::SqlStatisticsFatalError;

    match failure {
        DmlStatisticsFailure::BindingMissing => SqlStatisticsFatalError::BindingMissing,
        DmlStatisticsFailure::OwnerMismatch => SqlStatisticsFatalError::OwnerMismatch,
        DmlStatisticsFailure::IncarnationMismatch => SqlStatisticsFatalError::IncarnationMismatch,
        DmlStatisticsFailure::DataVersionMismatch => SqlStatisticsFatalError::DataVersionMismatch,
        DmlStatisticsFailure::CorruptEvidence(message) => {
            SqlStatisticsFatalError::CorruptEvidence(message)
        }
    }
}

fn evidence_to_base_statistics(
    evidence: &novarocks_spi::connector::StatisticsEvidence,
    columns: &[novarocks_catalog::schema::ColumnDef],
    optimizer_usable: bool,
) -> crate::optimizer::stats_input::BaseTableStatistics {
    use crate::optimizer::statistics::Confidence;
    use crate::optimizer::stats_input::{
        BaseColumnStatistics, BaseTableStatistics, StatValue, StatsMissingReason, StatsSource,
    };
    use novarocks_spi::connector::{
        StatisticsMetric, StatisticsMetricState, StatisticsMetricValue, StatisticsProvenance,
    };

    if !optimizer_usable {
        return BaseTableStatistics::missing(StatsMissingReason::CatalogLoadError(
            "connector evidence is not Full+Exact for the pinned table version".to_string(),
        ));
    }
    let source = match evidence.provenance {
        StatisticsProvenance::ProviderArtifact => StatsSource::IcebergPuffin,
        StatisticsProvenance::Manifest => StatsSource::IcebergManifest,
        StatisticsProvenance::VisibleRows | StatisticsProvenance::Provider(_) => {
            StatsSource::ConnectorEstimate
        }
    };
    let metric_u64 = |state: Option<&StatisticsMetricState>| match state {
        Some(StatisticsMetricState::Available(StatisticsMetricValue::U64(value))) => Some(*value),
        Some(StatisticsMetricState::Available(StatisticsMetricValue::I64(value))) => {
            u64::try_from(*value).ok()
        }
        Some(StatisticsMetricState::Available(StatisticsMetricValue::F64(value)))
            if value.is_finite() && *value >= 0.0 && *value <= u64::MAX as f64 =>
        {
            Some(*value as u64)
        }
        _ => None,
    };
    let metric_f64 = |state: Option<&StatisticsMetricState>,
                      data_type: Option<&arrow::datatypes::DataType>| {
        let value = match state {
            Some(StatisticsMetricState::Available(StatisticsMetricValue::U64(value))) => {
                *value as f64
            }
            Some(StatisticsMetricState::Available(StatisticsMetricValue::I64(value))) => {
                *value as f64
            }
            Some(StatisticsMetricState::Available(StatisticsMetricValue::F64(value))) => *value,
            Some(StatisticsMetricState::Available(StatisticsMetricValue::Bytes(value)))
                if matches!(data_type, Some(arrow::datatypes::DataType::FixedSizeBinary(width)) if *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH)
                    && value.len()
                        == usize::try_from(novarocks_types::largeint::LARGEINT_BYTE_WIDTH)
                            .ok()? =>
            {
                novarocks_types::largeint::i128_from_be_bytes(value).ok()? as f64
            }
            _ => return None,
        };
        value.is_finite().then_some(value)
    };

    let row_count = metric_u64(evidence.metrics.get(&StatisticsMetric::RowCount));
    let row_count_stat = row_count
        .map(|value| StatValue::known(value, Confidence::Exact, source))
        .unwrap_or_else(|| {
            StatValue::missing(StatsMissingReason::ColumnNotReported("row_count".into()))
        });
    let mut base_columns = std::collections::HashMap::new();
    for column in columns {
        let name = column.name.to_ascii_lowercase();
        let key = std::sync::Arc::<str>::from(column.name.as_str());
        let missing = || StatsMissingReason::ColumnNotReported(name.clone());
        let null_count = metric_u64(evidence.metrics.get(&StatisticsMetric::NullCount {
            column: std::sync::Arc::clone(&key),
        }));
        let nulls_fraction = match (null_count, row_count) {
            (Some(nulls), Some(rows)) if rows > 0 => {
                StatValue::known(nulls as f64 / rows as f64, Confidence::Exact, source)
            }
            (Some(0), Some(0)) => StatValue::known(0.0, Confidence::Exact, source),
            _ => StatValue::missing(missing()),
        };
        base_columns.insert(
            name.clone(),
            BaseColumnStatistics {
                nulls_fraction,
                average_row_size: metric_f64(
                    evidence.metrics.get(&StatisticsMetric::AverageSize {
                        column: std::sync::Arc::clone(&key),
                    }),
                    None,
                )
                .map(|value| StatValue::known(value, Confidence::Exact, source))
                .unwrap_or_else(|| StatValue::missing(missing())),
                min_value: metric_f64(
                    evidence.metrics.get(&StatisticsMetric::Minimum {
                        column: std::sync::Arc::clone(&key),
                    }),
                    Some(&column.data_type),
                )
                .map(|value| StatValue::known(value, Confidence::Exact, source))
                .unwrap_or_else(|| StatValue::missing(missing())),
                max_value: metric_f64(
                    evidence.metrics.get(&StatisticsMetric::Maximum {
                        column: std::sync::Arc::clone(&key),
                    }),
                    Some(&column.data_type),
                )
                .map(|value| StatValue::known(value, Confidence::Exact, source))
                .unwrap_or_else(|| StatValue::missing(missing())),
                ndv: StatValue::missing(StatsMissingReason::ColumnNotReported(format!(
                    "approximate Theta NDV for `{name}` is not an exact optimizer statistic"
                ))),
            },
        );
    }
    BaseTableStatistics {
        row_count: row_count_stat,
        columns: base_columns,
        source,
    }
}

/// Parse one normalized raw statement for DML admission.  The returned AST is
/// the upstream SQL parser's public syntax tree; NovaRocks custom syntax is
/// normalized before this boundary.
pub fn parse_raw_statement(sql: &str) -> Result<sqlparser::ast::Statement, String> {
    crate::parser::parse_sql_raw(sql)
}

/// Application-owned DTO for the only custom statement shape that DML needs
/// to inspect before catalog resolution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TruncateCommand {
    pub target_parts: Vec<String>,
    pub target_ref: String,
}

/// Classify one `TRUNCATE TABLE` command without performing a catalog lookup
/// or opening a connector session.
pub fn parse_truncate_command(sql: &str) -> Result<Option<TruncateCommand>, String> {
    let trimmed = sql.trim_start();
    let keyword_end = trimmed
        .char_indices()
        .find_map(|(index, ch)| (!ch.is_ascii_alphabetic()).then_some(index))
        .unwrap_or(trimmed.len());
    if !trimmed[..keyword_end].eq_ignore_ascii_case("truncate") {
        return Ok(None);
    }
    let mut statements = crate::parser::parse_sql(trimmed)?;
    if statements.len() != 1 {
        return Err("TRUNCATE request must contain exactly one statement".to_string());
    }
    match statements.remove(0) {
        crate::parser::ast::Statement::Truncate { name, target_ref } => Ok(Some(TruncateCommand {
            target_parts: name.parts,
            target_ref,
        })),
        _ => Ok(None),
    }
}

/// Application-neutral classification of the statistics statements recognized
/// by NovaRocks' dialect.  Catalog resolution and durable-job ownership stay
/// outside SQL.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatisticsCommand {
    AnalyzeTable {
        target_parts: Vec<String>,
        columns: Vec<String>,
    },
    ShowAnalyzeJobs,
    CancelAnalyze {
        job_id: String,
    },
    ShowTableStats {
        target_parts: Vec<String>,
    },
}

/// Parse one complete statistics command.  A statement outside this dialect
/// subset is reported as `None`, preserving the caller's normal SQL routing.
pub fn parse_statistics_command(sql: &str) -> Result<Option<StatisticsCommand>, String> {
    let normalized = crate::parser::dialect::normalize_for_raw_parse(sql)?;
    let mut statements = match crate::parser::parse_sql(&normalized) {
        Ok(statements) => statements,
        Err(_) => return Ok(None),
    };
    if statements.len() != 1 {
        return Err("statistics command accepts exactly one statement".to_string());
    }
    match statements.pop().expect("one checked statement") {
        crate::parser::ast::Statement::AnalyzeTable(statement) => {
            Ok(Some(StatisticsCommand::AnalyzeTable {
                target_parts: statement.name.parts,
                columns: statement.columns,
            }))
        }
        crate::parser::ast::Statement::ShowAnalyzeJobs(_) => {
            Ok(Some(StatisticsCommand::ShowAnalyzeJobs))
        }
        crate::parser::ast::Statement::CancelAnalyze(statement) => {
            Ok(Some(StatisticsCommand::CancelAnalyze {
                job_id: statement.job_id,
            }))
        }
        crate::parser::ast::Statement::ShowTableStats(statement) => {
            Ok(Some(StatisticsCommand::ShowTableStats {
                target_parts: statement.name.parts,
            }))
        }
        _ => Ok(None),
    }
}

/// The SQL-visible kind of terminal row-mutation writer.  This maps one
/// provider-signed Arrow input shape to its immutable SQL write contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DmlWriteSinkMode {
    Data,
    RowLineageData,
    PositionDeletes,
    DeletionVectors,
    EqualityDeletes,
}

impl From<DmlWriteSinkMode> for crate::planner::distributed::write::contract::SqlWriteSinkMode {
    fn from(value: DmlWriteSinkMode) -> Self {
        match value {
            DmlWriteSinkMode::Data => Self::Data,
            DmlWriteSinkMode::RowLineageData => Self::RowLineageData,
            DmlWriteSinkMode::PositionDeletes => Self::PositionDeletes,
            DmlWriteSinkMode::DeletionVectors => Self::DeletionVectors,
            DmlWriteSinkMode::EqualityDeletes => Self::EqualityDeletes,
        }
    }
}

/// One provider-signed input field retained in a SQL terminal contract.
#[derive(Clone, Debug, PartialEq)]
pub struct DmlWriteTargetField {
    pub token: novarocks_spi::connector::ConnectorWriteFieldToken,
    pub column: novarocks_catalog::schema::ColumnDef,
    pub is_hidden: bool,
}

/// Immutable SQL facts for an already admitted write target.  The binding is
/// only an opaque request-local token; no provider table or writer handle can
/// cross into this value.
#[derive(Clone, Debug, PartialEq)]
pub struct DmlWriteTarget {
    pub binding: crate::binding::SqlTableBindingId,
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub fields: Vec<DmlWriteTargetField>,
}

/// Opaque SQL terminal write contract.  Application code can construct it
/// from its admitted binding facts but cannot inspect or mutate the private
/// planner contract afterwards.
#[derive(Clone, Debug)]
pub struct DmlWritePlanInput(crate::planner::distributed::write::contract::SqlWritePlanInput);

impl DmlWritePlanInput {
    pub fn try_new(
        mode: DmlWriteSinkMode,
        target: DmlWriteTarget,
        input_columns: Vec<novarocks_catalog::schema::ColumnDef>,
        input: crate::plan_read::ConnectorWriteInputBinding,
    ) -> Result<Self, String> {
        use crate::planner::distributed::write::contract::{
            SqlWriteSinkContract, SqlWriteSinkTargetContract, SqlWriteTargetField,
        };
        use crate::planner::table::SqlTableIdentity;

        let target = SqlWriteSinkTargetContract::try_new(
            target.binding,
            SqlTableIdentity {
                catalog: target.catalog,
                namespace: target.namespace,
                table: target.table,
            },
            target
                .fields
                .into_iter()
                .map(|field| SqlWriteTargetField {
                    token: field.token,
                    column: field.column,
                    is_hidden: field.is_hidden,
                })
                .collect(),
        )?;
        Ok(Self(
            crate::planner::distributed::write::contract::SqlWritePlanInput {
                contract: SqlWriteSinkContract::try_new(mode.into(), target, input_columns)?,
                input,
                root_output_exprs: None,
            },
        ))
    }
}

/// Seal one application-admitted frozen connector source into an immutable
/// distributed terminal-write plan. The physical source and write contract
/// remain opaque outside SQL; provider bindings, leases, and provenance stay
/// retained by the application that admitted them.
pub fn build_frozen_connector_write_distributed_plan(
    source: crate::planning::query_execution::FrozenConnectorScanPlan,
    sink: DmlWritePlanInput,
    settings: &crate::compiler::SessionOptimizerSettings,
) -> Result<crate::plan_read::DistributedPlan, String> {
    crate::planner::pipeline::build_sql_write_distributed_plan_with_settings(
        source.into_physical(),
        sink.0,
        settings,
    )
}

/// Compile an immutable SQL request into a sealed connector-write plan.
/// Application code supplies only the already-admitted request context and
/// opaque terminal sink; optimizer and physical planner artifacts do not
/// cross this boundary.
pub fn compile_connector_write_distributed_plan(
    request: crate::compiler::SqlCompileRequest<'_>,
    sink: DmlWritePlanInput,
    settings: &crate::compiler::SessionOptimizerSettings,
) -> Result<crate::plan_read::DistributedPlan, String> {
    let crate::compiler::SqlCompileOutput::Optimized(compiled) =
        crate::compiler::SqlCompiler::compile(request).map_err(|error| error.to_string())?
    else {
        return Err("connector write intent did not produce optimized SQL facts".to_string());
    };
    let physical = crate::planner::optimizer_bridge::to_physical_plan(&compiled.optimized_tree)?;
    crate::planner::pipeline::build_sql_write_distributed_plan_with_settings(
        physical, sink.0, settings,
    )
}

/// Compile one immutable query request directly to its sealed distributed
/// plan. This is the read-side counterpart to the DML write entrypoints and
/// keeps optimized/scalar graphs inside SQL.
pub fn compile_query_distributed_plan(
    request: crate::compiler::SqlCompileRequest<'_>,
) -> Result<crate::plan_read::DistributedPlan, String> {
    crate::compiler::SqlCompiler::compile(request)
        .map_err(|error| error.to_string())?
        .into_distributed_plan()
        .map_err(|error| error.to_string())
}

/// SQL-owned, immutable CTAS source artifact. Its optimizer graph never
/// leaves this module; application code may inspect only the source schema,
/// stable capture fingerprint, and sealed write plan derived from it.
#[derive(Clone, Debug)]
pub struct DmlCtasSourcePlan {
    optimized: crate::optimizer::OptimizedOperatorNode,
}

/// One source output field exposed to CTAS target admission.
#[derive(Clone, Debug, PartialEq)]
pub struct DmlSourceColumn {
    pub name: String,
    pub data_type: arrow::datatypes::DataType,
    pub nullable: bool,
}

impl DmlCtasSourcePlan {
    pub fn output_columns(&self) -> Vec<DmlSourceColumn> {
        self.optimized
            .output_columns
            .iter()
            .map(|column| DmlSourceColumn {
                name: column.name.clone(),
                data_type: column.data_type.clone(),
                nullable: column.nullable,
            })
            .collect()
    }

    /// Versioned digest of the frozen in-memory optimizer artifact used to
    /// bind CTAS source preparation to exactly one compilation.
    pub fn capture_fingerprint(&self) -> [u8; 32] {
        use sha2::{Digest, Sha256};

        let material = format!("{:#?}", self.optimized);
        let mut digest = Sha256::new();
        for part in [
            b"novarocks.ctas-optimized-capture.v1".as_slice(),
            material.as_bytes(),
        ] {
            digest.update((part.len() as u64).to_be_bytes());
            digest.update(part);
        }
        digest.finalize().into()
    }
}

/// Compile one CTAS source into an opaque SQL artifact. The source must be
/// optimized, but no distributed sink is selected until the application has
/// completed target admission.
pub fn compile_ctas_source(
    request: crate::compiler::SqlCompileRequest<'_>,
) -> Result<DmlCtasSourcePlan, String> {
    let crate::compiler::SqlCompileOutput::Optimized(compiled) =
        crate::compiler::SqlCompiler::compile(request).map_err(|error| error.to_string())?
    else {
        return Err("CTAS source did not produce optimized SQL facts".to_string());
    };
    Ok(DmlCtasSourcePlan {
        optimized: compiled.optimized_tree,
    })
}

/// Attach an already admitted CTAS write schema to its frozen source and
/// return the sealed distributed write plan.
pub fn build_ctas_connector_write_distributed_plan(
    source: &DmlCtasSourcePlan,
    target_schema: arrow::datatypes::SchemaRef,
    settings: &crate::compiler::SessionOptimizerSettings,
) -> Result<crate::plan_read::DistributedPlan, String> {
    let physical = crate::planner::optimizer_bridge::to_physical_plan(&source.optimized)?;
    crate::planner::pipeline::build_connector_write_distributed_plan(
        physical,
        crate::planner::distributed::write::sink::ConnectorWritePlanInput {
            target_schema,
            input: crate::planner::distributed::write::contract::ConnectorWriteInputBinding::RootOutputByOrdinal,
            root_output_exprs: None,
        },
        settings,
    )
}

/// Test-only sealed connector-write fixture for application encoder tests.
/// The distributed graph remains opaque; callers receive only its read model.
#[doc(hidden)]
pub fn native_encoder_test_fixture_plan() -> Result<crate::plan_read::DistributedPlan, String> {
    crate::planner::distributed::native_encoder_test_fixture_plan()
}

/// Provider route facts that SQL binds to a change-stream producer.  Field
/// names are resolved against the sealed producer output inside SQL, never by
/// the application against an optimizer tree.
#[derive(Clone, Debug)]
pub struct DmlChangeStreamRoute {
    pub route_id: novarocks_spi::connector::ConnectorWriteRouteId,
    pub cohort_id: novarocks_spi::connector::ConnectorWriteCohortId,
    pub accepted_effects: Vec<novarocks_spi::connector::ConnectorRowMutationEffect>,
    pub input_fields: Vec<DmlChangeStreamRouteField>,
    pub partition_input_tokens: Vec<novarocks_spi::connector::ConnectorWriteFieldToken>,
    pub sink: DmlWritePlanInput,
}

#[derive(Clone, Debug)]
pub struct DmlChangeStreamRouteField {
    pub token: novarocks_spi::connector::ConnectorWriteFieldToken,
    pub output_name: String,
}

/// SQL-only specification of a generated row-mutation producer.
#[derive(Clone, Debug)]
pub enum DmlChangeStreamKind {
    Update {
        target_columns: Vec<novarocks_catalog::schema::ColumnDef>,
        new_sequence_number: i64,
    },
    Merge {
        target_columns: Vec<novarocks_catalog::schema::ColumnDef>,
        new_sequence_number: i64,
        matched_update: bool,
        matched_delete: bool,
        not_matched_insert: bool,
    },
}

/// Optional duplicate-match assertion installed immediately before change
/// event expansion.  It is a pure SQL physical constraint; lifecycle and
/// connector fencing remain application-owned.
#[derive(Clone, Debug)]
pub struct DmlPreExpandKeyedAssert {
    pub key_column_name: String,
    pub key_label: String,
    pub message_prefix: String,
}

/// A request that consumes one immutable compile input and a fully admitted,
/// provider-signed SQL write route set.
pub struct DmlChangeStreamCompileRequest<'a> {
    pub compile_request: crate::compiler::SqlCompileRequest<'a>,
    pub kind: DmlChangeStreamKind,
    pub routes: Vec<DmlChangeStreamRoute>,
    pub pre_expand_keyed_assert: Option<DmlPreExpandKeyedAssert>,
}

/// Optimizer policy for generated mutation change streams.  This stays in the
/// SQL facade so callers do not reproduce a physical-plan safety rule.
pub fn dml_change_stream_optimizer_settings() -> crate::optimizer::options::SessionOptimizerSettings
{
    let mut settings = crate::optimizer::options::SessionOptimizerSettings::default();
    // A generated mutation plan carries before/after rows over independent
    // branches. A query runtime filter can describe only one branch, so it
    // must not suppress locator rows required by a DELETE route.
    settings.enable_global_runtime_filter = Some(false);
    settings
}

/// Return deterministic SQL-owned optimizer settings material for a CTAS
/// execution digest. The exact canonicalization remains private to SQL.
pub fn optimizer_settings_stable_digest_material(
    settings: &crate::compiler::SessionOptimizerSettings,
) -> Vec<u8> {
    settings.stable_digest_material()
}

/// Read-only routing facts needed by Core when it binds a prepared write
/// operation to fragment cohorts.  SQL keeps the mutable writer topology and
/// all physical graph state private.
#[derive(Clone, Debug)]
pub struct DmlChangeStreamWriterRoute {
    pub route_id: novarocks_spi::connector::ConnectorWriteRouteId,
    pub cohort_id: novarocks_spi::connector::ConnectorWriteCohortId,
    pub accepted_effects: Vec<novarocks_spi::connector::ConnectorRowMutationEffect>,
    pub writer_fragment_id: crate::plan_read::FragmentId,
}

/// Sealed SQL plan plus the minimal immutable routing projection Core needs
/// for normal fragment preparation and provider-session registration.
pub struct DmlChangeStreamPlan {
    distributed_plan: crate::plan_read::DistributedPlan,
    writer_routes: Vec<DmlChangeStreamWriterRoute>,
}

impl DmlChangeStreamPlan {
    pub fn distributed_plan(&self) -> &crate::plan_read::DistributedPlan {
        &self.distributed_plan
    }

    pub fn into_parts(
        self,
    ) -> (
        crate::plan_read::DistributedPlan,
        Vec<DmlChangeStreamWriterRoute>,
    ) {
        (self.distributed_plan, self.writer_routes)
    }
}

/// Compile a generated UPDATE/MERGE change-stream query directly into a
/// sealed distributed write plan.  No optimized tree, scalar arena, physical
/// graph, or draft distributed plan escapes SQL.
pub fn compile_dml_change_stream(
    request: DmlChangeStreamCompileRequest<'_>,
) -> Result<DmlChangeStreamPlan, String> {
    let crate::compiler::SqlCompileOutput::Optimized(compiled) =
        crate::compiler::SqlCompiler::compile(request.compile_request)
            .map_err(|error| error.to_string())?
    else {
        return Err("change-stream intent did not produce an optimized SQL plan".to_string());
    };

    let producer = match request.kind {
        DmlChangeStreamKind::Update {
            target_columns,
            new_sequence_number,
        } => build_update_change_event_expand(
            compiled.optimized_tree,
            &target_columns,
            new_sequence_number,
        )?,
        DmlChangeStreamKind::Merge {
            target_columns,
            new_sequence_number,
            matched_update,
            matched_delete,
            not_matched_insert,
        } => build_merge_change_event_expand(
            compiled.optimized_tree,
            &target_columns,
            new_sequence_number,
            matched_update,
            matched_delete,
            not_matched_insert,
        )?,
    };
    seal_change_stream_producer(producer, request.routes, request.pre_expand_keyed_assert)
}

/// Seal an SQL-owned generated change-stream producer after a specialized
/// compiler terminal has applied its immutable transformation.  This stays
/// crate-private: public callers submit only value-only terminal contexts and
/// receive [`DmlChangeStreamPlan`], never an optimizer tree or draft DAG.
pub(crate) fn seal_change_stream_producer(
    producer: crate::optimizer::OptimizedOperatorNode,
    routes: Vec<DmlChangeStreamRoute>,
    pre_expand_keyed_assert: Option<DmlPreExpandKeyedAssert>,
) -> Result<DmlChangeStreamPlan, String> {
    seal_change_stream_producer_with_effect_column(
        producer,
        routes,
        crate::common::ROW_MUTATION_EFFECT_COLUMN,
        pre_expand_keyed_assert,
    )
}

/// Seal a specialized SQL-owned change-stream producer that uses a
/// terminal-specific effect output.  The effect name never crosses the public
/// boundary; the returned plan remains opaque to application code.
pub(crate) fn seal_change_stream_producer_with_effect_column(
    producer: crate::optimizer::OptimizedOperatorNode,
    routes: Vec<DmlChangeStreamRoute>,
    effect_output_name: &str,
    pre_expand_keyed_assert: Option<DmlPreExpandKeyedAssert>,
) -> Result<DmlChangeStreamPlan, String> {
    let dag = bind_route_layout(&producer.output_columns, routes, effect_output_name)?;
    let keyed_assert = pre_expand_keyed_assert.map(|assertion| {
        crate::planner::physical::PreExpandKeyedAssertSpec {
            key_column_name: assertion.key_column_name,
            key_label: assertion.key_label,
            message_prefix: assertion.message_prefix,
        }
    });
    let physical = crate::planner::optimizer_bridge::to_physical_plan(&producer)?;
    let settings = dml_change_stream_optimizer_settings();
    let planned = crate::planner::pipeline::build_sql_change_stream_distributed_plan_with_settings(
        physical,
        dag,
        keyed_assert,
        &settings,
    )?;
    let writer_routes = planned
        .topology
        .writer_routes
        .iter()
        .map(|route| DmlChangeStreamWriterRoute {
            route_id: route.route_id,
            cohort_id: route.cohort_id,
            accepted_effects: route.accepted_effects.clone(),
            writer_fragment_id: route.writer_fragment_id,
        })
        .collect();
    Ok(DmlChangeStreamPlan {
        distributed_plan: planned.distributed_plan,
        writer_routes,
    })
}

fn bind_route_layout(
    output_columns: &[crate::analysis::OutputColumn],
    routes: Vec<DmlChangeStreamRoute>,
    effect_output_name: &str,
) -> Result<crate::planner::distributed::write::change_stream::ChangeStreamWriteDagSpec, String> {
    use crate::planner::distributed::write::change_stream::{
        ChangeStreamWriteLayoutRequest, ChangeStreamWriteLayoutRoute,
        bind_change_stream_write_layout,
    };

    let effect_output_ordinal = output_columns
        .iter()
        .position(|column| column.name == effect_output_name)
        .ok_or_else(|| "row-mutation producer has no logical effect output".to_string())?;
    let routes = routes
        .into_iter()
        .map(|route| {
            let input_ordinals = route
                .input_fields
                .into_iter()
                .map(|field| {
                    output_columns
                        .iter()
                        .position(|column| column.name.eq_ignore_ascii_case(&field.output_name))
                        .ok_or_else(|| {
                            format!(
                                "row-mutation producer has no output for Provider route field `{}`",
                                field.output_name
                            )
                        })
                        .and_then(|ordinal| {
                            u32::try_from(ordinal).map_err(|_| {
                                "row-mutation producer output ordinal exceeds u32".to_string()
                            })
                        })
                        .map(|ordinal| {
                            novarocks_spi::connector::ConnectorMutationRouteInput::new(
                                field.token,
                                ordinal,
                            )
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(ChangeStreamWriteLayoutRoute {
                route_id: route.route_id,
                cohort_id: route.cohort_id,
                accepted_effects: route.accepted_effects,
                input_ordinals,
                partition_input_tokens: route.partition_input_tokens,
                sink: route.sink.0,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    bind_change_stream_write_layout(ChangeStreamWriteLayoutRequest {
        producer_output_columns: output_columns,
        effect_output_ordinal,
        routes,
    })
}

fn build_update_change_event_expand(
    optimized_tree: crate::optimizer::OptimizedOperatorNode,
    target_columns: &[novarocks_catalog::schema::ColumnDef],
    new_sequence_number: i64,
) -> Result<crate::optimizer::OptimizedOperatorNode, String> {
    let mut arena = clone_scalar_arena(&optimized_tree, "MOR UPDATE")?;
    let child_outputs = optimized_tree.output_columns.clone();
    let row_id = output_column_by_name(&child_outputs, "__nr_row_id", "UPDATE row id")?;
    let distributed = distribute_producer(optimized_tree, row_id.column_id);
    let (file, pos, targets, row_id, last_sequence, effect) =
        allocate_change_outputs(&distributed, target_columns);
    let mut assignments = vec![
        output_expr(
            &mut arena,
            &child_outputs,
            "__nr_file",
            "UPDATE old file",
            file.column_id,
        )?,
        output_expr(
            &mut arena,
            &child_outputs,
            "__nr_pos",
            "UPDATE old row position",
            pos.column_id,
        )?,
    ];
    for (name, output) in &targets {
        let new_name = format!("__nr_new_{name}");
        let expr = maybe_output_column_by_name(&child_outputs, &new_name)?
            .map(|column| intern_column(&mut arena, &column))
            .transpose()?
            .unwrap_or(child_expr(
                &mut arena,
                &child_outputs,
                name,
                "UPDATE unchanged target column",
            )?);
        assignments.push(crate::optimizer::operator::ChangeEventOutputExpr {
            output_column_id: output.column_id,
            expr: Some(expr),
        });
    }
    assignments.push(output_expr(
        &mut arena,
        &child_outputs,
        "__nr_row_id",
        "UPDATE old row id",
        row_id.column_id,
    )?);
    let sequence = arena.intern(
        crate::optimizer::scalar::ScalarNode::Literal(crate::optimizer::scalar::HashableLiteral(
            crate::analysis::LiteralValue::Int(new_sequence_number),
        )),
        arrow::datatypes::DataType::Int64,
        false,
    );
    assignments.push(crate::optimizer::operator::ChangeEventOutputExpr {
        output_column_id: last_sequence.column_id,
        expr: Some(sequence),
    });
    build_change_expand(
        distributed,
        arena,
        change_output_columns(&file, &pos, &targets, &row_id, &last_sequence, &effect),
        effect.column_id,
        vec![crate::optimizer::operator::ChangeEventSpec {
            predicate: None,
            effect: novarocks_spi::connector::ConnectorRowMutationEffect::Replace,
            assignments,
        }],
    )
}

#[allow(clippy::too_many_arguments)]
fn build_merge_change_event_expand(
    optimized_tree: crate::optimizer::OptimizedOperatorNode,
    target_columns: &[novarocks_catalog::schema::ColumnDef],
    new_sequence_number: i64,
    matched_update: bool,
    matched_delete: bool,
    not_matched_insert: bool,
) -> Result<crate::optimizer::OptimizedOperatorNode, String> {
    let mut arena = clone_scalar_arena(&optimized_tree, "MOR MERGE")?;
    let child_outputs = optimized_tree.output_columns.clone();
    let assert_key =
        output_column_by_name(&child_outputs, "__nr_merge_assert_key", "MERGE assert key")?;
    let distributed = distribute_producer(optimized_tree, assert_key.column_id);
    let (file, pos, targets, row_id, last_sequence, effect) =
        allocate_change_outputs(&distributed, target_columns);
    let mut delete_assignments = vec![
        output_expr(
            &mut arena,
            &child_outputs,
            "__nr_file",
            "MERGE old file",
            file.column_id,
        )?,
        output_expr(
            &mut arena,
            &child_outputs,
            "__nr_pos",
            "MERGE old row position",
            pos.column_id,
        )?,
    ];
    let mut reuse_assignments = vec![
        output_expr(
            &mut arena,
            &child_outputs,
            "__nr_file",
            "MERGE old file",
            file.column_id,
        )?,
        output_expr(
            &mut arena,
            &child_outputs,
            "__nr_pos",
            "MERGE old row position",
            pos.column_id,
        )?,
    ];
    let mut fresh_assignments = Vec::with_capacity(targets.len());
    for (name, output) in &targets {
        delete_assignments.push(output_expr(
            &mut arena,
            &child_outputs,
            name,
            "MERGE old target column",
            output.column_id,
        )?);
        let new_name = format!("__nr_new_{name}");
        let reuse = maybe_output_column_by_name(&child_outputs, &new_name)?
            .map(|column| intern_column(&mut arena, &column))
            .transpose()?
            .unwrap_or(child_expr(
                &mut arena,
                &child_outputs,
                name,
                "MERGE unchanged target column",
            )?);
        reuse_assignments.push(crate::optimizer::operator::ChangeEventOutputExpr {
            output_column_id: output.column_id,
            expr: Some(reuse),
        });
        let insert_name = format!("__nr_ins_{name}");
        if let Some(column) = maybe_output_column_by_name(&child_outputs, &insert_name)? {
            fresh_assignments.push(crate::optimizer::operator::ChangeEventOutputExpr {
                output_column_id: output.column_id,
                expr: Some(intern_column(&mut arena, &column)?),
            });
        }
    }
    reuse_assignments.push(output_expr(
        &mut arena,
        &child_outputs,
        "__nr_row_id",
        "MERGE old row id",
        row_id.column_id,
    )?);
    let sequence = arena.intern(
        crate::optimizer::scalar::ScalarNode::Literal(crate::optimizer::scalar::HashableLiteral(
            crate::analysis::LiteralValue::Int(new_sequence_number),
        )),
        arrow::datatypes::DataType::Int64,
        false,
    );
    reuse_assignments.push(crate::optimizer::operator::ChangeEventOutputExpr {
        output_column_id: last_sequence.column_id,
        expr: Some(sequence),
    });
    let mut events = Vec::new();
    if matched_update {
        events.push(crate::optimizer::operator::ChangeEventSpec {
            predicate: Some(merge_action_predicate(&mut arena, &child_outputs, 1)?),
            effect: novarocks_spi::connector::ConnectorRowMutationEffect::Replace,
            assignments: reuse_assignments,
        });
    }
    if matched_delete {
        events.push(crate::optimizer::operator::ChangeEventSpec {
            predicate: Some(merge_action_predicate(&mut arena, &child_outputs, 2)?),
            effect: novarocks_spi::connector::ConnectorRowMutationEffect::Delete,
            assignments: delete_assignments,
        });
    }
    if not_matched_insert {
        events.push(crate::optimizer::operator::ChangeEventSpec {
            predicate: Some(merge_action_predicate(&mut arena, &child_outputs, 3)?),
            effect: novarocks_spi::connector::ConnectorRowMutationEffect::Insert,
            assignments: fresh_assignments,
        });
    }
    if events.is_empty() {
        return Err("MOR MERGE change-stream expand requires at least one event".to_string());
    }
    build_change_expand(
        distributed,
        arena,
        change_output_columns(&file, &pos, &targets, &row_id, &last_sequence, &effect),
        effect.column_id,
        events,
    )
}

fn clone_scalar_arena(
    optimized_tree: &crate::optimizer::OptimizedOperatorNode,
    operation: &str,
) -> Result<crate::optimizer::scalar::ScalarArena, String> {
    optimized_tree
        .execution_props
        .scalar_arena
        .as_deref()
        .cloned()
        .ok_or_else(|| format!("{operation} physical plan is missing scalar arena"))
}

fn distribute_producer(
    optimized_tree: crate::optimizer::OptimizedOperatorNode,
    key: crate::column_id::ColumnId,
) -> crate::optimizer::OptimizedOperatorNode {
    let stats = optimized_tree.stats.clone();
    let output_columns = optimized_tree.output_columns.clone();
    crate::optimizer::OptimizedOperatorNode {
        op: crate::optimizer::operator::Operator::PhysicalDistribution(
            crate::optimizer::operator::PhysicalDistributionOp {
                spec: crate::optimizer::property::DistributionSpec::shuffle_agg([key]),
            },
        ),
        children: vec![optimized_tree],
        stats,
        explain_stats: crate::optimizer::optimized_tree::OptimizerExplainStats::default(),
        output_columns,
        execution_props: crate::optimizer::optimized_tree::PlanExecutionProps::default(),
    }
}

type ChangeOutputs = (
    crate::analysis::OutputColumn,
    crate::analysis::OutputColumn,
    Vec<(String, crate::analysis::OutputColumn)>,
    crate::analysis::OutputColumn,
    crate::analysis::OutputColumn,
    crate::analysis::OutputColumn,
);

fn allocate_change_outputs(
    node: &crate::optimizer::OptimizedOperatorNode,
    target_columns: &[novarocks_catalog::schema::ColumnDef],
) -> ChangeOutputs {
    let mut next = max_physical_column_id(node) + 1;
    let mut allocate =
        |name: &str, data_type: arrow::datatypes::DataType, nullable: bool, is_internal: bool| {
            let output = crate::analysis::OutputColumn {
                column_id: crate::column_id::ColumnId(next),
                name: name.to_string(),
                data_type,
                nullable,
                is_internal,
            };
            next += 1;
            output
        };
    let file = allocate(
        ICEBERG_FILE_PATH_COLUMN,
        arrow::datatypes::DataType::Utf8,
        true,
        true,
    );
    let pos = allocate(
        ICEBERG_ROW_POSITION_COLUMN,
        arrow::datatypes::DataType::Int64,
        true,
        true,
    );
    let targets = target_columns
        .iter()
        .map(|column| {
            (
                column.name.clone(),
                allocate(
                    &column.name,
                    column.data_type.clone(),
                    column.nullable,
                    false,
                ),
            )
        })
        .collect();
    let row_id = allocate(
        ICEBERG_ROW_ID_COLUMN,
        arrow::datatypes::DataType::Int64,
        true,
        true,
    );
    let last_sequence = allocate(
        ICEBERG_LAST_UPDATED_SEQUENCE_COLUMN,
        arrow::datatypes::DataType::Int64,
        true,
        true,
    );
    let effect = allocate(
        crate::common::change_stream::ROW_MUTATION_EFFECT_COLUMN,
        arrow::datatypes::DataType::Int8,
        false,
        true,
    );
    (file, pos, targets, row_id, last_sequence, effect)
}

fn change_output_columns(
    file: &crate::analysis::OutputColumn,
    pos: &crate::analysis::OutputColumn,
    targets: &[(String, crate::analysis::OutputColumn)],
    row_id: &crate::analysis::OutputColumn,
    last_sequence: &crate::analysis::OutputColumn,
    effect: &crate::analysis::OutputColumn,
) -> Vec<crate::analysis::OutputColumn> {
    let mut columns = Vec::with_capacity(targets.len() + 6);
    columns.push(file.clone());
    columns.push(pos.clone());
    columns.extend(targets.iter().map(|(_, column)| column.clone()));
    columns.push(row_id.clone());
    columns.push(last_sequence.clone());
    columns.push(effect.clone());
    columns
}

fn output_expr(
    arena: &mut crate::optimizer::scalar::ScalarArena,
    columns: &[crate::analysis::OutputColumn],
    name: &str,
    label: &str,
    output_column_id: crate::column_id::ColumnId,
) -> Result<crate::optimizer::operator::ChangeEventOutputExpr, String> {
    Ok(crate::optimizer::operator::ChangeEventOutputExpr {
        output_column_id,
        expr: Some(child_expr(arena, columns, name, label)?),
    })
}

fn intern_column(
    arena: &mut crate::optimizer::scalar::ScalarArena,
    column: &crate::analysis::OutputColumn,
) -> Result<crate::optimizer::scalar::ScalarId, String> {
    Ok(arena.intern(
        crate::optimizer::scalar::ScalarNode::ColumnRef(column.column_id),
        column.data_type.clone(),
        column.nullable,
    ))
}

fn child_expr(
    arena: &mut crate::optimizer::scalar::ScalarArena,
    columns: &[crate::analysis::OutputColumn],
    name: &str,
    label: &str,
) -> Result<crate::optimizer::scalar::ScalarId, String> {
    let column = output_column_by_name(columns, name, label)?;
    intern_column(arena, &column)
}

fn merge_action_predicate(
    arena: &mut crate::optimizer::scalar::ScalarArena,
    columns: &[crate::analysis::OutputColumn],
    action: i32,
) -> Result<crate::optimizer::scalar::ScalarId, String> {
    let action_expr = child_expr(arena, columns, "__nr_merge_action", "MERGE action")?;
    let literal = arena.intern(
        crate::optimizer::scalar::ScalarNode::Literal(crate::optimizer::scalar::HashableLiteral(
            crate::analysis::LiteralValue::Int(i64::from(action)),
        )),
        arrow::datatypes::DataType::Int64,
        false,
    );
    Ok(arena.intern(
        crate::optimizer::scalar::ScalarNode::BinaryOp {
            op: crate::common::BinOp::Eq,
            left: action_expr,
            right: literal,
        },
        arrow::datatypes::DataType::Boolean,
        false,
    ))
}

fn build_change_expand(
    child: crate::optimizer::OptimizedOperatorNode,
    arena: crate::optimizer::scalar::ScalarArena,
    output_columns: Vec<crate::analysis::OutputColumn>,
    effect_column_id: crate::column_id::ColumnId,
    events: Vec<crate::optimizer::operator::ChangeEventSpec>,
) -> Result<crate::optimizer::OptimizedOperatorNode, String> {
    let stats = child.stats.clone();
    let mut root = crate::optimizer::OptimizedOperatorNode {
        op: crate::optimizer::operator::Operator::PhysicalChangeEventExpand(
            crate::optimizer::operator::ChangeEventExpandOp {
                events,
                output_columns: output_columns.clone(),
                effect_column_id,
            },
        ),
        children: vec![child],
        stats,
        explain_stats: crate::optimizer::optimized_tree::OptimizerExplainStats::default(),
        output_columns,
        execution_props: crate::optimizer::optimized_tree::PlanExecutionProps::default(),
    };
    crate::optimizer::optimized_tree::attach_scalar_arena(&mut root, std::sync::Arc::new(arena));
    Ok(root)
}

fn output_column_by_name(
    columns: &[crate::analysis::OutputColumn],
    name: &str,
    label: &str,
) -> Result<crate::analysis::OutputColumn, String> {
    maybe_output_column_by_name(columns, name)?.ok_or_else(|| {
        format!("MOR UPDATE change-stream {label} column `{name}` not found in producer output")
    })
}

fn maybe_output_column_by_name(
    columns: &[crate::analysis::OutputColumn],
    name: &str,
) -> Result<Option<crate::analysis::OutputColumn>, String> {
    let mut matches = columns
        .iter()
        .filter(|column| column.name.eq_ignore_ascii_case(name));
    let Some(column) = matches.next() else {
        return Ok(None);
    };
    if matches.next().is_some() {
        return Err(format!(
            "MOR UPDATE change-stream producer column `{name}` is ambiguous"
        ));
    }
    Ok(Some(column.clone()))
}

fn max_physical_column_id(node: &crate::optimizer::OptimizedOperatorNode) -> u32 {
    node.output_columns
        .iter()
        .map(|column| column.column_id.0)
        .chain(node.children.iter().map(max_physical_column_id))
        .max()
        .unwrap_or(0)
}

/// Immutable synthetic scan facts used by Core's provider-neutral statistics
/// collector.  The binding was admitted by Core from an exact provider lease;
/// SQL only turns it into a sealed statistics distributed plan.
#[derive(Clone, Debug)]
pub struct StatisticsConnectorScan {
    pub binding: crate::binding::SqlTableBindingId,
    pub columns: Vec<novarocks_catalog::schema::ColumnDef>,
}

/// Build the SQL-owned physical and distributed statistics program from a
/// pinned synthetic connector scan.  Core retains the encoder, preparation,
/// provider resolver, and result finalization.
pub fn build_statistics_connector_plan(
    scan: StatisticsConnectorScan,
    metrics: novarocks_spi::connector::StatisticsMetricRequest,
    settings: &crate::compiler::SessionOptimizerSettings,
) -> Result<crate::plan_read::DistributedPlan, String> {
    let mut factory = crate::column_id::ColumnRefFactory::new();
    let scan_columns = scan
        .columns
        .iter()
        .map(|column| {
            let column_id = factory.create(
                None,
                column.name.clone(),
                column.data_type.clone(),
                column.nullable,
            );
            crate::analysis::OutputColumn {
                column_id,
                name: column.name.clone(),
                data_type: column.data_type.clone(),
                nullable: column.nullable,
                is_internal: false,
            }
        })
        .collect::<Vec<_>>();
    let physical = crate::planner::physical::PhysicalPlanNode {
        kind: crate::planner::physical::PhysicalPlanKind::Scan(
            crate::planner::payload::PlanScanNode {
                database: "__statistics".to_string(),
                table: crate::planner::table::TableDef {
                    name: "__connector_pinned_statistics".to_string(),
                    columns: scan.columns,
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source: crate::planner::table::ScanSource::Sql(
                        crate::planner::table::SqlScanSource::new(
                            scan.binding,
                            crate::planner::table::SqlTableIdentity {
                                catalog: "__statistics".to_string(),
                                namespace: "__statistics".to_string(),
                                table: "__connector_pinned_statistics".to_string(),
                            },
                            crate::planner::table::SqlScanKind::ConnectorRead,
                        ),
                    ),
                },
                alias: None,
                columns: scan_columns.clone(),
                predicates: Vec::new(),
                required_columns: None,
                variant_columns: Vec::new(),
                mv_rewritten_from: None,
            },
        ),
        children: Vec::new(),
        output_columns: scan_columns,
        stats: crate::planner::physical::PhysicalPlanStats {
            output_row_count: 0.0,
            row_count_confidence: crate::planner::physical::PlannerConfidence::Fallback,
            column_statistics: std::collections::HashMap::new(),
            cost_estimate: None,
            broadcast_decision: None,
        },
        probe_runtime_filters: Vec::new(),
    };
    crate::planner::pipeline::build_statistics_distributed_plan_with_settings(
        physical, metrics, settings,
    )
}

#[cfg(test)]
mod tests {
    use super::{
        DmlStatisticsSnapshot, StatisticsCommand, dml_change_stream_optimizer_settings,
        optimizer_settings_stable_digest_material, parse_statistics_command,
        parse_truncate_command,
    };
    use crate::compiler::SessionOptimizerSettings;

    #[test]
    fn optimizer_settings_digest_material_is_stable_across_rule_order_and_duplicates() {
        let mut unordered = SessionOptimizerSettings::default();
        unordered.disabled_rules = vec![
            "RuleB".to_string(),
            "RuleA".to_string(),
            "RuleB".to_string(),
        ];
        let mut canonical = SessionOptimizerSettings::default();
        canonical.disabled_rules = vec!["RuleA".to_string(), "RuleB".to_string()];

        assert_eq!(
            optimizer_settings_stable_digest_material(&unordered),
            optimizer_settings_stable_digest_material(&canonical)
        );
    }

    #[test]
    fn empty_statistics_snapshot_is_the_default_without_implying_zero_rows() {
        let _snapshot = DmlStatisticsSnapshot::default();
    }

    #[test]
    fn dml_facade_parses_truncate_without_exposing_the_custom_ast() {
        let command = parse_truncate_command("TRUNCATE TABLE ice.db.orders.branch_dev")
            .expect("parse truncate")
            .expect("truncate command");
        assert_eq!(command.target_parts, ["ice", "db", "orders"]);
        assert_eq!(command.target_ref, "dev");
    }

    #[test]
    fn dml_facade_projects_statistics_command_to_dto() {
        let command = parse_statistics_command("ANALYZE TABLE ice.db.orders (id, amount)")
            .expect("parse analyze")
            .expect("statistics command");
        assert_eq!(
            command,
            StatisticsCommand::AnalyzeTable {
                target_parts: vec!["ice".to_string(), "db".to_string(), "orders".to_string()],
                columns: vec!["id".to_string(), "amount".to_string()],
            }
        );
    }

    #[test]
    fn change_stream_facade_disables_global_runtime_filters() {
        assert_eq!(
            dml_change_stream_optimizer_settings().enable_global_runtime_filter,
            Some(false)
        );
    }
}
