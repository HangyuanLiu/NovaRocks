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

mod sql_shape;
use crate::binding::SqlTableBindingId;
use crate::column_id::ColumnRefFactory;
use crate::compiler::RootDistributionRequirement;
use crate::mv_refresh::aggregate_shape::{
    SQL_MV_AGG_RETRACTION_COUNT_STATE_COLUMN, SQL_MV_ROW_ID_COLUMN, SqlAggregateCalls,
    rewrite_select_sql_for_state, state_column_name,
};
use crate::mv_refresh::{AggregateFunctionKind, VisibleAggregateOutput};
use crate::planner::logical::LogicalPlanNode;
use crate::planner::vocabulary::BRANCH_ID_COLUMN_NAME;
use arrow::datatypes::{DataType, Schema, SchemaRef};
use std::collections::BTreeSet;

pub use self::sql_shape::SqlMvSnapshotPin;
use self::sql_shape::{
    branch_union_queries, pin_state_sql, prepare_projection_full_read_sql,
    prepare_union_projection_full_read_sql,
};

/// SQL-only input for one first-refresh planning step.
///
/// The application has already frozen the target binding before constructing
/// this value.  It deliberately carries neither a connector table handle nor
/// a write operation/cohort: those are lifecycle facts and are attached only
/// after the application admits an exact write lease.
pub(crate) struct SqlMvFirstRefreshPlannerInput {
    pub(crate) shape: MvFirstRefreshShape,
    pub(crate) target_contract: MvFirstRefreshTargetContract,
    pub(crate) target_binding: SqlTableBindingId,
    pub(crate) root_distribution: RootDistributionRequirement,
    pub(crate) artifact: SqlMvFirstRefreshArtifactInput,
}

/// A first-refresh artifact before it becomes an immutable plan.  The logical
/// variant contains only SQL planner values; it intentionally has no refresh
/// context or provider authority.
pub(crate) enum SqlMvFirstRefreshArtifactInput {
    Sql(MvFirstRefreshPhysicalSql),
    Logical {
        plan: LogicalPlanNode,
        factory: ColumnRefFactory,
        root_hash_column: String,
    },
}

/// Immutable SQL first-refresh artifact handed to the application lifecycle.
///
/// This is the complete SQL boundary: a logical/physical plan, shape, target
/// contract, root distribution requirement and query-local binding token.  In
/// particular, it contains no operation/cohort ID, connector handle/request
/// context, prepared write, catalog object or commit lifecycle value.
pub(crate) struct SqlMvFirstRefreshPlan {
    shape: MvFirstRefreshShape,
    target_contract: MvFirstRefreshTargetContract,
    target_binding: SqlTableBindingId,
    root_distribution: RootDistributionRequirement,
    artifact: SqlMvFirstRefreshPlanArtifact,
}

pub(crate) enum SqlMvFirstRefreshPlanArtifact {
    Sql(MvFirstRefreshPhysicalSql),
    Logical {
        plan: LogicalPlanNode,
        factory: ColumnRefFactory,
    },
}

/// Canonical, side-effect-free SQL planner for an MV first refresh.
pub(crate) struct SqlMvFirstRefreshPlanner;

impl SqlMvFirstRefreshPlanner {
    pub(crate) fn plan(
        input: SqlMvFirstRefreshPlannerInput,
    ) -> Result<SqlMvFirstRefreshPlan, String> {
        let (artifact, root_hash_column) = match input.artifact {
            SqlMvFirstRefreshArtifactInput::Sql(sql) => {
                let root_hash_column = sql.root_hash_column().to_string();
                (SqlMvFirstRefreshPlanArtifact::Sql(sql), root_hash_column)
            }
            SqlMvFirstRefreshArtifactInput::Logical {
                plan,
                factory,
                root_hash_column,
            } => {
                if root_hash_column.is_empty() {
                    return Err(
                        "MV first-refresh logical artifact has no root hash column".to_string()
                    );
                }
                (
                    SqlMvFirstRefreshPlanArtifact::Logical { plan, factory },
                    root_hash_column,
                )
            }
        };
        validate_root_distribution(
            &input.root_distribution,
            &root_hash_column,
            input.target_contract.hidden_hash_key(),
        )?;
        Ok(SqlMvFirstRefreshPlan {
            shape: input.shape,
            target_contract: input.target_contract,
            target_binding: input.target_binding,
            root_distribution: input.root_distribution,
            artifact,
        })
    }
}

impl SqlMvFirstRefreshPlan {
    pub(crate) const fn shape(&self) -> MvFirstRefreshShape {
        self.shape
    }

    pub(crate) fn target_contract(&self) -> &MvFirstRefreshTargetContract {
        &self.target_contract
    }

    pub(crate) const fn target_binding(&self) -> SqlTableBindingId {
        self.target_binding
    }

    pub(crate) fn root_distribution(&self) -> &RootDistributionRequirement {
        &self.root_distribution
    }

    pub(crate) fn into_artifact(self) -> SqlMvFirstRefreshPlanArtifact {
        self.artifact
    }
}

fn validate_root_distribution(
    requirement: &RootDistributionRequirement,
    root_hash_column: &str,
    target_hidden_hash_key: &str,
) -> Result<(), String> {
    if root_hash_column != target_hidden_hash_key {
        return Err(
            "MV first-refresh root distribution does not match the target hidden hash key"
                .to_string(),
        );
    }
    match requirement {
        RootDistributionRequirement::ShuffleOutputName(name) if name == root_hash_column => Ok(()),
        RootDistributionRequirement::ShuffleOutputName(_) => Err(
            "MV first-refresh root distribution output name does not match the SQL artifact"
                .to_string(),
        ),
        RootDistributionRequirement::ShuffleOutputOrdinal(_) => {
            Err("MV first-refresh requires a named root distribution key".to_string())
        }
        RootDistributionRequirement::Any => {
            Err("MV first-refresh requires an explicit root distribution key".to_string())
        }
    }
}

/// Immutable SQL artifact for a distributed first-refresh write.
///
/// `root_hash_column` is the target contract's hidden apply key. The native
/// planner must derive its actual writer fanout from the admitted topology.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MvFirstRefreshPhysicalSql {
    sql: String,
    root_hash_column: String,
}

/// Move-only SQL source artifact for a first-refresh write.
///
/// Applications can retain and hand this value back to SQL, but cannot read
/// its SQL text or obtain a logical/physical planner graph from it.
pub struct SqlMvFirstRefreshArtifact(MvFirstRefreshPhysicalSql);

impl SqlMvFirstRefreshArtifact {
    fn from_physical(physical: MvFirstRefreshPhysicalSql) -> Self {
        Self(physical)
    }

    pub fn root_hash_column(&self) -> &str {
        self.0.root_hash_column()
    }

    fn sql(&self) -> &str {
        self.0.sql()
    }
}

/// Immutable application facts required to consume one opaque first-refresh
/// source.  Connector handles, write leases, lifecycle state and wire payloads
/// are deliberately absent.
pub struct SqlMvFirstRefreshCompileContext<'a> {
    pub current_catalog: Option<String>,
    pub current_database: String,
    pub optimizer_settings: crate::compiler::SessionOptimizerSettings,
    pub environment: crate::compiler::SqlPlanningEnvironment,
    pub catalog: &'a dyn crate::compiler::SqlCatalogSnapshot,
    pub statistics: &'a crate::planning::dml::DmlStatisticsSnapshot,
    pub functions: &'a dyn crate::compiler::SqlFunctionCatalog,
    pub control: crate::compiler::SqlCompileControl,
    pub sink: crate::planning::dml::DmlWritePlanInput,
}

/// Compile an opaque first-refresh source directly into a sealed connector
/// write plan.  No raw SQL, logical plan, optimizer tree, or physical graph
/// can cross this terminal boundary.
pub fn compile_mv_first_refresh_connector_write(
    artifact: SqlMvFirstRefreshArtifact,
    context: SqlMvFirstRefreshCompileContext<'_>,
) -> Result<crate::plan_read::DistributedPlan, String> {
    let root_distribution = crate::compiler::RootDistributionRequirement::ShuffleOutputName(
        artifact.root_hash_column().to_string(),
    );
    let settings = context.optimizer_settings.clone();
    let request = crate::compiler::SqlCompileRequest::new(
        crate::compiler::SqlStatementInput::Sql(artifact.sql().to_string()),
        crate::compiler::SqlCompileIntent::IcebergWrite { root_distribution },
        crate::compiler::SqlSessionContext {
            current_catalog: context.current_catalog,
            current_database: context.current_database,
            optimizer_settings: context.optimizer_settings,
        },
        context.environment,
        context.catalog,
        context.statistics,
        context.functions,
        None,
        context.control,
    );
    crate::planning::dml::compile_connector_write_distributed_plan(request, context.sink, &settings)
}

/// Immutable inputs for the join-MV first-refresh terminal.  The snapshot is
/// already sealed by the compiler facade; the query is syntax only, not a
/// logical or physical planner graph.
pub struct SqlMvJoinFirstRefreshCompileContext<'a> {
    pub canonical_query: Box<sqlparser::ast::Query>,
    pub rewrite_snapshot: crate::compiler::SqlImvRewriteSnapshotHandle,
    pub expected_root_hash_column: String,
    pub current_catalog: Option<String>,
    pub current_database: String,
    pub optimizer_settings: crate::compiler::SessionOptimizerSettings,
    pub environment: crate::compiler::SqlPlanningEnvironment,
    pub catalog: &'a dyn crate::compiler::SqlCatalogSnapshot,
    pub statistics: &'a crate::planning::dml::DmlStatisticsSnapshot,
    pub functions: &'a dyn crate::compiler::SqlFunctionCatalog,
    pub control: crate::compiler::SqlCompileControl,
    pub sink: crate::planning::dml::DmlWritePlanInput,
}

/// Compile the canonical join first-refresh query all the way to a sealed
/// connector-write plan.  SQL alone creates the hidden join key, validates
/// frozen lineage and physicalizes the resulting append projection.
pub fn compile_join_first_refresh_connector_write(
    context: SqlMvJoinFirstRefreshCompileContext<'_>,
) -> Result<crate::plan_read::DistributedPlan, String> {
    let snapshot = context.rewrite_snapshot.snapshot();
    let root_hash_column = snapshot
        .schema_contract
        .target
        .hidden_apply_key
        .column_name
        .clone();
    if !root_hash_column.eq_ignore_ascii_case(&context.expected_root_hash_column) {
        return Err(
            "join first-refresh root hash column does not match the sealed target contract"
                .to_string(),
        );
    }
    let settings = context.optimizer_settings.clone();
    let mut query = *context.canonical_query;
    crate::planning::mv::strip_catalog_from_three_part_names(&mut query);
    let request = plain_join_first_refresh_logical_request(
        query,
        context.current_catalog.clone(),
        context.current_database.clone(),
        context.optimizer_settings.clone(),
        context.environment,
        context.catalog,
        context.statistics,
        context.functions,
        context.control.clone(),
    );
    let crate::compiler::SqlCompileOutput::Logical(logical) =
        crate::compiler::SqlCompiler::compile(request).map_err(|error| error.to_string())?
    else {
        return Err(
            "join first-refresh logical intent did not produce logical SQL facts".to_string(),
        );
    };
    let (plan, factory) = build_join_first_refresh_append_logical_plan(
        crate::planner::imv_rewrite::entrypoint::normalize_imv_rewrite_root_project(
            logical.logical_plan,
        ),
        logical.factory,
        snapshot,
    )?;
    let logical_request = crate::compiler::SqlCompileRequest::new_logical(
        plan,
        factory,
        crate::compiler::SqlCompileIntent::IcebergWrite {
            root_distribution: crate::compiler::RootDistributionRequirement::ShuffleOutputName(
                root_hash_column,
            ),
        },
        crate::compiler::SqlSessionContext {
            current_catalog: context.current_catalog,
            current_database: context.current_database,
            optimizer_settings: context.optimizer_settings,
        },
        context.environment,
        context.statistics,
        context.control,
    );
    crate::planning::dml::compile_connector_write_distributed_plan(
        logical_request,
        context.sink,
        &settings,
    )
}

/// Deliberately builds a plain `LogicalOnly` request.  The sealed rewrite
/// snapshot is consumed only after canonical planning to construct the join
/// append descriptor; injecting it here would silently change the prior Core
/// canonical-query semantics.
fn plain_join_first_refresh_logical_request<'a>(
    query: sqlparser::ast::Query,
    current_catalog: Option<String>,
    current_database: String,
    optimizer_settings: crate::compiler::SessionOptimizerSettings,
    environment: crate::compiler::SqlPlanningEnvironment,
    catalog: &'a dyn crate::compiler::SqlCatalogSnapshot,
    statistics: &'a crate::planning::dml::DmlStatisticsSnapshot,
    functions: &'a dyn crate::compiler::SqlFunctionCatalog,
    control: crate::compiler::SqlCompileControl,
) -> crate::compiler::SqlCompileRequest<'a> {
    crate::compiler::SqlCompileRequest::new(
        crate::compiler::SqlStatementInput::ParsedQuery(Box::new(query)),
        crate::compiler::SqlCompileIntent::LogicalOnly,
        crate::compiler::SqlSessionContext {
            current_catalog,
            current_database,
            optimizer_settings,
        },
        environment,
        catalog,
        statistics,
        functions,
        None,
        control,
    )
}

fn build_join_first_refresh_append_logical_plan(
    plan: crate::planner::logical::LogicalPlanNode,
    mut factory: crate::column_id::ColumnRefFactory,
    snapshot: &crate::compiler::mv_rewrite::SqlImvRewriteSnapshot,
) -> Result<
    (
        crate::planner::logical::LogicalPlanNode,
        crate::column_id::ColumnRefFactory,
    ),
    String,
> {
    let (left, right) = join_base_snapshots(snapshot)?;
    let crate::planner::logical::LogicalPlanNode {
        kind, mut children, ..
    } = plan;
    let crate::planner::logical::LogicalPlanKind::Project(mut project) = kind else {
        return Err("join first-refresh requires a root Project".to_string());
    };
    if children.len() != 1 {
        return Err(format!(
            "join first-refresh root Project expected one input, got {}",
            children.len()
        ));
    }
    let input = children.remove(0);
    let payload_columns = project
        .items
        .iter()
        .map(|item| crate::analysis::OutputColumn {
            column_id: item.output_column_id,
            name: item.output_name.clone(),
            data_type: item.expr.data_type.clone(),
            nullable: item.expr.nullable,
            is_internal: false,
        })
        .collect::<Vec<_>>();
    validate_join_payload(snapshot, &payload_columns)?;
    let left_scan = find_unique_base_scan(&input, &left.table, "left")?;
    let right_scan = find_unique_base_scan(&input, &right.table, "right")?;
    let left_row_id = find_row_id_column(&left_scan, "left")?;
    let right_row_id = find_row_id_column(&right_scan, "right")?;
    let key_pairs = join_key_pairs(snapshot, &left.table, &right.table, &left_scan, &right_scan)?;
    project.items.push(project_item(&left_row_id));
    project.items.push(project_item(&right_row_id));
    let input = crate::planner::logical::LogicalPlanNode::new(
        crate::planner::logical::LogicalPlanKind::Project(project),
        vec![input],
        None,
    );
    reserve_factory_for_plan(&mut factory, &input)?;
    let join_apply_key_id = factory.create(
        None,
        "__nova_join_row_key".to_string(),
        arrow::datatypes::DataType::Utf8,
        false,
    );
    let action_id = factory.create(
        None,
        crate::common::CHANGE_OP_COLUMN.to_string(),
        arrow::datatypes::DataType::Int8,
        false,
    );
    let join_apply_key = output_column(
        join_apply_key_id,
        "__nova_join_row_key",
        arrow::datatypes::DataType::Utf8,
        false,
        true,
    );
    let action = output_column(
        action_id,
        crate::common::CHANGE_OP_COLUMN,
        arrow::datatypes::DataType::Int8,
        false,
        true,
    );
    let descriptor = build_join_descriptor(
        snapshot,
        &left.table,
        &right.table,
        payload_columns,
        left_row_id,
        right_row_id,
        action,
        join_apply_key,
        key_pairs,
    )?;
    descriptor
        .validate()
        .map_err(|error| format!("join first-refresh descriptor is invalid: {error}"))?;
    let plan =
        crate::planner::imv_rewrite::join_refresh_builder::build_join_apply_key_append_project(
            input,
            &descriptor,
            &left.table_uuid,
            &right.table_uuid,
            join_apply_key_id.0,
        )
        .map_err(|error| format!("build join first-refresh append projection: {error}"))?;
    reserve_factory_for_plan(&mut factory, &plan)?;
    Ok((plan, factory))
}

fn join_base_snapshots(
    snapshot: &crate::compiler::mv_rewrite::SqlImvRewriteSnapshot,
) -> Result<
    (
        &crate::compiler::mv_rewrite::SqlImvBaseSnapshot,
        &crate::compiler::mv_rewrite::SqlImvBaseSnapshot,
    ),
    String,
> {
    let predicate = snapshot
        .schema_contract
        .join
        .as_ref()
        .and_then(|join| join.predicates.first())
        .ok_or_else(|| "join first-refresh snapshot has no join predicate facts".to_string())?;
    let left = snapshot
        .base_snapshots
        .iter()
        .find(|base| {
            base.table
                .fqn()
                .eq_ignore_ascii_case(&predicate.left.table_fqn)
        })
        .ok_or_else(|| {
            "join first-refresh left base is absent from the sealed snapshot".to_string()
        })?;
    let right = snapshot
        .base_snapshots
        .iter()
        .find(|base| {
            base.table
                .fqn()
                .eq_ignore_ascii_case(&predicate.right.table_fqn)
        })
        .ok_or_else(|| {
            "join first-refresh right base is absent from the sealed snapshot".to_string()
        })?;
    if left.table.fqn().eq_ignore_ascii_case(&right.table.fqn()) {
        return Err("join first-refresh requires distinct left and right bases".to_string());
    }
    Ok((left, right))
}

fn validate_join_payload(
    snapshot: &crate::compiler::mv_rewrite::SqlImvRewriteSnapshot,
    payload_columns: &[crate::analysis::OutputColumn],
) -> Result<(), String> {
    let expected = &snapshot.schema_contract.target.visible_columns;
    if payload_columns.len() != expected.len() {
        return Err(
            "join first-refresh payload count does not match the sealed target contract"
                .to_string(),
        );
    }
    for (actual, expected) in payload_columns.iter().zip(expected) {
        if !actual.name.eq_ignore_ascii_case(&expected.output_name) {
            return Err(format!(
                "join first-refresh payload column `{}` does not match target `{}`",
                actual.name, expected.output_name
            ));
        }
    }
    Ok(())
}

#[derive(Clone)]
struct JoinBaseScan {
    columns: Vec<crate::analysis::OutputColumn>,
}

fn find_unique_base_scan(
    plan: &crate::planner::logical::LogicalPlanNode,
    base: &novarocks_catalog::identifier::TableIdentity,
    role: &str,
) -> Result<JoinBaseScan, String> {
    let mut scans = Vec::new();
    collect_base_scans(plan, base, &mut scans);
    match scans.as_slice() {
        [scan] => Ok(scan.clone()),
        [] => Err(format!(
            "join first-refresh cannot find {role} base scan {}",
            base.fqn()
        )),
        _ => Err(format!(
            "join first-refresh found multiple {role} base scans {}",
            base.fqn()
        )),
    }
}

fn collect_base_scans(
    plan: &crate::planner::logical::LogicalPlanNode,
    base: &novarocks_catalog::identifier::TableIdentity,
    scans: &mut Vec<JoinBaseScan>,
) {
    if let crate::planner::logical::LogicalPlanKind::Scan(scan) = &plan.kind
        && let crate::planner::table::ScanSource::Sql(source) = &scan.table.source
        && source.table.catalog.eq_ignore_ascii_case(&base.catalog)
        && source.table.namespace.eq_ignore_ascii_case(&base.namespace)
        && source.table.table.eq_ignore_ascii_case(&base.table)
    {
        scans.push(JoinBaseScan {
            columns: scan.columns.clone(),
        });
    }
    for child in &plan.children {
        collect_base_scans(child, base, scans);
    }
}

fn find_row_id_column(
    scan: &JoinBaseScan,
    role: &str,
) -> Result<crate::analysis::OutputColumn, String> {
    let column = find_unique_column(
        &scan.columns,
        crate::common::ICEBERG_ROW_ID_COL,
        &format!("{role} row-id"),
    )?;
    if column.data_type != arrow::datatypes::DataType::Int64 || column.nullable {
        return Err(format!(
            "join first-refresh {role} row-id has invalid shape"
        ));
    }
    Ok(output_column(
        column.column_id,
        crate::common::ICEBERG_ROW_ID_COL,
        arrow::datatypes::DataType::Int64,
        false,
        true,
    ))
}

fn join_key_pairs(
    snapshot: &crate::compiler::mv_rewrite::SqlImvRewriteSnapshot,
    left: &novarocks_catalog::identifier::TableIdentity,
    right: &novarocks_catalog::identifier::TableIdentity,
    left_scan: &JoinBaseScan,
    right_scan: &JoinBaseScan,
) -> Result<Vec<crate::planner::imv_rewrite::join_refresh_descriptor::JoinRefreshJoinKeyPair>, String>
{
    let join = snapshot
        .schema_contract
        .join
        .as_ref()
        .ok_or_else(|| "join first-refresh snapshot has no join contract".to_string())?;
    join.predicates
        .iter()
        .map(|predicate| {
            let (left_lineage, right_lineage) =
                if predicate.left.table_fqn.eq_ignore_ascii_case(&left.fqn())
                    && predicate.right.table_fqn.eq_ignore_ascii_case(&right.fqn())
                {
                    (&predicate.left, &predicate.right)
                } else if predicate.left.table_fqn.eq_ignore_ascii_case(&right.fqn())
                    && predicate.right.table_fqn.eq_ignore_ascii_case(&left.fqn())
                {
                    (&predicate.right, &predicate.left)
                } else {
                    return Err(
                        "join first-refresh predicate does not align with sealed bases".to_string(),
                    );
                };
            let left_name = base_field_name(snapshot, &left.fqn(), left_lineage.field_id)?;
            let right_name = base_field_name(snapshot, &right.fqn(), right_lineage.field_id)?;
            Ok(
                crate::planner::imv_rewrite::join_refresh_descriptor::JoinRefreshJoinKeyPair {
                    left_column: find_unique_column(
                        &left_scan.columns,
                        &left_name,
                        "left join key",
                    )?,
                    right_column: find_unique_column(
                        &right_scan.columns,
                        &right_name,
                        "right join key",
                    )?,
                },
            )
        })
        .collect()
}

fn base_field_name(
    snapshot: &crate::compiler::mv_rewrite::SqlImvRewriteSnapshot,
    table_fqn: &str,
    field_id: i32,
) -> Result<String, String> {
    snapshot
        .schema_contract
        .bases
        .iter()
        .find(|base| base.table_fqn.eq_ignore_ascii_case(table_fqn))
        .and_then(|base| base.fields.iter().find(|field| field.field_id == field_id))
        .map(|field| field.name_at_create.clone())
        .ok_or_else(|| {
            format!(
                "join first-refresh lineage references unknown base field {table_fqn}#{field_id}"
            )
        })
}

fn build_join_descriptor(
    snapshot: &crate::compiler::mv_rewrite::SqlImvRewriteSnapshot,
    left: &novarocks_catalog::identifier::TableIdentity,
    right: &novarocks_catalog::identifier::TableIdentity,
    payload_columns: Vec<crate::analysis::OutputColumn>,
    left_row_id_column: crate::analysis::OutputColumn,
    right_row_id_column: crate::analysis::OutputColumn,
    action_column: crate::analysis::OutputColumn,
    join_apply_key_column: crate::analysis::OutputColumn,
    join_key_pairs: Vec<
        crate::planner::imv_rewrite::join_refresh_descriptor::JoinRefreshJoinKeyPair,
    >,
) -> Result<crate::planner::imv_rewrite::join_refresh_descriptor::JoinRefreshDescriptor, String> {
    use crate::planner::imv_rewrite::join_refresh_descriptor as descriptor;
    let mut output_mappings = payload_columns
        .iter()
        .map(|column| descriptor::JoinRefreshOutputMapping {
            mv_output_column: column.clone(),
            source: descriptor::JoinRefreshOutputSource::Payload(column.column_id),
        })
        .collect::<Vec<_>>();
    output_mappings.push(descriptor::JoinRefreshOutputMapping {
        mv_output_column: join_apply_key_column.clone(),
        source: descriptor::JoinRefreshOutputSource::JoinApplyKey(join_apply_key_column.column_id),
    });
    output_mappings.push(descriptor::JoinRefreshOutputMapping {
        mv_output_column: action_column.clone(),
        source: descriptor::JoinRefreshOutputSource::Action(action_column.column_id),
    });
    Ok(descriptor::JoinRefreshDescriptor {
        mode: descriptor::JoinRefreshMode::Full,
        mv_identity: descriptor::JoinRefreshMvIdentity {
            catalog: snapshot.target.catalog.clone(),
            database: snapshot.target.namespace.clone(),
            name: snapshot.target.table.clone(),
        },
        left_base_fqn: left.fqn(),
        right_base_fqn: right.fqn(),
        left_row_id_column,
        right_row_id_column,
        action_column,
        join_apply_key_column,
        payload_columns,
        join_key_pairs,
        output_mappings,
        branches: Vec::new(),
        needs_target_locator: false,
    })
}

fn find_unique_column(
    columns: &[crate::analysis::OutputColumn],
    name: &str,
    role: &str,
) -> Result<crate::analysis::OutputColumn, String> {
    let matches = columns
        .iter()
        .filter(|column| column.name.eq_ignore_ascii_case(name))
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [column] => Ok((*column).clone()),
        [] => Err(format!(
            "join first-refresh cannot find {role} column {name}"
        )),
        _ => Err(format!(
            "join first-refresh found multiple {role} columns named {name}"
        )),
    }
}

fn project_item(column: &crate::analysis::OutputColumn) -> crate::analysis::ProjectItem {
    crate::analysis::ProjectItem {
        expr: crate::analysis::TypedExpr {
            kind: crate::analysis::ExprKind::ColumnRef {
                column_id: column.column_id,
                qualifier: None,
                column: column.name.clone(),
            },
            data_type: column.data_type.clone(),
            nullable: column.nullable,
        },
        output_name: column.name.clone(),
        output_column_id: column.column_id,
    }
}

fn output_column(
    column_id: crate::column_id::ColumnId,
    name: &str,
    data_type: arrow::datatypes::DataType,
    nullable: bool,
    is_internal: bool,
) -> crate::analysis::OutputColumn {
    crate::analysis::OutputColumn {
        column_id,
        name: name.to_string(),
        data_type,
        nullable,
        is_internal,
    }
}

fn reserve_factory_for_plan(
    factory: &mut crate::column_id::ColumnRefFactory,
    plan: &crate::planner::logical::LogicalPlanNode,
) -> Result<(), String> {
    let mut max_id = crate::planner::plan_output_columns(plan)?
        .iter()
        .map(|column| column.column_id.0)
        .max()
        .unwrap_or(0);
    for child in &plan.children {
        max_id = max_id.max(max_plan_column_id(child)?);
    }
    factory.reserve_until(max_id.saturating_add(1));
    Ok(())
}

fn max_plan_column_id(plan: &crate::planner::logical::LogicalPlanNode) -> Result<u32, String> {
    let mut max_id = crate::planner::plan_output_columns(plan)?
        .iter()
        .map(|column| column.column_id.0)
        .max()
        .unwrap_or(0);
    for child in &plan.children {
        max_id = max_id.max(max_plan_column_id(child)?);
    }
    Ok(max_id)
}

impl MvFirstRefreshPhysicalSql {
    pub(crate) fn sql(&self) -> &str {
        &self.sql
    }

    pub(crate) fn root_hash_column(&self) -> &str {
        &self.root_hash_column
    }
}

/// Validated logical shape of a first-refresh append.  All variants have one
/// empty target and therefore one sealed primary append cohort.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MvFirstRefreshShape {
    Projection,
    UnionProjection,
    Aggregate,
    FanInAggregate,
    BranchUnionAggregate,
    Join,
    JoinAggregate,
    ComposedAggregate,
}

/// Target facts frozen before a first-refresh writer is admitted.  It carries
/// Arrow schema and field identities, never an Iceberg table/client or a
/// provider decoder.
/// Opaque, value-only target facts for first-refresh SQL shaping.
///
/// This is deliberately not an IMV planner graph: Core may construct it from
/// already frozen target facts, but it contains neither provider authority nor
/// a mutable planner tree.
#[derive(Clone)]
pub struct MvFirstRefreshTargetContract {
    schema: SchemaRef,
    field_ids: Vec<i32>,
    partition_spec_id: i32,
    hidden_hash_key: String,
}

impl MvFirstRefreshTargetContract {
    pub fn try_new(
        schema: SchemaRef,
        field_ids: Vec<i32>,
        partition_spec_id: i32,
        hidden_hash_key: String,
    ) -> Result<Self, String> {
        if schema.fields().is_empty()
            || schema.fields().len() != field_ids.len()
            || field_ids.iter().any(|field_id| *field_id <= 0)
            || field_ids.iter().collect::<BTreeSet<_>>().len() != field_ids.len()
            || partition_spec_id < 0
            || hidden_hash_key.is_empty()
        {
            return Err("invalid MV first-refresh target physical contract".to_string());
        }
        Ok(Self {
            schema,
            field_ids,
            partition_spec_id,
            hidden_hash_key,
        })
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn field_ids(&self) -> &[i32] {
        &self.field_ids
    }

    pub const fn partition_spec_id(&self) -> i32 {
        self.partition_spec_id
    }

    pub fn hidden_hash_key(&self) -> &str {
        &self.hidden_hash_key
    }

    /// Verify provider-observed target facts before a deferred writer is
    /// activated. This is value-only so the SQL contract retains neither a
    /// catalog handle nor a provider codec.
    pub(crate) fn validate_observed(
        &self,
        schema: &Schema,
        field_ids: &[i32],
        partition_spec_id: i32,
    ) -> Result<(), String> {
        if schema != self.schema.as_ref()
            || field_ids != self.field_ids
            || partition_spec_id != self.partition_spec_id
        {
            return Err(
                "MV first-refresh target physical contract drifted after preparation".to_string(),
            );
        }
        if !self
            .schema
            .fields()
            .iter()
            .any(|field| field.name() == &self.hidden_hash_key)
        {
            return Err(
                "MV first-refresh target contract has no hidden hash key field".to_string(),
            );
        }
        Ok(())
    }
}

pub(crate) fn prepare_projection_first_refresh_write_sql(
    select_sql: &str,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<SqlMvFirstRefreshArtifact, String> {
    let sql = prepare_projection_full_read_sql(select_sql, pin, current_catalog, current_database)?;
    Ok(SqlMvFirstRefreshArtifact::from_physical(
        MvFirstRefreshPhysicalSql {
            sql,
            root_hash_column: crate::planner::vocabulary::HIDDEN_APPLY_KEY_COLUMN_NAME.to_string(),
        },
    ))
}

pub(crate) fn prepare_union_projection_first_refresh_write_sql(
    select_sql: &str,
    branch_count: usize,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<SqlMvFirstRefreshArtifact, String> {
    let sql = prepare_union_projection_full_read_sql(
        select_sql,
        branch_count,
        pin,
        current_catalog,
        current_database,
    )?;
    Ok(SqlMvFirstRefreshArtifact::from_physical(
        MvFirstRefreshPhysicalSql {
            sql,
            root_hash_column: crate::planner::vocabulary::HIDDEN_APPLY_KEY_COLUMN_NAME.to_string(),
        },
    ))
}

pub(crate) fn prepare_aggregate_first_refresh_write_sql(
    select_sql: &str,
    calls: &SqlAggregateCalls,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<SqlMvFirstRefreshArtifact, String> {
    prepare_aggregate_first_refresh_write_sql_with_target_schema(
        select_sql,
        calls,
        pin,
        current_catalog,
        current_database,
        None,
    )
}

pub(crate) fn prepare_aggregate_first_refresh_write_sql_with_target_schema(
    select_sql: &str,
    calls: &SqlAggregateCalls,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
    target_schema: Option<&Schema>,
) -> Result<SqlMvFirstRefreshArtifact, String> {
    prepare_aggregate_first_refresh_write_sql_with_target_schema_and_input_types(
        select_sql,
        calls,
        pin,
        current_catalog,
        current_database,
        target_schema,
        None,
    )
}

pub(crate) fn prepare_aggregate_first_refresh_write_sql_with_target_schema_and_input_types(
    select_sql: &str,
    calls: &SqlAggregateCalls,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
    target_schema: Option<&Schema>,
    aggregate_input_types: Option<&[Option<DataType>]>,
) -> Result<SqlMvFirstRefreshArtifact, String> {
    let state_sql = prepare_aggregate_first_refresh_state_sql(
        select_sql,
        calls,
        pin,
        current_catalog,
        current_database,
    )?;
    Ok(SqlMvFirstRefreshArtifact::from_physical(
        MvFirstRefreshPhysicalSql {
            sql: aggregate_physical_sql(
                &state_sql,
                calls,
                None,
                target_schema,
                aggregate_input_types,
            )?,
            root_hash_column: SQL_MV_ROW_ID_COLUMN.to_string(),
        },
    ))
}

/// Fan-in aggregate first refresh uses the same state-shaped physical project
/// as a single aggregate.  The canonical SELECT already contains the pinned
/// UNION ALL input, so keeping this as a separate entry point makes the shape
/// contract explicit without reintroducing a frontend materialization phase.
pub(crate) fn prepare_fan_in_aggregate_first_refresh_write_sql(
    select_sql: &str,
    calls: &SqlAggregateCalls,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<SqlMvFirstRefreshArtifact, String> {
    prepare_fan_in_aggregate_first_refresh_write_sql_with_target_schema(
        select_sql,
        calls,
        pin,
        current_catalog,
        current_database,
        None,
    )
}

pub(crate) fn prepare_fan_in_aggregate_first_refresh_write_sql_with_target_schema(
    select_sql: &str,
    calls: &SqlAggregateCalls,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
    target_schema: Option<&Schema>,
) -> Result<SqlMvFirstRefreshArtifact, String> {
    prepare_fan_in_aggregate_first_refresh_write_sql_with_target_schema_and_input_types(
        select_sql,
        calls,
        pin,
        current_catalog,
        current_database,
        target_schema,
        None,
    )
}

pub(crate) fn prepare_fan_in_aggregate_first_refresh_write_sql_with_target_schema_and_input_types(
    select_sql: &str,
    calls: &SqlAggregateCalls,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
    target_schema: Option<&Schema>,
    aggregate_input_types: Option<&[Option<DataType>]>,
) -> Result<SqlMvFirstRefreshArtifact, String> {
    prepare_aggregate_first_refresh_write_sql_with_target_schema_and_input_types(
        select_sql,
        calls,
        pin,
        current_catalog,
        current_database,
        target_schema,
        aggregate_input_types,
    )
}

/// A composed aggregate (for example aggregate-over-join) is still one
/// state-shaped SELECT.  Its join/fan-in relationship lives below the common
/// aggregate project and therefore remains BE-owned all the way to the
/// connector writer.
pub(crate) fn prepare_composed_aggregate_first_refresh_write_sql(
    select_sql: &str,
    calls: &SqlAggregateCalls,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<SqlMvFirstRefreshArtifact, String> {
    prepare_aggregate_first_refresh_write_sql(
        select_sql,
        calls,
        pin,
        current_catalog,
        current_database,
    )
}

pub(crate) fn prepare_branch_union_aggregate_first_refresh_write_sql(
    select_sql: &str,
    branch_count: usize,
    first_branch_calls: &SqlAggregateCalls,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<SqlMvFirstRefreshArtifact, String> {
    prepare_branch_union_aggregate_first_refresh_write_sql_with_target_schema(
        select_sql,
        branch_count,
        first_branch_calls,
        pin,
        current_catalog,
        current_database,
        None,
    )
}

pub(crate) fn prepare_branch_union_aggregate_first_refresh_write_sql_with_target_schema(
    select_sql: &str,
    branch_count: usize,
    first_branch_calls: &SqlAggregateCalls,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
    target_schema: Option<&Schema>,
) -> Result<SqlMvFirstRefreshArtifact, String> {
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
            aggregate_physical_sql(&state_sql, &calls, Some(branch_id), target_schema, None)
        })
        .collect::<Result<Vec<_>, _>>()?
        .join(" UNION ALL ");
    Ok(SqlMvFirstRefreshArtifact::from_physical(
        MvFirstRefreshPhysicalSql {
            sql,
            root_hash_column: SQL_MV_ROW_ID_COLUMN.to_string(),
        },
    ))
}

fn prepare_aggregate_first_refresh_state_sql(
    select_sql: &str,
    calls: &SqlAggregateCalls,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<String, String> {
    let state_sql = rewrite_select_sql_for_state(select_sql, calls)?;
    pin_state_sql(&state_sql, pin, current_catalog, current_database)
}

fn prepare_branch_union_aggregate_first_refresh_state_sqls(
    select_sql: &str,
    branch_count: usize,
    first_branch_calls: &SqlAggregateCalls,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<Vec<(SqlAggregateCalls, String)>, String> {
    branch_union_queries(select_sql, branch_count)?
        .into_iter()
        .enumerate()
        .map(|(branch_index, (branch_query, branch_sql))| {
            let branch_calls = SqlAggregateCalls::extract(&branch_query)?;
            if branch_index == 0 && &branch_calls != first_branch_calls {
                return Err(
                    "branch UNION ALL aggregate first branch calls drifted from the validated contract"
                        .to_string(),
                );
            }
            let state_sql = prepare_aggregate_first_refresh_state_sql(
                &branch_sql,
                &branch_calls,
                pin,
                current_catalog,
                current_database,
            )?;
            Ok((branch_calls, state_sql))
        })
        .collect()
}

fn aggregate_physical_sql(
    state_sql: &str,
    calls: &SqlAggregateCalls,
    branch_id: Option<i32>,
    target_schema: Option<&Schema>,
    aggregate_input_types: Option<&[Option<DataType>]>,
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
        quote_sql_identifier(SQL_MV_ROW_ID_COLUMN),
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
                let witness = if matches!(
                    aggregate.function,
                    AggregateFunctionKind::Sum
                        | AggregateFunctionKind::Min
                        | AggregateFunctionKind::Max
                ) {
                    target_schema
                        .and_then(|schema| {
                            schema
                                .fields()
                                .iter()
                                .find(|field| field.name() == &aggregate.output_name)
                        })
                        .map(|field| aggregate_visible_type_witness(field.data_type()))
                        .transpose()?
                } else {
                    None
                };
                let args = if aggregate.function == AggregateFunctionKind::Avg {
                    let input_type = aggregate_input_types
                        .and_then(|types| types.get(*aggregate_index))
                        .and_then(Option::as_ref);
                    let output_witness = target_schema
                        .and_then(|schema| {
                            schema
                                .fields()
                                .iter()
                                .find(|field| field.name() == &aggregate.output_name)
                        })
                        .map(|field| aggregate_visible_type_witness(field.data_type()))
                        .transpose()?;
                    match output_witness {
                        Some(witness) => {
                            let input_scale = match input_type {
                                Some(DataType::Decimal128(_, scale)) => i64::from(*scale),
                                _ => -1,
                            };
                            format!(
                                "{}, CAST({input_scale} AS BIGINT), {witness}",
                                qualified_column("state", &state_name)
                            )
                        }
                        None => qualified_column("state", &state_name),
                    }
                } else {
                    match witness {
                        Some(witness) => {
                            format!("{}, {witness}", qualified_column("state", &state_name))
                        }
                        None => qualified_column("state", &state_name),
                    }
                };
                projection.push(format!(
                    "{}({args}) AS {}",
                    aggregate_visible_function(aggregate.function),
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
    if calls.needs_retraction_count_state() {
        projection.push(format!(
            "{} AS {}",
            qualified_column("state", SQL_MV_AGG_RETRACTION_COUNT_STATE_COLUMN),
            quote_sql_identifier(SQL_MV_AGG_RETRACTION_COUNT_STATE_COLUMN),
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

fn aggregate_visible_type_witness(data_type: &DataType) -> Result<String, String> {
    let sql_type = match data_type {
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 => "TINYINT".to_string(),
        DataType::Int16 => "SMALLINT".to_string(),
        DataType::Int32 => "INT".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 => "STRING".to_string(),
        DataType::Date32 => "DATE".to_string(),
        DataType::Timestamp(_, _) => "DATETIME".to_string(),
        DataType::Decimal128(precision, scale) => format!("DECIMAL({precision},{scale})"),
        other => {
            return Err(format!(
                "unsupported MV aggregate visible target type {other:?}"
            ));
        }
    };
    Ok(format!("CAST(NULL AS {sql_type})"))
}

fn validate_branch_aggregate_contract(
    branch_index: usize,
    calls: &SqlAggregateCalls,
    expected: &SqlAggregateCalls,
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
    use arrow::datatypes::{DataType, Field, Schema};
    use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
    use std::sync::Arc;

    use super::*;

    fn sqlx2_target_binding() -> SqlTableBindingId {
        SqlTableBindingId::new(
            crate::binding::SqlTableBindingScopeId::new(NonZeroU64::new(701).unwrap()),
            NonZeroU32::new(1).unwrap(),
        )
    }

    fn sqlx2_target_contract() -> MvFirstRefreshTargetContract {
        MvFirstRefreshTargetContract::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "__apply_key__",
                DataType::Utf8,
                false,
            )])),
            vec![1],
            0,
            "__apply_key__".to_string(),
        )
        .expect("valid SQL target contract")
    }

    struct CanonicalCatalog;

    impl crate::compiler::SqlCatalogSnapshot for CanonicalCatalog {
        fn planner_table_provider(&self) -> &dyn crate::catalog::PlannerTableProvider {
            panic!("plain canonical request construction must not resolve a catalog")
        }
    }

    struct CanonicalFunctions;

    impl crate::compiler::SqlFunctionCatalog for CanonicalFunctions {
        fn resolve_scalar_signature(
            &self,
            _name: &str,
            _arg_types: &[arrow::datatypes::DataType],
        ) -> Result<crate::functions::ResolvedScalarFunction, crate::functions::ResolveError>
        {
            panic!("plain canonical request construction must not resolve functions")
        }

        fn volatility(&self, _name: &str) -> crate::functions::FunctionVolatility {
            panic!("plain canonical request construction must not resolve functions")
        }
    }

    #[test]
    fn join_first_refresh_canonical_request_avoids_imv_rewrite_and_terminal_is_sealed() {
        let statement =
            crate::parser::parse_normalized_sql_raw("SELECT 1").expect("parse canonical query");
        let sqlparser::ast::Statement::Query(query) = statement else {
            panic!("fixture must be a query");
        };
        let catalog = CanonicalCatalog;
        let statistics = crate::planning::dml::DmlStatisticsSnapshot::empty();
        let functions = CanonicalFunctions;
        let request = plain_join_first_refresh_logical_request(
            *query,
            None,
            "db".to_string(),
            crate::compiler::SessionOptimizerSettings::default(),
            crate::compiler::SqlPlanningEnvironment::Distributed {
                backend_count: NonZeroUsize::new(1).expect("non-zero"),
            },
            &catalog,
            &statistics,
            &functions,
            crate::compiler::SqlCompileControl::unbounded(),
        );
        assert!(request.imv_rewrite.is_none());
        let _: fn(
            SqlMvJoinFirstRefreshCompileContext<'_>,
        ) -> Result<crate::plan_read::DistributedPlan, String> =
            compile_join_first_refresh_connector_write;
    }

    #[test]
    fn sqlx2_mv_first_refresh_plan_is_sql_only_and_binding_scoped() {
        let plan = SqlMvFirstRefreshPlanner::plan(SqlMvFirstRefreshPlannerInput {
            shape: MvFirstRefreshShape::Projection,
            target_contract: sqlx2_target_contract(),
            target_binding: sqlx2_target_binding(),
            root_distribution: RootDistributionRequirement::ShuffleOutputName(
                "__apply_key__".to_string(),
            ),
            artifact: SqlMvFirstRefreshArtifactInput::Sql(MvFirstRefreshPhysicalSql {
                sql: "SELECT 1 AS `__apply_key__`".to_string(),
                root_hash_column: "__apply_key__".to_string(),
            }),
        })
        .expect("pure SQL first-refresh plan");

        assert_eq!(plan.shape(), MvFirstRefreshShape::Projection);
        assert_eq!(plan.target_binding(), sqlx2_target_binding());
        assert_eq!(plan.target_contract().hidden_hash_key(), "__apply_key__");
        assert!(matches!(
            plan.into_artifact(),
            SqlMvFirstRefreshPlanArtifact::Sql(_)
        ));
    }

    #[test]
    fn sqlx2_mv_first_refresh_plan_rejects_implicit_or_wrong_distribution() {
        let make_input = |root_distribution| SqlMvFirstRefreshPlannerInput {
            shape: MvFirstRefreshShape::Projection,
            target_contract: sqlx2_target_contract(),
            target_binding: sqlx2_target_binding(),
            root_distribution,
            artifact: SqlMvFirstRefreshArtifactInput::Sql(MvFirstRefreshPhysicalSql {
                sql: "SELECT 1 AS `__apply_key__`".to_string(),
                root_hash_column: "__apply_key__".to_string(),
            }),
        };

        assert!(
            SqlMvFirstRefreshPlanner::plan(make_input(RootDistributionRequirement::Any)).is_err()
        );
        assert!(
            SqlMvFirstRefreshPlanner::plan(make_input(
                RootDistributionRequirement::ShuffleOutputName("other".to_string())
            ))
            .is_err()
        );
    }

    fn pin() -> SqlMvSnapshotPin {
        SqlMvSnapshotPin::from_entries_for_tests(&[("ice.db.fact", 42, "fact-uuid")])
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
            crate::planner::vocabulary::HIDDEN_APPLY_KEY_COLUMN_NAME
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
        let normalized = crate::parser::dialect::normalize_for_raw_parse(
            "SELECT k, sum(v) AS total FROM ice.db.fact GROUP BY k",
        )
        .unwrap();
        let statement = crate::parser::parse_normalized_sql_raw(&normalized).unwrap();
        let sqlparser::ast::Statement::Query(query) = statement else {
            panic!("expected SELECT")
        };
        let calls = SqlAggregateCalls::extract(&query).unwrap();
        let prepared = prepare_aggregate_first_refresh_write_sql(
            "SELECT k, sum(v) AS total FROM ice.db.fact GROUP BY k",
            &calls,
            &pin(),
            Some("ice"),
            "db",
        )
        .unwrap();
        assert_eq!(prepared.root_hash_column(), SQL_MV_ROW_ID_COLUMN);
        assert!(prepared.sql().contains("mv_group_row_id"));
        assert!(prepared.sql().contains("sum_state_visible"));
        assert!(prepared.sql().contains("__agg_state_total"));
        assert!(!prepared.sql().contains("RecordBatch"));
    }

    #[test]
    fn fan_in_aggregate_remains_one_pinned_be_state_project() {
        let sql = "SELECT k, sum(v) AS total FROM (SELECT k, v FROM ice.db.a UNION ALL SELECT k, v FROM ice.db.b) AS input GROUP BY k";
        let normalized = crate::parser::dialect::normalize_for_raw_parse(sql).unwrap();
        let statement = crate::parser::parse_normalized_sql_raw(&normalized).unwrap();
        let sqlparser::ast::Statement::Query(query) = statement else {
            panic!("expected SELECT")
        };
        let calls = SqlAggregateCalls::extract(&query).unwrap();
        let pin = SqlMvSnapshotPin::from_entries_for_tests(&[
            ("ice.db.a", 11, "a-uuid"),
            ("ice.db.b", 22, "b-uuid"),
        ]);
        let prepared =
            prepare_fan_in_aggregate_first_refresh_write_sql(sql, &calls, &pin, Some("ice"), "db")
                .unwrap();
        assert_eq!(prepared.root_hash_column(), SQL_MV_ROW_ID_COLUMN);
        assert!(prepared.sql().contains("VERSION AS OF 11"));
        assert!(prepared.sql().contains("VERSION AS OF 22"));
        assert!(prepared.sql().contains("sum_state_visible"));
    }

    #[test]
    fn fan_in_decimal_avg_freezes_input_scale_and_visible_type_in_be_sql() {
        let sql = "SELECT k, avg(d) AS a_d FROM (SELECT k, d FROM ice.db.a UNION ALL SELECT k, d FROM ice.db.b) AS input GROUP BY k";
        let normalized = crate::parser::dialect::normalize_for_raw_parse(sql).unwrap();
        let statement = crate::parser::parse_normalized_sql_raw(&normalized).unwrap();
        let sqlparser::ast::Statement::Query(query) = statement else {
            panic!("expected SELECT")
        };
        let calls = SqlAggregateCalls::extract(&query).unwrap();
        let target = Schema::new(vec![
            Field::new("k", DataType::Int32, true),
            Field::new("a_d", DataType::Decimal128(38, 12), true),
        ]);
        let prepared =
            prepare_fan_in_aggregate_first_refresh_write_sql_with_target_schema_and_input_types(
                sql,
                &calls,
                &SqlMvSnapshotPin::from_entries_for_tests(&[
                    ("ice.db.a", 11, "a"),
                    ("ice.db.b", 22, "b"),
                ]),
                Some("ice"),
                "db",
                Some(&target),
                Some(&[Some(DataType::Decimal128(20, 4))]),
            )
            .unwrap();
        assert!(prepared.sql().contains("avg_state_visible(`state`.`__agg_state_a_d`, CAST(4 AS BIGINT), CAST(NULL AS DECIMAL(38,12)))"), "{}", prepared.sql());
    }

    #[test]
    fn composed_aggregate_remains_one_pinned_be_state_project() {
        let sql = "SELECT a.k, count(*) AS total FROM ice.db.a AS a JOIN ice.db.b AS b ON a.k = b.k GROUP BY a.k";
        let normalized = crate::parser::dialect::normalize_for_raw_parse(sql).unwrap();
        let statement = crate::parser::parse_normalized_sql_raw(&normalized).unwrap();
        let sqlparser::ast::Statement::Query(query) = statement else {
            panic!("expected SELECT")
        };
        let calls = SqlAggregateCalls::extract(&query).unwrap();
        let pin = SqlMvSnapshotPin::from_entries_for_tests(&[
            ("ice.db.a", 11, "a-uuid"),
            ("ice.db.b", 22, "b-uuid"),
        ]);
        let prepared = prepare_composed_aggregate_first_refresh_write_sql(
            sql,
            &calls,
            &pin,
            Some("ice"),
            "db",
        )
        .unwrap();
        assert_eq!(prepared.root_hash_column(), SQL_MV_ROW_ID_COLUMN);
        assert!(prepared.sql().contains("VERSION AS OF 11"));
        assert!(prepared.sql().contains("VERSION AS OF 22"));
        assert!(prepared.sql().contains("count_state_visible"));
    }

    #[test]
    fn target_contract_rejects_schema_identity_and_partition_drift() {
        let expected = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, true),
            Field::new("__apply_key__", DataType::Utf8, false),
        ]));
        let contract = MvFirstRefreshTargetContract::try_new(
            Arc::clone(&expected),
            vec![1, 2],
            7,
            "__apply_key__".to_string(),
        )
        .expect("valid target contract");
        contract
            .validate_observed(expected.as_ref(), &[1, 2], 7)
            .expect("exact observed contract");
        assert!(
            contract
                .validate_observed(expected.as_ref(), &[1, 3], 7)
                .is_err()
        );
        assert!(
            contract
                .validate_observed(expected.as_ref(), &[1, 2], 8)
                .is_err()
        );
        let drifted_schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, false),
            Field::new("__apply_key__", DataType::Utf8, false),
        ]));
        assert!(
            contract
                .validate_observed(drifted_schema.as_ref(), &[1, 2], 7)
                .is_err()
        );
    }
}
