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
use crate::sql::binding::SqlTableBindingId;
use crate::sql::column_id::ColumnRefFactory;
use crate::sql::compiler::RootDistributionRequirement;
use crate::sql::mv_refresh::aggregate_shape::{
    SQL_MV_AGG_RETRACTION_COUNT_STATE_COLUMN, SQL_MV_ROW_ID_COLUMN, SqlAggregateCalls,
    rewrite_select_sql_for_state, state_column_name,
};
use crate::sql::mv_refresh::{AggregateFunctionKind, VisibleAggregateOutput};
use crate::sql::planner::logical::LogicalPlanNode;
use crate::sql::planner::vocabulary::BRANCH_ID_COLUMN_NAME;
use arrow::datatypes::{DataType, Schema, SchemaRef};
use std::collections::BTreeSet;

pub(crate) use self::sql_shape::SqlMvSnapshotPin;
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
    artifact: SqlMvFirstRefreshArtifact,
}

pub(crate) enum SqlMvFirstRefreshArtifact {
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
                (SqlMvFirstRefreshArtifact::Sql(sql), root_hash_column)
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
                    SqlMvFirstRefreshArtifact::Logical { plan, factory },
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

    pub(crate) fn into_artifact(self) -> SqlMvFirstRefreshArtifact {
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
#[derive(Clone)]
pub(crate) struct MvFirstRefreshTargetContract {
    schema: SchemaRef,
    field_ids: Vec<i32>,
    partition_spec_id: i32,
    hidden_hash_key: String,
}

impl MvFirstRefreshTargetContract {
    pub(crate) fn try_new(
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

    pub(crate) fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub(crate) fn field_ids(&self) -> &[i32] {
        &self.field_ids
    }

    pub(crate) const fn partition_spec_id(&self) -> i32 {
        self.partition_spec_id
    }

    pub(crate) fn hidden_hash_key(&self) -> &str {
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
) -> Result<MvFirstRefreshPhysicalSql, String> {
    let sql = prepare_projection_full_read_sql(select_sql, pin, current_catalog, current_database)?;
    Ok(MvFirstRefreshPhysicalSql {
        sql,
        root_hash_column: crate::sql::planner::vocabulary::HIDDEN_APPLY_KEY_COLUMN_NAME.to_string(),
    })
}

pub(crate) fn prepare_union_projection_first_refresh_write_sql(
    select_sql: &str,
    branch_count: usize,
    pin: &SqlMvSnapshotPin,
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
        root_hash_column: crate::sql::planner::vocabulary::HIDDEN_APPLY_KEY_COLUMN_NAME.to_string(),
    })
}

pub(crate) fn prepare_aggregate_first_refresh_write_sql(
    select_sql: &str,
    calls: &SqlAggregateCalls,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<MvFirstRefreshPhysicalSql, String> {
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
) -> Result<MvFirstRefreshPhysicalSql, String> {
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
) -> Result<MvFirstRefreshPhysicalSql, String> {
    let state_sql = prepare_aggregate_first_refresh_state_sql(
        select_sql,
        calls,
        pin,
        current_catalog,
        current_database,
    )?;
    Ok(MvFirstRefreshPhysicalSql {
        sql: aggregate_physical_sql(
            &state_sql,
            calls,
            None,
            target_schema,
            aggregate_input_types,
        )?,
        root_hash_column: SQL_MV_ROW_ID_COLUMN.to_string(),
    })
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
) -> Result<MvFirstRefreshPhysicalSql, String> {
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
) -> Result<MvFirstRefreshPhysicalSql, String> {
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
) -> Result<MvFirstRefreshPhysicalSql, String> {
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
) -> Result<MvFirstRefreshPhysicalSql, String> {
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
) -> Result<MvFirstRefreshPhysicalSql, String> {
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
            aggregate_physical_sql(&state_sql, &calls, Some(branch_id), target_schema, None)
        })
        .collect::<Result<Vec<_>, _>>()?
        .join(" UNION ALL ");
    Ok(MvFirstRefreshPhysicalSql {
        sql,
        root_hash_column: SQL_MV_ROW_ID_COLUMN.to_string(),
    })
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
    use std::num::{NonZeroU32, NonZeroU64};
    use std::sync::Arc;

    use super::*;

    fn sqlx2_target_binding() -> SqlTableBindingId {
        SqlTableBindingId::new(
            crate::sql::binding::SqlTableBindingScopeId::new(NonZeroU64::new(701).unwrap()),
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
            SqlMvFirstRefreshArtifact::Sql(_)
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
            crate::sql::planner::vocabulary::HIDDEN_APPLY_KEY_COLUMN_NAME
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
        let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql).unwrap();
        let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized).unwrap();
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
        let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql).unwrap();
        let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized).unwrap();
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
        let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql).unwrap();
        let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized).unwrap();
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
