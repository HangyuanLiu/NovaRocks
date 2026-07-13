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

use std::sync::Arc;

use crate::connector::iceberg::catalog::registry::{block_on_iceberg, build_iceberg_catalog};
use crate::engine::StandaloneState;
use crate::engine::query_options::StandaloneQueryOptions;
use crate::runtime::coordinator::CoordinatedQueryResult;
use crate::sql::analysis::OutputColumn;
use crate::sql::common::ChangeStreamBranchKind;
use crate::sql::optimizer::OptimizedOperatorNode;
use crate::sql::planner::distributed::write::change_stream::{
    ChangeStreamWriteBranchSpec, ChangeStreamWriteDagSpec,
};
use crate::sql::planner::distributed::write::sink::IcebergWriteSinkSpec;

pub(crate) const DML_CHANGE_STREAM_DATA_ROUTE_COLUMN: &str = "__change_data_route";

pub(crate) struct DmlChangeStreamWritePlan {
    pub(crate) producer: OptimizedOperatorNode,
    pub(crate) dag: ChangeStreamWriteDagSpec,
    pub(crate) pre_expand_keyed_assert: Option<DmlPreExpandKeyedAssert>,
}

#[derive(Clone, Debug)]
pub(crate) struct DmlPreExpandKeyedAssert {
    pub(crate) key_column_name: String,
    pub(crate) key_label: String,
    pub(crate) message_prefix: String,
}

#[derive(Debug)]
pub(crate) struct DmlChangeStreamWriteExecution {
    pub(crate) result: CoordinatedQueryResult,
    pub(crate) commit_plan:
        crate::engine::iceberg_change_stream_write::ChangeStreamWriterCommitPlan,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DmlChangeStreamBranchSet {
    UpdateMor,
    Merge {
        matched_update: bool,
        matched_delete: bool,
        not_matched_insert: bool,
    },
}

#[derive(Clone, Debug, Default)]
struct DmlChangeStreamWriteBranchSinkSpecs {
    delete_dv: Option<IcebergWriteSinkSpec>,
    reuse_data: Option<IcebergWriteSinkSpec>,
    fresh_data: Option<IcebergWriteSinkSpec>,
    target_partition_source_columns: Vec<String>,
}

impl DmlChangeStreamBranchSet {
    fn branch_kinds(self) -> Vec<ChangeStreamBranchKind> {
        match self {
            Self::UpdateMor => vec![
                ChangeStreamBranchKind::DeleteDv,
                ChangeStreamBranchKind::ReuseData,
            ],
            Self::Merge {
                matched_update,
                matched_delete,
                not_matched_insert,
            } => {
                let mut branches = Vec::with_capacity(3);
                if matched_update || matched_delete {
                    branches.push(ChangeStreamBranchKind::DeleteDv);
                }
                if matched_update {
                    branches.push(ChangeStreamBranchKind::ReuseData);
                }
                if not_matched_insert {
                    branches.push(ChangeStreamBranchKind::FreshData);
                }
                branches
            }
        }
    }
}

pub(crate) fn build_dml_change_stream_write_plan(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    producer: OptimizedOperatorNode,
    branch_set: DmlChangeStreamBranchSet,
    target_ref: &str,
) -> Result<DmlChangeStreamWritePlan, String> {
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&target.catalog)?
    };
    let catalog = build_iceberg_catalog(&entry)?;
    let table_ident = iceberg::TableIdent::new(
        iceberg::NamespaceIdent::new(target.namespace.clone()),
        target.table.clone(),
    );
    let table = block_on_iceberg(async { catalog.load_table(&table_ident).await })?
        .map_err(|e| format!("load iceberg table {}: {e}", &table_ident))?;
    let resolved = {
        let registry = state.connectors.read().expect("connector registry read");
        let backend = registry.catalog_backend("iceberg")?;
        backend.load_table(&target.catalog, &target.namespace, &target.table)?
    };

    let branch_kinds = branch_set.branch_kinds();
    if branch_kinds.is_empty() {
        return Err("DML change-stream write requires at least one branch".to_string());
    }
    let mut sink_specs = DmlChangeStreamWriteBranchSinkSpecs {
        target_partition_source_columns: target_partition_source_column_names(table.metadata())?,
        ..Default::default()
    };
    if branch_kinds.contains(&ChangeStreamBranchKind::DeleteDv) {
        sink_specs.delete_dv = Some(
            crate::engine::mutation_flow::build_mor_deletion_vector_sink_spec(
                target, &resolved, &table, &entry, target_ref,
            )?,
        );
    }
    if branch_kinds.contains(&ChangeStreamBranchKind::ReuseData) {
        sink_specs.reuse_data = Some(
            crate::engine::iceberg_writer::build_row_lineage_data_sink_spec(
                target, &resolved, &table, &entry,
            )?,
        );
    }
    if branch_kinds.contains(&ChangeStreamBranchKind::FreshData) {
        let write_columns = crate::engine::iceberg_writer::iceberg_insert_columns_from_schema(
            table.metadata().current_schema(),
        )?;
        sink_specs.fresh_data = Some(crate::engine::iceberg_writer::build_insert_write_sink_spec(
            target,
            &resolved,
            &table,
            &entry,
            &write_columns,
        )?);
    }

    let dag = build_dml_change_stream_dag_from_sink_specs(
        branch_set,
        &producer.output_columns,
        sink_specs,
    )?;
    Ok(DmlChangeStreamWritePlan {
        producer,
        dag,
        pre_expand_keyed_assert: None,
    })
}

fn build_dml_change_stream_dag_from_sink_specs(
    branch_set: DmlChangeStreamBranchSet,
    producer_output_columns: &[OutputColumn],
    mut sink_specs: DmlChangeStreamWriteBranchSinkSpecs,
) -> Result<ChangeStreamWriteDagSpec, String> {
    let branch_kinds = branch_set.branch_kinds();
    if branch_kinds.is_empty() {
        return Err("DML change-stream write requires at least one branch".to_string());
    }
    let has_data_branch = branch_kinds.iter().any(|kind| {
        matches!(
            kind,
            ChangeStreamBranchKind::ReuseData | ChangeStreamBranchKind::FreshData
        )
    });
    let change_op_output_ordinal = output_ordinal_by_name(
        producer_output_columns,
        crate::exec::change_op::CHANGE_OP_COLUMN,
        "change-op column",
        OutputBindingKind::Internal,
    )?;
    let data_route_output_ordinal = if has_data_branch {
        Some(output_ordinal_by_name(
            producer_output_columns,
            DML_CHANGE_STREAM_DATA_ROUTE_COLUMN,
            "data-route column",
            OutputBindingKind::Internal,
        )?)
    } else {
        None
    };
    let data_partition_ordinals = if has_data_branch {
        target_partition_source_ordinals(
            producer_output_columns,
            &sink_specs.target_partition_source_columns,
        )?
    } else {
        Vec::new()
    };

    let mut branches = Vec::with_capacity(branch_kinds.len());
    for (idx, branch_kind) in branch_kinds.into_iter().enumerate() {
        let (sink_spec, output_partition_ordinals) = match branch_kind {
            ChangeStreamBranchKind::DeleteDv => {
                let sink_spec = sink_specs
                    .delete_dv
                    .take()
                    .ok_or_else(|| "DML change-stream DeleteDv sink spec is missing".to_string())?;
                let file_ordinal = output_ordinal_by_name(
                    producer_output_columns,
                    crate::exec::row_position::ICEBERG_FILE_PATH_COL,
                    "delete file column",
                    OutputBindingKind::Internal,
                )?;
                (sink_spec, vec![file_ordinal])
            }
            ChangeStreamBranchKind::ReuseData => {
                let sink_spec = sink_specs.reuse_data.take().ok_or_else(|| {
                    "DML change-stream ReuseData sink spec is missing".to_string()
                })?;
                (sink_spec, data_partition_ordinals.clone())
            }
            ChangeStreamBranchKind::FreshData => {
                let sink_spec = sink_specs.fresh_data.take().ok_or_else(|| {
                    "DML change-stream FreshData sink spec is missing".to_string()
                })?;
                (sink_spec, data_partition_ordinals.clone())
            }
        };
        let stream_output_ordinals =
            output_ordinals_for_sink_columns(producer_output_columns, &sink_spec.target_columns)?;
        branches.push(ChangeStreamWriteBranchSpec {
            branch_id: i32::try_from(idx).map_err(|_| {
                "DML change-stream branch id overflow while building DAG".to_string()
            })?,
            branch_kind,
            stream_output_ordinals,
            output_partition_ordinals,
            sink_spec,
        });
    }

    let dag = ChangeStreamWriteDagSpec {
        change_op_output_ordinal: Some(change_op_output_ordinal),
        data_route_output_ordinal,
        branches,
    };
    dag.validate()?;
    Ok(dag)
}

pub(crate) fn execute_dml_change_stream_write(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    mut plan: DmlChangeStreamWritePlan,
    query_opts: Option<&StandaloneQueryOptions>,
) -> Result<DmlChangeStreamWriteExecution, String> {
    let crate::engine::PlannedIcebergChangeStreamWrite {
        build_result,
        commit_plan,
        #[cfg(test)]
        topology,
    } = plan_dml_change_stream_write(state, target, &mut plan)?;
    #[cfg(test)]
    if let Some(result) = crate::engine::observe_change_stream_write_build_for_test(&topology) {
        return dml_change_stream_write_execution(result, commit_plan);
    }
    let result = crate::engine::execute_planned_iceberg_change_stream_write(
        build_result,
        query_opts.cloned(),
    )?;
    dml_change_stream_write_execution(result, commit_plan)
}

pub(crate) fn plan_dml_change_stream_write(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    plan: &mut DmlChangeStreamWritePlan,
) -> Result<crate::engine::PlannedIcebergChangeStreamWrite, String> {
    let native_keyed_assert = plan.pre_expand_keyed_assert.clone();
    let planned =
        crate::engine::build_physical_plan_as_iceberg_change_stream_write_with_native_plan_mutation(
        state,
        Some(&target.catalog),
        &target.namespace,
        &plan.producer,
        &mut plan.dag,
        None,
        native_keyed_assert.map(|keyed_assert| {
            Box::new(
                move |native_plan: &mut crate::sql::planner::distributed::DistributedPlan| {
                    inject_dml_pre_expand_keyed_assert_into_native_plan(native_plan, &keyed_assert)
                },
            )
                as Box<
                    dyn FnOnce(
                        &mut crate::sql::planner::distributed::DistributedPlan,
                    ) -> Result<(), String>,
                >
        }),
    )?;
    Ok(planned)
}

pub(crate) fn inject_dml_pre_expand_keyed_assert_into_native_plan(
    plan: &mut crate::sql::planner::distributed::DistributedPlan,
    keyed_assert: &DmlPreExpandKeyedAssert,
) -> Result<(), String> {
    let mut candidate = plan.clone();
    let next_node_id = next_native_node_id(&candidate)?;
    let mut expand_count = 0usize;
    for fragment in &mut candidate.fragments {
        inject_native_keyed_assert_before_expand_node(
            &mut fragment.root,
            keyed_assert,
            next_node_id,
            &mut expand_count,
        )?;
    }
    if expand_count != 1 {
        return Err(format!(
            "DML change-stream keyed assert requires exactly one native ChangeEventExpand node, found {expand_count}"
        ));
    }
    *plan = candidate;
    Ok(())
}

fn inject_native_keyed_assert_before_expand_node(
    node: &mut crate::sql::planner::distributed::DistributedNode,
    keyed_assert: &DmlPreExpandKeyedAssert,
    next_node_id: i32,
    expand_count: &mut usize,
) -> Result<(), String> {
    for child in &mut node.children {
        inject_native_keyed_assert_before_expand_node(
            child,
            keyed_assert,
            next_node_id,
            expand_count,
        )?;
    }

    if !matches!(
        node.payload,
        crate::sql::planner::distributed::DistributedNodeKind::ChangeEventExpand(_)
    ) {
        return Ok(());
    }

    *expand_count += 1;
    if node.children.len() != 1 {
        return Err(format!(
            "DML change-stream native ChangeEventExpand node_id={} expected one child, got {}",
            node.node_id,
            node.children.len()
        ));
    }

    let key_column_id = find_native_key_column_id_for_pre_expand_assert(node, keyed_assert)?;
    let original_child = node.children.pop().expect("validated single child");
    let assert_node = crate::sql::planner::distributed::DistributedNode {
        node_id: next_node_id,
        fragment_id: original_child.fragment_id,
        tuple_ids: original_child.tuple_ids.clone(),
        nullable_tuple_ids: original_child.nullable_tuple_ids.clone(),
        limit: -1,
        build_runtime_filters: vec![],
        probe_runtime_filters: vec![],
        children: vec![original_child],
        stats: node.stats.clone(),
        payload: crate::sql::planner::distributed::DistributedNodeKind::AssertOneRow(
            crate::sql::planner::payload::PlanAssertOneRowNode::per_key_at_most_one(
                "DML change-stream matched row uniqueness",
                vec![key_column_id],
                vec![keyed_assert.key_label.clone()],
                keyed_assert.message_prefix.clone(),
            ),
        ),
    };
    node.children.push(assert_node);
    Ok(())
}

fn find_native_key_column_id_for_pre_expand_assert(
    expand_node: &crate::sql::planner::distributed::DistributedNode,
    keyed_assert: &DmlPreExpandKeyedAssert,
) -> Result<crate::sql::column_id::ColumnId, String> {
    let child = expand_node.children.first().ok_or_else(|| {
        format!(
            "DML change-stream native ChangeEventExpand node_id={} missing child",
            expand_node.node_id
        )
    })?;
    match find_output_column_id_by_name(child, &keyed_assert.key_column_name) {
        Ok(column_id) => Ok(column_id),
        Err(name_err) if can_derive_key_from_row_id_assignment(keyed_assert) => {
            if let Some(column_id) = find_native_key_column_id_from_change_event_assignment(
                expand_node,
                child,
                keyed_assert,
            )? {
                Ok(column_id)
            } else {
                Err(name_err)
            }
        }
        Err(err) => Err(err),
    }
}

fn can_derive_key_from_row_id_assignment(keyed_assert: &DmlPreExpandKeyedAssert) -> bool {
    keyed_assert
        .key_column_name
        .eq_ignore_ascii_case("__nr_row_id")
        && keyed_assert
            .key_label
            .eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_ROW_ID_COL)
}

fn find_native_key_column_id_from_change_event_assignment(
    expand_node: &crate::sql::planner::distributed::DistributedNode,
    child: &crate::sql::planner::distributed::DistributedNode,
    keyed_assert: &DmlPreExpandKeyedAssert,
) -> Result<Option<crate::sql::column_id::ColumnId>, String> {
    let crate::sql::planner::distributed::DistributedNodeKind::ChangeEventExpand(expand) =
        &expand_node.payload
    else {
        return Ok(None);
    };
    let mut output_columns = expand
        .output_columns
        .iter()
        .filter(|column| column.name.eq_ignore_ascii_case(&keyed_assert.key_label));
    let Some(output_column) = output_columns.next() else {
        return Ok(None);
    };
    if output_columns.next().is_some() {
        return Err(format!(
            "DML change-stream native keyed assert output column `{}` is ambiguous",
            keyed_assert.key_label
        ));
    }

    let mut key_column_id = None;
    for event in &expand.events {
        for assignment in &event.assignments {
            if assignment.output_column_id != output_column.column_id {
                continue;
            }
            let Some(expr) = assignment.expr.as_ref() else {
                continue;
            };
            let crate::sql::analysis::ExprKind::ColumnRef { column_id, .. } = &expr.kind else {
                continue;
            };
            validate_unique_column_in_native_output_scope(child, *column_id)?;
            if let Some(previous) = key_column_id {
                if previous != *column_id {
                    return Err(format!(
                        "DML change-stream native keyed assert output `{}` is assigned from multiple child columns: {:?} and {:?}",
                        keyed_assert.key_label, previous, column_id
                    ));
                }
            } else {
                key_column_id = Some(*column_id);
            }
        }
    }
    Ok(key_column_id)
}

fn validate_unique_column_in_native_output_scope(
    node: &crate::sql::planner::distributed::DistributedNode,
    column_id: crate::sql::column_id::ColumnId,
) -> Result<(), String> {
    let output_column_ids = native_node_output_column_ids(node)?;
    match output_column_ids
        .iter()
        .filter(|candidate| **candidate == column_id)
        .count()
    {
        1 => Ok(()),
        0 => Err(format!(
            "DML change-stream native keyed assert assignment ColumnId({}) is not in direct child output scope",
            column_id.0
        )),
        count => Err(format!(
            "DML change-stream native keyed assert assignment ColumnId({}) is ambiguous in direct child output scope ({count} bindings)",
            column_id.0
        )),
    }
}

fn native_node_output_column_ids(
    node: &crate::sql::planner::distributed::DistributedNode,
) -> Result<Vec<crate::sql::column_id::ColumnId>, String> {
    use crate::sql::planner::distributed::DistributedNodeKind;

    let ids = |columns: &[crate::sql::analysis::OutputColumn]| {
        columns
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>()
    };
    match &node.payload {
        DistributedNodeKind::Exchange(exchange) => Ok(ids(&exchange.output_columns)),
        DistributedNodeKind::Scan(scan) => {
            if scan
                .required_columns
                .as_ref()
                .is_none_or(|columns| columns.is_empty())
            {
                return Ok(ids(&scan.columns));
            }
            let required = scan
                .required_columns
                .as_ref()
                .expect("required columns presence was checked")
                .iter()
                .map(|name| name.to_ascii_lowercase())
                .collect::<std::collections::HashSet<_>>();
            Ok(scan
                .columns
                .iter()
                .filter(|column| required.contains(&column.name.to_ascii_lowercase()))
                .map(|column| column.column_id)
                .collect())
        }
        DistributedNodeKind::Project(project) => Ok(project
            .items
            .iter()
            .map(|item| item.output_column_id)
            .collect()),
        DistributedNodeKind::Sort(sort) => {
            if sort.output_columns.is_empty() {
                native_unary_passthrough_output_column_ids(node, "Sort")
            } else {
                Ok(ids(&sort.output_columns))
            }
        }
        DistributedNodeKind::Values(values) => Ok(ids(&values.columns)),
        DistributedNodeKind::Repeat(repeat) => {
            let mut output = native_unary_passthrough_output_column_ids(node, "Repeat")?;
            output.extend(
                repeat
                    .grouping_fn_ids
                    .iter()
                    .map(|(_, column_id)| *column_id),
            );
            Ok(output)
        }
        DistributedNodeKind::Window(window) => Ok(ids(&window.output_columns)),
        DistributedNodeKind::GenerateSeries(generate_series) => {
            Ok(vec![generate_series.output_column_id])
        }
        DistributedNodeKind::TableFunction(table_function) => {
            let mut output = native_unary_passthrough_output_column_ids(node, "TableFunction")?;
            output.extend(ids(&table_function.output_columns));
            Ok(output)
        }
        DistributedNodeKind::HashAggregate(aggregate) => {
            if aggregate.output_columns.is_empty() {
                Ok(ids(&aggregate.output_layout.full_output_columns()))
            } else {
                Ok(ids(&aggregate.output_columns))
            }
        }
        DistributedNodeKind::HashJoin(join) => {
            native_join_output_column_ids(join.join_type, &join.output_columns, &node.children)
        }
        DistributedNodeKind::NestLoopJoin(join) => {
            native_join_output_column_ids(join.join_type, &join.output_columns, &node.children)
        }
        DistributedNodeKind::SetOp(set_op) => Ok(ids(&set_op.output_columns)),
        DistributedNodeKind::ChangeEventExpand(expand) => Ok(ids(&expand.output_columns)),
        DistributedNodeKind::Filter(_)
        | DistributedNodeKind::AssertOneRow(_)
        | DistributedNodeKind::TopN(_) => {
            native_unary_passthrough_output_column_ids(node, "passthrough")
        }
    }
}

fn native_unary_passthrough_output_column_ids(
    node: &crate::sql::planner::distributed::DistributedNode,
    node_kind: &str,
) -> Result<Vec<crate::sql::column_id::ColumnId>, String> {
    let [child] = node.children.as_slice() else {
        return Err(format!(
            "DML change-stream native keyed assert {node_kind} child expected one child for output scope, got {}",
            node.children.len()
        ));
    };
    native_node_output_column_ids(child)
}

fn native_join_output_column_ids(
    join_type: crate::sql::analysis::JoinKind,
    declared: &[crate::sql::analysis::OutputColumn],
    children: &[crate::sql::planner::distributed::DistributedNode],
) -> Result<Vec<crate::sql::column_id::ColumnId>, String> {
    let child_ids = |index: usize| -> Result<Vec<_>, String> {
        children
            .get(index)
            .ok_or_else(|| {
                format!(
                    "DML change-stream native keyed assert join child {index} is missing from output scope"
                )
            })
            .and_then(native_node_output_column_ids)
    };
    let derived = match join_type {
        crate::sql::analysis::JoinKind::LeftSemi
        | crate::sql::analysis::JoinKind::LeftAnti
        | crate::sql::analysis::JoinKind::NullAwareLeftAnti => child_ids(0)?,
        crate::sql::analysis::JoinKind::RightSemi | crate::sql::analysis::JoinKind::RightAnti => {
            child_ids(1)?
        }
        crate::sql::analysis::JoinKind::Inner
        | crate::sql::analysis::JoinKind::Cross
        | crate::sql::analysis::JoinKind::LeftOuter
        | crate::sql::analysis::JoinKind::RightOuter
        | crate::sql::analysis::JoinKind::FullOuter => {
            let mut output = child_ids(0)?;
            output.extend(child_ids(1)?);
            output
        }
    };
    let declared_ids = declared
        .iter()
        .map(|column| column.column_id)
        .collect::<Vec<_>>();
    if declared_ids.is_empty() || declared_ids != derived {
        Ok(derived)
    } else {
        Ok(declared_ids)
    }
}

fn next_native_node_id(
    plan: &crate::sql::planner::distributed::DistributedPlan,
) -> Result<i32, String> {
    plan.fragments
        .iter()
        .flat_map(|fragment| native_node_ids(&fragment.root))
        .max()
        .unwrap_or_default()
        .checked_add(1)
        .ok_or_else(|| {
            "DML change-stream keyed assert cannot allocate a native node id after i32::MAX"
                .to_string()
        })
}

fn native_node_ids(node: &crate::sql::planner::distributed::DistributedNode) -> Vec<i32> {
    let mut ids = vec![node.node_id];
    for child in &node.children {
        ids.extend(native_node_ids(child));
    }
    ids
}

fn find_output_column_id_by_name(
    node: &crate::sql::planner::distributed::DistributedNode,
    column_name: &str,
) -> Result<crate::sql::column_id::ColumnId, String> {
    if let crate::sql::planner::distributed::DistributedNodeKind::Project(project) = &node.payload {
        let mut matches = project
            .items
            .iter()
            .filter(|item| item.output_name.eq_ignore_ascii_case(column_name));
        let item = matches.next().ok_or_else(|| {
            format!(
                "DML change-stream keyed assert column `{column_name}` not found in native Project child"
            )
        })?;
        if matches.next().is_some() {
            return Err(format!(
                "DML change-stream keyed assert column `{column_name}` is ambiguous in native Project child"
            ));
        }
        return Ok(item.output_column_id);
    }

    let columns = native_node_output_columns(node).ok_or_else(|| {
        format!(
            "DML change-stream keyed assert cannot infer output columns for native node {}",
            node.node_id
        )
    })?;
    let mut matches = columns
        .iter()
        .filter(|column| column.name.eq_ignore_ascii_case(column_name));
    let column = matches.next().ok_or_else(|| {
        format!("DML change-stream keyed assert column `{column_name}` not found in native child")
    })?;
    if matches.next().is_some() {
        return Err(format!(
            "DML change-stream keyed assert column `{column_name}` is ambiguous in native child"
        ));
    }
    Ok(column.column_id)
}

fn native_node_output_columns(
    node: &crate::sql::planner::distributed::DistributedNode,
) -> Option<&[crate::sql::analysis::OutputColumn]> {
    match &node.payload {
        crate::sql::planner::distributed::DistributedNodeKind::Exchange(exchange) => {
            Some(&exchange.output_columns)
        }
        crate::sql::planner::distributed::DistributedNodeKind::Scan(scan) => Some(&scan.columns),
        crate::sql::planner::distributed::DistributedNodeKind::Sort(sort) => {
            Some(&sort.output_columns)
        }
        crate::sql::planner::distributed::DistributedNodeKind::Values(values) => {
            Some(&values.columns)
        }
        crate::sql::planner::distributed::DistributedNodeKind::Window(window) => {
            Some(&window.output_columns)
        }
        crate::sql::planner::distributed::DistributedNodeKind::TableFunction(table_function) => {
            Some(&table_function.output_columns)
        }
        crate::sql::planner::distributed::DistributedNodeKind::HashAggregate(aggregate) => {
            Some(&aggregate.output_columns)
        }
        crate::sql::planner::distributed::DistributedNodeKind::SetOp(set_op) => {
            Some(&set_op.output_columns)
        }
        crate::sql::planner::distributed::DistributedNodeKind::ChangeEventExpand(expand) => {
            Some(&expand.output_columns)
        }
        crate::sql::planner::distributed::DistributedNodeKind::Filter(_)
        | crate::sql::planner::distributed::DistributedNodeKind::Project(_)
        | crate::sql::planner::distributed::DistributedNodeKind::AssertOneRow(_)
        | crate::sql::planner::distributed::DistributedNodeKind::TopN(_)
        | crate::sql::planner::distributed::DistributedNodeKind::HashJoin(_)
        | crate::sql::planner::distributed::DistributedNodeKind::NestLoopJoin(_)
        | crate::sql::planner::distributed::DistributedNodeKind::Repeat(_)
        | crate::sql::planner::distributed::DistributedNodeKind::GenerateSeries(_) => {
            node.children.first().and_then(native_node_output_columns)
        }
    }
}

fn dml_change_stream_write_execution(
    result: CoordinatedQueryResult,
    commit_plan: crate::engine::iceberg_change_stream_write::ChangeStreamWriterCommitPlan,
) -> Result<DmlChangeStreamWriteExecution, String> {
    if let Some(abort) = result.write_abort.as_ref() {
        return Err(abort.reason.clone());
    }
    if result.write_commit.is_none() {
        return Err("DML change-stream write completed without writer commit".to_string());
    }
    Ok(DmlChangeStreamWriteExecution {
        result,
        commit_plan,
    })
}

fn target_partition_source_column_names(
    metadata: &iceberg::spec::TableMetadata,
) -> Result<Vec<String>, String> {
    let schema = metadata.current_schema();
    metadata
        .default_partition_spec()
        .fields()
        .iter()
        .map(|field| {
            let source = schema.field_by_id(field.source_id).ok_or_else(|| {
                format!(
                    "DML change-stream partition source field id {} not found in target schema",
                    field.source_id
                )
            })?;
            Ok(source.name.clone())
        })
        .collect()
}

fn target_partition_source_ordinals(
    output_columns: &[OutputColumn],
    source_columns: &[String],
) -> Result<Vec<usize>, String> {
    source_columns
        .iter()
        .map(|name| {
            output_ordinal_by_name(
                output_columns,
                name,
                "target partition source column",
                OutputBindingKind::UserVisible,
            )
        })
        .collect()
}

fn output_ordinals_for_sink_columns(
    output_columns: &[OutputColumn],
    sink_columns: &[crate::engine::catalog::ColumnDef],
) -> Result<Vec<usize>, String> {
    sink_columns
        .iter()
        .map(|column| {
            output_ordinal_by_name(
                output_columns,
                &column.name,
                "sink input column",
                binding_kind_for_sink_column(&column.name),
            )
        })
        .collect()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OutputBindingKind {
    Internal,
    UserVisible,
}

fn binding_kind_for_sink_column(name: &str) -> OutputBindingKind {
    if is_reserved_internal_output_name(name) {
        OutputBindingKind::Internal
    } else {
        OutputBindingKind::UserVisible
    }
}

fn is_reserved_internal_output_name(name: &str) -> bool {
    name.eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_FILE_PATH_COL)
        || name.eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_ROW_POS_COL)
        || name.eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_ROW_ID_COL)
        || name.eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL)
        || name.eq_ignore_ascii_case(crate::exec::change_op::CHANGE_OP_COLUMN)
        || name.eq_ignore_ascii_case(DML_CHANGE_STREAM_DATA_ROUTE_COLUMN)
}

fn output_ordinal_by_name(
    output_columns: &[OutputColumn],
    name: &str,
    label: &str,
    binding_kind: OutputBindingKind,
) -> Result<usize, String> {
    let mut matches = output_columns
        .iter()
        .enumerate()
        .filter(|(_, column)| column.name.eq_ignore_ascii_case(name));
    let (ordinal, column) = matches
        .next()
        .ok_or_else(|| format!("DML change-stream {label} `{name}` not found in plan output"))?;
    if matches.next().is_some() {
        return Err(format!(
            "DML change-stream {label} `{name}` is ambiguous in plan output"
        ));
    }
    match binding_kind {
        OutputBindingKind::Internal if !column.is_internal => {
            return Err(format!(
                "DML change-stream {label} `{name}` must be marked internal in plan output"
            ));
        }
        OutputBindingKind::UserVisible if column.is_internal => {
            return Err(format!(
                "DML change-stream {label} `{name}` must be user-visible in plan output"
            ));
        }
        OutputBindingKind::Internal | OutputBindingKind::UserVisible => {}
    }
    Ok(ordinal)
}

#[cfg(test)]
mod native_mutation_tests {
    use super::*;

    struct EmptyCatalog;

    impl crate::sql::catalog::CatalogProvider for EmptyCatalog {
        fn get_table(
            &self,
            database: &str,
            table: &str,
        ) -> Result<crate::sql::catalog::TableDef, String> {
            Err(format!("unexpected table lookup {database}.{table}"))
        }
    }

    fn output() -> crate::sql::analysis::OutputColumn {
        output_column(1, "__nr_row_id")
    }

    fn output_column(id: u32, name: &str) -> crate::sql::analysis::OutputColumn {
        crate::sql::analysis::OutputColumn {
            column_id: crate::sql::column_id::ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            is_internal: true,
        }
    }

    fn column_ref(id: u32, name: &str) -> crate::sql::analysis::TypedExpr {
        crate::sql::analysis::TypedExpr {
            kind: crate::sql::analysis::ExprKind::ColumnRef {
                column_id: crate::sql::column_id::ColumnId::new_for_test(id),
                qualifier: None,
                column: name.to_string(),
            },
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
        }
    }

    fn stats() -> crate::sql::planner::physical::PhysicalPlanStats {
        crate::sql::planner::physical::PhysicalPlanStats {
            output_row_count: 1.0,
            row_count_confidence: crate::sql::planner::physical::PlannerConfidence::Exact,
            column_statistics: Default::default(),
            cost_estimate: None,
            broadcast_decision: None,
        }
    }

    fn native_change_event_expand_plan() -> crate::sql::planner::distributed::DistributedPlan {
        let child = crate::sql::planner::distributed::DistributedNode {
            node_id: 1,
            fragment_id: 0,
            tuple_ids: vec![1],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: Vec::new(),
            stats: stats(),
            payload: crate::sql::planner::distributed::DistributedNodeKind::Values(
                crate::sql::planner::payload::PlanValuesNode {
                    rows: Vec::new(),
                    columns: vec![output()],
                },
            ),
        };
        let expand = crate::sql::planner::distributed::DistributedNode {
            node_id: 2,
            fragment_id: 0,
            tuple_ids: vec![1],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: vec![child],
            stats: stats(),
            payload: crate::sql::planner::distributed::DistributedNodeKind::ChangeEventExpand(
                crate::sql::planner::physical::DistributedChangeEventExpandNode {
                    events: Vec::new(),
                    output_columns: vec![
                        output(),
                        crate::sql::analysis::OutputColumn {
                            column_id: crate::sql::column_id::ColumnId::new_for_test(2),
                            name: "__change_op".to_string(),
                            data_type: arrow::datatypes::DataType::Int32,
                            nullable: false,
                            is_internal: true,
                        },
                        crate::sql::analysis::OutputColumn {
                            column_id: crate::sql::column_id::ColumnId::new_for_test(3),
                            name: "__change_data_route".to_string(),
                            data_type: arrow::datatypes::DataType::Int32,
                            nullable: false,
                            is_internal: true,
                        },
                    ],
                    change_op_column_id: crate::sql::column_id::ColumnId::new_for_test(2),
                    data_route_column_id: Some(crate::sql::column_id::ColumnId::new_for_test(3)),
                },
            ),
        };
        crate::sql::planner::distributed::DistributedPlan {
            fragments: vec![crate::sql::planner::distributed::PlanFragment {
                fragment_id: 0,
                root: expand,
                data_partition: crate::sql::planner::distributed::DataPartition::unpartitioned(),
                output_partition: crate::sql::planner::distributed::DataPartition::unpartitioned(),
                sink: crate::sql::planner::distributed::DataSink::Result,
                output_exprs: None,
                output_columns: Vec::new(),
                cte_id: None,
                cte_exchange_nodes: Vec::new(),
            }],
            root_fragment_id: 0,
            edges: Vec::new(),
        }
    }

    fn keyed_assert() -> DmlPreExpandKeyedAssert {
        DmlPreExpandKeyedAssert {
            key_column_name: "__nr_row_id".to_string(),
            key_label: "_row_id".to_string(),
            message_prefix: "MOR UPDATE matched target row".to_string(),
        }
    }

    fn plan_snapshot(plan: &crate::sql::planner::distributed::DistributedPlan) -> String {
        format!("{plan:#?}")
    }

    fn plan_node_ids(plan: &crate::sql::planner::distributed::DistributedPlan) -> Vec<i32> {
        plan.fragments
            .iter()
            .flat_map(|fragment| native_node_ids(&fragment.root))
            .collect()
    }

    fn push_second_expand_fragment(
        plan: &mut crate::sql::planner::distributed::DistributedPlan,
        child_node_id: i32,
        expand_node_id: i32,
    ) {
        let mut second = plan.fragments[0].clone();
        second.fragment_id = 1;
        second.root.fragment_id = 1;
        second.root.node_id = expand_node_id;
        second.root.children[0].fragment_id = 1;
        second.root.children[0].node_id = child_node_id;
        second.sink = crate::sql::planner::distributed::DataSink::Noop;
        plan.fragments.push(second);
    }

    fn assignment_scope_plan(
        child_columns: Vec<crate::sql::analysis::OutputColumn>,
        assignment_column_id: u32,
    ) -> crate::sql::planner::distributed::DistributedPlan {
        let mut plan = native_change_event_expand_plan();
        let crate::sql::planner::distributed::DistributedNodeKind::Values(values) =
            &mut plan.fragments[0].root.children[0].payload
        else {
            panic!("fixture child must be Values");
        };
        values.columns = child_columns;

        let crate::sql::planner::distributed::DistributedNodeKind::ChangeEventExpand(expand) =
            &mut plan.fragments[0].root.payload
        else {
            panic!("fixture root must be ChangeEventExpand");
        };
        expand.output_columns[0] = output_column(10, crate::exec::row_position::ICEBERG_ROW_ID_COL);
        expand.events = vec![crate::sql::planner::physical::DistributedChangeEventSpec {
            predicate: None,
            branch_kind: crate::sql::common::ChangeStreamBranchKind::FreshData,
            assignments: vec![
                crate::sql::planner::physical::DistributedChangeEventOutputExpr {
                    output_column_id: crate::sql::column_id::ColumnId::new_for_test(10),
                    expr: Some(column_ref(assignment_column_id, "source_row_id")),
                },
            ],
        }];
        plan
    }

    fn native_assert_count(node: &crate::proto::plan::DistributedNode) -> usize {
        let here = matches!(
            node.payload.as_ref(),
            Some(crate::proto::plan::distributed_node::Payload::Physical(physical))
                if matches!(
                    physical.kind,
                    Some(crate::proto::plan::plan_node::Kind::AssertOneRow(_))
                )
        ) as usize;
        here + node.children.iter().map(native_assert_count).sum::<usize>()
    }

    fn native_assert(
        node: &crate::proto::plan::DistributedNode,
    ) -> Option<&crate::proto::plan::AssertOneRowNode> {
        if let Some(crate::proto::plan::distributed_node::Payload::Physical(physical)) =
            node.payload.as_ref()
            && let Some(crate::proto::plan::plan_node::Kind::AssertOneRow(assertion)) =
                physical.kind.as_ref()
        {
            return Some(assertion);
        }
        node.children.iter().find_map(native_assert)
    }

    #[test]
    fn keyed_assert_mutates_native_plan_once_before_fragment_build() {
        let mut plan = native_change_event_expand_plan();
        inject_dml_pre_expand_keyed_assert_into_native_plan(&mut plan, &keyed_assert())
            .expect("inject native keyed assert before build");

        assert_eq!(plan.fragments[0].root.node_id, 2);
        assert_eq!(plan.fragments[0].root.children[0].node_id, 3);
        assert_eq!(plan.fragments[0].root.children[0].children[0].node_id, 1);

        let build = crate::sql::codegen::fragment_builder::PlanFragmentBuilder::build(
            crate::sql::codegen::FragmentBuildRequest::result(
                &plan,
                &EmptyCatalog,
                &crate::connector::ConnectorRegistry::new(),
                None,
            ),
        )
        .expect("build mutated native plan");
        let root = build.native_fragments[&0]
            .root
            .as_ref()
            .expect("encoded native root");

        assert_eq!(native_assert_count(root), 1);
        let assertion = native_assert(root).expect("encoded native keyed assertion");
        assert_eq!(assertion.group_key_column_ids, vec![1]);
        assert_eq!(assertion.group_key_labels, vec!["_row_id"]);
        assert_eq!(
            assertion.keyed_message_prefix.as_deref(),
            Some("MOR UPDATE matched target row")
        );
        assert_eq!(
            build.native_fragments.keys().copied().collect::<Vec<_>>(),
            build
                .fragment_schedules
                .iter()
                .map(|schedule| schedule.fragment_id)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn keyed_assert_native_mutation_rejects_missing_expand_without_mutation() {
        let mut missing = native_change_event_expand_plan();
        missing.fragments[0].root = missing.fragments[0].root.children.remove(0);
        let before = plan_snapshot(&missing);
        let ids_before = plan_node_ids(&missing);
        let err =
            inject_dml_pre_expand_keyed_assert_into_native_plan(&mut missing, &keyed_assert())
                .expect_err("missing expand must fail");
        assert!(err.contains("found 0"), "{err}");
        assert_eq!(plan_snapshot(&missing), before);
        assert_eq!(plan_node_ids(&missing), ids_before);
    }

    #[test]
    fn keyed_assert_native_mutation_rejects_multiple_expands_atomically() {
        let mut multiple = native_change_event_expand_plan();
        push_second_expand_fragment(&mut multiple, 4, 5);
        let before = plan_snapshot(&multiple);
        let ids_before = plan_node_ids(&multiple);
        let err =
            inject_dml_pre_expand_keyed_assert_into_native_plan(&mut multiple, &keyed_assert())
                .expect_err("multiple expands must fail");
        assert!(err.contains("found 2"), "{err}");
        assert_eq!(plan_snapshot(&multiple), before);
        assert_eq!(plan_node_ids(&multiple), ids_before);
    }

    #[test]
    fn keyed_assert_native_mutation_rejects_later_malformed_expand_atomically() {
        let mut plan = native_change_event_expand_plan();
        push_second_expand_fragment(&mut plan, 4, 5);
        plan.fragments[1].root.children.clear();
        let before = plan_snapshot(&plan);
        let ids_before = plan_node_ids(&plan);

        let err = inject_dml_pre_expand_keyed_assert_into_native_plan(&mut plan, &keyed_assert())
            .expect_err("later malformed expand must fail");

        assert!(err.contains("node_id=5 expected one child, got 0"), "{err}");
        assert_eq!(plan_snapshot(&plan), before);
        assert_eq!(plan_node_ids(&plan), ids_before);
    }

    #[test]
    fn keyed_assert_native_mutation_rejects_exhausted_node_id_without_mutation() {
        let mut plan = native_change_event_expand_plan();
        plan.fragments[0].root.children[0].node_id = i32::MAX;
        let before = plan_snapshot(&plan);
        let ids_before = plan_node_ids(&plan);

        let err = inject_dml_pre_expand_keyed_assert_into_native_plan(&mut plan, &keyed_assert())
            .expect_err("exhausted native node id must fail");

        assert!(err.contains("cannot allocate"), "{err}");
        assert_eq!(plan_snapshot(&plan), before);
        assert_eq!(plan_node_ids(&plan), ids_before);
    }

    #[test]
    fn keyed_assert_rejects_assignment_column_outside_direct_child_scope() {
        let mut plan = assignment_scope_plan(vec![output_column(11, "payload")], 99);
        let before = plan_snapshot(&plan);

        let err = inject_dml_pre_expand_keyed_assert_into_native_plan(&mut plan, &keyed_assert())
            .expect_err("assignment column outside direct child scope must fail");

        assert!(err.contains("not in direct child output scope"), "{err}");
        assert_eq!(plan_snapshot(&plan), before);
    }

    #[test]
    fn keyed_assert_rejects_ambiguous_assignment_column_in_direct_child_scope() {
        let mut plan = assignment_scope_plan(
            vec![
                output_column(99, "left_row_id"),
                output_column(99, "right_row_id"),
            ],
            99,
        );
        let before = plan_snapshot(&plan);

        let err = inject_dml_pre_expand_keyed_assert_into_native_plan(&mut plan, &keyed_assert())
            .expect_err("ambiguous assignment column in direct child scope must fail");

        assert!(
            err.contains("ambiguous in direct child output scope"),
            "{err}"
        );
        assert_eq!(plan_snapshot(&plan), before);
    }

    #[test]
    fn keyed_assert_accepts_table_function_child_passthrough_assignment_column() {
        let mut plan = assignment_scope_plan(vec![output_column(99, "source_row_id")], 99);
        let values = plan.fragments[0].root.children.remove(0);
        plan.fragments[0]
            .root
            .children
            .push(crate::sql::planner::distributed::DistributedNode {
                node_id: 4,
                fragment_id: 0,
                tuple_ids: vec![1],
                nullable_tuple_ids: Vec::new(),
                limit: -1,
                build_runtime_filters: Vec::new(),
                probe_runtime_filters: Vec::new(),
                children: vec![values],
                stats: stats(),
                payload: crate::sql::planner::distributed::DistributedNodeKind::TableFunction(
                    crate::sql::planner::payload::PlanTableFunctionNode {
                        function_name: "unnest".to_string(),
                        args: Vec::new(),
                        output_columns: vec![output_column(12, "unnested_value")],
                        alias: None,
                        is_left_join: false,
                    },
                ),
            });

        inject_dml_pre_expand_keyed_assert_into_native_plan(&mut plan, &keyed_assert())
            .expect("table function child passthrough column belongs to direct output scope");

        let crate::sql::planner::distributed::DistributedNodeKind::AssertOneRow(assertion) =
            &plan.fragments[0].root.children[0].payload
        else {
            panic!("keyed assert must be inserted before expand");
        };
        assert_eq!(
            assertion.group_key_column_ids,
            vec![crate::sql::column_id::ColumnId::new_for_test(99)]
        );
    }

    fn wrap_expand_child_in_sort(
        plan: &mut crate::sql::planner::distributed::DistributedPlan,
        output_columns: Vec<crate::sql::analysis::OutputColumn>,
    ) {
        let values = plan.fragments[0].root.children.remove(0);
        plan.fragments[0]
            .root
            .children
            .push(crate::sql::planner::distributed::DistributedNode {
                node_id: 4,
                fragment_id: 0,
                tuple_ids: vec![1],
                nullable_tuple_ids: Vec::new(),
                limit: -1,
                build_runtime_filters: Vec::new(),
                probe_runtime_filters: Vec::new(),
                children: vec![values],
                stats: stats(),
                payload: crate::sql::planner::distributed::DistributedNodeKind::Sort(
                    crate::sql::planner::payload::PlanSortNode {
                        items: Vec::new(),
                        analytic_partition_by: Vec::new(),
                        output_columns,
                        offset: None,
                        partition_limit: None,
                        topn_type: None,
                    },
                ),
            });
    }

    #[test]
    fn keyed_assert_rejects_assignment_pruned_by_sort_output_scope_atomically() {
        let mut plan = assignment_scope_plan(vec![output_column(99, "source_row_id")], 99);
        wrap_expand_child_in_sort(&mut plan, vec![output_column(12, "sorted_only")]);
        let before = plan_snapshot(&plan);

        let err = inject_dml_pre_expand_keyed_assert_into_native_plan(&mut plan, &keyed_assert())
            .expect_err("Sort output scope must hide pruned child columns");

        assert!(err.contains("not in direct child output scope"), "{err}");
        assert_eq!(plan_snapshot(&plan), before);
    }

    #[test]
    fn keyed_assert_accepts_assignment_in_reordered_sort_output_scope() {
        let mut plan = assignment_scope_plan(
            vec![
                output_column(99, "source_row_id"),
                output_column(12, "payload"),
            ],
            99,
        );
        wrap_expand_child_in_sort(
            &mut plan,
            vec![
                output_column(12, "payload"),
                output_column(99, "source_row_id"),
            ],
        );

        inject_dml_pre_expand_keyed_assert_into_native_plan(&mut plan, &keyed_assert())
            .expect("reordered Sort output retains the assignment column");
    }

    #[test]
    fn keyed_assert_accepts_empty_sort_output_as_passthrough_scope() {
        let mut plan = assignment_scope_plan(vec![output_column(99, "source_row_id")], 99);
        wrap_expand_child_in_sort(&mut plan, Vec::new());

        inject_dml_pre_expand_keyed_assert_into_native_plan(&mut plan, &keyed_assert())
            .expect("empty Sort output is a passthrough scope");
    }
}

#[cfg(all(test, feature = "compat"))]
mod tests {
    use super::*;

    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use crate::sql::common::ChangeStreamBranchKind;

    fn output_column(name: &str, ordinal: u32) -> crate::sql::analysis::OutputColumn {
        output_column_with_internal(name, ordinal, name.starts_with('_'))
    }

    fn output_column_with_internal(
        name: &str,
        ordinal: u32,
        is_internal: bool,
    ) -> crate::sql::analysis::OutputColumn {
        crate::sql::analysis::OutputColumn {
            column_id: crate::sql::column_id::ColumnId::new_for_test(ordinal + 1),
            name: name.to_string(),
            data_type: DataType::Int32,
            nullable: false,
            is_internal,
        }
    }

    fn producer_output_columns() -> Vec<crate::sql::analysis::OutputColumn> {
        vec![
            output_column(crate::exec::row_position::ICEBERG_FILE_PATH_COL, 0),
            output_column(crate::exec::row_position::ICEBERG_ROW_POS_COL, 1),
            output_column("region", 2),
            output_column("id", 3),
            output_column(crate::exec::row_position::ICEBERG_ROW_ID_COL, 4),
            output_column(crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL, 5),
            output_column(crate::exec::change_op::CHANGE_OP_COLUMN, 6),
            output_column("__change_data_route", 7),
        ]
    }

    fn column(name: &str) -> crate::engine::catalog::ColumnDef {
        crate::engine::catalog::ColumnDef {
            name: name.to_string(),
            data_type: DataType::Int32,
            nullable: false,
            write_default: None,
            logical_type: None,
        }
    }

    fn sink_specs_for_partitioned_target() -> DmlChangeStreamWriteBranchSinkSpecs {
        let mut delete_dv =
            crate::sql::planner::distributed::write::sink::test_support::simple_sink_spec();
        delete_dv.mode =
            crate::sql::planner::distributed::write::sink::IcebergWriteSinkMode::DeletionVectors;
        delete_dv.target_columns = vec![
            column(crate::exec::row_position::ICEBERG_FILE_PATH_COL),
            column(crate::exec::row_position::ICEBERG_ROW_POS_COL),
            column("region"),
        ];

        let mut reuse_data =
            crate::sql::planner::distributed::write::sink::test_support::simple_sink_spec();
        reuse_data.mode =
            crate::sql::planner::distributed::write::sink::IcebergWriteSinkMode::RowLineageData;
        reuse_data.target_columns = vec![
            column("id"),
            column("region"),
            column(crate::exec::row_position::ICEBERG_ROW_ID_COL),
            column(crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL),
        ];

        let mut fresh_data =
            crate::sql::planner::distributed::write::sink::test_support::simple_sink_spec();
        fresh_data.mode = crate::sql::planner::distributed::write::sink::IcebergWriteSinkMode::Data;
        fresh_data.target_columns = vec![column("id"), column("region")];

        DmlChangeStreamWriteBranchSinkSpecs {
            delete_dv: Some(delete_dv),
            reuse_data: Some(reuse_data),
            fresh_data: Some(fresh_data),
            target_partition_source_columns: vec!["region".to_string()],
        }
    }

    fn sink_specs_for_unpartitioned_target() -> DmlChangeStreamWriteBranchSinkSpecs {
        DmlChangeStreamWriteBranchSinkSpecs {
            target_partition_source_columns: Vec::new(),
            ..sink_specs_for_partitioned_target()
        }
    }

    fn branch_kinds(
        dag: &crate::sql::planner::distributed::write::change_stream::ChangeStreamWriteDagSpec,
    ) -> Vec<ChangeStreamBranchKind> {
        dag.branches
            .iter()
            .map(|branch| branch.branch_kind)
            .collect()
    }

    fn physical_values_plan_for_execution_test() -> crate::sql::optimizer::OptimizedOperatorNode {
        use crate::sql::column_id::ColumnId;
        use crate::sql::optimizer::operator::{Operator, ValuesOp};
        use crate::sql::optimizer::optimized_tree::{
            OptimizedOperatorNode, OptimizerExplainStats, PlanExecutionProps, attach_scalar_arena,
        };
        use crate::sql::optimizer::scalar::ScalarArena;
        use crate::sql::optimizer::statistics::Statistics;

        let output_columns = vec![
            crate::sql::analysis::OutputColumn {
                column_id: ColumnId::new_for_test(1),
                name: crate::exec::change_op::CHANGE_OP_COLUMN.to_string(),
                data_type: DataType::Int32,
                nullable: false,
                is_internal: true,
            },
            crate::sql::analysis::OutputColumn {
                column_id: ColumnId::new_for_test(2),
                name: "__change_data_route".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                is_internal: true,
            },
            crate::sql::analysis::OutputColumn {
                column_id: ColumnId::new_for_test(3),
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                is_internal: false,
            },
        ];
        let mut physical_plan = OptimizedOperatorNode {
            op: Operator::PhysicalValues(ValuesOp {
                rows: Vec::new(),
                columns: output_columns.clone(),
            }),
            children: Vec::new(),
            stats: Statistics {
                output_row_count: 0.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            explain_stats: OptimizerExplainStats::default(),
            output_columns,
            execution_props: PlanExecutionProps::default(),
        };
        attach_scalar_arena(&mut physical_plan, Arc::new(ScalarArena::new()));
        physical_plan
    }

    fn execution_test_plan() -> DmlChangeStreamWritePlan {
        let mut branch = crate::sql::planner::distributed::write::change_stream::ChangeStreamWriteBranchSpec::reuse_data_for_test(vec![2]);
        branch.output_partition_ordinals = Vec::new();
        DmlChangeStreamWritePlan {
            producer: physical_values_plan_for_execution_test(),
            dag: crate::sql::planner::distributed::write::change_stream::ChangeStreamWriteDagSpec::for_test(
                Some(0),
                Some(1),
                vec![branch],
            ),
            pre_expand_keyed_assert: None,
        }
    }

    fn target_for_execution_test() -> crate::engine::backend_resolver::TargetBackend {
        crate::engine::backend_resolver::TargetBackend {
            backend_name: "iceberg",
            catalog: "test_catalog".to_string(),
            namespace: "default".to_string(),
            table: "target_orders".to_string(),
        }
    }

    fn keyed_assert_for_test() -> DmlPreExpandKeyedAssert {
        DmlPreExpandKeyedAssert {
            key_column_name: "__nr_row_id".to_string(),
            key_label: "_row_id".to_string(),
            message_prefix: "MOR UPDATE matched target row".to_string(),
        }
    }

    #[test]
    fn execution_return_type_carries_commit_plan() {
        let execution = DmlChangeStreamWriteExecution {
            result: CoordinatedQueryResult {
                query_result: crate::runtime::query_result::QueryResult::empty(),
                write_commit: Some(crate::runtime::write_coordinator::WriteCommitInput {
                    write_id: crate::common::types::UniqueId { hi: 1, lo: 2 },
                    writers: Vec::new(),
                }),
                write_abort: None,
                fragment_profiles: Vec::new(),
            },
            commit_plan:
                crate::engine::iceberg_change_stream_write::ChangeStreamWriterCommitPlan::new(
                    BTreeMap::new(),
                ),
        };

        assert!(execution.result.write_commit.is_some());
        assert!(execution.commit_plan.is_empty());
    }

    fn empty_writer_commit_for_test() -> crate::runtime::write_coordinator::WriteCommitInput {
        crate::runtime::write_coordinator::WriteCommitInput {
            write_id: crate::common::types::UniqueId { hi: 1, lo: 2 },
            writers: Vec::new(),
        }
    }

    fn empty_writer_result_for_test() -> CoordinatedQueryResult {
        CoordinatedQueryResult {
            query_result: crate::runtime::query_result::QueryResult::empty(),
            write_commit: Some(empty_writer_commit_for_test()),
            write_abort: None,
            fragment_profiles: Vec::new(),
        }
    }

    fn commit_plan_for_branches(
        branches: &[(i32, ChangeStreamBranchKind)],
    ) -> crate::engine::iceberg_change_stream_write::ChangeStreamWriterCommitPlan {
        crate::engine::iceberg_change_stream_write::ChangeStreamWriterCommitPlan::new(
            branches.iter().copied().collect(),
        )
    }

    #[test]
    fn update_mor_zero_rows_accepts_eos_without_branch_writer_reports() {
        let output_columns = producer_output_columns();
        let dag = build_dml_change_stream_dag_from_sink_specs(
            DmlChangeStreamBranchSet::UpdateMor,
            &output_columns,
            sink_specs_for_partitioned_target(),
        )
        .expect("update MOR change-stream DAG");
        assert_eq!(
            branch_kinds(&dag),
            vec![
                ChangeStreamBranchKind::DeleteDv,
                ChangeStreamBranchKind::ReuseData,
            ]
        );

        let execution = dml_change_stream_write_execution(
            empty_writer_result_for_test(),
            commit_plan_for_branches(&[
                (10, ChangeStreamBranchKind::DeleteDv),
                (11, ChangeStreamBranchKind::ReuseData),
            ]),
        )
        .expect("zero-row UPDATE should complete with query-level EOS");

        assert_eq!(
            execution.result.write_commit.expect("commit").writers.len(),
            0
        );
    }

    #[test]
    fn merge_matched_update_zero_rows_accepts_eos_without_branch_writer_reports() {
        let output_columns = producer_output_columns();
        let dag = build_dml_change_stream_dag_from_sink_specs(
            DmlChangeStreamBranchSet::Merge {
                matched_update: true,
                matched_delete: false,
                not_matched_insert: false,
            },
            &output_columns,
            sink_specs_for_partitioned_target(),
        )
        .expect("matched update DAG");
        assert_eq!(
            branch_kinds(&dag),
            vec![
                ChangeStreamBranchKind::DeleteDv,
                ChangeStreamBranchKind::ReuseData,
            ]
        );

        let execution = dml_change_stream_write_execution(
            empty_writer_result_for_test(),
            commit_plan_for_branches(&[
                (10, ChangeStreamBranchKind::DeleteDv),
                (11, ChangeStreamBranchKind::ReuseData),
            ]),
        )
        .expect("zero-row MERGE matched UPDATE should complete with query-level EOS");

        assert_eq!(
            execution.result.write_commit.expect("commit").writers.len(),
            0
        );
    }

    #[test]
    fn merge_empty_not_matched_insert_commits_no_writer_files() {
        let output_columns = producer_output_columns();
        let dag = build_dml_change_stream_dag_from_sink_specs(
            DmlChangeStreamBranchSet::Merge {
                matched_update: false,
                matched_delete: false,
                not_matched_insert: true,
            },
            &output_columns,
            sink_specs_for_partitioned_target(),
        )
        .expect("not matched insert DAG");
        assert_eq!(branch_kinds(&dag), vec![ChangeStreamBranchKind::FreshData]);

        let execution = dml_change_stream_write_execution(
            empty_writer_result_for_test(),
            commit_plan_for_branches(&[(12, ChangeStreamBranchKind::FreshData)]),
        )
        .expect("empty MERGE not-matched INSERT should not require writer files");

        assert_eq!(
            execution.result.write_commit.expect("commit").writers.len(),
            0
        );
    }

    #[test]
    fn execute_dml_change_stream_write_applies_keyed_assert_before_observer() {
        let _test_guard = crate::engine::acquire_standalone_test_guard();
        let _observer = crate::engine::install_change_stream_write_test_observer(true);
        let state = Arc::new(StandaloneState::default());
        let mut plan = execution_test_plan();
        plan.pre_expand_keyed_assert = Some(keyed_assert_for_test());

        let err = execute_dml_change_stream_write(&state, &target_for_execution_test(), plan, None)
            .expect_err("assert-bearing plan must be processed before the observer short-circuit");

        assert!(
            err.contains("requires exactly one native ChangeEventExpand node, found 0"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn execute_dml_change_stream_write_rejects_missing_writer_commit() {
        let _test_guard = crate::engine::acquire_standalone_test_guard();
        let _observer = crate::engine::install_change_stream_write_test_observer(true);
        let state = Arc::new(StandaloneState::default());

        let err = execute_dml_change_stream_write(
            &state,
            &target_for_execution_test(),
            execution_test_plan(),
            None,
        )
        .expect_err("missing writer commit must fail");

        assert!(err.contains("DML change-stream write completed without writer commit"));
    }

    #[test]
    fn update_mor_change_stream_plan_declares_delete_and_reuse_branches() {
        let output_columns = producer_output_columns();
        let dag = build_dml_change_stream_dag_from_sink_specs(
            DmlChangeStreamBranchSet::UpdateMor,
            &output_columns,
            sink_specs_for_partitioned_target(),
        )
        .expect("update MOR change-stream DAG");

        assert_eq!(
            branch_kinds(&dag),
            vec![
                ChangeStreamBranchKind::DeleteDv,
                ChangeStreamBranchKind::ReuseData,
            ]
        );
        assert_eq!(dag.change_op_output_ordinal, Some(6));
        assert_eq!(dag.data_route_output_ordinal, Some(7));

        let delete_dv = dag
            .branches
            .iter()
            .find(|branch| branch.branch_kind == ChangeStreamBranchKind::DeleteDv)
            .expect("delete branch");
        assert_eq!(delete_dv.output_partition_ordinals.as_slice(), &[0][..]);

        let reuse_data = dag
            .branches
            .iter()
            .find(|branch| branch.branch_kind == ChangeStreamBranchKind::ReuseData)
            .expect("reuse branch");
        assert_eq!(reuse_data.output_partition_ordinals.as_slice(), &[2][..]);
    }

    #[test]
    fn merge_change_stream_plan_declares_only_reachable_branches() {
        let output_columns = producer_output_columns();

        let matched_delete = build_dml_change_stream_dag_from_sink_specs(
            DmlChangeStreamBranchSet::Merge {
                matched_update: false,
                matched_delete: true,
                not_matched_insert: false,
            },
            &output_columns,
            sink_specs_for_partitioned_target(),
        )
        .expect("matched delete DAG");
        assert_eq!(
            branch_kinds(&matched_delete),
            vec![ChangeStreamBranchKind::DeleteDv]
        );

        let matched_update = build_dml_change_stream_dag_from_sink_specs(
            DmlChangeStreamBranchSet::Merge {
                matched_update: true,
                matched_delete: false,
                not_matched_insert: false,
            },
            &output_columns,
            sink_specs_for_partitioned_target(),
        )
        .expect("matched update DAG");
        assert_eq!(
            branch_kinds(&matched_update),
            vec![
                ChangeStreamBranchKind::DeleteDv,
                ChangeStreamBranchKind::ReuseData,
            ]
        );

        let insert_only = build_dml_change_stream_dag_from_sink_specs(
            DmlChangeStreamBranchSet::Merge {
                matched_update: false,
                matched_delete: false,
                not_matched_insert: true,
            },
            &output_columns,
            sink_specs_for_partitioned_target(),
        )
        .expect("not matched insert DAG");
        assert_eq!(
            branch_kinds(&insert_only),
            vec![ChangeStreamBranchKind::FreshData]
        );

        let update_and_insert = build_dml_change_stream_dag_from_sink_specs(
            DmlChangeStreamBranchSet::Merge {
                matched_update: true,
                matched_delete: false,
                not_matched_insert: true,
            },
            &output_columns,
            sink_specs_for_partitioned_target(),
        )
        .expect("matched update plus not matched insert DAG");
        assert_eq!(
            branch_kinds(&update_and_insert),
            vec![
                ChangeStreamBranchKind::DeleteDv,
                ChangeStreamBranchKind::ReuseData,
                ChangeStreamBranchKind::FreshData,
            ]
        );
    }

    #[test]
    fn unpartitioned_data_branch_has_empty_partition_ordinals() {
        let output_columns = producer_output_columns();
        let dag = build_dml_change_stream_dag_from_sink_specs(
            DmlChangeStreamBranchSet::Merge {
                matched_update: false,
                matched_delete: false,
                not_matched_insert: true,
            },
            &output_columns,
            sink_specs_for_unpartitioned_target(),
        )
        .expect("unpartitioned insert-only DAG");

        let fresh_data = dag
            .branches
            .iter()
            .find(|branch| branch.branch_kind == ChangeStreamBranchKind::FreshData)
            .expect("fresh branch");
        assert_eq!(
            fresh_data.output_partition_ordinals.as_slice(),
            &[] as &[usize]
        );
    }

    #[test]
    fn data_branch_requires_data_route_output_column() {
        let output_columns = producer_output_columns()
            .into_iter()
            .filter(|column| column.name != DML_CHANGE_STREAM_DATA_ROUTE_COLUMN)
            .collect::<Vec<_>>();
        let err = build_dml_change_stream_dag_from_sink_specs(
            DmlChangeStreamBranchSet::UpdateMor,
            &output_columns,
            sink_specs_for_partitioned_target(),
        )
        .expect_err("missing data route column must fail");

        assert!(err.contains("data-route column"));
        assert!(err.contains(DML_CHANGE_STREAM_DATA_ROUTE_COLUMN));
    }

    #[test]
    fn internal_route_and_file_columns_must_be_marked_internal() {
        let mut route_outputs = producer_output_columns();
        route_outputs[7] =
            output_column_with_internal(DML_CHANGE_STREAM_DATA_ROUTE_COLUMN, 7, false);
        let route_err = build_dml_change_stream_dag_from_sink_specs(
            DmlChangeStreamBranchSet::UpdateMor,
            &route_outputs,
            sink_specs_for_partitioned_target(),
        )
        .expect_err("non-internal data route column must fail");
        assert!(route_err.contains("data-route column"));
        assert!(route_err.contains("must be marked internal"));

        let mut file_outputs = producer_output_columns();
        file_outputs[0] =
            output_column_with_internal(crate::exec::row_position::ICEBERG_FILE_PATH_COL, 0, false);
        let file_err = build_dml_change_stream_dag_from_sink_specs(
            DmlChangeStreamBranchSet::UpdateMor,
            &file_outputs,
            sink_specs_for_partitioned_target(),
        )
        .expect_err("non-internal file column must fail");
        assert!(file_err.contains("delete file column"));
        assert!(file_err.contains("must be marked internal"));
    }

    #[test]
    fn user_target_sink_columns_must_not_bind_internal_outputs() {
        let mut outputs = producer_output_columns();
        outputs[3] = output_column_with_internal("id", 3, true);
        let err = build_dml_change_stream_dag_from_sink_specs(
            DmlChangeStreamBranchSet::UpdateMor,
            &outputs,
            sink_specs_for_partitioned_target(),
        )
        .expect_err("internal user target column must fail");

        assert!(err.contains("sink input column"));
        assert!(err.contains("must be user-visible"));
    }

    #[test]
    fn ambiguous_output_name_fails_fast() {
        let mut outputs = producer_output_columns();
        outputs.push(output_column("region", 8));
        let err = build_dml_change_stream_dag_from_sink_specs(
            DmlChangeStreamBranchSet::UpdateMor,
            &outputs,
            sink_specs_for_partitioned_target(),
        )
        .expect_err("duplicate output name must fail");

        assert!(err.contains("target partition source column"));
        assert!(err.contains("ambiguous"));
    }
}
