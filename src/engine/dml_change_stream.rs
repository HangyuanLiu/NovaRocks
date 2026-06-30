use std::sync::Arc;

use crate::connector::iceberg::catalog::registry::{block_on_iceberg, build_iceberg_catalog};
use crate::engine::StandaloneState;
use crate::runtime::coordinator::CoordinatedQueryResult;
use crate::sql::analysis::OutputColumn;
use crate::sql::codegen::iceberg_change_stream_write::{
    ChangeStreamWriteBranchKind, ChangeStreamWriteBranchSpec, IcebergChangeStreamWriteDagSpec,
};
use crate::sql::codegen::iceberg_write_sink::IcebergWriteSinkSpec;
use crate::sql::optimizer::OptimizerPhysicalNode;
use crate::thrift::internal_service::TQueryOptions;

pub(crate) const DML_CHANGE_STREAM_DATA_ROUTE_COLUMN: &str = "__change_data_route";

pub(crate) struct DmlChangeStreamWritePlan {
    pub(crate) producer: OptimizerPhysicalNode,
    pub(crate) dag: IcebergChangeStreamWriteDagSpec,
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
    fn branch_kinds(self) -> Vec<ChangeStreamWriteBranchKind> {
        match self {
            Self::UpdateMor => vec![
                ChangeStreamWriteBranchKind::DeleteDv,
                ChangeStreamWriteBranchKind::ReuseData,
            ],
            Self::Merge {
                matched_update,
                matched_delete,
                not_matched_insert,
            } => {
                let mut branches = Vec::with_capacity(3);
                if matched_update || matched_delete {
                    branches.push(ChangeStreamWriteBranchKind::DeleteDv);
                }
                if matched_update {
                    branches.push(ChangeStreamWriteBranchKind::ReuseData);
                }
                if not_matched_insert {
                    branches.push(ChangeStreamWriteBranchKind::FreshData);
                }
                branches
            }
        }
    }
}

pub(crate) fn build_dml_change_stream_write_plan(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    producer: OptimizerPhysicalNode,
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
    if branch_kinds.contains(&ChangeStreamWriteBranchKind::DeleteDv) {
        sink_specs.delete_dv = Some(
            crate::engine::mutation_flow::build_mor_deletion_vector_sink_spec(
                target, &resolved, &table, &entry, target_ref,
            )?,
        );
    }
    if branch_kinds.contains(&ChangeStreamWriteBranchKind::ReuseData) {
        sink_specs.reuse_data = Some(
            crate::engine::iceberg_writer::build_row_lineage_data_sink_spec(
                target, &resolved, &table, &entry,
            )?,
        );
    }
    if branch_kinds.contains(&ChangeStreamWriteBranchKind::FreshData) {
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
    Ok(DmlChangeStreamWritePlan { producer, dag })
}

fn build_dml_change_stream_dag_from_sink_specs(
    branch_set: DmlChangeStreamBranchSet,
    producer_output_columns: &[OutputColumn],
    mut sink_specs: DmlChangeStreamWriteBranchSinkSpecs,
) -> Result<IcebergChangeStreamWriteDagSpec, String> {
    let branch_kinds = branch_set.branch_kinds();
    if branch_kinds.is_empty() {
        return Err("DML change-stream write requires at least one branch".to_string());
    }
    let has_data_branch = branch_kinds.iter().any(|kind| {
        matches!(
            kind,
            ChangeStreamWriteBranchKind::ReuseData | ChangeStreamWriteBranchKind::FreshData
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
            ChangeStreamWriteBranchKind::DeleteDv => {
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
            ChangeStreamWriteBranchKind::ReuseData => {
                let sink_spec = sink_specs.reuse_data.take().ok_or_else(|| {
                    "DML change-stream ReuseData sink spec is missing".to_string()
                })?;
                (sink_spec, data_partition_ordinals.clone())
            }
            ChangeStreamWriteBranchKind::FreshData => {
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
            stream_output_slots: Vec::new(),
            stream_output_ordinals: Some(stream_output_ordinals),
            output_partition: unpartitioned_change_stream_output(),
            output_partition_ordinals: Some(output_partition_ordinals),
            sink_spec,
            writer_fragment_id: None,
        });
    }

    let mut dag = IcebergChangeStreamWriteDagSpec {
        change_op_slot: -1,
        change_op_output_ordinal: Some(change_op_output_ordinal),
        data_route_slot: None,
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
    query_opts: Option<&TQueryOptions>,
) -> Result<DmlChangeStreamWriteExecution, String> {
    let planned = crate::engine::build_physical_plan_as_iceberg_change_stream_write(
        state,
        Some(&target.catalog),
        &target.namespace,
        &plan.producer,
        &mut plan.dag,
        None,
    )?;
    let crate::engine::PlannedIcebergChangeStreamWrite {
        build_result,
        commit_plan,
    } = planned;
    #[cfg(test)]
    if let Some(result) = crate::engine::observe_change_stream_write_build_for_test(&plan.dag) {
        return dml_change_stream_write_execution(result, commit_plan);
    }
    let result = crate::engine::execute_planned_iceberg_change_stream_write(
        build_result,
        query_opts.cloned(),
    )?;
    dml_change_stream_write_execution(result, commit_plan)
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

fn unpartitioned_change_stream_output() -> crate::thrift::partitions::TDataPartition {
    crate::thrift::partitions::TDataPartition::new(
        crate::thrift::partitions::TPartitionType::UNPARTITIONED,
        None::<Vec<crate::thrift::exprs::TExpr>>,
        None::<Vec<crate::thrift::partitions::TRangePartition>>,
        None::<Vec<crate::thrift::partitions::TBucketProperty>>,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use crate::sql::codegen::iceberg_change_stream_write::ChangeStreamWriteBranchKind;

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
            crate::sql::codegen::iceberg_write_sink::test_support::simple_sink_spec();
        delete_dv.mode =
            crate::sql::codegen::iceberg_write_sink::IcebergWriteSinkMode::DeletionVectors;
        delete_dv.target_columns = vec![
            column(crate::exec::row_position::ICEBERG_FILE_PATH_COL),
            column(crate::exec::row_position::ICEBERG_ROW_POS_COL),
            column("region"),
        ];

        let mut reuse_data =
            crate::sql::codegen::iceberg_write_sink::test_support::simple_sink_spec();
        reuse_data.mode =
            crate::sql::codegen::iceberg_write_sink::IcebergWriteSinkMode::RowLineageData;
        reuse_data.target_columns = vec![
            column("id"),
            column("region"),
            column(crate::exec::row_position::ICEBERG_ROW_ID_COL),
            column(crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL),
        ];

        let mut fresh_data =
            crate::sql::codegen::iceberg_write_sink::test_support::simple_sink_spec();
        fresh_data.mode = crate::sql::codegen::iceberg_write_sink::IcebergWriteSinkMode::Data;
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
        dag: &crate::sql::codegen::iceberg_change_stream_write::IcebergChangeStreamWriteDagSpec,
    ) -> Vec<ChangeStreamWriteBranchKind> {
        dag.branches
            .iter()
            .map(|branch| branch.branch_kind)
            .collect()
    }

    fn physical_values_plan_for_execution_test() -> crate::sql::optimizer::OptimizerPhysicalNode {
        use crate::sql::column_id::ColumnId;
        use crate::sql::optimizer::operator::{Operator, ValuesOp};
        use crate::sql::optimizer::physical_tree::{
            OptimizerPhysicalNode, PlanExecutionProps, attach_scalar_arena,
        };
        use crate::sql::optimizer::scalar::ScalarArena;
        use crate::sql::optimizer::statistics::Statistics;

        let output_column = crate::sql::analysis::OutputColumn {
            column_id: ColumnId::new_for_test(3),
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
            is_internal: false,
        };
        let mut physical_plan = OptimizerPhysicalNode {
            op: Operator::PhysicalValues(ValuesOp {
                rows: Vec::new(),
                columns: vec![output_column.clone()],
            }),
            children: Vec::new(),
            stats: Statistics {
                output_row_count: 0.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            output_columns: vec![output_column],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        };
        attach_scalar_arena(&mut physical_plan, Arc::new(ScalarArena::new()));
        physical_plan
    }

    fn execution_test_plan() -> DmlChangeStreamWritePlan {
        let mut branch =
            crate::sql::codegen::iceberg_change_stream_write::ChangeStreamWriteBranchSpec::reuse_data_for_test(Vec::new());
        branch.stream_output_ordinals = Some(vec![0]);
        branch.output_partition_ordinals = Some(Vec::new());
        DmlChangeStreamWritePlan {
            producer: physical_values_plan_for_execution_test(),
            dag: crate::sql::codegen::iceberg_change_stream_write::IcebergChangeStreamWriteDagSpec::for_test(
                1,
                Some(2),
                vec![branch],
            ),
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

    #[test]
    fn execution_return_type_carries_commit_plan() {
        let execution = DmlChangeStreamWriteExecution {
            result: CoordinatedQueryResult {
                query_result: crate::runtime::query_result::QueryResult::empty(),
                write_commit: Some(crate::runtime::write_coordinator::WriteCommitInput {
                    write_id: crate::thrift::types::TUniqueId::new(1, 2),
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
                ChangeStreamWriteBranchKind::DeleteDv,
                ChangeStreamWriteBranchKind::ReuseData,
            ]
        );
        assert_eq!(dag.change_op_output_ordinal, Some(6));
        assert_eq!(dag.data_route_output_ordinal, Some(7));

        let delete_dv = dag
            .branches
            .iter()
            .find(|branch| branch.branch_kind == ChangeStreamWriteBranchKind::DeleteDv)
            .expect("delete branch");
        assert_eq!(
            delete_dv.output_partition_ordinals.as_deref(),
            Some(&[0][..])
        );

        let reuse_data = dag
            .branches
            .iter()
            .find(|branch| branch.branch_kind == ChangeStreamWriteBranchKind::ReuseData)
            .expect("reuse branch");
        assert_eq!(
            reuse_data.output_partition_ordinals.as_deref(),
            Some(&[2][..])
        );
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
            vec![ChangeStreamWriteBranchKind::DeleteDv]
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
                ChangeStreamWriteBranchKind::DeleteDv,
                ChangeStreamWriteBranchKind::ReuseData,
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
            vec![ChangeStreamWriteBranchKind::FreshData]
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
                ChangeStreamWriteBranchKind::DeleteDv,
                ChangeStreamWriteBranchKind::ReuseData,
                ChangeStreamWriteBranchKind::FreshData,
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
            .find(|branch| branch.branch_kind == ChangeStreamWriteBranchKind::FreshData)
            .expect("fresh branch");
        assert_eq!(
            fresh_data.output_partition_ordinals.as_deref(),
            Some(&[][..])
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
