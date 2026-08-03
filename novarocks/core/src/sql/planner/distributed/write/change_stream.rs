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

use std::collections::BTreeSet;

use crate::sql::analysis::OutputColumn;
use crate::sql::common::ChangeStreamBranchKind;

use super::super::FragmentId;
use super::contract::SqlWritePlanInput;

/// Canonical internal output used by SQL change-stream plans to route rows to
/// data writer branches. It is a planner contract and never reaches a user
/// visible result schema.
pub(crate) const CHANGE_STREAM_DATA_ROUTE_COLUMN: &str = "__change_data_route";

/// A logical branch selected by the mutation kernel. SQL owns binding its
/// sink columns to the producer output and assigning its stable branch id.
#[derive(Clone, Debug)]
pub(crate) struct ChangeStreamWriteLayoutBranch {
    pub(crate) branch_kind: ChangeStreamBranchKind,
    pub(crate) sink_spec: IcebergWriteSinkSpec,
}

/// Planner-owned input for binding a logical change-stream branch set to one
/// producer layout. The caller supplies no output ordinals or branch ids.
#[derive(Clone, Debug)]
pub(crate) struct ChangeStreamWriteLayoutRequest<'a> {
    pub(crate) producer_output_columns: &'a [OutputColumn],
    pub(crate) branches: Vec<ChangeStreamWriteLayoutBranch>,
    pub(crate) target_partition_source_columns: &'a [String],
}

#[derive(Clone, Debug)]
pub(crate) struct ChangeStreamWriteBranchSpec {
    pub(crate) branch_id: i32,
    pub(crate) branch_kind: ChangeStreamBranchKind,
    pub(crate) stream_output_ordinals: Vec<usize>,
    pub(crate) output_partition_ordinals: Vec<usize>,
    pub(crate) sink: SqlWritePlanInput,
}

#[derive(Clone, Debug)]
pub(crate) struct ChangeStreamWriteDagSpec {
    pub(crate) change_op_output_ordinal: Option<usize>,
    pub(crate) data_route_output_ordinal: Option<usize>,
    pub(crate) branches: Vec<ChangeStreamWriteBranchSpec>,
}

#[derive(Clone, Debug)]
pub(crate) struct ChangeStreamRouterSink {
    pub(crate) group_id: i32,
    pub(crate) change_op_output_ordinal: usize,
    pub(crate) data_route_output_ordinal: Option<usize>,
    pub(crate) branches: Vec<ChangeStreamBranchRoute>,
}

#[derive(Clone, Debug)]
pub(crate) struct ChangeStreamBranchRoute {
    pub(crate) branch_id: i32,
    pub(crate) branch_kind: ChangeStreamBranchKind,
    pub(crate) target_fragment_id: FragmentId,
    pub(crate) target_exchange_node_id: i32,
    pub(crate) output_ordinals: Vec<usize>,
    pub(crate) output_partition_ordinals: Vec<usize>,
}

#[derive(Clone, Debug)]
pub(crate) struct SqlChangeStreamWriteTopology {
    pub(crate) writer_branches: Vec<SqlChangeStreamWriterBranch>,
}

#[derive(Clone, Debug)]
pub(crate) struct SqlChangeStreamWriterBranch {
    pub(crate) branch_id: i32,
    pub(crate) branch_kind: ChangeStreamBranchKind,
    pub(crate) writer_fragment_id: FragmentId,
    pub(crate) sink: SqlWritePlanInput,
}

impl ChangeStreamWriteBranchSpec {
    #[cfg(test)]
    pub(crate) fn for_test(
        branch_id: i32,
        branch_kind: ChangeStreamBranchKind,
        stream_output_ordinals: Vec<usize>,
    ) -> Self {
        Self {
            branch_id,
            branch_kind,
            stream_output_ordinals,
            output_partition_ordinals: Vec::new(),
            sink: super::contract::test_support::simple_sql_write_plan_input(
                super::contract::ConnectorWriteInputBinding::RootOutputByOrdinal,
            ),
        }
    }

    #[cfg(test)]
    pub(crate) fn delete_dv_for_test(stream_output_ordinals: Vec<usize>) -> Self {
        Self::for_test(0, ChangeStreamBranchKind::DeleteDv, stream_output_ordinals)
    }

    #[cfg(test)]
    pub(crate) fn reuse_data_for_test(stream_output_ordinals: Vec<usize>) -> Self {
        Self::for_test(1, ChangeStreamBranchKind::ReuseData, stream_output_ordinals)
    }

    #[cfg(test)]
    pub(crate) fn fresh_data_for_test(stream_output_ordinals: Vec<usize>) -> Self {
        Self::for_test(2, ChangeStreamBranchKind::FreshData, stream_output_ordinals)
    }
}

impl ChangeStreamWriteDagSpec {
    pub(crate) fn validate(&self) -> Result<(), String> {
        validate_branch_set(&self.branches)?;
        let has_data_branch = self.branches.iter().any(|b| {
            matches!(
                b.branch_kind,
                ChangeStreamBranchKind::ReuseData | ChangeStreamBranchKind::FreshData
            )
        });
        if has_data_branch && self.data_route_output_ordinal.is_none() {
            return Err(
                "data_route_output_ordinal is required when data branches are declared".to_string(),
            );
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        change_op_output_ordinal: Option<usize>,
        data_route_output_ordinal: Option<usize>,
        branches: Vec<ChangeStreamWriteBranchSpec>,
    ) -> Self {
        Self {
            change_op_output_ordinal,
            data_route_output_ordinal,
            branches,
        }
    }
}

pub(crate) fn validate_branch_set(branches: &[ChangeStreamWriteBranchSpec]) -> Result<(), String> {
    let mut seen = BTreeSet::new();
    for branch in branches {
        if !seen.insert(branch.branch_kind) {
            return Err(format!(
                "duplicate change-stream branch kind {:?}",
                branch.branch_kind
            ));
        }
    }
    Ok(())
}

/// Bind a logical change-stream branch set to the immutable SQL producer
/// layout. All ordinal, visibility, partition and route validation happens
/// before a connector write topology can be constructed.
pub(crate) fn bind_change_stream_write_layout(
    mut request: ChangeStreamWriteLayoutRequest<'_>,
) -> Result<ChangeStreamWriteDagSpec, String> {
    if request.branches.is_empty() {
        return Err("DML change-stream write requires at least one branch".to_string());
    }

    let has_data_branch = request.branches.iter().any(|branch| {
        matches!(
            branch.branch_kind,
            ChangeStreamBranchKind::ReuseData | ChangeStreamBranchKind::FreshData
        )
    });
    let change_op_output_ordinal = output_ordinal_by_name(
        request.producer_output_columns,
        crate::exec::change_op::CHANGE_OP_COLUMN,
        "change-op column",
        OutputBindingKind::Internal,
    )?;
    let data_route_output_ordinal = has_data_branch
        .then(|| {
            output_ordinal_by_name(
                request.producer_output_columns,
                CHANGE_STREAM_DATA_ROUTE_COLUMN,
                "data-route column",
                OutputBindingKind::Internal,
            )
        })
        .transpose()?;
    let data_partition_ordinals = if has_data_branch {
        target_partition_source_ordinals(
            request.producer_output_columns,
            request.target_partition_source_columns,
        )?
    } else {
        Vec::new()
    };

    let mut branches = Vec::with_capacity(request.branches.len());
    for (idx, branch) in request.branches.drain(..).enumerate() {
        let output_partition_ordinals = match branch.branch_kind {
            ChangeStreamBranchKind::DeleteDv => vec![output_ordinal_by_name(
                request.producer_output_columns,
                crate::exec::row_position::ICEBERG_FILE_PATH_COL,
                "delete file column",
                OutputBindingKind::Internal,
            )?],
            ChangeStreamBranchKind::ReuseData | ChangeStreamBranchKind::FreshData => {
                data_partition_ordinals.clone()
            }
        };
        let stream_output_ordinals = output_ordinals_for_sink_columns(
            request.producer_output_columns,
            &branch.sink_spec.target_columns,
        )?;
        branches.push(ChangeStreamWriteBranchSpec {
            branch_id: i32::try_from(idx).map_err(|_| {
                "DML change-stream branch id overflow while binding layout".to_string()
            })?,
            branch_kind: branch.branch_kind,
            stream_output_ordinals,
            output_partition_ordinals,
            sink_spec: branch.sink_spec,
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
    sink_columns: &[novarocks_catalog::schema::ColumnDef],
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
        || name.eq_ignore_ascii_case(CHANGE_STREAM_DATA_ROUTE_COLUMN)
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
mod tests {
    use arrow::datatypes::DataType;
    use novarocks_catalog::schema::ColumnDef;

    use super::*;

    fn output_column(id: u32, name: &str, is_internal: bool) -> OutputColumn {
        OutputColumn {
            column_id: crate::sql::column_id::ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type: DataType::Int32,
            nullable: true,
            is_internal,
        }
    }

    fn dml_output_columns() -> Vec<OutputColumn> {
        vec![
            output_column(1, "id", false),
            output_column(2, crate::exec::row_position::ICEBERG_FILE_PATH_COL, true),
            output_column(3, crate::exec::row_position::ICEBERG_ROW_POS_COL, true),
            output_column(4, crate::exec::change_op::CHANGE_OP_COLUMN, true),
            output_column(5, CHANGE_STREAM_DATA_ROUTE_COLUMN, true),
        ]
    }

    fn sink_spec(columns: &[&str]) -> IcebergWriteSinkSpec {
        let mut sink_spec = super::super::sink::test_support::simple_sink_spec();
        sink_spec.target_columns = columns
            .iter()
            .map(|name| ColumnDef {
                name: (*name).to_string(),
                data_type: DataType::Int32,
                nullable: true,
                write_default: None,
                logical_type: None,
            })
            .collect();
        sink_spec
    }

    fn branch(kind: ChangeStreamBranchKind, columns: &[&str]) -> ChangeStreamWriteLayoutBranch {
        ChangeStreamWriteLayoutBranch {
            branch_kind: kind,
            sink_spec: sink_spec(columns),
        }
    }

    #[test]
    fn validate_rejects_duplicate_branch_kind() {
        let branches = vec![
            ChangeStreamWriteBranchSpec::for_test(0, ChangeStreamBranchKind::DeleteDv, Vec::new()),
            ChangeStreamWriteBranchSpec::for_test(1, ChangeStreamBranchKind::DeleteDv, Vec::new()),
        ];
        let err = validate_branch_set(&branches).expect_err("duplicate branch kind");
        assert!(err.contains("duplicate change-stream branch kind DeleteDv"));
    }

    #[test]
    fn validate_requires_data_route_when_data_branch_exists() {
        let spec = ChangeStreamWriteDagSpec::for_test(
            Some(0),
            None,
            vec![ChangeStreamWriteBranchSpec::for_test(
                0,
                ChangeStreamBranchKind::ReuseData,
                Vec::new(),
            )],
        );
        let err = spec.validate().expect_err("missing data_route");
        assert!(
            err.contains("data_route_output_ordinal is required when data branches are declared")
        );
    }

    #[test]
    fn validate_allows_delete_only_without_data_route() {
        let spec = ChangeStreamWriteDagSpec::for_test(
            Some(0),
            None,
            vec![ChangeStreamWriteBranchSpec::for_test(
                0,
                ChangeStreamBranchKind::DeleteDv,
                Vec::new(),
            )],
        );
        spec.validate()
            .expect("delete-only does not require data route");
    }

    #[test]
    fn validate_allows_delete_and_one_data_branch_with_data_route() {
        let spec = ChangeStreamWriteDagSpec::for_test(
            Some(0),
            Some(1),
            vec![
                ChangeStreamWriteBranchSpec::for_test(
                    0,
                    ChangeStreamBranchKind::DeleteDv,
                    Vec::new(),
                ),
                ChangeStreamWriteBranchSpec::for_test(
                    1,
                    ChangeStreamBranchKind::FreshData,
                    Vec::new(),
                ),
            ],
        );
        spec.validate()
            .expect("change_op alone distinguishes delete from one data branch");
    }

    #[test]
    fn bind_layout_assigns_update_mor_branches_and_ordinals() {
        let output_columns = dml_output_columns();
        let partition_columns = vec!["id".to_string()];
        let dag = bind_change_stream_write_layout(ChangeStreamWriteLayoutRequest {
            producer_output_columns: &output_columns,
            branches: vec![
                branch(
                    ChangeStreamBranchKind::DeleteDv,
                    &[
                        crate::exec::row_position::ICEBERG_FILE_PATH_COL,
                        crate::exec::row_position::ICEBERG_ROW_POS_COL,
                    ],
                ),
                branch(ChangeStreamBranchKind::ReuseData, &["id"]),
            ],
            target_partition_source_columns: &partition_columns,
        })
        .expect("update MOR layout");

        assert_eq!(dag.change_op_output_ordinal, Some(3));
        assert_eq!(dag.data_route_output_ordinal, Some(4));
        assert_eq!(dag.branches.len(), 2);
        assert_eq!(dag.branches[0].branch_id, 0);
        assert_eq!(
            dag.branches[0].branch_kind,
            ChangeStreamBranchKind::DeleteDv
        );
        assert_eq!(dag.branches[0].stream_output_ordinals, vec![1, 2]);
        assert_eq!(dag.branches[0].output_partition_ordinals, vec![1]);
        assert_eq!(dag.branches[1].branch_id, 1);
        assert_eq!(
            dag.branches[1].branch_kind,
            ChangeStreamBranchKind::ReuseData
        );
        assert_eq!(dag.branches[1].stream_output_ordinals, vec![0]);
        assert_eq!(dag.branches[1].output_partition_ordinals, vec![0]);
    }

    #[test]
    fn bind_layout_preserves_merge_branch_order() {
        let output_columns = dml_output_columns();
        let partition_columns = vec!["id".to_string()];
        let dag = bind_change_stream_write_layout(ChangeStreamWriteLayoutRequest {
            producer_output_columns: &output_columns,
            branches: vec![
                branch(
                    ChangeStreamBranchKind::DeleteDv,
                    &[crate::exec::row_position::ICEBERG_FILE_PATH_COL],
                ),
                branch(ChangeStreamBranchKind::ReuseData, &["id"]),
                branch(ChangeStreamBranchKind::FreshData, &["id"]),
            ],
            target_partition_source_columns: &partition_columns,
        })
        .expect("merge layout");

        assert_eq!(
            dag.branches
                .iter()
                .map(|branch| (branch.branch_id, branch.branch_kind))
                .collect::<Vec<_>>(),
            vec![
                (0, ChangeStreamBranchKind::DeleteDv),
                (1, ChangeStreamBranchKind::ReuseData),
                (2, ChangeStreamBranchKind::FreshData),
            ]
        );
    }

    #[test]
    fn bind_layout_rejects_missing_change_op_output() {
        let output_columns = vec![output_column(1, "id", false)];
        let partition_columns = Vec::new();
        let err = bind_change_stream_write_layout(ChangeStreamWriteLayoutRequest {
            producer_output_columns: &output_columns,
            branches: vec![branch(ChangeStreamBranchKind::FreshData, &["id"])],
            target_partition_source_columns: &partition_columns,
        })
        .expect_err("missing change-op output");
        assert!(err.contains("change-op column"));
        assert!(err.contains("not found"));
    }

    #[test]
    fn bind_layout_rejects_ambiguous_data_route_output() {
        let mut output_columns = dml_output_columns();
        output_columns.push(output_column(6, CHANGE_STREAM_DATA_ROUTE_COLUMN, true));
        let partition_columns = Vec::new();
        let err = bind_change_stream_write_layout(ChangeStreamWriteLayoutRequest {
            producer_output_columns: &output_columns,
            branches: vec![branch(ChangeStreamBranchKind::FreshData, &["id"])],
            target_partition_source_columns: &partition_columns,
        })
        .expect_err("ambiguous data route");
        assert!(err.contains("data-route column"));
        assert!(err.contains("ambiguous"));
    }

    #[test]
    fn bind_layout_rejects_user_visible_internal_column() {
        let mut output_columns = dml_output_columns();
        output_columns[3].is_internal = false;
        let partition_columns = Vec::new();
        let err = bind_change_stream_write_layout(ChangeStreamWriteLayoutRequest {
            producer_output_columns: &output_columns,
            branches: vec![branch(ChangeStreamBranchKind::FreshData, &["id"])],
            target_partition_source_columns: &partition_columns,
        })
        .expect_err("change-op must be internal");
        assert!(err.contains("change-op column"));
        assert!(err.contains("must be marked internal"));
    }

    #[test]
    fn bind_layout_rejects_internal_user_visible_sink_column() {
        let mut output_columns = dml_output_columns();
        output_columns[0].is_internal = true;
        let partition_columns = Vec::new();
        let err = bind_change_stream_write_layout(ChangeStreamWriteLayoutRequest {
            producer_output_columns: &output_columns,
            branches: vec![branch(ChangeStreamBranchKind::FreshData, &["id"])],
            target_partition_source_columns: &partition_columns,
        })
        .expect_err("sink column must be user-visible");
        assert!(err.contains("sink input column"));
        assert!(err.contains("must be user-visible"));
    }

    #[test]
    fn bind_layout_rejects_internal_partition_source_column() {
        let mut output_columns = dml_output_columns();
        output_columns[0].is_internal = true;
        let partition_columns = vec!["id".to_string()];
        let err = bind_change_stream_write_layout(ChangeStreamWriteLayoutRequest {
            producer_output_columns: &output_columns,
            branches: vec![branch(ChangeStreamBranchKind::FreshData, &["id"])],
            target_partition_source_columns: &partition_columns,
        })
        .expect_err("partition source must be user-visible");
        assert!(err.contains("target partition source column"));
        assert!(err.contains("must be user-visible"));
    }

    #[test]
    fn bind_layout_rejects_missing_partition_source_column() {
        let output_columns = dml_output_columns();
        let partition_columns = vec!["missing_partition".to_string()];
        let err = bind_change_stream_write_layout(ChangeStreamWriteLayoutRequest {
            producer_output_columns: &output_columns,
            branches: vec![branch(ChangeStreamBranchKind::FreshData, &["id"])],
            target_partition_source_columns: &partition_columns,
        })
        .expect_err("partition source missing from producer output");
        assert!(err.contains("target partition source column"));
        assert!(err.contains("not found"));
    }

    #[test]
    fn bind_layout_rejects_missing_data_route_for_data_branch() {
        let mut output_columns = dml_output_columns();
        output_columns.pop();
        let partition_columns = Vec::new();
        let err = bind_change_stream_write_layout(ChangeStreamWriteLayoutRequest {
            producer_output_columns: &output_columns,
            branches: vec![branch(ChangeStreamBranchKind::ReuseData, &["id"])],
            target_partition_source_columns: &partition_columns,
        })
        .expect_err("data route missing from producer output");
        assert!(err.contains("data-route column"));
        assert!(err.contains("not found"));
    }
}
