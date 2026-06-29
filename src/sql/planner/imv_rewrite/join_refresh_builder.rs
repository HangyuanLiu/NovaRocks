use arrow::datatypes::DataType;

use crate::sql::analysis::{ExprKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::planner::imv_rewrite::join_refresh_descriptor::{
    JoinRefreshDescriptor, JoinRefreshOutputMapping, JoinRefreshOutputSource,
};
use crate::sql::planner::plan::{LogicalPlanNode, LogicalProjectNode, PlanNodeKind};

pub(crate) fn build_join_apply_key_project(
    input: LogicalPlanNode,
    desc: &JoinRefreshDescriptor,
    left_uuid: &str,
    right_uuid: &str,
    apply_key_column_id: u32,
    action_column_id: u32,
) -> Result<LogicalPlanNode, String> {
    desc.validate()?;
    validate_apply_key_project_output_ids(desc, apply_key_column_id, action_column_id)?;
    let input_columns = crate::sql::planner::plan_output_columns(&input).map_err(|err| {
        format!("join refresh apply-key project cannot derive input columns: {err}")
    })?;

    let items = desc
        .output_mappings
        .iter()
        .map(|mapping| {
            project_item_for_mapping(mapping, desc, &input_columns, left_uuid, right_uuid)
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(LogicalPlanNode::new(
        PlanNodeKind::Project(LogicalProjectNode {
            items,
            output_qualifier: None,
        }),
        vec![input],
        None,
    ))
}

fn validate_apply_key_project_output_ids(
    desc: &JoinRefreshDescriptor,
    apply_key_column_id: u32,
    action_column_id: u32,
) -> Result<(), String> {
    let expected_apply_key = ColumnId(apply_key_column_id);
    let expected_action = ColumnId(action_column_id);
    let apply_key_output = desc
        .output_mappings
        .iter()
        .find(|mapping| {
            mapping.source
                == JoinRefreshOutputSource::JoinApplyKey(desc.join_apply_key_column.column_id)
        })
        .map(|mapping| mapping.mv_output_column.column_id)
        .ok_or_else(|| {
            "join refresh apply-key project missing join apply-key output mapping".to_string()
        })?;
    if apply_key_output != expected_apply_key {
        return Err(format!(
            "join refresh apply-key project apply-key output id mismatch: descriptor has {apply_key_output}, builder requested {expected_apply_key}"
        ));
    }

    let action_output = desc
        .output_mappings
        .iter()
        .find(|mapping| {
            mapping.source == JoinRefreshOutputSource::Action(desc.action_column.column_id)
        })
        .map(|mapping| mapping.mv_output_column.column_id)
        .ok_or_else(|| {
            "join refresh apply-key project missing action output mapping".to_string()
        })?;
    if action_output != expected_action {
        return Err(format!(
            "join refresh apply-key project action output id mismatch: descriptor has {action_output}, builder requested {expected_action}"
        ));
    }
    Ok(())
}

fn project_item_for_mapping(
    mapping: &JoinRefreshOutputMapping,
    desc: &JoinRefreshDescriptor,
    input_columns: &[OutputColumn],
    left_uuid: &str,
    right_uuid: &str,
) -> Result<ProjectItem, String> {
    let expr = match mapping.source {
        JoinRefreshOutputSource::Payload(column_id) => {
            let source = desc
                .payload_columns
                .iter()
                .find(|column| column.column_id == column_id)
                .ok_or_else(|| {
                    format!("join refresh apply-key project references unknown payload column {column_id}")
                })?;
            validate_input_column(input_columns, source)?;
            column_ref(source)
        }
        JoinRefreshOutputSource::Action(column_id) => {
            if column_id != desc.action_column.column_id {
                return Err(format!(
                    "join refresh apply-key project references unknown action column {column_id}"
                ));
            }
            validate_input_column(input_columns, &desc.action_column)?;
            TypedExpr {
                kind: ExprKind::Cast {
                    expr: Box::new(column_ref(&desc.action_column)),
                    target: DataType::Int8,
                },
                data_type: DataType::Int8,
                nullable: false,
            }
        }
        JoinRefreshOutputSource::JoinApplyKey(column_id) => {
            if column_id != desc.join_apply_key_column.column_id {
                return Err(format!(
                    "join refresh apply-key project references unknown join apply-key column {column_id}"
                ));
            }
            validate_input_column(input_columns, &desc.left_row_id_column)?;
            validate_input_column(input_columns, &desc.right_row_id_column)?;
            join_row_key_expr(desc, left_uuid, right_uuid)
        }
    };

    Ok(ProjectItem {
        expr,
        output_name: mapping.mv_output_column.name.clone(),
        output_column_id: mapping.mv_output_column.column_id,
    })
}

fn validate_input_column(
    input_columns: &[OutputColumn],
    expected: &OutputColumn,
) -> Result<(), String> {
    let matches = input_columns
        .iter()
        .filter(|column| column.column_id == expected.column_id)
        .collect::<Vec<_>>();
    let [actual] = matches.as_slice() else {
        if matches.is_empty() {
            return Err(format!(
                "join refresh apply-key project missing input column {} `{}`",
                expected.column_id, expected.name
            ));
        }
        return Err(format!(
            "join refresh apply-key project found duplicate input column id {}",
            expected.column_id
        ));
    };
    if !actual.name.eq_ignore_ascii_case(&expected.name)
        || actual.data_type != expected.data_type
        || actual.nullable != expected.nullable
        || !input_internal_matches(actual, expected)
    {
        return Err(format!(
            "join refresh apply-key project input column {} `{}` shape mismatch: actual name=`{}`, type={:?}, nullable={}, internal={}; expected type={:?}, nullable={}, internal={}",
            expected.column_id,
            expected.name,
            actual.name,
            actual.data_type,
            actual.nullable,
            actual.is_internal,
            expected.data_type,
            expected.nullable,
            expected.is_internal
        ));
    }
    Ok(())
}

fn input_internal_matches(actual: &OutputColumn, expected: &OutputColumn) -> bool {
    actual.is_internal == expected.is_internal
        || (expected.is_internal && !actual.is_internal && is_internal_output_name(&actual.name))
}

fn is_internal_output_name(name: &str) -> bool {
    name.eq_ignore_ascii_case(crate::exec::change_op::CHANGE_OP_COLUMN)
        || name.eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_ROW_ID_COL)
        || name.eq_ignore_ascii_case(
            crate::engine::mv::iceberg_target_apply::ICEBERG_MV_JOIN_APPLY_KEY_COLUMN,
        )
}

fn join_row_key_expr(desc: &JoinRefreshDescriptor, left_uuid: &str, right_uuid: &str) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::FunctionCall {
            name: "join_row_key".to_string(),
            args: vec![
                string_literal(left_uuid),
                column_ref(&desc.left_row_id_column),
                string_literal(right_uuid),
                column_ref(&desc.right_row_id_column),
            ],
            distinct: false,
        },
        data_type: DataType::Utf8,
        nullable: false,
    }
}

fn column_ref(column: &OutputColumn) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: column.column_id,
            qualifier: None,
            column: column.name.clone(),
        },
        data_type: column.data_type.clone(),
        nullable: column.nullable,
    }
}

fn string_literal(value: &str) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(LiteralValue::String(value.to_string())),
        data_type: DataType::Utf8,
        nullable: false,
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use crate::sql::analysis::{ExprKind, LiteralValue, OutputColumn, ProjectItem};
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::imv_rewrite::join_refresh_descriptor::{
        JoinRefreshBranchDescriptor, JoinRefreshBranchSide, JoinRefreshDescriptor,
        JoinRefreshJoinKeyPair, JoinRefreshMode, JoinRefreshMvIdentity, JoinRefreshOutputMapping,
        JoinRefreshOutputSource,
    };
    use crate::sql::planner::plan::{LogicalPlanNode, LogicalValuesNode, PlanNodeKind};

    #[test]
    fn apply_key_project_uses_output_mappings_and_validates_sources() {
        let input = test_values_plan(vec![
            out(1, "k", DataType::Int64, false, false),
            out(
                2,
                crate::exec::row_position::ICEBERG_ROW_ID_COL,
                DataType::Int64,
                false,
                true,
            ),
            out(
                3,
                crate::exec::row_position::ICEBERG_ROW_ID_COL,
                DataType::Int64,
                false,
                true,
            ),
            out(
                4,
                crate::exec::change_op::CHANGE_OP_COLUMN,
                DataType::Int8,
                false,
                true,
            ),
        ]);
        let desc = test_descriptor(JoinRefreshMode::AppendOnly);

        let plan =
            super::build_join_apply_key_project(input, &desc, "left-uuid", "right-uuid", 90, 91)
                .expect("apply-key project");

        let PlanNodeKind::Project(project) = &plan.kind else {
            panic!("expected Project");
        };
        assert_eq!(project.items.len(), 3);
        assert_payload_item(&project.items[0]);
        assert_join_apply_key_item(&project.items[1]);
        assert_action_item(&project.items[2]);
    }

    #[test]
    fn apply_key_project_rejects_missing_input_source_column() {
        let input = test_values_plan(vec![
            out(1, "k", DataType::Int64, false, false),
            out(
                2,
                crate::exec::row_position::ICEBERG_ROW_ID_COL,
                DataType::Int64,
                false,
                true,
            ),
            out(
                4,
                crate::exec::change_op::CHANGE_OP_COLUMN,
                DataType::Int8,
                false,
                true,
            ),
        ]);
        let desc = test_descriptor(JoinRefreshMode::AppendOnly);

        let err =
            super::build_join_apply_key_project(input, &desc, "left-uuid", "right-uuid", 90, 91)
                .expect_err("missing right row-id should fail closed");

        assert!(err.contains("missing input column c3"), "err={err}");
    }

    #[test]
    fn apply_key_project_rejects_output_id_mismatch() {
        let input = test_values_plan(test_input_columns());
        let desc = test_descriptor(JoinRefreshMode::AppendOnly);

        let err =
            super::build_join_apply_key_project(input, &desc, "left-uuid", "right-uuid", 900, 91)
                .expect_err("apply-key id mismatch should fail closed");

        assert!(err.contains("apply-key output id mismatch"), "err={err}");
    }

    fn test_input_columns() -> Vec<OutputColumn> {
        vec![
            out(1, "k", DataType::Int64, false, false),
            out(
                2,
                crate::exec::row_position::ICEBERG_ROW_ID_COL,
                DataType::Int64,
                false,
                true,
            ),
            out(
                3,
                crate::exec::row_position::ICEBERG_ROW_ID_COL,
                DataType::Int64,
                false,
                true,
            ),
            out(
                4,
                crate::exec::change_op::CHANGE_OP_COLUMN,
                DataType::Int8,
                false,
                true,
            ),
        ]
    }

    fn test_values_plan(columns: Vec<OutputColumn>) -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Values(LogicalValuesNode {
                rows: Vec::new(),
                columns,
            }),
            Vec::new(),
            None,
        )
    }

    fn test_descriptor(mode: JoinRefreshMode) -> JoinRefreshDescriptor {
        let payload = out(1, "k", DataType::Int64, false, false);
        let payload_output = out(80, "mv_k", DataType::Int64, false, false);
        let action = out(
            4,
            crate::exec::change_op::CHANGE_OP_COLUMN,
            DataType::Int8,
            false,
            true,
        );
        let action_output = out(
            91,
            crate::exec::change_op::CHANGE_OP_COLUMN,
            DataType::Int8,
            false,
            true,
        );
        let join_apply_key = out(
            5,
            crate::engine::mv::iceberg_target_apply::ICEBERG_MV_JOIN_APPLY_KEY_COLUMN,
            DataType::Utf8,
            false,
            true,
        );
        let join_apply_key_output = out(
            90,
            crate::engine::mv::iceberg_target_apply::ICEBERG_MV_JOIN_APPLY_KEY_COLUMN,
            DataType::Utf8,
            false,
            true,
        );

        JoinRefreshDescriptor {
            mode,
            mv_identity: JoinRefreshMvIdentity {
                catalog: "ice".to_string(),
                database: "db".to_string(),
                name: "mv_join".to_string(),
            },
            left_base_fqn: "ice.db.left_t".to_string(),
            right_base_fqn: "ice.db.right_t".to_string(),
            left_row_id_column: out(
                2,
                crate::exec::row_position::ICEBERG_ROW_ID_COL,
                DataType::Int64,
                false,
                true,
            ),
            right_row_id_column: out(
                3,
                crate::exec::row_position::ICEBERG_ROW_ID_COL,
                DataType::Int64,
                false,
                true,
            ),
            action_column: action.clone(),
            join_apply_key_column: join_apply_key.clone(),
            payload_columns: vec![payload.clone()],
            join_key_pairs: vec![JoinRefreshJoinKeyPair {
                left_column: out(6, "left_k", DataType::Int64, false, false),
                right_column: out(7, "right_k", DataType::Int64, false, false),
            }],
            output_mappings: vec![
                JoinRefreshOutputMapping {
                    mv_output_column: payload_output,
                    source: JoinRefreshOutputSource::Payload(payload.column_id),
                },
                JoinRefreshOutputMapping {
                    mv_output_column: join_apply_key_output,
                    source: JoinRefreshOutputSource::JoinApplyKey(join_apply_key.column_id),
                },
                JoinRefreshOutputMapping {
                    mv_output_column: action_output,
                    source: JoinRefreshOutputSource::Action(action.column_id),
                },
            ],
            branches: vec![JoinRefreshBranchDescriptor {
                side: JoinRefreshBranchSide::LeftDeltaRightSnapshot,
                action_column_id: action.column_id,
            }],
            needs_target_locator: false,
        }
    }

    fn out(
        id: u32,
        name: &str,
        data_type: DataType,
        nullable: bool,
        is_internal: bool,
    ) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: name.to_string(),
            data_type,
            nullable,
            is_internal,
        }
    }

    fn assert_payload_item(item: &ProjectItem) {
        assert_eq!(item.output_name, "mv_k");
        assert_eq!(item.output_column_id, ColumnId(80));
        assert_column_ref(&item.expr.kind, ColumnId(1), "k");
    }

    fn assert_join_apply_key_item(item: &ProjectItem) {
        assert!(item.output_name.eq_ignore_ascii_case(
            crate::engine::mv::iceberg_target_apply::ICEBERG_MV_JOIN_APPLY_KEY_COLUMN
        ));
        assert_eq!(item.output_column_id, ColumnId(90));
        let ExprKind::FunctionCall { name, args, .. } = &item.expr.kind else {
            panic!("expected join apply-key function call");
        };
        assert_eq!(name, "join_row_key");
        assert_eq!(args.len(), 4);
        assert_string_literal(&args[0].kind, "left-uuid");
        assert_column_ref(
            &args[1].kind,
            ColumnId(2),
            crate::exec::row_position::ICEBERG_ROW_ID_COL,
        );
        assert_string_literal(&args[2].kind, "right-uuid");
        assert_column_ref(
            &args[3].kind,
            ColumnId(3),
            crate::exec::row_position::ICEBERG_ROW_ID_COL,
        );
    }

    fn assert_action_item(item: &ProjectItem) {
        assert!(
            item.output_name
                .eq_ignore_ascii_case(crate::exec::change_op::CHANGE_OP_COLUMN)
        );
        assert_eq!(item.output_column_id, ColumnId(91));
        let ExprKind::Cast { target, .. } = &item.expr.kind else {
            panic!("expected action cast");
        };
        assert_eq!(target, &DataType::Int8);
        let ExprKind::Cast { expr, .. } = &item.expr.kind else {
            unreachable!("cast already matched");
        };
        assert_column_ref(
            &expr.kind,
            ColumnId(4),
            crate::exec::change_op::CHANGE_OP_COLUMN,
        );
    }

    fn assert_string_literal(kind: &ExprKind, expected: &str) {
        let ExprKind::Literal(LiteralValue::String(actual)) = kind else {
            panic!("expected string literal");
        };
        assert_eq!(actual, expected);
    }

    fn assert_column_ref(kind: &ExprKind, expected_id: ColumnId, expected_name: &str) {
        let ExprKind::ColumnRef {
            column_id, column, ..
        } = kind
        else {
            panic!("expected column ref");
        };
        assert_eq!(*column_id, expected_id);
        assert!(column.eq_ignore_ascii_case(expected_name));
    }
}
