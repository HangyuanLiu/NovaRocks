//! Transitional helpers between analyzer/planner expression wrappers and
//! memo-native `ScalarId` wrappers.

use arrow::datatypes::DataType;

use crate::sql::analysis::{ExprKind, OutputColumn, ProjectItem, SortItem, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::operator::{ScalarAggregateSpec, ScalarProjectItem, ScalarWindowSpec};
use crate::sql::optimizer::scalar::{ScalarArena, ScalarId, SortKey, intern_typed, materialize};
use crate::sql::planner::plan::{AggregateCall, WindowExpr};

pub(crate) fn intern_exprs(arena: &mut ScalarArena, exprs: &[TypedExpr]) -> Vec<ScalarId> {
    exprs.iter().map(|expr| intern_typed(arena, expr)).collect()
}

pub(crate) fn materialize_exprs(arena: &ScalarArena, exprs: &[ScalarId]) -> Vec<TypedExpr> {
    exprs.iter().map(|expr| materialize(arena, *expr)).collect()
}

pub(crate) fn intern_sort_item(arena: &mut ScalarArena, item: &SortItem) -> SortKey {
    SortKey {
        expr: intern_typed(arena, &item.expr),
        asc: item.asc,
        nulls_first: item.nulls_first,
    }
}

pub(crate) fn intern_sort_items(arena: &mut ScalarArena, items: &[SortItem]) -> Vec<SortKey> {
    items
        .iter()
        .map(|item| intern_sort_item(arena, item))
        .collect()
}

pub(crate) fn materialize_sort_key(arena: &ScalarArena, key: &SortKey) -> SortItem {
    SortItem {
        expr: materialize(arena, key.expr),
        asc: key.asc,
        nulls_first: key.nulls_first,
    }
}

pub(crate) fn materialize_sort_keys(arena: &ScalarArena, keys: &[SortKey]) -> Vec<SortItem> {
    keys.iter()
        .map(|key| materialize_sort_key(arena, key))
        .collect()
}

pub(crate) fn intern_project_item(
    arena: &mut ScalarArena,
    item: &ProjectItem,
) -> ScalarProjectItem {
    ScalarProjectItem {
        expr: intern_typed(arena, &item.expr),
        output_name: item.output_name.clone(),
        output_column_id: item.output_column_id,
    }
}

pub(crate) fn intern_project_items(
    arena: &mut ScalarArena,
    items: &[ProjectItem],
) -> Vec<ScalarProjectItem> {
    items
        .iter()
        .map(|item| intern_project_item(arena, item))
        .collect()
}

pub(crate) fn materialize_project_item(
    arena: &ScalarArena,
    item: &ScalarProjectItem,
) -> ProjectItem {
    ProjectItem {
        expr: materialize(arena, item.expr),
        output_name: item.output_name.clone(),
        output_column_id: item.output_column_id,
    }
}

pub(crate) fn materialize_project_items(
    arena: &ScalarArena,
    items: &[ScalarProjectItem],
) -> Vec<ProjectItem> {
    items
        .iter()
        .map(|item| materialize_project_item(arena, item))
        .collect()
}

pub(crate) fn intern_aggregate_call(
    arena: &mut ScalarArena,
    call: &AggregateCall,
) -> ScalarAggregateSpec {
    ScalarAggregateSpec {
        name: call.name.clone(),
        args: intern_exprs(arena, &call.args),
        distinct: call.distinct,
        order_by: intern_sort_items(arena, &call.order_by),
    }
}

pub(crate) fn intern_aggregate_calls(
    arena: &mut ScalarArena,
    calls: &[AggregateCall],
) -> Vec<ScalarAggregateSpec> {
    calls
        .iter()
        .map(|call| intern_aggregate_call(arena, call))
        .collect()
}

pub(crate) fn materialize_aggregate_call(
    arena: &ScalarArena,
    call: &ScalarAggregateSpec,
    output_column: Option<&OutputColumn>,
) -> AggregateCall {
    AggregateCall {
        name: call.name.clone(),
        args: materialize_exprs(arena, &call.args),
        distinct: call.distinct,
        result_type: output_column
            .map(|column| column.data_type.clone())
            .unwrap_or(DataType::Null),
        order_by: materialize_sort_keys(arena, &call.order_by),
        output_column_id: output_column
            .map(|column| column.column_id)
            .unwrap_or(ColumnId::UNSET),
    }
}

pub(crate) fn materialize_aggregate_calls(
    arena: &ScalarArena,
    calls: &[ScalarAggregateSpec],
    group_by_len: usize,
    output_columns: &[OutputColumn],
) -> Vec<AggregateCall> {
    calls
        .iter()
        .enumerate()
        .map(|(idx, call)| {
            materialize_aggregate_call(arena, call, output_columns.get(group_by_len + idx))
        })
        .collect()
}

pub(crate) fn intern_window_expr(arena: &mut ScalarArena, expr: &WindowExpr) -> ScalarWindowSpec {
    ScalarWindowSpec {
        name: expr.name.clone(),
        args: intern_exprs(arena, &expr.args),
        distinct: expr.distinct,
        partition_by: intern_exprs(arena, &expr.partition_by),
        order_by: intern_sort_items(arena, &expr.order_by),
        window_frame: expr.window_frame.clone(),
        ignore_nulls: expr.ignore_nulls,
    }
}

pub(crate) fn intern_window_exprs(
    arena: &mut ScalarArena,
    exprs: &[WindowExpr],
) -> Vec<ScalarWindowSpec> {
    exprs
        .iter()
        .map(|expr| intern_window_expr(arena, expr))
        .collect()
}

pub(crate) fn materialize_window_expr(
    arena: &ScalarArena,
    expr: &ScalarWindowSpec,
    output_column: Option<&OutputColumn>,
) -> WindowExpr {
    WindowExpr {
        name: expr.name.clone(),
        args: materialize_exprs(arena, &expr.args),
        distinct: expr.distinct,
        partition_by: materialize_exprs(arena, &expr.partition_by),
        order_by: materialize_sort_keys(arena, &expr.order_by),
        window_frame: expr.window_frame.clone(),
        result_type: output_column
            .map(|column| column.data_type.clone())
            .unwrap_or(DataType::Null),
        output_name: output_column
            .map(|column| column.name.clone())
            .unwrap_or_default(),
        output_column_id: output_column
            .map(|column| column.column_id)
            .unwrap_or(ColumnId::UNSET),
        ignore_nulls: expr.ignore_nulls,
    }
}

pub(crate) fn materialize_window_exprs(
    arena: &ScalarArena,
    exprs: &[ScalarWindowSpec],
    output_columns: &[OutputColumn],
) -> Vec<WindowExpr> {
    let window_output_start = output_columns.len().saturating_sub(exprs.len());
    exprs
        .iter()
        .enumerate()
        .map(|(idx, expr)| {
            materialize_window_expr(arena, expr, output_columns.get(window_output_start + idx))
        })
        .collect()
}

pub(crate) fn intern_column_sort_key(
    arena: &mut ScalarArena,
    key: &crate::sql::optimizer::property::SortKey,
) -> SortKey {
    let expr = TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: key.column,
            qualifier: None,
            column: format!("{}", key.column),
        },
        data_type: DataType::Null,
        nullable: true,
    };
    SortKey {
        expr: intern_typed(arena, &expr),
        asc: key.asc,
        nulls_first: key.nulls_first,
    }
}

pub(crate) fn column_id_expr(id: ColumnId, data_type: DataType, nullable: bool) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: id,
            qualifier: None,
            column: format!("col{}", id.0),
        },
        data_type,
        nullable,
    }
}
