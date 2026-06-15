use crate::sql::analysis::{JoinKind, OutputColumn, ProjectItem, SortItem, TypedExpr};
use crate::sql::catalog::TableDef;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::operator::{
    AggMode, JoinDistribution, PhysicalHashJoinEqCondition, TopNPhase,
};
use crate::sql::planner::plan::WindowExpr;
use crate::sql::planner::plan::{AggregateCall, DecodeMapping};
use crate::sql::planner::plan::{ScanDictionaryColumn, ScanVariantColumn};

#[derive(Clone, Debug)]
pub(crate) struct ScanBody {
    pub database: String,
    pub table: TableDef,
    pub alias: Option<String>,
    pub columns: Vec<OutputColumn>,
    /// Scan predicates plus any folded Filter conjuncts (see build_distributed_plan filter handling).
    pub predicates: Vec<TypedExpr>,
    pub required_columns: Option<Vec<String>>,
    pub dict_columns: Vec<ScanDictionaryColumn>,
    pub variant_columns: Vec<ScanVariantColumn>,
    pub mv_rewritten_from: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct ProjectBody {
    pub items: Vec<ProjectItem>,
    pub output_qualifier: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct SortBody {
    pub items: Vec<SortItem>,
    pub analytic_partition_exprs: Vec<TypedExpr>,
    pub output_columns: Vec<OutputColumn>,
    pub offset: Option<i64>,
}

#[derive(Clone, Debug)]
pub(crate) struct TopNBody {
    pub items: Vec<SortItem>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub phase: TopNPhase,
    pub is_split: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct HashAggregateBody {
    pub mode: AggMode,
    pub group_by: Vec<TypedExpr>,
    pub aggregates: Vec<AggregateCall>,
    pub is_merge: Vec<bool>,
    pub output_columns: Vec<OutputColumn>,
}

#[derive(Clone, Debug)]
pub(crate) struct HashJoinBody {
    pub join_type: JoinKind,
    pub eq_conditions: Vec<PhysicalHashJoinEqCondition>,
    pub other_condition: Option<TypedExpr>,
    pub distribution: JoinDistribution,
}

#[derive(Clone, Debug)]
pub(crate) struct NestLoopJoinBody {
    pub join_type: JoinKind,
    pub condition: Option<TypedExpr>,
}

#[derive(Clone, Debug)]
pub(crate) struct ValuesBody {
    pub rows: Vec<Vec<TypedExpr>>,
    pub columns: Vec<OutputColumn>,
}

#[derive(Clone, Debug)]
pub(crate) struct AssertOneRowBody {
    pub subquery_text: String,
}

#[derive(Clone, Debug)]
pub(crate) struct DecodeBody {
    pub mappings: Vec<DecodeMapping>,
    pub output_columns: Vec<OutputColumn>,
}

#[derive(Clone, Debug)]
pub(crate) struct RepeatBody {
    pub virtual_tuple_id: i32,
    pub repeat_column_ref_list: Vec<Vec<String>>,
    pub repeat_column_ref_ids: Vec<Vec<ColumnId>>,
    pub grouping_ids: Vec<u64>,
    pub all_rollup_columns: Vec<String>,
    pub all_rollup_column_ids: Vec<ColumnId>,
    pub grouping_key_aliases: Vec<(String, String)>,
    pub grouping_fn_args: Vec<(String, Vec<String>)>,
    pub grouping_fn_arg_ids: Vec<Vec<ColumnId>>,
    pub grouping_fn_ids: Vec<(String, ColumnId)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SetOpKind {
    UnionAll,
    Intersect,
    Except,
}

#[derive(Clone, Debug)]
pub(crate) struct SetOpBody {
    pub kind: SetOpKind,
    pub output_columns: Vec<OutputColumn>,
    pub child_output_columns: Vec<Vec<OutputColumn>>,
}

#[derive(Clone, Debug)]
pub(crate) struct WindowBody {
    pub window_exprs: Vec<WindowExpr>,
    pub output_columns: Vec<OutputColumn>,
}

#[derive(Clone, Debug)]
pub(crate) struct GenerateSeriesBody {
    pub start: i64,
    pub end: i64,
    pub step: i64,
    pub column_name: String,
    pub alias: Option<String>,
    pub output_column_id: ColumnId,
}

#[derive(Clone, Debug)]
pub(crate) struct TableFunctionBody {
    pub function_name: String,
    pub args: Vec<TypedExpr>,
    pub output_columns: Vec<OutputColumn>,
    pub alias: Option<String>,
    pub is_left_join: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multi_node_bodies_carry_only_semantic_fields() {
        let _window = WindowBody {
            window_exprs: Vec::new(),
            output_columns: Vec::new(),
        };
        let _generate_series = GenerateSeriesBody {
            start: 1,
            end: 3,
            step: 1,
            column_name: "value".to_string(),
            alias: Some("gs".to_string()),
            output_column_id: ColumnId::new_for_test(1),
        };
        let _table_function = TableFunctionBody {
            function_name: "unnest".to_string(),
            args: Vec::new(),
            output_columns: Vec::new(),
            alias: Some("u".to_string()),
            is_left_join: false,
        };
    }
}
