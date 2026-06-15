use crate::sql::analysis::{OutputColumn, ProjectItem, SortItem, TypedExpr};
use crate::sql::catalog::TableDef;
use crate::sql::optimizer::operator::AggMode;
use crate::sql::planner::plan::AggregateCall;
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
pub(crate) struct HashAggregateBody {
    pub mode: AggMode,
    pub group_by: Vec<TypedExpr>,
    pub aggregates: Vec<AggregateCall>,
    pub is_merge: Vec<bool>,
    pub output_columns: Vec<OutputColumn>,
}
