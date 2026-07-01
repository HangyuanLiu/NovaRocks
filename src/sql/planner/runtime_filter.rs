#![allow(dead_code)]

use crate::sql::analysis::TypedExpr;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JoinExecutionMode {
    Broadcast,
    Partitioned,
    Colocate,
}

#[derive(Clone, Debug)]
pub(crate) struct RuntimeFilterBuildIntent {
    pub filter_id: i32,
    pub build_expr: TypedExpr,
    pub probe_expr: TypedExpr,
    pub expr_order: usize,
    pub execution_mode: JoinExecutionMode,
}

#[derive(Clone, Debug)]
pub(crate) struct RuntimeFilterProbeIntent {
    pub filter_id: i32,
    pub probe_expr: TypedExpr,
}
