#![allow(dead_code)]

use crate::sql::analysis::TypedExpr;
use crate::sql::codegen::FragmentId;

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

#[derive(Clone, Debug)]
pub(crate) struct WiredRuntimeFilterBuild {
    pub filter_id: i32,
    pub build_expr: TypedExpr,
    pub probe_expr: TypedExpr,
    pub expr_order: usize,
    pub execution_mode: JoinExecutionMode,
    pub source_fragment_id: FragmentId,
    pub target_fragment_ids: Vec<FragmentId>,
}

#[derive(Clone, Debug)]
pub(crate) struct WiredRuntimeFilterProbe {
    pub filter_id: i32,
    pub probe_expr: TypedExpr,
    pub source_fragment_id: FragmentId,
}
