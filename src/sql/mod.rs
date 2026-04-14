pub(crate) mod catalog;
pub(crate) mod cte;
// FragmentId type alias is used; other items are legacy pre-cascades scaffolding.
#[allow(dead_code)]
pub(crate) mod fragment;
pub(crate) mod ir;
pub(crate) mod parser;
pub(crate) mod plan;
pub(crate) mod types;

pub(crate) mod statistics;

pub(crate) mod cascades;

pub(crate) mod analyzer;
pub(crate) mod explain;
pub(crate) mod physical;
// Legacy pre-cascades planner; kept for reference during optimizer migration.
#[allow(dead_code)]
pub(crate) mod planner;

pub(crate) use parser::ast::{
    ColumnAggregation, Literal, SqlType, TableColumnDef, TableKeyDesc, TableKeyKind,
};
