//! Planner-owned physical execution vocabulary.
//!
//! Aggregate phase / TopN phase / join distribution fallback / aggregate output
//! layout, expressed as planner-owned types so `PhysicalPlanNode` payloads carry
//! no `crate::sql::optimizer::*` type. The optimizer keeps its own equivalents;
//! `optimizer_bridge::physical` is the only converter. Enforced by
//! `tests/architecture_guard.rs`.

use crate::sql::column_id::ColumnId;
use crate::sql::common::OutputColumn;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AggMode {
    Single,
    Local,
    Global,
    /// Dedup by distinct-column + merge non-DISTINCT states (shuffle-receive
    /// phase of 3/4-phase DISTINCT aggregation).
    DistinctGlobal,
    /// Per-instance scalar rollup of DistinctGlobal output (4-phase scalar DISTINCT).
    DistinctLocal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub(crate) enum TopNPhase {
    Partial,
    #[default]
    Final,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum JoinDistribution {
    Unknown,
    Shuffle,
    Broadcast,
    Colocate,
}

#[derive(Clone, Debug)]
pub(crate) struct AggregateOutputLayout {
    pub group_key_columns: Vec<OutputColumn>,
    pub aggregate_columns: Vec<OutputColumn>,
}

impl AggregateOutputLayout {
    pub(crate) fn full_output_columns(&self) -> Vec<OutputColumn> {
        self.group_key_columns
            .iter()
            .chain(self.aggregate_columns.iter())
            .cloned()
            .collect()
    }

    pub(crate) fn contains_column_id(&self, column_id: ColumnId) -> bool {
        self.group_key_columns
            .iter()
            .chain(self.aggregate_columns.iter())
            .any(|column| column.column_id == column_id)
    }
}
