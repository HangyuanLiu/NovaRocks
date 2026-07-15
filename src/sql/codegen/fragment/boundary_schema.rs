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

//! Read-only projection of the planner's sealed boundary catalog.
//!
//! The planner (`crate::sql::planner::distributed::boundary`) is the sole owner
//! of boundary membership, occurrence identity, and column provenance:
//! `build_boundary_catalog` derives every boundary seam and numbers each column
//! occurrence with an [`ExecutionColumnId`]. This module does **not** discover
//! or re-select any boundary's logical schema. It only *projects* each sealed
//! [`BoundaryContract`] into the diagnostic [`BoundarySchemaReport`]. Today the
//! sole coordinator consumer (`validate_fragment_schedule_payloads`) reads only
//! each report's `fragment_id`; the projection still copies the planner's column
//! order (ExecutionColumnId occurrence order) and [`ColumnId`] provenance verbatim
//! so CGO-9C can consume the full boundary schema.

use arrow::datatypes::DataType;

use crate::sql::column_id::ColumnId;
use crate::sql::planner::distributed::{
    BoundaryColumn, BoundaryContract, BoundaryKind as PlannerBoundaryKind, DistributedPlan,
    ExecutionColumnId,
};

/// The kind of boundary seam a [`BoundarySchemaReport`] describes. Every variant
/// is the projection of a planner [`PlannerBoundaryKind`]; codegen never invents
/// a boundary kind of its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BoundaryKind {
    ResultRoot,
    ExchangeSender,
    ExchangeReceiver,
    IcebergWriteInput,
    ChangeStreamRouterInput,
}

impl BoundaryKind {
    /// Project the planner boundary kind. This is a total, 1:1 mapping so the
    /// projection is honest: every sealed boundary kind has a codegen variant.
    pub(crate) fn from_planner(kind: PlannerBoundaryKind) -> Self {
        match kind {
            PlannerBoundaryKind::ResultOutput => BoundaryKind::ResultRoot,
            PlannerBoundaryKind::ExchangeSend => BoundaryKind::ExchangeSender,
            PlannerBoundaryKind::ExchangeReceive => BoundaryKind::ExchangeReceiver,
            PlannerBoundaryKind::IcebergWriteInput => BoundaryKind::IcebergWriteInput,
            PlannerBoundaryKind::ChangeStreamRouterInput => BoundaryKind::ChangeStreamRouterInput,
        }
    }
}

#[cfg(test)]
impl BoundaryKind {
    pub(crate) fn label(self) -> &'static str {
        match self {
            BoundaryKind::ResultRoot => "RESULT_ROOT",
            BoundaryKind::ExchangeSender => "EXCHANGE_SEND",
            BoundaryKind::ExchangeReceiver => "EXCHANGE_RECV",
            BoundaryKind::IcebergWriteInput => "ICEBERG_WRITE_INPUT",
            BoundaryKind::ChangeStreamRouterInput => "CHANGE_STREAM_ROUTER_INPUT",
        }
    }
}

/// One boundary column, projected from a planner [`BoundaryColumn`].
///
/// `execution_column_id` and `column_id` are copied straight from the planner
/// occurrence so the report preserves both the query-scoped occurrence identity
/// and the logical [`ColumnId`] provenance. `slot_id` is derived deterministically
/// from the boundary-local occurrence position, preserving the historical 1-based
/// per-boundary slot numbering; it is not an independent selection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BoundarySchemaColumn {
    pub slot_id: i32,
    /// Planner provenance copied verbatim for CGO-9C to consume; not read by the
    /// coordinator today. `execution_column_id` is the query-scoped occurrence
    /// identity, `column_id` the logical column it originates from.
    pub execution_column_id: ExecutionColumnId,
    pub column_id: ColumnId,
    pub name: String,
    pub arrow_type: DataType,
    pub logical_type: Option<String>,
    pub nullable: bool,
}

/// A diagnostic report for one boundary seam, projected from a planner
/// [`BoundaryContract`]. `node_id` mirrors the contract: `Some(exchange_node_id)`
/// for Exchange send/receive seams, `None` for fragment-level sink seams (result,
/// Iceberg write input, change-stream router input).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BoundarySchemaReport {
    pub fragment_id: Option<i32>,
    pub node_id: Option<i32>,
    pub boundary_kind: BoundaryKind,
    pub columns: Vec<BoundarySchemaColumn>,
}

/// Project the sealed boundary catalog into codegen's diagnostic reports, in the
/// planner's canonical derivation order (fragment sink boundaries in fragment
/// order, then per-edge send/receive boundaries in edge order).
///
/// This is a pure, read-only projection: codegen never discovers boundary
/// membership, re-selects columns, or re-numbers occurrences. Every field is
/// copied from a [`BoundaryContract`] / [`BoundaryColumn`].
pub(crate) fn project_boundary_reports(plan: &DistributedPlan) -> Vec<BoundarySchemaReport> {
    plan.boundaries()
        .contracts()
        .iter()
        .map(project_boundary_report)
        .collect()
}

fn project_boundary_report(contract: &BoundaryContract) -> BoundarySchemaReport {
    BoundarySchemaReport {
        fragment_id: Some(contract.fragment_id as i32),
        node_id: contract.node_id,
        boundary_kind: BoundaryKind::from_planner(contract.kind),
        columns: contract
            .columns
            .iter()
            .map(project_boundary_column)
            .collect(),
    }
}

fn project_boundary_column(column: &BoundaryColumn) -> BoundarySchemaColumn {
    BoundarySchemaColumn {
        // Preserve the historical 1-based per-boundary slot numbering derived
        // from the planner's within-boundary occurrence position.
        slot_id: i32::try_from(column.output_ordinal + 1).unwrap_or(i32::MAX),
        execution_column_id: column.execution_column_id,
        column_id: column.column_id,
        name: column.name.clone(),
        arrow_type: column.data_type.clone(),
        logical_type: None,
        nullable: column.nullable,
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::planner::distributed::BoundaryKind as PlannerBoundaryKind;

    use super::BoundaryKind;

    #[test]
    fn planner_boundary_kinds_project_to_codegen_kinds() {
        assert_eq!(
            BoundaryKind::from_planner(PlannerBoundaryKind::ResultOutput),
            BoundaryKind::ResultRoot
        );
        assert_eq!(
            BoundaryKind::from_planner(PlannerBoundaryKind::ExchangeSend),
            BoundaryKind::ExchangeSender
        );
        assert_eq!(
            BoundaryKind::from_planner(PlannerBoundaryKind::ExchangeReceive),
            BoundaryKind::ExchangeReceiver
        );
        assert_eq!(
            BoundaryKind::from_planner(PlannerBoundaryKind::IcebergWriteInput),
            BoundaryKind::IcebergWriteInput
        );
        assert_eq!(
            BoundaryKind::from_planner(PlannerBoundaryKind::ChangeStreamRouterInput),
            BoundaryKind::ChangeStreamRouterInput
        );
    }

    #[test]
    fn boundary_kind_labels_are_stable() {
        assert_eq!(BoundaryKind::ResultRoot.label(), "RESULT_ROOT");
        assert_eq!(BoundaryKind::ExchangeSender.label(), "EXCHANGE_SEND");
        assert_eq!(BoundaryKind::ExchangeReceiver.label(), "EXCHANGE_RECV");
        assert_eq!(
            BoundaryKind::IcebergWriteInput.label(),
            "ICEBERG_WRITE_INPUT"
        );
        assert_eq!(
            BoundaryKind::ChangeStreamRouterInput.label(),
            "CHANGE_STREAM_ROUTER_INPUT"
        );
    }
}
