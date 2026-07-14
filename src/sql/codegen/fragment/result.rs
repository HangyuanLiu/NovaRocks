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

use arrow::datatypes::DataType;

use super::boundary_schema::BoundarySchemaReport;
use super::runtime_filter::PlannedRuntimeFilter;
use crate::sql::analysis::cte::CteId;
use crate::sql::column_id::ColumnId;
use crate::sql::planner::distributed::{FragmentEdge, FragmentId};

#[derive(Clone, Debug)]
pub(crate) struct OutputColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FragmentOutputKind {
    Result,
    TerminalWrite,
    NonTerminal,
}

impl FragmentOutputKind {
    pub(crate) fn is_terminal_write(self) -> bool {
        matches!(self, FragmentOutputKind::TerminalWrite)
    }
}

/// Native per-fragment metadata used by scheduling and coordinator routing.
/// This intentionally contains no Thrift plan, descriptor, sink, or exec-param
/// payload.
#[derive(Clone, Debug)]
pub(crate) struct FragmentSchedulingMetadata {
    pub fragment_id: FragmentId,
    pub has_scan_nodes: bool,
    pub output_kind: FragmentOutputKind,
    pub native_scan_ranges:
        std::collections::BTreeMap<i32, Vec<crate::runtime::scan_range::ScanRangeParams>>,
    pub output_columns: Vec<OutputColumn>,
    pub cte_id: Option<CteId>,
    pub cte_exchange_nodes: Vec<(CteId, i32, Vec<ColumnId>)>,
}

pub(crate) struct MultiFragmentBuildResult {
    pub fragment_schedules: Vec<FragmentSchedulingMetadata>,
    pub native_fragments: std::collections::BTreeMap<FragmentId, crate::proto::plan::PlanFragment>,
    /// Which fragment is the root (result sink).
    pub root_fragment_id: FragmentId,
    /// Fragment-to-fragment data edges.
    pub edges: Vec<FragmentEdge>,
    /// Read-only projection of the sealed planner boundary catalog, in the
    /// planner's canonical derivation order (one report per boundary contract).
    /// See `boundary_schema::project_boundary_reports`.
    pub boundary_schemas: Vec<BoundarySchemaReport>,
    /// Runtime filter planning result (populated for standalone mode).
    pub rf_plan: Option<RuntimeFilterPlanResult>,
}

/// Result of lowering runtime-filter annotations to execution wiring.
///
/// Assembled directly from the planner-side runtime-filter annotations on the
/// distributed plan. Consumed by the execution coordinator
/// (`setup_runtime_filter_params`).
pub(crate) struct RuntimeFilterPlanResult {
    /// filter_id -> native RF descriptor for coordinator-side wiring.
    pub all_filters: std::collections::HashMap<i32, PlannedRuntimeFilter>,
    /// fragment_id -> build-side filter IDs in that fragment.
    pub build_side_filters: std::collections::HashMap<FragmentId, Vec<i32>>,
    /// fragment_id -> (filter_id, probe_target_node_id) for probe-side targets.
    pub probe_side_filters: std::collections::HashMap<FragmentId, Vec<(i32, i32)>>,
}
