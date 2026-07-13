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

//! Physical plan layer — converts [`LogicalPlanNode`] into native execution plans.
//!
//! This layer allocates physical resources (tuple_id, slot_id, node_id),
//! compiles expressions, and assembles the plan structures expected by the
//! pipeline executor.

pub(crate) mod agg_type_infer;
pub(crate) mod boundary_schema;
pub(crate) mod connector_scan_planning;
pub(crate) mod fragment_builder;
pub(crate) mod fragment_request;
pub(crate) mod iceberg_delta_scan_planning;
pub(crate) mod iceberg_literal_json;
pub(crate) mod ir;
pub(crate) mod proto_encode;
pub(crate) mod runtime_filter;
pub(crate) mod scalar_materialize;

use arrow::datatypes::DataType;

use super::analysis::cte::CteId;
use super::column_id::ColumnId;
use crate::sql::planner::distributed::{FragmentEdge, FragmentId};

pub(crate) use fragment_request::FragmentBuildRequest;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

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
    pub boundary_schemas: Vec<boundary_schema::BoundarySchemaReport>,
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
    pub boundary_schemas: Vec<boundary_schema::BoundarySchemaReport>,
    /// Runtime filter planning result (populated for standalone mode).
    pub rf_plan: Option<RuntimeFilterPlanResult>,
}

/// Result of lowering runtime-filter annotations to execution wiring.
///
/// Assembled by [`fragment_builder::PlanFragmentBuilder`] directly from the
/// planner-side runtime-filter annotations on the distributed plan. Consumed by
/// the execution coordinator (`setup_runtime_filter_params`).
pub(crate) struct RuntimeFilterPlanResult {
    /// filter_id -> native RF descriptor for coordinator-side wiring.
    pub all_filters:
        std::collections::HashMap<i32, crate::sql::codegen::runtime_filter::PlannedRuntimeFilter>,
    /// fragment_id -> build-side filter IDs in that fragment.
    pub build_side_filters: std::collections::HashMap<FragmentId, Vec<i32>>,
    /// fragment_id -> (filter_id, probe_target_node_id) for probe-side targets.
    pub probe_side_filters: std::collections::HashMap<FragmentId, Vec<(i32, i32)>>,
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::proto_encode::types::decode_type;
    use crate::proto::common;

    #[test]
    fn proto_type_decode_is_available_to_sibling_lowering_modules() {
        let desc = common::TypeDesc {
            kind: Some(common::type_desc::Kind::Scalar(common::ScalarType {
                r#type: common::PrimitiveType::Int as i32,
                len: None,
                precision: None,
                scale: None,
                time_unit: None,
            })),
        };

        assert_eq!(
            decode_type(&desc).expect("decode int TypeDesc"),
            DataType::Int32
        );
    }
}
