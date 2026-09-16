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

//! Operational bounds on plan structure.
//!
//! These are the limits a real query can actually reach, so each one is a
//! deployment-overridable value rather than a compile-time constant. That
//! distinction matters more than it looks: a limit that is too low is not a
//! slow query, it is a query the engine refuses, and the operator has no way
//! to tell the difference between "this plan is malformed" and "this plan is
//! one conjunct past a number someone picked."
//!
//! Bounds that only fence off values no legitimate plan produces - a pipeline
//! parallelism of a million, a runtime-filter deadline of a day - stay as
//! constants next to the checks that read them. They are guard rails against
//! nonsense, and counting them as protection would overstate what is actually
//! bounded.
//!
//! Every field names the scope it applies to. A per-fragment bound says
//! nothing about a plan, and a per-plan bound says nothing about how many
//! plans a process is validating at once.

/// Structural bounds applied while validating one plan.
///
/// [`PlanLimits::default`] is the set frozen by the UEA-5 design; overriding
/// them is a deployment decision, not a per-query one.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlanLimits {
    /// Fragments in one plan.
    pub plan_fragments: usize,
    /// Edges in one plan.
    pub plan_edges: usize,
    /// Runtime filters in one plan.
    pub plan_runtime_filters: usize,
    /// Sealed-artifact references in one plan.
    pub plan_artifact_refs: usize,
    /// Total steps a plan-wide semantic trace may take before it gives up.
    pub plan_semantic_trace_work: usize,

    /// Nodes in one fragment.
    pub fragment_nodes: usize,
    /// Value definitions in one fragment.
    pub fragment_values: usize,
    /// Expression-arena nodes in one fragment.
    pub fragment_expressions: usize,
    /// Nesting depth of one expression, counted in contract terms.
    ///
    /// This is not protobuf nesting, and it is not the number of conditions a
    /// query states: boolean connectives are n-ary precisely so that a wide
    /// predicate stays shallow here.
    pub expression_semantic_depth: usize,

    /// Arena nodes in one runtime-filter coverage set.
    pub runtime_filter_coverage_nodes: usize,
    /// Nesting depth of one runtime-filter coverage set.
    pub runtime_filter_coverage_depth: usize,
    /// Steps one runtime-filter lineage walk may take.
    pub runtime_filter_lineage_steps: usize,
    /// Endpoints one runtime filter may name.
    pub runtime_filter_endpoints: usize,

    /// Mappings in one unpivot node.
    pub unpivot_mappings: usize,
    /// Constants in one unpivot node.
    pub unpivot_constants: usize,
    /// Items in one unpivot constant collection.
    pub unpivot_collection_items: usize,
}

impl PlanLimits {
    /// The bounds frozen by the design. Named so that a caller reading
    /// `PlanLimits::FROZEN` sees that the numbers are a decision, not a
    /// property of the code.
    pub const FROZEN: Self = Self {
        plan_fragments: 16_384,
        plan_edges: 65_536,
        plan_runtime_filters: 65_536,
        plan_artifact_refs: 65_536,
        plan_semantic_trace_work: 1 << 20,

        fragment_nodes: 4_096,
        fragment_values: 65_536,
        fragment_expressions: 262_144,
        expression_semantic_depth: 256,

        runtime_filter_coverage_nodes: 16_384,
        runtime_filter_coverage_depth: 256,
        runtime_filter_lineage_steps: 4_096,
        runtime_filter_endpoints: 4_096,

        unpivot_mappings: 4_096,
        unpivot_constants: 16_384,
        unpivot_collection_items: 4_096,
    };
}

impl Default for PlanLimits {
    fn default() -> Self {
        Self::FROZEN
    }
}
