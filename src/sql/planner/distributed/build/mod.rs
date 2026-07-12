#![allow(dead_code)]
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

mod fragment_cut;
mod lowering;
mod runtime_filter_binding;

use crate::sql::planner::distributed::DistributedPlan;
use crate::sql::planner::physical::PhysicalPlanNode;

pub(crate) fn build_distributed_plan(plan: &PhysicalPlanNode) -> Result<DistributedPlan, String> {
    let mut cut = fragment_cut::cut(plan)?;
    runtime_filter_binding::bind_runtime_filters(&mut cut.plan.fragments, cut.bindings);
    Ok(cut.plan)
}

pub(crate) fn union_distinct_must_be_rewritten_error() -> &'static str {
    "UNION DISTINCT must be rewritten by UnionDistinctToAggregate before distributed build"
}

#[cfg(test)]
mod tests;
