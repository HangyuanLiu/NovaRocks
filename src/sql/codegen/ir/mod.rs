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

//! Compatibility exports for the planner-owned DistributedPlan Bridge 2 IR.

pub(crate) mod explain;
pub(crate) mod kind;
#[cfg(feature = "compat")]
pub(crate) mod lowering;
#[cfg(not(feature = "compat"))]
mod lowering_native;

#[cfg(all(test, feature = "compat"))]
pub(crate) mod equiv;

#[cfg(test)]
pub(crate) use crate::sql::planner::{
    DataPartition, DataSink, DistributedNode, DistributedPayload, DistributedPlan, PartitionKind,
    PlanFragment,
};
pub(crate) use explain::{explain_distributed_plan, explain_distributed_plan_analyze};
#[cfg(feature = "compat")]
pub(crate) use lowering::{lower_distributed_plan, refresh_distributed_plan_for_native_sidecar};
#[cfg(not(feature = "compat"))]
pub(crate) use lowering_native::{
    lower_distributed_plan, refresh_distributed_plan_for_native_sidecar,
};

#[cfg(test)]
mod tests {
    use std::path::Path;

    #[test]
    fn bridge2_owner_modules_are_split_into_files() {
        for module_file in [
            "distributed_fragment.rs",
            "distributed_node.rs",
            "distributed_plan_build.rs",
        ] {
            let path = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("src/sql/planner")
                .join(module_file);
            assert!(path.is_file(), "{} should exist", path.display());
        }
    }
}
