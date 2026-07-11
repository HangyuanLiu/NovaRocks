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

//! Feature-neutral entry point for the native fragment builder.

use crate::sql::codegen::{FragmentBuildRequest, MultiFragmentBuildResult};

pub(crate) struct PlanFragmentBuilder;

impl PlanFragmentBuilder {
    pub(crate) fn build(
        request: FragmentBuildRequest<'_>,
    ) -> Result<MultiFragmentBuildResult, String> {
        crate::sql::codegen::ir::lower_distributed_plan(
            request.distributed_plan,
            request.catalog,
            request.connectors,
            request.mv_refresh_ctx,
        )
    }
}
