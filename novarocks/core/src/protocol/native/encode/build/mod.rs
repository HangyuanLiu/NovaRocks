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

//! Test-only full-plan encoder fixtures.

use super::NativeFragmentBundle;
use super::boundary_schema::{BoundarySchemaReport, project_boundary_reports};
use crate::connector::ConnectorRegistry;
use crate::query_execution::preparation::scan::ScanBindingResolver;
use crate::query_execution::preparation::{PreparedFragmentSet, prepare_fragments};
use crate::sql::catalog::PlannerTableProvider;
use crate::sql::planner::distributed::DistributedPlan;

struct TestBuildRequest<'a> {
    distributed_plan: &'a DistributedPlan,
    catalog: &'a dyn PlannerTableProvider,
    connectors: &'a ConnectorRegistry,
    scan_binding_resolver: Option<&'a dyn ScanBindingResolver>,
}

impl<'a> TestBuildRequest<'a> {
    fn result(
        distributed_plan: &'a DistributedPlan,
        catalog: &'a dyn PlannerTableProvider,
        connectors: &'a ConnectorRegistry,
        scan_binding_resolver: Option<&'a dyn ScanBindingResolver>,
    ) -> Self {
        Self {
            distributed_plan,
            catalog,
            connectors,
            scan_binding_resolver,
        }
    }
}

fn build_for_test(
    request: TestBuildRequest<'_>,
) -> Result<
    (
        PreparedFragmentSet,
        NativeFragmentBundle,
        Vec<BoundarySchemaReport>,
    ),
    String,
> {
    let _ = request.catalog;
    let controls = crate::connector::LegacyFixtureControlResolver::new(request.connectors.clone());
    let prepared = prepare_fragments(
        request.distributed_plan,
        request.connectors,
        &controls,
        &crate::connector::test_request_context(),
        request.scan_binding_resolver,
    )?;
    let native_bundle = super::encode_native_fragment_bundle(request.distributed_plan, &prepared)?;
    Ok((
        prepared,
        native_bundle,
        project_boundary_reports(request.distributed_plan),
    ))
}

#[cfg(test)]
mod tests;
