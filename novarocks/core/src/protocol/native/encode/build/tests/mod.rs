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

use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::super::boundary_schema::{BoundaryKind, BoundarySchemaColumn, project_boundary_reports};
use super::*;
use crate::connector::ConnectorRegistry;
use novarocks_spi::connector::{
    ConnectorControlResolver, ConnectorInstanceId, ConnectorReadSelector, ConnectorTableIdentity,
    ConnectorTableRequest, ConnectorTableResolution,
};
use novarocks_sql::catalog::ResolvedAnalyzerTable;
use novarocks_sql::plan_read::{DistributedNodeKind, DistributedPlan};
use novarocks_sql::test_support::native_scan_fixture_binding;

/// Build-only tests model application admission explicitly: the SQL fixture
/// supplies a copied identity snapshot, and Core creates the request-local
/// provider binding. No test receives a mutable SQL plan or source carrier.
pub(super) fn fixture_query_table_bindings(
    plan: &DistributedPlan,
    controls: &crate::connector::FixtureControlResolver,
) -> Option<crate::query_execution::planning::bindings::QueryTableBindingStore> {
    use crate::query_execution::planning::bindings::{
        QueryScanMaterialization, QueryTableBinding, QueryTableBindingKey, QueryTableBindingStore,
    };

    let scan = plan
        .fragments()
        .iter()
        .find_map(|fragment| match &fragment.root.payload {
            DistributedNodeKind::Scan(scan) => Some(scan),
            _ => None,
        })?;
    let fixture = native_scan_fixture_binding(plan)?;
    let store = QueryTableBindingStore::try_new_with_scope_for_test(
        NonZeroU64::new(1).expect("fixture scope"),
    );
    let planning_lease = controls
        .acquire_current(
            &ConnectorInstanceId::parse(&fixture.catalog)
                .expect("fixture catalog must be a valid connector instance"),
        )
        .ok();
    if planning_lease.is_none() && fixture.is_delta {
        // Resolver-only negative tests deliberately omit connector admission so
        // they can assert resolver failure before generic read planning.
        return Some(store);
    }
    let planner = scan.table.clone();

    store
        .resolve_or_insert_with_id(
            QueryTableBindingKey::strict_base(
                &fixture.catalog,
                &fixture.namespace,
                &fixture.table,
            ),
            |_| {
                let lease = planning_lease.clone().ok_or_else(|| {
                    "build fixture must acquire an exact connector lease".to_string()
                })?;
                let metadata = lease
                    .binding()
                    .metadata()
                    .load_table(ConnectorTableRequest {
                        table: ConnectorTableIdentity {
                            instance_id: ConnectorInstanceId::parse(&fixture.catalog)
                                .expect("fixture catalog must be valid"),
                            namespace: Arc::from(fixture.namespace.as_str()),
                            table: Arc::from(fixture.table.as_str()),
                        },
                        resolution: ConnectorTableResolution::StrictBaseTable,
                        context: crate::connector::test_request_context(),
                    })
                    .map_err(|error| error.to_string())?;
                Ok(QueryTableBinding {
                    resolved: ResolvedAnalyzerTable::from_planner(
                        Some(&fixture.catalog),
                        &fixture.namespace,
                        planner.clone(),
                    ),
                    statistics_pin: None,
                    admission:
                        crate::query_execution::planning::bindings::QueryTableBindingAdmission::Exact(
                            lease.clone(),
                        ),
                    scan_materialization: Some(QueryScanMaterialization {
                        table: metadata.table,
                        schema: metadata.schema,
                        selector: ConnectorReadSelector::Current,
                        statistics_pin: None,
                        planning_lease: lease,
                    }),
                    write_target_admission: None,
                    mv_target_read: None,
                    frozen_snapshot_materializations: std::collections::BTreeMap::new(),
                    admitted_change_scans: std::collections::BTreeMap::new(),
                })
            },
        )
        .expect("fixture query binding");
    Some(store)
}

mod boundary;
mod preparation;
mod scan;
mod topology;
