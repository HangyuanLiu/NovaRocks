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
//! Native catalog-reachability wire projection.

use std::collections::BTreeSet;

use novarocks_proto_codec::catalog::{PruneCatalogsRequest, PruneCatalogsResponse};
use novarocks_proto_models::catalog;
use novarocks_spi::connector::CatalogHandle;
use novarocks_worker::CatalogPruneResult;

/// The BE-local catalog lease owner. It receives an already wire-valid,
/// complete reachability snapshot and remains the sole authority that may
/// revoke a process-local catalog runtime.
pub trait CatalogReachabilityAuthority: Send + Sync + 'static {
    fn prune_unreachable_catalogs(&self, reachable: BTreeSet<CatalogHandle>) -> CatalogPruneResult;
}

const STALE_SNAPSHOT_DETAIL: &str = "catalog reachability snapshot omits one or more live catalogs";

/// Decodes a prune request, invokes its BE-local authority, and projects the
/// verdict back to the closed Native response vocabulary.
pub fn handle_prune_catalogs(
    authority: &dyn CatalogReachabilityAuthority,
    raw: catalog::PruneCatalogsRequest,
) -> Result<catalog::PruneCatalogsResponse, tonic::Status> {
    let request = PruneCatalogsRequest::parse(raw)
        .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
    let reachable = request
        .reachable_catalogs()
        .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?
        .into_iter()
        .collect();
    let response = match authority.prune_unreachable_catalogs(reachable) {
        CatalogPruneResult::Pruned { .. } => PruneCatalogsResponse::accepted(),
        CatalogPruneResult::Rejected { .. } => {
            PruneCatalogsResponse::rejected(STALE_SNAPSHOT_DETAIL)
                .expect("the fixed stale-snapshot detail is a bounded safe detail")
        }
    };
    Ok(response.as_proto().clone())
}
