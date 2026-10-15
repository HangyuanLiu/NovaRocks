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

//! Read-only process-local activity. These facts grant no admission, retry,
//! cleanup, or publication authority. Detail follows live owners, not history.

use super::CatalogScopeIdentity;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
#[repr(usize)]
pub enum CatalogActivityPhase {
    MaterializationWaiting,
    MaterializationWorker,
    Bridge,
    PreDispatch,
    TableCoordination,
    /// Prepared mechanism in flight, including its local filesystem steps.
    Dispatch,
    /// Whole evidence interval, overlapping Dispatch; no socket-write completion claim.
    Evidence,
    Finalization,
    SessionInitializer,
    AuthorizationInitializer,
}
impl CatalogActivityPhase {
    pub const ALL: [Self; 10] = [
        Self::MaterializationWaiting,
        Self::MaterializationWorker,
        Self::Bridge,
        Self::PreDispatch,
        Self::TableCoordination,
        Self::Dispatch,
        Self::Evidence,
        Self::Finalization,
        Self::SessionInitializer,
        Self::AuthorizationInitializer,
    ];
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct CatalogPhaseActivity {
    pub phase: CatalogActivityPhase,
    pub inflight: u64,
    pub started: u64,
    pub exited: u64,
    pub elapsed_count: u64,
    pub elapsed_sum_ns: u64,
    pub elapsed_max_ns: u64,
}
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct CatalogActivityEvents {
    pub pre_dispatch_cancelled: u64,
    pub pre_dispatch_timed_out: u64,
    pub evidence_expired: u64,
    pub bridge_spawn_failed: u64,
}
#[derive(Clone, Debug, serde::Serialize)]
pub struct CatalogGenerationActivity {
    /// None while constructing an unpublished generation. Never an invented epoch.
    pub generation: Option<uuid::Uuid>,
    pub phases: Vec<CatalogPhaseActivity>,
    pub events: CatalogActivityEvents,
}
#[derive(Clone, Debug, serde::Serialize)]
pub struct CatalogActivitySnapshot {
    pub provider_id: String,
    pub catalog: String,
    pub source_entry_id: uuid::Uuid,
    /// Fixed-size cumulative counters for this logical scope.
    pub phases: Vec<CatalogPhaseActivity>,
    pub events: CatalogActivityEvents,
    /// Only generations still retained by resources or actual work.
    pub generations: Vec<CatalogGenerationActivity>,
}
pub trait CatalogActivityReader: Send + Sync {
    fn snapshot(&self) -> CatalogActivitySnapshot;
}
/// An absent reader means unsupported observation, never healthy zero activity.
#[derive(Clone)]
pub struct CatalogActivitySource {
    pub identity: CatalogScopeIdentity,
    pub current: bool,
    pub reader: Option<Arc<dyn CatalogActivityReader>>,
}
