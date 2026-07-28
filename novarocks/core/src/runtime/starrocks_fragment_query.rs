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

//! Narrow query-runtime services consumed by the StarRocks compat adapter.
//!
//! The facade keeps `QueryContextManager` and its generation bookkeeping inside
//! core while letting the compat adapter own admission, handoff, report, and
//! cleanup sequencing around the protocol-neutral fragment kernel.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::cache::CacheOptions;
use crate::common::ids::SlotId;
use crate::common::types::UniqueId;
use crate::exec::row_position::RowPositionDescriptor;
use crate::runtime::descriptor_snapshot::DescriptorSnapshot;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::query_context::{
    FragmentFinishReportDecision, LookupFetcherLifecycle, QueryCleanupLease, QueryContextManager,
    QueryExecutionKey, QueryId, StarRocksQueryGeneration, StarRocksQueryHandoff,
    query_context_manager,
};

#[derive(Clone)]
pub struct StarRocksFragmentQueryRuntime {
    manager: Arc<QueryContextManager>,
}

impl StarRocksFragmentQueryRuntime {
    pub fn global() -> Self {
        Self {
            manager: query_context_manager(),
        }
    }

    pub fn prepare_admission(
        &self,
        query_id: QueryId,
        delivery_expire: Duration,
        query_expire: Duration,
        cache_options: CacheOptions,
    ) -> Result<StarRocksFragmentAdmission, String> {
        let query_mem_tracker = self.manager.prepare_starrocks_admission(
            query_id,
            delivery_expire,
            query_expire,
            cache_options,
        )?;
        Ok(StarRocksFragmentAdmission {
            runtime: self.clone(),
            query_id,
            query_mem_tracker,
            active: true,
        })
    }

    pub fn commit_handoff(
        &self,
        handoff: StarRocksFragmentHandoff,
        make_cleanup_lease: impl FnOnce() -> Option<QueryCleanupLease>,
    ) -> Result<Arc<MemTracker>, String> {
        self.manager
            .commit_starrocks_handoff(handoff.inner, make_cleanup_lease)
    }

    pub fn incremental_change_op_slot(
        &self,
        fragment_instance_id: UniqueId,
        node_id: i32,
    ) -> Result<Option<SlotId>, String> {
        self.manager
            .incremental_change_op_slot(fragment_instance_id, node_id)
    }

    pub fn append_incremental_scan_ranges(
        &self,
        fragment_instance_id: UniqueId,
        node_id: i32,
        ranges: Vec<crate::exec::node::scan::IncrementalScanRange>,
    ) -> Result<(), String> {
        self.manager
            .append_incremental_scan_ranges(fragment_instance_id, node_id, ranges)
    }

    pub fn cancel_query(
        &self,
        execution: StarRocksFragmentExecution,
        error: String,
    ) -> Vec<UniqueId> {
        self.manager.cancel_query_execution(execution.inner, error)
    }

    pub fn finish_fragment_for_report(
        &self,
        execution: StarRocksFragmentExecution,
    ) -> StarRocksFragmentReportDecision {
        StarRocksFragmentReportDecision {
            inner: self
                .manager
                .finish_fragment_for_report_execution(execution.inner),
        }
    }

    pub fn unregister_fragment(
        &self,
        fragment_instance_id: UniqueId,
        execution: StarRocksFragmentExecution,
    ) {
        self.manager
            .unregister_finst_execution(fragment_instance_id, execution.inner);
    }

    pub fn cleanup_after_fragment_report(
        &self,
        query_id: QueryId,
        decision: StarRocksFragmentReportDecision,
    ) {
        self.manager
            .cleanup_after_fragment_report(query_id, decision.inner);
    }

    pub fn finish_fragment(&self, execution: StarRocksFragmentExecution) {
        self.manager.finish_fragment_execution(execution.inner);
    }

    pub fn rollback_handoff(
        &self,
        execution: StarRocksFragmentExecution,
        fragment_instance_ids: &[UniqueId],
    ) {
        let rolled_back = self
            .manager
            .rollback_starrocks_handoff(execution.inner, fragment_instance_ids);
        debug_assert!(
            rolled_back,
            "committed StarRocks handoff must be rollbackable before batch start"
        );
    }
}

pub struct StarRocksFragmentAdmission {
    runtime: StarRocksFragmentQueryRuntime,
    query_id: QueryId,
    query_mem_tracker: Arc<MemTracker>,
    active: bool,
}

impl StarRocksFragmentAdmission {
    pub fn query_mem_tracker(&self) -> Arc<MemTracker> {
        Arc::clone(&self.query_mem_tracker)
    }

    pub fn fragment_mem_tracker(&self, fragment_instance_id: UniqueId) -> Arc<MemTracker> {
        MemTracker::new_child(
            format!(
                "fragment_{:x}_{:x}",
                fragment_instance_id.hi, fragment_instance_id.lo
            ),
            &self.query_mem_tracker,
        )
    }
}

impl Drop for StarRocksFragmentAdmission {
    fn drop(&mut self) {
        if self.active {
            self.runtime
                .manager
                .release_starrocks_admission(self.query_id);
            self.active = false;
        }
    }
}

#[derive(Clone, Copy)]
pub struct StarRocksFragmentExecution {
    inner: QueryExecutionKey,
}

impl StarRocksFragmentExecution {
    pub const fn query_id(self) -> QueryId {
        self.inner.query_id()
    }
}

pub struct StarRocksFragmentHandoff {
    inner: StarRocksQueryHandoff,
    execution: StarRocksFragmentExecution,
}

impl StarRocksFragmentHandoff {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        query_id: QueryId,
        generation: u64,
        delivery_expire: Duration,
        query_expire: Duration,
        cache_options: CacheOptions,
        descriptor_snapshot: Option<Arc<DescriptorSnapshot>>,
        total_fragments: Option<usize>,
        row_pos_descs: HashMap<i32, RowPositionDescriptor>,
        lookup_fetchers: HashMap<i32, LookupFetcherLifecycle>,
        instances: Vec<(UniqueId, HashMap<i32, Option<SlotId>>)>,
    ) -> Result<Self, String> {
        let generation = StarRocksQueryGeneration::new(generation)?;
        let execution = QueryExecutionKey::starrocks(query_id, generation);
        Ok(Self {
            inner: StarRocksQueryHandoff {
                execution,
                delivery_expire,
                query_expire,
                fragment_count: instances.len(),
                cache_options,
                descriptor_snapshot,
                total_fragments,
                row_pos_descs,
                lookup_fetchers,
                instances,
            },
            execution: StarRocksFragmentExecution { inner: execution },
        })
    }

    pub const fn execution(&self) -> StarRocksFragmentExecution {
        self.execution
    }

    pub const fn query_id(&self) -> QueryId {
        self.execution.query_id()
    }

    pub const fn delivery_expire(&self) -> Duration {
        self.inner.delivery_expire
    }

    pub const fn query_expire(&self) -> Duration {
        self.inner.query_expire
    }

    pub fn cache_options(&self) -> CacheOptions {
        self.inner.cache_options.clone()
    }
}

pub struct StarRocksFragmentReportDecision {
    inner: FragmentFinishReportDecision,
}

impl StarRocksFragmentReportDecision {
    pub const fn include_runtime_filter_profile(&self) -> bool {
        self.inner.include_runtime_filter_profile
    }
}
