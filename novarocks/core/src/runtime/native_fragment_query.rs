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

//! Narrow query-runtime services consumed by the native backend adapter.
//!
//! This facade deliberately exposes only the operations needed to compose a
//! native fragment around the protocol-neutral fragment kernel. The underlying
//! query manager remains private to core.

use std::sync::Arc;
use std::time::Duration;

use crate::cache::CacheOptions;
use crate::common::types::UniqueId;
use crate::runtime::fragment::FragmentPrepareContext;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::Profiler;
use crate::runtime::query_context::{
    FragmentFinishReportDecision, QueryContextManager, QueryId, query_context_manager,
};
use crate::runtime_filter::service::NativeRuntimeFilterExecutionContext;

#[derive(Clone)]
pub struct NativeFragmentQueryRuntime {
    manager: Arc<QueryContextManager>,
}

impl NativeFragmentQueryRuntime {
    pub fn global() -> Self {
        Self {
            manager: query_context_manager(),
        }
    }

    pub fn prepare_admission(
        &self,
        query_id: QueryId,
        fragment_instance_id: UniqueId,
        delivery_expire: Duration,
        query_expire: Duration,
        cache_options: CacheOptions,
        has_runtime_filter_bindings: bool,
    ) -> Result<NativeFragmentAdmissionResources, String> {
        self.manager
            .ensure_native_context(query_id, false, delivery_expire, query_expire)?;
        let runtime_filter = if has_runtime_filter_bindings {
            Some(
                self.manager
                    .runtime_filter_context_for_native_execution(query_id, fragment_instance_id)?,
            )
        } else {
            None
        };
        self.manager.set_cache_options(query_id, cache_options)?;
        let query_mem_tracker = self
            .manager
            .query_mem_tracker(query_id)
            .ok_or_else(|| "QueryContext missing mem_tracker".to_string())?;
        let fragment_label = format!(
            "fragment_{:x}_{:x}",
            fragment_instance_id.hi, fragment_instance_id.lo
        );
        let fragment_mem_tracker = MemTracker::new_child(fragment_label, &query_mem_tracker);
        Ok(NativeFragmentAdmissionResources {
            query_mem_tracker,
            fragment_mem_tracker,
            runtime_filter,
        })
    }

    pub fn register_fragment(
        &self,
        query_id: QueryId,
        fragment_instance_id: UniqueId,
        delivery_expire: Duration,
        query_expire: Duration,
    ) -> Result<NativeFragmentRegistrationLease, String> {
        self.manager
            .get_or_register_native(query_id, false, delivery_expire, query_expire)?;
        self.manager.register_finst(fragment_instance_id, query_id);
        Ok(NativeFragmentRegistrationLease {
            runtime: self.clone(),
            query_id,
            fragment_instance_id,
            active: true,
        })
    }

    pub fn cancel_query(&self, query_id: QueryId, reason: String) -> Vec<UniqueId> {
        self.manager.cancel_query(query_id, reason)
    }

    pub fn finish_fragment_for_report(&self, query_id: QueryId) -> NativeFragmentReportDecision {
        NativeFragmentReportDecision {
            inner: self.manager.finish_fragment_for_report(query_id),
        }
    }

    pub fn unregister_fragment(&self, fragment_instance_id: UniqueId) {
        self.manager.unregister_finst(fragment_instance_id);
    }

    pub fn cleanup_after_fragment_report(
        &self,
        query_id: QueryId,
        decision: NativeFragmentReportDecision,
    ) {
        self.manager
            .cleanup_after_fragment_report(query_id, decision.inner);
    }
}

pub struct NativeFragmentRegistrationLease {
    runtime: NativeFragmentQueryRuntime,
    query_id: QueryId,
    fragment_instance_id: UniqueId,
    active: bool,
}

impl NativeFragmentRegistrationLease {
    pub fn into_running(mut self) {
        self.active = false;
    }
}

impl Drop for NativeFragmentRegistrationLease {
    fn drop(&mut self) {
        if self.active {
            let _ = self
                .runtime
                .manager
                .rollback_pre_ready_native_fragment(self.query_id, self.fragment_instance_id);
            self.active = false;
        }
    }
}

pub struct NativeFragmentAdmissionResources {
    query_mem_tracker: Arc<MemTracker>,
    fragment_mem_tracker: Arc<MemTracker>,
    runtime_filter: Option<NativeRuntimeFilterExecutionContext>,
}

impl NativeFragmentAdmissionResources {
    pub fn query_mem_tracker(&self) -> Arc<MemTracker> {
        Arc::clone(&self.query_mem_tracker)
    }

    pub fn fragment_mem_tracker(&self) -> Arc<MemTracker> {
        Arc::clone(&self.fragment_mem_tracker)
    }

    pub fn into_prepare_context(self, profiler: Option<Profiler>) -> FragmentPrepareContext {
        FragmentPrepareContext::new(
            profiler,
            Some(self.fragment_mem_tracker),
            self.runtime_filter,
        )
    }
}

pub struct NativeFragmentReportDecision {
    inner: FragmentFinishReportDecision,
}

impl NativeFragmentReportDecision {
    pub fn include_runtime_filter_profile(&self) -> bool {
        self.inner.include_runtime_filter_profile
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::NativeFragmentQueryRuntime;
    use crate::common::types::UniqueId;
    use crate::runtime::query_context::{QueryContextManager, QueryId};

    #[test]
    fn pre_start_registration_lease_drop_rolls_back_only_its_fragment() {
        let manager = QueryContextManager::new_for_test();
        let runtime = NativeFragmentQueryRuntime {
            manager: manager.clone(),
        };
        let query_id = QueryId::new(91_001, 91_002);
        let first = UniqueId { hi: 91_003, lo: 1 };
        let second = UniqueId { hi: 91_003, lo: 2 };

        let first_registration = runtime
            .register_fragment(
                query_id,
                first,
                Duration::from_secs(1),
                Duration::from_secs(5),
            )
            .expect("register first fragment");
        let second_registration = runtime
            .register_fragment(
                query_id,
                second,
                Duration::from_secs(1),
                Duration::from_secs(5),
            )
            .expect("register second fragment");

        drop(second_registration);
        assert_eq!(manager.fragment_counts_for_test(query_id), Some((1, 1)));
        assert_eq!(manager.query_id_by_finst(first), Some(query_id));
        assert_eq!(manager.query_id_by_finst(second), None);

        first_registration.into_running();
    }
}
