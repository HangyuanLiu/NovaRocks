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

use std::sync::Arc;

use crate::runtime::execution_services::ExecutionServices;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::Profiler;
use crate::runtime::query_context::QueryContextManager;

pub(crate) struct FragmentLaunchContext {
    query_context_manager: Arc<QueryContextManager>,
    profiler: Option<Profiler>,
    mem_tracker: Arc<MemTracker>,
    execution_services: &'static ExecutionServices,
}

impl FragmentLaunchContext {
    pub(crate) fn new(
        query_context_manager: Arc<QueryContextManager>,
        profiler: Option<Profiler>,
        mem_tracker: Arc<MemTracker>,
        execution_services: &'static ExecutionServices,
    ) -> Self {
        Self {
            query_context_manager,
            profiler,
            mem_tracker,
            execution_services,
        }
    }

    pub(crate) fn query_context_manager(&self) -> &Arc<QueryContextManager> {
        &self.query_context_manager
    }

    pub(crate) fn profiler(&self) -> Option<&Profiler> {
        self.profiler.as_ref()
    }

    pub(crate) fn mem_tracker(&self) -> &Arc<MemTracker> {
        &self.mem_tracker
    }

    pub(crate) fn execution_services(&self) -> &'static ExecutionServices {
        self.execution_services
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::FragmentLaunchContext;
    use crate::runtime::execution_services::execution_services;
    use crate::runtime::mem_tracker::MemTracker;
    use crate::runtime::query_context::query_context_manager;

    #[test]
    fn preserves_caller_supplied_resource_identity_without_profiler() {
        let manager = query_context_manager();
        let tracker = MemTracker::new_root("fragment-launch-context-test");
        let services = execution_services().expect("execution services");
        let context =
            FragmentLaunchContext::new(Arc::clone(&manager), None, Arc::clone(&tracker), services);

        assert!(Arc::ptr_eq(context.query_context_manager(), &manager));
        assert!(Arc::ptr_eq(context.mem_tracker(), &tracker));
        assert!(std::ptr::eq(context.execution_services(), services));
        assert!(context.profiler().is_none());
    }
}
