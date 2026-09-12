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

//! Worker-local policy for one task registry.

use std::time::Duration;

use novarocks_types::identity::BackendProcessId;

use crate::{
    AdmissionTicketConfig, LeaseBounds, METRIC_PUBLISH_MIN_INTERVAL, OperationWaitCaps,
    RequestHorizon,
};

/// The bounds and budgets one Worker task owner runs with.
///
/// The role composition root supplies the two frozen transport capacities.
/// The Worker retains no dependency on a codec or native transport model.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct TaskExecutionRegistryConfig {
    pub backend_process_id: BackendProcessId,
    pub lease_bounds: LeaseBounds,
    pub wait_caps: OperationWaitCaps,
    pub admission_tickets: AdmissionTicketConfig,
    pub request_horizon: RequestHorizon,
    pub metric_publish_min_interval: Duration,
    pub max_tasks_per_context: usize,
    pub max_active_tasks_per_backend: usize,
    pub retained_task_capacity: usize,
    pub retained_task_max_bytes: usize,
    pub retained_context_capacity: usize,
    pub gone_fence_capacity: usize,
    pub termination_grace: Duration,
    pub gate_poll_interval: Duration,
}

impl TaskExecutionRegistryConfig {
    pub fn for_process(
        backend_process_id: BackendProcessId,
        max_tasks_per_context: usize,
        max_active_tasks_per_backend: usize,
    ) -> Self {
        Self {
            backend_process_id,
            lease_bounds: LeaseBounds::DEFAULT,
            wait_caps: OperationWaitCaps::DEFAULT,
            admission_tickets: AdmissionTicketConfig::DEFAULT,
            request_horizon: RequestHorizon::DEFAULT,
            metric_publish_min_interval: METRIC_PUBLISH_MIN_INTERVAL,
            max_tasks_per_context,
            max_active_tasks_per_backend,
            retained_task_capacity: max_tasks_per_context,
            retained_task_max_bytes: 16 * 1024 * 1024,
            retained_context_capacity: 1024,
            gone_fence_capacity: max_tasks_per_context,
            termination_grace: RequestHorizon::DEFAULT.server_wait(),
            gate_poll_interval: Duration::from_millis(50),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TaskExecutionRegistryConfig;
    use novarocks_types::identity::BackendProcessId;

    #[test]
    fn process_policy_uses_only_the_composed_task_capacities() {
        let config = TaskExecutionRegistryConfig::for_process(BackendProcessId::new_v7(), 17, 9);

        assert_eq!(config.max_tasks_per_context, 17);
        assert_eq!(config.max_active_tasks_per_backend, 9);
        assert_eq!(config.retained_task_capacity, 17);
        assert_eq!(config.gone_fence_capacity, 17);
    }
}
