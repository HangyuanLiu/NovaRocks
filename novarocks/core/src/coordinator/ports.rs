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
use std::time::Duration;

use async_trait::async_trait;

use crate::common::types::UniqueId;
use crate::coordinator::cluster::LiveBackendSnapshot;
use crate::coordinator::dispatch::FragmentDispatcher;
use crate::coordinator::runtime_filter_deployment::{
    DeploymentEpochAllocator, NativeRuntimeFilterDeploymentPolicyProvider,
};
use crate::protocol::native::RuntimeFilterQueryLifecycleOptions;
use crate::runtime::endpoint::RuntimeEndpoint;
use crate::runtime_filter::deployment::RuntimeFilterQueryDeploymentPolicy;
use crate::runtime_filter::model::graph::RuntimeFilterGraph;
use crate::runtime_filter::port::identity::{DeploymentEpoch, RuntimeFilterParticipantId};
use crate::runtime_filter::port::install::RuntimeFilterParticipantInstall;

pub(crate) trait RuntimeFilterDeploymentPolicyProvider: Send + Sync + 'static {
    fn policy_for(
        &self,
        graph: &RuntimeFilterGraph,
        backends: &LiveBackendSnapshot,
    ) -> Result<RuntimeFilterQueryDeploymentPolicy, String>;
}

#[async_trait]
pub(crate) trait RuntimeFilterDeploymentControlPort: Send + Sync + 'static {
    async fn install(
        &self,
        query_id: UniqueId,
        lifecycle: RuntimeFilterQueryLifecycleOptions,
        deadline: Duration,
        participant: RuntimeFilterParticipantId,
        install: RuntimeFilterParticipantInstall,
    ) -> Result<(), String>;

    async fn abort(
        &self,
        query_id: UniqueId,
        epoch: DeploymentEpoch,
        deadline: Duration,
        participant: RuntimeFilterParticipantId,
    ) -> Result<(), String>;
}

pub(crate) trait CoordinatorObserver: Send + Sync + 'static {
    fn fragment_scheduled(&self);
}

pub(crate) trait CoordinatorReportHandler: Send + Sync + 'static {
    fn handle_exec_status_report(
        &self,
        report: crate::proto::novarocks::ExecStatusReport,
    ) -> Result<(), crate::common::engine_error::EngineError>;
}

pub(crate) struct CoordinatorExecutionPorts {
    pub(crate) dispatcher: Arc<dyn FragmentDispatcher>,
    pub(crate) report_endpoint: RuntimeEndpoint,
    pub(crate) observer: Arc<dyn CoordinatorObserver>,
    pub(crate) runtime_filter_policy_provider: Arc<dyn RuntimeFilterDeploymentPolicyProvider>,
    pub(crate) deployment_epoch_allocator: DeploymentEpochAllocator,
    pub(crate) runtime_filter_deployment_control: Arc<dyn RuntimeFilterDeploymentControlPort>,
}

impl CoordinatorExecutionPorts {
    pub(crate) fn new(
        dispatcher: Arc<dyn FragmentDispatcher>,
        report_endpoint: RuntimeEndpoint,
        observer: Arc<dyn CoordinatorObserver>,
        runtime_filter_deployment_control: Arc<dyn RuntimeFilterDeploymentControlPort>,
    ) -> Self {
        Self {
            dispatcher,
            report_endpoint,
            observer,
            runtime_filter_policy_provider: Arc::new(
                NativeRuntimeFilterDeploymentPolicyProvider::new(
                    crate::common::config::data_runtime_worker_threads(),
                ),
            ),
            deployment_epoch_allocator: DeploymentEpochAllocator,
            runtime_filter_deployment_control,
        }
    }
}

#[cfg(test)]
pub(crate) struct RejectingTestRuntimeFilterDeploymentControl;

#[cfg(test)]
#[async_trait]
impl RuntimeFilterDeploymentControlPort for RejectingTestRuntimeFilterDeploymentControl {
    async fn install(
        &self,
        _query_id: UniqueId,
        _lifecycle: RuntimeFilterQueryLifecycleOptions,
        _deadline: Duration,
        _participant: RuntimeFilterParticipantId,
        _install: RuntimeFilterParticipantInstall,
    ) -> Result<(), String> {
        Err("test coordinator did not inject a runtime filter deployment control".to_string())
    }

    async fn abort(
        &self,
        _query_id: UniqueId,
        _epoch: DeploymentEpoch,
        _deadline: Duration,
        _participant: RuntimeFilterParticipantId,
    ) -> Result<(), String> {
        Err("test coordinator did not inject a runtime filter deployment control".to_string())
    }
}
