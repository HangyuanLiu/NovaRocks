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

use crate::coordinator::cluster::LiveBackendSnapshot;
use crate::coordinator::dispatch::FragmentDispatcher;
use crate::coordinator::runtime_filter_deployment::{
    DeploymentEpochAllocator, NativeRuntimeFilterDeploymentPolicyProvider,
};
use crate::runtime::endpoint::RuntimeEndpoint;
use crate::runtime_filter::deployment::RuntimeFilterQueryDeploymentPolicy;
use crate::runtime_filter::model::graph::RuntimeFilterGraph;

pub(crate) trait RuntimeFilterDeploymentPolicyProvider: Send + Sync + 'static {
    fn policy_for(
        &self,
        graph: &RuntimeFilterGraph,
        backends: &LiveBackendSnapshot,
    ) -> Result<RuntimeFilterQueryDeploymentPolicy, String>;
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
}

impl CoordinatorExecutionPorts {
    pub(crate) fn new(
        dispatcher: Arc<dyn FragmentDispatcher>,
        report_endpoint: RuntimeEndpoint,
        observer: Arc<dyn CoordinatorObserver>,
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
        }
    }
}
