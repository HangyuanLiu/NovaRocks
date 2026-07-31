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

//! Backend fragment-decode boundary.
//!
//! This value owns the production request surface.  Its current bridge keeps
//! the mature core decoder reachable while expression/node/scan/sink decoder
//! modules are migrated without changing validation order or error text.

use std::sync::Arc;
use std::time::Duration;

use novarocks::cache::CacheOptions;
use novarocks::common::types::UniqueId;
use novarocks::connector::ConnectorRegistry;
use novarocks::query_execution::lifecycle::QueryExecutionId;
use novarocks::runtime::endpoint::RuntimeEndpoint;
use novarocks::runtime::fragment::submission::FragmentSubmission;
use novarocks::runtime::query_context::QueryId;
use novarocks_protocol::{novarocks as proto, plan};
use novarocks_spi::connector::ConnectorExecutionResolver;

use super::ingress::NativeFragmentIngressError;

pub(crate) struct NativeFragmentRequest {
    inner: novarocks::service::native_fragment_ingress::NativeFragmentRequest,
}

pub(crate) fn decode_native_query_execution_id(
    execution_id: &proto::QueryExecutionId,
) -> Result<QueryExecutionId, NativeFragmentIngressError> {
    novarocks::service::native_fragment_ingress::decode_native_query_execution_id(execution_id)
        .map_err(NativeFragmentIngressError::new)
}

impl NativeFragmentRequest {
    pub(crate) fn try_decode(
        execution_id: QueryExecutionId,
        fragment: plan::PlanFragment,
        instance_params: proto::InstanceParams,
        connectors: Arc<ConnectorRegistry>,
    ) -> Result<Self, NativeFragmentIngressError> {
        novarocks::service::native_fragment_ingress::NativeFragmentRequest::try_decode(
            execution_id,
            fragment,
            instance_params,
            connectors,
        )
        .map(|inner| Self { inner })
        .map_err(NativeFragmentIngressError::new)
    }

    pub(crate) fn try_decode_with_execution_resolver(
        execution_id: QueryExecutionId,
        fragment: plan::PlanFragment,
        instance_params: proto::InstanceParams,
        connectors: Arc<ConnectorRegistry>,
        execution_resolver: Arc<dyn ConnectorExecutionResolver>,
    ) -> Result<Self, NativeFragmentIngressError> {
        novarocks::service::native_fragment_ingress::NativeFragmentRequest::try_decode_with_execution_resolver(
            execution_id, fragment, instance_params, connectors, execution_resolver,
        )
        .map(|inner| Self { inner })
        .map_err(NativeFragmentIngressError::new)
    }

    pub(crate) const fn execution_id(&self) -> QueryExecutionId {
        self.inner.execution_id()
    }
    pub(crate) const fn query_id(&self) -> QueryId {
        self.inner.query_id()
    }
    pub(crate) const fn fragment_instance_id(&self) -> UniqueId {
        self.inner.fragment_instance_id()
    }
    pub(crate) const fn backend_num(&self) -> i32 {
        self.inner.backend_num()
    }
    pub(crate) fn report_endpoint(&self) -> Option<&RuntimeEndpoint> {
        self.inner.report_endpoint()
    }
    pub(crate) fn enable_profile(&self) -> bool {
        self.inner.enable_profile()
    }
    pub(crate) fn runtime_profile_report_interval_seconds(&self) -> Option<i64> {
        self.inner.runtime_profile_report_interval_seconds()
    }
    pub(crate) fn query_expire_durations(&self) -> (Duration, Duration) {
        self.inner.query_expire_durations()
    }
    pub(crate) fn cache_options(&self) -> Result<CacheOptions, NativeFragmentIngressError> {
        self.inner
            .cache_options()
            .map_err(NativeFragmentIngressError::new)
    }
    pub(crate) fn has_runtime_filter_bindings(&self) -> bool {
        self.inner.has_runtime_filter_bindings()
    }
    pub(crate) fn uses_result_sink(&self) -> bool {
        self.inner.uses_result_sink()
    }
    pub(crate) fn root_plan_node_id(&self) -> i32 {
        self.inner.root_plan_node_id()
    }
    pub(crate) fn into_submission(self) -> FragmentSubmission {
        self.inner.into_submission()
    }
}
