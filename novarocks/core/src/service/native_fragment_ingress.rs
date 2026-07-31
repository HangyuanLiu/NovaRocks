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

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use crate::cache::CacheOptions;
use crate::common::types::UniqueId;
use crate::proto;
use crate::protocol::native::decode::{
    decode_fragment_submission_with_connectors_and_execution_resolver, decode_query_execution_id,
};
use crate::query_execution::lifecycle::QueryExecutionId;
use crate::runtime::fragment::submission::FragmentSubmission;
use crate::runtime::query_context::QueryId;
use crate::runtime::query_options::QueryOptions;

pub struct NativeFragmentRequest {
    execution_id: QueryExecutionId,
    submission: FragmentSubmission,
    backend_num: i32,
}

/// Decode the query identity before choosing the query-scoped BE execution
/// resolver. This is transport validation only; it does not resolve or install
/// any Connector binding.
pub fn decode_native_query_execution_id(
    execution_id: &proto::novarocks::QueryExecutionId,
) -> Result<QueryExecutionId, NativeFragmentIngressError> {
    decode_query_execution_id(execution_id).map_err(NativeFragmentIngressError::new)
}

impl NativeFragmentRequest {
    pub fn try_decode_wire(
        execution_id: proto::novarocks::QueryExecutionId,
        fragment: proto::plan::PlanFragment,
        instance_params: proto::novarocks::InstanceParams,
        connectors: Arc<crate::connector::ConnectorRegistry>,
    ) -> Result<Self, NativeFragmentIngressError> {
        Self::try_decode_wire_with_execution_resolver(
            execution_id,
            fragment,
            instance_params,
            connectors,
            Arc::new(MissingExecutionResolver),
        )
    }

    pub fn try_decode_wire_with_execution_resolver(
        execution_id: proto::novarocks::QueryExecutionId,
        fragment: proto::plan::PlanFragment,
        instance_params: proto::novarocks::InstanceParams,
        connectors: Arc<crate::connector::ConnectorRegistry>,
        execution_resolver: Arc<dyn novarocks_spi::connector::ConnectorExecutionResolver>,
    ) -> Result<Self, NativeFragmentIngressError> {
        let execution_id =
            decode_query_execution_id(&execution_id).map_err(NativeFragmentIngressError::new)?;
        Self::try_decode_with_execution_resolver(
            execution_id,
            fragment,
            instance_params,
            connectors,
            execution_resolver,
        )
    }

    pub fn try_decode(
        execution_id: QueryExecutionId,
        fragment: proto::plan::PlanFragment,
        instance_params: proto::novarocks::InstanceParams,
        connectors: Arc<crate::connector::ConnectorRegistry>,
    ) -> Result<Self, NativeFragmentIngressError> {
        Self::try_decode_with_execution_resolver(
            execution_id,
            fragment,
            instance_params,
            connectors,
            Arc::new(MissingExecutionResolver),
        )
    }

    pub fn try_decode_with_execution_resolver(
        execution_id: QueryExecutionId,
        fragment: proto::plan::PlanFragment,
        instance_params: proto::novarocks::InstanceParams,
        connectors: Arc<crate::connector::ConnectorRegistry>,
        execution_resolver: Arc<dyn novarocks_spi::connector::ConnectorExecutionResolver>,
    ) -> Result<Self, NativeFragmentIngressError> {
        let decoded = decode_fragment_submission_with_connectors_and_execution_resolver(
            &fragment,
            &instance_params,
            connectors,
            execution_resolver,
        )
        .map_err(NativeFragmentIngressError::new)?;
        let (submission, metadata) = decoded.into_parts();
        if execution_id.query_id().high() != submission.instance().query_id().hi()
            || execution_id.query_id().low() != submission.instance().query_id().lo()
        {
            return Err(NativeFragmentIngressError::new(
                "native fragment execution_id query_id does not match instance_params query_id",
            ));
        }
        debug_assert_eq!(
            metadata.typed_result_sink(),
            submission.instance().runtime_options().typed_result_sink()
        );
        debug_assert_eq!(
            metadata.backend_num(),
            submission.instance().backend_num().get()
        );
        Ok(Self {
            execution_id,
            submission,
            backend_num: metadata.backend_num(),
        })
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub const fn query_id(&self) -> QueryId {
        self.submission.instance().query_id()
    }

    pub const fn fragment_instance_id(&self) -> UniqueId {
        self.submission.instance().fragment_instance_id().get()
    }

    pub const fn backend_num(&self) -> i32 {
        self.backend_num
    }

    pub fn enable_profile(&self) -> bool {
        self.query_options().enable_profile
    }

    pub fn runtime_profile_report_interval_seconds(&self) -> Option<i64> {
        self.query_options().runtime_profile_report_interval
    }

    pub fn query_expire_durations(&self) -> (Duration, Duration) {
        crate::runtime::query_options::query_expire_durations(Some(self.query_options()))
    }

    pub fn cache_options(&self) -> Result<CacheOptions, NativeFragmentIngressError> {
        CacheOptions::from_query_options(Some(self.query_options()))
            .map_err(NativeFragmentIngressError::new)
    }

    pub fn has_runtime_filter_bindings(&self) -> bool {
        self.submission.program().runtime_filters().has_bindings()
    }

    pub fn uses_result_sink(&self) -> bool {
        self.submission.program().sink().kind()
            == crate::exec::fragment::program::FragmentSinkKind::Result
    }

    pub fn root_plan_node_id(&self) -> i32 {
        self.submission.program().root_plan_node_id().get()
    }

    pub fn into_submission(self) -> FragmentSubmission {
        self.submission
    }

    fn query_options(&self) -> &QueryOptions {
        self.submission.instance().runtime_options().query_options()
    }
}

struct MissingExecutionResolver;

impl novarocks_spi::connector::ConnectorExecutionResolver for MissingExecutionResolver {
    fn resolve(
        &self,
        _key: &novarocks_spi::connector::ConnectorExecutionBindingKey,
    ) -> Result<
        Arc<novarocks_spi::connector::ConnectorExecutionBinding>,
        novarocks_spi::connector::ConnectorError,
    > {
        Err(novarocks_spi::connector::ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Unavailable,
            "native ConnectorReadSource execution resolver is not configured",
        ))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeFragmentCancelRequest {
    query_id: QueryId,
    fragment_instance_ids: Vec<UniqueId>,
    reason: String,
}

impl NativeFragmentCancelRequest {
    pub fn new(
        query_id: QueryId,
        fragment_instance_ids: Vec<UniqueId>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            query_id,
            fragment_instance_ids,
            reason: reason.into(),
        }
    }

    pub const fn query_id(&self) -> QueryId {
        self.query_id
    }

    pub fn fragment_instance_ids(&self) -> &[UniqueId] {
        &self.fragment_instance_ids
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeFragmentIngressError {
    message: String,
}

impl NativeFragmentIngressError {
    pub fn new(error: impl fmt::Display) -> Self {
        Self {
            message: error.to_string(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for NativeFragmentIngressError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for NativeFragmentIngressError {}

pub trait NativeFragmentIngress: Send + Sync + 'static {
    fn ensure_connector_execution_binding(
        &self,
        _execution_id: QueryExecutionId,
        _declaration: novarocks_spi::connector::ConnectorExecutionDeclaration,
        _context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<(), NativeFragmentIngressError> {
        Err(NativeFragmentIngressError::new(
            "connector binding ingress is not configured",
        ))
    }

    fn retire_connector_execution_binding(
        &self,
        _key: novarocks_spi::connector::ConnectorExecutionBindingKey,
    ) -> Result<(), NativeFragmentIngressError> {
        Err(NativeFragmentIngressError::new(
            "connector binding ingress is not configured",
        ))
    }

    fn cancel(
        &self,
        request: NativeFragmentCancelRequest,
    ) -> Result<(), NativeFragmentIngressError>;
}
