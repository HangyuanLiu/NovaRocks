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

//! Narrow execution-domain decode port used by the backend role.
//!
//! The port deliberately returns only a fully validated submission and the
//! immutable transport metadata required by backend ownership.  It never
//! exposes decoder contexts, runtime registries, or connector internals.

use std::sync::Arc;

use crate::connector::ConnectorRegistry;
use crate::proto::{novarocks, plan};
use crate::protocol::native::decode::{
    decode_fragment_submission_with_connectors_and_execution_resolver, decode_query_execution_id,
};
use crate::query_execution::lifecycle::QueryExecutionId;
use crate::runtime::endpoint::RuntimeEndpoint;
use crate::runtime::fragment::submission::FragmentSubmission;

pub struct DecodedNativeFragmentSubmission {
    submission: FragmentSubmission,
    backend_num: i32,
    report_endpoint: Option<RuntimeEndpoint>,
}

impl DecodedNativeFragmentSubmission {
    pub fn into_parts(self) -> (FragmentSubmission, i32, Option<RuntimeEndpoint>) {
        (self.submission, self.backend_num, self.report_endpoint)
    }
}

pub fn decode_query_execution_id_for_backend(
    execution_id: &novarocks::QueryExecutionId,
) -> Result<QueryExecutionId, String> {
    decode_query_execution_id(execution_id).map_err(|error| error.to_string())
}

pub fn decode_fragment_submission_for_backend(
    fragment: &plan::PlanFragment,
    instance_params: &novarocks::InstanceParams,
    connectors: Arc<ConnectorRegistry>,
    execution_resolver: Arc<dyn novarocks_spi::connector::ConnectorExecutionResolver>,
) -> Result<DecodedNativeFragmentSubmission, String> {
    let decoded = decode_fragment_submission_with_connectors_and_execution_resolver(
        fragment,
        instance_params,
        connectors,
        execution_resolver,
    )
    .map_err(|error| error.to_string())?;
    let (submission, metadata) = decoded.into_parts();
    Ok(DecodedNativeFragmentSubmission {
        submission,
        backend_num: metadata.backend_num(),
        report_endpoint: metadata.report_endpoint().cloned(),
    })
}
