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

use crate::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};
use crate::query_execution::lifecycle_plan::QueryInitPlan;
use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_proto::lifecycle::QueryExecutionId;
use novarocks_proto::lifecycle::{ParticipantManifestDigest, ParticipantRole, QueryInitRequest};
use novarocks_proto_models::novarocks as protocol_wire;

use super::QueryLifecycleTarget;

pub(super) struct MaterializedQueryInit {
    pub execution_id: QueryExecutionId,
    pub participants: Vec<MaterializedParticipant>,
}

#[derive(Clone)]
pub(super) struct MaterializedParticipant {
    pub target: QueryLifecycleTarget,
    pub request: QueryInitRequest,
    pub digest: ParticipantManifestDigest,
    pub fragment_participant: bool,
}

pub(super) fn materialize(
    plan: QueryInitPlan,
) -> Result<MaterializedQueryInit, DistributedQueryError> {
    let execution_id = plan.execution_id();
    let mut participants = Vec::with_capacity(plan.participant_count());
    for participant in plan.into_participants() {
        let (backend_idx, backend, manifest, digest) = participant.into_parts();
        let endpoint = backend.endpoint().map_err(|error| {
            contract_error(format!(
                "query lifecycle backend {backend_idx} endpoint is invalid: {error}"
            ))
        })?;
        let target = QueryLifecycleTarget::new(
            backend_idx,
            RuntimeEndpoint::new(endpoint.host(), i32::from(endpoint.port()))
                .map_err(|error| contract_error(error.to_string()))?,
            backend.process_id().map_err(|error| {
                contract_error(format!(
                    "query lifecycle backend {backend_idx} process id is invalid: {error}"
                ))
            })?,
        );
        let fragment_participant = manifest
            .roles()
            .map_err(|error| {
                contract_error(format!(
                    "query lifecycle backend {backend_idx} manifest is invalid: {error}"
                ))
            })?
            .contains(&ParticipantRole::FragmentExecutor);
        let request = QueryInitRequest::parse(protocol_wire::InitQueryRequest {
            manifest: Some(manifest.as_proto().clone()),
        })
        .map_err(|error| {
            contract_error(format!(
                "query lifecycle backend {backend_idx} request is invalid: {error}"
            ))
        })?;
        participants.push(MaterializedParticipant {
            target,
            request,
            digest,
            fragment_participant,
        });
    }
    Ok(MaterializedQueryInit {
        execution_id: core_execution_id(execution_id)?,
        participants,
    })
}

/// Core retains the lease/registry orchestration identity in CLS-R1, while
/// the Init plan and every neutral wire carrier are Protocol-owned. This is a
/// role-local handoff into the existing Frontend orchestration API, not a
/// lifecycle codec or a second wire representation.
fn core_execution_id(
    execution_id: novarocks_proto::lifecycle::QueryExecutionId,
) -> Result<QueryExecutionId, DistributedQueryError> {
    let attempt = novarocks_proto::lifecycle::AttemptId::new(execution_id.attempt_id().get())
        .map_err(|error| contract_error(error.to_string()))?;
    QueryExecutionId::new(execution_id.query_id(), attempt)
        .map_err(|error| contract_error(error.to_string()))
}

fn contract_error(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, message)
}
