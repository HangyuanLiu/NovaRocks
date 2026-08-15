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

//! Temporary BE-local bridge between the Protocol-owned wire values and the
//! legacy registry implementation.
//!
//! Native ingress validates `novarocks_protocol::lifecycle` values before it
//! reaches this module.  Keeping the conversion next to the existing registry
//! avoids a Core facade while the registry's terminal/profile representation is
//! migrated to generated Protocol carriers.

use novarocks::query_execution::lifecycle::contract::{
    decode_abort_query_request, decode_query_control_attach, decode_query_control_command,
    decode_query_init_request, decode_query_stage_request, decode_query_start_request,
    encode_abort_query_response, encode_query_init_response, encode_query_stage_response,
    encode_query_start_response,
};
use novarocks::query_execution::lifecycle::{
    AttemptId as LegacyAttemptId, QueryAbortRequest as LegacyQueryAbortRequest,
    QueryControlAttach as LegacyQueryControlAttach,
    QueryControlCommand as LegacyQueryControlCommand, QueryExecutionId as LegacyQueryExecutionId,
    QueryInitAck as LegacyQueryInitAck, QueryInitRequest as LegacyQueryInitRequest,
    QueryLifecycleError, QueryStageAck as LegacyQueryStageAck,
    QueryStageRequest as LegacyQueryStageRequest, QueryStartAck as LegacyQueryStartAck,
    QueryStartRequest as LegacyQueryStartRequest, QueryTerminationAck as LegacyQueryTerminationAck,
};
use novarocks_protocol::lifecycle::{
    QueryAbortRequest, QueryControlAttach, QueryInitAck, QueryInitRequest, QueryStageAck,
    QueryStageRequest, QueryStartAck, QueryStartRequest, QueryTerminationAck,
};
use novarocks_types::QueryId;

pub(crate) fn legacy_init_request(
    request: QueryInitRequest,
) -> Result<LegacyQueryInitRequest, QueryLifecycleError> {
    decode_query_init_request(request.as_proto())
}

pub(crate) fn protocol_init_ack(value: &LegacyQueryInitAck) -> QueryInitAck {
    QueryInitAck::parse(encode_query_init_response(value))
        .expect("legacy InitQuery acknowledgement remains a valid Protocol response")
}

pub(crate) fn legacy_stage_request(
    request: QueryStageRequest,
) -> Result<LegacyQueryStageRequest, QueryLifecycleError> {
    decode_query_stage_request(request.as_proto())
}

pub(crate) fn protocol_stage_ack(value: &LegacyQueryStageAck) -> QueryStageAck {
    QueryStageAck::parse(encode_query_stage_response(value))
        .expect("legacy StageFragments acknowledgement remains a valid Protocol response")
}

pub(crate) fn legacy_start_request(
    request: QueryStartRequest,
) -> Result<LegacyQueryStartRequest, QueryLifecycleError> {
    decode_query_start_request(request.as_proto())
}

pub(crate) fn protocol_start_ack(value: &LegacyQueryStartAck) -> QueryStartAck {
    QueryStartAck::parse(encode_query_start_response(value))
        .expect("legacy StartPreparedQuery acknowledgement remains a valid Protocol response")
}

pub(crate) fn legacy_abort_request(
    request: QueryAbortRequest,
) -> Result<LegacyQueryAbortRequest, QueryLifecycleError> {
    decode_abort_query_request(request.as_proto())
}

pub(crate) fn protocol_termination_ack(value: &LegacyQueryTerminationAck) -> QueryTerminationAck {
    QueryTerminationAck::parse(encode_abort_query_response(value))
        .expect("legacy AbortQuery acknowledgement remains a valid Protocol response")
}

pub(crate) fn legacy_control_attach(
    attach: QueryControlAttach,
) -> Result<LegacyQueryControlAttach, QueryLifecycleError> {
    let frame = novarocks_protocol::novarocks::QueryControlRequest {
        command: Some(
            novarocks_protocol::novarocks::query_control_request::Command::Attach(
                attach.as_proto().clone(),
            ),
        ),
    };
    decode_query_control_attach(&frame)
}

pub(crate) fn legacy_control_command(
    command: novarocks_protocol::lifecycle::QueryControlCommand,
) -> Result<LegacyQueryControlCommand, QueryLifecycleError> {
    decode_query_control_command(command.as_proto())
}

pub(crate) fn legacy_execution_id(
    execution_id: novarocks_protocol::lifecycle::QueryExecutionId,
) -> Result<LegacyQueryExecutionId, QueryLifecycleError> {
    LegacyQueryExecutionId::new(
        QueryId::new(
            execution_id.query_id().high(),
            execution_id.query_id().low(),
        ),
        LegacyAttemptId::new(execution_id.attempt_id().get())?,
    )
}

pub(crate) fn protocol_execution_id(
    execution_id: LegacyQueryExecutionId,
) -> novarocks_protocol::lifecycle::QueryExecutionId {
    novarocks_protocol::lifecycle::QueryExecutionId::new(
        QueryId::new(
            execution_id.query_id().high(),
            execution_id.query_id().low(),
        ),
        novarocks_protocol::lifecycle::AttemptId::new(execution_id.attempt_id().get())
            .expect("legacy query execution id always has a nonzero attempt"),
    )
    .expect("legacy query execution id is always valid under the Protocol contract")
}
