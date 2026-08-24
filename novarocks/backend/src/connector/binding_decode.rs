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

use novarocks_protocol::lifecycle::QueryExecutionId;
use novarocks_protocol::novarocks::{
    EnsureConnectorExecutionBindingRequest, RetireConnectorExecutionBindingRequest,
};
use novarocks_protocol::provider::{
    ConnectorExecutionBindingKey as ProtocolBindingKey, EnsureConnectorExecutionBindingRejection,
    EnsureConnectorExecutionBindingRejectionReason, EnsureConnectorExecutionBindingResult,
    RetireConnectorExecutionBindingOutcome, RetireConnectorExecutionBindingResult,
};
use novarocks_spi::connector::{
    ConnectorExecutionBindingKey, ConnectorExecutionDeclaration, ConnectorInstanceId,
    ConnectorInstanceIncarnation,
};

use crate::fragment::decode::request::decode_native_query_execution_id;

pub(crate) fn decode_ensure_request(
    request: EnsureConnectorExecutionBindingRequest,
) -> Result<(QueryExecutionId, ConnectorExecutionDeclaration), EnsureConnectorExecutionBindingResult>
{
    let execution_id = request.execution_id.as_ref().ok_or_else(|| {
        invalid_declaration("connector execution binding request is missing execution_id")
    })?;
    let execution_id = decode_native_query_execution_id(execution_id)
        .map_err(|error| invalid_declaration(&error.to_string()))?;
    let declaration = request.declaration.ok_or_else(|| {
        invalid_declaration("connector execution binding request is missing declaration")
    })?;
    let declaration = ConnectorExecutionDeclaration::try_from_proto(declaration)
        .map_err(|error| invalid_declaration(&error.to_string()))?;
    Ok((execution_id, declaration))
}

pub(crate) fn decode_retire_request(
    request: RetireConnectorExecutionBindingRequest,
) -> Result<ConnectorExecutionBindingKey, RetireConnectorExecutionBindingResult> {
    let key =
        ProtocolBindingKey::try_new(request.instance_id, request.incarnation).map_err(|_| {
            RetireConnectorExecutionBindingResult::new(
                RetireConnectorExecutionBindingOutcome::InvalidKey,
            )
        })?;
    Ok(ConnectorExecutionBindingKey {
        instance_id: ConnectorInstanceId::parse(key.instance_id())
            .expect("Protocol validates canonical retire instance IDs"),
        incarnation: ConnectorInstanceIncarnation::from_bytes(key.incarnation()),
    })
}

fn invalid_declaration(detail: &str) -> EnsureConnectorExecutionBindingResult {
    let mut end = detail.len().min(512);
    while end > 0 && !detail.is_char_boundary(end) {
        end -= 1;
    }
    let rejection = EnsureConnectorExecutionBindingRejection::try_new(
        EnsureConnectorExecutionBindingRejectionReason::InvalidDeclaration,
        false,
        detail[..end].to_string(),
        None,
    )
    .expect("fixed invalid declaration outcome is Protocol-valid");
    EnsureConnectorExecutionBindingResult::rejected(rejection)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ensure_request_rejects_missing_typed_declaration() {
        let result = decode_ensure_request(EnsureConnectorExecutionBindingRequest {
            execution_id: Some(novarocks_protocol::novarocks::QueryExecutionId {
                query_id: Some(novarocks_protocol::common::UniqueId { hi: 7, lo: 9 }),
                attempt_id: 1,
            }),
            declaration: None,
        });
        assert!(result.is_err());
    }
}
