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
use std::time::{Duration, Instant};

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorCancellation, ConnectorError, ConnectorErrorKind, ConnectorInstanceDeclaration,
    ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorInstanceIncarnation,
    ConnectorProviderId, ConnectorRequestContext, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
    MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
};

use crate::proto::novarocks::{InstallConnectorInstanceRequest, RetireConnectorInstanceRequest};

const CONNECTOR_BINDING_CONTEXT_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) fn decode_install_request(
    request: InstallConnectorInstanceRequest,
) -> Result<ConnectorInstanceDeclaration, ConnectorError> {
    let provider_id = ConnectorProviderId::parse(&request.provider_id)?;
    let instance_id = ConnectorInstanceId::parse(&request.instance_id)?;
    let incarnation = decode_incarnation(&request.incarnation)?;
    ConnectorInstanceDeclaration::try_new(
        ConnectorInstanceDescriptor {
            provider_id,
            instance_id,
        },
        incarnation,
        Bytes::from(request.declaration_payload),
    )
}

pub(crate) fn decode_retire_request(
    request: RetireConnectorInstanceRequest,
) -> Result<(ConnectorInstanceId, ConnectorInstanceIncarnation), ConnectorError> {
    Ok((
        ConnectorInstanceId::parse(&request.instance_id)?,
        decode_incarnation(&request.incarnation)?,
    ))
}

pub(crate) fn install_request_context() -> Result<ConnectorRequestContext, ConnectorError> {
    ConnectorRequestContext::try_new(
        Instant::now() + CONNECTOR_BINDING_CONTEXT_TIMEOUT,
        Arc::new(NotCancelled),
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
}

fn decode_incarnation(bytes: &[u8]) -> Result<ConnectorInstanceIncarnation, ConnectorError> {
    let bytes: [u8; 16] = bytes.try_into().map_err(|_| {
        ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector instance incarnation must contain exactly 16 bytes",
        )
    })?;
    Ok(ConnectorInstanceIncarnation::from_bytes(bytes))
}

struct NotCancelled;

impl ConnectorCancellation for NotCancelled {
    fn is_cancelled(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn install_request_rejects_invalid_incarnation_length() {
        let error = decode_install_request(InstallConnectorInstanceRequest {
            provider_id: "iceberg".to_string(),
            instance_id: "catalog.analytics".to_string(),
            incarnation: vec![7; 15],
            declaration_payload: Vec::new(),
        })
        .expect_err("short incarnation must be rejected");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }
}
