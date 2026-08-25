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
    EnsureConnectorExecutionBindingRejection, EnsureConnectorExecutionBindingRejectionReason,
    EnsureConnectorExecutionBindingResult, RetireConnectorExecutionBindingOutcome,
    RetireConnectorExecutionBindingResult, connector_execution_binding_declaration_digest,
};
use novarocks_spi::connector::{
    ConnectorExecutionBindingKey, ConnectorExecutionDeclaration, ConnectorInstanceId,
    ConnectorInstanceIncarnation,
};

use crate::fragment::decode::request::decode_native_query_execution_id;

// Design: ADR-0105 (docs/adr/ADR-0105-wire-authority-and-domain-carrier-separation.md)
/// Backend-local result of wire validation. The digest is deliberately made
/// from the original generated DTO before it is translated into the SPI domain
/// declaration; the execution Host never recomputes a wire identity.
#[derive(Clone, Debug)]
pub struct AdmittedConnectorExecutionDeclaration {
    digest: [u8; 32],
    declaration: ConnectorExecutionDeclaration,
}

impl AdmittedConnectorExecutionDeclaration {
    #[doc(hidden)]
    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }

    #[doc(hidden)]
    pub fn declaration(&self) -> &ConnectorExecutionDeclaration {
        &self.declaration
    }
}

#[cfg(test)]
pub(crate) fn admitted_for_tests(
    declaration: ConnectorExecutionDeclaration,
    digest: [u8; 32],
) -> AdmittedConnectorExecutionDeclaration {
    AdmittedConnectorExecutionDeclaration {
        digest,
        declaration,
    }
}

pub(crate) fn decode_ensure_request(
    request: EnsureConnectorExecutionBindingRequest,
) -> Result<
    (QueryExecutionId, AdmittedConnectorExecutionDeclaration),
    EnsureConnectorExecutionBindingResult,
> {
    let execution_id = request.execution_id.as_ref().ok_or_else(|| {
        invalid_declaration("connector execution binding request is missing execution_id")
    })?;
    let execution_id = decode_native_query_execution_id(execution_id)
        .map_err(|error| invalid_declaration(&error.to_string()))?;
    let declaration = request.declaration.ok_or_else(|| {
        invalid_declaration("connector execution binding request is missing declaration")
    })?;
    let declaration = decode_connector_execution_declaration(declaration)
        .map_err(|_| invalid_declaration("invalid connector execution declaration"))?;
    Ok((execution_id, declaration))
}

/// Decodes a generated native DTO using the production BE admission path.
///
/// It is public only for the end-to-end carrier-contract test; callers must
/// pass the resulting admitted pair directly to the BE Host.
#[doc(hidden)]
pub fn decode_connector_execution_declaration(
    raw: novarocks_protocol::novarocks::ConnectorExecutionBindingDeclaration,
) -> Result<AdmittedConnectorExecutionDeclaration, String> {
    let digest = connector_execution_binding_declaration_digest(&raw)
        .map_err(|_| "cannot canonicalize connector execution declaration".to_string())?;
    let incarnation: [u8; 16] = raw
        .incarnation
        .as_slice()
        .try_into()
        .map_err(|_| "connector execution declaration has invalid incarnation".to_string())?;
    let declaration = match raw.provider {
        Some(
            novarocks_protocol::novarocks::connector_execution_binding_declaration::Provider::Iceberg(
                provider,
            ),
        ) => ConnectorExecutionDeclaration::iceberg(&raw.instance_id, incarnation, provider.access_binding),
        Some(
            novarocks_protocol::novarocks::connector_execution_binding_declaration::Provider::Starrocks(
                provider,
            ),
        ) => ConnectorExecutionDeclaration::starrocks(&raw.instance_id, incarnation, provider.local_binding),
        None => return Err("connector execution declaration has no provider".to_string()),
    }
    .map_err(|_| "connector execution declaration is invalid".to_string())?;
    Ok(AdmittedConnectorExecutionDeclaration {
        digest,
        declaration,
    })
}

pub(crate) fn decode_retire_request(
    request: RetireConnectorExecutionBindingRequest,
) -> Result<ConnectorExecutionBindingKey, RetireConnectorExecutionBindingResult> {
    let incarnation: [u8; 16] = request.incarnation.as_slice().try_into().map_err(|_| {
        RetireConnectorExecutionBindingResult::new(
            RetireConnectorExecutionBindingOutcome::InvalidKey,
        )
    })?;
    Ok(ConnectorExecutionBindingKey {
        instance_id: ConnectorInstanceId::try_from_canonical(&request.instance_id).map_err(
            |_| {
                RetireConnectorExecutionBindingResult::new(
                    RetireConnectorExecutionBindingOutcome::InvalidKey,
                )
            },
        )?,
        incarnation: ConnectorInstanceIncarnation::from_bytes(incarnation),
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

    #[test]
    fn wire_declaration_rejects_noncanonical_instance_id() {
        let result = decode_connector_execution_declaration(
            novarocks_protocol::novarocks::ConnectorExecutionBindingDeclaration {
                instance_id: "MyCatalog".to_string(),
                incarnation: vec![7; 16],
                provider: Some(
                    novarocks_protocol::novarocks::connector_execution_binding_declaration::Provider::Iceberg(
                        novarocks_protocol::novarocks::IcebergExecutionBindingDeclaration {
                            access_binding: "local".to_string(),
                        },
                    ),
                ),
            },
        );
        assert!(result.is_err());
    }
}
