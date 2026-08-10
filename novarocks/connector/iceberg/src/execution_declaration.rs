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

//! Provider-owned declaration for one installed Iceberg execution binding.
//!
//! The declaration is intentionally secret-free: it names a startup-composed
//! access binding, while credentials, catalog clients, and runtime objects
//! remain process local to the execution installer.

use std::time::Instant;

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorExecutionDeclaration,
    ConnectorExecutionDistribution, ConnectorInstanceDescriptor, ConnectorInstanceIncarnation,
    ConnectorRequestContext,
};
use serde::{Deserialize, Serialize};

const ICEBERG_DECLARATION_V1: u16 = 1;
const DEFAULT_ACCESS_BINDING: &str = "default";

#[derive(Deserialize, Serialize)]
struct IcebergDeclarationV1 {
    version: u16,
    access_binding: String,
}

/// Declaration producer for one exact Iceberg instance generation.
#[derive(Clone)]
pub struct IcebergInstanceDistribution {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
}

impl IcebergInstanceDistribution {
    pub fn new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ConnectorInstanceIncarnation,
    ) -> Self {
        Self {
            descriptor,
            incarnation,
        }
    }
}

impl ConnectorExecutionDistribution for IcebergInstanceDistribution {
    fn declaration(
        &self,
        context: &ConnectorRequestContext,
    ) -> Result<ConnectorExecutionDeclaration, ConnectorError> {
        if context.cancellation().is_cancelled() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Cancelled,
                "connector request was cancelled",
            ));
        }
        if Instant::now() >= context.deadline() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::DeadlineExceeded,
                "connector request deadline elapsed",
            ));
        }
        ConnectorExecutionDeclaration::try_new(
            self.descriptor.clone(),
            self.incarnation,
            encode_declaration_payload(&IcebergDeclarationV1 {
                version: ICEBERG_DECLARATION_V1,
                access_binding: DEFAULT_ACCESS_BINDING.to_string(),
            })?,
        )
    }
}

/// Decodes the provider-private, bounded declaration payload at installation.
pub fn decode_access_binding(
    declaration: &ConnectorExecutionDeclaration,
) -> Result<String, ConnectorError> {
    let payload: IcebergDeclarationV1 =
        serde_json::from_slice(declaration.payload()).map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                format!("decode Iceberg execution declaration: {error}"),
            )
        })?;
    if payload.version != ICEBERG_DECLARATION_V1 {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            format!(
                "unsupported Iceberg execution declaration version {}",
                payload.version
            ),
        ));
    }
    Ok(payload.access_binding)
}

fn encode_declaration_payload(payload: &IcebergDeclarationV1) -> Result<Bytes, ConnectorError> {
    serde_json::to_vec(payload)
        .map(Bytes::from)
        .map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                format!("serialize Iceberg execution declaration: {error}"),
            )
        })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorInstanceId, ConnectorProviderId,
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    };

    use super::*;

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    #[test]
    fn declaration_round_trips_the_default_access_binding() {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("valid provider ID"),
            instance_id: ConnectorInstanceId::parse("catalog").expect("valid instance ID"),
        };
        let context = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            Arc::new(NeverCancelled),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("valid request context");
        let declaration = IcebergInstanceDistribution::new(
            descriptor,
            ConnectorInstanceIncarnation::from_bytes([7; 16]),
        )
        .declaration(&context)
        .expect("declaration");

        assert_eq!(
            decode_access_binding(&declaration).expect("payload"),
            "default"
        );
    }
}
