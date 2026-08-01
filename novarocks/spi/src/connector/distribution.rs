// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to You under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the License
// at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;

use bytes::Bytes;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{
    ConnectorError, ConnectorErrorKind, ConnectorExecutionBindingKey, ConnectorInstanceDescriptor,
};

/// Largest provider-owned declaration accepted by the process binding control
/// plane. Declarations identify a local startup binding; they never carry
/// credentials, clients, or arbitrary execution state.
pub const MAX_CONNECTOR_INSTANCE_DECLARATION_PAYLOAD_BYTES: usize = 64 * 1024;

/// A monotonically generated identity for one logical connector instance
/// generation. UUID v7 is used so a newer catalog generation cannot be
/// overwritten by a delayed install request.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorInstanceIncarnation(Uuid);

impl ConnectorInstanceIncarnation {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }

    pub fn to_bytes(self) -> [u8; 16] {
        *self.0.as_bytes()
    }
}

impl Default for ConnectorInstanceIncarnation {
    fn default() -> Self {
        Self::new()
    }
}

/// Bounded provider declaration carried only by the execution-binding control
/// plane. It has no credentials, client, or runtime object and identifies one
/// exact logical binding generation.
#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorExecutionDeclaration {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
    payload: Bytes,
}

impl ConnectorExecutionDeclaration {
    pub fn try_new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ConnectorInstanceIncarnation,
        payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        if payload.len() > MAX_CONNECTOR_INSTANCE_DECLARATION_PAYLOAD_BYTES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                format!(
                    "connector execution declaration payload exceeds {MAX_CONNECTOR_INSTANCE_DECLARATION_PAYLOAD_BYTES} bytes"
                ),
            ));
        }
        Ok(Self {
            descriptor,
            incarnation,
            payload,
        })
    }

    pub fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    pub fn incarnation(&self) -> ConnectorInstanceIncarnation {
        self.incarnation
    }

    pub fn binding_key(&self) -> ConnectorExecutionBindingKey {
        ConnectorExecutionBindingKey {
            instance_id: self.descriptor.instance_id.clone(),
            incarnation: self.incarnation,
        }
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    pub fn digest(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(self.descriptor.provider_id.as_str().as_bytes());
        hasher.update([0]);
        hasher.update(self.descriptor.instance_id.as_str().as_bytes());
        hasher.update([0]);
        hasher.update(self.incarnation.to_bytes());
        hasher.update(self.payload.as_ref());
        hasher.finalize().into()
    }
}

impl fmt::Debug for ConnectorExecutionDeclaration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectorExecutionDeclaration")
            .field("descriptor", &self.descriptor)
            .field("incarnation", &self.incarnation)
            .field("payload_len", &self.payload.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::{ConnectorInstanceId, ConnectorProviderId};

    fn descriptor() -> ConnectorInstanceDescriptor {
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider ID"),
            instance_id: ConnectorInstanceId::parse("catalog.analytics").expect("instance ID"),
        }
    }

    #[test]
    fn declaration_debug_is_redacted_and_digest_is_stable() {
        let declaration = ConnectorExecutionDeclaration::try_new(
            descriptor(),
            ConnectorInstanceIncarnation::from_bytes([7; 16]),
            Bytes::from_static(b"secret=must-not-appear"),
        )
        .expect("bounded declaration");
        let debug = format!("{declaration:?}");
        assert!(!debug.contains("must-not-appear"));
        assert_eq!(declaration.digest(), declaration.digest());
    }

    #[test]
    fn declaration_rejects_oversized_payload() {
        let error = ConnectorExecutionDeclaration::try_new(
            descriptor(),
            ConnectorInstanceIncarnation::from_bytes([8; 16]),
            Bytes::from(vec![
                0;
                MAX_CONNECTOR_INSTANCE_DECLARATION_PAYLOAD_BYTES + 1
            ]),
        )
        .expect_err("oversized declaration must fail");
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
    }

    #[test]
    fn execution_declaration_is_redacted_and_has_a_typed_binding_key() {
        let declaration = ConnectorExecutionDeclaration::try_new(
            descriptor(),
            ConnectorInstanceIncarnation::from_bytes([9; 16]),
            Bytes::from_static(b"secret=must-not-appear"),
        )
        .expect("bounded execution declaration");

        let debug = format!("{declaration:?}");
        assert!(!debug.contains("must-not-appear"));
        assert_eq!(
            declaration.binding_key().instance_id.as_str(),
            "catalog.analytics"
        );
        assert_eq!(declaration.binding_key().incarnation.to_bytes(), [9; 16]);
    }
}
