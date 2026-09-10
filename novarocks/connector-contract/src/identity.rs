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

use std::{error::Error, fmt, sync::Arc};

const MAX_PROVIDER_ID_BYTES: usize = 64;
const MAX_INSTANCE_ID_BYTES: usize = 128;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ConnectorIdentityError {
    InvalidProviderId,
    InvalidInstanceId,
    InvalidCanonicalInstanceId,
    InvalidWriteTargetOrdinal,
}

impl fmt::Display for ConnectorIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidProviderId => "invalid connector provider ID",
            Self::InvalidInstanceId => "invalid connector instance ID",
            Self::InvalidCanonicalInstanceId => "invalid canonical connector instance ID",
            Self::InvalidWriteTargetOrdinal => {
                "connector write target ordinal exceeds the sealed target bound"
            }
        })
    }
}

impl Error for ConnectorIdentityError {}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorProviderId(Arc<str>);

impl ConnectorProviderId {
    pub fn parse(value: &str) -> Result<Self, ConnectorIdentityError> {
        if !is_provider_id(value) {
            return Err(ConnectorIdentityError::InvalidProviderId);
        }
        Ok(Self(Arc::from(value)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorInstanceId(Arc<str>);

impl ConnectorInstanceId {
    /// Catalog admission preserves the existing case-insensitive SQL behavior.
    pub fn parse(value: &str) -> Result<Self, ConnectorIdentityError> {
        if !value.is_ascii() {
            return Err(ConnectorIdentityError::InvalidInstanceId);
        }
        let normalized = value.to_ascii_lowercase();
        if !is_instance_id(&normalized) {
            return Err(ConnectorIdentityError::InvalidInstanceId);
        }
        Ok(Self(Arc::from(normalized)))
    }

    /// Native wire ingress accepts only an already-canonical identity.
    pub fn try_from_canonical(value: &str) -> Result<Self, ConnectorIdentityError> {
        if !value.is_ascii() || !is_instance_id(value) {
            return Err(ConnectorIdentityError::InvalidCanonicalInstanceId);
        }
        Ok(Self(Arc::from(value)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorInstanceDescriptor {
    pub provider_id: ConnectorProviderId,
    pub instance_id: ConnectorInstanceId,
}

fn is_provider_id(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= MAX_PROVIDER_ID_BYTES
        && bytes[0].is_ascii_lowercase()
        && bytes[1..].iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        })
}

fn is_instance_id(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= MAX_INSTANCE_ID_BYTES
        && (bytes[0].is_ascii_lowercase() || bytes[0] == b'_')
        && bytes[1..].iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'.' | b'-')
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_admission_normalizes_but_wire_ingress_requires_canonical_form() {
        assert_eq!(
            ConnectorInstanceId::parse("MyCatalog.Analytics")
                .unwrap()
                .as_str(),
            "mycatalog.analytics"
        );
        assert_eq!(
            ConnectorInstanceId::try_from_canonical("MyCatalog.Analytics"),
            Err(ConnectorIdentityError::InvalidCanonicalInstanceId)
        );
    }
}
