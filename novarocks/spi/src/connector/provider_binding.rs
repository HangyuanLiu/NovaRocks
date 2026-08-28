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

// Design: ADR-0125 (docs/adr/ADR-0125-query-leased-catalog-runtime-and-provider-binding-evidence.md)
use std::sync::Arc;

use super::{ConnectorError, ConnectorErrorKind, ConnectorInstanceId, ProviderBindingEpoch};

const MAX_LOCAL_BINDING_BYTES: usize = 256;

/// The closed provider variant carried by a provider-private binding.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ConnectorProviderBindingKind {
    Iceberg,
    StarRocks,
}

impl ConnectorProviderBindingKind {
    pub const ALL: [Self; 2] = [Self::Iceberg, Self::StarRocks];

    pub const fn provider_id(self) -> &'static str {
        match self {
            Self::Iceberg => "iceberg",
            Self::StarRocks => "starrocks",
        }
    }
}

/// Immutable provider-private identity used to fence FE effects and late
/// materialization. It is not a BE execution identity and never crosses the
/// native fragment or terminal-report wire contracts.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorProviderBindingKey {
    pub instance_id: ConnectorInstanceId,
    pub incarnation: ProviderBindingEpoch,
}

impl ConnectorProviderBindingKey {
    pub fn instance_id(&self) -> &str {
        self.instance_id.as_str()
    }

    pub fn incarnation(&self) -> [u8; 16] {
        self.incarnation.to_bytes()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ConnectorProviderBindingSource {
    Iceberg { access_binding: Arc<str> },
    StarRocks { local_binding: Arc<str> },
}

/// Borrowed, transport-neutral provider facts from a validated provider
/// binding. Consumers must match this closed enum rather than infer a
/// provider from an identifier string.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorProviderBindingProvider<'a> {
    Iceberg { access_binding: &'a str },
    StarRocks { local_binding: &'a str },
}

impl ConnectorProviderBindingProvider<'_> {
    pub const fn kind(self) -> ConnectorProviderBindingKind {
        match self {
            Self::Iceberg { .. } => ConnectorProviderBindingKind::Iceberg,
            Self::StarRocks { .. } => ConnectorProviderBindingKind::StarRocks,
        }
    }
}

impl ConnectorProviderBindingSource {
    const fn kind(&self) -> ConnectorProviderBindingKind {
        match self {
            Self::Iceberg { .. } => ConnectorProviderBindingKind::Iceberg,
            Self::StarRocks { .. } => ConnectorProviderBindingKind::StarRocks,
        }
    }
}

/// Transport-neutral, validated provider binding admitted by connector control.
///
/// Its fields remain private so a provider binding can only be constructed
/// through the bounded constructors below.  Protocol adapters are owned by
/// the FE and BE applications, not by SPI.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorProviderBinding {
    binding_key: ConnectorProviderBindingKey,
    provider: ConnectorProviderBindingSource,
}

impl ConnectorProviderBinding {
    pub fn iceberg(
        instance_id: impl AsRef<str>,
        incarnation: [u8; 16],
        access_binding: impl AsRef<str>,
    ) -> Result<Self, ConnectorError> {
        Self::try_new(
            instance_id.as_ref(),
            incarnation,
            ConnectorProviderBindingSource::Iceberg {
                access_binding: bounded_binding(access_binding.as_ref())?,
            },
        )
    }

    pub fn starrocks(
        instance_id: impl AsRef<str>,
        incarnation: [u8; 16],
        local_binding: impl AsRef<str>,
    ) -> Result<Self, ConnectorError> {
        Self::try_new(
            instance_id.as_ref(),
            incarnation,
            ConnectorProviderBindingSource::StarRocks {
                local_binding: bounded_binding(local_binding.as_ref())?,
            },
        )
    }

    fn try_new(
        instance_id: &str,
        incarnation: [u8; 16],
        provider: ConnectorProviderBindingSource,
    ) -> Result<Self, ConnectorError> {
        Ok(Self {
            binding_key: ConnectorProviderBindingKey {
                instance_id: ConnectorInstanceId::try_from_canonical(instance_id)?,
                incarnation: ProviderBindingEpoch::from_bytes(incarnation),
            },
            provider,
        })
    }

    pub fn binding_key(&self) -> &ConnectorProviderBindingKey {
        &self.binding_key
    }

    pub fn provider(&self) -> ConnectorProviderBindingProvider<'_> {
        match &self.provider {
            ConnectorProviderBindingSource::Iceberg { access_binding } => {
                ConnectorProviderBindingProvider::Iceberg { access_binding }
            }
            ConnectorProviderBindingSource::StarRocks { local_binding } => {
                ConnectorProviderBindingProvider::StarRocks { local_binding }
            }
        }
    }

    pub const fn provider_kind(&self) -> ConnectorProviderBindingKind {
        self.provider.kind()
    }

    pub const fn provider_id(&self) -> &'static str {
        self.provider_kind().provider_id()
    }

    pub fn iceberg_access_binding(&self) -> Option<&str> {
        match &self.provider {
            ConnectorProviderBindingSource::Iceberg { access_binding } => Some(access_binding),
            ConnectorProviderBindingSource::StarRocks { .. } => None,
        }
    }

    pub fn starrocks_local_binding(&self) -> Option<&str> {
        match &self.provider {
            ConnectorProviderBindingSource::Iceberg { .. } => None,
            ConnectorProviderBindingSource::StarRocks { local_binding } => Some(local_binding),
        }
    }
}

impl From<&ConnectorProviderBinding> for ConnectorProviderBindingKey {
    fn from(binding: &ConnectorProviderBinding) -> Self {
        binding.binding_key.clone()
    }
}

fn bounded_binding(value: &str) -> Result<Arc<str>, ConnectorError> {
    if value.is_empty() || value.len() > MAX_LOCAL_BINDING_BYTES || !value.is_ascii() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector provider local binding must be non-empty bounded ASCII",
        ));
    }
    Ok(Arc::from(value))
}

#[cfg(test)]
mod tests {
    use super::{
        ConnectorProviderBinding, ConnectorProviderBindingKind, ConnectorProviderBindingProvider,
    };

    #[test]
    fn constructors_validate_canonical_identity_and_local_binding() {
        assert!(ConnectorProviderBinding::iceberg("MyCatalog", [1; 16], "local").is_err());
        assert!(ConnectorProviderBinding::iceberg("catalog", [1; 16], "").is_err());
        assert!(ConnectorProviderBinding::starrocks("catalog", [1; 16], "x".repeat(257)).is_err());
        let binding = ConnectorProviderBinding::iceberg("catalog", [1; 16], "local").unwrap();
        assert_eq!(
            binding.provider_kind(),
            ConnectorProviderBindingKind::Iceberg
        );
        assert_eq!(binding.provider_id(), "iceberg");
        assert!(matches!(
            binding.provider(),
            ConnectorProviderBindingProvider::Iceberg {
                access_binding: "local"
            }
        ));
    }
}
