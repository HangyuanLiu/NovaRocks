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

use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind, ConnectorInstanceId};

use crate::STARROCKS_CONTRACT_VERSION;

/// Names one process-local StarRocks execution binding.
///
/// The frontend declares this name; a backend resolves it against its own
/// startup composition. It never carries an endpoint or a credential.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StarRocksLocalBindingRef(Arc<str>);

impl StarRocksLocalBindingRef {
    pub fn parse(value: impl AsRef<str>) -> Result<Self, ConnectorError> {
        let value = value.as_ref();
        if value.is_empty() || value.len() > 256 || !value.is_ascii() {
            return Err(invalid("invalid StarRocks local binding reference"));
        }
        Ok(Self(Arc::from(value)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for StarRocksLocalBindingRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("StarRocksLocalBindingRef")
            .field(&self.0)
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct StarRocksConnectorConfig {
    pub instance_id: ConnectorInstanceId,
    pub local_binding: StarRocksLocalBindingRef,
}

impl StarRocksConnectorConfig {
    pub fn new(instance_id: ConnectorInstanceId, local_binding: StarRocksLocalBindingRef) -> Self {
        Self {
            instance_id,
            local_binding,
        }
    }
}

/// What a remote StarRocks cluster claims it can serve.
///
/// Only the API contract version survives the read cut: the read-transport and
/// direct-read readiness flags described capabilities that no longer have a
/// consumer, and a future typed read must republish its own readiness rather
/// than inherit a stale claim.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StarRocksCapabilitySnapshot {
    pub api_contract_version: u16,
}

impl StarRocksCapabilitySnapshot {
    pub fn validate(&self) -> Result<(), ConnectorError> {
        if self.api_contract_version != STARROCKS_CONTRACT_VERSION {
            return Err(unsupported("unsupported StarRocks API contract version"));
        }
        Ok(())
    }
}

/// One table as the external StarRocks cluster resolved it.
#[derive(Clone)]
pub struct StarRocksResolvedTable {
    pub namespace: Arc<str>,
    pub table: Arc<str>,
    pub schema: SchemaRef,
    pub schema_version: Bytes,
    pub data_version: Bytes,
    pub capability: StarRocksCapabilitySnapshot,
}

impl StarRocksResolvedTable {
    pub fn try_new(
        namespace: impl Into<Arc<str>>,
        table: impl Into<Arc<str>>,
        schema: SchemaRef,
        schema_version: Bytes,
        data_version: Bytes,
        capability: StarRocksCapabilitySnapshot,
    ) -> Result<Self, ConnectorError> {
        let namespace = namespace.into();
        let table = table.into();
        if namespace.is_empty() || table.is_empty() {
            return Err(invalid("StarRocks namespace and table must not be empty"));
        }
        if schema_version.is_empty() || data_version.is_empty() {
            return Err(invalid("StarRocks table versions must not be empty"));
        }
        capability.validate()?;
        Ok(Self {
            namespace,
            table,
            schema,
            schema_version,
            data_version,
            capability,
        })
    }
}

pub(crate) fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}
pub(crate) fn unsupported(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unsupported, message.into())
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    #[test]
    fn metadata_resolution_rejects_a_foreign_api_contract_version() {
        let error = match StarRocksResolvedTable::try_new(
            "db",
            "t",
            schema(),
            Bytes::from_static(b"schema-1"),
            Bytes::from_static(b"data-1"),
            StarRocksCapabilitySnapshot {
                api_contract_version: STARROCKS_CONTRACT_VERSION + 1,
            },
        ) {
            Ok(_) => panic!("a cluster on another contract version must not resolve a table"),
            Err(error) => error,
        };

        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
    }

    #[test]
    fn metadata_resolution_requires_both_table_versions() {
        for (schema_version, data_version) in [
            (Bytes::new(), Bytes::from_static(b"data-1")),
            (Bytes::from_static(b"schema-1"), Bytes::new()),
        ] {
            let error = match StarRocksResolvedTable::try_new(
                "db",
                "t",
                schema(),
                schema_version,
                data_version,
                StarRocksCapabilitySnapshot {
                    api_contract_version: STARROCKS_CONTRACT_VERSION,
                },
            ) {
                Ok(_) => panic!("an unversioned table answer is not a resolution"),
                Err(error) => error,
            };

            assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        }
    }

    #[test]
    fn a_local_binding_reference_is_bounded_ascii() {
        assert!(StarRocksLocalBindingRef::parse("default").is_ok());
        assert!(StarRocksLocalBindingRef::parse("").is_err());
        assert!(StarRocksLocalBindingRef::parse("no\u{4e2d}ascii").is_err());
        assert!(StarRocksLocalBindingRef::parse("x".repeat(257)).is_err());
    }
}
