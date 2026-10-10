// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Runtime negotiation adapters for the shared immutable read facts.
use super::ConnectorReadRelationVersion;
use crate::connector::{ConnectorError, ConnectorErrorKind};

pub use novarocks_connector_contract::{
    ConnectorReadArtifactCoverage, ConnectorReadBucketLayout, ConnectorReadDistribution,
    ConnectorReadInputVersion, ConnectorReadMetadataKind, ConnectorReadMetadataRequest,
    ConnectorReadMetadataVersion, ConnectorReadNullOrdering, ConnectorReadOrderingKey,
    ConnectorReadPartitionCountDomain, ConnectorReadPartitionHash, ConnectorReadProperties,
    ConnectorReadSortDirection, ConnectorReadStaticFacts, MAX_READ_COVERAGE_EVIDENCE_BYTES,
    MAX_READ_INPUT_VERSION_BYTES, MAX_READ_PROPERTY_KEYS,
};

impl TryFrom<ConnectorReadRelationVersion> for ConnectorReadMetadataVersion {
    type Error = ConnectorError;

    fn try_from(value: ConnectorReadRelationVersion) -> Result<Self, Self::Error> {
        match value {
            ConnectorReadRelationVersion::Current => Ok(Self::Current),
            ConnectorReadRelationVersion::SnapshotId(id) => Self::try_new(Self::SnapshotId(id)),
            ConnectorReadRelationVersion::Reference => Err(invalid(
                "connector metadata request cannot preserve a reference without its name",
            )),
        }
    }
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}
