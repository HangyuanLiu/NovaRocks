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

//! The wire codec boundary for transport-neutral connector reads.
//!
//! A codec is selected as part of an exact installed connector binding.  It
//! is the only boundary that may turn a validated closed carrier into an SPI
//! runtime handle or turn such a handle back into a central-IDL message.
//! Metadata, split enumeration, reader creation, registries, and lifecycle
//! state deliberately do not live here.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use crate::{FieldPath, ProtocolError, ProtocolErrorKind};
use novarocks_proto_models::connector_read as dto;
use novarocks_spi::connector::read_stack::{
    Assignment, ConnectorReadColumnHandle, ConnectorReadProviderFactory, ConnectorReadRelation,
    ConnectorReadSplit, ConnectorReadTransactionHandle, ConnectorReadWorkSource, TupleDomain,
};
use novarocks_spi::connector::{ConnectorError, ConnectorExecutionBindingKey};

use super::{
    CatalogTableHandle, ConnectorTableScanSource, ScheduledSplit, ValidatedColumnHandle,
    ValidatedConnectorSplit, ValidatedTransactionHandle, decode_tuple_domain, encode_tuple_domain,
    encode_value_type,
};

/// A codec error keeps the original wire field path and adds the binding owner
/// that selected this codec.  It never stores arbitrary provider payloads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorReadCodecError {
    owner: Arc<str>,
    protocol: ProtocolError,
}

impl ConnectorReadCodecError {
    pub fn new(owner: impl AsRef<str>, protocol: ProtocolError) -> Self {
        Self {
            owner: Arc::from(owner.as_ref()),
            protocol,
        }
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }

    pub const fn protocol(&self) -> &ProtocolError {
        &self.protocol
    }

    pub fn invalid(owner: impl AsRef<str>, path: FieldPath, detail: impl Into<String>) -> Self {
        Self::new(
            owner,
            ProtocolError::new(path, ProtocolErrorKind::InvalidValue, detail),
        )
    }
}

impl fmt::Display for ConnectorReadCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "connector read codec '{}' rejected: {}",
            self.owner, self.protocol
        )
    }
}

impl std::error::Error for ConnectorReadCodecError {}

/// One complete worker-side read bundle for an exact execution binding.
///
/// The backend installs this pair atomically after its existing Host has
/// admitted the binding.  It has no registry or lifecycle authority; the
/// factory merely gives the role matching provider services and codec for the
/// same opaque-handle family.
#[derive(Clone)]
pub struct ConnectorReadExecutionBundle {
    provider_factory: Arc<dyn ConnectorReadProviderFactory>,
    codec: Arc<dyn ConnectorReadCodec>,
}

impl ConnectorReadExecutionBundle {
    pub fn new(
        provider_factory: Arc<dyn ConnectorReadProviderFactory>,
        codec: Arc<dyn ConnectorReadCodec>,
    ) -> Self {
        Self {
            provider_factory,
            codec,
        }
    }

    pub fn provider_factory(&self) -> Arc<dyn ConnectorReadProviderFactory> {
        Arc::clone(&self.provider_factory)
    }

    pub fn codec(&self) -> Arc<dyn ConnectorReadCodec> {
        Arc::clone(&self.codec)
    }
}

impl fmt::Debug for ConnectorReadExecutionBundle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectorReadExecutionBundle")
            .finish_non_exhaustive()
    }
}

/// Provider-owned constructor for a worker read bundle.
///
/// Server composition supplies implementations by provider kind. The backend
/// invokes it only for a Host-admitted exact key and owns all subsequent
/// installation, retirement, and query lifecycle state.
pub trait ConnectorReadExecutionBundleFactory: Send + Sync {
    fn build(
        &self,
        key: &ConnectorExecutionBindingKey,
    ) -> Result<ConnectorReadExecutionBundle, ConnectorError>;
}

/// Immutable evidence retained by a role for exact TaskUpdate replay.
///
/// The bytes are the received validated message encoding. They are not rebuilt
/// from a recovered provider split, because internal map order can differ from
/// the frozen protocol's canonical order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReceivedScheduledSplitEvidence {
    sequence_id: u64,
    plan_node_id: i32,
    canonical_bytes: Arc<[u8]>,
}

impl ReceivedScheduledSplitEvidence {
    pub fn from_scheduled(split: &ScheduledSplit) -> Self {
        Self {
            sequence_id: split.sequence_id(),
            plan_node_id: split.plan_node_id(),
            canonical_bytes: Arc::from(split.canonical_bytes()),
        }
    }

    pub const fn sequence_id(&self) -> u64 {
        self.sequence_id
    }

    pub const fn plan_node_id(&self) -> i32 {
        self.plan_node_id
    }

    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }
}

#[derive(Clone, Debug)]
pub struct DecodedScheduledReadSplit {
    evidence: ReceivedScheduledSplitEvidence,
    split: ConnectorReadSplit,
}

impl DecodedScheduledReadSplit {
    pub const fn new(evidence: ReceivedScheduledSplitEvidence, split: ConnectorReadSplit) -> Self {
        Self { evidence, split }
    }

    pub const fn evidence(&self) -> &ReceivedScheduledSplitEvidence {
        &self.evidence
    }

    pub const fn split(&self) -> &ConnectorReadSplit {
        &self.split
    }

    pub fn into_parts(self) -> (ReceivedScheduledSplitEvidence, ConnectorReadSplit) {
        (self.evidence, self.split)
    }
}

/// A provider-owned codec for one exact installed connector binding.
pub trait ConnectorReadCodec: Send + Sync {
    /// A stable, non-secret diagnostic name for the selected binding.
    fn owner(&self) -> &str;

    fn decode_relation(
        &self,
        relation: &CatalogTableHandle,
    ) -> Result<ConnectorReadRelation, ConnectorReadCodecError>;

    fn encode_relation(
        &self,
        relation: &ConnectorReadRelation,
    ) -> Result<dto::CatalogTableHandle, ConnectorReadCodecError>;

    fn decode_column(
        &self,
        column: &ValidatedColumnHandle,
    ) -> Result<ConnectorReadColumnHandle, ConnectorReadCodecError>;

    fn encode_column(
        &self,
        column: &ConnectorReadColumnHandle,
    ) -> Result<dto::ColumnHandle, ConnectorReadCodecError>;

    fn decode_transaction(
        &self,
        transaction: &ValidatedTransactionHandle,
    ) -> Result<ConnectorReadTransactionHandle, ConnectorReadCodecError>;

    fn encode_transaction(
        &self,
        transaction: &ConnectorReadTransactionHandle,
    ) -> Result<dto::ConnectorTransactionHandle, ConnectorReadCodecError>;

    fn decode_split(
        &self,
        split: &ValidatedConnectorSplit,
    ) -> Result<ConnectorReadSplit, ConnectorReadCodecError>;

    fn encode_split(
        &self,
        split: &ConnectorReadSplit,
    ) -> Result<dto::ConnectorSplit, ConnectorReadCodecError>;

    fn decode_tuple_domain(
        &self,
        domain: &dto::TupleDomain,
        path: FieldPath,
    ) -> Result<TupleDomain<ConnectorReadColumnHandle>, ConnectorReadCodecError> {
        let validated = decode_tuple_domain(domain, path)
            .map_err(|error| ConnectorReadCodecError::new(self.owner(), error))?;
        let Some(domains) = validated.domains() else {
            return Ok(TupleDomain::none());
        };
        let mut decoded = BTreeMap::new();
        for (column, value) in domains {
            decoded.insert(self.decode_column(column)?, value.clone());
        }
        TupleDomain::with_column_domains(decoded).map_err(|error| {
            ConnectorReadCodecError::invalid(
                self.owner(),
                FieldPath::root("tuple_domain"),
                error.to_string(),
            )
        })
    }

    fn decode_validated_tuple_domain(
        &self,
        domain: &TupleDomain<ValidatedColumnHandle>,
    ) -> Result<TupleDomain<ConnectorReadColumnHandle>, ConnectorReadCodecError> {
        let Some(domains) = domain.domains() else {
            return Ok(TupleDomain::none());
        };
        let mut decoded = BTreeMap::new();
        for (column, value) in domains {
            decoded.insert(self.decode_column(column)?, value.clone());
        }
        TupleDomain::with_column_domains(decoded).map_err(|error| {
            ConnectorReadCodecError::invalid(
                self.owner(),
                FieldPath::root("tuple_domain"),
                error.to_string(),
            )
        })
    }

    fn encode_tuple_domain(
        &self,
        domain: &TupleDomain<ConnectorReadColumnHandle>,
        path: FieldPath,
    ) -> Result<dto::TupleDomain, ConnectorReadCodecError> {
        let Some(domains) = domain.domains() else {
            return Ok(encode_tuple_domain(&TupleDomain::none()));
        };
        let mut validated = BTreeMap::new();
        for (column, value) in domains {
            let raw = self.encode_column(column)?;
            let raw = ValidatedColumnHandle::parse(raw, path.clone().field("column"))
                .map_err(|error| ConnectorReadCodecError::new(self.owner(), error))?;
            validated.insert(raw, value.clone());
        }
        Ok(encode_tuple_domain(
            &TupleDomain::with_column_domains(validated).map_err(|error| {
                ConnectorReadCodecError::invalid(self.owner(), path, error.to_string())
            })?,
        ))
    }

    fn decode_assignment(
        &self,
        assignment: &dto::ScanAssignment,
        path: FieldPath,
    ) -> Result<Assignment<ConnectorReadColumnHandle>, ConnectorReadCodecError> {
        let validated = super::ScanAssignment::parse(assignment.clone(), path.clone())
            .map_err(|error| ConnectorReadCodecError::new(self.owner(), error))?;
        Assignment::try_new(
            validated.variable(),
            self.decode_column(validated.column())?,
            validated.value_type(),
        )
        .map_err(|error| ConnectorReadCodecError::invalid(self.owner(), path, error.to_string()))
    }

    fn encode_assignment(
        &self,
        assignment: &Assignment<ConnectorReadColumnHandle>,
        _path: FieldPath,
    ) -> Result<dto::ScanAssignment, ConnectorReadCodecError> {
        Ok(dto::ScanAssignment {
            variable: assignment.variable().to_owned(),
            column: Some(self.encode_column(assignment.column())?),
            value_type: Some(encode_value_type(assignment.value_type())),
        })
    }

    fn decode_scheduled_split(
        &self,
        scheduled: &ScheduledSplit,
    ) -> Result<DecodedScheduledReadSplit, ConnectorReadCodecError> {
        Ok(DecodedScheduledReadSplit::new(
            ReceivedScheduledSplitEvidence::from_scheduled(scheduled),
            self.decode_split(scheduled.split())?,
        ))
    }

    fn encode_scheduled_split(
        &self,
        sequence_id: u64,
        plan_node_id: i32,
        split: &ConnectorReadSplit,
    ) -> Result<dto::ScheduledSplit, ConnectorReadCodecError> {
        Ok(dto::ScheduledSplit {
            sequence_id,
            plan_node_id,
            split: Some(self.encode_split(split)?),
        })
    }
}

/// Codec-only form of a frozen scan. Roles use this only at a protocol edge;
/// after decoding they retain the SPI values rather than DTO-backed handles.
#[derive(Clone, Debug)]
pub struct DecodedConnectorReadScan {
    relation: ConnectorReadRelation,
    assignments: Vec<Assignment<ConnectorReadColumnHandle>>,
    enforced_predicate: TupleDomain<ConnectorReadColumnHandle>,
    unenforced_predicate: TupleDomain<ConnectorReadColumnHandle>,
    remaining_expression: Option<novarocks_spi::connector::read_stack::ConnectorExpression>,
    work_source: ConnectorReadWorkSource,
}

impl DecodedConnectorReadScan {
    pub fn decode(
        codec: &dyn ConnectorReadCodec,
        source: &ConnectorTableScanSource,
    ) -> Result<Self, ConnectorReadCodecError> {
        let relation = codec.decode_relation(source.table())?;
        let assignments = source
            .assignments()
            .iter()
            .enumerate()
            .map(|(index, assignment)| {
                codec.decode_assignment(
                    assignment.as_proto(),
                    FieldPath::root("connector_table_scan_source")
                        .field("assignments")
                        .index(index),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            relation,
            assignments,
            enforced_predicate: codec.decode_validated_tuple_domain(source.enforced_predicate())?,
            unenforced_predicate: codec
                .decode_validated_tuple_domain(source.unenforced_predicate())?,
            remaining_expression: source.remaining_expression().cloned(),
            work_source: match source.work_source() {
                super::ScanWorkSource::RuntimeSplits => ConnectorReadWorkSource::RuntimeSplits,
                super::ScanWorkSource::WholeRelation => ConnectorReadWorkSource::WholeRelation,
            },
        })
    }

    pub const fn relation(&self) -> &ConnectorReadRelation {
        &self.relation
    }

    pub fn assignments(&self) -> &[Assignment<ConnectorReadColumnHandle>] {
        &self.assignments
    }

    pub const fn enforced_predicate(&self) -> &TupleDomain<ConnectorReadColumnHandle> {
        &self.enforced_predicate
    }

    pub const fn unenforced_predicate(&self) -> &TupleDomain<ConnectorReadColumnHandle> {
        &self.unenforced_predicate
    }

    pub const fn remaining_expression(
        &self,
    ) -> Option<&novarocks_spi::connector::read_stack::ConnectorExpression> {
        self.remaining_expression.as_ref()
    }

    pub const fn work_source(&self) -> ConnectorReadWorkSource {
        self.work_source
    }
}
