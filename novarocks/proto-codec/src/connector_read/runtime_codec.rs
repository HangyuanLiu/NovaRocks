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
use novarocks_spi::connector::{CatalogProperties, ConnectorError};

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

/// FE-to-wire half of the connector read codec contract.
///
/// An encoder can create only carrier values.  It has no methods that turn
/// untrusted carrier data into provider-owned runtime handles.
pub trait ConnectorReadEncoder: Send + Sync {
    fn owner(&self) -> &str;

    fn encode_relation(
        &self,
        relation: &ConnectorReadRelation,
    ) -> Result<dto::CatalogTableHandle, ConnectorReadCodecError>;

    fn encode_column(
        &self,
        column: &ConnectorReadColumnHandle,
    ) -> Result<dto::ColumnHandle, ConnectorReadCodecError>;

    fn encode_transaction(
        &self,
        transaction: &ConnectorReadTransactionHandle,
    ) -> Result<dto::ConnectorTransactionHandle, ConnectorReadCodecError>;

    fn encode_split(
        &self,
        split: &ConnectorReadSplit,
    ) -> Result<dto::ConnectorSplit, ConnectorReadCodecError>;

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

/// Wire-to-BE half of the connector read codec contract.
///
/// A decoder is the only directional interface allowed to construct opaque
/// provider handles from a validated carrier.
pub trait ConnectorReadDecoder: Send + Sync {
    fn owner(&self) -> &str;

    fn decode_relation(
        &self,
        relation: &CatalogTableHandle,
    ) -> Result<ConnectorReadRelation, ConnectorReadCodecError>;

    fn decode_column(
        &self,
        column: &ValidatedColumnHandle,
    ) -> Result<ConnectorReadColumnHandle, ConnectorReadCodecError>;

    fn decode_transaction(
        &self,
        transaction: &ValidatedTransactionHandle,
    ) -> Result<ConnectorReadTransactionHandle, ConnectorReadCodecError>;

    fn decode_split(
        &self,
        split: &ValidatedConnectorSplit,
    ) -> Result<ConnectorReadSplit, ConnectorReadCodecError>;

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

    fn decode_scheduled_split(
        &self,
        scheduled: &ScheduledSplit,
    ) -> Result<DecodedScheduledReadSplit, ConnectorReadCodecError> {
        Ok(DecodedScheduledReadSplit::new(
            ReceivedScheduledSplit::from_scheduled(scheduled),
            self.decode_split(scheduled.split())?,
        ))
    }
}

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
/// invokes it only for complete CatalogManager-admitted immutable properties and owns all
/// subsequent
/// installation, retirement, and query lifecycle state.
pub trait ConnectorReadExecutionBundleFactory: Send + Sync {
    fn build(
        &self,
        properties: &CatalogProperties,
    ) -> Result<ConnectorReadExecutionBundle, ConnectorError>;
}

/// Sequence facts carried beside a provider-private split after decoding.
///
/// They are scheduling metadata only. The receiver retains no payload evidence
/// for retransmission: duplicate classification is solely the queue watermark.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReceivedScheduledSplit {
    sequence_id: u64,
    plan_node_id: i32,
}

impl ReceivedScheduledSplit {
    pub fn from_scheduled(split: &ScheduledSplit) -> Self {
        Self {
            sequence_id: split.sequence_id(),
            plan_node_id: split.plan_node_id(),
        }
    }

    pub const fn sequence_id(&self) -> u64 {
        self.sequence_id
    }

    pub const fn plan_node_id(&self) -> i32 {
        self.plan_node_id
    }
}

#[derive(Clone, Debug)]
pub struct DecodedScheduledReadSplit {
    received: ReceivedScheduledSplit,
    split: ConnectorReadSplit,
}

impl DecodedScheduledReadSplit {
    pub const fn new(received: ReceivedScheduledSplit, split: ConnectorReadSplit) -> Self {
        Self { received, split }
    }

    pub const fn received(&self) -> &ReceivedScheduledSplit {
        &self.received
    }

    pub const fn split(&self) -> &ConnectorReadSplit {
        &self.split
    }

    pub fn into_parts(self) -> (ReceivedScheduledSplit, ConnectorReadSplit) {
        (self.received, self.split)
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
            ReceivedScheduledSplit::from_scheduled(scheduled),
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

// T01 keeps the bidirectional spelling as a compatibility surface while the
// role migrations move their call sites to one directional half.  This is not
// a fallback: it preserves the exact already-installed provider codec until
// T06 removes all legacy callers.
impl<T: ConnectorReadCodec + ?Sized> ConnectorReadEncoder for T {
    fn owner(&self) -> &str {
        ConnectorReadCodec::owner(self)
    }

    fn encode_relation(
        &self,
        relation: &ConnectorReadRelation,
    ) -> Result<dto::CatalogTableHandle, ConnectorReadCodecError> {
        ConnectorReadCodec::encode_relation(self, relation)
    }

    fn encode_column(
        &self,
        column: &ConnectorReadColumnHandle,
    ) -> Result<dto::ColumnHandle, ConnectorReadCodecError> {
        ConnectorReadCodec::encode_column(self, column)
    }

    fn encode_transaction(
        &self,
        transaction: &ConnectorReadTransactionHandle,
    ) -> Result<dto::ConnectorTransactionHandle, ConnectorReadCodecError> {
        ConnectorReadCodec::encode_transaction(self, transaction)
    }

    fn encode_split(
        &self,
        split: &ConnectorReadSplit,
    ) -> Result<dto::ConnectorSplit, ConnectorReadCodecError> {
        ConnectorReadCodec::encode_split(self, split)
    }

    fn encode_tuple_domain(
        &self,
        domain: &TupleDomain<ConnectorReadColumnHandle>,
        path: FieldPath,
    ) -> Result<dto::TupleDomain, ConnectorReadCodecError> {
        ConnectorReadCodec::encode_tuple_domain(self, domain, path)
    }

    fn encode_assignment(
        &self,
        assignment: &Assignment<ConnectorReadColumnHandle>,
        path: FieldPath,
    ) -> Result<dto::ScanAssignment, ConnectorReadCodecError> {
        ConnectorReadCodec::encode_assignment(self, assignment, path)
    }

    fn encode_scheduled_split(
        &self,
        sequence_id: u64,
        plan_node_id: i32,
        split: &ConnectorReadSplit,
    ) -> Result<dto::ScheduledSplit, ConnectorReadCodecError> {
        ConnectorReadCodec::encode_scheduled_split(self, sequence_id, plan_node_id, split)
    }
}

impl<T: ConnectorReadCodec + ?Sized> ConnectorReadDecoder for T {
    fn owner(&self) -> &str {
        ConnectorReadCodec::owner(self)
    }

    fn decode_relation(
        &self,
        relation: &CatalogTableHandle,
    ) -> Result<ConnectorReadRelation, ConnectorReadCodecError> {
        ConnectorReadCodec::decode_relation(self, relation)
    }

    fn decode_column(
        &self,
        column: &ValidatedColumnHandle,
    ) -> Result<ConnectorReadColumnHandle, ConnectorReadCodecError> {
        ConnectorReadCodec::decode_column(self, column)
    }

    fn decode_transaction(
        &self,
        transaction: &ValidatedTransactionHandle,
    ) -> Result<ConnectorReadTransactionHandle, ConnectorReadCodecError> {
        ConnectorReadCodec::decode_transaction(self, transaction)
    }

    fn decode_split(
        &self,
        split: &ValidatedConnectorSplit,
    ) -> Result<ConnectorReadSplit, ConnectorReadCodecError> {
        ConnectorReadCodec::decode_split(self, split)
    }

    fn decode_tuple_domain(
        &self,
        domain: &dto::TupleDomain,
        path: FieldPath,
    ) -> Result<TupleDomain<ConnectorReadColumnHandle>, ConnectorReadCodecError> {
        ConnectorReadCodec::decode_tuple_domain(self, domain, path)
    }

    fn decode_validated_tuple_domain(
        &self,
        domain: &TupleDomain<ValidatedColumnHandle>,
    ) -> Result<TupleDomain<ConnectorReadColumnHandle>, ConnectorReadCodecError> {
        ConnectorReadCodec::decode_validated_tuple_domain(self, domain)
    }

    fn decode_assignment(
        &self,
        assignment: &dto::ScanAssignment,
        path: FieldPath,
    ) -> Result<Assignment<ConnectorReadColumnHandle>, ConnectorReadCodecError> {
        ConnectorReadCodec::decode_assignment(self, assignment, path)
    }

    fn decode_scheduled_split(
        &self,
        scheduled: &ScheduledSplit,
    ) -> Result<DecodedScheduledReadSplit, ConnectorReadCodecError> {
        ConnectorReadCodec::decode_scheduled_split(self, scheduled)
    }
}

/// Wraps one existing bidirectional provider codec in a FE-only encoder view.
/// This compatibility constructor is intentionally explicit so a caller never
/// obtains decoder authority by accident while the old codec spelling exists.
pub fn legacy_read_encoder(codec: Arc<dyn ConnectorReadCodec>) -> Arc<dyn ConnectorReadEncoder> {
    Arc::new(LegacyReadEncoder { codec })
}

/// Wraps one existing bidirectional provider codec in a BE-only decoder view.
pub fn legacy_read_decoder(codec: Arc<dyn ConnectorReadCodec>) -> Arc<dyn ConnectorReadDecoder> {
    Arc::new(LegacyReadDecoder { codec })
}

struct LegacyReadEncoder {
    codec: Arc<dyn ConnectorReadCodec>,
}

impl ConnectorReadEncoder for LegacyReadEncoder {
    fn owner(&self) -> &str {
        self.codec.owner()
    }
    fn encode_relation(
        &self,
        value: &ConnectorReadRelation,
    ) -> Result<dto::CatalogTableHandle, ConnectorReadCodecError> {
        self.codec.encode_relation(value)
    }
    fn encode_column(
        &self,
        value: &ConnectorReadColumnHandle,
    ) -> Result<dto::ColumnHandle, ConnectorReadCodecError> {
        self.codec.encode_column(value)
    }
    fn encode_transaction(
        &self,
        value: &ConnectorReadTransactionHandle,
    ) -> Result<dto::ConnectorTransactionHandle, ConnectorReadCodecError> {
        self.codec.encode_transaction(value)
    }
    fn encode_split(
        &self,
        value: &ConnectorReadSplit,
    ) -> Result<dto::ConnectorSplit, ConnectorReadCodecError> {
        self.codec.encode_split(value)
    }
    fn encode_tuple_domain(
        &self,
        value: &TupleDomain<ConnectorReadColumnHandle>,
        path: FieldPath,
    ) -> Result<dto::TupleDomain, ConnectorReadCodecError> {
        self.codec.encode_tuple_domain(value, path)
    }
    fn encode_assignment(
        &self,
        value: &Assignment<ConnectorReadColumnHandle>,
        path: FieldPath,
    ) -> Result<dto::ScanAssignment, ConnectorReadCodecError> {
        self.codec.encode_assignment(value, path)
    }
    fn encode_scheduled_split(
        &self,
        sequence_id: u64,
        plan_node_id: i32,
        value: &ConnectorReadSplit,
    ) -> Result<dto::ScheduledSplit, ConnectorReadCodecError> {
        self.codec
            .encode_scheduled_split(sequence_id, plan_node_id, value)
    }
}

struct LegacyReadDecoder {
    codec: Arc<dyn ConnectorReadCodec>,
}

impl ConnectorReadDecoder for LegacyReadDecoder {
    fn owner(&self) -> &str {
        self.codec.owner()
    }
    fn decode_relation(
        &self,
        value: &CatalogTableHandle,
    ) -> Result<ConnectorReadRelation, ConnectorReadCodecError> {
        self.codec.decode_relation(value)
    }
    fn decode_column(
        &self,
        value: &ValidatedColumnHandle,
    ) -> Result<ConnectorReadColumnHandle, ConnectorReadCodecError> {
        self.codec.decode_column(value)
    }
    fn decode_transaction(
        &self,
        value: &ValidatedTransactionHandle,
    ) -> Result<ConnectorReadTransactionHandle, ConnectorReadCodecError> {
        self.codec.decode_transaction(value)
    }
    fn decode_split(
        &self,
        value: &ValidatedConnectorSplit,
    ) -> Result<ConnectorReadSplit, ConnectorReadCodecError> {
        self.codec.decode_split(value)
    }
    fn decode_tuple_domain(
        &self,
        value: &dto::TupleDomain,
        path: FieldPath,
    ) -> Result<TupleDomain<ConnectorReadColumnHandle>, ConnectorReadCodecError> {
        self.codec.decode_tuple_domain(value, path)
    }
    fn decode_validated_tuple_domain(
        &self,
        value: &TupleDomain<ValidatedColumnHandle>,
    ) -> Result<TupleDomain<ConnectorReadColumnHandle>, ConnectorReadCodecError> {
        self.codec.decode_validated_tuple_domain(value)
    }
    fn decode_assignment(
        &self,
        value: &dto::ScanAssignment,
        path: FieldPath,
    ) -> Result<Assignment<ConnectorReadColumnHandle>, ConnectorReadCodecError> {
        self.codec.decode_assignment(value, path)
    }
    fn decode_scheduled_split(
        &self,
        value: &ScheduledSplit,
    ) -> Result<DecodedScheduledReadSplit, ConnectorReadCodecError> {
        self.codec.decode_scheduled_split(value)
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
        codec: &dyn ConnectorReadDecoder,
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
