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

//! Pure generation decoding and the original session's bounded receiving side.

use super::*;
use crate::query_execution::write_result::RootWriteDecodeFailure;
use novarocks_proto_codec::connector_write::{ConnectorWriteCodecError, ValidatedCommitFragment};
use novarocks_spi::connector::ConnectorWriteFragmentWireDecoder;
use std::sync::Weak;

/// This capability exposes the exact generation's decode direction only. The
/// original stable session retains its non-cloneable catalog generation lease.
#[derive(Clone)]
pub(crate) struct WriteComputationDecode {
    origin: Arc<()>,
    decoder: Arc<dyn ConnectorWriteFragmentWireDecoder>,
    query_targets: Arc<[WriteTargetOrdinal]>,
}

/// One actual graph after a single canonical-to-Provider conversion. No
/// Provider publication predicate or terminal action has been invoked.
pub(crate) struct SessionDecodedWriteSegment {
    origin: Arc<()>,
    prepared: ConnectorPreparedWriteSet,
    fragment_sizes: Vec<usize>,
    statistics: Vec<novarocks_spi::connector::write_stack::WriteStatisticsArtifact>,
    retention: Option<crate::query_execution::internal_result_cpu::InternalResultRetention>,
}

#[derive(Debug)]
struct OriginalFragmentCodecFailure(ConnectorWriteCodecError);
impl std::fmt::Display for OriginalFragmentCodecFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for OriginalFragmentCodecFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl novarocks_failure_contract::FailureProjection for OriginalFragmentCodecFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::FailureProjection;
        self.0.protocol().failure_fact()
    }
}

impl WriteComputationDecode {
    pub(crate) fn interpret(
        &self,
        prepared: DecodedPreparedWriteSet,
    ) -> Result<SessionDecodedWriteSegment, RootWriteDecodeFailure> {
        let (row_count, fragments, statistics, retention) = prepared.into_parts();
        let fragment_sizes = fragments.iter().map(|(_, bytes)| bytes.len()).collect();
        // The original buffers remain governed until all decoded Provider
        // objects have been admitted; no independent byte clone is made.
        let decoded = self.interpret_borrowed(row_count, &fragments)?;
        drop(fragments);
        Ok(SessionDecodedWriteSegment {
            origin: Arc::clone(&self.origin),
            prepared: decoded,
            fragment_sizes,
            statistics,
            retention,
        })
    }

    pub(super) fn interpret_borrowed(
        &self,
        row_count: u64,
        fragments: &[(WriteTargetOrdinal, Vec<u8>)],
    ) -> Result<ConnectorPreparedWriteSet, RootWriteDecodeFailure> {
        use novarocks_proto_codec::{FieldPath, ProtocolError, ProtocolErrorKind};
        use prost::Message;
        let mut decoded = Vec::with_capacity(fragments.len());
        for (index, (target, bytes)) in fragments.iter().enumerate() {
            let raw =
                write_dto::ConnectorCommitFragment::decode(bytes.as_slice()).map_err(|error| {
                    RootWriteDecodeFailure::source(ProtocolError::new(
                        FieldPath::root("prepared_write_set").index(index),
                        ProtocolErrorKind::InvalidValue,
                        format!("invalid commit fragment protobuf: {error}"),
                    ))
                })?;
            let validated = ValidatedCommitFragment::parse(
                raw,
                FieldPath::root("prepared_write_set").index(index),
            )
            .map_err(RootWriteDecodeFailure::source)?;
            let fragment = self
                .decoder
                .decode_commit_fragment(&validated)
                .map_err(|error| {
                    RootWriteDecodeFailure::source(OriginalFragmentCodecFailure(error))
                })?;
            decoded.push((*target, fragment));
        }
        ConnectorPreparedWriteSet::try_new(row_count, decoded, &self.query_targets)
            .map_err(RootWriteDecodeFailure::source)
    }
}

/// An exact one-time receiving fact, with no publication permission. Payloads
/// and their original allowance already belong to the session receiving state.
pub(crate) struct AcceptedWriteSegment {
    origin: Arc<()>,
    row_count: u64,
    empty: bool,
}
impl AcceptedWriteSegment {
    pub(crate) fn row_count(&self) -> u64 {
        self.row_count
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.empty
    }
    pub(crate) fn belongs_to(&self, session: &ConnectorWriteSession) -> bool {
        Arc::ptr_eq(&self.origin, &session.decode_origin)
    }
}

/// A concrete receiving-only endpoint. It cannot finish, abort or reconcile.
/// The session itself remains in stable custody outside every CPU future.
#[derive(Clone)]
pub(crate) struct WriteReceivingPort {
    session: Weak<ConnectorWriteSession>,
}
impl WriteReceivingPort {
    pub(crate) fn accept(
        &self,
        segment: SessionDecodedWriteSegment,
    ) -> Result<AcceptedWriteSegment, ConnectorError> {
        let session = self.session.upgrade().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "original write receiving session exited",
            )
        })?;
        session.accept_interpreted(segment)
    }
}

impl ConnectorWriteSession {
    /// Until the P04 multi-segment router is installed, refuse replacing an
    /// existing original capability. A failed later seed cannot erase it.
    pub(crate) fn retain_computation_storage_resolver(
        &self,
        resolver: Arc<dyn ConnectorStorageResolver>,
    ) -> Result<(), ConnectorError> {
        let mut storage = self.lock_terminal_storage()?;
        match storage.as_ref() {
            Some(original) if !Arc::ptr_eq(original, &resolver) => Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "write receiving requires its original terminal storage capability",
            )),
            Some(_) => Ok(()),
            None => {
                *storage = Some(resolver);
                Ok(())
            }
        }
    }
    pub(crate) fn computation_decode(
        &self,
        query_targets: &[WriteTargetOrdinal],
    ) -> Result<WriteComputationDecode, ConnectorError> {
        crate::query_execution::write_result::RootWriteDecodeContract::try_new(
            query_targets,
            self.targets(),
        )
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::InvalidRequest, error))?;
        Ok(WriteComputationDecode {
            origin: Arc::clone(&self.decode_origin),
            decoder: self.lease.fragment_decoder(),
            query_targets: Arc::from(query_targets),
        })
    }
    pub(crate) fn receiving_port(self: &Arc<Self>) -> WriteReceivingPort {
        WriteReceivingPort {
            session: Arc::downgrade(self),
        }
    }
    pub(super) fn receiving_terminal_guard(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, Option<TerminalDecision>>, ConnectorError> {
        let terminal = self.lock_terminal()?;
        if terminal.is_some() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write session already reached a terminal decision",
            ));
        }
        Ok(terminal)
    }
    pub(super) fn accept_interpreted(
        &self,
        segment: SessionDecodedWriteSegment,
    ) -> Result<AcceptedWriteSegment, ConnectorError> {
        // Keep the original decision lock through the pure receiving cut.
        // A concurrent abort cannot pass between the check and payload move.
        let _terminal = self.receiving_terminal_guard()?;
        if !Arc::ptr_eq(&segment.origin, &self.decode_origin) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "write segment belongs to a different original session",
            ));
        }
        let SessionDecodedWriteSegment {
            prepared,
            fragment_sizes,
            statistics,
            retention,
            ..
        } = segment;
        let row_count = prepared.row_count();
        let fragments = prepared.into_fragments();
        if fragments.len() != fragment_sizes.len() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "interpreted write lost its canonical byte ledger",
            ));
        }
        let empty = fragments.is_empty();
        let mut accumulated = self.lock_accumulated()?;
        if let (Some(current), Some(incoming)) = (&accumulated.retention, &retention) {
            if !current.is_same_admission(incoming) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "write union received a different result allowance or attribution",
                ));
            }
        } else if accumulated.retention.is_some() && retention.is_none()
            || accumulated.retention.is_none()
                && retention.is_some()
                && (!accumulated.fragments.is_empty() || !accumulated.statistics.is_empty())
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "write union cannot mix governed and legacy payload ownership",
            ));
        }
        let mut next_rows = accumulated.rows;
        next_rows.add(row_count)?;
        let mut next_ledger = accumulated.ledger;
        for bytes in fragment_sizes {
            next_ledger.reserve_fragment(bytes)?;
        }
        let expected = &self.statistics_expected;
        let mut observed = std::collections::BTreeSet::new();
        if accumulated
            .statistics
            .len()
            .checked_add(statistics.len())
            .is_none_or(|count| {
                count > novarocks_spi::connector::MAX_CONNECTOR_STATISTICS_ARTIFACTS
            })
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "write session statistics artifact count exceeded its limit",
            ));
        }
        let mut next_body_bytes = accumulated.statistics_body_bytes;
        let mut next_property_bytes = accumulated.statistics_property_bytes;
        for artifact in &statistics {
            let key = (artifact.target(), artifact.draft().identity().clone());
            if !expected.contains(&key)
                || accumulated.statistics_identities.contains(&key)
                || !observed.insert(key)
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "write session accumulated an unknown or duplicate statistics artifact",
                ));
            }
            next_body_bytes = next_body_bytes
                .checked_add(artifact.draft().body().len())
                .filter(|bytes| {
                    *bytes <= novarocks_spi::connector::MAX_CONNECTOR_STATISTICS_RESULT_BODY_BYTES
                })
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::ResourceExhausted,
                        "write session statistics body bytes exceeded their limit",
                    )
                })?;
            let property_bytes =
                artifact
                    .draft()
                    .properties()
                    .iter()
                    .try_fold(0usize, |total, (key, value)| {
                        total
                            .checked_add(key.len())
                            .and_then(|sum| sum.checked_add(value.len()))
                    });
            next_property_bytes = property_bytes
                .and_then(|bytes| next_property_bytes.checked_add(bytes))
                .filter(|bytes| {
                    *bytes <= novarocks_spi::connector::MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES
                })
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::ResourceExhausted,
                        "write session statistics property bytes exceeded their limit",
                    )
                })?;
        }
        if accumulated.retention.is_none() {
            accumulated.retention = retention;
        }
        accumulated.rows = next_rows;
        accumulated.ledger = next_ledger;
        accumulated.fragments.extend(fragments);
        accumulated.statistics_body_bytes = next_body_bytes;
        accumulated.statistics_property_bytes = next_property_bytes;
        accumulated.statistics_identities.extend(observed);
        accumulated.statistics.extend(statistics);
        Ok(AcceptedWriteSegment {
            origin: Arc::clone(&self.decode_origin),
            row_count,
            empty,
        })
    }
}
