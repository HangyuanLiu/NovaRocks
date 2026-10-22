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

//! Frontend assembly of domain records from relayed root bodies.
//!
//! Internal-facts records may cross segment boundaries. A record that lies
//! wholly inside one body is handed to its decoder without a copy. A record
//! that crosses bodies is assembled only after its fixed header has been
//! parsed and its declared length checked against the caller's bound; the
//! assembly buffer is then reserved once at exactly that length. End inside
//! an unfinished record is refused. Taking a body into assembly is what lets
//! its stream item be acknowledged before the whole record arrives; the
//! assembled bytes are still not published facts until the decoder accepts
//! the complete record.

use crate::root_cow_selection_codec::{COW_SELECTION_HEADER_BYTES, CowSelectionRecordHeader};
use crate::root_statistics_codec::{STATISTICS_HEADER_BYTES, StatisticsArtifactHeader};
use crate::root_write_commit_codec::{WRITE_COMMIT_HEADER_BYTES, WriteCommitRecordHeader};

/// The record framing of one internal-facts domain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RootRecordDomain {
    WriteCommit,
    Statistics,
    CowSelection,
}

impl RootRecordDomain {
    const fn header_bytes(self) -> usize {
        match self {
            Self::WriteCommit => WRITE_COMMIT_HEADER_BYTES,
            Self::Statistics => STATISTICS_HEADER_BYTES,
            Self::CowSelection => COW_SELECTION_HEADER_BYTES,
        }
    }

    /// The complete record length its validated header declares.
    fn record_bytes(self, prefix: &[u8]) -> Result<usize, RootRecordFramingError> {
        match self {
            Self::WriteCommit => WriteCommitRecordHeader::parse(prefix)
                .map(WriteCommitRecordHeader::record_bytes)
                .map_err(RootRecordFramingError::Write),
            Self::Statistics => StatisticsArtifactHeader::parse(prefix)
                .map(StatisticsArtifactHeader::record_bytes)
                .map_err(RootRecordFramingError::Statistics),
            Self::CowSelection => CowSelectionRecordHeader::parse(prefix)
                .map_err(RootRecordFramingError::Cow)
                .and_then(|header| {
                    usize::try_from(header.record_bytes())
                        .map_err(|_| RootRecordFramingError::LengthOverflow)
                }),
        }
    }
}

/// Technical framing facts retain their original codec producer. The
/// caller's domain error stays distinct from a framing error.
#[derive(Debug)]
pub enum RootRecordFramingError {
    Write(crate::root_write_commit_codec::WriteCommitCodecError),
    Statistics(crate::root_statistics_codec::StatisticsCodecError),
    Cow(crate::root_cow_selection_codec::CowSelectionCodecError),
    Capacity { requested: usize, limit: usize },
    Incomplete,
    LengthOverflow,
}
impl std::fmt::Display for RootRecordFramingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Write(e) => e.fmt(f),
            Self::Statistics(e) => e.fmt(f),
            Self::Cow(e) => e.fmt(f),
            Self::Capacity { requested, limit } => write!(
                f,
                "domain record declares {requested} bytes, beyond its {limit} byte assembly bound"
            ),
            Self::Incomplete => f.write_str("root stream ended inside an unfinished domain record"),
            Self::LengthOverflow => f.write_str("domain record length exceeds usize"),
        }
    }
}
impl std::error::Error for RootRecordFramingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Write(error) => Some(error),
            Self::Statistics(error) => Some(error),
            Self::Cow(error) => Some(error),
            Self::Capacity { .. } | Self::Incomplete | Self::LengthOverflow => None,
        }
    }
}
impl novarocks_execution_contract::failure::FailureProjection for RootRecordFramingError {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::*;
        match self {
            Self::Write(e) => e.failure_fact(),
            Self::Statistics(e) => e.failure_fact(),
            Self::Cow(e) => e.failure_fact(),
            Self::Capacity { .. } => FailureFact::new(
                FailureClass::Resource,
                FailureReason::new(domain::NATIVE, std::num::NonZeroU16::new(221).unwrap()),
            ),
            Self::Incomplete => FailureFact::new(
                FailureClass::Data,
                FailureReason::new(domain::NATIVE, std::num::NonZeroU16::new(222).unwrap()),
            ),
            Self::LengthOverflow => FailureFact::new(
                FailureClass::Representation,
                FailureReason::new(domain::NATIVE, std::num::NonZeroU16::new(223).unwrap()),
            ),
        }
    }
}
#[derive(Debug)]
pub enum RootRecordAssemblyError<E> {
    Framing(RootRecordFramingError),
    Sink(E),
}
impl<E: std::fmt::Display> std::fmt::Display for RootRecordAssemblyError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Framing(e) => e.fmt(f),
            Self::Sink(e) => e.fmt(f),
        }
    }
}
impl<E: std::error::Error + 'static> std::error::Error for RootRecordAssemblyError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Framing(error) => Some(error),
            Self::Sink(error) => Some(error),
        }
    }
}

/// One root stream's record frontier across relayed bodies.
#[derive(Debug)]
pub struct RootRecordAssembly {
    domain: RootRecordDomain,
    max_record_bytes: usize,
    partial: Vec<u8>,
    needed: Option<usize>,
}

impl RootRecordAssembly {
    /// `max_record_bytes` bounds every assembled record; it is the share of
    /// the caller's window that assembly may hold.
    pub fn new(domain: RootRecordDomain, max_record_bytes: usize) -> Self {
        Self {
            domain,
            max_record_bytes,
            partial: Vec::new(),
            needed: None,
        }
    }

    /// Bytes currently held by assembly.
    pub fn retained_bytes(&self) -> usize {
        self.partial.capacity()
    }

    /// Feed one relayed body. Each complete record is passed to `sink` in
    /// order; a record still incomplete at the end of `body` is kept.
    pub fn push<E>(
        &mut self,
        mut body: &[u8],
        mut sink: impl FnMut(&[u8]) -> Result<(), E>,
    ) -> Result<(), RootRecordAssemblyError<E>> {
        let header = self.domain.header_bytes();
        while !body.is_empty() {
            if self.partial.is_empty() && body.len() >= header {
                // A whole record inside this body needs no copy.
                let length = self
                    .checked_length(&body[..header])
                    .map_err(RootRecordAssemblyError::Framing)?;
                if body.len() >= length {
                    sink(&body[..length]).map_err(RootRecordAssemblyError::Sink)?;
                    body = &body[length..];
                    continue;
                }
            }
            if self.partial.len() < header {
                let take = (header - self.partial.len()).min(body.len());
                if self.partial.capacity() == 0 {
                    self.partial.reserve_exact(header);
                }
                self.partial.extend_from_slice(&body[..take]);
                body = &body[take..];
                if self.partial.len() < header {
                    continue;
                }
                let length = self
                    .checked_length(&self.partial[..header])
                    .map_err(RootRecordAssemblyError::Framing)?;
                self.partial.reserve_exact(length - self.partial.len());
                self.needed = Some(length);
            }
            let needed = self.needed.expect("a parsed header declares its length");
            let take = (needed - self.partial.len()).min(body.len());
            self.partial.extend_from_slice(&body[..take]);
            body = &body[take..];
            if self.partial.len() == needed {
                sink(&self.partial).map_err(RootRecordAssemblyError::Sink)?;
                self.partial = Vec::new();
                self.needed = None;
            }
        }
        Ok(())
    }

    /// The stream's End: no record may remain unfinished.
    pub fn finish(&self) -> Result<(), RootRecordFramingError> {
        if self.partial.is_empty() {
            Ok(())
        } else {
            Err(RootRecordFramingError::Incomplete)
        }
    }

    fn checked_length(&self, prefix: &[u8]) -> Result<usize, RootRecordFramingError> {
        let length = self.domain.record_bytes(prefix)?;
        if length > self.max_record_bytes {
            return Err(RootRecordFramingError::Capacity {
                requested: length,
                limit: self.max_record_bytes,
            });
        }
        Ok(length)
    }
}
