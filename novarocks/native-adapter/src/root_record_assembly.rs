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
    fn record_bytes(self, prefix: &[u8]) -> Result<usize, String> {
        match self {
            Self::WriteCommit => WriteCommitRecordHeader::parse(prefix)
                .map(WriteCommitRecordHeader::record_bytes)
                .map_err(|error| error.to_string()),
            Self::Statistics => StatisticsArtifactHeader::parse(prefix)
                .map(StatisticsArtifactHeader::record_bytes)
                .map_err(|error| error.to_string()),
            Self::CowSelection => CowSelectionRecordHeader::parse(prefix)
                .map_err(|error| error.to_string())
                .and_then(|header| {
                    usize::try_from(header.record_bytes())
                        .map_err(|_| "COW selection record length exceeds usize".to_string())
                }),
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
    pub fn push(
        &mut self,
        mut body: &[u8],
        mut sink: impl FnMut(&[u8]) -> Result<(), String>,
    ) -> Result<(), String> {
        let header = self.domain.header_bytes();
        while !body.is_empty() {
            if self.partial.is_empty() && body.len() >= header {
                // A whole record inside this body needs no copy.
                let length = self.checked_length(&body[..header])?;
                if body.len() >= length {
                    sink(&body[..length])?;
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
                let length = self.checked_length(&self.partial[..header])?;
                self.partial.reserve_exact(length - self.partial.len());
                self.needed = Some(length);
            }
            let needed = self.needed.expect("a parsed header declares its length");
            let take = (needed - self.partial.len()).min(body.len());
            self.partial.extend_from_slice(&body[..take]);
            body = &body[take..];
            if self.partial.len() == needed {
                sink(&self.partial)?;
                self.partial = Vec::new();
                self.needed = None;
            }
        }
        Ok(())
    }

    /// The stream's End: no record may remain unfinished.
    pub fn finish(&self) -> Result<(), String> {
        if self.partial.is_empty() {
            Ok(())
        } else {
            Err("root stream ended inside an unfinished domain record".to_string())
        }
    }

    fn checked_length(&self, prefix: &[u8]) -> Result<usize, String> {
        let length = self.domain.record_bytes(prefix)?;
        if length > self.max_record_bytes {
            return Err(format!(
                "domain record declares {length} bytes, beyond its {} byte assembly bound",
                self.max_record_bytes
            ));
        }
        Ok(length)
    }
}
