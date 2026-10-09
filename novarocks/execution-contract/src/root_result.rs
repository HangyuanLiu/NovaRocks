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

//! Context-owned root channel contracts. These values do not retain a task
//! registry entry or imply success, physical release, or query-context close.

use crate::identity::TaskIdentity;
use bytes::Bytes;
use novarocks_result_contract::{RootOutputKind, RootProfileId, RootProfileV1};
use std::fmt;
use std::num::NonZeroU64;
use std::time::Duration;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootResultRead {
    root_task: TaskIdentity,
    profile: RootProfileId,
    kind: RootOutputKind,
    wanted: Option<NonZeroU64>,
    consumed: u64,
    max_wait: Duration,
}
impl RootResultRead {
    pub fn try_new(
        root_task: TaskIdentity,
        profile: RootProfileId,
        kind: RootOutputKind,
        wanted: Option<NonZeroU64>,
        consumed: u64,
        max_wait: Duration,
    ) -> Result<Self, RootResultError> {
        if max_wait.is_zero()
            || max_wait > Duration::from_millis(1000)
            || !max_wait.subsec_nanos().is_multiple_of(1_000_000)
        {
            return Err(RootResultError::Wait);
        }
        Ok(Self {
            root_task,
            profile,
            kind,
            wanted,
            consumed,
            max_wait,
        })
    }
    pub const fn root_task(&self) -> TaskIdentity {
        self.root_task
    }
    pub const fn profile(&self) -> RootProfileId {
        self.profile
    }
    pub const fn kind(&self) -> RootOutputKind {
        self.kind
    }
    /// None is ACK-only; it never takes a data position or synthesizes a read.
    pub const fn wanted(&self) -> Option<NonZeroU64> {
        self.wanted
    }
    pub const fn consumed(&self) -> u64 {
        self.consumed
    }
    pub const fn max_wait(&self) -> Duration {
        self.max_wait
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RootResultEnd {
    pub sequence: NonZeroU64,
    /// Checked count of the original root plan's final output rows.
    pub output_rows: u64,
}

/// An immutable data item. Sharing Bytes does not authorize releasing the
/// backing lease: runtime holders must carry the corresponding owner guard.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootResultData {
    kind: RootOutputKind,
    sequence: NonZeroU64,
    body: Bytes,
    end_after_data: Option<RootResultEnd>,
}
impl RootResultData {
    pub fn try_new(
        kind: RootOutputKind,
        sequence: NonZeroU64,
        body: Bytes,
        end_after_data: Option<RootResultEnd>,
    ) -> Result<Self, RootResultError> {
        if matches!(kind, RootOutputKind::CountOnly)
            || body.is_empty()
            || body.len() > RootProfileV1::SEGMENT_BYTES
        {
            return Err(RootResultError::Payload);
        }
        if let Some(end) = end_after_data
            && sequence.get().checked_add(1) != Some(end.sequence.get())
        {
            return Err(RootResultError::EndSequence);
        }
        Ok(Self {
            kind,
            sequence,
            body,
            end_after_data,
        })
    }
    pub const fn kind(&self) -> RootOutputKind {
        self.kind
    }
    pub const fn sequence(&self) -> NonZeroU64 {
        self.sequence
    }
    pub fn body(&self) -> &Bytes {
        &self.body
    }
    pub const fn end_after_data(&self) -> Option<RootResultEnd> {
        self.end_after_data
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RootReadOutcome {
    AckOnly,
    NotReady,
    Retired,
    AwaitTerminalControl,
    Data(RootResultData),
    End(RootResultEnd),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootResultReply {
    pub root_task: TaskIdentity,
    pub profile: RootProfileId,
    pub kind: RootOutputKind,
    /// Returned even when the wanted item is NotReady or already retired.
    pub accepted_consumed: u64,
    pub outcome: RootReadOutcome,
}

impl RootResultReply {
    pub fn validate(&self) -> Result<(), RootResultError> {
        match &self.outcome {
            RootReadOutcome::Data(data)
                if data.kind() != self.kind || data.sequence().get() <= self.accepted_consumed =>
            {
                return Err(RootResultError::Payload);
            }
            RootReadOutcome::End(end) if end.sequence.get() <= self.accepted_consumed => {
                return Err(RootResultError::EndSequence);
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RootResultError {
    Wait,
    Payload,
    EndSequence,
}
impl fmt::Display for RootResultError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Wait => "root-result wait must be whole milliseconds in 1..=1000",
            Self::Payload => "root-result data violates its output kind or profile",
            Self::EndSequence => "root-result adjacent End sequence must follow Data exactly",
        })
    }
}
impl std::error::Error for RootResultError {}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn count_only_has_no_data_and_end_is_adjacent() {
        let seq = NonZeroU64::new(1).unwrap();
        let end = RootResultEnd {
            sequence: NonZeroU64::new(2).unwrap(),
            output_rows: 7,
        };
        assert!(
            RootResultData::try_new(
                RootOutputKind::CountOnly,
                seq,
                Bytes::from_static(b"x"),
                None
            )
            .is_err()
        );
        assert!(
            RootResultData::try_new(RootOutputKind::ClientRows, seq, Bytes::new(), None).is_err()
        );
        assert_eq!(
            RootResultData::try_new(
                RootOutputKind::ClientRows,
                seq,
                Bytes::from_static(b"x"),
                Some(end)
            )
            .unwrap()
            .end_after_data(),
            Some(end)
        );
        assert!(
            RootResultData::try_new(
                RootOutputKind::ClientRows,
                NonZeroU64::new(u64::MAX).unwrap(),
                Bytes::from_static(b"x"),
                Some(end)
            )
            .is_err()
        );
    }
}
