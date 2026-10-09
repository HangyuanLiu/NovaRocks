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
use std::num::NonZeroU32;

/// Frozen client-row bounds. A new row prefix and its first payload byte
/// always occupy the same body.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClientRowProfile {
    segment_bytes: usize,
    row_payload_bytes: NonZeroU32,
}

impl ClientRowProfile {
    pub fn try_new(segment_bytes: usize, row_payload_bytes: u32) -> Result<Self, ClientBodyError> {
        if segment_bytes < 5 || row_payload_bytes == 0 || row_payload_bytes > 1 << 30 {
            return Err(ClientBodyError::InvalidProfile);
        }
        Ok(Self {
            segment_bytes,
            row_payload_bytes: NonZeroU32::new(row_payload_bytes).unwrap(),
        })
    }
    pub fn segment_bytes(self) -> usize {
        self.segment_bytes
    }
    pub fn row_payload_bytes(self) -> NonZeroU32 {
        self.row_payload_bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientBodyError {
    InvalidProfile,
    EmptyBody,
    SegmentTooLarge,
    PrefixWithoutPayload,
    InvalidRowLength,
    RowCountOverflow,
    EndInsideRow,
}
impl fmt::Display for ClientBodyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidProfile => "invalid client-row profile",
            Self::EmptyBody => "empty client-row data body",
            Self::SegmentTooLarge => "client-row body exceeds segment limit",
            Self::PrefixWithoutPayload => "client-row prefix has no payload in the same body",
            Self::InvalidRowLength => "client-row length exceeds the frozen profile",
            Self::RowCountOverflow => "client-row count overflow",
            Self::EndInsideRow => "client-row end occurs inside an unfinished row",
        })
    }
}
impl std::error::Error for ClientBodyError {}

/// Validation frontier only. Socket delivery and cumulative ACK are separate
/// owners and must not advance merely because a body validates.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ClientRowStreamCursor {
    remaining: u32,
    completed_rows: u64,
}
impl ClientRowStreamCursor {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn remaining(self) -> u32 {
        self.remaining
    }
    pub fn completed_rows(self) -> u64 {
        self.completed_rows
    }
    pub fn validate_end(self) -> Result<(), ClientBodyError> {
        if self.remaining == 0 {
            Ok(())
        } else {
            Err(ClientBodyError::EndInsideRow)
        }
    }
    /// Validate the entire body before exposing any payload spans. Failure
    /// leaves the caller's frontier unchanged, including malformed suffixes.
    pub fn validate_body<'a>(
        self,
        profile: ClientRowProfile,
        body: &'a [u8],
    ) -> Result<ValidatedClientBody<'a>, ClientBodyError> {
        if body.is_empty() {
            return Err(ClientBodyError::EmptyBody);
        }
        if body.len() > profile.segment_bytes {
            return Err(ClientBodyError::SegmentTooLarge);
        }
        let mut next = self;
        let mut offset = 0;
        while offset < body.len() {
            if next.remaining == 0 {
                if body.len() - offset < 5 {
                    return Err(ClientBodyError::PrefixWithoutPayload);
                }
                let length = u32::from_le_bytes(body[offset..offset + 4].try_into().unwrap());
                if length == 0 || length > profile.row_payload_bytes.get() {
                    return Err(ClientBodyError::InvalidRowLength);
                }
                next.remaining = length;
                offset += 4;
            }
            let n = (next.remaining as usize).min(body.len() - offset);
            offset += n;
            next.remaining -= n as u32;
            if next.remaining == 0 {
                next.completed_rows = next
                    .completed_rows
                    .checked_add(1)
                    .ok_or(ClientBodyError::RowCountOverflow)?;
            }
        }
        Ok(ValidatedClientBody {
            body,
            before: self,
            after: next,
        })
    }
}

#[derive(Debug)]
pub struct ValidatedClientBody<'a> {
    body: &'a [u8],
    before: ClientRowStreamCursor,
    after: ClientRowStreamCursor,
}
impl<'a> ValidatedClientBody<'a> {
    pub fn body(&self) -> &'a [u8] {
        self.body
    }
    pub fn before(&self) -> ClientRowStreamCursor {
        self.before
    }
    pub fn after(&self) -> ClientRowStreamCursor {
        self.after
    }
    pub fn payload_spans(&self) -> impl Iterator<Item = BorrowedPayloadSpan<'a>> {
        PayloadSpans {
            body: self.body,
            offset: 0,
            remaining: self.before.remaining,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BorrowedPayloadSpan<'a> {
    pub starts_row: Option<NonZeroU32>,
    pub bytes: &'a [u8],
    pub completes_row: bool,
}
struct PayloadSpans<'a> {
    body: &'a [u8],
    offset: usize,
    remaining: u32,
}
impl<'a> Iterator for PayloadSpans<'a> {
    type Item = BorrowedPayloadSpan<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.offset == self.body.len() {
            return None;
        }
        let starts_row = if self.remaining == 0 {
            let n = u32::from_le_bytes(self.body[self.offset..self.offset + 4].try_into().unwrap());
            self.offset += 4;
            self.remaining = n;
            NonZeroU32::new(n)
        } else {
            None
        };
        let n = (self.remaining as usize).min(self.body.len() - self.offset);
        let bytes = &self.body[self.offset..self.offset + n];
        self.offset += n;
        self.remaining -= n as u32;
        Some(BorrowedPayloadSpan {
            starts_row,
            bytes,
            completes_row: self.remaining == 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn profile() -> ClientRowProfile {
        ClientRowProfile::try_new(16, 64).unwrap()
    }
    #[test]
    fn new_prefix_requires_payload_in_same_body() {
        let prefix = 3u32.to_le_bytes();
        for n in 1..=4 {
            assert_eq!(
                ClientRowStreamCursor::new()
                    .validate_body(profile(), &prefix[..n])
                    .unwrap_err(),
                ClientBodyError::PrefixWithoutPayload
            );
        }
        let body = [3, 0, 0, 0, b'a'];
        let v = ClientRowStreamCursor::new()
            .validate_body(profile(), &body)
            .unwrap();
        assert_eq!(v.after().remaining(), 2);
        assert_eq!(v.after().validate_end(), Err(ClientBodyError::EndInsideRow));
    }
    #[test]
    fn short_continuations_and_multiple_rows_preserve_payload() {
        let v = ClientRowStreamCursor::new()
            .validate_body(profile(), &[4, 0, 0, 0, b'a'])
            .unwrap();
        let v = v.after().validate_body(profile(), b"b").unwrap();
        assert_eq!(v.after().remaining(), 2);
        let body = [b'c', b'd', 1, 0, 0, 0, b'e'];
        let v = v.after().validate_body(profile(), &body).unwrap();
        let spans: Vec<_> = v.payload_spans().collect();
        assert_eq!(spans.len(), 2);
        assert_eq!(
            spans[0],
            BorrowedPayloadSpan {
                starts_row: None,
                bytes: b"cd",
                completes_row: true
            }
        );
        assert_eq!(spans[1].starts_row.unwrap().get(), 1);
        assert_eq!(spans[1].bytes, b"e");
        assert_eq!(v.after().completed_rows(), 2);
        assert_eq!(v.after().validate_end(), Ok(()));
    }
    #[test]
    fn malformed_suffix_rejects_entire_body() {
        let cursor = ClientRowStreamCursor::new();
        for n in 1..=4 {
            let mut body = vec![1, 0, 0, 0, b'a'];
            body.extend_from_slice(&[1, 0, 0, 0][..n]);
            assert_eq!(
                cursor.validate_body(profile(), &body).unwrap_err(),
                ClientBodyError::PrefixWithoutPayload
            );
            assert_eq!(cursor.completed_rows(), 0);
        }
    }
    #[test]
    fn bounds_and_overflow_are_checked_before_frontier_changes() {
        assert!(ClientRowProfile::try_new(4, 1).is_err());
        assert!(ClientRowProfile::try_new(5, 0).is_err());
        assert!(ClientRowProfile::try_new(5, (1 << 30) + 1).is_err());
        assert_eq!(
            ClientRowStreamCursor::new()
                .validate_body(profile(), &[])
                .unwrap_err(),
            ClientBodyError::EmptyBody
        );
        assert_eq!(
            ClientRowStreamCursor::new()
                .validate_body(profile(), &[0; 17])
                .unwrap_err(),
            ClientBodyError::SegmentTooLarge
        );
        for length in [0u32, 65] {
            let mut body = length.to_le_bytes().to_vec();
            body.push(0);
            assert_eq!(
                ClientRowStreamCursor::new()
                    .validate_body(profile(), &body)
                    .unwrap_err(),
                ClientBodyError::InvalidRowLength
            );
        }
        let cursor = ClientRowStreamCursor {
            remaining: 0,
            completed_rows: u64::MAX,
        };
        assert_eq!(
            cursor
                .validate_body(profile(), &[1, 0, 0, 0, 0])
                .unwrap_err(),
            ClientBodyError::RowCountOverflow
        );
        assert_eq!(cursor.completed_rows(), u64::MAX);
    }
}
