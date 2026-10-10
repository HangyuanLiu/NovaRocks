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

//! Borrowed standard Arrow IPC framing primitives.
//!
//! Callers own stream profiles, resource admission, message order and control
//! observations around the opaque official FlatBuffer verifier. This owner
//! neither decodes arrays nor supplies implicit verifier or allocation limits.

use std::ops::Range;

pub use flatbuffers::VerifierOptions;

pub const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RangeError {
    Overflow,
    OutOfBounds,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrefixError {
    Range(RangeError),
    NotContinuation,
    LengthTooLarge,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContinuationPrefix {
    End { next_offset: usize },
    Metadata { start: usize, len: usize },
}

/// Reads only the fixed continuation prefix, leaving metadata admission to the caller.
pub fn continuation_prefix(bytes: &[u8], offset: usize) -> Result<ContinuationPrefix, PrefixError> {
    let range = checked_range(bytes.len(), offset, 8).map_err(PrefixError::Range)?;
    let prefix = &bytes[range.clone()];
    if prefix[..4] != CONTINUATION_MARKER {
        return Err(PrefixError::NotContinuation);
    }
    let len = usize::try_from(u32::from_le_bytes([
        prefix[4], prefix[5], prefix[6], prefix[7],
    ]))
    .map_err(|_| PrefixError::LengthTooLarge)?;
    if len == 0 {
        Ok(ContinuationPrefix::End {
            next_offset: range.end,
        })
    } else {
        Ok(ContinuationPrefix::Metadata {
            start: range.end,
            len,
        })
    }
}

/// Checks an extent without reading or allocating its contents.
pub fn checked_range(
    total_len: usize,
    start: usize,
    len: usize,
) -> Result<Range<usize>, RangeError> {
    let end = start.checked_add(len).ok_or(RangeError::Overflow)?;
    if end > total_len {
        return Err(RangeError::OutOfBounds);
    }
    Ok(start..end)
}

pub fn metadata_slice(bytes: &[u8], start: usize, len: usize) -> Result<&[u8], RangeError> {
    checked_range(bytes.len(), start, len).map(|range| &bytes[range])
}

/// Uses Arrow's verified borrowed accessor with the caller's exact options.
/// Malformed diagnostics can allocate; this call has no internal control callback.
pub fn verified_message<'a>(
    metadata: &'a [u8],
    options: &VerifierOptions,
) -> Result<arrow_ipc::Message<'a>, flatbuffers::InvalidFlatbuffer> {
    arrow_ipc::root_as_message_with_opts(options, metadata)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InvalidLength;

pub fn nonnegative_length(value: i64) -> Result<usize, InvalidLength> {
    usize::try_from(value).map_err(|_| InvalidLength)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AlignmentError {
    InvalidAlignment,
    Overflow,
}

/// Rounds up to an explicit nonzero power-of-two alignment.
pub fn align_up(offset: usize, alignment: usize) -> Result<usize, AlignmentError> {
    if !alignment.is_power_of_two() {
        return Err(AlignmentError::InvalidAlignment);
    }
    let mask = alignment - 1;
    offset
        .checked_add(mask)
        .map(|value| value & !mask)
        .ok_or(AlignmentError::Overflow)
}

#[cfg(test)]
mod tests;
